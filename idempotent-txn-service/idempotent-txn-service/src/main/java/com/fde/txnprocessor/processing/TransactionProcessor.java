package com.fde.txnprocessor.processing;

import com.fde.txnprocessor.ledger.LedgerRepository;
import com.fde.txnprocessor.model.FailureReason;
import com.fde.txnprocessor.model.RawRecord;
import com.fde.txnprocessor.model.TransactionRecord;
import com.fde.txnprocessor.model.TransactionRequest;
import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.observability.EventLog;
import com.fde.txnprocessor.observability.Fields;
import com.fde.txnprocessor.report.ProcessingReport;
import com.fde.txnprocessor.resilience.BusinessRuleException;
import com.fde.txnprocessor.resilience.FailureInjector;
import com.fde.txnprocessor.resilience.RetryPolicy;
import com.fde.txnprocessor.resilience.TransientFailureException;
import com.fde.txnprocessor.store.IdempotencyStore;
import com.fde.txnprocessor.validation.RequestValidator;
import com.fde.txnprocessor.validation.ValidationResult;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Core business logic, free of any input/output concerns.
 *
 * <p>Pipeline per row: validate -> deduplicate (idempotency store) -> sequence gate -> execute with
 * bounded retry -> atomic ledger commit. Every row ends in exactly one disposition and one bad row
 * never stops the batch. All entry points are synchronized: a single logical writer per processor.
 */
public final class TransactionProcessor {
    private static final int MAX_ID_LENGTH = 64;

    private enum Kind { ACCEPTED, DUPLICATE, REJECTED }

    private record Row(int rowNumber, String requestId, String transactionId, Kind kind,
                       TransactionRecord record, String detail, FailureReason reason) {}

    private final RequestValidator validator = new RequestValidator();
    private final IdempotencyStore store;
    private final LedgerRepository ledger;
    private final FailureInjector injector;
    private final RetryPolicy retryPolicy;
    private final SequenceScope scope;
    private final EventLog log;

    private final Map<String, SequenceGate> gates = new LinkedHashMap<>();
    private final List<Row> rows = new ArrayList<>();
    private int retriesPerformed;

    public TransactionProcessor(IdempotencyStore store, LedgerRepository ledger, FailureInjector injector,
                                RetryPolicy retryPolicy, SequenceScope scope, EventLog log) {
        this.store = store;
        this.ledger = ledger;
        this.injector = injector;
        this.retryPolicy = retryPolicy;
        this.scope = scope;
        this.log = log;
    }

    /** Handles one input row. Never throws for bad data: the row is isolated and reported. */
    public synchronized void submit(RawRecord raw) {
        try {
            doSubmit(raw);
        } catch (RuntimeException e) {
            // Safe error handling: log the type only, never the message (it may carry customer data).
            rows.add(new Row(raw.rowNumber(), clip(raw.get("requestId")), clip(raw.get("transactionId")),
                    Kind.REJECTED, null, "unexpected error: " + e.getClass().getSimpleName(), FailureReason.UNEXPECTED_ERROR));
            log.event("ROW_ERROR", Fields.of("row", raw.rowNumber(), "error", e.getClass().getSimpleName()));
        }
    }

    private void doSubmit(RawRecord raw) {
        ValidationResult v = validator.validate(raw);
        if (!v.isValid()) {
            reject(raw, v);
            return;
        }
        TransactionRequest request = v.request();

        IdempotencyStore.Registration reg = store.tryRegister(request, raw.rowNumber());
        if (!reg.created()) {
            duplicate(raw, request, reg);
            return;
        }

        TransactionRecord record = reg.record();
        rows.add(new Row(raw.rowNumber(), request.requestId(), request.transactionId(), Kind.ACCEPTED, record, null, null));
        log.event("RECEIVED", Fields.of("row", raw.rowNumber(), "transactionId", request.transactionId(),
                "requestId", request.requestId(), "sequence", request.sequenceNumber(),
                "account", Fields.mask(request.accountId())));

        SequenceGate gate = gates.computeIfAbsent(gateKey(request.accountId()), k -> new SequenceGate());
        switch (gate.admit(record)) {
            case READY -> {
                execute(record);
                gate.completeCurrent();
                drain(gate);
            }
            case BUFFERED -> {
                record.setStatus(TransactionStatus.PENDING_ORDER);
                log.event("PENDING_ORDER", Fields.of("transactionId", record.transactionId(),
                        "sequence", request.sequenceNumber(), "expectedSequence", gate.nextExpected()));
            }
            case STALE, TAKEN -> fail(record, FailureReason.SEQUENCE_CONFLICT, "sequence slot already used by another transaction");
        }
    }

    private void reject(RawRecord raw, ValidationResult v) {
        String detail = "invalid: " + String.join("; ", v.errors());
        rows.add(new Row(raw.rowNumber(), clip(raw.get("requestId")), clip(raw.get("transactionId")),
                Kind.REJECTED, null, detail, FailureReason.VALIDATION));
        log.event("VALIDATION_FAILED", Fields.of("row", raw.rowNumber(), "errors", String.join("; ", v.errors())));

        // Release the sequence slot so one bad row cannot block every later transaction.
        boolean scopeKnown = scope == SequenceScope.GLOBAL || v.accountId() != null;
        if (v.sequenceNumber() != null && scopeKnown) {
            SequenceGate gate = gates.computeIfAbsent(gateKey(v.accountId()), k -> new SequenceGate());
            gate.skip(v.sequenceNumber());
            log.event("SEQUENCE_SKIPPED", Fields.of("sequence", v.sequenceNumber(), "reason", "INVALID_RECORD"));
            drain(gate);
        }
    }

    private void duplicate(RawRecord raw, TransactionRequest request, IdempotencyStore.Registration reg) {
        TransactionRecord original = reg.record();
        boolean mismatch = !original.request().fingerprint().equals(request.fingerprint());
        String detail = reg.matchedOn() + " of row " + original.rowNumber() + (mismatch ? " (PAYLOAD MISMATCH, ignored)" : "");
        rows.add(new Row(raw.rowNumber(), request.requestId(), request.transactionId(), Kind.DUPLICATE, original, detail, null));
        log.event("DUPLICATE", Fields.of("row", raw.rowNumber(), "transactionId", request.transactionId(),
                "requestId", request.requestId(), "matchedOn", reg.matchedOn(), "originalStatus", original.status().name()));
        if (mismatch) {
            log.event("PAYLOAD_MISMATCH", Fields.of("transactionId", request.transactionId(), "row", raw.rowNumber()));
        }
    }

    private void drain(SequenceGate gate) {
        Optional<TransactionRecord> next;
        while ((next = gate.pollNext()).isPresent()) {
            log.event("RELEASED_FROM_BUFFER", Fields.of("transactionId", next.get().transactionId()));
            execute(next.get());
            gate.completeCurrent();
        }
    }

    /** Runs one transaction to a terminal state (PROCESSED or FAILED) with bounded retries. */
    private void execute(TransactionRecord record) {
        while (true) {
            record.setStatus(TransactionStatus.PROCESSING);
            record.incrementAttempts();
            final int attempt = record.attempts();
            log.event("PROCESSING", Fields.of("transactionId", record.transactionId(), "attempt", attempt));
            try {
                boolean applied = ledger.apply(record.request(),
                        () -> injector.beforeCommit(record.transactionId(), attempt));
                record.markProcessed();
                log.event(applied ? "PROCESSED" : "PROCESSED_LEDGER_ALREADY_APPLIED",
                        Fields.of("transactionId", record.transactionId(), "attempts", attempt));
                return;
            } catch (TransientFailureException e) {
                if (attempt >= retryPolicy.maxAttempts()) {
                    fail(record, FailureReason.RETRY_EXHAUSTED, "gave up after " + attempt + " attempts");
                    return;
                }
                record.setStatus(TransactionStatus.RETRY_PENDING);
                retriesPerformed++;
                log.event("RETRY_SCHEDULED", Fields.of("transactionId", record.transactionId(), "failedAttempt", attempt));
                retryPolicy.pauseAfter(attempt);
            } catch (BusinessRuleException e) {
                fail(record, e.reason(), e.getMessage());
                return;
            } catch (RuntimeException e) {
                fail(record, FailureReason.UNEXPECTED_ERROR, e.getClass().getSimpleName());
                return;
            }
        }
    }

    private void fail(TransactionRecord record, FailureReason reason, String detail) {
        record.markFailed(reason, detail);
        log.event("FAILED", Fields.of("transactionId", record.transactionId(), "reason", reason.name(),
                "attempts", record.attempts(), "detail", detail));
    }

    /**
     * Operational recovery: re-drives a transaction that failed only because retries ran out.
     * Safe to call repeatedly; the ledger's per-transaction guard prevents a second effect.
     */
    public synchronized TransactionStatus reprocess(String transactionId) {
        TransactionRecord record = store.findByTransactionId(transactionId)
                .orElseThrow(() -> new IllegalArgumentException("unknown transaction"));
        if (record.status() != TransactionStatus.FAILED || !record.failureReason().isRecoverable()) {
            log.event("REPROCESS_REJECTED", Fields.of("transactionId", transactionId, "status", record.status().name()));
            return record.status();
        }
        log.event("REPROCESS_REQUESTED", Fields.of("transactionId", transactionId));
        record.resetForReprocess();
        execute(record);
        return record.status();
    }

    /** Ends the batch and builds the report. Buffered transactions stay PENDING_ORDER with their gaps listed. */
    public synchronized ProcessingReport finish() {
        List<ProcessingReport.Line> lines = new ArrayList<>();
        Map<TransactionStatus, Integer> counts = new EnumMap<>(TransactionStatus.class);
        Map<FailureReason, Integer> failures = new EnumMap<>(FailureReason.class);

        for (Row row : rows) {
            TransactionStatus status;
            String detail;
            switch (row.kind()) {
                case DUPLICATE -> {
                    status = TransactionStatus.DUPLICATE;
                    detail = row.detail();
                }
                case REJECTED -> {
                    status = TransactionStatus.FAILED;
                    detail = row.detail();
                    failures.merge(row.reason(), 1, Integer::sum);
                }
                default -> {
                    TransactionRecord r = row.record();
                    status = r.status();
                    detail = describe(r);
                    if (status == TransactionStatus.FAILED) {
                        failures.merge(r.failureReason(), 1, Integer::sum);
                    }
                }
            }
            counts.merge(status, 1, Integer::sum);
            lines.add(new ProcessingReport.Line(row.rowNumber(), row.requestId(), row.transactionId(), status, detail));
        }

        Map<String, List<Long>> missing = new LinkedHashMap<>();
        Map<String, List<String>> pending = new LinkedHashMap<>();
        gates.forEach((key, gate) -> {
            List<String> ids = gate.pendingTransactionIds();
            if (!ids.isEmpty()) {
                pending.put(key, ids);
                missing.put(key, gate.missingSequences());
            }
        });

        log.event("BATCH_COMPLETE", Fields.of("rows", rows.size(), "processed", counts.getOrDefault(TransactionStatus.PROCESSED, 0),
                "duplicates", counts.getOrDefault(TransactionStatus.DUPLICATE, 0),
                "failed", counts.getOrDefault(TransactionStatus.FAILED, 0),
                "pendingOrder", counts.getOrDefault(TransactionStatus.PENDING_ORDER, 0),
                "retries", retriesPerformed));

        return new ProcessingReport(lines, counts, failures, retriesPerformed, missing, pending,
                ledger.balances(), ledger.entries().size());
    }

    private static String describe(TransactionRecord r) {
        return switch (r.status()) {
            case PROCESSED -> "attempts=" + r.attempts();
            case FAILED -> r.failureReason() + " (" + r.failureDetail() + "), attempts=" + r.attempts();
            case PENDING_ORDER -> "waiting for earlier sequence numbers";
            default -> "";
        };
    }

    private String gateKey(String accountId) {
        return scope == SequenceScope.GLOBAL ? "GLOBAL" : accountId;
    }

    private static String clip(String s) {
        if (s == null) {
            return null;
        }
        String cleaned = s.replaceAll("[^A-Za-z0-9._-]", "?");
        return cleaned.length() > MAX_ID_LENGTH ? cleaned.substring(0, MAX_ID_LENGTH) : cleaned;
    }
}
