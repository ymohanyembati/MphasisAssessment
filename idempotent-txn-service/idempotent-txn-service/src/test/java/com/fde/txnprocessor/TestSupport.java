package com.fde.txnprocessor;

import com.fde.txnprocessor.ledger.InMemoryLedgerRepository;
import com.fde.txnprocessor.model.RawRecord;
import com.fde.txnprocessor.observability.CollectingEventLog;
import com.fde.txnprocessor.processing.SequenceScope;
import com.fde.txnprocessor.processing.TransactionProcessor;
import com.fde.txnprocessor.resilience.ConfigurableFailureInjector;
import com.fde.txnprocessor.resilience.FailureInjector;
import com.fde.txnprocessor.resilience.RetryPolicy;
import com.fde.txnprocessor.store.InMemoryIdempotencyStore;

import java.util.LinkedHashMap;
import java.util.Map;

/** Builds a fully wired in-memory processor (no sleeping, no I/O) and raw rows. */
final class TestSupport {
    final InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
    final InMemoryLedgerRepository ledger = new InMemoryLedgerRepository();
    final CollectingEventLog log = new CollectingEventLog();
    final TransactionProcessor processor;
    private int row = 1;

    TestSupport() {
        this(Map.of(), 3, SequenceScope.GLOBAL);
    }

    TestSupport(Map<String, Integer> failures, int maxAttempts, SequenceScope scope) {
        this(new ConfigurableFailureInjector(failures), maxAttempts, scope);
    }

    TestSupport(FailureInjector injector, int maxAttempts, SequenceScope scope) {
        processor = new TransactionProcessor(store, ledger, injector, RetryPolicy.noDelay(maxAttempts), scope, log);
    }

    RawRecord row(String txn, String req, String seq, String account, String type, String amount, String currency) {
        Map<String, String> f = new LinkedHashMap<>();
        f.put("transactionId", txn);
        f.put("requestId", req);
        f.put("sequenceNumber", seq);
        f.put("accountId", account);
        f.put("transactionType", type);
        f.put("amount", amount);
        f.put("currency", currency);
        return RawRecord.of(++row, f);
    }

    void submit(String txn, String req, int seq, String account, String type, String amount) {
        processor.submit(row(txn, req, Integer.toString(seq), account, type, amount, "USD"));
    }
}
