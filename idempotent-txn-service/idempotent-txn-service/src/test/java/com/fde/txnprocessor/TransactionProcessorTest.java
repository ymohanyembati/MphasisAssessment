package com.fde.txnprocessor;

import com.fde.txnprocessor.ledger.LedgerEntry;
import com.fde.txnprocessor.model.FailureReason;
import com.fde.txnprocessor.model.TransactionRecord;
import com.fde.txnprocessor.model.TransactionRequest;
import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.model.TransactionType;
import com.fde.txnprocessor.processing.SequenceScope;
import com.fde.txnprocessor.report.ProcessingReport;
import com.fde.txnprocessor.resilience.FailureInjector;
import com.fde.txnprocessor.resilience.TransientFailureException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransactionProcessorTest {

    private static BigDecimal bd(String v) {
        return new BigDecimal(v);
    }

    private TransactionRecord record(TestSupport t, String id) {
        return t.store.findByTransactionId(id).orElseThrow();
    }

    // ---- the suggested L2 dataset, end to end -------------------------------------------------

    @Test
    void suggestedDatasetProducesExpectedOutcomes() {
        TestSupport t = new TestSupport(Map.of("TXN-2005", 1), 3, SequenceScope.GLOBAL);
        t.submit("TXN-2002", "REQ-2", 2, "ACC-201", "DEBIT", "40");
        t.submit("TXN-2001", "REQ-1", 1, "ACC-201", "CREDIT", "100");
        t.submit("TXN-2002", "REQ-2-DUP", 2, "ACC-201", "DEBIT", "40");
        t.submit("TXN-2003", "REQ-3", 3, "ACC-201", "CREDIT", "25");
        t.submit("TXN-2004", "REQ-4", 4, "ACC-202", "DEBIT", "-10");
        t.submit("TXN-2005", "REQ-5", 5, "ACC-203", "CREDIT", "500");

        ProcessingReport report = t.processor.finish();

        assertEquals(4, report.count(TransactionStatus.PROCESSED));
        assertEquals(1, report.count(TransactionStatus.DUPLICATE));
        assertEquals(1, report.count(TransactionStatus.FAILED));
        assertEquals(0, report.count(TransactionStatus.PENDING_ORDER));
        assertEquals(1, report.retriesPerformed());
        assertEquals(2, record(t, "TXN-2005").attempts());
        assertEquals(0, bd("85").compareTo(t.ledger.balance("ACC-201", "USD")));
        assertEquals(0, bd("500").compareTo(t.ledger.balance("ACC-203", "USD")));
        assertEquals(4, t.ledger.entries().size());
    }

    // ---- duplicates ---------------------------------------------------------------------------

    @Test
    void duplicateDoesNotChangeBalanceOrLedger() {
        TestSupport t = new TestSupport();
        t.submit("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100");
        t.submit("TXN-1", "REQ-1-RETRY", 1, "ACC-1", "CREDIT", "100"); // same txn, new request id
        t.submit("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100");       // exact replay
        ProcessingReport report = t.processor.finish();

        assertEquals(2, report.count(TransactionStatus.DUPLICATE));
        assertEquals(1, t.ledger.entries().size());
        assertEquals(0, bd("100").compareTo(t.ledger.balance("ACC-1", "USD")));
    }

    @Test
    void duplicateArrivingWhileOriginalIsBufferedIsStillDuplicate() {
        TestSupport t = new TestSupport();
        t.submit("TXN-2", "REQ-2", 2, "ACC-1", "CREDIT", "10");      // buffered, waiting for seq 1
        t.submit("TXN-2", "REQ-2-DUP", 2, "ACC-1", "CREDIT", "10");  // replay while pending
        t.submit("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "5");
        ProcessingReport report = t.processor.finish();

        assertEquals(1, report.count(TransactionStatus.DUPLICATE));
        assertEquals(2, report.count(TransactionStatus.PROCESSED));
        assertEquals(0, bd("15").compareTo(t.ledger.balance("ACC-1", "USD")));
    }

    @Test
    void replayWithDifferentPayloadIsIgnoredAndFlagged() {
        TestSupport t = new TestSupport();
        t.submit("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100");
        t.submit("TXN-1", "REQ-1-X", 1, "ACC-1", "CREDIT", "999");
        t.processor.finish();

        assertEquals(0, bd("100").compareTo(t.ledger.balance("ACC-1", "USD")));
        assertEquals(1L, t.log.count("PAYLOAD_MISMATCH"));
    }

    @Test
    void concurrentReplaysProduceExactlyOneEffect() throws Exception {
        TestSupport t = new TestSupport();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            final int n = i;
            futures.add(pool.submit(() -> t.submit("TXN-1", "REQ-" + n, 1, "ACC-1", "CREDIT", "100")));
        }
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        ProcessingReport report = t.processor.finish();
        assertEquals(1, report.count(TransactionStatus.PROCESSED));
        assertEquals(49, report.count(TransactionStatus.DUPLICATE));
        assertEquals(0, bd("100").compareTo(t.ledger.balance("ACC-1", "USD")));
    }

    // ---- ordering -----------------------------------------------------------------------------

    @Test
    void outOfOrderRequestsAreBufferedThenAppliedInSequenceOrder() {
        TestSupport t = new TestSupport();
        t.submit("TXN-3", "REQ-3", 3, "ACC-1", "DEBIT", "20");
        t.submit("TXN-2", "REQ-2", 2, "ACC-1", "DEBIT", "30");
        assertEquals(TransactionStatus.PENDING_ORDER, record(t, "TXN-3").status());
        assertEquals(TransactionStatus.PENDING_ORDER, record(t, "TXN-2").status());

        t.submit("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100"); // unblocks both

        assertEquals(TransactionStatus.PROCESSED, record(t, "TXN-2").status());
        assertEquals(TransactionStatus.PROCESSED, record(t, "TXN-3").status());
        List<String> order = t.ledger.entries().stream().map(LedgerEntry::transactionId).toList();
        assertEquals(List.of("TXN-1", "TXN-2", "TXN-3"), order);
        assertEquals(0, bd("50").compareTo(t.ledger.balance("ACC-1", "USD")));
    }

    @Test
    void debitThatArrivesBeforeItsFundingCreditIsNotRejected() {
        TestSupport t = new TestSupport();
        t.submit("TXN-2", "REQ-2", 2, "ACC-1", "DEBIT", "40");   // would be INSUFFICIENT_FUNDS if run first
        t.submit("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100");
        assertEquals(TransactionStatus.PROCESSED, record(t, "TXN-2").status());
    }

    @Test
    void sequenceGapIsReportedAndLaterTransactionsStayPending() {
        TestSupport t = new TestSupport();
        t.submit("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "10");
        t.submit("TXN-4", "REQ-4", 4, "ACC-1", "CREDIT", "10");
        ProcessingReport report = t.processor.finish();

        assertEquals(1, report.count(TransactionStatus.PENDING_ORDER));
        assertEquals(List.of(2L, 3L), report.missingSequences().get("GLOBAL"));
        assertEquals(List.of("TXN-4"), report.pendingTransactions().get("GLOBAL"));
        assertEquals(0, bd("10").compareTo(t.ledger.balance("ACC-1", "USD")));
    }

    @Test
    void sequenceSlotAlreadyUsedByAnotherTransactionIsAConflict() {
        TestSupport t = new TestSupport();
        t.submit("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "10");
        t.submit("TXN-OTHER", "REQ-X", 1, "ACC-1", "CREDIT", "10");
        TransactionRecord other = record(t, "TXN-OTHER");

        assertEquals(TransactionStatus.FAILED, other.status());
        assertEquals(FailureReason.SEQUENCE_CONFLICT, other.failureReason());
        assertEquals(1, t.ledger.entries().size());
    }

    @Test
    void perAccountScopeKeepsIndependentSequences() {
        TestSupport t = new TestSupport(Map.of(), 3, SequenceScope.PER_ACCOUNT);
        t.submit("TXN-A1", "REQ-A1", 1, "ACC-A", "CREDIT", "10");
        t.submit("TXN-B1", "REQ-B1", 1, "ACC-B", "CREDIT", "20");
        t.submit("TXN-A3", "REQ-A3", 3, "ACC-A", "CREDIT", "1");
        ProcessingReport report = t.processor.finish();

        assertEquals(2, report.count(TransactionStatus.PROCESSED));
        assertEquals(List.of(2L), report.missingSequences().get("ACC-A"));
    }

    // ---- validation / isolation ---------------------------------------------------------------

    @Test
    void invalidRowsAreIsolatedAndDoNotBlockTheBatch() {
        TestSupport t = new TestSupport();
        t.processor.submit(t.row("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "10", "USD"));
        t.processor.submit(t.row("", "REQ-2", "2", "ACC-1", "CREDIT", "10", "USD"));       // missing id
        t.processor.submit(t.row("TXN-3", "REQ-3", "3", "ACC-1", "CREDIT", "abc", "USD")); // non-numeric
        t.processor.submit(t.row("TXN-4", "REQ-4", "4", "ACC-1", "CREDIT", "-5", "USD"));  // negative
        t.processor.submit(t.row("TXN-5", "REQ-5", "5", "ACC-1", "CREDIT", "5", "XX"));    // bad currency
        t.processor.submit(t.row("TXN-6", "REQ-6", "6", "ACC-1", "TRANSFER", "5", "USD")); // bad type
        t.processor.submit(t.row("TXN-7", "REQ-7", "7", "ACC-1", "CREDIT", "7", "USD"));   // valid, after 5 bad rows
        ProcessingReport report = t.processor.finish();

        assertEquals(2, report.count(TransactionStatus.PROCESSED));
        assertEquals(5, report.count(TransactionStatus.FAILED));
        assertEquals(5, (int) report.failuresByReason().get(FailureReason.VALIDATION));
        assertEquals(0, report.count(TransactionStatus.PENDING_ORDER));
        assertEquals(0, bd("17").compareTo(t.ledger.balance("ACC-1", "USD")));
    }

    @Test
    void rejectedRowIsNotRegisteredSoACorrectedResendCanBeProcessed() {
        TestSupport t = new TestSupport();
        t.processor.submit(t.row("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "-10", "USD"));
        assertFalse(t.store.findByTransactionId("TXN-1").isPresent());
        t.processor.submit(t.row("TXN-1", "REQ-1B", "2", "ACC-1", "CREDIT", "10", "USD"));
        // sequence 1 was released by the invalid row, so seq 2 is next in line
        assertEquals(TransactionStatus.PROCESSED, record(t, "TXN-1").status());
    }

    // ---- retries and failure handling ---------------------------------------------------------

    @Test
    void transientFailureIsRetriedAndEffectIsAppliedOnce() {
        TestSupport t = new TestSupport(Map.of("TXN-1", 2), 3, SequenceScope.GLOBAL);
        t.submit("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100");
        TransactionRecord r = record(t, "TXN-1");

        assertEquals(TransactionStatus.PROCESSED, r.status());
        assertEquals(3, r.attempts());
        assertEquals(1, t.ledger.entries().size());
        assertEquals(0, bd("100").compareTo(t.ledger.balance("ACC-1", "USD")));
        assertEquals(2L, t.log.count("RETRY_SCHEDULED"));
    }

    @Test
    void retryExhaustionEndsInFailedWithNoPartialStateAndStreamContinues() {
        TestSupport t = new TestSupport(Map.of("TXN-1", 99), 3, SequenceScope.GLOBAL);
        t.submit("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100");
        t.submit("TXN-2", "REQ-2", 2, "ACC-1", "CREDIT", "7");
        TransactionRecord failed = record(t, "TXN-1");

        assertEquals(TransactionStatus.FAILED, failed.status());
        assertEquals(FailureReason.RETRY_EXHAUSTED, failed.failureReason());
        assertEquals(3, failed.attempts());
        assertEquals(TransactionStatus.PROCESSED, record(t, "TXN-2").status());
        assertEquals(0, bd("7").compareTo(t.ledger.balance("ACC-1", "USD"))); // failed txn left no trace
    }

    @Test
    void exhaustedTransactionCanBeReprocessedExactlyOnce() {
        AtomicBoolean outage = new AtomicBoolean(true);
        FailureInjector injector = (id, attempt) -> {
            if (outage.get()) {
                throw new TransientFailureException("simulated outage");
            }
        };
        TestSupport t = new TestSupport(injector, 3, SequenceScope.GLOBAL);
        t.submit("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "100");
        assertEquals(TransactionStatus.FAILED, record(t, "TXN-1").status());
        assertEquals(0, t.ledger.entries().size());

        outage.set(false); // downstream recovers
        assertEquals(TransactionStatus.PROCESSED, t.processor.reprocess("TXN-1"));
        assertEquals(TransactionStatus.PROCESSED, t.processor.reprocess("TXN-1")); // not eligible any more: no-op
        assertEquals(1, t.ledger.entries().size());
        assertEquals(0, bd("100").compareTo(t.ledger.balance("ACC-1", "USD")));
        assertEquals(1L, t.log.count("REPROCESS_REJECTED"));
    }

    @Test
    void insufficientFundsIsPermanentAndNotRetried() {
        TestSupport t = new TestSupport();
        t.submit("TXN-1", "REQ-1", 1, "ACC-1", "DEBIT", "50");
        TransactionRecord r = record(t, "TXN-1");

        assertEquals(TransactionStatus.FAILED, r.status());
        assertEquals(FailureReason.INSUFFICIENT_FUNDS, r.failureReason());
        assertEquals(1, r.attempts());
        assertEquals(0L, t.log.count("RETRY_SCHEDULED"));
    }

    @Test
    void failureAtCommitPointLeavesLedgerUntouched() {
        TestSupport t = new TestSupport();
        TransactionRequest request = new TransactionRequest(
                "TXN-1", "REQ-1", 1, "ACC-1", TransactionType.CREDIT, bd("100"), "USD");
        boolean thrown = false;
        try {
            t.ledger.apply(request, () -> {
                throw new TransientFailureException("boom");
            });
        } catch (Exception e) {
            thrown = true;
        }
        assertTrue(thrown);
        assertEquals(0, t.ledger.entries().size());
        assertEquals(0, BigDecimal.ZERO.compareTo(t.ledger.balance("ACC-1", "USD")));
    }

    // ---- observability / data handling --------------------------------------------------------

    @Test
    void logsDoNotContainRawAccountIdsOrAmounts() {
        TestSupport t = new TestSupport();
        t.submit("TXN-1", "REQ-1", 1, "ACC-SECRET-12345", "CREDIT", "98765.43");
        t.processor.finish();
        String all = t.log.events().toString();

        assertFalse(all.contains("ACC-SECRET-12345"));
        assertFalse(all.contains("98765.43"));
        assertTrue(all.contains("***345"));
    }

    @Test
    void reportContainsCountsForEveryStatusAndBalances() {
        TestSupport t = new TestSupport();
        t.submit("TXN-1", "REQ-1", 1, "ACC-1", "CREDIT", "10");
        String text = t.processor.finish().toText();

        assertTrue(text.contains("PROCESSED"));
        assertTrue(text.contains("DUPLICATE"));
        assertTrue(text.contains("ACC-1 USD"));
        assertNull(t.processor.finish().missingSequences().get("GLOBAL"));
    }
}
