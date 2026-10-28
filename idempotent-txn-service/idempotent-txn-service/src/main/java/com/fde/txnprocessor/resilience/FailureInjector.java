package com.fde.txnprocessor.resilience;

/** Test/demo seam for simulating transient faults right before the ledger commit. */
@FunctionalInterface
public interface FailureInjector {
    void beforeCommit(String transactionId, int attempt) throws TransientFailureException;

    static FailureInjector none() {
        return (id, attempt) -> { };
    }
}
