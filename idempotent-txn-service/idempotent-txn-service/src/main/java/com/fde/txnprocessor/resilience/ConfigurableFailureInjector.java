package com.fde.txnprocessor.resilience;

import java.util.Map;

/** Fails the first N attempts of the configured transactions, then lets them through. */
public final class ConfigurableFailureInjector implements FailureInjector {
    private final Map<String, Integer> failuresByTransaction;

    public ConfigurableFailureInjector(Map<String, Integer> failuresByTransaction) {
        this.failuresByTransaction = Map.copyOf(failuresByTransaction);
    }

    @Override
    public void beforeCommit(String transactionId, int attempt) throws TransientFailureException {
        int failures = failuresByTransaction.getOrDefault(transactionId, 0);
        if (attempt <= failures) {
            throw new TransientFailureException("simulated transient failure on attempt " + attempt);
        }
    }
}
