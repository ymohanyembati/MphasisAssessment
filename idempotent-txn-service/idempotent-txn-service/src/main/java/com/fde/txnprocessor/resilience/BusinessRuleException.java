package com.fde.txnprocessor.resilience;

import com.fde.txnprocessor.model.FailureReason;

/** A permanent, non-retryable rejection by a business rule. */
public class BusinessRuleException extends Exception {
    private final FailureReason reason;

    public BusinessRuleException(FailureReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public FailureReason reason() {
        return reason;
    }
}
