package com.fde.txnprocessor.model;

/** Why a record ended in FAILED. Only RETRY_EXHAUSTED can be re-driven through reprocess(). */
public enum FailureReason {
    VALIDATION(false),
    INSUFFICIENT_FUNDS(false),
    SEQUENCE_CONFLICT(false),
    RETRY_EXHAUSTED(true),
    UNEXPECTED_ERROR(false);

    private final boolean recoverable;

    FailureReason(boolean recoverable) {
        this.recoverable = recoverable;
    }

    public boolean isRecoverable() {
        return recoverable;
    }
}
