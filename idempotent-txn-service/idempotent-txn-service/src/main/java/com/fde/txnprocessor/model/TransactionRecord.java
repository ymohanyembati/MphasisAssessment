package com.fde.txnprocessor.model;

import java.time.Instant;

/** Mutable processing state of one unique business transaction. Mutated only by the processor. */
public final class TransactionRecord {
    private final TransactionRequest request;
    private final int rowNumber;
    private volatile TransactionStatus status = TransactionStatus.RECEIVED;
    private volatile int attempts;
    private volatile FailureReason failureReason;
    private volatile String failureDetail;
    private volatile Instant updatedAt = Instant.now();

    public TransactionRecord(TransactionRequest request, int rowNumber) {
        this.request = request;
        this.rowNumber = rowNumber;
    }

    public TransactionRequest request() { return request; }
    public String transactionId() { return request.transactionId(); }
    public int rowNumber() { return rowNumber; }
    public TransactionStatus status() { return status; }
    public int attempts() { return attempts; }
    public FailureReason failureReason() { return failureReason; }
    public String failureDetail() { return failureDetail; }
    public Instant updatedAt() { return updatedAt; }

    public void setStatus(TransactionStatus s) {
        this.status = s;
        this.updatedAt = Instant.now();
    }

    public void incrementAttempts() { attempts++; }

    public void markProcessed() {
        this.failureReason = null;
        this.failureDetail = null;
        setStatus(TransactionStatus.PROCESSED);
    }

    public void markFailed(FailureReason reason, String detail) {
        this.failureReason = reason;
        this.failureDetail = detail;
        setStatus(TransactionStatus.FAILED);
    }

    public void resetForReprocess() {
        this.attempts = 0;
        this.failureReason = null;
        this.failureDetail = null;
        setStatus(TransactionStatus.RECEIVED);
    }
}
