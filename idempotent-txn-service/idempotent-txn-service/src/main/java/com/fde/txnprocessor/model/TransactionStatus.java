package com.fde.txnprocessor.model;

/**
 * Lifecycle of a transaction.
 *
 * <pre>
 * RECEIVED -> PENDING_ORDER -> PROCESSING -> PROCESSED
 *                                  |  ^
 *                                  v  |
 *                             RETRY_PENDING -> FAILED (retries exhausted)
 * RECEIVED -> PROCESSING -> FAILED (business rule / sequence conflict / unexpected error)
 * </pre>
 *
 * DUPLICATE is a disposition of an input row (a replay), never the state of the original transaction.
 */
public enum TransactionStatus {
    RECEIVED,
    PENDING_ORDER,
    PROCESSING,
    RETRY_PENDING,
    PROCESSED,
    FAILED,
    DUPLICATE
}
