package com.fde.txnprocessor.store;

import com.fde.txnprocessor.model.TransactionRecord;
import com.fde.txnprocessor.model.TransactionRequest;

import java.util.Collection;
import java.util.Optional;

/**
 * Deduplication + transaction state store. In production this is a table with unique constraints on
 * transactionId and requestId; {@link #tryRegister} maps to a single atomic INSERT.
 */
public interface IdempotencyStore {

    /**
     * Atomically registers a new transaction, or reports the existing one.
     *
     * @param created  true when this call created the record
     * @param record   the new record, or the original transaction when this is a replay
     * @param matchedOn why it was a replay (REQUEST_ID_REPLAY / TRANSACTION_ID_REPLAY), null if created
     */
    record Registration(boolean created, TransactionRecord record, String matchedOn) {}

    Registration tryRegister(TransactionRequest request, int rowNumber);

    Optional<TransactionRecord> findByTransactionId(String transactionId);

    Collection<TransactionRecord> all();
}
