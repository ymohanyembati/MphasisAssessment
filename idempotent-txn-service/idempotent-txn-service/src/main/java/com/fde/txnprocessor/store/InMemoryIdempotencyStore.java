package com.fde.txnprocessor.store;

import com.fde.txnprocessor.model.TransactionRecord;
import com.fde.txnprocessor.model.TransactionRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** In-memory store. Methods are synchronized so check-and-register is atomic. */
public final class InMemoryIdempotencyStore implements IdempotencyStore {
    private final Map<String, TransactionRecord> byTransactionId = new LinkedHashMap<>();
    private final Map<String, String> transactionIdByRequestId = new HashMap<>();

    @Override
    public synchronized Registration tryRegister(TransactionRequest request, int rowNumber) {
        String owner = transactionIdByRequestId.get(request.requestId());
        if (owner != null) {
            return new Registration(false, byTransactionId.get(owner), "REQUEST_ID_REPLAY");
        }
        TransactionRecord existing = byTransactionId.get(request.transactionId());
        if (existing != null) {
            // Same business transaction under a new requestId: remember the alias so later replays hit the first check.
            transactionIdByRequestId.put(request.requestId(), existing.transactionId());
            return new Registration(false, existing, "TRANSACTION_ID_REPLAY");
        }
        TransactionRecord created = new TransactionRecord(request, rowNumber);
        byTransactionId.put(request.transactionId(), created);
        transactionIdByRequestId.put(request.requestId(), request.transactionId());
        return new Registration(true, created, null);
    }

    @Override
    public synchronized Optional<TransactionRecord> findByTransactionId(String transactionId) {
        return Optional.ofNullable(byTransactionId.get(transactionId));
    }

    @Override
    public synchronized Collection<TransactionRecord> all() {
        List<TransactionRecord> copy = new ArrayList<>(byTransactionId.values());
        return copy;
    }
}
