package com.fde.txnprocessor.source;

import com.fde.txnprocessor.model.RawRecord;

import java.util.List;

/** Serves one fixed batch, then reports exhaustion. Handy for tests and demos. */
public final class InMemoryTransactionSource implements TransactionSource {
    private List<RawRecord> pending;

    public InMemoryTransactionSource(List<RawRecord> records) {
        this.pending = List.copyOf(records);
    }

    @Override
    public synchronized List<RawRecord> nextBatch() {
        List<RawRecord> out = pending;
        pending = List.of();
        return out;
    }
}
