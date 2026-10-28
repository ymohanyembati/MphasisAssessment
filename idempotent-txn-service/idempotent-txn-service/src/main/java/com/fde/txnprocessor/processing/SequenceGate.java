package com.fde.txnprocessor.processing;

import com.fde.txnprocessor.model.TransactionRecord;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Admits transactions strictly in sequence order. Early arrivals wait in a buffer; sequence slots
 * released by rejected/invalid rows are skipped so they cannot block the stream forever.
 * Not thread-safe: the processor serialises access.
 */
final class SequenceGate {
    enum Admission { READY, BUFFERED, STALE, TAKEN }

    private long next = 1;
    private final TreeMap<Long, TransactionRecord> pending = new TreeMap<>();
    private final Set<Long> skipped = new HashSet<>();

    long nextExpected() {
        return next;
    }

    Admission admit(TransactionRecord record) {
        long seq = record.request().sequenceNumber();
        if (seq < next || skipped.contains(seq)) {
            return Admission.STALE;      // slot already consumed by another transaction
        }
        if (pending.containsKey(seq)) {
            return Admission.TAKEN;      // another transaction is already waiting on this slot
        }
        if (seq == next) {
            return Admission.READY;
        }
        pending.put(seq, record);
        return Admission.BUFFERED;
    }

    /** Called after the READY/drained transaction reached a terminal state: its slot is consumed. */
    void completeCurrent() {
        next++;
    }

    /** Releases a slot that will never carry a transaction (e.g. the row was invalid). */
    void skip(long seq) {
        if (seq < next || pending.containsKey(seq)) {
            return;
        }
        if (seq == next) {
            next++;
        } else {
            skipped.add(seq);
        }
    }

    /** Next buffered transaction if it is now in order, advancing over skipped slots. */
    Optional<TransactionRecord> pollNext() {
        while (skipped.remove(next)) {
            next++;
        }
        return Optional.ofNullable(pending.remove(next));
    }

    List<Long> missingSequences() {
        List<Long> missing = new ArrayList<>();
        if (pending.isEmpty()) {
            return missing;
        }
        for (long s = next; s < pending.lastKey(); s++) {
            if (!pending.containsKey(s) && !skipped.contains(s)) {
                missing.add(s);
            }
        }
        return missing;
    }

    List<String> pendingTransactionIds() {
        List<String> ids = new ArrayList<>();
        pending.values().forEach(r -> ids.add(r.transactionId()));
        return ids;
    }
}
