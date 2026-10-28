package com.fde.txnprocessor.source;

import com.fde.txnprocessor.model.RawRecord;

import java.io.IOException;
import java.util.List;

/**
 * Replaceable input port. Queue-like on purpose: each call returns the next batch and an empty
 * list means "nothing more right now". A Kafka/SQS/DB-polling adapter implements the same contract.
 */
public interface TransactionSource {
    List<RawRecord> nextBatch() throws IOException;
}
