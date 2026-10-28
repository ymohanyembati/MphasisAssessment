package com.fde.txnprocessor.processing;

import com.fde.txnprocessor.model.RawRecord;
import com.fde.txnprocessor.report.ProcessingReport;
import com.fde.txnprocessor.source.TransactionSource;

import java.io.IOException;
import java.util.List;

/** Glue between an input source and the processor: poll until empty, then report. */
public final class BatchRunner {
    private final TransactionProcessor processor;

    public BatchRunner(TransactionProcessor processor) {
        this.processor = processor;
    }

    public ProcessingReport run(TransactionSource source) throws IOException {
        List<RawRecord> batch;
        while (!(batch = source.nextBatch()).isEmpty()) {
            batch.forEach(processor::submit);
        }
        return processor.finish();
    }
}
