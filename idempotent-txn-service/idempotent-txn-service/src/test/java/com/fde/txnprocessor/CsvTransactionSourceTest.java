package com.fde.txnprocessor;

import com.fde.txnprocessor.model.RawRecord;
import com.fde.txnprocessor.processing.BatchRunner;
import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.report.ProcessingReport;
import com.fde.txnprocessor.source.CsvTransactionSource;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CsvTransactionSourceTest {

    @Test
    void readsRowsSkipsBlanksFlagsMalformedRowsAndThenReportsExhaustion() throws Exception {
        Path file = Files.createTempFile("txns", ".csv");
        Files.writeString(file, String.join("\n",
                "transactionId,requestId,sequenceNumber,accountId,transactionType,amount,currency",
                "TXN-1,REQ-1,1,ACC-1,CREDIT,10,USD",
                "",
                "TXN-2,REQ-2,2",
                "TXN-3,REQ-3,3,ACC-1,CREDIT,5,USD"));
        CsvTransactionSource source = new CsvTransactionSource(file);

        List<RawRecord> batch = source.nextBatch();
        assertEquals(3, batch.size());
        assertEquals(2, batch.get(0).rowNumber());
        assertEquals(4, batch.get(1).rowNumber());
        assertNull(batch.get(0).parseError());
        assertTrue(batch.get(1).parseError().contains("columns"));
        assertTrue(source.nextBatch().isEmpty());
        Files.deleteIfExists(file);
    }

    @Test
    void batchRunnerProcessesCsvEndToEndAndIsolatesTheMalformedRow() throws Exception {
        Path file = Files.createTempFile("txns", ".csv");
        Files.writeString(file, String.join("\n",
                "transactionId,requestId,sequenceNumber,accountId,transactionType,amount,currency",
                "TXN-1,REQ-1,1,ACC-1,CREDIT,10,USD",
                "TXN-2,REQ-2,2",                      // malformed: its sequence is unreadable, so no slot is released
                "TXN-3,REQ-3,2,ACC-1,CREDIT,5,USD"));  // the next valid row still processes
        TestSupport t = new TestSupport();

        ProcessingReport report = new BatchRunner(t.processor).run(new CsvTransactionSource(file));

        assertEquals(2, report.count(TransactionStatus.PROCESSED));
        assertEquals(1, report.count(TransactionStatus.FAILED));
        Files.deleteIfExists(file);
    }
}
