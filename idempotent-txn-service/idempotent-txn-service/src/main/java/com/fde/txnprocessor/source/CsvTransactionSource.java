package com.fde.txnprocessor.source;

import com.fde.txnprocessor.model.RawRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads a header-based CSV file (simple comma split, no quoted fields). Rows whose column count
 * does not match the header are still emitted, flagged with a parse error, so they are isolated by
 * the validator instead of aborting the batch.
 */
public final class CsvTransactionSource implements TransactionSource {
    private final Path file;
    private boolean consumed;

    public CsvTransactionSource(Path file) {
        this.file = file;
    }

    @Override
    public synchronized List<RawRecord> nextBatch() throws IOException {
        if (consumed) {
            return List.of();
        }
        consumed = true;
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        List<RawRecord> out = new ArrayList<>();
        String[] header = null;
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isBlank()) {
                continue;
            }
            String[] cols = line.split(",", -1);
            if (header == null) {
                header = trimAll(cols);
                continue;
            }
            int rowNumber = i + 1;
            if (cols.length != header.length) {
                out.add(new RawRecord(rowNumber, Map.of(), "row has " + cols.length + " columns, expected " + header.length));
                continue;
            }
            Map<String, String> fields = new LinkedHashMap<>();
            for (int c = 0; c < header.length; c++) {
                fields.put(header[c], cols[c].trim());
            }
            out.add(RawRecord.of(rowNumber, fields));
        }
        return out;
    }

    private static String[] trimAll(String[] cols) {
        String[] out = new String[cols.length];
        for (int i = 0; i < cols.length; i++) {
            out[i] = cols[i].trim();
        }
        return out;
    }
}
