package com.fde.txnprocessor.model;

import java.util.Map;

/**
 * One unvalidated input row, independent of where it came from (CSV, queue, API...).
 *
 * @param rowNumber  position in the source, used for tracing
 * @param fields     raw field values by name
 * @param parseError set by the source when the row could not even be split into fields
 */
public record RawRecord(int rowNumber, Map<String, String> fields, String parseError) {

    public static RawRecord of(int rowNumber, Map<String, String> fields) {
        return new RawRecord(rowNumber, fields, null);
    }

    public String get(String name) {
        String v = fields.get(name);
        return v == null ? null : v.trim();
    }
}
