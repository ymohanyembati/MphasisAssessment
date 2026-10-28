package com.fde.txnprocessor.validation;

import com.fde.txnprocessor.model.TransactionRequest;

import java.util.List;

/**
 * @param request        the validated request, or null when invalid
 * @param errors         field-level messages (never echo raw values: they may be sensitive)
 * @param sequenceNumber the sequence if that field alone was valid, so an invalid row can release its slot
 * @param accountId      the account if that field alone was valid (needed for per-account sequencing)
 */
public record ValidationResult(TransactionRequest request, List<String> errors, Long sequenceNumber, String accountId) {
    public boolean isValid() {
        return request != null;
    }
}
