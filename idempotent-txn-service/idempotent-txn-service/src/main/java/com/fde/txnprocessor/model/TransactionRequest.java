package com.fde.txnprocessor.model;

import java.math.BigDecimal;

/** A validated, immutable transaction request. */
public record TransactionRequest(
        String transactionId,
        String requestId,
        long sequenceNumber,
        String accountId,
        TransactionType type,
        BigDecimal amount,
        String currency) {

    /** Business payload identity, used to detect a replay that carries different content. */
    public String fingerprint() {
        return String.join("|", accountId, type.name(), amount.stripTrailingZeros().toPlainString(),
                currency, Long.toString(sequenceNumber));
    }
}
