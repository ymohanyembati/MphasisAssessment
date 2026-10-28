package com.fde.txnprocessor.ledger;

import com.fde.txnprocessor.model.TransactionType;

import java.math.BigDecimal;

public record LedgerEntry(
        String transactionId,
        String accountId,
        String currency,
        TransactionType type,
        BigDecimal amount,
        BigDecimal balanceAfter,
        long sequenceNumber) {}
