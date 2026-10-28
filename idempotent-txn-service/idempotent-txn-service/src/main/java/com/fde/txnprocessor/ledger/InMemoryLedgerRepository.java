package com.fde.txnprocessor.ledger;

import com.fde.txnprocessor.model.FailureReason;
import com.fde.txnprocessor.model.TransactionRequest;
import com.fde.txnprocessor.model.TransactionType;
import com.fde.txnprocessor.resilience.BusinessRuleException;
import com.fde.txnprocessor.resilience.TransientFailureException;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Commit-or-nothing in-memory ledger. All checks and the pre-commit hook run before any state is
 * touched, so a failure at any point leaves balances and entries exactly as they were.
 * The entry map keyed by transactionId is a second, effect-level idempotency guard.
 */
public final class InMemoryLedgerRepository implements LedgerRepository {
    private final Map<String, BigDecimal> balances = new LinkedHashMap<>();
    private final Map<String, LedgerEntry> entriesByTransactionId = new LinkedHashMap<>();

    @Override
    public synchronized boolean apply(TransactionRequest r, PreCommitHook hook)
            throws TransientFailureException, BusinessRuleException {
        if (entriesByTransactionId.containsKey(r.transactionId())) {
            return false;
        }
        String key = key(r.accountId(), r.currency());
        BigDecimal current = balances.getOrDefault(key, BigDecimal.ZERO);
        BigDecimal updated = r.type() == TransactionType.CREDIT ? current.add(r.amount()) : current.subtract(r.amount());
        if (updated.signum() < 0) {
            throw new BusinessRuleException(FailureReason.INSUFFICIENT_FUNDS, "debit exceeds available balance");
        }

        hook.run(); // last point at which a failure can occur; nothing has been mutated yet

        balances.put(key, updated);
        entriesByTransactionId.put(r.transactionId(), new LedgerEntry(
                r.transactionId(), r.accountId(), r.currency(), r.type(), r.amount(), updated, r.sequenceNumber()));
        return true;
    }

    @Override
    public synchronized BigDecimal balance(String accountId, String currency) {
        return balances.getOrDefault(key(accountId, currency), BigDecimal.ZERO);
    }

    @Override
    public synchronized Map<String, BigDecimal> balances() {
        return new LinkedHashMap<>(balances);
    }

    @Override
    public synchronized List<LedgerEntry> entries() {
        return new ArrayList<>(entriesByTransactionId.values());
    }

    private static String key(String accountId, String currency) {
        return accountId + " " + currency;
    }
}
