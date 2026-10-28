package com.fde.txnprocessor.ledger;

import com.fde.txnprocessor.model.TransactionRequest;
import com.fde.txnprocessor.resilience.BusinessRuleException;
import com.fde.txnprocessor.resilience.TransientFailureException;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/** The business side effect: balances and the append-only ledger. */
public interface LedgerRepository {

    /** Runs just before the commit point; may throw to simulate a failure at the worst moment. */
    @FunctionalInterface
    interface PreCommitHook {
        void run() throws TransientFailureException;
    }

    /**
     * Applies the request atomically: balance update and ledger entry are committed together or not at all.
     *
     * @return true if applied now, false if a ledger entry for this transactionId already existed (no-op)
     */
    boolean apply(TransactionRequest request, PreCommitHook hook) throws TransientFailureException, BusinessRuleException;

    BigDecimal balance(String accountId, String currency);

    /** Balances keyed by "accountId currency", in insertion order. */
    Map<String, BigDecimal> balances();

    List<LedgerEntry> entries();
}
