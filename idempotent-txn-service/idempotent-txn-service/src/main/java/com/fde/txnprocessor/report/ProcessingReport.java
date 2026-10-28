package com.fde.txnprocessor.report;

import com.fde.txnprocessor.model.FailureReason;
import com.fde.txnprocessor.model.TransactionStatus;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/** Immutable end-of-run summary: counts by status, per-row outcomes, gaps and final balances. */
public record ProcessingReport(
        List<Line> lines,
        Map<TransactionStatus, Integer> countsByStatus,
        Map<FailureReason, Integer> failuresByReason,
        int retriesPerformed,
        Map<String, List<Long>> missingSequences,
        Map<String, List<String>> pendingTransactions,
        Map<String, BigDecimal> balances,
        int ledgerEntries) {

    public record Line(int row, String requestId, String transactionId, TransactionStatus status, String detail) {}

    public int count(TransactionStatus status) {
        return countsByStatus.getOrDefault(status, 0);
    }

    public String toText() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Processing Summary ===\n");
        sb.append(String.format("Input rows            : %d%n", lines.size()));
        for (TransactionStatus s : TransactionStatus.values()) {
            sb.append(String.format("  %-18s: %d%n", s, count(s)));
        }
        if (!failuresByReason.isEmpty()) {
            sb.append("Failures by reason    :\n");
            failuresByReason.forEach((r, n) -> sb.append(String.format("  %-18s: %d%n", r, n)));
        }
        sb.append(String.format("Retries performed     : %d%n", retriesPerformed));
        sb.append(String.format("Ledger entries        : %d%n", ledgerEntries));

        sb.append("\n=== Per-row outcome ===\n");
        sb.append(String.format("%-4s %-12s %-14s %-14s %s%n", "ROW", "TRANSACTION", "REQUEST", "STATUS", "DETAIL"));
        for (Line l : lines) {
            sb.append(String.format("%-4d %-12s %-14s %-14s %s%n", l.row(),
                    l.transactionId() == null ? "-" : l.transactionId(),
                    l.requestId() == null ? "-" : l.requestId(), l.status(), l.detail()));
        }

        sb.append("\n=== Final balances ===\n");
        if (balances.isEmpty()) {
            sb.append("(none)\n");
        }
        balances.forEach((k, v) -> sb.append(String.format("%-16s %s%n", k, v.toPlainString())));

        if (!pendingTransactions.isEmpty()) {
            sb.append("\n=== Still pending (ordering gaps) ===\n");
            pendingTransactions.forEach((scope, ids) -> sb.append(String.format(
                    "scope=%s waiting=%s missingSequences=%s%n", scope, ids, missingSequences.get(scope))));
        }
        return sb.toString();
    }
}
