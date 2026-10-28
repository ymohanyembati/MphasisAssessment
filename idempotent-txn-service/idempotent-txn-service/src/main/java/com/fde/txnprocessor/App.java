package com.fde.txnprocessor;

import com.fde.txnprocessor.ledger.InMemoryLedgerRepository;
import com.fde.txnprocessor.observability.StructuredEventLog;
import com.fde.txnprocessor.processing.BatchRunner;
import com.fde.txnprocessor.processing.SequenceScope;
import com.fde.txnprocessor.processing.TransactionProcessor;
import com.fde.txnprocessor.report.ProcessingReport;
import com.fde.txnprocessor.resilience.ConfigurableFailureInjector;
import com.fde.txnprocessor.resilience.RetryPolicy;
import com.fde.txnprocessor.source.CsvTransactionSource;
import com.fde.txnprocessor.store.InMemoryIdempotencyStore;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Command-line entry point.
 *
 * <pre>
 * java -cp target/classes com.fde.txnprocessor.App sample/sample-input.csv \
 *      [--scope=global|per-account] [--max-attempts=3] [--fail=TXN-2005:1]...
 * </pre>
 * The report goes to stdout, structured JSON-lines events to stderr.
 */
public final class App {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: App <input.csv> [--scope=global|per-account] [--max-attempts=N] [--fail=TXN-ID:N]...");
            System.exit(2);
        }
        Path input = Path.of(args[0]);
        SequenceScope scope = SequenceScope.GLOBAL;
        int maxAttempts = 3;
        Map<String, Integer> simulatedFailures = new HashMap<>();

        for (int i = 1; i < args.length; i++) {
            String a = args[i];
            if (a.startsWith("--scope=")) {
                scope = a.substring(8).equalsIgnoreCase("per-account") ? SequenceScope.PER_ACCOUNT : SequenceScope.GLOBAL;
            } else if (a.startsWith("--max-attempts=")) {
                maxAttempts = Integer.parseInt(a.substring(15));
            } else if (a.startsWith("--fail=")) {
                String[] parts = a.substring(7).split(":");
                simulatedFailures.put(parts[0], Integer.parseInt(parts[1]));
            } else {
                System.err.println("unknown option: " + a);
                System.exit(2);
            }
        }

        TransactionProcessor processor = new TransactionProcessor(
                new InMemoryIdempotencyStore(),
                new InMemoryLedgerRepository(),
                new ConfigurableFailureInjector(simulatedFailures),
                new RetryPolicy(maxAttempts, 50, Thread::sleep),
                scope,
                new StructuredEventLog(System.err));

        ProcessingReport report = new BatchRunner(processor).run(new CsvTransactionSource(input));
        System.out.println(report.toText());
    }
}
