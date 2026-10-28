# Idempotent Background Transaction Processing Service (FDE L2)

A small, dependency-free Java service that processes transaction requests which may arrive **out of order**,
**duplicated**, or **fail transiently**, and still produces exactly one correct business outcome per transaction.

- Idempotent processing + duplicate/replay detection (by `transactionId` and `requestId`)
- Out-of-order handling via **sequence-gated buffering** (Option A), with missing sequences reported
- Bounded retries with exponential backoff; no duplicate effect after retry; `FAILED` after exhaustion; operator `reprocess`
- Atomic ledger commit (balance + ledger entry together or not at all)
- Structured JSON-lines logs + an end-of-run processing report with counts by status
- 26 unit tests covering duplicate, out-of-order, invalid, retry, concurrency and data-handling scenarios

Design rationale, trade-offs, AI usage and production hardening notes: see **[DESIGN.md](DESIGN.md)**.

## Requirements

Java 17+ (developed on 21). Maven 3.8+ for the build/test commands. No runtime dependencies.

## Build and test

```bash
mvn test            # runs the 26 JUnit 5 tests
mvn package         # builds target/idempotent-txn-service-1.0.0.jar
```

## Run

Input is a header-based CSV (`transactionId,requestId,sequenceNumber,accountId,transactionType,amount,currency`).
The report prints to **stdout**, structured events to **stderr**.

```bash
# The suggested L2 dataset; TXN-2005 fails once (transient) and succeeds on retry
java -jar target/idempotent-txn-service-1.0.0.jar sample/sample-input.csv --fail=TXN-2005:1

# Edge cases: gaps, replays, payload mismatch, insufficient funds, retry exhaustion, malformed rows
java -jar target/idempotent-txn-service-1.0.0.jar sample/sample-input-edge-cases.csv --fail=TXN-3005:5
```

Options: `--scope=global|per-account` (default `global`), `--max-attempts=N` (default 3), `--fail=TXN-ID:N` (repeatable;
fail the first N attempts of that transaction to simulate transient faults).

Without Maven:

```bash
javac -d out $(find src/main -name '*.java')
java -cp out com.fde.txnprocessor.App sample/sample-input.csv --fail=TXN-2005:1
```

## Sample input and output

Captured output is in [`sample/output/`](sample/output): `*.report.txt` (stdout) and `*.events.jsonl` (stderr).
Abridged result for the suggested dataset:

```
Input rows            : 6
  PROCESSED           : 4
  FAILED              : 1      (VALIDATION: amount must be greater than zero)
  DUPLICATE           : 1      (TXN-2002 replayed as REQ-2-DUP)
Retries performed     : 1      (TXN-2005 succeeded on attempt 2)
Final balances        : ACC-201 USD 85, ACC-203 USD 500
```

TXN-2002 (seq 2) arrived before TXN-2001 (seq 1): it was buffered as `PENDING_ORDER`, then released and applied
right after TXN-2001, so the debit never ran against an unfunded account.

## Project layout

```
src/main/java/com/fde/txnprocessor/
  model/          request, record, status/failure enums (no logic about I/O)
  validation/     RequestValidator: pure field validation
  source/         TransactionSource port + CSV and in-memory adapters
  store/          IdempotencyStore port + in-memory adapter (atomic register-or-replay)
  ledger/         LedgerRepository port + in-memory adapter (atomic commit, per-txn guard)
  resilience/     RetryPolicy, transient/business exceptions, failure injector
  processing/     TransactionProcessor (core rules), SequenceGate, BatchRunner
  observability/  EventLog, JSON-lines logger, masking helper
  report/         ProcessingReport (counts by status, gaps, balances)
  App.java        CLI entry point
src/test/java/    JUnit 5 tests
sample/           sample inputs and captured outputs
```
