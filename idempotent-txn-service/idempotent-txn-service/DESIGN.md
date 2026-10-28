# Design Note: Idempotent Background Transaction Processing Service

## 1. Problem and assumptions

Transaction requests arrive from an unreliable upstream: out of order, duplicated, and retried after partial failures.
The service must apply each business transaction to the ledger exactly once and report clearly on everything else.

Assumptions (stated so they can be challenged):

- `transactionId` is the business idempotency key; `requestId` is the delivery/retry key. Either one repeating means "replay".
- `sequenceNumber` is **contiguous from 1**. The suggested test data uses one sequence across all accounts (ACC-201 gets 1-3, ACC-202 gets 4, ACC-203 gets 5), so the default scope is `GLOBAL`. `PER_ACCOUNT` is a switch.
- Only `CREDIT` and `DEBIT` are modelled. A debit may not take a balance below zero.
- Persistence is in-memory behind interfaces; this is a deliberate assessment-scope choice, not the production design.

## 2. Architecture

```
TransactionSource --> BatchRunner --> TransactionProcessor --> LedgerRepository
 (CSV / in-memory)                      |   validate (RequestValidator)
                                        |   deduplicate (IdempotencyStore)
                                        |   order (SequenceGate)
                                        |   retry (RetryPolicy + FailureInjector)
                                        +-> EventLog, ProcessingReport
```

Business rules live in `TransactionProcessor`, `RequestValidator` and `SequenceGate`. Everything that touches the outside
world (input, state store, ledger, logging) sits behind a small interface, so a queue consumer or database-backed store
replaces an adapter without touching the rules. Input handling never contains business logic.

## 3. Status lifecycle

`RECEIVED -> PENDING_ORDER -> PROCESSING -> PROCESSED`, with `PROCESSING -> RETRY_PENDING -> PROCESSING` on a transient
failure and `FAILED` as the other terminal state. `DUPLICATE` is the disposition of a replayed **input row**; the original
transaction keeps its own status. The report therefore counts rows by status, and the original is never overwritten by a replay.

## 4. Idempotency and duplicate detection

1. `IdempotencyStore.tryRegister` is a single atomic "register or return the existing record" step. It checks `requestId`
   first (exact replay), then `transactionId` (same business transaction under a new request id), and records the alias so
   later replays are caught by the first check. In a database this is one `INSERT` against unique constraints.
2. Detection does not depend on arrival position: a replay is caught whether the original is `PROCESSED`, buffered, retrying or failed.
3. A replay whose payload differs from the original (different amount, account, etc.) is still ignored, but logged as `PAYLOAD_MISMATCH`
   so a human can look at it. Silently applying either version would be dangerous.
4. **Defence in depth:** the ledger independently refuses a second entry for the same `transactionId`. If a crash happens after
   the ledger commit but before the status update, the retry is a safe no-op and the event `PROCESSED_LEDGER_ALREADY_APPLIED` is logged.
5. Rejected (invalid) rows are not registered, so a corrected resend is accepted rather than treated as a duplicate.

## 5. Out-of-order handling (Option A: sequence-based)

`SequenceGate` tracks the next expected sequence. An early arrival is buffered as `PENDING_ORDER`; when the gap fills, the buffer is
drained in order. This matters for correctness, not just tidiness: in the suggested data, TXN-2002 (debit 40) arrives before the
credit that funds it, and executing it immediately would wrongly fail it.

- **Missing sequences** are reported at the end of the run, with the transactions still waiting.
- **Invalid rows release their slot** (when their sequence field is readable) so one bad row cannot stall everything behind it.
  This is required for the suggested data (TXN-2004 is invalid, TXN-2005 must still process). A row whose sequence cannot be read
  cannot release anything, and the resulting gap is reported rather than guessed.
- **Failed transactions consume their slot** (see section 6), so later transactions proceed.
- A different transaction claiming an already-used slot fails as `SEQUENCE_CONFLICT`.

Trade-off: strict ordering gives deterministic, auditable results but a missing event holds back everything after it. Option C
(latest-state wins) tolerates gaps but is wrong for a ledger where every credit and debit must be applied. A production system would
add a gap timeout that alerts and lets an operator skip or backfill.

## 6. Retry and failure handling

- Transient failures are simulated at the worst moment: **after** all checks, **just before** the ledger commit.
- `RetryPolicy` allows a bounded number of attempts (default 3) with exponential backoff (injectable sleeper, so tests are instant).
- Retrying cannot double-apply: nothing is committed on a failed attempt, and the ledger guard covers the crash-after-commit case.
- After the last attempt the record becomes `FAILED / RETRY_EXHAUSTED`, with attempt count and reason retained.
- Business failures (`INSUFFICIENT_FUNDS`) are permanent and are never retried.
- `reprocess(transactionId)` re-drives only `RETRY_EXHAUSTED` records once the downstream recovers. It is repeatable with no extra effect.
- Unexpected exceptions are contained per row (`UNEXPECTED_ERROR`); the batch always completes.

Trade-off: a failed transaction frees its sequence slot, so later transactions on the same account may run before it is reprocessed.
That is acceptable for commutative credits/debits (the balance is the same), but a stricter domain would halt the account until resolved.

## 7. Data consistency

`InMemoryLedgerRepository.apply` performs every check and runs the pre-commit hook **before** mutating anything, then updates the
balance and appends the ledger entry in one synchronized step. There is no partially-applied state to compensate. A test asserts that a
failure at the commit point leaves balances and entries untouched. In a database this is one transaction.

## 8. Observability

- **Structured logs:** one JSON object per line (`RECEIVED`, `PENDING_ORDER`, `PROCESSING`, `RETRY_SCHEDULED`, `PROCESSED`, `DUPLICATE`,
  `FAILED`, `SEQUENCE_SKIPPED`, `PAYLOAD_MISMATCH`, `BATCH_COMPLETE`, ...), each carrying transactionId, attempt and reason.
- **Processing report:** counts by status, failures by reason, retries performed, per-row outcome, still-pending transactions with their
  missing sequences, and final balances.
- Account ids are masked in logs and amounts are never logged.

## 9. Security and data handling

Input is validated field by field (format, range, ISO 4217 currency, precision) and error messages name fields without echoing values.
No secrets exist in the code. Error handling logs exception types only, never messages that might carry customer data. Identifiers shown for
rejected rows are sanitised.

## 10. AI-assisted development

> Review and edit this section so it reflects your own workflow before submitting; you must be able to explain it.

How AI assistance was used: to scaffold the module layout, draft the validator/processor/ledger and the test suite from the problem
statement, and to generate sample data. How it was validated rather than trusted:

- Compiled and ran everything; executed both sample files end to end and read the actual output against the expected outcomes.
- Wrote tests for each acceptance criterion, then investigated every failure instead of adjusting expectations blindly. One test failure
  exposed a real design point (a malformed row with an unreadable sequence cannot release its slot), which is now documented behaviour and the
  test data was corrected, not the code. Another caught an ambiguous-overload bug in the tests themselves.
- Reviewed the invariants by hand: atomic register-or-replay, effect-level guard, no mutation before commit, one writer per processor.

## 11. Production hardening recommendations

| Area | Recommendation |
| --- | --- |
| Persistence | Replace in-memory stores with a database: unique constraints on `transactionId`/`requestId`, ledger and state update in one transaction (or transactional outbox). |
| Concurrency | The processor is single-writer. Scale by partitioning on sequence scope (account or key) so each partition has one consumer; use row-level locks or optimistic versioning on balances. |
| Queue semantics | Assume at-least-once delivery: acknowledge only after the commit; use a dead-letter queue for `FAILED` records, with `reprocess` as the replay path. |
| Ordering | Add a gap timeout with alert and operator skip/backfill; persist the gate (`nextExpected`, buffer) so a restart resumes correctly. |
| Retries | Add jitter, a circuit breaker for downstream outages, and move retries off the hot path so one slow record does not block its scope. |
| Monitoring | Export metrics (processed, duplicate, failed, retry, pending-age, gap-count) and alert on `RETRY_EXHAUSTED`, `PAYLOAD_MISMATCH` and stuck gaps; add trace/correlation ids. |
| Security | Authenticated ingestion, encrypted storage, PII-safe logging reviews, secrets from a vault. |
| Domain | Add `REVERSAL` (reference the original, allow once), multi-currency policy, and idempotency-key expiry/retention. |

## 12. Known limitations

In-memory only (state is lost on exit); CSV parsing is a simple comma split without quoted fields; one account's failure frees its
sequence slot (see section 6); a gap that never fills leaves later transactions pending by design; `REVERSAL` is not implemented.
