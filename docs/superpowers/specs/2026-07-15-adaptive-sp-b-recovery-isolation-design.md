# SP-B (part 1) — RecoveryCoordinator per-table failure isolation

**Status:** design approved (Option 2 — suspend the failing table). OSS core, branch `nw_adaptive_commit`.
**Origin:** the one production-robustness finding surfaced by SP-D D1 (W3 multi-table), which the D1
whole-branch review told us to prioritize. Verified real by inspection; not a reproduced runtime bug.

## Problem

`RecoveryCoordinator.recover()` (`RecoveryCoordinator.java:86`) iterates every WAL table and calls
`recoverTable(token, …)` inside a bare `for` loop (lines 102–116) with **no try/catch**. `recoverTable`
does the physical restore of the durable epoch cut — `restoreFile` ×2, `fsyncFile` ×2, `fsyncDir`
(lines 198–206) — and each of those throws `CairoException.critical(ff.errno())` on a **genuine I/O
error** (disk full, EIO, a bad sector under `_txn.epoch`).

Every *logical* bad-state path in `recoverTable` already degrades gracefully by `return`ing → the table
falls through to normal open + full WAL replay (missing `_snapshot` marker :127, un-loadable marker :137,
absent `.epoch` copy :153, torn/mismatched copy :170, stale epoch over a restored `_txn` :185). Only the
**physical-restore I/O throw** is unguarded. When it fires for one table, the exception propagates out of
`recover()` → `CairoEngine.completeInit()`, so:

- every adaptive table **after** the failing one in iteration order is never visited → comes up
  un-rewound (torn ahead of its epoch), or
- boot fails outright.

One table's bad sector can brick a multi-table boot. That is the gap.

## Decision (user-selected): suspend just the failing table

On a genuine I/O error during one table's roll-forward, **catch it, suspend that table, and continue** so
healthy siblings still roll forward. Chosen over skip-and-continue because it matches QuestDB's existing
"suspend the table on a WAL-apply error" idiom — the failing table becomes visibly broken in
`wal_tables()` (`suspended=true` + `errorTag` + `errorMessage`) and is not served or applied until an
operator resolves the I/O condition, rather than silently falling back.

## Mechanism (validated against the code)

Mirror the apply-job suspend idiom at `ApplyWal2TableJob.java:805–840`. `RecoveryCoordinator` is in
package `io.questdb.cairo`, so `ErrorTag` / `CairoException` / `CairoError` need no imports.

```java
try {
    recoverTable(token, src, dst, dir);
} catch (CairoException | CairoError e) {
    // One table's genuine restore I/O error must NOT strand healthy siblings or brick boot.
    // Suspend just this table (idiomatic to WAL-apply error handling) and continue.
    final ErrorTag errorTag;
    final String errorMessage;
    if (e instanceof CairoException ce) {
        errorTag = ErrorTag.resolveTag(ce.getErrno());
        errorMessage = ce.getFlyweightMessage().toString();
    } else {
        errorTag = ErrorTag.NONE;
        errorMessage = e.getMessage();
    }
    LOG.critical().$("adaptive epoch roll-forward failed, table suspended [table=").$(token)
            .$(", error=").$(e).I$();
    try {
        engine.getTableSequencerAPI().suspendTable(token, errorTag, errorMessage);
    } catch (CairoException | CairoError se) {
        LOG.critical().$("could not suspend table after failed roll-forward [table=").$(token)
                .$(", error=").$safe(se instanceof CairoException c ? c.getFlyweightMessage() : se.getMessage())
                .I$();
    }
}
```

Facts that make this correct (each confirmed in the source):

- **Catch scope `CairoException | CairoError`.** `restoreFile`/`fsyncFile` throw
  `CairoException.critical(ff.errno())` (`RecoveryCoordinator.java:339–340,351`); `CairoError` covers a
  defensive SIGBUS. `CrashSimulationError extends Error` (a bare `Error`, **not** `CairoError`), so the
  crash-test harness's simulated crash is **not** caught here — the catch cannot mask it.
- **Boot-time suspend sticks.** `SeqTxnTracker.setSuspended` writes the fields then `suspendedState = -1`
  last (`SeqTxnTracker.java:278–288`). A later lazy `initTxns` CAS's `suspendedState` **from 0**
  (`:184`); once we've set `-1`, that CAS fails and leaves the table suspended into the apply path. The
  per-token `SeqTxnTracker` is a stable singleton (`getTxnTracker` == `getSeqTxnTracker`, `:257–258`) —
  the same object `recover()` already touches for `bumpRecoveryIncarnation()` (`:214`).
- **Fail-safe suspend.** `suspendTable` is itself wrapped (as the apply job does) so that if suspension
  fails we log critical and continue — never re-brick boot from the error handler.

## Operational semantics

Suspend state is **in-memory only** (not persisted). The intended recovery action is a **restart**: on
the next boot `recover()` re-runs, and because the `.epoch` copies are immutable the restore is idempotent
— it completes cleanly once the I/O condition is resolved, or re-suspends the table if it is not. A
partial restore (`_txn` restored, `_cv` restore then failed) leaves the documented "safe skew" (`_txn`
behind `_cv`, no dangling column-version reference — `RecoveryCoordinator.java:193–197`) that next-boot
recovery completes. In the current session the table is suspended, so it is never served or applied with
an inconsistent cut. (`RESUME WAL` without a restart is not the intended recovery path for this condition;
note it in operator guidance.)

## Safety argument

- **No data loss.** The durable WAL is intact; the failing table always has normal open + full WAL replay
  from the durable frontier as its floor. Suspending only *defers serving* until the operator acts.
- **Siblings unaffected.** The loop continues, so every other adaptive table rolls forward regardless of
  iteration order.
- **Cannot re-brick boot.** Both the restore failure and a (theoretical) suspend failure are contained;
  `recover()` always returns normally.

## Test plan (TDD)

Add to `core/src/test/java/io/questdb/test/cairo/RecoveryCoordinatorTest.java` (co-located with the 3
baseline recovery oracles). Use a `FilesFacade` that fails `copy()` with a chosen errno (e.g. `ENOSPC`
→ `DISK_FULL`) for exactly the target table's `_txn.epoch → _txn` restore, passing all other paths
through.

1. **Failing test first.** Build ≥2 adaptive tables, each with a durable epoch + a lazy gap on disk
   (apply K rows epoch-every-batch, then M rows epoch-disabled — the crash-sweep setup, no crash needed;
   live `_txn` sits ahead of the epoch so `recover()` will attempt the restore). Run `recover()` with the
   copy-failing facade. Assert: (a) the target table is **suspended** with `errorTag == DISK_FULL` and a
   non-empty message; (b) **every other** adaptive table is recovered (`recoveryIncarnation` bumped / rows
   rolled back to the epoch then re-derivable); (c) `recover()` returns without throwing. Against the
   unpatched loop this is RED (recover throws; the sibling is stranded).
2. **Apply the fix → GREEN.**
3. **Order independence.** Assert "every table other than the injected one recovered", so the test does
   not depend on `getTableTokens` ordering.
4. **fsync path (optional, same handler).** A facade that fails `fsync` on the restored `_txn` exercises
   the identical catch → suspend.

Regression: the 3 existing `RecoveryCoordinatorTest` oracles + the D1 adaptive crash suite stay green.

## Scope

OSS core only: one method (`recover()`'s loop body) + test coverage. No API changes, no config. Enterprise
inherits it via a submodule bump with no ent-side change (the ent replica/role paths do not call
`recover()` differently). Branch kept as-is per the standing integration/merge exclusion.

## Out of scope (SP-B backlog, not this change)

The other SP-B/D2 items from the D1 findings — `flushPendingDurable` whole-fd fdatasync fidelity at
W > 0, the W5 provisional-partition-dir purge hygiene, and the deferred SP-F metric-guard minors — are
separate and tracked in the ledger.
