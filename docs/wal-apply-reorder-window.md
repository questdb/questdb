# WAL Apply Reorder Window

Status: proposed

Scope: WAL-table apply scheduling in QuestDB core

## 1. Summary

QuestDB already reorders rows efficiently when several WAL transactions are
available to `WalApply` at the same time. `WalTxnDetails` looks ahead over the
available sequencer transactions, and `TableWriter` can sort and apply a block
of transactions as one O3 operation.

The missing behavior is temporal: `WalApply` does not wait for a later
transaction. It always forces the newest transaction it can currently observe
to a full commit. If the apply worker catches up between two client batches, a
later batch with older timestamps causes another O3 merge. If both batches
happen to be queued before the worker runs, the existing block path can merge
them together. Apply performance therefore depends on a scheduling race and
on client batch size.

This design adds an optional, fixed wall-clock reorder window before WAL data
enters `TableWriter`. During the window, transactions remain durable and
unmodified in WAL. When the window expires, `WalApply` releases the table's
pending backlog to the existing look-ahead and block-sort implementation.
Transactions that arrive while the released backlog is being dequeued may
join the same apply run; the existing per-table time quota remains the
fairness boundary.

The design deliberately does not add another row sorter, split WAL
transactions, retain the live tip in `TableWriter` lag, or impose a second
transaction horizon on the current apply cursor.

## 2. Motivation

Some producers generate several streams for the same table at different
frequencies. For example, a high-frequency symbol may produce a batch before a
lower-frequency symbol produces rows whose designated timestamps overlap or
precede that batch.

A client can avoid repeated O3 work by holding the faster stream until the
slower stream catches up and then sending both together. This has several
problems:

- Every client and protocol must implement the same policy.
- Independent clients cannot coordinate their batches.
- A small change in batch size can expose or hide the problem.
- The server already has table-wide ordering information that clients do not.
- The effectiveness of the workaround depends on when the apply worker runs.

QuestDB accepts out-of-order WAL data correctly today. This proposal is not a
correctness fix for query ordering. It targets repeated partition rewrites,
write amplification, and unstable apply throughput when disorder crosses WAL
transaction boundaries.

The policy belongs to the WAL apply subsystem rather than an ingestion
protocol. The table sequencer provides a single order across all WAL writers
and protocols, while `WalApply` already owns transaction look-ahead, DDL
barriers, memory-pressure limits, and O3 block application.

## 3. Current behavior

### 3.1 WAL transaction metadata

The sequencer supports two on-disk transaction-log formats:

- V1 stores structure version, WAL and segment coordinates, segment
  transaction, and sequencer commit timestamp.
- V2 additionally stores transaction minimum and maximum designated
  timestamps and row count.

V1 is the default for newly created tables:
`TableTransactionLog.createTxnLogFileInstance()` selects V2 only when
`cairo.default.sequencer.part.txn.count` is positive, whose default is zero.
The selected version is written to the sequencer-log header and is used when
that table is reopened. There is no automatic V1-to-V2 conversion.

`TableSequencerImpl.nextTxn()` assigns the table-wide sequencer transaction.
The commit timestamp and WAL coordinates are available through
`TransactionLogCursor` on both versions. Calling
`getTxnRowCount()`, `getTxnMinTimestamp()`, or
`getTxnMaxTimestamp()` on a V1 cursor throws
`UnsupportedOperationException`; code using those summaries must first check
`getVersion()`.

The exact WAL transaction type, designated timestamp range, row range,
out-of-order flag, deduplication mode, and replace-range bounds live in the WAL
event file. `WalTxnDetails.readObservableTxnMeta()` opens those event files to
load the exact apply metadata. A reorder preflight that promises not to open
WAL files therefore cannot reuse that method.

The commit timestamp is server wall-clock time at sequencing. It is distinct
from the designated timestamps carried by the rows.

### 3.2 Look-ahead

`ApplyWal2TableJob.applyOutstandingWalTransactions()` opens a sequencer cursor
after `TableWriter.getAppliedSeqTxn()` and calls
`TableWriter.readWalTxnDetails()`.

`WalTxnDetails.readObservableTxnMeta()` loads metadata for transactions that
already exist. It then walks backwards and assigns each transaction a
`commitToTimestamp` derived from the minimum timestamp of the later observed
transactions. This lets `TableWriter` keep unsafe rows invisible until a later
transaction makes their order known.

The newest observed transaction receives `LAST_ROW_COMMIT`, which
`getCommitToTimestamp()` maps to `FORCE_FULL_COMMIT`. Look-ahead therefore
never keeps the current sequencer tip waiting for a transaction that has not
arrived yet.

Look-ahead is bounded by the per-table apply time quota and row/memory limits.
Those bounds are important for fairness and must remain in force.

### 3.3 Block apply and WAL lag

When no WAL lag exists, `TableWriter` can group consecutive data
transactions into one block. `processWalCommitBlock()` maps all contributing
WAL segments and performs one O3 apply operation. It normally sorts the
combined timestamp index and shuffles the columns. A non-deduplicating,
single-segment block already known to be in order takes an optimized path that
skips the radix sort.

When block apply is unavailable, the single-transaction path can copy complete
transactions into the invisible WAL lag area of the last native partition.
The lag is a staging representation inside table storage. It is not available
when the last partition is Parquet, and filling it writes the rows once before
the eventual sort/apply writes them again.

The final observed transaction forces a full commit, so no rows remain in WAL
lag after normal catch-up.

### 3.4 Timing-dependent example

Consider two WAL transactions:

```text
T1: timestamps [100, 110]
T2: timestamps [ 90,  99]
```

If both transactions are pending when `WalApply` reads ahead, it can sort them
together or retain T1 until T2 is processed:

```text
sequencer: T1, T2
apply:     sort(T1 + T2) -> one O3 operation
```

If the worker runs between the commits, T1 is the newest observed transaction
and is forced visible:

```text
sequencer: T1
apply:     force T1 visible

sequencer: T1, T2
apply:     merge T2 into already committed data -> another O3 operation
```

The table result is correct in both cases. The amount of storage work is not.

### 3.5 Why existing settings do not create a reorder window

- `cairo.wal.apply.look.ahead.txn.count` controls how much already-committed
  WAL metadata is read. It does not wait for a future commit.
- `cairo.wal.squash.uncommitted.rows.multiplier`,
  `cairo.wal.max.lag.size`, and `cairo.wal.max.lag.txn.count` bound
  `TableWriter` lag after future disorder is observable.
- `cairo.commit.latency` is an upper bound that can force a full commit. It
  cannot override the tip's `FORCE_FULL_COMMIT` in the other direction.
- Table `o3MaxLag` controls the non-WAL `TableWriter` O3 path. `WalWriter`
  ignores the `ic(o3MaxLag)` argument, and WAL apply does not use this table
  parameter.

## 4. Goals

The initial implementation must:

1. Coalesce WAL transactions that arrive within a configured wall-clock
   window before applying them to table storage.
2. Work across WAL writers, clients, and ingestion protocols.
3. Reuse the current `WalTxnDetails` and block-apply ordering algorithms.
4. Preserve WAL durability and acknowledgement behavior.
5. Preserve sequencer order and transaction atomicity.
6. Add no intentional delay when the window is zero.
7. Bound the intentional wait from the first pending transaction; new commits
   must not slide the deadline.
8. Preserve the current apply time quota, memory-pressure control, and
   notification queue recovery.
9. Recover after restart without adding a new durable state file.
10. Work when the destination's last partition is native or Parquet.
11. Work with both V1 and V2 sequencer logs.
12. Allow mixed workloads to enable or disable the window per table.
13. Make timer delivery a latency optimization rather than a correctness
    dependency.

## 5. Non-goals

The initial implementation will not:

- guarantee in-order storage work for arbitrarily late data;
- define a client-visible event-time watermark;
- split one WAL transaction at a row boundary;
- retain a row-level suffix in persistent `TableWriter` lag;
- adapt the window automatically from observed disorder;
- change WAL, QWP, ILP, or SQL commit acknowledgement semantics;
- guarantee a strict transaction boundary at timer expiry;
- migrate V1 sequencer logs to V2.

A transaction arriving after the configured window may still require O3 work.
That is expected. A strict bounded-lateness guarantee requires source
watermarks or a deeper row-level buffering design.

## 6. Terminology and state

The design keeps the existing watermarks unchanged:

| Name | Meaning |
|---|---|
| `seqTxn` | Latest transaction durably assigned by the table sequencer. |
| `writerTxn` | Latest sequencer transaction visible to table readers. |
| `dirtyWriterTxn` | Latest transaction processed by the writer, including complete transactions staged in WAL lag. |

The reorder scheduler adds no transaction watermark. In particular, it does
not add a `readyThroughTxn`: once a window is released, the existing apply
cursor may consume transactions that arrive before the worker reaches the
sequencer tip. The table time quota, look-ahead limits, and pressure control
remain the bounds on one apply run.

The scheduler does keep ephemeral per-table state:

| Name | Meaning |
|---|---|
| `deferredFromTxn` | First pending sequencer transaction that opened the current window. |
| `deferredDeadlineMicros` | Fixed release deadline derived from that transaction's persisted commit time. |
| `lastForceApplyTxn` | Highest structural or conservatively classified zero-row transaction observed by the live sequencer and recorded in the tracker. |

These fields are scheduling state. None is a durability or reader-visibility
watermark.

## 7. Proposed semantics

Add the server setting:

```properties
cairo.wal.apply.reorder.window=0
```

The value is a duration. The default `0` disables the feature and preserves
current behavior. `CairoConfiguration` exposes the parsed value in
microseconds. Negative durations are rejected.

Add a table metadata override named `walApplyReorderWindow`, configurable at
table creation and through:

```sql
ALTER TABLE trades
    SET PARAM walApplyReorderWindow = 25ms;

ALTER TABLE trades
    SET PARAM walApplyReorderWindow = DEFAULT;
```

The stored value is either a non-negative duration or `INHERIT`. `INHERIT`
uses the server setting. An explicit zero disables the window for that table.
Metadata written before this field existed reads as `INHERIT`; the field uses
the existing `_meta` header extension and minor-version mechanism, so no
offline table migration is required.

Materialized-view destination tables are an exception: `INHERIT` resolves to
zero for them. A base table may still use a positive window, but its refresh
commit is not delayed by a second inherited window on the materialized view.
An explicit positive override may enable the feature on a materialized view
if a measured workload justifies it.

The effective duration is captured when a window is armed and ordinary data
commits do not recalculate it. Changing the table parameter is itself a
structural WAL transaction, so it force-releases an active window; the new
value governs the next window. Reorder state is not persisted, so a restart
reconstructs the deadline with the effective setting in force after restart.

For a positive value:

1. The first apply notification for pending data still reaches an apply
   worker.
2. After acquiring `TableWriter`, the worker first observes existing reorder
   state. `RELEASED` work bypasses preflight; a young `DEFERRED` duplicate
   returns; an expired `DEFERRED` duplicate promotes the state and applies.
   Only in `NONE` does the worker resolve the table's effective window and
   open a dedicated sequencer-only cursor starting after
   `TableWriter.getAppliedSeqTxn()`. It does not call
   `WalTxnDetails.readObservableTxnMeta()`.
3. It calculates:

   ```text
   deadline = firstPendingCommitTimestamp + reorderWindow
   ```

   The addition is saturating so an extreme configured duration cannot wrap
   into an already-expired deadline.

4. If the deadline has passed, the worker marks the backlog `RELEASED` and
   applies immediately.
5. Otherwise, it installs `DEFERRED` before scanning the rest of the current
   sequencer range. It then checks the live `lastForceApplyTxn` watermark and
   scans a captured range for barriers visible in the on-disk log.
6. A force-release found by either check moves the table to `RELEASED` and the
   worker applies immediately. If the table remains `DEFERRED`, the worker
   registers one delayed callback, releases the writer, and returns.
7. More data transactions update `seqTxn` but do not reset the deadline or
   publish duplicate apply work.
8. When the deadline expires, the callback moves the table to `RELEASED` and
   publishes the normal WAL apply notification.
9. `WalApply` uses today's cursor and look-ahead behavior while the table is
   `RELEASED`. The state returns to `NONE` only after `writerTxn` catches
   `seqTxn`; a later transition from caught-up to pending opens the next
   window.

The window starts at the persisted commit time of the first pending
transaction, not when a worker happens to dequeue the notification. Queue
delay and server downtime therefore consume the window instead of adding a
second full window.

The configured duration bounds only the intentional holdback. Normal queue
delay, writer contention, memory-pressure backoff, and apply time remain
additional sources of visibility latency.

### 7.1 Example

With a positive window, the timing-dependent example becomes:

```text
time 0:    T1 [100, 110] commits to WAL
           apply notification inspects T1 and arms deadline D

time < D:  T2 [90, 99] commits to WAL
           T2 joins the existing window; D does not move

time D:    timer releases the pending backlog
           WalApply sees T1 and T2 together
           existing block sorter applies one ordered block
```

If T2 commits just after D but before the released worker reads the sequencer,
it may join the same apply run. If it arrives after the table catches up, it
opens the next window and may still cause another O3 merge. This dequeue-time
boundary is deliberately not a client-visible contract.

## 8. Reorder state machine

`SeqTxnTracker` owns the per-table ephemeral state because it already
coordinates the sequencer, apply workers, missed-notification scans, and
visibility waiters.

The reorder state has three values:

| State | Meaning |
|---|---|
| `NONE` | No window is waiting and no released backlog is being continued. |
| `DEFERRED` | A fixed deadline is armed for `deferredFromTxn`. |
| `RELEASED` | Pending work must apply without another reorder delay until the writer catches the sequencer. |

The tracker stores:

- `deferredFromTxn`;
- `deferredDeadlineMicros`;
- `lastForceApplyTxn`;
- the active timer entry;
- a monotonically increasing generation used to reject stale timer callbacks.

The state follows these transitions:

| Current state | Event | Next state and action |
|---|---|---|
| `NONE` | Worker finds young pending data | `DEFERRED`; register timer. |
| `NONE` | Worker finds expired data | `RELEASED`; apply. |
| `NONE` | Preflight finds a force-release record | `RELEASED`; apply. |
| `DEFERRED` | Data commit | Stay `DEFERRED`; update `seqTxn`, do not move deadline. |
| `DEFERRED` | Force-release commit | `RELEASED`; cancel timer and notify apply. |
| `DEFERRED` | Timer or missed-notification sweep sees expiry | `RELEASED`; notify apply. |
| `RELEASED` | New commit | Stay `RELEASED`; preserve the existing commit-notification edge predicate. |
| `RELEASED` | Apply stops while `writerTxn < seqTxn` | Stay `RELEASED`; republish without another delay. |
| `RELEASED` | `writerTxn >= seqTxn` under the tracker transition | `NONE`. A racing later commit observes the caught-up edge and publishes normal preflight work. |
| Any non-terminal state | Suspend | Preserve the state, but suppress apply publication while suspended. |
| Suspended `DEFERRED` | Deadline expires | Move to `RELEASED`, but wait for resume to publish apply work. |
| Suspended state | Resume | Atomically clear suspension and publish if `writerTxn < seqTxn`; do not route this wake-up through ordinary deferred-work suppression. |
| Any | Drop or engine shutdown | Cancel the timer entry and invalidate its generation; do not publish delayed work. |

The state machine must preserve these invariants:

- At most one timer generation owns a table's `DEFERRED` state.
- `deferredFromTxn` and its deadline do not change when data joins a window.
- `RELEASED` work is never put through preflight again until the writer has
  caught the sequencer.
- `writerTxn` advances only through the existing table commit path.
- Apply never skips a sequencer transaction or exposes part of one.

The timer entry follows the existing `DelayedFireable` terminal-state
contract. It has `PENDING`, `FIRED`, and `CANCELLED` states, and `expire()`,
force release, drop, and shutdown compete through CAS. Its deadline, tracker,
and expected generation can be final, but the entry itself is not immutable:
the terminal state is deliberately mutable. `DelayedFireable` does not require
pooling; the initial implementation may use a single-use entry per window.
It must not reset and re-register an entry while an older heap registration
can still observe a changed deadline.

The generation is a second guard at the tracker boundary. A timer entry that
survives in a heap after cancellation, rename, drop, or a later window must be
a no-op even if its callback runs.

Existing `writerTxn` and `dirtyWriterTxn` semantics must not be overloaded to
represent a deferred window. In particular, `writerTxn` must continue to mean
reader visibility so that `wait_wal_table()` remains correct.

## 9. Apply scheduling integration

### 9.1 Preflight

`ApplyWal2TableJob.applyWal()` currently acquires `TableWriter` before opening
the sequencer cursor. The first implementation should perform reorder
preflight after acquiring the writer but before calling
`readWalTxnDetails()`:

- `TableWriter.getAppliedSeqTxn()` is authoritative even when a previous
  apply run persisted complete transactions in WAL lag.
- No WAL event or column file needs to be opened merely to arm the timer.
- The writer is released immediately when preflight defers the table.

Before opening the preflight cursor, the worker checks the tracker under the
same state-transition rules used by notification paths. A duplicate
notification for a young `DEFERRED` table releases the writer and returns. An
expired `DEFERRED` table is promoted to `RELEASED`; an already `RELEASED`
table goes directly to normal apply. The effective setting is resolved only
when state is `NONE`, so a configuration lookup cannot accidentally re-arm or
disable an existing window.

Preflight first reads the commit timestamp of the first pending sequencer
entry. Commit timestamp is available on both V1 and V2. If the deadline is in
the future, the worker creates a timer entry and atomically installs
`DEFERRED` before scanning the captured pending range, including the first
entry. It then:

1. checks whether the tracker's live `lastForceApplyTxn` is above the applied
   transaction;
2. captures the current `seqTxn`; and
3. walks a new, sequencer-only cursor through that captured transaction.

Installing `DEFERRED` before step 2 closes the race without retrying on every
new commit. A force-release committed after installation sees `DEFERRED` in
the tracker and releases it directly. A commit before the capture is included
in the scan and, in a live process, in `lastForceApplyTxn`. Ordinary data
commits require neither a rescan nor a moving compare-and-retry loop.

The persisted scan classifies:

- recognized negative sequencer IDs (`METADATA_WALID` and
  `DROP_TABLE_WAL_ID`) as an immediate force release; these work on V1 and
  V2;
- a V2 entry with `txnRowCount == 0` as a conservative force release.

The cursor version must be checked before reading row count. V1 cannot
identify a historic zero-row data record from the sequencer log alone. The
initial design accepts that limitation rather than opening each WAL event
during preflight or requiring a V2 migration. After tracker recreation or
restart, a V1 zero-row data transaction that was already pending may wait for
the remaining reorder window. It is still applied in order and is never
skipped. Structural V1 records remain visible through `walId`.

Moving preflight ahead of writer acquisition is a possible later
optimization. It would first require `SeqTxnTracker` initialization to restore
the dirty watermark from `_txn` lag metadata, rather than only the visible
writer transaction.

If the cursor has no transaction, the notification was stale and preflight
returns without arming a timer. If the commit timestamp cannot be read,
preflight applies immediately; it must fail toward visibility, not indefinite
deferral.

### 9.2 Released apply

`RELEASED` does not add an inclusive sequencer high-water mark.
`applyOutstandingWalTransactions()` and
`WalTxnDetails.readObservableTxnMeta()` keep their existing cursor semantics.
If transactions arrive between timer expiry and dequeue, or while incremental
look-ahead still has time, they may join the apply run.

This is preferable to a fixed `readyThroughTxn` for the first implementation:

- later transactions can only improve coalescing;
- the current table time quota already prevents one table from monopolizing
  an apply worker;
- look-ahead row limits and pressure control still bound mapped data and O3
  memory;
- no new limit must be threaded through initial look-ahead, incremental
  look-ahead, cursor iteration, block calculation, and cached
  `WalTxnDetails`.

The tracker remains `RELEASED` if the time quota, writer contention, or memory
pressure stops apply while `writerTxn < seqTxn`. Continued work is republished
without another reorder delay. It moves to `NONE` only when a writer update
observes `writerTxn >= seqTxn` under the same tracker transition. Continuous
ingestion may therefore keep a backlogged table released, which matches
today's catch-up behavior; the time quota provides cross-table fairness.

When the configured window is zero, `ApplyWal2TableJob` bypasses preflight and
all reorder-state transitions. It uses the current cursor and notification
behavior unchanged.

### 9.3 Commit-time barriers

Some operations should not sit behind a young data window.
`TableSequencerImpl` knows at sequencing time whether a commit is:

- a structural transaction through `nextStructureTxn()`; or
- a transaction with zero rows through `nextTxn()`.

The sequencer-to-tracker notification carries a `forceApply` flag for both,
and the drop-table path supplies its real drop transaction as a force release
instead of relying only on the current `Long.MAX_VALUE` publication bypass.
The tracker records the transaction in `lastForceApplyTxn` regardless of its
current reorder or suspension state; queue publication remains separately
gated by suspension. If the tracker is `DEFERRED`, the same transition moves
it to `RELEASED`, cancels the timer, and publishes apply work when allowed.

Zero row count is deliberately a conservative classification, not an exact
transaction-type test. WAL SQL, truncate, view-definition, and materialized-
view invalidation records normally have zero rows, but so can:

- a `DATA` replace-range transaction that deletes a range without inserting
  rows;
- the empty `DATA` seed transactions emitted by `ALTER TABLE ... REBASE WAL`;
- an exotic cancelled-row data commit.

Releasing any of these early is correct because the transaction is applied,
not skipped. In V2, ordinary zero-row WAL entries have
`minTimestamp = Long.MAX_VALUE`, `maxTimestamp = -1`, and row count zero.
Structural entries also have row count zero but reuse the minimum-timestamp
slot for the ALTER command type. Preflight must classify by `walId` before
interpreting that slot.

Replace-range mode is stored in the WAL event rather than the sequencer
summary. A non-empty replace-range transaction may therefore wait until the
normal deadline. Once apply starts, the existing `FORCE_FULL_COMMIT` handling
remains authoritative. This changes latency by at most the configured
holdback and does not change correctness.

### 9.4 Notification and republisher behavior

The notification paths do not currently share one predicate. In particular,
`SeqTxnTracker.notifyOnCommit()` publishes only for tracker initialization or
when the writer is exactly one transaction behind the new commit. That edge
test avoids filling the queue with one notification per commit while a table
already has a backlog. By contrast, the missed-notification check and
republisher use `writerTxn < seqTxn`.

The following paths must consult reorder state:

- `SeqTxnTracker.notifyOnCommit()`;
- `SeqTxnTracker.notifyOnCheck()`;
- `SeqTxnTracker.updateWriterTxns()`;
- `CheckWalTransactionsJob.republishNotificationsFromTrackers()`.

They must retain their current path-specific anti-duplication behavior:

- `notifyOnCommit`: preserve the existing initialization/exactly-one-behind
  edge predicate in `NONE` and `RELEASED`; suppress ordinary data publication
  in `DEFERRED`; a `forceApply` commit releases and publishes.
- `notifyOnCheck`: update `seqTxn`; promote an expired `DEFERRED` state; publish
  pending work only for `NONE` or `RELEASED`, subject to suspension and
  pressure control.
- `updateWriterTxns`: preserve `DEFERRED` or `RELEASED` across partial
  progress; clear `RELEASED` only on a synchronized caught-up observation.
- the republisher: suppress a young `DEFERRED` table, promote an expired one,
  and republish a lagging `NONE` or `RELEASED` table.

`CairoEngine.notifyWalTxnRepublisher()` currently calls
`updateWriterTxns(UNINITIALIZED_TXN, UNINITIALIZED_TXN)` as a documented hack
when the notification queue is full. The new reorder fields, force-release
watermark, timer generation, and `DEFERRED`/`RELEASED` state must survive that
reset. Only the existing writer and dirty-writer scheduling values are
uninitialized. If a timer publication fails, the unpublished counter wakes
the existing republisher, which observes the preserved released state.

### 9.5 Timer

Use the engine-owned `TimerShards` rather than sleeping on an apply worker.

Each window registers one CAS-mutable `DelayedFireable` containing:

- table token;
- deadline;
- tracker reference;
- expected generation.

`expire()` first wins `PENDING -> FIRED`, then asks the tracker to release the
matching `DEFERRED` generation and publishes a normal WAL notification.
`shutdown()` only invalidates/cancels the ephemeral wait. It does not force
table application during engine shutdown; the WAL remains the recovery
authority.

The callback must tolerate table rename, drop, suspension, engine shutdown,
and a full notification queue. It must never call `TableWriter` or perform WAL
I/O on a timer thread.

`TimerShards` currently has no per-entry removal API. Add an identity-based
`unregister()` routed to the entry's shard, backed by a synchronized
`DelayHeap.removeIdentity()` that wakes the shard thread if it removed the
head. Early release first wins `PENDING -> CANCELLED` and then unregisters the
entry. If the timer thread already removed it, unregister returns false and
the callback's CAS observes `CANCELLED`. This keeps cancellation bounded
without weakening the terminal-state race contract.

The timer is not the only release path. `CheckWalTransactionsJob` already
sweeps trackers at `cairo.wal.sequencer.check.interval`, whose default is
10 seconds. Both `notifyOnCheck()` and
`republishNotificationsFromTrackers()` must compare a `DEFERRED` deadline with
the current microsecond clock and perform the same release transition when it
has expired. This adds a constant-time check to an existing sweep, not a new
O(N) job. A lost timer entry therefore adds at most the normal sweep delay; it
cannot park the table forever.

If the table is suspended when either expiry path runs, the tracker records
`RELEASED` but does not publish. Resume needs a dedicated tracker transition
that clears suspension and reports whether work must be published. The current
`TableSequencerImpl.resumeTable()` publishes a `Long.MAX_VALUE` wake-up before
calling `setUnsuspended()`; the reorder integration must not pass that wake-up
through the ordinary `DEFERRED` suppression rule. Suspension must not discard
or restart the original window.

## 10. Storage, durability, and recovery

### 10.1 WAL durability and acknowledgements

The reorder decision happens after the sequencer transaction exists.
Transactions remain in their ordinary WAL segment and sequencer log until
apply advances the table.

Consequently:

- QWP durable acknowledgement coverage does not change.
- Other ingestion protocols retain their existing WAL commit semantics.
- Normal WAL `ALTER` and `UPDATE` `OperationFuture` instances complete after
  `WalWriter` appends and sequences the operation; they do not wait for
  `TableWriter` WAL apply. The window can delay when the operation takes
  effect, not its ordinary sequencing acknowledgement.
- Client replay watermarks do not depend on table visibility.
- No client needs to know that a reorder window is armed.

`OperationDispatcher` does have an exceptional
`EntryUnavailableException` fallback that returns a future for a command
queued to a writer. That future waits for the queued command to execute. This
is existing writer-contention behavior, not a wait for an already sequenced
WAL transaction to reach `writerTxn`; the reorder feature must not route the
normal WAL path through that fallback.

### 10.2 Restart

Reorder state is intentionally not persisted.

After restart, `CheckWalTransactionsJob` observes `writerTxn < seqTxn` and
publishes apply work. Preflight reads the first pending transaction's persisted
sequencer commit timestamp and reconstructs the deadline:

- if the deadline passed while the server was down, apply starts immediately;
- otherwise, preflight registers a timer for the remaining interval.

The live `lastForceApplyTxn` watermark is also reconstructed only as far as
the persisted format permits. Structural records are visible in both
versions, and V2 exposes zero row count. V1 zero-row data records that predate
tracker reconstruction may wait for the remaining window as described in
section 9.1.

No new recovery record is required. The table-level configuration field uses
metadata minor-version compatibility rather than an offline migration.

### 10.3 Snapshot and checkpoint behavior

The design leaves deferred rows in WAL and does not advance `_txn`.
Snapshots therefore see an ordinary WAL backlog rather than a new table
storage representation.

This is important because snapshot restore currently resets `TableWriter` WAL
lag and replays complete WAL transactions. The design never exposes a prefix
of a WAL transaction while retaining its suffix, so it does not change that
replay invariant.

### 10.4 WAL purge

`WalPurgeJob` derives its safe boundary from the table's visible sequencer
transaction in `_txn`. A deferred window does not advance that value, so the
segments containing deferred transactions remain pending and cannot be
purged as applied.

### 10.5 Parquet tables

Because rows remain in WAL until the window closes, the design does not depend
on the destination's ability to hold `TableWriter` WAL lag. At expiry, the
existing block path can batch Parquet O3 work under its current pressure and
merge rules.

## 11. Visibility and dependent work

Until the admitted batch applies:

- readers continue to see the previous `writerTxn`;
- `wait_wal_table()` continues waiting for its target;
- materialized-view base-table commit notification does not fire;
- WAL table status continues to show `sequencerTxn > writerTxn`.

These effects are intentional consequences of the configured latency versus
throughput tradeoff. The design must not advance waiters from
`dirtyWriterTxn`, reorder state, or timer expiry.

An explicit wait does not bypass the reorder window in the initial
implementation. Giving `wait_wal_table()` flush semantics would turn a
read-side synchronization operation into a write-scheduling control and would
make batching depend on observer activity.

Materialized-view latency needs separate accounting. A base table's window
delays the apply-side notification that triggers refresh. The refresh then
writes its result to the materialized view's own WAL, and a dependent
materialized view is triggered only after that WAL applies. A globally
inherited positive window would therefore add approximately one intentional
holdback at every destination table in a dependency chain, in addition to
refresh and queue time.

For that reason, materialized-view tables resolve an inherited window to zero.
The base table may still delay once, but refresh commits and downstream
cascades are not given another reorder delay unless an operator explicitly
enables one on a specific materialized view.

## 12. Resource bounds and fairness

Deferral maps no WAL columns and allocates no O3 sort buffers. Its live cost is
one tracker state and one active timer entry per deferred table. The rows
consume their existing WAL disk space for up to the configured window.

Identity removal prevents early force releases from retaining cancelled timer
entries until their original deadlines. A timer-take race may temporarily
leave one callback in flight, but its terminal CAS makes it a no-op.

The deadline is anchored to the first pending transaction and never slides.
Continuous ingestion therefore cannot keep a table in `DEFERRED` forever.

At expiry, all existing limits continue to apply:

- WAL apply table time quota;
- look-ahead transaction and row limits;
- `TableWriterPressureControl`;
- maximum block row count;
- WAL lag row, byte, and transaction limits when the single-transaction path
  uses lag.

The initial implementation does not add another row or transaction cap to the
deferred state. A fixed, non-sliding duration already bounds how long WAL can
accumulate, while existing apply limits bound memory after release. If
benchmarks show that very high transaction rates create impractically large
windows, a later change can promote a window early by sequencer transaction
count without changing the state machine.

After release, the existing table time quota remains the fairness boundary.
Transactions arriving while apply runs may join until the quota or another
existing limit ejects the table. A table that cannot catch its sequencer stays
`RELEASED` across those ejections rather than repeatedly paying the reorder
window.

## 13. Observability

Existing observability already shows most of the effect:

- `wal_transactions(table)` exposes sequencer commit timestamp on both log
  versions. Row count and designated min/max timestamps are available only
  for positive-row V2 records; the function already returns `NULL` for those
  columns on V1 and for zero-row entries.
- WAL table status exposes the gap between `sequencerTxn` and `writerTxn`.
- `WAL_TXN_APPLY_START` telemetry latency includes the intentional reorder
  wait because it is measured from sequencer commit time.
- apply logs already report transaction count, rows, throughput, and physical
  write amplification.

Expose both the stored and effective `walApplyReorderWindow` in the table
catalogue so an operator can tell inheritance, explicit disablement, and
materialized-view exemption apart.

Add the following metrics:

| Metric | Type | Meaning |
|---|---|---|
| `wal_apply_reorder_waiting_tables` | gauge | Tables currently in `DEFERRED`. |
| `wal_apply_reorder_windows` | counter | Windows moved from `DEFERRED` to `RELEASED`. |
| `wal_apply_reorder_transactions` | counter | Pending transaction count observed when a window is released. |
| `wal_apply_reorder_force_releases` | counter | Deferred windows released early by a structural or conservative zero-row classification. |
| `wal_apply_reorder_sweep_releases` | counter | Expired windows recovered by the missed-notification sweep instead of their timer callback. |

Log window arm and release at debug level. Do not log every joining data
transaction.

## 14. Compatibility

With the default window of zero:

- apply scheduling remains unchanged;
- visibility latency remains unchanged;
- no timer entries are created;
- commit notification stays on its current hot path;
- no sequencer file format changes.

The table override adds a minor-versioned field in reserved `_meta` header
space. Metadata created before that minor version reads as `INHERIT`.
Readers that do not know the field continue to use the existing header fields
because no column offset or sequencer record moves.

When reorder state is `NONE`, the implementation should resolve the effective
window after acquiring the writer and branch on zero before entering new
reorder transitions. The commit-time API still carries one `forceApply` bit so
a live tracker can remember barriers before preflight, but ordinary data
commits must retain their existing exactly-one-behind notification predicate
and avoid new per-commit allocation.

Do not reuse table `o3MaxLag`. It is currently documented and implemented as a
non-WAL ingestion optimization, and existing tables contain its default value
even when users did not set it explicitly. Reinterpreting it for WAL tables
would silently introduce a large visibility delay after upgrade.

Do not reuse `cairo.commit.latency`. That setting bounds how long
`TableWriter` lag may remain uncommitted; the reorder window deliberately
holds transactions before `TableWriter`.

## 15. Test plan

### 15.1 Core behavior

- Window zero: one in-order transaction becomes visible with the same number
  of apply-job ticks as today.
- Positive window: the first young transaction arms a timer and remains
  invisible.
- A later lower-timestamp transaction joins the window.
- Timer expiry applies both transactions and produces the fully sorted table.
- A transaction committed after expiry but before dequeue may join the
  released run.
- A transaction committed after the writer catches up opens the next window.
- Repeated data commits do not move the first deadline.
- A table override can enable one table while another inherits or explicitly
  disables the feature.
- An inherited materialized-view table resolves to zero; an explicit positive
  override enables it.
- Committing a table-parameter change force-releases an armed window as a
  structural transaction, and the new duration takes effect on the next
  window.
- Negative durations are rejected and deadline addition does not overflow.

Use controllable clocks; do not use wall-clock sleeps.

### 15.2 Exact production batching sequence

Cover the motivating sequence with:

- two `WalWriter` instances committing alternating timestamp ranges;
- non-deferred QWP frames that each close a normal WAL transaction;
- small batches where an apply notification is consumed between commits.

The regression must interleave commit, one apply-job tick, second commit, and
timer release. Tests that enqueue every transaction before draining WAL do not
exercise the bug.

### 15.3 State-machine and notification races

- Data commit racing timer expiry.
- Barrier commit racing timer registration.
- Barrier already sequenced before preflight attempts to defer.
- Data or barrier commit between `DEFERRED` installation and the captured
  sequencer scan.
- Stale timer from an earlier generation.
- Notification queue full at timer expiry, followed by republisher recovery.
- Queue-full `UNINITIALIZED_TXN` reset preserves both young `DEFERRED` and
  `RELEASED` state.
- Timer registration deliberately omitted or cancelled, followed by expiry
  promotion from `CheckWalTransactionsJob`.
- Repeated early force releases remove their timer entries; removal racing a
  timer pop produces one no-op callback and no leaked registration.
- Duplicate apply notifications.
- Writer busy while a backlog is released.
- Apply time-quota ejection while `RELEASED`, followed by continuation without
  another window.
- A commit racing the caught-up `RELEASED -> NONE` transition.
- Memory-pressure backoff while a backlog is released.

### 15.4 Lifecycle

- Restart before deadline reconstructs the remaining wait.
- Restart after deadline applies immediately.
- Restart with a changed server default reconstructs the deadline using that
  new effective setting.
- Shutdown cancels timers without suspending tables.
- Drop while deferred produces no late apply notification.
- Rename while deferred applies using the current table token.
- Suspend and resume while deferred do not lose pending work.
- Resume publishes promptly despite the current
  publish-before-`setUnsuspended()` ordering.

### 15.5 Storage variants

- Native last partition.
- Parquet last partition and empty `FORMAT PARQUET` table.
- V1 sequencer log: commit-time zero-row release, persisted structural
  detection, and documented historic-zero-row holdback.
- V2 sequencer log: persisted structural and zero-row detection without
  unsupported cursor calls.
- Existing persisted whole-transaction WAL lag.
- Deduplication enabled.
- Non-empty and empty replace-range transactions; empty replace range is
  applied after conservative early release, not treated as non-data.
- Empty rebase seed transactions.
- DDL, SQL update, truncate, view definition, and materialized-view
  invalidation barriers.
- Snapshot/restore with a deferred window.
- WAL purge while transactions are deferred.

### 15.6 Visibility

- `wait_wal_table()` does not complete on timer expiry alone.
- It completes when `writerTxn` reaches its target.
- A normal WAL `ALTER` or `UPDATE` future completes after sequencing rather
  than waiting for the reorder window, while its visible effect remains
  delayed until apply.
- The existing writer-contention async fallback retains its separate queued-
  execution future behavior.
- Materialized-view refresh notification occurs only after table commit.
- A base-table -> materialized-view -> dependent-view chain pays no inherited
  reorder window on either materialized-view destination.
- WAL status reports the pending sequencer/writer gap during the window.

## 16. Performance validation

Measure at least:

- logical WAL rows;
- physically written apply rows;
- number of O3 operations or partition rewrites;
- apply throughput;
- end-to-end visibility latency;
- timer and notification overhead with the window disabled;
- continuous-ingestion fairness across multiple WAL tables.

For the two-transaction motivating case, if both transactions fit the existing
block limits, the positive-window run should perform one block O3 operation
only when there is no pre-existing `TableWriter` WAL lag and pressure control
admits the block. `TableWriter.calculateInsertTransactionBlock()` deliberately
falls back to one transaction when lag rows already exist.

Do not use invocation of the radix sorter as the universal block-apply
assertion. `processWalCommitBlock()` skips that sort for a non-deduplicating,
single-segment block whose data is already in order. Assert the block
transaction count, O3 operation or partition rewrite count, physical rows,
and final table ordering; assert sorting only for a workload that actually
requires it. The zero-window baseline should retain current behavior.

No nonzero production recommendation should be baked into the first change
without workload measurements. The useful value depends on the producer's
inter-batch skew and the acceptable visibility latency. Default zero provides
a safe rollout boundary.

## 17. Acceptance criteria

The implementation is complete only when all of the following are true:

1. With an effective window of zero, the existing deterministic WAL apply
   tests require the same number of apply ticks, create no timer entry, and
   retain the current commit-notification predicate.
2. With a positive window, the exact motivating sequence—first commit, one
   apply tick, lower-timestamp second commit, then release—produces a sorted
   result and one block apply under the preconditions in section 16.
3. The first deadline never slides. A functioning timer releases at that
   deadline; a deliberately lost timer is recovered by the existing
   sequencer-check sweep, so no table remains deferred indefinitely.
4. Queue-full reset, duplicate notification, writer contention, time-quota
   ejection, pressure backoff, suspend/resume, rename, drop, shutdown, restart,
   and a commit racing catch-up neither lose work nor apply a sequencer
   transaction twice.
5. V1 tests never call unsupported row-count or timestamp-summary accessors.
   Live zero-row commits force release on V1 and V2; restart tests demonstrate
   the documented V1 historic-zero-row limitation and full V2 detection.
6. Normal WAL `ALTER` and `UPDATE` acknowledgement timing is unchanged,
   `wait_wal_table()` still follows `writerTxn`, and WAL purge and snapshot
   tests retain their current durability boundaries.
7. A mixed workload can enable the window on one ordinary WAL table and
   disable it on another. Inherited materialized-view tables do not add a
   reorder delay at each dependency level.
8. Metrics and catalogue output identify deferred tables, release cause,
   stored override, and effective window.
9. The motivating benchmark demonstrates reduced O3 operations or physical
   writes for the disordered workload. Disabled-mode throughput, CPU, and
   allocation results are recorded against a repeated baseline.

The numerical disabled-mode regression budget must be agreed before the
implementation is merged, using the repository's benchmark variance. This
design does not invent a percentage without baseline data.

## 18. Alternatives considered

### 18.1 Increase look-ahead

Rejected as a solution to live disorder. Look-ahead can only inspect
transactions already present in the sequencer. Increasing it helps backlog
processing but cannot observe a future transaction.

### 18.2 Retain the sequencer tip in `TableWriter` WAL lag

Not selected for the first implementation.

A simple sentinel change is insufficient:

- normal catch-up currently does not persist a retained tip;
- notification and republisher code would continuously see
  `writerTxn < seqTxn`;
- new commit notification assumes the visible writer transaction is the
  scheduling watermark;
- Parquet last partitions cannot use WAL lag;
- rows are copied into lag before being sorted/applied, adding physical writes;
- persistent partial-transaction lag conflicts with snapshot replay.

Whole-transaction tip lag could be made correct, but it still requires a
deferred scheduler and offers fewer advantages than leaving the transaction in
WAL.

### 18.3 Protocol-specific buffering

Rejected as the server solution. It cannot coordinate independent clients,
duplicates logic across protocols, and makes storage performance depend on
client batch policy. Clients may still batch for their own efficiency.

### 18.4 Strict event-time watermark

Deferred.

V2 sequencer min/max timestamps can contribute to an observed event-time
watermark, but V1 does not store them. Even on V2, apply must advance through a
contiguous sequence of complete WAL transactions. A transaction containing
rows on both sides of the watermark must be held entirely or split. Holding
it entirely can stall safe older rows; splitting it requires a durable
row-level apply watermark and changes snapshot/replay assumptions. An
event-time watermark also needs an idle flush because event time stops
advancing when producers stop.

The wall-clock window addresses short cross-batch skew without those changes.

### 18.5 Fixed transaction horizon

Not selected for the first implementation.

Capturing `readyThroughTxn` at expiry would make the boundary deterministic,
but it would also require horizon checks in initial and incremental
look-ahead, cursor iteration, block calculation, and cached
`WalTxnDetails`. Transactions arriving before dequeue would be excluded even
though admitting them improves coalescing. The existing table time quota
already supplies the fairness property originally attributed to the horizon.

The design retains a `RELEASED` state across quota ejection so admitted backlog
does not pay another window, but it deliberately does not retain a fixed high
transaction.

### 18.6 Adaptive window

Deferred.

Apply already records write amplification and throughput, so a later design
could activate or tune the window after observing repeated cross-transaction
O3. The first implementation should establish deterministic scheduling and
metrics before adding feedback control.

## 19. Implementation map

| Area | Expected change |
|---|---|
| `PropertyKey`, `CairoConfiguration`, `PropServerConfiguration`, `server.conf` | Add the duration setting, default zero. |
| `_meta`, table metadata readers/writers, SQL parser and ALTER operation, table catalogue | Add the minor-versioned `walApplyReorderWindow` override and effective-value reporting. |
| `SeqTxnTracker` | Add `NONE`/`DEFERRED`/`RELEASED` state, fixed deadline, live force-release watermark, active timer generation, and path-specific notification predicates. |
| `TableSequencerImpl` | Pass structural and zero-row force-release information to the tracker and make resume an explicit state transition. |
| `ApplyWal2TableJob` | Resolve the effective table setting, add a dedicated version-aware sequencer preflight, arm timers, and bypass preflight for `RELEASED` work. |
| `CheckWalTransactionsJob` | Suppress young `DEFERRED` work, promote expired windows as a backstop, and republish `RELEASED` work. |
| `TimerShards`, `DelayHeap`, timer consumer | Add identity-based unregister/removal and a CAS-terminal WAL apply deadline entry. |
| `WalMetrics` | Add reorder-window gauge and counters. |
| WAL tests | Add deterministic V1/V2 timing, lifecycle, force-release, Parquet, QWP, materialized-view, and regression coverage. |

`TableWriter.processWalCommit()` and `processWalCommitBlock()` should not need a
new ordering algorithm. The feature exists to present better transaction
batches to those methods.

## 20. Delivery sequence

Implement in reviewable stages:

1. Server default, table override, metadata compatibility, and disabled-path
   tests.
2. Tracker state machine and timer callback tests without table I/O.
3. Version-aware sequencer preflight and released apply.
4. Force-release notification, queue-full preservation, timer backstop, and
   resume recovery.
5. Native, Parquet, V1/V2, and materialized-view integration tests.
6. QWP production-sequence regression test.
7. Metrics, catalogue/configuration documentation, and benchmarks.

The feature remains disabled by default until the integration tests cover
restart, notification loss, and continuous ingestion, and benchmarks establish
the latency/write-amplification tradeoff.

## 21. Open follow-ups

- Exact restart-time classification of V1 zero-row data would require opening
  WAL event files during preflight, persisting an additional barrier
  watermark, or migrating that sequencer to V2. The first implementation does
  none of those.
- A strict transaction horizon can be reconsidered if measurements show that
  dequeue-time admission causes a concrete latency or fairness problem not
  handled by the existing table quota.
- Adaptive duration remains a later feature after fixed-window metrics provide
  enough workload evidence.
