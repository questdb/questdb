# Adaptive durable-epoch: time-OR-backlog trigger

**Status:** design
**Branch / worktree:** `nw_adaptive_commit` @ `~/claude/wt/oss/adaptive-commit`
**Scope:** OSS core. One self-contained change to when the adaptive durable epoch fires.

## Goal

Decouple the adaptive durable-epoch *cadence* from the safety bound it currently
also controls, so the default interval can be lengthened (1 s → **60 s**) without
risking unbounded WAL retention or a long post-crash catch-up. The epoch fires on
**interval-elapsed OR un-epoched row backlog ≥ a cap**, whichever comes first.

## Background (why this is needed)

Two independent durable frontiers exist; they are easy to conflate but do different jobs:

| frontier | advances when | consumed by |
|---|---|---|
| `localDurableSeqTxn` | the WAL commit is fdatasync-durable (W=0: every commit; W>0: after the ≤W batch flush) | **reader visibility** (INV-3) + the QWP client durable-ack frame |
| `durableEpochSeqTxn` | `fsyncMaterializedState` + `_snapshot` marker lands | **only** `WalPurgeJob` (purge floor) + `wal_tables()` observability |

Verified by grep: **no reader or query path consults `durableEpochSeqTxn`.** So the
epoch interval does **not** affect read freshness, client durable-ack, or throughput
(measured ≈0.22 % at 1 s). It affects exactly three things:

1. **WAL disk retention** — the purge floor *is* `durableEpochSeqTxn`, so un-epoched
   WAL is pinned until the next epoch. At interval × ingest-rate this can be large
   (≈12 GB at 1 M rows/s × 60 s).
2. **Recovery catch-up lag** — after a crash the DB is *up* but replays the un-epoched
   tail; lag ≈ tail ÷ catch-up-rate (measured ≈3 M rows/s).
3. **`syncfs` frequency** — each epoch is a whole-filesystem flush; frequency = 1/interval.

The trigger today (`ApplyWal2TableJob.maybeAdvanceDurableEpoch`, lines 692–703) is
**pure wall-clock**:

```java
final long intervalMs = config.getAdaptiveEpochIntervalMs();
if (intervalMs < 0) return;                                    // operator opt-out
...
if (lastEpochTs != 0 && (nowMs - lastEpochTs) < intervalMs) return;   // not yet
advance(...);                                                  // fsyncMaterializedState + marker
```

Because the same interval sets both the cadence *and* the disk/recovery bound,
lengthening it to reduce `syncfs` churn simultaneously enlarges the worst-case WAL
footprint and recovery lag. That coupling is the problem.

## Design

Add a second, independent reason to fire the epoch: a **row backlog cap**. The epoch
advances when *either* the interval has elapsed *or* the number of rows applied since
the last epoch reaches the cap.

### 1. The trigger

Extend the cadence gate so it short-circuits to `advance(...)` when the un-epoched
row backlog reaches the cap, regardless of elapsed time:

```
if intervalMs < 0: return                       // unchanged: epochs fully disabled
if replica-skip / not-adaptive: return          // unchanged short-circuits, evaluated first
timeElapsed  = lastEpochTs == 0 || (nowMs - lastEpochTs) >= intervalMs
backlogHit   = maxRows > 0 && rowsSinceEpoch >= maxRows
if not (timeElapsed || backlogHit): return
advance(...)                                     // resets rowsSinceEpoch (see §3)
```

The existing early returns (negative interval disables epochs entirely; replica skip;
non-adaptive table) are evaluated **first and unchanged** — the cap is only consulted
for a live, local-durable, adaptive table.

### 2. New config: `cairo.adaptive.epoch.max.rows`

- **Meaning:** maximum rows applied to a table since its last durable epoch before an
  epoch is forced. Bounds both recovery-replay work and pinned WAL.
- **Default:** `5_000_000` (≈1–2 s replay at the measured catch-up rate; ≈1 GB WAL at
  typical row widths).
- **Disable sentinel:** `≤ 0` → cap never fires; only the interval gates (restores the
  current pure-time behavior for operators who want it).
- **Unit rationale (resolved with user):** rows, not bytes or txns. Rows bound
  recovery-replay directly and track disk closely, need only one cheap per-table
  counter, and are immune to the commit-size variance that makes a txn cap a poor
  safety bound. A byte-precise cap is a possible future refinement (out of scope).

### 3. Default interval change

`PropServerConfiguration` shipped default for `cairo.adaptive.epoch.interval.ms`:
**1000 → 60000**. With the cap in place this is safe: at moderate ingest the 60 s
interval binds (≈1 epoch/min, minimal `syncfs`); at high ingest the cap binds first and
bounds disk + recovery. The `CairoConfiguration` interface fallback (test default) is
left as-is unless a test needs otherwise — production default lives in
`PropServerConfiguration`.

### 4. The counter

`SeqTxnTracker` (already per-table, already holds `durableEpochSeqTxn` and `lastEpochTs`)
gains a **monotonic `rowsSinceEpoch`** counter:

- **Incremented** on the apply path by each applied batch's row count, **before** the
  gate check for that batch, so the just-applied rows count toward the decision.
- **Reset to 0** inside `advance(...)`, in the same published step that sets
  `lastEpochTs` / `durableEpochSeqTxn` (line ~790), so a fired epoch — for either reason
  — restarts the count. Reset lives on the *success* path (after the epoch is published),
  never in the `advance` failure `catch`, so a failed epoch does not clear the backlog.
- A monotonic *applied-rows* counter (not a `writer.size()` delta) is chosen so
  `DROP PARTITION` / TTL / dedup that shrink the table cannot mask real re-apply work.
- **Thread-safety:** increment, read, and reset all happen on the single apply worker
  that holds the table's writer, so the field is apply-worker-local — a plain `long`, no
  `volatile` (unlike the cross-thread-read `durableEpochSeqTxn`). If a future metric reads
  it off-thread, revisit then (YAGNI).

### 5. Behavior matrix (the point of the change)

| situation | which reason fires | effect |
|---|---|---|
| quiet / moderate ingest (< cap per interval) | **time** (60 s) | ~1 epoch/min, minimal `syncfs` — the win |
| high-ingest burst | **backlog** (every `maxRows`) | self-regulating: epoch rate rises with ingest, bounding WAL + recovery exactly when needed |
| post-crash catch-up | **backlog** | a long replay lands epochs every `maxRows`, so a re-crash resumes near the frontier and replayed segments purge — bounded transient WAL |
| operator wants pure-time | set `max.rows ≤ 0` | today's behavior |
| operator wants epochs off | set `interval.ms < 0` | unchanged — fully disabled |

### Invariants preserved

- **INV-5 epoch ordering** (`advance` strict order: fsyncMaterializedState → marker →
  scoreboard pin → publish) is untouched — the change only affects *when* `advance` is
  called, never *how*.
- **Recovery** (`RecoveryCoordinator`) is unchanged: it still rolls to the last epoch
  and replays the tail. The cap only makes "the last epoch" closer to the frontier.
- **Reads** are unaffected — they gate on `localDurableSeqTxn`, never the epoch.
- **W (group-commit window)** is orthogonal — the epoch lives on the apply path; the cap
  works identically under W=0 and W>0.

## Files touched

- `core/.../PropertyKey.java` — add `CAIRO_ADAPTIVE_EPOCH_MAX_ROWS("cairo.adaptive.epoch.max.rows")`.
- `core/.../PropServerConfiguration.java` — read the new key (default `5_000_000`);
  add field + `getAdaptiveEpochMaxRows()` override; change interval default `1000`→`60000`.
- `core/.../CairoConfiguration.java` — add `default long getAdaptiveEpochMaxRows()`.
- `core/.../CairoConfigurationWrapper.java` — delegate the new getter.
- `core/.../cairo/wal/seq/SeqTxnTracker.java` — add `rowsSinceEpoch` counter: add-delta,
  read, and reset accessors.
- `core/.../cairo/wal/ApplyWal2TableJob.java` — feed the counter per applied batch;
  extend the gate (692–703) with the `backlogHit` term; reset the counter in `advance`.
- Test: `AdaptiveEpochTriggerTest` (new) or an extension of the existing adaptive apply
  test — see below.

## Testing strategy

Fluent `AbstractCairoTest` house style; **pin the microClock** so the time and backlog
reasons can be exercised independently (the cap path is deliberately clock-free, which
makes it directly testable).

1. **Backlog fires (clock frozen):** large interval, small `max.rows`, ingest+apply past
   the cap without advancing the clock → assert `durableEpochSeqTxn` advanced.
2. **Time fires (backlog small):** large `max.rows`, advance the clock past the interval
   → assert an epoch advanced.
3. **Cap disabled:** `max.rows ≤ 0` with a frozen clock and ingest past any plausible cap
   → assert **no** epoch advanced (only time can fire).
4. **Counter resets:** after one backlog-triggered epoch, a further sub-cap ingest does
   **not** re-fire; another full cap-worth does.
5. **Catch-up is bounded:** build a large un-epoched tail, drain it with a frozen clock →
   assert `durableEpochSeqTxn` advances mid-drain (≈ every `maxRows`), not just at the end.
6. **W-independence:** cases 1–2 pass under both W=0 and W>0.
7. **No hot-path regression:** the gate adds one add + one compare per batch and no new
   sync; assert the two disable sentinels still short-circuit before any counter read.

## Out of scope

- Byte-precise WAL-retention cap (rows is the v1 proxy).
- Per-table override of `max.rows` (global config only; per-table `commit_mode` already
  selects adaptive-ness).
- Any change to recovery, the epoch record format, or read-visibility gating.
