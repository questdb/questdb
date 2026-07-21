# Adaptive commit mode

Operator guide to QuestDB's `adaptive` commit mode: crash-safe recovery with
write performance close to the default, at a tunable recovery-point objective (RPO).

> **Audience:** operators running QuestDB. `adaptive` is the **default** commit mode as of
> this release. `nosync`, `sync`, and `async` remain available via `cairo.commit.mode` or a
> per-table `commit_mode` override for deployments that want different semantics.

---

## 1. Overview — what it is and when to use it

QuestDB tables are backed by a Write-Ahead Log (WAL). A commit travels:

```
client ─▶ WAL segment (.d/.i) ─▶ WAL-e events ─▶ sequencer record ─▶ ApplyWal2TableJob ─▶ table partitions (_txn)
          append                  append          append              (materialize, async)   readers read here
```

There are two frontiers: the **WAL** (the durable log of what was committed) and
the **materialized table** (the applied result that readers see). `adaptive`
splits their durability guarantees:

- **The WAL commit is made durable** — an `fdatasync` of the small, append-only
  log (segment column data → WAL-e events → sequencer record, in that order)
  before the commit is acknowledged. So **every acknowledged transaction is
  recoverable**.
- **The table apply stays lazy** — the materialized partitions are an `msync`-only
  (page-cache) *rebuildable cache* of the durable WAL, exactly as under `nosync`.
  The apply path issues **no per-commit column `fdatasync`** under adaptive.

The `fdatasync` cost lands on the small log, not on the large table, and never on
the read path. A background **durable epoch** periodically pins a consistent
applied cut of the table so that after a crash, recovery replays only the bounded
WAL tail past the last epoch, rather than the entire WAL.

**When to use it.** Choose `adaptive` when you want crash-safe recovery — every
acked write survives a power loss — but cannot pay `sync`'s cost of flushing the
whole table on every commit. It targets "crash-safe recovery **and** good write
performance." Stay on `nosync` if you accept losing recent un-flushed commits on
power loss; use `sync` if you specifically need every table partition column
flushed on every commit.

**The durable epoch is a RECOVERY mechanism, not a backup.** It is internal,
continuous, and automatic; it only bounds boot-time replay. It is **orthogonal to
`CHECKPOINT`**, which remains the external-filesystem snapshot/backup tool, and it
is not a substitute for backups or replication. Turning adaptive on does not change
your backup strategy.

Confirmed in source: `CommitMode.ADAPTIVE = 3`
(`core/src/main/java/io/questdb/cairo/CommitMode.java:47`); the apply path excludes
adaptive from per-commit column sync
(`CommitMode.appliesColumnSync` returns true only for `SYNC`/`ASYNC` —
`CommitMode.java:76`).

---

## 2. Enabling it

Adaptive can be set **globally** (the instance default) or **per table**. A
per-table setting always wins over the global one, so mixed-mode databases are
supported. Prefer enabling it per table first — smaller blast radius.

### Global default

```properties
cairo.commit.mode=adaptive
```

Accepted values: `nosync`, `sync`, `async`, `adaptive` (default). This affects
every table that has **not** set an explicit per-table override.

### Per table — at creation

```sql
CREATE TABLE trades (
    ts TIMESTAMP, sym SYMBOL, px DOUBLE
) TIMESTAMP(ts) PARTITION BY DAY WAL
WITH commit_mode='adaptive';
```

### Per table — on an existing table

```sql
ALTER TABLE trades SET PARAM commit_mode='adaptive';
```

Takes effect on the next commit. From then on the table's WAL commits are made
durable and it begins taking durable epochs.

### Reverting a table to the global default

```sql
ALTER TABLE trades SET PARAM commit_mode='unset';   -- fall back to cairo.commit.mode
```

The accepted `commit_mode` tokens for both `WITH` and `SET PARAM` are:
`nosync`, `sync`, `async`, `adaptive`, `unset` (case-insensitive). An unrecognized
value is rejected with a precise SQL error.

Verify the effective mode of any table:

```sql
SELECT name, commitMode FROM wal_tables();
```

`commitMode` reports the **effective** mode (per-table override if set, else the
global) — so a `WITH commit_mode='adaptive'` table reads `adaptive` even when the
instance default is `nosync`.

Confirmed in source: global key `cairo.commit.mode`
(`PropertyKey.java:48`), default `adaptive`
(`PropServerConfiguration.java:2605`; an unrecognized value falls back to `nosync`,
`:2626`); `CREATE TABLE ... WITH
commit_mode='...'` (`SqlParser.java:1780`); `ALTER TABLE ... SET PARAM
commit_mode='...'` (`SqlCompilerImpl.java:1722`); token parsing incl. `unset`
(`CommitMode.fromString`, `CommitMode.java:101`); per-table-wins resolution
(`CommitMode.effectiveCommitMode`, `CommitMode.java:91`); `wal_tables().commitMode`
value (`WalTableListFunctionFactory.java:304`).

---

## 3. The RPO knob — the group-commit window `W`

```properties
cairo.adaptive.commit.group.window=50ms     # default 50ms (RPO <= 50ms); 0 = synchronous, zero-loss
```

`W` trades **recovery-point objective (RPO)** against **throughput**:

- **`W = 0` — zero loss.** Every acked commit is `fdatasync`'d before it
  returns; it is immediately device-durable. RPO is exactly zero. Cost is
  `sync`-class per-commit latency (you are paying a device flush per commit). Use
  when RPO must be 0.
- **`W > 0` (default: `50_000` us / 50 ms) — RPO ≤ `W`.** The commit returns after the transaction is sequenced
  (`msync`'d to the page cache, **not** yet device-durable); the `fdatasync` is
  performed by a batched flush within the window `W`, shared across concurrent
  commits. A power loss can therefore lose at most the last `W` microseconds of
  acknowledged commits. The durable-ack frontier (`localDurableSeqTxn`) advances
  only when the batch `fdatasync` completes.

> **Idle-tail caveat — the effective RPO bound is `W` + the background flush-sweep
> cadence, not `W` alone.** While commits keep arriving, each commit re-arms the
> in-window batch flush, so the bound is `~W`. When ingestion **stops** part-way
> through a window (an idle tail), the last batch's `fdatasync` is driven not by a
> following commit but by the background group-commit flusher in `WalPurgeJob`,
> which runs on a shared worker. The effective RPO for that final batch is therefore
> bounded by `W + shared-worker sleep cadence`
> (`shared.worker.sleep.timeout`), not by `W` alone. Keep the sweep cadence
> commensurate with `W`: a **large `shared.worker.sleep.timeout` combined with a tiny
> `W`** widens the idle-tail RPO well past `W` (the flusher may sleep far longer than
> the window before it fires). If you set a sub-millisecond `W`, keep
> `shared.worker.sleep.timeout` short too, or accept that the *idle-tail* worst case
> is the sleep cadence rather than `W`.

**Recommended starting point: `W` = 1–10 ms** (`1000`–`10000` us) for a
throughput-oriented deployment — RPO of 1–10 ms with latency approaching `nosync`.

**The throughput trade-off is workload-dependent:**

- **Small-batch / high-commit-rate** ingestion benefits most: the window batches
  many small per-commit flushes into one device flush, collapsing per-commit
  latency toward `nosync`.
- **Large-batch** ingestion is nearly `W`-insensitive: at thousands of rows per
  commit the single `fdatasync` is already amortized over the batch, so the window
  has little to batch. Leave `W` small.
- **Wide tables** (many columns) have the highest `W = 0` cost (one segment-column
  `fdatasync` per column) and therefore benefit most from `W > 0`.
- Gains **saturate**: past the point where the flush is fully amortized, a larger
  `W` buys little more throughput but a strictly larger RPO. Bigger is not better
  past the knee.

> **Performance figures below are directional only** (single shared dev box, short
> JMH runs, relative magnitudes). They establish direction, **not** absolute
> numbers, and are **not** a GA verdict — absolutes require controlled, quiesced
> hardware. Source: `docs/superpowers/specs/2026-07-17-adaptive-sp-c-perf-validation-design.md`.

Directional, from the SP-C harness (shared box, high-commit-rate `SMALL_BATCH`
workload, p99 commit latency):

| Config | p99 commit latency | vs `nosync` |
|---|---|---|
| `nosync` | ~37 us | 1× (baseline) |
| `adaptive`, `W = 0` (zero-loss) | ~24.7 ms | `sync`-class |
| `adaptive`, `W = 5 ms` | ~45 us | ~1.2× |

Reading: at the recommended production window, adaptive's p99 commit latency is
close to `nosync`; at `W = 0` it is `sync`-class, by design.

Confirmed in source: key `cairo.adaptive.commit.group.window`
(`PropertyKey.java:67`), default `50_000` (50ms), clamped to ≥ 0
(`PropServerConfiguration.java:1574`); the `W=0` zero-loss vs `W>0` RPO≤W semantics
are documented at `PropertyKey.java:57-63`.

---

## 4. QWP durable acknowledgements

A client receives durable-ack frames only when it opts in with the
`X-QWP-Request-Durable-Ack` handshake header. Without the header, no durable-ack frames
flow, so the client cannot safely retry within the group-commit window `W` (a retried
commit could double-apply). Durable-ack progress advances only for tables whose effective
commit mode is `adaptive`; on other modes an opted-in client receives the handshake but no
frames.

---

## 5. Observability

Two surfaces, matching QuestDB's established split: **`wal_tables()`** for
per-table drill-down, and **Prometheus** for process-level aggregates.

### 5.1 `wal_tables()` columns

```sql
SELECT name, commitMode, sequencerTxn, writerTxn,
       localDurableSeqTxn, durableEpochSeqTxn, walRetentionTxn,
       lastEpochTs, recoveryIncarnation
FROM wal_tables();
```

| Column | Type | Meaning |
|---|---|---|
| `commitMode` | STRING | Effective commit mode for the table (`adaptive`, `nosync`, …). |
| `sequencerTxn` | LONG | The latest acked (sequenced) transaction — the visible/apply frontier's upper bound. |
| `writerTxn` | LONG | The transaction the table writer has applied. |
| `localDurableSeqTxn` | LONG | The **local durable frontier**: the highest seqTxn whose WAL is device-durable (`fdatasync`'d). Advances only on adaptive tables. |
| `durableEpochSeqTxn` | LONG | The seqTxn of the last durable **epoch** (the fast-boot anchor / recovery base). |
| `walRetentionTxn` | LONG | The WAL retention floor — the seqTxn below which WAL segments may be purged. Under adaptive this equals `durableEpochSeqTxn` (the epoch is the retention floor). |
| `lastEpochTs` | TIMESTAMP | Wall-clock time of the last durable epoch; `NULL` if no epoch has been taken yet. |
| `recoveryIncarnation` | LONG | Per-table count of recovery roll-forwards — bumps each time this table is recovered. A crash/recover detector. |

**Durable-frontier lag** (RPO exposure under `W > 0`) is **computed**, not a
stored column:

```sql
-- per-table durable-frontier lag: acked txns not yet locally durable
SELECT name, sequencerTxn - localDurableSeqTxn AS durable_lag
FROM wal_tables()
WHERE commitMode = 'adaptive';

-- per-table epoch/retention lag: how far recovery would have to replay
SELECT name, sequencerTxn - durableEpochSeqTxn AS epoch_lag
FROM wal_tables();
```

Confirmed in source (`WalTableListFunctionFactory.java`): `commitMode` STRING
(`:409`), `durableEpochSeqTxn` LONG (`:411`), `walRetentionTxn` LONG (`:413`, set
from `getDurableEpochSeqTxn()` at `:314` — hence equal to `durableEpochSeqTxn`),
`recoveryIncarnation` LONG (`:415`), `localDurableSeqTxn` LONG (`:417`),
`lastEpochTs` TIMESTAMP (`:419`, `NULL` when no epoch, `:321`); plus existing
`sequencerTxn` (`:400`) and `writerTxn` (`:396`).

### 5.2 Prometheus metrics

Three adaptive-related series are exported on the metrics endpoint. **Get the
scraped names right** — counters carry a `_total` suffix, gauges do not, and all
QuestDB metrics carry a `questdb_` prefix.

| Registered name | Type | Scraped name | Meaning |
|---|---|---|---|
| `wal_apply_local_durable_seq_txn` | gauge | `questdb_wal_apply_local_durable_seq_txn` | Aggregate local durable (adaptive-`fdatasync`) frontier. |
| `wal_apply_seq_txn` | gauge | `questdb_wal_apply_seq_txn` | Aggregate acked/sequenced frontier (all WAL tables). |
| `wal_adaptive_epoch_advances` | counter | `questdb_wal_adaptive_epoch_advances_total` | Count of successful durable-epoch advances. |
| `wal_adaptive_recovery_events` | counter | `questdb_wal_adaptive_recovery_events_total` | Process-wide count of successful table recoveries at boot. |

**Global durable-frontier lag** is computed Prometheus-side (it is deliberately
**not** a third gauge — both operands are already exported, so the lag definition
lives in exactly one place, the query):

```promql
questdb_wal_apply_seq_txn - questdb_wal_apply_local_durable_seq_txn
```

> **Caveat — this global lag OVERSTATES on mixed adaptive + `nosync` deployments.**
> `questdb_wal_apply_seq_txn` advances for **all** WAL tables, but
> `questdb_wal_apply_local_durable_seq_txn` advances **only** on adaptive tables'
> durable flushes. So on an instance running both modes, `nosync` tables inflate
> the numerator and the global difference reads higher than the real adaptive-table
> lag. For an accurate figure, use the **per-table** `wal_tables()` computation in
> §5.1, filtered to `commitMode = 'adaptive'`.

Confirmed in source (`WalMetrics.java`): registrations
`wal_apply_local_durable_seq_txn` gauge (`:52`), `wal_adaptive_epoch_advances`
counter (`:51`), `wal_adaptive_recovery_events` counter (`:53`), `wal_apply_seq_txn`
gauge (`:55`). Scrape naming: `questdb_` prefix + `_total` for counters
(`PrometheusFormatUtils.java:32,35-39`; `CounterImpl.java:58-60`); gauges get the
prefix but **no** `_total` (`AbstractLongGauge.java:51-54`). Update/increment sites:
local-durable gauge at `SeqTxnTracker.java:315`; the aggregate seqTxn gauge advances
for all tables at `SeqTxnTracker.java:192,233` (grounding the mixed-deployment
caveat); epoch-advances at `ApplyWal2TableJob.java:791`; recovery-events at
`RecoveryCoordinator.java:267`.

---

## 6. Operations runbook

### What the signals mean

- **`localDurableSeqTxn` should track `sequencerTxn` closely.** The gap
  (`sequencerTxn - localDurableSeqTxn`) is your live RPO exposure: acked commits not
  yet on the device. Under `W = 0` it is ~0; under `W > 0` it oscillates up to
  roughly one window `W` of commits and returns to 0 after each batch flush.
- **`durableEpochSeqTxn` / `lastEpochTs`** show the last fast-boot anchor. The gap
  `sequencerTxn - durableEpochSeqTxn` is how far a crash would have to replay on the
  next boot (and how much WAL is being retained).
- **`recoveryIncarnation` / `questdb_wal_adaptive_recovery_events_total`** are your
  crash detector: an unexpected increment means a table was recovered at boot.

### Alerting

- **"Durable frontier falling behind."** Alert when the **per-table** durable lag
  (`sequencerTxn - localDurableSeqTxn` on adaptive tables) stays elevated and
  growing rather than oscillating around `W`. A persistently rising lag means the
  batched WAL flush is not keeping up — investigate device saturation or a stalled
  flush. (Remember §5.1: use per-table, not the global Prometheus difference, on
  mixed deployments.)
- **`lastEpochTs` stale / `questdb_wal_adaptive_epoch_advances_total` flat** while a
  table is actively ingesting means epochs are not advancing — recovery time and WAL
  retention will grow unbounded. Check that `cairo.adaptive.epoch.interval` is not
  negative (disabled) and that the apply worker is healthy.
- **Unexpected `recoveryIncarnation` / recovery-events increment** flags a crash and
  a successful recovery — cross-check with an unclean shutdown.

### Recovery expectations

Worst-case boot recovery time decomposes as:

```
recovery ≈ fixed_boot + (post-epoch WAL tail ÷ catch-up rate)
```

- **Fixed boot** (roll the table to the last durable epoch cut) is ~constant,
  independent of how much WAL is queued — directionally single-digit to low-tens of
  milliseconds.
- **Catch-up** (re-applying the WAL tail past the epoch) is ~linear in the size of
  that tail.

You bound the tail — and therefore worst-case recovery time — with whichever of
the two epoch triggers fires first: the **interval** (wall-clock cadence) or the
**row cap** (un-epoched applied-row backlog). At typical ingest rates the row cap
binds first and does the actual bounding; the interval is a backstop for quiet
tables (see below). (All recovery figures are directional; controlled HW is needed
for absolutes.)

### Tuning the three knobs

**`cairo.adaptive.commit.group.window` (`W`) — RPO ↔ throughput** (see §3).
Default `50_000` us (50ms). Start at 1–10 ms for a throughput-oriented deployment;
`0` for zero-loss. Increase only up to the saturation knee; beyond it you buy RPO
exposure for no throughput gain.

**`cairo.adaptive.epoch.interval` — recovery-time ↔ apply-overhead, time-based.**

```properties
cairo.adaptive.epoch.interval=60000     # default: at most one epoch per 60 seconds per table
```

- **Larger interval** → fewer time-triggered epoch flushes, but a longer tail on a
  table that is too quiet to ever hit the row cap below.
- **Smaller interval** → faster worst-case recovery for quiet tables, more apply
  overhead.
- **`0`** → take an epoch on **every** apply batch (fastest recovery, highest
  overhead — the worst case for apply cost).
- **Negative** → **epochs disabled entirely**, including the row cap below. This is
  NOT a routine opt-out — it removes the durability *fast-boot anchor*, so it has two
  compounding consequences: (1) **unbounded WAL retention** — the epoch is the
  `WalPurgeJob` retention floor, so with no epoch the WAL below the frontier is never
  purged and grows without bound for as long as the table ingests; and (2)
  **full-replay recovery** — a restart must replay the *entire* WAL from the base
  cut, so boot time grows with the retained WAL. Use only as a deliberate operator
  opt-out / test isolation, and only on tables whose WAL you are certain stays small.

**`cairo.adaptive.epoch.max.rows` — recovery-time ↔ apply-overhead, row-based.**

```properties
cairo.adaptive.epoch.max.rows=5000000     # default: force an epoch once this many
                                           # rows have been applied since the last one
```

- Independent of elapsed time, an epoch also fires as soon as the **un-epoched
  applied-row backlog** for a table (rows applied since its last durable epoch)
  reaches `max.rows`. On an actively-ingesting table this is what actually bounds
  the replay tail — the interval alone, at its 60 s default, would otherwise let
  the tail (and the pinned/retained WAL) grow with the ingest rate.
- **Larger cap** → fewer row-triggered epoch flushes under sustained high-rate
  ingest, but a larger tail when the cap is what binds.
- **Smaller cap** → faster worst-case recovery under high-rate ingest, more apply
  overhead.
- **`<= 0`** → **the row cap is disabled**; only the interval bounds the tail (the
  original, interval-only behavior).

**Why the interval could move from `1000` ms to `60000` ms.** The two triggers are
not independent settings so much as two bounds on the same replay tail, and only
the tighter one matters at any given moment:

- **Active ingest** → the table accumulates `max.rows` un-epoched rows well before
  60 s elapses, so the **row cap binds**. At the default 5,000,000 rows this keeps
  the post-epoch tail — and hence the recovery replay and the WAL retained for it —
  to directionally ~1–2 seconds of ingest at typical high write rates, regardless
  of how the interval is set.
- **Idle / low-rate ingest** → the table never reaches `max.rows` within 60 s, so
  the **interval binds** as a backstop: an epoch still fires every 60 s, so a quiet
  table's tail never grows unbounded either.

Because the row cap now does the safety-bounding under load, the interval no
longer has to be short to keep worst-case recovery bounded — it only has to bound
the idle case — which is why the shipped default moved from `1000` ms to `60000`
ms: fewer time-triggered epoch flushes (less apply overhead) with no loss of
bounding, since the row cap covers the case the shorter interval used to guard.

Both triggers amortize the same per-epoch cost (directionally ~2 ms, paid whenever
either one fires) to comfortably under 1% of apply throughput at typical ingest
rates. Derive your actual worst case from your recovery SLO:
`worst-case recovery ≈ fixed_boot + (rows_since_last_epoch ÷ catch-up_rate)`, where
`rows_since_last_epoch` is bounded by `max.rows` under load, or by
`ingest_rate × interval` when the table is idle enough that the row cap never
fires.

**Recovery kill-switch.**

```properties
cairo.adaptive.recovery.roll.forward.enabled=true    # default true
```

Leave this `true`. Setting it `false` makes the boot-time epoch roll-forward a
no-op — an operator kill-switch / negative-control hook, not a normal setting.

Confirmed in source: `cairo.adaptive.epoch.interval` (`PropertyKey.java:53`;
`0` = every batch, negative = disabled, per `:50-52`), default `60000`
(`PropServerConfiguration.java:1570`); `cairo.adaptive.epoch.max.rows`
(`PropertyKey.java:57`; `<= 0` disables the cap, per `:54-56`), default
`5_000_000` (`PropServerConfiguration.java:1571`); the two triggers combined, OR'd
together (`ApplyWal2TableJob.maybeAdvanceDurableEpoch`,
`ApplyWal2TableJob.java:703-709`); `cairo.adaptive.recovery.roll.forward.enabled`
(`PropertyKey.java:60`), default `true` (`PropServerConfiguration.java:1575`),
consumed at `RecoveryCoordinator.java:89`.
Tuning framing from `docs/superpowers/specs/2026-07-17-adaptive-sp-c-perf-validation-design.md` §7.

---

## 7. Upgrade and downgrade

Adaptive is **opt-in** and turning it on or off is a **runtime commit-mode change**:
there is **no on-disk migration and no file rewrite**. All new adaptive artifacts
are additive and gated, so upgrade and downgrade are safe and reversible.

### Enabling on an existing database (upgrade)

Recommended order for a multi-node / load-balanced deployment:

1. **Roll the new binary** across all nodes with commit mode unchanged.
2. **Verify** the cluster is healthy on the new binary.
3. **Enable adaptive** (per table first, then globally if desired — §2).

Existing tables are untouched on disk until their next commit. Once adaptive, a
table's WAL commits are made durable, it begins taking durable epochs (the
`_snapshot` / `_txn.epoch` / `_cv.epoch` artifacts appear in its directory), and its
WAL segments are retained back to the last durable epoch (the recovery floor).

### Turning adaptive off (downgrade)

```sql
ALTER TABLE trades SET PARAM commit_mode='nosync';   -- or 'sync' / 'async'
-- or revert to the global default:
ALTER TABLE trades SET PARAM commit_mode='unset';
```

Globally, set `cairo.commit.mode` back to `nosync`.

What happens:

- The table immediately applies WAL under the new mode. No suspend, no corruption;
  `wal_tables()` reports the new mode.
- The leftover `_snapshot` / `_txn.epoch` / `_cv.epoch` files are **not** deleted —
  they stay on disk but become **inert**. The live read/apply path never opens them.
- The WAL purge floor drops from the durable epoch back to the applied seqTxn (the
  epoch floor applies only under adaptive), so normal `nosync` retention resumes.
- **On the next restart**, recovery sees the leftover `_snapshot` marker but **skips
  roll-forward** because the table's effective mode is no longer adaptive — no
  rollback, no data loss, no suspend.

After downgrade the table has the crash semantics of its new mode (`nosync` = the
pre-adaptive default). The adaptive epoch is a recovery mechanism that only applies
while adaptive is on. Leftover `.epoch`/`_snapshot` files are small and safe to
leave; drop and recreate the table if you want them gone.

### Mixed-version (rolling upgrade) — old binaries are safe

Every adaptive artifact is **inert to a binary that does not understand it**:

- `_snapshot`, `_txn.epoch`, `_cv.epoch` are **separate files** an old binary never
  opens.
- The `_meta` `commit_mode` field lives in reserved header space an old binary never
  reads — it defers to its own global `cairo.commit.mode`.
- The `_txn` / `_cv` / `_event` body/record checksums are magic/zero-gated trailers
  an old binary never inspects.

So an old binary boots on an adaptive-written database, reads all tables, and serves
correct data. What it does **not** do is honor the per-table `commit_mode` override
(it has no such concept) or run adaptive recovery. This is why you should **finish
the binary upgrade before flipping commit mode**: flipping earlier is safe but the
old nodes simply won't act adaptively.

**Rollback order:** (1) turn adaptive off (above); (2) roll back binaries. Turning
adaptive off first keeps the mental model simple — the epoch artifacts are already
inert to the old binary (they would be inert regardless).

> A real-old-binary open matrix and rolling-upgrade cluster test are an external
> verification step (they cannot run inside the JUnit suite). See the SP-E runbook.

Full detail and the per-artifact inertness evidence:
`docs/superpowers/specs/2026-07-17-adaptive-sp-e-upgrade-downgrade-runbook.md`.
Downgrade-skips-roll-forward gate confirmed at `RecoveryCoordinator.java:89`.

---

## Appendix — configuration key reference

| Key | Default | Semantics |
|---|---|---|
| `cairo.commit.mode` | `adaptive` | Global commit mode. `nosync` \| `sync` \| `async` \| `adaptive`. |
| `cairo.adaptive.commit.group.window` | `50ms` (`50_000` us) | Group-commit / RPO window. `0` = `fdatasync`-before-ack (zero loss). `> 0` = batched flush, RPO ≤ `W`. Clamped to ≥ 0. |
| `cairo.adaptive.epoch.interval` | `60000` | Min interval between durable epochs per table. `0` = every apply batch. Negative = epochs disabled (also disables the row cap below). |
| `cairo.adaptive.epoch.max.rows` | `5_000_000` | Forces an epoch once this many rows are applied to a table since its last one, independent of the interval. Bounds WAL retention + recovery replay under active ingest. `<= 0` disables the cap (interval-only). |
| `cairo.adaptive.recovery.roll.forward.enabled` | `true` | Run the durable-epoch recovery roll-forward at boot. `false` = no-op kill-switch. |

Per-table override (wins over the global): `WITH commit_mode='…'` at `CREATE TABLE`,
or `ALTER TABLE … SET PARAM commit_mode='…'`. Tokens: `nosync`, `sync`, `async`,
`adaptive`, `unset`.

---

*This is a user/operator guide. The internal design and validation specs live under
`docs/superpowers/specs/` (`2026-06-25-adaptive-commit-mode-design.md`,
`2026-07-17-adaptive-sp-c-perf-validation-design.md`,
`2026-07-15-adaptive-sp-f-metrics-slice-design.md`,
`2026-07-17-adaptive-sp-e-upgrade-downgrade-runbook.md`). All configuration keys,
defaults, metric names, SQL syntax, and `wal_tables()` columns above were verified
against the source tree on branch `nw_adaptive_commit`.*
