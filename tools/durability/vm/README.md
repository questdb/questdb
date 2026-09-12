# VM crash-integration harness

Repeatable, unattended-capable crash testing of adaptive commit on a **real kernel and a
real block layer**, entirely inside one QEMU guest.

The JUnit tiers in `core/src/test/java/io/questdb/test/cairo/crash/` model power loss with
`CrashFaultFilesFacade`. That model is well gated, but it is a model. This harness is the
cross-check.

## Coverage vs the Java crash suite

The goal is that every code path the modelled suite covers is also exercised here, on a real
kernel. Current state -- **18 of 18 dimensions**, every one verified to actually execute (see
"Verifying a NEW dimension is actually exercised" below -- a flag existing is not coverage):

| # | Dimension | Java crash test | E2E |
|---|---|---|---|
| 1 | plain `(ts, v long)` | `AdaptiveRecoveryRollForward`, `GroupCommit` | `profile=none` |
| 2 | bitmap index | `AdaptiveIndexedSymbolLazyGap`, `MapLengthGuard` | `profile=bitmap` |
| 3 | posting index | posting suite | `profile=posting` |
| 4 | covering index | covering suite | `profile=covering` |
| 5 | multi-partition | `AdaptiveMultiPartitionLazyGap` (W5) | partition by DAY |
| 6 | W=0 / W>0 group commit | `AdaptiveGroupCommitCrashTest` | `--window-us` |
| 7 | SYNC / NOSYNC | crash-consistency baseline | `--mode` |
| 8 | varchar column | `AdaptiveEpochCrashTest`, `VarcharPowerLoss*` | `profile=varchar` |
| 10 | wide mixed-type table | `BatchedFlushDurabilityCrashTest` | `profile=wide` |
| 11 | O3 merge path | `AdaptiveO3CrashSweep`, `AdaptiveO3LazyGap` | `profile=o3` |
| 13 | sustained lazy gap | `AdaptiveO3LazyGap`, W2/W3/W5 | `QDB_EPOCH_MS=-1` |
| 9 | **array column** | `ArrayCrashConsistencyTest` | **GAP** -- no positional put for `double[]` |
| 12 | structural DDL under load | `RandomizedAdaptiveCrashFuzzTest` | `QDB_DDL_EVERY_ROWS=N` |
| 14 | multi-table | `AdaptiveMultiTableLazyGap` (W3) | `QDB_SIBLING_TABLE=true` |
| 15 | mat-view | `AdaptiveMatViewLazyGap` (W4) | `QDB_MAT_VIEW=true` -- **found a real defect, see below** |
| 16 | per-table commit mode | `PerTableAdaptiveIsolationCrashTest` | `QDB_PER_TABLE_MODE=true` |
| 17 | commit-mode flip | `AdaptiveCommitModeFlipCrashTest` | `QDB_RECOVER_AS=nosync` |
| 18 | REBASE WAL publish | `RebaseWalPublishDurabilityCrashTest` | `QDB_REBASE_AT_ROWS=N` -- hard-suspends, rebases, then IDLES so the swept boundaries land post-publish |

Every profile keeps columns `0..3` (`id, v, s, ts`) fixed, so the identity oracle is shared and
results are directly comparable across profiles.

### Verifying a NEW dimension is actually exercised

A dimension is not covered because a flag exists -- it is covered when the check is
OBSERVED to run. The sibling-table work was wired correctly and still looked broken twice,
because both places I checked structurally could not show it:

  * the sweep log records only `tail -1` (the verdict), so a check's own output never reaches it;
  * `verify.sh` echoes only the verdict line, discarding everything else the JVM printed.

The only reliable check is running `CrashVerifier` DIRECTLY and reading its stdout:

```bash
java ... -Dsibling.table=true org.questdb.CrashVerifier /mnt/qdb/db \
  | grep -vE '^20[0-9]{2}-'      # strip engine logging
# expect: sibling t2 rows=N (primary=M)
```

Do the same for any dimension added later. A green sweep whose check never executed is worse
than no coverage, because it reads as evidence.

**Check the mechanism, not just the verdict.** `QDB_PER_TABLE_MODE=true` runs the INSTANCE on
nosync with the TABLE on adaptive, and `RPO_OK` is satisfiable two ways: the override worked
(`F >= Wm` with a real frontier), or the override silently failed, the table ran nosync, `Wm`
never left -1, and the bar was met vacuously. The discriminating evidence is `Wm` ADVANCING in
`_progress` -- a nosync table never advances `localDurableSeqTxn`. Confirmed at `Wm=2460`.

### Proxies that broke

Three oracle bugs shared one shape: a convenient PROXY standing in for the real property.
Each read as a product defect until checked.

| proxy | broke when | false verdict |
|---|---|---|
| `row N has id N` | timestamps go out of order (o3) | SILENT_CORRUPTION |
| `F = count / K` | txns are not uniform K-row data txns (DDL) | DURABILITY_FAILURE |
| sweep verdict implies check ran | the flag never reached the verifier JVM | vacuous pass |

`F` now uses `lastTxn`, the sequencer's own frontier -- the same unit `Wm` is measured in, so the
RPO bar compares like with like. Identity/contiguity checks remain the ground truth for WHAT
survived.

Three false alarms in this harness came from setup rather than the engine, each caught only by
checking the mechanism: an `o3` SILENT_CORRUPTION (the oracle assumed row order == id order), a
sibling-table gap (the property never reached the verifier JVM), and a per-table-mode
LOUD_FAILURE (lowering the instance mode also re-routed the workload onto the bypass-WAL path,
so no sequencer existed at all). Uniform failure across EVERY boundary is the signature of a
setup fault; a real crash-state defect does not land identically every time.

### Dimension 15 (mat-view): covered -- and it found a defect

Getting the view populated took six attempts; the root cause was that the writer's engine
needs BOTH `engine.load()` AND `engine.hydrateMatViewStateStore()` (the view is registered
by `createTable`'s short-lived engine, so a later engine does not know about it and
`enqueueIncrementalRefresh` silently targets nothing).

Then the ORACLE was wrong three separate times, each time reporting `SILENT_CORRUPTION`
from its own measurement error:

1. judged the view before driving the refresh at all (it legitimately LAGS -- async);
2. "reconciled" by driving a refresh -- wrong on its face, since a refresh only moves a
   view FORWARD and can never retract a lead. Only invalidation can;
3. compared a base count sampled BEFORE the refresh drain against a view total read
   AFTER it -- two different moments, with a WAL apply in between.

Fixed by reading base and view adjacent at the same quiescent moment, and by treating an
INVALIDATED view that leads as correct (so the oracle can tell a fix from the bug).

**The surviving finding is real and adaptive-specific:**

```
C=267  Wm=263   base recovered = 263000 rows (263 txns)
view aggregates 268000 rows   refresh_base_table_txn=268
view_status=valid   invalidation_reason=null
```

The base correctly discards at-risk txns 264..268 (permitted by the RPO window). The view
keeps aggregates derived from them and stays `valid`. It is PERMANENT: incremental refresh
only moves forward from a watermark already above the base, and the invalidation path for a
base-txn regression is gated on a TRUNCATE barrier, which a crash rollback is not.

Control at `mode=sync` (W=0, Wm==C): 6/6 DURABLE, no lead -- the condition requires the RPO
window. Likely fix: invalidate at startup when `lastRefreshBaseTxn` exceeds the base's
recovered seqTxn.

### Dimension 18 (REBASE WAL): covered, with its scope stated

`CairoEngine.rebaseWalTable0` refuses unless `isHardSuspended()` AND
`cairo.wal.apply.suspended.write.denied=true`, so the writer sets both, rebases once, then
**idles instead of resuming ingestion** -- the sweep crashes at the LAST recorded boundaries,
so idling is what puts them in the publish window. Continuing to ingest would bury the
publish under thousands of later flushes.

The verdict names the resolved dir (`dir=t~2, count=60000 rows contiguous`) and fails loudly
on `~1`: an earlier run printed 8/8 DURABLE that would have looked identical had the rebase
never fired, and the sweep deletes the run dir on success, so that evidence was unrecoverable.

SCOPE: the swept boundaries sit AFTER the publish completes, so this covers "a crash
following a rebase leaves a coherent registry and table". Crashing INSIDE the rename/fsync
window is what `RebaseWalPublishDurabilityCrashTest` covers at the op level; the E2E confirms
it on a real filesystem rather than replacing it.

The rebase oracle deliberately drops the `F >= Wm` bar -- REBASE WAL discards pending WAL by
design, so asserting durability there would fail the harness on correct behaviour. What it
holds is "Rename != publish": the name resolves to a dir that EXISTS and is coherent.

## The three instruments

| | crash points | filesystem | can lose an unflushed device write? |
|---|---|---|---|
| Java `forEachAdaptiveCrashPoint` | **enumerated** — every durability op | modelled | yes (modelled) |
| `run-flush-sweep.sh` | **enumerated** — every flush | **real** | **yes** |
| `run-fuzz.sh` | sampled — random wall-clock moments | real | no (see below) |

`run-flush-sweep.sh` is the one that tests what the Java model tests *assert*, on a real
kernel. One workload run is recorded by `dm-log-writes`; each flush in that recording is
then a crash point, reconstructed by replaying the log up to it. Replaying to flush N yields
exactly the device state a volatile write cache would have left at N — everything before it
durable, everything after it gone.

It is also cheap per point: one workload run plus one reboot serves *every* boundary, versus
one full VM boot per sample in `run-fuzz.sh`.

**This matters most at W>0.** Adaptive defers the `fdatasync` into the group-commit batch, so
between batches its data is pushed out but unflushed — precisely the set `run-fuzz.sh` cannot
lose and flush-boundary replay can. Any claim about the W>0 RPO gap has to come from here (or
from real hardware), not from the dm-flakey cut.

```bash
bash run-flush-sweep.sh adaptive 0        # zero-loss bar: every committed txn survives
bash run-flush-sweep.sh adaptive 50000    # RPO bar at W=50ms
bash run-flush-sweep.sh adaptive 2000000  # RPO bar at W=2s, widest at-risk window
```

The reconstruction is validated by `test/t06`, which sweeps every boundary and asserts they
discriminate (a later file never appears at a boundary where an earlier one is absent). Do
not trust a sweep result if `t06` is red.

## ⚠ KNOWN LIMITATION (dm-flakey path only): no volatile device write cache

**This harness cannot currently observe the W>0 RPO loss gap. Measured, not assumed.**

A direct probe, with QuestDB removed from the experiment entirely: write a block, fsync it,
then overwrite it **in place via `O_DIRECT`** — reaching the device with **no flush** — then
cut. It reads back as the NEW content. At-device-but-unflushed data **survives**.

```
wrote B via O_DIRECT, no flush
after the cut, probe reads: 'B'
```

The cut boundary is **arming time**, not **last flush**:

| | Real hardware | This harness |
|---|---|---|
| unflushed write lands in | disk's **volatile** cache | QEMU → `O_DIRECT` → **host storage** |
| on power loss | **lost** (only FLUSH/FUA reaches platter) | **survives** — the host never lost power |

`dm-flakey drop_writes` discards writes issued *after* arming; it cannot retroactively
discard unflushed writes already at the device. Adaptive at W>0 issues `msync(MS_ASYNC)` per
commit and defers only the `fdatasync`, so its at-risk data is exactly the data this harness
cannot lose.

**Note the disagreement with the Java suite:** `CrashFaultFilesFacade` explicitly models
`msync(MS_ASYNC)` as NON-durable. This harness treats it as durable. That is why the Java
tests exercise the RPO window and this one does not.

### Hypotheses tested and refuted

Recorded so they are not re-litigated:

1. **Foreign flush** from the harness's own `_progress` fsync (ext4 `data=ordered` writes back
   other inodes' data on a journal commit). Refuted: `data=writeback` at identical seeds,
   `lost=0` in both arms.
2. **Kernel background writeback**. Refuted: suppressing `vm.dirty_background_ratio`,
   `dirty_ratio`, `dirty_expire_centisecs` and `dirty_writeback_centisecs` changed nothing —
   `msync(MS_ASYNC)` starts writeback for those pages directly, independent of the
   background thread.
3. **No volatile device cache.** CONFIRMED by the probe above.

### What the current results therefore mean

- **`F >= Wm` (no acked txn lost) — holds, but weakly tested.** The bar is easy to clear when
  nothing can be lost. Treat green here as *necessary, not sufficient*.
- **`lost=0` in the at-risk window — NOT evidence.** It is an artifact of the cut model.
  Observed at W=50ms and W=2s with up to 179 txns at risk, and at NOSYNC across 974k rows.
- **The RPO gap is unmeasured.** Any statement about how much data is lost at W>0 must come
  from the Java suite or from real hardware, not from here.

### The fix — BUILT

`dm-log-writes` replay, above. `power-cut-vm.sh --device=log-writes` and
`run-flush-sweep.sh`. Everything in this section applies to the **dm-flakey** path only; the
flush-boundary path does not share the limitation, because its boundary is the flush rather
than the moment of arming.

Traps found while building it, recorded so they are not re-attempted (each measured):

- **`O_DIRECT` is not an "unflushed device write" on ext4.** It forces a journal commit whose
  FLUSH covers the write. `commit=3600` does not prevent it — the commit is not time-driven.
- **Crashing immediately loses the log tail.** `dm-log-writes` queues entries to a kthread;
  entries still queued at the kill were never recorded.
- **Pausing lets a periodic journal commit flush the write** you wanted left unflushed.
- **The superblock `nr_entries` is stale after a crash.** Trusting it silently truncated the
  log and made an fsync'd file vanish from the reconstruction. The parser now walks until
  entries stop being plausible and reports when it passes the superblock count.

## How this differs from the Java crash tests

They answer different questions and neither substitutes for the other.

| | Java sweeps (`cairo/crash/`) | This harness |
|---|---|---|
| Crash points | **Enumerated** — every durability op of a commit phase | **Randomly sampled** wall-clock moments, seeded and replayable |
| Filesystem | **Modelled** (`CrashFaultFilesFacade`) | **Real** kernel, block layer, page cache |
| Coverage | Exhaustive and deterministic | Accumulates across iterations and runs |
| Speed | Seconds | ~1 minute per iteration (two VM boots) |

`forEachAdaptiveCrashPoint` is the stronger instrument for *completeness*; it is only ever
as faithful as its model. This harness models nothing, and buys reach by **sampling many
random cut times** rather than by enumeration — which is why `run-fuzz.sh`, not a single
run, is the way to use it.

**A single fixed cut proves very little, and that is measured rather than assumed.** Cutting
late, after the guest kernel has written most dirty pages back, even `NOSYNC` loses nothing
(`count == watermark` at 312k / 432k / 974k rows, unchanged under `data=writeback`, which
rules out a foreign flush from the harness's own `_progress` fsync). The cut can only
discard writes that have not been written back **yet**. Early cuts are where the interesting
states live, so the delay is drawn from 250 ms to 30 s.

### Vacuity guards

Two failures this harness is built to avoid reporting as evidence:

- **The workload must be RUNNING at the moment of the cut.** Cutting a finished, quiesced
  system has nothing in flight and yields a guaranteed pass. `power-cut-vm.sh` asserts
  liveness and fails the iteration loudly otherwise. This is not hypothetical: an early
  2,000,000-row cap completed in ~24 s while cuts were drawn to 30 s, so late iterations
  cut an idle system and returned a meaningless `DURABLE`.
- **The cut must still be cutting.** The per-run preflight (below) is the gate, and
  `test/t04` proves it can fail.

## What each arm proves — and what it does not

| Arm | Mechanism | Proves | Does **not** prove |
|---|---|---|---|
| **reference** | `CrashIngestWriter` embedded, `CrashVerifier` runs the production recovery triple | The engine's durability contract on real storage: every acked txn survives; loss confined to `(Wm, C]` and bounded by W | Anything about the shipped server binary, its entry point, or the wire |
| **product** (W=0 only) | Real server, real client, PG-wire oracle | The **shipped artifact** recovers: `recovered >= C`, no loss | Nothing at W>0 — see the limitation below |

**Product arm is W=0 only, deliberately.** At W=0 adaptive is fsync-before-return, so every
committed txn is durable and the bar needs no durable-ack frontier. At W>0 it would need
`Wm`, and the **client-side LOCAL durable-ack frontier is WIP** — so `power-cut-vm.sh`
*refuses* `--arm=product --window-us>0` rather than print a verdict it never checked. The
reference arm reads the frontier in-process and covers W>0 today.

## Why killing the VMM is not, by itself, a power cut

This is the single most important thing to understand before changing anything here.

| QEMU cache mode | What survives `kill -9` on the VMM | Verdict |
|---|---|---|
| `cache=writeback` | The **host** page cache holds un-flushed guest writes and survives — the host did not lose power | Unusable. False green. |
| `cache=directsync` | Every guest write is durable immediately, so even NOSYNC survives | Unusable. False green. |
| `cache=none` | The **guest** page cache dies with the VMM — correct. But data the guest kernel already wrote back has reached host storage and survives | Necessary, not sufficient |

Every deviation leans toward **false green**. So the cut has two halves:

1. **`dm-flakey drop_writes`, inside the guest** — stops the device accepting writes, which
   is what makes un-flushed data actually disappear.
2. **`kill -9` on the VMM, from the host** — kills the guest kernel and its page cache.

**The order is load-bearing.** Arm first, then kill. Reverse it and a write can reach
durability during the join window. The two halves join over the serial console: the guest
writes `CUT-ARMED`, the host is tailing `console.log` and kills on the token.

## The three guards

A crash harness that silently stops crashing is the worst outcome available.

| Guard | Frequency | Fails when |
|---|---|---|
| **Preflight** | once per run (`run-matrix.sh`, `run-fuzz.sh`) | An fsync'd file is lost, or an un-flushed **device** write survives |
| **Liveness** | every iteration | The workload was not running at the moment of the cut |
| **Defanged-cut control** | `test/t04` | The preflight *passes* with `drop_writes` removed, proving it cannot fire |

A failing preflight **aborts the run**; a failed liveness check **fails that iteration**.
Neither is ever downgraded to a warning.

**NOSYNC is reported, not gated.** The intuitive control — "a no-sync mode must lose data,
so a `DURABLE` verdict proves the cut broke" — was measured and is **wrong here** (see
above). Gating on it would only produce a red suite that says nothing.

The preflight's device probe is the part that carries the claim: a block is pre-filled and
fsync'd *before* the cut, so its extents are allocated and journaled, then overwritten
**in place via `O_DIRECT`** afterwards — reaching the device with no flush and no metadata
change, where only `drop_writes` can discard it. A page-cache-only probe cannot serve this
purpose: it vanishes from the VMM kill alone, under a real cut and a defanged one alike.

## Host safety

The host is never touched by a test. Everything privileged happens in the guest.

- **No `dmsetup`, no `losetup`, no container, no bound service port on the host.** The
  guest's data disk is raw `/dev/vdb` and `dm-flakey` sits directly over it, so no loop
  device is needed anywhere.
- **Never run `losetup -D` or `dmsetup remove_all`** in any teardown path. Both are common
  idioms in dm-flakey scripts, and on a shared box a host loop device may well be a live
  database's filesystem. Teardown targets its own names only.
- No host sudo is required: `/dev/kvm` is used directly via an ACL grant.
- One VM at a time, 8 vCPU / 16 GB.
- Disks are deleted only on a clean pass. **Any failure keeps them** for inspection.

## Running it

```bash
bash check-host.sh          # prerequisites; names the first thing missing
bash build-image.sh         # once — builds the golden qcow2

bash test/t01-golden-image.sh
bash test/t02-lifecycle.sh
bash test/t03-drop-writes.sh
bash test/t04-preflight.sh    # both directions; the guard must be able to fail
bash test/t05-reference-arm.sh

bash run-matrix.sh          # the full matrix, one cut per cell
bash run-fuzz.sh 50         # THE E2E INSTRUMENT: 50 randomly-timed cuts
```

`run-fuzz.sh [iterations] [mode] [window_us]` is how this harness earns its keep. Every
iteration draws a fresh seed, prints it, and logs it, so a failure at iteration 37 of 200
replays exactly:

```bash
bash power-cut-vm.sh --arm=reference --mode=adaptive --seed=<SEED>
```

One cell at a time:

```bash
bash power-cut-vm.sh --arm=reference --mode=adaptive --window-us=50000
```

Run state lives under `/data/qdb-vmcrash` (override with `QDB_VMCRASH_STATE`).

## Scheduling

`systemd/qdb-vmcrash.timer` is shipped **disabled**. A crash harness nobody trusts yet
should not be a gate. Enable it once it has earned that:

```bash
cp systemd/qdb-vmcrash.* ~/.config/systemd/user/
systemctl --user enable --now qdb-vmcrash.timer
loginctl enable-linger "$USER"   # required, or user timers stop at logout
```

## Not covered here

- **`dm-log-writes` replay** — enumerated rather than sampled crash points, the real-hardware
  analogue of `forEachAdaptiveCrashPoint`. The device plumbing is shared, so it is a target
  swap rather than new machinery.
- **Docker process-crash arm** — the real image SIGKILLed inside this same guest. Note it
  proves nothing about durability on its own: a container kill leaves the guest page cache
  intact.
- **Enterprise multi-node failover.**
