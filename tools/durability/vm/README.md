# VM crash-integration harness

Repeatable, unattended-capable crash testing of adaptive commit on a **real kernel and a
real block layer**, entirely inside one QEMU guest.

The JUnit tiers in `core/src/test/java/io/questdb/test/cairo/crash/` model power loss with
`CrashFaultFilesFacade`. That model is well gated, but it is a model. This harness is the
cross-check.

## Coverage vs the Java crash suite

The goal is that every code path the modelled suite covers is also exercised here, on a real
kernel. Current state -- **17 of 17 dimensions**, every one verified to actually execute (see
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
| 9 | array column | `ArrayCrashConsistencyTest` | `profile=array` -- length varies per row (`id % 10`), CONTENTS verified |
| 12 | structural DDL under load | `RandomizedAdaptiveCrashFuzzTest` | `QDB_DDL_EVERY_ROWS=N` |
| 14 | multi-table | `AdaptiveMultiTableLazyGap` (W3) | `QDB_SIBLING_TABLE=true` |
| 15 | mat-view | `AdaptiveMatViewLazyGap` (W4) | `QDB_MAT_VIEW=true` -- **found a real defect, see below** |
| 16 | commit-mode flip | `AdaptiveCommitModeFlipCrashTest` | `QDB_RECOVER_AS=nosync` (restart under a different GLOBAL mode) |
| 17 | REBASE WAL publish | `RebaseWalPublishDurabilityCrashTest` | `QDB_REBASE_AT_ROWS=N` -- hard-suspends, rebases, then IDLES so the swept boundaries land post-publish |

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

**Check the mechanism, not just the verdict.** (Historical example: `QDB_PER_TABLE_MODE` ran the INSTANCE on
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
sibling-table gap (the property never reached the verifier JVM), and a per-table-mode (since removed)
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
| **qwp** | A real classpath-launched server plus the real WebSocket client; the SERVER tracks and recovers | The wire protocol's write path — frame decode, ingress buffering, server-side commit — and that the server alone honours the bar | That a client can close the server's RPO window; anything about packaging or the entry point |
| **qwp-sf** | As `qwp`, plus the client requests the LOCAL durable-ack tier and holds un-acked rows in a store-and-forward buffer **on the crashed device** | That the client puts back what the server's RPO window legitimately dropped: measured as a delta between two verifications of the same boundary, not implied | Anything about packaging or the entry point — it runs the same shade-jar as `qwp` |
| **product** | The **release tarball** (`questdb-<ver>-no-jre-bin.tar.gz`) unpacked in the guest and started by the real `questdb.sh`, plus the same client and oracle `qwp-sf` uses | The **shipped artifact** writes, survives a cut, and **recovers**: same RPO bar as `qwp-sf`, but on the artifact a user actually downloads, launched as a **named JPMS module** | That the enterprise distribution ships correctly (it is a different artifact, and this arm refuses `QDB_EDITION=ent`); that the bundled web console ships (built without `-P build-web-console`) |

**The product arm is `qwp-sf` with one variable changed: the server binary and how it is
launched.** That is a runtime-configuration difference, not a packaging detail:

| | every other arm | the shipped launcher |
|---|---|---|
| how | `java -cp benchmarks.jar io.questdb.ServerMain` | `java -p questdb.jar -m io.questdb/io.questdb.ServerMain` |
| module | unnamed, classpath | **named module `io.questdb`** |
| native access | `--enable-native-access=ALL-UNNAMED` | `--enable-native-access=io.questdb` |
| opens | `--add-opens=...=ALL-UNNAMED` | `--add-opens=...=io.questdb` |
| artifact | a JMH shade-jar (`Main-Class: org.openjdk.jmh.Main`) | the assembled release artifact |

Reflection, native-access enforcement, resource loading and native-library extraction all
follow the module, so a regression in any of them is **invisible to every other arm**.

**The arm asserts its own premise, twice, and both checks are demonstrated able to fail:**

1. the running JVM's command line must carry `-m io.questdb/io.questdb.ServerMain`. Run
   against a classpath-launched server it refuses (`rc=64`).
2. `build()` must report the **commit hash baked into the tarball's manifest**. Run against a
   correctly module-launched server with a doctored manifest hash it refuses (`rc=64`).

Both pass their evidence into the archived per-boundary output as
`DETAIL PRODUCT_PREMISE moduleLaunched=yes dist=<ver> commit=<sha>`, so a green run can be
audited rather than trusted. A silent fallback to a classpath server would otherwise report
product coverage that never happened — both servers write byte-identical data, so nothing
downstream could tell.

**W>0 is supported and is the interesting case.** It was W=0-only while the client-side LOCAL
durable-ack frontier did not exist; that landed with the `qwp-sf` arm, and was re-verified
through the shipped launcher (`localAcks` and `trimAdvances` both advance, `Wm` tracks) before
this arm was enabled. `run-matrix.sh` still runs the product cells at W=0 only — but that is a
limit of the LIVE-CUT flow, which cannot create a W>0 gap at all, not a limit of the arm.

**The shipped artifact does the recovering.** Before the oracle opens the crashed database, the
product arm starts the shipped server on it and lets ITS recovery run, then waits for the WAL
to drain and reports `DETAIL PRODUCT_RECOVERY shippedServerRecoveredRows=N`. Without that pass
the arm would prove only that the shipped server WROTE the data: `CrashVerifier` opens the root
first and would have completed recovery itself. Costs one extra server start/stop per boundary;
`QDB_PRODUCT_RECOVERY_PASS=false` opts out and says so in the output.

**Getting the tarball.** The harness ships what the real assembly produced and never assembles
one itself:

```bash
JAVA_HOME=<a stock JDK> mvn -pl core -am package -P build-binaries -Dmaven.test.skip=true
```

A Nix/flox JDK fails the `jlink` step with `libmanagement_ext.so has been modified` — its native
libraries are patchelf-rewritten, so jlink refuses to link from it. That is the JDK, not the
product build, and not the `--compress=2` deprecation warning printed just above the error.

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

## The guards

A crash harness that silently stops crashing is the worst outcome available. So is one
whose oracle silently stops checking.

| Guard | Frequency | Fails when |
|---|---|---|
| **Preflight** | once per run (`run-matrix.sh`, `run-fuzz.sh`) | An fsync'd file is lost, or an un-flushed **device** write survives |
| **Liveness** | every iteration | The workload was not running at the moment of the cut |
| **Defanged-cut control** | `test/t04` | The preflight *passes* with `drop_writes` removed, proving it cannot fire |
| **Boundary discrimination** | `test/t06` | Replaying to boundary N leaks writes issued after it |
| **Oracle control** | `test/t07` | Corrupt data still verifies clean, proving the oracle cannot fire |
| **Barrier control** | `test/t10` | A WAL table committing with NO durability barrier still verifies clean |

A failing preflight **aborts the run**; a failed liveness check **fails that iteration**.
Neither is ever downgraded to a warning.

`t04`, `t07` and `t10` are a set of three, and none substitutes for another: `t04` proves
the **cut** can fail, `t07` proves the **oracle** can, `t10` proves a missing **barrier** is
noticed. A harness needs all three, because a green verdict is the product of a working cut
AND a working check AND a product that is actually flushing — and any one of them failing
silently produces the same reassuring output.

**Scope of `t07`, so a green is not over-read.** It corrupts DATA — eight bytes of a
committed column file, in place, after a real replay and mount — and requires
`SILENT_CORRUPTION` naming the exact row. It says nothing about a missing durability
BARRIER: a product that stops calling `fdatasync` fails in a completely different way —
every byte that arrives is correct, there are simply fewer of them than were acknowledged.
That is `t10`'s job.

**`t10`, the barrier control.** Runs the same workload twice with one variable changed:

| arm | configuration | required outcome |
|---|---|---|
| A | WAL table, `commitMode=SYNC` — barriered | every boundary green, and `count >= watermark` |
| B | WAL table, `commitMode=NOSYNC` — no barrier | every boundary **red** |

It needs no production change and no test-only mutation: `WalWriter.syncIfRequired0` gates
the barrier on `commitMode != NOSYNC`, so `NOSYNC` **is** the mutation, and it is a
supported product configuration — the control doubles as coverage. What it did need was
`-Dwal.table` (`QDB_WAL_TABLE`), because the table kind used to be implied by the commit
mode (`SYNC`/`NOSYNC` → bypass wal, `adaptive` → WAL), which made "WAL table, no barrier"
inexpressible.

The non-WAL half of the same experiment was measured first and discriminates completely:
`SYNC` 9/9 `DURABLE` with `count == watermark` exactly, `NOSYNC` 9/9 `SILENT_CORRUPTION`
with `count=0`.

`t10` checks every verification reports `wal.table=true` before counting its verdict. A
`NOSYNC` run that quietly fell back to the bypass-WAL path would go red for the reason
`t07` already covers and would look like a passing control.

**A WAL table at `SYNC` or `NOSYNC` has no durable frontier at all.** `WalWriter` advances
`localDurableSeqTxn` only under `ADAPTIVE`, so `Wm` stays `-1` and the RPO bar cannot be
drawn from it. `CrashVerifier` grades both against the acknowledged frontier `C` instead and
ignores `W`, which only `ADAPTIVE` reads. Without that branch the `W>0` bar would find
`Wm=-1`, skip its comparison and print `RPO_OK` — a pass produced precisely *because* the
durability under test is absent, which is the exact false negative this control exists to
rule out.

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
bash test/t04-preflight.sh    # both directions; the CUT must be able to fail
bash test/t05-reference-arm.sh
bash test/t06-log-writes-replay.sh   # boundaries must discriminate, or sweeps are fiction
bash test/t07-oracle-negative-control.sh  # the ORACLE must be able to fail

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

### A VM-free backend is possible, and is deliberately not built

Worth knowing before anyone proposes it as new work. The flush sweep never uses the machine
dying as its test mechanism — `kill -9` on QEMU only STOPS THE RECORDING, and every write is
discarded later, at replay time, by a Python script. On a dedicated agent with root it would
need only two block devices (files behind `losetup`), one `dmsetup create ... log-writes`, the
workload, and then replay/mount/verify in a loop.

That would remove the golden image and its 600 MB download, cloud-init, the shipped JDK, two
VM boots per run, **one SSH round trip per boundary**, and ~700 lines of QEMU/SSH machinery.

It is not built, for one reason that outweighs all of that: **the guest pins the kernel and the
ext4 version.** A durability result that moves because the agent pool was upgraded is a result
nobody can act on. The VM also keeps the harness runnable without root on a shared machine,
which is what the host-safety rules above exist for.

If it is ever revisited, make the execution location a backend (`vm` default, `local` for CI)
rather than a fork of the sweep, assert `dm-log-writes` in the agent kernel and never skip on
its absence, and note the trap: after `dmsetup remove` and replay, the block device's buffer
cache can serve STALE PAGES to the next mount — `blockdev --flushbufs` first, and run `test/t06`
in that backend too, since t06 is the guard that catches exactly this.

## Not covered here

- **`dm-log-writes` replay** — enumerated rather than sampled crash points, the real-hardware
  analogue of `forEachAdaptiveCrashPoint`. The device plumbing is shared, so it is a target
  swap rather than new machinery.
- **Docker process-crash arm** — the real image SIGKILLed inside this same guest. Note it
  proves nothing about durability on its own: a container kill leaves the guest page cache
  intact.
- **Enterprise multi-node failover.**

## Dimensions retired when the product changed

**Per-table commit mode (was dimension 16)** -- removed from the product by "Remove per-table
commit mode", which deleted `PerTableCommitModeTest` / `PerTableAdaptiveIsolationCrashTest`, the
`commit_mode` table param, and `resolveEffectiveCommitMode`. The harness knob went with it.

**Mid-run commit-mode flip (`QDB_FLIP_AT_ROWS`)** -- implemented as
`alter table t set param commit_mode='nosync'`, SQL that no longer exists. The surviving
`AdaptiveCommitModeFlipCrashTest` flips the GLOBAL mode, which the harness already covers via
`QDB_RECOVER_AS` (restart under a different mode).

A dimension whose Java counterpart has been deleted is not coverage, it is residue: the flag
still parses, the sweep still goes green, and nothing is tested. Check this list against the
Java suite whenever the branch merges.

### The payload oracle

`bitCheckRows` used to select only `id, v, s, ts`, so the array, varchar and wide profiles wrote
their columns and the oracle never read them back: a torn array or varchar aux vector -- exactly
what those dimensions exist to catch -- passed silently. Every extra column is a deterministic
function of `id` (array length `id % 10` holding `id + j`, varchar indexed `id % 4`, wide columns
arithmetic), so each is now checked per row and a mismatch is `SILENT_CORRUPTION`.

Proven by negative control: inverting the expected array value makes the oracle fail
(`SILENT_CORRUPTION payload id=1 profile=array`); restoring it passes on array, varchar and wide.
A payload check that never executes is indistinguishable from one that always passes.

### Choosing sweep points

The replay/verify loop reuses ONE booted VM, so the expensive parts -- recording the workload,
rebooting, rebuilding the dm-log-writes stack -- are paid once per RUN, not per point. Measured on
the bitmap profile:

| points | wall | per point | recording |
|---|---|---|---|
| 3 (the old tail default) | ~180s | ~60s | — |
| 102 | 375s | 3.7s | 5,170 boundaries |
| 403 | 2,620s | 6.5s | 12,523 boundaries |

Per-point cost grows with table size because the oracle scans every recovered row, but stays
cheap. A 3-point tail sweep spent ~95% of its wall clock on setup and then threw away the part
that was nearly free.

`QDB_SWEEP_MODE=stride` (default) spreads the points across the whole recording; `tail` keeps the
old behaviour. The floor skips the first 10% -- those boundaries predate the table and verify as
NO_COMMIT, spending a round trip to prove nothing.

On any failure the sweep DENSIFIES: it verifies `n-2, n-1, n+1` so the report gives a bracket
rather than a point. A strided sweep says "it breaks somewhere in this gap"; the useful question
is which boundary FIRST breaks, because that names the operation responsible. Disable with
`QDB_SWEEP_DENSIFY=false`.

Suggested: 40 for a quick check (default), 400 for a thorough run (~45 min).
