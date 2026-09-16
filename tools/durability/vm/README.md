# VM crash-integration harness

Repeatable, unattended-capable crash testing of adaptive commit on a **real kernel and a real
block layer**, inside one QEMU guest.

The JUnit tiers in `core/src/test/java/io/questdb/test/cairo/crash/` model power loss with
`CrashFaultFilesFacade`. That model is well gated, but it is a model. This harness is the
cross-check.

## The instrument

One workload run is recorded by `dm-log-writes`; every flush in that recording is a crash
point, reconstructed by replaying the log up to it. Replaying to flush N yields exactly the
device state a volatile write cache would have left at N: everything before it durable,
everything after it gone.

```bash
bash run-flush-sweep.sh adaptive 0        # zero-loss bar: every committed txn survives
bash run-flush-sweep.sh adaptive 50000    # RPO bar at W=50ms
bash run-flush-sweep.sh adaptive 2000000  # RPO bar at W=2s, widest at-risk window
```

Killing QEMU only stops the recording. Nothing about the kill decides what is durable — the
replay does, which is what lets this instrument observe the W>0 RPO gap at all. Adaptive defers
the `fdatasync` into the group-commit batch, so between batches its data is pushed out but
unflushed, and only flush-boundary replay can drop exactly that set.

**The device must be reset between replays.** `dm-log-writes` is a pass-through target: during
the recording every write also reaches `/dev/vdb`, so after the reboot the data device still
holds the final crashed state. Replaying to boundary N re-applies the writes up to N but cannot
revert the ones issued after it, so without a reset boundary N+1 inherits boundary N's state.

| knob | default | meaning |
|---|---|---|
| `QDB_REPLAY_RESET` | `blkdiscard` | reset the data device before every replay; `none` disables it |
| `QDB_VM_DATA_DISCARD` | `ignore` | `unmap` on the REPLAY boot only. The recording boot must not have it, or a discard issued by the workload becomes a DISCARD entry in the log and changes what was recorded |

**The trap, if you touch this.** Under QEMU's default `discard=ignore` the guest still
advertises discard support and `blkdiscard` still returns success in 8 ms having reverted
nothing. The drive option and the reset command therefore live in the same file
(`lib/qemu.sh`), and `replay_reset_assert()` verifies once per sweep that the device really
zeroes, sampling five offsets across it — one sample cannot tell a full discard from a partial
one.

The VM pins the kernel and the ext4 version, which is the reason it is a VM: a durability result
that moves because the agent pool was upgraded is a result nobody can act on. It also keeps the
harness runnable without root on a shared machine.

`cache=none` is mandatory for the recording boot. `cache=writeback` leaves unflushed guest
writes in the host page cache and `cache=directsync` makes every guest write durable
immediately; both produce false green.

## Coverage vs the Java crash suite

Every code path the modelled suite covers is also exercised here, on a real kernel — **17 of 17
dimensions**, each verified to actually execute.

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
| 9 | array column | `ArrayCrashConsistencyTest` | `profile=array` -- length varies per row (`id % 10`), CONTENTS verified |
| 10 | wide mixed-type table | `BatchedFlushDurabilityCrashTest` | `profile=wide` |
| 11 | O3 merge path | `AdaptiveO3CrashSweep`, `AdaptiveO3LazyGap` | `profile=o3` |
| 12 | structural DDL under load | `RandomizedAdaptiveCrashFuzzTest` | `QDB_DDL_EVERY_ROWS=N` |
| 13 | sustained lazy gap | `AdaptiveO3LazyGap`, W2/W3/W5 | `QDB_EPOCH_MS=-1` |
| 14 | multi-table | `AdaptiveMultiTableLazyGap` (W3) | `QDB_SIBLING_TABLE=true` |
| 15 | mat-view | `AdaptiveMatViewLazyGap` (W4) | `QDB_MAT_VIEW=true` -- found a defect, see below |
| 16 | commit-mode flip | `AdaptiveCommitModeFlipCrashTest` | `QDB_RECOVER_AS=nosync` (restart under a different GLOBAL mode) |
| 17 | REBASE WAL publish | `RebaseWalPublishDurabilityCrashTest` | `QDB_REBASE_AT_ROWS=N` -- hard-suspends, rebases, then IDLES so the swept boundaries land post-publish |

Every profile keeps columns `0..3` (`id, v, s, ts`) fixed, so the identity oracle is shared and
results are comparable across profiles.

**A dimension is covered when its check is observed to run, not when its flag exists.** Neither
the sweep log (`tail -1`, the verdict only) nor `verify.sh` (the verdict line only) can show
that a check executed. Run `CrashVerifier` directly and read its stdout:

```bash
java ... -Dsibling.table=true org.questdb.CrashVerifier /mnt/qdb/db \
  | grep -vE '^20[0-9]{2}-'      # strip engine logging
# expect: sibling t2 rows=N (primary=M)
```

Do the same for any dimension added later. A green sweep whose check never executed reads as
evidence and is not.

A uniform failure across every boundary is the signature of a setup fault rather than a product
defect; a real crash-state defect does not land identically every time.

### Dimension 15 (mat-view): an open finding

The base correctly discards at-risk txns permitted by the RPO window. The view keeps aggregates
derived from them and stays `valid`:

```
C=267  Wm=263   base recovered = 263000 rows (263 txns)
view aggregates 268000 rows   refresh_base_table_txn=268
view_status=valid   invalidation_reason=null
```

It is permanent: incremental refresh only moves forward from a watermark already above the
base, and the invalidation path for a base-txn regression is gated on a TRUNCATE barrier, which
a crash rollback is not. The control at `mode=sync` (W=0, Wm==C) shows no lead, so the condition
requires the RPO window. Likely fix: invalidate at startup when `lastRefreshBaseTxn` exceeds the
base's recovered seqTxn.

### Dimension 17 (REBASE WAL): scope

`CairoEngine.rebaseWalTable0` refuses unless `isHardSuspended()` and
`cairo.wal.apply.suspended.write.denied=true`, so the writer sets both, rebases once, then idles
instead of resuming ingestion — the sweep verifies the last recorded boundaries, and idling is
what puts them in the publish window.

The swept boundaries sit after the publish completes, so this covers "a crash following a rebase
leaves a coherent registry and table". Crashing inside the rename/fsync window is
`RebaseWalPublishDurabilityCrashTest`'s job at the op level. The rebase oracle drops the
`F >= Wm` bar deliberately — REBASE WAL discards pending WAL by design — and holds "rename is
not publish" instead: the name must resolve to a directory that exists and is coherent.

## What each arm proves — and what it does not

| Arm | Mechanism | Proves | Does **not** prove |
|---|---|---|---|
| **reference** | `CrashIngestWriter` embedded, `CrashVerifier` runs the production recovery triple | The engine's durability contract on real storage: every acked txn survives; loss confined to `(Wm, C]` and bounded by W | Anything about the shipped server binary, its entry point, or the wire |
| **qwp** | A real classpath-launched server plus the real WebSocket client; the SERVER tracks and recovers | The wire protocol's write path — frame decode, ingress buffering, server-side commit — and that the server alone honours the bar | That a client can close the server's RPO window; anything about packaging |
| **qwp-sf** | As `qwp`, plus the client requests the LOCAL durable-ack tier and holds un-acked rows in a store-and-forward buffer **on the crashed device** | That the client puts back what the server's RPO window legitimately dropped, measured as a delta between two verifications of the same boundary | Anything about packaging or the entry point — same shade-jar as `qwp` |
| **product** | The release tarball unpacked in the guest and started by the real `questdb.sh`, plus the same client and oracle `qwp-sf` uses | The **shipped artifact** writes, survives a cut, and **recovers**: same RPO bar as `qwp-sf`, on the artifact a user downloads, launched as a named JPMS module | That the enterprise distribution ships correctly (different artifact, and this arm refuses `QDB_EDITION=ent`); that the bundled web console ships |

The product arm is `qwp-sf` with one variable changed, and the difference is runtime
configuration rather than packaging:

| | every other arm | the shipped launcher |
|---|---|---|
| how | `java -cp benchmarks.jar io.questdb.ServerMain` | `java -p questdb.jar -m io.questdb/io.questdb.ServerMain` |
| module | unnamed, classpath | named module `io.questdb` |
| native access | `--enable-native-access=ALL-UNNAMED` | `--enable-native-access=io.questdb` |
| opens | `--add-opens=...=ALL-UNNAMED` | `--add-opens=...=io.questdb` |
| artifact | a JMH shade-jar | the assembled release artifact |

Reflection, native-access enforcement, resource loading and native-library extraction all follow
the module, so a regression in any of them is invisible to every other arm.

The arm asserts its own premise twice, and both checks are demonstrated able to fail: the
running JVM's command line must carry `-m io.questdb/io.questdb.ServerMain`, and `build()` must
report the commit hash baked into the tarball's manifest. Both pass their evidence into the
archived per-boundary output as `DETAIL PRODUCT_PREMISE moduleLaunched=yes dist=<ver>
commit=<sha>`, because a silent fallback to a classpath server writes byte-identical data and
nothing downstream could tell.

The shipped artifact also does the recovering: before the oracle opens the crashed database, the
product arm starts the shipped server on it and lets its recovery run, then reports
`DETAIL PRODUCT_RECOVERY shippedServerRecoveredRows=N`. Without that pass the arm would prove
only that the shipped server wrote the data. `QDB_PRODUCT_RECOVERY_PASS=false` opts out and says
so in the output.

**Getting the tarball.** The harness ships what the real assembly produced and never assembles
one itself:

```bash
JAVA_HOME=<a stock JDK> mvn -pl core -am package -P build-binaries -Dmaven.test.skip=true
```

A Nix/flox JDK fails the `jlink` step with `libmanagement_ext.so has been modified`: its native
libraries are patchelf-rewritten, so jlink refuses to link from it. That is the JDK, not the
product build.

## The guards

A harness that silently stops crashing is the worst outcome available; so is one whose oracle
silently stops checking.

| Guard | Test | Fails when |
|---|---|---|
| **Golden image** | `t01` | The image does not boot, or lacks what the sweep needs |
| **Boundary discrimination** | `t06` | Replaying to boundary N leaks writes issued after it |
| **Oracle control** | `t07` | Corrupt data still verifies clean, proving the oracle cannot fire |
| **Barrier control** | `t10` | A WAL table committing with NO durability barrier still verifies clean |
| **State reaper** | `t08` | The reaper deletes a live run, misses a directory prefix, or reports success having reclaimed nothing |
| **CI report** | `t09` | `junit.xml` is malformed, mis-counts, omits a schema-required element, or loses the run identity |
| **Builder parity** | `t11` | A setting reaches the verifier but not the workload, an invalid `QDB_WAL_TABLE` falls back to a default, or a defanged negative control still holds a live ack tier |
| **Verdict vocabulary** | `t12` | A verdict line classifies to the wrong token, a token is added without a test row, or a pass/instrument-fault predicate changes meaning |
| **Device reset** | every sweep, `replay_reset_assert()` | `blkdiscard` reports success and the device is not zeroed, including a partial zero |
| **Informative sample** | every sweep | The workload never reached its first commit, or every boundary verified as `NO_COMMIT` |

`t07` and `t10` are a pair and neither substitutes for the other: `t07` proves the **oracle** can
fail, `t10` proves a missing **barrier** is noticed. A green verdict is the product of a working
check and a product that is actually flushing, and either failing silently produces the same
reassuring output.

`t07` corrupts data — eight bytes of a committed column file, in place, after a real replay and
mount — and requires `SILENT_CORRUPTION` naming the exact row. It says nothing about a missing
barrier: a product that stops calling `fdatasync` fails differently, in that every byte that
arrives is correct and there are simply fewer of them than were acknowledged.

`t10` runs the same workload twice with one variable changed:

| arm | configuration | required outcome |
|---|---|---|
| A | WAL table, `commitMode=SYNC` — barriered | every boundary green |
| B | WAL table, `commitMode=NOSYNC` — no barrier | every boundary **red** |

It needs no production change and no test-only mutation: `WalWriter.syncIfRequired0` gates the
barrier on `commitMode != NOSYNC`, so NOSYNC *is* the mutation and it is a supported
configuration. It does need `QDB_WAL_TABLE`, because the table kind is otherwise implied by the
commit mode (`SYNC`/`NOSYNC` → bypass WAL, `adaptive` → WAL), which makes "WAL table, no
barrier" inexpressible. `t10` therefore asserts every verification reports `wal.table=true`
before counting its verdict: a NOSYNC run that fell back to the bypass-WAL path would go red for
`t07`'s reason and look like a passing control.

**A WAL table at SYNC or NOSYNC has no durable frontier at all.** `WalWriter` advances
`localDurableSeqTxn` only under ADAPTIVE, so `Wm` stays `-1` and the RPO bar cannot be drawn
from it. `CrashVerifier` grades both against the acknowledged frontier `C` instead and ignores
`W`. Without that branch the W>0 bar would find `Wm=-1`, skip its comparison and print `RPO_OK`
— a pass produced precisely because the durability under test is absent.

## Running it

```bash
bash check-host.sh          # prerequisites; names the first thing missing
bash build-image.sh         # once — builds the golden qcow2

# no VM, no root, seconds — run these first, they cost nothing
bash test/t08-state-reaper.sh        # the reaper must not eat the wrong thing
bash test/t09-junit-xml.sh           # the CI report must be valid and honest
bash test/t11-builder-parity.sh      # both JVMs must get the same settings
bash test/t12-verdict-vocabulary.sh  # a verdict line must mean what it says
bash test/t06-log-writes-replay.sh --self-test    # the cross-check logic, VM-free
bash test/t10-wal-barrier-control.sh --self-test  # the barrier judge, VM-free

# these boot a VM
bash test/t01-golden-image.sh
bash test/t06-log-writes-replay.sh        # boundaries must discriminate, or sweeps are fiction
bash test/t07-oracle-negative-control.sh  # the ORACLE must be able to fail
bash test/t10-wal-barrier-control.sh      # a missing BARRIER must be noticed

bash run-flush-sweep.sh adaptive 0 40     # the instrument: 40 enumerated boundaries

bash reap-state.sh --keep=3 --keep-days=14          # dry run; shows what it WOULD reclaim
bash reap-state.sh --keep=3 --keep-days=14 --apply  # note the = form; a space is rejected
```

Run the VM-free guards first. They take seconds and they cover the parts that fail quietly: on
an unattended agent the report and the reaper are what stand between a red night and a disk that
silently fills.

`t06` is the guard the sweep rests on, so do not trust a sweep result if `t06` is red. It runs
both reset regimes and cross-checks them, because a pass that uses the instrument's own
configuration cannot see a common-mode fault: if the reset silently did nothing, the sweep and
`t06` would be wrong in the same direction. With `QDB_REPLAY_RESET=none` it fails outright —
both files appear at the first boundary and the boundaries stop discriminating.

Run state lives under `/data/qdb-vmcrash` (override with `QDB_VMCRASH_STATE`).

`run-sf-replay.sh` is a separate driver for the cross-machine claim: the client runs on the
host, survives the cut, and replays from its store-and-forward buffer. It is not part of the
sweep and CI does not run it.

### Choosing sweep points

The replay/verify loop reuses one booted VM, so recording the workload, rebooting and rebuilding
the dm-log-writes stack are paid once per run rather than per point: ~3.7 s/point at 102 points
against ~60 s/point at 3. Per-point cost grows with table size because the oracle scans every
recovered row.

`QDB_SWEEP_MODE=stride` (default) spreads points across the whole recording; `tail` keeps the
last N. The floor skips the first 10%, whose boundaries predate the table and verify as
`NO_COMMIT`. On any failure the sweep densifies around it (`n-2, n-1, n+1`) so the report gives a
bracket rather than a point; disable with `QDB_SWEEP_DENSIFY=false`.

Suggested: 40 points for a quick check (default), 400 for a thorough run (~45 min).

## Machine-readable output

Every sweep writes `junit.xml` next to its per-boundary evidence in `$OUTDIR`, alongside the text
log rather than instead of it. The text log is the evidence trail; this is for the dashboard,
which otherwise sees a pass/fail exit code and a log blob.

| outcome | rendered as | meaning |
|---|---|---|
| `DURABLE`, `RPO_OK` | pass | the boundary was measured and the bar held |
| `NO_COMMIT` | `<skipped>` | the cut landed before anything was committed — a legitimate but uninformative sample |
| `DURABILITY_FAILURE`, `SILENT_CORRUPTION`, `MOUNT_FAILED`, `LOUD_FAILURE` | `<failure>` | the product failed |
| `NOT_EVALUATED`, `UNPARSEABLE` | `<error>` | the rig broke and nothing was measured |

The `<error>`/`<failure>` split decides who gets paged, so the token list lives in
`verdict_is_instrument_fault` (`lib/verdict.sh`) next to `verdict_is_pass`, never in
`lib/junit.sh`. `NO_COMMIT` is not a pass: counting it as one lets a run where most boundaries
measured nothing report as a wall of green. `MOUNT_FAILED` is a product finding — an ext4 that
will not mount after a power cut is the damage this instrument hunts.

**The report must satisfy the JUnit XSD that `PublishTestResults@2` names**, because a report the
publisher rejects does not fail the job: the run goes green and the dashboard shows nothing. The
schema models `testsuite` as a sequence of `properties`, `testcase*`, `system-out`, `system-err`
with none of the four optional, the root is a single `<testsuite>` (the `<testsuites>` aggregate
would require `package=` and `id=` on every child), and `timestamp=`'s pattern forbids a
timezone, so the trailing `Z` used elsewhere in this harness is stripped there. `t09` pins all of
it, and validates against the real schema when `xmllint` and `QDB_JUNIT_XSD` are available.

The run identity (arm, mode, window, profile, epoch, `nflush`, points, reset mode, harness
commit, product dist, degrade state) is emitted twice, as `<properties>` and again in
`<system-out>`, because Azure DevOps has limited support for `<properties>` and an identity the
dashboard cannot display does not answer "which build produced this?".

## Host safety

The host is never touched by a test. Everything privileged happens in the guest.

- **No `dmsetup`, no `losetup`, no container, no bound service port on the host.** The guest's
  data disk is raw `/dev/vdb` and the device-mapper stack sits over it inside the guest.
- **Never run `losetup -D` or `dmsetup remove_all`** in any teardown path. On a shared box a host
  loop device may well be a live database's filesystem. Teardown targets its own names only.
- No host sudo is required: `/dev/kvm` is used directly via an ACL grant.
- One VM at a time, 8 vCPU / 16 GB. Disks are deleted only on a clean pass; any failure keeps
  them for inspection.
- **Every script that boots a VM kills it on every exit path**, via `vm_kill_on_exit` or its own
  trap. A leaked QEMU holds its `qemu.pid`, and `reap-state.sh` refuses any directory whose pid
  is alive, so the leak makes its own run dir permanently unreapable and the disk reappears
  weeks later at `check-host.sh`'s free-space gate looking like an infrastructure outage.
- A trap cannot help against `SIGKILL`, and a graceful `SIGTERM` is deferred until the in-flight
  foreground command returns. An unattended runner needs its own orphan sweep
  (`pkill -f 'qemu.*qdb-vmcrash'`) after a cancelled run.
- `reap-state.sh` targets `$QDB_VMCRASH_STATE` by name, resolves a symlinked state dir before
  acting, archives the cheap evidence before deleting the disks, refuses a directory whose QEMU
  is alive, and exits non-zero when a delete fails.

## On a CI agent

The intended home is a nightly job sharing the existing single-agent fuzz pool: `check-host.sh`
as a hard gate, then the VM-free guards, then the VM guards, then the sweeps, then publish, and
only then `reap-state.sh --apply`. Order matters in both directions — a sweep whose controls did
not run that night is not evidence, and a reaper that runs before the artifacts are uploaded has
destroyed the thing the upload was for.

- **One VM at a time.** The pool must run one agent, or two builds boot two QEMUs against the
  same `$QDB_VMCRASH_STATE`.
- **Keep the state directory outside the workspace.** A workspace clean wipes it, and the golden
  image is ~2.7 GB to rebuild. Keep `qdb-vmcrash` in its path if the runner's orphan sweep
  matches on that name.
- **No `/dev/kvm`, no run.** `lib/qemu.sh` passes `-enable-kvm` unconditionally, so QEMU refuses
  to start rather than falling back to TCG, which would re-time every crash point. A missing
  `/dev/kvm` is an infrastructure answer and should be reported as an `<error>` against the
  agent, not as a durability `<failure>`.

## Not covered here

- **Docker process-crash arm.** A container kill leaves the guest page cache intact, so it
  proves nothing about durability on its own.
- **Enterprise multi-node failover.**
- **A root-on-agent backend without the VM.** Possible — the sweep never uses the machine dying
  as its mechanism — but it would unpin the kernel and the ext4 version, which is the whole
  point of the guest. If it is ever revisited, make the execution location a backend rather than
  a fork of the sweep, and run `t06` in that backend too: after `dmsetup remove` and replay the
  block device's buffer cache can serve stale pages to the next mount.

Check the dimension table against the Java suite whenever the branch merges. A dimension whose
Java counterpart has been deleted is not coverage, it is residue: the flag still parses, the
sweep still goes green, and nothing is tested.
