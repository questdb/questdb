# Adaptive Replica Durability Skip (D4 · seam S5)

**Date:** 2026-07-13
**Branch (OSS):** `nw_adaptive_commit` · **Branch (Ent):** `nw_adaptive_commit_ent`
**Status:** design approved, pre-plan
**Depends on:** OSS adaptive D3 (apply-side durable epoch in `ApplyWal2TableJob.maybeAdvanceDurableEpoch` / `advance`); Enterprise replication role model (`RoleState` → `PrimaryRoleState` | `ReplicaRoleState`), `WalDownloader`.
**Relates to:** [adaptive-commit-mode design §17 seam S5](2026-06-25-adaptive-commit-mode-design.md); mirror-image of [multi-tier durable-ack](2026-07-13-adaptive-multi-tier-durable-ack.md) (that seam made ack-side *reporting* role-aware; this makes apply-side *durability* role-aware).

## 1. Problem

Under `CommitMode.ADAPTIVE`, WAL-apply is lazy (columns written but not forced durable) and the materialized state is made power-loss-safe by a periodic **durable epoch**. That epoch is fired from exactly one place:

```java
// ApplyWal2TableJob.maybeAdvanceDurableEpoch  (OSS core) — called per apply batch
if (writer.getEffectiveCommitMode() != CommitMode.ADAPTIVE) return;
if (intervalMs < 0) return;                       // operator opt-out
if (cadence not elapsed) return;                  // per-table interval gate
advance(tableToken, writer, tracker, nowMs);      // fsyncMaterializedState + _snapshot marker
                                                  //   + durable _txn.epoch/_cv.epoch + scoreboard pin
```

`ApplyWal2TableJob` is **not role-aware**. A table's commit mode lives in its `_meta`, which replication copies from the primary — so an ADAPTIVE table on a **replica** fires the full durable epoch on every apply batch. On a replica this is pure waste:

- A replica's applied columns are a **rebuildable cache** of object-store truth. Its crash recovery is *re-download + re-apply* from the last-applied position via `WalDownloader` (replication tracks that position independently of the adaptive epoch). It never rolls forward from a local epoch anchor.
- So the per-batch `fsyncMaterializedState` is wasted I/O on an already-I/O-heavy apply path, and the durable `_txn.epoch`/`_cv.epoch` copies write a **local durability anchor the replica's recovery model never consults** — dead state at best, a recovery-model inconsistency at worst.

The epoch governs **durability**, not **visibility**: lazy apply still writes the columns, so reads are unaffected. Skipping the epoch on a replica drops only a durability guarantee the replica does not need.

## 2. Goals / Non-goals

**Goals**
- Make the adaptive apply-side durable epoch **role-aware**: fire on a primary / single-node, skip on a replica.
- Do it with the **same pluggable-seam idiom** the adaptive design already established for `DurableAckRegistry` (a role-installed component on `CairoEngine`), so the two role-aware durability behaviors are symmetric and discoverable.
- **Fail-safe polarity:** the default and any unknown/transitional state is *always-on* (do the fsync). Skip is narrowly scoped to a live replica. A node is never under-durable by accident — only a node that is definitively a replica skips.
- OSS single-node behavior is **byte-for-byte unchanged**.

**Non-goals (explicitly deferred)**
- Per-table skip granularity. A replica applies *all* tables from object-store truth, so the decision is whole-node; no per-table variation exists. (The seam can gain a `TableToken` arg later at zero cost if a real need appears.)
- A config knob to keep local epochs on a replica for faster restart. The approved recovery model is "skip entirely, re-fetch on restart" (§4). Not configurable.
- Skipping anything on the **ingest** path. The `WalWriter` ADAPTIVE fsync barrier is ingest-only; a replica never ingests, so there is nothing to gate there.
- `WalDownloader` / segment-write fsyncs (Rust transfer layer). Those are replication correctness, not adaptive local durability, and are out of scope.

## 3. Recovery model (the invariant this rests on)

A replica that skips all adaptive epochs must still recover correctly. It does, because replica recovery does not use the adaptive epoch:

1. Replication tracks the replica's applied/downloaded position durably in its own bookkeeping (the `WalDownloader` target), separate from `_txn.epoch`/`_cv.epoch`.
2. On restart, the replica resumes downloading from that position and re-applies. Any lazily-applied, non-durable columns lost to a crash are simply re-produced by re-apply.
3. Object store is the source of truth; the replica's local materialized state is a cache. There is nothing a local epoch anchor would add that re-fetch does not already provide.

This invariant is the headline correctness test (§6): a replica that fired **zero** epochs recovers identical data after a restart.

## 4. Design

### 4.1 OSS — a pluggable `LocalDurabilityPolicy` on the engine

A minimal functional seam, mirroring `CairoEngine`'s `volatile DurableAckRegistry` field + `get/setDurableAckRegistry`:

```java
// io.questdb.cairo.wal.LocalDurabilityPolicy
public interface LocalDurabilityPolicy {
    /**
     * Whether this node forces materialized WAL-apply state locally durable under ADAPTIVE.
     * True on a primary / single-node (local disk holds not-yet-uploaded truth). False on a
     * replica, whose applied columns are a rebuildable cache of object-store truth
     * (recovery = re-download + re-apply), making the per-batch durable epoch redundant.
     */
    boolean isLocalDurabilityEnabled();

    /** OSS default and primary / single-node behavior: always fire the epoch. */
    LocalDurabilityPolicy ALWAYS_ON = () -> true;

    /** Installed by Enterprise while a node is a replica: skip the epoch. */
    LocalDurabilityPolicy REPLICA_SKIP = () -> false;
}
```

- `CairoEngine` gains `private volatile @NotNull LocalDurabilityPolicy localDurabilityPolicy;`, defaulted to `LocalDurabilityPolicy.ALWAYS_ON` in the constructor, with `@NotNull getLocalDurabilityPolicy()` / `setLocalDurabilityPolicy(@NotNull ...)` — the exact shape of the existing `durableAckRegistry` accessors (`CairoEngine:279,1007,2101`). `volatile` because role transitions run on a different thread than the apply workers that read it.

### 4.2 OSS — one gate in the epoch decision

`ApplyWal2TableJob` already holds `engine` (`:96,117`). Add one early return in `maybeAdvanceDurableEpoch`, alongside the existing gates:

```java
if (writer.getEffectiveCommitMode() != CommitMode.ADAPTIVE) return;
if (!engine.getLocalDurabilityPolicy().isLocalDurabilityEnabled()) return;   // <-- S5: replica skips
if (intervalMs < 0) return;
...
```

Placed after the per-table ADAPTIVE check (cheapest, most-selective gate stays first) and before the interval/tracker work. One volatile read per batch on the hot apply path — negligible.

### 4.3 Enterprise — install the policy at role transitions

Symmetric with how `PrimaryRoleState` installs `DurableUploadRegistry` in its constructor (`PrimaryRoleState:122-134`) and restores on `close()` (`:216`). Both role states already hold `EntCairoEngine engine` (which extends `CairoEngine`, inheriting the new seam):

- **`ReplicaRoleState` constructor** → `engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP)`. Set in the ctor (not `openLoops`) so the skip is active before the apply workers can run an epoch on this node, matching the "install registry in ctor" rationale already documented on `PrimaryRoleState`.
- **`ReplicaRoleState.close()`** → `engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON)`. Restores the fail-safe default when the replica tenure ends (demote / shutdown), mirroring `PrimaryRoleState.close()` swapping its registry back.
- **`PrimaryRoleState` constructor** → `engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON)` (defensive re-assert). A primary must be always-on; asserting it on entry makes the switch cascade (primary↔replica failover) robust regardless of the previous state's teardown — each role asserts the policy it needs on entry.

**Net invariant:** `REPLICA_SKIP` is installed **iff** a `ReplicaRoleState` is live; every other state (single-node OSS, primary, transitional) is `ALWAYS_ON`. The only way to be under-durable is to be a definitively-live replica — the fail-safe direction.

## 5. Affected components

**OSS core (`questdb/core`)**
- `cairo/wal/LocalDurabilityPolicy.java` — **new** functional interface + `ALWAYS_ON` / `REPLICA_SKIP` constants.
- `cairo/CairoEngine.java` — add the `volatile` field (default `ALWAYS_ON`), getter, setter; construct default.
- `cairo/wal/ApplyWal2TableJob.java` — one gate in `maybeAdvanceDurableEpoch`.

**Enterprise (`questdb-ent`)**
- `lifecycle/ReplicaRoleState.java` — install `REPLICA_SKIP` in ctor; restore `ALWAYS_ON` in `close()`.
- `lifecycle/PrimaryRoleState.java` — assert `ALWAYS_ON` in ctor.

## 6. Testing (TDD — write the failing test first)

**OSS core**
- `ApplyWal2TableJob` epoch gate: with `setLocalDurabilityPolicy(REPLICA_SKIP)`, an ADAPTIVE table applies rows but fires **zero** durable epochs (assert via the epoch observable — `SeqTxnTracker.getLastEpochTs()` stays 0 / no `_snapshot` marker / durable-epoch seqTxn does not advance); with `ALWAYS_ON` (default), it fires as today.
- Engine default: a freshly constructed `CairoEngine` returns `ALWAYS_ON` (regression guard — single-node byte-for-byte unchanged).
- Visibility unaffected: under `REPLICA_SKIP`, applied rows are still readable (skip touches durability, not visibility).

**Enterprise**
- Entering `ReplicaRoleState` installs `REPLICA_SKIP` (`engine.getLocalDurabilityPolicy().isLocalDurabilityEnabled() == false`); `close()` restores `ALWAYS_ON`.
- Entering `PrimaryRoleState` yields `ALWAYS_ON`.
- Switch cascade: primary→replica→primary leaves `ALWAYS_ON` after landing on primary; primary→replica leaves `REPLICA_SKIP`.
- **Headline correctness:** a replica applying an ADAPTIVE table with epochs skipped, restarted mid-stream, recovers identical data by re-download + re-apply (proves §3 — the epoch was genuinely redundant on a replica).

**Regression**
- Existing adaptive epoch suite stays green (single-node/primary path is `ALWAYS_ON`). Existing `RoleState` / replication suites stay green.

## 7. Backward compatibility

- OSS default `ALWAYS_ON` = today's behavior exactly; no single-node change.
- No `_meta` / on-disk / protocol change. No new config knob.
- Enterprise: a primary is unchanged (always-on); only replicas change, and only to *stop doing redundant work*. No durability guarantee is weakened for any node whose local disk is a source of truth.

## 8. Cross-repo coordination

Two-branch change like the rest of adaptive: OSS core carries the interface + engine seam + gate; Enterprise carries the two role-state installs. Land OSS first (or together); the Ent submodule bump follows. Both stay `9.4.4-SNAPSHOT`-coupled and compile green.

## 9. Open questions / risks

- **Reliance on replication's own applied-position durability (§3).** The design assumes the `WalDownloader` target position is tracked durably, independent of `_txn.epoch`. This is inherent to how replicas already recover; the headline restart test (§6) validates it end-to-end. If that assumption were false, replicas would already be unsafe today — S5 does not introduce the dependency, it relies on it.
- **Transitional window during failover.** The policy is a volatile read per batch, so an in-flight role switch is observed on the next batch. Fail-safe polarity means the worst transient case is a replica doing one extra always-on epoch (harmless) — never a replica skipping when it should not, nor a primary skipping.
- **Seam vs. boolean.** Chosen a functional interface (Approach A) over a raw engine boolean for symmetry with `DurableAckRegistry` and clean test doubles. The two named constants keep it as light as a boolean while reading as a policy at the install sites.

## 10. Post-review extension (S5.1) — materialized-view durability + lifecycle hardening

The whole-branch review found the apply-side durable epoch is **not the only** ADAPTIVE durability force, and a boot-time lifecycle gap. This section extends S5 to close both (user-approved after review). The core S5 epoch-skip (§1–§9) is unchanged; this adds two complementary surfaces.

### 10.1 Materialized-view / view-definition durability (was "the only place" — corrected)

Materialized-view refresh-state and view/mat-view-definition writes go through `BlockFileWriter`, whose `commitMode` is fixed at construction from the **raw global** `configuration.getCommitMode()` and which treats any mode ≠ `NOSYNC` as sync-now — bypassing `LocalDurabilityPolicy`. So a replica under `commit_mode=adaptive` still fsyncs these. They are the same rebuildable-cache class as the epoch (reconstructed by re-download + re-apply from object-store truth), so a replica should skip them too.

- **Mechanism:** a shared helper `LocalDurabilityPolicy.resolveCommitMode(int commitMode, LocalDurabilityPolicy policy)` → returns `NOSYNC` iff `commitMode == ADAPTIVE && !policy.isLocalDurabilityEnabled()`, else `commitMode` unchanged. Gating on `ADAPTIVE` specifically preserves an operator's explicit `SYNC`/`ASYNC` choice — mirroring the epoch gate's own `getEffectiveCommitMode() != ADAPTIVE` precondition. `BlockFileWriter.commitMode` becomes non-`final` with a `setCommitMode(int)` (the field is already read fresh in `commit()`; the writer is single-thread-owned, so no concurrency concern).
- **In-scope sites (apply-path, high-value, OSS-testable):**
  - `ApplyWal2TableJob.updateMatViewRefreshState` (`_mv.s`) — per mat-view refresh apply (frequent). Covers `MAT_VIEW_DATA` + `MAT_VIEW_INVALIDATE`. Has `engine` + `config`.
  - `ApplyWal2TableJob` `VIEW_DEFINITION` apply (`_view`).
  - `TableWriter.updateMatViewDefinition` (`_mv`) — `ALTER MATERIALIZED VIEW … SET REFRESH …` via WAL apply. Has `engine` + `configuration`.
- **Correctness:** each writes state derived purely from the already-durable WAL txn/event being applied, so a lost non-synced write is re-derived by re-apply. The `DatabaseCheckpointAgent` `BlockFileWriter` (`CHECKPOINT CREATE`) is **explicitly excluded** — a checkpoint is an operator-requested backup artifact, not a rebuildable cache; its durability must never be role-downgraded. `SettingsStore` / `SqlCompilerImpl` sites are not replica-reachable and are left alone.
- **Deferred (documented follow-up, not in S5.1):** the `SequencerMetadata.create` sites (`VIEW_DEFINITION` / `MAT_VIEW_DEFINITION` at create/rebase) are replica-reachable **only** via `rebaseWalTableInto` (`REBASE WAL INTO`), which OSS's `CairoEngine.assertRebaseRole` rejects for `replicaVariant` — permitted only by the Enterprise engine subclass. That path is rare (one-time-per-table), threads a new parameter through 4 methods (`SequencerMetadata.create` → `TableSequencerImpl.createSequencerFiles` ×2 → `WalUtils.cloneTableDirForRebase` → `CairoEngine.rebaseWalTable0`), and **cannot be exercised end-to-end from an OSS test**. Deferred as an Enterprise-suite follow-up to keep S5.1 to the value-dense, verifiable apply-path sites the review's option named.

### 10.2 Boot-time role-state cleanup (lifecycle hardening)

S5 made `ReplicaRoleState`'s ctor install `REPLICA_SKIP` — engine wiring reset only by `close()`. `EntServerMain.switchRole` already guards this (try/catch → `close()` → null → rethrow, `:1555-1570`; its comment already names the "ctor-installed wiring reset only by close()" reason). But the two **other** `openLoops()` sites do not: `ReplicationEnvelope.start()`'s catch-up branch (`:1316-1325`) and `onDependencyState()` (`:1274-1280`). A failure there could leave `REPLICA_SKIP` on a node that is not a live replica. (The common synchronous boot path is already safe — the orchestrator aborts boot and the engine never serves — but the async post-start path is not provably safe.)

- **Fix:** wrap `startBound()`/`openLoops()` at both sites in the same `try { … } catch (Throwable t) { close(); currentState = null; throw t; }` shape `switchRole` uses (with `addSuppressed` on a close-time throw). Fail-safe: on any replica-start failure the node is left `ALWAYS_ON`, never silently under-durable.
- **Test:** model on `RoleStateOpenLoopsFaultInjectionTest` (it already injects `openLoops` faults); assert that after an injected `openLoops` failure on a replica the engine policy is `ALWAYS_ON` (not leaked `REPLICA_SKIP`).

### 10.3 Task breakdown (S5.1)

- **Task 4** (OSS): `resolveCommitMode` helper + `BlockFileWriter.setCommitMode` + `BlockFileWriter` mechanism unit test.
- **Task 5** (OSS): wire the three apply-path sites through the helper + msync-interception tests (`_mv.s`/`_mv`/`_view`, skip-vs-sync) modeled on `AdaptiveWalDurabilityTest`'s facade + `AdaptiveReplicaEpochSkipTest` recipe.
- **Task 6** (Ent): harden the two `openLoops()` sites + fault-injection test. Submodule bump to the Task-5 OSS head.
