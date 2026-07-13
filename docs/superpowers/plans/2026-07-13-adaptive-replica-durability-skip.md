# Adaptive Replica Durability Skip (D4 · seam S5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the ADAPTIVE apply-side durable epoch role-aware so a replica skips the redundant per-batch `fsyncMaterializedState` + durable-epoch write (its applied state is a rebuildable cache of object-store truth), while a primary / single-node keeps firing it.

**Architecture:** A minimal pluggable `LocalDurabilityPolicy` on `CairoEngine` (OSS-default `ALWAYS_ON`), consulted by one new early-return gate in `ApplyWal2TableJob.maybeAdvanceDurableEpoch`. Enterprise installs `REPLICA_SKIP` while a node is a live replica and restores `ALWAYS_ON` when that tenure ends — the exact install idiom the codebase already uses for `DurableAckRegistry`. Fail-safe polarity: skip is active **iff** a `ReplicaRoleState` is live; every other state is always-on.

**Tech Stack:** Java 25 (QuestDB core + questdb-ent), Maven 3.8.7, JUnit 4.

## Global Constraints

- JDK 25 at `/usr/lib/jvm/java-25-openjdk-amd64`; Maven 3.8.7. Set `JAVA_HOME` on every build/test command.
- Two-branch change: OSS `nw_adaptive_commit` (worktree `~/claude/wt/oss/adaptive-commit`) carries the interface + engine seam + gate; Enterprise `nw_adaptive_commit_ent` (worktree `~/claude/wt/ent/adaptive`, superproject; OSS core is the `questdb` git submodule) carries the two role-state installs + a submodule pointer bump. Both stay `9.4.4-SNAPSHOT`-coupled and compile green.
- **Fail-safe polarity (binding):** the OSS default and every non-replica state is `ALWAYS_ON` (fire the epoch). Only a definitively-live replica is `REPLICA_SKIP`. A node must never be under-durable by accident.
- **OSS single-node behavior is byte-for-byte unchanged** — the default policy is `ALWAYS_ON`, so a single-node instance fires epochs exactly as today.
- No `_meta` / on-disk / protocol change. No new config knob (the approved recovery model is "replica skips entirely, re-fetch on restart", not configurable).
- **Test style:** use the fluent `assertSql(expected, sql)` / `assertQuery(...)` helpers, never raw `printSql` + `TestUtils.assertEquals`.
- Every commit message ends with the `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer.

## File Structure

**OSS core (`questdb/core`, worktree `~/claude/wt/oss/adaptive-commit`)**
- `core/src/main/java/io/questdb/cairo/wal/LocalDurabilityPolicy.java` — **new.** Functional interface + `ALWAYS_ON` / `REPLICA_SKIP` constants. One responsibility: express whether this node forces adaptive local durability.
- `core/src/main/java/io/questdb/cairo/CairoEngine.java` — **modify.** Add the `volatile` field (default `ALWAYS_ON`), getter, setter, import — mirroring the sibling `durableAckRegistry` seam.
- `core/src/main/java/io/questdb/cairo/wal/ApplyWal2TableJob.java` — **modify.** One early-return gate in `maybeAdvanceDurableEpoch`.
- `core/src/test/java/io/questdb/test/cairo/wal/LocalDurabilityPolicyTest.java` — **new.** Unit test for the seam.
- `core/src/test/java/io/questdb/test/cairo/wal/AdaptiveReplicaEpochSkipTest.java` — **new.** Behavioral test for the gate.

**Enterprise (`questdb-ent`, worktree `~/claude/wt/ent/adaptive`)**
- `questdb-ent/src/main/java/com/questdb/lifecycle/ReplicaRoleState.java` — **modify.** Install `REPLICA_SKIP` in the (all-args) ctor; restore `ALWAYS_ON` at the top of `close()`.
- `questdb-ent/src/main/java/com/questdb/lifecycle/PrimaryRoleState.java` — **modify.** Assert `ALWAYS_ON` in the ctor (right after the `setDurableAckRegistry` install).
- `questdb-ent/src/test/java/com/questdb/lifecycle/RoleStateOpenLoopsFaultInjectionTest.java` — **modify.** Add role-wiring + switch-cascade tests (this file already has every helper: `AbstractEntCairoTest`, `getEngine()`, `MinimalEntServerConfiguration`, `NO_OP_UPLOAD_LISTENER`, `NO_OP_DOWNLOAD_LISTENER`, both role-state constructions).
- The `questdb` submodule gitlink (in the superproject) — **bump** to the OSS Task 2 head.

---

## Task 1: `LocalDurabilityPolicy` interface + `CairoEngine` seam (OSS)

**Files:**
- Create: `core/src/main/java/io/questdb/cairo/wal/LocalDurabilityPolicy.java`
- Modify: `core/src/main/java/io/questdb/cairo/CairoEngine.java` (field near the `durableAckRegistry` field ~`:279`; default is inline; getter near `:1007`; setter near `:2101`; import near the other `io.questdb.cairo.wal.*` imports ~`:83`)
- Test: `core/src/test/java/io/questdb/test/cairo/wal/LocalDurabilityPolicyTest.java`

**Interfaces:**
- Consumes: nothing (foundational).
- Produces:
  - `interface io.questdb.cairo.wal.LocalDurabilityPolicy { boolean isLocalDurabilityEnabled(); LocalDurabilityPolicy ALWAYS_ON; LocalDurabilityPolicy REPLICA_SKIP; }` — `ALWAYS_ON.isLocalDurabilityEnabled()==true`, `REPLICA_SKIP.isLocalDurabilityEnabled()==false`.
  - `@NotNull LocalDurabilityPolicy CairoEngine.getLocalDurabilityPolicy()`
  - `void CairoEngine.setLocalDurabilityPolicy(@NotNull LocalDurabilityPolicy)`
  - Engine default (fresh instance) is `LocalDurabilityPolicy.ALWAYS_ON`.

- [ ] **Step 1: Write the failing test**

Create `core/src/test/java/io/questdb/test/cairo/wal/LocalDurabilityPolicyTest.java`:

```java
/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo.wal;

import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * S5: the LocalDurabilityPolicy seam that lets Enterprise make the adaptive apply-side durable
 * epoch role-aware (skip on a replica). This tests only the seam; the gate behavior is in
 * AdaptiveReplicaEpochSkipTest and the role wiring is in the Enterprise suite.
 */
public class LocalDurabilityPolicyTest extends AbstractCairoTest {

    @Test
    public void testConstantsHaveExpectedPolarity() {
        Assert.assertTrue("ALWAYS_ON must enable local durability",
                LocalDurabilityPolicy.ALWAYS_ON.isLocalDurabilityEnabled());
        Assert.assertFalse("REPLICA_SKIP must disable local durability",
                LocalDurabilityPolicy.REPLICA_SKIP.isLocalDurabilityEnabled());
    }

    @Test
    public void testEngineDefaultIsAlwaysOn() {
        // Fail-safe default: a fresh engine (single-node / OSS) forces local durability.
        Assert.assertSame(LocalDurabilityPolicy.ALWAYS_ON, engine.getLocalDurabilityPolicy());
        Assert.assertTrue(engine.getLocalDurabilityPolicy().isLocalDurabilityEnabled());
    }

    @Test
    public void testEngineSetGetRoundTrips() {
        try {
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            Assert.assertSame(LocalDurabilityPolicy.REPLICA_SKIP, engine.getLocalDurabilityPolicy());
            Assert.assertFalse(engine.getLocalDurabilityPolicy().isLocalDurabilityEnabled());
        } finally {
            // engine is a static shared across the suite — restore the fail-safe default so this
            // test cannot leak REPLICA_SKIP into a sibling test.
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd ~/claude/wt/oss/adaptive-commit && JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
  mvn -pl core -am test -Dtest=LocalDurabilityPolicyTest -Dsurefire.failIfNoSpecifiedTests=false
```
Expected: **compilation failure** — `cannot find symbol: class LocalDurabilityPolicy` and `cannot find symbol: method getLocalDurabilityPolicy()` / `setLocalDurabilityPolicy(...)`. (A compile failure is the RED state for a brand-new type.)

- [ ] **Step 3: Create the interface**

Create `core/src/main/java/io/questdb/cairo/wal/LocalDurabilityPolicy.java`:

```java
/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.wal;

/**
 * Governs whether this node forces materialized WAL-apply state locally durable under
 * {@link io.questdb.cairo.CommitMode#ADAPTIVE}. Installed on {@link io.questdb.cairo.CairoEngine}
 * and consulted once per apply batch in
 * {@code ApplyWal2TableJob.maybeAdvanceDurableEpoch}.
 *
 * <p>Fail-safe polarity: the OSS default is {@link #ALWAYS_ON}. Only a definitively-live Enterprise
 * replica installs {@link #REPLICA_SKIP}; every other state (single-node, primary, transitional) is
 * always-on, so a node is never accidentally under-durable.
 */
@FunctionalInterface
public interface LocalDurabilityPolicy {

    /**
     * OSS default and primary / single-node behavior: always fire the adaptive durable epoch. The
     * local disk holds not-yet-uploaded truth, so it must be forced durable.
     */
    LocalDurabilityPolicy ALWAYS_ON = () -> true;

    /**
     * Installed by Enterprise while a node is a replica: skip the adaptive durable epoch. A
     * replica's applied columns are a rebuildable cache of object-store truth (recovery =
     * re-download + re-apply via the WalDownloader), so the per-batch {@code fsyncMaterializedState}
     * + durable epoch copies are redundant I/O.
     */
    LocalDurabilityPolicy REPLICA_SKIP = () -> false;

    /**
     * @return true iff this node should fire the adaptive apply-side durable epoch.
     */
    boolean isLocalDurabilityEnabled();
}
```

- [ ] **Step 4: Add the engine seam**

In `core/src/main/java/io/questdb/cairo/CairoEngine.java`:

(a) Add the import next to the existing `io.questdb.cairo.wal` imports (near line 83–85, alphabetical among them):

```java
import io.questdb.cairo.wal.LocalDurabilityPolicy;
```

(b) Add the field immediately after the existing `durableAckRegistry` field (the line `private volatile @NotNull DurableAckRegistry durableAckRegistry;`, ~`:279`):

```java
    // Governs the adaptive apply-side durable epoch (ApplyWal2TableJob.maybeAdvanceDurableEpoch).
    // OSS default ALWAYS_ON; Enterprise installs REPLICA_SKIP while a node is a live replica and
    // restores ALWAYS_ON when that tenure ends. volatile: swapped by EntCairoEngine role
    // transitions on the lifecycle thread, read by apply worker threads. Fail-safe: the default and
    // any non-replica state is ALWAYS_ON. Matches the sibling volatile durableAckRegistry.
    private volatile @NotNull LocalDurabilityPolicy localDurabilityPolicy = LocalDurabilityPolicy.ALWAYS_ON;
```

(c) Add the getter next to `getDurableAckRegistry()` (~`:1007`):

```java
    public @NotNull LocalDurabilityPolicy getLocalDurabilityPolicy() {
        return localDurabilityPolicy;
    }
```

(d) Add the setter next to `setDurableAckRegistry(...)` (~`:2101`):

```java
    public void setLocalDurabilityPolicy(@NotNull LocalDurabilityPolicy localDurabilityPolicy) {
        this.localDurabilityPolicy = localDurabilityPolicy;
    }
```

No constructor change is needed — the field is initialized inline to the `ALWAYS_ON` constant (unlike `durableAckRegistry`, which needs `this`).

- [ ] **Step 5: Run test to verify it passes**

Run:
```bash
cd ~/claude/wt/oss/adaptive-commit && JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
  mvn -pl core -am test -Dtest=LocalDurabilityPolicyTest -Dsurefire.failIfNoSpecifiedTests=false
```
Expected: `Tests run: 3, Failures: 0, Errors: 0, Skipped: 0` — BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
cd ~/claude/wt/oss/adaptive-commit && git add \
  core/src/main/java/io/questdb/cairo/wal/LocalDurabilityPolicy.java \
  core/src/main/java/io/questdb/cairo/CairoEngine.java \
  core/src/test/java/io/questdb/test/cairo/wal/LocalDurabilityPolicyTest.java && \
git -c commit.gpgsign=false commit -m "feat(adaptive): LocalDurabilityPolicy seam on CairoEngine (S5)

Pluggable per-node policy governing the adaptive apply-side durable epoch.
OSS default ALWAYS_ON (single-node/primary force local durability); Enterprise
will install REPLICA_SKIP while a node is a live replica. Mirrors the sibling
volatile durableAckRegistry seam. No behavior change yet — the gate lands next.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: role-aware gate in `ApplyWal2TableJob.maybeAdvanceDurableEpoch` (OSS)

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/ApplyWal2TableJob.java` (inside `maybeAdvanceDurableEpoch`, ~`:591`, right after the `getEffectiveCommitMode() != ADAPTIVE` early return and before `final long intervalMs = ...`)
- Test: `core/src/test/java/io/questdb/test/cairo/wal/AdaptiveReplicaEpochSkipTest.java`

**Interfaces:**
- Consumes: `CairoEngine.getLocalDurabilityPolicy()` and `LocalDurabilityPolicy.isLocalDurabilityEnabled()` (Task 1). `ApplyWal2TableJob` already holds `this.engine` (`:96,117`).
- Produces: the observable behavior — under `REPLICA_SKIP`, `maybeAdvanceDurableEpoch` returns before firing, so `SeqTxnTracker.getDurableEpochSeqTxn()` and `getLastEpochTs()` stay `0`; under `ALWAYS_ON` (default) they advance as today.

- [ ] **Step 1: Write the failing test**

Create `core/src/test/java/io/questdb/test/cairo/wal/AdaptiveReplicaEpochSkipTest.java`:

```java
/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * S5: the adaptive apply-side durable epoch is skipped when the engine's LocalDurabilityPolicy is
 * REPLICA_SKIP (the behavior Enterprise installs on a replica), and fires under the default
 * ALWAYS_ON. The epoch interval is set to 0 so the epoch is eligible on every apply batch — thus
 * the ONLY thing that suppresses it under REPLICA_SKIP is the new policy gate, not the cadence.
 */
public class AdaptiveReplicaEpochSkipTest extends AbstractCairoTest {

    @Test
    public void testReplicaSkipFiresNoEpochButDataStillReadable() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0); // epoch eligible every batch
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            try {
                execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
                drainWalQueue();

                final TableToken tt = engine.verifyTableName("x");
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                Assert.assertEquals("REPLICA_SKIP must not advance the durable epoch frontier",
                        0L, tracker.getDurableEpochSeqTxn());
                Assert.assertEquals("REPLICA_SKIP must fire no epoch (lastEpochTs stays 0)",
                        0L, tracker.getLastEpochTs());

                // Visibility is unaffected — the epoch governs durability, not apply. Lazy apply
                // still writes the columns, so the row is readable.
                assertSql("count\n1\n", "select count() from x");
            } finally {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
        });
    }

    @Test
    public void testAlwaysOnFiresEpoch() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            // default policy = ALWAYS_ON (no setLocalDurabilityPolicy call)
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
            Assert.assertTrue("ALWAYS_ON (default) must advance the durable epoch frontier",
                    tracker.getDurableEpochSeqTxn() > 0L);
        });
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd ~/claude/wt/oss/adaptive-commit && JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
  mvn -pl core -am test -Dtest=AdaptiveReplicaEpochSkipTest -Dsurefire.failIfNoSpecifiedTests=false
```
Expected: `testReplicaSkipFiresNoEpochButDataStillReadable` **FAILS** — without the gate the epoch still fires under REPLICA_SKIP, so `getDurableEpochSeqTxn()` is `> 0` and the assertion `expected 0 but was <n>` fails. `testAlwaysOnFiresEpoch` already PASSES (default behavior).

- [ ] **Step 3: Add the gate**

In `core/src/main/java/io/questdb/cairo/wal/ApplyWal2TableJob.java`, inside `maybeAdvanceDurableEpoch`, insert the policy gate immediately after the existing effective-commit-mode early return and before the `intervalMs` fetch. The method's opening changes from:

```java
    private void maybeAdvanceDurableEpoch(TableToken tableToken, TableWriter writer) {
        // Per-table EFFECTIVE mode (Deferred 1): the epoch lifecycle is driven by THIS table's mode, so a
        // WITH commit_mode='adaptive' table fires epochs even under a NOSYNC instance default, while a
        // sibling NOSYNC table never does (fastest path).
        if (writer.getEffectiveCommitMode() != CommitMode.ADAPTIVE) {
            return;
        }
        final long intervalMs = config.getAdaptiveEpochIntervalMs();
```

to:

```java
    private void maybeAdvanceDurableEpoch(TableToken tableToken, TableWriter writer) {
        // Per-table EFFECTIVE mode (Deferred 1): the epoch lifecycle is driven by THIS table's mode, so a
        // WITH commit_mode='adaptive' table fires epochs even under a NOSYNC instance default, while a
        // sibling NOSYNC table never does (fastest path).
        if (writer.getEffectiveCommitMode() != CommitMode.ADAPTIVE) {
            return;
        }
        // S5: role-aware skip. On a replica the adaptive durable epoch is redundant — the applied
        // columns are a rebuildable cache of object-store truth (recovery = re-download + re-apply
        // via the WalDownloader), so skip the per-batch fsyncMaterializedState + durable epoch
        // copies. Fail-safe: the OSS default is ALWAYS_ON; only a live Enterprise replica installs
        // REPLICA_SKIP. One volatile read on the hot apply path.
        if (!engine.getLocalDurabilityPolicy().isLocalDurabilityEnabled()) {
            return;
        }
        final long intervalMs = config.getAdaptiveEpochIntervalMs();
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd ~/claude/wt/oss/adaptive-commit && JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
  mvn -pl core -am test -Dtest=AdaptiveReplicaEpochSkipTest -Dsurefire.failIfNoSpecifiedTests=false
```
Expected: `Tests run: 2, Failures: 0, Errors: 0, Skipped: 0` — BUILD SUCCESS.

- [ ] **Step 5: Run the existing adaptive-epoch regression to confirm the default path is unchanged**

Run:
```bash
cd ~/claude/wt/oss/adaptive-commit && JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
  mvn -pl core -am test -Dtest=AdaptiveWalDurabilityTest,AdaptiveGroupCommitTest,LocalDurableAckRegistryTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```
Expected: all green — the default `ALWAYS_ON` policy leaves the single-node/primary epoch path byte-for-byte as before.

- [ ] **Step 6: Commit**

```bash
cd ~/claude/wt/oss/adaptive-commit && git add \
  core/src/main/java/io/questdb/cairo/wal/ApplyWal2TableJob.java \
  core/src/test/java/io/questdb/test/cairo/wal/AdaptiveReplicaEpochSkipTest.java && \
git -c commit.gpgsign=false commit -m "feat(adaptive): skip apply-side durable epoch under REPLICA_SKIP (S5)

One early-return gate in maybeAdvanceDurableEpoch: when the engine's
LocalDurabilityPolicy reports local durability disabled (a live replica), skip
the per-batch fsyncMaterializedState + durable epoch — the replica's applied
state is a rebuildable cache of object-store truth. Default ALWAYS_ON leaves the
single-node/primary path unchanged. Visibility (lazy apply) is untouched.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Cross-repo sync (controller step — run between Task 2 and Task 3)

Task 3 edits Enterprise code that references `LocalDurabilityPolicy`, which lives in the OSS `core` — compiled in the Enterprise build from the `questdb` git submodule. The submodule must therefore point at the Task 2 head before Task 3 can compile. The submodule already has a local remote `adaptive` → the OSS worktree, so this is a fast local fetch (no network).

```bash
# Capture the OSS Task 2 head
OSS_HEAD=$(cd ~/claude/wt/oss/adaptive-commit && git rev-parse HEAD)

# Point the Enterprise submodule at it
cd ~/claude/wt/ent/adaptive/questdb && git fetch adaptive && git checkout "$OSS_HEAD"

# Sanity: the new interface is now visible in the submodule checkout
test -f core/src/main/java/io/questdb/cairo/wal/LocalDurabilityPolicy.java \
  && echo "OK: LocalDurabilityPolicy present in ent submodule" \
  || echo "MISSING: sync did not land Task 1"
```

Expected: `OK: LocalDurabilityPolicy present in ent submodule`. (The gitlink bump is committed at the end of Task 3.)

---

## Task 3: install the policy at Enterprise role transitions (Ent)

**Files:**
- Modify: `questdb-ent/src/main/java/com/questdb/lifecycle/ReplicaRoleState.java` (all-args ctor body ~`:69-72`, after `this.injectedRegistry = injectedRegistry;`; and the first line of `close()` ~`:75`)
- Modify: `questdb-ent/src/main/java/com/questdb/lifecycle/PrimaryRoleState.java` (ctor, immediately after `engine.setDurableAckRegistry(durableUploadRegistry);` ~`:134`)
- Modify: `questdb-ent/src/test/java/com/questdb/lifecycle/RoleStateOpenLoopsFaultInjectionTest.java` (add test methods + the `LocalDurabilityPolicy` import)
- Bump: the `questdb` submodule gitlink in the superproject `~/claude/wt/ent/adaptive`

**Interfaces:**
- Consumes: `engine.getLocalDurabilityPolicy()` / `setLocalDurabilityPolicy(...)` and the constants `LocalDurabilityPolicy.ALWAYS_ON` / `REPLICA_SKIP` (Task 1; `EntCairoEngine extends CairoEngine`). Both role states already hold `this.engine` (an `EntCairoEngine`).
- Produces: the invariant — `engine.getLocalDurabilityPolicy() == REPLICA_SKIP` while a `ReplicaRoleState` is live; `== ALWAYS_ON` after its `close()` and for a `PrimaryRoleState`.

- [ ] **Step 1: Write the failing tests**

In `questdb-ent/src/test/java/com/questdb/lifecycle/RoleStateOpenLoopsFaultInjectionTest.java`, add the import (with the other `io.questdb.cairo.wal` imports):

```java
import io.questdb.cairo.wal.LocalDurabilityPolicy;
```

and add these four test methods to the class body (they reuse the file's existing `getEngine()`, `MinimalEntServerConfiguration`, `NO_OP_UPLOAD_LISTENER`, `NO_OP_DOWNLOAD_LISTENER`, and `LOG`):

```java
    @Test
    public void replicaRoleStateInstallsReplicaSkip() {
        final EntCairoEngine engine = getEngine();
        final EntServerConfiguration serverConfig = new MinimalEntServerConfiguration(engine.getConfiguration());

        // Precondition: engine default is the fail-safe ALWAYS_ON.
        Assert.assertSame(LocalDurabilityPolicy.ALWAYS_ON, engine.getLocalDurabilityPolicy());

        final ReplicaRoleState state = new ReplicaRoleState(
                engine, serverConfig, NO_OP_DOWNLOAD_LISTENER, LOG, null, null);
        try {
            Assert.assertSame(
                    "constructing a ReplicaRoleState must install REPLICA_SKIP so the adaptive "
                            + "apply-side durable epoch is skipped on this node",
                    LocalDurabilityPolicy.REPLICA_SKIP, engine.getLocalDurabilityPolicy());
            Assert.assertFalse(engine.getLocalDurabilityPolicy().isLocalDurabilityEnabled());
        } finally {
            state.close();
        }
    }

    @Test
    public void replicaRoleStateCloseRestoresAlwaysOn() {
        final EntCairoEngine engine = getEngine();
        final EntServerConfiguration serverConfig = new MinimalEntServerConfiguration(engine.getConfiguration());

        final ReplicaRoleState state = new ReplicaRoleState(
                engine, serverConfig, NO_OP_DOWNLOAD_LISTENER, LOG, null, null);
        Assert.assertSame(LocalDurabilityPolicy.REPLICA_SKIP, engine.getLocalDurabilityPolicy());

        state.close();

        Assert.assertSame(
                "ReplicaRoleState.close() must restore the fail-safe ALWAYS_ON so a node with no "
                        + "live replica tenure never silently skips the durable epoch",
                LocalDurabilityPolicy.ALWAYS_ON, engine.getLocalDurabilityPolicy());
    }

    @Test
    public void primaryRoleStateAssertsAlwaysOn() {
        final EntCairoEngine engine = getEngine();
        final EntServerConfiguration serverConfig = new MinimalEntServerConfiguration(engine.getConfiguration());

        // Even if a prior replica tenure left REPLICA_SKIP installed, entering PRIMARY must assert
        // ALWAYS_ON on entry (a primary's local disk holds not-yet-uploaded truth).
        engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);

        final PrimaryRoleState state = new PrimaryRoleState(
                engine, serverConfig, 0L, false, () -> 0, NO_OP_UPLOAD_LISTENER, reason -> {
        }, LOG, null, null);
        try {
            Assert.assertSame(
                    "constructing a PrimaryRoleState must assert ALWAYS_ON",
                    LocalDurabilityPolicy.ALWAYS_ON, engine.getLocalDurabilityPolicy());
            Assert.assertTrue(engine.getLocalDurabilityPolicy().isLocalDurabilityEnabled());
        } finally {
            state.close();
        }
    }

    @Test
    public void switchCascadePrimaryReplicaPrimaryLandsAlwaysOn() {
        final EntCairoEngine engine = getEngine();
        final EntServerConfiguration serverConfig = new MinimalEntServerConfiguration(engine.getConfiguration());

        // primary -> replica -> primary. After landing on primary, the policy is ALWAYS_ON.
        final PrimaryRoleState p1 = new PrimaryRoleState(
                engine, serverConfig, 0L, false, () -> 0, NO_OP_UPLOAD_LISTENER, reason -> {
        }, LOG, null, null);
        Assert.assertSame(LocalDurabilityPolicy.ALWAYS_ON, engine.getLocalDurabilityPolicy());
        p1.close();

        final ReplicaRoleState r = new ReplicaRoleState(
                engine, serverConfig, NO_OP_DOWNLOAD_LISTENER, LOG, null, null);
        Assert.assertSame("replica tenure installs REPLICA_SKIP",
                LocalDurabilityPolicy.REPLICA_SKIP, engine.getLocalDurabilityPolicy());
        r.close();
        Assert.assertSame("replica close restores ALWAYS_ON",
                LocalDurabilityPolicy.ALWAYS_ON, engine.getLocalDurabilityPolicy());

        final PrimaryRoleState p2 = new PrimaryRoleState(
                engine, serverConfig, 0L, false, () -> 0, NO_OP_UPLOAD_LISTENER, reason -> {
        }, LOG, null, null);
        try {
            Assert.assertSame("after the full cascade, primary is ALWAYS_ON",
                    LocalDurabilityPolicy.ALWAYS_ON, engine.getLocalDurabilityPolicy());
        } finally {
            p2.close();
        }
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run (from the superproject root so the `questdb` core submodule is on the reactor):
```bash
cd ~/claude/wt/ent/adaptive && JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
  mvn -pl questdb-ent -am test -Dtest=RoleStateOpenLoopsFaultInjectionTest -Dsurefire.failIfNoSpecifiedTests=false
```
Expected: the four new methods **FAIL** — `replicaRoleStateInstallsReplicaSkip` asserts `REPLICA_SKIP` but the unmodified `ReplicaRoleState` never installs it, so the engine still reports `ALWAYS_ON` (`expected …REPLICA_SKIP but was …ALWAYS_ON`). (`primaryRoleStateAssertsAlwaysOn` also fails: the ctor doesn't yet re-assert, so the test's pre-set `REPLICA_SKIP` survives.) Existing methods in the file stay green.

- [ ] **Step 3: Install `REPLICA_SKIP` in `ReplicaRoleState`**

In `questdb-ent/src/main/java/com/questdb/lifecycle/ReplicaRoleState.java`:

(a) Add the import with the other `io.questdb.cairo.wal` imports (near the top, e.g. next to `import com.questdb.cairo.wal.transfer.WalDownloader;`):

```java
import io.questdb.cairo.wal.LocalDurabilityPolicy;
```

(b) In the all-args (`@TestOnly`) constructor — the one every ctor path delegates to — after the last field assignment `this.injectedRegistry = injectedRegistry;`, install the skip policy:

```java
        this.injectedRegistry = injectedRegistry;
        // S5: while this node is a replica, skip the adaptive apply-side durable epoch. Installed in
        // the ctor (before openLoops) so the skip is active before apply workers can fire an epoch —
        // mirrors PrimaryRoleState installing its DurableUploadRegistry in the ctor. Restored to the
        // fail-safe ALWAYS_ON in close().
        engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
```

(c) As the **first** statement of `close()` (so it runs even if later teardown throws, and is idempotent across repeated close), restore the default:

```java
    @Override
    public void close() {
        // S5: this replica tenure is ending — restore the fail-safe ALWAYS_ON so a node with no live
        // replica never silently skips the durable epoch. Idempotent; safe on partial construction.
        engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
        WalDownloader downloader = this.walDownloader;
        // ... existing close body unchanged ...
```

(Insert only the comment + the single `setLocalDurabilityPolicy` line at the very top of the existing `close()`; leave the rest of the method as-is.)

- [ ] **Step 4: Assert `ALWAYS_ON` in `PrimaryRoleState`**

In `questdb-ent/src/main/java/com/questdb/lifecycle/PrimaryRoleState.java`:

(a) Add the import with the other `io.questdb.cairo.wal` imports (next to `import io.questdb.cairo.wal.DefaultDurableAckRegistry;` if present, else among the `io.questdb.cairo.wal` group):

```java
import io.questdb.cairo.wal.LocalDurabilityPolicy;
```

(b) In the constructor, immediately after `engine.setDurableAckRegistry(durableUploadRegistry);` (~`:134`), assert the primary policy:

```java
        engine.setDurableAckRegistry(durableUploadRegistry);
        // S5: a primary's local disk holds not-yet-uploaded truth, so it must force local durability.
        // Assert ALWAYS_ON on entry (defensive against a prior replica tenure that left REPLICA_SKIP)
        // so the switch cascade is robust regardless of the previous state's teardown.
        engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
```

- [ ] **Step 5: Run tests to verify they pass**

Run:
```bash
cd ~/claude/wt/ent/adaptive && JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
  mvn -pl questdb-ent -am test -Dtest=RoleStateOpenLoopsFaultInjectionTest -Dsurefire.failIfNoSpecifiedTests=false
```
Expected: all methods green (the four new + the file's existing fault-injection tests) — BUILD SUCCESS.

- [ ] **Step 6: Commit the Ent changes and the submodule bump**

The submodule `questdb` was moved to the OSS Task 2 head in the cross-repo sync step; record that gitlink alongside the Ent Java changes.

```bash
cd ~/claude/wt/ent/adaptive && git add \
  questdb \
  questdb-ent/src/main/java/com/questdb/lifecycle/ReplicaRoleState.java \
  questdb-ent/src/main/java/com/questdb/lifecycle/PrimaryRoleState.java \
  questdb-ent/src/test/java/com/questdb/lifecycle/RoleStateOpenLoopsFaultInjectionTest.java && \
git -c commit.gpgsign=false commit -m "feat(adaptive): role-install LocalDurabilityPolicy — replicas skip the durable epoch (S5)

ReplicaRoleState installs REPLICA_SKIP in its ctor and restores ALWAYS_ON in
close(); PrimaryRoleState asserts ALWAYS_ON on entry. Net invariant: REPLICA_SKIP
is active iff a ReplicaRoleState is live; every other state is ALWAYS_ON
(fail-safe). Bumps the questdb submodule to the OSS S5 core (LocalDurabilityPolicy
+ the apply-side gate).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Scoping note — spec §6 "headline restart" correctness test

The spec (§6) frames a "replica applies with epochs skipped, restarts, recovers identical data by re-download + re-apply" test. During planning this resolves to **two mechanism tests already in this plan**, not a new object-store restart harness:

- S5's entire behavioral diff is the one gate keyed on `engine.getLocalDurabilityPolicy()`. Task 2 proves both halves of it: under `REPLICA_SKIP` no epoch fires **and the data is still readable** (durability dropped, visibility intact); under `ALWAYS_ON` it fires.
- The "recovers by re-fetch on restart" property is **pre-existing replication behavior** that S5 does not touch — a replica already recovers by resuming its `WalDownloader` position from object-store truth, independent of `_txn.epoch` (spec §3, and §9 notes "S5 does not introduce the dependency, it relies on it"). It is covered by the existing Enterprise replication recovery suites (e.g. `DurableWatermarkAcrossDemoteTest`, `GracefulFailoverLossDirectionTest`), which remain green.

Building a from-scratch replica-crash-plus-object-store restart test would exercise replication code S5 leaves unchanged, for no added coverage of S5's diff. This note records the decision so the plan/spec reconcile explicitly rather than silently dropping the bullet.

---

## Self-Review

**1. Spec coverage.**
- §4.1 registry-style seam on the engine → Task 1 (field/getter/setter, default `ALWAYS_ON`, `volatile`).
- §4.1 the `LocalDurabilityPolicy` interface + `ALWAYS_ON`/`REPLICA_SKIP` constants → Task 1.
- §4.2 the one gate in `maybeAdvanceDurableEpoch` → Task 2.
- §4.3 `ReplicaRoleState` install/restore + `PrimaryRoleState` assert → Task 3.
- §3 recovery invariant / §6 headline test → Task 2 mechanism tests + Scoping note (pre-existing replication recovery).
- §5 affected components → File Structure map (3 OSS files incl. 2 tests; 2 Ent main + 1 Ent test + submodule bump).
- §7 backward compat (default `ALWAYS_ON`, single-node unchanged) → Task 2 Step 5 regression run.
- §8 cross-repo coordination / submodule bump → Cross-repo sync step + Task 3 Step 6.
No spec requirement is left without a task or an explicit scoping decision.

**2. Placeholder scan.** No `TBD`/`TODO`/"handle edge cases"/"similar to Task N". Every code step shows complete code; every run step gives an exact command + expected output.

**3. Type consistency.** `LocalDurabilityPolicy` (SAM `isLocalDurabilityEnabled()`, constants `ALWAYS_ON`/`REPLICA_SKIP`) is defined identically in Task 1 and consumed with the same names/signatures in Tasks 2 and 3. `CairoEngine.getLocalDurabilityPolicy()` / `setLocalDurabilityPolicy(@NotNull …)` match across the getter/setter definition (Task 1) and every call site (Tasks 2, 3, tests). Epoch observables `SeqTxnTracker.getDurableEpochSeqTxn()` / `getLastEpochTs()` match the real accessors. Enterprise ctor signatures (`new ReplicaRoleState(engine, serverConfig, listener, LOG, null, null)`, `new PrimaryRoleState(engine, serverConfig, 0L, false, () -> 0, NO_OP_UPLOAD_LISTENER, reason -> {}, LOG, null, null)`) match the existing usages in `RoleStateOpenLoopsFaultInjectionTest`.

Plan is internally consistent and fully covers the spec.
