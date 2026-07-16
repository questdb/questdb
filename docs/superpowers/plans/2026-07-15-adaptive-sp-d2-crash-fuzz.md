# SP-D D2 — Randomized Adaptive Crash-Fuzz Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove adaptive commit mode is crash-safe (no silent corruption) across *randomized workload shapes* by driving the seeded WAL fuzzer through the D1 crash+recover sweep harness with a fingerprint-membership oracle.

**Architecture:** One new JUnit test class `RandomizedAdaptiveCrashFuzzTest extends AbstractAdaptiveCrashSweepTest` composing a `FuzzRunner` field. Per seed: build a WAL *reference twin* applied without crash, snapshotting a per-transaction state fingerprint history `fp[0..M]` in memory, then drop the twin; then run `forEachAdaptiveCrashPoint` over a second WAL table under adaptive commit, asserting after recovery at each crash point that the recovered state's fingerprint equals some `fp[P]` (a real committed snapshot). W=0 adds exact RPO bars (full-at-N + monotone staircase); W>0 adds a sampled NOSYNC-control bound.

**Tech Stack:** Java 25 (JDK25 at `/usr/lib/jvm/java-25-openjdk-amd64`), QuestDB core test harness (`AbstractCairoTest`), Maven surefire. Test-only change — no production code.

## Global Constraints

- OSS core only, worktree `~/claude/wt/oss/adaptive-commit`, branch `nw_adaptive_commit`. Test-only; touch **no** `core/src/main`.
- JDK25. Run tests via `export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64` then `mvn -q -pl core test -Dtest=RandomizedAdaptiveCrashFuzzTest -DfailIfNoTests=false`.
- House style: `AbstractCairoTest`, fluent `assertQuery`/`execute`/`printSql`; JUnit `Assert`. Match the surrounding crash-test style in `core/src/test/java/io/questdb/test/cairo/crash/`.
- **Determinism is mandatory:** `walWriterCount = 1`; initial table row count `0` (all rows come from deterministic fuzz txns, avoiding `FuzzRunner`'s nondeterministic `data_temp` seed); generate the fuzz transaction list **once per seed** and cache it. The sweep driver asserts determinism (`fired`) and fails loudly on any residual nondeterminism.
- **Composition rules (from the reuse analysis — violating these breaks the sweep):**
  - Install the crash facade **only** via the inherited `runWithCrashFacade(...)`; never call `assertMemoryLeak(fuzzer.getFileFacade(), ...)`.
  - Drive WAL commits via `fuzzer.applyToWal(txns, table, 1, applyRnd)` **+ the inherited no-arg `drainWalQueue()`**; **never** `fuzzer.applyWal(...)` (its internal `assertFalse(isSuspended)` throws the wrong exception type into the sweep).
  - Build the twin + its `fp[]` history **before** any crash is armed, cache `fp[]` in memory, then **drop** the twin table — `crash(dbRoot)` rolls back every file under dbRoot.
- **Op/schema scope:** full destructive op library (insert/O3/add-drop-rename-col/type-change/truncate/drop-partition/replace-range-dedup/set-TTL/parquet-native-convert/covering-index); `probabilityOfDropTable = 0`; `equalTsRowsProb = 0` (keeps the `order by ts` fingerprint canonical). Rich multi-type schema = `FuzzRunner`'s fixed schema (symbol/varchar/binary/long128/ipv4/indexed-symbol).
- **Budget:** CI-fast default = a few fixed seeds + one logged random seed, small counts; nightly override via `-Dfuzz.adaptive.crash.seeds=<n>` (and larger counts). Sweep cap = 200 (`DEFAULT_ADAPTIVE_CRASH_POINT_CAP`), truncation logged.
- Commit after each task with a `feat(test):`/`test(crash):` message ending with the standard `Co-Authored-By:` trailer.

---

## Reference APIs (confirmed at these paths — read them, do not re-derive)

- `AbstractAdaptiveCrashSweepTest` (`core/src/test/java/io/questdb/test/cairo/crash/AbstractAdaptiveCrashSweepTest.java`): `forEachAdaptiveCrashPoint(AdaptiveCrashWorkload, int cap)` (:164) → `SweepResult` (public `.n`, `.sweptPoints`, `.cap`, `.truncated`, `int[] recoveredByK()`); interface `AdaptiveCrashWorkload { TableToken[] setup(int iteration); void commit(); int oracle(int k,int n); default void teardown(); }` (:85); `boolean anyTableSuspended(TableToken...)` (:140); `DEFAULT_ADAPTIVE_CRASH_POINT_CAP = 200` (:78). Recovery + resets handled internally by `recoverAfterCrash`.
- `AbstractCrashConsistencyTest` (`.../crash/AbstractCrashConsistencyTest.java`): `crashFf` field; `runWithCrashFacade(TestUtils.LeakProneCode body)` (:21) — the facade install point; `markDurableBaseline()` (:27).
- `FuzzRunner` (`core/src/test/java/io/questdb/test/cairo/fuzz/FuzzRunner.java`): `setFuzzProbabilities(...)` (:623 smallest arity, 16 doubles: cancelRows, notSet, nullSet, rollback, colAdd, colRemove, colRename, colTypeChange, dataAdd, equalTsRows, partitionDrop, truncate, tableDrop, setTtl, replaceInsert, symbolAccessValidation); `setFuzzCounts(boolean isO3,int fuzzRowCount,int transactionCount,int strLen,int symbolStrLenMax,int symbolCountMax,int initialRowCount,int partitionCount)` (:575); `generateTransactions(String tableName, Rnd rnd)` (:526) → `ObjList<FuzzTransaction>`; `applyToWal(ObjList<FuzzTransaction>, String, int walWriterCount, Rnd)` (:364) — write+commit, no drain; `createInitialTableWal(String, int initialRowCount)` (:463) → `TableToken`; `generateRandom(Log)` (:467) / `generateRandom(Log,long,long)` (:474); `withDb(engine, sqlExecutionContext)` (:805); `after()` (:150) prints the seed pair.
- `FuzzTransaction` (`core/src/test/java/io/questdb/test/fuzz/FuzzTransaction.java`): `public ObjList<FuzzTransactionOperation> operationList`, `public boolean rollback`, `public boolean reopenTable`, `public LongList getNoCommitIntervals()`. `FuzzTransactionOperation.apply(Rnd, CairoEngine, TableWriterAPI, int, LongList)`.
- `printSql(CharSequence sql, MutableUtf16Sink sink)` (`AbstractCairoTest.java:911`); `io.questdb.std.str.StringSink` is the concrete sink.
- `engine.getTableSequencerAPI().getTxnTracker(TableToken)` (`TableSequencerAPI.java:256`) → `SeqTxnTracker`; getters: `getLocalDurableSeqTxn()` (:141), `getWriterTxn()` (:179, the applied/materialized seqTxn), `getSeqTxn()` (:170, log frontier).
- Config keys (`PropertyKey`): `CAIRO_COMMIT_MODE` ("adaptive"/"nosync"), `CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US` (W), `CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS` (0=every batch), `CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED`.

## File Structure

- **Create:** `core/src/test/java/io/questdb/test/cairo/crash/RandomizedAdaptiveCrashFuzzTest.java` — the whole increment: the test class, its inner `FuzzCrashWorkload implements AdaptiveCrashWorkload`, and the fingerprint/membership helpers (private methods). One file: the pieces are tightly coupled test code and share the `fuzzer`/`engine` fields.

Everything else is reuse. No production files change.

---

### Task 1: Fingerprint + membership primitive

**Files:**
- Create: `core/src/test/java/io/questdb/test/cairo/crash/RandomizedAdaptiveCrashFuzzTest.java`

**Interfaces:**
- Produces: `String fingerprint(String table)` (canonical ordered dump); `static int lastMatch(ObjList<String> history, CharSequence state)` (largest index whose fingerprint equals `state`, else -1). Consumed by Tasks 2–7.

- [ ] **Step 1: Write the failing test.** Create the class extending `AbstractAdaptiveCrashSweepTest`, with the two helpers and a unit test that builds a tiny WAL table, snapshots fingerprints across three inserts, and checks membership.

```java
package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.fuzz.FuzzRunner;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class RandomizedAdaptiveCrashFuzzTest extends AbstractAdaptiveCrashSweepTest {

    private final FuzzRunner fuzzer = new FuzzRunner();

    // Canonical committed-state fingerprint: full ordered dump to a String.
    private String fingerprint(String table) throws SqlException {
        StringSink fp = new StringSink();
        printSql("select * from " + table + " order by ts", fp);
        return fp.toString();
    }

    // Largest index whose recorded fingerprint equals `state`; -1 if none (conservative on coincident txns).
    private static int lastMatch(ObjList<String> history, CharSequence state) {
        for (int i = history.size() - 1; i >= 0; i--) {
            if (TestUtils.equals(history.getQuick(i), state)) {
                return i;
            }
        }
        return -1;
    }

    @Test
    public void testFingerprintMembershipPrimitive() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table fp (ts timestamp, v long) timestamp(ts) partition by day wal");
            ObjList<String> history = new ObjList<>();
            history.add(fingerprint("fp"));                          // fp[0] empty
            for (int i = 0; i < 3; i++) {
                execute("insert into fp values (" + (i * 1_000_000L) + ", " + i + ")");
                drainWalQueue();
                history.add(fingerprint("fp"));                      // fp[1..3]
            }
            // the current (full) state must match the last snapshot
            Assert.assertEquals(3, lastMatch(history, fingerprint("fp")));
            // an intermediate snapshot is found at its own index
            Assert.assertEquals(1, lastMatch(history, history.getQuick(1)));
            // a fabricated state matches nothing
            Assert.assertEquals(-1, lastMatch(history, "not a real dump"));
        });
    }
}
```

- [ ] **Step 2: Run to verify it fails (compile-first).** `mvn -q -pl core test -Dtest=RandomizedAdaptiveCrashFuzzTest#testFingerprintMembershipPrimitive -DfailIfNoTests=false`. Expected initially: it should PASS once the class compiles (the helpers are complete). If it fails, it is a real bug in the helpers — fix before moving on. (Confirm `TestUtils.equals(CharSequence, CharSequence)` exists — `TestUtils.java:538`; if the exact name differs, use `io.questdb.std.Chars.equals`.)

- [ ] **Step 3: Commit.**

```bash
git add core/src/test/java/io/questdb/test/cairo/crash/RandomizedAdaptiveCrashFuzzTest.java
git commit -m "test(crash): SP-D D2 fingerprint + membership primitive"
```

---

### Task 2: Deterministic WAL-twin fingerprint-history builder

**Files:**
- Modify: `.../crash/RandomizedAdaptiveCrashFuzzTest.java`

**Interfaces:**
- Consumes: `fingerprint`, `FuzzRunner`.
- Produces: `ObjList<String> buildTwinFingerprints(String twinName, ObjList<FuzzTransaction> txns, Rnd applyRnd)` — creates a WAL twin (0 initial rows), applies each txn with a drain, snapshots `fp[0..M]`, drops the twin, returns the cached history. Used by Tasks 3–7.

- [ ] **Step 1: Add the builder + a determinism test.** Mirror `FuzzRunner.applyNonWal`'s per-txn loop (`FuzzRunner.java:264-292`) but drive WAL commits via `applyToWal` + inherited `drainWalQueue()`. Add `@Before` glue so `fuzzer` is wired (mirrors `AbstractFuzzTest.setUp`).

```java
    @org.junit.Before
    public void setUpFuzzer() {
        fuzzer.withDb(engine, sqlExecutionContext);
        fuzzer.clearSeeds();
    }

    @org.junit.After
    public void tearDownFuzzer() {
        fuzzer.after();
    }

    // Default = full destructive op library; the machinery self-check (Task 3) flips this to run a
    // minimal insert/O3 profile. Field lives here; Task 3 only toggles it.
    private boolean fuzzOverrideMinimal = false;

    // Uses the 22-arg overload (FuzzRunner.java:757) — the ONLY one that enables partitionToParquet
    // (12th), partitionToNative (13th), setParquetEncoding (20th), and addCoveringIndex (22nd). The
    // 16-arg overload silently leaves those at 0, so parquet/covering-index would NOT be exercised.
    private void configureFuzz() {
        if (fuzzOverrideMinimal) {
            //   cancel notSet null  rollbk cAdd cRem cRen cTyp  data eqTs pDrop pPq  pNat trunc tDrop ttl  repl symV qry  pEnc tFmt cIdx
            fuzzer.setFuzzProbabilities(
                    0.05, 0.2, 0.05, 0.0,  0, 0, 0, 0,   0.6, 0.0, 0, 0,   0, 0, 0, 0,   0, 0, 0, 0,   0, 0);
        } else {
            fuzzer.setFuzzProbabilities(
                    0.05, 0.2, 0.05, 0.0,       // cancelRows, notSet, nullSet, rollback(=0: clean seqTxn map)
                    0.1, 0.05, 0.05, 0.05,      // colAdd, colRemove, colRename, colTypeChange
                    0.5, 0.0, 0.05, 0.03,       // dataAdd, equalTsRows(=0: canonical dump), partitionDrop, partitionToParquet
                    0.03, 0.05, 0.0, 0.05,      // partitionToNative, truncate, tableDrop(=0), setTtl
                    0.1, 0.0, 0.0, 0.02,        // replaceInsert(dedup), symbolAccessValidation, query, setParquetEncoding
                    0.0, 0.03);                 // setTableFormat(=0), addCoveringIndex
        }
        //                     isO3, fuzzRowCount, txns, strLen, symStrLen, symCount, initialRows=0, partitions
        fuzzer.setFuzzCounts(true, 200, 20, 4, 4, 4, 0, 3);
    }

    private ObjList<FuzzTransaction> generateTxns(Rnd rnd, String walTableName) throws Exception {
        configureFuzz();
        fuzzer.createInitialTableWal(walTableName, 0);   // 0 initial rows → deterministic (no nondeterministic data_temp seed)
        return fuzzer.generateTransactions(walTableName, rnd);
    }

    private ObjList<String> buildTwinFingerprints(String twinName, ObjList<FuzzTransaction> txns, Rnd applyRnd) throws Exception {
        fuzzer.createInitialTableWal(twinName, 0);
        ObjList<String> history = new ObjList<>();
        history.add(fingerprint(twinName));                          // fp[0] = empty
        final ObjList<FuzzTransaction> one = new ObjList<>();
        for (int i = 0, n = txns.size(); i < n; i++) {
            one.clear();
            one.add(txns.getQuick(i));
            fuzzer.applyToWal(one, twinName, 1, applyRnd);
            drainWalQueue();
            history.add(fingerprint(twinName));                      // fp[i+1] = state after txn i
        }
        execute("drop table " + twinName);                          // crash(dbRoot) must not see the twin
        return history;
    }

    @Test
    public void testTwinFingerprintsDeterministic() throws Exception {
        assertMemoryLeak(() -> {
            final long s0 = 42L, s1 = 99L;
            ObjList<String> h1 = runTwinOnce("wal_a", "twin_a", s0, s1);
            ObjList<String> h2 = runTwinOnce("wal_b", "twin_b", s0, s1);
            Assert.assertEquals("fp history length must be deterministic", h1.size(), h2.size());
            for (int i = 0; i < h1.size(); i++) {
                Assert.assertTrue("fp[" + i + "] must be identical across two runs of the same seed",
                        TestUtils.equals(h1.getQuick(i), h2.getQuick(i)));
            }
            Assert.assertTrue("fp history must be non-trivial", h1.size() > 3);
        });
    }

    private ObjList<String> runTwinOnce(String walName, String twinName, long s0, long s1) throws Exception {
        Rnd genRnd = new Rnd(s0, s1);
        ObjList<FuzzTransaction> txns = generateTxns(genRnd, walName);
        return buildTwinFingerprints(twinName, txns, new Rnd(s0, s1));
    }
```

- [ ] **Step 2: Run to verify.** `mvn -q -pl core test -Dtest=RandomizedAdaptiveCrashFuzzTest#testTwinFingerprintsDeterministic -DfailIfNoTests=false`. Expected: PASS. If the two histories differ, determinism is broken — most likely a non-fixed rnd or a nondeterministic op; do **not** paper over it (the sweep depends on this). If a specific op class is nondeterministic on apply, reduce its probability to 0 and note it.

- [ ] **Step 3: Commit.** `git commit -am "test(crash): SP-D D2 deterministic WAL-twin fingerprint builder"`

---

### Task 3: FuzzCrashWorkload + sweep wiring + W=0 self-check (machinery)

Validate the whole sweep machinery with a **minimal** op profile first (inserts + O3 only) so a failure here is a wiring bug, not a destructive-op surprise.

**Files:**
- Modify: `.../crash/RandomizedAdaptiveCrashFuzzTest.java`

**Interfaces:**
- Consumes: `buildTwinFingerprints`, `generateTxns`, `fingerprint`, `lastMatch`, `forEachAdaptiveCrashPoint`, `anyTableSuspended`.
- Produces: `FuzzCrashWorkload` (inner class); `SweepResult runSeedSweep(long s0, long s1, int windowUs)`; `void assertW0Bars(SweepResult r)`.

- [ ] **Step 1: Write the workload + self-check.** The workload regenerates the WAL table per `setup(k)` (deterministic), commits via `applyToWal`+`drainWalQueue`, and its `oracle(k,n)` asserts bar 1 (membership) + bar 2 (clean reopen), returning `P`.

```java
    private static final String WAL_TABLE = "cf_wal";
    private static final String TWIN_TABLE = "cf_twin";

    private final class FuzzCrashWorkload implements AdaptiveCrashWorkload {
        private final long s0, s1;
        private ObjList<FuzzTransaction> txns;
        private ObjList<String> fp;   // built lazily, once
        private TableToken walToken;

        FuzzCrashWorkload(long s0, long s1) { this.s0 = s0; this.s1 = s1; }

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            execute("drop table if exists " + WAL_TABLE);
            txns = generateTxns(new Rnd(s0, s1), WAL_TABLE);          // recreates cf_wal (0 rows), same txns
            walToken = engine.verifyTableName(WAL_TABLE);
            if (fp == null) {
                fp = buildTwinFingerprints(TWIN_TABLE, txns, new Rnd(s0, s1)); // once; drops the twin
            }
            return new TableToken[]{walToken};
        }

        @Override
        public void commit() throws Exception {
            fuzzer.applyToWal(txns, WAL_TABLE, 1, new Rnd(s0, s1));
            drainWalQueue();
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            Assert.assertFalse("table left suspended after recovery at k=" + k, anyTableSuspended(walToken)); // bar 2
            String recovered = fingerprint(WAL_TABLE);
            int p = lastMatch(fp, recovered);                        // bar 1 (membership)
            Assert.assertTrue("recovered state at k=" + k + " matches NO committed snapshot (corruption?)\n"
                    + recovered, p >= 0);
            return p;
        }
    }

    private SweepResult runSeedSweep(long s0, long s1, int windowUs) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, windowUs);
        return forEachAdaptiveCrashPoint(new FuzzCrashWorkload(s0, s1));
    }

    // W=0 bar 3: full-at-N + monotone staircase over the returned per-k P array.
    private void assertW0Bars(SweepResult r) {
        int[] p = r.recoveredByK();
        int prev = -1;
        for (int k = 1; k <= r.sweptPoints; k++) {
            Assert.assertTrue("staircase non-monotone at k=" + k + " (" + p[k] + " < " + prev + ")", p[k] >= prev);
            prev = p[k];
        }
        if (!r.truncated) {
            Assert.assertEquals("k=N must recover the full committed history", r.n, p[r.sweptPoints]);
        }
    }

    @Test
    public void testSelfCheckW0MinimalProfile() throws Exception {
        runWithCrashFacade(() -> {
            fuzzOverrideMinimal = true;               // inserts + O3 only, to validate the sweep machinery
            try {
                assertW0Bars(runSeedSweep(1234L, 5678L, 0));
            } finally {
                fuzzOverrideMinimal = false;
            }
        });
    }
```

(`configureFuzz()` and the `fuzzOverrideMinimal` field are already defined in Task 2 — this task only toggles the flag; it does **not** redefine them.)

- [ ] **Step 2: Run to verify GREEN.** `mvn -q -pl core test -Dtest=RandomizedAdaptiveCrashFuzzTest#testSelfCheckW0MinimalProfile -DfailIfNoTests=false`. Expected: PASS — the sweep runs, every crash point's recovered fingerprint is a member of `fp[]`, the staircase is monotone, and k=N is full. If membership fails at some k, first confirm it is not a fingerprint-canonicalization artifact (same-ts ordering) — with the minimal profile and `equalTsRowsProb=0` it should not be; a genuine non-member recovered state is a **real adaptive bug** → stop and file it (this is the point of Prove-it).

- [ ] **Step 3: Commit.** `git commit -am "test(crash): SP-D D2 fuzz-crash workload + W=0 self-check (minimal profile)"`

---

### Task 4: Full destructive op library at W=0

**Files:**
- Modify: `.../crash/RandomizedAdaptiveCrashFuzzTest.java`

**Interfaces:** Consumes everything from Task 3. Produces nothing new — flips the profile to full.

- [ ] **Step 1: Add the full-library W=0 sweep test** (a few fixed seeds).

```java
    private static final long[] FIXED_SEEDS0 = {1234L, 22L, 8080L};
    private static final long[] FIXED_SEEDS1 = {5678L, 33L, 9090L};

    @Test
    public void testFullLibraryW0() throws Exception {
        runWithCrashFacade(() -> {
            for (int s = 0; s < FIXED_SEEDS0.length; s++) {
                SweepResult r = runSeedSweep(FIXED_SEEDS0[s], FIXED_SEEDS1[s], 0);
                assertW0Bars(r);
            }
        });
    }
```

- [ ] **Step 2: Run to verify.** `mvn -q -pl core test -Dtest=RandomizedAdaptiveCrashFuzzTest#testFullLibraryW0 -DfailIfNoTests=false`. Expected: PASS for all three seeds. A failure is one of: (a) a fingerprint-canonicalization artifact (a truncate/drop-partition producing a state that `order by ts` renders ambiguously) — verify by dumping the mismatch; if so, strengthen the dump's ORDER BY with a deterministic tiebreaker column present across schema versions, or lower the offending op's probability and note it; (b) a **real adaptive durability bug** → stop, capture the seed (`s0,s1`), file a GA-blocker. Do not weaken a bar to get green.

- [ ] **Step 3: Commit.** `git commit -am "test(crash): SP-D D2 full destructive op library at W=0"`

---

### Task 5: Necessity negative control (bar 5)

**Files:**
- Modify: `.../crash/RandomizedAdaptiveCrashFuzzTest.java`

**Interfaces:** Consumes `FuzzCrashWorkload`, `forEachAdaptiveCrashPoint`. Produces `int[] sweepPArray(...)` if a helper is useful.

- [ ] **Step 1: Add the control.** With roll-forward disabled, at least one crash point must recover *less* than with it enabled (recovery is load-bearing). Run the same seed twice — enabled vs disabled — and compare the per-k `P` arrays.

```java
    @Test
    public void testRecoveryIsLoadBearingW0() throws Exception {
        runWithCrashFacade(() -> {
            setProperty(PropertyKey.CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED, true);
            int[] withRec = runSeedSweep(1234L, 5678L, 0).recoveredByK();

            setProperty(PropertyKey.CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED, false);
            int[] without = runSeedSweep(1234L, 5678L, 0).recoveredByK();

            boolean recoveryHelpedSomewhere = false;
            for (int k = 1; k < withRec.length && k < without.length; k++) {
                Assert.assertTrue("recovery must never recover LESS than no-recovery at k=" + k,
                        withRec[k] >= without[k]);
                if (withRec[k] > without[k]) recoveryHelpedSomewhere = true;
            }
            Assert.assertTrue("roll-forward must be load-bearing at >=1 crash point", recoveryHelpedSomewhere);
        });
    }
```

Note for the implementer: with roll-forward disabled the recovered state is still expected to be a *member* of `fp[]` (a valid older snapshot) — the workload's `oracle` already asserts membership, so a disabled-recovery run that produces a non-member is itself a finding. Reset `CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED` to default in a `finally` if needed.

- [ ] **Step 2: Run to verify.** Expected: PASS (and the disabled arm demonstrably recovers less at ≥1 k). If disabled and enabled are identical everywhere, the workload never builds a lazy gap the epoch must roll forward — increase `transactionCount` or `CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS` so epochs lag apply, mirroring D1's lazy-gap setup.

- [ ] **Step 3: Commit.** `git commit -am "test(crash): SP-D D2 recovery-is-load-bearing negative control"`

---

### Task 6: Multi-seed CI-fast entry point + nightly override

**Files:**
- Modify: `.../crash/RandomizedAdaptiveCrashFuzzTest.java`

**Interfaces:** Consumes `runSeedSweep`, `assertW0Bars`, `generateRandom`. Produces the public CI entry `testAdaptiveCrashFuzzW0`.

- [ ] **Step 1: Add the seed-count-driven entry.** Fixed seeds always run; a logged random seed always runs (broadens coverage, reproducible from its printed pair); `-Dfuzz.adaptive.crash.seeds` raises the fixed-seed count for nightly.

```java
    @Test
    public void testAdaptiveCrashFuzzW0() throws Exception {
        final int seeds = Integer.getInteger("fuzz.adaptive.crash.seeds", FIXED_SEEDS0.length);
        runWithCrashFacade(() -> {
            for (int s = 0; s < Math.min(seeds, FIXED_SEEDS0.length); s++) {
                assertW0Bars(runSeedSweep(FIXED_SEEDS0[s], FIXED_SEEDS1[s], 0));
            }
            // one fresh random seed, logged via fuzzer.after() for repro
            Rnd rnd = fuzzer.generateRandom(LOG);
            assertW0Bars(runSeedSweep(rnd.getSeed0(), rnd.getSeed1(), 0));
        });
    }
```

Ensure `LOG` is available (add `private static final Log LOG = LogFactory.getLog(RandomizedAdaptiveCrashFuzzTest.class);` with imports `io.questdb.log.Log`, `io.questdb.log.LogFactory`). Fold `testFullLibraryW0` into this if it becomes redundant, or keep it as the fixed-seed-only variant.

- [ ] **Step 2: Run to verify** with the default and a raised count: `mvn -q -pl core test -Dtest=RandomizedAdaptiveCrashFuzzTest#testAdaptiveCrashFuzzW0 -DfailIfNoTests=false`, then again with `-Dfuzz.adaptive.crash.seeds=6` (still ≤ fixed-seed array length, or extend the arrays). Expected: PASS; the run log prints the random seed pair. Confirm total default runtime ≤ ~10 min.

- [ ] **Step 3: Commit.** `git commit -am "test(crash): SP-D D2 multi-seed CI-fast entry + nightly -D override"`

---

### Task 7: W>0 corruption-freedom pass (group-commit batching)

Exercises the deferred-flush batching path (`flushPendingDurable`) under crash — a distinct code path from W=0's synchronous fsync, and the exact D2-backlog fidelity concern. Per-k the workload's `oracle` already enforces bars 1–2 (membership + clean-reopen); the W=0-only `assertW0Bars` is **not** applied. The precise W>0 RPO *quantity* is out of v1 scope (per the spec: the crash index is a durability-op count and does not align across commit modes, so a valid NOSYNC/W comparison needs a txn-boundary crash harness).

**Files:**
- Modify: `.../crash/RandomizedAdaptiveCrashFuzzTest.java`

**Interfaces:** Consumes `FuzzCrashWorkload`, `runSeedSweep`, `FIXED_SEEDS0/1`. Produces `testAdaptiveCrashFuzzWindowed`.

- [ ] **Step 1: Add the windowed sweep test.** For each W ∈ {small, large} and each fixed seed, run the sweep. A non-member recovered state under batching throws inside `FuzzCrashWorkload.oracle` (bar 1) — a corruption GA-blocker. Assert the sweep exercised crash points (a floor on N).

```java
    @Test
    public void testAdaptiveCrashFuzzWindowed() throws Exception {
        final int[] windowsUs = {50, 500};   // one small, one larger group-commit window (microseconds)
        runWithCrashFacade(() -> {
            for (int windowUs : windowsUs) {
                for (int s = 0; s < FIXED_SEEDS0.length; s++) {
                    // bars 1-2 enforced per-k inside FuzzCrashWorkload.oracle; a non-member recovered state
                    // under batching would throw here (corruption GA-blocker). No W=0 tighteners at W>0.
                    SweepResult r = runSeedSweep(FIXED_SEEDS0[s], FIXED_SEEDS1[s], windowUs);
                    Assert.assertTrue("sweep must exercise >=1 crash point at W=" + windowUs, r.sweptPoints > 0);
                }
            }
        });
    }
```

- [ ] **Step 2: Run to verify.** `mvn -q -pl core test -Dtest=RandomizedAdaptiveCrashFuzzTest#testAdaptiveCrashFuzzWindowed -DfailIfNoTests=false`. Expected: PASS — under both windows, every recovered state across every seed's sweep is a member of `fp[]` (no corruption from deferred-flush batching) and no table is left suspended. A membership failure under W>0 is a real corruption GA-blocker (the `flushPendingDurable` W>0 path is the D2-backlog fidelity concern) → capture the seed (`s0,s1`) and file it; do not weaken the oracle.

- [ ] **Step 3: Commit.** `git commit -am "test(crash): SP-D D2 W>0 corruption-freedom pass (group-commit batching)"`

---

## Final verification (after all tasks)

- [ ] Full class green: `mvn -q -pl core test -Dtest=RandomizedAdaptiveCrashFuzzTest -DfailIfNoTests=false` → all tests pass, default runtime ≤ ~10 min.
- [ ] Regression spot-check that the shared harness is untouched behaviorally: `mvn -q -pl core test -Dtest=AdaptiveCrashSweepSelfCheckTest,RecoveryCoordinatorTest -DfailIfNoTests=false` → green.
- [ ] Confirm no `core/src/main` file changed: `git diff --stat HEAD~7 -- core/src/main` → empty.

## Notes for the executor

- If any sweep surfaces a **non-member recovered state**, that is the deliverable working — a real adaptive corruption. Stop, record the exact `s0,s1` seed (printed by `fuzzer.after()`), and escalate rather than adjusting the oracle.
- The single largest risk is fingerprint **canonicalization** under destructive ops (Task 4). The self-check (Task 3) and determinism test (Task 2) are the guards: if they are green and Task 4 goes red, suspect a real bug before suspecting the fingerprint. Only strengthen the dump ORDER BY (never weaken a safety bar) if a mismatch is provably an ordering artifact of legitimately-equal rows.
- Keep `walWriterCount = 1` everywhere; parallel apply is D3 territory and breaks sweep determinism.
