# ASOF JOIN algorithm auto-selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the QuestDB optimiser auto-select the fast single-symbol ASOF cursors (`asof_index` when the slave symbol is indexed and the master is small; `asof_memoized` for sparse-ts non-indexed masters) instead of always defaulting to Dense — with a do-no-harm fallback and the decision surfaced in EXPLAIN.

**Architecture:** Two phases. **Phase 1** adds a row-count-ratio decision in `SqlCodeGenerator.generateJoinAsof` (single-symbol branch, after hint checks, before the Dense default) that picks `AsOfJoinIndexedRecordCursorFactory` when the slave symbol is indexed and the master is confidently small; estimates come from `getTableToken()` + the reader (threaded `executionContext`), plus master `LIMIT`. **Phase 2** makes `asof_memoized` safe to auto-select by adding a runtime dense-timestamp guard to its cursor (abandon memoization → resilient single-symbol forward scan once an equal-timestamp run exceeds a swept threshold K), then sweeps K to pick the default.

**Tech Stack:** Java 17, QuestDB core (`/data/questdb-oss`), JMH (`benchmarks/`), JUnit (`core/src/test`). Build: `mvn -q -pl core -am -o -DskipTests -Dcheckstyle.skip package`. Tests: `mvn -q -pl core -o -Dtest=AsOfJoinTest test`.

## Global Constraints

- All ASOF cursor factories are **result-equivalent**; only performance differs. Every change must keep `AsOfJoinTest` (116 cases) and `AsOfJoinFuzzTest` (6) green.
- **Do no harm:** when any signal (row count) is unavailable, fall through to today's `AsOfJoinDenseSingleSymbolRecordCursorFactory`. Never regress the current default.
- Explicit hints always win: auto-selection is inserted **after** the `asof_index`/`asof_memoized`/`asof_fast` hint returns and **before** the Dense default (`SqlCodeGenerator` ~L5188).
- Config getter naming mirrors the existing `getSqlAsOfAdaptiveBackScanBudget` pattern across all 6 config files.
- EXPLAIN plan display strings come from `sink.type(...)`/`sink.attr(...)`; tests assert on those strings, not class names.
- The adaptive Fast↔Dense prelude (`cairo.sql.asof.adaptive.backscan.budget`) is **out of scope** and untouched.

---

## File Structure

**Phase 1 (auto-index):**
- `core/src/main/java/io/questdb/PropertyKey.java` — add 2 property-key enum constants.
- `core/src/main/java/io/questdb/cairo/CairoConfiguration.java` — 2 getter declarations.
- `core/src/main/java/io/questdb/cairo/CairoConfigurationWrapper.java` — 2 delegating overrides.
- `core/src/main/java/io/questdb/cairo/DefaultCairoConfiguration.java` — 2 default-value overrides.
- `core/src/main/java/io/questdb/PropServerConfiguration.java` — 2 fields + 2 reads + 2 getter overrides.
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — thread `executionContext` into `generateJoinAsof`; add estimate helper + auto-index decision.
- `core/src/main/java/io/questdb/griffin/engine/join/AsOfJoinIndexedRecordCursorFactory.java` — EXPLAIN reason attr.
- `core/src/main/java/io/questdb/griffin/engine/join/AsOfJoinDenseRecordCursorFactoryBase.java` — EXPLAIN note attr (why Dense kept).
- `core/src/test/java/io/questdb/test/griffin/engine/join/AsOfJoinTest.java` — new tests.

**Phase 2 (memoized dense-ts guard + sweep):**
- `core/src/main/java/io/questdb/PropertyKey.java` — 1 property key.
- config quartet — 1 getter each (`getSqlAsOfMemoizedDenseRunThreshold`).
- `core/src/main/java/io/questdb/griffin/engine/join/AsOfJoinMemoizedRecordCursorFactory.java` — run-length counter + fallback in `performKeyMatching`, threshold from config.
- `benchmarks/src/main/java/org/questdb/AsOfJoinAlgorithmBenchmark.java` — parameterize threshold + a ts-density × K sweep shape.
- `run_memoized_k_sweep.sh` — sweep driver.

---

# PHASE 1 — auto-select `asof_index`

### Task 1: Config option `cairo.sql.asof.auto.algo` (boolean, default true)

**Files:**
- Modify: `core/src/main/java/io/questdb/PropertyKey.java` (near L707)
- Modify: `core/src/main/java/io/questdb/cairo/CairoConfiguration.java` (near L590)
- Modify: `core/src/main/java/io/questdb/cairo/CairoConfigurationWrapper.java` (near L861)
- Modify: `core/src/main/java/io/questdb/cairo/DefaultCairoConfiguration.java` (near L863)
- Modify: `core/src/main/java/io/questdb/PropServerConfiguration.java` (field near L435, read near L1659, getter near L4503)

**Interfaces:**
- Produces: `boolean CairoConfiguration.isSqlAsOfAutoAlgoEnabled()`

- [ ] **Step 1: Add the property key.** In `PropertyKey.java`, after the `CAIRO_SQL_ASOF_ADAPTIVE_BACKSCAN_BUDGET(...)` constant add:
```java
    CAIRO_SQL_ASOF_AUTO_ALGO("cairo.sql.asof.auto.algo"),
    CAIRO_SQL_ASOF_INDEX_MAX_MASTER_BP("cairo.sql.asof.index.max.master.bp"),
```

- [ ] **Step 2: Declare getters.** In `CairoConfiguration.java`, after `getSqlAsOfAdaptiveBackScanBudget();`:
```java
    /** When true, the optimiser may auto-select asof_index / asof_memoized for single-symbol ASOF. */
    boolean isSqlAsOfAutoAlgoEnabled();

    /** Max master/slave row ratio (in basis points, /10000) below which asof_index is auto-selected. */
    int getSqlAsOfIndexMaxMasterBp();
```

- [ ] **Step 3: Wrapper delegation.** In `CairoConfigurationWrapper.java`, after the adaptive-budget override:
```java
    @Override
    public boolean isSqlAsOfAutoAlgoEnabled() {
        return getDelegate().isSqlAsOfAutoAlgoEnabled();
    }

    @Override
    public int getSqlAsOfIndexMaxMasterBp() {
        return getDelegate().getSqlAsOfIndexMaxMasterBp();
    }
```

- [ ] **Step 4: Defaults.** In `DefaultCairoConfiguration.java`, after the adaptive-budget override:
```java
    @Override
    public boolean isSqlAsOfAutoAlgoEnabled() {
        return true;
    }

    @Override
    public int getSqlAsOfIndexMaxMasterBp() {
        return 200; // 2.00% — index wins below ~2% master/slave ratio (crossover ~1-2.5%, see bench)
    }
```

- [ ] **Step 5: PropServerConfiguration wiring.** Add fields (near L435):
```java
    private final boolean sqlAsOfAutoAlgo;
    private final int sqlAsOfIndexMaxMasterBp;
```
Add reads in the constructor (near L1659, beside the adaptive-budget read):
```java
            this.sqlAsOfAutoAlgo = getBoolean(properties, env, PropertyKey.CAIRO_SQL_ASOF_AUTO_ALGO, true);
            this.sqlAsOfIndexMaxMasterBp = getInt(properties, env, PropertyKey.CAIRO_SQL_ASOF_INDEX_MAX_MASTER_BP, 200);
```
Add getter overrides in the inner config class (near L4503):
```java
        @Override
        public boolean isSqlAsOfAutoAlgoEnabled() {
            return sqlAsOfAutoAlgo;
        }

        @Override
        public int getSqlAsOfIndexMaxMasterBp() {
            return sqlAsOfIndexMaxMasterBp;
        }
```

- [ ] **Step 6: Build to verify wiring compiles.**
Run: `mvn -q -pl core -am -o -DskipTests -Dcheckstyle.skip package 2>&1 | grep -iE "BUILD|ERROR" | head`
Expected: no ERROR lines (empty).

- [ ] **Step 7: Commit.**
```bash
git add core/src/main/java/io/questdb/PropertyKey.java core/src/main/java/io/questdb/cairo/CairoConfiguration.java core/src/main/java/io/questdb/cairo/CairoConfigurationWrapper.java core/src/main/java/io/questdb/cairo/DefaultCairoConfiguration.java core/src/main/java/io/questdb/PropServerConfiguration.java
git commit -m "feat(sql): config for ASOF auto-algo selection (cairo.sql.asof.auto.algo, index.max.master.bp)"
```

---

### Task 2: Thread `executionContext` into `generateJoinAsof` + row-count estimate helper

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (`generateJoinAsof` sig ~L5062; call site ~L5720; add helper)

**Interfaces:**
- Consumes: `SqlExecutionContext` (already a param of enclosing `generateJoins`).
- Produces: `private long estimateBaseRowCount(RecordCursorFactory f, SqlExecutionContext ec)` — returns table row count via `getTableToken()`, or `-1` if unknown. `private static long masterLimitOrMinus1(IQueryModel masterModel)`.

- [ ] **Step 1: Add `executionContext` param to `generateJoinAsof`.** Change the signature (L5062-5071) to append:
```java
            final RecordMetadata slaveMetadata,
            final SqlExecutionContext executionContext
    ) throws SqlException {
```
And the call site (~L5720):
```java
                                        ? generateJoinAsof(isSelfJoin, model, slaveModel, master, masterMetadata, masterAlias, slaveToFree, slaveMetadata, executionContext)
```

- [ ] **Step 2: Add the estimate helpers** (private methods in `SqlCodeGenerator`, near `generateJoinAsof`):
```java
    // Cheap plan-time base-table row-count estimate. Returns -1 when unknown (subquery/join/no token).
    // NOTE: getTableToken() on a filtered factory returns the BASE table, so a filtered master is
    // over-estimated — that only ever makes us MORE conservative (skip index), never wrong results.
    private long estimateBaseRowCount(RecordCursorFactory f, SqlExecutionContext ec) {
        final TableToken token = f.getTableToken();
        if (token == null) {
            return -1;
        }
        final long tracked = ec.getCairoEngine().getRecentWriteTracker().getRowCount(token);
        if (tracked != Numbers.LONG_NULL) {
            return tracked;
        }
        try (TableReader r = ec.getReader(token)) {
            return r.size();
        } catch (CairoException e) {
            return -1;
        }
    }

    private static long masterLimitOrMinus1(IQueryModel masterModel) {
        final ExpressionNode lo = masterModel.getLimitLo();
        final ExpressionNode hi = masterModel.getLimitHi();
        final ExpressionNode lim = hi != null ? hi : lo;
        if (lim != null && lim.type == ExpressionNode.CONSTANT) {
            try {
                return Numbers.parseLong(lim.token);
            } catch (NumericException ignore) {
                return -1;
            }
        }
        return -1;
    }
```
(Confirm imports: `TableToken`, `Numbers`, `TableReader`, `CairoException`, `ExpressionNode`, `NumericException` — most already imported in this file; add any missing.)

- [ ] **Step 3: Build to verify.**
Run: `mvn -q -pl core -am -o -DskipTests -Dcheckstyle.skip package 2>&1 | grep -iE "BUILD|ERROR" | head`
Expected: empty (no errors).

- [ ] **Step 4: Commit.**
```bash
git add core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java
git commit -m "feat(sql): thread executionContext into generateJoinAsof + base-row-count estimate helper"
```

---

### Task 3: Auto-index decision + EXPLAIN reason

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (single-symbol branch, ~L5188, before the DenseSingleSymbol default return)
- Modify: `core/src/main/java/io/questdb/griffin/engine/join/AsOfJoinIndexedRecordCursorFactory.java` (constructor + `toPlan`)

**Interfaces:**
- Consumes: `estimateBaseRowCount`, `masterLimitOrMinus1`, `isSqlAsOfAutoAlgoEnabled()`, `getSqlAsOfIndexMaxMasterBp()`.
- Produces: `AsOfJoinIndexedRecordCursorFactory` gains an optional `CharSequence autoReason` (null when hint-selected).

- [ ] **Step 1: Write the failing test** (in `AsOfJoinTest.java`) — indexed slave symbol + tiny master must auto-pick the indexed scan without a hint:
```java
    @Test
    public void testAutoSelectIndexedForSmallMasterIndexedSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table quotes (sym symbol index, ts timestamp, bid double) timestamp(ts) partition by day bypass wal");
            execute("insert into quotes select ('s'||(x%1000))::symbol, (x*1000)::timestamp, x from long_sequence(200000)");
            execute("create table trades (sym symbol, ts timestamp, px double) timestamp(ts) partition by day bypass wal");
            execute("insert into trades select 's1', (x*1000000)::timestamp, x from long_sequence(50)");
            printSql("EXPLAIN SELECT sum(q.bid) FROM trades t ASOF JOIN quotes q ON (sym)");
            TestUtils.assertContains(sink, "AsOf Join Indexed Scan");
        });
    }
```

- [ ] **Step 2: Run it to confirm it fails.**
Run: `mvn -q -pl core -o -Dtest=AsOfJoinTest#testAutoSelectIndexedForSmallMasterIndexedSymbol test 2>&1 | tail -20`
Expected: FAIL — plan contains "AsOf Join Dense", not "Indexed Scan".

- [ ] **Step 3: Add the decision block.** In `generateJoinAsof`, in the single-symbol branch immediately before the `// Default single-symbol ASOF: forward-scan DenseSingleSymbol` return (~L5188):
```java
                            if (configuration.isSqlAsOfAutoAlgoEnabled()
                                    && slaveMetadata.isColumnIndexed(slaveSymbolColumnIndex)) {
                                long slaveN = estimateBaseRowCount(slave, executionContext);
                                long masterN = estimateBaseRowCount(master, executionContext);
                                long masterLimit = masterLimitOrMinus1(model.getJoinModels().getQuick(0));
                                long effMaster = masterLimit >= 0
                                        ? (masterN >= 0 ? Math.min(masterN, masterLimit) : masterLimit)
                                        : masterN;
                                int bp = configuration.getSqlAsOfIndexMaxMasterBp();
                                if (slaveN > 0 && effMaster >= 0 && effMaster * 10000L <= slaveN * (long) bp) {
                                    writeSymbolAsString.unset(slaveSymbolColumnIndex);
                                    return new AsOfJoinIndexedRecordCursorFactory(
                                            configuration, joinMetadata, master, slave, joinColumnSplit,
                                            slaveSymbolColumnIndex, symbolJoinKeyMapping, slaveContext, toleranceInterval,
                                            "auto:master≈" + effMaster + " slave≈" + slaveN + " bp≤" + bp
                                    );
                                }
                            }
```
(Check whether `AsOfJoinIndexedRecordCursorFactory` needs `writeSymbolAsString.unset(...)` like the fast path at L5172 — mirror the indexed-hint construction at L5138 exactly for arg order; only ADD the trailing reason arg.)

- [ ] **Step 4: Add the `autoReason` param + EXPLAIN attr.** In `AsOfJoinIndexedRecordCursorFactory.java`: add `private final CharSequence autoReason;` field, accept it as the last constructor arg (default existing hint call sites pass `null`), and in `toPlan`:
```java
    public void toPlan(PlanSink sink) {
        sink.type("AsOf Join Indexed Scan");
        if (autoReason != null) {
            sink.attr("select").val(autoReason);
        }
        sink.attr("condition").val(joinContext);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }
```
Update the hint call site (`SqlCodeGenerator` L5138) to pass `null` as the new last arg.

- [ ] **Step 5: Run the test to confirm it passes.**
Run: `mvn -q -pl core -o -Dtest=AsOfJoinTest#testAutoSelectIndexedForSmallMasterIndexedSymbol test 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 6: Regression — full ASOF suite stays green.**
Run: `mvn -q -pl core -o -Dtest=AsOfJoinTest,AsOfJoinFuzzTest test 2>&1 | tail -15`
Expected: BUILD SUCCESS, 0 failures.

- [ ] **Step 7: Commit.**
```bash
git add core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java core/src/main/java/io/questdb/griffin/engine/join/AsOfJoinIndexedRecordCursorFactory.java core/src/test/java/io/questdb/test/griffin/engine/join/AsOfJoinTest.java
git commit -m "feat(sql): auto-select asof_index for small-master indexed single-symbol ASOF"
```

---

### Task 4: Do-no-harm guards — large master & unknown estimate keep Dense

**Files:**
- Modify: `core/src/test/java/io/questdb/test/griffin/engine/join/AsOfJoinTest.java`

**Interfaces:** none new (asserts Task 3 behaviour).

- [ ] **Step 1: Write the guard tests.**
```java
    @Test
    public void testAutoSelectKeepsDenseForLargeMaster() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table quotes (sym symbol index, ts timestamp, bid double) timestamp(ts) partition by day bypass wal");
            execute("insert into quotes select ('s'||(x%1000))::symbol, (x*10)::timestamp, x from long_sequence(200000)");
            execute("create table trades (sym symbol, ts timestamp, px double) timestamp(ts) partition by day bypass wal");
            execute("insert into trades select 's1', (x*10)::timestamp, x from long_sequence(200000)"); // master == slave size
            printSql("EXPLAIN SELECT sum(q.bid) FROM trades t ASOF JOIN quotes q ON (sym)");
            TestUtils.assertNotContains(sink, "Indexed Scan"); // ratio 100% > 2% -> Dense
        });
    }

    @Test
    public void testHintStillOverridesAutoSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table quotes (sym symbol index, ts timestamp, bid double) timestamp(ts) partition by day bypass wal");
            execute("insert into quotes select ('s'||(x%1000))::symbol, (x*1000)::timestamp, x from long_sequence(200000)");
            execute("create table trades (sym symbol, ts timestamp, px double) timestamp(ts) partition by day bypass wal");
            execute("insert into trades select 's1', (x*1000000)::timestamp, x from long_sequence(50)");
            printSql("EXPLAIN SELECT /*+ asof_dense(t q) */ sum(q.bid) FROM trades t ASOF JOIN quotes q ON (sym)");
            TestUtils.assertContains(sink, "AsOf Join Dense"); // explicit hint wins over auto-index
        });
    }
```

- [ ] **Step 2: Run to confirm PASS** (behaviour already implemented in Task 3).
Run: `mvn -q -pl core -o -Dtest=AsOfJoinTest#testAutoSelectKeepsDenseForLargeMaster+testHintStillOverridesAutoSelect test 2>&1 | tail -15`
Expected: PASS both.

- [ ] **Step 3: Commit.**
```bash
git add core/src/test/java/io/questdb/test/griffin/engine/join/AsOfJoinTest.java
git commit -m "test(sql): auto-index do-no-harm guards (large master + hint override keep Dense)"
```

---

### Task 5: Result-equivalence test (auto-index vs forced-Dense oracle)

**Files:**
- Modify: `core/src/test/java/io/questdb/test/griffin/engine/join/AsOfJoinTest.java`

- [ ] **Step 1: Write the equivalence test** — same query with auto-index vs `asof_dense` hint must return identical rows:
```java
    @Test
    public void testAutoIndexResultsMatchDense() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table quotes (sym symbol index, ts timestamp, bid double) timestamp(ts) partition by day bypass wal");
            execute("insert into quotes select ('s'||(x%50))::symbol, (x*1000)::timestamp, x::double from long_sequence(100000)");
            execute("create table trades (sym symbol, ts timestamp, px double) timestamp(ts) partition by day bypass wal");
            execute("insert into trades select ('s'||(x%50))::symbol, (x*137)::timestamp, x::double from long_sequence(500)");
            String auto  = "SELECT t.ts, t.sym, q.bid FROM trades t ASOF JOIN quotes q ON (sym)";
            String dense = "SELECT /*+ asof_dense(t q) */ t.ts, t.sym, q.bid FROM trades t ASOF JOIN quotes q ON (sym)";
            printSql("EXPLAIN " + auto);
            TestUtils.assertContains(sink, "AsOf Join Indexed Scan"); // confirm auto path engaged
            assertSqlCursorsEqual(dense, auto); // helper below or inline: compare two result sets
        });
    }
```
(If no `assertSqlCursorsEqual` helper exists, compare via two `printSql` captures into separate sinks and `TestUtils.assertEquals`.)

- [ ] **Step 2: Run to confirm PASS.**
Run: `mvn -q -pl core -o -Dtest=AsOfJoinTest#testAutoIndexResultsMatchDense test 2>&1 | tail -15`
Expected: PASS.

- [ ] **Step 3: Commit.**
```bash
git add core/src/test/java/io/questdb/test/griffin/engine/join/AsOfJoinTest.java
git commit -m "test(sql): auto-index results equal forced-Dense oracle"
```

---

### Task 6: Full regression + benchmark re-confirmation

- [ ] **Step 1: Run the broader join test set.**
Run: `mvn -q -pl core -o -Dtest="AsOfJoin*,LtJoin*" test 2>&1 | tail -15`
Expected: BUILD SUCCESS.

- [ ] **Step 2: Re-run the illiquid_idx benchmark WITHOUT a hint** (default algo now auto-selects). Confirm the `default` column drops from ~25ms to ~0.5ms:
Run: `./run_idx_sweep.sh && grep -A3 "cardinality=1000" /data/asofbench/idx_sweep_summary.txt`
Expected: `default` ≈ index ≈ 0.5 ms (auto-index engaged); kill hung JVMs after (see driver).

- [ ] **Step 3: Commit any benchmark/doc updates + mark Phase 1 done in this plan.**
```bash
git add -A && git commit -m "docs(sql): Phase 1 auto-index confirmed on benchmark (default now ~0.5ms)"
```

---

# PHASE 2 — memoized dense-timestamp fallback + threshold sweep

**Design:** `asof_memoized` is 2–25× faster than Dense for sparse-ts single-symbol selective masters but cliffs (3372 ms) when many rows share a timestamp, because `performKeyMatching` back-scans the whole equal-timestamp run. Fix: count the current equal-timestamp run length during the back-scan; once it exceeds threshold **K**, abandon memoization and complete the remaining master rows with a resilient **single-symbol forward scan** (O(slave), never cliffs). Because memoized is single-symbol-only, the fallback needs only a *single-key* forward scan (last matching slave row per the one sought symbol) — much smaller than the general Dense two-map machinery, so we implement it directly in the memoized cursor rather than refactoring the Dense base.

> **START-OF-PHASE-2 CHECK — DONE (2026-07-22).** Read confirmed: DenseSingleSymbol's resilient scan lives entirely in `AsOfJoinDenseRecordCursorBase.hasNext()`/`scanForward()` and depends on a per-symbol last-row map (`fwdScanKeyToRowId`) — required because a single-symbol join still has a MULTI-symbol master. The memoized cursor extends `AbstractKeyedAsOfJoinRecordCursor`, not the Dense base, so it cannot inherit it. **DECISION (user): Extract & share (structural)** — refactor the Dense resilient forward/backward scan + maps out of `AsOfJoinDenseRecordCursorBase` into a reusable component both the Dense cursor and the memoized cursor drive, behaviour-preserving (guarded by AsOfJoinTest 120 + Fuzz 6). Tasks 8-11 below are re-scoped around this extraction.

### Extraction design (from read-gate)

New abstract class `AbstractDenseScanAsOfJoinRecordCursor extends AbstractKeyedAsOfJoinRecordCursor`
holding what is today private in `AsOfJoinDenseRecordCursorBase`: the two maps
(`fwdScanKeyToRowId`, `bwdScanKeyToRowId`), scan state (`forwardRowId`, `backwardRowId`,
`forwardScanExhausted`, `backwardScanExhausted`, `slaveCursorReadyForForwardScan`), `scanForward`,
`setupSlaveRec`, `resetDenseScanState`, map lifecycle (close/reopenClear/setMemoryTracker), the five
abstract hooks (`getSlaveJoinKey`, `joinKeysMatch`, `putSlaveJoinKey`, `putSlaveKeyToFind`,
`setupSymbolKeyToFind`), and a new callable `boolean resolveViaDenseScan(long masterTimestamp, long
minSlaveTimestamp, int slaveKeyToFind)` = the body of today's Dense `hasNext()` lines 223-306.
`AsOfJoinDenseRecordCursorBase.AsOfJoinDenseRecordCursorBase` extends it, keeps the adaptive prelude +
`performKeyMatching`, and its `hasNext()` becomes: adaptive-prelude wrapper + master iteration +
`resolveViaDenseScan(...)`. Behaviour-preserving — Dense/Fuzz tests are the guard.

### Task 8: Config `cairo.sql.asof.memoized.dense.run.threshold` (int, default 4096)

**Files:** `PropertyKey.java`, `CairoConfiguration.java`, `CairoConfigurationWrapper.java`, `DefaultCairoConfiguration.java`, `PropServerConfiguration.java` — same 6-site pattern as Task 1.

**Interfaces:** Produces `int CairoConfiguration.getSqlAsOfMemoizedDenseRunThreshold()`.

- [ ] **Step 1:** Add property key `CAIRO_SQL_ASOF_MEMOIZED_DENSE_RUN_THRESHOLD("cairo.sql.asof.memoized.dense.run.threshold")`.
- [ ] **Step 2:** Declare `int getSqlAsOfMemoizedDenseRunThreshold();` in `CairoConfiguration`.
- [ ] **Step 3:** Wrapper delegating override.
- [ ] **Step 4:** `DefaultCairoConfiguration` returns `4096` (provisional; finalized by Task 11 sweep).
- [ ] **Step 5:** `PropServerConfiguration` field + `getInt(..., 4096)` read + getter override.
- [ ] **Step 6:** Build: `mvn -q -pl core -am -o -DskipTests -Dcheckstyle.skip package 2>&1 | grep -iE "BUILD|ERROR"` → empty.
- [ ] **Step 7:** Commit `feat(sql): config cairo.sql.asof.memoized.dense.run.threshold`.

### Task 8: Memoized cursor dense-ts fallback

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/engine/join/AsOfJoinMemoizedRecordCursorFactory.java`

**Interfaces:**
- Consumes: `configuration.getSqlAsOfMemoizedDenseRunThreshold()` (set into cursor in factory ctor, mirroring `setAdaptiveBackScanBudget` wiring).
- Produces: memoized cursor that never cliffs on dense timestamps.

- [ ] **Step 1: Write the failing test** — memoized on dense-ts data returns correct results AND (proxy for "didn't cliff") completes. Correctness vs Dense oracle:
```java
    @Test
    public void testMemoizedDenseTimestampFallbackMatchesDense() throws Exception {
        assertMemoryLeak(() -> {
            // 10000 rows per timestamp (dense) — memoized's historical cliff shape
            execute("create table md (sym symbol, ts timestamp, v double) timestamp(ts) partition by day bypass wal");
            execute("insert into md select ('s'||(x%2000))::symbol, ((x/10000))::timestamp, x::double from long_sequence(200000)");
            execute("create table ord (sym symbol, ts timestamp, o double) timestamp(ts) partition by day bypass wal");
            execute("insert into ord select 's7', ((x*3))::timestamp, x::double from long_sequence(300)");
            String memo  = "SELECT /*+ asof_memoized(o md) */ o.ts, md.v FROM ord o ASOF JOIN md ON (sym)";
            String dense = "SELECT /*+ asof_dense(o md) */ o.ts, md.v FROM ord o ASOF JOIN md ON (sym)";
            assertSqlCursorsEqual(dense, memo);
        });
    }
```

- [ ] **Step 2: Run to confirm current behaviour** (should already PASS for correctness — memoized is correct, just slow; this test locks correctness before adding the fallback).
Run: `mvn -q -pl core -o -Dtest=AsOfJoinTest#testMemoizedDenseTimestampFallbackMatchesDense test 2>&1 | tail -15`
Expected: PASS (or slow-PASS). Keep it as the regression oracle.

- [ ] **Step 3: Implement the fallback in `performKeyMatching`** (memoized cursor, ~L386-503). Add cursor fields `private int denseRunThreshold = Integer.MAX_VALUE; private boolean denseFallbackMode; private long prevRunTimestamp = Long.MIN_VALUE; private int curRunLen;` plus a setter `setDenseRunThreshold(int)`. In the back-scan loop, after reading `slaveTimestamp` (L387): if `slaveTimestamp == prevRunTimestamp` increment `curRunLen` else set `curRunLen = 1, prevRunTimestamp = slaveTimestamp`; when `curRunLen > denseRunThreshold` set `denseFallbackMode = true` and `break` out to a new `resolveViaForwardScan(masterTimestamp)` that does a resilient single-key forward scan for the sought symbol (last slave row with matching symbol and ts ≤ masterTimestamp). Once `denseFallbackMode` is set, route all subsequent `performKeyMatching` calls straight to `resolveViaForwardScan`. (Mirror `switchToDenseMode()`'s state-reset discipline for the memoized `scannedRange*`/`rememberedSymbols`/`earliestRowId` fields.) Exact `resolveViaForwardScan` body per the START-OF-PHASE-2 decision.

- [ ] **Step 4: Wire the threshold** in the factory constructor (mirror `AsOfJoinDenseSingleSymbolRecordCursorFactory.java:74`):
```java
        this.cursor.setDenseRunThreshold(configuration.getSqlAsOfMemoizedDenseRunThreshold());
```

- [ ] **Step 5: EXPLAIN attr** — add to memoized `toPlan`: `sink.attr("denseRunThreshold").val(denseRunThreshold);`

- [ ] **Step 6: Run correctness test + full suite.**
Run: `mvn -q -pl core -o -Dtest=AsOfJoinTest,AsOfJoinFuzzTest test 2>&1 | tail -15`
Expected: BUILD SUCCESS.

- [ ] **Step 7: Commit** `feat(sql): memoized ASOF dense-timestamp fallback to single-key forward scan`.

### Task 9: Force a tiny threshold to prove the fallback path is exercised

- [ ] **Step 1: Test with threshold=1** (via a test-scoped config override so the fallback triggers immediately) still equals the Dense oracle — proves `resolveViaForwardScan` is correct on its own:
```java
    @Test
    public void testMemoizedForwardFallbackCorrectAtThresholdOne() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_ASOF_MEMOIZED_DENSE_RUN_THRESHOLD, 1);
        assertMemoryLeak(() -> { /* same data + assertSqlCursorsEqual(dense, memo) as Task 8 */ });
    }
```
(Confirm the test harness property-override mechanism — `node1.setProperty` or `overrideProperty` in `AbstractCairoTest`.)

- [ ] **Step 2: Run → PASS.** Run: `mvn -q -pl core -o -Dtest=AsOfJoinTest#testMemoizedForwardFallbackCorrectAtThresholdOne test 2>&1 | tail -15`
- [ ] **Step 3: Commit** `test(sql): memoized forward-scan fallback correct at threshold=1`.

### Task 10: Auto-select memoized for non-indexed sparse-ts small master (optional, gated)

- [ ] **Step 1:** In `generateJoinAsof`, extend the Task 3 auto block: when the slave symbol is NOT indexed but `isSqlAsOfAutoAlgoEnabled()` and the master is confidently small (same `effMaster`/ratio test), return `AsOfJoinMemoizedRecordCursorFactory` (with the fallback now protecting it), carrying an `auto:` reason. Keep behind `isSqlAsOfAutoAlgoEnabled()` (already the master switch).
- [ ] **Step 2:** Test: non-indexed small-master query auto-picks "AsOf Join Memoized Scan" and results equal Dense oracle.
- [ ] **Step 3:** Full regression green. Commit `feat(sql): auto-select asof_memoized for small-master non-indexed sparse-ts ASOF`.

### Task 11: Sweep K to choose the default threshold

**Files:**
- Modify: `benchmarks/src/main/java/org/questdb/AsOfJoinAlgorithmBenchmark.java` — read threshold from `-Dasof.bench.memoized.k` and set it on the config (the bench already builds a `DefaultCairoConfiguration` override; add `getSqlAsOfMemoizedDenseRunThreshold()` returning the system prop). Add a `dense_sym` variant already exists; add a `memoized` run over a **timestamp-density sweep** (rows-per-ts ∈ {1, 10, 100, 1000, 10000}).
- Create: `run_memoized_k_sweep.sh` — for K ∈ {64, 256, 1024, 4096, 16384, MAX} × density ∈ {1,100,10000}, run `algo=memoized`, kill hung JVM per run (reuse the lock-safe driver pattern from `run_idx_sweep.sh`).

- [ ] **Step 1:** Add the density-parameterized memoized shape + `-Dasof.bench.memoized.k` wiring; rebuild benchmarks (`mvn -q -pl benchmarks -am -o -DskipTests package`).
- [ ] **Step 2:** Run `./run_memoized_k_sweep.sh`; collect `/data/asofbench/memoized_k_summary.txt`.
- [ ] **Step 3:** Pick K = smallest value that keeps dense-ts (rows/ts=10000) within ~1.5× of Dense **and** costs <5% on sparse-ts (rows/ts=1). Update `DefaultCairoConfiguration.getSqlAsOfMemoizedDenseRunThreshold()` to that value; note the rationale inline.
- [ ] **Step 4:** Re-run `AsOfJoinTest,AsOfJoinFuzzTest` → green. Commit `perf(sql): set memoized dense-run threshold default from K sweep (K=<value>)`.

---

## Self-Review

- **Spec coverage:** RFC §"decision logic" → Tasks 1-4,10; §"approxRowCount signal" → Task 2 (via getTableToken, simpler than a new interface method — deviation noted); §"EXPLAIN reason" → Tasks 3,8; §"memoized runtime guard" → Tasks 7-9; §"sweep K" → Task 11; §"do-no-harm" → Task 4; §"correctness/fuzz" → Tasks 5,8,9. Covered.
- **Deviation from RFC:** RFC proposed a new `RecordCursorFactory.approxRowCount()`; verification showed base factories lack context, so estimation moved to the call site via existing `getTableToken()` + threaded `executionContext` (less invasive, no interface sprawl). WHERE-time-filtered masters are over-estimated → conservatively skip index (documented, safe); capturing them is future work (needs interval-scan size estimate).
- **Placeholders:** Task 8 Step 3 (`resolveViaForwardScan` body) and Task 9/11 harness specifics are deliberately deferred to the START-OF-PHASE-2 read of `AsOfJoinDenseSingleSymbolRecordCursorFactory` — Phase 2 is not code-final until that check runs. Phase 1 (Tasks 1-6) is fully code-complete.
- **Type consistency:** `isSqlAsOfAutoAlgoEnabled()`, `getSqlAsOfIndexMaxMasterBp()`, `getSqlAsOfMemoizedDenseRunThreshold()`, `estimateBaseRowCount`, `masterLimitOrMinus1`, `autoReason`, `setDenseRunThreshold` used consistently across tasks.
