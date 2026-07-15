# SP-D1 Crash-Sweep Workloads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Run the four schema variants that the adaptive epoch/recovery cycle has never been crash-tested against — O3, indexed-symbol, multi-table, mat-view — through the proven `forEachAdaptiveCrashPoint` sweep, so a real adaptive durability/recovery bug is either found (a GA-blocker to fix) or ruled out at every crash point.

**Architecture:** Each task adds ONE `AdaptiveCrashWorkload` for a schema and one test class extending `AbstractAdaptiveCrashSweepTest`, driving it through the existing `forEachAdaptiveCrashPoint(workload, cap)` sweep. No driver changes (the driver is validated: commit `25518b78`). The template to mirror is `AdaptiveCrashSweepSelfCheckTest` (the W0 baseline).

**Tech Stack:** Java (QuestDB core test), JDK25, JUnit4, `AbstractCrashConsistencyTest`/`AbstractAdaptiveCrashSweepTest`, `CrashFaultFilesFacade`.

## Global Constraints

- Worktree `~/claude/wt/oss/adaptive-commit`, branch `nw_adaptive_commit`. JDK25 `/usr/lib/jvm/java-25-openjdk-amd64`.
- Test/run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -f pom.xml -pl core test -Dtest='<Class>' -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`. Read `core/target/surefire-reports/<FQCN>.txt`. (`SeqTxnMetricsTest` :9009 bind error is pre-existing/unrelated — never run it.)
- **Fixed W = 0** (`CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US=0`), `CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS=0`, `modelSharedJournal=false` — same as the self-check. W>0 RPO sweeps are D2, not here.
- **Driver interface (do NOT modify the driver):** `forEachAdaptiveCrashPoint(AdaptiveCrashWorkload w, int cap)` → `SweepResult`. `AdaptiveCrashWorkload` = `TableToken[] setup(int iteration)` (create fresh table(s), `drop table if exists`, set props; return the tokens recovery must scan), `void commit()` (the swept commit phase — **MUST poll `anyTableSuspended(tokens)` after each apply step and return early when a crash has fired**, else post-crash commits mask the injection point), `int oracle(int k, int n)` (assert the per-k oracle, RETURN the recovered committed-row count), `default void teardown()`.
- **ORACLE (§D1.b) — every workload asserts ALL of, at every k:** (1) **no silent corruption** — surviving rows are an exact prefix of the committed identity sequence, NO gaps, NO wrong values (reuse `assertNoSilentCorruption` semantics + an explicit prefix check; tolerate ONLY a loud `CairoException`/`CairoError`/`InternalError` torn read); (2) **not suspended** after recovery + a follow-up insert+read succeeds; (3) **full restore at k=N** (W=0 ⇒ every committed row back); (4) **[review watch-item ii] Bar-2 durable-survival FLOOR** — assert the recovered count is *monotonic non-decreasing in k* across the sweep (collect `SweepResult.recoveredByK()` and assert `r[k] >= r[k-1]`), so a transient intermediate under-count that self-heals by N cannot slip through unnoticed.
- **Negative control per workload:** one `@Test` with `CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED=false` at a representative crash point must leave the table WRONG/short (mirror `AdaptiveRecoveryRollForwardCrashTest`'s control) — proving recovery does real work for this schema.
- **[review watch-item i]** Any workload with a VARSIZE/symbol/array column (W2 especially) must, in its report, RE-VERIFY the driver's "no-fsync-on-close" fidelity for that column type: confirm the pre-`crash()` WAL-writer release does not fsync/journal the varsize aux (`setAppendAuxMemAppendPosition` path). If it does, the crash is easier than reality for that schema — STOP and flag it (do not weaken the oracle to pass).
- Sweeps take ~1–2 min each; a `@Test` slow-marker is acceptable if runtime warrants (implementer's call; note it).

---

### Task W1: O3 (out-of-order) adaptive crash sweep

**Files:** Create `core/src/test/java/io/questdb/test/cairo/crash/AdaptiveO3CrashSweepTest.java`.

**Interfaces:** Consumes the driver (above). Produces nothing downstream.

- [ ] **Step 1 (RED):** Write `AdaptiveO3CrashSweepTest extends AbstractAdaptiveCrashSweepTest`, mirroring `AdaptiveCrashSweepSelfCheckTest`, with a workload whose `commit()` inserts a fixed sequence of rows with **deliberately out-of-order timestamps** that force the O3 merge path (e.g. commit v=0..5 with timestamps that interleave across an existing partition boundary — alternate a late ts then an earlier ts so each commit triggers an O3 merge, not a pure append). `v` still equals commit order (0..5) so the oracle can order by `v` and assert the identity prefix independent of ts ordering. Run through `forEachAdaptiveCrashPoint`. Assert the full oracle (all 4 clauses; order the prefix check by `v`). Run → expect it to compile-fail first (new class), then RED only if a real bug exists.
- [ ] **Step 2:** Run the test. Read the surefire report. If GREEN at all crash points → O3 is crash-safe under adaptive (record the N and the recovered staircase). If RED → you have found a **real adaptive O3 recovery bug**: root-cause it (systematic-debugging), capture the exact failing crash point k + the corrupt/short read, and REPORT it as a GA-blocker (do NOT paper over it or weaken the oracle).
- [ ] **Step 3:** Add the negative control (`ROLL_FORWARD_ENABLED=false`) proving recovery does real work for O3.
- [ ] **Step 4 (commit):** `git commit -m "test(crash): adaptive O3 out-of-order crash sweep (SP-D W1)"` (add a note if a real bug was found + filed).

---

### Task W2: Indexed-symbol adaptive crash sweep

**Files:** Create `core/src/test/java/io/questdb/test/cairo/crash/AdaptiveIndexedSymbolCrashSweepTest.java`.

- [ ] **Step 1 (RED):** Workload table `(ts timestamp, s symbol index, v long) timestamp(ts) partition by day wal`. `commit()` inserts v=0..K with a small rotating set of symbol values (e.g. `s in {'a','b','c'}`) so the symbol dictionary + index both grow. Oracle additions beyond the base: after recovery, (a) the base identity-prefix check by `v`; (b) **index/data consistency** — for each symbol value present, `select v from t where s = '<val>'` must return exactly the `v`s that were committed with that symbol among the surviving prefix (an index that references rewound-away rows, or misses surviving rows, is corruption). Drive through the sweep.
- [ ] **Step 2:** Run. GREEN → indexed-symbol crash-safe under adaptive. RED → real bug (likely a symbol dictionary or index/data skew after recovery) → root-cause + report as GA-blocker.
- [ ] **Step 3 — [watch-item i] fidelity re-verification:** In the report, confirm whether the pre-`crash()` WAL-writer release fsyncs/journals the symbol column's varsize aux (`.k`/`.v`/symbol offset files). Read the WAL-writer close path for the symbol/varsize column. State the finding explicitly. If it adds durability the crash shouldn't have, flag it (the sweep would be easier than reality for symbols).
- [ ] **Step 4:** Negative control.
- [ ] **Step 5 (commit):** `test(crash): adaptive indexed-symbol crash sweep (SP-D W2)`.

---

### Task W3: Multi-table simultaneous adaptive crash sweep

**Files:** Create `core/src/test/java/io/questdb/test/cairo/crash/AdaptiveMultiTableCrashSweepTest.java`.

- [ ] **Step 1 (RED):** Workload creates **2 all-adaptive** tables (`t1`, `t2`) — start with 2 (the prior single-table limitation was a torn NOSYNC sequencer on teardown; all-adaptive should avoid it; if 2 is stable, optionally try 3). `commit()` interleaves commits across both (e.g. round-robin insert into t1, t2, t1, t2 …), polling `anyTableSuspended(t1tok, t2tok)`. `setup` returns both tokens so ONE `recover()` pass restores both. Oracle: BOTH tables independently satisfy the base oracle (identity prefix by v, not suspended, reopen), and neither is left behind by `RecoveryCoordinator.recoverTable()`'s per-table loop. `oracle(k,n)` returns the SUM of both tables' recovered counts (monotonic across k).
- [ ] **Step 2:** Run. If teardown is unstable at 3 tables, cap at 2 and document. GREEN → the per-table recovery loop is crash-safe across simultaneously-adaptive tables. RED → real cross-table recovery bug → root-cause + report.
- [ ] **Step 3:** Negative control (recovery disabled loses rows in at least one table).
- [ ] **Step 4 (commit):** `test(crash): adaptive multi-table simultaneous crash sweep (SP-D W3)`.

---

### Task W4: Mat-view adaptive crash sweep (highest complexity — split to D1.5 if intractable)

**Files:** Create `core/src/test/java/io/questdb/test/cairo/crash/AdaptiveMatViewCrashSweepTest.java`.

- [ ] **Step 1 (RED):** Workload: a base adaptive table `base(ts, v)` + a materialized view `mv` over it (a simple aggregation, e.g. `select ts, count() from base sample by 1h` or the simplest mat-view the codebase supports on a WAL table — read an existing `MatViewTest` for the exact DDL). `commit()` inserts into `base` and drains so the view refreshes. `setup` returns the tokens recovery must scan (base + the view's own table token). Oracle: after recovery at each k, **base and view are mutually consistent** — the view's refreshed contents must not reference base rows that recovery rewound away (no view row whose source `base` rows are gone), and the view is not left suspended. This is the explicit open risk from `adaptive-commit-mode-design.md:244`.
- [ ] **Step 2:** Run. GREEN → mat-view + adaptive epoch/recovery compose safely. RED → a real mat-view recovery consistency bug (a GA-relevant finding — mat-views were the flagged risk) → root-cause + report. **If the mat-view refresh machinery makes the sweep intractable** (e.g. refresh runs on a background thread breaking count-determinism, or the view can't be driven synchronously), STOP, document precisely why, and recommend splitting this to its own increment D1.5 rather than weakening the oracle.
- [ ] **Step 3:** Negative control.
- [ ] **Step 4 (commit):** `test(crash): adaptive mat-view crash sweep (SP-D W4)` (or the D1.5 hand-off note).

---

## Self-Review

**Coverage:** W1–W4 close exactly the four schema gaps the SP-D spec named (§Gaps: O3, indexed-symbol, multi-table, mat-view through the adaptive cycle). W0 baseline is the already-green self-check. **Watch-items:** review item (i) is Task W2 Step 3; item (ii) is a global-constraint oracle clause (monotonic floor) every task asserts. **No placeholders:** each task gives the schema, the oracle additions, and the run/root-cause/report loop; the intricate crash+recover+oracle plumbing is inherited from the proven driver + the self-check template (pointed to explicitly), so tasks are "new workload + schema-specific oracle," not fabricated crash code. **Type consistency:** every workload implements the same `AdaptiveCrashWorkload` interface and drives the same `forEachAdaptiveCrashPoint`.
