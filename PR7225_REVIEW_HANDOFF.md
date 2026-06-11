# PR #7225 — Level-3 Review Handoff

> Working document, NOT part of the PR. Untracked / do not commit. Author-facing
> summary of the blocking review of `fix(sql): fix wrong results in splice, right
> and full outer joins with a left-table filter`.

## Identity

- PR: **#7225**, branch `puzpuzpuz_splice_join_filter_bug`, state OPEN.
- HEAD reviewed: **840ee148f5** ("Fix filter leaks past reordering outer joins").
- Merge-base: `3cc1c862374ba150c11a166e7eae1a3b9eecb662`.
- Labels: `Bug`, `SQL`, `DO NOT MERGE`.
- Diff: 11 files, +1387 / −169. Production: `SqlOptimiser.java` (+371),
  `EmptySymbolMapReader.java`, `EmptyTableRandomRecordCursor.java`, `QueryModel.java`.
- Review: `/review-pr 7225 --level=3` on 2026-06-11. All 11 agent mandates (6
  parallel dispatches: correctness, cross-context, tests, fresh-adversarial,
  adversarial-perf, concurrency+resource+quality) plus own empirical reproduction.

## Verdict: REQUEST CHANGES / needs a scope decision

The core machinery is sound, the six prior leaks in this family are genuinely
fixed, and **no new regression, race, leak, or performance problem was introduced
by this PR**. Open items:

1. **F1 (headline)** — a confirmed, empirically-reproduced wrong-result in
   RIGHT/FULL OUTER joins from a two-table master-side equality. Pre-existing and
   outside the PR's *literal* single-table scope, but the same family and
   uncovered. Needs a fix-or-scope-out decision.
2. **F2** — the cyclic-join `!isNullingExecOrderValid` fallback (commit
   1cf0726840) has zero tests.
3. **F3** — the `keyOf(null)` change is an observable, untested behavior change
   beyond the optimiser fix.

`DO NOT MERGE` is already set, consistent with these open items.

---

## F1 (HEADLINE) — `WHERE a.x = b.y` (two-table equality) leaks past a downstream RIGHT/FULL OUTER join

- **Severity:** confirmed silent wrong-results. Pre-existing (NOT a regression of
  this PR), outside the PR's literal scope, in the same family, uncovered by tests.
- **Location (out-of-diff):** `core/src/main/java/io/questdb/griffin/SqlOptimiser.java:1748-1783`
  — the `analyseEquals` `case 1 / bSize == 1 / lhi != rhi` branch (two different
  tables, single-column equality). This branch is **byte-identical on merge-base**;
  last touched by PR #7150, not #7225.

### Root cause

The PR added the master-nulling guard to the single-table-const branches
(`analyseEquals` lines 1688, 1798) and the **same-table** `col = col` branch
(line 1730). It did **not** guard the **two-table** equality branch, which
unconditionally folds `a.x = b.y` into a join context via `addJoinContext`
(line 1776) whenever the higher-indexed referenced table is not itself a barrier.
When a *separate, later* RIGHT/FULL OUTER join NULL-extends that composite, the
equality was already applied as an inner-join key *before* null-extension, so the
NULL-master rows synthesized by the outer join bypass it. The predicate never
reaches `parsedWhere`, so `assignFilters` / `moveWhereInsideSubQueries` and their
exec-order anchors never see it.

This is provably distinct from what the PR fixes:
- single-table `col = const`, single-table `col = col`: guarded by the PR.
- cross-table **inequality** `a.x > b.y`: `functionCount > 0` →
  `parent.addParsedWhereNode` → `assignFilters` multi-table nulling-hold → correct
  (this is the existing `testMultiTableMasterFilterStaysPostJoin` `<` shape).
- cross-table **equality** `a.x = b.y`: uniquely triggers the join-context fold →
  **leaks**. `hasNonEquiNullingJoin` is false here (the RIGHT/FULL join is equi),
  so that path doesn't help either.

### Empirical reproduction (ran via JUnit probe under AbstractCairoTest)

```sql
CREATE TABLE t0 (a INT, k INT); INSERT INTO t0 VALUES (1, 1);
CREATE TABLE t1 (b INT, k INT); INSERT INTO t1 VALUES (5, 1);
CREATE TABLE t2 (k INT);        INSERT INTO t2 VALUES (1), (2);

SELECT t0.a, t1.b, t2.k
FROM t0 JOIN t1 ON t0.k = t1.k
RIGHT OUTER JOIN t2 ON t2.k = t1.k
WHERE t0.a = t1.b;
```

- **Actual (HEAD): 2 rows** — `(null,null,1)`, `(null,null,2)`.
- **SQL-correct: 0 rows** — full join yields `(1,5,1)` and `(null,null,2)`;
  `WHERE t0.a=t1.b` drops `(1,5,1)` (`1=5` false) and `(null,null,2)`
  (`null=null` unknown).
- **FULL OUTER JOIN leaks identically.**
- **literal == bind** (no constant) — the PR's literal-vs-bind fuzzer invariant
  CANNOT catch this; only an oracle (the wrapped/held form) does. This is why the
  fuzzer never surfaced it.

Plan (confirms the fold — no post-join `Filter` node):

```
SelectedRecord
    Hash Right Outer Join Light
        Hash Join Light          <- t0.a = t1.b folded into inner-join keys
            PageFrame ... t0
            PageFrame ... t1
        PageFrame ... t2          <- null-master rows leak, never filtered
```

Found independently by 3 review agents (correctness, fresh-adversarial, tests)
and confirmed by the probe.

### Decision required (yours)

The PR body honestly scopes itself to "a filter that referenced **only the master
table**" (single table), so the body is technically accurate. But the
title/release-note ("fix wrong results in … right and full outer joins") will read
to users as "RIGHT/FULL are now correct for master-side filters."

**Option 1 — fix it here (recommended; machinery is present).**
In the `lhi != rhi` branch, before `addJoinContext`/`linkDependencies`
(SqlOptimiser.java:1771-1780), when

```
joinIndex < 0
  && (masterNullingJoinIndex(lhi) >= 0
      || masterNullingJoinIndex(rhi) >= 0
      || hasNonEquiNullingJoin)
```

route the node to `parent.addParsedWhereNode(node, innerPredicate)` instead. The
tables are still joined by their own `ON` clause; `assignFilters`' multi-table
branch then anchors the equality post-join via `lastNullingJoinAfterReferencedTables`.
- The guard only fires when a nulling join is downstream, so the common
  equi-predicate-into-join-key optimization is unaffected (no perf regression in
  the common case).
- Risk: may churn a few `SqlParserTest` model assertions (same as the col=col
  guard did — e.g. testJoinReorder3 family). Re-run SqlParserTest / SqlOptimiserTest
  / JoinTest / ExplainPlanTest / SqlCompilerImplTest after.
- Must add a regression test mirroring the repro (RIGHT + FULL, literal + bind
  parity, `withPlanContaining("Filter filter: ...")` to assert post-join, and the
  0-row result). Prove non-tautological by git-stashing the prod change.
- When `t0`/`t1` are connected ONLY by `a = b` (no other `ON`), deferring makes
  them a CROSS + post-filter (correct but potentially cartesian) — only when a
  nulling join is downstream. Consistent with the PR's correctness-over-cheaper-plan
  stance; mention in the Tradeoff section.

**Option 2 — scope it out explicitly.**
Add a sentence to the body's follow-up paragraph (next to the strength-reduction
note) that a two-table-equality master-side predicate is a known remaining gap,
and add an `@Ignore`d / known-fail regression test so it is not silently lost.

---

## F2 (Moderate) — cyclic-join `!isNullingExecOrderValid` fallback is untested

- Commit **1cf0726840** added `isNullingExecOrderValid ?
  lastNullingJoinAfterOrderedPosition(i) : masterNullingJoinIndex(maxRef)` in
  `assignFilters`, plus the model-order fallbacks in `masterNullingJoinIndexInOrder`
  and `lastNullingJoinAfterReferencedTables`.
- `git show 1cf0726840 --stat`: **only `SqlOptimiser.java`, zero test files.**
- No test builds a cyclic join graph that makes `ordered.size() != n`
  (`CyclicalAstTest` is about expression ASTs, not joins).
- Under `-ea` the trailing `assert postFilterRemoved.size() == pc` masks a wrong
  anchor; a production build (no `-ea`) would silently anchor at a garbage model
  index. The guard itself was verified correct in prior passes — this is a
  coverage gap, not a known bug.
- **Action:** add a cyclic-join regression test, or document the branch as
  deliberately uncovered.

## F3 (Moderate) — `keyOf(null)` change is an observable, untested behavior change

- `EmptySymbolMapReader.keyOf(null)` now returns `VALUE_IS_NULL` (was
  `VALUE_NOT_FOUND`). Reachable beyond the post-join SPLICE path the body
  describes:
  - `ExtraNullColumnRecordCursor.getSymbolTable`/`newSymbolTable` return
    `EmptySymbolMapReader.INSTANCE` for always-NULL projected symbol columns
    (window/horizon-join projections); `ExtraNullColumnRecord.getInt` returns
    `Numbers.INT_NULL` (= `VALUE_IS_NULL`).
  - `InSymbolVarcharArrayFunctionFactory.init` calls `keyOf(null)` for a NULL
    IN-list element.
  - So `<nullSymCol> IN (NULL)` now **matches** where it previously did not.
- This is a **correctness improvement** (aligns the empty reader with
  `SymbolMapReaderImpl`; matches QuestDB's `null = null` is TRUE convention). Full
  inventory of 47 `keyOf` callers shows no caller breaks.
- **Action:** add a regression test for `<symbol> IN (NULL)` on a
  null-extended/empty symbol column. Optionally widen the body's "Secondary fixes"
  note to mention this `IN (NULL)` behavior, not just the SPLICE round-trip.

---

## Minor

- **Error-position regression for invalid queries.** `precomputeHasNonEquiNullingJoin`
  runs `criteriaHasCrossTableEquality` → `traverseNamesAndIndices` on RIGHT/FULL
  ON-criteria *before* `processJoinConditions` processes the master WHERE. For an
  already-invalid query with bad columns in both, the reported `SqlException` may
  point at the ON-clause column instead of the WHERE column. No effect on valid
  queries.
- **`criteriaHasCrossTableEquality` double-traverses ON `=` nodes** vs. the
  immediately-following `processJoinConditions` (both call `traverseNamesAndIndices`
  on the same nodes). Compile-path CPU only, no allocation, bounded by
  ON-size × RIGHT/FULL-join-count, both sides early-exit. Defensible to leave.
- **`testColumnEqColumnReorderedFilterStaysPostJoinSymbol` omits `.noRandomAccess()`**
  while its INT sibling includes it. Defensible (the symbol variant adds `ORDER BY`,
  so the top factory is a sort that supports random access). Worth a one-line note
  for consistency.
- **Label:** the documented index/interval-scan + parallel + JIT loss is a real
  perf tradeoff; consider adding a `Performance` or `regression` label so it is
  discoverable.

---

## Verified clean / downgraded (do NOT re-tread)

- **SPLICE context-less reorder leak — DISPROVEN empirically (probe passed).**
  Hypothesis: since `precomputeHasNonEquiNullingJoin` omits SPLICE and
  `homogenizeCrossJoins` never converts SPLICE, a context-less/no-ON SPLICE
  (`a SPLICE JOIN b JOIN c ON c.k=a.k WHERE c.cv=1`) would be appended last by
  `doReorderTables` and null-extend a higher-index table. FALSE:
  `createImpliedDependencies` gives EVERY SPLICE a `JoinContext parents={0}` plus a
  dependency `0 -> spliceIdx`, so it is never a dependency-free cross; it always
  executes right after table 0 (`IntSortedList` polls lowest first) and only nulls
  its own lower-index master, which model-order `masterNullingJoinIndex` catches.
  **Omitting SPLICE from `precomputeHasNonEquiNullingJoin` is CORRECT.**
- **Concurrency race on the new instance fields** — FALSE. `SqlOptimiser` is one
  per `SqlCompilerImpl`, checked out from `SqlCompilerPool` via CAS; no concurrent
  access. Re-entrancy safe: anchors are read before the nested-recursion loop in
  both `optimiseJoins` and `moveWhereInsideSubQueries`; each nested level re-runs
  its own precompute.
- **Resource leak / double-free of `EmptySymbolMapReader.INSTANCE`** — FALSE. The
  `SymbolMapReader`/`SymbolTable` hierarchy is not `Closeable`;
  `Misc.freeIfCloseable` is a no-op on it. The deferred `contextPool.next()` in the
  hold-branch is mass-released by `clear()`.
- **Algorithmic / zero-GC** — FALSE positive of any regression. Net improvement
  (per-predicate O(P·J) scans → O(J) precompute + O(1) lookups). New `IntList`
  fields reuse backing arrays via `setAll`/`clear`; no `java.util.*`, autoboxing,
  capturing lambdas, or string concat on the compile path; no data-path work added.
- **Cross-context callers** of `keyOf` / `newSymbolTable` / `parseWhereClause` /
  `clearConstNameToMaps` — all SAFE; `clearForUnionModelInJoin` fully removed with
  zero dangling references.
- **Code quality** — member ordering correct (alphabetical within visibility
  groups), boolean naming correct (`is`/`has`), no banner comments, no non-ASCII in
  the diff, removed dead `innerPredicate` re-writes confirmed redundant, all three
  new `IntList` fields are read.
- **The six prior leak fixes** (LEAK-A/B, C1, C2, CRIT-1, CRIT-2) — re-verified
  sound.

---

## PR metadata

- Title: OK — Conventional Commits, repeats the verb, end-user-facing.
- Body: OK — level-headed; discloses the index-scan/parallel/JIT-loss tradeoff
  prominently and accurately scopes to single-master-table filters. If F1 is scoped
  out, add the two-table-equality gap to the follow-up paragraph.
- Labels: `Bug` + `SQL` + `DO NOT MERGE` (iterating). Consider
  `Performance`/`regression`. No `Fixes #NNN` — correct (fuzzer-found, no issue).

---

## Resolution table

| ID | Item | Severity | In/Out diff | Status |
|----|------|----------|-------------|--------|
| F1 | Two-table `WHERE a.x=b.y` leaks past RIGHT/FULL OUTER | Confirmed wrong results | Out-of-diff (pre-existing, same family) | OPEN — fix or scope-out + test |
| F2 | Cyclic-join `!isNullingExecOrderValid` fallback untested | Moderate (coverage) | In-diff (1cf0726840) | OPEN — add test or document |
| F3 | `keyOf(null)` `IN (NULL)` behavior change untested | Moderate (coverage + scope note) | In-diff | OPEN — add test |
| m1 | Error position may point at ON col for invalid queries | Minor | In-diff | OPEN — accept or fix |
| m2 | `criteriaHasCrossTableEquality` double-traverses ON `=` | Minor (perf, bounded, no alloc) | In-diff | Accept (defensible) |
| m3 | `...PostJoinSymbol` test omits `.noRandomAccess()` | Minor | In-diff (test) | Accept (defensible) or align |
| m4 | Missing `Performance`/`regression` label | Minor (process) | n/a | OPEN — your call |
| — | SPLICE context-less reorder leak | — | — | DISPROVEN (probe) |
| — | Concurrency / resource / perf / cross-context | — | — | VERIFIED CLEAN |

---

## Appendix — F1 reproduction recipe (for whoever fixes/tests it)

A scratch JUnit probe (since removed; tree is clean) under
`AbstractCairoTest` (e.g. in `JoinTest`):

```java
execute("CREATE TABLE t0 (a INT, k INT)"); execute("INSERT INTO t0 VALUES (1, 1)");
execute("CREATE TABLE t1 (b INT, k INT)"); execute("INSERT INTO t1 VALUES (5, 1)");
execute("CREATE TABLE t2 (k INT)");        execute("INSERT INTO t2 VALUES (1), (2)");
// buggy: returns 2 rows; SQL-correct is 0 (empty, header only):
sink.clear();
printSql("SELECT t0.a, t1.b, t2.k FROM t0 JOIN t1 ON t0.k = t1.k "
        + "RIGHT OUTER JOIN t2 ON t2.k = t1.k WHERE t0.a = t1.b", sink);
Assert.assertEquals("a\tb\tk\n", sink.toString()); // fails on HEAD: "...\nnull\tnull\t2\nnull\tnull\t1\n"
```

Run a single method: `mvn -Dtest="JoinTest#methodName" test -pl core -P local-client`
(System.out lands in `core/target/surefire-reports/...JoinTest.txt`; logs interleave
on the console). FULL OUTER leaks the same way.

## Appendix — the six prior leaks (family context)

All fixed and re-verified sound. Same root cause: a master-side predicate that
`analyseEquals` pushes/folds *before* a NULL-extending join runs, leaking NULL-master
rows. F1 is the seventh instance and the last known hole in the family.

- LEAK-A / LEAK-B (ebc75ec94c) — `moveWhereInsideSubQueries` multi-table and
  barrier branches bypassed the guard for wrapped queries.
- C1 (bef203ce95) — keyed-SPLICE transitive slave-const push not result-neutral;
  `addTransitiveFilters` now skips SPLICE.
- C2 (9ffdc2feef) — non-equi RIGHT/FULL that homogenizes to CROSS and reorders
  last; `hasNonEquiNullingJoin` + `criteriaHasCrossTableEquality` defer the
  col=const predicate.
- CRIT-1 (840ee148f5) — col=col single-table branch missing `hasNonEquiNullingJoin`.
- CRIT-2 (840ee148f5) — `criteriaHasCrossTableEquality` false-positive for
  forward/earlier-ref equi ON; now requires `max(aIdx,bIdx) == joinIndex`.
- M1 (1cf0726840) — exec-order anchor read on a cyclic graph (guarded by
  `isNullingExecOrderValid`). See F2 — still untested.
