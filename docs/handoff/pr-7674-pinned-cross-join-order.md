# Hand-off: a pinned CROSS JOIN runs ahead of keyed joins

Status: open. PR #7674 (branch `fix/join-optimiser-lost-rows`) introduces this slowdown and does not
fix it; issue #7759 tracks the same plan on master. This note was written at 534ec1f1f6 for the agent
that picks up the fix. Delete this file in the commit that fixes the problem, and in any case before
PR #7674 merges: the PR is squash-merged, so the file would otherwise land on master.

Origin: review claim 4 on PR #7674. A `fix-pr` run proposed two fixes; independent reviewers
rejected both before any code changed (section 3). This note gives the problem, the root cause, both
rejected attempts with their counterexamples, and the proposed full fix with the tests that should
pin it.

## 1. Problem

Same rows, much slower than master:

```sql
CREATE TABLE a AS (SELECT x::INT k, x::INT x FROM long_sequence(100_000));
CREATE TABLE b AS (SELECT x::INT k FROM long_sequence(100));
CREATE TABLE c AS (SELECT x::INT y FROM long_sequence(1_000));
CREATE TABLE d AS (SELECT 50 k FROM long_sequence(1));

SELECT * FROM a CROSS JOIN c JOIN b ON a.k = b.k RIGHT JOIN d ON a.x >= d.k;
```

- Branch plan: `Hash Join Light b.k=a.k` over `Cross Join(a, c)`. The hash join probes |a| x |c|
  rows.
- Master plan: `Cross Join(Hash Join(a, b), c)`. The hash join probes |a| rows, and `c` joins only
  the rows that matched `b`.

The reviewer's timings, with 100,000 rows in `a`, 100 in `b` and 1 in `d`:

| rows in c | RIGHT JOIN, branch / master | FULL JOIN, branch / master |
|---|---|---|
| 10 | 41.7 / 4.2 ms | 24.2 / 4.6 ms |
| 100 | 117 / 5.0 ms | 115 / 6.2 ms |
| 1,000 | 901 / 5.7 ms | 888 / 17.6 ms |

The cost grows linearly with the rows of the CROSS-joined table.

Related shapes:

- The equi variant `... RIGHT JOIN d ON a.k = d.k` gets the same slow plan on the branch (about 1 s).
  Master runs it fast but returns wrong rows for it.
- Issue #7759: when another join follows the RIGHT/FULL join, master gets the slow plan too:
  `SELECT * FROM a CROSS JOIN c JOIN b ON a.k = b.k RIGHT JOIN d ON a.x >= d.k JOIN b b2 ON b2.k = d.k`
  took about 1.6 s on master and on the branch, against about 5 ms when the query names `c` after the
  keyed join.

The PR body lists the regression under "Tradeoffs" > "Slower or different plans".

## 2. Root cause

`optimiseJoins()` (SqlOptimiser.java:8549) runs these steps for each join level:

```
homogenizeCrossJoins(model);                      // 8606
constrainRightAndFullJoinsAfterPrefix(model);     // 8608
constrainJoinsAfterReorderedNullingJoins(model);  // 8610
constrainOuterJoinsAfterExpressionParents(model); // 8611
reorderTables(model);                             // 8613, calls doReorderTables()
validateNonEquiNullingJoinOrder(model);           // 8615
assignFilters(model);                             // 8617
```

- `constrainRightAndFullJoinsAfterPrefix()` (3714) pins every prefix model of a RIGHT/FULL join ahead
  of it with `recordOrderingConstraint(parent, prefixIndex, boundaryIndex)` (9838), which calls
  `addOrderingConstraint()` (1962). The RIGHT/FULL join gets the prefix model as a context parent,
  and the prefix model gets the RIGHT/FULL join as a dependency.
  `constrainJoinsAfterReorderedNullingJoins()` pins the same way through `recordNullingJoinPrefix()`
  (9830); that is the path master takes in #7759.
- `doReorderTables()` (4770) runs Kahn's algorithm. A model without context parents goes to
  `orderingStack` if it has dependencies, and to `tempCrossIndexes` if it has none. `orderingStack`
  is a priority queue keyed by model index, or by `joinModelsByPriority` when time-series joins are
  prioritised. `doReorderTables()` appends `tempCrossIndexes` after every other model (4829).
- Without the pin, the key-less CROSS model `c` has no dependency, lands in `tempCrossIndexes` and
  runs after the keyed joins: master's order a, b, c, d. With the pin, `c` has the dependency `d`, so
  it is a root in `orderingStack` with priority 1. When `a` (priority 0) leaves, `b` becomes ready
  with priority 2, and `c` wins: the order becomes a, c, b, d.

The pin is correct and must stay. Without it, `c` can run after the RIGHT/FULL join, which repeats
every NULL-extended row of `d` once per row of `c`. That is one of the wrong-result bugs PR #7674
fixes. What is wrong is the ordering policy: once a key-less CROSS model has an ordering edge,
nothing keeps it as late as possible any more.

## 3. What was tried

### Attempt 1: hard edges (rejected)

In `constrainRightAndFullJoinsAfterPrefix()`, add `recordOrderingConstraint(q, c)` from every pinned
keyed INNER prefix model `q` to the pinned CROSS model `c`.

A reviewer rejected it with two counterexamples:

1. **A cycle, so a new compile error.** Query:
   `a CROSS JOIN c LEFT JOIN L ON L.k = a.k AND L.k = c.y JOIN b ON b.k = L.k RIGHT JOIN d ON a.x >= d.k`.
   - `homogenizeCrossJoins()` makes the key-less model 0 (`a`) a JOIN_CROSS.
   - `mergeContexts` turns `a.k = c.y` into an outer-join expression and unlinks a -> L, so `a`
     qualifies as a pinned CROSS model.
   - The new edge b -> a, the edge a -> L that `constrainOuterJoinsAfterExpressionParents()` records
     later, and b's key edge L -> b form a cycle.
   - `reorderTables()` then fails with "could not determine join order". The branch orders this
     query today.
2. **A LEFT join reads `c` before `c` joins.** Query:
   `a CROSS JOIN c JOIN b ON a.k = b.k LEFT JOIN e ON e.k = a.k AND e.z > c.y JOIN f ON f.k = b.k RIGHT JOIN d ON a.x >= d.k`.
   - The non-equi conjunct `e.z > c.y` creates no c -> e edge: `addOuterJoinExpression()` (1974)
     links only `joinIndex - 1`, here b -> e.
   - With `c` held behind `b` and `f`, `e` runs before `c` and evaluates `c.y` before `c` joins.

Lesson: new hard edges interact with edges that later steps record, and with references that create
no edge.

### Attempt 2: hold one CROSS model in doReorderTables (incomplete, not committed)

No new edges. When `doReorderTables()` polls a key-less CROSS model, it holds it back while keyed
INNER joins of its own run of INNER/CROSS joins are ready. It releases the model before any other
model. The patch is in the appendix.

Validation at 3aa3dae092, with the patched SqlOptimiser on the classpath:

- Both counterexamples above keep the branch's plan and rows.
- 21,000 random join queries gave the same rows and errors as the branch, with 349-651 plan changes
  per run. They had 4-8 tables, CROSS/INNER/LEFT/RIGHT/FULL/ASOF/LT joins, ON conjuncts reading the
  CROSS table, and WHERE clauses.
- 4,050 tests passed: join, LATERAL, parser, explain-plan, optimiser, ASOF, compile-time, RIGHT/FULL
  fuzz and sqllogictest classes.
- `JoinReorderCompileTimeTest` compile times stayed the same: 42.6 / 34.0 ms, against 43.1 / 34.4 ms.
- The section 1 query took 5.1 ms instead of 969 ms (FULL: 10.4 instead of 997 ms; equi RIGHT: 4.2
  instead of 1026 ms). The #7759 shape got master's fast plan too.

A second reviewer found no breakage, but two shapes it misses:

1. **Two CROSS tables:** `a CROSS JOIN c1 CROSS JOIN c2 JOIN b ON a.k = b.k RIGHT JOIN d ON a.x >= d.k`.
   It holds `c1`, then polls `c2`, which is not a keyed INNER join, so it releases `c1` and holds
   `c2`. The order is a, c1, b, c2, so the hash join still probes |a| x |c1| rows. Master runs a, b,
   c1, c2.
2. **A LEFT join between:** `a CROSS JOIN c LEFT JOIN e ON e.k = a.k JOIN b ON a.k = b.k RIGHT JOIN d ON a.x >= d.k`.
   `crossJoinPassBound()` stops at the LEFT join, so `c` is never held. The order is a, c, e, b;
   master runs a, e, b, c.

It was not committed because it special-cases one held model inside one run of INNER/CROSS joins
instead of restoring the "as late as possible" placement.

## 4. Proposed fix: defer key-less CROSS roots

Change only which ready model `doReorderTables()` emits next. Add no edges.

### Definitions

- A **deferrable** model is a JOIN_CROSS model with no context parents and at least one dependency,
  other than the model the order starts with (see 5.3). Today these models go to `orderingStack` as
  roots. Key-less CROSS models without dependencies keep going to `tempCrossIndexes`.
- A ready model `m` **can pass** a deferred model `c` when running `c` after `m` cannot change the
  rows:
  - `m` is JOIN_INNER or JOIN_CROSS. A cross product commutes with an inner join. A key of `m` on `c`
    makes `c` a context parent of `m`, so `m` cannot be ready before `c`. A non-key conjunct of `m`
    that reads `c` should be a filter that `assignFilters()` anchors at the last referenced model in
    execution order (verify, 5.1).
  - `m` is a LEFT join (JOIN_LEFT_OUTER, JOIN_CROSS_LEFT) whose ON clause does not read `c`:
    `(a x c) LEFT JOIN e` equals `(a LEFT JOIN e) x c` when the condition of `e` does not reference
    `c`.
  - No other model can pass: RIGHT/FULL joins (equi or not), ASOF, LT, SPLICE, WINDOW, HORIZON,
    LATERAL and UNNEST joins, and any LEFT join whose ON clause reads `c` or has a name that does not
    resolve to one model.

### Loop

This replaces the `while (orderingStack.notEmpty())` loop:

```
deferred = priorities of the deferrable models, ascending (they no longer go to orderingStack)
while (orderingStack.notEmpty() || deferred.notEmpty()) {
    if (orderingStack is empty) {
        emit(deferred.pollLowest());         // nothing else is ready: release in query order
        continue;
    }
    m = orderingStack.peekLowest();
    c = lowest deferred c with priority(c) < priority(m) and !canPass(m, c);
    if (c exists) {
        emit(c);                             // m stays queued; models waiting on c may become ready
        continue;
    }
    emit(orderingStack.poll());
}
```

`emit()` is the existing loop body: `ordered.add(index)`, the cost update and the in-count
decrements that queue newly ready models. The `priority(c) < priority(m)` condition keeps today's
order wherever the current code would already emit `m` before `c`.

### Why it is safe

- **No cycles, same completeness.** The edges are the same as today, so no cycle can appear (attempt
  1's failure). Every model still leaves through `emit()`, and a deferred model is released as soon
  as nothing else is ready. The loop therefore emits exactly the models the current code emits, and
  the `Integer.MAX_VALUE` check after it stays valid.
- **Same root.** The cost (`+10` per JOIN_CROSS, `+5` otherwise, `tempCrossIndexes` free) depends on
  which models leave through the loop, not on their order, so `reorderTables()` keeps choosing the
  same root candidate.
- **The pin still holds.** `c` stays ahead of its RIGHT/FULL join, because the pin edge keeps the join
  from becoming ready until `c` leaves.
- **No early reads.** A model that reads `c` without an edge cannot pass it, which avoids attempt 1's
  counterexample 2.
- **Both gaps of attempt 2 close.** Every deferred model waits, not just one, and a LEFT join that
  does not read `c` can pass it.

### Expected orders with the fix

These are predictions for the tests to assert:

- `a CROSS JOIN c JOIN b ON a.k = b.k RIGHT JOIN d ON a.x >= d.k`: a, b, c, d (master: the same).
  The FULL and equi RIGHT variants give the same order.
- `a CROSS JOIN c1 CROSS JOIN c2 JOIN b ON a.k = b.k RIGHT JOIN d ON a.x >= d.k`: a, b, c1, c2, d.
- `a CROSS JOIN c LEFT JOIN e ON e.k = a.k JOIN b ON a.k = b.k RIGHT JOIN d ON a.x >= d.k`:
  a, e, b, c, d.
- Attempt 1, counterexample 2: a, b, c, e, f, d. `b` passes `c`, and `e` reads `c`, which releases it.
- Attempt 1, counterexample 1: compiles, with the branch's current order and rows.
- #7759: `... RIGHT JOIN d ON a.x >= d.k JOIN b b2 ON b2.k = d.k` gives a, b, c, d, b2.

## 5. Verify while implementing

1. **INNER join conjuncts.** Confirm that every non-key ON conjunct of an INNER join is a post-join
   filter at reorder time, not a filter inside that join. If some conjunct stays inside the join,
   for example as a hash-join filter, an INNER join that reads `c` must not pass it either.
2. **Where a LEFT join's ON clause lives at reorder time.** Keys sit in the JoinContext (edges),
   non-key conjuncts in `getOuterJoinExpressionClause()` (see `addOuterJoinExpression()`, 1974), and
   possibly in `getJoinCriteria()`.
   - Resolve the referenced models with `collectReferencedJoinModels()` (3384). When it returns false
     (an unresolved or ambiguous name), the join cannot pass anything.
   - `doReorderTables()` runs once per root candidate in `reorderTables()` (9961), so compute these
     sets once per level, before that loop.
   - Check whether `swapJoinOrder()` (15402) moves conjuncts into or out of outer-join expressions
     between candidates. If it does, recompute per candidate.
3. **Model 0.** `homogenizeCrossJoins()` can make a key-less model 0 a JOIN_CROSS (attempt 1,
   counterexample 1). Never defer the model the order starts with.
4. **Prioritised time-series joins.** With `joinModelsByPriority`, work on priorities throughout and
   release deferred models in ascending priority.
5. **`IntSortedList`.** `orderingStack` offers `add()`, `poll()`, `notEmpty()` and `size()` but no
   peek. Add `peek()`, or poll and re-add.
6. **Code that reads the order.** Check `validateNonEquiNullingJoinOrder()` (15966), `assignFilters()`
   (2672) with the nulling-join anchors, `lastReferencedModelInExecOrder()` (added by 62976b61ca) and
   `SqlCodeGenerator.generateJoins()`. A LEFT join can now run ahead of a CROSS model it used to
   follow; filters anchored after it must still see both.
7. **Compile time.** `JoinReorderCompileTimeTest` (241-table RIGHT/FULL chains) must stay flat.
   `canPass()` needs O(1) lookups into the precomputed sets; it must not resolve names per call.
8. **#7700.** Outer-join ON clauses that read CROSS-joined tables already fail with `Invalid column`,
   partly because `addOuterJoinExpression()` links only `joinIndex - 1`. The "reads c" rule must not
   make that failure more reachable.
9. **A parallel change to `reorderTables()`.** Branch `puzpuzpuz_parallel_fused_hash_join` (commit
   c4a195b4a3, "Keep timestamped first table first in joins") adds a post-pass to `reorderTables()`,
   `findJoinModelsToAnchor()` and `anchorJoinModels()`, that keeps a timestamped first table as the
   join driver. When this note was written, that commit was on neither master nor this branch. If it
   lands first, merge it in and check how the deferral interacts with that post-pass.

## 6. Tests

Add them to `JoinTest`, using `assertQuery(...)` with `.returns(...)` and `.withPlan(...)` per
CLAUDE.md. These small tables come from attempt 2:

```sql
CREATE TABLE a AS (SELECT x::INT k, (x * 3)::INT x FROM long_sequence(6));
CREATE TABLE b AS (SELECT (x * 2)::INT k FROM long_sequence(4));
CREATE TABLE c AS (SELECT x::INT y FROM long_sequence(3));
CREATE TABLE d AS (SELECT 7::INT k FROM long_sequence(1));
CREATE TABLE e AS (SELECT x::INT k, (x % 4)::INT z FROM long_sequence(6));
CREATE TABLE f AS (SELECT (x * 2)::INT k FROM long_sequence(3));
CREATE TABLE L AS (SELECT x::INT k FROM long_sequence(4));
```

In the rows below, `{1,2,3}` stands for one row per value of `c.y`.

- **The claim (red today).**
  `SELECT a.k, a.x, c.y, b.k bk, d.k dk FROM a CROSS JOIN c JOIN b ON a.k = b.k RIGHT JOIN d ON a.x >= d.k`.
  Plan: `Nested Loop Right Join` (filter `a.x>=d.k`) over `Cross Join`, whose master is
  `Hash Join Light condition: b.k=a.k` and whose slave is `c`. Rows: `4 12 {1,2,3} 4 7` and
  `6 18 {1,2,3} 6 7`.
- **FULL variant (red today).** `Nested Loop Full Join`, the same rows plus `2 6 {1,2,3} 2 null`.
- **Equi RIGHT variant (red today).** `RIGHT JOIN d ON a.k = d.k`: one row, `null null null null 7`.
- **Two CROSS tables** (`c c1`, `c c2`). The hash join must run on `a` alone, below both cross joins.
- **A LEFT join between:**
  `a CROSS JOIN c LEFT JOIN e ON e.k = a.k JOIN b ON a.k = b.k RIGHT JOIN d ON a.x >= d.k`. The cross
  join must sit above the LEFT and hash joins.
- **A LEFT join that reads `c` stays behind it (attempt 1, counterexample 2).**
  `SELECT a.k, c.y, b.k bk, e.k ek, e.z, f.k fk, d.k dk FROM a CROSS JOIN c JOIN b ON a.k = b.k LEFT JOIN e ON e.k = a.k AND e.z > c.y JOIN f ON f.k = b.k RIGHT JOIN d ON a.x >= d.k`.
  Plan: `Hash Left Outer Join Light` (filter `c.y<e.z`) over `Cross Join(Hash Join Light b.k=a.k, c)`.
  Rows: `4 {1,2,3} 4 null null 4 7`, `6 1 6 6 2 6 7`, `6 {2,3} 6 null null 6 7`.
- **No cycle (attempt 1, counterexample 1).**
  `SELECT a.k, c.y, L.k lk, b.k bk, d.k dk FROM a CROSS JOIN c LEFT JOIN L ON L.k = a.k AND L.k = c.y JOIN b ON b.k = L.k RIGHT JOIN d ON a.x >= d.k`
  compiles and returns `null null null null 7`.
- **#7759.** `... RIGHT JOIN d ON a.x >= d.k JOIN b b2 ON b2.k = d.k` gets
  `Cross Join(Hash Join(a, b), c)` below the outer join.
- **Time-series barrier.** A pinned CROSS model must not pass an ASOF/LT join. This needs a
  timestamped fixture.
- **Unresolved names.** A LEFT join whose ON clause has a name that does not resolve to one model
  must not pass a deferred CROSS model.

## 7. Validation and wrap-up

- **Test classes.** Run JoinTest, LateralJoinTest, LateralJoinNullRejectionTest,
  LateralJoinNullRejectionFuzzTest, LateralJoinSharedCursorTest, SqlParserTest, ExplainPlanTest,
  SqlOptimiserTest, AsOfJoinTest, HashJoinTest, WindowJoinTest, HorizonJoinTest,
  MarkoutHorizonCrossJoinTest, JoinReorderCompileTimeTest, RightFullJoinPrefixFuzzTest (several
  runs), JoinMemoryTrackerTest and the sqllogictest `SqlTest` (which includes
  `outer_join_implied_keys.test`).
- **Plan changes.** Review every changed plan in ExplainPlanTest and SqlParserTest. Plans should
  change only where a key-less CROSS model now runs later.
- **Fuzz.** Run a differential fuzz against the current branch: random chains of 4-8 tables with
  CROSS/INNER/LEFT/RIGHT/FULL/ASOF/LT joins, ON conjuncts that read the CROSS table, and WHERE
  clauses. Require identical rows and identical errors.
- **Timing.** Re-measure the section 1 query. Expect master's range: about 5 ms for RIGHT and 10-18
  ms for FULL.
- **PR #7674 body.**
  - Remove the CROSS JOIN item from "Tradeoffs" > "Slower or different plans".
  - Add a bullet under "Fixes".
  - Add `fixes #7759` to the first line and remove #7759 from "Not fixed here".
  - Remove the sentence that mentions this note.
- **This note.** Delete it in the same commit.

## Appendix: attempt 2 patch (reference only)

This patch was written for 3aa3dae092. `doReorderTables()` has not changed since. It shows how the
loop can hold a model back without new edges; the full fix generalises the held model to a set and
widens the pass rule.

The head of the `while (orderingStack.notEmpty())` loop in `doReorderTables()` becomes:

```java
        int heldPriority = -1;
        int passBound = -1;
        while (orderingStack.notEmpty() || heldPriority != -1) {
            int priority;
            if (orderingStack.notEmpty()) {
                // remove the node with the lowest priority from orderingStack
                priority = orderingStack.poll();
                final IQueryModel candidate = joinModels.getQuick(isPrioritised ? joinModelsByPriority.getQuick(priority) : priority);
                if (heldPriority == -1) {
                    if (candidate.getJoinType() == IQueryModel.JOIN_CROSS) {
                        final int bound = crossJoinPassBound(joinModels, candidate, priority, isPrioritised);
                        if (bound > priority + 1) {
                            heldPriority = priority;
                            passBound = bound;
                            continue;
                        }
                    }
                } else if (priority <= heldPriority || priority >= passBound || !isKeyedInnerJoin(candidate)) {
                    orderingStack.add(priority);
                    priority = heldPriority;
                    heldPriority = -1;
                }
            } else {
                priority = heldPriority;
                heldPriority = -1;
            }
            final int index = isPrioritised ? joinModelsByPriority.getQuick(priority) : priority;
            // ... unchanged from here (ordered.add, cost, dependency inCount decrement)
```

Helpers:

```java
    // Returns the priority from which a keyed INNER join can no longer run ahead of the CROSS join with
    // the given priority: the first model after it that is neither an INNER nor a CROSS join, or the
    // first model the CROSS join has an ordering edge to.
    private int crossJoinPassBound(ObjList<IQueryModel> joinModels, IQueryModel crossModel, int crossPriority, boolean isPrioritised) {
        int bound = crossPriority + 1;
        for (int n = joinModels.size(); bound < n; bound++) {
            final int joinType = joinModels.getQuick(isPrioritised ? joinModelsByPriority.getQuick(bound) : bound).getJoinType();
            if (joinType != IQueryModel.JOIN_INNER && joinType != IQueryModel.JOIN_CROSS) {
                break;
            }
        }
        final IntHashSet dependencies = crossModel.getDependencies();
        for (int i = 0, n = dependencies.size(); i < n; i++) {
            bound = Math.min(bound, getJoinModelPriority(dependencies.get(i)));
        }
        return bound;
    }

    private static boolean isKeyedInnerJoin(IQueryModel model) {
        return model.getJoinType() == IQueryModel.JOIN_INNER && model.getJoinContext() != null && model.getJoinContext().parents.size() > 0;
    }
```
