# Row expiry: accepted limitations

This file lists behaviors of `EXPIRE ROWS` that are deliberate. Each one was
raised in review, weighed, and accepted as it stands. A review should not report
one of them again unless the change under review alters that behavior, or it
brings evidence that the reasoning below no longer holds.

This file is for reviewers and maintainers. The public documentation covers only
what a user runs into and has to act on. `row-expiry-latest-by.md` records the
separate decision to scope the latest-by rewrite to scalar expiry.

Each entry states the behavior, why it is accepted, the tests that pin it, and
the follow-up work, if any.

## Query surface

### Time-frame joins and `LATEST BY` over a view whose policy is not a timestamp cutoff

A policy that is a single comparison between the designated timestamp and a
value `SqlParser.isTimestampFlippablePredicate` proves non-NULL, such as a
constant, `now()` or `dateadd()` over them (`WHEN ts < dateadd('d', -30, now())`,
`WHEN ts < '2020-01-01'`), is a timestamp cutoff. The parser flips it into an
interval, and the view reads like a table. Arithmetic on the clock
(`now() - 3_600_000_000L`) does not qualify; see "Clock arithmetic thresholds do
not prune". Every other policy reads the view through a filtered sub-query,
which these queries do not accept:

| Query over the view | Timestamp cutoff | Any other policy |
| --- | --- | --- |
| Right-hand side of WINDOW JOIN or HORIZON JOIN | works | rejected |
| Left-hand side of either join | works | works |
| `LATEST ON` result fed to SAMPLE BY, ASOF JOIN or LT JOIN | works | works under a value `WHEN`, rejected under `KEEP` and window policies |
| Deprecated `LATEST BY` result fed to the same | rejected | rejected |

- **Why accepted:** every case fails at compile time; no query returns wrong
  rows. The joins require a right-hand side that provides time frames, and no
  row-filtered read does, including a filtered sub-query written by hand over a
  plain table. The join errors name the view
  (`SqlCodeGenerator.unsupportedTimeFrameSlave()`). The `LATEST ON` and
  `LATEST BY` cases keep the generic
  `TIMESTAMP column is required but not provided`.
- **Pinned by:**
  `MatViewExpireRowsTest.testTimeFrameJoinOverPoliciedViewNamesTheView`,
  `MatViewExpireRowsTest.testRelativePoliciedViewRejectsTimestampOperatorAboveLatestOn`.
- **Follow-up:** time-frame support for a row-filtered right-hand side, and
  carrying the designated timestamp through `LATEST BY` and `LATEST ON` over a
  sub-query.

### Passthrough views keep the columns they were created with

CREATE stores a passthrough view's query with its top-level `SELECT *` expanded
into the base table's columns at that moment, each name double-quoted, for
example `select "sym", "price", "ts" from base_price`. A query written without
`SELECT`, such as `base_price WHERE price > 0`, is stored with
`select <columns> from` in front. A column the base table gains
later, through `ALTER TABLE ... ADD COLUMN` or a new ILP field, stays out of the
view. The view keeps refreshing its original columns and stays valid. Picking
up the new column takes a drop and re-create.

- **Why accepted:** the view's table schema is fixed at CREATE. Following an
  added base column means changing that schema as part of refresh. Dropping or
  renaming a base column the view reads still invalidates the view, as for any
  materialized view. The public docs describe this, because ILP users hit it.
- **Pinned by:** `MatViewTest.testPassthroughSelectStarStoresExpandedColumns`,
  `MatViewTest.testPassthroughSelectStarKeepsColumnsWhenBaseGainsColumn`,
  `MatViewTest.testPassthroughWildcardSpellingsStoreExpandedColumns`,
  `MatViewTest.testPassthroughShowCreateRoundTrips`,
  `MatViewTest.testBareFunctionNameColumnLossInvalidatesView`.
- **Follow-up:** add new base columns to the view automatically.

A wildcard that repeats a column the select list also names, such as
`SELECT ts, * FROM base_price`, is rejected at CREATE with "could not expand the
wildcard of the materialized view query, list the columns explicitly". List the
columns, or alias the extra one: `SELECT *, ts AS ts2 FROM base_price` works.

- **Why accepted:** the expansion takes its names from the view's columns, and
  the view renames the repeated column (`ts1`), which the base table does not
  have. Using the source's names instead means tracking which table or
  sub-query each wildcard expands from. A view with a duplicated column is
  unusual.
- **Pinned by:** `MatViewTest.testPassthroughSelectStarRepeatingColumnRejected`.

### Subqueries that read the base table in a view definition

A definition such as `WHERE v = (SELECT max(v) FROM base)` passes CREATE but
fails refresh when the outer query also reads `base`.

- **Why accepted:** the defect predates this feature and affects `SAMPLE BY`
  views the same way; passthrough views only add exposure to it. Removing the
  NPE alone is not enough: a diagnostic patch showed wrong results in chunked
  FULL refresh and stale rows in incremental refresh after a new global maximum.
- **Follow-up:** [#7614](https://github.com/questdb/questdb/issues/7614), for
  both view kinds.

### `LIMIT` in a view definition is rejected

CREATE rejects `LIMIT` in any branch that reads the base table, nested queries
included, for aggregating and passthrough views. A `LIMIT` in a subquery over
another table is still allowed. Existing views keep refreshing; re-creating one,
including replaying `SHOW CREATE MATERIALIZED VIEW` output, requires removing
the limit.

- **Why accepted:** incremental refresh runs the query over each changed time
  range, so a global row cap cannot hold.
- **Pinned by:** `MatViewRefreshRecompileCompatibilityTest`.

### Dependent views

CREATE of a materialized or live view that reads a view with an active policy
is rejected, including through joins and subqueries. `SET EXPIRE` is allowed
while dependents exist: each dependent becomes invalid when it next refreshes,
not at the `ALTER`, and keeps the rows it already stored. `DROP EXPIRE` does not
restore an invalidated dependent: a materialized view needs a FULL refresh, a
live view needs re-creation.

- **Why accepted:** a dependent cannot copy a row set that the policy keeps
  changing. Detecting the conflict at the dependent's next refresh needs no
  coordination between the `ALTER` and every dependent; an idle dependent can
  report itself active until then.
- **Pinned by:** `LiveViewMatViewBaseTest`,
  `MatViewExpireRowsHardeningTest.testCreateDependentDuringExpiryMetaSwapRejected`.

### Aggregating views accept a policy

A `SAMPLE BY` view accepts a policy with a define-time advisory; only
passthrough views are the intended target.

- **Why accepted:** a later refresh can regenerate rows that cleanup reclaimed,
  so base-table retention has to be aligned by the user. The advisory says so.
- **Pinned by:** `MatViewExpireRowsTest.testCreateAggregatingMatViewWithExpireAllowed`.

## Semantics

### NULL rows follow the predicate's truth value

A row expires only when the predicate is `TRUE` for it. `v < 2.0` is false for a
NULL `v`, so the row stays; `NOT (v >= 2.0)`, `v != 2.0` and `v IS NULL` are true
for it, so the row expires. `KEEP HIGHEST/LOWEST` keeps NULL rows. Under
`KEEP N`, where a NULL ranks depends on the column type.

- **Why accepted:** these are QuestDB's comparison semantics, and QuestDB has no
  `NULLS LAST`. Rewriting predicates to treat NULL uniformly would make a policy
  mean something other than its text.
- **Pinned by:** `MatViewExpireRowsTest.testExpireScalarNullRowsFollowPredicateTruthValue`.

### Cleanup classification is conservative

`SqlCompilerImpl.validateExpiryPredicateOnMetadata` reclaims only under a
predicate it can prove safe, and classifies every other policy as
`FILTER_ONLY`, so cleanup leaves its expired rows on disk. Two shapes count as
proven:

- a row-only predicate: every AST node is a column of the view, a constant, or
  an operator or function on the `EXPIRY_ROW_ONLY_FUNCTIONS` list (comparisons,
  arithmetic, `AND`/`OR`/`NOT`, `IN`, `BETWEEN`, `LIKE`, `~`, casts and a set of
  pure scalar functions), with no date variable in its string constants;
- a proven advancing-clock threshold such as `ts < dateadd('d', -30, now())`.

Everything else filters only, for example:

- a clock read through a date variable (`$now`, `$today`, `$yesterday`,
  `$tomorrow`), including a string literal that only looks like one
  (`owner = '$now'`);
- a session value (`current_user()`, `session_user()`), wherever it appears,
  including inside an `IN` list or a regex pattern;
- any function not on the list, including pure ones such as `sqrt()`;
- an `IN` whose values are not all constants, for example
  `ts IN concat('$', 'today')` or `ts IN s`: an `IN` over the timestamp expands
  date variables in its string values at runtime, so a computed or column value
  can read the clock;
- a non-monotonic threshold, for example `ts > now()`.

The proof reads the AST because a function's `isNonDeterministic()`,
`isRuntimeConstant()` and `isRandom()` are reliable only when they return true,
and some functions keep an operand where the bound function tree does not
expose it.

- **Why accepted:** cleanup evaluates the predicate once per sweep, as the root
  user. Reclaiming under any of these would delete rows that later reads, or
  readers in other sessions, still show. Over-classifying costs disk; the read
  filter keeps results correct either way.
- **Pinned by:**
  `MatViewExpireRowsHardeningTest.testExpiryValidationReturnsReusableClassification`,
  `MatViewExpireRowsHardeningTest.testExpireEnforcementMatchesCleanupBehaviour`,
  `MatViewExpireRowsHardeningTest.testDateVariablePredicateCleanupSkippedAndRowsSurvive`,
  `MatViewExpireRowsHardeningTest.testSessionDependentPredicateCleanupSkippedAndRowsSurvive`,
  `MatViewExpireRowsHardeningTest.testSessionValueInListOrPatternCleanupSkippedAndRowsSurvive`,
  `MatViewExpireRowsHardeningTest.testComposedDateVariableCleanupSkippedAndRowsSurvive`,
  `MatViewExpireRowsTest.testReadFilterCorrectForNonMonotonicFuturePredicate`.

### Relative and window policies never reclaim disk

`KEEP LATEST`, `KEEP [N] HIGHEST/LOWEST` and window `WHEN` policies are always
`FILTER_ONLY`. A `KEEP LATEST` view holds a full copy of its base table unless
the view's own TTL bounds it.

- **Why accepted:** the kept set depends on the other rows in the view, so a
  refresh can promote an older row back into it, and cleanup cannot rebuild a
  row it has deleted.

## Performance

### Read cost of relative and window policies

These policies compute the kept set over the whole view on every read. A
caller's timestamp filter sits above the window; only an equality on every
`PARTITION BY` key is copied below it. On a 20M-row view, a one-day read under
`KEEP 2 HIGHEST ON v PARTITION BY k` took 5.0 s against 7.2 ms with no policy.

- **Why accepted:** narrowing by timestamp below the window would change which
  rows win, because a kept set depends on every row of its group across all
  time. The public docs state the cost.
- **Follow-up:** push `IN` lists, ranges and partially constrained composite
  keys below the window. They are excluded by predicate shape, not semantics.

### Clock arithmetic thresholds do not prune

`WHEN ts < now() - 3_600_000_000L` reads through a full scan with a per-row
filter, so the time-frame join limitation above applies to it as well.
`WHEN ts < dateadd('h', -1, now())` expresses the same window and reads as an
interval scan.

- **Why accepted:** the interval flip requires a threshold that is provably
  non-NULL, and arithmetic with a clock under it is not provable at DDL time.
  The policy still keeps the right rows and still reclaims disk. The public docs
  recommend `dateadd`.
- **Pinned by:** `MatViewExpireRowsTest.testClockArithmeticThresholdKeepsUnflippedFilter`.

### Fixed thresholds reclaim through the survivor scan

Cleanup drops or skips a whole partition by comparing its bounds to the
threshold only for clock-shaped thresholds, which bind as timestamps. A fixed
threshold, whether string (`ts < '2024-01-03'`) or numeric, binds as STRING or
LONG and reclaims through the survivor scan. The generation cache limits that
scan to once per partition per back-fill.
`cairo.mat.view.row.expiry.cleanup.min.expired.fraction` applies to clock-based
predicates only.

- **Why accepted:** a bounds threshold derived from a bare LONG would have to
  guess its time unit, which differs between TIMESTAMP and TIMESTAMP_NS columns,
  and a wrong guess feeds the partition wipe that skips the scan.

### Latest-by rewrite scoped to scalar expiry

See `row-expiry-latest-by.md`. On a hoisted scalar-expiry read, natural output
order is timestamp order rather than key-map insertion order, and the residual
predicate runs row by row, which can be slower than the unhoisted plan.

- **Pinned by:** `LatestByHoistEquivalenceTest`.

## Operations

### A downgrade can drop the policy

The policy lives in a trailing `_meta` section gated on minor version 3. A
downgrade to a binary without `EXPIRE ROWS`, followed by any DDL on a policied
view, rewrites `_meta` at minor version 2 and drops the policy. A later upgrade
reads "no policy" and stops hiding expired rows, with no error or log line.

- **Why accepted:** this is how the `_meta` minor-version mechanism treats any
  trailing section. It matters more here because the lost field is a retention
  control, so it needs an operator note wherever downgrades are documented.
