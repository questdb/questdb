# Row expiry: accepted limitations

This file lists behaviors of `EXPIRE ROWS` that are deliberate. Each one was
raised in review, weighed, and accepted as it stands. A review should not report
one of them again unless the change under review alters that behavior, or it
brings evidence that the reasoning below no longer holds.

This file is for reviewers and maintainers. The public documentation covers only
what a user runs into and has to act on.

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
not prune". Every other policy reads the view through a filtered sub-query: a
per-row filter under a value `WHEN`, and a `LATEST ON` or window-function
sub-query under `KEEP` and window `WHEN` policies. These queries accept that
sub-query only in part:

| Query over the view | Timestamp cutoff | Any other policy |
| --- | --- | --- |
| Right-hand side of WINDOW JOIN | works | rejected |
| Right-hand side of HORIZON JOIN | works | works under a value `WHEN`, rejected under `KEEP` and window policies |
| Left-hand side of either join | works | works |
| `LATEST ON` result fed to SAMPLE BY, ASOF JOIN or LT JOIN | works | works under a value `WHEN`, rejected under `KEEP` and window policies |
| Deprecated `LATEST BY` result fed to the same | rejected | rejected |

- **Why accepted:** every rejected case fails at compile time; no query returns
  wrong rows. Both joins require a right-hand side that provides time frames,
  and no row-filtered read does, including a filtered sub-query written by hand
  over a plain table. HORIZON JOIN takes a per-row filter off its right-hand
  side, reads the table's time frames, and applies the filter while it looks up
  the prevailing row (`SqlCodeGenerator.generateHorizonJoinFactory()` and
  `generateMultiHorizonJoinFactory()`). A `KEEP` or window `WHEN` policy decides
  each row against the other rows of the view, so its read has no per-row filter
  to take, and WINDOW JOIN takes no filter at all. The join errors name the view
  (`SqlCodeGenerator.unsupportedTimeFrameSlave()`): WINDOW JOIN's names the
  missing timestamp cutoff, and HORIZON JOIN's names the `KEEP` or window-function
  policy when the parser has recorded one on the model
  (`IQueryModel.isExpiryStructuralPolicy()`), and the missing cutoff otherwise.
  The `LATEST ON` and
  `LATEST BY` cases keep the generic
  `TIMESTAMP column is required but not provided`.
- **Pinned by:**
  `MatViewExpireRowsTest.testTimeFrameJoinOverPoliciedViewNamesTheView`,
  `MatViewExpireRowsTest.testRelativePoliciedViewRejectsTimestampOperatorAboveLatestOn`.
- **Follow-up:** time-frame support for a row-filtered right-hand side of WINDOW
  JOIN and a windowed right-hand side of either join, and carrying the
  designated timestamp through `LATEST BY` and `LATEST ON` over a sub-query.

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

### Window functions nested in an expression are rejected

CREATE checks the SELECT list of every query level it reaches through `FROM`
nesting, `JOIN` and `UNION`, including a subquery under `SAMPLE BY`. In each such
level that reads the base table, it rejects a window function wherever it sits in
a column's expression: as an operand, under a cast, as a function argument or in
a `CASE` branch. For example:
`SELECT ts, sum(rn) FROM (SELECT ts, row_number() OVER (ORDER BY ts) + 1 rn FROM base) SAMPLE BY 1h`.
`SqlParser.validateMatViewQuery()` walks each column's expression tree through
its operators and function arguments, and reports the position of the window
function. It does not check a subquery inside an expression, such as
`IN (SELECT ...)` or a scalar subquery. A window function that forms a
whole column was already rejected; one nested in an expression is the new case,
and earlier versions created such views. Only CREATE runs this check, so an
existing view keeps refreshing, with the wrong values described below.
Re-creating one, including replaying `SHOW CREATE MATERIALIZED VIEW` output,
requires removing the window function.

- **Why accepted:** incremental refresh runs the query over each changed time
  range, so the window sees only the rows in that range and its values differ
  from running the query over the whole table. Such a view reported itself
  valid while holding those wrong values.
- **Pinned by:**
  `CreateMatViewTest.testCreateMatViewWindowFunctionInSampleBySubquery`,
  `CreateMatViewTest.testCreateMatViewWindowFunctionNestedInExpression`,
  `CreateMatViewTest.testCreateMatViewWindowFunctions`.

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

### A window policy's subquery can lose its table

A window `WHEN` predicate can read another table through a subquery, for
example `WHEN v < max(v) OVER (PARTITION BY k) AND k IN (SELECT k FROM lookup)`.
`SET EXPIRE` checks the predicate against `lookup` as it is then, and nothing
records the dependency, so `DROP TABLE`, `RENAME TABLE`, `DROP COLUMN`,
`RENAME COLUMN` and `ALTER COLUMN TYPE` on `lookup` all succeed. Every read of
the view compiles the stored predicate again (`SqlParser.expandExpiringTable`),
so after such a change every read fails with the raw compile error, such as
`table does not exist [table=lookup]` or `Invalid column: k`. The error does not
name the view or its policy, and its position is an offset into the query that
`expandExpiringTable` builds around the stored predicate, not into the reader's
SQL. Refresh does not read the policy, so `materialized_views()` keeps
reporting the view as `valid` with no reason. Reads work again after
`DROP EXPIRE`, or once the table and columns the subquery names exist again.

The subquery resolves `lookup` by name on every read. A table created or
renamed to `lookup` later is the one the policy reads, whatever it holds, and
its rows decide which view rows expire. A type change that keeps the predicate
legal keeps working: SYMBOL to VARCHAR under `k IN (SELECT k FROM lookup)`
expires the same rows. One that does not, such as DOUBLE to VARCHAR under
`v > (SELECT min(x) FROM lookup)`, fails every read with
`cannot compare DOUBLE and VARCHAR`.

- **Why accepted:** a read that cannot apply the policy fails closed. Serving
  the rows without the policy would show rows the user configured to expire.
  SQL views behave the same way: a read of a view whose table was dropped fails
  with the raw error from its definition. Blocking the DDL needs a persisted
  dependency registry that table and column DDL, WAL apply and replicas all
  consult. Reporting the error at the view reference needs wrapping in
  `SqlOptimiser` and `SqlCodeGenerator`, where the wrap also sees errors that
  have nothing to do with the policy.
- **Pinned by:** `RowExpiryWindowSubqueryDependencyTest`.
- **Follow-up:** name the view and its policy in the error, positioned at the
  view reference, and have `materialized_views()` report the view as invalid
  with the reason, as `views()` does for a SQL view.

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
filter, so the WINDOW JOIN limitation above applies to it as well; HORIZON JOIN
accepts it. `WHEN ts < dateadd('h', -1, now())` expresses the same window and
reads as an interval scan.

- **Why accepted:** the interval flip requires a threshold that is provably
  non-NULL, and arithmetic with a clock under it is not provable at DDL time.
  The policy still keeps the right rows and still reclaims disk. The public docs
  recommend `dateadd`.
- **Pinned by:** `MatViewExpireRowsTest.testClockArithmeticThresholdKeepsUnflippedFilter`,
  `MatViewExpireRowsTest.testTimeFrameJoinOverPoliciedViewNamesTheView`.

### Fixed thresholds reclaim through the survivor scan

Cleanup drops or skips a whole partition by comparing its bounds to the
threshold only for clock-shaped thresholds, which bind as timestamps. A fixed
threshold, whether string (`ts < '2024-01-03'`) or numeric, binds as STRING or
LONG and reclaims through the survivor scan. The generation cache limits that
scan to once per partition per back-fill, for as many partitions as
`cairo.mat.view.row.expiry.cleanup.max.cached.partitions` allows across all
views (16384 by default). Each sweep scans the partitions past that number
again, and the first sweep after a restart scans every partition once, because
the cache lives in memory only.
`cairo.mat.view.row.expiry.cleanup.min.expired.fraction` applies to clock-based
predicates only.

- **Why accepted:** a bounds threshold derived from a bare LONG would have to
  guess its time unit, which differs between TIMESTAMP and TIMESTAMP_NS columns,
  and a wrong guess feeds the partition wipe that skips the scan.

### Latest-by rewrite scoped to scalar expiry

A scalar `EXPIRE ROWS WHEN` policy turns a reference to `mv` into
`SELECT * FROM mv WHERE <keep-filter>`, so a latest-by query over the view sees
a subquery instead of a physical table. The random-access
`LatestByLightRecordCursorFactory` returns its winning rows in key-map insertion
order, not timestamp order, and correctly advertises no designated timestamp.
The direct-table latest-by factories return rows in timestamp order.
`SqlOptimiser.pushLatestByToTableModel()` hoists the latest-by out of the
synthetic wrapper onto the physical read, so adding a scalar policy does not by
itself break `SAMPLE BY` or timestamp joins above a direct latest-by read that
was valid before. This holds for INT and other keys, non-indexed SYMBOL keys and
indexed SYMBOL keys alike.

The rewrite applies only to physical reads that `SqlParser.expandExpiringTable()`
introduces for a scalar policy. The parser marks such a read with
`isScalarExpiryRead`; it does not mark ordinary tables, ordinary view expansion,
materialized views without a policy, or the relative and window policy
expansions. An ordinary view or CTE that refers to a scalar-expiry view can
still contain a marked read, and the existing shape and semantic guards must
also accept the path to it. Mentioning an expiring view elsewhere in a join or
UNION does not opt other reads into the rewrite. The marker records where the
parser expanded the read, rather than looking up the current policy again
during optimisation, and `QueryModel.clear()` resets it when the model pool
reuses a model. It is separate from `isExpiryKeepFilter`, which protects
nullable predicates from Boolean inversion and excludes timestamp predicates
that the optimiser can turn into intervals. Those predicates still need the
rewrite.

The existing guards still reject rewrites that change timestamp choice,
projection, alias scope, filters, joins, limits or ordering. Live views stay
excluded, because direct latest-by reads their disk tier without the published
in-memory lead. Relative and window policies keep their existing restrictions
on timestamp-dependent operators above another latest-by.

On a hoisted read, `generateLatestByTableQuery()` consumes the residual
predicate, so the later filtering stage cannot install an async/JIT filter below
it. `LatestByAllFilteredRecordCursor`, for example, scans backward and calls
`filter.getBool(record)` row by row. A selective full-scan query over an
expiring view can therefore run slower than the same filtered subquery with the
hoist disabled, including a query that does not need timestamp-ordered output.
Index access, timestamp interval pruning and SYMBOL-key early termination still
apply, depending on the factory and predicate chosen; an index does not
guarantee an index-seek plan.

At revision `5204d37e82`, the PR discussion measured a broad hoist that applied
to every subquery: about 6 ms without the hoist against 15-18 ms with it for a
selective filtered query, and about 96 ms against 1-2 ms the other way for an
unfiltered indexed query. These measure the broad hoist, not the scoped rewrite,
and do not predict every expiry workload. See the
[performance review](https://github.com/questdb/questdb/pull/7263#issuecomment-5379388414)
and the earlier
[ordering correction](https://github.com/questdb/questdb/pull/7263#issuecomment-5354023131).

Scoping the rewrite to scalar-expiry reads keeps the plans of ordinary nested
table queries, ordinary views over tables, and nested reads of materialized
views without scalar expiry: a selective predicate there still uses async/JIT
filtering before `LatestBy light`. It also gives up the broad indexed-subquery
speedup and any extra timestamp-dependent query shapes that only the broad
hoist enabled. Direct-table latest-by is unchanged.

- **Why accepted:** the rewrite keeps the timestamp-order contract for
  scalar-expiry reads and limits the row-by-row cost to them. Advertising a
  timestamp on the unordered light output would permit wrong sampling, joins or
  ORDER BY elision. Disabling the hoist for all predicates gives up the
  contract, and restricting it to indexed keys makes query validity depend on an
  index again. The cost comes from the current implementation, not from expiry
  semantics. Widening the gate as a general performance optimisation would
  spread the cost to unrelated queries.
- **Pinned by:** `LatestByHoistEquivalenceTest` compares plans, rows,
  projections and timestamp metadata with the hoist on and off: ordinary shapes
  keep identical plans, and accepted scalar-expiry shapes change plans. Its
  selective-filter regression covers INT, indexed SYMBOL and composite keys,
  ordinary subqueries and SQL views, JIT on and off, NULL keys and filtering
  before latest-by, and its scope tests fail when the marker check is removed.
  It also covers model-pool reuse, a mixed ordinary and expiry UNION,
  materialized views without expiry, and installing and dropping a flippable
  timestamp policy. `MatViewExpireRowsTest` covers downstream SAMPLE BY and
  ASOF without an index. `LatestByTest`, `ExplainPlanTest` and the live-view
  read tests pin the ordinary plans, ordering and freshness. These assert plans
  and results, not wall-clock speed.
- **Follow-up:** an execution path that combines async/JIT residual filtering
  with latest-by and genuinely timestamp-ordered output, keeping interval and
  index restriction and SYMBOL early exits rather than a full scan. One
  candidate sorts only the winning rows before replay, which adds work
  proportional to the number of keys and is not automatically faster; another
  consumes filtered matches frame by frame in timestamp order. Filtering must
  come before choosing winners: if A's newest row fails the predicate and an
  older row passes, the answer contains the older row. NULL policy semantics,
  NULL keys, equal timestamps, timestamp precision, cancellation and
  native-memory ownership must carry over. Benchmark candidates against both
  existing paths across:
  - indexed and non-indexed SYMBOL, INT and composite keys, at low and high
    cardinality;
  - selective, broad, absent and non-JIT-compatible residual predicates;
  - timestamp intervals and indexed key restrictions, with and without
    residuals;
  - early termination near the newest rows, and missing keys that need a full
    scan;
  - JIT on and off, supported platforms, one and multiple workers, empty
    inputs;
  - plain latest-by, explicit ORDER BY, and downstream SAMPLE BY, ASOF, LT and
    SPLICE.

  Report regressions alongside improvements before widening the scope, and
  restore designated-timestamp metadata only after verifying the actual output
  order.

## Operations

### A downgrade can drop the policy

The policy lives in a trailing `_meta` section gated on minor version 3. A
downgrade to a binary without `EXPIRE ROWS`, followed by any DDL on a policied
view, rewrites `_meta` at minor version 2 and drops the policy. A later upgrade
reads "no policy" and stops hiding expired rows, with no error or log line.

- **Why accepted:** this is how the `_meta` minor-version mechanism treats any
  trailing section. It matters more here because the lost field is a retention
  control, so it needs an operator note wherever downgrades are documented.

### `REBASE WAL` drops a policy set by `ALTER`

`ALTER MATERIALIZED VIEW ... REBASE WAL` drops a policy that `SET EXPIRE` added,
with no error or log line. This holds for both forms, including the replica's
`REBASE WAL INTO`. After the rebase, reads show the rows the policy hid, cleanup
stops, and `SHOW CREATE MATERIALIZED VIEW` prints no policy. Refresh,
`REFRESH FULL` and a restart do not bring it back, and the next DDL on the view
rewrites `_meta` without it.

`WalUtils.cloneTableDirForRebase()` resets the metadata version in `_meta` to 0
and leaves the minor-version field as it was. That field carries a checksum of
the metadata version and the column count, so after the reset
`TableUtils.isMetaFormatAtLeast()` fails for every minor version. The reader, the
writer and `MetadataCache` then all read the policy section as absent. A policy
given at `CREATE` survives as long as no DDL has run on the view since, because
the view's metadata version is still 0.

- **Why accepted:** the defect predates `EXPIRE ROWS`, and it lives in rebase
  code that this feature does not change. The same sequence drops a TTL set by
  `ALTER`, along with every other setting gated on the minor version. A rebase is
  a recovery step that an admin runs on a suspended view, and only with
  `cairo.wal.apply.suspended.write.denied=true`. Running `SET EXPIRE` again after
  the rebase restores the policy: `TableWriter` writes a fresh minor-version
  field with every DDL. Wherever `REBASE WAL` is documented, it needs an operator
  note saying so.
- **Follow-up:** when `cloneTableDirForRebase()` resets the metadata version, it
  should rewrite the minor-version field with
  `TableUtils.calculateMetaFormatMinorVersionField(0, columnCount)`. That fixes
  TTL as well.

### WAL apply does not re-check a structural policy

When a user runs `SET EXPIRE` with KEEP LATEST, KEEP HIGHEST/LOWEST or a window
WHEN, the server compiles and opens the query a read of the view will run, and
rejects the policy if that fails. WAL apply re-compiles the stored `SET EXPIRE`
on the primary and on every replica, and skips that check. Every node therefore
stores the policy as the primary accepted it. When a replica runs a different
QuestDB version from the primary, for example during a rolling upgrade, and that
version cannot compile the generated read query, every read of the view on that
replica fails until `DROP EXPIRE`.

- **Why accepted:** the check borrows a compiler and a view reader and opens the
  view's files. At apply, any of these can fail under load for reasons that have
  nothing to do with the policy, and the check cannot tell such a failure from a
  bad policy. Treating it as a bad policy would discard a `SET EXPIRE` the user
  was told succeeded; treating it as fatal would suspend the view. The check
  already ran when the statement did, against the same view, whose column names
  and types cannot change before apply. On a node running the same version, a
  second run can only fail for reasons unrelated to the policy.
- **Pinned by:**
  `MatViewReaderPoolRetryTest.testSetExpireSurvivesViewReaderPoolExhaustionDuringApply`.

### A restart does not resume `REFRESH FULL` of an invalid view

`REFRESH FULL` of an invalid view keeps the view invalid until the rebuild
completes. The request lives only in memory: the refresh queue holds its task,
and `MatViewState` holds its owner. When a shutdown or crash interrupts the
rebuild, startup finds the view invalid, with the rows the rebuild committed
before it stopped, and does not resume it. `materialized_views()` reports the
view as invalid with its original reason, and it stays that way until the user
runs `REFRESH FULL` again. Startup does not rebuild invalid views on its own,
because most of them wait for the user after a base table drop, truncate or
schema change, and the persisted state of an interrupted rebuild looks the same
as theirs.

On a writable node, the request survives a second refresh worker. A full
refresh that loses the view lock leaves a handoff on the view, and the lock
holder's release enqueues the pending owner, including one the holder deferred
itself. A release on a read-only node claims the handoff and enqueues nothing,
and the owner stays on the view with no task queued to run it. A demote turns
the node read-only before its writer drain and live-view fence. When either one
refuses the demote, the node becomes writable again, and the owner runs only if
a later lock holder's release wakes it. When the demote completes, the request
is lost: the promote that follows rebuilds the view states from disk and
schedules no refresh for an invalid view. In both cases the view stays invalid
until a refresh runs, as after a restart, and `REFRESH FULL` recovers it.

- **Why accepted:** resuming the rebuild after a restart needs a durable "full
  refresh pending" flag in the view's persisted state, which is a format change
  to the `MAT_VIEW_INVALIDATE` WAL event and the `_mv.s` file. Persisting the
  view as valid while the rebuild runs would avoid the format change, but after
  a restart the view would report valid while it holds partial rows. The view
  reports itself invalid, so the state is visible, and `REFRESH FULL` repairs
  it.
- **Pinned by:**
  `MatViewFullRefreshRedeliveryTest.testFullRefreshInterruptedByShutdownStaysInvalidUntilRefreshFull`.
- **Follow-up:** persist the pending request with the view state, so that
  startup resumes the rebuild.

## Access control

### A failed `SET EXPIRE` can show values from rows the current policy hides

To check a KEEP LATEST, KEEP HIGHEST/LOWEST or window WHEN policy, `ALTER
MATERIALIZED VIEW ... SET EXPIRE ROWS` compiles and opens the query a read of the
view will run once the new policy replaces the current one. That query reads the
view without its current policy. A window WHEN predicate can hold a subquery, and
a subquery that names the view itself reads it without any policy, in the check
and in every later read. A subquery that reaches the view through a SQL view is
rejected, as `ALTER VIEW` rejects the SQL view that closes the same cycle from
the other side. Opening the check's cursor runs such a
subquery. When it fails, the error carries the reason, and the reason can include
a row value. In QuestDB Enterprise, with `WHEN secret < 20` in force, a user who
holds `SET TABLE PARAM` and SELECT on `k`, `v` and `sym`, but not on `secret`,
runs:

    ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN v < max(v) OVER (PARTITION BY k)
        AND k IN (SELECT k FROM mv WHERE sym = 'B' AND v::string = 12345)

The statement fails with

    invalid EXPIRE ROWS policy: inconvertible value: `3.0` [STRING -> INT]

where `3.0` is the `v` of a row that `secret < 20` hides. The current policy
stays in place. The same user cannot read the view at all, because every read
through `secret < 20` needs SELECT on `secret`.

- **Why accepted:** `SET TABLE PARAM` already lets the user run `DROP EXPIRE` and
  read every row. The error shows values only from columns the user holds SELECT
  on, because the check asks for SELECT on every column the new policy reads. It
  shows nothing about the current policy's own columns, which the check does not
  read. Reading the view through the current policy in the check would make
  replacing a policy require SELECT on the replaced policy's columns, which
  neither `DROP EXPIRE` nor a scalar `SET EXPIRE` requires. Unlike `DROP EXPIRE`,
  the failed statement leaves the current policy and the view unchanged.
- **Pinned by:** in QuestDB Enterprise,
  `EntRowExpiryAclTest.testAlterMatViewSetExpireOverPolicyRequiresSelectOnNewPolicyColumnsOnly`
  replaces a policy on a column the user cannot read, and
  `EntRowExpiryAclTest.testAlterMatViewSetExpireSubqueryRequiresSelectOnOtherViewPolicyColumns`
  shows that only the view itself is read without its policy.
- **Follow-up:** replacing the reason with a generic message for failures raised
  while the check opens its cursor would stop the values from showing, at the
  cost of the detail for every policy error found there.
