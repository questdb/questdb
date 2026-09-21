/*+*****************************************************************************
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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshSqlExecutionContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Upgrade-compatibility regression tests.
 * <p>
 * A rejected CREATE is annoying but visible. The severe failure mode is different: persisted view
 * SQL is recompiled under {@link MatViewRefreshSqlExecutionContext} on every cache miss and after
 * every restart, and a {@code SqlException} there routes to
 * {@code MatViewRefreshJob#refreshFailState}, which persists {@code invalid=true}, replicates it to
 * peers and cascades it to dependent views. Recovery needs a manual REFRESH ... FULL after
 * rewriting SQL that the guard still rejects.
 * <p>
 * These tests exercise exactly the refresh job's compile step, so a guard that over-rejects fails
 * here even when CREATE was accepted on the node that ran it.
 * <p>
 * The lateral-join cases pin the stored-definition contract of the lateral rewrite guards
 * (negative-LIMIT rejection and the narrowed scalar-count compensation): every lateral shape that
 * CREATE accepts must keep recompiling under the refresh context, and the shapes the guard rejects
 * must be rejected visibly at CREATE with a stable message, because a stored definition that stops
 * compiling after an upgrade is a deterministic view outage.
 * <p>
 * The external-source case pins the other half of that contract for a break this PR does intend:
 * a definition over {@code read_parquet()} persisted by an older binary must fail <em>closed</em>
 * at its first post-upgrade refresh — {@code invalid=true} with the same stable message CREATE
 * surfaces, base table untouched, view still droppable — rather than crash, leak, or retry-loop.
 */
@RunWith(Parameterized.class)
public class MatViewRefreshRecompileCompatibilityTest extends AbstractCairoTest {
    private static final String NEGATIVE_LIMIT_ERROR = "negative LIMIT is not supported in a correlated lateral sub-query";
    private final boolean parallel;

    public MatViewRefreshRecompileCompatibilityTest(boolean parallel) {
        this.parallel = parallel;
    }

    @Parameterized.Parameters(name = "parallel={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{true}, {false}});
    }

    @Test
    public void testCreateRejectsNegativeLimitInCrossLateral() throws Exception {
        assertCreateLateralRejected(
                "SELECT base.ts, sum(l.x) AS s FROM base "
                        + "CROSS JOIN LATERAL (SELECT dim.n x FROM dim WHERE dim.k = base.k LIMIT -1) l "
                        + "SAMPLE BY 1h",
                "-1",
                NEGATIVE_LIMIT_ERROR
        );
    }

    @Test
    public void testCreateRejectsNegativeLimitInInnerLateral() throws Exception {
        assertCreateLateralRejected(
                "SELECT base.ts, sum(l.x) AS s FROM base "
                        + "JOIN LATERAL (SELECT dim.n x FROM dim WHERE dim.k = base.k LIMIT -1) l ON true "
                        + "SAMPLE BY 1h",
                "-1",
                NEGATIVE_LIMIT_ERROR
        );
    }

    @Test
    public void testCreateRejectsNegativeLimitInLeftLateral() throws Exception {
        // The upgrade-visible break for this shape: a definition like this persisted by an older
        // binary stops compiling, so reads fail and the next refresh persists invalid=true.
        // Rejecting it at CREATE (with the same message the refresh would surface) is the visible
        // half of that contract.
        assertCreateLateralRejected(
                "SELECT base.ts, sum(l.x) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT dim.n x FROM dim WHERE dim.k = base.k LIMIT -1) l ON true "
                        + "SAMPLE BY 1h",
                "-1",
                NEGATIVE_LIMIT_ERROR
        );
    }

    @Test
    public void testCreateRejectsNegativeLimitOverScalarCount() throws Exception {
        // The scalar-count walk reports the provable negative with the specific message, ahead of
        // the generic scalar-count guard.
        assertCreateLateralRejected(
                "SELECT base.ts, sum(l.c) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT count() c FROM dim WHERE dim.k = base.k LIMIT -1) l ON true "
                        + "SAMPLE BY 1h",
                "-1",
                NEGATIVE_LIMIT_ERROR
        );
    }

    @Test
    public void testCreateRejectsNegativeLimitRangeHi() throws Exception {
        // LIMIT lo,hi with a negative hi is rejected just like a negative lo.
        assertCreateLateralRejected(
                "SELECT base.ts, sum(l.x) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT dim.n x FROM dim WHERE dim.k = base.k LIMIT 1,-1) l ON true "
                        + "SAMPLE BY 1h",
                "-1",
                NEGATIVE_LIMIT_ERROR
        );
    }

    @Test
    public void testCreateRejectsOuterColumnLimitOverScalarCount() throws Exception {
        assertCreateLateralRejected(
                "SELECT base.ts, sum(l.c) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT count() c FROM dim WHERE dim.k = base.k LIMIT base.n) l ON true "
                        + "SAMPLE BY 1h",
                "base.n",
                "LIMIT referencing an outer column is not supported over a scalar count"
        );
    }

    @Test
    public void testRefreshFailsClosedForPersistedExternalSourceDefinition() throws Exception {
        // Upgrade-break regression (intended break): older binaries accepted an external-source
        // sub-query (read_parquet) in a materialized-view definition; this binary rejects it, so
        // the stored SQL cannot be recreated through CREATE. Install it the way the ALTER apply
        // path does -- swap the definition object in both the graph and the state store -- and
        // drive the first cold refresh over it. Cold matters: the view has never refreshed, so
        // the state holds no cached factory, exactly like the first refresh after the upgrade
        // restart. The refresh must fail closed: invalid=true with the stable guard message as
        // the invalidation reason, no crash, no native leak, base table unaffected, view
        // droppable.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(parallel));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, String.valueOf(parallel));
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE ext AS (SELECT '2024-01-01T00:00:00.000000Z'::TIMESTAMP AS value FROM long_sequence(3))");
            encodeParquet("ext", "ext.parquet");
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE cfg (ts TIMESTAMP, k SYMBOL, lim TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cfg VALUES ('2024-01-01T00:00:00Z', 'a', '2024-01-01T00:00:00Z')");
            execute("INSERT INTO base VALUES ('2024-01-02T00:00:00Z', 'a', 1.0)");
            drainWalQueue();

            // MANUAL DEFERRED: no refresh runs at CREATE, keeping the factory cache cold.
            execute("CREATE MATERIALIZED VIEW mv WITH BASE base REFRESH MANUAL DEFERRED AS ("
                    + "SELECT ts, sum(v) AS s FROM base WHERE ts > (SELECT max(lim) FROM cfg) SAMPLE BY 1h"
                    + ") PARTITION BY DAY");
            drainWalQueue();

            // The SQL an older binary would have persisted; same column shape as the benign
            // definition above, so only the guard can make the refresh fail.
            final String legacySql = "SELECT ts, sum(v) AS s FROM base "
                    + "WHERE ts > (SELECT max(value) FROM read_parquet('ext.parquet')) SAMPLE BY 1h";
            final TableToken viewToken = engine.verifyTableName("mv");
            final MatViewDefinition current = engine.getDependentViewGraph().getViewDefinition(viewToken);
            final MatViewDefinition legacy = new MatViewDefinition();
            legacy.init(
                    current.getRefreshType(),
                    current.isDeferred(),
                    ColumnType.TIMESTAMP_MICRO,
                    viewToken,
                    legacySql,
                    current.getBaseTableName(),
                    current.getSamplingInterval(),
                    current.getSamplingIntervalUnit(),
                    current.getTimeZone(),
                    current.getTimeZoneOffset(),
                    current.getRefreshLimitHoursOrMonths(),
                    current.getTimerInterval(),
                    current.getTimerUnit(),
                    current.getTimerStartUs(),
                    current.getTimerTimeZone(),
                    current.getPeriodLength(),
                    current.getPeriodLengthUnit(),
                    current.getPeriodDelay(),
                    current.getPeriodDelayUnit()
            );
            // Mirror TableWriter's definition-swap: both the graph and the state store, so the
            // refresh job (which reads viewState.getViewDefinition()) sees the legacy SQL.
            engine.getDependentViewGraph().updateViewDefinition(viewToken, legacy);
            engine.getMatViewStateStore().updateViewDefinition(viewToken, legacy);

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();

            // SqlException.toSink renders "[position]: message"; the position points at the
            // rejected sub-query in the stored SQL.
            final int subQueryPos = legacySql.indexOf("(SELECT max(value)") + 1;
            assertQuery("select view_name, view_status, invalidation_reason from materialized_views")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_name\tview_status\tinvalidation_reason\n"
                            + "mv\tinvalid\t[" + subQueryPos
                            + "]: non-deterministic function cannot be used in materialized view: sub-query\n");

            // The failure is contained: the base table keeps working and the view drops cleanly.
            assertQuery("select count() from base").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
            execute("DROP MATERIALIZED VIEW mv");
            drainWalQueue();
            assertQuery("select view_name, view_status, invalidation_reason from materialized_views")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_name\tview_status\tinvalidation_reason\n");
        });
    }

    @Test
    public void testRefreshRecompileSucceedsAfterUnrelatedAddIndex() throws Exception {
        // An index on the *config* table swaps the sub-query's filtered scan for an index-driven row
        // cursor. That must not retroactively invalidate an already-created view.
        assertRefreshRecompiles(
                "ts > (SELECT max(lim) FROM cfg WHERE k = 'a')",
                true
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForAggregateSubQuery() throws Exception {
        assertRefreshRecompiles("ts > (SELECT max(lim) FROM cfg)", false);
    }

    @Test
    public void testRefreshRecompileSucceedsForCrossLateralRowBody() throws Exception {
        assertLateralRefreshRecompiles(
                "SELECT base.ts, sum(l.x) AS s FROM base "
                        + "CROSS JOIN LATERAL (SELECT dim.n x FROM dim WHERE dim.k = base.k) l "
                        + "SAMPLE BY 1h"
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForGroupBySubQuery() throws Exception {
        assertRefreshRecompiles("ts > (SELECT max(lim) FROM cfg WHERE n > 0 GROUP BY k LIMIT 1)", false);
    }

    @Test
    public void testRefreshRecompileSucceedsForInnerLateralLimitRowBody() throws Exception {
        assertLateralRefreshRecompiles(
                "SELECT base.ts, sum(l.x) AS s FROM base "
                        + "JOIN LATERAL (SELECT dim.n x FROM dim WHERE dim.k = base.k ORDER BY dim.dts LIMIT 1) l ON true "
                        + "SAMPLE BY 1h"
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForJoinSubQuery() throws Exception {
        assertRefreshRecompiles("ts > (SELECT max(c1.lim) FROM cfg c1 JOIN cfg c2 ON c1.k = c2.k)", false);
    }

    @Test
    public void testRefreshRecompileSucceedsForLateralCastCountBody() throws Exception {
        // Expression-wrapped count: never compensated (NULL fill on both old and new binaries), and
        // it must stay compilable.
        assertLateralRefreshRecompiles(
                "SELECT base.ts, sum(l.c) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT cast(count() AS int) c FROM dim WHERE dim.k = base.k) l ON true "
                        + "SAMPLE BY 1h"
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForLateralCountBelowAggregateLimit() throws Exception {
        // A LIMIT below the aggregate caps the counted input and can never drop the aggregate row;
        // the guard must not include it and the stored shape must keep compiling.
        assertLateralRefreshRecompiles(
                "SELECT base.ts, sum(l.c) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT count() c FROM (SELECT n FROM dim WHERE dim.k = base.k LIMIT 5)) l ON true "
                        + "SAMPLE BY 1h"
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForLateralCountFilteredWrapperBody() throws Exception {
        // A filtering wrapper over a scalar count fails closed (NULL, not 0) under the narrowed
        // compensation; the value changes across the upgrade, but the definition must keep
        // compiling — the view stays valid.
        assertLateralRefreshRecompiles(
                "SELECT base.ts, sum(l.c) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT c FROM (SELECT count() c FROM dim WHERE dim.k = base.k) WHERE c >= 0) l ON true "
                        + "SAMPLE BY 1h"
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForLateralCountGroupByBody() throws Exception {
        // GROUP BY disqualifies the scalar-count compensation (0 becomes NULL for unmatched outer
        // rows), but the stored definition must keep compiling under the refresh context.
        assertLateralRefreshRecompiles(
                "SELECT base.ts, sum(l.c) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT count() c FROM dim WHERE dim.k = base.k GROUP BY dim.n) l ON true "
                        + "SAMPLE BY 1h"
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForLateralCountLimitOneBody() throws Exception {
        // LIMIT 1 provably keeps the single aggregate row: folded at compile time, no guard.
        assertLateralRefreshRecompiles(
                "SELECT base.ts, sum(l.c) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT count() c FROM dim WHERE dim.k = base.k LIMIT 1) l ON true "
                        + "SAMPLE BY 1h"
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForLateralCountLimitZeroBody() throws Exception {
        // LIMIT 0 provably drops the aggregate row: the body is empty, NULL fill is correct, and
        // the shape must keep compiling.
        assertLateralRefreshRecompiles(
                "SELECT base.ts, sum(l.c) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT count() c FROM dim WHERE dim.k = base.k LIMIT 0) l ON true "
                        + "SAMPLE BY 1h"
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForLateralCountPlusOneBody() throws Exception {
        assertLateralRefreshRecompiles(
                "SELECT base.ts, sum(l.c) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT count() + 1 c FROM dim WHERE dim.k = base.k) l ON true "
                        + "SAMPLE BY 1h"
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForLateralCountUnionBody() throws Exception {
        // A set operation disqualifies the scalar-count compensation but must not stop the stored
        // definition from compiling.
        assertLateralRefreshRecompiles(
                "SELECT base.ts, sum(l.c) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT count() c FROM dim WHERE dim.k = base.k "
                        + "UNION ALL SELECT CAST(100 AS LONG) FROM long_sequence(0)) l ON true "
                        + "SAMPLE BY 1h"
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForLateralPositiveLimitRowBody() throws Exception {
        // compensateLimit rewrites LIMIT into a per-outer-row row_number() filter; the rewritten
        // shape must survive the refresh recompile.
        assertLateralRefreshRecompiles(
                "SELECT base.ts, sum(l.x) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT dim.n x FROM dim WHERE dim.k = base.k ORDER BY dim.dts LIMIT 1) l ON true "
                        + "SAMPLE BY 1h"
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForLateralRangeLimitRowBody() throws Exception {
        assertLateralRefreshRecompiles(
                "SELECT base.ts, sum(l.x) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT dim.n x FROM dim WHERE dim.k = base.k ORDER BY dim.dts LIMIT 1,2) l ON true "
                        + "SAMPLE BY 1h"
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForLateralScalarCountBody() throws Exception {
        // The bare scalar-count body is the positively-proven compensated shape (0, not NULL, for
        // unmatched outer rows). It is the most common stored lateral definition and must keep
        // recompiling under the refresh context.
        assertLateralRefreshRecompiles(
                "SELECT base.ts, sum(l.c) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT count() c FROM dim WHERE dim.k = base.k) l ON true "
                        + "SAMPLE BY 1h"
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForMonotonicWrapperAfterAddIndex() throws Exception {
        // Unlike the bare-ts shape above, a monotonic wrapper over the designated timestamp keeps
        // the original predicate as a residual filter that re-reads the pruning bound through
        // ScalarSubQueryBoundRefFunction, so this exercises the shared-holder path in the refresh
        // recompile. ADD INDEX flips the sub-query factory's fail-safe determinism hint under the
        // live view; the guard must not read that hint.
        assertRefreshRecompiles(
                "dateadd('h', 1, ts) >= (SELECT max(lim) FROM cfg WHERE k = 'a')",
                true
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForOrderedLimitSubQuery() throws Exception {
        assertRefreshRecompiles("ts > (SELECT lim FROM cfg ORDER BY ts DESC LIMIT 1)", false);
    }

    @Test
    public void testRefreshRecompileSucceedsForUncorrelatedNegativeLimitLateralBody() throws Exception {
        // An UNCORRELATED body is not decorrelated, so a negative LIMIT there stays legal. A stored
        // definition with this shape must not be caught by the correlated-lateral rejection.
        assertLateralRefreshRecompiles(
                "SELECT base.ts, sum(l.x) AS s FROM base "
                        + "LEFT JOIN LATERAL (SELECT dim.n x FROM dim LIMIT -1) l ON true "
                        + "SAMPLE BY 1h"
        );
    }

    private void assertCreateLateralRejected(String viewQuery, String errorToken, String expectedError) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(parallel));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, String.valueOf(parallel));
        assertMemoryLeak(() -> {
            createLateralTables();
            final String sql = "CREATE MATERIALIZED VIEW mv WITH BASE base AS (" + viewQuery + ") PARTITION BY DAY";
            assertExceptionNoLeakCheck(sql, sql.indexOf(errorToken), expectedError);
        });
    }

    private void assertLateralRefreshRecompiles(String viewQuery) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(parallel));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, String.valueOf(parallel));
        assertMemoryLeak(() -> {
            createLateralTables();
            execute("CREATE MATERIALIZED VIEW mv WITH BASE base AS (" + viewQuery + ") PARTITION BY DAY");
            drainWalQueue();
            recompileAsRefreshJobWould(viewQuery);
        });
    }

    private void assertRefreshRecompiles(String predicate, boolean addIndexAfterCreate) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(parallel));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, String.valueOf(parallel));
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE, n LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE cfg (ts TIMESTAMP, k SYMBOL, lim TIMESTAMP, n LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cfg VALUES ('2024-01-01T00:00:00Z', 'a', '2024-01-01T00:00:00Z', 1)");
            execute("INSERT INTO base VALUES ('2024-01-02T00:00:00Z', 'a', 1.0, 1)");
            drainWalQueue();

            execute("CREATE MATERIALIZED VIEW mv AS SELECT ts, sum(v) AS s FROM base WHERE "
                    + predicate + " SAMPLE BY 1h");
            drainWalQueue();

            if (addIndexAfterCreate) {
                execute("ALTER TABLE cfg ALTER COLUMN k ADD INDEX");
                drainWalQueue();
                engine.releaseInactive();
            }

            recompileAsRefreshJobWould(predicate);
        });
    }

    private static void encodeParquet(CharSequence tableName, CharSequence fileName) {
        try (
                Path path = new Path();
                PartitionDescriptor descriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader(tableName)
        ) {
            path.of(root).concat(fileName);
            engine.getConfiguration().getFilesFacade().remove(path.$());
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            PartitionEncoder.encode(descriptor, path);
            Assert.assertTrue(Files.exists(path.$()));
        }
    }

    private void createLateralTables() throws Exception {
        // dim's designated timestamp is named dts so that the outer SAMPLE BY query can reference
        // base's ts without ambiguity.
        execute("CREATE TABLE base (ts TIMESTAMP, k INT, v DOUBLE, n INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE dim (dts TIMESTAMP, k INT, n INT) TIMESTAMP(dts) PARTITION BY DAY WAL");
        execute("INSERT INTO base VALUES ('2024-01-01T00:00:00Z', 1, 1.0, 1), ('2024-01-01T01:00:00Z', 3, 3.0, 1)");
        execute("INSERT INTO dim VALUES ('2024-01-01T00:00:00Z', 1, 10), ('2024-01-01T00:10:00Z', 1, 20), ('2024-01-01T00:20:00Z', 2, 30)");
        drainWalQueue();
    }

    /**
     * Mirrors the refresh job's cache-miss path: compile the persisted view SQL under the refresh
     * execution context, which is the context that disallows non-deterministic functions.
     */
    private void recompileAsRefreshJobWould(String shape) {
        final TableToken viewToken = engine.verifyTableName("mv");
        final MatViewDefinition definition = engine.getDependentViewGraph().getViewDefinition(viewToken);
        final String viewSql = definition.getMatViewSql();
        final TableToken baseToken = engine.verifyTableName(definition.getBaseTableName());
        try (
                MatViewRefreshSqlExecutionContext refreshContext = new MatViewRefreshSqlExecutionContext(engine, 1);
                TableReader baseReader = engine.getReader(baseToken)
        ) {
            refreshContext.of(baseReader);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compile(viewSql, refreshContext).getRecordCursorFactory().close();
            }
        } catch (Throwable e) {
            throw new AssertionError(
                    "persisted materialized-view SQL must recompile under the refresh context. Failing "
                            + "here means the view is marked invalid=true on the next refresh or restart, "
                            + "replicated to peers and cascaded to dependent views.\n  shape: "
                            + shape + "\n  parallel=" + parallel + "\n  error: " + e.getMessage(), e);
        }
    }
}
