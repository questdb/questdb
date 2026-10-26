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
import io.questdb.griffin.SqlExecutionRequirements;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TableFunctionTestUtils;
import io.questdb.test.tools.TableFunctionTestUtils.CloseCountingRecordCursorFactory;
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
    private static final String SQL_VIEW_INITIAL_ROWS = """
            ts\tk\ta\tb\tc
            2024-01-01T00:00:00.000000Z\tx\t1.0\t2.0\t3.0
            """;
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
    public void testIncrementalRefreshAcceptsChangedGeneratedColumnNames() throws Exception {
        assertRefreshAcceptsChangedGeneratedColumnNames("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
    }

    @Test
    public void testIncrementalRefreshKeepsBareZeroArgCallsInLegacyAggregatingDefinition() throws Exception {
        assertRefreshKeepsBareZeroArgCalls("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
    }

    @Test
    public void testRangeRefreshAcceptsChangedGeneratedColumnNames() throws Exception {
        assertRefreshAcceptsChangedGeneratedColumnNames(
                "REFRESH MATERIALIZED VIEW mv RANGE FROM '2024-01-01T00:00:00Z' TO '2024-01-01T02:00:00Z'"
        );
    }

    @Test
    public void testRangeRefreshKeepsBareZeroArgCallsInLegacyAggregatingDefinition() throws Exception {
        assertRefreshKeepsBareZeroArgCalls(
                "REFRESH MATERIALIZED VIEW mv RANGE FROM '2024-01-01T00:00:00Z' TO '2024-01-01T02:00:00Z'"
        );
    }

    @Test
    public void testRefreshAcceptsChangedGeneratedColumnNames() throws Exception {
        assertRefreshAcceptsChangedGeneratedColumnNames("REFRESH MATERIALIZED VIEW mv FULL");
    }

    @Test
    public void testRefreshAcceptsSqlViewColumnRenamedInPlace() throws Exception {
        // The name changed at the same position, so refresh copies the column by position.
        assertSqlViewChange(
                "SELECT * FROM vagg",
                "ALTER VIEW vagg AS (SELECT ts, k, avg(a) a, avg(b) b, avg(c) c2 FROM base SAMPLE BY 1h)",
                "mv\tvalid\t\n",
                """
                        ts\tk\ta\tb\tc
                        2024-01-01T00:00:00.000000Z\tx\t1.0\t2.0\t3.0
                        2024-01-01T01:00:00.000000Z\tx\t10.0\t20.0\t30.0
                        """
        );
    }

    @Test
    public void testRefreshAcceptsSqlViewColumnReplacedInPlace() throws Exception {
        // A name can't tell a replaced column from a redefinition of c under its own name, which refresh
        // accepts as well, so the replacement lands in c.
        assertSqlViewChange(
                "SELECT * FROM vagg",
                "ALTER VIEW vagg AS (SELECT ts, k, avg(a) a, avg(b) b, max(a) d FROM base SAMPLE BY 1h)",
                "mv\tvalid\t\n",
                """
                        ts\tk\ta\tb\tc
                        2024-01-01T00:00:00.000000Z\tx\t1.0\t2.0\t1.0
                        2024-01-01T01:00:00.000000Z\tx\t10.0\t20.0\t10.0
                        """
        );
    }

    @Test
    public void testRefreshFailsClosedForPersistedAdministrativeFunction() throws Exception {
        // An administrative function needs the Enterprise security context of its caller, or has side
        // effects, so a materialized view rejects it at refresh too, however its definition got there.
        // The refresh must reject it before the function is created at all.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(parallel));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, String.valueOf(parallel));
        assertMemoryLeak(() -> {
            final ObjList<CloseCountingRecordCursorFactory> factories = new ObjList<>();
            TableFunctionTestUtils.register(
                    engine,
                    "ent_admin_cursor",
                    SqlExecutionRequirements.REQUIRES_ENTERPRISE_SECURITY_CONTEXT,
                    factories
            );
            try {
                execute("CREATE TABLE base (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO base VALUES ('2024-01-01T00:00:00Z', 1)");
                drainWalQueue();
                execute("CREATE MATERIALIZED VIEW mv WITH BASE base REFRESH MANUAL DEFERRED AS ("
                        + "SELECT ts, count() c FROM base SAMPLE BY 1d) PARTITION BY DAY");
                drainWalQueue();
                final String legacySql = "SELECT base.ts, count() c FROM base CROSS JOIN ent_admin_cursor() SAMPLE BY 1d";
                installPersistedViewSql("mv", legacySql);

                execute("REFRESH MATERIALIZED VIEW mv FULL");
                drainWalAndMatViewQueues();
                assertQuery("SELECT view_name, view_status, invalidation_reason FROM materialized_views")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("view_name\tview_status\tinvalidation_reason\n"
                                + "mv\tinvalid\t[" + legacySql.indexOf("ent_admin_cursor")
                                + "]: administrative function cannot be used in materialized view: ent_admin_cursor\n");
                Assert.assertEquals(0, factories.size());
            } finally {
                TableFunctionTestUtils.unregister(engine, "ent_admin_cursor");
            }
        });
    }

    @Test
    public void testRefreshFailsClosedForPersistedCatalogueFunctionFromView() throws Exception {
        // A definition that reads a catalogue function through a regular view, which CREATE now rejects,
        // stops refreshing after the upgrade: the view's definition may have changed since CREATE, so
        // the refresh cannot tell who chose what the materialized view would store.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(parallel));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, String.valueOf(parallel));
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00Z', 1)");
            execute("CREATE VIEW names AS (SELECT table_name FROM tables())");
            drainWalAndViewQueues();
            execute("CREATE MATERIALIZED VIEW mv WITH BASE base REFRESH MANUAL DEFERRED AS ("
                    + "SELECT ts, count() c FROM base SAMPLE BY 1d) PARTITION BY DAY");
            drainWalQueue();
            final String legacySql = "SELECT base.ts, count() c FROM base CROSS JOIN names SAMPLE BY 1d";
            installPersistedViewSql("mv", legacySql);

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM materialized_views")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_name\tview_status\tinvalidation_reason\n"
                            + "mv\tinvalid\t[" + legacySql.indexOf("names")
                            + "]: catalogue function from view names cannot be used in materialized view: tables\n");
        });
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
            installPersistedViewSql("mv", legacySql);

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
    public void testRefreshKeepsBareZeroArgCallsInLegacyAggregatingDefinition() throws Exception {
        assertRefreshKeepsBareZeroArgCalls("REFRESH MATERIALIZED VIEW mv FULL");
    }

    @Test
    public void testRefreshPreservesPersistedCatalogueFunction() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(parallel));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, String.valueOf(parallel));
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00Z', 1)");
            drainWalQueue();

            // Older releases stored this definition whoever created it, while CREATE now requires SYSTEM
            // ADMIN for the catalogue function: the refresh keeps compiling a function the definition writes.
            execute("CREATE MATERIALIZED VIEW mv WITH BASE base REFRESH MANUAL DEFERRED AS ("
                    + "SELECT ts, count() c FROM base SAMPLE BY 1d) PARTITION BY DAY");
            drainWalQueue();
            installPersistedViewSql("mv", "SELECT base.ts, count() c FROM base CROSS JOIN tables() SAMPLE BY 1d");

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            assertQuery("SELECT count() FROM mv").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
            assertQuery("SELECT view_name, view_status FROM materialized_views")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_name\tview_status\nmv\tvalid\n");

            execute("INSERT INTO base VALUES ('2024-01-02T00:00:00Z', 2)");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
            drainWalAndMatViewQueues();
            assertQuery("SELECT count() FROM mv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
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

    @Test
    public void testRefreshRejectsSqlViewAddedColumn() throws Exception {
        assertSqlViewChange(
                "SELECT * FROM vagg",
                "ALTER VIEW vagg AS (SELECT ts, k, avg(a) a, avg(b) b, avg(c) c, count() n FROM base SAMPLE BY 1h)",
                "mv\tinvalid\t[-1]: materialized view query does not match view schema [view=mv, queryColumnCount=6, viewColumnCount=5]\n",
                SQL_VIEW_INITIAL_ROWS
        );
    }

    @Test
    public void testRefreshRejectsSqlViewColumnsRotated() throws Exception {
        assertSqlViewChange(
                "SELECT * FROM vagg",
                "ALTER VIEW vagg AS (SELECT ts, k, avg(c) c, avg(a) a, avg(b) b FROM base SAMPLE BY 1h)",
                "mv\tinvalid\t[-1]: materialized view query does not match view schema [view=mv, columnIndex=2, queryColumn=c, viewColumn=a]\n",
                SQL_VIEW_INITIAL_ROWS
        );
    }

    @Test
    public void testRefreshRejectsSqlViewColumnsSwapped() throws Exception {
        // A query written without SELECT gets a wildcard from the parser, so both spellings take the
        // SQL view's column order.
        for (String viewQuery : new String[]{"SELECT * FROM vagg", "vagg"}) {
            assertSqlViewChange(
                    viewQuery,
                    "ALTER VIEW vagg AS (SELECT ts, k, avg(b) b, avg(a) a, avg(c) c FROM base SAMPLE BY 1h)",
                    "mv\tinvalid\t[-1]: materialized view query does not match view schema [view=mv, columnIndex=2, queryColumn=b, viewColumn=a]\n",
                    SQL_VIEW_INITIAL_ROWS
            );
        }
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

    /**
     * A binary without passthrough views stores an aggregating view's query as written, so the stored query
     * can call a zero-argument function through its bare name (count, pi). The refresh of an aggregating view
     * reads such a name as a call, so the view keeps refreshing after an upgrade. Full, range and incremental
     * refresh each tell the refresh context whether the view is passthrough, so each one runs this check. The
     * view has never refreshed, so each kind compiles the stored query for the first time, and each covers all
     * of the base table's rows: incremental because there is no earlier refresh to continue from, range
     * because the caller's bounds span the data.
     */
    /**
     * A view keeps the column names CREATE generated, while refresh compiles the stored query under the
     * alias setting in force now. A view created by 8.2.3-8.3.3, which named {@code avg(v)} as
     * {@code avg}, reaches refresh the same way as a view created with
     * {@code cairo.sql.column.alias.expression.enabled=false} on a server that later enables it. With
     * expression aliases enabled, CREATE requires an explicit alias for such a column, so the change
     * only goes from disabled to enabled. Refresh copies the renamed column by position and the view
     * stays valid.
     */
    private void assertRefreshAcceptsChangedGeneratedColumnNames(String refreshSql) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(parallel));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, String.valueOf(parallel));
        record Case(String viewSql, String createSuffix, boolean expectSize, String expected) {
        }
        final String aggregatedRows = """
                ts\tk\tavg
                2024-01-01T00:00:00.000000Z\ta\t1.5
                2024-01-01T01:00:00.000000Z\ta\t3.0
                """;
        final Case[] cases = {
                new Case("SELECT ts, k, avg(v) FROM base SAMPLE BY 1h", "", true, aggregatedRows),
                new Case("SELECT * FROM (SELECT ts, k, avg(v) FROM base SAMPLE BY 1h)", "", true, aggregatedRows),
                new Case("WITH c AS (SELECT ts, k, avg(v) FROM base SAMPLE BY 1h) SELECT * FROM c", "", true, aggregatedRows),
                new Case(
                        "SELECT ts, k, v * 2 FROM base",
                        "",
                        true,
                        """
                                ts\tk\tcolumn
                                2024-01-01T00:00:00.000000Z\ta\t2.0
                                2024-01-01T00:30:00.000000Z\ta\t4.0
                                2024-01-01T01:30:00.000000Z\ta\t6.0
                                """
                ),
                // The policy reads the view's own column, which keeps its CREATE name.
                new Case(
                        "SELECT ts, k, avg(v) FROM base SAMPLE BY 1h",
                        " EXPIRE ROWS WHEN avg < 2.0",
                        false,
                        """
                                ts\tk\tavg
                                2024-01-01T01:00:00.000000Z\ta\t3.0
                                """
                )
        };
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                        ('2024-01-01T00:00:00Z', 'a', 1.0),
                        ('2024-01-01T00:30:00Z', 'a', 2.0),
                        ('2024-01-01T01:30:00Z', 'a', 3.0)
                    """);
            drainWalQueue();

            for (Case c : cases) {
                setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
                // MANUAL DEFERRED: no refresh runs at CREATE, so the factory cache stays cold and the refresh
                // below compiles the stored query under the settings in force then.
                execute("CREATE MATERIALIZED VIEW mv WITH BASE base REFRESH MANUAL DEFERRED AS (" + c.viewSql + ") PARTITION BY DAY" + c.createSuffix);
                drainWalQueue();
                setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");

                execute(refreshSql);
                drainWalAndMatViewQueues();

                // materialized_views reports a range-refreshed view as refreshing (questdb/questdb#7766), so this
                // reads the invalidation reason rather than the status.
                assertQuery("select view_name, invalidation_reason from materialized_views")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("view_name\tinvalidation_reason\nmv\t\n");
                assertQuery("mv")
                        .timestamp("ts")
                        .expectSize(c.expectSize)
                        .noLeakCheck()
                        .returns(c.expected);

                execute("DROP MATERIALIZED VIEW mv");
                drainWalQueue();
            }
        });
    }

    private void assertRefreshKeepsBareZeroArgCalls(String refreshSql) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(parallel));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, String.valueOf(parallel));
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                        ('2024-01-01T00:00:00Z', 'a', 1.0),
                        ('2024-01-01T00:30:00Z', 'a', 2.0),
                        ('2024-01-01T01:30:00Z', 'a', 3.0)
                    """);
            drainWalQueue();

            final String[][] cases = {
                    {
                            "SELECT ts, count FROM base SAMPLE BY 1h",
                            """
                            ts\tcount
                            2024-01-01T00:00:00.000000Z\t2
                            2024-01-01T01:00:00.000000Z\t1
                            """
                    },
                    {
                            "SELECT ts, pi, sum(v) s FROM base SAMPLE BY 1h",
                            """
                            ts\tpi\ts
                            2024-01-01T00:00:00.000000Z\t3.141592653589793\t3.0
                            2024-01-01T01:00:00.000000Z\t3.141592653589793\t3.0
                            """
                    }
            };
            for (String[] c : cases) {
                final String legacySql = c[0];
                // MANUAL DEFERRED: no refresh runs at CREATE, so the factory cache stays cold, as on the first
                // refresh after an upgrade restart. CREATE stores the call with its parentheses.
                execute("CREATE MATERIALIZED VIEW mv WITH BASE base REFRESH MANUAL DEFERRED AS (" + legacySql + ") PARTITION BY DAY");
                drainWalQueue();
                installPersistedViewSql("mv", legacySql);

                execute(refreshSql);
                drainWalAndMatViewQueues();

                // materialized_views reports a range-refreshed view as refreshing (questdb/questdb#7766), so this
                // reads the invalidation reason rather than the status. A refresh that fails to compile the stored
                // query leaves either an invalidation reason or an empty view.
                assertQuery("select view_name, invalidation_reason from materialized_views")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("view_name\tinvalidation_reason\nmv\t\n");
                assertQuery("mv")
                        .timestamp("ts")
                        .expectSize()
                        .noLeakCheck()
                        .returns(c[1]);

                execute("DROP MATERIALIZED VIEW mv");
                drainWalQueue();
            }
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

    /**
     * A materialized view that reads a SQL view through a top-level wildcard takes the SQL view's column
     * order, so ALTER VIEW can change what the view's query projects at each position. The view refreshes
     * once before the change and once after it, both times as a FULL refresh, which compiles the stored
     * query.
     */
    private void assertSqlViewChange(String viewQuery, String alterViewSql, String expectedStatus, String expectedRows) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(parallel));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, String.valueOf(parallel));
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, a DOUBLE, b DOUBLE, c DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00Z', 'x', 1.0, 2.0, 3.0)");
            drainWalQueue();
            execute("CREATE VIEW vagg AS (SELECT ts, k, avg(a) a, avg(b) b, avg(c) c FROM base SAMPLE BY 1h)");
            execute("CREATE MATERIALIZED VIEW mv WITH BASE base REFRESH MANUAL DEFERRED AS (" + viewQuery + ") PARTITION BY DAY");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            assertQuery("mv")
                    .timestamp("ts")
                    .expectSize()
                    .noLeakCheck()
                    .returns(SQL_VIEW_INITIAL_ROWS);

            execute(alterViewSql);
            execute("INSERT INTO base VALUES ('2024-01-01T01:00:00Z', 'x', 10.0, 20.0, 30.0)");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();

            assertQuery("select view_name, view_status, invalidation_reason from materialized_views")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_name\tview_status\tinvalidation_reason\n" + expectedStatus);
            assertQuery("mv")
                    .timestamp("ts")
                    .expectSize()
                    .noLeakCheck()
                    .returns(expectedRows);

            execute("DROP MATERIALIZED VIEW mv");
            execute("DROP VIEW vagg");
            execute("DROP TABLE base");
            drainWalQueue();
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

    private static void installPersistedViewSql(String viewName, String sql) {
        final TableToken viewToken = engine.verifyTableName(viewName);
        final MatViewDefinition current = engine.getDependentViewGraph().getViewDefinition(viewToken);
        final MatViewDefinition legacy = new MatViewDefinition();
        legacy.init(
                current.getRefreshType(),
                current.isDeferred(),
                ColumnType.TIMESTAMP_MICRO,
                viewToken,
                sql,
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
                current.getPeriodDelayUnit(),
                current.isPassthrough()
        );
        // A persisted definition reaches refresh through the graph and the state store.
        engine.getDependentViewGraph().updateViewDefinition(viewToken, legacy);
        engine.getMatViewStateStore().updateViewDefinition(viewToken, legacy);
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
            refreshContext.of(baseReader, viewToken, definition.isPassthrough());
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
