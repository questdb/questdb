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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Loads materialized views written by the previous release, QuestDB 10.1.0, and checks that the upgrade invalidates
 * no valid view and that every valid view keeps writing the values the previous release writes.
 * <p>
 * The fixture {@code /migration/mat_views_10.1.0.zip} is the data directory of a QuestDB 10.1.0 server run with
 * {@code cairo.sql.column.alias.expression.enabled=false}, after these statements:
 * <pre>
 * CREATE TABLE trades (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL;
 * CREATE TABLE trades2 (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL;
 * INSERT INTO trades VALUES ('a', 1.5, 10.0, '2024-01-01T00:10:00.000000Z'), ('b', 2.5, 20.0, '2024-01-01T00:20:00.000000Z'),
 *     ('a', 3.5, 30.0, '2024-01-01T00:40:00.000000Z'), ('a', 4.5, 40.0, '2024-01-01T01:10:00.000000Z'),
 *     ('b', 5.5, 50.0, '2024-01-02T10:30:00.000000Z');
 * INSERT INTO trades2 VALUES ('a', 1.25, 1.0, '2024-01-01T00:10:00.000000Z'), ('b', 2.25, 2.0, '2024-01-01T00:20:00.000000Z'),
 *     ('a', 3.25, 3.0, '2024-01-01T01:40:00.000000Z');
 * CREATE MATERIALIZED VIEW valid_mv AS (SELECT ts, sym, avg(price) AS avg_price, sum(amount) AS total FROM trades SAMPLE BY 1h) PARTITION BY DAY;
 * CREATE MATERIALIZED VIEW count_mv AS (SELECT ts, sym, count FROM trades SAMPLE BY 1h) PARTITION BY DAY;
 * CREATE MATERIALIZED VIEW gen_mv AS (SELECT ts, sym, avg(price), max(amount) FROM trades SAMPLE BY 1h) PARTITION BY DAY;
 * CREATE MATERIALIZED VIEW never_mv REFRESH MANUAL DEFERRED AS (SELECT ts, sym, last(price) AS last_price FROM trades SAMPLE BY 1d) PARTITION BY DAY;
 * CREATE MATERIALIZED VIEW invalid_mv AS (SELECT ts, sym, first(price) AS first_price FROM trades2 SAMPLE BY 1h) PARTITION BY DAY;
 * -- once the views have refreshed
 * ALTER TABLE trades2 DROP COLUMN amount;
 * </pre>
 * It holds a valid view, a view whose SQL calls bare {@code count}, a view whose SQL relies on generated names
 * ({@code avg} and {@code max} under the legacy alias rules), a never-refreshed view and an invalid view. The
 * expected rows after the further statements below are what the same 10.1.0 server returned for them, up to the
 * recipe that the release notes give for older views, ALTER MATERIALIZED VIEW ... AS with the SQL that SHOW CREATE
 * prints, which this version alone runs; the rows after it follow from the base rows.
 */
@RunWith(Parameterized.class)
public class MatViewUpgradeTest extends AbstractCairoTest {
    private static final String COUNT_MV_AFTER_INSERTS = """
            ts\tsym\tcount
            2024-01-01T00:00:00.000000Z\ta\t3
            2024-01-01T00:00:00.000000Z\tb\t1
            2024-01-01T01:00:00.000000Z\ta\t1
            2024-01-02T10:00:00.000000Z\tb\t2
            2024-01-03T05:00:00.000000Z\tc\t1
            """;
    // The *_AFTER_RECIPE rows follow the release notes' recipe for older views and a row of 'a' at 00:55, 9.5 and 90.0.
    private static final String COUNT_MV_AFTER_RECIPE = """
            ts\tsym\tcount
            2024-01-01T00:00:00.000000Z\ta\t4
            2024-01-01T00:00:00.000000Z\tb\t1
            2024-01-01T01:00:00.000000Z\ta\t1
            2024-01-02T10:00:00.000000Z\tb\t2
            2024-01-03T05:00:00.000000Z\tc\t1
            """;
    private static final String COUNT_MV_LOADED = """
            ts\tsym\tcount
            2024-01-01T00:00:00.000000Z\ta\t2
            2024-01-01T00:00:00.000000Z\tb\t1
            2024-01-01T01:00:00.000000Z\ta\t1
            2024-01-02T10:00:00.000000Z\tb\t1
            """;
    private static final String FIXTURE = "/migration/mat_views_10.1.0.zip";
    private static final String GEN_MV_AFTER_INSERTS = """
            ts\tsym\tavg\tmax
            2024-01-01T00:00:00.000000Z\ta\t3.8333333333333335\t60.0
            2024-01-01T00:00:00.000000Z\tb\t2.5\t20.0
            2024-01-01T01:00:00.000000Z\ta\t4.5\t40.0
            2024-01-02T10:00:00.000000Z\tb\t6.5\t70.0
            2024-01-03T05:00:00.000000Z\tc\t8.5\t80.0
            """;
    private static final String GEN_MV_AFTER_RECIPE = """
            ts\tsym\tavg\tmax
            2024-01-01T00:00:00.000000Z\ta\t5.25\t90.0
            2024-01-01T00:00:00.000000Z\tb\t2.5\t20.0
            2024-01-01T01:00:00.000000Z\ta\t4.5\t40.0
            2024-01-02T10:00:00.000000Z\tb\t6.5\t70.0
            2024-01-03T05:00:00.000000Z\tc\t8.5\t80.0
            """;
    private static final String GEN_MV_LOADED = """
            ts\tsym\tavg\tmax
            2024-01-01T00:00:00.000000Z\ta\t2.5\t30.0
            2024-01-01T00:00:00.000000Z\tb\t2.5\t20.0
            2024-01-01T01:00:00.000000Z\ta\t4.5\t40.0
            2024-01-02T10:00:00.000000Z\tb\t5.5\t50.0
            """;
    private static final String INVALID_MV = """
            ts\tsym\tfirst_price
            2024-01-01T00:00:00.000000Z\ta\t1.25
            2024-01-01T00:00:00.000000Z\tb\t2.25
            2024-01-01T01:00:00.000000Z\ta\t3.25
            """;
    private static final String NEVER_MV_AFTER_INSERTS = """
            ts\tsym\tlast_price
            2024-01-01T00:00:00.000000Z\ta\t4.5
            2024-01-01T00:00:00.000000Z\tb\t2.5
            2024-01-02T00:00:00.000000Z\tb\t7.5
            2024-01-03T00:00:00.000000Z\tc\t8.5
            """;
    private static final String NEVER_MV_LOADED = """
            ts\tsym\tlast_price
            """;
    private static final String STATUS_LOADED = """
            view_name\tview_status\tinvalidation_reason
            count_mv\tvalid\t
            gen_mv\tvalid\t
            invalid_mv\tinvalid\tdrop column operation
            never_mv\tvalid\t
            valid_mv\tvalid\t
            """;
    private static final String VALID_MV_AFTER_INSERTS = """
            ts\tsym\tavg_price\ttotal
            2024-01-01T00:00:00.000000Z\ta\t3.8333333333333335\t100.0
            2024-01-01T00:00:00.000000Z\tb\t2.5\t20.0
            2024-01-01T01:00:00.000000Z\ta\t4.5\t40.0
            2024-01-02T10:00:00.000000Z\tb\t6.5\t120.0
            2024-01-03T05:00:00.000000Z\tc\t8.5\t80.0
            """;
    private static final String VALID_MV_AFTER_RECIPE = """
            ts\tsym\tavg_price\ttotal
            2024-01-01T00:00:00.000000Z\ta\t5.25\t190.0
            2024-01-01T00:00:00.000000Z\tb\t2.5\t20.0
            2024-01-01T01:00:00.000000Z\ta\t4.5\t40.0
            2024-01-02T10:00:00.000000Z\tb\t6.5\t120.0
            2024-01-03T05:00:00.000000Z\tc\t8.5\t80.0
            """;
    private static final String VALID_MV_LOADED = """
            ts\tsym\tavg_price\ttotal
            2024-01-01T00:00:00.000000Z\ta\t2.5\t40.0
            2024-01-01T00:00:00.000000Z\tb\t2.5\t20.0
            2024-01-01T01:00:00.000000Z\ta\t4.5\t40.0
            2024-01-02T10:00:00.000000Z\tb\t5.5\t50.0
            """;
    private final boolean isColumnAliasExpressionEnabled;

    public MatViewUpgradeTest(boolean isColumnAliasExpressionEnabled) {
        this.isColumnAliasExpressionEnabled = isColumnAliasExpressionEnabled;
    }

    @Parameterized.Parameters(name = "aliasExpressions={0}")
    public static Collection<Object[]> data() {
        // true is the server default; the fixture's server ran with false.
        return Arrays.asList(new Object[][]{{true}, {false}});
    }

    @Test
    public void testUpgradeKeepsViewsValidAndWritingTheSameValues() throws Exception {
        assertMemoryLeak(() -> {
            final String root = temp.newFolder().getAbsolutePath();
            unzipFixture(root);
            final DefaultTestCairoConfiguration conf = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getCommitMode() {
                    return configuration.getCommitMode();
                }

                @Override
                public boolean isColumnAliasExpressionEnabled() {
                    return isColumnAliasExpressionEnabled;
                }
            };

            try (CairoEngine upgraded = new CairoEngine(conf)) {
                upgraded.load();
                upgraded.hydrateMatViewStateStore();
                // Every view loads with its rows, and the upgrade invalidates no valid view.
                assertStatus(upgraded, STATUS_LOADED);
                assertViews(upgraded, VALID_MV_LOADED, COUNT_MV_LOADED, GEN_MV_LOADED, NEVER_MV_LOADED);
                // The previous release persisted no base table id.
                for (String view : new String[]{"valid_mv", "count_mv", "gen_mv", "never_mv", "invalid_mv"}) {
                    assertBaseTableId(upgraded, view, MatViewState.BASE_TABLE_ID_UNKNOWN);
                }

                upgraded.execute("""
                        INSERT INTO trades VALUES
                        ('a', 6.5, 60.0, '2024-01-01T00:50:00.000000Z'),
                        ('b', 7.5, 70.0, '2024-01-02T10:45:00.000000Z'),
                        ('c', 8.5, 80.0, '2024-01-03T05:00:00.000000Z')
                        """);
                upgraded.execute("INSERT INTO trades2 VALUES ('a', 4.25, '2024-01-01T00:30:00.000000Z')");
                drainWalAndMatViewQueues(upgraded);
                upgraded.execute("REFRESH MATERIALIZED VIEW never_mv INCREMENTAL");
                drainWalAndMatViewQueues(upgraded);
                assertStatus(upgraded, STATUS_LOADED);
                assertViews(upgraded, VALID_MV_AFTER_INSERTS, COUNT_MV_AFTER_INSERTS, GEN_MV_AFTER_INSERTS, NEVER_MV_AFTER_INSERTS);

                // The view that relies on generated names copies by position; the others map their columns by name.
                assertRecordCopy(upgraded, "gen_mv", true);
                assertRecordCopy(upgraded, "valid_mv", false);
                assertRecordCopy(upgraded, "count_mv", false);
                assertRecordCopy(upgraded, "never_mv", false);

                // Each valid view's first refresh on this version persisted its base table id; the invalid view
                // did not refresh.
                final int tradesId = upgraded.verifyTableName("trades").getTableId();
                for (String view : new String[]{"valid_mv", "count_mv", "gen_mv", "never_mv"}) {
                    assertBaseTableId(upgraded, view, tradesId);
                }
                assertBaseTableId(upgraded, "invalid_mv", MatViewState.BASE_TABLE_ID_UNKNOWN);
            }

            try (CairoEngine restarted = new CairoEngine(conf)) {
                restarted.load();
                restarted.hydrateMatViewStateStore();
                assertStatus(restarted, STATUS_LOADED);
                assertViews(restarted, VALID_MV_AFTER_INSERTS, COUNT_MV_AFTER_INSERTS, GEN_MV_AFTER_INSERTS, NEVER_MV_AFTER_INSERTS);
                // A restart right after the first refresh on this version keeps the base table id known.
                final int tradesId = restarted.verifyTableName("trades").getTableId();
                for (String view : new String[]{"valid_mv", "count_mv", "gen_mv", "never_mv"}) {
                    assertBaseTableId(restarted, view, tradesId);
                }
                assertBaseTableId(restarted, "invalid_mv", MatViewState.BASE_TABLE_ID_UNKNOWN);

                for (String view : new String[]{"valid_mv", "count_mv", "gen_mv", "never_mv"}) {
                    restarted.execute("REFRESH MATERIALIZED VIEW " + view + " FULL");
                }
                drainWalAndMatViewQueues(restarted);
                assertStatus(restarted, STATUS_LOADED);
                assertViews(restarted, VALID_MV_AFTER_INSERTS, COUNT_MV_AFTER_INSERTS, GEN_MV_AFTER_INSERTS, NEVER_MV_AFTER_INSERTS);

                // The previous release invalidated the view without persisting a base table id, so its backlog counts
                // as lost: plain RESUME refuses with the cause, and RESUME SKIP BACKLOG jumps to the base table's
                // latest applied transaction, keeping the view's rows.
                try {
                    restarted.execute("REFRESH MATERIALIZED VIEW invalid_mv RESUME");
                    Assert.fail("RESUME of a view invalidated by the previous release must refuse");
                } catch (SqlException e) {
                    Assert.assertEquals("REFRESH MATERIALIZED VIEW ".length(), e.getPosition());
                    TestUtils.assertEquals(
                            "materialized view backlog is lost [reason=no base table id]; RESUME cannot catch up on base changes"
                                    + " made while the view was invalid. Run REFRESH MATERIALIZED VIEW invalid_mv RESUME SKIP BACKLOG,"
                                    + " then REFRESH ... RANGE over the gap",
                            e.getFlyweightMessage()
                    );
                }
                assertStatus(restarted, STATUS_LOADED);
                restarted.execute("REFRESH MATERIALIZED VIEW invalid_mv RESUME SKIP BACKLOG");
                drainWalAndMatViewQueues(restarted);
                assertStatus(restarted, STATUS_LOADED.replace("invalid_mv\tinvalid\tdrop column operation", "invalid_mv\tvalid\t"));
                assertBaseTableId(restarted, "invalid_mv", restarted.verifyTableName("trades2").getTableId());
                assertViews(restarted, VALID_MV_AFTER_INSERTS, COUNT_MV_AFTER_INSERTS, GEN_MV_AFTER_INSERTS, NEVER_MV_AFTER_INSERTS);

                // The release notes' recipe for an older view: ALTER ... AS with the SQL that SHOW CREATE prints, which
                // writes the bare call with its parentheses and each generated name as an alias of its view column.
                // Both views stay valid and keep their rows, and the view that relied on generated names maps its
                // columns by name from then on.
                Assert.assertEquals("SELECT ts, sym, count() AS count FROM trades SAMPLE BY 1h", showCreateSql(restarted, "count_mv"));
                Assert.assertEquals("SELECT ts, sym, avg(price) AS avg, max(amount) AS max FROM trades SAMPLE BY 1h", showCreateSql(restarted, "gen_mv"));
                for (String view : new String[]{"count_mv", "gen_mv"}) {
                    restarted.execute("ALTER MATERIALIZED VIEW " + view + " AS (" + showCreateSql(restarted, view) + ")");
                }
                drainWalAndMatViewQueues(restarted);
                restarted.execute("INSERT INTO trades VALUES ('a', 9.5, 90.0, '2024-01-01T00:55:00.000000Z')");
                drainWalAndMatViewQueues(restarted);
                assertStatus(restarted, STATUS_LOADED.replace("invalid_mv\tinvalid\tdrop column operation", "invalid_mv\tvalid\t"));
                assertViews(restarted, VALID_MV_AFTER_RECIPE, COUNT_MV_AFTER_RECIPE, GEN_MV_AFTER_RECIPE, NEVER_MV_AFTER_INSERTS);
                assertRecordCopy(restarted, "gen_mv", false);
                assertRecordCopy(restarted, "count_mv", false);
            }
        });
    }

    private static void assertBaseTableId(CairoEngine engine, String viewName, int expected) {
        final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName(viewName));
        Assert.assertNotNull(state);
        Assert.assertEquals(viewName, expected, state.getBaseTableId());
    }

    private static void assertRecordCopy(CairoEngine engine, String viewName, boolean isPositional) {
        final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName(viewName));
        Assert.assertNotNull(state);
        Assert.assertNotNull(viewName, state.getRecordToRowCopier());
        Assert.assertEquals(viewName, isPositional, state.isRecordCopyPositional());
        Assert.assertNull(viewName, state.getRecordProjectionForTesting());
    }

    // The SQL that SHOW CREATE MATERIALIZED VIEW prints for the view, between "AS (" and ")".
    private static String showCreateSql(CairoEngine engine, String viewName) throws SqlException {
        final StringSink sink = new StringSink();
        try (
                SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1);
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile("SHOW CREATE MATERIALIZED VIEW " + viewName, context).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(context)
        ) {
            Assert.assertTrue(cursor.hasNext());
            CursorPrinter.printColumn(cursor.getRecord(), factory.getMetadata(), 0, sink);
        }
        final String ddl = sink.toString();
        final int lo = ddl.indexOf("AS (\n") + "AS (\n".length();
        final int hi = ddl.indexOf("\n)", lo);
        Assert.assertTrue(ddl, lo > "AS (\n".length() - 1 && hi > lo);
        return ddl.substring(lo, hi);
    }

    private static void unzipFixture(String root) throws IOException {
        final byte[] buffer = new byte[64 * 1024];
        try (InputStream is = MatViewUpgradeTest.class.getResourceAsStream(FIXTURE)) {
            Assert.assertNotNull(is);
            try (ZipInputStream zip = new ZipInputStream(is)) {
                ZipEntry entry;
                while ((entry = zip.getNextEntry()) != null) {
                    final File dest = new File(root, entry.getName());
                    if (entry.isDirectory()) {
                        Assert.assertTrue(dest.isDirectory() || dest.mkdirs());
                    } else {
                        final File parent = dest.getParentFile();
                        Assert.assertTrue(parent.isDirectory() || parent.mkdirs());
                        try (OutputStream out = new FileOutputStream(dest)) {
                            int n;
                            while ((n = zip.read(buffer)) > 0) {
                                out.write(buffer, 0, n);
                            }
                        }
                    }
                    zip.closeEntry();
                }
            }
        }
    }

    private void assertStatus(CairoEngine engine, String expected) throws Exception {
        try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1)) {
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM materialized_views ORDER BY view_name")
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .returns(expected);
        }
    }

    private void assertViews(CairoEngine engine, String validMv, String countMv, String genMv, String neverMv) throws Exception {
        try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1)) {
            final String[] views = {"valid_mv", "count_mv", "gen_mv", "never_mv", "invalid_mv"};
            final String[] expected = {validMv, countMv, genMv, neverMv, INVALID_MV};
            for (int i = 0; i < views.length; i++) {
                assertQuery(views[i] + " ORDER BY ts, sym")
                        .withEngine(engine)
                        .withContext(context)
                        .timestamp("ts")
                        .expectSize()
                        .noLeakCheck()
                        .returns(expected[i]);
            }
        }
    }
}
