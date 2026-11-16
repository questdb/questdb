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
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlException;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Materialized view refresh through each of the three record copier flavours, which
 * {@code debug.cairo.copier.type} forces: single method, chunked and looping. Production picks the looping copier
 * for wide views when chunked copiers are disabled or fail, so every flavour must refresh the same views to the
 * same rows.
 * <ul>
 *     <li>A view column dropped and added back under the same name leaves a dropped slot with that name in the
 *     view's metadata. Refresh, RANGE, FULL and the copier rebuild between batch commits all write such a view.</li>
 *     <li>A value that does not convert fails the refresh with the value's error. The error names the query and
 *     view column only when the column it names is the one whose value failed (section 1 of the design): a
 *     conversion the writer row performs, such as text to DECIMAL or UUID, is not replayed, and its error keeps
 *     today's text.</li>
 *     <li>Every column type goes through the name-mapped copier, with the values the base holds, NULL and the
 *     extremes included.</li>
 * </ul>
 * The value error texts are those of {@code INSERT INTO ... SELECT} into a plain table, which uses the same copiers.
 */
@RunWith(Parameterized.class)
public class MatViewRefreshCopierTypesTest extends AbstractCairoTest {
    // Value conversions that the copier performs itself: the replay finds their column.
    // {base column type, failing value, good value, view column type, error text}
    private static final String[][] COPIER_CONVERSIONS = {
            {"VARCHAR", "'XY'", "'Z'", "CHAR", "inconvertible value: `XY` [VARCHAR -> CHAR]"},
            {"VARCHAR", "'abc'", "'1.5'", "DOUBLE", "inconvertible value: `abc` [VARCHAR -> DOUBLE]"},
            {"VARCHAR", "'not-a-date'", "'2024-01-01'", "DATE", "inconvertible value: `not-a-date` [VARCHAR -> DATE]"},
            {"STRING", "'xyz'", "'0x01'", "LONG256", "inconvertible value: `xyz` [STRING -> LONG256]"},
    };
    // Value conversions that the copiers hand to the writer row: the replay into a row that discards the values
    // does not run them, so it cannot name their column.
    // {base column type, failing value, good value, view column type, error text}
    private static final String[][] ROW_CONVERSIONS = {
            {"VARCHAR", "'123456.5'", "'12.5'", "DECIMAL(6,2)", "inconvertible value: `123456.5` [VARCHAR -> DECIMAL(6,2)]"},
            {"STRING", "'123456.5'", "'12.5'", "DECIMAL(6,2)", "inconvertible value: `123456.5` [STRING -> DECIMAL(6,2)]"},
            {"CHAR", "'x'", "'7'", "DECIMAL(6,2)", "inconvertible value: x [CHAR -> DECIMAL(6,2)]"},
            {"VARCHAR", "'not-a-uuid'", "'11111111-1111-1111-1111-111111111111'", "UUID", "inconvertible value: `not-a-uuid` [STRING -> UUID]"},
            {"STRING", "'not-a-uuid'", "'11111111-1111-1111-1111-111111111111'", "UUID", "inconvertible value: `not-a-uuid` [STRING -> UUID]"},
            {"VARCHAR", "'b!!e'", "'u33d'", "GEOHASH(4c)", "inconvertible value: `b!!e` [VARCHAR -> GEOHASH(4c)]"},
            {"STRING", "'b!!e'", "'u33d'", "GEOHASH(4c)", "inconvertible value: `b!!e` [STRING -> GEOHASH(4c)]"},
            {"VARCHAR", "'xyz'", "'0x01'", "LONG256", "inconvertible value: `xyz` [STRING -> LONG256]"},
    };
    private final int copierType;

    public MatViewRefreshCopierTypesTest(String name, int copierType) {
        this.copierType = copierType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> params() {
        return Arrays.asList(new Object[][]{
                {"single-method", RecordToRowCopierUtils.COPIER_TYPE_SINGLE_METHOD},
                {"chunked", RecordToRowCopierUtils.COPIER_TYPE_CHUNKED},
                {"looping", RecordToRowCopierUtils.COPIER_TYPE_LOOPING},
        });
    }

    @Override
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
    }

    @Test
    public void testAllColumnTypesThroughMappedCopier() throws Exception {
        // A passthrough view copies every column type by identity, then, after the view drops i, through the
        // name-mapped copier, and after the view adds i back as LONG, with the INT to LONG cast. The base holds the
        // values the view must hold, so the base is the oracle.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE base (
                        ts TIMESTAMP, ip IPV4, l256 LONG256, g1 GEOHASH(1c), g2 GEOHASH(2c), g4 GEOHASH(4c), g8 GEOHASH(8c),
                        d2 DECIMAL(2,1), d4 DECIMAL(4,1), d9 DECIMAL(9,2), d18 DECIMAL(18,3), d38 DECIMAL(38,2), d76 DECIMAL(76,10),
                        arr DOUBLE[], arr2 DOUBLE[][], by BYTE, sh SHORT, ch CHAR, dt DATE, tsn TIMESTAMP_NS, u UUID,
                        bo BOOLEAN, sym SYMBOL, vc VARCHAR, s STRING, i INT, lg LONG, f FLOAT, dbl DOUBLE, bin BINARY
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
            insertAllTypesRows("2024-01-01");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String columns = "ts, ip, l256, g1, g2, g4, g8, d2, d4, d9, d18, d38, d76, arr, arr2, by, sh, ch, dt, tsn, u, bo, sym, vc, s, lg, f, dbl, bin";
            assertViewValid("mv");
            Assert.assertNull(getViewState("mv").getRecordProjectionForTesting());
            assertSqlCursors("SELECT " + columns + ", i FROM base ORDER BY ts", "SELECT " + columns + ", i FROM mv ORDER BY ts");

            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN i");
            drainWalQueue();
            insertAllTypesRows("2024-01-02");
            drainWalAndMatViewQueues();
            assertViewValid("mv");
            Assert.assertNotNull(getViewState("mv").getRecordProjectionForTesting());
            assertSqlCursors("SELECT " + columns + " FROM base ORDER BY ts", "SELECT " + columns + " FROM mv ORDER BY ts");

            execute("ALTER MATERIALIZED VIEW mv ADD COLUMN i LONG");
            drainWalQueue();
            insertAllTypesRows("2024-01-03");
            drainWalAndMatViewQueues();
            assertViewValid("mv");
            assertSqlCursors(
                    "SELECT " + columns + ", i::LONG AS i FROM base WHERE ts >= '2024-01-03' ORDER BY ts",
                    "SELECT " + columns + ", i FROM mv WHERE ts >= '2024-01-03' ORDER BY ts"
            );
            // INT NULL becomes LONG NULL, not the INT's smallest value; the INT extremes keep their values.
            assertQuery("SELECT ts, i FROM mv WHERE ts IN '2024-01-03'").timestamp("ts").noLeakCheck().returns("""
                    ts\ti
                    2024-01-03T00:00:00.000000Z\tnull
                    2024-01-03T01:00:00.000000Z\t-2147483647
                    2024-01-03T02:00:00.000000Z\t2147483647
                    2024-01-03T03:00:00.000000Z\t0
                    """);
            // The rows written before the view dropped i keep NULL in the new column.
            assertQuery("SELECT count() FROM mv WHERE i IS NULL").noLeakCheck().noRandomAccess().expectSize().returns("count\n9\n");
        });
    }

    @Test
    public void testFailingColumnNamedOnlyWhenItsValueFailed() throws Exception {
        // Two view columns fed from text: x through a conversion the writer row performs, y through one the copier
        // performs. The copy stops at the first column, in query order, whose value does not convert. The error
        // names the column only when it is the copier's own conversion that failed; otherwise it keeps today's text.
        assertMemoryLeak(() -> {
            for (int i = 0; i < ROW_CONVERSIONS.length; i++) {
                final String[] rowConversion = ROW_CONVERSIONS[i];
                final String[] copierConversion = COPIER_CONVERSIONS[i % COPIER_CONVERSIONS.length];
                final String rowError = rowConversion[4];
                final String copierError = copierConversion[4] + " [from=y, to=y]";
                // only x fails
                assertValueError(rowConversion, copierConversion, true, rowConversion[1], copierConversion[2], rowError);
                // only y fails
                assertValueError(rowConversion, copierConversion, true, rowConversion[2], copierConversion[1], copierError);
                // both fail, x first: the replay must not name y, whose value is not the one the error reports
                assertValueError(rowConversion, copierConversion, true, rowConversion[1], copierConversion[1], rowError);
                // both fail, y first
                assertValueError(rowConversion, copierConversion, false, rowConversion[1], copierConversion[1], copierError);
            }
        });
    }

    @Test
    public void testFailingColumnNamedPastNullAndEmptyRowConversionText() throws Exception {
        // A NULL text, and an empty one for LONG256, converts to NULL in the writer row. x holds one, and the copy
        // goes on to y, whose value fails: the error names y.
        assertMemoryLeak(() -> {
            for (int i = 0; i < ROW_CONVERSIONS.length; i++) {
                final String[] rowConversion = ROW_CONVERSIONS[i];
                final String[] copierConversion = COPIER_CONVERSIONS[i % COPIER_CONVERSIONS.length];
                final String copierError = copierConversion[4] + " [from=y, to=y]";
                assertValueError(rowConversion, copierConversion, true, "NULL", copierConversion[1], copierError);
                if (rowConversion[3].equals("LONG256")) {
                    assertValueError(rowConversion, copierConversion, true, "''", copierConversion[1], copierError);
                }
            }
        });
    }

    @Test
    public void testRefreshAfterViewColumnReAddedWithOtherType() throws Exception {
        assertRefreshAfterViewColumnReAdded("LONG");
    }

    @Test
    public void testRefreshAfterViewColumnReAddedWithSameType() throws Exception {
        assertRefreshAfterViewColumnReAdded("INT");
    }

    @Test
    public void testRefreshRebuildsCopierBetweenBatchCommitsAfterViewColumnReAdded() throws Exception {
        // The view drops sym and adds it back before the refresh, then drops price between two batch commits of
        // the refresh. The copier the refresh rebuilds for the rest of its rows maps the re-added sym.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_INSERT_AS_SELECT_BATCH_SIZE, 10);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 2);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym VARCHAR, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH WAL");
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT sym, last(price) AS price, ts FROM trades SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN sym");
            drainWalQueue();
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN sym VARCHAR");
            drainWalQueue();
            execute("INSERT INTO trades SELECT concat('sym', x), x, timestamp_sequence('2022-02-24', 1000000L*60*60*2) FROM long_sequence(30)");
            drainWalQueue();

            final AtomicInteger batchCommits = new AtomicInteger();
            try (MatViewRefreshJob job = createMatViewRefreshJob()) {
                job.setOnBatchCommittedForTesting(() -> {
                    if (batchCommits.getAndIncrement() == 0) {
                        try {
                            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN price");
                        } catch (SqlException e) {
                            throw new AssertionError(e);
                        }
                    }
                });
                drainMatViewQueue(job);
            }
            drainWalQueue();
            Assert.assertTrue(batchCommits.get() > 1);
            assertViewValid("price_1h");
            // one base row every 2 hours from 2022-02-24, one view bucket each
            final StringBuilder expected = new StringBuilder("ts\tsym\n");
            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
            for (int i = 1; i <= 30; i++) {
                expected.append(LocalDateTime.of(2022, 2, 24, 0, 0).plusHours(2L * (i - 1)).format(formatter))
                        .append('\t').append("sym").append(i).append('\n');
            }
            assertQuery("price_1h ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(expected.toString());
        });
    }

    @Test
    public void testValueErrorAfterViewColumnReAddedWithOtherType() throws Exception {
        assertValueErrorAfterViewColumnReAdded("LONG");
    }

    @Test
    public void testValueErrorAfterViewColumnReAddedWithSameType() throws Exception {
        assertValueErrorAfterViewColumnReAdded("INT");
    }

    @Test
    public void testValueErrorKeptWhenReplayFails() throws Exception {
        // The replay that names the failing column fails itself. The refresh fails with the value's error, as it is,
        // and the view keeps its rows. Two views read the base, and the replay fails for the first one only: the
        // second one's error names the column.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, i INT, d DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 1, 1.5)");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv1 AS (SELECT * FROM base) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW mv2 AS (SELECT * FROM base) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base ALTER COLUMN d TYPE VARCHAR");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('2024-01-02T00:00:00.000000Z', 2, 'abc')");
            drainWalQueue();

            final AtomicInteger replays = new AtomicInteger();
            try (MatViewRefreshJob job = createMatViewRefreshJob()) {
                job.setOnCastErrorReplayForTesting(() -> {
                    if (replays.incrementAndGet() == 1) {
                        throw new IllegalStateException("replay failure");
                    }
                });
                drainMatViewQueue(job);
            }
            drainWalQueue();
            Assert.assertEquals(2, replays.get());
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() ORDER BY invalidation_reason")
                    .noLeakCheck()
                    .returns("""
                            view_status\tinvalidation_reason
                            invalid\tinconvertible value: `abc` [VARCHAR -> DOUBLE]
                            invalid\tinconvertible value: `abc` [VARCHAR -> DOUBLE] [from=d, to=d]
                            """);
            for (String viewName : new String[]{"mv1", "mv2"}) {
                assertQuery(viewName + " ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                        ts\ti\td
                        2024-01-01T00:00:00.000000Z\t1\t1.5
                        """);
            }
        });
    }

    // Base (ts, i, d, s); a passthrough view drops i, the designated timestamp's neighbour, adds it back with the
    // given type, and adds a column the SQL does not produce. Incremental refresh, RANGE and FULL write the view
    // through the name-mapped copier.
    private void assertRefreshAfterViewColumnReAdded(String reAddedType) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, i INT, d DOUBLE, s VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 1, 1.5, 'a')");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN i");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW mv ADD COLUMN i " + reAddedType);
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW mv ADD COLUMN extra INT");
            drainWalAndMatViewQueues();

            execute("INSERT INTO base VALUES ('2024-01-02T00:00:00.000000Z', 2, 2.5, 'b')");
            drainWalAndMatViewQueues();
            assertViewValid("mv");
            assertMapping("mv", "extra", "");
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\td\ts\ti\textra
                    2024-01-01T00:00:00.000000Z\t1.5\ta\tnull\tnull
                    2024-01-02T00:00:00.000000Z\t2.5\tb\t2\tnull
                    """);

            // RANGE recomputes the first day, which now gets i. A RANGE reports no view status of its own until
            // the next refresh, so the FULL below checks it.
            execute("REFRESH MATERIALIZED VIEW mv RANGE FROM '2024-01-01' TO '2024-01-01T12:00:00.000000Z'");
            drainWalAndMatViewQueues();
            assertQuery("SELECT invalidation_reason, last_refresh_error FROM materialized_views()").noLeakCheck().noRandomAccess()
                    .returns("invalidation_reason\tlast_refresh_error\n\t\n");
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\td\ts\ti\textra
                    2024-01-01T00:00:00.000000Z\t1.5\ta\t1\tnull
                    2024-01-02T00:00:00.000000Z\t2.5\tb\t2\tnull
                    """);

            execute("INSERT INTO base VALUES ('2024-01-03T00:00:00.000000Z', 3, 3.5, 'c')");
            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            assertViewValid("mv");
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\td\ts\ti\textra
                    2024-01-01T00:00:00.000000Z\t1.5\ta\t1\tnull
                    2024-01-02T00:00:00.000000Z\t2.5\tb\t2\tnull
                    2024-01-03T00:00:00.000000Z\t3.5\tc\t3\tnull
                    """);
        });
    }

    private void assertMapping(String viewName, String unmappedViewColumns, String ignoredQueryColumns) throws Exception {
        assertQuery("SELECT unmapped_view_columns, ignored_query_columns FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("unmapped_view_columns\tignored_query_columns\n" + unmappedViewColumns + '\t' + ignoredQueryColumns + '\n');
    }

    // Base (ts, a, b) feeds view columns x from a and y from b. The query produces x before y, or y before x.
    private void assertValueError(
            String[] xConversion,
            String[] yConversion,
            boolean isXFirst,
            String aValue,
            String bValue,
            String expectedReason
    ) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, a " + xConversion[0] + ", b " + yConversion[0] + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, a, b FROM base) PARTITION BY DAY");
        execute("ALTER MATERIALIZED VIEW mv ADD COLUMN x " + xConversion[3]);
        execute("ALTER MATERIALIZED VIEW mv ADD COLUMN y " + yConversion[3]);
        drainWalAndMatViewQueues();
        execute("ALTER MATERIALIZED VIEW mv AS (SELECT ts, a, b, " + (isXFirst ? "a AS x, b AS y" : "b AS y, a AS x") + " FROM base)");
        drainWalAndMatViewQueues();
        execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', " + aValue + ", " + bValue + ")");
        drainWalAndMatViewQueues();
        final String label = xConversion[0] + " -> " + xConversion[3] + " = " + aValue + ", " + yConversion[0] + " -> " + yConversion[3]
                + " = " + bValue + (isXFirst ? ", x first" : ", y first");
        try {
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views()")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("view_status\tinvalidation_reason\ninvalid\t" + expectedReason + '\n');
        } catch (AssertionError e) {
            throw new AssertionError(label, e);
        }
        execute("DROP MATERIALIZED VIEW mv");
        execute("DROP TABLE base");
        drainWalAndMatViewQueues();
    }

    // Base (ts, i, d); a passthrough view drops i and adds it back. The base then changes d to VARCHAR, and a
    // value that does not convert to the view's DOUBLE fails the refresh: the view goes invalid with the value's
    // error, which names d, and keeps its rows.
    private void assertValueErrorAfterViewColumnReAdded(String reAddedType) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, i INT, d DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 1, 1.5)");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN i");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW mv ADD COLUMN i " + reAddedType);
            drainWalAndMatViewQueues();

            execute("ALTER TABLE base ALTER COLUMN d TYPE VARCHAR");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('2024-01-02T00:00:00.000000Z', 2, 'abc')");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views()").noLeakCheck().noRandomAccess().returns("""
                    view_status\tinvalidation_reason
                    invalid\tinconvertible value: `abc` [VARCHAR -> DOUBLE] [from=d, to=d]
                    """);
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\td\ti
                    2024-01-01T00:00:00.000000Z\t1.5\tnull
                    """);
        });
    }

    private void assertViewValid(String viewName) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\nvalid\t\n");
    }

    private static MatViewState getViewState(String viewName) {
        final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName(viewName));
        Assert.assertNotNull(state);
        return state;
    }

    // Four rows an hour apart: every nullable column NULL, the smallest values, the largest values, and special
    // values (empty and multi-byte text, -0.0, the smallest positive FLOAT and DOUBLE). BINARY has no literal, so
    // the rows go through a staging table, and a seeded generator fills the BINARY column.
    private static void insertAllTypesRows(String day) throws SqlException {
        final String d76 = "9".repeat(66) + "." + "9".repeat(10);
        final String d38 = "9".repeat(36) + ".99";
        final String longText = "x".repeat(1_000);
        execute("CREATE TABLE src AS (SELECT * FROM base WHERE 1 = 2)");
        execute("ALTER TABLE src DROP COLUMN bin");
        execute("INSERT INTO src VALUES "
                + "('" + day + "T00:00:00.000000Z', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
                + "NULL, NULL, 0, 0, NULL, NULL, NULL, NULL, false, NULL, NULL, NULL, NULL, NULL, NULL, NULL), "
                + "('" + day + "T01:00:00.000000Z', '0.0.0.1', '0x01', '0', '00', '0000', '00000000', "
                + "-9.9m, -999.9m, -9999999.99m, -999999999999999.999m, -" + d38 + "m, -" + d76 + "m, "
                + "ARRAY[-1.7976931348623157E308], ARRAY[[-1.0, -2.0], [-3.0, -4.0]], -128, -32768, 'A', "
                + "'1969-12-31T23:59:59.999Z', '1969-12-31T23:59:59.999999999Z', '00000000-0000-0000-0000-000000000001', "
                + "false, 'a', 'a', 'a', -2147483647, -9223372036854775807, -3.4028234663852886E38, -1.7976931348623157E308), "
                + "('" + day + "T02:00:00.000000Z', '255.255.255.255', '0x" + "f".repeat(64) + "', 'z', 'zz', 'zzzz', 'zzzzzzzz', "
                + "9.9m, 999.9m, 9999999.99m, 999999999999999.999m, " + d38 + "m, " + d76 + "m, "
                + "ARRAY[1.7976931348623157E308], ARRAY[[1.0, 2.0], [3.0, 4.0]], 127, 32767, 'ы', "
                + "'9999-12-31T23:59:59.999Z', '2261-12-31T23:59:59.999999999Z', 'ffffffff-ffff-ffff-ffff-ffffffffffff', "
                + "true, 'é𝄞', '" + longText + "é𝄞', '" + longText + "é𝄞', 2147483647, 9223372036854775807, 3.4028234663852886E38, 1.7976931348623157E308), "
                + "('" + day + "T03:00:00.000000Z', '10.0.0.1', '0x02', 'u', 'u3', 'u33d', 'u33dbcde', "
                + "0.0m, 0.0m, 0.0m, 0.0m, 0.0m, 0.0m, "
                + "ARRAY[-0.0, 4.9E-324], ARRAY[[0.0]], 0, 0, 'é', "
                + "'1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000000000Z', '11111111-1111-1111-1111-111111111111', "
                + "true, '', '', '', 0, 0, 1.401298464324817E-45, -0.0)");
        sqlExecutionContext.setRandom(new Rnd(1_234_567L, 7_654_321L));
        execute("INSERT INTO base SELECT src.*, CASE WHEN ts = '" + day + "T00:00:00.000000Z' THEN NULL ELSE rnd_bin(1, 32, 0) END FROM src");
        execute("DROP TABLE src");
    }
}
