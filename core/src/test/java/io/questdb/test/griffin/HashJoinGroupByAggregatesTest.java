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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.HashJoinGroupByAggregates;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.assertDifferential;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.context;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.fused;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.plan;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.result;

/**
 * Fused on/off differential tests for the aggregates that HashJoinGroupByAggregates admits. The positive test
 * runs on worker threads, so partial results from several slots merge in the fused plan, while the ordinary
 * plan aggregates serially.
 */
public class HashJoinGroupByAggregatesTest extends AbstractCairoTest {
    // Two-argument aggregates declare DOUBLE parameters. The function parser converts these column types.
    // CHAR converts too, but a NULL CHAR fails the conversion, so CHAR arguments run separately.
    private static final String[] BINARY_ARGUMENTS = {"d", "f", "l", "i", "h", "y", "dt", "t"};
    private static final String[] BINARY_FUNCTIONS = {"covar_samp", "covar_pop", "corr", "regr_slope", "regr_r2",
            "regr_intercept", "weighted_stddev_rel", "weighted_stddev_freq", "weighted_avg", "vwap"};
    private static final int CHUNK_SIZE = 24;
    // DOUBLE results depend on how rows split into partial results: merges divide by counts (stddev, variance,
    // skewness, kurtosis, covariance, corr, regression, weighted stddev), geomean goes through log and exp, and
    // ksum's merge overwrites the destination compensation. The data set holds multiples of 0.25, so a dropped
    // or doubled partial result moves a result by far more than this bound, relative to max(1, |result|).
    // Arguments stay below 1e5, so the rounding of a cancelling covariance stays below it as well. FLOAT
    // results compare exactly: only sum, min and max return FLOAT, and sums of these values are exact in FLOAT.
    private static final double DOUBLE_TOLERANCE = 1e-9;
    private static final String FUSED = "Async Hash Join Group By";
    private static final String[] JOINS = {" JOIN ", " LEFT JOIN ", " RIGHT JOIN "};

    @Test
    public void testEveryAdmittedAggregateMatchesOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            TestWorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(pool, (db, compiler, ctx) -> {
                createTables(db, ctx);
                ObjList<String> aggregates = new ObjList<>();
                addUnaryAggregates("r", aggregates);
                addUnaryAggregates("p", aggregates);
                addBinaryAggregates(aggregates);
                // The CHAR column holds no NULL, and an INNER join null-extends nothing.
                ObjList<String> charAggregates = new ObjList<>();
                for (int f = 0; f < BINARY_FUNCTIONS.length; f++) {
                    String other = BINARY_ARGUMENTS[f % BINARY_ARGUMENTS.length];
                    charAggregates.add(BINARY_FUNCTIONS[f] + "(r.c, p." + other + ")");
                    charAggregates.add(BINARY_FUNCTIONS[f] + "(r." + other + ", p.c)");
                }
                ObjHashSet<Class<?>> fusedClasses = new ObjHashSet<>();
                try (SqlExecutionContextImpl context = context(db, 4)) {
                    context.changePageFrameSizes(1, 4);
                    assertChunks(aggregates, JOINS, context, fusedClasses);
                    assertChunks(charAggregates, new String[]{JOINS[0]}, context, fusedClasses);
                }
                StringBuilder missing = new StringBuilder();
                ObjList<Class<?>> supported = HashJoinGroupByAggregates.getSupportedClasses();
                for (int i = 0, n = supported.size(); i < n; i++) {
                    if (!fusedClasses.contains(supported.getQuick(i))) {
                        missing.append(supported.getQuick(i).getName()).append('\n');
                    }
                }
                Assert.assertEquals("admitted classes that no fused plan compiled", "", missing.toString());
                Assert.assertEquals(supported.size(), fusedClasses.size());
            }, configuration, LOG);
        });
    }

    @Test
    public void testExcludedAggregatesKeepOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables(engine, sqlExecutionContext);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 4);
                for (String join : JOINS) {
                    String from = " FROM r" + join + "p ON r.k = p.k";
                    for (String side : new String[]{"r", "p"}) {
                        for (String aggregate : new String[]{
                                // Joined pairs have no order.
                                "first(" + side + ".d)", "last(" + side + ".d)",
                                "first_not_null(" + side + ".d)", "last_not_null(" + side + ".d)",
                                // Ties resolve by row order.
                                "arg_min(" + side + ".d, " + side + ".l)", "arg_max(" + side + ".d, " + side + ".l)",
                                // No parallel merge.
                                "isOrdered(" + side + ".l)", "haversine_dist_deg(" + side + ".d, " + side + ".d, " + side + ".ts)",
                                // Per-group state outside the map value.
                                "count_distinct(" + side + ".i)", "approx_count_distinct(" + side + ".l)",
                                "string_agg(" + side + ".s::STRING, ',')", "string_distinct_agg(" + side + ".s, ',')",
                                "approx_percentile(" + side + ".d + 6, 0.5)", "approx_percentile(" + side + ".l + 100, 0.5)",
                                "approx_median(" + side + ".d + 6)", "mode(" + side + ".b)", "mode(" + side + ".d)",
                                "min(" + side + ".s::STRING)", "max(" + side + ".s::VARCHAR)",
                                "array_agg(" + side + ".d)",
                                // Admitted classes over argument types outside their registry entries.
                                "sum(" + side + ".y)", "avg(" + side + ".y)", "min(" + side + ".y)", "max(" + side + ".y)",
                                "avg(" + side + ".b)", "min(" + side + ".b)", "max(" + side + ".b)",
                                "avg(" + side + ".f)", "count(" + side + ".ts)", "count(" + side + ".i::SHORT)"
                        }) {
                            assertDifferential("SELECT " + aggregate + from, context, false);
                            assertDifferential("SELECT r.g rg, p.g pg, " + aggregate + from + " ORDER BY rg, pg", context, false);
                        }
                        // twap() takes the designated timestamp of the join, so some spellings fail to compile.
                        // The ordinary compile reports the error.
                        String twap = "SELECT twap(" + side + ".d, " + side + ".ts)" + from;
                        String expected = outcome(twap, context, false);
                        Assert.assertEquals(twap, expected, outcome(twap, context, true));
                        if (!expected.startsWith("compile error:")) {
                            try (RecordCursorFactory factory = engine.select(twap, context)) {
                                Assert.assertFalse(twap, plan(factory, context).contains(FUSED));
                            }
                        }
                        // An argument that is not stable within the execution keeps the ordinary plan.
                        for (String aggregate : new String[]{"sum(rnd_double())", "corr(" + side + ".d, rnd_double())",
                                "corr(rnd_double(), " + side + ".d)"}) {
                            try (RecordCursorFactory factory = engine.select("SELECT " + aggregate + from, context)) {
                                Assert.assertFalse(aggregate, plan(factory, context).contains(FUSED));
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testNullCharArgumentFailsInBothPlans() throws Exception {
        assertMemoryLeak(() -> {
            createTables(engine, sqlExecutionContext);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 4);
                // Unmatched rows null-extend the build's CHAR column, and a NULL CHAR fails the DOUBLE conversion
                // of a two-argument aggregate. The fused plan reports the ordinary plan's error.
                for (String sql : new String[]{
                        "SELECT corr(r.d, p.c) FROM r LEFT JOIN p ON r.k = p.k",
                        "SELECT r.g, weighted_avg(r.c, p.d) FROM r RIGHT JOIN p ON r.k = p.k ORDER BY r.g"
                }) {
                    try (RecordCursorFactory factory = engine.select(sql, context)) {
                        Assert.assertTrue(sql, plan(factory, context).contains(FUSED));
                    }
                    String error = outcome(sql, context, false);
                    Assert.assertTrue(error, error.startsWith("execution error:"));
                    Assert.assertEquals(sql, error, outcome(sql, context, true));
                }
            }
        });
    }

    private static void addBinaryAggregates(ObjList<String> sink) {
        // Rotating the argument types per position covers every type in both positions and on both sides
        // without the full cross product.
        for (int f = 0; f < BINARY_FUNCTIONS.length; f++) {
            String function = BINARY_FUNCTIONS[f];
            for (int a = 0; a < BINARY_ARGUMENTS.length; a++) {
                String x = BINARY_ARGUMENTS[a];
                String y = BINARY_ARGUMENTS[(a + f + 1) % BINARY_ARGUMENTS.length];
                sink.add(function + "(r." + x + ", p." + y + ")");
                sink.add(function + "(p." + x + ", r." + y + ")");
                // A negative weight makes a weighted stddev group NULL, and most groups hold one. abs() keeps
                // groups with values; DATE and TIMESTAMP weights are non-negative already.
                if (function.startsWith("weighted_stddev") && !y.equals("dt") && !y.equals("t")) {
                    sink.add(function + "(r." + x + ", abs(p." + y + "))");
                    sink.add(function + "(p." + x + ", abs(r." + y + "))");
                }
            }
        }
    }

    private static void addUnaryAggregates(String side, ObjList<String> sink) {
        String x = side + ".";
        // One cast per DECIMAL width: DECIMAL8 to DECIMAL256.
        for (String decimal : new String[]{x + "i::DECIMAL(2,0)", x + "d::DECIMAL(4,1)", x + "d::DECIMAL(9,2)",
                x + "l::DECIMAL(18,2)", x + "l::DECIMAL(38,2)", x + "d::DECIMAL(60,2)"}) {
            sink.add("avg(" + decimal + ")");
            sink.add("avg(" + decimal + ", 3)");
            sink.add("sum(" + decimal + ")");
            sink.add("min(" + decimal + ")");
            sink.add("max(" + decimal + ")");
            sink.add("count(" + decimal + ")");
        }
        for (String column : new String[]{"d", "i", "l", "h"}) {
            sink.add("avg(" + x + column + ")");
        }
        for (String column : new String[]{"d", "f", "i", "l", "h"}) {
            sink.add("sum(" + x + column + ")");
        }
        sink.add("sum(" + x + "l::LONG256)");
        sink.add("ksum(" + x + "d)");
        sink.add("nsum(" + x + "d)");
        sink.add("geomean(" + x + "d + 6)");
        for (String column : new String[]{"c", "dt", "d", "f", "i", "l", "h", "t", "ts", "ns", "i::IPv4"}) {
            sink.add("min(" + x + column + ")");
            sink.add("max(" + x + column + ")");
        }
        for (String column : new String[]{"y", "h", "i", "l"}) {
            sink.add("bit_and(" + x + column + ")");
            sink.add("bit_or(" + x + column + ")");
            sink.add("bit_xor(" + x + column + ")");
        }
        sink.add("bool_and(" + x + "b)");
        sink.add("bool_or(" + x + "b)");
        for (String argument : new String[]{"d", "f", "i", "l", "s", "i::IPv4", "l::LONG256", "s::STRING", "s::VARCHAR",
                "l::GEOHASH(5b)", "l::GEOHASH(10b)", "l::GEOHASH(20b)", "l::GEOHASH(40b)"}) {
            sink.add("count(" + x + argument + ")");
        }
        sink.add("count(to_uuid(" + x + "l, " + x + "i))");
        for (String function : new String[]{"stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop",
                "skewness", "skewness_samp", "skewness_pop", "kurtosis", "kurtosis_samp", "kurtosis_pop"}) {
            sink.add(function + "(" + x + "d)");
        }
    }

    private static void assertChunks(
            ObjList<String> aggregates,
            String[] joins,
            SqlExecutionContextImpl context,
            ObjHashSet<Class<?>> fusedClasses
    ) throws SqlException {
        for (int lo = 0; lo < aggregates.size(); lo += CHUNK_SIZE) {
            StringBuilder select = new StringBuilder("count() pairs");
            for (int i = lo, hi = Math.min(lo + CHUNK_SIZE, aggregates.size()); i < hi; i++) {
                select.append(", ").append(aggregates.getQuick(i));
            }
            for (String join : joins) {
                String from = " FROM r" + join + "p ON r.k = p.k";
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, Integer.MAX_VALUE);
                assertFusedMatchesOrdinary("SELECT " + select + from, context, fusedClasses);
                // Threshold 1 shards the keyed maps, so partial results also merge per shard.
                for (int threshold : new int[]{Integer.MAX_VALUE, 1}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                    assertFusedMatchesOrdinary("SELECT r.g rg, p.g pg, " + select + from + " ORDER BY rg, pg",
                            context, fusedClasses);
                }
            }
        }
    }

    private static void assertFusedMatchesOrdinary(
            String sql,
            SqlExecutionContextImpl context,
            ObjHashSet<Class<?>> fusedClasses
    ) throws SqlException {
        ObjList<Object> expected;
        context.setParallelHashJoinGroupByEnabled(false);
        try (RecordCursorFactory baseline = context.getCairoEngine().select(sql, context)) {
            Assert.assertFalse(sql, plan(baseline, context).contains(FUSED));
            expected = cells(baseline, context);
            context.setParallelHashJoinGroupByEnabled(true);
            try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
                Assert.assertTrue(sql, plan(factory, context).contains(FUSED));
                RecordMetadata metadata = factory.getMetadata();
                Assert.assertEquals(sql, baseline.getMetadata().getColumnCount(), metadata.getColumnCount());
                for (int col = 0, n = metadata.getColumnCount(); col < n; col++) {
                    Assert.assertEquals(sql, baseline.getMetadata().getColumnType(col), metadata.getColumnType(col));
                    Assert.assertEquals(sql, baseline.getMetadata().getColumnName(col), metadata.getColumnName(col));
                }
                ObjList<GroupByFunction> functions = fused(factory).getAtom().getFunctions().getGroupByFunctions(-1);
                for (int i = 0, n = functions.size(); i < n; i++) {
                    fusedClasses.add(functions.getQuick(i).getClass());
                }
                // The second read catches cursor reuse defects as well as wrong values.
                for (int read = 0; read < 2; read++) {
                    ObjList<Object> actual = cells(factory, context);
                    Assert.assertEquals(sql + "\n" + format(expected, metadata) + "\n" + format(actual, metadata),
                            "", mismatches(expected, actual, metadata));
                }
            }
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
        Assert.assertNull(context.getMemoryTracker());
    }

    private static ObjList<Object> cells(RecordCursorFactory factory, SqlExecutionContext context) throws SqlException {
        ObjList<Object> cells = new ObjList<>();
        RecordMetadata metadata = factory.getMetadata();
        StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(context)) {
            Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                for (int col = 0, n = metadata.getColumnCount(); col < n; col++) {
                    if (ColumnType.tagOf(metadata.getColumnType(col)) == ColumnType.DOUBLE) {
                        cells.add(record.getDouble(col));
                    } else {
                        sink.clear();
                        CursorPrinter.printColumn(record, metadata, col, sink);
                        cells.add(sink.toString());
                    }
                }
            }
        }
        return cells;
    }

    private static void createTables(CairoEngine engine, SqlExecutionContext context) throws SqlException {
        // Keys repeat on both sides. Key 4 exists only in r and key 7 only in p, so LEFT and RIGHT joins
        // null-extend rows. Every other nullable column holds NULLs, and r's day partitions hold 3, 45 and 12
        // rows. CHAR holds digits and no NULL, because a CHAR argument converts to DOUBLE by parsing the digit.
        for (String table : new String[]{"r", "p"}) {
            engine.execute("CREATE TABLE " + table + " (k INT, g SYMBOL, b BOOLEAN, y BYTE, h SHORT, c CHAR, i INT,"
                    + " l LONG, dt DATE, t TIMESTAMP, ns TIMESTAMP_NS, f FLOAT, d DOUBLE, s SYMBOL, ts TIMESTAMP)"
                    + " TIMESTAMP(ts) PARTITION BY DAY", context);
        }
        insertRows(engine, context, "r", 60, "CASE WHEN x % 11 = 0 THEN NULL ELSE (x % 5)::INT END", 0);
        insertRows(engine, context, "p", 40, "CASE WHEN x % 9 = 0 THEN NULL WHEN x % 13 = 0 THEN 7 ELSE (x % 4)::INT END", 1);
    }

    private static String format(ObjList<Object> cells, RecordMetadata metadata) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0, n = cells.size(), columns = metadata.getColumnCount(); i < n; i++) {
            sb.append(cells.getQuick(i)).append((i + 1) % columns == 0 ? '\n' : '\t');
        }
        return sb.toString();
    }

    private static void insertRows(
            CairoEngine engine,
            SqlExecutionContext context,
            String table,
            int rows,
            String key,
            int shift
    ) throws SqlException {
        engine.execute("INSERT INTO " + table + " SELECT " + key + ", ('g' || ((x + " + shift + ") % 3))::SYMBOL,"
                + " (x + " + shift + ") % 3 = 0, ((x + " + shift + ") % 7 - 3)::BYTE, ((x + " + shift + ") % 9 - 4)::SHORT,"
                + " (48 + (x + " + shift + ") % 5)::INT::CHAR,"
                + " CASE WHEN x % 7 = 0 THEN NULL ELSE ((x + " + shift + ") % 13 - 6)::INT END,"
                + " CASE WHEN x % 8 = 0 THEN NULL ELSE (x + " + shift + ") * 3 - 50 END,"
                + " CASE WHEN x % 9 = 0 THEN NULL ELSE ((x + " + shift + ") * 1_000)::DATE END,"
                + " CASE WHEN x % 12 = 0 THEN NULL ELSE ((x + " + shift + ") * 1_000)::TIMESTAMP END,"
                + " CASE WHEN x % 10 = 0 THEN NULL ELSE ((x + " + shift + ") * 1_000_000_123)::TIMESTAMP_NS END,"
                + " CASE WHEN x % 6 = 1 THEN NULL ELSE (((x + " + shift + ") % 17) * 0.25 - 2)::FLOAT END,"
                + " CASE WHEN x % 5 = 0 THEN NULL ELSE ((x + " + shift + ") % 23) * 0.5 - 5 END,"
                + " (CASE WHEN x % 4 = 0 THEN NULL ELSE 's' || ((x + " + shift + ") % 6) END)::SYMBOL,"
                + " ((CASE WHEN x <= 3 THEN x WHEN x <= 48 THEN 86_400 + x * 60 ELSE 172_800 + x * 60 END) * 1_000_000)::TIMESTAMP"
                + " FROM long_sequence(" + rows + ")", context);
    }

    private static boolean isClose(Object expected, Object actual) {
        if (expected instanceof Double e && actual instanceof Double a
                && !Double.isNaN(e) && !Double.isNaN(a) && !Double.isInfinite(e) && !Double.isInfinite(a)) {
            return Math.abs(e - a) <= DOUBLE_TOLERANCE * Math.max(1.0, Math.max(Math.abs(e), Math.abs(a)));
        }
        return expected.equals(actual);
    }

    private static String mismatches(ObjList<Object> expected, ObjList<Object> actual, RecordMetadata metadata) {
        if (expected.size() != actual.size()) {
            return "cell count: " + expected.size() + " != " + actual.size();
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0, n = expected.size(), columns = metadata.getColumnCount(); i < n; i++) {
            if (!isClose(expected.getQuick(i), actual.getQuick(i))) {
                sb.append("row ").append(i / columns).append(' ').append(metadata.getColumnName(i % columns))
                        .append(": ").append(expected.getQuick(i)).append(" != ").append(actual.getQuick(i)).append('\n');
            }
        }
        return sb.toString();
    }

    // Formats the result or the error of one plan, reading the result twice.
    private static String outcome(String sql, SqlExecutionContextImpl context, boolean isEnabled) throws Exception {
        context.setParallelHashJoinGroupByEnabled(isEnabled);
        try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
            try {
                return result(factory, context) + result(factory, context);
            } catch (CairoException e) {
                return "execution error: " + e.getFlyweightMessage();
            } catch (ImplicitCastException e) {
                return "execution error: " + e.getFlyweightMessage();
            }
        } catch (SqlException e) {
            return "compile error: [" + e.getPosition() + "] " + e.getFlyweightMessage();
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
    }
}
