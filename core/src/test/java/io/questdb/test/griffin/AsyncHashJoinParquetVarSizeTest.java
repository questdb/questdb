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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * Joins whose probe tasks would read a STRING, VARCHAR, BINARY or ARRAY payload column of a build
 * that reads Parquet frames, a table with Parquet partitions or {@code read_parquet()}, keep the
 * ordinary plan: a probe slot's reader decodes such a payload through its share of the Parquet
 * decode budget, which the payload outgrows, and the payload copy holds fixed-size columns alone. A
 * later join of a stack wraps the operator of the joins before it as an ordinary join. The tasks
 * read such columns for an ON residual or a post-join WHERE over them, and, once an execution swaps
 * the first join's inputs, for a later join's keys and filters over the left input's columns, which
 * the first stage then holds. The same shapes over native builds, over fixed-size Parquet payload
 * columns, or reading the left input's columns from the probe scan, run the operator. Every result
 * equals the ordinary plan's, in order.
 */
public class AsyncHashJoinParquetVarSizeTest extends AbstractCairoTest {
    // A predicate over each var-size payload type of d and a column of t, so that no conjunct reads
    // the build alone, and one over a fixed-size column.
    private static final String[] FIXED_SIZE_PREDICATES = {"d.w > t.v", "d.dts > t.ts"};
    private static final String[] VAR_SIZE_PREDICATES = {
            "length(d.str) > t.v % 4",
            "d.vc <> t.note",
            "length(d.bin) <> t.v % 3",
            "d.arr[1] <> t.v % 5",
    };

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // read_parquet() reads files under the input root.
        inputRoot = root;
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testFiltersOverVarSizeParquetPayloads() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(5, 5);
            for (String build : new String[]{"dn", "dp", "read_parquet('d.parquet')"}) {
                final boolean isParquet = !build.equals("dn");
                for (String predicate : VAR_SIZE_PREDICATES) {
                    for (String sql : new String[]{
                            "SELECT t.v, d.w FROM t LEFT JOIN " + build + " d ON t.k = d.k AND " + predicate,
                            "SELECT t.v, d.w, d.vc FROM t LEFT JOIN " + build + " d ON t.k = d.k WHERE " + predicate,
                    }) {
                        if (isParquet) {
                            assertOrdinaryPlan(sql, sqlExecutionContext);
                        } else {
                            assertParallelHashJoin(sql, sqlExecutionContext);
                        }
                    }
                }
                // The consumer alone reads the var-size payload, and the filters read fixed-size columns.
                for (String predicate : FIXED_SIZE_PREDICATES) {
                    assertParallelHashJoin("SELECT t.v, d.w, d.str, d.vc, d.bin, d.arr FROM t LEFT JOIN " + build + " d ON t.k = d.k AND " + predicate, sqlExecutionContext);
                    assertParallelHashJoin("SELECT t.v, d.str, d.vc FROM t LEFT JOIN " + build + " d ON t.k = d.k WHERE " + predicate, sqlExecutionContext);
                }
                // A filter over the probe's own var-size column reads the probe scan.
                assertParallelHashJoin("SELECT t.v, d.vc FROM t LEFT JOIN " + build + " d ON t.k = d.k AND length(t.note) > d.w % 5", sqlExecutionContext);
            }
            // An INNER join's post-join WHERE.
            assertOrdinaryPlan("SELECT t.v, d.w FROM t JOIN dp d ON t.k = d.k WHERE d.vc <> t.note", sqlExecutionContext);
            assertParallelHashJoin("SELECT t.v, d.w FROM t JOIN dn d ON t.k = d.k WHERE d.vc <> t.note", sqlExecutionContext);
            assertParallelHashJoin("SELECT t.v, d.vc FROM t JOIN dp d ON t.k = d.k WHERE d.w <> t.v", sqlExecutionContext);
        });
    }

    @Test
    public void testLaterJoinOverEarlierParquetPayload() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(5, 5);
            for (String build : new String[]{"dn", "dp", "read_parquet('d.parquet')"}) {
                final int stageCount = build.equals("dn") ? 2 : 1;
                // The later join's residual and post-join WHERE read the first join's var-size payload.
                assertLaterJoin("SELECT t.v, d.w, e.w ew FROM t LEFT JOIN " + build + " d ON t.k = d.k "
                        + "LEFT JOIN dn e ON t.lk = e.k AND length(d.vc) < e.w + t.v", stageCount);
                assertLaterJoin("SELECT t.v, d.w, e.w ew FROM t LEFT JOIN " + build + " d ON t.k = d.k "
                        + "LEFT JOIN dn e ON t.lk = e.k WHERE length(d.str) > e.w % 7", stageCount);
                assertLaterJoin("SELECT t.v, d.w, e.w ew FROM t LEFT JOIN " + build + " d ON t.k = d.k "
                        + "JOIN dn e ON t.lk = e.k WHERE d.arr[1] <> e.w % 3", stageCount);
                // Over the first join's fixed-size payload, every join runs in the operator.
                assertLaterJoin("SELECT t.v, d.w, d.vc, e.w ew FROM t LEFT JOIN " + build + " d ON t.k = d.k "
                        + "LEFT JOIN dn e ON t.lk = e.k AND d.w < e.w + t.v", 2);
            }
            // The later join's own var-size Parquet payload keeps it out of the operator, and the first
            // join's runs in it.
            assertLaterJoin("SELECT t.v, d.w, e.w ew FROM t LEFT JOIN dn d ON t.k = d.k "
                    + "LEFT JOIN dp e ON t.lk = e.k AND length(e.bin) <> t.v % 3", 1);
        });
    }

    @Test
    public void testLiveViewBuild() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE base (ts TIMESTAMP, k INT, vc VARCHAR, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, k, vc, y, " +
                    "sum(y) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) sy FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 1, 'n1', 1.0), ('2024-01-01T00:00:30.000000Z', 2, 'n2', 2.0), " +
                        "('2024-01-01T00:01:00.000000Z', 1, NULL, 3.0), ('2024-01-01T00:01:30.000000Z', 3, '', 4.0)");
                drainWalQueue();
                //noinspection StatementWithEmptyBody
                for (int i = 0; i < 64 && job.run(); i++) {
                }
                drainWalQueue();
            }
            // A live view's scan reads no Parquet frame, so the probe tasks read its VARCHAR where it
            // lives.
            assertParallelHashJoin("SELECT t.v, lv.y FROM t LEFT JOIN lv ON t.k = lv.k AND lv.vc <> t.note", sqlExecutionContext);
            assertParallelHashJoin("SELECT t.v, lv.y FROM t LEFT JOIN lv ON t.k = lv.k WHERE lv.vc = t.note", sqlExecutionContext);
        });
    }

    @Test
    public void testSwappedStackOverParquetLeftInput() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // l, smaller than r, which a first join over both hashes, swapping the inputs.
            for (String l : new String[]{"ln", "lp"}) {
                execute("CREATE TABLE " + l + " AS (" +
                        "SELECT (x % 4)::INT k, (x % 3)::LONG lk, 'str' || (x % 3) str, ('vc' || (x % 4))::VARCHAR vc, x v, " +
                        "timestamp_sequence('2024-01-01', 900_000_000) ts FROM long_sequence(12)) TIMESTAMP(ts) PARTITION BY HOUR");
            }
            execute("ALTER TABLE lp CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-01T02'");
            execute("CREATE TABLE r AS (SELECT (x % 5)::INT k, x w FROM long_sequence(40))");
            execute("CREATE TABLE e AS (SELECT ('vc' || (x % 3))::VARCHAR vc, 'str' || (x % 2) str, x::LONG lk, x * 100 ew FROM long_sequence(4))");
            for (String l : new String[]{"ln", "lp"}) {
                final boolean isParquet = l.equals("lp");
                final String first = "SELECT l.v, r.w, e.ew FROM " + l + " l JOIN r ON l.k = r.k ";
                // The later join's key reads a var-size column of l, which the swapped first stage holds.
                assertLaterJoin(first + "JOIN e ON l.vc = e.vc", isParquet ? 1 : 2, true);
                assertLaterJoin(first + "LEFT JOIN e ON l.str = e.str", isParquet ? 1 : 2, true);
                // ... or its residual or post-join WHERE.
                assertLaterJoin(first + "LEFT JOIN e ON l.lk = e.lk AND e.vc <> l.vc", isParquet ? 1 : 2, true);
                assertLaterJoin(first + "JOIN e ON l.lk = e.lk WHERE length(l.str) > e.ew % 7", isParquet ? 1 : 2, true);
                // A fixed-size key, and filters over fixed-size columns, run in the operator.
                assertLaterJoin(first + "JOIN e ON l.lk = e.lk", 2, true);
                assertLaterJoin(first + "LEFT JOIN e ON l.lk = e.lk AND e.ew > l.v", 2, true);
                // The first join's own post-join WHERE over l's var-size column.
                final String postFiltered = "SELECT l.v, r.w FROM " + l + " l JOIN r ON l.k = r.k WHERE length(l.vc) > r.w % 4";
                if (isParquet) {
                    assertOrdinaryPlan(postFiltered, sqlExecutionContext);
                } else {
                    assertSwapped(postFiltered, 1);
                }
                // A filtered left input never swaps, so its frames are the probe's, whatever they hold.
                assertParallelHashJoin("SELECT l.v, r.w FROM (SELECT * FROM " + l + " WHERE v > 1) l JOIN r ON l.k = r.k WHERE length(l.vc) > r.w % 4", sqlExecutionContext);
                assertParallelHashJoin("SELECT l.v, r.w, e.ew FROM (SELECT * FROM " + l + " WHERE v > 1) l JOIN r ON l.k = r.k JOIN e ON l.vc = e.vc", sqlExecutionContext);
            }
        });
    }

    // Asserts that the query runs the operator with the given stages, under an ordinary join for each
    // join past them, and returns the ordinary plan's rows, in order.
    private static void assertLaterJoin(String sql, int stageCount) throws SqlException {
        assertLaterJoin(sql, stageCount, false);
    }

    // As assertLaterJoin(String, int), for a first join that may swap its inputs, as the plan says,
    // and, when the operator runs every join, does in an execution, as given.
    private static void assertLaterJoin(String sql, int stageCount, boolean isSwapped) throws SqlException {
        final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
        final int joinCount = countMatches(sql, " JOIN ");
        Assert.assertEquals(sql + '\n' + plan, stageCount, getStageCount(plan));
        Assert.assertEquals(sql + '\n' + plan, joinCount - stageCount, countMatches(plan, "Hash Join Light") + countMatches(plan, "Hash Left Outer Join"));
        Assert.assertEquals(sql + '\n' + plan, isSwapped, plan.contains("inputSwap: when the left input is smaller"));
        // An ordinary join hides its master from the operator's lookup.
        if (stageCount == joinCount) {
            assertSwapped(sql, stageCount, isSwapped);
        }
    }

    private static void assertSwapped(String sql, int stageCount) throws SqlException {
        assertParallelHashJoin(sql, sqlExecutionContext);
        assertSwapped(sql, stageCount, true);
    }

    private static void assertSwapped(String sql, int stageCount, boolean isSwapped) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            Assert.assertEquals(sql, stageCount, join.getSpec().getStageCount());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(sql, cursor.hasNext());
                Assert.assertEquals(sql, isSwapped, join.getAtom().isSwapped());
            }
        }
    }

    // The stages that the operator's plan names.
    private static int getStageCount(String plan) {
        final int start = plan.indexOf("stages: ") + "stages: ".length();
        int end = start;
        while (end < plan.length() && Character.isDigit(plan.charAt(end))) {
            end++;
        }
        return Integer.parseInt(plan.substring(start, end));
    }

    private static int countMatches(String text, String sub) {
        int count = 0;
        for (int i = text.indexOf(sub); i > -1; i = text.indexOf(sub, i + sub.length())) {
            count++;
        }
        return count;
    }

    // t, the probe, with a VARCHAR of its own; dn and dp, dimensions with a payload of every var-size
    // type, NULL and empty values and 4-byte UTF-8 among them, dp over Parquet partitions, and
    // d.parquet, dn's first partition, for read_parquet().
    private static void createTables() throws Exception {
        execute("CREATE TABLE t AS (" +
                "SELECT CASE WHEN x % 13 = 0 THEN NULL ELSE (x % 9)::INT END k, (x % 6)::INT lk, x v, " +
                "CASE WHEN x % 4 = 0 THEN NULL ELSE ('n' || (x % 7))::VARCHAR END note, " +
                "timestamp_sequence('2024-01-01', 1_800_000_000) ts FROM long_sequence(96)) TIMESTAMP(ts) PARTITION BY DAY");
        for (String d : new String[]{"dn", "dp"}) {
            execute("CREATE TABLE " + d + " AS (" +
                    "SELECT CASE WHEN x % 11 = 0 THEN NULL ELSE (x % 7)::INT END k, x w, " +
                    "CASE WHEN x % 5 = 0 THEN NULL WHEN x % 5 = 1 THEN '' WHEN x % 5 = 2 THEN '\uD83D\uDE00' || x " +
                    "     ELSE 'a longer string, row ' || x END::STRING str, " +
                    "CASE WHEN x % 6 = 0 THEN NULL WHEN x % 6 = 1 THEN '' ELSE ('n' || (x % 8))::VARCHAR END vc, " +
                    "CASE WHEN x % 4 = 0 THEN NULL WHEN x % 4 = 1 THEN from_base64('') ELSE rnd_bin(1, 40, 0) END bin, " +
                    "CASE WHEN x % 7 = 0 THEN NULL::DOUBLE[] WHEN x % 7 = 1 THEN ARRAY[]::DOUBLE[] " +
                    "     ELSE ARRAY[(x % 9)::DOUBLE, -x::DOUBLE] END arr, " +
                    "timestamp_sequence('2024-01-01', 4_320_000_000) dts FROM long_sequence(60)) TIMESTAMP(dts) PARTITION BY DAY");
        }
        execute("ALTER TABLE dp CONVERT PARTITION TO PARQUET WHERE dts < '2024-01-03'");
        try (
                Path path = new Path();
                PartitionDescriptor descriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader("dn")
        ) {
            path.of(root).concat("d.parquet");
            engine.getConfiguration().getFilesFacade().remove(path.$());
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            PartitionEncoder.encode(descriptor, path);
            Assert.assertTrue(Files.exists(path.$()));
        }
    }
}
