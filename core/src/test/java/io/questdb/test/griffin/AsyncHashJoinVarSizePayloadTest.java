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

import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.assertOrdinaryPlan;
import static io.questdb.test.griffin.ParallelHashJoinTestUtils.assertParallelHashJoin;
import static io.questdb.test.griffin.ParallelHashJoinTestUtils.find;

/**
 * STRING, VARCHAR, BINARY and array payloads of the parallel hash join's builds, over native and
 * mixed native and Parquet partitions, read where the probes need them: by the consumer for the
 * joined rows, by the probe tasks for an ON residual and a post-join filter, and, in an execution
 * that swaps the first join's inputs, by the probe tasks for a later join's key, which comes from
 * the first join's build rows. Every result compares with the ordinary plan, in order. A payload
 * reader reads a native frame's values through the build's address table, so it binds its page
 * frame record only to read a Parquet frame. The probe tasks of a join never read such a payload
 * of a build with Parquet frames: the join keeps the ordinary plan instead, or, for a later join,
 * wraps the operator as an ordinary join.
 */
public class AsyncHashJoinVarSizePayloadTest extends AbstractCairoTest {
    // The shapes name the tables #t, #d and #e, and the joins whose payload readers they check.
    private static final String[] SHAPES = {
            // The consumer reads every var-size type, the second array below a column top in the first days.
            "SELECT t.v, d.k, d.str, d.vc, d.bin, d.arr, d.arr2 FROM #t t LEFT JOIN #d d ON t.k = d.k",
            // The probe tasks read them for an ON residual, which starts with a conjunct on both
            // inputs, so that no conjunct runs over the build rows instead.
            "SELECT t.v, d.vc, d.arr FROM #t t LEFT JOIN #d d ON t.k = d.k AND d.vc <> t.note AND length(d.str) > t.v % 4 "
                    + "AND length(d.bin) <> t.v % 3 AND d.arr[1] <> t.v % 5",
            // ... and for a post-join filter.
            "SELECT t.v, d.str, d.arr2 FROM #t t JOIN #d d ON t.k = d.k WHERE length(d.vc) > t.v % 6 OR d.arr2[1, 1] > t.v % 4",
    };
    private static final String[] STORAGES = {"native", "mixed"};
    // The first join hashes d, the smaller input, and the second join's key, d.vc, comes from its build rows.
    private static final String SWAP_SHAPE = "SELECT d.k, d.str, t.v, e.ew FROM #d d JOIN #t t ON d.k = t.k JOIN #e e ON d.vc = e.vc";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testVarSizePayloads() throws Exception {
        assertMemoryLeak(() -> {
            for (String storage : STORAGES) {
                createTables(storage);
            }
            sqlExecutionContext.changePageFrameSizes(5, 5);
            for (String storage : STORAGES) {
                final boolean isNative = storage.equals("native");
                for (int i = 0; i < SHAPES.length; i++) {
                    final String sql = SHAPES[i].replace("#t", "t_" + storage).replace("#d", "d_" + storage);
                    if (isNative || i == 0) {
                        assertParallelHashJoin(sql, sqlExecutionContext);
                        assertRecordBinds(sql, false, isNative ? 0 : 1);
                    } else {
                        // The probe tasks would read the payload of d's Parquet frames.
                        assertOrdinaryPlan(sql, sqlExecutionContext);
                    }
                }
                final String sql = SWAP_SHAPE.replace("#t", "t_" + storage).replace("#d", "d_" + storage).replace("#e", "e_native");
                final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
                if (isNative) {
                    assertRecordBinds(sql, true, 0, 0);
                } else {
                    // The second join's key, d.vc, would come from the Parquet frames of the swapped
                    // first stage's payload, so the second join wraps the operator as an ordinary join.
                    TestUtils.assertContains(plan, "stages: 1");
                    Assert.assertFalse(plan, plan.contains("stages: 2"));
                    TestUtils.assertContains(plan, "Hash Join Light");
                    // The first join alone, swapped: its consumer reads d's columns.
                    final String first = sql.substring(0, sql.indexOf(" JOIN e_native")).replace(", e.ew", "");
                    assertParallelHashJoin(first, sqlExecutionContext);
                    assertRecordBinds(first, true, 1);
                }
            }
        });
    }

    // Runs the query once, reading every column of every row, and reads, before its cursor closes, the
    // times each join's payload readers bound their records: none for a join whose expected count is
    // zero, at least that many otherwise.
    private static void assertRecordBinds(String sql, boolean isSwapped, long... minBinds) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            Assert.assertEquals(sql, minBinds.length, join.getSpec().getStageCount());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                // Every column of every row, so that the consumer reads every payload.
                final Record record = cursor.getRecord();
                final StringSink sink = new StringSink();
                long rows = 0;
                while (cursor.hasNext()) {
                    CursorPrinter.println(record, factory.getMetadata(), sink, true);
                    rows++;
                }
                Assert.assertTrue(sql, rows > 0);
                Assert.assertEquals(sql, isSwapped, join.getAtom().isSwapped());
                for (int k = 0; k < minBinds.length; k++) {
                    final long binds = join.getAtom().getStage(k).getPayloadRecordBindCount();
                    if (minBinds[k] == 0) {
                        Assert.assertEquals(sql + ", join " + k, 0, binds);
                    } else {
                        Assert.assertTrue(sql + ", join " + k + ": " + binds, binds >= minBinds[k]);
                    }
                }
            }
        }
    }

    private static void assertCount(String sql, long expected) throws SqlException {
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(sql, cursor.hasNext());
            Assert.assertEquals(sql, expected, cursor.getRecord().getLong(0));
        }
    }

    private static void createTables(String storage) throws Exception {
        execute("CREATE TABLE t_" + storage + " AS (" +
                "SELECT CASE WHEN x % 13 = 0 THEN NULL ELSE (x % 9)::INT END k, x v, " +
                "CASE WHEN x % 4 = 0 THEN NULL ELSE ('n' || (x % 7))::VARCHAR END note, " +
                "timestamp_sequence('2024-01-01', 1_800_000_000) ts FROM long_sequence(160)) TIMESTAMP(ts) PARTITION BY DAY");
        // NULL, empty, 4-byte UTF-8 and values long enough to leave a VARCHAR's inline bytes, by turns.
        execute("CREATE TABLE d_" + storage + " AS (" +
                "SELECT CASE WHEN x % 11 = 0 THEN NULL ELSE (x % 7)::INT END k, " +
                "CASE WHEN x % 5 = 0 THEN NULL WHEN x % 5 = 1 THEN '' WHEN x % 5 = 2 THEN '\uD83D\uDE00' || x " +
                "     ELSE 'a longer string, row ' || x END::STRING str, " +
                "CASE WHEN x % 6 = 0 THEN NULL WHEN x % 6 = 1 THEN '' WHEN x % 6 = 2 THEN 'v\u00e9\uD83D\uDE00' || (x % 4) " +
                "     ELSE 'a longer varchar ' || (x % 4) END::VARCHAR vc, " +
                "CASE WHEN x % 4 = 0 THEN NULL WHEN x % 4 = 1 THEN from_base64('') ELSE rnd_bin(1, 40, 0) END bin, " +
                "CASE WHEN x % 7 = 0 THEN NULL::DOUBLE[] WHEN x % 7 = 1 THEN ARRAY[]::DOUBLE[] " +
                "     ELSE ARRAY[(x % 9)::DOUBLE, -x::DOUBLE] END arr, " +
                "timestamp_sequence('2024-01-01', 4_320_000_000) dts FROM long_sequence(60)) TIMESTAMP(dts) PARTITION BY DAY");
        // A column added after the first days, whose early rows sit below a column top.
        execute("ALTER TABLE d_" + storage + " ADD COLUMN arr2 DOUBLE[][]");
        execute("INSERT INTO d_" + storage + " (k, str, vc, bin, arr, arr2, dts) " +
                "SELECT (x % 7)::INT, 'late' || x, ('late v ' || (x % 3))::VARCHAR, rnd_bin(1, 20, 0), ARRAY[x::DOUBLE], " +
                "CASE WHEN x % 3 = 0 THEN NULL ELSE ARRAY[[x::DOUBLE, 0.5], [1.5, -x::DOUBLE]] END, " +
                "timestamp_sequence('2024-01-03T12', 4_320_000_000) FROM long_sequence(24)");
        if (storage.equals("mixed")) {
            execute("ALTER TABLE t_mixed CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-03'");
            execute("ALTER TABLE d_mixed CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
        }
        for (String table : new String[]{"t_" + storage, "d_" + storage}) {
            assertCount("SELECT count() FROM table_partitions('" + table + "') WHERE isParquet", storage.equals("mixed") ? (table.startsWith("t") ? 2 : 1) : 0);
        }
        if (storage.equals("native")) {
            // The second join of the swapped shape looks d's VARCHARs up in e.
            execute("CREATE TABLE e_native AS (SELECT CASE WHEN x % 6 = 0 THEN NULL WHEN x % 6 = 1 THEN '' " +
                    "WHEN x % 6 = 2 THEN 'v\u00e9\uD83D\uDE00' || (x % 4) ELSE 'a longer varchar ' || (x % 4) END::VARCHAR vc, " +
                    "x::INT ew FROM long_sequence(8))");
        }
    }
}
