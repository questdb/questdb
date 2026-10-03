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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * {@code ORDER BY <indexed symbol>} over an index scan ({@code sym = ...}, {@code sym IN (...)},
 * or the sorted-symbol-index scan) may drop the sort only when the scan emits the result in key
 * order as a whole, not merely within each page frame. A partition larger than
 * {@code cairo.sql.page.frame.max.rows} is split into several page frames, so these tests shrink
 * the frame size until every partition spans several frames.
 * <p>
 * Fixture: row {@code x} (1..24) has {@code sym} A, B or C for {@code x % 3} = 1, 2, 0 and
 * {@code ts} = (x - 1) * 2h. Under {@code PARTITION BY DAY} that is two partitions of 12 rows,
 * 1970-01-01 (x 1..12) and 1970-01-02 (x 13..24).
 */
@RunWith(Parameterized.class)
public class OrderByIndexedSymbolKeyMajorTest extends AbstractCairoTest {
    private static final long HOUR = 3_600_000_000L;
    private static final String[] PARTITION_BYS = {"NONE", "DAY"};
    private static final int ROWS = 24;
    private final String indexType;

    public OrderByIndexedSymbolKeyMajorTest(String indexType) {
        this.indexType = indexType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"bitmap"},
                {"posting"},
        });
    }

    @Override
    public void setUp() {
        super.setUp();
        // 4 rows per page frame: a 24-row PARTITION BY NONE table is 6 frames and each
        // 12-row day partition is 3 frames. reset() in the next setUp() restores the defaults.
        sqlExecutionContext.changePageFrameSizes(1, 4);
    }

    @Test
    public void testInListBindVariablesOrderBySymNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            bindVariableService.clear();
            bindVariableService.setStr(0, "B");
            bindVariableService.setStr(1, "A");
            assertQuery("select sym, x, ts from t where sym in ($1, $2) order by sym")
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"A", "B"}, false, 1, ROWS));
            assertKeyMajorPlan("select sym, x, ts from t where sym in ($1, $2) order by sym", true);
        });
    }

    @Test
    public void testInListBindVariablesOrderBySymTsNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            bindVariableService.clear();
            bindVariableService.setStr("a", "B");
            bindVariableService.setStr("b", "C");
            assertQuery("select sym, x, ts from t where sym in (:a, :b) order by sym, ts")
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"B", "C"}, false, 1, ROWS));
            assertKeyMajorPlan("select sym, x, ts from t where sym in (:a, :b) order by sym, ts", true);
        });
    }

    @Test
    public void testInListOrderBySymDayMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            final String query = "select sym, x, ts from t where sym in ('A', 'B') order by sym";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"A", "B"}, false, 1, ROWS));
            // two partitions, but 2 keys x 6 frames is well under the cursor-open threshold
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymDayMultiPartitionWithInterval() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            final String query = "select sym, x, ts from t where sym in ('C', 'A') and ts >= '1970-01-01T06:00' order by sym, ts";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"A", "C"}, false, 4, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymDayOnePartitionInterval() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            final String query = "select sym, x, ts from t where sym in ('A', 'B') and ts in '1970-01-02' order by sym";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"A", "B"}, false, 13, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymDescNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym in ('A', 'B', 'C') order by sym desc";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"C", "B", "A"}, false, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymDescTsDescNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym in ('A', 'B') order by sym desc, ts desc";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"B", "A"}, true, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym in ('A', 'B') order by sym";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"A", "B"}, false, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymNoneMultiFrameWithFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            // x % 2 = 0 is a residual filter evaluated inside the index cursor
            final String query = "select sym, x, ts from t where sym in ('A', 'B') and x % 2 = 0 order by sym";
            final StringSink expected = new StringSink();
            expected.put("sym\tx\tts\n");
            appendRows(expected, "A", false, 1, ROWS, true);
            appendRows(expected, "B", false, 1, ROWS, true);
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected);
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymTsDescNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym in ('A', 'B') order by sym, ts desc";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"A", "B"}, true, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListWindowOrderBySymDayMultiPartition() throws Exception {
        // the shape of NYSE TAQ query 50, which has no time filter: several partitions
        assertWindowOrderBySym("DAY");
    }

    @Test
    public void testInListWindowOrderBySymNoneMultiFrame() throws Exception {
        // The shape of NYSE TAQ query 50: a per-symbol moving window over an IN list, ordered by
        // symbol. The key-major scan feeds the window in symbol order, so there is no sort, and a
        // covering index is not used: its k-way merge into timestamp order would only be undone
        // by the sort.
        assertWindowOrderBySym("NONE");
    }


    @Test
    public void testInListOrderBySymPrefersKeyMajorOverCovering() throws Exception {
        Assume.assumeTrue("posting".equals(indexType));
        assertMemoryLeak(() -> {
            execute("create table c (sym symbol index type posting include (x), x long, ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into c select case when x % 3 = 1 then 'A' when x % 3 = 2 then 'B' else 'C' end, x, ((x - 1) * " + (2 * HOUR) + ")::timestamp from long_sequence(" + ROWS + ")");
            final String query = "select sym, x, ts from c where sym in ('A', 'B') order by sym";
            // the covering merge into timestamp order would only be undone by the sort
            assertQuery(query)
                    .withPlanNotContaining("CoveringIndex")
                    .returns(expected(new String[]{"A", "B"}, false, 1, ROWS));
            assertKeyMajorPlan(query, true);
            // with the key-major scan off across partitions, covering serves the scan again
            setProperty(PropertyKey.CAIRO_SQL_INDEX_KEY_MAJOR_MAX_PARTITION_PASSES, 0);
            assertQuery(query)
                    .withPlanContaining("CoveringIndex")
                    .sizeMayVary()
                    .returns(expected(new String[]{"A", "B"}, false, 1, ROWS));
            assertKeyMajorPlan(query, false);
        });
    }

    @Test
    public void testInListWithLimitNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym in ('B', 'A') order by sym limit 6";
            // the sort knows its size, the index scan does not
            assertQuery(query).sizeMayVary().returns("""
                    sym\tx\tts
                    A\t1\t1970-01-01T00:00:00.000000Z
                    A\t4\t1970-01-01T06:00:00.000000Z
                    A\t7\t1970-01-01T12:00:00.000000Z
                    A\t10\t1970-01-01T18:00:00.000000Z
                    A\t13\t1970-01-02T00:00:00.000000Z
                    A\t16\t1970-01-02T06:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testInListOrderByTsDayMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            // timestamp order: the heap cursor merges the keys per frame, frames are in order
            final String query = "select sym, x, ts from t where sym in ('A', 'C') order by ts";
            final StringSink expected = new StringSink();
            expected.put("sym\tx\tts\n");
            for (int x = 1; x <= ROWS; x++) {
                if (!"B".equals(symOf(x))) {
                    appendRows(expected, symOf(x), false, x, x, false);
                }
            }
            assertQuery(query).timestamp("ts").returns(expected);
        });
    }

    @Test
    public void testInListOrderBySymMaxKeys() throws Exception {
        assertMemoryLeak(() -> {
            // 3 keys, and every partition is several frames: key-major revisits each frame per key
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym in ('A', 'B', 'C') order by sym";
            final String expected = expected(new String[]{"A", "B", "C"}, false, 1, ROWS);

            setProperty(PropertyKey.CAIRO_SQL_INDEX_KEY_MAJOR_MAX_KEYS, 3);
            assertQuery(query).returns(expected);
            assertKeyMajorPlan(query, true);

            setProperty(PropertyKey.CAIRO_SQL_INDEX_KEY_MAJOR_MAX_KEYS, 2);
            assertQuery(query).sizeMayVary().returns(expected);
            assertKeyMajorPlan(query, false);

            // keys missing from the symbol table are not walked
            assertKeyMajorPlan("select sym, x, ts from t where sym in ('A', 'B', 'nope') order by sym", true);

            // 0 turns the key-major scan off altogether
            setProperty(PropertyKey.CAIRO_SQL_INDEX_KEY_MAJOR_MAX_KEYS, 0);
            assertKeyMajorPlan("select sym, x, ts from t where sym in ('A', 'B') order by sym", false);
        });
    }

    @Test
    public void testInListOrderBySymMaxKeysIgnoredForSingleFramePartitions() throws Exception {
        // with one frame per partition the key-major walk reads each frame once, as the sort does
        assertMemoryLeak(() -> {
            createTable("NONE");
            sqlExecutionContext.changePageFrameSizes(1, 1_000_000);
            setProperty(PropertyKey.CAIRO_SQL_INDEX_KEY_MAJOR_MAX_KEYS, 1);
            final String query = "select sym, x, ts from t where sym in ('A', 'B', 'C') order by sym";
            assertQuery(query).returns(expected(new String[]{"A", "B", "C"}, false, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymMaxPartitionPasses() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            // two partitions, counted independently of the planner
            assertQuery("select count() from table_partitions('t')")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n2\n");
            final String expected = expected(new String[]{"A", "B"}, false, 1, ROWS);
            final String query = "select sym, x, ts from t where sym in ('A', 'B') order by sym";

            // 2 keys x 2 partitions
            setProperty(PropertyKey.CAIRO_SQL_INDEX_KEY_MAJOR_MAX_PARTITION_PASSES, 4);
            assertQuery(query).returns(expected);
            assertKeyMajorPlan(query, true);
            // one partition counts its frames instead: 12 rows in 4-row frames, 2 keys x 3 frames
            final String oneDay = "select sym, x, ts from t where sym in ('A', 'B') and ts in '1970-01-02' order by sym";
            assertKeyMajorPlan(oneDay, false);
            // a static interval that hits both partitions counts both
            assertKeyMajorPlan("select sym, x, ts from t where sym in ('A', 'B') and ts >= '1970-01-01T12:00' order by sym", true);

            setProperty(PropertyKey.CAIRO_SQL_INDEX_KEY_MAJOR_MAX_PARTITION_PASSES, 3);
            assertQuery(query).sizeMayVary().returns(expected);
            assertKeyMajorPlan(query, false);
            assertKeyMajorPlan("select sym, x, ts from t where sym in ('A', 'B') and ts >= '1970-01-01T12:00' order by sym", false);

            setProperty(PropertyKey.CAIRO_SQL_INDEX_KEY_MAJOR_MAX_PARTITION_PASSES, 6);
            assertQuery(oneDay).returns(expected(new String[]{"A", "B"}, false, 13, ROWS));
            assertKeyMajorPlan(oneDay, true);

            // 0 leaves the key-major scan to one partition of one frame
            setProperty(PropertyKey.CAIRO_SQL_INDEX_KEY_MAJOR_MAX_PARTITION_PASSES, 0);
            assertKeyMajorPlan(query, false);
            assertKeyMajorPlan(oneDay, false);
            sqlExecutionContext.changePageFrameSizes(1, 1_000_000);
            assertKeyMajorPlan(oneDay, true);
        });
    }

    @Test
    public void testInListOrderBySymMultiPartitionWithParquetPartition() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createTable("DAY");
            execute("alter table t convert partition to parquet list '1970-01-01'");
            // a Parquet partition anywhere in the table keeps the sort across partitions: each
            // row group would be decoded again for every key that misses the decode cache
            final String query = "select sym, x, ts from t where sym in ('A', 'C') order by sym, ts";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"A", "C"}, false, 1, ROWS));
            assertKeyMajorPlan(query, false);
        });
    }

    @Test
    public void testInListOrderBySymMaxPartitionPassesCountsFramesOfOnePartition() throws Exception {
        // one PARTITION BY NONE partition of 24 rows in 4-row frames: 3 keys x 6 frames = 18
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym in ('A', 'B', 'C') order by sym";
            final String expected = expected(new String[]{"A", "B", "C"}, false, 1, ROWS);
            setProperty(PropertyKey.CAIRO_SQL_INDEX_KEY_MAJOR_MAX_PARTITION_PASSES, 18);
            assertQuery(query).returns(expected);
            assertKeyMajorPlan(query, true);
            setProperty(PropertyKey.CAIRO_SQL_INDEX_KEY_MAJOR_MAX_PARTITION_PASSES, 17);
            assertQuery(query).sizeMayVary().returns(expected);
            assertKeyMajorPlan(query, false);
        });
    }

    @Test
    public void testInListOrderBySymParquetPartition() throws Exception {
        // A Parquet partition is split into page frames by row group: 3 row groups of 4 rows. The
        // key-major scan would decode every row group once per key, so the sort stays, for the
        // IN list and for the sorted symbol index scan alike.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createTable("DAY");
            execute("alter table t convert partition to parquet list '1970-01-01'");
            final String query = "select sym, x, ts from t where sym in ('C', 'A') and ts in '1970-01-01' order by sym desc, ts desc";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns(expected(new String[]{"C", "A"}, true, 1, 12));
            assertKeyMajorPlan(query, false);
            final String sortedQuery = "select sym, x, ts from t where ts in '1970-01-01' order by sym";
            assertQuery(sortedQuery).returns(expected(new String[]{"A", "B", "C"}, false, 1, 12));
            assertKeyMajorPlan(sortedQuery, false);
            // the native partition is not affected
            assertKeyMajorPlan("select sym, x, ts from t where sym in ('C', 'A') and ts in '1970-01-02' order by sym", true);
        });
    }

    @Test
    public void testInListOrderBySymParquetPartitionCachedPlan() throws Exception {
        // a plan made over a native partition still runs after the partition turns Parquet: the
        // key-major scan stays correct, it is only no longer the cheaper plan
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createUnevenTables("DAY");
            final String query = "select sym, x, ts from u where sym in ('B', 'D', 'A') order by sym desc, ts desc";
            final String oracleQuery = "select sym, x, ts from u_twin where sym in ('B', 'D', 'A') order by sym desc, ts desc";
            try (RecordCursorFactory factory = select(query)) {
                assertFactory(factory, oracle(oracleQuery));
                execute("alter table u convert partition to parquet list '1970-01-01'");
                assertFactory(factory, oracle(oracleQuery));
            }
            assertKeyMajorPlan(query, false);
        });
    }

    @Test
    public void testInListOrderBySymTsDescSplitPartitionMultiPartition() throws Exception {
        // as below, but over the whole table: three physical partitions, walked backward
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTable("DAY");
            try (TableReader ignore = getReader("t")) {
                execute("insert into t values ('B', 101, '1970-01-01T21:00'), ('A', 103, '1970-01-01T21:30')");
            }
            assertQuery("select count() from table_partitions('t')")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n3\n");
            final String query = "select sym, x, ts from t where sym in ('B', 'A') order by sym desc, ts desc";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns("""
                            sym\tx\tts
                            B\t23\t1970-01-02T20:00:00.000000Z
                            B\t20\t1970-01-02T14:00:00.000000Z
                            B\t17\t1970-01-02T08:00:00.000000Z
                            B\t14\t1970-01-02T02:00:00.000000Z
                            B\t101\t1970-01-01T21:00:00.000000Z
                            B\t11\t1970-01-01T20:00:00.000000Z
                            B\t8\t1970-01-01T14:00:00.000000Z
                            B\t5\t1970-01-01T08:00:00.000000Z
                            B\t2\t1970-01-01T02:00:00.000000Z
                            A\t22\t1970-01-02T18:00:00.000000Z
                            A\t19\t1970-01-02T12:00:00.000000Z
                            A\t16\t1970-01-02T06:00:00.000000Z
                            A\t13\t1970-01-02T00:00:00.000000Z
                            A\t103\t1970-01-01T21:30:00.000000Z
                            A\t10\t1970-01-01T18:00:00.000000Z
                            A\t7\t1970-01-01T12:00:00.000000Z
                            A\t4\t1970-01-01T06:00:00.000000Z
                            A\t1\t1970-01-01T00:00:00.000000Z
                            """);
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testInListOrderBySymTsSplitPartition() throws Exception {
        // An O3 insert near the end of a partition, with a reader holding it open, splits the
        // partition in two. The interval below still hits one logical partition, so the sort is
        // dropped, but the scan spans two physical partitions: the key order must hold across them.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTable("DAY");
            try (TableReader ignore = getReader("t")) {
                execute("insert into t values ('B', 101, '1970-01-01T21:00'), ('A', 103, '1970-01-01T21:30')");
            }
            // the split is what this test is about, so make sure it happened
            assertQuery("select count() from table_partitions('t')")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n3\n");
            final String query = "select sym, x, ts from t where sym in ('B', 'A') and ts in '1970-01-01' order by sym, ts";
            assertQuery(query)
                    .withPlanContaining("FilterOnValues")
                    .returns("""
                            sym\tx\tts
                            A\t1\t1970-01-01T00:00:00.000000Z
                            A\t4\t1970-01-01T06:00:00.000000Z
                            A\t7\t1970-01-01T12:00:00.000000Z
                            A\t10\t1970-01-01T18:00:00.000000Z
                            A\t103\t1970-01-01T21:30:00.000000Z
                            B\t2\t1970-01-01T02:00:00.000000Z
                            B\t5\t1970-01-01T08:00:00.000000Z
                            B\t8\t1970-01-01T14:00:00.000000Z
                            B\t11\t1970-01-01T20:00:00.000000Z
                            B\t101\t1970-01-01T21:00:00.000000Z
                            """);
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testNotEqualsOrderBySymNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym != 'B' order by sym";
            assertQuery(query)
                    .withPlanContaining("FilterOnExcludedValues")
                    .returns(expected(new String[]{"A", "C"}, false, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testNotInOrderBySymDayMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            final String query = "select sym, x, ts from t where sym not in ('B') order by sym";
            assertQuery(query)
                    .withPlanContaining("FilterOnExcludedValues")
                    .returns(expected(new String[]{"A", "C"}, false, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testNotInOrderBySymDescTsDescNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym not in ('A') order by sym desc, ts desc";
            assertQuery(query)
                    .withPlanContaining("FilterOnExcludedValues")
                    .returns(expected(new String[]{"C", "B"}, true, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testSingleKeyOrderBySymTsDescDayOnePartitionInterval() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            final String query = "select sym, x, ts from t where sym = 'C' and ts in '1970-01-01' order by sym, ts desc";
            assertQuery(query).returns(expected(new String[]{"C"}, true, 1, 12));
            // a single key walked key-major: frames backward, each frame scanned backward
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testSingleKeyOrderByTsDescDayMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            final String query = "select sym, x, ts from t where sym = 'A' order by ts desc";
            assertQuery(query).timestampDesc("ts").returns(expected(new String[]{"A"}, true, 1, ROWS));
        });
    }

    @Test
    public void testSingleKeyOrderByTsDescNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym = 'B' order by ts desc";
            assertQuery(query).timestampDesc("ts").returns(expected(new String[]{"B"}, true, 1, ROWS));
        });
    }

    @Test
    public void testSingleKeyOrderBySymNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym = 'A' order by sym";
            assertQuery(query).returns(expected(new String[]{"A"}, false, 1, ROWS));
            // a single key is trivially key-major, whatever the frame count
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testSingleKeyOrderBySymTsDescNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym = 'A' order by sym, ts desc";
            assertQuery(query).returns(expected(new String[]{"A"}, true, 1, ROWS));
            // a single key walked key-major: frames backward, each frame scanned backward
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testSingleKeyOrderBySymTsNoneMultiFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable("NONE");
            final String query = "select sym, x, ts from t where sym = 'B' order by sym, ts";
            assertQuery(query).returns(expected(new String[]{"B"}, false, 1, ROWS));
            assertKeyMajorPlan(query, true);
        });
    }

    @Test
    public void testSortedSymbolIndexOrderBySymOnePartitionInterval() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            // no key filter: with a bitmap index this is the SortedSymbolIndex scan, a posting
            // index sorts
            final String query = "select sym, x, ts from t where ts in '1970-01-01' order by sym";
            assertQuery(query).returns(expected(new String[]{"A", "B", "C"}, false, 1, 12));
            assertKeyMajorPlan(query, "bitmap".equals(indexType));
        });
    }

    @Test
    public void testSortedSymbolIndexOrderBySymTsDescOnePartitionInterval() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            final String query = "select sym, x, ts from t where ts in '1970-01-02' order by sym desc, ts desc";
            assertQuery(query).returns(expected(new String[]{"C", "B", "A"}, true, 13, ROWS));
            assertKeyMajorPlan(query, "bitmap".equals(indexType));
        });
    }

    @Test
    public void testUnevenBindValuesChangeBetweenRuns() throws Exception {
        assertMemoryLeak(() -> {
            for (String partitionBy : PARTITION_BYS) {
                createUnevenTables(partitionBy);
                final String query = "select sym, x, ts from u where sym in ($1, $2) order by sym";
                try (RecordCursorFactory factory = select(query)) {
                    for (String[] binds : new String[][]{{"A", "B"}, {"D", "A"}, {"A", "A"}, {"ZZZ", "C"}, {null, "B"}}) {
                        bindVariableService.clear();
                        bindVariableService.setStr(0, binds[0]);
                        bindVariableService.setStr(1, binds[1]);
                        final String expected = oracle("select sym, x, ts from u_twin where sym in ($1, $2) order by sym, ts");
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            assertCursorTwoPass(expected, cursor, factory.getMetadata());
                        }
                    }
                }
                bindVariableService.clear();
            }
        });
    }

    @Test
    public void testUnevenCachedPlanAfterTableChanges() throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createUnevenTables("DAY");
            // E is not in the table yet, so it is a deferred key at compile time
            final String query = "select sym, x, ts from u where sym in ('E', 'A', 'D') order by sym desc, ts desc";
            final String oracleQuery = "select sym, x, ts from u_twin where sym in ('E', 'A', 'D') order by sym desc, ts desc";
            try (RecordCursorFactory factory = select(query)) {
                assertFactory(factory, oracle(oracleQuery));
                // a new partition, a new symbol, an O3 insert that splits the first partition
                for (String table : new String[]{"u", "u_twin"}) {
                    try (TableReader ignore = getReader(table)) {
                        execute("insert into " + table + " values" +
                                " ('E', 101, '1970-01-03T01:00'), ('A', 102, '1970-01-03T02:00'), (null, 103, '1970-01-03T03:00')," +
                                " ('E', 104, '1970-01-01T22:30'), ('D', 105, '1970-01-01T23:00')");
                    }
                }
                assertFactory(factory, oracle(oracleQuery));
                execute("alter table u drop partition list '1970-01-02'");
                execute("alter table u_twin drop partition list '1970-01-02'");
                assertFactory(factory, oracle(oracleQuery));
            }
        });
    }

    @Test
    public void testUnevenInListWithNullAndUnknownKeys() throws Exception {
        assertMemoryLeak(() -> {
            for (String partitionBy : PARTITION_BYS) {
                createUnevenTables(partitionBy);
                assertDifferential("sym in (null, 'A') order by sym", "sym in (null, 'A') order by sym, ts", true);
                assertDifferential("sym in ('A', 'ZZZ', 'D', null) order by sym desc, ts desc", "sym in ('A', 'ZZZ', 'D', null) order by sym desc, ts desc", true);
                assertDifferential("sym in ('A', 'B', 'D') order by sym, ts desc", "sym in ('A', 'B', 'D') order by sym, ts desc", true);
            }
        });
    }

    @Test
    public void testUnevenNotEqualsAndNotInWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            for (String partitionBy : PARTITION_BYS) {
                createUnevenTables(partitionBy);
                // the NULL key is included: it sorts first ascending and last descending
                assertDifferential("sym != 'B' order by sym", "sym != 'B' order by sym, ts", true);
                assertDifferential("sym != 'B' order by sym desc", "sym != 'B' order by sym desc, ts", true);
                assertDifferential("sym not in ('A', 'C') order by sym, ts desc", "sym not in ('A', 'C') order by sym, ts desc", true);
                assertDifferential("sym not in ('D') order by sym desc, ts desc", "sym not in ('D') order by sym desc, ts desc", true);
            }
        });
    }

    @Test
    public void testUnevenSingleKeyOrderBySymTsDesc() throws Exception {
        assertMemoryLeak(() -> {
            createUnevenTables("NONE");
            assertDifferential("sym = 'A' order by sym, ts desc", "sym = 'A' order by sym, ts desc", true);
            assertDifferential("sym = null order by sym, ts desc", "sym = null order by sym, ts desc", true);
        });
    }

    @Test
    public void testUnevenSortedSymbolIndexWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            createUnevenTables("DAY");
            // with a bitmap index this is the SortedSymbolIndex scan, a posting index sorts
            final boolean keyMajor = "bitmap".equals(indexType);
            assertDifferential("ts in '1970-01-01' order by sym", "ts in '1970-01-01' order by sym, ts", keyMajor);
            assertDifferential("ts in '1970-01-02' order by sym desc", "ts in '1970-01-02' order by sym desc, ts", keyMajor);
            assertDifferential("ts in '1970-01-02' order by sym desc, ts desc", "ts in '1970-01-02' order by sym desc, ts desc", keyMajor);
        });
    }

    private static void appendRows(StringSink sink, String sym, boolean desc, int xLo, int xHi, boolean evenOnly) {
        for (int i = 0, n = xHi - xLo + 1; i < n; i++) {
            final int x = desc ? xHi - i : xLo + i;
            if (!sym.equals(symOf(x)) || (evenOnly && x % 2 != 0)) {
                continue;
            }
            sink.put(sym).put('\t').put(x).put('\t');
            MicrosFormatUtils.appendDateTimeUSec(sink, (x - 1) * 2 * HOUR);
            sink.put('\n');
        }
    }

    private static String expected(String[] symOrder, boolean tsDesc, int xLo, int xHi) {
        final StringSink sink = new StringSink();
        sink.put("sym\tx\tts\n");
        for (String sym : symOrder) {
            appendRows(sink, sym, tsDesc, xLo, xHi, false);
        }
        return sink.toString();
    }

    private static String symOf(long x) {
        return switch ((int) (x % 3)) {
            case 1 -> "A";
            case 2 -> "B";
            default -> "C";
        };
    }

    private void assertWindowOrderBySym(String partitionBy) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table q (sym symbol index type " + indexType
                            + ("posting".equals(indexType) ? " include (x)" : "")
                            + ", x long, ts timestamp) timestamp(ts) partition by " + partitionBy
            );
            execute(
                    "insert into q select" +
                            " case when x % 3 = 1 then 'A' when x % 3 = 2 then 'B' else 'C' end," +
                            " x," +
                            " ((x - 1) * " + (2 * HOUR) + ")::timestamp" +
                            " from long_sequence(" + ROWS + ")"
            );
            final String query = "select sym, x, mavg from (" +
                    " select sym, x, ts, avg(x) over (partition by sym rows between 4 preceding and current row) mavg" +
                    " from q where sym in ('B', 'A') order by sym)";
            final StringSink expected = new StringSink();
            expected.put("sym\tx\tmavg\n");
            for (String sym : new String[]{"A", "B"}) {
                final long[] window = new long[5];
                int n = 0;
                for (int x = 1; x <= ROWS; x++) {
                    if (sym.equals(symOf(x))) {
                        window[n++ % 5] = x;
                        long sum = 0;
                        for (int i = 0, m = Math.min(n, 5); i < m; i++) {
                            sum += window[i];
                        }
                        expected.put(sym).put('\t').put(x).put('\t').put((double) sum / Math.min(n, 5)).put('\n');
                    }
                }
            }
            assertQuery(query)
                    .withPlanContaining("Window", "FilterOnValues symbolOrder: asc")
                    .withPlanNotContaining("CoveringIndex")
                    .noRandomAccess()
                    .returns(expected);
            assertKeyMajorPlan(query, true);
        });
    }

    private void assertDifferential(String where, String twinWhere, boolean expectKeyMajor) throws Exception {
        final String query = "select sym, x, ts from u where " + where;
        final String expected = oracle("select sym, x, ts from u_twin where " + twinWhere);
        assertQuery(query).sizeMayVary().returns(expected);
        assertKeyMajorPlan(query, expectKeyMajor);
    }

    private void assertFactory(RecordCursorFactory factory, String expected) throws Exception {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            assertCursorTwoPass(expected, cursor, factory.getMetadata());
        }
    }

    /**
     * Two copies of the same uneven data: {@code u} with the index under test and {@code u_twin}
     * without any index, whose sorted output is the oracle. 40 rows, {@code ts} = (x - 1) * 72min, so
     * under DAY 1970-01-01 holds x 1..20 and 1970-01-02 holds x 21..40, each 5 page frames of 4 rows.
     * <ul>
     *     <li>x 1..8: A on even x, NULL on odd x</li>
     *     <li>x 9..16: B only, so B is absent from every other frame and from the second day</li>
     *     <li>x 17..28: C, A, NULL by x % 3</li>
     *     <li>x 29..40: D only, so D exists in the second day only</li>
     * </ul>
     */
    private void createUnevenTables(String partitionBy) throws Exception {
        execute("drop table if exists u");
        execute("drop table if exists u_twin");
        for (String table : new String[]{"u", "u_twin"}) {
            execute(
                    "create table " + table + " (sym symbol" + ("u".equals(table) ? " index type " + indexType : "")
                            + ", x long, ts timestamp) timestamp(ts) partition by " + partitionBy
            );
            execute(
                    "insert into " + table + " select" +
                            " case" +
                            "   when x <= 8 then case when x % 2 = 0 then 'A' else null end" +
                            "   when x <= 16 then 'B'" +
                            "   when x <= 28 then case x % 3 when 0 then 'C' when 1 then 'A' else null end" +
                            "   else 'D'" +
                            " end," +
                            " x," +
                            " ((x - 1) * 4_320_000_000L)::timestamp" +
                            " from long_sequence(40)"
            );
        }
    }

    private String oracle(String twinQuery) throws Exception {
        final StringSink expected = new StringSink();
        printSql(twinQuery, expected);
        return expected.toString();
    }

    private void assertKeyMajorPlan(String query, boolean expectSortElided) throws Exception {
        final StringSink plan = new StringSink();
        printSql("explain " + query, plan);
        boolean hasSort = false;
        for (String line : plan.toString().split("\n")) {
            final String trimmed = line.trim();
            // every node that orders rows: Sort / Sort light, Encode sort, Radix sort, (Async) Top K
            if (trimmed.startsWith("Sort") && !trimmed.startsWith("SortedSymbolIndex")
                    || trimmed.startsWith("Encode sort")
                    || trimmed.startsWith("Radix sort")
                    || trimmed.contains("Top K")) {
                hasSort = true;
                break;
            }
        }
        Assert.assertEquals("sort elided [query=" + query + ", plan=\n" + plan + "]", expectSortElided, !hasSort);
        // An index key scan that replaces the sort must be the key-major one, and say so: the
        // per-frame scan is key-major within one page frame only.
        final String planText = plan.toString();
        final boolean keyScan = planText.contains("FilterOnValues") || planText.contains("FilterOnExcludedValues")
                || planText.contains("SortedSymbolIndex");
        if (keyScan) {
            Assert.assertEquals(
                    "key-major scan [query=" + query + ", plan=\n" + plan + "]",
                    expectSortElided,
                    planText.contains("keyMajor: true")
            );
        }
    }

    private void createTable(String partitionBy) throws Exception {
        execute(
                "create table t (sym symbol index type " + indexType + ", x long, ts timestamp) timestamp(ts) partition by " + partitionBy
        );
        execute(
                "insert into t select" +
                        " case when x % 3 = 1 then 'A' when x % 3 = 2 then 'B' else 'C' end," +
                        " x," +
                        " ((x - 1) * " + (2 * HOUR) + ")::timestamp" +
                        " from long_sequence(" + ROWS + ")"
        );
    }
}
