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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.HashJoinGroupByKeys;
import io.questdb.griffin.HashJoinKeyLayout;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.table.AsyncFilterContext;
import io.questdb.griffin.engine.table.AsyncHashJoinAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinSpec;
import io.questdb.griffin.engine.table.HashJoinInput;
import io.questdb.griffin.engine.table.HashJoinStageSpec;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Files;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * Late materialization of a parallel hash join's Parquet probe frames: a probe task decodes the
 * columns it reads itself first, those of the probe filter, the stages' keys and their ON residuals
 * and post-join filters, and the other columns, which only the consumer reads, once it has joined the
 * frame, for the rows the consumer reads. A frame whose rows the filter or the INNER lookups all
 * reject decodes no more. Every query compares with the ordinary plan, in order, and the counts of
 * decoded columns come from the shape of the data.
 * <p>
 * The probe table t holds 120 rows, 30 per daily partition, in Parquet row groups of 10 rows. Its
 * first three partitions convert to Parquet, nine row groups, each a probe frame, and the fourth
 * stays native. In the first two partitions the row groups alternate: the even ones, 0, 2 and 4,
 * keep no row, and the odd ones, 1, 3 and 5, keep one row in ten, {@code f = 1} and a key that hits
 * d; the other rows have {@code f} 0 or 2, so that no row group's statistics let the scan skip it for
 * {@code f = 1}, and a key that misses d. Every row of the third partition's groups, 6, 7 and 8, and
 * of the native one, keeps: {@code f = 1} and a key that hits d. The groups that keep every row
 * come last in the scan, so a task decodes each earlier frame late: the share of a frame's rows that
 * the consumer reads, whose average decides that, stays below a fifth until then. The tests run
 * without query workers, so the owner joins every frame, in the scan's order.
 */
public class AsyncHashJoinLateMaterializationTest extends AbstractCairoTest {
    // The statements that create t and d; see the class documentation.
    static final String[] DDL = {
            """
                    CREATE TABLE t AS (
                        SELECT
                            x id,
                            ((x - 1) / 10)::INT g,
                            CASE WHEN x > 60 OR ((x - 1) / 10 % 2 = 1 AND x % 10 = 5) THEN 1 ELSE ((x % 2) * 2)::INT END f,
                            CASE WHEN x > 60 OR ((x - 1) / 10 % 2 = 1 AND x % 10 = 5) THEN (x % 4)::INT ELSE (100 + x)::INT END k,
                            x v,
                            x / 4.0 d,
                            ('s' || (x % 3))::SYMBOL s,
                            (CASE WHEN x % 7 = 0 THEN NULL WHEN x % 11 = 0 THEN '' ELSE 'vé😀' || x END)::VARCHAR vc,
                            (CASE WHEN x % 5 = 0 THEN NULL ELSE 'str' || x END)::STRING str,
                            ARRAY[x::DOUBLE, x / 2.0] arr,
                            timestamp_sequence('2024-01-01', 2_880_000_000) ts
                        FROM long_sequence(120)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """,
            "ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-04'",
            "CREATE TABLE d AS (SELECT (x - 1)::INT k, (x * 10)::INT w, ('dv' || x)::VARCHAR dv FROM long_sequence(4))",
    };
    // The probe frames that read t's Parquet partitions.
    static final int PARQUET_FRAMES = 9;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // read_parquet() reads files under the input root.
        inputRoot = root;
        sqlExecutionContext.setParallelHashJoinEnabled(true);
        // Three row groups, so three Parquet frames, per partition.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 10);
    }

    @After
    public void restoreContext() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testCancelledTaskDecodesNoMore() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A post-join filter makes the task walk its pairs, and one-row page frames make it check
            // the breaker at each pair; one frame in flight at a time.
            sqlExecutionContext.changePageFrameSizes(1, 1);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1);
            final String sql = "SELECT t.id, t.vc, t.arr, d.w FROM t JOIN d ON t.k = d.k WHERE d.w + t.v > 0";
            final String expected = ordinary(sql, sqlExecutionContext);
            final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
            // Groups 1, 3 and 5 check the breaker on their one pair each, and group 6 on each of its
            // rows' pairs: the fifth check cancels the query on group 6's second row, once its task
            // wrote the first row's tuple.
            final ProbeBreaker breaker = new ProbeBreaker(5);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                final AsyncHashJoinAtom atom = join.getAtom();
                final int probeColumns = join.getSpec().getLeftInput().getMetadata().getColumnCount();
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.fail("expected cancellation");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.isCancellation());
                    TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                } finally {
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
                }
                // The cancelled task decoded no more than its own columns, and no frame after it was
                // read.
                Assert.assertEquals(5, breaker.checks);
                Assert.assertEquals(7, atom.getParquetProbeFrameCount());
                Assert.assertEquals(3 * probeColumns + 4 * 2, atom.getDecodedProbeColumnCount());
                Assert.assertEquals(0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                Assert.assertEquals(PARQUET_FRAMES, atom.getParquetProbeFrameCount());
            }
        });
    }

    @Test
    public void testColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A column added after the conversion is absent from the Parquet files, whose frames read
            // it as NULL.
            execute("ALTER TABLE t ADD COLUMN extra INT");
            // The native partition's rows have a column top, and a new native partition holds values.
            execute("INSERT INTO t (id, g, f, k, v, vc, extra, ts) SELECT 200 + x, 20, 1, (x % 4)::INT, 200 + x, 'new' || x, (x % 4)::INT, " +
                    "timestamp_sequence('2024-01-05', 3_600_000_000) FROM long_sequence(10)");
            // The absent column as a column only the consumer reads, as a probe filter column and as
            // a key, which every Parquet row misses.
            assertDecoding("SELECT t.id, t.extra, t.vc, d.w FROM t JOIN d ON t.k = d.k WHERE t.f = 1", 2, 6);
            assertDecoding("SELECT t.id, t.extra, t.vc, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.extra IS NULL AND t.f = 1", 3, 6);
            assertDecoding("SELECT t.id, t.vc, d.w FROM t JOIN d ON t.extra = d.k", 1, 0);
            assertDecoding("SELECT t.id, t.vc, d.w FROM t LEFT JOIN d ON t.extra = d.k", 1, PARQUET_FRAMES);
            // A probe table whose Parquet partitions have a column top: the column was added after
            // the first rows of each partition.
            execute("""
                    CREATE TABLE tt AS (
                        SELECT x id, (x % 4)::INT k, ('vé😀' || x)::VARCHAR vc, timestamp_sequence('2024-01-01', 5_760_000_000) ts
                        FROM long_sequence(60)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("ALTER TABLE tt ADD COLUMN extra INT");
            execute("INSERT INTO tt SELECT 100 + x, (x % 5)::INT, 'late' || x, timestamp_sequence('2024-01-01T12', 5_760_000_000), (x % 3)::INT FROM long_sequence(60)");
            execute("ALTER TABLE tt CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-04'");
            for (String sql : new String[]{
                    "SELECT tt.id, tt.extra, tt.vc, d.w FROM tt JOIN d ON tt.k = d.k WHERE tt.id % 10 = 3",
                    "SELECT tt.id, tt.extra, tt.vc, d.w FROM tt JOIN d ON tt.extra = d.k",
                    "SELECT tt.id, tt.extra, tt.vc, d.w FROM tt LEFT JOIN d ON tt.k = d.k WHERE tt.extra IS NULL",
            }) {
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    sqlExecutionContext.setJitMode(jitMode);
                    final String expected = ordinary(sql, sqlExecutionContext);
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(sql, join);
                        Assert.assertEquals(sql, expected, result(factory, sqlExecutionContext));
                        Assert.assertNotNull(sql, join.getAtom().getProbeTaskColumns());
                        Assert.assertEquals(sql, 9, join.getAtom().getParquetProbeFrameCount());
                    }
                }
            }
        });
    }

    @Test
    public void testContinuations() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Three rows of d2 per key, under a post-join filter, which walks them on the workers: a
            // frame whose ten rows all match writes more tuples than the cap of the frame's ten rows,
            // and leaves the frame's rest to the consumer, whose rows the task decodes too.
            execute("CREATE TABLE d2 AS (SELECT (x % 4)::INT k, x::INT w FROM long_sequence(12))");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            for (String sql : new String[]{
                    "SELECT t.id, t.vc, t.arr, d2.w FROM t JOIN d2 ON t.k = d2.k WHERE d2.w <> t.g",
                    "SELECT t.id, t.vc, t.arr, d2.w FROM t JOIN d2 ON t.k = d2.k WHERE d2.w <> t.g AND t.f = 1",
            }) {
                // The frames of a forward scan that keep rows decode every column, and the third
                // partition's first frame is decoded late, as the two before it.
                final int taskColumns = sql.contains("t.f") ? 3 : 2;
                // Three distinct rows match in the first two Parquet partitions. All 30 in the
                // third either produce a tuple in their task or are left to a continuation.
                final long consumerRows = 33;
                Assert.assertTrue(sql, assertDecoding(sql, taskColumns, PARQUET_FRAMES, 6, consumerRows).getContinuationCount() > 0);
                // A backward scan joins the third partition's frames first, late, the first two that
                // its tasks join, and leaves their rest, the frames' lowest rows, to continuations.
                final String backward = sql + " ORDER BY t.ts DESC";
                TestUtils.assertContains(assertParallelHashJoin(backward, sqlExecutionContext), "Frame backward scan on: t");
                try (RecordCursorFactory factory = select(backward)) {
                    final AsyncHashJoinAtom atom = find(factory).getAtom();
                    Assert.assertEquals(backward, ordinary(backward, sqlExecutionContext), result(factory, sqlExecutionContext));
                    Assert.assertTrue(backward, atom.getContinuationCount() > 0);
                    Assert.assertEquals(backward, PARQUET_FRAMES, atom.getParquetProbeFrameCount());
                    Assert.assertEquals(backward, consumerRows, atom.getConsumerProbeRowCount());
                }
                // A count after the first row counts the rest of the frame on the owner, over the
                // columns that its task decoded, and the cursor then reads every row again.
                for (String query : new String[]{sql, backward}) {
                    final String expected = ordinary(query, sqlExecutionContext);
                    try (RecordCursorFactory factory = select(query)) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            final RecordCursor.Counter counter = new RecordCursor.Counter();
                            cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                            Assert.assertEquals(query, rowCount(expected) - 1, counter.get());
                        }
                        Assert.assertEquals(query, expected, result(factory, sqlExecutionContext));
                    }
                }
            }
        });
    }

    @Test
    public void testCountOnlyTasksDecodeTheirColumnsAlone() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT t.id, t.v, t.vc, t.str, t.arr, d.w FROM t JOIN d ON t.k = d.k WHERE t.f = 1";
            final String expected = ordinary(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final AsyncHashJoinAtom atom = join.getAtom();
                final int probeColumns = join.getSpec().getLeftInput().getMetadata().getColumnCount();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // A count reads no probe row: every Parquet frame decodes the filter's and the key's
                    // columns alone, whatever rows it keeps.
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(rowCount(expected), counter.get());
                    // Every frame, the native partition's one included, ran a count-only task.
                    Assert.assertEquals(PARQUET_FRAMES + 1, join.getCountOnlyFrameCount());
                    Assert.assertEquals(PARQUET_FRAMES, atom.getParquetProbeFrameCount());
                    Assert.assertEquals(PARQUET_FRAMES * 2, atom.getDecodedProbeColumnCount());
                    // The rows, read again, decode the rest of the frames that keep rows.
                    cursor.toTop();
                    long rows = 0;
                    while (cursor.hasNext()) {
                        rows++;
                    }
                    Assert.assertEquals(counter.get(), rows);
                    Assert.assertEquals(2 * PARQUET_FRAMES, atom.getParquetProbeFrameCount());
                    Assert.assertEquals(PARQUET_FRAMES * 2 + 6 * probeColumns + 3 * 2, atom.getDecodedProbeColumnCount());
                }
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            }
            // One frame in flight: a count that begins once the frames that keep every row stopped the
            // late decoding counts the next frames on count-only tasks, which still decode the task's
            // columns alone.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                final AsyncHashJoinAtom atom = join.getAtom();
                final int probeColumns = join.getSpec().getLeftInput().getMetadata().getColumnCount();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // The rows of groups 1, 3 and 5, one each, those of group 6 and the first of group 7.
                    for (int i = 0; i < 14; i++) {
                        Assert.assertTrue(cursor.hasNext());
                    }
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(rowCount(expected) - 14, counter.get());
                    // Groups 8 and the native partition ran count-only tasks.
                    Assert.assertEquals(2, join.getCountOnlyFrameCount());
                    Assert.assertEquals(PARQUET_FRAMES, atom.getParquetProbeFrameCount());
                    Assert.assertEquals(5 * probeColumns + 4 * 2, atom.getDecodedProbeColumnCount());
                }
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            }
        });
    }

    @Test
    public void testFilterColumnsThatThePlannerDidNotRecord() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A join planned by hand, as a caller other than the planner may plan it, whose filters
            // come without the columns they read: the tasks then decode whole frames, since any
            // column may be one a filter reads.
            final RecordMetadata probeMetadata = metadata("t");
            final int f = probeMetadata.getColumnIndex("f");
            final int k = probeMetadata.getColumnIndex("k");
            final int v = probeMetadata.getColumnIndex("v");
            final IntHashSet probeFilterColumns = new IntHashSet();
            probeFilterColumns.add(f);
            // The probe filter's columns, recorded and not.
            try (
                    RecordCursorFactory probeQuery = select("SELECT * FROM t");
                    RecordCursorFactory buildQuery = select("SELECT * FROM d");
                    AsyncHashJoinSpec spec = newSpec(probeQuery, buildQuery, new TrueFilter(), probeFilterColumns, null)
            ) {
                assertColumns(spec.newProbeTaskColumns(false), f, k);
                final HashJoinStageSpec stage = spec.getStage(0);
                // The joined row's columns of a post-join filter, recorded and not; t's columns lead
                // the joined row.
                final IntHashSet postFilterColumns = new IntHashSet();
                postFilterColumns.add(v);
                stage.ofPostFilter(new TrueFilter(), null, postFilterColumns);
                assertColumns(spec.newProbeTaskColumns(false), f, k, v);
                stage.ofPostFilter(null, null);
                assertColumns(spec.newProbeTaskColumns(false), f, k);
                stage.ofPostFilter(new TrueFilter(), null);
                Assert.assertNull(spec.newProbeTaskColumns(false));
            }
            try (
                    RecordCursorFactory probeQuery = select("SELECT * FROM t");
                    RecordCursorFactory buildQuery = select("SELECT * FROM d");
                    AsyncHashJoinSpec spec = newSpec(probeQuery, buildQuery, new TrueFilter(), null, null)
            ) {
                Assert.assertNull(spec.newProbeTaskColumns(false));
            }
            // A LEFT join's ON residual, whose columns are recorded and not, with no probe filter.
            try (
                    RecordCursorFactory probeQuery = select("SELECT * FROM t");
                    RecordCursorFactory buildQuery = select("SELECT * FROM d");
                    AsyncHashJoinSpec spec = newSpec(probeQuery, buildQuery, null, null, new TrueFilter())
            ) {
                Assert.assertNull(spec.newProbeTaskColumns(false));
                final IntHashSet onFilterColumns = new IntHashSet();
                onFilterColumns.add(v);
                // A build column, which the probe scan does not hold.
                onFilterColumns.add(probeMetadata.getColumnCount());
                spec.getStage(0).setOnFilterColumns(onFilterColumns);
                assertColumns(spec.newProbeTaskColumns(false), k, v);
            }
        });
    }

    @Test
    public void testFiltersAndKeysReadBeforeTheRest() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // e holds the row groups 1, 3, 6, 7 and 8, so that a second join rejects every row that the
            // first one keeps of group 5, and keeps those of the frames that keep every row, the last
            // ones of the scan.
            execute("CREATE TABLE e AS (SELECT (CASE x WHEN 1 THEN 1 WHEN 2 THEN 3 ELSE x + 3 END)::INT g, x::INT ew FROM long_sequence(5))");
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                // A post-join filter reads a probe column that neither a key nor the probe filter
                // reads, and so does a LEFT join's ON residual, which keeps every probe row.
                assertDecoding("SELECT t.id, t.vc, t.arr, d.w FROM t JOIN d ON t.k = d.k WHERE d.w + t.v > 0", 2, 6);
                assertDecoding("SELECT t.id, t.vc, t.arr, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w < t.v", 2, PARQUET_FRAMES);
                assertDecoding("SELECT t.id, t.vc, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w < t.v WHERE t.f = 1", 3, 6);
                // A second stage's key and its post-join filter read probe columns too.
                assertDecoding("SELECT t.id, t.vc, d.w, e.ew FROM t JOIN d ON t.k = d.k JOIN e ON t.g = e.g", 2, 5);
                assertDecoding("SELECT t.id, t.vc, d.w, e.ew FROM t JOIN d ON t.k = d.k JOIN e ON t.g = e.g WHERE e.ew < t.d", 3, 5);
                assertDecoding("SELECT t.id, t.vc, d.w, e.ew FROM t JOIN d ON t.k = d.k LEFT JOIN e ON t.g = e.g AND e.ew < t.d", 3, 6);
            }
        });
    }

    @Test
    public void testInnerLookupsRejectEveryRowSomeRowsAndNone() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The INNER join keeps no row of the even groups of the first two partitions, one row of
            // the odd ones, and every row of the third partition's.
            assertDecoding("SELECT t.id, t.v, t.d, t.s, t.vc, t.str, t.arr, d.w FROM t JOIN d ON t.k = d.k", 1, 6);
            // Every row misses.
            execute("CREATE TABLE d0 AS (SELECT (x + 1_000)::INT k, x::INT w FROM long_sequence(4))");
            assertDecoding("SELECT t.id, t.v, t.vc, d0.w FROM t JOIN d0 ON t.k = d0.k", 1, 0);
            // A LEFT join keeps every row.
            assertDecoding("SELECT t.id, t.v, t.vc, d0.w FROM t LEFT JOIN d0 ON t.k = d0.k", 1, PARQUET_FRAMES);
            // A second INNER stage that misses every row keeps no row of any frame, and one that
            // keeps every row keeps those the first one keeps.
            assertDecoding("SELECT t.id, t.vc, d.w, d0.w FROM t JOIN d ON t.k = d.k JOIN d0 ON t.k = d0.k", 1, 0);
            assertDecoding("SELECT t.id, t.vc, d.w, d0.w FROM t JOIN d ON t.k = d.k LEFT JOIN d0 ON t.g = d0.k", 2, 6);
        });
    }

    @Test
    public void testLateDecodingStopsOnceMostRowsReachTheConsumer() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A backward scan joins the third partition's frames first, whose rows all reach the
            // consumer: the first two frames decode late, the share of rows the consumer reads then
            // stops being low, and every later frame decodes whole, in one pass, also those that
            // keep no row.
            final String sql = "SELECT t.id, t.v, t.vc, d.w FROM t JOIN d ON t.k = d.k ORDER BY t.ts DESC";
            assertDecoding(sql, 1, PARQUET_FRAMES, PARQUET_FRAMES);
            // A backward scan of the first two partitions alone keeps a low share of rows throughout,
            // and decodes every frame late.
            assertDecoding("SELECT t.id, t.v, t.vc, d.w FROM t JOIN d ON t.k = d.k WHERE t.g % 100 < 6 ORDER BY t.ts DESC", 2, PARQUET_FRAMES, 3);
        });
    }

    @Test
    public void testMemoryLimitInLateDecoding() throws Exception {
        // One frame in flight, whose ring entry the later frames reuse, so that a task of a frame
        // after the first allocates nothing that the query's limit counts between its two decodes.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1);
        assertMemoryLeak(() -> {
            // Six Parquet frames that keep one row each, so that every frame decodes late and then
            // decodes the rest of its columns for that row: a short text in the first frame and a long
            // one in the later frames, whose second decode then needs more memory than the first
            // frame's whole task.
            execute("""
                    CREATE TABLE tf AS (
                        SELECT x id, x % 10 = 5 f, (x % 4)::INT k,
                            (CASE WHEN x > 10 AND x % 10 = 5 THEN rpad('v', 20_000, 'w') ELSE 'v' || x END)::VARCHAR vc,
                            ARRAY[x::DOUBLE, x / 2.0] arr, x / 3.0 d,
                            timestamp_sequence('2024-01-01', 2_880_000_000) ts
                        FROM long_sequence(90)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("ALTER TABLE tf CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
            execute("CREATE TABLE d AS (SELECT (x - 1)::INT k, (x * 10)::INT w FROM long_sequence(4))");
            final String sql = "SELECT tf.id, tf.vc, tf.arr, tf.d, d.w FROM tf JOIN d ON tf.k = d.k WHERE tf.f";
            final String expected = ordinary(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final AsyncHashJoinAtom atom = join.getAtom();
                final int probeColumns = join.getSpec().getLeftInput().getMetadata().getColumnCount();
                final int taskColumns = 2;
                // The first power of two that fits sizes the sweep's steps.
                long fitting = 1024;
                while (!isFitting(factory, atom, fitting, expected)) {
                    fitting *= 2;
                }
                final long step = Math.max(64, fitting / 2_000);
                // Limits from a step up fail the query at every allocation in turn. A failure in the
                // second decode of a frame after the first leaves the frame's first decode counted
                // alone.
                int failures = 0;
                int lateFailures = 0;
                for (long limit = step; ; limit += step) {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                    try {
                        Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                        break;
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        failures++;
                        final long frames = atom.getParquetProbeFrameCount();
                        if (frames > 1 && atom.getDecodedProbeColumnCount() == (frames - 1) * probeColumns + taskColumns) {
                            lateFailures++;
                        }
                    }
                    // Each failure released everything the execution held.
                    Assert.assertEquals(0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                    Assert.assertEquals(0, atom.getRing().getAllocatedBytes());
                    Assert.assertNull(atom.getStage(0).getFrozenBuild());
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                }
                Assert.assertTrue("failures: " + failures, lateFailures > 0);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                Assert.assertEquals(6, atom.getParquetProbeFrameCount());
                Assert.assertEquals(6 * probeColumns, atom.getDecodedProbeColumnCount());
            }
            // The table still works.
            assertQuery("SELECT count() FROM tf WHERE f").noLeakCheck().noRandomAccess().expectSize().returns("count\n9\n");
        });
    }

    @Test
    public void testNativeProbeAndWholeFrameTasks() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A native probe decodes nothing.
            execute("CREATE TABLE tn AS (SELECT * FROM t) TIMESTAMP(ts) PARTITION BY DAY");
            try (RecordCursorFactory factory = select("SELECT tn.id, tn.vc, d.w FROM tn JOIN d ON tn.k = d.k WHERE tn.f = 1")) {
                final AsyncHashJoinAtom atom = find(factory).getAtom();
                result(factory, sqlExecutionContext);
                Assert.assertEquals(0, atom.getParquetProbeFrameCount());
                Assert.assertEquals(0, atom.getDecodedProbeColumnCount());
            }
            // A probe task that reads every column of the probe scan decodes whole frames, in one
            // pass.
            final String sql = "SELECT t.k, t.f, d.w FROM t JOIN d ON t.k = d.k WHERE t.f = 1";
            assertParallelHashJoin(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertEquals(2, join.getSpec().getLeftInput().getMetadata().getColumnCount());
                Assert.assertEquals(sql, ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                Assert.assertNull(join.getAtom().getProbeTaskColumns());
                Assert.assertEquals(PARQUET_FRAMES, join.getAtom().getParquetProbeFrameCount());
                Assert.assertEquals(PARQUET_FRAMES * 2, join.getAtom().getDecodedProbeColumnCount());
            }
        });
    }

    @Test
    public void testProbeFilterRejectsEveryRowSomeRowsAndNone() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                // The LEFT join keeps every row that the filter keeps: none of the even groups of the
                // first two partitions, one of each odd one, and every row of the third partition's.
                assertDecoding("SELECT t.id, t.v, t.d, t.s, t.vc, t.str, t.arr, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.f = 1", 2, 6);
                assertDecoding("SELECT t.id, t.v, t.d, t.s, t.vc, t.str, t.arr, d.w FROM t JOIN d ON t.k = d.k WHERE t.f = 1", 2, 6);
                // The filter rejects every row of every frame.
                assertDecoding("SELECT t.id, t.v, t.d, t.s, t.vc, t.str, t.arr, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.f % 4 = 3", 2, 0);
                // The filter keeps every row of every frame.
                assertDecoding("SELECT t.id, t.v, t.d, t.s, t.vc, t.str, t.arr, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.f >= 0", 2, PARQUET_FRAMES);
                // A filter over several columns, bind variables included.
                bindVariableService.clear();
                bindVariableService.setInt(0, 1);
                bindVariableService.setStr(1, "s1");
                assertDecoding("SELECT t.id, t.v, t.vc, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.f = $1 AND t.s <> $2 AND t.g % 100 < 6", 4, 2);
                bindVariableService.clear();
            }
        });
    }

    @Test
    public void testReadParquetProbe() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelReadParquetEnabled(true);
            createTables();
            // The first day of t, in three row groups: the first and the last keep no row, the
            // second one row.
            execute("CREATE TABLE tn AS (SELECT * FROM t WHERE ts < '2024-01-02') TIMESTAMP(ts) PARTITION BY DAY");
            writeParquet("tn", "t.parquet");
            assertDecoding("SELECT p.id, p.vc, p.arr, d.w FROM read_parquet('t.parquet') p LEFT JOIN d ON p.k = d.k WHERE p.f = 1", 2, 3, 1);
            assertDecoding("SELECT p.id, p.vc, p.arr, d.w FROM read_parquet('t.parquet') p LEFT JOIN d ON p.k = d.k", 1, 3, 3);
        });
    }

    @Test
    public void testSwappedInputs() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The left input is the smaller one, so an execution hashes it and streams t, whose
            // frames the tasks join with the swapped keys.
            execute("CREATE TABLE s AS (SELECT (x - 1)::INT k, x a, (x * 2 - 1)::INT g FROM long_sequence(4))");
            execute("CREATE TABLE e AS (SELECT (x * 2 - 1)::INT g, x::INT ew FROM long_sequence(6))");
            final AsyncHashJoinAtom single = assertDecoding("SELECT s.a, t.id, t.vc, t.arr FROM s JOIN t ON s.k = t.k", 1, 6);
            Assert.assertTrue(single.isSwapped());
            // A later stage's key reads the first stage's payload, the left input's, and its post-join
            // filter reads a column of t, which the swapped probe holds.
            final AsyncHashJoinAtom stack = assertDecoding("SELECT s.a, t.id, t.vc, e.ew FROM s JOIN t ON s.k = t.k JOIN e ON s.g = e.g WHERE e.ew < t.v", 2, 6);
            Assert.assertTrue(stack.isSwapped());
        });
    }

    @Test
    public void testTypeCasts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Columns whose type changed after the conversion, which the record converts per row: a
            // key, a probe filter column and columns that only the consumer reads.
            execute("ALTER TABLE t ALTER COLUMN k TYPE LONG");
            execute("ALTER TABLE d ALTER COLUMN k TYPE LONG");
            execute("ALTER TABLE t ALTER COLUMN f TYPE SHORT");
            execute("ALTER TABLE t ALTER COLUMN d TYPE VARCHAR");
            execute("ALTER TABLE t ALTER COLUMN v TYPE STRING");
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                assertDecoding("SELECT t.id, t.d, t.v, t.vc, d.w FROM t JOIN d ON t.k = d.k WHERE t.f = 1", 2, 6);
                assertDecoding("SELECT t.id, t.d, t.v, t.vc, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.f = 1 AND t.d <> '2.5'", 3, 6);
                assertDecoding("SELECT t.id, t.d, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND t.v <> '15'", 2, PARQUET_FRAMES);
            }
        });
    }

    private static void assertColumns(@Nullable IntHashSet columns, int... expected) {
        Assert.assertNotNull(columns);
        Assert.assertEquals(columns.toString(), expected.length, columns.size());
        for (int column : expected) {
            Assert.assertTrue(columns.toString(), columns.contains(column));
        }
    }

    /**
     * Runs the query with the operator on, twice, compares each execution with the ordinary plan,
     * and asserts that its PARQUET_FRAMES Parquet probe frames decoded the probe task's columns alone,
     * taskColumns of them, except wholeFrames frames, which decoded every column of the probe scan:
     * those whose rows the consumer reads some of, and those that a slot decoded whole.
     */
    private static AsyncHashJoinAtom assertDecoding(String sql, int taskColumns, int wholeFrames) throws SqlException {
        return assertDecoding(sql, taskColumns, PARQUET_FRAMES, wholeFrames);
    }

    private static AsyncHashJoinAtom assertDecoding(String sql, int taskColumns, int parquetFrames, int wholeFrames) throws SqlException {
        return assertDecoding(sql, taskColumns, parquetFrames, wholeFrames, -1);
    }

    private static AsyncHashJoinAtom assertDecoding(String sql, int taskColumns, int parquetFrames, int wholeFrames, long consumerRows) throws SqlException {
        final SqlExecutionContext context = sqlExecutionContext;
        final String expected = ordinary(sql, context);
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            final AsyncHashJoinAtom atom = join.getAtom();
            for (int run = 0; run < 2; run++) {
                Assert.assertEquals(sql, expected, result(factory, context));
                final int probeColumns = atom.isSwapped()
                        ? join.getSpec().getStage(0).getBuildInput().getMetadata().getColumnCount()
                        : join.getSpec().getLeftInput().getMetadata().getColumnCount();
                final IntHashSet probeTaskColumns = atom.getProbeTaskColumns();
                Assert.assertNotNull(sql, probeTaskColumns);
                Assert.assertEquals(sql, taskColumns, probeTaskColumns.size());
                Assert.assertTrue(sql, taskColumns < probeColumns);
                Assert.assertEquals(sql, parquetFrames, atom.getParquetProbeFrameCount());
                Assert.assertEquals(
                        sql,
                        (long) wholeFrames * probeColumns + (long) (parquetFrames - wholeFrames) * taskColumns,
                        atom.getDecodedProbeColumnCount()
                );
                if (consumerRows >= 0) {
                    Assert.assertEquals(sql, consumerRows, atom.getConsumerProbeRowCount());
                }
            }
            return atom;
        }
    }

    private static boolean isFitting(RecordCursorFactory factory, AsyncHashJoinAtom atom, long limit, String expected) throws SqlException {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
        try {
            Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            return true;
        } catch (CairoException e) {
            Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
            Assert.assertEquals(0, atom.getPerWorkerLocks().getAcquiredSlotCount());
            return false;
        }
    }

    // The metadata of the table's scan, as SELECT * reads it.
    private static RecordMetadata metadata(String table) throws SqlException {
        try (RecordCursorFactory factory = select("SELECT * FROM " + table)) {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            GenericRecordMetadata.copyColumns(factory.getMetadata(), metadata);
            return metadata;
        }
    }

    // t LEFT JOIN d ON t.k = d.k, planned by hand over the scans of the two queries, which the spec
    // takes, and which the queries' wrappers close again, a no-op. The probe filter, if any, reads the
    // given columns, which, given null, the spec does not know. The ON residual, if any, comes without
    // the columns it reads. The spec takes the filters.
    private static AsyncHashJoinSpec newSpec(
            RecordCursorFactory probeQuery,
            RecordCursorFactory buildQuery,
            @Nullable Function probeFilter,
            @Nullable IntHashSet probeFilterColumns,
            @Nullable Function onFilter
    ) throws SqlException {
        final RecordCursorFactory probe = probeQuery.getBaseFactory();
        final RecordCursorFactory build = buildQuery.getBaseFactory();
        final RecordMetadata probeMetadata = probe.getMetadata();
        final RecordMetadata buildMetadata = build.getMetadata();
        final int probeKey = probeMetadata.getColumnIndex("k");
        final int buildKey = buildMetadata.getColumnIndex("k");
        final HashJoinGroupByKeys keys = new HashJoinGroupByKeys();
        Assert.assertTrue(keys.add(probeKey, probeMetadata.getColumnType(probeKey), buildKey, buildMetadata.getColumnType(buildKey)));
        final IntList probeColumns = identity(probeMetadata.getColumnCount());
        final IntList buildColumns = identity(buildMetadata.getColumnCount());
        final HashJoinKeyLayout keyLayout = new HashJoinKeyLayout(configuration, new BytecodeAssembler(), keys,
                "t", probeMetadata, probeColumns, "d", buildMetadata, buildColumns);
        final JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, probeColumns.size() + buildColumns.size());
        metadata.copyColumnMetadataFrom("t", probeMetadata);
        metadata.copyColumnMetadataFrom("d", buildMetadata);
        final HashJoinStageSpec stage = new HashJoinStageSpec(
                new HashJoinInput(build, buildColumns, new AsyncFilterContext(configuration, null, null, null, null, null, null, 1, 0, 0, 0)),
                keyLayout,
                true,
                "t.k=d.k",
                metadata,
                onFilter,
                null
        );
        return new AsyncHashJoinSpec(
                new HashJoinInput(probe, probeColumns, new AsyncFilterContext(configuration, null, null, null, probeFilter, probeFilterColumns, null, 1, 0, 0, 0)),
                stage,
                metadata,
                probe.followedOrderByAdvice(),
                probe.getScanDirection()
        );
    }

    // The rows of a result that ParallelHashJoinTestUtils.result() printed: two lines each, the
    // values and the symbol tables' and B views' readings, after the timestamp index's line and the
    // header's.
    private static long rowCount(String result) {
        long lines = 0;
        for (int i = 0, n = result.length(); i < n; i++) {
            if (result.charAt(i) == '\n') {
                lines++;
            }
        }
        return (lines - 2) / 2;
    }

    // Writes the table's first partition to a Parquet file under the input root, in row groups of
    // ten rows, for read_parquet().
    private static void writeParquet(String table, String file) {
        try (
                Path path = new Path();
                PartitionDescriptor descriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader(table)
        ) {
            path.of(root).concat(file);
            engine.getConfiguration().getFilesFacade().remove(path.$());
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            PartitionEncoder.encodeWithOptions(
                    descriptor,
                    path,
                    ParquetCompression.COMPRESSION_UNCOMPRESSED,
                    true,
                    false,
                    10,
                    0,
                    ParquetVersion.PARQUET_VERSION_V1,
                    0.0
            );
            Assert.assertTrue(Files.exists(path.$()));
        }
    }

    // t and d; see the class documentation.
    private void createTables() throws Exception {
        for (String ddl : DDL) {
            execute(ddl);
        }
        assertQuery("SELECT count() c, sum(isParquet::INT) p FROM table_partitions('t')")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("""
                        c\tp
                        4\t3
                        """);
    }

    // Cancels the query at the given check of its state that the probe tasks make while they join a
    // frame's rows; the tests run the tasks on the owner alone. A breaker of a class of its own is
    // thread safe, so the tasks consult it directly. The steppers are hidden classes, whose frames a
    // stack walk does not show, so the walk looks for the atom's method that runs them.
    private static class ProbeBreaker extends AtomicBooleanCircuitBreaker {
        private final int cancellingCheck;
        private int checks;

        private ProbeBreaker(int cancellingCheck) {
            super(engine);
            this.cancellingCheck = cancellingCheck;
        }

        @Override
        public int getState() {
            if (checks < cancellingCheck && StackWalker.getInstance().walk(frames -> frames.anyMatch(frame -> frame.getMethodName().equals("joinRows")))) {
                if (++checks == cancellingCheck) {
                    cancel();
                }
            }
            return super.getState();
        }
    }

    // A filter that keeps every row.
    private static final class TrueFilter extends BooleanFunction {

        @Override
        public boolean getBool(Record rec) {
            return true;
        }
    }
}
