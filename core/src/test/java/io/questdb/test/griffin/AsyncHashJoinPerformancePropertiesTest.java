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
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * Deterministic performance properties of the parallel hash join. The probe reducer and the
 * consumer allocate nothing on the heap once an execution's first row is out, whatever the rows
 * that follow: the probe tasks, which the owner runs here, as no worker serves the test's context,
 * and the consumer that reads their tuples, over one stage and over stacks, swapped stacks, the
 * odometer of several stages' chains, native var-size and Parquet payloads, Parquet probes and a
 * moving ring window; and so does a count of the rows. And an execution builds each stage once, in a known
 * number of rounds of tasks, however often its cursor goes back to the top or counts its rows, and
 * indexes the chain lengths of a stage that hands its chains on once, for its counts, whose breaker
 * checks then follow the frames and the MiB of the indexed rows rather than the chains' links.
 */
public class AsyncHashJoinPerformancePropertiesTest extends AbstractCairoTest {

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
    public void testBuildAndRoundCountsPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 256);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "1000000");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "1000000");
            final String left = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k";
            // On the owner: one build and no round per execution, and one pass that indexes the chain
            // lengths of d's duplicate keys, however often the execution counts.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, Long.MAX_VALUE);
            assertCounts(left, 1, 0, 1);
            // In rounds: one build of two rounds, one that sorts the frames' rows into partitions and
            // one that fills the partitions.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
            assertCounts(left, 1, 2, 1);
            // And one more round when the build copies its payload.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0");
            assertCounts(left, 1, 3, 1);
            // The swapped orientation builds the left input, once per execution too, and indexes its
            // chains, which its lone stage hands on.
            assertCounts("SELECT d.w, t.v FROM d JOIN t ON d.k = t.k", 1, 3, 1);
            // A build of one row per key has no chain, and a stage that walks its matches to run its
            // residual reads no chain length: their counts index nothing.
            assertCounts("SELECT t.k, t.v, u.w FROM t LEFT JOIN u ON t.k = u.k", 1, 3, 0);
            assertCounts("SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v % 100", 1, 3, 0);
            // An execution that only reads its rows indexes nothing either.
            try (RecordCursorFactory factory = select(left)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final HashJoinBuildStage stage = join.getAtom().getStage(0);
                final long passes = stage.getChainLengthIndexCount();
                Assert.assertEquals(ordinary(left, sqlExecutionContext), result(factory, sqlExecutionContext));
                Assert.assertEquals(passes, stage.getChainLengthIndexCount());
            }
            // An INNER join over an empty build builds once, and dispatches no probe task.
            execute("CREATE TABLE e (k INT, w LONG)");
            try (RecordCursorFactory factory = select("SELECT t.k, e.w FROM t JOIN e ON t.k = e.k WHERE t.ts IN '2024-01-01'")) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final HashJoinBuildStage stage = join.getAtom().getStage(0);
                final long builds = stage.getBuildCount();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertFalse(cursor.hasNext());
                    cursor.toTop();
                    Assert.assertFalse(cursor.hasNext());
                    Assert.assertEquals(0, join.getProbeFrameCount());
                }
                Assert.assertEquals(builds + 1, stage.getBuildCount());
            }
            // The fused GROUP BY's lone count over a partitioned build counts its key rows in a
            // third round.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 1);
            sqlExecutionContext.changePageFrameSizes(1_000, 100_000);
            try (RecordCursorFactory factory = select("SELECT count() FROM t JOIN d ON t.k = d.k")) {
                AsyncHashJoinGroupByRecordCursorFactory fused = null;
                for (RecordCursorFactory f = factory; f != null && fused == null; f = f.getBaseFactory()) {
                    if (f instanceof AsyncHashJoinGroupByRecordCursorFactory candidate) {
                        fused = candidate;
                    }
                }
                Assert.assertNotNull(fused);
                final AsyncHashJoinGroupByAtom atom = fused.getAtom();
                final HashJoinBuildStage stage = atom.getBuildingStage();
                final long builds = stage.getBuildCount();
                final long rounds = stage.getRoundCount();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(atom.isCountedInRounds());
                }
                Assert.assertEquals(builds + 1, stage.getBuildCount());
                Assert.assertEquals(rounds + 3, stage.getRoundCount());
            }
        });
    }

    @Test
    public void testCountReadsChainLengthsWithoutWalkingChains() throws Exception {
        // Each of f's 100_000 rows meets the 1_000_000 rows of key 1 in d: 10^11 joined rows. A
        // count reads each probe row's chain length from the index that one pass over d's rows
        // builds, rather than walking the key's chain, so its breaker checks follow the frames, not
        // the links: one per frame, at most a few more, and one per MiB of d's rows in the pass.
        // A breaker check every so many walked links, or one per chain that the consumer walks, would
        // pass the caps, and the breaker then cancels the count.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 2);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE f AS (SELECT 1 k, x v FROM long_sequence(100_000))");
            execute("CREATE TABLE d AS (SELECT 1 k, x w FROM long_sequence(1_000_000))");
            execute("INSERT INTO d SELECT (x + 1)::INT, x FROM long_sequence(4)");
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            // A LEFT join keeps its inputs: an INNER join would hash f, the smaller one.
            final String sql = "SELECT f.v, d.w FROM f LEFT JOIN d ON f.k = d.k";
            final int frameCount = 100;
            // d's 1_000_004 rows of 16 bytes fill 16 MiB of the build's heap, but for a part.
            final int passCheckCount = 16;
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                for (int prefix = 0; prefix < 2; prefix++) {
                    final CountCheckBreaker breaker = new CountCheckBreaker(frameCount + 16, frameCount + passCheckCount + 16);
                    final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        for (int i = 0; i < prefix; i++) {
                            Assert.assertTrue(cursor.hasNext());
                        }
                        breaker.isArmed = true;
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        try {
                            cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                        } catch (CairoException e) {
                            throw new AssertionError("the count after " + prefix + " rows failed with " + breaker.stateChecks
                                    + " state checks and " + breaker.throttledChecks + " time-throttled checks: " + e.getFlyweightMessage(), e);
                        }
                        breaker.isArmed = false;
                        Assert.assertEquals(100_000_000_000L - prefix, counter.get());
                        Assert.assertFalse(join.getAtom().isSwapped());
                        Assert.assertEquals(frameCount, join.getProbeFrameCount());
                    } finally {
                        ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
                    }
                    // One pass per execution.
                    Assert.assertEquals(prefix + 1, join.getAtom().getStage(0).getChainLengthIndexCount());
                    final String checks = "after " + prefix + " rows: " + breaker.stateChecks + " state checks, " + breaker.throttledChecks
                            + " time-throttled checks, " + breaker.passChecks + " of them in the pass";
                    Assert.assertTrue(checks, breaker.stateChecks.get() <= frameCount + 16);
                    Assert.assertTrue(checks, breaker.throttledChecks.get() <= frameCount + passCheckCount + 16);
                    Assert.assertEquals(checks, passCheckCount, breaker.passChecks.get());
                }
            }
        });
    }

    @Test
    public void testNoHeapAllocationPerRowInProbeReducerAndConsumer() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 4);
            for (String sql : new String[]{
                    // Unique matches, whose locators the consumer positions its reader at.
                    "SELECT t.k, t.v, u.w FROM t LEFT JOIN u ON t.k = u.k",
                    // Several matches per key, whose chains the consumer walks.
                    "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k",
                    // A residual, which the probe reducer runs per pair, and the continuations that
                    // the cap of one tuple per task leaves to the consumer.
                    "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v % 100",
                    // A translated SYMBOL key, a staged LONG key and a probe filter.
                    "SELECT t.v, d.w, d.s FROM t JOIN d ON t.s = d.s WHERE t.ts IN '2024-01-01'",
                    "SELECT t.v, d.w FROM t LEFT JOIN d ON t.l = d.l WHERE t.v % 3 = 0",
                    // The swapped orientation, which probes t and reads d's columns from its build.
                    "SELECT d.w, d.s, t.v FROM d JOIN t ON d.k = t.k",
            }) {
                try (
                        RecordCursorFactory factory = select(sql);
                        TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope()
                ) {
                    Assert.assertNotNull(sql, find(factory));
                    final RecordMetadata metadata = factory.getMetadata();
                    long minAllocated = Long.MAX_VALUE;
                    long rows = 0;
                    // The first executions warm up the steppers and the readers; a compile that the
                    // JVM queues inside the measured window charges the thread, which a later attempt
                    // does not repeat.
                    for (int attempt = 0; attempt < 12 && minAllocated > 0; attempt++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            // The first row builds and dispatches the first frames.
                            Assert.assertTrue(sql, cursor.hasNext());
                            final Record record = cursor.getRecord();
                            final long before = scope.getBean().getCurrentThreadAllocatedBytes();
                            long count = 1;
                            long checksum = 0;
                            while (cursor.hasNext()) {
                                count++;
                                checksum += readRow(record, metadata);
                            }
                            final long allocated = scope.getBean().getCurrentThreadAllocatedBytes() - before;
                            Assert.assertTrue(checksum != 0);
                            rows = count;
                            minAllocated = Math.min(minAllocated, allocated);
                        }
                    }
                    Assert.assertTrue(sql + ": " + rows, rows > 10_000);
                    Assert.assertEquals(sql, 0, minAllocated);
                }
            }
        });
    }

    @Test
    public void testNoHeapAllocationPerRowOverLateMaterializedParquetProbe() throws Exception {
        // Parquet row groups of 1_000 rows, as many as a frame.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1_000);
        assertMemoryLeak(() -> {
            createTables();
            // A probe of Parquet partitions, which the probe tasks decode late, past the probe filter.
            execute("CREATE TABLE tp AS (SELECT * FROM t) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE tp CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            assertNoHeapAllocationPerRow("SELECT tp.v, tp.s, d.w FROM tp JOIN d ON tp.k = d.k WHERE tp.v % 3 = 0");
        });
    }

    @Test
    public void testNoHeapAllocationPerRowOverParquetBuildPayload() throws Exception {
        // Parquet row groups of 1_000 rows, as many as a frame.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1_000);
        assertMemoryLeak(() -> {
            createTables();
            createVarSizeBuild("dvp");
            execute("ALTER TABLE dvp CONVERT PARTITION TO PARQUET WHERE dts >= 0");
            // The consumer's output reader decodes the build's var-size payload from its Parquet row
            // groups, which no copy holds, and keeps the row groups it decoded within its budget.
            assertNoHeapAllocationPerRow("SELECT t.v, dvp.w, dvp.str, dvp.vc FROM t LEFT JOIN dvp ON t.k = dvp.k");
        });
    }

    @Test
    public void testNoHeapAllocationPerRowGroupInFusedGroupByOverParquetBuildPayload() throws Exception {
        // The fused GROUP BY's probe tasks read the build's payload from its Parquet row groups, which
        // no copy holds, through the same readers as the join's. An execution after the first ones
        // allocates no more for a build of ten times as many row groups: the readers decode into the
        // buffers of the executions before.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, 0);
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            final long[] allocated = new long[2];
            final int[] rowGroupSizes = {1_000, 100};
            for (int i = 0; i < rowGroupSizes.length; i++) {
                setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, rowGroupSizes[i]);
                final String build = "dp" + rowGroupSizes[i];
                execute("CREATE TABLE " + build + " AS (SELECT (x % 1_800)::INT k, x w, timestamp_sequence('2024-01-01', 20_000_000) dts"
                        + " FROM long_sequence(5_000)) TIMESTAMP(dts) PARTITION BY DAY");
                execute("ALTER TABLE " + build + " CONVERT PARTITION TO PARQUET WHERE dts >= 0");
                final String sql = "SELECT t.s, sum(" + build + ".w) sw, count() c FROM t JOIN " + build + " ON t.k = " + build + ".k";
                try (
                        RecordCursorFactory factory = select(sql);
                        TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope()
                ) {
                    final AsyncHashJoinGroupByRecordCursorFactory fused = findFused(factory);
                    Assert.assertNotNull(sql, fused);
                    final RecordMetadata metadata = factory.getMetadata();
                    long minAllocated = Long.MAX_VALUE;
                    long rows = 0;
                    for (int attempt = 0; attempt < 12; attempt++) {
                        final long before = scope.getBean().getCurrentThreadAllocatedBytes();
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            final Record record = cursor.getRecord();
                            long count = 0;
                            while (cursor.hasNext()) {
                                count++;
                                readRow(record, metadata);
                            }
                            rows = count;
                            // The probe tasks read the payload from the Parquet frames.
                            Assert.assertFalse(sql, fused.getAtom().getBuildingStage().isPayloadCopied());
                        }
                        minAllocated = Math.min(minAllocated, scope.getBean().getCurrentThreadAllocatedBytes() - before);
                    }
                    Assert.assertEquals(sql, 300, rows);
                    allocated[i] = minAllocated;
                }
            }
            Assert.assertEquals("allocated per execution at 1_000 and 100 rows per row group", allocated[0], allocated[1]);
        });
    }

    @Test
    public void testParquetDecodeChargesEachExecutionsMemoryLimit() throws Exception {
        // The consumer's output reader decodes the build's Parquet row groups into buffers that it
        // keeps from one execution to the next. Each execution charges the decode to its own
        // per-query memory limit: the smallest limit that the query fits under is the same for a
        // factory whose readers decode into new buffers and for one whose readers reuse the buffers
        // of an execution before.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, 0);
        assertMemoryLeak(() -> {
            createTables();
            createVarSizeBuild("dvp");
            createVarSizeBuild("dvn");
            execute("ALTER TABLE dvp CONVERT PARTITION TO PARQUET WHERE dts >= 0");
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            final String sql = "SELECT t.v, dvp.w, dvp.vc FROM t LEFT JOIN dvp ON t.k = dvp.k";
            final String expected = ordinary(sql, sqlExecutionContext);
            try {
                final long freshLimit = smallestFittingLimit(sql, expected, null);
                // The same join over a native copy of the build decodes nothing, and fits under a
                // smaller limit.
                final String nativeSql = sql.replace("dvp", "dvn");
                final long nativeLimit = smallestFittingLimit(nativeSql, ordinary(nativeSql, sqlExecutionContext), null);
                Assert.assertTrue("Parquet " + freshLimit + ", native " + nativeLimit, freshLimit > nativeLimit);
                try (RecordCursorFactory factory = select(sql)) {
                    Assert.assertNotNull(find(factory));
                    // An execution without a limit leaves the readers' buffers to the next one.
                    Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                    Assert.assertEquals(freshLimit, smallestFittingLimit(sql, expected, factory));
                }
            } finally {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
            }
        });
    }

    @Test
    public void testNoHeapAllocationPerRowOverStacks() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            createVarSizeBuild("dv");
            assertNoHeapAllocationPerRow(
                    // Two stages of several matches per key, whose chains the consumer expands,
                    // odometer-style.
                    "SELECT t.v, d.w, e.w ew FROM t LEFT JOIN d ON t.k = d.k LEFT JOIN d e ON t.l = e.l",
                    // A swapped stack: the first stage enumerates d, the build, on the workers, and the
                    // later key reads d's column through the first stage's payload.
                    "SELECT d.w, t.v, u.w uw FROM d JOIN t ON d.k = t.k LEFT JOIN u ON d.k = u.k",
                    // A record over five sources, of a star of four joins.
                    "SELECT t.v, d.w, u.w uw, e.w ew, f.w fw FROM t LEFT JOIN d ON t.k = d.k LEFT JOIN u ON t.k = u.k"
                            + " JOIN d e ON t.l = e.l LEFT JOIN u f ON t.k = f.k",
                    // The native var-size payload, which the readers read through the address table.
                    "SELECT t.v, dv.str, dv.vc, dv.bin, dv.arr FROM t LEFT JOIN dv ON t.k = dv.k"
            );
        });
    }

    @Test
    public void testNoHeapAllocationPerRowWithMovingRingWindow() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A ring budget of two frames of tuples: the entries that fan-out grows give their memory
            // back, and the window narrows.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 2 * 1_000 * 2 * 8);
            assertNoHeapAllocationPerRow("SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v % 100");
        });
    }

    @Test
    public void testNoHeapAllocationPerRowInCount() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 4);
            for (String sql : new String[]{
                    "SELECT t.v, d.w, e.w ew FROM t LEFT JOIN d ON t.k = d.k LEFT JOIN d e ON t.l = e.l",
                    "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v % 100",
                    "SELECT d.w, t.v, u.w uw FROM d JOIN t ON d.k = t.k LEFT JOIN u ON d.k = u.k",
                    // An INNER stage of duplicate keys, whose chains the count's pass indexes.
                    "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k",
            }) {
                try (
                        RecordCursorFactory factory = select(sql);
                        TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope()
                ) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(sql, join);
                    long minAllocated = Long.MAX_VALUE;
                    long rows = 0;
                    for (int attempt = 0; attempt < 12 && minAllocated > 0; attempt++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            // The first row builds and dispatches the first frames; the count then
                            // meets the rest of the held frame, its chains, its continuation and the
                            // tasks dispatched before it, and counts the later frames' rows.
                            Assert.assertTrue(sql, cursor.hasNext());
                            final RecordCursor.Counter counter = new RecordCursor.Counter();
                            final long before = scope.getBean().getCurrentThreadAllocatedBytes();
                            cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                            final long allocated = scope.getBean().getCurrentThreadAllocatedBytes() - before;
                            rows = counter.get() + 1;
                            minAllocated = Math.min(minAllocated, allocated);
                            Assert.assertTrue(sql, join.getCountOnlyFrameCount() > 0);
                        }
                    }
                    Assert.assertTrue(sql + ": " + rows, rows > 10_000);
                    Assert.assertEquals(sql, 0, minAllocated);
                }
            }
        });
    }

    // The smallest per-query memory limit, to the byte, that the query returns its rows under, on the
    // given factory, or on a new factory for each limit when it is null; every larger limit fits too.
    private static long smallestFittingLimit(String sql, String expected, RecordCursorFactory factory) throws Exception {
        long lo = 0;
        long hi = 256 * 1024 * 1024;
        Assert.assertTrue(isFitting(sql, expected, factory, hi));
        while (hi - lo > 1) {
            final long mid = (lo + hi) >>> 1;
            if (isFitting(sql, expected, factory, mid)) {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        return hi;
    }

    private static boolean isFitting(String sql, String expected, RecordCursorFactory factory, long limit) throws Exception {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
        try {
            if (factory != null) {
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            } else {
                try (RecordCursorFactory fresh = select(sql)) {
                    Assert.assertEquals(expected, result(fresh, sqlExecutionContext));
                }
            }
            return true;
        } catch (io.questdb.cairo.CairoException e) {
            Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
            TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
            return false;
        } finally {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
        }
    }

    private static AsyncHashJoinGroupByRecordCursorFactory findFused(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncHashJoinGroupByRecordCursorFactory fused) {
                return fused;
            }
        }
        return null;
    }

    // The rows of d, with var-size payload columns.
    private static void createVarSizeBuild(String name) throws Exception {
        execute("CREATE TABLE " + name + " AS (" +
                "SELECT (x % 1_800)::INT k, x w, 's' || (x % 7) str, ('v' || (x % 11))::VARCHAR vc, " +
                "rnd_bin(1, 8, 0) bin, ARRAY[x::DOUBLE, (x % 5)::DOUBLE] arr, " +
                "timestamp_sequence('2024-01-01', 20_000_000) dts FROM long_sequence(5_000)" +
                ") TIMESTAMP(dts) PARTITION BY DAY");
    }

    // Frames of 1_000 rows, a cap of one tuple per task and a dispatch limit of four, as
    // testNoHeapAllocationPerRowInProbeReducerAndConsumer runs.
    private static void assertNoHeapAllocationPerRow(String... queries) throws Exception {
        sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 4);
        for (String sql : queries) {
            try (
                    RecordCursorFactory factory = select(sql);
                    TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope()
            ) {
                Assert.assertNotNull(sql, find(factory));
                final RecordMetadata metadata = factory.getMetadata();
                long minAllocated = Long.MAX_VALUE;
                long rows = 0;
                for (int attempt = 0; attempt < 12 && minAllocated > 0; attempt++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(sql, cursor.hasNext());
                        final Record record = cursor.getRecord();
                        // The first row's reads take the symbol tables that the record creates on
                        // first use.
                        long checksum = readRow(record, metadata);
                        final long before = scope.getBean().getCurrentThreadAllocatedBytes();
                        long count = 1;
                        while (cursor.hasNext()) {
                            count++;
                            checksum += readRow(record, metadata);
                        }
                        final long allocated = scope.getBean().getCurrentThreadAllocatedBytes() - before;
                        Assert.assertTrue(checksum != 0);
                        rows = count;
                        minAllocated = Math.min(minAllocated, allocated);
                    }
                }
                Assert.assertTrue(sql + ": " + rows, rows > 10_000);
                Assert.assertEquals(sql, 0, minAllocated);
            }
        }
    }

    private static void assertCounts(String sql, int buildsPerExecution, int roundsPerExecution, int passesPerExecution) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            for (int execution = 0; execution < 2; execution++) {
                final HashJoinBuildStage stage;
                final long builds;
                final long rounds;
                final long passes;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // The execution's first stage, as its orientation picks it.
                    stage = join.getAtom().getStage(0);
                    builds = stage.getBuildCount();
                    rounds = stage.getRoundCount();
                    passes = stage.getChainLengthIndexCount();
                    long count = 0;
                    while (cursor.hasNext()) {
                        count++;
                    }
                    // Back to the top, a count of the rows and another pass reuse the build.
                    cursor.toTop();
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(count, counter.get());
                    cursor.toTop();
                    Assert.assertTrue(cursor.hasNext());
                    cursor.toTop();
                    long again = 0;
                    while (cursor.hasNext()) {
                        again++;
                    }
                    Assert.assertEquals(count, again);
                }
                Assert.assertEquals(sql, builds + buildsPerExecution, stage.getBuildCount());
                Assert.assertEquals(sql, rounds + roundsPerExecution, stage.getRoundCount());
                Assert.assertEquals(sql, passes + passesPerExecution, stage.getChainLengthIndexCount());
            }
        }
    }

    private static void createTables() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT (x % 2_000)::INT k, x v, (x % 1_500)::LONG l, ('s' || (x % 300))::SYMBOL s,
                        timestamp_sequence('2024-01-01', 2_000_000) ts
                    FROM long_sequence(60_000)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        // d repeats its keys, u holds one row per key.
        execute("""
                CREATE TABLE d AS (
                    SELECT (x % 1_800)::INT k, x w, (x % 1_500)::LONG l, ('s' || (x % 250))::SYMBOL s
                    FROM long_sequence(5_000)
                )
                """);
        execute("CREATE TABLE u AS (SELECT x::INT k, x w FROM long_sequence(1_900))");
    }

    /**
     * Counts, once armed, the breaker checks of a count of a parallel hash join's rows outside its
     * build: the state checks, which the frames' tasks make before each frame and the steppers every so
     * many walked pairs, and the time-throttled checks, which the consumer makes, those of the pass
     * that indexes the chain lengths among them. Cancels the count once either passes its cap, so
     * that a count that checks the breaker per walked link, or per chain, fails fast. A breaker of a
     * class of its own is thread safe, so the tasks consult it directly rather than through a copy.
     */
    private static class CountCheckBreaker extends AtomicBooleanCircuitBreaker {
        private static final String CURSOR = "io.questdb.griffin.engine.table.AsyncHashJoinRecordCursor";
        private static final String HEAP = "io.questdb.griffin.engine.join.HashJoinRowHeap";
        private static final StackWalker STACK_WALKER = StackWalker.getInstance();
        private final AtomicLong passChecks = new AtomicLong();
        private final AtomicLong stateChecks = new AtomicLong();
        private final long stateCheckCap;
        private final long throttledCheckCap;
        private final AtomicLong throttledChecks = new AtomicLong();
        private volatile boolean isArmed;

        private CountCheckBreaker(long stateCheckCap, long throttledCheckCap) {
            super(engine);
            this.stateCheckCap = stateCheckCap;
            this.throttledCheckCap = throttledCheckCap;
        }

        @Override
        public int getState() {
            if (isArmed && !isOnStack(CURSOR, "build") && stateChecks.incrementAndGet() > stateCheckCap) {
                cancel();
            }
            return super.getState();
        }

        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
            if (isArmed && !isOnStack(CURSOR, "build")) {
                if (isOnStack(HEAP, "indexChainLengths")) {
                    passChecks.incrementAndGet();
                }
                if (throttledChecks.incrementAndGet() > throttledCheckCap) {
                    cancel();
                }
            }
            super.statefulThrowExceptionIfTrippedTimeThrottled();
        }

        private static boolean isOnStack(String className, String methodName) {
            return STACK_WALKER.walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(className)
                    && frame.getMethodName().equals(methodName)));
        }
    }

    // Reads every column of the row through the getter of its type, as a client would, and returns a
    // value that depends on what it read.
    private static long readRow(Record record, RecordMetadata metadata) {
        long checksum = 0;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            switch (metadata.getColumnType(i)) {
                case io.questdb.cairo.ColumnType.INT -> checksum += record.getInt(i);
                case io.questdb.cairo.ColumnType.LONG -> checksum += record.getLong(i);
                case io.questdb.cairo.ColumnType.SYMBOL -> {
                    final CharSequence value = record.getSymA(i);
                    checksum += value != null ? value.length() : 0;
                }
                case io.questdb.cairo.ColumnType.STRING -> {
                    final CharSequence value = record.getStrA(i);
                    checksum += value != null ? value.length() : 0;
                }
                case io.questdb.cairo.ColumnType.VARCHAR -> {
                    final io.questdb.std.str.Utf8Sequence value = record.getVarcharA(i);
                    checksum += value != null ? value.size() : 0;
                }
                case io.questdb.cairo.ColumnType.BINARY -> checksum += record.getBinLen(i);
                default -> {
                    final int type = metadata.getColumnType(i);
                    if (io.questdb.cairo.ColumnType.isArray(type)) {
                        checksum += record.getArray(i, type).getCardinality();
                    } else {
                        checksum++;
                    }
                }
            }
        }
        return checksum;
    }
}
