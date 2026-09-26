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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.table.HashJoinBuildFrames;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.LongPredicate;

/**
 * The parallel build of {@link IntHashJoinBuild}: frame tasks that sort each frame's rows into
 * hash buckets, partition tasks that fill each partition's region of the heap and its table, and
 * the probes of the result, against the rows the frames hold.
 */
public class IntHashJoinPartitionedBuildTest extends AbstractCairoTest {
    private static final SqlExecutionCircuitBreaker NOOP = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testCancellationInsidePartitionAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            // One partition of 150_000 rows, which checks the breaker at rows 0, 65_536 and 131_072.
            execute("CREATE TABLE t (k INT, v LONG)");
            execute("INSERT INTO t SELECT x::INT, x FROM long_sequence(150_000)");
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("t");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(1), factory.getMetadata());
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 64, 64, true)) {
                frames.of(factory, sqlExecutionContext);
                final PartitionCheckBreaker counting = new PartitionCheckBreaker(-1);
                FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildIntPartitioned(configuration, build, frames, 0,
                        1_000_000, -1, null, tracker, counting);
                Assert.assertEquals(1, build.getPartitionCount());
                Assert.assertEquals(3, counting.partitionChecks);
                Assert.assertEquals(150_000, frozen.getKeyCount());
                build.close();
                Assert.assertEquals(0, tracker.getUsed());

                for (int trip = 1; trip <= 3; trip++) {
                    final PartitionCheckBreaker cancelling = new PartitionCheckBreaker(trip);
                    CairoException error = Assert.assertThrows(CairoException.class, () -> FrameBuilds.buildIntPartitioned(
                            configuration, build, frames, 0, 1_000_000, -1, null, tracker, cancelling));
                    Assert.assertTrue(error.isCancellation());
                    Assert.assertEquals("cancellation releases every allocation", 0, tracker.getUsed());
                    Assert.assertEquals(0, build.getSizeInBytes());
                }

                frozen = FrameBuilds.buildIntPartitioned(configuration, build, frames, 0, 1_000_000, -1, null, tracker, NOOP);
                try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                    for (int key = 1; key <= 150_000; key += 4_999) {
                        Assert.assertTrue(probe.findSingleUnchecked(key));
                        Assert.assertEquals(key, probe.getRecord().getLong(0));
                    }
                    Assert.assertFalse(probe.findSingleUnchecked(150_001));
                }
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testChainsKeepInputOrderAcrossFramesAndPartitions() throws Exception {
        assertMemoryLeak(() -> {
            // Ten daily partitions of 1_000 rows, 101 keys plus NULL, so that every chain crosses
            // frames and table partitions.
            execute("CREATE TABLE t (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t
                    SELECT CASE WHEN x % 97 = 0 THEN NULL ELSE (x * 7_919 % 101 - 50)::INT END, x,
                           timestamp_sequence('2020-01-01', 86_400_000L)
                    FROM long_sequence(10_000)
                    """);
            sqlExecutionContext.changePageFrameSizes(100, 300);
            final LongPredicate everyRow = null;
            final LongPredicate twoRowsInThree = rowId -> Rows.toLocalRowID(rowId) % 3 != 1;
            final LongPredicate firstFrameOnly = rowId -> Rows.toPartitionIndex(rowId) == 0;
            for (boolean hasPayload : new boolean[]{true, false}) {
                try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                     RecordCursorFactory factory = select("t");
                     HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, hasPayload ? ints(1) : ints(),
                             factory.getMetadata());
                     IntHashJoinBuild build = new IntHashJoinBuild(hasPayload, 2, 16)) {
                    frames.of(factory, sqlExecutionContext);
                    Assert.assertTrue(frames.getFrameCount() > 20);
                    Class<?> serialProbeClass = null;
                    Class<?> partitionedProbeClass = null;
                    for (LongPredicate keep : new LongPredicate[]{everyRow, twoRowsInThree, firstFrameOnly}) {
                        final Map<Integer, List<Long>> expected = expectedChains(frames, keep);
                        for (long rowsPerPartition : new long[]{1, 7, 1_000_000}) {
                            FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildIntPartitioned(configuration, build, frames, 0,
                                    rowsPerPartition, -1, keep, tracker, NOOP);
                            final int partitionCount = build.getPartitionCount();
                            if (rowsPerPartition == 1_000_000) {
                                Assert.assertEquals(1, partitionCount);
                            } else {
                                Assert.assertTrue("partitions: " + partitionCount, partitionCount > 1 && partitionCount <= IntHashJoinBuild.MAX_PARTITIONS);
                            }
                            assertChains(frozen, expected, hasPayload);
                            assertSegmentsCoverTheHeap(build, frozen, frames, hasPayload);
                            try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                                if (partitionCount == 1) {
                                    serialProbeClass = probe.getClass();
                                } else {
                                    partitionedProbeClass = probe.getClass();
                                }
                            }
                            build.close();
                            Assert.assertEquals(0, tracker.getUsed());
                        }
                    }
                    // A build of one partition freezes as a serial build, and probes it the same way.
                    Assert.assertNotNull(serialProbeClass);
                    Assert.assertNotNull(partitionedProbeClass);
                    Assert.assertNotEquals(serialProbeClass, partitionedProbeClass);
                    build.open(null, NOOP);
                    build.append(1, 0);
                    try (FrozenHashJoinBuild.IntProbe probe = build.freeze(frames).newProbe()) {
                        Assert.assertEquals(serialProbeClass, probe.getClass());
                    }
                    build.close();
                }
            }
        });
    }

    @Test
    public void testConcurrentTasksMatchSerialBuildAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            // Four days of 50_000 rows over 50_000 keys: every key has a row in each day.
            execute("CREATE TABLE t (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t
                    SELECT (x % 50_000)::INT, x, timestamp_sequence('2020-01-01', 1_728_000L)
                    FROM long_sequence(200_000)
                    """);
            sqlExecutionContext.changePageFrameSizes(5_000, 10_000);
            final ExecutorService executor = Executors.newFixedThreadPool(4);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("t");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(1), factory.getMetadata());
                 IntHashJoinBuild serial = new IntHashJoinBuild(true, 64, 64);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 64, 64, true)) {
                final FrozenHashJoinBuild.IntKeyed expected = FrameBuilds.buildInt(configuration, serial, frames, factory, 0, sqlExecutionContext);
                Assert.assertTrue(frames.getFrameCount() > 16);
                for (int execution = 0; execution < 3; execution++) {
                    build.open(tracker, NOOP);
                    build.beginPartitioning(frames.getFrameCount(), frames.getRowCount(), 4_096);
                    final List<Future<?>> tasks = new ArrayList<>();
                    for (int frameIndex = 0; frameIndex < frames.getFrameCount(); frameIndex++) {
                        final int frame = frameIndex;
                        tasks.add(executor.submit(() -> {
                            try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
                                 PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
                                pool.of(frames.getAddressCache());
                                record.init(pool.navigateTo(frame));
                                build.partitionFrame(frame, record, 0, frames.getFrameRowCount(frame));
                            }
                        }));
                    }
                    awaitAll(tasks);
                    final int partitionCount = build.planPartitions(4_096, -1);
                    Assert.assertEquals(64, partitionCount);
                    for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                        final int partition = partitionIndex;
                        tasks.add(executor.submit(() -> build.buildPartition(partition, NOOP)));
                    }
                    awaitAll(tasks);
                    final FrozenHashJoinBuild.IntKeyed frozen = build.freezePartitioned(frames);
                    Assert.assertEquals(expected.getRowCount(), frozen.getRowCount());
                    Assert.assertEquals(expected.getKeyCount(), frozen.getKeyCount());
                    try (FrozenHashJoinBuild.IntProbe serialProbe = expected.newProbe();
                         FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                        for (int key = -1; key <= 50_000; key++) {
                            serialProbe.find(key);
                            probe.findUnchecked(key);
                            while (serialProbe.hasNext()) {
                                Assert.assertTrue(probe.hasNext());
                                serialProbe.next();
                                probe.next();
                                Assert.assertEquals(serialProbe.getRecord().getLong(0), probe.getRecord().getLong(0));
                            }
                            Assert.assertFalse(probe.hasNext());
                        }
                    }
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                }
            } finally {
                executor.shutdownNow();
            }
        });
    }

    @Test
    public void testIllegalSequencesCloseTheBuild() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT, v LONG)");
            execute("INSERT INTO t SELECT (x % 10)::INT, x FROM long_sequence(100)");
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("t");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(1), factory.getMetadata());
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                frames.of(factory, sqlExecutionContext);
                // A build that appended rows cannot switch to partitions.
                build.open(tracker, NOOP);
                build.append(1, 0);
                Assert.assertThrows(IllegalStateException.class, () -> build.beginPartitioning(1, 100, 10));
                Assert.assertEquals(0, tracker.getUsed());
                // A partitioned build freezes through its own method, and a serial one through the other.
                build.open(tracker, NOOP);
                build.beginPartitioning(frames.getFrameCount(), frames.getRowCount(), 10);
                Assert.assertThrows(IllegalStateException.class, () -> build.freeze(frames));
                Assert.assertEquals(0, tracker.getUsed());
                build.open(tracker, NOOP);
                Assert.assertThrows(IllegalStateException.class, () -> build.freezePartitioned(frames));
                Assert.assertThrows(IllegalStateException.class, () -> build.planPartitions(10, -1));
                Assert.assertEquals(0, tracker.getUsed());
                // The build is still usable.
                FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildIntPartitioned(configuration, build, frames, 0, 10, -1, null, tracker, NOOP);
                Assert.assertEquals(100, frozen.getRowCount());
                Assert.assertEquals(10, frozen.getKeyCount());
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testKeyCountHintPresizesPartitionTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT)");
            execute("INSERT INTO t SELECT x::INT FROM long_sequence(100_000)");
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("t");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(), factory.getMetadata());
                 IntHashJoinBuild build = new IntHashJoinBuild(false, 64, 64)) {
                frames.of(factory, sqlExecutionContext);
                final PartitionCheckBreaker grown = new PartitionCheckBreaker(-1);
                FrameBuilds.buildIntPartitioned(configuration, build, frames, 0, 8_192, -1, null, tracker, grown);
                final int partitionCount = build.getPartitionCount();
                Assert.assertEquals(16, partitionCount);
                build.close();
                // Each partition presizes for its own rows, so no table rehashes a key: every rehash
                // check comes from the presize, which runs once per partition over the empty table.
                final PartitionCheckBreaker presized = new PartitionCheckBreaker(-1);
                FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildIntPartitioned(configuration, build, frames, 0, 8_192,
                        100_000, null, tracker, presized);
                Assert.assertEquals(100_000, frozen.getKeyCount());
                Assert.assertEquals(partitionCount, presized.rehashChecks);
                Assert.assertTrue("grown: " + grown.rehashChecks, grown.rehashChecks > 5 * partitionCount);
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testMemoryLimitAtEveryPartitionedAllocationAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t
                    SELECT (x % 500)::INT, x, timestamp_sequence('2020-01-01', 123_428_571L)
                    FROM long_sequence(2_100)
                    """);
            sqlExecutionContext.changePageFrameSizes(100, 300);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("t");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(1), factory.getMetadata());
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16, true)) {
                frames.of(factory, sqlExecutionContext);
                final Map<Integer, List<Long>> expected = expectedChains(frames, null);
                final Set<String> failedSites = new HashSet<>();
                // A zero limit is no limit.
                long limit = 1;
                FrozenHashJoinBuild.IntKeyed frozen = null;
                while (frozen == null) {
                    tracker.setLimit(limit);
                    try {
                        frozen = FrameBuilds.buildIntPartitioned(configuration, build, frames, 0, 64, -1, null, tracker, NOOP);
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "memory limit exceeded");
                        Assert.assertEquals("a failed build releases every allocation", 0, tracker.getUsed());
                        failedSites.add(buildSite(e));
                        limit += 16;
                    }
                }
                Assert.assertTrue("partitions: " + build.getPartitionCount(), build.getPartitionCount() > 1);
                // Every allocation up to the peak failed once: the initial table, the bucket tables, a
                // frame's chunk, the heap, and a partition's table or its growth. The directory never
                // does: the chunks, which it replaces, are larger.
                Assert.assertTrue(failedSites.toString(), failedSites.containsAll(List.of(
                        "open", "beginPartitioning", "partitionFrame", "planPartitions", "buildPartition")));
                assertChains(frozen, expected, true);
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
                tracker.setLimit(Long.MAX_VALUE);
                frozen = FrameBuilds.buildIntPartitioned(configuration, build, frames, 0, 64, -1, null, tracker, NOOP);
                assertChains(frozen, expected, true);
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testReusableProbesFollowTheirOwnSnapshotKind() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT, v LONG)");
            execute("INSERT INTO t SELECT x::INT, x * 10 FROM long_sequence(1_000)");
            try (RecordCursorFactory factory = select("t");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(1), factory.getMetadata());
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 64, 64, true)) {
                // A serial execution first.
                FrozenHashJoinBuild.IntProbe serialProbe = FrameBuilds.buildInt(configuration, build, frames, factory, 0, sqlExecutionContext).newProbe();
                FrozenHashJoinBuild.IntProbe partitionedProbe = null;
                try {
                    assertFinds(serialProbe);
                    serialProbe.close();
                    build.close();
                    frames.clear();

                    // A partitioned one: the serial probe's snapshot has expired, the partitioned kind is current.
                    frames.of(factory, sqlExecutionContext);
                    FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildIntPartitioned(configuration, build, frames, 0, 16, -1, null, null, NOOP);
                    Assert.assertTrue(build.getPartitionCount() > 1);
                    final FrozenHashJoinBuild.IntProbe expiredProbe = serialProbe;
                    Assert.assertThrows(IllegalStateException.class, expiredProbe::reopen);
                    partitionedProbe = frozen.newProbe();
                    assertFinds(partitionedProbe);
                    partitionedProbe.close();
                    build.close();

                    // Each kind reopens over the next snapshot of its own kind.
                    frames.clear();
                    frames.of(factory, sqlExecutionContext);
                    FrameBuilds.buildIntPartitioned(configuration, build, frames, 0, 16, -1, null, null, NOOP);
                    partitionedProbe.reopen();
                    assertFinds(partitionedProbe);
                    partitionedProbe.close();
                    build.close();
                    frames.clear();
                    FrameBuilds.buildInt(configuration, build, frames, factory, 0, sqlExecutionContext);
                    final FrozenHashJoinBuild.IntProbe stalePartitionedProbe = partitionedProbe;
                    Assert.assertThrows(IllegalStateException.class, stalePartitionedProbe::reopen);
                    serialProbe.reopen();
                    assertFinds(serialProbe);
                } finally {
                    serialProbe.close();
                    if (partitionedProbe != null) {
                        partitionedProbe.close();
                    }
                }
                build.close();
            }
        });
    }

    private static void assertChains(FrozenHashJoinBuild.IntKeyed frozen, Map<Integer, List<Long>> expected, boolean hasPayload) {
        long rowCount = 0;
        for (List<Long> values : expected.values()) {
            rowCount += values.size();
        }
        Assert.assertEquals(rowCount, frozen.getRowCount());
        Assert.assertEquals(expected.size(), frozen.getKeyCount());
        final boolean isUnique = rowCount == expected.size();
        try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
            final IntList keys = new IntList();
            keys.add(Numbers.INT_NULL);
            for (int key = -60; key <= 600; key++) {
                keys.add(key);
            }
            for (int i = 0, n = keys.size(); i < n; i++) {
                final int key = keys.getQuick(i);
                final List<Long> values = expected.get(key);
                for (int pass = 0; pass < 2; pass++) {
                    if (pass == 0) {
                        probe.find(key);
                    } else {
                        probe.findUnchecked(key);
                    }
                    if (values != null) {
                        // A chain runs from the key's last row to its first, as a serial build's does.
                        for (int j = values.size() - 1; j >= 0; j--) {
                            Assert.assertTrue(probe.hasNext());
                            final long handle = probe.next();
                            if (hasPayload) {
                                Assert.assertEquals(values.get(j).longValue(), probe.getRecord().getLong(0));
                                probe.recordAt(handle);
                                Assert.assertEquals(values.get(j).longValue(), probe.getRecord().getLong(0));
                            }
                        }
                    }
                    Assert.assertFalse(probe.hasNext());
                }
                if (isUnique) {
                    Assert.assertEquals(values != null, probe.findSingleUnchecked(key));
                    if (values != null && hasPayload) {
                        Assert.assertEquals(values.get(0).longValue(), probe.getRecord().getLong(0));
                    }
                }
            }
        }
    }

    private static void assertFinds(FrozenHashJoinBuild.IntProbe probe) {
        for (int key = 1; key <= 1_000; key += 37) {
            Assert.assertTrue(probe.findSingleUnchecked(key));
            Assert.assertEquals(key * 10L, probe.getRecord().getLong(0));
        }
        Assert.assertFalse(probe.findSingleUnchecked(0));
    }

    // Every heap ordinal belongs to exactly one frame's segment of one partition, and holds a row of that frame.
    private static void assertSegmentsCoverTheHeap(IntHashJoinBuild build, FrozenHashJoinBuild.IntKeyed frozen,
                                                   HashJoinBuildFrames frames, boolean hasPayload) {
        final long rowCount = frozen.getRowCount();
        final boolean[] covered = new boolean[(int) rowCount];
        for (int frame = 0, frameCount = frames.getFrameCount(); frame < frameCount; frame++) {
            for (int partition = 0, n = build.getPartitionCount(); partition < n; partition++) {
                final long start = build.getSegmentStart(frame, partition);
                final long count = build.getSegmentRowCount(frame, partition);
                for (long ordinal = start; ordinal < start + count; ordinal++) {
                    Assert.assertFalse(covered[(int) ordinal]);
                    covered[(int) ordinal] = true;
                    if (hasPayload) {
                        Assert.assertEquals(frame, Rows.toPartitionIndex(frozen.getRowId(ordinal)));
                    }
                }
            }
        }
        for (boolean isCovered : covered) {
            Assert.assertTrue(isCovered);
        }
    }

    private static void awaitAll(List<Future<?>> tasks) throws Exception {
        for (int i = 0, n = tasks.size(); i < n; i++) {
            tasks.get(i).get();
        }
        tasks.clear();
    }

    // The method of IntHashJoinBuild whose allocation failed.
    private static String buildSite(Throwable error) {
        for (StackTraceElement element : error.getStackTrace()) {
            if (element.getClassName().equals(IntHashJoinBuild.class.getName())) {
                return element.getMethodName();
            }
        }
        return "";
    }

    // The payload value (column 1) of every row the frames keep, per key, in the frames' order.
    private static Map<Integer, List<Long>> expectedChains(HashJoinBuildFrames frames, LongPredicate keep) {
        final Map<Integer, List<Long>> expected = new HashMap<>();
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
            pool.of(frames.getAddressCache());
            for (int frame = 0, frameCount = frames.getFrameCount(); frame < frameCount; frame++) {
                record.init(pool.navigateTo(frame));
                for (long row = 0, n = frames.getFrameRowCount(frame); row < n; row++) {
                    if (keep == null || keep.test(Rows.toRowID(frame, row))) {
                        record.setRowIndex(row);
                        expected.computeIfAbsent(record.getInt(0), k -> new ArrayList<>()).add(record.getLong(1));
                    }
                }
            }
        }
        return expected;
    }

    private static IntList ints(int... values) {
        final IntList list = new IntList();
        for (int value : values) {
            list.add(value);
        }
        return list;
    }

    /**
     * Counts the checks that partition tasks make per row run and that key tables make while they
     * rehash, and cancels on the given partition check, counting from one; -1 never cancels.
     */
    private static class PartitionCheckBreaker extends CountingSqlExecutionCircuitBreaker {
        private final int tripAt;
        private final StackWalker walker = StackWalker.getInstance();
        private int partitionChecks;
        private int rehashChecks;

        private PartitionCheckBreaker(int tripAt) {
            super(NOOP);
            this.tripAt = tripAt;
        }

        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
            super.statefulThrowExceptionIfTrippedTimeThrottled();
            final String site = walker.walk(frames -> frames
                    .filter(frame -> frame.getClassName().startsWith("io.questdb.griffin.engine.join."))
                    .findFirst()
                    .map(StackWalker.StackFrame::getMethodName)
                    .orElse(""));
            switch (site) {
                case "buildPartition" -> {
                    if (++partitionChecks == tripAt) {
                        throw CairoException.queryCancelled(1);
                    }
                }
                case "grow" -> rehashChecks++;
                default -> {
                }
            }
        }
    }
}
