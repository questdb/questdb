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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.map.MapProbeView;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.MapHashJoinBuild;
import io.questdb.griffin.engine.table.HashJoinBuildFrames;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.str.Utf8Sequence;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.LongPredicate;

/**
 * The parallel build of {@link MapHashJoinBuild}: frame tasks that stage each frame's keys and sort
 * the rows into hash buckets, partition tasks that fill each partition's map and its region of the
 * heap, and the probes of the result, against the rows the frames hold. The keys cover the three
 * map layouts: a LONG key's {@code Unordered8Map}, whose zero key lives outside the table, and the
 * {@code OrderedMap} of a fixed-size and of a var-size composite key.
 */
public class MapHashJoinPartitionedBuildTest extends AbstractCairoTest {
    private static final SqlExecutionCircuitBreaker NOOP = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
    // Columns of t: l LONG, i INT, s VARCHAR, v LONG, ts TIMESTAMP. v is the payload.
    private static final int PAYLOAD_COLUMN = 3;

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testCancellationInsidePartitionAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            // One partition of 150_000 rows, which checks the breaker as it starts and at rows 0,
            // 65_536 and 131_072.
            execute("CREATE TABLE t (l LONG, i INT, s VARCHAR, v LONG)");
            execute("INSERT INTO t SELECT x, (x % 13)::INT, 'k' || x, x FROM long_sequence(150_000)");
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("t");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(PAYLOAD_COLUMN), factory.getMetadata());
                 MapHashJoinBuild build = KeyLayout.COMPOSITE_VAR_SIZE.newBuild(true)) {
                frames.of(factory, sqlExecutionContext);
                final RecordSink sink = KeyLayout.COMPOSITE_VAR_SIZE.newSink(factory.getMetadata());
                final PartitionCheckBreaker counting = new PartitionCheckBreaker(-1);
                FrozenHashJoinBuild.RecordKeyed frozen = FrameBuilds.buildMapPartitioned(configuration, build, frames, sink,
                        1_000_000, -1, null, tracker, counting);
                Assert.assertEquals(1, build.getPartitionCount());
                Assert.assertEquals(4, counting.partitionChecks);
                Assert.assertEquals(150_000, frozen.getKeyCount());
                build.close();
                Assert.assertEquals(0, tracker.getUsed());

                for (int trip = 1; trip <= 4; trip++) {
                    final PartitionCheckBreaker cancelling = new PartitionCheckBreaker(trip);
                    CairoException error = Assert.assertThrows(CairoException.class, () -> FrameBuilds.buildMapPartitioned(
                            configuration, build, frames, sink, 1_000_000, -1, null, tracker, cancelling));
                    Assert.assertTrue(error.isCancellation());
                    Assert.assertEquals("cancellation releases every allocation", 0, tracker.getUsed());
                    Assert.assertEquals(0, build.getSizeInBytes());
                }

                frozen = FrameBuilds.buildMapPartitioned(configuration, build, frames, sink, 1_000_000, -1, null, tracker, NOOP);
                final Map<String, List<Long>> expected = expectedChains(frames, KeyLayout.COMPOSITE_VAR_SIZE, null);
                assertChains(frozen, frames, KeyLayout.COMPOSITE_VAR_SIZE, factory.getMetadata(), expected, true);
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testChainsKeepInputOrderAcrossFramesAndPartitions() throws Exception {
        assertMemoryLeak(() -> {
            // Ten daily partitions of 500 rows. Every key repeats across frames and table
            // partitions, and each key column holds NULL; the LONG column holds zero as well, and
            // the VARCHAR column the empty string.
            createKeyTable(5_000, 172_800_000L);
            sqlExecutionContext.changePageFrameSizes(100, 300);
            final LongPredicate twoRowsInThree = rowId -> Rows.toLocalRowID(rowId) % 3 != 1;
            final LongPredicate firstFrameOnly = rowId -> Rows.toPartitionIndex(rowId) == 0;
            for (KeyLayout layout : KeyLayout.values()) {
                for (boolean hasPayload : new boolean[]{true, false}) {
                    try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                         RecordCursorFactory factory = select("t");
                         HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, hasPayload ? ints(PAYLOAD_COLUMN) : ints(),
                                 factory.getMetadata());
                         MapHashJoinBuild build = layout.newBuild(hasPayload)) {
                        frames.of(factory, sqlExecutionContext);
                        Assert.assertEquals(20, frames.getFrameCount());
                        final RecordSink sink = layout.newSink(factory.getMetadata());
                        Class<?> serialProbeClass = null;
                        Class<?> partitionedProbeClass = null;
                        for (LongPredicate keep : new LongPredicate[]{null, twoRowsInThree, firstFrameOnly}) {
                            final Map<String, List<Long>> expected = expectedChains(frames, layout, keep);
                            for (long rowsPerPartition : new long[]{1, 7, 1_000_000}) {
                                final FrozenHashJoinBuild.RecordKeyed frozen = FrameBuilds.buildMapPartitioned(configuration, build,
                                        frames, sink, rowsPerPartition, -1, keep, tracker, NOOP);
                                final int partitionCount = build.getPartitionCount();
                                if (rowsPerPartition == 1_000_000) {
                                    Assert.assertEquals(1, partitionCount);
                                } else {
                                    Assert.assertTrue("partitions: " + partitionCount, partitionCount > 1 && partitionCount <= MapHashJoinBuild.MAX_PARTITIONS);
                                }
                                assertChains(frozen, frames, layout, factory.getMetadata(), expected, hasPayload);
                                assertSegmentsCoverTheHeap(build, frozen, frames, hasPayload);
                                try (FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(layout.newSink(factory.getMetadata()))) {
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
                        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
                             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
                            pool.of(frames.getAddressCache());
                            record.init(pool.navigateTo(0));
                            record.setRowIndex(0);
                            build.append(record, sink);
                        }
                        try (FrozenHashJoinBuild.RecordProbe probe = build.freeze(frames).newProbe(layout.newSink(factory.getMetadata()))) {
                            Assert.assertEquals(serialProbeClass, probe.getClass());
                        }
                        build.close();
                    }
                }
            }
        });
    }

    @Test
    public void testConcurrentTasksMatchSerialBuildAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            // Four days of 50_000 rows over 50_000 var-size keys: every key has a row in each day.
            execute("CREATE TABLE t (l LONG, i INT, s VARCHAR, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t
                    SELECT x % 50_000, (x % 50_000 % 7)::INT, 'key' || (x % 50_000), x, timestamp_sequence('2020-01-01', 1_728_000L)
                    FROM long_sequence(200_000)
                    """);
            sqlExecutionContext.changePageFrameSizes(5_000, 10_000);
            final int workerCount = 4;
            final ExecutorService executor = Executors.newFixedThreadPool(workerCount);
            final KeyLayout layout = KeyLayout.COMPOSITE_VAR_SIZE;
            final ObjList<RecordSink> workerSinks = new ObjList<>();
            final ObjList<MapProbeView> workerStagers = new ObjList<>();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("t");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(PAYLOAD_COLUMN), factory.getMetadata());
                 MapHashJoinBuild serial = layout.newBuild(true);
                 MapHashJoinBuild build = layout.newReusableBuild(true)) {
                final FrozenHashJoinBuild.RecordKeyed expected = FrameBuilds.buildMap(configuration, serial, frames, factory,
                        layout.newSink(factory.getMetadata()), sqlExecutionContext);
                Assert.assertTrue(frames.getFrameCount() > 16);
                for (int w = 0; w < workerCount; w++) {
                    // Sinks hold scratch state, and a stager holds the key it staged, so each worker has its own.
                    workerSinks.add(layout.newSink(factory.getMetadata()));
                    workerStagers.add(build.newKeyStager());
                }
                final List<Future<?>> tasks = new ArrayList<>();
                for (int execution = 0; execution < 3; execution++) {
                    build.open(tracker, NOOP);
                    build.beginPartitioning(frames.getFrameCount(), frames.getRowCount(), 4_096);
                    for (int w = 0; w < workerCount; w++) {
                        final int worker = w;
                        tasks.add(executor.submit(() -> {
                            try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
                                 PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
                                pool.of(frames.getAddressCache());
                                for (int frame = worker; frame < frames.getFrameCount(); frame += workerCount) {
                                    record.init(pool.navigateTo(frame));
                                    build.partitionFrame(frame, record, workerSinks.getQuick(worker), workerStagers.getQuick(worker),
                                            frames.getFrameRowCount(frame));
                                }
                            }
                        }));
                    }
                    awaitAll(tasks);
                    final int partitionCount = build.planPartitions(4_096, -1);
                    Assert.assertEquals(64, partitionCount);
                    for (int w = 0; w < workerCount; w++) {
                        final int worker = w;
                        tasks.add(executor.submit(() -> {
                            for (int partition = worker; partition < partitionCount; partition += workerCount) {
                                build.buildPartition(partition, NOOP);
                            }
                        }));
                    }
                    awaitAll(tasks);
                    final FrozenHashJoinBuild.RecordKeyed frozen = build.freezePartitioned(frames);
                    Assert.assertEquals(expected.getRowCount(), frozen.getRowCount());
                    Assert.assertEquals(expected.getKeyCount(), frozen.getKeyCount());
                    assertSameChains(expected, frozen, frames, layout, factory.getMetadata());
                    // The stagers release what this execution charged, as the worker slots do.
                    Misc.freeObjListAndKeepObjects(workerStagers);
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                }
            } finally {
                executor.shutdownNow();
                Misc.freeObjList(workerStagers);
            }
        });
    }

    @Test
    public void testIllegalSequencesCloseTheBuild() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (l LONG, i INT, s VARCHAR, v LONG)");
            execute("INSERT INTO t SELECT x % 10, (x % 10)::INT, 'k' || (x % 10), x FROM long_sequence(100)");
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("t");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(PAYLOAD_COLUMN), factory.getMetadata());
                 MapHashJoinBuild build = KeyLayout.LONG.newBuild(true)) {
                frames.of(factory, sqlExecutionContext);
                final RecordSink sink = KeyLayout.LONG.newSink(factory.getMetadata());
                // A build that appended rows cannot switch to partitions.
                build.open(tracker, NOOP);
                try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
                     PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
                    pool.of(frames.getAddressCache());
                    record.init(pool.navigateTo(0));
                    record.setRowIndex(0);
                    build.append(record, sink);
                }
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
                FrozenHashJoinBuild.RecordKeyed frozen = FrameBuilds.buildMapPartitioned(configuration, build, frames, sink, 10, -1, null, tracker, NOOP);
                Assert.assertEquals(100, frozen.getRowCount());
                Assert.assertEquals(10, frozen.getKeyCount());
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testKeyCountHintPresizesPartitionMaps() throws Exception {
        assertMemoryLeak(() -> {
            // 100_000 rows over 1_000 keys in every layout.
            execute("CREATE TABLE t (l LONG, i INT, s VARCHAR, v LONG)");
            execute("INSERT INTO t SELECT x % 1_000, (x % 1_000 % 13)::INT, 'k' || (x % 1_000), x FROM long_sequence(100_000)");
            final double loadFactor = configuration.getSqlFastMapLoadFactor();
            for (KeyLayout layout : KeyLayout.values()) {
                try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                     RecordCursorFactory factory = select("t");
                     HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(), factory.getMetadata());
                     MapHashJoinBuild build = layout.newBuild(false)) {
                    frames.of(factory, sqlExecutionContext);
                    final RecordSink sink = layout.newSink(factory.getMetadata());
                    FrameBuilds.buildMapPartitioned(configuration, build, frames, sink, 8_192, -1, null, tracker, NOOP);
                    Assert.assertEquals(16, build.getPartitionCount());
                    final LongList grown = keyCapacities(build);
                    build.close();
                    // A key hint of the row count, as the operator passes for a build no larger than its
                    // probe, gives each partition the smaller of its rows and twice its share of the
                    // hint: its rows here, since the keys repeat. The maps that grew hold the keys alone.
                    FrozenHashJoinBuild.RecordKeyed frozen = FrameBuilds.buildMapPartitioned(configuration, build, frames, sink, 8_192,
                            100_000, null, tracker, NOOP);
                    Assert.assertEquals(1_000, frozen.getKeyCount());
                    final LongList presized = keyCapacities(build);
                    for (int p = 0; p < 16; p++) {
                        final long hint = Math.min(build.getPartitionRowCount(p), 12_500);
                        Assert.assertEquals(layout + " partition " + p, Numbers.ceilPow2((int) (hint / loadFactor)), presized.getQuick(p));
                        Assert.assertTrue(layout + " partition " + p + ": " + grown, grown.getQuick(p) < presized.getQuick(p));
                    }
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testMemoryLimitAtEveryPartitionedAllocationAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTable(2_100, 123_428_571L);
            sqlExecutionContext.changePageFrameSizes(100, 300);
            for (KeyLayout layout : KeyLayout.values()) {
                try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                     RecordCursorFactory factory = select("t");
                     HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(PAYLOAD_COLUMN), factory.getMetadata());
                     MapHashJoinBuild build = layout.newReusableBuild(true)) {
                    frames.of(factory, sqlExecutionContext);
                    final RecordSink sink = layout.newSink(factory.getMetadata());
                    final Map<String, List<Long>> expected = expectedChains(frames, layout, null);
                    final Set<String> failedSites = new HashSet<>();
                    // A zero limit is no limit.
                    long limit = 1;
                    FrozenHashJoinBuild.RecordKeyed frozen = null;
                    while (frozen == null) {
                        tracker.setLimit(limit);
                        try {
                            frozen = FrameBuilds.buildMapPartitioned(configuration, build, frames, sink, 64, -1, null, tracker, NOOP);
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "memory limit exceeded");
                            Assert.assertEquals("a failed build releases every allocation", 0, tracker.getUsed());
                            failedSites.add(buildSite(e));
                            limit += 16;
                        }
                    }
                    Assert.assertTrue("partitions: " + build.getPartitionCount(), build.getPartitionCount() > 1);
                    // Every allocation up to the peak failed once: the first map, the bucket tables, a
                    // stager's key or a frame's chunk, the heap, and a partition's map or its growth.
                    Assert.assertTrue(layout + ": " + failedSites, failedSites.containsAll(List.of(
                            "open", "beginPartitioning", "partitionFrame", "planPartitions", "buildPartition")));
                    assertChains(frozen, frames, layout, factory.getMetadata(), expected, true);
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                    tracker.setLimit(Long.MAX_VALUE);
                    frozen = FrameBuilds.buildMapPartitioned(configuration, build, frames, sink, 64, -1, null, tracker, NOOP);
                    assertChains(frozen, frames, layout, factory.getMetadata(), expected, true);
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testReusableProbesFollowTheirOwnSnapshotKind() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (l LONG, i INT, s VARCHAR, v LONG)");
            execute("INSERT INTO t SELECT x, (x % 13)::INT, 'k' || x, x * 10 FROM long_sequence(1_000)");
            final KeyLayout layout = KeyLayout.LONG;
            try (RecordCursorFactory factory = select("t");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(PAYLOAD_COLUMN), factory.getMetadata());
                 MapHashJoinBuild build = layout.newReusableBuild(true)) {
                final RecordMetadata metadata = factory.getMetadata();
                final RecordSink buildSink = layout.newSink(metadata);
                // A serial execution first.
                FrozenHashJoinBuild.RecordProbe serialProbe = FrameBuilds.buildMap(configuration, build, frames, factory, buildSink, sqlExecutionContext)
                        .newProbe(layout.newSink(metadata));
                FrozenHashJoinBuild.RecordProbe partitionedProbe = null;
                try {
                    assertFinds(serialProbe, frames);
                    serialProbe.close();
                    build.close();
                    frames.clear();

                    // A partitioned one: the serial probe's snapshot has expired, the partitioned kind is current.
                    frames.of(factory, sqlExecutionContext);
                    FrozenHashJoinBuild.RecordKeyed frozen = FrameBuilds.buildMapPartitioned(configuration, build, frames, buildSink,
                            16, -1, null, null, NOOP);
                    Assert.assertTrue(build.getPartitionCount() > 1);
                    final FrozenHashJoinBuild.RecordProbe expiredProbe = serialProbe;
                    Assert.assertThrows(IllegalStateException.class, expiredProbe::reopen);
                    partitionedProbe = frozen.newProbe(layout.newSink(metadata));
                    assertFinds(partitionedProbe, frames);
                    partitionedProbe.close();
                    build.close();

                    // Each kind reopens over the next snapshot of its own kind.
                    frames.clear();
                    frames.of(factory, sqlExecutionContext);
                    FrameBuilds.buildMapPartitioned(configuration, build, frames, buildSink, 16, -1, null, null, NOOP);
                    partitionedProbe.reopen();
                    assertFinds(partitionedProbe, frames);
                    partitionedProbe.close();
                    build.close();
                    frames.clear();
                    FrameBuilds.buildMap(configuration, build, frames, factory, buildSink, sqlExecutionContext);
                    final FrozenHashJoinBuild.RecordProbe stalePartitionedProbe = partitionedProbe;
                    Assert.assertThrows(IllegalStateException.class, stalePartitionedProbe::reopen);
                    serialProbe.reopen();
                    assertFinds(serialProbe, frames);
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

    /**
     * Probes one row of every key the frames hold, kept or not, and checks the chain of payload
     * values the probe walks, through every lookup a probe offers.
     */
    private static void assertChains(
            FrozenHashJoinBuild.RecordKeyed frozen,
            HashJoinBuildFrames frames,
            KeyLayout layout,
            RecordMetadata metadata,
            Map<String, List<Long>> expected,
            boolean hasPayload
    ) {
        long rowCount = 0;
        for (List<Long> values : expected.values()) {
            rowCount += values.size();
        }
        Assert.assertEquals(rowCount, frozen.getRowCount());
        Assert.assertEquals(expected.size(), frozen.getKeyCount());
        final boolean isUnique = rowCount == expected.size();
        final Map<String, Long> probeRows = keyRows(frames, layout);
        Assert.assertTrue(probeRows.keySet().containsAll(expected.keySet()));
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
             FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(layout.newSink(metadata))) {
            pool.of(frames.getAddressCache());
            record.of(frames.getSymbolTableSource());
            for (Map.Entry<String, Long> probeRow : probeRows.entrySet()) {
                final long rowId = probeRow.getValue();
                record.init(pool.navigateTo(Rows.toPartitionIndex(rowId)));
                record.setRowIndex(Rows.toLocalRowID(rowId));
                final List<Long> values = expected.get(probeRow.getKey());
                for (int pass = 0; pass < 2; pass++) {
                    if (pass == 0) {
                        probe.find(record);
                    } else {
                        probe.findUnchecked(record);
                    }
                    if (values != null) {
                        // A chain runs from the key's last row to its first, as a serial build's does.
                        for (int j = values.size() - 1; j >= 0; j--) {
                            Assert.assertTrue(probeRow.getKey(), probe.hasNext());
                            final long handle = probe.next();
                            if (hasPayload) {
                                Assert.assertEquals(values.get(j).longValue(), probe.getRecord().getLong(0));
                                probe.recordAt(handle);
                                Assert.assertEquals(values.get(j).longValue(), probe.getRecord().getLong(0));
                            }
                        }
                    }
                    Assert.assertFalse(probeRow.getKey(), probe.hasNext());
                }
                if (isUnique) {
                    Assert.assertEquals(values != null, probe.findSingleUnchecked(record));
                    if (values != null && hasPayload) {
                        Assert.assertEquals(values.get(0).longValue(), probe.getRecord().getLong(0));
                    }
                }
            }
        }
    }

    // Every tenth key of t's l column, whose v is ten times l, hits; a key past the table misses.
    private static void assertFinds(FrozenHashJoinBuild.RecordProbe probe, HashJoinBuildFrames frames) {
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
            pool.of(frames.getAddressCache());
            record.init(pool.navigateTo(0));
            for (int row = 0; row < 1_000; row += 37) {
                record.setRowIndex(row);
                Assert.assertTrue(probe.findSingleUnchecked(record));
                Assert.assertEquals(record.getLong(0) * 10, probe.getRecord().getLong(0));
            }
        }
    }

    // Both builds hold the same chains, in the same order, for every key of the frames.
    private static void assertSameChains(
            FrozenHashJoinBuild.RecordKeyed expected,
            FrozenHashJoinBuild.RecordKeyed frozen,
            HashJoinBuildFrames frames,
            KeyLayout layout,
            RecordMetadata metadata
    ) {
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
             FrozenHashJoinBuild.RecordProbe serialProbe = expected.newProbe(layout.newSink(metadata));
             FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(layout.newSink(metadata))) {
            pool.of(frames.getAddressCache());
            record.of(frames.getSymbolTableSource());
            for (long rowId : keyRows(frames, layout).values()) {
                record.init(pool.navigateTo(Rows.toPartitionIndex(rowId)));
                record.setRowIndex(Rows.toLocalRowID(rowId));
                serialProbe.find(record);
                probe.findUnchecked(record);
                Assert.assertTrue(serialProbe.hasNext());
                while (serialProbe.hasNext()) {
                    Assert.assertTrue(probe.hasNext());
                    serialProbe.next();
                    probe.next();
                    Assert.assertEquals(serialProbe.getRecord().getLong(0), probe.getRecord().getLong(0));
                }
                Assert.assertFalse(probe.hasNext());
            }
        }
    }

    // Every heap ordinal belongs to exactly one frame's segment of one partition, and holds a row of that frame.
    private static void assertSegmentsCoverTheHeap(MapHashJoinBuild build, FrozenHashJoinBuild.RecordKeyed frozen,
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

    // The method of MapHashJoinBuild whose allocation failed.
    private static String buildSite(Throwable error) {
        for (StackTraceElement element : error.getStackTrace()) {
            if (element.getClassName().equals(MapHashJoinBuild.class.getName())) {
                return element.getMethodName();
            }
        }
        return "";
    }

    /**
     * Ten daily partitions of t, whose key columns repeat across frames and partitions: l over 101
     * values, zero and NULL; i over 13 values and NULL; s over 17 values, NULL and the empty string.
     */
    private static void createKeyTable(int rowCount, long timestampStep) throws Exception {
        execute("CREATE TABLE t (l LONG, i INT, s VARCHAR, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t SELECT"
                + " CASE WHEN x % 97 = 0 THEN NULL ELSE x * 7_919 % 101 - 50 END,"
                + " CASE WHEN x % 89 = 0 THEN NULL ELSE (x % 13)::INT END,"
                + " CASE WHEN x % 83 = 0 THEN NULL WHEN x % 79 = 0 THEN '' ELSE 'k' || (x % 17) END,"
                + " x, timestamp_sequence('2020-01-01', " + timestampStep + ")"
                + " FROM long_sequence(" + rowCount + ")");
    }

    // The payload value of every row the frames keep, per key, in the frames' order.
    private static Map<String, List<Long>> expectedChains(HashJoinBuildFrames frames, KeyLayout layout, LongPredicate keep) {
        final Map<String, List<Long>> expected = new HashMap<>();
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
            pool.of(frames.getAddressCache());
            for (int frame = 0, frameCount = frames.getFrameCount(); frame < frameCount; frame++) {
                record.init(pool.navigateTo(frame));
                for (long row = 0, n = frames.getFrameRowCount(frame); row < n; row++) {
                    if (keep == null || keep.test(Rows.toRowID(frame, row))) {
                        record.setRowIndex(row);
                        expected.computeIfAbsent(layout.keyOf(record), k -> new ArrayList<>()).add(record.getLong(PAYLOAD_COLUMN));
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

    // The key capacity of each partition's map of the last build, read before it closes.
    @SuppressWarnings("unchecked")
    private static LongList keyCapacities(MapHashJoinBuild build) throws Exception {
        final java.lang.reflect.Field field = MapHashJoinBuild.class.getDeclaredField("maps");
        field.setAccessible(true);
        final ObjList<io.questdb.cairo.map.Map> maps = (ObjList<io.questdb.cairo.map.Map>) field.get(build);
        final LongList capacities = new LongList();
        for (int p = 0, n = build.getPartitionCount(); p < n; p++) {
            capacities.add(maps.getQuick(p).getKeyCapacity());
        }
        return capacities;
    }

    // The row id of the first row of every key the frames hold.
    private static Map<String, Long> keyRows(HashJoinBuildFrames frames, KeyLayout layout) {
        final Map<String, Long> rows = new LinkedHashMap<>();
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
            pool.of(frames.getAddressCache());
            for (int frame = 0, frameCount = frames.getFrameCount(); frame < frameCount; frame++) {
                record.init(pool.navigateTo(frame));
                for (long row = 0, n = frames.getFrameRowCount(frame); row < n; row++) {
                    record.setRowIndex(row);
                    rows.putIfAbsent(layout.keyOf(record), Rows.toRowID(frame, row));
                }
            }
        }
        return rows;
    }

    // The three map layouts a staged key takes, over t's key columns.
    private enum KeyLayout {
        // l alone: an Unordered8Map.
        LONG(new ArrayColumnTypes().add(ColumnType.LONG), 0),
        // i and l: an OrderedMap of twelve-byte keys.
        COMPOSITE_FIXED_SIZE(new ArrayColumnTypes().add(ColumnType.INT).add(ColumnType.LONG), 1, 0),
        // i and s: an OrderedMap of var-size keys.
        COMPOSITE_VAR_SIZE(new ArrayColumnTypes().add(ColumnType.INT).add(ColumnType.VARCHAR), 1, 2);

        private final int[] columns;
        private final ArrayColumnTypes keyTypes;

        KeyLayout(ArrayColumnTypes keyTypes, int... columns) {
            this.keyTypes = keyTypes;
            this.columns = columns;
        }

        String keyOf(PageFrameMemoryRecord record) {
            final StringBuilder key = new StringBuilder();
            for (int column : columns) {
                switch (column) {
                    case 0 -> key.append(record.getLong(0));
                    case 1 -> key.append(record.getInt(1));
                    default -> {
                        final Utf8Sequence value = record.getVarcharA(2);
                        key.append(value == null ? "<null>" : "'" + value + "'");
                    }
                }
                key.append('|');
            }
            return key.toString();
        }

        MapHashJoinBuild newBuild(boolean hasPayload) {
            return new MapHashJoinBuild(configuration, keyTypes, hasPayload, 4, 1024, 16);
        }

        MapHashJoinBuild newReusableBuild(boolean hasPayload) {
            return new MapHashJoinBuild(configuration, keyTypes, hasPayload, 4, 1024, 16, true);
        }

        // A sink of the key columns, as the build and the probe each generate their own.
        RecordSink newSink(RecordMetadata metadata) {
            final ListColumnFilter filter = new ListColumnFilter();
            for (int column : columns) {
                filter.add(column + 1);
            }
            return RecordSinkFactory.getInstance(configuration, new BytecodeAssembler(), metadata, filter, null);
        }
    }

    /** Counts the checks that partition tasks make, and cancels on the given one, counting from one; -1 never cancels. */
    private static class PartitionCheckBreaker extends CountingSqlExecutionCircuitBreaker {
        private final int tripAt;
        private final StackWalker walker = StackWalker.getInstance();
        private int partitionChecks;

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
            if (site.equals("buildPartition") && ++partitionChecks == tripAt) {
                throw CairoException.queryCancelled(1);
            }
        }
    }
}
