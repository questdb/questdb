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

package io.questdb.test.cairo.lv;

import com.sun.management.ThreadMXBean;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateBuilder;
import io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateReader;
import io.questdb.cairo.lv.LiveViewCheckpointDataSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapEntry;
import io.questdb.cairo.lv.LiveViewCheckpointRingStateSource;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryWriter;
import io.questdb.cairo.lv.LiveViewCheckpointStateCodec;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.std.Decimals;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LiveViewCheckpointRangeRingStateTest extends AbstractCairoTest {

    private static final int ALLOCATION_ROUNDS = 4;
    private static final String DATA_SEGMENT_PATH_FRAGMENT =
            LiveViewCheckpointLayout.DATA_DIR_NAME + Files.SEPARATOR + LiveViewCheckpointLayout.DATA_SEGMENT_PREFIX;
    private static final byte[] KEY = new byte[]{1, 2, 3};
    private static final String LV_DIR = "lv_avg_range_chunks";
    private static final int NARROW_PARTITIONS = 1_024;
    // A warm walk over the partitions of a root allocates nothing per partition. This leaves
    // room for a few objects per walk; one object per partition exceeds it well before 1,024.
    private static final long PER_WALK_ALLOCATION_LIMIT_BYTES = 1_024;
    private static final int TWO_CHUNK_RING_ROWS = 6;
    private static final int WARM_PARTITIONS = 8_192;

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            checkpointsDir(path).concat(LiveViewCheckpointLayout.DATA_DIR_NAME).slash();
            configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode());
            checkpointsDir(path).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
            configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testSealedChunksAreSharedAndOnlyTheAppendedTailIsWritten() throws Exception {
        assertMemoryLeak(() -> {
            final long[] secondSegmentBytes = new long[1];
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry()) {
                writeInitial(first, directory, 1, 4_106);

                try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                     LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                     Path dir = new Path()) {
                    builder.of(first, LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
                    builder.dropHeadRows(5);
                    writer.of(checkpointsDir(dir), 2);
                    builder.append(writer, 4_106_000, Double.doubleToRawLongBits(10_000.0));
                    builder.append(writer, 4_107_000, Double.doubleToRawLongBits(-0.0));
                    builder.append(writer, 4_108_000, Double.doubleToRawLongBits(10_002.0));
                    LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, Double.doubleToRawLongBits(-0.0), 0, 0, 0, 4_104, second);
                    secondSegmentBytes[0] = writer.commit();
                    directory.addSegment(2, secondSegmentBytes[0]);
                }

                // Both chunks the first root sealed are referenced verbatim; the
                // three appended rows become a chunk of their own. The head's five
                // expired rows stay inside the shared chunk 0 - the offset moves,
                // the page does not.
                Assert.assertEquals(4, first.getStatePageCount());
                Assert.assertEquals(6, second.getStatePageCount());
                for (int i = 0; i < 4; i++) {
                    assertRefEquals(first.getStatePageRef(i), second.getStatePageRef(i));
                }
                Assert.assertEquals(1, first.getStatePageRef(2).getSegmentId());
                Assert.assertEquals(2, second.getStatePageRef(4).getSegmentId());
                Assert.assertEquals(3, second.getStatePageRef(4).getRowCount());
                Assert.assertEquals(3, second.getStatePageRef(5).getRowCount());
                // The second root paid for its three rows and nothing else, over a
                // frame of 4104.
                Assert.assertTrue(
                        "second root wrote " + secondSegmentBytes[0] + " bytes for three appended rows",
                        secondSegmentBytes[0] < 128
                );

                final LongList firstTimestamps = new LongList();
                final LongList firstValues = new LongList();
                for (int i = 0; i < 4_106; i++) {
                    firstTimestamps.add(i * 1_000L);
                    firstValues.add(Double.doubleToRawLongBits(i + 0.25));
                }
                assertRestored(first, directory, firstTimestamps, firstValues);

                final LongList secondTimestamps = new LongList();
                final LongList secondValues = new LongList();
                for (int i = 5; i < 4_106; i++) {
                    secondTimestamps.add(i * 1_000L);
                    secondValues.add(Double.doubleToRawLongBits(i + 0.25));
                }
                secondTimestamps.add(4_106_000);
                secondTimestamps.add(4_107_000);
                secondTimestamps.add(4_108_000);
                secondValues.add(Double.doubleToRawLongBits(10_000.0));
                secondValues.add(Double.doubleToRawLongBits(-0.0));
                secondValues.add(Double.doubleToRawLongBits(10_002.0));
                assertRestored(second, directory, secondTimestamps, secondValues);
                try (LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
                     Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), directory.reader, second);
                    Assert.assertEquals(5, reader.getHeadOffset());
                    Assert.assertEquals(4_104, reader.getRowCount());
                    Assert.assertEquals(Double.doubleToRawLongBits(-0.0), reader.getScalarBits());
                }
            }
        });
    }

    @Test
    public void testADroppedHeadChunkNeverSharesAReferenceObjectWithALiveOne() throws Exception {
        // The builder pools its chunk reference objects across partitions. Expiring a whole
        // head chunk shifts the live references down, and the objects the expired chunk held
        // must move to the slots the shift vacates: a vacated slot left naming an object still
        // live below it would let the next sealed tail, or the next partition's carried-forward
        // references, overwrite a live reference through the other slot. A value ring spends
        // two references per chunk and a valueless one spends one, so both are covered.
        assertMemoryLeak(() -> {
            assertDroppedHeadChunkKeepsReferencesApart(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 90);
            assertDroppedHeadChunkKeepsReferencesApart(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_NONE, 91);
        });
    }

    @Test
    public void testAReaderReusedForANarrowerEntryReadsOnlyThatEntrysChunks() throws Exception {
        // The reader keeps the reference objects of the widest entry it has opened. An entry
        // with fewer chunks opened after it must expose its own references alone: the walk,
        // the page count and the page accessor all stop at that entry's count rather than at
        // what the reader keeps.
        assertMemoryLeak(() -> {
            final int valueKind = LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE;
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointPartitionMapEntry narrow = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointPartitionMapEntry middle = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointPartitionMapEntry wide = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
                 Path dir = new Path()) {
                try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                     LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration)) {
                    writer.of(checkpointsDir(dir), 96);
                    builder.ofEmpty(valueKind, 1);
                    appendRingRows(builder, writer, valueKind, 0, 3);
                    LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0, 0, 0, 0, 3, narrow);
                    builder.of(narrow, valueKind, 1);
                    appendRingRows(builder, writer, valueKind, 3, 6);
                    LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0, 0, 0, 0, 6, middle);
                    builder.of(middle, valueKind, 1);
                    appendRingRows(builder, writer, valueKind, 6, 9);
                    LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0, 0, 0, 0, 9, wide);
                    directory.addSegment(96, writer.commit());
                }
                final LongList timestamps = new LongList();
                final LongList values = new LongList();
                for (int row = 0; row < 9; row++) {
                    timestamps.add(row * 1_000L);
                    values.add(Double.doubleToRawLongBits(row + 0.25));
                }
                assertWalked(reader, dir, directory, wide, timestamps, values);
                Assert.assertEquals(6, reader.getStatePageCount());

                timestamps.setPos(3);
                values.setPos(3);
                assertWalked(reader, dir, directory, narrow, timestamps, values);
                Assert.assertEquals(2, reader.getStatePageCount());
                final LiveViewCheckpointStatePageRef ref = new LiveViewCheckpointStatePageRef();
                for (int i = 0; i < 2; i++) {
                    reader.getStatePageRef(i, ref);
                    assertRefEquals(narrow.getStatePageRef(i), ref);
                }
                try {
                    reader.getStatePageRef(2, ref);
                    Assert.fail("a reference past the entry's count must not be readable");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "RANGE ring state page reference out of bounds");
                }
            }
        });
    }

    @Test
    public void testAWarmRingRestoreAndValidateAllocateNoHeapPerPartition() throws Exception {
        // A restore opens every ring partition of a root through one reader and walks its
        // rows; a validation only opens its metadata. The reader copies each entry's chunk
        // references into reference objects it keeps, so once warm neither walk allocates
        // heap per partition: 8,192 partitions cost what 1,024 do.
        assertMemoryLeak(() -> {
            final ObjList<LiveViewCheckpointPartitionMapEntry> entries = new ObjList<>();
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
                 TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope();
                 Path dir = new Path()) {
                writeTwoChunkRings(entries, directory, 100, WARM_PARTITIONS);
                checkpointsDir(dir);
                final long[] checksum = new long[1];
                final LiveViewCheckpointRingStateSource.RowConsumer consumer =
                        (timestamp, valueBits) -> checksum[0] += timestamp ^ valueBits;
                // Warm-up: the reader grows its reference objects and maps the segment once.
                restoreEach(reader, dir, directory, entries, WARM_PARTITIONS, consumer);

                final long validateNarrow = minValidateAllocation(scope, reader, entries, NARROW_PARTITIONS);
                final long validateWide = minValidateAllocation(scope, reader, entries, WARM_PARTITIONS);
                final long restoreNarrow = minRestoreAllocation(scope, reader, dir, directory, entries, NARROW_PARTITIONS, consumer);
                final long restoreWide = minRestoreAllocation(scope, reader, dir, directory, entries, WARM_PARTITIONS, consumer);
                assertPerPartitionAllocation("ring validation", validateNarrow, validateWide);
                assertPerPartitionAllocation("ring restore", restoreNarrow, restoreWide);

                checksum[0] = 0;
                Assert.assertEquals(
                        (long) TWO_CHUNK_RING_ROWS * NARROW_PARTITIONS,
                        restoreEach(reader, dir, directory, entries, NARROW_PARTITIONS, consumer)
                );
                long expectedChecksum = 0;
                for (int i = 0; i < NARROW_PARTITIONS; i++) {
                    for (int row = 0; row < TWO_CHUNK_RING_ROWS; row++) {
                        expectedChecksum += (row * 1_000L) ^ Double.doubleToRawLongBits(i + row * 0.5);
                    }
                }
                Assert.assertEquals(expectedChecksum, checksum[0]);
            } finally {
                Misc.freeObjList(entries);
            }
        });
    }

    @Test
    public void testAWarmRingSealAllocatesNoHeapPerPartition() throws Exception {
        // A cadence seal runs the builder once per ring partition: carry the previous entry's
        // chunks forward, expire the head - here past a chunk boundary, so a whole chunk goes -
        // append the batch's rows and freeze. The builder reuses its reference objects and
        // encodes the scalar into native scratch it keeps, and the entry it freezes into
        // reuses its own, so a warm seal allocates no heap per partition: 8,192 partitions
        // cost what 1,024 do.
        assertMemoryLeak(() -> {
            final ObjList<LiveViewCheckpointPartitionMapEntry> entries = new ObjList<>();
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                 LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                 LiveViewCheckpointPartitionMapEntry out = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointTestKeys key = new LiveViewCheckpointTestKeys();
                 TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope();
                 Path dir = new Path()) {
                writeTwoChunkRings(entries, directory, 110, WARM_PARTITIONS);
                writer.of(checkpointsDir(dir), 111);
                final byte[] keyBytes = new byte[Integer.BYTES];
                // Warm-up: the builder and the entry grow their reference objects and scratch.
                sealEach(builder, writer, key, keyBytes, entries, WARM_PARTITIONS, out);

                final long narrow = minSealAllocation(scope, builder, writer, key, keyBytes, entries, NARROW_PARTITIONS, out);
                final long wide = minSealAllocation(scope, builder, writer, key, keyBytes, entries, WARM_PARTITIONS, out);
                assertPerPartitionAllocation("ring seal", narrow, wide);

                // The last partition's freeze: the surviving chunk carried forward, then the
                // one-row tail this seal wrote.
                final LiveViewCheckpointPartitionMapEntry last = entries.getQuick(WARM_PARTITIONS - 1);
                Assert.assertEquals(4, out.getStatePageCount());
                assertRefEquals(last.getStatePageRef(2), out.getStatePageRef(0));
                assertRefEquals(last.getStatePageRef(3), out.getStatePageRef(1));
                Assert.assertEquals(111, out.getStatePageRef(2).getSegmentId());
                Assert.assertEquals(1, out.getStatePageRef(2).getRowCount());
                Assert.assertEquals(1, out.getStatePageRef(3).getRowCount());
                writer.discard();
            } finally {
                Misc.freeObjList(entries);
            }
        });
    }

    @Test
    public void testChunksOfOnePartitionCarryDifferentCodecs() throws Exception {
        // Codec selection is per page, so the chunks of one partition may each land
        // under a different format-1 tag. A boundary that appended a dense cadence of
        // decimal prices compresses both of its pages; a boundary that appended three
        // far-apart rows carrying a NaN payload stores both raw, because a covering
        // header costs more than the three words it would describe. Prove all three
        // tags coexist in one root, that no page exceeds the payload it decodes to,
        // and that the mixed ring still walks back bit-exactly across a head offset
        // sitting inside a compressed chunk.
        assertMemoryLeak(() -> {
            final int denseRows = LiveViewCheckpointStateCodec.CHUNK_ROWS;
            final int sparseRows = 3;
            final int dropRows = denseRows - 6;
            final long sparseStep = 1L << 40;
            final long nanBits = 0x7ff8_dead_beef_1234L;
            final LongList timestamps = new LongList();
            final LongList values = new LongList();
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointPartitionMapEntry third = new LiveViewCheckpointPartitionMapEntry()) {
                try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                     LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                     Path dir = new Path()) {
                    builder.ofEmpty(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
                    writer.of(checkpointsDir(dir), 50);
                    for (int i = 0; i < denseRows; i++) {
                        final long ts = i * 1_000L;
                        final long bits = Double.doubleToRawLongBits(100.0 + i * 0.01);
                        builder.append(writer, ts, bits);
                        timestamps.add(ts);
                        values.add(bits);
                    }
                    LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0L, 0, 0, 0, timestamps.size(), first);
                    directory.addSegment(50, writer.commit());
                }

                long lastTimestamp = timestamps.getLast();
                try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                     LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                     Path dir = new Path()) {
                    builder.of(first, LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
                    writer.of(checkpointsDir(dir), 51);
                    for (int i = 0; i < sparseRows; i++) {
                        final long ts = lastTimestamp + (i + 1) * sparseStep;
                        builder.append(writer, ts, nanBits);
                        timestamps.add(ts);
                        values.add(nanBits);
                    }
                    LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0L, 0, 0, 0, timestamps.size(), second);
                    directory.addSegment(51, writer.commit());
                }

                lastTimestamp = timestamps.getLast();
                try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                     LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                     Path dir = new Path()) {
                    builder.of(second, LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
                    // The drop stops inside the first chunk, so the ring keeps a head
                    // offset into a covering page the seal never rewrites.
                    builder.dropHeadRows(dropRows);
                    for (int i = 0; i < dropRows; i++) {
                        timestamps.removeIndex(0);
                        values.removeIndex(0);
                    }
                    writer.of(checkpointsDir(dir), 52);
                    for (int i = 0; i < denseRows; i++) {
                        final long ts = lastTimestamp + (i + 1) * 1_000L;
                        final long bits = Double.doubleToRawLongBits(42.5);
                        builder.append(writer, ts, bits);
                        timestamps.add(ts);
                        values.add(bits);
                    }
                    LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0L, 0, 0, 0, timestamps.size(), third);
                    directory.addSegment(52, writer.commit());
                }

                // A regular cadence is a linear-prediction long block, a decimal price
                // series and a repeated value are ALP blocks, and both of the sparse
                // NaN chunk's pages are raw.
                assertPageCodecs(
                        third,
                        LiveViewCheckpointStateCodec.COVERING_LONG, LiveViewCheckpointStateCodec.COVERING_DOUBLE,
                        LiveViewCheckpointStateCodec.RAW_64, LiveViewCheckpointStateCodec.RAW_64,
                        LiveViewCheckpointStateCodec.COVERING_LONG, LiveViewCheckpointStateCodec.COVERING_DOUBLE
                );
                assertRestored(third, directory, timestamps, values);
                try (LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
                     Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), directory.reader, third);
                    Assert.assertEquals(dropRows, reader.getHeadOffset());
                    Assert.assertEquals(denseRows + sparseRows + denseRows - dropRows, reader.getRowCount());
                }
            }
        });
    }

    @Test
    public void testChunksSpanningManySegmentsMapEachSegmentOncePerBinding() throws Exception {
        // A cadence seal appends each boundary's rows as a fresh chunk in that
        // boundary's own data segment and carries the older chunks forward by
        // reference, so one partition's ring spans as many segments as boundaries
        // it survived - and a restore walks that same span again for every
        // partition it rehydrates. Prove the reader maps each segment once and
        // reuses the mapping for every later binding, until detach drops it.
        final int boundaries = 40;
        final int rowsPerBoundary = 3;
        final int[] dataSegmentOpens = {0};
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.containsAscii(name, DATA_SEGMENT_PATH_FRAGMENT)) {
                    dataSegmentOpens[0]++;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            final LongList timestamps = new LongList();
            final LongList values = new LongList();
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointPartitionMapEntry even = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointPartitionMapEntry odd = new LiveViewCheckpointPartitionMapEntry()) {
                // Each boundary freezes into the entry the one before it did not use, so
                // the builder still reads the previous root while the next one is sealed.
                LiveViewCheckpointPartitionMapEntry root = null;
                for (int boundary = 0; boundary < boundaries; boundary++) {
                    final long segmentId = boundary + 1;
                    final LiveViewCheckpointPartitionMapEntry sealed = root == even ? odd : even;
                    try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                         LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                         Path dir = new Path()) {
                        if (root == null) {
                            builder.ofEmpty(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
                        } else {
                            builder.of(root, LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
                        }
                        writer.of(checkpointsDir(dir), segmentId);
                        for (int i = 0; i < rowsPerBoundary; i++) {
                            final long ts = (boundary * rowsPerBoundary + i) * 1_000L;
                            final long bits = Double.doubleToRawLongBits(ts / 1_000.0);
                            timestamps.add(ts);
                            values.add(bits);
                            builder.append(writer, ts, bits);
                        }
                        LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0L, 0, 0, 0, timestamps.size(), sealed);
                        directory.addSegment(segmentId, writer.commit());
                    }
                    root = sealed;
                }

                // One chunk - a timestamp page and a value page - per boundary, each
                // in the segment that boundary sealed.
                Assert.assertEquals(2 * boundaries, root.getStatePageCount());
                for (int i = 0; i < root.getStatePageCount(); i++) {
                    Assert.assertEquals(i / 2 + 1, root.getStatePageRef(i).getSegmentId());
                }

                try (LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
                     Path dir = new Path()) {
                    dataSegmentOpens[0] = 0;
                    assertWalked(reader, dir, directory, root, timestamps, values);
                    Assert.assertEquals(boundaries, dataSegmentOpens[0]);

                    // A second binding of the same partition - the shape a restore
                    // takes for every partition after the first - reads the whole
                    // span off the mappings the first one made.
                    assertWalked(reader, dir, directory, root, timestamps, values);
                    Assert.assertEquals(boundaries, dataSegmentOpens[0]);

                    // Detach drops them, so nothing survives into a timeline that may
                    // be retired, repaired or compacted in between.
                    reader.detach();
                    assertWalked(reader, dir, directory, root, timestamps, values);
                    Assert.assertEquals(2 * boundaries, dataSegmentOpens[0]);
                }
            }
        });
    }

    @Test
    public void testClosingTheBuilderFreesItsScalarScratchAfterAFailedFreeze() throws Exception {
        // freeze encodes the partition's scalar into native scratch the builder keeps from
        // one partition to the next. Closing the builder hands it back, also when the last
        // freeze failed before it reached the scalar.
        assertMemoryLeak(() -> {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
            try (LiveViewCheckpointPartitionMapEntry out = new LiveViewCheckpointPartitionMapEntry()) {
                try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                     LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                     Path dir = new Path()) {
                    writer.of(checkpointsDir(dir), 95);
                    builder.ofEmpty(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
                    builder.append(writer, 1_000, Double.doubleToRawLongBits(1.5));
                    LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, Double.doubleToRawLongBits(1.5), 0, 0, 0, 1, out);
                    Assert.assertEquals(
                            LiveViewCheckpointRangeRingStateReader.scalarStateBytes(1),
                            out.getScalarLength()
                    );

                    builder.ofEmpty(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
                    builder.append(writer, 2_000, Double.doubleToRawLongBits(2.5));
                    try {
                        LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0, 0, 0, 0, -1, out);
                        Assert.fail("a negative frame size must not freeze");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "RANGE ring frame size out of bounds");
                    }
                    writer.discard();
                }
                Assert.assertEquals(
                        "a closed builder must hold no native memory",
                        baseline + out.getRetainedBufferBytesForTest(),
                        Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM)
                );
            }
        });
    }

    @Test
    public void testDequePageKindsShareChunksAndRoundTripSortedOracle() throws Exception {
        // The max/min monotonic-deque family stores the same (ts, value) frame ring
        // as the value functions but tags its value pages with the deque page kinds,
        // so a deque root's pages stay distinct from a value-ring root's. Feed each
        // kind a strictly decreasing value run at increasing timestamps - a max
        // deque snapshot, the sorted oracle - and prove the tail chunk shares, the
        // value pages self-identify as the deque kind, and every row round-trips.
        assertMemoryLeak(() -> {
            assertDequeRingSharesAndRoundTrips(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_LONG,
                    LiveViewCheckpointRangeRingStateReader.DEQUE_LONG_VALUE_PAGE_KIND,
                    true
            );
            assertDequeRingSharesAndRoundTrips(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_DOUBLE,
                    LiveViewCheckpointRangeRingStateReader.DEQUE_DOUBLE_VALUE_PAGE_KIND,
                    false
            );
        });
    }

    @Test
    public void testDequeRingsSelectCodecPerValueStream() throws Exception {
        // A max deque's snapshot is a strictly decreasing value run, and the codec its
        // value page lands under follows those words rather than the deque page kind: a
        // narrow long run and a whole-number double run both compress, while a run
        // spanning the whole 64-bit range and a run of denormals both fall back to raw
        // because the covering block would be longer than the payload it describes.
        assertMemoryLeak(() -> {
            final int rows = LiveViewCheckpointStateCodec.CHUNK_ROWS + 5;
            // A stride wide enough that one full chunk spans more than 2^63 and packs
            // no narrower than 64 bits, while the whole run still fits the range.
            final long stride = 3_000_000_000_000_000L;
            final LongList timestamps = new LongList();
            final LongList narrowLongs = new LongList();
            final LongList wideLongs = new LongList();
            final LongList wholeDoubles = new LongList();
            final LongList denormalDoubles = new LongList();
            for (int i = 0; i < rows; i++) {
                timestamps.add(i * 1_000L);
                narrowLongs.add(rows - i);
                wideLongs.add(Long.MAX_VALUE - i * stride);
                wholeDoubles.add(Double.doubleToRawLongBits(rows - i));
                // No decimal transform in the ALP tables reproduces a denormal: every
                // candidate exponent rounds it to zero, so every value is an exception.
                denormalDoubles.add(Double.doubleToRawLongBits((rows - i) * Double.MIN_VALUE));
            }
            assertValueCodecSelection(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_LONG,
                    LiveViewCheckpointRangeRingStateReader.DEQUE_LONG_VALUE_PAGE_KIND,
                    84, timestamps, narrowLongs, LiveViewCheckpointStateCodec.COVERING_LONG
            );
            assertValueCodecSelection(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_LONG,
                    LiveViewCheckpointRangeRingStateReader.DEQUE_LONG_VALUE_PAGE_KIND,
                    85, timestamps, wideLongs, LiveViewCheckpointStateCodec.RAW_64
            );
            assertValueCodecSelection(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_DOUBLE,
                    LiveViewCheckpointRangeRingStateReader.DEQUE_DOUBLE_VALUE_PAGE_KIND,
                    86, timestamps, wholeDoubles, LiveViewCheckpointStateCodec.COVERING_DOUBLE
            );
            assertValueCodecSelection(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_DOUBLE,
                    LiveViewCheckpointRangeRingStateReader.DEQUE_DOUBLE_VALUE_PAGE_KIND,
                    87, timestamps, denormalDoubles, LiveViewCheckpointStateCodec.RAW_64
            );
        });
    }

    @Test
    public void testLongValueRingRoundTripsAnAllMaxValueStride() throws Exception {
        assertMemoryLeak(() -> {
            // Every row of the frame holds the same LONG payload, and that payload
            // is the largest a LONG column can hold. The stride's minimum then
            // equals the value the plain-FoR encoder seeds its scan with, which is
            // the one input that used to make the block store a base of 0 and
            // decode the whole page back as 0. The page still has to select the
            // covering block - a 13-byte header against 8 bytes per raw row - and
            // every row still has to read back as Long.MAX_VALUE.
            final long[] payload = new long[64];
            Arrays.fill(payload, Long.MAX_VALUE);
            assertLongValueRingRoundTrips(payload, 11, LiveViewCheckpointStateCodec.COVERING_LONG);
        });
    }

    @Test
    public void testLongValueRingRoundTripsRawBits() throws Exception {
        assertMemoryLeak(() -> {
            // A raw 64-bit value column must round-trip any bit pattern verbatim,
            // including LONG_NULL and a bit pattern that would be a NaN if read as a
            // double - proof the long ring never routes a value through a double.
            // This payload spans the whole 64-bit range, so FoR cannot narrow it and
            // the selection falls back to raw.
            final long[] payload = {
                    Long.MIN_VALUE, // LONG_NULL
                    0L,
                    -1L,
                    Long.MAX_VALUE,
                    0x7ff0_0000_0000_0001L, // a signaling-NaN bit pattern as a raw long
                    42L,
            };
            assertLongValueRingRoundTrips(payload, 7, LiveViewCheckpointStateCodec.RAW_64);
        });
    }

    @Test
    public void testMalformedMetadataAndDataAreRejected() throws Exception {
        assertMemoryLeak(() -> {
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointPartitionMapEntry valid = new LiveViewCheckpointPartitionMapEntry()) {
                writeInitial(valid, directory, 30, 3);

                final byte[] shortScalar = Arrays.copyOf(
                        valid.copyScalarStateForTest(),
                        LiveViewCheckpointRangeRingStateReader.scalarStateBytes(1) - 1
                );
                assertInvalid(shortScalar, refs(valid), directory, false, "scalar state size mismatch");

                final byte[] badVersion = Arrays.copyOf(valid.copyScalarStateForTest(), valid.getScalarLength());
                badVersion[0] = 3;
                assertInvalid(badVersion, refs(valid), directory, false, "format version mismatch");

                final byte[] badHead = Arrays.copyOf(valid.copyScalarStateForTest(), valid.getScalarLength());
                badHead[4] = 127;
                assertInvalid(badHead, refs(valid), directory, false, "logical chunk bounds invalid");

                final LiveViewCheckpointStatePageRef[] oddRefs = Arrays.copyOf(refs(valid), 1);
                assertInvalid(valid.copyScalarStateForTest(), oddRefs, directory, false, "reference count invalid");

                final LiveViewCheckpointStatePageRef[] badKind = refs(valid);
                final LiveViewCheckpointStatePageRef timestampRef = badKind[0];
                badKind[0] = copy(timestampRef).of(
                        timestampRef.getSegmentId(), timestampRef.getOffset(), timestampRef.getStoredLength(),
                        timestampRef.getDecodedLength(), LiveViewCheckpointRangeRingStateReader.DOUBLE_VALUE_PAGE_KIND,
                        timestampRef.getCodec(), timestampRef.getRowCount(), timestampRef.getFlags()
                );
                assertInvalid(valid.copyScalarStateForTest(), badKind, directory, false, "timestamp page kind or codec invalid");

                final LiveViewCheckpointStatePageRef[] mismatched = refs(valid);
                final LiveViewCheckpointStatePageRef valueRef = mismatched[1];
                mismatched[1] = copy(valueRef).of(
                        valueRef.getSegmentId(), valueRef.getOffset(), valueRef.getStoredLength(),
                        2 * Long.BYTES, valueRef.getPageKind(), valueRef.getCodec(), 2, valueRef.getFlags()
                );
                assertInvalid(valid.copyScalarStateForTest(), mismatched, directory, false, "row counts differ");

                // A regular three-row cadence fits the plain FoR block, and the
                // block's own embedded count is the first thing the checked decoder
                // cross-checks against the page reference.
                Assert.assertEquals(LiveViewCheckpointStateCodec.COVERING_LONG, valid.getStatePageRef(0).getCodec());
                corruptDataPage(directory, 30, valid.getStatePageRef(0).getOffset(), 0, -1);
                assertInvalid(valid, directory, true, "covering long block rejected");

                // A page whose framing stays well formed is caught by the walk
                // instead: this ring's timestamps are far enough apart that the page
                // stores raw, so flipping one word makes the sequence decrease
                // without disturbing anything the decoder validates.
                try (LiveViewCheckpointPartitionMapEntry sparse = new LiveViewCheckpointPartitionMapEntry()) {
                    writeInitial(sparse, directory, 31, 3, 1L << 40);
                    Assert.assertEquals(LiveViewCheckpointStateCodec.RAW_64, sparse.getStatePageRef(0).getCodec());
                    corruptDataPage(directory, 31, sparse.getStatePageRef(0).getOffset(), Long.BYTES, -1);
                    assertInvalid(sparse, directory, true, "rows are not canonical");
                }
            }
        });
    }

    @Test
    public void testRandomizedCircularBufferCheckpointsAgainstOracle() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd();
            final LongList timestamps = new LongList();
            final LongList values = new LongList();
            final List<LiveViewCheckpointPartitionMapEntry> roots = new ArrayList<>();
            final List<LongList> timestampSnapshots = new ArrayList<>();
            final List<LongList> valueSnapshots = new ArrayList<>();
            long nextTimestamp = 0;
            try (Catalogue directory = new Catalogue()) {
                LiveViewCheckpointPartitionMapEntry previous = null;
                for (int generation = 0; generation < 40; generation++) {
                    final LiveViewCheckpointPartitionMapEntry next = new LiveViewCheckpointPartitionMapEntry();
                    roots.add(next);
                    final int drop = timestamps.size() == 0 ? 0 : rnd.nextInt(timestamps.size() / 3 + 1);
                    for (int i = 0; i < drop; i++) {
                        timestamps.removeIndex(0);
                        values.removeIndex(0);
                    }
                    final int append = 1 + rnd.nextInt(300);
                    try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                         LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                         Path dir = new Path()) {
                        if (generation == 0) {
                            builder.ofEmpty(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
                        } else {
                            builder.of(previous, LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
                            builder.dropHeadRows(drop);
                        }
                        writer.of(checkpointsDir(dir), 100 + generation);
                        for (int i = 0; i < append; i++) {
                            nextTimestamp += rnd.nextInt(4);
                            final double value = rnd.nextDouble() * 10_000.0 - 5_000.0;
                            builder.append(writer, nextTimestamp, Double.doubleToRawLongBits(value));
                            timestamps.add(nextTimestamp);
                            values.add(Double.doubleToRawLongBits(value));
                        }
                        LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, Double.doubleToRawLongBits(generation + 0.125), 0, 0, 0, timestamps.size(), next);
                        directory.addSegment(100 + generation, writer.commit());
                    }
                    timestampSnapshots.add(new LongList(timestamps));
                    valueSnapshots.add(new LongList(values));
                    assertRestored(next, directory, timestamps, values);
                    final int old = rnd.nextInt(roots.size());
                    assertRestored(roots.get(old), directory, timestampSnapshots.get(old), valueSnapshots.get(old));
                    previous = next;
                }
            } finally {
                for (int i = 0, n = roots.size(); i < n; i++) {
                    roots.get(i).close();
                }
            }
        });
    }

    @Test
    public void testTimestampOnlyRingSharesChunksAndRoundTripsSortedOracle() throws Exception {
        // count's per-row state is the designated timestamp itself, so its ring stores
        // no value and a chunk is the timestamp page alone. Prove the single-page chunk
        // shares its sealed prefix by reference like a two-page one, that every page is
        // a timestamp page, that the whole run round-trips in order, and that the
        // valued overloads refuse a valueless ring rather than read a value page that
        // is not there.
        assertMemoryLeak(() -> {
            final int initialRows = 4_106;
            final int dropRows = 5;
            final int appendRows = 3;
            final LongList firstTimestamps = new LongList();
            final long[] secondSegmentBytes = new long[1];
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry()) {
                try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                     LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                     Path dir = new Path()) {
                    builder.ofEmpty(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_NONE, 1);
                    writer.of(checkpointsDir(dir), 40);
                    for (int i = 0; i < initialRows; i++) {
                        // Repeated timestamps too: several base rows may share one
                        // designated timestamp, and count buffers one ring row each.
                        final long ts = (i / 2) * 1_000L;
                        builder.append(writer, ts);
                        firstTimestamps.add(ts);
                    }
                    LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0L, 0, 0, 0, initialRows, first);
                    directory.addSegment(40, writer.commit());
                }

                final LongList secondTimestamps = new LongList();
                try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                     LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                     Path dir = new Path()) {
                    builder.of(first, LiveViewCheckpointRangeRingStateReader.VALUE_KIND_NONE, 1);
                    builder.dropHeadRows(dropRows);
                    writer.of(checkpointsDir(dir), 41);
                    for (int i = dropRows; i < initialRows; i++) {
                        secondTimestamps.add(firstTimestamps.getQuick(i));
                    }
                    for (int i = 0; i < appendRows; i++) {
                        final long ts = (initialRows + i) * 1_000L;
                        builder.append(writer, ts);
                        secondTimestamps.add(ts);
                    }
                    LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0L, 0, 0, 0, secondTimestamps.size(), second);
                    secondSegmentBytes[0] = writer.commit();
                    directory.addSegment(41, secondSegmentBytes[0]);
                }

                // One page per chunk: 4106 rows fill a whole chunk plus a partial one,
                // so the first root holds two pages and the second adds one for its
                // three appended rows, reusing both of the first root's verbatim.
                Assert.assertEquals(2, first.getStatePageCount());
                Assert.assertEquals(3, second.getStatePageCount());
                for (int i = 0; i < first.getStatePageCount(); i++) {
                    assertRefEquals(first.getStatePageRef(i), second.getStatePageRef(i));
                    Assert.assertEquals(
                            LiveViewCheckpointRangeRingStateReader.TIMESTAMP_PAGE_KIND,
                            first.getStatePageRef(i).getPageKind()
                    );
                }
                Assert.assertEquals(41, second.getStatePageRef(2).getSegmentId());
                Assert.assertEquals(appendRows, second.getStatePageRef(2).getRowCount());
                Assert.assertTrue(
                        "second root wrote " + secondSegmentBytes[0] + " bytes for three appended rows",
                        secondSegmentBytes[0] < 128
                );

                assertTimestampsRestored(first, directory, firstTimestamps);
                assertTimestampsRestored(second, directory, secondTimestamps);
                try (LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
                     Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), directory.reader, second);
                    Assert.assertEquals(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_NONE, reader.getValueKind());
                    Assert.assertEquals(dropRows, reader.getHeadOffset());
                    try {
                        reader.forEachRow((timestamp, valueBits) -> {
                        });
                        Assert.fail("expected a valueless ring to refuse the one-word overload");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "value width mismatch");
                    }
                }
            }
        });
    }

    @Test
    public void testWideDecimalRingsSelectCodecPerWordStream() throws Exception {
        // A wide decimal's value page is one flattened word stream, so its codec follows
        // those words: a run whose high words repeat and whose low words stay in a
        // narrow range compresses under flattened FoR, while a run whose words span the
        // whole 64-bit range packs no narrower than raw and falls back to it. The deque
        // kinds carry the same payload, so they must select the same way the value-ring
        // kinds do.
        assertMemoryLeak(() -> {
            assertWideCodecSelection(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL128,
                    LiveViewCheckpointRangeRingStateReader.DECIMAL128_VALUE_PAGE_KIND,
                    90, true, LiveViewCheckpointStateCodec.COVERING_LONG
            );
            assertWideCodecSelection(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL128,
                    LiveViewCheckpointRangeRingStateReader.DECIMAL128_VALUE_PAGE_KIND,
                    91, false, LiveViewCheckpointStateCodec.RAW_64
            );
            assertWideCodecSelection(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL256,
                    LiveViewCheckpointRangeRingStateReader.DECIMAL256_VALUE_PAGE_KIND,
                    92, true, LiveViewCheckpointStateCodec.COVERING_LONG
            );
            assertWideCodecSelection(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL256,
                    LiveViewCheckpointRangeRingStateReader.DECIMAL256_VALUE_PAGE_KIND,
                    93, false, LiveViewCheckpointStateCodec.RAW_64
            );
            assertWideCodecSelection(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_DECIMAL128,
                    LiveViewCheckpointRangeRingStateReader.DEQUE_DECIMAL128_VALUE_PAGE_KIND,
                    94, true, LiveViewCheckpointStateCodec.COVERING_LONG
            );
            assertWideCodecSelection(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_DECIMAL128,
                    LiveViewCheckpointRangeRingStateReader.DEQUE_DECIMAL128_VALUE_PAGE_KIND,
                    95, false, LiveViewCheckpointStateCodec.RAW_64
            );
            assertWideCodecSelection(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_DECIMAL256,
                    LiveViewCheckpointRangeRingStateReader.DEQUE_DECIMAL256_VALUE_PAGE_KIND,
                    96, true, LiveViewCheckpointStateCodec.COVERING_LONG
            );
            assertWideCodecSelection(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_DECIMAL256,
                    LiveViewCheckpointRangeRingStateReader.DEQUE_DECIMAL256_VALUE_PAGE_KIND,
                    97, false, LiveViewCheckpointStateCodec.RAW_64
            );
        });
    }

    @Test
    public void testWideDecimalRingsShareChunksAndRoundTripRawWords() throws Exception {
        // A DECIMAL128/DECIMAL256 ring spends two or four raw 64-bit words per row.
        // The chunk row cap shrinks by the same factor so one chunk's value page still
        // fits a single codec scratch buffer, and the page kinds keep a wide root's
        // pages distinct from a narrow root's and a value ring's from a deque's. Drive
        // each of the four wide kinds against a strictly decreasing oracle - so the
        // deque kinds carry a legitimate monotonic snapshot - and prove the tail chunk
        // shares, every word round-trips verbatim including the NULL sentinels, and the
        // wide scalar comes back exactly.
        assertMemoryLeak(() -> {
            assertWideRingSharesAndRoundTrips(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL128,
                    LiveViewCheckpointRangeRingStateReader.DECIMAL128_VALUE_PAGE_KIND, 2, 70
            );
            assertWideRingSharesAndRoundTrips(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_DECIMAL128,
                    LiveViewCheckpointRangeRingStateReader.DEQUE_DECIMAL128_VALUE_PAGE_KIND, 2, 74
            );
            assertWideRingSharesAndRoundTrips(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL256,
                    LiveViewCheckpointRangeRingStateReader.DECIMAL256_VALUE_PAGE_KIND, 4, 78
            );
            assertWideRingSharesAndRoundTrips(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_DECIMAL256,
                    LiveViewCheckpointRangeRingStateReader.DEQUE_DECIMAL256_VALUE_PAGE_KIND, 4, 82
            );
        });
    }

    @Test
    public void testWholeChunkDropReusesRemainingTailWithoutWritingData() throws Exception {
        assertMemoryLeak(() -> {
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry()) {
                writeInitial(first, directory, 20, LiveViewCheckpointStateCodec.CHUNK_ROWS + 12);
                try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                     LiveViewCheckpointDataSegmentWriter unopenedWriter = new LiveViewCheckpointDataSegmentWriter(configuration);
                     Path dir = new Path()) {
                    builder.of(first, LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
                    builder.dropHeadRows(LiveViewCheckpointStateCodec.CHUNK_ROWS + 5L);
                    LiveViewCheckpointTestKeys.freeze(builder, unopenedWriter, KEY, Double.doubleToRawLongBits(1.25), 0, 0, 0, 7, second);
                }
                Assert.assertEquals(2, second.getStatePageCount());
                assertRefEquals(first.getStatePageRef(2), second.getStatePageRef(0));
                assertRefEquals(first.getStatePageRef(3), second.getStatePageRef(1));
                try (LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
                     Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), directory.reader, second);
                    Assert.assertEquals(5, reader.getHeadOffset());
                    Assert.assertEquals(7, reader.getRowCount());
                }
            }
        });
    }

    /**
     * Carries a three-chunk ring forward through one builder twice: once expiring the head
     * chunk and sealing a tail, once whole. Both freezes must name exactly the chunks the ring
     * holds, and no two reference slots of the builder may share an object.
     */
    private static void assertDroppedHeadChunkKeepsReferencesApart(int valueKind, long segmentId) {
        final int pagesPerChunk = valueKind == LiveViewCheckpointRangeRingStateReader.VALUE_KIND_NONE ? 1 : 2;
        try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
             LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
             LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
             LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry();
             LiveViewCheckpointPartitionMapEntry third = new LiveViewCheckpointPartitionMapEntry();
             LiveViewCheckpointPartitionMapEntry out = new LiveViewCheckpointPartitionMapEntry();
             Path dir = new Path()) {
            writer.of(checkpointsDir(dir), segmentId);
            // Three chunks of three rows, one per boundary.
            builder.ofEmpty(valueKind, 1);
            appendRingRows(builder, writer, valueKind, 0, 3);
            LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0, 0, 0, 0, 3, first);
            builder.of(first, valueKind, 1);
            appendRingRows(builder, writer, valueKind, 3, 6);
            LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0, 0, 0, 0, 6, second);
            builder.of(second, valueKind, 1);
            appendRingRows(builder, writer, valueKind, 6, 9);
            LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0, 0, 0, 0, 9, third);
            Assert.assertEquals(3 * pagesPerChunk, third.getStatePageCount());

            // One partition expires the head chunk and a row of the next, then seals a tail.
            builder.of(third, valueKind, 1);
            builder.dropHeadRows(4);
            assertPooledReferencesDistinct(builder);
            appendRingRows(builder, writer, valueKind, 9, 10);
            LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0, 0, 0, 0, 6, out);
            assertPooledReferencesDistinct(builder);
            Assert.assertEquals(3 * pagesPerChunk, out.getStatePageCount());
            for (int i = 0; i < 2 * pagesPerChunk; i++) {
                assertRefEquals(third.getStatePageRef(pagesPerChunk + i), out.getStatePageRef(i));
            }
            for (int i = 2 * pagesPerChunk; i < 3 * pagesPerChunk; i++) {
                final LiveViewCheckpointStatePageRef tail = out.getStatePageRef(i);
                Assert.assertEquals(segmentId, tail.getSegmentId());
                Assert.assertEquals(1, tail.getRowCount());
                for (int j = 0, n = third.getStatePageCount(); j < n; j++) {
                    Assert.assertNotEquals(
                            "the sealed tail must be a page of its own",
                            third.getStatePageRef(j).getOffset(),
                            tail.getOffset()
                    );
                }
            }

            // The next partition carries the whole ring forward through the same slots.
            builder.of(third, valueKind, 1);
            LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0, 0, 0, 0, 9, out);
            assertPooledReferencesDistinct(builder);
            Assert.assertEquals(3 * pagesPerChunk, out.getStatePageCount());
            for (int i = 0; i < 3 * pagesPerChunk; i++) {
                assertRefEquals(third.getStatePageRef(i), out.getStatePageRef(i));
            }
            writer.discard();
        }
    }

    private static void assertPerPartitionAllocation(String what, long narrow, long wide) {
        Assert.assertTrue(
                "a warm " + what + " of " + NARROW_PARTITIONS + " partitions allocated " + narrow + " bytes on the Java heap",
                narrow < PER_WALK_ALLOCATION_LIMIT_BYTES
        );
        Assert.assertTrue(
                "a warm " + what + " must allocate nothing per partition [allocatedAt" + NARROW_PARTITIONS + '=' + narrow
                        + ", allocatedAt" + WARM_PARTITIONS + '=' + wide + ']',
                wide - narrow < PER_WALK_ALLOCATION_LIMIT_BYTES
        );
    }

    /**
     * Asserts that no two reference slots of {@code builder} name one object. The slots past
     * the references the builder holds keep objects for reuse, so they count too.
     */
    private static void assertPooledReferencesDistinct(LiveViewCheckpointRangeRingStateBuilder builder) {
        final LiveViewCheckpointStatePageRef[] refs;
        try {
            final Field field = LiveViewCheckpointRangeRingStateBuilder.class.getDeclaredField("refs");
            field.setAccessible(true);
            refs = (LiveViewCheckpointStatePageRef[]) field.get(builder);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
        for (int i = 0; i < refs.length; i++) {
            for (int j = i + 1; j < refs.length; j++) {
                if (refs[i] != null && refs[i] == refs[j]) {
                    Assert.fail("reference slots " + i + " and " + j + " share one object");
                }
            }
        }
    }

    private static void assertDequeRingSharesAndRoundTrips(int valueKind, int expectedPageKind, boolean longColumn) {
        final int initialRows = 4_106;
        final int dropRows = 5;
        final int appendRows = 3;
        final long firstSegment = longColumn ? 60 : 62;
        final long secondSegment = firstSegment + 1;
        final LongList firstTimestamps = new LongList();
        final LongList firstValues = new LongList();
        try (Catalogue directory = new Catalogue();
             LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
             LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry()) {
            try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                 LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                 Path dir = new Path()) {
                builder.ofEmpty(valueKind, 1);
                writer.of(checkpointsDir(dir), firstSegment);
                for (int i = 0; i < initialRows; i++) {
                    final long ts = i * 1_000L;
                    // Strictly decreasing candidate values, greatest at the front: a
                    // monotonic max deque snapshot, which is the sorted oracle.
                    final long valueBits = dequeValueBits(initialRows - i, longColumn);
                    builder.append(writer, ts, valueBits);
                    firstTimestamps.add(ts);
                    firstValues.add(valueBits);
                }
                LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0L, 0, 0, 0, initialRows, first);
                directory.addSegment(firstSegment, writer.commit());
            }

            final LongList secondTimestamps = new LongList();
            final LongList secondValues = new LongList();
            try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                 LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                 Path dir = new Path()) {
                builder.of(first, valueKind, 1);
                builder.dropHeadRows(dropRows);
                writer.of(checkpointsDir(dir), secondSegment);
                for (int i = dropRows; i < initialRows; i++) {
                    secondTimestamps.add(firstTimestamps.getQuick(i));
                    secondValues.add(firstValues.getQuick(i));
                }
                for (int i = 0; i < appendRows; i++) {
                    final long ts = (initialRows + i) * 1_000L;
                    final long valueBits = dequeValueBits(-i - 1, longColumn);
                    builder.append(writer, ts, valueBits);
                    secondTimestamps.add(ts);
                    secondValues.add(valueBits);
                }
                LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0L, 0, 0, 0, secondTimestamps.size(), second);
                directory.addSegment(secondSegment, writer.commit());
            }

            // Every value page self-identifies as the deque page kind, distinct from
            // the value-ring kinds.
            Assert.assertEquals(expectedPageKind, first.getStatePageRef(1).getPageKind());
            for (int i = 1; i < second.getStatePageCount(); i += 2) {
                Assert.assertEquals(expectedPageKind, second.getStatePageRef(i).getPageKind());
            }
            // The second root references at least one chunk the first root sealed
            // rather than re-encoding it, which is the whole point of sharing.
            boolean shared = false;
            for (int i = 0; i < second.getStatePageCount(); i++) {
                if (second.getStatePageRef(i).getSegmentId() == firstSegment) {
                    shared = true;
                    break;
                }
            }
            Assert.assertTrue("deque ring did not share any chunk from the first root", shared);

            assertRestored(first, directory, firstTimestamps, firstValues);
            assertRestored(second, directory, secondTimestamps, secondValues);
            try (LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir), directory.reader, second);
                Assert.assertEquals(valueKind, reader.getValueKind());
            }
        }
    }

    private static long dequeValueBits(long value, boolean longColumn) {
        return longColumn ? value : Double.doubleToRawLongBits((double) value);
    }

    private static void assertInvalid(
            byte[] scalar,
            LiveViewCheckpointStatePageRef[] refs,
            Catalogue directory,
            boolean readPayload,
            CharSequence message
    ) {
        try (LiveViewCheckpointPartitionMapEntry entry = entry(scalar, refs)) {
            assertInvalid(entry, directory, readPayload, message);
        }
    }

    private static void assertInvalid(
            LiveViewCheckpointPartitionMapEntry entry,
            Catalogue directory,
            boolean readPayload,
            CharSequence message
    ) {
        try (LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
             Path dir = new Path()) {
            try {
                reader.of(checkpointsDir(dir), directory.reader, entry);
                if (readPayload) {
                    reader.forEachRow((timestamp, value) -> {
                    });
                }
                Assert.fail("expected corrupt avg RANGE state rejection");
            } catch (CairoException e) {
                Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }
        }
    }

    /**
     * Seals a one-word LONG value ring holding {@code payload} into {@code segmentId}, asserts
     * every value page self-identifies as the long page kind and landed under
     * {@code expectedValueCodec}, then walks the ring back against {@code payload}.
     * <p>
     * {@link #assertRestored} cannot stand in for the walk: it neither asserts the restored
     * value kind nor takes the payload as a {@code long[]}.
     */
    private static void assertLongValueRingRoundTrips(long[] payload, long segmentId, int expectedValueCodec) {
        try (Catalogue directory = new Catalogue();
             LiveViewCheckpointPartitionMapEntry root = new LiveViewCheckpointPartitionMapEntry()) {
            try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                 LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                 Path dir = new Path()) {
                builder.ofEmpty(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_LONG, 1);
                writer.of(checkpointsDir(dir), segmentId);
                for (int i = 0; i < payload.length; i++) {
                    builder.append(writer, i * 1_000L, payload[i]);
                }
                LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0L, 0, 0, 0, payload.length, root);
                directory.addSegment(segmentId, writer.commit());
            }
            for (int i = 1; i < root.getStatePageCount(); i += 2) {
                Assert.assertEquals(
                        LiveViewCheckpointRangeRingStateReader.LONG_VALUE_PAGE_KIND,
                        root.getStatePageRef(i).getPageKind()
                );
                Assert.assertEquals(expectedValueCodec, root.getStatePageRef(i).getCodec());
            }
            try (LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir), directory.reader, root);
                Assert.assertEquals(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_LONG, reader.getValueKind());
                Assert.assertEquals(payload.length, reader.getRowCount());
                final int[] index = {0};
                reader.forEachRow((timestamp, valueBits) -> {
                    final int i = index[0]++;
                    Assert.assertEquals(i * 1_000L, timestamp);
                    Assert.assertEquals(payload[i], valueBits);
                });
                Assert.assertEquals(payload.length, index[0]);
            }
        }
    }

    /**
     * Asserts {@code entry} holds exactly the given per-page format-1 codec tags, in page order -
     * the page count has to match the expectation exactly, not merely cover it - and that no page
     * stores more bytes than the payload it decodes to.
     */
    private static void assertPageCodecs(LiveViewCheckpointPartitionMapEntry entry, int... expectedCodecs) {
        Assert.assertEquals(expectedCodecs.length, entry.getStatePageCount());
        for (int i = 0; i < expectedCodecs.length; i++) {
            final LiveViewCheckpointStatePageRef ref = entry.getStatePageRef(i);
            Assert.assertEquals("page " + i + " codec", expectedCodecs[i], ref.getCodec());
            assertPageDoesNotExpand(ref, i);
        }
    }

    /**
     * Asserts one page kept the hard invariant of the format: raw always participates
     * in selection and wins ties, so a stored page never exceeds its decoded payload.
     */
    private static void assertPageDoesNotExpand(LiveViewCheckpointStatePageRef ref, int index) {
        Assert.assertTrue(
                "page " + index + " stored " + ref.getStoredLength()
                        + " bytes for a " + ref.getDecodedLength() + "-byte payload",
                ref.getStoredLength() <= ref.getDecodedLength()
        );
    }

    private static void assertRefEquals(LiveViewCheckpointStatePageRef expected, LiveViewCheckpointStatePageRef actual) {
        Assert.assertEquals(expected.getSegmentId(), actual.getSegmentId());
        Assert.assertEquals(expected.getOffset(), actual.getOffset());
        Assert.assertEquals(expected.getStoredLength(), actual.getStoredLength());
        Assert.assertEquals(expected.getDecodedLength(), actual.getDecodedLength());
        Assert.assertEquals(expected.getPageKind(), actual.getPageKind());
        Assert.assertEquals(expected.getCodec(), actual.getCodec());
        Assert.assertEquals(expected.getRowCount(), actual.getRowCount());
        Assert.assertEquals(expected.getFlags(), actual.getFlags());
    }

    /**
     * Rebinds {@code reader} to {@code entry} and asserts the whole ring reads
     * back, so the caller can drive several bindings through one reader.
     */
    private static void assertWalked(
            LiveViewCheckpointRangeRingStateReader reader,
            Path dir,
            Catalogue directory,
            LiveViewCheckpointPartitionMapEntry entry,
            LongList expectedTimestamps,
            LongList expectedValues
    ) {
        reader.of(checkpointsDir(dir), directory.reader, entry);
        Assert.assertEquals(expectedTimestamps.size(), reader.getRowCount());
        final int[] index = {0};
        reader.forEachRow((timestamp, value) -> {
            final int i = index[0]++;
            Assert.assertEquals(expectedTimestamps.getQuick(i), timestamp);
            Assert.assertEquals(expectedValues.getQuick(i), value);
        });
        Assert.assertEquals(expectedTimestamps.size(), index[0]);
    }

    private static void assertRestored(
            LiveViewCheckpointPartitionMapEntry entry,
            Catalogue directory,
            LongList expectedTimestamps,
            LongList expectedValues
    ) {
        try (LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
             Path dir = new Path()) {
            reader.of(checkpointsDir(dir), directory.reader, entry);
            Assert.assertEquals(expectedTimestamps.size(), reader.getRowCount());
            final int[] index = {0};
            reader.forEachRow((timestamp, value) -> {
                final int i = index[0]++;
                Assert.assertEquals(expectedTimestamps.getQuick(i), timestamp);
                Assert.assertEquals(expectedValues.getQuick(i), value);
            });
            Assert.assertEquals(expectedTimestamps.size(), index[0]);
        }
    }

    private static void assertTimestampsRestored(
            LiveViewCheckpointPartitionMapEntry entry,
            Catalogue directory,
            LongList expectedTimestamps
    ) {
        try (LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
             Path dir = new Path()) {
            reader.of(checkpointsDir(dir), directory.reader, entry);
            Assert.assertEquals(expectedTimestamps.size(), reader.getRowCount());
            final int[] index = {0};
            reader.forEachTimestamp(timestamp -> {
                final int i = index[0]++;
                Assert.assertEquals(expectedTimestamps.getQuick(i), timestamp);
            });
            Assert.assertEquals(expectedTimestamps.size(), index[0]);
        }
    }

    /**
     * Seals one {@code valueKind} partition from {@code timestamps} and
     * {@code wordStream} - which holds one row's value words consecutively - then
     * asserts every value page landed under {@code expectedValueCodec}, that every
     * page stayed within the payload it decodes to, and that the ring walks back
     * against the oracle. The row count spills past the width's chunk cap, so the
     * assertions cover the partial tail chunk as well as the full one.
     */
    private static void assertValueCodecSelection(
            int valueKind,
            int expectedPageKind,
            long segmentId,
            LongList timestamps,
            LongList wordStream,
            int expectedValueCodec
    ) {
        final int words = LiveViewCheckpointRangeRingStateReader.valueWords(valueKind);
        try (Catalogue directory = new Catalogue();
             LiveViewCheckpointPartitionMapEntry root = new LiveViewCheckpointPartitionMapEntry()) {
            try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                 LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                 Path dir = new Path()) {
                builder.ofEmpty(valueKind, words);
                writer.of(checkpointsDir(dir), segmentId);
                for (int i = 0, n = timestamps.size(); i < n; i++) {
                    final long timestamp = timestamps.getQuick(i);
                    switch (words) {
                        case 1 -> builder.append(writer, timestamp, wordStream.getQuick(i));
                        case 2 -> builder.append(
                                writer, timestamp,
                                wordStream.getQuick(2 * i), wordStream.getQuick(2 * i + 1)
                        );
                        default -> builder.append(
                                writer, timestamp,
                                wordStream.getQuick(4 * i), wordStream.getQuick(4 * i + 1),
                                wordStream.getQuick(4 * i + 2), wordStream.getQuick(4 * i + 3)
                        );
                    }
                }
                LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 0L, 0, 0, 0, timestamps.size(), root);
                directory.addSegment(segmentId, writer.commit());
            }
            Assert.assertTrue(
                    "expected the run to span more than one chunk, got " + root.getStatePageCount() + " pages",
                    root.getStatePageCount() > 2
            );
            for (int i = 0, n = root.getStatePageCount(); i < n; i++) {
                final LiveViewCheckpointStatePageRef ref = root.getStatePageRef(i);
                assertPageDoesNotExpand(ref, i);
                if ((i & 1) == 0) {
                    Assert.assertEquals(
                            LiveViewCheckpointRangeRingStateReader.TIMESTAMP_PAGE_KIND, ref.getPageKind()
                    );
                } else {
                    Assert.assertEquals(expectedPageKind, ref.getPageKind());
                    Assert.assertEquals("value page " + i + " codec", expectedValueCodec, ref.getCodec());
                }
            }
            if (words == 1) {
                assertRestored(root, directory, timestamps, wordStream);
            } else {
                assertWideRestored(root, directory, timestamps, wordStream, words);
            }
        }
    }

    /**
     * Drives one wide-decimal ring of {@code valueKind} over two chunks and asserts
     * its flattened word stream selected {@code expectedValueCodec}. A narrow run
     * repeats the high word and keeps the rest a few thousand apart; a wide run puts
     * both ends of the 64-bit range in every row, so every chunk - the partial tail
     * included - forces a 64-bit width.
     */
    private static void assertWideCodecSelection(
            int valueKind,
            int expectedPageKind,
            long segmentId,
            boolean narrow,
            int expectedValueCodec
    ) {
        final int words = LiveViewCheckpointRangeRingStateReader.valueWords(valueKind);
        final int rows = LiveViewCheckpointRangeRingStateReader.maxChunkRows(valueKind) + 5;
        final LongList timestamps = new LongList();
        final LongList wordStream = new LongList();
        for (int i = 0; i < rows; i++) {
            timestamps.add(i * 1_000L);
            for (int w = 0; w < words; w++) {
                if (narrow) {
                    wordStream.add(w == 0 ? 0 : i + w);
                } else {
                    wordStream.add((w & 1) == 0 ? Long.MIN_VALUE + i : Long.MAX_VALUE - i);
                }
            }
        }
        assertValueCodecSelection(valueKind, expectedPageKind, segmentId, timestamps, wordStream, expectedValueCodec);
    }

    private static void assertWideRestored(
            LiveViewCheckpointPartitionMapEntry entry,
            Catalogue directory,
            LongList expectedTimestamps,
            LongList expectedWords,
            int words
    ) {
        try (LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
             Path dir = new Path()) {
            reader.of(checkpointsDir(dir), directory.reader, entry);
            Assert.assertEquals(expectedTimestamps.size(), reader.getRowCount());
            final int[] index = {0};
            if (words == 2) {
                reader.forEachRow((timestamp, hi, lo) -> {
                    final int i = index[0]++;
                    Assert.assertEquals(expectedTimestamps.getQuick(i), timestamp);
                    Assert.assertEquals(expectedWords.getQuick(2 * i), hi);
                    Assert.assertEquals(expectedWords.getQuick(2 * i + 1), lo);
                });
            } else {
                reader.forEachRow((timestamp, hh, hl, lh, ll) -> {
                    final int i = index[0]++;
                    Assert.assertEquals(expectedTimestamps.getQuick(i), timestamp);
                    Assert.assertEquals(expectedWords.getQuick(4 * i), hh);
                    Assert.assertEquals(expectedWords.getQuick(4 * i + 1), hl);
                    Assert.assertEquals(expectedWords.getQuick(4 * i + 2), lh);
                    Assert.assertEquals(expectedWords.getQuick(4 * i + 3), ll);
                });
            }
            Assert.assertEquals(expectedTimestamps.size(), index[0]);
            // The narrow overload must refuse a wide ring rather than hand back the
            // most significant word alone.
            try {
                reader.forEachRow((timestamp, valueBits) -> {
                });
                Assert.fail("expected a value width mismatch");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "value width mismatch");
            }
        }
    }

    private static void assertWideRingSharesAndRoundTrips(
            int valueKind,
            int expectedPageKind,
            int words,
            long firstSegment
    ) {
        final int initialRows = 4_106;
        final int dropRows = 5;
        final int appendRows = 3;
        final long secondSegment = firstSegment + 1;
        final int maxChunkRows = LiveViewCheckpointRangeRingStateReader.maxChunkRows(valueKind);
        Assert.assertEquals(LiveViewCheckpointStateCodec.CHUNK_ROWS / words, maxChunkRows);
        final LongList firstTimestamps = new LongList();
        final LongList firstWords = new LongList();
        try (Catalogue directory = new Catalogue();
             LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
             LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry()) {
            try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                 LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                 Path dir = new Path()) {
                builder.ofEmpty(valueKind, words);
                writer.of(checkpointsDir(dir), firstSegment);
                for (int i = 0; i < initialRows; i++) {
                    final long ts = i * 1_000L;
                    appendWideRow(builder, writer, ts, initialRows - i, words, firstTimestamps, firstWords);
                }
                LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 1, 2, 3, 4, initialRows, first);
                directory.addSegment(firstSegment, writer.commit());
            }

            final LongList secondTimestamps = new LongList();
            final LongList secondWords = new LongList();
            try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                 LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                 Path dir = new Path()) {
                builder.of(first, valueKind, words);
                builder.dropHeadRows(dropRows);
                writer.of(checkpointsDir(dir), secondSegment);
                for (int i = dropRows; i < initialRows; i++) {
                    secondTimestamps.add(firstTimestamps.getQuick(i));
                    for (int w = 0; w < words; w++) {
                        secondWords.add(firstWords.getQuick(i * words + w));
                    }
                }
                for (int i = 0; i < appendRows; i++) {
                    final long ts = (initialRows + i) * 1_000L;
                    appendWideRow(builder, writer, ts, -i - 1, words, secondTimestamps, secondWords);
                }
                LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, 5, 6, 7, 8, secondTimestamps.size(), second);
                directory.addSegment(secondSegment, writer.commit());
            }

            // Every value page self-identifies as this kind, carries the row count in
            // rows and the payload in rows*words, and stops at the width's chunk cap.
            for (int i = 1; i < first.getStatePageCount(); i += 2) {
                final LiveViewCheckpointStatePageRef ref = first.getStatePageRef(i);
                Assert.assertEquals(expectedPageKind, ref.getPageKind());
                Assert.assertTrue(ref.getRowCount() <= maxChunkRows);
                Assert.assertEquals(ref.getRowCount() * words * Long.BYTES, ref.getDecodedLength());
            }
            Assert.assertEquals(maxChunkRows, first.getStatePageRef(0).getRowCount());
            // The second root references at least one chunk the first root sealed
            // rather than re-encoding it, which is the whole point of sharing.
            boolean shared = false;
            for (int i = 0; i < second.getStatePageCount(); i++) {
                if (second.getStatePageRef(i).getSegmentId() == firstSegment) {
                    shared = true;
                    break;
                }
            }
            Assert.assertTrue("wide ring did not share any chunk from the first root", shared);

            assertWideRestored(first, directory, firstTimestamps, firstWords, words);
            assertWideRestored(second, directory, secondTimestamps, secondWords, words);
            try (LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir), directory.reader, second);
                Assert.assertEquals(valueKind, reader.getValueKind());
                Assert.assertEquals(words, reader.getScalarWordCount());
                for (int w = 0; w < words; w++) {
                    Assert.assertEquals(5 + w, reader.getScalarWord(w));
                }
            }
        }
    }

    private static void appendWideRow(
            LiveViewCheckpointRangeRingStateBuilder builder,
            LiveViewCheckpointDataSegmentWriter writer,
            long timestamp,
            long seed,
            int words,
            LongList timestamps,
            LongList out
    ) {
        timestamps.add(timestamp);
        if (words == 2) {
            // Every 97th row is the DECIMAL128 NULL sentinel, which the ring must carry
            // verbatim rather than reject.
            final long hi = seed % 97 == 0 ? Decimals.DECIMAL128_HI_NULL : seed;
            final long lo = seed % 97 == 0 ? Decimals.DECIMAL128_LO_NULL : ~seed;
            builder.append(writer, timestamp, hi, lo);
            out.add(hi);
            out.add(lo);
            return;
        }
        final long hh = seed % 97 == 0 ? Decimals.DECIMAL256_HH_NULL : seed;
        final long hl = seed % 97 == 0 ? Decimals.DECIMAL256_HL_NULL : ~seed;
        final long lh = seed % 97 == 0 ? Decimals.DECIMAL256_LH_NULL : seed * 31;
        final long ll = seed % 97 == 0 ? Decimals.DECIMAL256_LL_NULL : Long.MIN_VALUE + seed;
        builder.append(writer, timestamp, hh, hl, lh, ll);
        out.add(hh);
        out.add(hl);
        out.add(lh);
        out.add(ll);
    }

    private static void appendRingRows(
            LiveViewCheckpointRangeRingStateBuilder builder,
            LiveViewCheckpointDataSegmentWriter writer,
            int valueKind,
            int fromRow,
            int toRow
    ) {
        for (int row = fromRow; row < toRow; row++) {
            if (valueKind == LiveViewCheckpointRangeRingStateReader.VALUE_KIND_NONE) {
                builder.append(writer, row * 1_000L);
            } else {
                builder.append(writer, row * 1_000L, Double.doubleToRawLongBits(row + 0.25));
            }
        }
    }

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    /**
     * Writes {@code value} over one 64-bit word of a published data page, which is
     * how a test forges the payload corruption a data page carries no checksum to
     * detect.
     */
    private static void corruptDataPage(
            Catalogue directory,
            long segmentId,
            long pageOffset,
            long byteOffset,
            long value
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        final long fileLength = directory.reader.getFileLength(segmentId);
        try (Path path = new Path()) {
            final long fd = ff.openRW(dataPath(path, segmentId).$(), 0);
            final long address = ff.mmap(fd, fileLength, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
            try {
                Unsafe.putLong(address + pageOffset + byteOffset, value);
            } finally {
                ff.munmap(address, fileLength, MemoryTag.MMAP_DEFAULT);
                ff.close(fd);
            }
        }
    }

    private static LiveViewCheckpointStatePageRef copy(LiveViewCheckpointStatePageRef ref) {
        return new LiveViewCheckpointStatePageRef().of(
                ref.getSegmentId(), ref.getOffset(), ref.getStoredLength(), ref.getDecodedLength(),
                ref.getPageKind(), ref.getCodec(), ref.getRowCount(), ref.getFlags()
        );
    }

    /**
     * Writes {@code value} into {@code bytes} as a little-endian int, so a loop can key each
     * partition without a heap array per key.
     */
    private static byte[] keyBytes(byte[] bytes, int value) {
        for (int i = 0; i < Integer.BYTES; i++) {
            bytes[i] = (byte) (value >>> (i * 8));
        }
        return bytes;
    }

    private static long minRestoreAllocation(
            TestUtils.ThreadMetricsScope<ThreadMXBean> scope,
            LiveViewCheckpointRangeRingStateReader reader,
            Path dir,
            Catalogue directory,
            ObjList<LiveViewCheckpointPartitionMapEntry> entries,
            int count,
            LiveViewCheckpointRingStateSource.RowConsumer consumer
    ) {
        long min = Long.MAX_VALUE;
        for (int round = 0; round < ALLOCATION_ROUNDS; round++) {
            final long before = scope.getBean().getCurrentThreadAllocatedBytes();
            restoreEach(reader, dir, directory, entries, count, consumer);
            min = Math.min(min, scope.getBean().getCurrentThreadAllocatedBytes() - before);
        }
        return min;
    }

    private static long minSealAllocation(
            TestUtils.ThreadMetricsScope<ThreadMXBean> scope,
            LiveViewCheckpointRangeRingStateBuilder builder,
            LiveViewCheckpointDataSegmentWriter writer,
            LiveViewCheckpointTestKeys key,
            byte[] keyBytes,
            ObjList<LiveViewCheckpointPartitionMapEntry> entries,
            int count,
            LiveViewCheckpointPartitionMapEntry out
    ) {
        long min = Long.MAX_VALUE;
        for (int round = 0; round < ALLOCATION_ROUNDS; round++) {
            final long before = scope.getBean().getCurrentThreadAllocatedBytes();
            sealEach(builder, writer, key, keyBytes, entries, count, out);
            min = Math.min(min, scope.getBean().getCurrentThreadAllocatedBytes() - before);
        }
        return min;
    }

    private static long minValidateAllocation(
            TestUtils.ThreadMetricsScope<ThreadMXBean> scope,
            LiveViewCheckpointRangeRingStateReader reader,
            ObjList<LiveViewCheckpointPartitionMapEntry> entries,
            int count
    ) {
        long min = Long.MAX_VALUE;
        for (int round = 0; round < ALLOCATION_ROUNDS; round++) {
            final long before = scope.getBean().getCurrentThreadAllocatedBytes();
            for (int i = 0; i < count; i++) {
                reader.ofMetadata(entries.getQuick(i));
            }
            min = Math.min(min, scope.getBean().getCurrentThreadAllocatedBytes() - before);
        }
        return min;
    }

    /**
     * Opens and walks the first {@code count} rings of {@code entries} through one reader,
     * as a restore does.
     *
     * @return the rows the rings hold
     */
    private static long restoreEach(
            LiveViewCheckpointRangeRingStateReader reader,
            Path dir,
            Catalogue directory,
            ObjList<LiveViewCheckpointPartitionMapEntry> entries,
            int count,
            LiveViewCheckpointRingStateSource.RowConsumer consumer
    ) {
        long rows = 0;
        for (int i = 0; i < count; i++) {
            reader.of(dir, directory.reader, entries.getQuick(i));
            reader.forEachRow(consumer);
            rows += reader.getRowCount();
        }
        return rows;
    }

    /**
     * Seals the first {@code count} rings of {@code entries} as a cadence seal does: each
     * carries its chunks forward, expires its whole first chunk and one row of the second,
     * appends one row and freezes into {@code out}.
     */
    private static void sealEach(
            LiveViewCheckpointRangeRingStateBuilder builder,
            LiveViewCheckpointDataSegmentWriter writer,
            LiveViewCheckpointTestKeys key,
            byte[] keyBytes,
            ObjList<LiveViewCheckpointPartitionMapEntry> entries,
            int count,
            LiveViewCheckpointPartitionMapEntry out
    ) {
        for (int i = 0; i < count; i++) {
            key.of(keyBytes(keyBytes, i));
            builder.of(entries.getQuick(i), LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
            builder.dropHeadRows(4);
            builder.append(writer, TWO_CHUNK_RING_ROWS * 1_000L, Double.doubleToRawLongBits(i));
            builder.freeze(writer, key.address(), key.length(), Double.doubleToRawLongBits(i), 0, 0, 0, 3, out);
        }
    }

    private static Path dataPath(Path path, long segmentId) {
        try (Path dir = new Path()) {
            return LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir(dir), segmentId);
        }
    }

    private static LiveViewCheckpointPartitionMapEntry entry(
            byte[] scalar,
            LiveViewCheckpointStatePageRef[] refs
    ) {
        return LiveViewCheckpointTestKeys.of(new LiveViewCheckpointPartitionMapEntry(), KEY, scalar, refs);
    }

    private static LiveViewCheckpointStatePageRef[] refs(LiveViewCheckpointPartitionMapEntry entry) {
        final LiveViewCheckpointStatePageRef[] refs = new LiveViewCheckpointStatePageRef[entry.getStatePageCount()];
        for (int i = 0; i < refs.length; i++) {
            refs[i] = copy(entry.getStatePageRef(i));
        }
        return refs;
    }

    private static void writeInitial(
            LiveViewCheckpointPartitionMapEntry out,
            Catalogue directory,
            long segmentId,
            int rows
    ) {
        writeInitial(out, directory, segmentId, rows, 1_000L);
    }

    /**
     * Writes a fresh double ring of {@code rows} rows spaced {@code step} apart.
     * The spacing decides which codec the timestamp page lands under, so a test
     * that corrupts a payload picks the spacing that gives it the layout it means
     * to corrupt.
     */
    private static void writeInitial(
            LiveViewCheckpointPartitionMapEntry out,
            Catalogue directory,
            long segmentId,
            int rows,
            long step
    ) {
        try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
             LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
             Path dir = new Path()) {
            builder.ofEmpty(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
            writer.of(checkpointsDir(dir), segmentId);
            for (int i = 0; i < rows; i++) {
                builder.append(writer, i * step, Double.doubleToRawLongBits(i + 0.25));
            }
            LiveViewCheckpointTestKeys.freeze(builder, writer, KEY, Double.doubleToRawLongBits(42.5), 0, 0, 0, rows, out);
            directory.addSegment(segmentId, writer.commit());
        }
    }

    /**
     * Seals {@code count} double rings into data segment {@code segmentId} and publishes it.
     * Ring {@code i} holds two chunks of three rows, at 0 to 5,000, whose values are
     * {@code i + row * 0.5}; its entry is appended to {@code entries}.
     */
    private static void writeTwoChunkRings(
            ObjList<LiveViewCheckpointPartitionMapEntry> entries,
            Catalogue directory,
            long segmentId,
            int count
    ) {
        try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
             LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
             LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
             LiveViewCheckpointTestKeys key = new LiveViewCheckpointTestKeys();
             Path dir = new Path()) {
            writer.of(checkpointsDir(dir), segmentId);
            final byte[] keyBytes = new byte[Integer.BYTES];
            final int chunkRows = TWO_CHUNK_RING_ROWS / 2;
            for (int i = 0; i < count; i++) {
                key.of(keyBytes(keyBytes, i));
                builder.ofEmpty(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
                for (int row = 0; row < chunkRows; row++) {
                    builder.append(writer, row * 1_000L, Double.doubleToRawLongBits(i + row * 0.5));
                }
                builder.freeze(writer, key.address(), key.length(), 0, 0, 0, 0, chunkRows, first);
                builder.of(first, LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
                for (int row = chunkRows; row < TWO_CHUNK_RING_ROWS; row++) {
                    builder.append(writer, row * 1_000L, Double.doubleToRawLongBits(i + row * 0.5));
                }
                final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
                entries.add(entry);
                builder.freeze(writer, key.address(), key.length(), 0, 0, 0, 0, TWO_CHUNK_RING_ROWS, entry);
                Assert.assertEquals(4, entry.getStatePageCount());
            }
            directory.addSegment(segmentId, writer.commit());
        }
    }

    /**
     * Publishes each added data segment into a fresh copy-on-write directory
     * generation and keeps a reader bound to the newest root, which is the
     * bounds source a chunk read validates its page references against.
     */
    private static final class Catalogue implements Closeable {

        // Static, because a test method builds several catalogues over one
        // checkpoints directory and a published metadata segment is immutable:
        // a per-instance counter restarts at the same id and the second
        // catalogue republishes a name the first one already took. POSIX
        // rename() hides that by replacing the file; Windows MoveFileW refuses
        // it, which is how the reuse surfaced in the first place.
        private static long nextMetaSegmentId = 1_000;
        private final LiveViewCheckpointSegmentDirectoryReader reader =
                new LiveViewCheckpointSegmentDirectoryReader(configuration);
        private final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();

        @Override
        public void close() {
            reader.close();
        }

        private void addSegment(long segmentId, long fileLength) {
            try (LiveViewCheckpointSegmentDirectoryWriter writer = new LiveViewCheckpointSegmentDirectoryWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                writer.begin(root);
                writer.addSegment(segmentId, fileLength, 1);
                writer.publish(nextMetaSegmentId++, 1, root);
                reader.of(checkpointsDir(dir), root);
            }
        }
    }
}
