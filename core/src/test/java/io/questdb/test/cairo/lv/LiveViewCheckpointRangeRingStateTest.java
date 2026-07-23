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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateBuilder;
import io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateReader;
import io.questdb.cairo.lv.LiveViewCheckpointDataSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapEntry;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryWriter;
import io.questdb.cairo.lv.LiveViewCheckpointStateCodec;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LiveViewCheckpointRangeRingStateTest extends AbstractCairoTest {

    private static final byte[] KEY = new byte[]{1, 2, 3};
    private static final String LV_DIR = "lv_avg_range_chunks";

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
            final LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
            final LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry();
            final long[] secondSegmentBytes = new long[1];
            try (Catalogue directory = new Catalogue()) {
                writeInitial(first, directory, 1, 4_106);

                try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                     LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                     Path dir = new Path()) {
                    builder.of(first, LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE);
                    builder.dropHeadRows(5);
                    writer.of(checkpointsDir(dir), 2);
                    builder.append(writer, 4_106_000, Double.doubleToRawLongBits(10_000.0));
                    builder.append(writer, 4_107_000, Double.doubleToRawLongBits(-0.0));
                    builder.append(writer, 4_108_000, Double.doubleToRawLongBits(10_002.0));
                    builder.freeze(writer, KEY, Double.doubleToRawLongBits(-0.0), 4_104, second);
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
    public void testLongValueRingRoundTripsRawBits() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPartitionMapEntry root = new LiveViewCheckpointPartitionMapEntry();
            // A raw 64-bit value column must round-trip any bit pattern verbatim,
            // including LONG_NULL and a bit pattern that would be a NaN if read as a
            // double - proof the long ring never routes a value through a double.
            final long[] payload = {
                    Long.MIN_VALUE, // LONG_NULL
                    0L,
                    -1L,
                    Long.MAX_VALUE,
                    0x7ff0_0000_0000_0001L, // a signaling-NaN bit pattern as a raw long
                    42L,
            };
            try (Catalogue directory = new Catalogue()) {
                try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                     LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                     Path dir = new Path()) {
                    builder.ofEmpty(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_LONG);
                    writer.of(checkpointsDir(dir), 7);
                    for (int i = 0; i < payload.length; i++) {
                        builder.append(writer, i * 1_000L, payload[i]);
                    }
                    builder.freeze(writer, KEY, 0L, payload.length, root);
                    directory.addSegment(7, writer.commit());
                }
                // The value pages self-identify as the long page kind, stored raw.
                for (int i = 1; i < root.getStatePageCount(); i += 2) {
                    Assert.assertEquals(
                            LiveViewCheckpointRangeRingStateReader.LONG_VALUE_PAGE_KIND,
                            root.getStatePageRef(i).getPageKind()
                    );
                    Assert.assertEquals(
                            LiveViewCheckpointStateCodec.LONG_RAW_64,
                            root.getStatePageRef(i).getCodec()
                    );
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
        });
    }

    @Test
    public void testMalformedMetadataAndDataAreRejected() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPartitionMapEntry valid = new LiveViewCheckpointPartitionMapEntry();
            try (Catalogue directory = new Catalogue()) {
                writeInitial(valid, directory, 30, 3);

                final byte[] shortScalar = Arrays.copyOf(
                        valid.getScalarState(),
                        LiveViewCheckpointRangeRingStateReader.SCALAR_STATE_BYTES - 1
                );
                assertInvalid(entry(shortScalar, refs(valid)), directory, false, "scalar state size mismatch");

                final byte[] badVersion = Arrays.copyOf(valid.getScalarState(), valid.getScalarState().length);
                badVersion[0] = 2;
                assertInvalid(entry(badVersion, refs(valid)), directory, false, "format version mismatch");

                final byte[] badHead = Arrays.copyOf(valid.getScalarState(), valid.getScalarState().length);
                badHead[4] = 127;
                assertInvalid(entry(badHead, refs(valid)), directory, false, "logical chunk bounds invalid");

                final LiveViewCheckpointStatePageRef[] oddRefs = Arrays.copyOf(refs(valid), 1);
                assertInvalid(entry(valid.getScalarState(), oddRefs), directory, false, "reference count invalid");

                final LiveViewCheckpointStatePageRef[] badKind = refs(valid);
                final LiveViewCheckpointStatePageRef timestampRef = badKind[0];
                badKind[0] = copy(timestampRef).of(
                        timestampRef.getSegmentId(), timestampRef.getOffset(), timestampRef.getStoredLength(),
                        timestampRef.getDecodedLength(), LiveViewCheckpointRangeRingStateReader.DOUBLE_VALUE_PAGE_KIND,
                        timestampRef.getCodec(), timestampRef.getRowCount(), timestampRef.getFlags()
                );
                assertInvalid(entry(valid.getScalarState(), badKind), directory, false, "timestamp page kind or codec invalid");

                final LiveViewCheckpointStatePageRef[] mismatched = refs(valid);
                final LiveViewCheckpointStatePageRef valueRef = mismatched[1];
                mismatched[1] = copy(valueRef).of(
                        valueRef.getSegmentId(), valueRef.getOffset(), valueRef.getStoredLength(),
                        2 * Long.BYTES, valueRef.getPageKind(), valueRef.getCodec(), 2, valueRef.getFlags()
                );
                assertInvalid(entry(valid.getScalarState(), mismatched), directory, false, "row counts differ");

                Assert.assertEquals(LiveViewCheckpointStateCodec.TIMESTAMP_RAW_64, valid.getStatePageRef(0).getCodec());
                final FilesFacade ff = configuration.getFilesFacade();
                final long fileLength = directory.reader.getFileLength(30);
                try (Path path = new Path()) {
                    final long fd = ff.openRW(dataPath(path, 30).$(), 0);
                    final long address = ff.mmap(fd, fileLength, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
                    try {
                        Unsafe.putLong(address + valid.getStatePageRef(0).getOffset() + Long.BYTES, -1);
                    } finally {
                        ff.munmap(address, fileLength, MemoryTag.MMAP_DEFAULT);
                        ff.close(fd);
                    }
                }
                assertInvalid(valid, directory, true, "rows are not canonical");
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
                LiveViewCheckpointPartitionMapEntry previous = new LiveViewCheckpointPartitionMapEntry();
                for (int generation = 0; generation < 40; generation++) {
                    final LiveViewCheckpointPartitionMapEntry next = new LiveViewCheckpointPartitionMapEntry();
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
                            builder.ofEmpty(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE);
                        } else {
                            builder.of(previous, LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE);
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
                        builder.freeze(writer, KEY, Double.doubleToRawLongBits(generation + 0.125), timestamps.size(), next);
                        directory.addSegment(100 + generation, writer.commit());
                    }
                    roots.add(next);
                    timestampSnapshots.add(new LongList(timestamps));
                    valueSnapshots.add(new LongList(values));
                    assertRestored(next, directory, timestamps, values);
                    final int old = rnd.nextInt(roots.size());
                    assertRestored(roots.get(old), directory, timestampSnapshots.get(old), valueSnapshots.get(old));
                    previous = next;
                }
            }
        });
    }

    @Test
    public void testWholeChunkDropReusesRemainingTailWithoutWritingData() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
            final LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry();
            try (Catalogue directory = new Catalogue()) {
                writeInitial(first, directory, 20, LiveViewCheckpointStateCodec.CHUNK_ROWS + 12);
                try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                     LiveViewCheckpointDataSegmentWriter unopenedWriter = new LiveViewCheckpointDataSegmentWriter(configuration);
                     Path dir = new Path()) {
                    builder.of(first, LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE);
                    builder.dropHeadRows(LiveViewCheckpointStateCodec.CHUNK_ROWS + 5L);
                    builder.freeze(unopenedWriter, KEY, Double.doubleToRawLongBits(1.25), 7, second);
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

    private static void assertDequeRingSharesAndRoundTrips(int valueKind, int expectedPageKind, boolean longColumn) {
        final int initialRows = 4_106;
        final int dropRows = 5;
        final int appendRows = 3;
        final long firstSegment = longColumn ? 60 : 62;
        final long secondSegment = firstSegment + 1;
        final LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
        final LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry();
        final LongList firstTimestamps = new LongList();
        final LongList firstValues = new LongList();
        try (Catalogue directory = new Catalogue()) {
            try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                 LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                 Path dir = new Path()) {
                builder.ofEmpty(valueKind);
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
                builder.freeze(writer, KEY, 0L, initialRows, first);
                directory.addSegment(firstSegment, writer.commit());
            }

            final LongList secondTimestamps = new LongList();
            final LongList secondValues = new LongList();
            try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                 LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                 Path dir = new Path()) {
                builder.of(first, valueKind);
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
                builder.freeze(writer, KEY, 0L, secondTimestamps.size(), second);
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

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private static LiveViewCheckpointStatePageRef copy(LiveViewCheckpointStatePageRef ref) {
        return new LiveViewCheckpointStatePageRef().of(
                ref.getSegmentId(), ref.getOffset(), ref.getStoredLength(), ref.getDecodedLength(),
                ref.getPageKind(), ref.getCodec(), ref.getRowCount(), ref.getFlags()
        );
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
        return new LiveViewCheckpointPartitionMapEntry().of(KEY, scalar, refs);
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
        try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
             LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
             Path dir = new Path()) {
            builder.ofEmpty(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE);
            writer.of(checkpointsDir(dir), segmentId);
            for (int i = 0; i < rows; i++) {
                builder.append(writer, i * 1_000L, Double.doubleToRawLongBits(i + 0.25));
            }
            builder.freeze(writer, KEY, Double.doubleToRawLongBits(42.5), rows, out);
            directory.addSegment(segmentId, writer.commit());
        }
    }

    /**
     * Publishes each added data segment into a fresh copy-on-write directory
     * generation and keeps a reader bound to the newest root, which is the
     * bounds source a chunk read validates its page references against.
     */
    private static final class Catalogue implements Closeable {

        private final LiveViewCheckpointSegmentDirectoryReader reader =
                new LiveViewCheckpointSegmentDirectoryReader(configuration);
        private final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
        private long nextMetaSegmentId = 1_000;

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
                writer.publish(nextMetaSegmentId++, root);
                reader.of(checkpointsDir(dir), root);
            }
        }
    }
}
