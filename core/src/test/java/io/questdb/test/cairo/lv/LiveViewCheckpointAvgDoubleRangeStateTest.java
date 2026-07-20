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
import io.questdb.cairo.lv.LiveViewCheckpointAvgDoubleRangeStateBuilder;
import io.questdb.cairo.lv.LiveViewCheckpointAvgDoubleRangeStateReader;
import io.questdb.cairo.lv.LiveViewCheckpointDataSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapEntry;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LiveViewCheckpointAvgDoubleRangeStateTest extends AbstractCairoTest {

    private static final byte[] KEY = new byte[]{1, 2, 3};
    private static final String LV_DIR = "lv_avg_range_chunks";

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            checkpointsDir(path).concat(LiveViewCheckpointLayout.DATA_DIR_NAME).slash();
            configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testCopyOnWriteTailAndPartialSharedHeadSurviveRestart() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
            final LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry();
            try (LiveViewCheckpointSegmentDirectory directory = new LiveViewCheckpointSegmentDirectory(configuration)) {
                writeInitial(first, directory, 1, 4_106);

                try (LiveViewCheckpointAvgDoubleRangeStateBuilder builder = new LiveViewCheckpointAvgDoubleRangeStateBuilder(configuration);
                     LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                     Path dir = new Path()) {
                    builder.of(checkpointsDir(dir), directory, first);
                    builder.dropHeadRows(5);
                    writer.of(checkpointsDir(dir), 2);
                    builder.append(writer, 4_106_000, 10_000.0);
                    builder.append(writer, 4_107_000, -0.0);
                    builder.append(writer, 4_108_000, 10_002.0);
                    builder.freeze(writer, KEY, -0.0, 4_104, second);
                    directory.addSegment(2, writer.commit(), 1);
                }

                Assert.assertEquals(4, first.getStatePageCount());
                Assert.assertEquals(4, second.getStatePageCount());
                assertRefEquals(first.getStatePageRef(0), second.getStatePageRef(0));
                assertRefEquals(first.getStatePageRef(1), second.getStatePageRef(1));
                Assert.assertEquals(1, first.getStatePageRef(2).getSegmentId());
                Assert.assertEquals(2, second.getStatePageRef(2).getSegmentId());

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
                try (LiveViewCheckpointAvgDoubleRangeStateReader reader = new LiveViewCheckpointAvgDoubleRangeStateReader(configuration);
                     Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), directory, second);
                    Assert.assertEquals(5, reader.getHeadOffset());
                    Assert.assertEquals(4_104, reader.getRowCount());
                    Assert.assertEquals(
                            Double.doubleToRawLongBits(-0.0),
                            Double.doubleToRawLongBits(reader.getSum())
                    );
                }
            }
        });
    }

    @Test
    public void testMalformedMetadataAndDataAreRejected() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPartitionMapEntry valid = new LiveViewCheckpointPartitionMapEntry();
            try (LiveViewCheckpointSegmentDirectory directory = new LiveViewCheckpointSegmentDirectory(configuration)) {
                writeInitial(valid, directory, 30, 3);

                final byte[] shortScalar = Arrays.copyOf(
                        valid.getScalarState(),
                        LiveViewCheckpointAvgDoubleRangeStateReader.SCALAR_STATE_BYTES - 1
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
                        timestampRef.getDecodedLength(), LiveViewCheckpointAvgDoubleRangeStateReader.VALUE_PAGE_KIND,
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
                final long fileLength = directory.getFileLength(30);
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
            try (LiveViewCheckpointSegmentDirectory directory = new LiveViewCheckpointSegmentDirectory(configuration)) {
                LiveViewCheckpointPartitionMapEntry previous = new LiveViewCheckpointPartitionMapEntry();
                for (int generation = 0; generation < 40; generation++) {
                    final LiveViewCheckpointPartitionMapEntry next = new LiveViewCheckpointPartitionMapEntry();
                    final int drop = timestamps.size() == 0 ? 0 : rnd.nextInt(timestamps.size() / 3 + 1);
                    for (int i = 0; i < drop; i++) {
                        timestamps.removeIndex(0);
                        values.removeIndex(0);
                    }
                    final int append = 1 + rnd.nextInt(300);
                    try (LiveViewCheckpointAvgDoubleRangeStateBuilder builder = new LiveViewCheckpointAvgDoubleRangeStateBuilder(configuration);
                         LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                         Path dir = new Path()) {
                        if (generation == 0) {
                            builder.ofEmpty();
                        } else {
                            builder.of(checkpointsDir(dir), directory, previous);
                            builder.dropHeadRows(drop);
                        }
                        writer.of(checkpointsDir(dir), 100 + generation);
                        for (int i = 0; i < append; i++) {
                            nextTimestamp += rnd.nextInt(4);
                            final double value = rnd.nextDouble() * 10_000.0 - 5_000.0;
                            builder.append(writer, nextTimestamp, value);
                            timestamps.add(nextTimestamp);
                            values.add(Double.doubleToRawLongBits(value));
                        }
                        builder.freeze(writer, KEY, generation + 0.125, timestamps.size(), next);
                        directory.addSegment(100 + generation, writer.commit(), 1);
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
            try (LiveViewCheckpointSegmentDirectory directory = new LiveViewCheckpointSegmentDirectory(configuration)) {
                writeInitial(first, directory, 20, LiveViewCheckpointStateCodec.CHUNK_ROWS + 12);
                try (LiveViewCheckpointAvgDoubleRangeStateBuilder builder = new LiveViewCheckpointAvgDoubleRangeStateBuilder(configuration);
                     LiveViewCheckpointDataSegmentWriter unopenedWriter = new LiveViewCheckpointDataSegmentWriter(configuration);
                     Path dir = new Path()) {
                    builder.of(checkpointsDir(dir), directory, first);
                    builder.dropHeadRows(LiveViewCheckpointStateCodec.CHUNK_ROWS + 5L);
                    builder.freeze(unopenedWriter, KEY, 1.25, 7, second);
                }
                Assert.assertEquals(2, second.getStatePageCount());
                assertRefEquals(first.getStatePageRef(2), second.getStatePageRef(0));
                assertRefEquals(first.getStatePageRef(3), second.getStatePageRef(1));
                try (LiveViewCheckpointAvgDoubleRangeStateReader reader = new LiveViewCheckpointAvgDoubleRangeStateReader(configuration);
                     Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), directory, second);
                    Assert.assertEquals(5, reader.getHeadOffset());
                    Assert.assertEquals(7, reader.getRowCount());
                }
            }
        });
    }

    private static void assertInvalid(
            LiveViewCheckpointPartitionMapEntry entry,
            LiveViewCheckpointSegmentDirectory directory,
            boolean readPayload,
            CharSequence message
    ) {
        try (LiveViewCheckpointAvgDoubleRangeStateReader reader = new LiveViewCheckpointAvgDoubleRangeStateReader(configuration);
             Path dir = new Path()) {
            try {
                reader.of(checkpointsDir(dir), directory, entry);
                if (readPayload) {
                    reader.forEach((timestamp, value) -> {
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
            LiveViewCheckpointSegmentDirectory directory,
            LongList expectedTimestamps,
            LongList expectedValues
    ) {
        try (LiveViewCheckpointAvgDoubleRangeStateReader reader = new LiveViewCheckpointAvgDoubleRangeStateReader(configuration);
             Path dir = new Path()) {
            reader.of(checkpointsDir(dir), directory, entry);
            Assert.assertEquals(expectedTimestamps.size(), reader.getRowCount());
            final int[] index = {0};
            reader.forEach((timestamp, value) -> {
                final int i = index[0]++;
                Assert.assertEquals(expectedTimestamps.getQuick(i), timestamp);
                Assert.assertEquals(expectedValues.getQuick(i), Double.doubleToRawLongBits(value));
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
            LiveViewCheckpointSegmentDirectory directory,
            long segmentId,
            int rows
    ) {
        try (LiveViewCheckpointAvgDoubleRangeStateBuilder builder = new LiveViewCheckpointAvgDoubleRangeStateBuilder(configuration);
             LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
             Path dir = new Path()) {
            builder.ofEmpty();
            writer.of(checkpointsDir(dir), segmentId);
            for (int i = 0; i < rows; i++) {
                builder.append(writer, i * 1_000L, i + 0.25);
            }
            builder.freeze(writer, KEY, 42.5, rows, out);
            directory.addSegment(segmentId, writer.commit(), 1);
        }
    }
}
