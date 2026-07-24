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
import io.questdb.cairo.lv.LiveViewCheckpointPageCache;
import io.questdb.cairo.lv.LiveViewCheckpointPageCacheBudget;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapEntry;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryWriter;
import io.questdb.cairo.lv.LiveViewCheckpointStateCodec;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.std.Decimals;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LiveViewCheckpointRangeRingStateTest extends AbstractCairoTest {

    // Shape of the ring the page cache differential walks: one chunk per boundary,
    // each in its own data segment, and enough of them that a half-admitting cache
    // reliably ends up holding some chunks and not others.
    private static final int CACHE_RING_BOUNDARIES = 24;
    private static final int CACHE_RING_HEAD_DROP = 3;
    private static final int CACHE_RING_ROWS_PER_BOUNDARY = 5;
    // Rows per boundary of the ring the self-tuning differential walks. Wide enough
    // that a chunk's pages are worth several slots each, so a cap below the working
    // set is a cap the budget can actually express.
    private static final int CACHE_RING_WIDE_ROWS_PER_BOUNDARY = 2048;
    private static final String DATA_SEGMENT_PATH_FRAGMENT =
            LiveViewCheckpointLayout.DATA_DIR_NAME + Files.SEPARATOR + LiveViewCheckpointLayout.DATA_SEGMENT_PREFIX;
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
                    builder.of(first, LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
                    builder.dropHeadRows(5);
                    writer.of(checkpointsDir(dir), 2);
                    builder.append(writer, 4_106_000, Double.doubleToRawLongBits(10_000.0));
                    builder.append(writer, 4_107_000, Double.doubleToRawLongBits(-0.0));
                    builder.append(writer, 4_108_000, Double.doubleToRawLongBits(10_002.0));
                    builder.freeze(writer, KEY, Double.doubleToRawLongBits(-0.0), 0, 0, 0, 4_104, second);
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
            try (Catalogue directory = new Catalogue()) {
                LiveViewCheckpointPartitionMapEntry root = null;
                for (int boundary = 0; boundary < boundaries; boundary++) {
                    final long segmentId = boundary + 1;
                    final LiveViewCheckpointPartitionMapEntry sealed = new LiveViewCheckpointPartitionMapEntry();
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
                        builder.freeze(writer, KEY, 0L, 0, 0, 0, timestamps.size(), sealed);
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
    public void testCachedPagesWalkExactlyAsDecodedPagesDo() throws Exception {
        // The anti-corruption test for the decoded page cache: a page the cache
        // serves must produce the same row run the codec would have produced from
        // the segment, for every value width and with a non-zero head offset. The
        // walk with no cache is the oracle; a cache that holds everything, and one
        // that holds a hash-selected half of it, must both reproduce it verbatim.
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
            assertCacheWalksIdentically(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1, true, 200, dataSegmentOpens);
            assertCacheWalksIdentically(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_LONG, 1, false, 240, dataSegmentOpens);
            assertCacheWalksIdentically(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DEQUE_DOUBLE, 1, true, 280, dataSegmentOpens);
            assertCacheWalksIdentically(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL128, 2, false, 320, dataSegmentOpens);
            assertCacheWalksIdentically(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL256, 4, false, 360, dataSegmentOpens);
            // count's ring is timestamps alone, so a chunk is one page rather than two.
            assertCacheWalksIdentically(
                    LiveViewCheckpointRangeRingStateReader.VALUE_KIND_NONE, 0, false, 400, dataSegmentOpens);
        });
    }

    @Test
    public void testSelfTunedCacheUnderACapBelowTheWorkingSetWalksIdentically() throws Exception {
        // What the admission fraction is for: a cap the ring does not fit into must
        // leave the cache serving the share of the ring the cap covers, restore
        // after restore, and every row it serves must be the row the codec would
        // have produced. A wide ring, so the working set is worth several slabs and
        // the cap can sit under it.
        assertMemoryLeak(() -> {
            try (Catalogue directory = new Catalogue()) {
                final LiveViewCheckpointPartitionMapEntry root = writeManyChunkRing(
                        directory,
                        LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE,
                        1,
                        true,
                        600,
                        CACHE_RING_WIDE_ROWS_PER_BOUNDARY
                );
                final int pages = root.getStatePageCount();
                final LongList decoded = new LongList();
                walkRing(null, directory, root, 1, decoded);

                final LongList cached = new LongList();
                final long workingSet;
                final LiveViewCheckpointPageCacheBudget generous =
                        new LiveViewCheckpointPageCacheBudget(64L * LiveViewCheckpointPageCache.SLAB_BYTES);
                try (LiveViewCheckpointPageCache cache = new LiveViewCheckpointPageCache(generous)) {
                    // A cap the ring fits into leaves the fraction where it started.
                    walkRing(cache, directory, root, 1, cached);
                    assertRingWalkEquals(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, decoded, cached);
                    Assert.assertEquals(1.0, cache.getAdmissionFraction(), 0.0);
                    Assert.assertEquals(pages, cache.getPageCount());
                    workingSet = cache.getWorkingSetBytes();
                    Assert.assertEquals(cache.getUsedBytes(), workingSet);
                }
                Assert.assertEquals(0, generous.getUsedBytes());

                // A third of it. The first restore fills the budget before it has
                // measured anything, and the fraction it settles on afterwards is
                // the share it can hold.
                final long capacity = workingSet / 3;
                final LiveViewCheckpointPageCacheBudget tight = new LiveViewCheckpointPageCacheBudget(capacity);
                try (LiveViewCheckpointPageCache cache = new LiveViewCheckpointPageCache(tight)) {
                    walkRing(cache, directory, root, 1, cached);
                    assertRingWalkEquals(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, decoded, cached);
                    Assert.assertEquals(workingSet, cache.getWorkingSetBytes());
                    Assert.assertEquals(1 / 3.0, cache.getAdmissionFraction(), 1e-6);
                    Assert.assertEquals(capacity, cache.getUsedBytes());
                    Assert.assertEquals(capacity, tight.getUsedBytes());
                    final int held = cache.getPageCount();
                    Assert.assertTrue("the cap held nothing [pages=" + held + ']', held > 0);
                    Assert.assertTrue("the cap held everything [pages=" + held + ']', held < pages);

                    // Every later restore serves exactly what the cache holds: the
                    // subset does not move, so the hit rate is the share of the ring
                    // that fits rather than the nothing an LRU would leave.
                    for (int pass = 0; pass < 3; pass++) {
                        final long hitsBefore = cache.getHits();
                        walkRing(cache, directory, root, 1, cached);
                        assertRingWalkEquals(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, decoded, cached);
                        Assert.assertEquals(held, cache.getHits() - hitsBefore);
                        Assert.assertEquals(held, cache.getPageCount());
                        Assert.assertEquals(capacity, tight.getUsedBytes());
                    }

                    // With slots free and a fraction below one, the hash is what
                    // decides which pages come back - and the walk still matches.
                    cache.bumpEpoch();
                    Assert.assertEquals(0, cache.getPageCount());
                    walkRing(cache, directory, root, 1, cached);
                    assertRingWalkEquals(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, decoded, cached);
                    final int hashHeld = cache.getPageCount();
                    Assert.assertTrue("the hash admitted nothing [pages=" + hashHeld + ']', hashHeld > 0);
                    Assert.assertTrue("the hash admitted past the cap [pages=" + hashHeld + ']', hashHeld <= held);

                    final long hitsBefore = cache.getHits();
                    walkRing(cache, directory, root, 1, cached);
                    assertRingWalkEquals(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, decoded, cached);
                    Assert.assertEquals(hashHeld, cache.getHits() - hitsBefore);
                }
                Assert.assertEquals(0, tight.getUsedBytes());
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
                    builder.ofEmpty(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_LONG, 1);
                    writer.of(checkpointsDir(dir), 7);
                    for (int i = 0; i < payload.length; i++) {
                        builder.append(writer, i * 1_000L, payload[i]);
                    }
                    builder.freeze(writer, KEY, 0L, 0, 0, 0, payload.length, root);
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
                        LiveViewCheckpointRangeRingStateReader.scalarStateBytes(1) - 1
                );
                assertInvalid(entry(shortScalar, refs(valid)), directory, false, "scalar state size mismatch");

                final byte[] badVersion = Arrays.copyOf(valid.getScalarState(), valid.getScalarState().length);
                badVersion[0] = 3;
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
                        builder.freeze(writer, KEY, Double.doubleToRawLongBits(generation + 0.125), 0, 0, 0, timestamps.size(), next);
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
            final LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
            final LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry();
            final LongList firstTimestamps = new LongList();
            final long[] secondSegmentBytes = new long[1];
            try (Catalogue directory = new Catalogue()) {
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
                    builder.freeze(writer, KEY, 0L, 0, 0, 0, initialRows, first);
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
                    builder.freeze(writer, KEY, 0L, 0, 0, 0, secondTimestamps.size(), second);
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
            final LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
            final LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry();
            try (Catalogue directory = new Catalogue()) {
                writeInitial(first, directory, 20, LiveViewCheckpointStateCodec.CHUNK_ROWS + 12);
                try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                     LiveViewCheckpointDataSegmentWriter unopenedWriter = new LiveViewCheckpointDataSegmentWriter(configuration);
                     Path dir = new Path()) {
                    builder.of(first, LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
                    builder.dropHeadRows(LiveViewCheckpointStateCodec.CHUNK_ROWS + 5L);
                    builder.freeze(unopenedWriter, KEY, Double.doubleToRawLongBits(1.25), 0, 0, 0, 7, second);
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
     * Walks a many-chunk ring of {@code valueKind} with no cache, with a cache that
     * admits everything and with one that admits half, and asserts the three runs
     * are identical. Also asserts the cached walk really did take the cache: it
     * maps no data segment at all, which is only possible when every page came back
     * from a probe.
     */
    private static void assertCacheWalksIdentically(
            int valueKind,
            int words,
            boolean doubleColumn,
            long firstSegment,
            int[] dataSegmentOpens
    ) {
        try (Catalogue directory = new Catalogue()) {
            final LiveViewCheckpointPartitionMapEntry root = writeManyChunkRing(
                    directory, valueKind, words, doubleColumn, firstSegment, CACHE_RING_ROWS_PER_BOUNDARY);
            final int pagesPerChunk = words == 0 ? 1 : 2;
            Assert.assertEquals(pagesPerChunk * CACHE_RING_BOUNDARIES, root.getStatePageCount());

            final LongList decoded = new LongList();
            walkRing(null, directory, root, words, decoded);
            Assert.assertTrue("the ring must carry rows to compare", decoded.size() > 0);

            final LiveViewCheckpointPageCacheBudget budget =
                    new LiveViewCheckpointPageCacheBudget(1024L * LiveViewCheckpointPageCache.SLAB_BYTES);
            final LongList cached = new LongList();
            try (LiveViewCheckpointPageCache cache = new LiveViewCheckpointPageCache(budget)) {
                // Cold: every page is decoded off the segment and offered to the cache.
                dataSegmentOpens[0] = 0;
                walkRing(cache, directory, root, words, cached);
                assertRingWalkEquals(valueKind, decoded, cached);
                Assert.assertEquals(0, cache.getHits());
                Assert.assertEquals(root.getStatePageCount(), cache.getMisses());
                Assert.assertEquals(root.getStatePageCount(), cache.getPageCount());
                Assert.assertEquals(CACHE_RING_BOUNDARIES, dataSegmentOpens[0]);

                // Warm: the same run, and not one segment mapped to produce it.
                dataSegmentOpens[0] = 0;
                walkRing(cache, directory, root, words, cached);
                assertRingWalkEquals(valueKind, decoded, cached);
                Assert.assertEquals(root.getStatePageCount(), cache.getHits());
                Assert.assertEquals(root.getStatePageCount(), cache.getMisses());
                Assert.assertEquals(0, dataSegmentOpens[0]);
            }
            Assert.assertEquals(0, budget.getUsedBytes());

            // Half the pages pinned by the admission hash: every walk mixes cached
            // chunks with decoded ones, in whatever order the ring lays them out.
            try (LiveViewCheckpointPageCache cache = new LiveViewCheckpointPageCache(budget)) {
                cache.setAdmissionFraction(0.5);
                for (int pass = 0; pass < 3; pass++) {
                    walkRing(cache, directory, root, words, cached);
                    assertRingWalkEquals(valueKind, decoded, cached);
                }
                Assert.assertTrue(
                        "a half-admitted cache served nothing [valueKind=" + valueKind + ']',
                        cache.getHits() > 0
                );
                Assert.assertTrue(
                        "a half-admitted cache admitted everything [valueKind=" + valueKind
                                + ", pages=" + cache.getPageCount() + ']',
                        cache.getPageCount() < root.getStatePageCount()
                );
            }
            Assert.assertEquals(0, budget.getUsedBytes());
        }
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
                builder.freeze(writer, KEY, 0L, 0, 0, 0, initialRows, first);
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
                builder.freeze(writer, KEY, 0L, 0, 0, 0, secondTimestamps.size(), second);
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

    private static void assertRingWalkEquals(int valueKind, LongList expected, LongList actual) {
        Assert.assertEquals(
                "the cached walk produced a different number of words [valueKind=" + valueKind + ']',
                expected.size(),
                actual.size()
        );
        for (int i = 0, n = expected.size(); i < n; i++) {
            if (expected.getQuick(i) != actual.getQuick(i)) {
                Assert.fail("the cached walk diverged at word " + i + " [valueKind=" + valueKind
                        + ", expected=" + expected.getQuick(i)
                        + ", actual=" + actual.getQuick(i) + ']');
            }
        }
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
        final LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
        final LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry();
        final LongList firstTimestamps = new LongList();
        final LongList firstWords = new LongList();
        try (Catalogue directory = new Catalogue()) {
            try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                 LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                 Path dir = new Path()) {
                builder.ofEmpty(valueKind, words);
                writer.of(checkpointsDir(dir), firstSegment);
                for (int i = 0; i < initialRows; i++) {
                    final long ts = i * 1_000L;
                    appendWideRow(builder, writer, ts, initialRows - i, words, firstTimestamps, firstWords);
                }
                builder.freeze(writer, KEY, 1, 2, 3, 4, initialRows, first);
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
                builder.freeze(writer, KEY, 5, 6, 7, 8, secondTimestamps.size(), second);
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

    /**
     * Appends one row of whatever width {@code words} names, so one builder loop
     * drives every value kind. The payload only has to be deterministic and varied:
     * the walk with no cache is what the cached walks are compared against.
     */
    private static void appendRingRow(
            LiveViewCheckpointRangeRingStateBuilder builder,
            LiveViewCheckpointDataSegmentWriter writer,
            long timestamp,
            long seed,
            int words,
            boolean doubleColumn
    ) {
        switch (words) {
            case 0 -> builder.append(writer, timestamp);
            case 1 -> builder.append(writer, timestamp, doubleColumn ? Double.doubleToRawLongBits(seed + 0.25) : seed);
            case 2 -> builder.append(writer, timestamp, seed, ~seed);
            default -> builder.append(writer, timestamp, seed, ~seed, seed * 31, Long.MIN_VALUE + seed);
        }
    }

    /**
     * Reads the whole ring through a reader bound to {@code cache} - null for the
     * decode-everything oracle - and records the consumer callbacks verbatim, one
     * timestamp followed by its value words per row.
     * <p>
     * The reader is built per walk, so its own segment mappings never carry over:
     * whatever a second walk saves is the page cache's doing and nothing else's.
     * <p>
     * One walk stands for one restore, which is what the cache measures its working
     * set over: a restore of a real view walks this partition and then the next,
     * and a partition is the smallest thing a test can drive.
     */
    private static void walkRing(
            LiveViewCheckpointPageCache cache,
            Catalogue directory,
            LiveViewCheckpointPartitionMapEntry entry,
            int words,
            LongList out
    ) {
        out.clear();
        if (cache != null) {
            cache.beginRestore();
        }
        try (LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
             Path dir = new Path()) {
            reader.of(checkpointsDir(dir), directory.reader, entry, cache);
            switch (words) {
                case 0 -> reader.forEachTimestamp(out::add);
                case 1 -> reader.forEachRow((timestamp, valueBits) -> {
                    out.add(timestamp);
                    out.add(valueBits);
                });
                case 2 -> reader.forEachRow((timestamp, hi, lo) -> {
                    out.add(timestamp);
                    out.add(hi);
                    out.add(lo);
                });
                default -> reader.forEachRow((timestamp, hh, hl, lh, ll) -> {
                    out.add(timestamp);
                    out.add(hh);
                    out.add(hl);
                    out.add(lh);
                    out.add(ll);
                });
            }
        }
        if (cache != null) {
            cache.endRestore();
        }
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
            builder.ofEmpty(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1);
            writer.of(checkpointsDir(dir), segmentId);
            for (int i = 0; i < rows; i++) {
                builder.append(writer, i * 1_000L, Double.doubleToRawLongBits(i + 0.25));
            }
            builder.freeze(writer, KEY, Double.doubleToRawLongBits(42.5), 0, 0, 0, rows, out);
            directory.addSegment(segmentId, writer.commit());
        }
    }

    /**
     * Seals one chunk per boundary, each into a data segment of its own, and
     * expires three rows off the head at the last one. That is the shape a restore
     * meets: a ring spread over as many segments as boundaries it survived, read
     * from a head offset inside its first chunk.
     *
     * @return the newest root
     */
    private static LiveViewCheckpointPartitionMapEntry writeManyChunkRing(
            Catalogue directory,
            int valueKind,
            int words,
            boolean doubleColumn,
            long firstSegment,
            int rowsPerBoundary
    ) {
        final int scalarWords = Math.max(1, words);
        LiveViewCheckpointPartitionMapEntry root = null;
        long liveRows = 0;
        for (int boundary = 0; boundary < CACHE_RING_BOUNDARIES; boundary++) {
            final long segmentId = firstSegment + boundary;
            final LiveViewCheckpointPartitionMapEntry sealed = new LiveViewCheckpointPartitionMapEntry();
            try (LiveViewCheckpointRangeRingStateBuilder builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
                 LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                 Path dir = new Path()) {
                if (root == null) {
                    builder.ofEmpty(valueKind, scalarWords);
                } else {
                    builder.of(root, valueKind, scalarWords);
                }
                if (boundary == CACHE_RING_BOUNDARIES - 1) {
                    builder.dropHeadRows(CACHE_RING_HEAD_DROP);
                    liveRows -= CACHE_RING_HEAD_DROP;
                }
                writer.of(checkpointsDir(dir), segmentId);
                for (int i = 0; i < rowsPerBoundary; i++) {
                    final long row = (long) boundary * rowsPerBoundary + i;
                    appendRingRow(builder, writer, row * 1_000L, row + 1, words, doubleColumn);
                    liveRows++;
                }
                builder.freeze(writer, KEY, 1, 2, 3, 4, liveRows, sealed);
                directory.addSegment(segmentId, writer.commit());
            }
            root = sealed;
        }
        return root;
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
