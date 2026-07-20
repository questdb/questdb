/*******************************************************************************
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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileUtils;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.lv.LiveViewCheckpointRingManifest;
import io.questdb.cairo.lv.LiveViewCheckpointRingManifestReader;
import io.questdb.cairo.lv.LiveViewCheckpointRingManifestWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Standalone codec coverage for the {@code _checkpoints/_ring} manifest - the
 * durable allow-list of the retained-checkpoint ring.
 * <p>
 * The manifest is what lets a restart resume an O3 replay from the nearest
 * sealed checkpoint instead of rebuilding the view from its START FROM
 * boundary. It is also derived state, so the contract cuts both ways: a valid
 * manifest must round-trip exactly, and anything less than valid must be
 * rejected whole rather than yielding a partial ring. A rejected manifest costs
 * one boundary rebuild; a wrongly accepted one resurrects a checkpoint whose
 * window state predates an already-consumed late row, which is silent wrong
 * data.
 * <p>
 * These tests drive the codec against real files with no refresh worker and no
 * live view: round trips through the writer, and rejection of every structural
 * defect a crash or a version skew can leave behind. The invariant-violation
 * cases write the block raw, bypassing the writer's mirror-image asserts -
 * which is the honest shape anyway, since only a corrupt file or a foreign
 * writer can produce one.
 */
public class LiveViewCheckpointRingManifestTest extends AbstractCairoTest {

    private static final long[] THREE_ENTRIES = {
            // lvSeqTxn, maxTs, baseSeqTxn, lvRowsTotal, stateBytes
            10, 1_000, 10, 500, 4_096,
            20, 2_000, 20, 900, 8_192,
            30, 3_000, 30, 1_400, 16_384
    };
    private static final TableToken TOKEN = new TableToken("lv_ring", "lv_ring~1", null, 1, true, false, false);

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            final FilesFacade ff = configuration.getFilesFacade();
            path.of(configuration.getDbRoot()).concat(TOKEN.getDirName()).concat("_checkpoints").slash();
            ff.mkdirs(path, configuration.getMkDirMode());
            ff.removeQuiet(ringPath(path).$());
        }
    }

    @Test
    public void testBaseSeqTxnDecreasingRejected() throws Exception {
        assertMemoryLeak(() -> {
            writeRawManifest(
                    LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION, 1, 100, 2,
                    10, 1_000, 30, 500, 4_096,
                    20, 2_000, 20, 900, 8_192
            );
            assertRejected("baseSeqTxn decreasing");
        });
    }

    @Test
    public void testChecksumCorruptionRejected() throws Exception {
        assertMemoryLeak(() -> {
            publish(1, 100, THREE_ENTRIES);
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), ringPath(path).$(), MemoryTag.MMAP_DEFAULT);
                // First payload byte of region A: the format version field.
                mem.putInt(BlockFileUtils.HEADER_SIZE + BlockFileUtils.REGION_HEADER_SIZE + 8, -42);
            }
            // The block file layer owns checksums, so this surfaces as its own
            // critical error rather than a manifest-invalid one. Both land in
            // the same conservative fallback; there is deliberately no A/B
            // fallback to the prior region.
            final LiveViewCheckpointRingManifestReader reader = new LiveViewCheckpointRingManifestReader();
            try {
                read(reader);
                Assert.fail("expected a checksum mismatch");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "block file checksum mismatch");
            }
            assertCleared(reader);
        });
    }

    @Test
    public void testEntryCountBelowPayloadRejected() throws Exception {
        assertMemoryLeak(() -> {
            // Declares one entry, carries three: a reader that trusted the count
            // would silently drop two anchors rather than reject the manifest.
            writeRawManifest(LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION, 1, 100, 1, THREE_ENTRIES);
            assertRejected("manifest entry count does not match block length");
        });
    }

    @Test
    public void testEntryCountNegativeRejected() throws Exception {
        assertMemoryLeak(() -> {
            writeRawManifest(LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION, 1, 100, -1, THREE_ENTRIES);
            assertRejected("manifest entry count does not match block length");
        });
    }

    @Test
    public void testEntryCountOverflowRejected() throws Exception {
        assertMemoryLeak(() -> {
            // The length arithmetic must run in long: a count this large times
            // 40 bytes wraps an int back into a plausible payload length.
            writeRawManifest(
                    LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION, 1, 100,
                    Integer.MAX_VALUE, THREE_ENTRIES
            );
            assertRejected("manifest entry count does not match block length");
        });
    }

    @Test
    public void testEntryCountOverPayloadRejected() throws Exception {
        assertMemoryLeak(() -> {
            // Declares three entries, carries one. Reading the declared count
            // would run off the payload into whatever follows it.
            writeRawManifest(
                    LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION, 1, 100, 3,
                    10, 1_000, 10, 500, 4_096
            );
            assertRejected("manifest entry count does not match block length");
        });
    }

    @Test
    public void testFullRingRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            final int entryCount = 16;
            final LongList entries = new LongList();
            for (int i = 1; i <= entryCount; i++) {
                entries.add(i * 10L);      // lvSeqTxn
                entries.add(i * 1_000L);   // maxTs
                entries.add(i * 10L);      // baseSeqTxn
                entries.add(i * 100L);     // lvRowsTotal
                entries.add(i * 4_096L);   // stateBytes
            }
            publish(7, entryCount * 10L, entries);

            final LiveViewCheckpointRingManifestReader reader = read(new LiveViewCheckpointRingManifestReader());
            Assert.assertEquals(7, reader.getGeneration());
            Assert.assertEquals(entryCount * 10L, reader.getCoveredBaseSeqTxn());
            Assert.assertEquals(entryCount, reader.getEntryCount());
            TestUtils.assertEquals(entries, reader.getEntries());
        });
    }

    @Test
    public void testLvSeqTxnAboveCoveredRejected() throws Exception {
        assertMemoryLeak(() -> {
            // An entry keyed above the seqTxn it claims sealedness at is a
            // contradiction: covered is the position every listed entry is
            // proven sealed at.
            writeRawManifest(
                    LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION, 1, 15, 2,
                    10, 1_000, 10, 500, 4_096,
                    20, 2_000, 10, 900, 8_192
            );
            assertRejected("lvSeqTxn above coveredBaseSeqTxn");
        });
    }

    @Test
    public void testLvSeqTxnNotStrictlyIncreasingRejected() throws Exception {
        assertMemoryLeak(() -> {
            writeRawManifest(
                    LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION, 1, 100, 2,
                    10, 1_000, 10, 500, 4_096,
                    10, 2_000, 20, 900, 8_192
            );
            assertRejected("lvSeqTxn not strictly increasing");
        });
    }

    @Test
    public void testManifestBlockNotFound() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path();
                 BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), CommitMode.NOSYNC)) {
                writer.of(ringPath(path).$());
                final AppendableBlock block = writer.append();
                block.putLong(42);
                block.commit(LiveViewCheckpointRingManifest.RING_MANIFEST_BLOCK_TYPE + 1);
                writer.commit();
            }
            assertRejected("manifest block not found");
        });
    }

    @Test
    public void testMaxTsDuplicateRejected() throws Exception {
        assertMemoryLeak(() -> {
            // Equal maxTs breaks the anchor search: two entries would both
            // qualify as the newest checkpoint below a late row.
            writeRawManifest(
                    LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION, 1, 100, 2,
                    10, 1_000, 10, 500, 4_096,
                    20, 1_000, 20, 900, 8_192
            );
            assertRejected("maxTs not strictly increasing");
        });
    }

    @Test
    public void testMaxTsNotStrictlyIncreasingRejected() throws Exception {
        assertMemoryLeak(() -> {
            writeRawManifest(
                    LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION, 1, 100, 2,
                    10, 2_000, 10, 500, 4_096,
                    20, 1_000, 20, 900, 8_192
            );
            assertRejected("maxTs not strictly increasing");
        });
    }

    @Test
    public void testNegativeStateBytesRejected() throws Exception {
        assertMemoryLeak(() -> {
            writeRawManifest(
                    LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION, 1, 100, 1,
                    10, 1_000, 10, 500, -1
            );
            assertRejected("negative stateBytes");
        });
    }

    @Test
    public void testRepublishReplacesMembership() throws Exception {
        assertMemoryLeak(() -> {
            publish(1, 100, THREE_ENTRIES);

            // An O3 retirement drops the two newest entries and publishes the
            // survivors at the new covered position. BlockFileWriter alternates
            // regions, so this exercises the region flip as well as the shrink.
            final LongList survivors = new LongList();
            survivors.add(10);
            survivors.add(1_000);
            survivors.add(10);
            survivors.add(500);
            survivors.add(4_096);
            publish(2, 140, survivors);

            final LiveViewCheckpointRingManifestReader reader = read(new LiveViewCheckpointRingManifestReader());
            Assert.assertEquals(2, reader.getGeneration());
            Assert.assertEquals(140, reader.getCoveredBaseSeqTxn());
            Assert.assertEquals(1, reader.getEntryCount());
            Assert.assertEquals(10, reader.getEntryLvSeqTxn(0));
            Assert.assertEquals(1_000, reader.getEntryMaxTs(0));
        });
    }

    @Test
    public void testRingManifestPathIsUnderCheckpointsDir() throws Exception {
        assertMemoryLeak(() -> {
            // The name must not end in .cp: the startup sweep unlinks .cp files
            // it does not recognise and leaves everything else alone, so the
            // manifest survives only by staying out of that namespace.
            try (Path path = new Path(); Path liveViewDir = new Path()) {
                liveViewDir.of(configuration.getDbRoot()).concat(TOKEN.getDirName());
                LiveViewCheckpointRingManifest.ringManifestPath(path, liveViewDir);
                TestUtils.assertContains(path.toString(), "_checkpoints");
                Assert.assertTrue(path.toString().endsWith("_ring"));
            }
        });
    }

    @Test
    public void testRoundTripEmptyRing() throws Exception {
        assertMemoryLeak(() -> {
            // A full boundary rebuild retires the whole ring, so an empty
            // manifest at a live covered position is a normal publication, not
            // a degenerate one.
            publish(3, 900, new LongList());

            final LiveViewCheckpointRingManifestReader reader = read(new LiveViewCheckpointRingManifestReader());
            Assert.assertEquals(3, reader.getGeneration());
            Assert.assertEquals(900, reader.getCoveredBaseSeqTxn());
            Assert.assertEquals(0, reader.getEntryCount());
            Assert.assertEquals(0, reader.getEntries().size());
        });
    }

    @Test
    public void testRoundTripSingleEntry() throws Exception {
        assertMemoryLeak(() -> {
            final LongList entries = new LongList();
            entries.add(10);
            entries.add(1_000);
            entries.add(9);
            entries.add(500);
            entries.add(4_096);
            publish(1, 12, entries);

            final LiveViewCheckpointRingManifestReader reader = read(new LiveViewCheckpointRingManifestReader());
            Assert.assertEquals(1, reader.getGeneration());
            Assert.assertEquals(12, reader.getCoveredBaseSeqTxn());
            Assert.assertEquals(1, reader.getEntryCount());
            Assert.assertEquals(10, reader.getEntryLvSeqTxn(0));
            Assert.assertEquals(1_000, reader.getEntryMaxTs(0));
            Assert.assertEquals(9, reader.getEntryBaseSeqTxn(0));
            Assert.assertEquals(500, reader.getEntryLvRowsTotal(0));
            Assert.assertEquals(4_096, reader.getEntryStateBytes(0));
        });
    }

    @Test
    public void testShortBlockRejected() throws Exception {
        assertMemoryLeak(() -> {
            // A payload shorter than the fixed header must be caught before any
            // field read: block getters are raw offsets into the mapped region,
            // so the alternative is reading a neighbour's bytes as our own.
            try (Path path = new Path();
                 BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), CommitMode.NOSYNC)) {
                writer.of(ringPath(path).$());
                final AppendableBlock block = writer.append();
                block.putInt(LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION);
                block.commit(LiveViewCheckpointRingManifest.RING_MANIFEST_BLOCK_TYPE);
                writer.commit();
            }
            assertRejected("manifest block too short");
        });
    }

    @Test
    public void testStateBytesSumOverflowRejected() throws Exception {
        assertMemoryLeak(() -> {
            writeRawManifest(
                    LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION, 1, 100, 2,
                    10, 1_000, 10, 500, Long.MAX_VALUE,
                    20, 2_000, 20, 900, Long.MAX_VALUE
            );
            assertRejected("stateBytes sum overflow");
        });
    }

    @Test
    public void testTruncatedFileRejected() throws Exception {
        assertMemoryLeak(() -> {
            publish(1, 100, THREE_ENTRIES);
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                ringPath(path);
                final long fd = ff.openRW(path.$(), configuration.getWriterFileOpenOpts());
                Assert.assertTrue(fd > -1);
                try {
                    // Lop off the tail of the region. getCursor() re-extends the
                    // mapping with zeroes, so the truncation surfaces as a
                    // checksum mismatch rather than a short read.
                    Assert.assertTrue(ff.truncate(fd, BlockFileUtils.HEADER_SIZE + BlockFileUtils.REGION_HEADER_SIZE));
                } finally {
                    ff.close(fd);
                }
            }
            final LiveViewCheckpointRingManifestReader reader = new LiveViewCheckpointRingManifestReader();
            try {
                read(reader);
                Assert.fail("expected a torn manifest to be rejected");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "checksum mismatch");
            }
            assertCleared(reader);
        });
    }

    @Test
    public void testVersionSkewRejected() throws Exception {
        assertMemoryLeak(() -> {
            // A newer build's manifest is ignored, never a reason to invalidate
            // the view: ring state is derived, so an older build falls back to
            // the highest .cp and rebuilds the ring as it goes.
            writeRawManifest(
                    LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION + 1, 1, 100, 3, THREE_ENTRIES
            );
            assertRejected("manifest format version not supported");
        });
    }

    @Test
    public void testVersionBelowFloorRejected() throws Exception {
        // m10: version 1 is the first format; a below-floor version (0 / negative) is a zeroed
        // or torn header, not a legacy v1. The guard now rejects it rather than parsing it as v1
        // (which would matter the moment the format version bumps past 1).
        assertMemoryLeak(() -> {
            writeRawManifest(0, 1, 100, 3, THREE_ENTRIES);
            assertRejected("manifest format version not supported");
        });
    }

    private static void assertCleared(LiveViewCheckpointRingManifestReader reader) {
        Assert.assertEquals(Numbers.LONG_NULL, reader.getCoveredBaseSeqTxn());
        Assert.assertEquals(Numbers.LONG_NULL, reader.getGeneration());
        Assert.assertEquals(0, reader.getEntryCount());
    }

    private static Path ringPath(Path path) {
        try (Path liveViewDir = new Path()) {
            liveViewDir.of(configuration.getDbRoot()).concat(TOKEN.getDirName());
            return LiveViewCheckpointRingManifest.ringManifestPath(path, liveViewDir);
        }
    }

    private void assertRejected(String expectedReason) {
        final LiveViewCheckpointRingManifestReader reader = new LiveViewCheckpointRingManifestReader();
        try {
            read(reader);
            Assert.fail("expected the manifest to be rejected: " + expectedReason);
        } catch (CairoException e) {
            Assert.assertEquals(CairoException.LV_CHECKPOINT_RING_MANIFEST_INVALID, e.getErrno());
            TestUtils.assertContains(e.getFlyweightMessage(), expectedReason);
        }
        // A rejected manifest must leave no candidate behind: a half-populated
        // reader would be a ring nothing on disk backs.
        assertCleared(reader);
    }

    private void publish(long generation, long coveredBaseSeqTxn, LongList entries) {
        try (Path liveViewDir = new Path();
             LiveViewCheckpointRingManifestWriter writer = new LiveViewCheckpointRingManifestWriter(configuration)) {
            liveViewDir.of(configuration.getDbRoot()).concat(TOKEN.getDirName());
            writer.publish(liveViewDir, generation, coveredBaseSeqTxn, entries);
        }
    }

    private void publish(long generation, long coveredBaseSeqTxn, long[] entries) {
        final LongList list = new LongList();
        for (long entry : entries) {
            list.add(entry);
        }
        publish(generation, coveredBaseSeqTxn, list);
    }

    private LiveViewCheckpointRingManifestReader read(LiveViewCheckpointRingManifestReader manifestReader) {
        try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
            reader.of(ringPath(path).$());
            manifestReader.of(reader, TOKEN);
        }
        // Read the fields back only after both the block file and its mapping
        // are gone: the reader must own copies, not flyweights into the region.
        return manifestReader;
    }

    private void writeRawManifest(
            int formatVersion,
            long generation,
            long coveredBaseSeqTxn,
            int declaredEntryCount,
            long... entries
    ) {
        // Deliberately bypasses LiveViewCheckpointRingManifest.append(), whose
        // asserts mirror the reader's validation. Only a corrupt file or a
        // foreign writer produces these payloads.
        try (Path path = new Path();
             BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), CommitMode.NOSYNC)) {
            writer.of(ringPath(path).$());
            final AppendableBlock block = writer.append();
            block.putInt(formatVersion);
            block.putLong(generation);
            block.putLong(coveredBaseSeqTxn);
            block.putInt(declaredEntryCount);
            for (long entry : entries) {
                block.putLong(entry);
            }
            block.commit(LiveViewCheckpointRingManifest.RING_MANIFEST_BLOCK_TYPE);
            writer.commit();
        }
    }
}
