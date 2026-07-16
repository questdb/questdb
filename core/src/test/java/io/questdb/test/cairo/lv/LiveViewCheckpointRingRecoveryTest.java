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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.lv.LiveViewCheckpointManifest;
import io.questdb.cairo.lv.LiveViewCheckpointRingCandidate;
import io.questdb.cairo.lv.LiveViewCheckpointRingManifest;
import io.questdb.cairo.lv.LiveViewCheckpointRingManifestReader;
import io.questdb.cairo.lv.LiveViewCheckpointRingManifestWriter;
import io.questdb.cairo.lv.LiveViewCheckpointWriter;
import io.questdb.cairo.lv.LiveViewRecovery;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Startup recovery's half of the {@code _checkpoints/_ring} contract: reading a
 * manifest into a candidate, and sweeping {@code _checkpoints/} with that
 * candidate as an allow-list.
 * <p>
 * The sweep's job here is narrow and worth stating, because it is easy to
 * over-read. It <b>makes no trust decision</b>. Trust compares the manifest's
 * {@code coveredBaseSeqTxn} against the <em>reconciled</em> applied floor, and
 * that floor does not exist on this thread - the sweep runs inside
 * {@code CairoEngine.buildViewGraphs()} before any refresh worker, so all it can
 * see is the raw {@code _lv.s}, which trails the view's real durable position by
 * design. What the sweep owes the worker is simply that the files are still
 * there to rehydrate.
 * <p>
 * Hence the two properties these tests pin. A listed {@code .cp} survives both
 * retirement rules, because both would otherwise destroy the ring on
 * appearances alone: the orphan gate reads a stale watermark, and every entry
 * below the head is "older than the highest survivor" by construction. But a
 * listed {@code .cp} never becomes the fallback head, because the fallback is
 * what runs when the manifest turns out <em>not</em> to be trusted, and only the
 * trust decision separates a stale-{@code _lv.s} false positive from a genuine
 * orphan whose commit never landed. Restoring the latter as a head would walk
 * the applied watermark up over base commits the LV table never materialised.
 */
public class LiveViewCheckpointRingRecoveryTest extends AbstractCairoTest {

    private static final long[] THREE_ENTRIES = {
            // lvSeqTxn, maxTs, baseSeqTxn, lvRowsTotal, stateBytes
            10, 1_000, 10, 500, 4_096,
            20, 2_000, 20, 900, 8_192,
            30, 3_000, 30, 1_400, 16_384
    };
    private static final TableToken TOKEN = new TableToken("lv_ring_recovery", "lv_ring_recovery~1", null, 1, true, false, false);

    @Before
    public void setUp() {
        super.setUp();
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path liveViewDir = liveViewDir(); Path checkpointsDir = new Path()) {
            ff.mkdirs(liveViewDir, configuration.getMkDirMode());
            // Wipe any state a prior test in this class left behind.
            checkpointsDir.of(liveViewDir).concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME).slash();
            ff.rmdir(checkpointsDir);
            ff.mkdirs(checkpointsDir, configuration.getMkDirMode());
        }
    }

    @Test
    public void testCorruptManifestYieldsNoCandidate() throws Exception {
        assertMemoryLeak(() -> {
            writeCheckpoints(10, 20, 30);
            publish(1, 30, THREE_ENTRIES);
            try (Path path = new Path(); Path liveViewDir = liveViewDir()) {
                // Flip a byte inside the published region. BlockFileReader
                // selects a region by version parity and throws on a checksum
                // mismatch - there is no automatic fallback to the prior region,
                // and the exception is the block file layer's own, not
                // LV_CHECKPOINT_RING_MANIFEST_INVALID. The read must swallow it
                // all the same.
                LiveViewCheckpointRingManifest.ringManifestPath(path, liveViewDir);
                overwriteByteInFile(path, 64, (byte) 0xAB);
            }
            final LiveViewCheckpointRingCandidate candidate = read();
            Assert.assertFalse("a checksum failure must not yield a candidate", candidate.isStructurallyValid());
            assertCleared(candidate);
        });
    }

    @Test
    public void testManifestFileSurvivesTheSweep() throws Exception {
        assertMemoryLeak(() -> {
            writeCheckpoints(10);
            publish(1, 10, new long[]{10, 1_000, 10, 500, 4_096});
            // _ring deliberately carries no .cp extension, so both sweep passes
            // leave it alone as foreign noise. If this ever fails, the manifest
            // has been renamed into the .cp namespace and recovery deletes its
            // own allow-list on the first restart.
            Assert.assertEquals(10L, sweep(100, read()));
            try (Path path = new Path(); Path liveViewDir = liveViewDir()) {
                LiveViewCheckpointRingManifest.ringManifestPath(path, liveViewDir);
                Assert.assertTrue("_ring survives the sweep", configuration.getFilesFacade().exists(path.$()));
            }
        });
    }

    @Test
    public void testManifestNamingMissingCheckpointRejectedWhole() throws Exception {
        assertMemoryLeak(() -> {
            // 20's .cp is gone - the shape the add path leaves when a prune
            // unlinks an eviction whose publication then failed. Reject the
            // manifest whole: a partial ring is a claim nothing on disk backs.
            writeCheckpoints(10, 30);
            publish(1, 30, THREE_ENTRIES);
            final LiveViewCheckpointRingCandidate candidate = read();
            Assert.assertFalse(
                    "a manifest naming a missing .cp must not yield a candidate",
                    candidate.isStructurallyValid()
            );
            assertCleared(candidate);
            // Without an allow-list the sweep is legacy: highest only.
            Assert.assertEquals(30L, sweep(100, candidate));
            Assert.assertFalse(existsCp(10));
            Assert.assertTrue(existsCp(30));
        });
    }

    @Test
    public void testNoManifestYieldsNoCandidate() throws Exception {
        assertMemoryLeak(() -> {
            writeCheckpoints(10, 20, 30);
            final LiveViewCheckpointRingCandidate candidate = read();
            Assert.assertFalse("no _ring is the legacy shape, not an error", candidate.isStructurallyValid());
            assertCleared(candidate);
            Assert.assertFalse("nothing is listed without a manifest", candidate.isListed(10));
            // Legacy sweep: highest survives, the rest retire.
            Assert.assertEquals(30L, sweep(100, candidate));
            Assert.assertFalse(existsCp(10));
            Assert.assertFalse(existsCp(20));
            Assert.assertTrue(existsCp(30));
        });
    }

    @Test
    public void testSweepKeepsListedCheckpointAheadOfRawWatermarkButNotAsHead() throws Exception {
        assertMemoryLeak(() -> {
            writeCheckpoints(10, 30);
            publish(1, 30, new long[]{
                    10, 1_000, 10, 500, 4_096,
                    30, 3_000, 30, 1_400, 16_384
            });
            // Raw _lv.s says 10 while the manifest claims sealedness at 30.
            // That is the routine crash window - the publication is ordered
            // ahead of the commit, and persistState cannot persist-then-publish
            // - so 30's .cp must survive for the reconciled floor to validate.
            final LiveViewCheckpointRingCandidate candidate = read();
            Assert.assertTrue(candidate.isStructurallyValid());
            final long head = sweep(10, candidate);
            Assert.assertTrue("listed .cp above the raw watermark survives", existsCp(30));
            Assert.assertTrue(existsCp(10));
            // ...but it is not the head. The head is the fallback used when the
            // manifest is NOT trusted, and an untrusted 30 may be a genuine
            // orphan whose commit never landed; restoring it would stamp the
            // applied watermark at 30 over rows the LV table never holds.
            Assert.assertEquals("the fallback head keeps the conservative raw-watermark gate", 10L, head);
        });
    }

    @Test
    public void testSweepKeepsListedCheckpointsBelowHead() throws Exception {
        assertMemoryLeak(() -> {
            writeCheckpoints(10, 20, 30);
            publish(1, 30, THREE_ENTRIES);
            final LiveViewCheckpointRingCandidate candidate = read();
            Assert.assertTrue(candidate.isStructurallyValid());
            final long head = sweep(100, candidate);
            Assert.assertEquals(30L, head);
            // The whole point: without the allow-list the second pass retires
            // everything but the head, which is every entry the ring exists to
            // offer as a resume anchor. testNoManifestYieldsNoCandidate pins
            // that contrast on the same three files.
            Assert.assertTrue("listed .cp below the head survives", existsCp(10));
            Assert.assertTrue("listed .cp below the head survives", existsCp(20));
            Assert.assertTrue(existsCp(30));
        });
    }

    @Test
    public void testSweepRetiresUnlistedCheckpointAheadOfWatermark() throws Exception {
        assertMemoryLeak(() -> {
            writeCheckpoints(10, 15);
            publish(1, 10, new long[]{10, 1_000, 10, 500, 4_096});
            // 15 is unlisted, so the orphan rule applies unchanged.
            Assert.assertEquals(10L, sweep(10, read()));
            Assert.assertTrue(existsCp(10));
            Assert.assertFalse("unlisted .cp ahead of the watermark still retires", existsCp(15));
        });
    }

    @Test
    public void testSweepRetiresUnlistedCheckpoints() throws Exception {
        assertMemoryLeak(() -> {
            // 25 is a poisoned straggler: an O3 retired it, the removeQuiet
            // unlink failed, and it now sits below the head with lvSeqTxn under
            // the watermark - indistinguishable on disk from a sealed entry.
            // The manifest is what tells them apart.
            writeCheckpoints(10, 20, 25, 30);
            publish(1, 30, THREE_ENTRIES);
            final LiveViewCheckpointRingCandidate candidate = read();
            Assert.assertTrue(candidate.isStructurallyValid());
            Assert.assertFalse("the poisoned .cp is not listed", candidate.isListed(25));
            Assert.assertEquals(30L, sweep(100, candidate));
            Assert.assertFalse("unlisted .cp retires even below the head", existsCp(25));
            Assert.assertTrue(existsCp(10));
            Assert.assertTrue(existsCp(20));
            Assert.assertTrue(existsCp(30));
        });
    }

    @Test
    public void testValidManifestPopulatesCandidate() throws Exception {
        assertMemoryLeak(() -> {
            writeCheckpoints(10, 20, 30);
            publish(7, 30, THREE_ENTRIES);
            final LiveViewCheckpointRingCandidate candidate = read();
            Assert.assertTrue(candidate.isStructurallyValid());
            Assert.assertEquals(7L, candidate.getGeneration());
            Assert.assertEquals(30L, candidate.getCoveredBaseSeqTxn());
            Assert.assertEquals(3, candidate.getEntryCount());
            for (int i = 0; i < 3; i++) {
                final int base = i * LiveViewCheckpointRingManifest.ENTRY_SIZE;
                Assert.assertEquals(THREE_ENTRIES[base], candidate.getEntryLvSeqTxn(i));
                Assert.assertEquals(THREE_ENTRIES[base + 1], candidate.getEntryMaxTs(i));
                Assert.assertEquals(THREE_ENTRIES[base + 2], candidate.getEntryBaseSeqTxn(i));
                Assert.assertEquals(THREE_ENTRIES[base + 3], candidate.getEntryLvRowsTotal(i));
                Assert.assertEquals(THREE_ENTRIES[base + 4], candidate.getEntryStateBytes(i));
            }
            Assert.assertTrue(candidate.isListed(10));
            Assert.assertTrue(candidate.isListed(20));
            Assert.assertTrue(candidate.isListed(30));
            Assert.assertFalse(candidate.isListed(25));
            Assert.assertFalse("maxTs must not be mistaken for a member key", candidate.isListed(1_000));
        });
    }

    @Test
    public void testVersionSkewYieldsNoCandidate() throws Exception {
        assertMemoryLeak(() -> {
            writeCheckpoints(10, 20, 30);
            writeRawManifest(
                    LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION + 1, 1, 30, 3, THREE_ENTRIES
            );
            final LiveViewCheckpointRingCandidate candidate = read();
            Assert.assertFalse("a newer format falls back rather than invalidating", candidate.isStructurallyValid());
            assertCleared(candidate);
            // Ring state is derived: a skewed manifest costs one boundary
            // rebuild and the view keeps serving off the highest .cp.
            Assert.assertEquals(30L, sweep(100, candidate));
        });
    }

    private static void assertCleared(LiveViewCheckpointRingCandidate candidate) {
        Assert.assertEquals(Numbers.LONG_NULL, candidate.getCoveredBaseSeqTxn());
        Assert.assertEquals(Numbers.LONG_NULL, candidate.getGeneration());
        Assert.assertEquals(0, candidate.getEntryCount());
    }

    private static boolean existsCp(long lvSeqTxn) {
        try (Path probe = new Path(); Path liveViewDir = liveViewDir()) {
            probe.of(liveViewDir).concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME).slash();
            LiveViewCheckpointWriter.appendCpFileName(probe, lvSeqTxn);
            return configuration.getFilesFacade().exists(probe.$());
        }
    }

    private static Path liveViewDir() {
        final Path path = new Path();
        return path.of(configuration.getDbRoot()).concat(TOKEN.getDirName());
    }

    private static void overwriteByteInFile(Path path, long offset, byte value) {
        try (MemoryCMARW mem = Vm.getCMARWInstance()) {
            mem.of(
                    configuration.getFilesFacade(),
                    path.$(),
                    configuration.getFilesFacade().getPageSize(),
                    offset + Byte.BYTES,
                    MemoryTag.MMAP_DEFAULT,
                    CairoConfiguration.O_NONE
            );
            mem.putByte(offset, value);
            mem.sync(false);
        }
    }

    private static void publish(long generation, long coveredBaseSeqTxn, long[] entries) {
        final LongList list = new LongList();
        for (long entry : entries) {
            list.add(entry);
        }
        try (Path liveViewDir = liveViewDir();
             LiveViewCheckpointRingManifestWriter writer = new LiveViewCheckpointRingManifestWriter(configuration)) {
            writer.publish(liveViewDir, generation, coveredBaseSeqTxn, list);
        }
    }

    private static LiveViewCheckpointRingCandidate read() {
        final LiveViewCheckpointRingCandidate candidate = new LiveViewCheckpointRingCandidate();
        try (Path path = new Path();
             Path liveViewDir = liveViewDir();
             BlockFileReader reader = new BlockFileReader(configuration)) {
            LiveViewRecovery.readRingCandidate(
                    configuration.getFilesFacade(),
                    path,
                    liveViewDir,
                    TOKEN,
                    reader,
                    new LiveViewCheckpointRingManifestReader(),
                    candidate
            );
        }
        return candidate;
    }

    private static long sweep(long appliedWatermark, LiveViewCheckpointRingCandidate candidate) {
        try (Path scratch = new Path(); Path liveViewDir = liveViewDir()) {
            return LiveViewRecovery.sweepCheckpoints(
                    configuration.getFilesFacade(),
                    scratch,
                    liveViewDir,
                    appliedWatermark,
                    new StringSink(),
                    candidate.isStructurallyValid() ? candidate : null
            );
        }
    }

    private static void writeCheckpoints(long... lvSeqTxns) {
        for (long lvSeqTxn : lvSeqTxns) {
            try (Path liveViewDir = liveViewDir();
                 LiveViewCheckpointWriter writer = new LiveViewCheckpointWriter(configuration)) {
                writer.of(liveViewDir.$(), lvSeqTxn);
                writer.writeManifestBlock(new LiveViewCheckpointManifest()
                        .setLvSeqTxn(lvSeqTxn)
                        .setLvRowPosition(0)
                        .setBaseSeqTxn(lvSeqTxn)
                        .setMaxTimestamp(lvSeqTxn * 100)
                        .setKind(LiveViewCheckpointManifest.KIND_STEADY));
                // No prior-head unlink: the sweep is the unlink driver here.
                writer.commit(Long.MIN_VALUE);
            }
        }
    }

    private static void writeRawManifest(
            int formatVersion,
            long generation,
            long coveredBaseSeqTxn,
            int declaredEntryCount,
            long[] entries
    ) {
        // Bypasses LiveViewCheckpointRingManifest.append(), whose asserts mirror
        // the reader's validation. Only a corrupt file or a future writer
        // produces such a payload.
        try (Path path = new Path();
             Path liveViewDir = liveViewDir();
             BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), CommitMode.NOSYNC)) {
            writer.of(LiveViewCheckpointRingManifest.ringManifestPath(path, liveViewDir).$());
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
