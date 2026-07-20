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

import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Zip;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Standalone coverage for the fixed A/B {@code _checkpoints/_timeline}
 * superblock, the sole commit point for a versioned-checkpoint-timeline
 * generation (design section 8.2).
 * <p>
 * Two independently-checksummed slots give the crash-safety property under test:
 * a publication writes the inactive slot and its higher generation wins on
 * selection, while a torn or corrupt newest slot falls back to the previous slot
 * with no wider scan. The contract is that a reader always sees one complete
 * generation, never a partial one, and never loses a valid slot to a
 * neighbour's corruption.
 */
public class LiveViewCheckpointSuperblockTest extends AbstractCairoTest {

    private static final String LV_DIR = "lv_superblock";

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            final FilesFacade ff = configuration.getFilesFacade();
            checkpointsDir(path).slash();
            ff.mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testAlternatingSlotsAcrossManyPublications() throws Exception {
        assertMemoryLeak(() -> {
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                for (long gen = 1; gen <= 8; gen++) {
                    setFields(sb, gen);
                    sb.publish();
                    // Publications alternate slot 0, 1, 0, 1, ...
                    Assert.assertEquals((int) ((gen - 1) & 1), sb.getSelectedSlot());
                    assertFields(sb, gen);
                }
            }
            // A fresh open must still pick the last (highest-generation) slot.
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                Assert.assertTrue(sb.isValid());
                Assert.assertEquals(1, sb.getSelectedSlot());
                assertFields(sb, 8);
            }
        });
    }

    @Test
    public void testBothSlotsCorruptSelectsNoSlot() throws Exception {
        assertMemoryLeak(() -> {
            publish(1);
            publish(2);
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                // Break the checksum of both slots.
                corruptGenerationNoCrcFix(mem, 0);
                corruptGenerationNoCrcFix(mem, 1);
            }
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                Assert.assertFalse(sb.isValid());
                Assert.assertEquals(LiveViewCheckpointSuperblock.NO_SLOT, sb.getSelectedSlot());
                Assert.assertEquals(0, sb.generation);
                Assert.assertTrue(sb.timelineRootRef.isNull());
            }
        });
    }

    @Test
    public void testFallbackWhenNewestSlotChecksumCorrupt() throws Exception {
        assertMemoryLeak(() -> {
            publish(1); // slot 0
            publish(2); // slot 1 (newest)
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                // Torn newest slot: change a field but leave a stale CRC.
                corruptGenerationNoCrcFix(mem, 1);
            }
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                // Falls back to the intact previous generation.
                Assert.assertEquals(0, sb.getSelectedSlot());
                assertFields(sb, 1);
            }
        });
    }

    @Test
    public void testFallbackWhenNewestSlotMagicCorrupt() throws Exception {
        assertMemoryLeak(() -> {
            publish(1); // slot 0
            publish(2); // slot 1
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                final long base = LiveViewCheckpointSuperblock.SLOT_SIZE;
                mem.putLong(base + LiveViewCheckpointSuperblock.SLOT_MAGIC_OFFSET, 0);
            }
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                Assert.assertEquals(0, sb.getSelectedSlot());
                assertFields(sb, 1);
            }
        });
    }

    @Test
    public void testFirstPublishRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                Assert.assertFalse(sb.isValid());
                setFields(sb, 1);
                sb.publish();
                Assert.assertEquals(0, sb.getSelectedSlot());
                assertFields(sb, 1);
            }
            // Re-open in a fresh instance; every field must survive unmap/remap.
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                Assert.assertTrue(sb.isValid());
                Assert.assertEquals(0, sb.getSelectedSlot());
                assertFields(sb, 1);
            }
        });
    }

    @Test
    public void testFormatVersionSkewSlotIgnored() throws Exception {
        assertMemoryLeak(() -> {
            publish(1); // slot 0
            publish(2); // slot 1
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                // A newer format version with an otherwise valid checksum: the
                // slot is a real, but unreadable, future generation - ignore it
                // and use the readable older slot rather than misparsing it.
                final long base = LiveViewCheckpointSuperblock.SLOT_SIZE;
                mem.putInt(base + LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION_OFFSET, LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION + 1);
                fixSlotCrc(mem, 1);
            }
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                Assert.assertEquals(0, sb.getSelectedSlot());
                assertFields(sb, 1);
            }
        });
    }

    @Test
    public void testFreshFileHasNoSlot() throws Exception {
        assertMemoryLeak(() -> {
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                Assert.assertFalse(sb.isValid());
                Assert.assertEquals(LiveViewCheckpointSuperblock.NO_SLOT, sb.getSelectedSlot());
                Assert.assertEquals(0, sb.generation);
                Assert.assertTrue(sb.timelineRootRef.isNull());
                Assert.assertTrue(sb.rowPositionDeltaRootRef.isNull());
                Assert.assertTrue(sb.segmentDirectoryRootRef.isNull());
            }
        });
    }

    @Test
    public void testHigherGenerationWinsRegardlessOfSlot() throws Exception {
        assertMemoryLeak(() -> {
            publish(10); // slot 0
            publish(20); // slot 1 (higher generation)
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                Assert.assertEquals(1, sb.getSelectedSlot());
                assertFields(sb, 20);
            }
        });
    }

    @Test
    public void testNullAndNonNullRefsRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                sb.generation = 5;
                sb.timelineRootRef.of(2, 4_096, 256);
                sb.rowPositionDeltaRootRef.clear();
                sb.segmentDirectoryRootRef.of(9, 0, 24);
                sb.publish();
            }
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                Assert.assertEquals(5, sb.generation);
                Assert.assertFalse(sb.timelineRootRef.isNull());
                Assert.assertEquals(2, sb.timelineRootRef.getSegmentId());
                Assert.assertEquals(4_096, sb.timelineRootRef.getOffset());
                Assert.assertEquals(256, sb.timelineRootRef.getLength());
                Assert.assertTrue(sb.rowPositionDeltaRootRef.isNull());
                Assert.assertFalse(sb.segmentDirectoryRootRef.isNull());
                Assert.assertEquals(9, sb.segmentDirectoryRootRef.getSegmentId());
                Assert.assertEquals(24, sb.segmentDirectoryRootRef.getLength());
            }
        });
    }

    private static void assertFields(LiveViewCheckpointSuperblock sb, long gen) {
        Assert.assertEquals(gen, sb.generation);
        Assert.assertEquals(gen * 10 + 1, sb.definitionTxn);
        Assert.assertEquals(gen * 10 + 2, sb.historyEpoch);
        Assert.assertEquals(gen * 10 + 3, sb.normalizedBaseSeqTxn);
        Assert.assertEquals(gen * 10 + 4, sb.coveredLvSeqTxn);
        Assert.assertEquals(gen * 10 + 5, sb.nextCheckpointId);
        Assert.assertEquals(gen * 10 + 6, sb.nextSegmentId);
        Assert.assertEquals(gen * 10 + 7, sb.metadataBytes);
        Assert.assertEquals(gen * 10 + 8, sb.dataBytes);
        Assert.assertEquals(gen, sb.timelineRootRef.getSegmentId());
        Assert.assertEquals(gen * 100, sb.timelineRootRef.getOffset());
        Assert.assertEquals((int) (gen * 4), sb.timelineRootRef.getLength());
        Assert.assertEquals(gen + 1, sb.rowPositionDeltaRootRef.getSegmentId());
        Assert.assertEquals(gen * 200, sb.rowPositionDeltaRootRef.getOffset());
        Assert.assertEquals((int) (gen * 5), sb.rowPositionDeltaRootRef.getLength());
        Assert.assertTrue(sb.segmentDirectoryRootRef.isNull());
    }

    private static Path checkpointsDir(Path path) {
        path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
        return path;
    }

    private static void corruptGenerationNoCrcFix(MemoryCMARW mem, int slot) {
        final long base = (long) slot * LiveViewCheckpointSuperblock.SLOT_SIZE;
        final long current = mem.getLong(base + LiveViewCheckpointSuperblock.SLOT_GENERATION_OFFSET);
        mem.putLong(base + LiveViewCheckpointSuperblock.SLOT_GENERATION_OFFSET, current ^ 0x5A5A_5A5AL);
    }

    private static void fixSlotCrc(MemoryCMARW mem, int slot) {
        final long base = (long) slot * LiveViewCheckpointSuperblock.SLOT_SIZE;
        final int crc = Zip.crc32(0, mem.addressOf(base), LiveViewCheckpointSuperblock.SLOT_CRC_COVERAGE);
        mem.putInt(base + LiveViewCheckpointSuperblock.SLOT_CRC_OFFSET, crc);
    }

    private static void setFields(LiveViewCheckpointSuperblock sb, long gen) {
        sb.generation = gen;
        sb.definitionTxn = gen * 10 + 1;
        sb.historyEpoch = gen * 10 + 2;
        sb.normalizedBaseSeqTxn = gen * 10 + 3;
        sb.coveredLvSeqTxn = gen * 10 + 4;
        sb.nextCheckpointId = gen * 10 + 5;
        sb.nextSegmentId = gen * 10 + 6;
        sb.metadataBytes = gen * 10 + 7;
        sb.dataBytes = gen * 10 + 8;
        sb.timelineRootRef.of(gen, gen * 100, (int) (gen * 4));
        sb.rowPositionDeltaRootRef.of(gen + 1, gen * 200, (int) (gen * 5));
        sb.segmentDirectoryRootRef.clear();
    }

    private static Path timelinePath(Path path) {
        try (Path dir = new Path()) {
            return LiveViewCheckpointLayout.timelinePath(path, checkpointsDir(dir));
        }
    }

    private void publish(long gen) {
        try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
            try (Path dir = new Path()) {
                sb.of(checkpointsDir(dir));
            }
            setFields(sb, gen);
            sb.publish();
        }
    }
}
