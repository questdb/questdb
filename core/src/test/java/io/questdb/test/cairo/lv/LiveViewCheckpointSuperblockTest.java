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
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Zip;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Standalone coverage for the fixed A/B {@code _checkpoints/_timeline}
 * superblock, the sole commit point for a versioned-checkpoint-timeline
 * generation.
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
    public void testCorruptPendingDirectoryRegistrationInvalidatesSlot() throws Exception {
        // The deferred directory registration is either absent outright or fully
        // described: a valid-CRC slot carrying a half-set triple would make the next
        // publication catalogue a segment with a length or page count that names
        // nothing. Each broken shape must invalidate the slot; the A/B fallback covers it.
        assertMemoryLeak(() -> {
            publish(1); // slot 0
            publish(2); // slot 1 (newest; carries a full registration: 20, 600, 6)
            final long[][] combos = {
                    // {segmentId, bytes, pages}
                    {Numbers.LONG_NULL, 600, 6}, // absent id with leftover bytes and pages
                    {Numbers.LONG_NULL, 0, 6},   // absent id with leftover pages
                    {20, 0, 6},                  // registered id with no byte length
                    {20, 600, 0},                // registered id with no page count
                    {-2, 600, 6},                // negative id that is not the null sentinel
            };
            for (long[] combo : combos) {
                try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                    mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                    final long base = LiveViewCheckpointSuperblock.SLOT_SIZE;
                    mem.putLong(base + LiveViewCheckpointSuperblock.SLOT_PENDING_DIRECTORY_SEGMENT_ID_OFFSET, combo[0]);
                    mem.putLong(base + LiveViewCheckpointSuperblock.SLOT_PENDING_DIRECTORY_SEGMENT_BYTES_OFFSET, combo[1]);
                    mem.putLong(base + LiveViewCheckpointSuperblock.SLOT_PENDING_DIRECTORY_SEGMENT_PAGES_OFFSET, combo[2]);
                    fixSlotCrc(mem, 1);
                }
                try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                    try (Path dir = new Path()) {
                        sb.of(checkpointsDir(dir));
                    }
                    final String detail = "combo [id=" + combo[0] + ", bytes=" + combo[1] + ", pages=" + combo[2] + ']';
                    Assert.assertEquals(detail, 0, sb.getSelectedSlot());
                    assertFields(sb, 1);
                }
                // Restore the full registration so the next combo starts from a valid slot.
                try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                    mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                    final long base = LiveViewCheckpointSuperblock.SLOT_SIZE;
                    mem.putLong(base + LiveViewCheckpointSuperblock.SLOT_PENDING_DIRECTORY_SEGMENT_ID_OFFSET, 20);
                    mem.putLong(base + LiveViewCheckpointSuperblock.SLOT_PENDING_DIRECTORY_SEGMENT_BYTES_OFFSET, 600);
                    mem.putLong(base + LiveViewCheckpointSuperblock.SLOT_PENDING_DIRECTORY_SEGMENT_PAGES_OFFSET, 6);
                    fixSlotCrc(mem, 1);
                }
            }
            // The restore itself must be sound: the newest slot reads back once more.
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                Assert.assertEquals(1, sb.getSelectedSlot());
                assertFields(sb, 2);
            }
        });
    }

    @Test
    public void testCorruptRetiredCountInvalidatesSlot() throws Exception {
        // The live boundary count is nextCheckpointId minus the retired count, so a
        // valid-CRC slot whose retired count is negative or exceeds the allocated ids
        // must not be selected - it would derive a negative live count. The A/B
        // fallback covers it.
        assertMemoryLeak(() -> {
            publish(1); // slot 0
            publish(2); // slot 1 (newest; nextCheckpointId = 25, retired = 2)
            final long[] forged = {25 + 1, -1};
            for (long retired : forged) {
                try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                    mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                    final long base = LiveViewCheckpointSuperblock.SLOT_SIZE;
                    mem.putLong(base + LiveViewCheckpointSuperblock.SLOT_RETIRED_CHECKPOINT_COUNT_OFFSET, retired);
                    fixSlotCrc(mem, 1);
                }
                try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                    try (Path dir = new Path()) {
                        sb.of(checkpointsDir(dir));
                    }
                    Assert.assertEquals("retired=" + retired, 0, sb.getSelectedSlot());
                    assertFields(sb, 1);
                }
            }
            // The boundary value is still a valid slot: retiring every allocated id is
            // exactly what a whole-history truncate leaves behind.
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                final long base = LiveViewCheckpointSuperblock.SLOT_SIZE;
                mem.putLong(base + LiveViewCheckpointSuperblock.SLOT_RETIRED_CHECKPOINT_COUNT_OFFSET, 25);
                fixSlotCrc(mem, 1);
            }
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                Assert.assertEquals(1, sb.getSelectedSlot());
                Assert.assertEquals(2, sb.generation);
                Assert.assertEquals(25, sb.retiredCheckpointCount);
            }
        });
    }

    @Test
    public void testCorruptSeedCursorOffsetInvalidatesSlot() throws Exception {
        // The seed cursor is a row offset a restart skips the base cursor forward by, so a
        // valid-CRC slot carrying a negative one that is not the sentinel must not be selected -
        // it would resume the sweep at an arbitrary position. The A/B fallback covers it.
        assertMemoryLeak(() -> {
            publish(1); // slot 0
            publish(2); // slot 1 (newest)
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                final long base = LiveViewCheckpointSuperblock.SLOT_SIZE;
                mem.putLong(base + LiveViewCheckpointSuperblock.SLOT_SEED_CURSOR_OFFSET_OFFSET, -7);
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
    public void testPublishRejectsNegativeSeedCursorOffset() throws Exception {
        assertMemoryLeak(() -> {
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                setFields(sb, 1);
                sb.seedCursorOffset = -1;
                try {
                    sb.publish();
                    Assert.fail("expected a negative seed cursor offset to be rejected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "seed cursor offset must be non-negative");
                }
                Assert.assertFalse("a rejected publication must leave no valid slot", sb.isValid());
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
    public void testEveryNewestSlotByteCorruptionFallsBack() throws Exception {
        assertMemoryLeak(() -> {
            publish(1); // slot 0 fallback
            publish(2); // slot 1 newest
            for (int byteOffset = 0; byteOffset < LiveViewCheckpointSuperblock.SLOT_SIZE; byteOffset++) {
                final long offset = LiveViewCheckpointSuperblock.SLOT_SIZE + byteOffset;
                final byte original;
                try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                    mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                    original = mem.getByte(offset);
                    mem.putByte(offset, (byte) (original ^ 1));
                }
                try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                    try (Path dir = new Path()) {
                        sb.of(checkpointsDir(dir));
                    }
                    Assert.assertEquals("corrupt byte " + byteOffset, 0, sb.getSelectedSlot());
                    assertFields(sb, 1);
                }
                try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                    mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                    mem.putByte(offset, original);
                }
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
    public void testForeignFormatProbeAcceptsNativeAndTornSlots() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                Assert.assertFalse(
                        "a _timeline no build has written yet is not classified",
                        LiveViewCheckpointSuperblock.isForeignFormat(ff, timelinePath(path).$())
                );
            }

            publish(1); // slot 0
            publish(2); // slot 1
            try (Path path = new Path()) {
                Assert.assertFalse(LiveViewCheckpointSuperblock.isForeignFormat(ff, timelinePath(path).$()));
            }

            // A slot torn mid-publication carries this build's magic and version -
            // both written ahead of the CRC - so the probe leaves it to ordinary
            // A/B fallback rather than condemning the whole directory.
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(ff, timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                corruptGenerationNoCrcFix(mem, 1);
            }
            try (Path path = new Path()) {
                Assert.assertFalse(LiveViewCheckpointSuperblock.isForeignFormat(ff, timelinePath(path).$()));
            }

            // A zeroed slot pair is outside the magic family altogether.
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(ff, timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                mem.zero();
            }
            try (Path path = new Path()) {
                Assert.assertFalse(LiveViewCheckpointSuperblock.isForeignFormat(ff, timelinePath(path).$()));
            }
        });
    }

    @Test
    public void testForeignFormatProbeDetectsMagicVersionSkew() throws Exception {
        assertMemoryLeak(() -> {
            publish(1); // slot 0
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                // Same family, later version nibble: another build's superblock.
                mem.putLong(LiveViewCheckpointSuperblock.SLOT_MAGIC_OFFSET, LiveViewCheckpointSuperblock.SLOT_MAGIC + 1);
                fixSlotCrc(mem, 0);
            }
            try (Path path = new Path()) {
                Assert.assertTrue(LiveViewCheckpointSuperblock.isForeignFormat(
                        configuration.getFilesFacade(),
                        timelinePath(path).$()
                ));
            }
        });
    }

    @Test
    public void testForeignFormatProbeDetectsSlotFormatVersionSkew() throws Exception {
        assertMemoryLeak(() -> {
            publish(1); // slot 0
            publish(2); // slot 1
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                final long base = LiveViewCheckpointSuperblock.SLOT_SIZE;
                mem.putInt(
                        base + LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION_OFFSET,
                        LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION + 1
                );
                fixSlotCrc(mem, 1);
            }
            try (Path path = new Path()) {
                Assert.assertTrue(
                        "one foreign slot condemns the file, even beside a readable one",
                        LiveViewCheckpointSuperblock.isForeignFormat(
                                configuration.getFilesFacade(),
                                timelinePath(path).$()
                        )
                );
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
                // A primary never gets this far, because lifecycle reconciliation
                // classifies the directory as foreign and resets it first; this is
                // the disposition for a reader that does not reconcile.
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

    @Test
    public void testPublishRejectsBackwardWatermarksWithoutTouchingFallback() throws Exception {
        assertMemoryLeak(() -> {
            publish(1);
            publish(2);
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }

                // The generation advances, so only the watermark guard stands
                // between a slot and a base seqTxn below the one a durable slot
                // already declared valid. Publishing it would release WAL the
                // fallback still needs, and would let recovery replay a base
                // transaction the roots have already incorporated.
                setFields(sb, 3);
                sb.normalizedBaseSeqTxn = 22;
                try {
                    sb.publish();
                    Assert.fail("expected a backward base watermark to be rejected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "watermarks must not move backwards");
                }
                Assert.assertEquals(1, sb.getSelectedSlot());

                setFields(sb, 3);
                sb.coveredLvSeqTxn = 23;
                try {
                    sb.publish();
                    Assert.fail("expected a backward live-view watermark to be rejected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "watermarks must not move backwards");
                }
                Assert.assertEquals(1, sb.getSelectedSlot());
            }
            // A rejected publication writes nothing, so the slot it would have
            // targeted still holds generation 1 and can still be recovered from.
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                corruptGenerationNoCrcFix(mem, 1);
            }
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                assertFields(sb, 1);
            }
        });
    }

    @Test
    public void testPublishRejectsHalfSetPendingDirectoryRegistration() throws Exception {
        assertMemoryLeak(() -> {
            publish(1);
            publish(2);
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                final long[][] combos = {
                        // {segmentId, bytes, pages}
                        {Numbers.LONG_NULL, 1, 0}, // absent id with leftover bytes
                        {Numbers.LONG_NULL, 0, 1}, // absent id with leftover pages
                        {7, 0, 1},                 // registered id with no byte length
                        {7, 1, 0},                 // registered id with no page count
                        {-2, 1, 1},                // negative id that is not the null sentinel
                };
                for (long[] combo : combos) {
                    setFields(sb, 3);
                    sb.pendingDirectorySegmentId = combo[0];
                    sb.pendingDirectorySegmentBytes = combo[1];
                    sb.pendingDirectorySegmentPages = combo[2];
                    try {
                        sb.publish();
                        Assert.fail("expected a half-set pending directory registration to be rejected [id="
                                + combo[0] + ", bytes=" + combo[1] + ", pages=" + combo[2] + ']');
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "pending directory segment registration invalid");
                    }
                    Assert.assertEquals(1, sb.getSelectedSlot());
                }
            }
            // A rejected publication writes nothing, so the slot it would have
            // targeted still holds generation 1 and can still be recovered from.
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                corruptGenerationNoCrcFix(mem, 1);
            }
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                assertFields(sb, 1);

                // A fully-described registration is the valid shape and publishes.
                setFields(sb, 3);
                sb.pendingDirectorySegmentId = 7;
                sb.pendingDirectorySegmentBytes = 640;
                sb.pendingDirectorySegmentPages = 2;
                sb.publish();
            }
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                Assert.assertEquals(3, sb.generation);
                Assert.assertEquals(7, sb.pendingDirectorySegmentId);
                Assert.assertEquals(640, sb.pendingDirectorySegmentBytes);
                Assert.assertEquals(2, sb.pendingDirectorySegmentPages);
            }
        });
    }

    @Test
    public void testPublishRejectsNonAdvancingGenerationWithoutTouchingFallback() throws Exception {
        assertMemoryLeak(() -> {
            publish(1);
            publish(2);
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                setFields(sb, 2);
                try {
                    sb.publish();
                    Assert.fail("expected non-advancing generation rejection");
                } catch (io.questdb.cairo.CairoException e) {
                    Assert.assertTrue(e.getFlyweightMessage().toString().contains("generation must advance"));
                }
                Assert.assertEquals(1, sb.getSelectedSlot());
            }
            // Both original slots remain intact, including generation 1 fallback.
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                corruptGenerationNoCrcFix(mem, 1);
            }
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                assertFields(sb, 1);
            }
        });
    }

    @Test
    public void testPublishRejectsOutOfRangeRetiredCount() throws Exception {
        assertMemoryLeak(() -> {
            publish(1);
            publish(2);
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                setFields(sb, 3);
                sb.retiredCheckpointCount = -1;
                try {
                    sb.publish();
                    Assert.fail("expected a negative retired count to be rejected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "retired boundary count out of range");
                }
                Assert.assertEquals(1, sb.getSelectedSlot());

                setFields(sb, 3);
                sb.retiredCheckpointCount = sb.nextCheckpointId + 1;
                try {
                    sb.publish();
                    Assert.fail("expected a retired count above the allocated ids to be rejected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "retired boundary count out of range");
                }
                Assert.assertEquals(1, sb.getSelectedSlot());
            }
            // A rejected publication writes nothing, so the slot it would have
            // targeted still holds generation 1 and can still be recovered from.
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                corruptGenerationNoCrcFix(mem, 1);
            }
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                assertFields(sb, 1);

                // Retiring every allocated id is the boundary and publishes: it is
                // exactly what a whole-history truncate leaves behind.
                setFields(sb, 3);
                sb.retiredCheckpointCount = sb.nextCheckpointId;
                sb.publish();
            }
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration)) {
                try (Path dir = new Path()) {
                    sb.of(checkpointsDir(dir));
                }
                Assert.assertEquals(3, sb.generation);
                Assert.assertEquals(sb.nextCheckpointId, sb.retiredCheckpointCount);
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
        // Alternating so both a real mid-sweep cursor and the "steady seal" sentinel
        // round-trip through every publication test in this class.
        Assert.assertEquals(seedCursorOffset(gen), sb.seedCursorOffset);
        Assert.assertEquals(gen, sb.retiredCheckpointCount);
        if ((gen & 1) == 0) {
            Assert.assertEquals(gen * 10, sb.pendingDirectorySegmentId);
            Assert.assertEquals(gen * 300, sb.pendingDirectorySegmentBytes);
            Assert.assertEquals(gen * 3, sb.pendingDirectorySegmentPages);
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, sb.pendingDirectorySegmentId);
            Assert.assertEquals(0, sb.pendingDirectorySegmentBytes);
            Assert.assertEquals(0, sb.pendingDirectorySegmentPages);
        }
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
        sb.seedCursorOffset = seedCursorOffset(gen);
        sb.retiredCheckpointCount = gen;
        // Alternating like the seed cursor, so both the "wrote a directory segment"
        // and the "reused the previous root" shapes round-trip through every
        // publication test in this class.
        if ((gen & 1) == 0) {
            sb.pendingDirectorySegmentId = gen * 10;
            sb.pendingDirectorySegmentBytes = gen * 300;
            sb.pendingDirectorySegmentPages = gen * 3;
        } else {
            sb.pendingDirectorySegmentId = Numbers.LONG_NULL;
            sb.pendingDirectorySegmentBytes = 0;
            sb.pendingDirectorySegmentPages = 0;
        }
        sb.timelineRootRef.of(gen, gen * 100, (int) (gen * 4));
        sb.rowPositionDeltaRootRef.of(gen + 1, gen * 200, (int) (gen * 5));
        sb.segmentDirectoryRootRef.clear();
    }

    private static long seedCursorOffset(long gen) {
        return (gen & 1) == 0 ? gen * 10 + 9 : Numbers.LONG_NULL;
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

    @Test
    public void testWalPurgeFloorTracksBothSlotsAndRejectsNegativeCoordinate() throws Exception {
        assertMemoryLeak(() -> {
            publish(1);
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration);
                 Path dir = new Path()) {
                sb.of(checkpointsDir(dir));
                Assert.assertEquals(13, sb.getWalPurgeFloor());
            }

            publish(2);
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration);
                 Path dir = new Path()) {
                sb.of(checkpointsDir(dir));
                Assert.assertEquals("the fallback slot still needs generation 1 WAL", 13, sb.getWalPurgeFloor());
            }

            publish(3);
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration);
                 Path dir = new Path()) {
                sb.of(checkpointsDir(dir));
                Assert.assertEquals("generation 3 overwrites generation 1", 23, sb.getWalPurgeFloor());
            }

            // Forge a valid-CRC negative normalized coordinate in the fallback
            // slot. It must be rejected as a slot, never interpreted as a
            // sentinel that silently drops the WAL floor.
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), timelinePath(path).$(), MemoryTag.MMAP_DEFAULT);
                mem.putLong(
                        LiveViewCheckpointSuperblock.SLOT_SIZE
                                + LiveViewCheckpointSuperblock.SLOT_NORMALIZED_BASE_SEQTXN_OFFSET,
                        -1
                );
                fixSlotCrc(mem, 1);
            }
            try (LiveViewCheckpointSuperblock sb = new LiveViewCheckpointSuperblock(configuration);
                 Path dir = new Path()) {
                sb.of(checkpointsDir(dir));
                Assert.assertEquals(3, sb.generation);
                Assert.assertEquals(33, sb.getWalPurgeFloor());
            }
        });
    }
}
