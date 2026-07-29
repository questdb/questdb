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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.HighBoundTag;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.RepairPublicationStage;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointRepairState;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Standalone coverage for {@code _checkpoints/repair/r.<repairId>}, the durable
 * descriptor of one in-progress localized out-of-order repair.
 * <p>
 * The descriptor is the only record that a repair's temporary segments exist:
 * nothing in the timeline names them until the range splice commits the
 * superblock. What is under test is therefore what the crash path needs -
 * a record that is never observed torn, a validation that refuses anything it
 * cannot trust, and a sweep that discards a crashed candidate together with the
 * files it owned while leaving published names alone.
 */
public class LiveViewCheckpointRepairStateTest extends AbstractCairoTest {

    private static final String LV_DIR = "lv_repair_state";
    private static final long REPAIR_ID = 42;

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
    public void testBeginRejectsNegativeIdentity() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                    Path dir = new Path()
            ) {
                checkpointsDir(dir);
                try {
                    state.begin(dir, -1, 7, 0, 3, 11, 10, 100, 50, 80, 200, HighBoundTag.FINITE);
                    Assert.fail("expected an invalid repair identity rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "invalid live view checkpoint repair identity");
                }
                Assert.assertFalse(state.isOpen());
            }
        });
    }

    @Test
    public void testCorruptDescriptorFailsValidationAndIsSwept() throws Exception {
        assertMemoryLeak(() -> {
            begin(REPAIR_ID, 5);
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), descriptorPath(path).$(), MemoryTag.MMAP_DEFAULT);
                // Flip a field and leave the CRC stale, the shape a torn or damaged
                // record has.
                final long generation = mem.getLong(40);
                mem.putLong(40, generation ^ 0x5A5A_5A5AL);
            }
            try (
                    LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                    Path dir = new Path()
            ) {
                Assert.assertFalse(state.load(checkpointsDir(dir), REPAIR_ID));
            }
            final LiveViewCheckpointRepairState.SweepResult result = sweep();
            // Unreadable ownership is still a candidate to discard: the descriptor
            // goes, and the temporary segments it may have staged are left to the
            // generic .tmp cleanup that runs beside the sweep.
            Assert.assertEquals(1, result.getDiscardedRepairCount());
            Assert.assertEquals(0, result.getRemovedSegmentCount());
            Assert.assertEquals(0, result.getFailedCount());
            Assert.assertFalse(descriptorExists(REPAIR_ID));
        });
    }

    @Test
    public void testDiscardRemovesTheDescriptorAndSilencesLaterWrites() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                    Path dir = new Path()
            ) {
                checkpointsDir(dir);
                state.begin(dir, REPAIR_ID, 7, 0, 3, REPAIR_ID, 41, 100, 50, 80, 200, HighBoundTag.FINITE);
                Assert.assertTrue(descriptorExists(REPAIR_ID));

                Assert.assertTrue(state.discard());
                Assert.assertFalse(state.isOpen());
                Assert.assertFalse(descriptorExists(REPAIR_ID));

                // A discarded descriptor is not resurrected by the stage/progress
                // mirrors: the repair that owned it is over.
                state.recordStage(RepairPublicationStage.TIMELINE_GENERATION_PUBLISHED);
                state.recordProgress(1_000, 9);
                Assert.assertFalse(descriptorExists(REPAIR_ID));
                // Discarding twice is the no-op every unwinding path relies on.
                Assert.assertTrue(state.discard());
            }
        });
    }

    @Test
    public void testForeignAndShortFilesFailValidation() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path dir = new Path(); Path path = new Path()) {
                checkpointsDir(dir);
                LiveViewCheckpointLayout.repairDirPath(path, dir).slash();
                ff.mkdirs(path, configuration.getMkDirMode());
            }
            // A file too short to hold even an empty record.
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.of(
                        ff,
                        descriptorPath(path).$(),
                        ff.getPageSize(),
                        -1,
                        MemoryTag.MMAP_DEFAULT,
                        CairoConfiguration.O_NONE,
                        -1
                );
                mem.putLong(LiveViewCheckpointRepairState.MAGIC);
                mem.close(true, Vm.TRUNCATE_TO_POINTER);
            }
            try (
                    LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                    Path dir = new Path()
            ) {
                Assert.assertFalse(state.load(checkpointsDir(dir), REPAIR_ID));
            }

            // A full-length record with a foreign magic.
            begin(REPAIR_ID, 1);
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(ff, descriptorPath(path).$(), MemoryTag.MMAP_DEFAULT);
                mem.putLong(LiveViewCheckpointRepairState.MAGIC_OFFSET, 0x0102_0304_0506_0708L);
            }
            try (
                    LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                    Path dir = new Path()
            ) {
                Assert.assertFalse(state.load(checkpointsDir(dir), REPAIR_ID));
            }
        });
    }

    @Test
    public void testRecordsIdentityBoundsCursorStageAndOwnership() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                    Path dir = new Path()
            ) {
                checkpointsDir(dir);
                state.begin(dir, REPAIR_ID, 7, 0, 3, REPAIR_ID, 41, 100, 50, 80, 200, HighBoundTag.FINITE);
                state.addOwnedSegmentId(11);
                state.addOwnedSegmentId(12);
                state.recordProgress(150, 9);
                state.recordStage(RepairPublicationStage.LV_WAL_REPLACEMENT_COMMITTED);
            }

            try (
                    LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                    Path dir = new Path()
            ) {
                Assert.assertTrue(state.load(checkpointsDir(dir), REPAIR_ID));
                Assert.assertEquals(REPAIR_ID, state.getRepairId());
                Assert.assertEquals(7, state.getDefinitionTxn());
                Assert.assertEquals(0, state.getHistoryEpoch());
                Assert.assertEquals(3, state.getGeneration());
                Assert.assertEquals(REPAIR_ID, state.getPinnedBaseSeqTxn());
                Assert.assertEquals(41, state.getTriggerBaseSeqTxn());
                Assert.assertEquals(100, state.getCorrectionTs());
                Assert.assertEquals(50, state.getReplayLowTs());
                Assert.assertEquals(80, state.getOutputLowTs());
                Assert.assertEquals(200, state.getHighTsExclusive());
                Assert.assertEquals(HighBoundTag.FINITE, state.getHighBoundTag());
                Assert.assertEquals(150, state.getLastCompletedTimestampGroup());
                Assert.assertEquals(9, state.getNextCheckpointId());
                Assert.assertEquals(RepairPublicationStage.LV_WAL_REPLACEMENT_COMMITTED, state.getStage());
                Assert.assertEquals(2, state.getOwnedSegmentIdCount());
                Assert.assertEquals(11, state.getOwnedSegmentId(0));
                Assert.assertEquals(12, state.getOwnedSegmentId(1));
            }
        });
    }

    @Test
    public void testRewritesTheDescriptorWhenRenameRejectsAnExistingTarget() throws Exception {
        // Windows MoveFileW refuses an existing destination (errno 183) where
        // POSIX rename replaces it atomically. begin() creates the descriptor and
        // every later update republishes over that same name, so on Windows the
        // first addOwnedSegmentId() aborts the repair.
        assertMemoryLeak(new TestFilesFacadeImpl() {
            private boolean renameRejected;

            @Override
            public int errno() {
                return renameRejected ? CairoException.ERRNO_ALREADY_EXISTS_WIN : super.errno();
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (exists(to)) {
                    renameRejected = true;
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                renameRejected = false;
                return super.rename(from, to);
            }
        }, () -> {
            try (
                    LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                    Path dir = new Path()
            ) {
                checkpointsDir(dir);
                state.begin(dir, REPAIR_ID, 7, 0, 3, 11, 10, 100, 50, 80, 200, HighBoundTag.FINITE);
                // Throwing updates: these abort the repair rather than degrade it.
                state.addOwnedSegmentId(5);
                state.addOwnedSegmentId(6);
                // Best-effort updates: these disable the descriptor on failure.
                state.recordStage(RepairPublicationStage.LV_WAL_REPLACEMENT_COMMITTED);
                state.recordProgress(1_000, 9);
                Assert.assertTrue(state.isOpen());
            }
            try (
                    LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                    Path dir = new Path()
            ) {
                Assert.assertTrue(state.load(checkpointsDir(dir), REPAIR_ID));
                Assert.assertEquals(RepairPublicationStage.LV_WAL_REPLACEMENT_COMMITTED, state.getStage());
                Assert.assertEquals(1_000, state.getLastCompletedTimestampGroup());
                Assert.assertEquals(9, state.getNextCheckpointId());
                Assert.assertEquals(2, state.getOwnedSegmentIdCount());
            }
        });
    }

    @Test
    public void testSweepDiscardsTheCandidateAndTheSegmentsItOwns() throws Exception {
        assertMemoryLeak(() -> {
            begin(REPAIR_ID, 3, 11, 12);
            // Segment 11 is still staged, as data and metadata; segment 12 got as far
            // as its published name before the crash; segment 13 belongs to nobody.
            createSegment(11, false, true);
            createSegment(11, true, true);
            createSegment(12, false, false);
            createSegment(13, false, true);

            final LiveViewCheckpointRepairState.SweepResult result = sweep();
            Assert.assertEquals(1, result.getDiscardedRepairCount());
            Assert.assertEquals("d.11.tmp and m.11.tmp", 2, result.getRemovedSegmentCount());
            Assert.assertEquals(0, result.getFailedCount());

            Assert.assertFalse(descriptorExists(REPAIR_ID));
            Assert.assertFalse(segmentExists(11, false, true));
            Assert.assertFalse(segmentExists(11, true, true));
            // A published name is never touched here, even when the descriptor claims
            // it - monotonic allocation and the final-orphan rules own it, and a later
            // publication is what durably advances past it.
            Assert.assertTrue(segmentExists(12, false, false));
            // Unowned by any descriptor: the generic orphan cleanup collects it, not
            // this sweep.
            Assert.assertTrue(segmentExists(13, false, true));
        });
    }

    @Test
    public void testSweepIsANoOpWithoutDescriptorsAndForAReplica() throws Exception {
        assertMemoryLeak(() -> {
            // No repair directory at all.
            LiveViewCheckpointRepairState.SweepResult result = sweep();
            Assert.assertEquals(0, result.getDiscardedRepairCount());
            Assert.assertEquals(0, result.getRemovedSegmentCount());
            Assert.assertEquals(0, result.getFailedCount());

            begin(REPAIR_ID, 3, 11);
            createSegment(11, false, true);
            try (Path dir = new Path()) {
                result = LiveViewCheckpointRepairState.sweep(configuration, checkpointsDir(dir), false);
            }
            Assert.assertEquals(0, result.getDiscardedRepairCount());
            Assert.assertTrue("a replica owns no timeline and may not sweep one", descriptorExists(REPAIR_ID));
            Assert.assertTrue(segmentExists(11, false, true));
        });
    }

    @Test
    public void testTornDescriptorWriteLeavesOnlyATmpForTheSweep() throws Exception {
        assertMemoryLeak(() -> {
            // A crash between staging and rename: the previous record is still the
            // one a reader sees, and the .tmp is the sweep's to remove.
            begin(REPAIR_ID, 3);
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path dir = new Path(); Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                LiveViewCheckpointLayout.repairDescriptorTmpPath(path, checkpointsDir(dir), REPAIR_ID);
                mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                mem.putLong(0, 0);
            }
            try (
                    LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                    Path dir = new Path()
            ) {
                Assert.assertTrue("the published record must survive a torn stage", state.load(checkpointsDir(dir), REPAIR_ID));
                Assert.assertEquals(3, state.getGeneration());
            }

            final LiveViewCheckpointRepairState.SweepResult result = sweep();
            Assert.assertEquals(1, result.getDiscardedRepairCount());
            Assert.assertEquals(0, result.getFailedCount());
            Assert.assertFalse(descriptorExists(REPAIR_ID));
            try (Path dir = new Path(); Path path = new Path()) {
                LiveViewCheckpointLayout.repairDescriptorTmpPath(path, checkpointsDir(dir), REPAIR_ID);
                Assert.assertFalse(ff.exists(path.$()));
            }
        });
    }

    @Test
    public void testUnboundedHighBoundAndAnAbsentCursorRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                    Path dir = new Path()
            ) {
                checkpointsDir(dir);
                state.begin(
                        dir,
                        REPAIR_ID,
                        7,
                        0,
                        3,
                        REPAIR_ID,
                        41,
                        100,
                        50,
                        80,
                        Numbers.LONG_NULL,
                        HighBoundTag.EOF
                );
            }
            try (
                    LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                    Path dir = new Path()
            ) {
                Assert.assertTrue(state.load(checkpointsDir(dir), REPAIR_ID));
                Assert.assertEquals(HighBoundTag.EOF, state.getHighBoundTag());
                Assert.assertEquals(Numbers.LONG_NULL, state.getHighTsExclusive());
                Assert.assertEquals(Numbers.LONG_NULL, state.getLastCompletedTimestampGroup());
                Assert.assertEquals(Numbers.LONG_NULL, state.getNextCheckpointId());
                Assert.assertEquals(RepairPublicationStage.PLAN, state.getStage());
                Assert.assertEquals(0, state.getOwnedSegmentIdCount());
            }
        });
    }

    private static Path checkpointsDir(Path path) {
        path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
        return path;
    }

    private static void createSegment(long segmentId, boolean meta, boolean temporary) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path dir = new Path(); Path path = new Path()) {
            checkpointsDir(dir);
            LiveViewCheckpointLayout.metaDirPath(path, dir).slash();
            ff.mkdirs(path, configuration.getMkDirMode());
            LiveViewCheckpointLayout.dataDirPath(path, dir).slash();
            ff.mkdirs(path, configuration.getMkDirMode());
            segmentPath(path, dir, segmentId, meta, temporary);
            ff.touch(path.$());
        }
    }

    private static Path descriptorPath(Path path) {
        try (Path dir = new Path()) {
            return LiveViewCheckpointLayout.repairDescriptorPath(path, checkpointsDir(dir), REPAIR_ID);
        }
    }

    private static boolean descriptorExists(long repairId) {
        try (Path dir = new Path(); Path path = new Path()) {
            LiveViewCheckpointLayout.repairDescriptorPath(path, checkpointsDir(dir), repairId);
            return configuration.getFilesFacade().exists(path.$());
        }
    }

    private static Path segmentPath(Path dst, Path checkpointsDir, long segmentId, boolean meta, boolean temporary) {
        if (meta) {
            return temporary
                    ? LiveViewCheckpointLayout.metaSegmentTmpPath(dst, checkpointsDir, segmentId)
                    : LiveViewCheckpointLayout.metaSegmentPath(dst, checkpointsDir, segmentId);
        }
        return temporary
                ? LiveViewCheckpointLayout.dataSegmentTmpPath(dst, checkpointsDir, segmentId)
                : LiveViewCheckpointLayout.dataSegmentPath(dst, checkpointsDir, segmentId);
    }

    private static boolean segmentExists(long segmentId, boolean meta, boolean temporary) {
        try (Path dir = new Path(); Path path = new Path()) {
            checkpointsDir(dir);
            segmentPath(path, dir, segmentId, meta, temporary);
            return configuration.getFilesFacade().exists(path.$());
        }
    }

    private static LiveViewCheckpointRepairState.SweepResult sweep() {
        try (Path dir = new Path()) {
            return LiveViewCheckpointRepairState.sweep(configuration, checkpointsDir(dir), true);
        }
    }

    private void begin(long repairId, long generation, long... ownedSegmentIds) {
        try (
                LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                Path dir = new Path()
        ) {
            checkpointsDir(dir);
            state.begin(dir, repairId, 7, 0, generation, repairId, repairId - 1, 100, 50, 80, 200, HighBoundTag.FINITE);
            for (long segmentId : ownedSegmentIds) {
                state.addOwnedSegmentId(segmentId);
            }
        }
    }
}
