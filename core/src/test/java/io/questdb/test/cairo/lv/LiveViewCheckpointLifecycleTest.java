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
import io.questdb.cairo.lv.LiveViewCheckpointContracts.HighBoundTag;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.RepairPublicationStage;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointLifecycle;
import io.questdb.cairo.lv.LiveViewCheckpointMetaSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointRepairState;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Zip;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LiveViewCheckpointLifecycleTest extends AbstractCairoTest {

    private static final String LV_DIR = "lv_checkpoint_lifecycle";

    @Test
    public void testDefinitionAndHistoryChangeRetiresOldEpoch() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            Assert.assertTrue(timelineExists());

            final LiveViewCheckpointLifecycle.ReconcileResult result;
            try (Path dir = checkpointsDir()) {
                result = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 8, 1, true);
            }
            Assert.assertTrue(result.isEpochReplaced());
            Assert.assertFalse(timelineExists());
            Assert.assertFalse(metaDirExists());
            Assert.assertFalse(dataDirExists());
        });
    }

    @Test
    public void testForeignFormatResetFailureFailsReconciliationAndRetries() throws Exception {
        final boolean[] failRmdir = {false};
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public boolean rmdir(Path name, boolean haltOnError) {
                if (failRmdir[0]) {
                    failRmdir[0] = false;
                    return false;
                }
                return super.rmdir(name, haltOnError);
            }
        };
        assertMemoryLeak(ff, () -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            touchTopLevel("_ring");

            // A directory that is half one format and half another is exactly what
            // the reset exists to prevent, so a removal that does not complete
            // fails the reconciliation rather than letting it continue.
            failRmdir[0] = true;
            try (Path dir = checkpointsDir()) {
                LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
                Assert.fail("expected the failed reset to fail reconciliation");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not reset live view checkpoint directory");
            }
            Assert.assertTrue(checkpointsDirExists());

            // The next reconciliation meets the survivors, classifies the
            // directory as foreign again, and finishes the reset.
            final LiveViewCheckpointLifecycle.ReconcileResult retry;
            try (Path dir = checkpointsDir()) {
                retry = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertTrue(retry.isFormatReset());
            Assert.assertFalse(checkpointsDirExists());
        });
    }

    @Test
    public void testForeignTimelineVersionResetsCheckpointDirectory() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            touchFinal(false, 4);
            Assert.assertTrue(timelineExists());

            // A slot whose checksum agrees with its body but whose layout version
            // this build does not write: a real generation another build owns, not
            // a torn write. Recovering the readable half of the directory would be
            // the mixed-format recovery the reset rules out.
            bumpTimelineFormatVersion(0);

            final LiveViewCheckpointLifecycle.ReconcileResult result;
            try (Path dir = checkpointsDir()) {
                result = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertTrue(result.isFormatReset());
            Assert.assertFalse(result.isEpochReplaced());
            Assert.assertEquals(-1, result.getWalPurgeFloor());
            Assert.assertEquals(Numbers.LONG_NULL, result.getNormalizedBaseSeqTxn());
            Assert.assertFalse("segments the foreign generation named go with it", segmentExists(false, 4, false));
            Assert.assertFalse("the whole derived directory goes, not just _timeline", checkpointsDirExists());
        });
    }

    @Test
    public void testLegacyFormatArtefactsResetCheckpointDirectory() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            // The retained-ring manifest and a per-checkpoint state file, both at
            // the top level of a directory an earlier development build owned.
            touchTopLevel("_ring");
            touchTopLevel("0000000000000004.cp");

            final LiveViewCheckpointLifecycle.ReconcileResult result;
            try (Path dir = checkpointsDir()) {
                result = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertTrue(result.isFormatReset());
            Assert.assertFalse(checkpointsDirExists());

            // A rebuilt directory holds only current-layout names, so the next
            // reconciliation takes the ordinary path.
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            final LiveViewCheckpointLifecycle.ReconcileResult rebuilt;
            try (Path dir = checkpointsDir()) {
                rebuilt = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertFalse(rebuilt.isFormatReset());
            Assert.assertEquals(1, rebuilt.getWalPurgeFloor());
        });
    }

    @Test
    public void testOrphanCleanupProtectsBothSlotsAndPreservesMonotonicIds() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            publish(1, 1, 7, 0, 10);
            publish(2, 2, 7, 0, 20);

            touchFinal(false, 19);
            touchFinal(true, 19);
            touchFinal(false, 20);
            touchFinal(true, 21);
            touchTemp(false, 22);
            touchTemp(true, 23);

            final LiveViewCheckpointLifecycle.ReconcileResult reconciliation;
            try (Path dir = checkpointsDir()) {
                reconciliation = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertEquals(2, reconciliation.getRemovedOrphanCount());
            Assert.assertEquals(0, reconciliation.getFailedOrphanCount());
            Assert.assertEquals(22, reconciliation.getFinalOrphanUpperBound());
            Assert.assertTrue(segmentExists(false, 19, false));
            Assert.assertTrue(segmentExists(true, 19, false));
            Assert.assertTrue(segmentExists(false, 20, false));
            Assert.assertTrue(segmentExists(true, 21, false));
            Assert.assertFalse(segmentExists(false, 22, true));
            Assert.assertFalse(segmentExists(true, 23, true));

            // Advance the A/B commit point beyond the pre-existing final names.
            // Only after this publication may cleanup unlink that orphan range.
            try (LiveViewCheckpointMetaStore store = openStore()) {
                final LiveViewCheckpointSuperblock superblock = store.getSuperblock();
                superblock.generation = 3;
                superblock.nextSegmentId = 22;
                store.publish();
            }
            final LiveViewCheckpointLifecycle.CleanupStats cleanup;
            try (Path dir = checkpointsDir()) {
                cleanup = LiveViewCheckpointLifecycle.purgeFinalOrphans(
                        configuration,
                        dir,
                        20,
                        reconciliation.getFinalOrphanUpperBound(),
                        true
                );
            }
            Assert.assertEquals(2, cleanup.getRemovedCount());
            Assert.assertEquals(0, cleanup.getFailedCount());
            Assert.assertFalse(segmentExists(false, 20, false));
            Assert.assertFalse(segmentExists(true, 21, false));
            Assert.assertTrue("fallback-protected data survives", segmentExists(false, 19, false));
            Assert.assertTrue("fallback-protected metadata survives", segmentExists(true, 19, false));
        });
    }

    @Test
    public void testPendingRepairCandidateIsDiscardedAndReplanned() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            publish(1, 1, 7, 0, 10);
            // A repair that crashed with its candidate staged: the descriptor is the
            // only record naming the segment it wrote, because no metadata references
            // one until the splice commits the superblock.
            try (
                    LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                    Path dir = checkpointsDir()
            ) {
                state.begin(dir, 31, 7, 0, 2, 31, 30, 1_000, 500, 800, 2_000, HighBoundTag.FINITE);
                state.addOwnedSegmentId(11);
                state.recordStage(RepairPublicationStage.CANDIDATE_ROOTS_AND_RUNTIME_READY);
                state.recordProgress(1_500, 4);
            }
            touchTemp(false, 11);

            final LiveViewCheckpointLifecycle.ReconcileResult reconciliation;
            try (Path dir = checkpointsDir()) {
                reconciliation = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            // The pinned snapshot the candidate was built against cannot be reopened,
            // so it is discarded and replanned rather than resumed.
            Assert.assertEquals(1, reconciliation.getDiscardedRepairCount());
            Assert.assertEquals(0, reconciliation.getFailedRepairCount());
            Assert.assertFalse(repairDescriptorExists(31));
            Assert.assertFalse(segmentExists(false, 11, true));
            // The generation the candidate would have spliced into is untouched: a
            // discarded repair costs the timeline nothing.
            Assert.assertTrue(timelineExists());
            try (LiveViewCheckpointMetaStore store = openStore()) {
                Assert.assertTrue(store.isValid());
                Assert.assertEquals(1, store.getSuperblock().generation);
            }
        });
    }

    @Test
    public void testPrimaryOwnershipAndPinnedRetirement() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);

            try (LiveViewCheckpointMetaStore store = openStore()) {
                final LiveViewCheckpointGenerationPin pin = store.pin();
                try (Path dir = checkpointsDir()) {
                    Assert.assertFalse(LiveViewCheckpointLifecycle.retireTimeline(
                            configuration,
                            dir,
                            store,
                            true
                    ));
                    Assert.assertTrue(timelineExists());
                    Assert.assertFalse("replica retirement must be a no-op",
                            LiveViewCheckpointLifecycle.retireTimeline(configuration, dir, store, false));
                    Assert.assertTrue(timelineExists());
                }
                pin.close();
                try (Path dir = checkpointsDir()) {
                    Assert.assertTrue(LiveViewCheckpointLifecycle.retireTimeline(
                            configuration,
                            dir,
                            store,
                            true
                    ));
                }
            }
            Assert.assertFalse(timelineExists());

            // The writer's ownership gate fires before it creates any files or
            // inspects function state.
            ensureDirs();
            try (LiveViewCheckpointTimelineStoreWriter writer =
                         new LiveViewCheckpointTimelineStoreWriter(configuration);
                 Path dir = checkpointsDir()) {
                try {
                    writer.append(
                            dir, new ObjList<>(), null, 7, 0, 0, 0, 0, false, 1, 0,
                            Numbers.LONG_NULL, Numbers.LONG_NULL
                    );
                    Assert.fail("expected replica publication rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "replica must not publish");
                }
            }
            Assert.assertFalse(timelineExists());
        });
    }

    @Test
    public void testTemporaryOrphanUnlinkRetriesOnNextReconcile() throws Exception {
        final boolean[] failFirstRemove = {true};
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (failFirstRemove[0]) {
                    failFirstRemove[0] = false;
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            ensureDirs();
            touchTemp(false, 3);

            LiveViewCheckpointLifecycle.ReconcileResult first;
            try (Path dir = checkpointsDir()) {
                first = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertEquals(1, first.getFailedOrphanCount());
            Assert.assertTrue(segmentExists(false, 3, true));

            LiveViewCheckpointLifecycle.ReconcileResult second;
            try (Path dir = checkpointsDir()) {
                second = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertEquals(1, second.getRemovedOrphanCount());
            Assert.assertEquals(0, second.getFailedOrphanCount());
            Assert.assertFalse(segmentExists(false, 3, true));
        });
    }

    @Test
    public void testTornSlotIsNotAForeignFormat() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            publish(1, 1, 7, 0, 10); // slot 0
            publish(2, 2, 7, 0, 20); // slot 1

            // A publication writes the magic and the layout version ahead of the
            // checksum, so a slot torn by a crash still identifies itself as this
            // build's. It is a fallback case, not a format case.
            corruptSlotGeneration(1);

            final LiveViewCheckpointLifecycle.ReconcileResult result;
            try (Path dir = checkpointsDir()) {
                result = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertFalse(result.isFormatReset());
            Assert.assertTrue(timelineExists());
            Assert.assertEquals(1, result.getWalPurgeFloor());
            Assert.assertEquals(1, result.getNormalizedBaseSeqTxn());
        });
    }

    private static Path checkpointsDir() {
        return new Path().of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private void bumpTimelineFormatVersion(int slot) {
        withTimelineMemory(mem -> {
            final long base = (long) slot * LiveViewCheckpointSuperblock.SLOT_SIZE;
            mem.putInt(
                    base + LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION_OFFSET,
                    LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION + 1
            );
            mem.putInt(
                    base + LiveViewCheckpointSuperblock.SLOT_CRC_OFFSET,
                    Zip.crc32(0, mem.addressOf(base), LiveViewCheckpointSuperblock.SLOT_CRC_COVERAGE)
            );
        });
    }

    private boolean checkpointsDirExists() {
        try (Path dir = checkpointsDir()) {
            return configuration.getFilesFacade().exists(dir.$());
        }
    }

    private void corruptSlotGeneration(int slot) {
        withTimelineMemory(mem -> {
            final long offset = (long) slot * LiveViewCheckpointSuperblock.SLOT_SIZE
                    + LiveViewCheckpointSuperblock.SLOT_GENERATION_OFFSET;
            mem.putLong(offset, mem.getLong(offset) ^ 0x5A5A_5A5AL);
        });
    }

    private boolean dataDirExists() {
        try (Path dir = checkpointsDir(); Path path = new Path()) {
            return configuration.getFilesFacade().exists(
                    LiveViewCheckpointLayout.dataDirPath(path, dir).$()
            );
        }
    }

    private void ensureDirs() {
        try (Path dir = checkpointsDir(); Path path = new Path()) {
            final FilesFacade ff = configuration.getFilesFacade();
            ff.mkdirs(LiveViewCheckpointLayout.metaDirPath(path, dir).slash(), configuration.getMkDirMode());
            ff.mkdirs(LiveViewCheckpointLayout.dataDirPath(path, dir).slash(), configuration.getMkDirMode());
        }
    }

    private boolean metaDirExists() {
        try (Path dir = checkpointsDir(); Path path = new Path()) {
            return configuration.getFilesFacade().exists(
                    LiveViewCheckpointLayout.metaDirPath(path, dir).$()
            );
        }
    }

    private LiveViewCheckpointMetaStore openStore() {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        try (Path dir = checkpointsDir()) {
            store.of(dir);
        }
        return store;
    }

    private void publish(
            long generation,
            long metadataSegmentId,
            long definitionTxn,
            long historyEpoch,
            long nextSegmentId
    ) {
        final LiveViewCheckpointPageRef directoryRoot = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointMetaSegmentWriter writer =
                     new LiveViewCheckpointMetaSegmentWriter(configuration);
             LiveViewCheckpointMetaStore store = openStore();
             Path dir = checkpointsDir()) {
            writer.of(dir, metadataSegmentId);
            writeEmptySegmentDirectoryRoot(writer, directoryRoot);
            writer.commit();

            final LiveViewCheckpointSuperblock superblock = store.getSuperblock();
            superblock.generation = generation;
            superblock.definitionTxn = definitionTxn;
            superblock.historyEpoch = historyEpoch;
            superblock.normalizedBaseSeqTxn = generation;
            superblock.coveredLvSeqTxn = generation;
            superblock.nextSegmentId = nextSegmentId;
            superblock.segmentDirectoryRootRef.of(
                    directoryRoot.getSegmentId(),
                    directoryRoot.getOffset(),
                    directoryRoot.getLength()
            );
            store.publish();
        }
    }

    /**
     * An empty catalogue is a tree with no entries. These cases publish no data
     * segment at all, so the root they name is an empty leaf.
     */
    private static void writeEmptySegmentDirectoryRoot(
            LiveViewCheckpointMetaSegmentWriter writer,
            LiveViewCheckpointPageRef out
    ) {
        final MemoryA page = writer.beginPage(LiveViewCheckpointSegmentDirectory.PAGE_KIND_LEAF);
        page.putInt(0);
        writer.endPage(out);
    }

    private boolean repairDescriptorExists(long repairId) {
        try (Path dir = checkpointsDir(); Path path = new Path()) {
            LiveViewCheckpointLayout.repairDescriptorPath(path, dir, repairId);
            return configuration.getFilesFacade().exists(path.$());
        }
    }

    private boolean segmentExists(boolean metadata, long segmentId, boolean temporary) {
        try (Path dir = checkpointsDir(); Path path = new Path()) {
            if (metadata) {
                if (temporary) {
                    LiveViewCheckpointLayout.metaSegmentTmpPath(path, dir, segmentId);
                } else {
                    LiveViewCheckpointLayout.metaSegmentPath(path, dir, segmentId);
                }
            } else if (temporary) {
                LiveViewCheckpointLayout.dataSegmentTmpPath(path, dir, segmentId);
            } else {
                LiveViewCheckpointLayout.dataSegmentPath(path, dir, segmentId);
            }
            return configuration.getFilesFacade().exists(path.$());
        }
    }

    private void touchFinal(boolean metadata, long segmentId) {
        touchSegment(metadata, segmentId, false);
    }

    private void touchSegment(boolean metadata, long segmentId, boolean temporary) {
        try (Path dir = checkpointsDir(); Path path = new Path()) {
            if (metadata) {
                if (temporary) {
                    LiveViewCheckpointLayout.metaSegmentTmpPath(path, dir, segmentId);
                } else {
                    LiveViewCheckpointLayout.metaSegmentPath(path, dir, segmentId);
                }
            } else if (temporary) {
                LiveViewCheckpointLayout.dataSegmentTmpPath(path, dir, segmentId);
            } else {
                LiveViewCheckpointLayout.dataSegmentPath(path, dir, segmentId);
            }
            Assert.assertTrue(configuration.getFilesFacade().touch(path.$()));
        }
    }

    private void touchTemp(boolean metadata, long segmentId) {
        touchSegment(metadata, segmentId, true);
    }

    private void touchTopLevel(CharSequence name) {
        try (Path dir = checkpointsDir(); Path path = new Path()) {
            path.of(dir).concat(name);
            Assert.assertTrue(configuration.getFilesFacade().touch(path.$()));
        }
    }

    private boolean timelineExists() {
        try (Path dir = checkpointsDir(); Path path = new Path()) {
            return configuration.getFilesFacade().exists(
                    LiveViewCheckpointLayout.timelinePath(path, dir).$()
            );
        }
    }

    private void withTimelineMemory(TimelineMutation mutation) {
        try (Path dir = checkpointsDir(); Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
            mem.smallFile(
                    configuration.getFilesFacade(),
                    LiveViewCheckpointLayout.timelinePath(path, dir).$(),
                    MemoryTag.MMAP_DEFAULT
            );
            mutation.apply(mem);
        }
    }

    @FunctionalInterface
    private interface TimelineMutation {
        void apply(MemoryCMARW mem);
    }
}
