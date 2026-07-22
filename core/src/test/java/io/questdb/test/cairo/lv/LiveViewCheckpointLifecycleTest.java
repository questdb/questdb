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
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
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
                    writer.append(dir, new ObjList<>(), null, 7, 0, 0, 0, 0, false, 1, 0, Numbers.LONG_NULL);
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

    private static Path checkpointsDir() {
        return new Path().of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
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
        try (LiveViewCheckpointSegmentDirectory directory =
                     new LiveViewCheckpointSegmentDirectory(configuration);
             LiveViewCheckpointMetaSegmentWriter writer =
                     new LiveViewCheckpointMetaSegmentWriter(configuration);
             LiveViewCheckpointMetaStore store = openStore();
             Path dir = checkpointsDir()) {
            writer.of(dir, metadataSegmentId);
            directory.writeTo(writer, directoryRoot);
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

    private boolean timelineExists() {
        try (Path dir = checkpointsDir(); Path path = new Path()) {
            return configuration.getFilesFacade().exists(
                    LiveViewCheckpointLayout.timelinePath(path, dir).$()
            );
        }
    }
}
