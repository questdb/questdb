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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.HighBoundTag;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.RepairPublicationStage;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointLifecycle;
import io.questdb.cairo.lv.LiveViewCheckpointMetaSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewCheckpointRepairState;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryWriter;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Zip;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.CRC32;

public class LiveViewCheckpointLifecycleTest extends AbstractCairoTest {

    private static final LongList EMPTY_SEGMENT_IDS = new LongList();
    // The shapes of a repair marker an older history can leave beside the next one; see
    // writeLeftoverMarker.
    private static final String LEFTOVER_MARKER_BASE_5 = "a record naming base generation 5 and a seqTxn";
    private static final String LEFTOVER_MARKER_BOTH_NAMES = "a record naming base generation 5 beside an empty staged sibling";
    private static final String LEFTOVER_MARKER_NO_BASE = "a record naming no base generation";
    private static final String LEFTOVER_MARKER_STAGED_ALONE = "a staged record alone";
    private static final String LEFTOVER_MARKER_TORN = "a record that fails its checksum";
    private static final String LEFTOVER_MARKER_VERSION_1 = "a version 1 record";
    private static final String[] LEFTOVER_MARKER_SHAPES = {
            LEFTOVER_MARKER_BASE_5,
            LEFTOVER_MARKER_NO_BASE,
            LEFTOVER_MARKER_TORN,
            LEFTOVER_MARKER_VERSION_1,
            LEFTOVER_MARKER_STAGED_ALONE,
            LEFTOVER_MARKER_BOTH_NAMES
    };
    private static final long LIFECYCLE_IDENTITY = 101;
    private static final String LV_DIR = "lv_checkpoint_lifecycle";
    // The generation and view seqTxn the repair marker of writeRetireFixture names.
    private static final long MARKER_GENERATION = 1;
    private static final long MARKER_LV_SEQ_TXN = 17;

    @Test
    public void testADamagedMagicNibbleInOneSlotFallsBackToTheOther() throws Exception {
        assertMemoryLeak(() -> assertOneDamagedFormatFieldFallsBack(LiveViewCheckpointSuperblock.SLOT_MAGIC_OFFSET));
    }

    @Test
    public void testADamagedVersionFieldInOneSlotFallsBackToTheOther() throws Exception {
        assertMemoryLeak(() -> assertOneDamagedFormatFieldFallsBack(LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION_OFFSET));
    }

    @Test
    public void testADeclaredVersionBesideAnIntactSlotStillBlocks() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            publish(1, 1, 7, 0, 10); // slot 0
            publish(2, 2, 7, 0, 20); // slot 1

            // What a newer build leaves after one publication over this build's directory:
            // its own slot, naming its version in both fields that carry one, beside this
            // build's older slot. The intact slot is no licence to fall back. Adopting it
            // would hand the newer generation's files to the orphan sweep and its slot to
            // the next publication, destroying what a return to that build needs.
            bumpTimelineFormatVersion(1);

            final LiveViewCheckpointLifecycle.ReconcileResult result;
            try (Path dir = checkpointsDir()) {
                result = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertTrue(result.isFormatBlocked());
            Assert.assertFalse("a newer version is a rollback, not an upgrade", result.isFormatUpgrade());
            Assert.assertFalse(result.isFormatReset());
            Assert.assertEquals(
                    LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION + 1,
                    result.getForeignFormatVersion()
            );
            Assert.assertNull("the intact slot's generation must not be adopted either", result.getStats());
            Assert.assertTrue(segmentExists(true, 1, false));
            Assert.assertTrue(
                    "the newer generation's segment is the newer build's to explain",
                    segmentExists(true, 2, false)
            );
        });
    }

    @Test
    public void testAppendBesideAChecksumValidSlotWithUnusableRootsKeepsItsMarker() throws Exception {
        // Both generations name a timeline root page that fails validation, so the store selects no
        // generation, but both slots pass their checksum: the history the marker guards is still on
        // disk, and a restart judges the marker against it. The append opens no history there - the
        // publication gate refuses generation 1 below the checksum-valid slots, as it always has -
        // and leaves the marker as it is.
        final RetireRemovalFault ff = new RetireRemovalFault();
        assertMemoryLeak(ff, () -> {
            ensureDirs();
            try (LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration)) {
                Assert.assertEquals(1, appendRoot(writer, 10, Numbers.LONG_NULL));
                final LiveViewCheckpointPageRef firstRoot = timelineRootRef();
                Assert.assertEquals(2, appendRoot(writer, 20, Numbers.LONG_NULL));
                final LiveViewCheckpointPageRef secondRoot = timelineRootRef();
                corruptMetaPage(firstRoot);
                corruptMetaPage(secondRoot);
                try (LiveViewCheckpointMetaStore store = openStore()) {
                    Assert.assertFalse("no generation passes its root validation", store.isValid());
                    Assert.assertEquals("both slots pass their checksum", 1, store.getSuperblock().getOldestValidGeneration());
                }
                try (Path dir = checkpointsDir()) {
                    LiveViewCheckpointRepairMarker.write(configuration, dir, 7, 0, 2, 1_000, MARKER_LV_SEQ_TXN);
                }
                ff.refuseMarkerUnlinks(true);
                try {
                    appendRoot(writer, 30, Numbers.LONG_NULL);
                    Assert.fail("a generation below the checksum-valid slots must not publish");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "live view checkpoint generation must advance [current=2, next=1]");
                }
                Assert.assertEquals("no unlink of the marker was attempted", 0, ff.getRefusedUnlinks());
                assertRepairMarkerReads(2, MARKER_LV_SEQ_TXN);
            }
        });
    }

    @Test
    public void testAppendBesideAValidGenerationKeepsItsMarker() throws Exception {
        // A marker beside a valid generation is the repair's own, whatever the append that follows
        // it: no seal, runtime or seed, removes it or tries to.
        final RetireRemovalFault ff = new RetireRemovalFault();
        assertMemoryLeak(ff, () -> {
            for (long seedCursorOffset : new long[]{Numbers.LONG_NULL, 5}) {
                removeCheckpointsDir();
                ensureDirs();
                try (LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration)) {
                    Assert.assertEquals(1, appendRoot(writer, 10, seedCursorOffset));
                    try (Path dir = checkpointsDir()) {
                        LiveViewCheckpointRepairMarker.write(configuration, dir, 7, 0, 1, 1_000, MARKER_LV_SEQ_TXN);
                    }
                    Assert.assertEquals(2, appendRoot(writer, 20, nextSeedCursorOffset(seedCursorOffset)));
                    assertRepairMarkerReads(1, MARKER_LV_SEQ_TXN);

                    ff.refuseMarkerUnlinks(true);
                    Assert.assertEquals(3, appendRoot(writer, 30, nextSeedCursorOffset(seedCursorOffset)));
                    ff.refuseMarkerUnlinks(false);
                    Assert.assertEquals("no unlink of the marker was attempted", 0, ff.getRefusedUnlinks());
                    assertRepairMarkerReads(1, MARKER_LV_SEQ_TXN);
                }
            }
        });
    }

    @Test
    public void testAppendOverATimelineWithNoValidSlotOpensAHistoryWithoutTheMarker() throws Exception {
        // A _timeline whose slots are zeroed, or both fail their checksum, holds no history, so the
        // append over it opens one, and removes the marker an older history left first.
        assertMemoryLeak(() -> {
            for (boolean isTorn : new boolean[]{false, true}) {
                removeCheckpointsDir();
                ensureDirs();
                try (LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration)) {
                    if (isTorn) {
                        Assert.assertEquals(1, appendRoot(writer, 10, Numbers.LONG_NULL));
                        Assert.assertEquals(2, appendRoot(writer, 20, Numbers.LONG_NULL));
                        withTimelineMemory(mem -> {
                            for (int slot = 0; slot < 2; slot++) {
                                final long offset = (long) slot * LiveViewCheckpointSuperblock.SLOT_SIZE + LiveViewCheckpointSuperblock.SLOT_CRC_OFFSET;
                                mem.putByte(offset, (byte) (mem.getByte(offset) ^ 1));
                            }
                        });
                    } else {
                        try (LiveViewCheckpointMetaStore store = openStore()) {
                            Assert.assertFalse(store.isValid());
                        }
                        Assert.assertTrue("the open left a zero-filled _timeline", timelineExists());
                    }
                    try (LiveViewCheckpointMetaStore store = openStore()) {
                        Assert.assertEquals("no slot passes its checksum", Long.MAX_VALUE, store.getSuperblock().getOldestValidGeneration());
                    }
                    writeLeftoverMarker(LEFTOVER_MARKER_NO_BASE);
                    Assert.assertEquals(1, appendRoot(writer, 30, Numbers.LONG_NULL));
                }
                assertSeedCursorOffset(Numbers.LONG_NULL);
                Assert.assertFalse("the history opened without the marker", isRepairMarkerFilePresent(false));
                Assert.assertFalse(isRepairMarkerFilePresent(true));
            }
        });
    }

    @Test
    public void testAppendThatOpensAHistoryRemovesALeftoverMarker() throws Exception {
        // A marker beside no generation was written for a history that is gone: whatever its shape,
        // the append that opens the next history removes it, a seed opening as a runtime one, so
        // the history opens without it.
        assertMemoryLeak(() -> {
            for (String shape : LEFTOVER_MARKER_SHAPES) {
                for (long seedCursorOffset : new long[]{Numbers.LONG_NULL, 7}) {
                    removeCheckpointsDir();
                    ensureDirs();
                    writeLeftoverMarker(shape);
                    try (LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration)) {
                        Assert.assertEquals(shape, 1, appendRoot(writer, 10, seedCursorOffset));
                    }
                    assertSeedCursorOffset(seedCursorOffset);
                    Assert.assertFalse(shape + ": the marker is gone", isRepairMarkerFilePresent(false));
                    Assert.assertFalse(shape + ": and its staged sibling", isRepairMarkerFilePresent(true));
                }
            }
        });
    }

    @Test
    public void testAppendWithInvalidCoordinatesLeavesTheMarker() throws Exception {
        // An append that refuses its input refuses it before anything else, the marker removal
        // included.
        assertMemoryLeak(() -> {
            ensureDirs();
            writeLeftoverMarker(LEFTOVER_MARKER_BOTH_NAMES);
            try (LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration)) {
                appendRoot(writer, 10, -1);
                Assert.fail("a negative seed cursor that is not LONG_NULL must refuse");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid live view normal checkpoint coordinates");
            }
            Assert.assertTrue(isRepairMarkerFilePresent(false));
            Assert.assertTrue(isRepairMarkerFilePresent(true));
            assertRepairMarkerReads(5, MARKER_LV_SEQ_TXN);
        });
    }

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
    public void testForeignTimelineMagicWithoutADeclaredVersionStillResets() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            touchFinal(false, 4);

            // A magic outside what this build writes, over a version field that
            // still reads native. Nothing here declares a format to preserve the
            // directory for, so this stays what it has always been: an unreadable
            // directory of derived state, removed and rebuilt.
            withTimelineMemory(mem -> {
                mem.putLong(
                        LiveViewCheckpointSuperblock.SLOT_MAGIC_OFFSET,
                        LiveViewCheckpointSuperblock.SLOT_MAGIC + 1
                );
                mem.putInt(
                        LiveViewCheckpointSuperblock.SLOT_CRC_OFFSET,
                        Zip.crc32(0, mem.addressOf(0), LiveViewCheckpointSuperblock.SLOT_CRC_COVERAGE)
                );
            });

            final LiveViewCheckpointLifecycle.ReconcileResult result;
            try (Path dir = checkpointsDir()) {
                result = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertTrue(result.isFormatReset());
            Assert.assertFalse(result.isFormatBlocked());
            Assert.assertFalse(result.isFormatUpgrade());
            Assert.assertEquals(
                    LiveViewCheckpointSuperblock.NO_FOREIGN_FORMAT,
                    result.getForeignFormatVersion()
            );
            Assert.assertFalse("the whole derived directory goes, not just _timeline", checkpointsDirExists());
        });
    }

    @Test
    public void testForeignTimelineVersionBlocksAndLeavesEverythingWhereItIs() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            touchFinal(false, 4);
            // A repair that crashed with its candidate staged. The sweep that
            // discards one runs below the format gate, so a blocked reconciliation
            // must not reach it either: those bytes are the other build's too.
            try (
                    LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                    Path dir = checkpointsDir()
            ) {
                state.begin(dir, 31, 7, 0, 2, 31, 30, 1_000, 500, 800, 2_000, HighBoundTag.FINITE);
                state.addOwnedSegmentId(11);
                state.recordStage(RepairPublicationStage.CANDIDATE_ROOTS_AND_RUNTIME_READY);
                state.recordProgress(1_500, 4);
            }
            Assert.assertTrue(timelineExists());

            // A slot naming a layout version this build does not write, in both
            // fields that carry one and under a checksum that agrees with its body: a
            // real generation another build owns, not a torn write or a flipped bit.
            // It announces itself, which is what separates it from damage - and what
            // this build cannot do is prove that rebuilding the view from today's
            // surviving source rows reproduces the output those roots stand for.
            bumpTimelineFormatVersion(0);

            final LiveViewCheckpointLifecycle.ReconcileResult result;
            try (Path dir = checkpointsDir()) {
                result = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertTrue(result.isFormatBlocked());
            Assert.assertFalse("a newer version is a rollback, not an upgrade", result.isFormatUpgrade());
            Assert.assertFalse("blocking is the opposite disposition to resetting", result.isFormatReset());
            Assert.assertEquals(
                    LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION + 1,
                    result.getForeignFormatVersion()
            );
            Assert.assertFalse(result.isEpochReplaced());
            Assert.assertEquals(-1, result.getWalPurgeFloor());
            Assert.assertEquals(Numbers.LONG_NULL, result.getNormalizedBaseSeqTxn());
            Assert.assertNull("no generation may be adopted out of a layout this build cannot read", result.getStats());

            // Every count is zero because nothing was walked, not because nothing
            // was found: the orphan below the ceiling, the staged repair segment and
            // the descriptor naming it are all still there.
            Assert.assertEquals(0, result.getRemovedOrphanCount());
            Assert.assertEquals(0, result.getPurgedSegmentCount());
            Assert.assertEquals(0, result.getDiscardedRepairCount());
            Assert.assertTrue(checkpointsDirExists());
            Assert.assertTrue(timelineExists());
            Assert.assertTrue(metaDirExists());
            Assert.assertTrue(dataDirExists());
            Assert.assertTrue("an uncatalogued name is the other build's to explain", segmentExists(false, 4, false));
            Assert.assertTrue(repairDescriptorExists(31));

            // The disposition is read off the superblock rather than consumed, so a
            // second pass - a restart, a retrying publication - reaches it again.
            final LiveViewCheckpointLifecycle.ReconcileResult second;
            try (Path dir = checkpointsDir()) {
                second = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertTrue(second.isFormatBlocked());
            Assert.assertTrue(checkpointsDirExists());
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
    public void testLeftoverStagedRetirementQueueKeepsTheCheckpointDirectory() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            // What a crash between the staged write of the retirement work set and its rename
            // leaves: a current-layout name, unlike the legacy artefacts above.
            touchTopLevel(LiveViewCheckpointLayout.RETIREMENT_QUEUE_TMP_FILE_NAME);

            final LiveViewCheckpointLifecycle.ReconcileResult result;
            try (Path dir = checkpointsDir()) {
                result = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertFalse("a staged work set must not reset the directory", result.isFormatReset());
            Assert.assertFalse(result.isFormatBlocked());
            Assert.assertTrue(timelineExists());
            Assert.assertEquals(1, result.getWalPurgeFloor());
        });
    }

    @Test
    public void testOrphanCleanupCollectsWhatNoGenerationCatalogues() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            touchFinal(false, 5);
            // Two generations of a real catalogue. Generation 2 registers the
            // directory segment generation 1 left pending, then supersedes its pages,
            // so that entry ends at a zero reference count against a retirement
            // generation the fallback slot has not reached - the shape the purge
            // sweep defers.
            publishCatalogue(1, 1, 7, 10, segmentIds(5), segmentIds());
            publishCatalogue(2, 2, 7, 20, segmentIds(), segmentIds());

            // Everything else in the two directories is a file no generation ever
            // catalogued: below the older slot's ceiling, between the two, and above
            // both. The id-ceiling rule tells those three apart and defers all of
            // them; the catalogue rule does not have to, and defers none.
            touchFinal(false, 9);
            touchFinal(true, 9);
            touchFinal(false, 19);
            touchFinal(true, 21);
            touchTemp(false, 22);
            touchTemp(true, 23);

            final LiveViewCheckpointLifecycle.ReconcileResult reconciliation;
            try (Path dir = checkpointsDir()) {
                reconciliation = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertEquals(0, reconciliation.getFailedOrphanCount());
            Assert.assertEquals("four uncatalogued final names and two temporaries",
                    6, reconciliation.getRemovedOrphanCount());
            Assert.assertFalse(segmentExists(false, 9, false));
            Assert.assertFalse(segmentExists(true, 9, false));
            Assert.assertFalse(segmentExists(false, 19, false));
            Assert.assertFalse(segmentExists(true, 21, false));
            Assert.assertFalse(segmentExists(false, 22, true));
            Assert.assertFalse(segmentExists(true, 23, true));

            Assert.assertTrue("a referenced segment survives", segmentExists(false, 5, false));
            Assert.assertTrue("a fallback-protected entry survives", segmentExists(true, 1, false));
            Assert.assertTrue("the pending directory segment survives", segmentExists(true, 2, false));

            // And the deferred rule is left with nothing: it records a range only
            // where no catalogue could answer, and here one did.
            Assert.assertEquals(20, reconciliation.getFinalOrphanUpperBound());
        });
    }

    @Test
    public void testOrphanCleanupDefersWhereNoGenerationCanAnswer() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            // No _timeline, so no catalogue exists to name anything and the deferred
            // id-ceiling rule is the only one left. Its ceiling is zero, which makes
            // every final name an orphan - and it still removes none of them until a
            // publication has durably advanced past their ids, which is what keeps
            // allocation monotonic across the crash this shape comes from.
            touchFinal(false, 20);
            touchFinal(true, 21);
            touchTemp(false, 22);

            final LiveViewCheckpointLifecycle.ReconcileResult reconciliation;
            try (Path dir = checkpointsDir()) {
                reconciliation = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertEquals("only the temporary goes now", 1, reconciliation.getRemovedOrphanCount());
            Assert.assertEquals(0, reconciliation.getFailedOrphanCount());
            Assert.assertEquals(22, reconciliation.getFinalOrphanUpperBound());
            Assert.assertTrue(segmentExists(false, 20, false));
            Assert.assertTrue(segmentExists(true, 21, false));
            Assert.assertFalse(segmentExists(false, 22, true));

            // The publication allocates above the recorded bound, exactly as a seal
            // does with what reconciliation handed it, and only then may the rule
            // unlink the range below.
            publish(1, 22, 7, 0, 23);
            final LiveViewCheckpointLifecycle.CleanupStats cleanup;
            try (Path dir = checkpointsDir()) {
                cleanup = LiveViewCheckpointLifecycle.purgeFinalOrphans(
                        configuration,
                        dir,
                        0,
                        reconciliation.getFinalOrphanUpperBound(),
                        true
                );
            }
            Assert.assertEquals(2, cleanup.getRemovedCount());
            Assert.assertEquals(0, cleanup.getFailedCount());
            Assert.assertFalse(segmentExists(false, 20, false));
            Assert.assertFalse(segmentExists(true, 21, false));
            Assert.assertTrue("the publication's own segment sits above the bound", segmentExists(true, 22, false));
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
                            dir, new ObjList<>(), null, 7, 0, 0, 0, 0, LIFECYCLE_IDENTITY, false, 1, 0,
                            Numbers.LONG_NULL, Numbers.LONG_NULL, null
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
    public void testPurgeThatCannotLoadTheRetirementQueueKeepsTheAdoptedFloor() throws Exception {
        // The queue load maps _retirements read-write, so an mmap the kernel refuses - an
        // exhausted vm.max_map_count, say - fails the sweep before it reclaims anything.
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            try (Path dir = checkpointsDir()) {
                // A clean pass leaves the queue the next pass loads.
                Assert.assertFalse(LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true).isPurgeFailed());
                Assert.assertTrue(isRetirementQueuePresent(false));

                fault.arm(LiveViewCheckpointDiskFullFault.OP_MMAP, LiveViewCheckpointLayout.RETIREMENT_QUEUE_FILE_NAME, 1);
                final LiveViewCheckpointLifecycle.ReconcileResult result =
                        LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
                Assert.assertEquals(LiveViewCheckpointLayout.RETIREMENT_QUEUE_FILE_NAME, fault.getFailedFile());
                assertAdoptedThroughAFailedPurge(result);
            }
        });
    }

    @Test
    public void testPurgeThatCannotRewriteTheRetirementQueueKeepsTheAdoptedFloor() throws Exception {
        // A full disk fails the reserve of the staged _retirements image. The sweep has
        // already unlinked what it could, and the queue it could not prune is a superset the
        // next sweep re-checks against the catalogue, so only the reclamation is deferred.
        final LiveViewCheckpointDiskFullFault fault = new LiveViewCheckpointDiskFullFault();
        assertMemoryLeak(fault, () -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            try (Path dir = checkpointsDir()) {
                fault.arm(LiveViewCheckpointDiskFullFault.OP_ALLOCATE, LiveViewCheckpointLayout.RETIREMENT_QUEUE_FILE_NAME, 1);
                final LiveViewCheckpointLifecycle.ReconcileResult result =
                        LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
                Assert.assertEquals(LiveViewCheckpointLayout.RETIREMENT_QUEUE_TMP_FILE_NAME, fault.getFailedFile());
                assertAdoptedThroughAFailedPurge(result);
                Assert.assertFalse("the failed rewrite must not publish a queue", isRetirementQueuePresent(false));

                // The next pass retries the sweep, and its rewrite consumes the staged file the
                // failed one left behind.
                final LiveViewCheckpointLifecycle.ReconcileResult retry =
                        LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
                Assert.assertFalse(retry.isPurgeFailed());
                Assert.assertEquals(1, retry.getWalPurgeFloor());
                Assert.assertTrue(isRetirementQueuePresent(false));
                Assert.assertFalse(isRetirementQueuePresent(true));
            }
        });
    }

    @Test
    public void testRetireKeepsTheRepairMarkerBesideATimelineItCannotRemove() throws Exception {
        // A _timeline the retire cannot unlink is a generation a restart can still restore, and the
        // repair marker beside it is what holds that restore off. The retire removes everything else
        // and keeps the marker and its staged sibling. The retire that removes _timeline takes both.
        final RetireRemovalFault ff = new RetireRemovalFault();
        assertMemoryLeak(ff, () -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            writeRetireFixture();
            ff.refuseUnlinkOf(LiveViewCheckpointLayout.TIMELINE_FILE_NAME);
            try (Path dir = checkpointsDir()) {
                Assert.assertFalse(LiveViewCheckpointLifecycle.retireTimeline(configuration, dir, null, true));
            }
            Assert.assertTrue(timelineExists());
            Assert.assertTrue("the marker stays", isRepairMarkerFilePresent(false));
            Assert.assertTrue("and its staged sibling", isRepairMarkerFilePresent(true));
            assertRepairMarkerReads(MARKER_GENERATION, MARKER_LV_SEQ_TXN);
            assertRetireRemovedAllButTheTimelineAndTheMarker();

            ff.refuseUnlinkOf(null);
            try (Path dir = checkpointsDir()) {
                Assert.assertTrue(LiveViewCheckpointLifecycle.retireTimeline(configuration, dir, null, true));
            }
            Assert.assertFalse(timelineExists());
            Assert.assertFalse(isRepairMarkerFilePresent(false));
            Assert.assertFalse(isRepairMarkerFilePresent(true));
            assertRepairMarkerReads(Numbers.LONG_NULL, Numbers.LONG_NULL);
        });
    }

    @Test
    public void testRetireRemovesTheRepairMarkerOnceTheTimelineIsGone() throws Exception {
        // The segment trees a retire cannot remove hold no generation a restart could restore
        // without _timeline, so once _timeline is gone the marker has nothing left to guard.
        final RetireRemovalFault ff = new RetireRemovalFault();
        assertMemoryLeak(ff, () -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            writeRetireFixture();
            ff.refuseTreeRemovals(true);
            try (Path dir = checkpointsDir()) {
                Assert.assertFalse(LiveViewCheckpointLifecycle.retireTimeline(configuration, dir, null, true));
            }
            Assert.assertFalse(timelineExists());
            Assert.assertFalse(isRepairMarkerFilePresent(false));
            Assert.assertFalse(isRepairMarkerFilePresent(true));
            Assert.assertFalse(isRetirementQueuePresent(false));
            Assert.assertFalse(isRetirementQueuePresent(true));
            Assert.assertTrue(metaDirExists());
            Assert.assertTrue(dataDirExists());
            Assert.assertTrue(repairDirExists());
        });
    }

    @Test
    public void testRetireTimelineIntoTheCallersPathRemovesWhatTheAllocatingFormRemoves() throws Exception {
        // The retire that builds its names in the caller's Path removes what the allocating form
        // removes, keeps what it keeps beside a _timeline it cannot remove, leaves the directory's
        // Path as it was, and opens no Path and allocates nothing up to its unlinks of _timeline and
        // of the staged marker. Both forms of the marker removal take both names.
        final RetireRemovalFault ff = new RetireRemovalFault();
        assertMemoryLeak(ff, () -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            writeRetireFixture();
            try (Path dir = checkpointsDir(); Path path = new Path()) {
                final String dirName = dir.toString();
                ff.sampleTheNextUnlinks();
                final long pathBytes = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PATH);
                final long allocations = Unsafe.getMallocCount() + Unsafe.getReallocCount();
                Assert.assertTrue(LiveViewCheckpointLifecycle.retireTimeline(configuration, dir, null, true, path));
                Assert.assertEquals("no Path stands open at the unlink of _timeline", pathBytes, ff.getPathBytesAtTheTimelineUnlink());
                Assert.assertEquals("nothing allocates up to it", allocations, ff.getAllocationsAtTheTimelineUnlink());
                Assert.assertEquals("nor at the unlink of the staged marker", pathBytes, ff.getPathBytesAtTheStagedMarkerUnlink());
                Assert.assertEquals("nor up to it", allocations, ff.getAllocationsAtTheStagedMarkerUnlink());
                Assert.assertEquals("the directory's Path stays as it was", dirName, dir.toString());
            }
            Assert.assertFalse(timelineExists());
            Assert.assertFalse(isRepairMarkerFilePresent(false));
            Assert.assertFalse(isRepairMarkerFilePresent(true));
            assertRetireRemovedAllButTheTimelineAndTheMarker();

            ensureDirs();
            publish(1, 1, 7, 0, 5);
            writeRetireFixture();
            ff.refuseUnlinkOf(LiveViewCheckpointLayout.TIMELINE_FILE_NAME);
            try (Path dir = checkpointsDir(); Path path = new Path()) {
                Assert.assertFalse(LiveViewCheckpointLifecycle.retireTimeline(configuration, dir, null, true, path));
            } finally {
                ff.refuseUnlinkOf(null);
            }
            Assert.assertTrue(timelineExists());
            Assert.assertTrue("the marker stays", isRepairMarkerFilePresent(false));
            Assert.assertTrue("and its staged sibling", isRepairMarkerFilePresent(true));
            assertRepairMarkerReads(MARKER_GENERATION, MARKER_LV_SEQ_TXN);
            assertRetireRemovedAllButTheTimelineAndTheMarker();

            try (Path dir = checkpointsDir(); Path path = new Path()) {
                Assert.assertTrue(LiveViewCheckpointLifecycle.removeRepairMarker(configuration, dir, path));
                Assert.assertFalse(isRepairMarkerFilePresent(false));
                Assert.assertFalse(isRepairMarkerFilePresent(true));

                writeRetireFixture();
                Assert.assertTrue(LiveViewCheckpointLifecycle.removeRepairMarker(configuration, dir));
                Assert.assertFalse(isRepairMarkerFilePresent(false));
                Assert.assertFalse(isRepairMarkerFilePresent(true));
            }
        });
    }

    @Test
    public void testRetireTimelineIntoTheCallersPathReportsEachNameItCannotRemove() throws Exception {
        // A name the retire cannot unlink fails the retire, stays, and is logged, and the retire
        // removes every other name all the same: the staged repair marker once _timeline is gone, and
        // either name of the retirement work set. With the fault gone a retire removes what is left.
        final RetireRemovalFault ff = new RetireRemovalFault();
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(ff, () -> {
                final String[] refusedNames = {
                        LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME + LiveViewCheckpointLayout.TMP_SUFFIX,
                        LiveViewCheckpointLayout.RETIREMENT_QUEUE_FILE_NAME,
                        LiveViewCheckpointLayout.RETIREMENT_QUEUE_TMP_FILE_NAME
                };
                for (int i = 0; i < refusedNames.length; i++) {
                    final String refusedName = refusedNames[i];
                    ensureDirs();
                    publish(1, 1, 7, 0, 5);
                    writeRetireFixture();
                    final int refusedBefore = ff.getRefusedUnlinks();
                    ff.refuseUnlinkOf(refusedName);
                    try (Path dir = checkpointsDir(); Path path = new Path()) {
                        Assert.assertFalse(refusedName, LiveViewCheckpointLifecycle.retireTimeline(configuration, dir, null, true, path));
                    } finally {
                        ff.refuseUnlinkOf(null);
                    }
                    Assert.assertEquals(refusedName, refusedBefore + 1, ff.getRefusedUnlinks());
                    capture.drain();
                    Assert.assertEquals(
                            refusedName,
                            i + 1,
                            capture.countOccurrences("could not remove live view checkpoint lifecycle file [path=")
                    );
                    Assert.assertFalse(refusedName, timelineExists());
                    Assert.assertFalse(refusedName, isRepairMarkerFilePresent(false));
                    Assert.assertEquals(refusedName, i == 0, isRepairMarkerFilePresent(true));
                    Assert.assertEquals(refusedName, i == 1, isRetirementQueuePresent(false));
                    Assert.assertEquals(refusedName, i == 2, isRetirementQueuePresent(true));
                    Assert.assertFalse(refusedName, metaDirExists());
                    Assert.assertFalse(refusedName, dataDirExists());
                    Assert.assertFalse(refusedName, repairDirExists());

                    try (Path dir = checkpointsDir(); Path path = new Path()) {
                        Assert.assertTrue(refusedName, LiveViewCheckpointLifecycle.retireTimeline(configuration, dir, null, true, path));
                    }
                    Assert.assertFalse(refusedName, isRepairMarkerFilePresent(true));
                    Assert.assertFalse(refusedName, isRetirementQueuePresent(false));
                    Assert.assertFalse(refusedName, isRetirementQueuePresent(true));
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testRetireTimelineRefusesItsDirectoryAsItsPath() throws Exception {
        // Pointing a Path at itself keeps the name it held, so the retire refuses a caller's Path
        // that is the checkpoint directory's own, ahead of anything it removes.
        assertMemoryLeak(() -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            writeRetireFixture();
            boolean isRefused = false;
            try (Path dir = checkpointsDir()) {
                try {
                    LiveViewCheckpointLifecycle.retireTimeline(configuration, dir, null, true, dir);
                } catch (AssertionError e) {
                    isRefused = true;
                }
            }
            Assert.assertTrue("the retire refuses the directory's own Path", isRefused);
            Assert.assertTrue(timelineExists());
            Assert.assertTrue(isRepairMarkerFilePresent(false));
            Assert.assertTrue(isRepairMarkerFilePresent(true));
            Assert.assertTrue(isRetirementQueuePresent(false));
            Assert.assertTrue(isRetirementQueuePresent(true));
            Assert.assertTrue(metaDirExists());
            Assert.assertTrue(dataDirExists());
            Assert.assertTrue(repairDirExists());
        });
    }

    @Test
    public void testRetireThatCannotRemoveTheMarkerLeavesItWithoutTheTimeline() throws Exception {
        // _timeline goes first, so a marker name the retire cannot unlink - the published record or
        // its staged sibling - stays behind with no generation to guard, and the other name goes.
        final RetireRemovalFault ff = new RetireRemovalFault();
        assertMemoryLeak(ff, () -> {
            for (boolean isStagedRefused : new boolean[]{false, true}) {
                ensureDirs();
                publish(1, 1, 7, 0, 5);
                writeRetireFixture();
                ff.refuseUnlinkOf(isStagedRefused
                        ? LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME + LiveViewCheckpointLayout.TMP_SUFFIX
                        : LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME);
                try (Path dir = checkpointsDir()) {
                    Assert.assertFalse(LiveViewCheckpointLifecycle.retireTimeline(configuration, dir, null, true));
                }
                Assert.assertFalse(timelineExists());
                Assert.assertEquals("only the refused name stays", !isStagedRefused, isRepairMarkerFilePresent(false));
                Assert.assertEquals("only the refused name stays", isStagedRefused, isRepairMarkerFilePresent(true));
                if (isStagedRefused) {
                    // The empty staged sibling is the only record left, and names no generation.
                    assertRepairMarkerReads(Numbers.LONG_NULL, Numbers.LONG_NULL);
                } else {
                    assertRepairMarkerReads(MARKER_GENERATION, MARKER_LV_SEQ_TXN);
                }
                assertRetireRemovedAllButTheTimelineAndTheMarker();

                // With the fault gone a retire removes what is left.
                ff.refuseUnlinkOf(null);
                try (Path dir = checkpointsDir()) {
                    Assert.assertTrue(LiveViewCheckpointLifecycle.retireTimeline(configuration, dir, null, true));
                }
                Assert.assertFalse(isRepairMarkerFilePresent(false));
                Assert.assertFalse(isRepairMarkerFilePresent(true));
            }
        });
    }

    @Test
    public void testRetireWithoutATimelineRemovesALeftoverMarker() throws Exception {
        // No _timeline at all, as an earlier retire that removed it and crashed before the marker
        // leaves the directory: the marker guards nothing, and the retire removes it.
        assertMemoryLeak(() -> {
            ensureDirs();
            writeRetireFixture();
            Assert.assertFalse(timelineExists());
            try (Path dir = checkpointsDir()) {
                Assert.assertTrue(LiveViewCheckpointLifecycle.retireTimeline(configuration, dir, null, true));
            }
            Assert.assertFalse(isRepairMarkerFilePresent(false));
            Assert.assertFalse(isRepairMarkerFilePresent(true));
            assertRepairMarkerReads(Numbers.LONG_NULL, Numbers.LONG_NULL);
            assertRetireRemovedAllButTheTimelineAndTheMarker();
        });
    }

    @Test
    public void testRuntimeHistoryOpeningRefusesWhileTheMarkerStays() throws Exception {
        // A runtime history that opened beside a marker an older history left would be read against
        // it by every restart. While either name stays the append opens nothing: it fails ahead of
        // its first write, and the next append tries the removal again.
        final RetireRemovalFault ff = new RetireRemovalFault();
        assertMemoryLeak(ff, () -> {
            for (boolean isStagedRefused : new boolean[]{false, true}) {
                removeCheckpointsDir();
                ensureDirs();
                writeLeftoverMarker(LEFTOVER_MARKER_BOTH_NAMES);
                ff.refuseUnlinkOf(isStagedRefused
                        ? LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME + LiveViewCheckpointLayout.TMP_SUFFIX
                        : LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME);
                try (LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration)) {
                    try {
                        appendRoot(writer, 10, Numbers.LONG_NULL);
                        Assert.fail("a runtime history must not open beside a marker it cannot remove");
                    } catch (CairoException e) {
                        TestUtils.assertContains(
                                e.getFlyweightMessage(),
                                "could not remove the repair marker an earlier live view checkpoint timeline left [path="
                        );
                    }
                    try (LiveViewCheckpointMetaStore store = openStore()) {
                        Assert.assertFalse("no generation is published", store.isValid());
                    }
                    Assert.assertEquals("only the refused name stays", !isStagedRefused, isRepairMarkerFilePresent(false));
                    Assert.assertEquals("only the refused name stays", isStagedRefused, isRepairMarkerFilePresent(true));
                    Assert.assertEquals("no metadata segment is written", 0, countFiles(LiveViewCheckpointLayout.META_DIR_NAME));
                    Assert.assertEquals("no data segment is written", 0, countFiles(LiveViewCheckpointLayout.DATA_DIR_NAME));

                    ff.refuseUnlinkOf(null);
                    Assert.assertEquals("the next append opens the history", 1, appendRoot(writer, 10, Numbers.LONG_NULL));
                }
                Assert.assertFalse(isRepairMarkerFilePresent(false));
                Assert.assertFalse(isRepairMarkerFilePresent(true));
            }
        });
    }

    @Test
    public void testSeedHistoryOpeningPublishesBesideAMarkerItCannotRemove() throws Exception {
        // No reader of a seed history reads the marker, so a seed opening that cannot remove it
        // publishes all the same, and the appends that follow it try no removal. The runtime opening
        // after a retire of that history is refused while the marker stays, and opens the history
        // once it goes. Cursor 0 is a seed opening too: only LONG_NULL marks a runtime one.
        final RetireRemovalFault ff = new RetireRemovalFault();
        assertMemoryLeak(ff, () -> {
            for (long seedCursorOffset : new long[]{0, 3}) {
                removeCheckpointsDir();
                ensureDirs();
                writeLeftoverMarker(LEFTOVER_MARKER_NO_BASE);
                final int refusedBefore = ff.getRefusedUnlinks();
                ff.refuseMarkerUnlinks(true);
                try (LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration)) {
                    Assert.assertEquals("the seed history opens", 1, appendRoot(writer, 10, seedCursorOffset));
                    assertSeedCursorOffset(seedCursorOffset);
                    Assert.assertEquals("the opening tried the removal", refusedBefore + 1, ff.getRefusedUnlinks());
                    Assert.assertTrue("beside the marker it could not remove", isRepairMarkerFilePresent(false));
                    assertRepairMarkerReads(Numbers.LONG_NULL, Numbers.LONG_NULL);

                    Assert.assertEquals(2, appendRoot(writer, 20, seedCursorOffset + 2));
                    assertSeedCursorOffset(seedCursorOffset + 2);
                    Assert.assertEquals("no removal beside a valid generation", refusedBefore + 1, ff.getRefusedUnlinks());

                    try (Path dir = checkpointsDir()) {
                        Assert.assertFalse(LiveViewCheckpointLifecycle.retireTimeline(configuration, dir, null, true));
                    }
                    Assert.assertFalse(timelineExists());
                    Assert.assertTrue("the retire keeps the marker it could not remove", isRepairMarkerFilePresent(false));
                    Assert.assertEquals(refusedBefore + 2, ff.getRefusedUnlinks());
                    try {
                        appendRoot(writer, 30, Numbers.LONG_NULL);
                        Assert.fail("a runtime history must not open beside a marker it cannot remove");
                    } catch (CairoException e) {
                        TestUtils.assertContains(
                                e.getFlyweightMessage(),
                                "could not remove the repair marker an earlier live view checkpoint timeline left [path="
                        );
                    }
                    Assert.assertEquals(refusedBefore + 3, ff.getRefusedUnlinks());

                    ff.refuseMarkerUnlinks(false);
                    Assert.assertEquals("the runtime history opens once the marker goes", 1, appendRoot(writer, 30, Numbers.LONG_NULL));
                    assertSeedCursorOffset(Numbers.LONG_NULL);
                }
                Assert.assertFalse(isRepairMarkerFilePresent(false));
                Assert.assertFalse(isRepairMarkerFilePresent(true));
            }
        });
    }

    @Test
    public void testSupersededTimelineVersionRefusesASealOverIt() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            declareTimelineFormatVersion(0, LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION - 1);
            final Map<String, byte[]> filesBefore = snapshotCheckpointFiles();

            // The writer's backstop, which an upgrade must not narrow. No reader accepts the
            // older slot, so an append that went past its reconciliation would find both
            // slots invalid and publish this build's superblock over the older build's
            // segments: the mixed-format directory the boundary exists to prevent. The
            // upgrade rebuild retires the older directory before its first seal; a seal that
            // still meets it refuses.
            try (
                    LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration);
                    Path dir = checkpointsDir()
            ) {
                writer.append(
                        dir, new ObjList<>(), null, 7, 0, 2, 2, 0, LIFECYCLE_IDENTITY, true, 10, 1,
                        Numbers.LONG_NULL, Numbers.LONG_NULL, null
                );
                Assert.fail("a seal over an older-format timeline must refuse");
            } catch (CairoException e) {
                Assert.assertEquals(CairoException.LV_CHECKPOINT_FORMAT_BLOCKED, e.getErrno());
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "[version=" + (LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION - 1)
                );
            }
            assertCheckpointFilesUnchanged(filesBefore);
        });
    }

    @Test
    public void testSupersededTimelineVersionReportsAnUpgradeAndTouchesNothing() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);
            touchFinal(false, 4);
            // A repair that crashed with its candidate staged. The sweep that discards one runs
            // below the format gate, and an upgrade must not reach it any more than a block does.
            try (
                    LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration);
                    Path dir = checkpointsDir()
            ) {
                state.begin(dir, 31, 7, 0, 2, 31, 30, 1_000, 500, 800, 2_000, HighBoundTag.FINITE);
                state.addOwnedSegmentId(11);
                state.recordStage(RepairPublicationStage.CANDIDATE_ROOTS_AND_RUNTIME_READY);
            }

            // The migration direction, which is the one a released build would actually meet:
            // a slot an EARLIER layout wrote, naming its version in both fields that carry one,
            // as a real 10.0.x slot does. This build reads none of it, and the upgrade rebuilds
            // the view from its base table instead. The reconciliation's part is to say so and
            // leave every byte where it is: the older superblock is what re-derives the pending
            // rebuild on a restart that comes before the rebuild retires the directory.
            declareTimelineFormatVersion(0, LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION - 1);
            final Map<String, byte[]> filesBefore = snapshotCheckpointFiles();

            final LiveViewCheckpointLifecycle.ReconcileResult result;
            try (Path dir = checkpointsDir()) {
                result = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertTrue(result.isFormatUpgrade());
            Assert.assertTrue("an upgrade refines the block, so a seal still refuses on it", result.isFormatBlocked());
            Assert.assertFalse("the older directory is the witness, not something to reset", result.isFormatReset());
            Assert.assertEquals(
                    LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION - 1,
                    result.getForeignFormatVersion()
            );
            Assert.assertFalse(result.isEpochReplaced());
            Assert.assertEquals(-1, result.getWalPurgeFloor());
            Assert.assertEquals(Numbers.LONG_NULL, result.getNormalizedBaseSeqTxn());
            Assert.assertNull(result.getStats());
            Assert.assertEquals(0, result.getRemovedOrphanCount());
            Assert.assertEquals(0, result.getDiscardedRepairCount());
            assertCheckpointFilesUnchanged(filesBefore);

            // Read off the superblock rather than consumed, so a restart reaches it again.
            try (Path dir = checkpointsDir()) {
                Assert.assertTrue(LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true).isFormatUpgrade());
            }
            assertCheckpointFilesUnchanged(filesBefore);
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
            // build's. It is a fallback case, not a format case - and neither of
            // the two format dispositions may claim it: resetting would discard a
            // recoverable generation, and blocking would strand a view on a crash.
            corruptSlotGeneration(1);

            final LiveViewCheckpointLifecycle.ReconcileResult result;
            try (Path dir = checkpointsDir()) {
                result = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertFalse(result.isFormatReset());
            Assert.assertFalse("a torn slot carries this build's own version", result.isFormatBlocked());
            Assert.assertTrue(timelineExists());
            Assert.assertEquals(1, result.getWalPurgeFloor());
            Assert.assertEquals(1, result.getNormalizedBaseSeqTxn());
        });
    }

    @Test
    public void testUpgradableFormatVersionsAreTheOlderReleasedOnes() {
        Assert.assertFalse(LiveViewCheckpointLifecycle.isUpgradableFormatVersion(LiveViewCheckpointSuperblock.NO_FOREIGN_FORMAT));
        Assert.assertFalse("no released build wrote version 0", LiveViewCheckpointLifecycle.isUpgradableFormatVersion(0));
        for (int version = 1; version < LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION; version++) {
            Assert.assertTrue(LiveViewCheckpointLifecycle.isUpgradableFormatVersion(version));
        }
        Assert.assertFalse(LiveViewCheckpointLifecycle.isUpgradableFormatVersion(LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION));
        Assert.assertFalse(LiveViewCheckpointLifecycle.isUpgradableFormatVersion(LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION + 1));
    }

    @Test
    public void testVersionZeroDeclarationStillBlocks() throws Exception {
        assertMemoryLeak(() -> {
            ensureDirs();
            publish(1, 1, 7, 0, 5);

            // A magic nibble of 0 beside a version field of 0 names version 0 twice, which is a
            // declaration, but no released build wrote it. It stays a block rather than an
            // upgrade: nothing a real upgrade meets has this shape, so rebuilding on it would be
            // acting on a layout nobody produced.
            declareTimelineFormatVersion(0, 0);
            final Map<String, byte[]> filesBefore = snapshotCheckpointFiles();

            final LiveViewCheckpointLifecycle.ReconcileResult result;
            try (Path dir = checkpointsDir()) {
                result = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
            }
            Assert.assertTrue(result.isFormatBlocked());
            Assert.assertFalse(result.isFormatUpgrade());
            Assert.assertFalse(result.isFormatReset());
            Assert.assertEquals(0, result.getForeignFormatVersion());
            assertCheckpointFilesUnchanged(filesBefore);
        });
    }

    // Appends a root at maxTimestamp over one scalar window function, as the view of definition txn 7
    // and history epoch 0 seals one, carrying seedCursorOffset, and returns the generation it
    // published.
    private static long appendRoot(LiveViewCheckpointTimelineStoreWriter writer, long maxTimestamp, long seedCursorOffset) {
        try (Path dir = checkpointsDir(); ScalarStateStub stub = new ScalarStateStub()) {
            final ObjList<WindowFunction> functions = new ObjList<>();
            functions.add(stub);
            return writer.append(
                    dir, functions, null, 7, 0, maxTimestamp, maxTimestamp, 0, LIFECYCLE_IDENTITY, true, maxTimestamp, 1,
                    Numbers.LONG_NULL, seedCursorOffset, null
            ).getGeneration();
        }
    }

    private static Path checkpointsDir() {
        return new Path().of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    // The seed cursor of the append after one that carried seedCursorOffset: LONG_NULL for a steady
    // seal, the next row offset for a seed seal.
    private static long nextSeedCursorOffset(long seedCursorOffset) {
        return seedCursorOffset == Numbers.LONG_NULL ? Numbers.LONG_NULL : seedCursorOffset + 1;
    }

    /**
     * Asserts that a reconciliation whose purge sweep failed still reports what startup
     * publishes from the generation it adopted: the base WAL floor and the head coordinate.
     */
    private void assertAdoptedThroughAFailedPurge(LiveViewCheckpointLifecycle.ReconcileResult result) {
        Assert.assertTrue(result.isPurgeFailed());
        Assert.assertFalse(result.isFormatReset());
        Assert.assertFalse(result.isEpochReplaced());
        Assert.assertEquals("the adopted generation's base WAL floor", 1, result.getWalPurgeFloor());
        Assert.assertEquals("the adopted generation's head coordinate", 1, result.getNormalizedBaseSeqTxn());
        Assert.assertNotNull(result.getStats());
        Assert.assertEquals(0, result.getRetirableSegmentIds().size());
        Assert.assertTrue(timelineExists());
    }

    private void assertCheckpointFilesUnchanged(Map<String, byte[]> before) throws IOException {
        final Map<String, byte[]> after = snapshotCheckpointFiles();
        Assert.assertEquals("no file may be added or removed", before.keySet(), after.keySet());
        for (Map.Entry<String, byte[]> entry : before.entrySet()) {
            Assert.assertArrayEquals("file changed: " + entry.getKey(), entry.getValue(), after.get(entry.getKey()));
        }
    }

    /**
     * Drives reconciliation over a timeline whose newest slot took one flipped bit in
     * {@code fieldOffset}, and asserts the disposition a torn write gets: the intact slot's
     * generation is adopted, and the next publication overwrites the damaged slot.
     */
    private void assertOneDamagedFormatFieldFallsBack(long fieldOffset) {
        ensureDirs();
        publish(1, 1, 7, 0, 10); // slot 0
        publish(2, 2, 7, 0, 20); // slot 1, the newest

        // One bit and no checksum repair: rot rather than a write. Bit 0 of the field's low
        // byte turns this build's 2 into 3, the value a newer build would stamp - into both
        // fields, which is the whole difference.
        withTimelineMemory(mem -> {
            final long offset = LiveViewCheckpointSuperblock.SLOT_SIZE + fieldOffset;
            mem.putByte(offset, (byte) (mem.getByte(offset) ^ 1));
        });

        final LiveViewCheckpointLifecycle.ReconcileResult result;
        try (Path dir = checkpointsDir()) {
            result = LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true);
        }
        Assert.assertFalse("one damaged field declares no format to block on", result.isFormatBlocked());
        Assert.assertFalse(
                "resetting would discard the generation the intact slot names",
                result.isFormatReset()
        );
        Assert.assertFalse(result.isEpochReplaced());
        Assert.assertTrue(timelineExists());
        Assert.assertNotNull("the intact slot's generation is adopted", result.getStats());
        Assert.assertEquals(1, result.getWalPurgeFloor());
        Assert.assertEquals(1, result.getNormalizedBaseSeqTxn());

        // The next publication targets the slot selection passed over, so one seal clears
        // the damage and both slots are this build's again.
        publish(2, 3, 7, 0, 30);
        try (LiveViewCheckpointMetaStore store = openStore()) {
            final LiveViewCheckpointSuperblock superblock = store.getSuperblock();
            Assert.assertEquals(1, superblock.getSelectedSlot());
            Assert.assertEquals(2, superblock.generation);
            Assert.assertEquals("both slots must validate again", 1, superblock.getOldestValidGeneration());
        }
        try (Path dir = checkpointsDir()) {
            Assert.assertFalse(LiveViewCheckpointLifecycle.reconcile(configuration, dir, 7, 0, true).isFormatReset());
        }
    }

    // The selected generation carries seedCursorOffset.
    private void assertSeedCursorOffset(long seedCursorOffset) {
        try (LiveViewCheckpointMetaStore store = openStore()) {
            Assert.assertTrue(store.isValid());
            Assert.assertEquals("the seed cursor the append carried", seedCursorOffset, store.getSuperblock().seedCursorOffset);
        }
    }

    // Flips one byte of the page ref names, past its header, so the page fails its checksum.
    private void corruptMetaPage(LiveViewCheckpointPageRef ref) {
        try (Path dir = checkpointsDir(); Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
            LiveViewCheckpointLayout.metaSegmentPath(path, dir, ref.getSegmentId());
            mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
            final long offset = ref.getOffset() + LiveViewCheckpointLayout.PAGE_HEADER_SIZE;
            mem.putByte(offset, (byte) (mem.getByte(offset) ^ 1));
        }
    }

    // The files in the checkpoint directory's subdirectory dirName.
    private long countFiles(String dirName) throws IOException {
        try (Path dir = checkpointsDir(); Stream<java.nio.file.Path> files = Files.list(Paths.get(dir.toString(), dirName))) {
            return files.count();
        }
    }

    private void removeCheckpointsDir() {
        try (Path dir = checkpointsDir()) {
            final FilesFacade ff = configuration.getFilesFacade();
            if (ff.exists(dir.$())) {
                Assert.assertTrue(ff.rmdir(dir.slash()));
            }
        }
    }

    // The timeline root the selected generation names.
    private LiveViewCheckpointPageRef timelineRootRef() {
        try (LiveViewCheckpointMetaStore store = openStore()) {
            final LiveViewCheckpointPageRef ref = store.getSuperblock().timelineRootRef;
            return new LiveViewCheckpointPageRef().of(ref.getSegmentId(), ref.getOffset(), ref.getLength());
        }
    }

    /**
     * Writes one of {@link #LEFTOVER_MARKER_SHAPES} into the checkpoint directory: what an older
     * history's repair marker can look like once that history is gone.
     */
    private void writeLeftoverMarker(String shape) throws IOException {
        try (Path dir = checkpointsDir(); Path path = new Path(); Path stagedPath = new Path()) {
            LiveViewCheckpointLayout.repairingMarkerPath(path, dir);
            LiveViewCheckpointLayout.repairingMarkerPath(stagedPath, dir).put(LiveViewCheckpointLayout.TMP_SUFFIX);
            final java.nio.file.Path markerFile = Paths.get(path.toString());
            if (shape.equals(LEFTOVER_MARKER_NO_BASE)) {
                LiveViewCheckpointRepairMarker.write(configuration, dir, 7, 0, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            } else if (shape.equals(LEFTOVER_MARKER_VERSION_1)) {
                final ByteBuffer record = ByteBuffer.allocate(LiveViewCheckpointRepairMarker.V1_SIZE).order(ByteOrder.LITTLE_ENDIAN);
                record.putLong(LiveViewCheckpointRepairMarker.MAGIC_OFFSET, LiveViewCheckpointRepairMarker.MARKER_MAGIC);
                record.putInt(LiveViewCheckpointRepairMarker.FORMAT_VERSION_OFFSET, LiveViewCheckpointRepairMarker.V1_FORMAT_VERSION);
                record.putLong(LiveViewCheckpointRepairMarker.DEFINITION_TXN_OFFSET, 7);
                record.putLong(LiveViewCheckpointRepairMarker.HISTORY_EPOCH_OFFSET, 0);
                record.putLong(LiveViewCheckpointRepairMarker.BASE_GENERATION_OFFSET, 5);
                record.putLong(LiveViewCheckpointRepairMarker.FLOOR_TIMESTAMP_OFFSET, 1_000);
                final CRC32 crc = new CRC32();
                crc.update(record.array(), 0, LiveViewCheckpointRepairMarker.V1_CRC_OFFSET);
                record.putInt(LiveViewCheckpointRepairMarker.V1_CRC_OFFSET, (int) crc.getValue());
                Files.write(markerFile, record.array());
            } else {
                LiveViewCheckpointRepairMarker.write(configuration, dir, 7, 0, 5, 1_000, MARKER_LV_SEQ_TXN);
                if (shape.equals(LEFTOVER_MARKER_TORN)) {
                    final byte[] bytes = Files.readAllBytes(markerFile);
                    bytes[LiveViewCheckpointRepairMarker.BASE_GENERATION_OFFSET] ^= 1;
                    Files.write(markerFile, bytes);
                } else if (shape.equals(LEFTOVER_MARKER_STAGED_ALONE)) {
                    Assert.assertEquals(
                            io.questdb.std.Files.FILES_RENAME_OK,
                            configuration.getFilesFacade().rename(path.$(), stagedPath.$())
                    );
                } else if (shape.equals(LEFTOVER_MARKER_BOTH_NAMES)) {
                    Assert.assertTrue(configuration.getFilesFacade().touch(stagedPath.$()));
                } else {
                    Assert.assertEquals(LEFTOVER_MARKER_BASE_5, shape);
                }
            }
            Assert.assertTrue(shape, LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
            final boolean isNamingBase5 = !shape.equals(LEFTOVER_MARKER_NO_BASE) && !shape.equals(LEFTOVER_MARKER_TORN);
            Assert.assertEquals(
                    shape,
                    isNamingBase5 ? 5 : Numbers.LONG_NULL,
                    LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir)
            );
        }
    }

    // The repair marker reads baseGeneration and lvSeqTxn, LONG_NULL in both when none is on disk.
    private void assertRepairMarkerReads(long baseGeneration, long lvSeqTxn) {
        try (Path dir = checkpointsDir()) {
            Assert.assertEquals(baseGeneration, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
            Assert.assertEquals(lvSeqTxn, LiveViewCheckpointRepairMarker.readLvSeqTxn(configuration, dir));
        }
    }

    // The retire removed the retirement work set, both names, and the meta, data and repair trees.
    private void assertRetireRemovedAllButTheTimelineAndTheMarker() {
        Assert.assertFalse(isRetirementQueuePresent(false));
        Assert.assertFalse(isRetirementQueuePresent(true));
        Assert.assertFalse(metaDirExists());
        Assert.assertFalse(dataDirExists());
        Assert.assertFalse(repairDirExists());
    }

    /**
     * Stamps the magic and layout version a later build writes into {@code slot}.
     * The magic's trailing nibble tracks the version, so a build one version on
     * carries both one higher.
     */
    private void bumpTimelineFormatVersion(int slot) {
        withTimelineMemory(mem -> {
            final long base = (long) slot * LiveViewCheckpointSuperblock.SLOT_SIZE;
            mem.putLong(
                    base + LiveViewCheckpointSuperblock.SLOT_MAGIC_OFFSET,
                    LiveViewCheckpointSuperblock.SLOT_MAGIC + 1
            );
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

    /**
     * Stamps the magic and layout version a build of {@code formatVersion} writes into
     * {@code slot}, checksum and all. The magic's trailing nibble tracks the version, so
     * a released 10.0.x slot carries both as 1.
     */
    private void declareTimelineFormatVersion(int slot, int formatVersion) {
        withTimelineMemory(mem -> {
            final long base = (long) slot * LiveViewCheckpointSuperblock.SLOT_SIZE;
            mem.putLong(
                    base + LiveViewCheckpointSuperblock.SLOT_MAGIC_OFFSET,
                    LiveViewCheckpointSuperblock.SLOT_MAGIC_FAMILY | formatVersion
            );
            mem.putInt(
                    base + LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION_OFFSET,
                    formatVersion
            );
            mem.putInt(
                    base + LiveViewCheckpointSuperblock.SLOT_CRC_OFFSET,
                    Zip.crc32(0, mem.addressOf(base), LiveViewCheckpointSuperblock.SLOT_CRC_COVERAGE)
            );
        });
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
            final long directoryBytes = writer.commit();

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
            // A directory tree cannot hold an entry naming the file it is written
            // into, so a real publication carries that one registration forward
            // and reconciliation accepts exactly this segment as uncatalogued.
            superblock.pendingDirectorySegmentId = metadataSegmentId;
            superblock.pendingDirectorySegmentBytes = directoryBytes;
            superblock.pendingDirectorySegmentPages = 1;
            store.publish();
        }
    }

    /**
     * Publishes one generation over a real catalogue: {@code registered} joins it at
     * a reference count of one, {@code released} loses one, and the directory
     * segment the previous generation left pending is registered as that
     * publication's own, exactly as a seal does it. The publication then supersedes
     * the catalogue pages it path-copies, so the previous directory segment ends at
     * a zero count against this generation.
     */
    private void publishCatalogue(
            long generation,
            long directorySegmentId,
            long definitionTxn,
            long nextSegmentId,
            LongList registered,
            LongList released
    ) {
        try (
                LiveViewCheckpointSegmentDirectoryWriter directoryWriter =
                        new LiveViewCheckpointSegmentDirectoryWriter(configuration);
                LiveViewCheckpointMetaStore store = openStore();
                Path dir = checkpointsDir()
        ) {
            directoryWriter.of(dir);
            final LiveViewCheckpointSuperblock superblock = store.getSuperblock();
            directoryWriter.begin(superblock.segmentDirectoryRootRef);
            if (superblock.pendingDirectorySegmentId != Numbers.LONG_NULL) {
                directoryWriter.addSegment(
                        superblock.pendingDirectorySegmentId,
                        superblock.pendingDirectorySegmentBytes,
                        superblock.pendingDirectorySegmentPages,
                        LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_META
                );
            }
            for (int i = 0, n = registered.size(); i < n; i++) {
                directoryWriter.addSegment(registered.getQuick(i), 1, 1);
            }
            if (released.size() > 0) {
                directoryWriter.applyRootReferenceChanges(released, EMPTY_SEGMENT_IDS, generation);
            }
            final LiveViewCheckpointPageRef directoryRoot = new LiveViewCheckpointPageRef();
            directoryWriter.publish(directorySegmentId, generation, directoryRoot);

            superblock.generation = generation;
            superblock.definitionTxn = definitionTxn;
            superblock.historyEpoch = 0;
            superblock.normalizedBaseSeqTxn = generation;
            superblock.coveredLvSeqTxn = generation;
            superblock.nextSegmentId = nextSegmentId;
            superblock.segmentDirectoryRootRef.of(
                    directoryRoot.getSegmentId(),
                    directoryRoot.getOffset(),
                    directoryRoot.getLength()
            );
            superblock.pendingDirectorySegmentId = directorySegmentId;
            superblock.pendingDirectorySegmentBytes = directoryWriter.getLastSegmentBytes();
            superblock.pendingDirectorySegmentPages = directoryWriter.getLastSegmentPageCount();
            store.publish();
        }
    }

    private static LongList segmentIds(long... ids) {
        final LongList list = new LongList();
        for (long id : ids) {
            list.add(id);
        }
        return list;
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

    private boolean isRepairMarkerFilePresent(boolean isStaged) {
        try (Path dir = checkpointsDir(); Path path = new Path()) {
            LiveViewCheckpointLayout.repairingMarkerPath(path, dir);
            if (isStaged) {
                path.put(LiveViewCheckpointLayout.TMP_SUFFIX);
            }
            return configuration.getFilesFacade().exists(path.$());
        }
    }

    private boolean repairDirExists() {
        try (Path dir = checkpointsDir(); Path path = new Path()) {
            return configuration.getFilesFacade().exists(LiveViewCheckpointLayout.repairDirPath(path, dir).$());
        }
    }

    private boolean repairDescriptorExists(long repairId) {
        try (Path dir = checkpointsDir(); Path path = new Path()) {
            LiveViewCheckpointLayout.repairDescriptorPath(path, dir, repairId);
            return configuration.getFilesFacade().exists(path.$());
        }
    }

    private boolean isRetirementQueuePresent(boolean isTemporary) {
        try (Path dir = checkpointsDir(); Path path = new Path()) {
            LiveViewCheckpointLayout.retirementQueuePath(path, dir);
            if (isTemporary) {
                path.put(LiveViewCheckpointLayout.TMP_SUFFIX);
            }
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

    /**
     * Every file under the checkpoint directory, by relative path, with its bytes. What a
     * format disposition that must touch nothing is compared against.
     */
    private Map<String, byte[]> snapshotCheckpointFiles() throws IOException {
        final Map<String, byte[]> files = new TreeMap<>();
        try (Path dir = checkpointsDir()) {
            final java.nio.file.Path root = Paths.get(dir.toString());
            try (Stream<java.nio.file.Path> walk = Files.walk(root)) {
                for (java.nio.file.Path file : (Iterable<java.nio.file.Path>) walk.filter(Files::isRegularFile)::iterator) {
                    files.put(root.relativize(file).toString(), Files.readAllBytes(file));
                }
            }
        }
        return files;
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

    /**
     * Everything a retire removes beside the timeline: a repair marker naming
     * {@link #MARKER_GENERATION} and {@link #MARKER_LV_SEQ_TXN} beside an empty staged sibling, what
     * a crash inside a rewrite's staged write leaves, the retirement work set under both names, and
     * a repair descriptor.
     */
    private void writeRetireFixture() {
        try (Path dir = checkpointsDir(); Path path = new Path()) {
            LiveViewCheckpointRepairMarker.write(configuration, dir, 7, 0, MARKER_GENERATION, 1_000, MARKER_LV_SEQ_TXN);
            touchTopLevel(LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME + LiveViewCheckpointLayout.TMP_SUFFIX);
            touchTopLevel(LiveViewCheckpointLayout.RETIREMENT_QUEUE_FILE_NAME);
            touchTopLevel(LiveViewCheckpointLayout.RETIREMENT_QUEUE_TMP_FILE_NAME);
            final FilesFacade ff = configuration.getFilesFacade();
            Assert.assertEquals(0, ff.mkdirs(LiveViewCheckpointLayout.repairDirPath(path, dir).slash(), configuration.getMkDirMode()));
            Assert.assertTrue(ff.touch(LiveViewCheckpointLayout.repairDescriptorPath(path, dir, 3).$()));
        }
        Assert.assertTrue(isRepairMarkerFilePresent(false));
        Assert.assertTrue(isRepairMarkerFilePresent(true));
        assertRepairMarkerReads(MARKER_GENERATION, MARKER_LV_SEQ_TXN);
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

    // A scalar (map-less) whole-state window function, so an append walks the production
    // publication path over a fixed, tiny state image.
    private static final class ScalarStateStub extends BaseWindowFunction {

        private ScalarStateStub() {
            super(null);
            setCheckpointCompilerMetadata(
                    new LiveViewCheckpointFunctionIdentity(
                            "w0",
                            "lifecycle_stub()",
                            0,
                            "",
                            "ts asc",
                            "lifecycle-stub-v1"
                    ),
                    new LiveViewCheckpointDependency(
                            LiveViewCheckpointContracts.DependencyKind.UNBOUNDED_CUMULATIVE_NO_RESET,
                            "",
                            "ts asc",
                            Long.MIN_VALUE,
                            0,
                            Long.MIN_VALUE,
                            ColumnType.TIMESTAMP,
                            false,
                            false,
                            false,
                            LiveViewCheckpointDependency.StructuralConvergence.EXACT,
                            LiveViewCheckpointDependency.NumericConvergence.EXACT
                    )
            );
        }

        @Override
        public int checkpointStateFormatVersion() {
            return 1;
        }

        @Override
        public void freezeCheckpointState(LiveViewStatePageWriter sink, MapValue value) {
            sink.putLong(0);
        }

        @Override
        public String getName() {
            return "lifecycle_stub";
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public long restoreCheckpointState(LiveViewStatePageReader source, long offset, MapValue value) {
            return Long.BYTES;
        }

        @Override
        public boolean supportsCheckpointState() {
            return true;
        }
    }

    // Refuses the unlink of the checkpoint file whose name ends with the refused name, when set
    // the unlinks of both repair marker names, and, when set, every directory removal under the
    // checkpoint directory: a file lock or a permission fault a retire has to work around. Counts
    // the unlinks it refuses. Once armed, samples the native memory tagged NATIVE_PATH and
    // Unsafe's malloc and realloc count at the next unlink of _timeline and at the next unlink of
    // the staged repair marker.
    private static final class RetireRemovalFault extends TestFilesFacadeImpl {
        private final AtomicInteger refusedUnlinks = new AtomicInteger();
        private volatile long allocationsAtTheStagedMarkerUnlink = -1;
        private volatile long allocationsAtTheTimelineUnlink = -1;
        private volatile boolean isRefusingMarkerUnlinks;
        private volatile boolean isRefusingTreeRemovals;
        private volatile boolean isSamplingTheStagedMarkerUnlink;
        private volatile boolean isSamplingTheTimelineUnlink;
        private volatile long pathBytesAtTheStagedMarkerUnlink = -1;
        private volatile long pathBytesAtTheTimelineUnlink = -1;
        private volatile String refusedUnlinkName;

        @Override
        public boolean removeQuiet(LPSZ name) {
            if (isSamplingTheTimelineUnlink && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME)) {
                isSamplingTheTimelineUnlink = false;
                pathBytesAtTheTimelineUnlink = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PATH);
                allocationsAtTheTimelineUnlink = Unsafe.getMallocCount() + Unsafe.getReallocCount();
            }
            if (isSamplingTheStagedMarkerUnlink
                    && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME + LiveViewCheckpointLayout.TMP_SUFFIX)) {
                isSamplingTheStagedMarkerUnlink = false;
                pathBytesAtTheStagedMarkerUnlink = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PATH);
                allocationsAtTheStagedMarkerUnlink = Unsafe.getMallocCount() + Unsafe.getReallocCount();
            }
            final String refused = refusedUnlinkName;
            if (refused != null && Utf8s.containsAscii(name, LV_DIR) && Utf8s.endsWithAscii(name, refused)) {
                refusedUnlinks.incrementAndGet();
                return false;
            }
            if (isRefusingMarkerUnlinks
                    && Utf8s.containsAscii(name, LV_DIR)
                    && (Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                    || Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME + LiveViewCheckpointLayout.TMP_SUFFIX))) {
                refusedUnlinks.incrementAndGet();
                return false;
            }
            return super.removeQuiet(name);
        }

        long getAllocationsAtTheStagedMarkerUnlink() {
            return allocationsAtTheStagedMarkerUnlink;
        }

        long getAllocationsAtTheTimelineUnlink() {
            return allocationsAtTheTimelineUnlink;
        }

        long getPathBytesAtTheStagedMarkerUnlink() {
            return pathBytesAtTheStagedMarkerUnlink;
        }

        long getPathBytesAtTheTimelineUnlink() {
            return pathBytesAtTheTimelineUnlink;
        }

        int getRefusedUnlinks() {
            return refusedUnlinks.get();
        }

        void refuseMarkerUnlinks(boolean isRefusing) {
            isRefusingMarkerUnlinks = isRefusing;
        }

        @Override
        public boolean rmdir(Path name, boolean haltOnError) {
            if (isRefusingTreeRemovals && Utf8s.containsAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)) {
                return false;
            }
            return super.rmdir(name, haltOnError);
        }

        void refuseTreeRemovals(boolean isRefusing) {
            isRefusingTreeRemovals = isRefusing;
        }

        void refuseUnlinkOf(@Nullable String name) {
            refusedUnlinkName = name;
        }

        void sampleTheNextUnlinks() {
            allocationsAtTheStagedMarkerUnlink = -1;
            allocationsAtTheTimelineUnlink = -1;
            pathBytesAtTheStagedMarkerUnlink = -1;
            pathBytesAtTheTimelineUnlink = -1;
            isSamplingTheStagedMarkerUnlink = true;
            isSamplingTheTimelineUnlink = true;
        }
    }
}
