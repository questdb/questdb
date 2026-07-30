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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.lv.LiveViewCheckpointAnchorRoot;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewFunctionSnapshot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class LiveViewCheckpointTimelineSealTest extends AbstractLiveViewTest {

    // Ten commits fill the dense view's 1000-second frame, so its head root has nine
    // earlier chunks to reference; two more take the sharing past the first refill.
    private static final int DENSE_COMMITS = 12;
    // Rows one dense commit adds, at one-second spacing. Above
    // LiveViewCheckpointRingSeal.MIN_SHARED_CHUNK_ROWS, so the chunk each seal writes
    // carries enough rows to be worth a later root's reference.
    private static final int DENSE_ROWS_PER_COMMIT = 100;

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Test
    public void testDropRetiresTimelineAndReleasesWalOwnership() throws Exception {
        assertMemoryLeak(() -> {
            createView(false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                appendAndRefresh(job, 10, 1);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertTrue(instance.getCheckpointTimelineWalPurgeFloor() >= 0);
                try (Path checkpointsDir = checkpointsDir(instance); Path timelinePath = new Path()) {
                    LiveViewCheckpointLayout.timelinePath(timelinePath, checkpointsDir);
                    Assert.assertTrue(configuration.getFilesFacade().exists(timelinePath.$()));

                    execute("DROP LIVE VIEW lv");

                    Assert.assertEquals(Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                    Assert.assertFalse(configuration.getFilesFacade().exists(timelinePath.$()));
                    Assert.assertFalse(configuration.getFilesFacade().exists(
                            LiveViewCheckpointLayout.metaDirPath(timelinePath, checkpointsDir).$()
                    ));
                    Assert.assertFalse(configuration.getFilesFacade().exists(
                            LiveViewCheckpointLayout.dataDirPath(timelinePath, checkpointsDir).$()
                    ));
                }
            }
        });
    }

    @Before
    public void setUpCheckpointCadence() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testCrashAfterDataPublicationDoesNotExposeGenerationAndRetrySkipsOrphan() throws Exception {
        assertCrashBeforeSuperblockPublish(
                LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_DATA_PUBLISH,
                1
        );
    }

    @Test
    public void testCrashAfterMetadataPublicationDoesNotExposeGenerationAndRetrySkipsOrphans() throws Exception {
        assertCrashBeforeSuperblockPublish(
                LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH,
                5
        );
    }

    @Test
    public void testRepeatedSealFailureRetiresTheTimelineAndReleasesTheWalFloor() throws Exception {
        assertMemoryLeak(() -> {
            createView(false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                appendAndRefresh(job, 10, 1);
                appendAndRefresh(job, 20, 2);

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertTrue("a healthy view holds the head arm", instance.getHeadCheckpointBaseSeqTxn() > -1);
                Assert.assertTrue("a healthy view holds the timeline arm", instance.getCheckpointTimelineWalPurgeFloor() > -1);
                Assert.assertEquals(0, instance.getCheckpointSealFailures());

                // Fails at the same point on every later seal, which is what makes the
                // fault deterministic rather than transient.
                job.setCheckpointTimelineTestFailureStage(
                        LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_DATA_PUBLISH
                );

                // Below the budget both arms stay held. A held writer or a momentarily
                // full disk must not cost the view its restart recovery state.
                appendAndRefresh(job, 30, 3);
                Assert.assertEquals(1, instance.getCheckpointSealFailures());
                Assert.assertTrue(instance.getHeadCheckpointBaseSeqTxn() > -1);
                Assert.assertTrue(instance.getCheckpointTimelineWalPurgeFloor() > -1);

                appendAndRefresh(job, 40, 4);
                Assert.assertEquals(2, instance.getCheckpointSealFailures());
                Assert.assertTrue(instance.getHeadCheckpointBaseSeqTxn() > -1);
                Assert.assertTrue(instance.getCheckpointTimelineWalPurgeFloor() > -1);

                // The third spends MAX_CONSECUTIVE_SEAL_FAILURES. Both WalPurgeJob floor
                // arms release, so the base WAL stops being retained for a root that is
                // never going to be written.
                appendAndRefresh(job, 50, 5);
                Assert.assertEquals(3, instance.getCheckpointSealFailures());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                try (Path checkpointsDir = checkpointsDir(instance); Path timelinePath = new Path()) {
                    LiveViewCheckpointLayout.timelinePath(timelinePath, checkpointsDir);
                    Assert.assertFalse(configuration.getFilesFacade().exists(timelinePath.$()));
                }
                assertQuery("SELECT checkpoint_seal_failures FROM live_views()")
                        .noLeakCheck().noRandomAccess()
                        .returns("checkpoint_seal_failures\n3\n");

                // The cooldown suppresses the seal outright rather than re-streaming the
                // whole ring only to throw at the same point again. A cleared head would
                // otherwise force a seal past the cadence gate on every cycle.
                appendAndRefresh(job, 51, 6);
                Assert.assertEquals(3, instance.getCheckpointSealFailures());

                // Past the cooldown, with the fault cleared, the view seals on its own and
                // re-establishes both arms - no restart, no DROP.
                job.setCheckpointTimelineTestFailureStage(0);
                setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
                appendAndRefresh(job, 52, 7);
                Assert.assertEquals(
                        "the seal-failure count is a lifetime total, not a streak",
                        3,
                        instance.getCheckpointSealFailures()
                );
                Assert.assertTrue(instance.getHeadCheckpointBaseSeqTxn() > -1);
                Assert.assertTrue(instance.getCheckpointTimelineWalPurgeFloor() > -1);
            }
        });
    }

    private void assertCrashBeforeSuperblockPublish(int failureStage, long expectedRetryDataSegmentId) throws Exception {
        assertMemoryLeak(() -> {
            createView(false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                job.setCheckpointTimelineTestFailureStage(failureStage);
                appendAndRefresh(job, 10, 1);

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertEquals(
                        "a pre-superblock crash must not advance the off-thread WAL floor",
                        Numbers.LONG_NULL,
                        instance.getCheckpointTimelineWalPurgeFloor());
                try (LiveViewCheckpointMetaStore store = openStore(instance)) {
                    Assert.assertFalse("immutable-file publication is not the timeline commit point", store.isValid());
                }

                job.setCheckpointTimelineTestFailureStage(0);
                appendAndRefresh(job, 20, 2);

                try (
                        LiveViewCheckpointMetaStore store = openStore(instance);
                        LiveViewCheckpointGenerationPin pin = store.pin();
                        LiveViewCheckpointTimelineReader reader = openTimelineReader(instance)
                ) {
                    Assert.assertEquals(1, pin.getGeneration());
                    Assert.assertEquals(1, reader.size(pin.getTimelineRootRef()));
                    Assert.assertEquals(instance.getLastProcessedSeqTxn(), pin.getNormalizedBaseSeqTxn());
                    Assert.assertEquals(store.getSuperblock().coveredLvSeqTxn, pin.getCoveredLvSeqTxn());
                    Assert.assertEquals(
                            "the first durable slot is its own WAL floor",
                            pin.getNormalizedBaseSeqTxn(),
                            store.getWalPurgeFloor());
                    Assert.assertEquals(store.getWalPurgeFloor(), instance.getCheckpointTimelineWalPurgeFloor());
                    final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
                    Assert.assertTrue(reader.last(pin.getTimelineRootRef(), entry));
                    Assert.assertEquals(0, entry.checkpointId);
                    Assert.assertEquals(ts(timestamp(20)), entry.maxTimestamp);

                    try (LiveViewCheckpointSegmentDirectoryReader directory =
                                 new LiveViewCheckpointSegmentDirectoryReader(configuration)) {
                        try (Path checkpointsDir = checkpointsDir(instance)) {
                            directory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
                        }
                        Assert.assertEquals(1, countDataSegments(directory));
                        Assert.assertEquals(
                                "the retry must not reuse the final-name orphan's segment id",
                                expectedRetryDataSegmentId,
                                lastDataSegmentId(directory)
                        );
                        Assert.assertEquals(1, directory.getReferenceCount(expectedRetryDataSegmentId));
                    }
                }
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testNormalCadenceAppendsPermanentRootsAndPublishesCompleteState() throws Exception {
        assertMemoryLeak(() -> {
            createView(true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                appendAndRefresh(job, 10, 1);
                appendAndRefresh(job, 20, 2);
                appendAndRefresh(job, 30, 3);

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                try (
                        LiveViewCheckpointMetaStore store = openStore(instance);
                        LiveViewCheckpointGenerationPin pin = store.pin();
                        LiveViewCheckpointTimelineReader reader = openTimelineReader(instance);
                        LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                        LiveViewCheckpointAnchorRoot anchorRoot = new LiveViewCheckpointAnchorRoot(configuration);
                        LiveViewCheckpointFunctionDirectory functions =
                                new LiveViewCheckpointFunctionDirectory(configuration);
                        Path checkpointsDir = checkpointsDir(instance)
                ) {
                    Assert.assertEquals(3, pin.getGeneration());
                    Assert.assertEquals(3, reader.size(pin.getTimelineRootRef()));
                    Assert.assertEquals(instance.getLastProcessedSeqTxn(), store.getSuperblock().normalizedBaseSeqTxn);
                    Assert.assertEquals(
                            engine.getTableSequencerAPI().getTxnTracker(instance.getLiveViewToken()).getWriterTxn(),
                            store.getSuperblock().coveredLvSeqTxn
                    );
                    Assert.assertEquals(store.getSuperblock().normalizedBaseSeqTxn, pin.getNormalizedBaseSeqTxn());
                    Assert.assertEquals(store.getSuperblock().coveredLvSeqTxn, pin.getCoveredLvSeqTxn());
                    Assert.assertTrue(
                            "the fallback A/B slot must retain the prior generation's base WAL",
                            store.getWalPurgeFloor() < store.getSuperblock().normalizedBaseSeqTxn
                    );
                    Assert.assertEquals(store.getWalPurgeFloor(), instance.getCheckpointTimelineWalPurgeFloor());

                    final LongList rows = new LongList();
                    reader.iterateAll(pin.getTimelineRootRef(), entry -> {
                        rows.add(entry.checkpointId);
                        rows.add(entry.maxTimestamp);
                        rows.add(entry.baseLvRowPosition);
                        rows.add(entry.logicalStateBytes);
                    });
                    Assert.assertEquals(12, rows.size());
                    for (int i = 0; i < 3; i++) {
                        Assert.assertEquals(i, rows.getQuick(i * 4));
                        Assert.assertEquals(ts(timestamp((i + 1) * 10)), rows.getQuick(i * 4 + 1));
                        Assert.assertEquals(i + 1, rows.getQuick(i * 4 + 2));
                        Assert.assertTrue(rows.getQuick(i * 4 + 3) > 0);
                    }

                    final LiveViewCheckpointTimelineEntry oldest = new LiveViewCheckpointTimelineEntry();
                    Assert.assertTrue(reader.findExact(
                            pin.getTimelineRootRef(),
                            ts(timestamp(10)),
                            0,
                            oldest
                    ));
                    root.of(checkpointsDir, oldest.rootRef);
                    Assert.assertEquals(0, root.getCheckpointId());

                    final LiveViewCheckpointTimelineEntry latest = new LiveViewCheckpointTimelineEntry();
                    Assert.assertTrue(reader.last(pin.getTimelineRootRef(), latest));
                    root.of(checkpointsDir, latest.rootRef);
                    Assert.assertEquals(latest.checkpointId, root.getCheckpointId());
                    Assert.assertEquals(latest.maxTimestamp, root.getMaxTimestamp());
                    final LiveViewCheckpointPageRef anchorRootRef = new LiveViewCheckpointPageRef();
                    root.getAnchorRootRef(anchorRootRef);
                    Assert.assertFalse(anchorRootRef.isNull());
                    anchorRoot.of(checkpointsDir, anchorRootRef);
                    Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, anchorRoot.getAnchorValueType());
                    final LiveViewCheckpointPageRef anchorMapRootRef = new LiveViewCheckpointPageRef();
                    anchorRoot.getPartitionMapRootRef(anchorMapRootRef);
                    Assert.assertFalse(anchorMapRootRef.isNull());
                    try (LiveViewCheckpointPartitionMapReader anchorMap =
                                 new LiveViewCheckpointPartitionMapReader(configuration)) {
                        anchorMap.of(checkpointsDir);
                        Assert.assertEquals(1, anchorMap.size(anchorMapRootRef));
                        anchorMap.iterateAll(anchorMapRootRef, entry -> {
                            Assert.assertEquals(0, entry.getStatePageCount());
                            Assert.assertEquals(Long.BYTES, entry.getScalarState().length);
                        });
                    }
                    final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
                    root.getFunctionDirectoryRef(functionDirectoryRef);
                    functions.of(checkpointsDir, functionDirectoryRef);
                    Assert.assertEquals(1, functions.size());

                    try (LiveViewCheckpointSegmentDirectoryReader directory =
                                 new LiveViewCheckpointSegmentDirectoryReader(configuration)) {
                        directory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
                        // One data segment per seal, each named by exactly one root.
                        Assert.assertEquals(3, countDataSegments(directory));
                        directory.iterateAll(entry -> {
                            if (!entry.isMetadata()) {
                                Assert.assertEquals(1, entry.referenceCount);
                            }
                        });
                        // The anchor reaches no data segment, so the newest root's
                        // only one is the function state the same seal wrote.
                        Assert.assertEquals(1, root.getSegmentIdCount());
                        Assert.assertTrue(directory.getFileLength(root.getSegmentId(0)) > 0);
                    }
                }
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testDenseFrameSealCarriesTheHeadRootsChunksForwardByReference() throws Exception {
        assertMemoryLeak(() -> {
            // A frame ten commits wide, refilled a hundred rows at a time, so a seal
            // has nine earlier chunks to reference and one to write. Each seal's
            // chunk stays big enough that referencing it beats re-encoding it - the
            // line LiveViewCheckpointRingSeal.chunkCap draws.
            createDenseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= DENSE_COMMITS; commit++) {
                    commitDenseAndRefresh(job, commit);
                }
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);
                final RuntimeSnapshot newestState = snapshotRuntime(functions, null);

                final long earlyMaxTs = ts(denseTimestamp(DENSE_ROWS_PER_COMMIT * 3 - 1));
                final long newestMaxTs = ts(denseTimestamp(DENSE_ROWS_PER_COMMIT * DENSE_COMMITS - 1));
                try (
                        LiveViewCheckpointMetaStore store = openStore(instance);
                        LiveViewCheckpointGenerationPin pin = store.pin();
                        LiveViewCheckpointTimelineReader timeline = openTimelineReader(instance);
                        LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                        LiveViewCheckpointSegmentDirectoryReader directory =
                                new LiveViewCheckpointSegmentDirectoryReader(configuration);
                        Path checkpointsDir = checkpointsDir(instance)
                ) {
                    Assert.assertEquals(DENSE_COMMITS, timeline.size(pin.getTimelineRootRef()));
                    final LiveViewCheckpointTimelineEntry newest = new LiveViewCheckpointTimelineEntry();
                    Assert.assertTrue(timeline.last(pin.getTimelineRootRef(), newest));
                    root.of(checkpointsDir, newest.rootRef);

                    // The head root names one data segment per chunk its frame still
                    // holds, and all but the newest of those were written by earlier
                    // seals. Without sharing this would be one - the segment this
                    // seal wrote its own complete image into.
                    Assert.assertTrue(
                            "the head root names " + root.getSegmentIdCount() + " data segments",
                            root.getSegmentIdCount() > 1
                    );
                    directory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
                    final int[] sharedSegments = {0};
                    directory.iterateAll(entry -> {
                        if (entry.referenceCount > 1) {
                            sharedSegments[0]++;
                        }
                    });
                    Assert.assertTrue(
                            "no data segment is referenced by more than one root",
                            sharedSegments[0] > 0
                    );
                }

                // Sharing is only worth having if both ends of it restore exactly.
                // An early root and the newest one are rebuilt from overlapping page
                // sets, so a chunk spliced into the wrong root's ring would show up
                // as a state mismatch here.
                try (
                        Path checkpointsDir = checkpointsDir(instance);
                        LiveViewCheckpointTimelineStoreReader reader =
                                new LiveViewCheckpointTimelineStoreReader(configuration)
                ) {
                    reader.of(checkpointsDir);
                    reader.restore(earlyMaxTs, 2, instance.getLiveViewToken().getTableId(), functions, null);
                    reader.restore(newestMaxTs, DENSE_COMMITS - 1, instance.getLiveViewToken().getTableId(), functions, null);
                    assertRuntimeSnapshot(newestState, functions, null);
                }
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testOneReaderRestoresAcrossViewsAfterDetach() throws Exception {
        // A refresh worker holds one reader for its whole life and rebinds it per
        // restore, so a bind must start from nothing the previous one left behind:
        // another view's generation does not continue this one's, and its segment
        // ids name entirely different files.
        assertMemoryLeak(() -> {
            createView(false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                appendAndRefresh(job, 10, 1);
                appendAndRefresh(job, 20, 2);
                appendAndRefresh(job, 30, 3);

                // Created after three seals, so its generations start well below the
                // first view's - which is what the reader must not carry over.
                execute(
                        "CREATE LIVE VIEW lv2 FLUSH EVERY 100ms START FROM NOW AS " +
                                "SELECT ts, sym, sum(x) OVER (" +
                                "PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW" +
                                ") s FROM base"
                );
                appendAndRefresh(job, 40, 4);

                final LiveViewInstance first = engine.getLiveViewRegistry().getViewInstance("lv");
                final LiveViewInstance second = engine.getLiveViewRegistry().getViewInstance("lv2");
                Assert.assertNotNull(first);
                Assert.assertNotNull(second);
                final ObjList<WindowFunction> firstFunctions = unwrapWindowFunctions(first);
                final ObjList<WindowFunction> secondFunctions = unwrapWindowFunctions(second);
                final RuntimeSnapshot firstState = snapshotRuntime(firstFunctions, first.getAnchorWindow());
                final RuntimeSnapshot secondState = snapshotRuntime(secondFunctions, second.getAnchorWindow());

                try (LiveViewCheckpointTimelineStoreReader reader =
                             new LiveViewCheckpointTimelineStoreReader(configuration)) {
                    // A bind that fails must leave nothing half-open behind it, or
                    // every later bind of this reader would fail on that instead.
                    try (Path missingDir = new Path().of(configuration.getDbRoot())
                            .concat("no_such_view")
                            .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)) {
                        reader.of(missingDir);
                        Assert.fail("expected a bind to a directory with no timeline to fail");
                    } catch (CairoException ignore) {
                    }

                    final long firstGeneration;
                    try (Path checkpointsDir = checkpointsDir(first)) {
                        reader.of(checkpointsDir);
                        firstGeneration = reader.restoreLatest(
                                first.getLiveViewToken().getTableId(),
                                firstFunctions,
                                first.getAnchorWindow()
                        ).generation;
                        assertRuntimeSnapshot(firstState, firstFunctions, first.getAnchorWindow());
                        // A bind that meets a reader still attached is a caller that
                        // lost its finally, and must raise rather than restore against
                        // whatever the previous bind left open.
                        try {
                            reader.of(checkpointsDir);
                            Assert.fail("expected an already-open reader to refuse a second bind");
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "already open");
                        }
                        reader.detach();
                    }

                    final long secondGeneration;
                    try (Path checkpointsDir = checkpointsDir(second)) {
                        reader.of(checkpointsDir);
                        secondGeneration = reader.restoreLatest(
                                second.getLiveViewToken().getTableId(),
                                secondFunctions,
                                second.getAnchorWindow()
                        ).generation;
                        assertRuntimeSnapshot(secondState, secondFunctions, second.getAnchorWindow());
                        reader.detach();
                    }
                    Assert.assertTrue(
                            "the second view sealed at generation " + secondGeneration
                                    + ", not below the first view's " + firstGeneration,
                            secondGeneration < firstGeneration
                    );

                    // And back, which is the ordinary case: the same view restored
                    // again through a reader that has meanwhile served another one.
                    try (Path checkpointsDir = checkpointsDir(first)) {
                        reader.of(checkpointsDir);
                        Assert.assertEquals(
                                firstGeneration,
                                reader.restoreLatest(
                                        first.getLiveViewToken().getTableId(),
                                        firstFunctions,
                                        first.getAnchorWindow()
                                ).generation
                        );
                        assertRuntimeSnapshot(firstState, firstFunctions, first.getAnchorWindow());
                        reader.detach();
                    }
                }
                assertNoRefreshFaults("lv");
                assertNoRefreshFaults("lv2");
            }
        });
    }

    @Test
    public void testRestoreNewestAndOldestLogicalRoot() throws Exception {
        assertMemoryLeak(() -> {
            createView(true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                appendAndRefresh(job, 10, 1);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);
                final RuntimeSnapshot oldestState = snapshotRuntime(functions, instance.getAnchorWindow());

                appendAndRefresh(job, 20, 2);
                appendAndRefresh(job, 30, 3);
                appendAndRefresh(job, 40, 4);
                final RuntimeSnapshot newestState = snapshotRuntime(functions, instance.getAnchorWindow());

                try (
                        Path checkpointsDir = checkpointsDir(instance);
                        LiveViewCheckpointTimelineStoreReader reader =
                                new LiveViewCheckpointTimelineStoreReader(configuration)
                ) {
                    reader.of(checkpointsDir);
                    final LiveViewCheckpointTimelineStoreReader.Result oldest = reader.restore(
                            ts(timestamp(10)),
                            0,
                            instance.getLiveViewToken().getTableId(),
                            functions,
                            instance.getAnchorWindow()
                    );
                    Assert.assertEquals(4, oldest.generation);
                    Assert.assertEquals(1, oldest.effectiveLvRowPosition);
                    assertRuntimeSnapshot(oldestState, functions, instance.getAnchorWindow());

                    final LiveViewCheckpointTimelineStoreReader.Result newest = reader.restore(
                            ts(timestamp(40)),
                            3,
                            instance.getLiveViewToken().getTableId(),
                            functions,
                            instance.getAnchorWindow()
                    );
                    Assert.assertEquals(4, newest.generation);
                    Assert.assertEquals(4, newest.effectiveLvRowPosition);
                    assertRuntimeSnapshot(newestState, functions, instance.getAnchorWindow());
                }
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testSteadySealPublishesNoSeedResumePoint() throws Exception {
        // The seed cursor a generation carries is what tells a restart mid-sweep where to put the
        // base cursor back. A steady cadence seal is not a mid-sweep event, so it must publish the
        // sentinel instead - otherwise a view whose sweep finished, and whose _lv.s crash-landed
        // still reading SEEDING, would resume the cursor from a coordinate the steady seal never
        // measured.
        assertMemoryLeak(() -> {
            createView(true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                appendAndRefresh(job, 10, 1);
                appendAndRefresh(job, 20, 2);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);

                try (
                        Path checkpointsDir = checkpointsDir(instance);
                        LiveViewCheckpointTimelineStoreReader reader =
                                new LiveViewCheckpointTimelineStoreReader(configuration)
                ) {
                    reader.of(checkpointsDir);
                    final LiveViewCheckpointTimelineStoreReader.Result newest = reader.restoreLatest(
                            instance.getLiveViewToken().getTableId(),
                            functions,
                            instance.getAnchorWindow()
                    );
                    Assert.assertEquals(
                            "restoreLatest must land on the newest boundary",
                            ts(timestamp(20)),
                            newest.maxTimestamp
                    );
                    Assert.assertEquals(2, newest.effectiveLvRowPosition);
                    Assert.assertEquals(
                            "a steady seal must not advertise a seed resume point",
                            Numbers.LONG_NULL,
                            newest.seedCursorOffset
                    );
                }
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAnchorRootsRestoreTheAnchorValuesTheirOwnBoundaryHeld() throws Exception {
        assertMemoryLeak(() -> {
            createView(true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertAndRefresh(job, "2026-01-01T00:00:10.000000Z", "a");
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);
                final LiveViewWindow anchorWindow = instance.getAnchorWindow();
                Assert.assertNotNull(anchorWindow);

                insertAndRefresh(job, "2026-01-01T00:00:20.000000Z", "b");
                // Both symbols anchored on day one.
                final RuntimeSnapshot dayOne = snapshotRuntime(functions, anchorWindow);
                Assert.assertEquals(2, anchorWindow.getAnchorMapSize());

                // Crossing midnight moves only symbol 'a' into a new segment, so
                // the newest root shares 'b' with the previous one and versions 'a'.
                insertAndRefresh(job, "2026-01-02T00:00:10.000000Z", "a");
                final RuntimeSnapshot dayTwo = snapshotRuntime(functions, anchorWindow);
                Assert.assertEquals(2, anchorWindow.getAnchorMapSize());
                Assert.assertFalse(Arrays.equals(dayOne.anchor, dayTwo.anchor));

                try (
                        Path checkpointsDir = checkpointsDir(instance);
                        LiveViewCheckpointTimelineStoreReader reader =
                                new LiveViewCheckpointTimelineStoreReader(configuration)
                ) {
                    reader.of(checkpointsDir);
                    reader.restore(
                            ts("2026-01-01T00:00:20.000000Z"),
                            1,
                            instance.getLiveViewToken().getTableId(),
                            functions,
                            anchorWindow
                    );
                    assertRuntimeSnapshot(dayOne, functions, anchorWindow);

                    reader.restore(
                            ts("2026-01-02T00:00:10.000000Z"),
                            2,
                            instance.getLiveViewToken().getTableId(),
                            functions,
                            anchorWindow
                    );
                    assertRuntimeSnapshot(dayTwo, functions, anchorWindow);
                }
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testRestoreRejectsRootWhoseStateVersionDisagreesWithItsIdentity() throws Exception {
        assertMemoryLeak(() -> {
            createView(false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                appendAndRefresh(job, 10, 1);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final ObjList<WindowFunction> compiled = unwrapWindowFunctions(instance);
                Assert.assertEquals(1, compiled.size());

                // A build that bumps a function's state layout also changes its codec
                // identity, so its roots no longer resolve and the directory lookup turns
                // them away first. Keeping the identity and moving only the recorded
                // version is the malformed root the version check is left to catch.
                final ObjList<WindowFunction> drifted = new ObjList<>();
                final VersionDriftStub stub = new VersionDriftStub(compiled.getQuick(0));
                drifted.add(stub);
                try (
                        Path checkpointsDir = checkpointsDir(instance);
                        LiveViewCheckpointTimelineStoreReader reader =
                                new LiveViewCheckpointTimelineStoreReader(configuration)
                ) {
                    reader.of(checkpointsDir);
                    try {
                        reader.restore(
                                ts(timestamp(10)),
                                0,
                                instance.getLiveViewToken().getTableId(),
                                drifted,
                                null
                        );
                        Assert.fail("expected function state format version rejection");
                    } catch (CairoException e) {
                        Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                        TestUtils.assertContains(
                                e.getFlyweightMessage(),
                                "function state format version does not match the compiled runtime"
                        );
                    }
                } finally {
                    Misc.free(stub);
                }
                Assert.assertFalse(
                        "validation must reject before the restore clears any function state",
                        stub.restoreBegun
                );
            }
        });
    }

    @Test
    public void testRestoreRejectsTruncatedOldRootBeforeMutatingRuntime() throws Exception {
        assertMemoryLeak(() -> {
            createView(false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                appendAndRefresh(job, 10, 1);
                appendAndRefresh(job, 20, 2);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);
                final RuntimeSnapshot before = snapshotRuntime(functions, null);

                long segmentId;
                long fileLength;
                try (
                        LiveViewCheckpointMetaStore store = openStore(instance);
                        LiveViewCheckpointGenerationPin pin = store.pin();
                        LiveViewCheckpointTimelineReader timeline = openTimelineReader(instance);
                        LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                        LiveViewCheckpointSegmentDirectoryReader directory =
                                new LiveViewCheckpointSegmentDirectoryReader(configuration);
                        Path checkpointsDir = checkpointsDir(instance)
                ) {
                    final LiveViewCheckpointTimelineEntry oldest = new LiveViewCheckpointTimelineEntry();
                    Assert.assertTrue(timeline.findExact(pin.getTimelineRootRef(), ts(timestamp(10)), 0, oldest));
                    root.of(checkpointsDir, oldest.rootRef);
                    segmentId = root.getSegmentId(0);
                    directory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
                    fileLength = directory.getFileLength(segmentId);
                }
                try (Path checkpointsDir = checkpointsDir(instance); Path dataPath = new Path()) {
                    LiveViewCheckpointLayout.dataSegmentPath(
                            dataPath,
                            checkpointsDir,
                            segmentId
                    );
                    final FilesFacade ff = configuration.getFilesFacade();
                    final long fd = ff.openRW(dataPath.$(), 0);
                    try {
                        Assert.assertTrue(ff.truncate(fd, fileLength - 1));
                    } finally {
                        ff.close(fd);
                    }
                    try (LiveViewCheckpointTimelineStoreReader reader =
                                 new LiveViewCheckpointTimelineStoreReader(configuration)) {
                        reader.of(checkpointsDir);
                        try {
                            reader.restore(
                                    ts(timestamp(10)),
                                    0,
                                    instance.getLiveViewToken().getTableId(),
                                    functions,
                                    null
                            );
                            Assert.fail("expected truncated logical-root data rejection");
                        } catch (CairoException e) {
                            Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                            TestUtils.assertContains(e.getFlyweightMessage(), "data segment file length mismatch");
                        }
                    }
                }
                assertRuntimeSnapshot(before, functions, null);
            }
        });
    }

    private static String denseTimestamp(int second) {
        return String.format(
                "2026-01-01T%02d:%02d:%02d.000000Z",
                second / 3600,
                (second % 3600) / 60,
                second % 60
        );
    }

    private void appendAndRefresh(LiveViewRefreshJob job, int second, long value) throws Exception {
        setCurrentMicros(currentMicros + 200_000);
        execute("INSERT INTO base VALUES ('" + timestamp(second) + "', 'a', " + value + ")");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void insertAndRefresh(LiveViewRefreshJob job, String timestamp, String symbol) throws Exception {
        setCurrentMicros(currentMicros + 200_000);
        execute("INSERT INTO base VALUES ('" + timestamp + "', '" + symbol + "', 1)");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private static int countDataSegments(LiveViewCheckpointSegmentDirectoryReader directory) {
        final int[] count = {0};
        directory.iterateAll(entry -> {
            if (!entry.isMetadata()) {
                count[0]++;
            }
        });
        return count[0];
    }

    private static long lastDataSegmentId(LiveViewCheckpointSegmentDirectoryReader directory) {
        final long[] last = {-1};
        directory.iterateAll(entry -> {
            if (!entry.isMetadata()) {
                last[0] = Math.max(last[0], entry.segmentId);
            }
        });
        return last[0];
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private void commitDenseAndRefresh(LiveViewRefreshJob job, int commit) throws Exception {
        setCurrentMicros(currentMicros + 200_000);
        final StringBuilder sql = new StringBuilder("INSERT INTO base VALUES ");
        final int firstSecond = (commit - 1) * DENSE_ROWS_PER_COMMIT;
        for (int i = 0; i < DENSE_ROWS_PER_COMMIT; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("('").append(denseTimestamp(firstSecond + i)).append("', 'a', ")
                    .append(firstSecond + i).append(')');
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createDenseView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute(
                "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                        "SELECT ts, sym, sum(x) OVER (" +
                        "PARTITION BY sym ORDER BY ts RANGE BETWEEN '1000' SECOND PRECEDING AND CURRENT ROW" +
                        ") s FROM base"
        );
    }

    private void createView(boolean anchored) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        if (anchored) {
            execute(
                    "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                            "SELECT ts, sym, row_number() OVER w s FROM base " +
                            "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')"
            );
        } else {
            execute(
                    "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                            "SELECT ts, sym, sum(x) OVER (" +
                            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW" +
                            ") s FROM base"
            );
        }
    }

    private LiveViewCheckpointMetaStore openStore(LiveViewInstance instance) {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        try (Path checkpointsDir = checkpointsDir(instance)) {
            store.of(checkpointsDir);
        }
        return store;
    }

    private LiveViewCheckpointTimelineReader openTimelineReader(LiveViewInstance instance) {
        final LiveViewCheckpointTimelineReader reader = new LiveViewCheckpointTimelineReader(configuration);
        try (Path checkpointsDir = checkpointsDir(instance)) {
            reader.of(checkpointsDir);
        }
        return reader;
    }

    private static void assertRuntimeSnapshot(
            RuntimeSnapshot expected,
            ObjList<WindowFunction> functions,
            LiveViewWindow anchorWindow
    ) {
        final RuntimeSnapshot actual = snapshotRuntime(functions, anchorWindow);
        Assert.assertArrayEquals(expected.anchor, actual.anchor);
        Assert.assertEquals(expected.functions.length, actual.functions.length);
        for (int i = 0; i < expected.functions.length; i++) {
            Assert.assertArrayEquals("function snapshot mismatch at index " + i,
                    expected.functions[i], actual.functions[i]);
        }
    }

    private static byte[] copyBytes(MemoryCARW memory) {
        final int length = (int) memory.getAppendOffset();
        final byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = memory.getByte(i);
        }
        return bytes;
    }

    private static RuntimeSnapshot snapshotRuntime(
            ObjList<WindowFunction> functions,
            LiveViewWindow anchorWindow
    ) {
        byte[] anchor = null;
        if (anchorWindow != null) {
            try (MemoryCARW sink = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                anchorWindow.snapshot(sink);
                anchor = copyBytes(sink);
            }
        }
        final byte[][] states = new byte[functions.size()][];
        int count = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (!function.supportsCheckpointState()) {
                continue;
            }
            try (MemoryCARW sink = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                LiveViewFunctionSnapshot.write(sink, function);
                states[count++] = copyBytes(sink);
            }
        }
        return new RuntimeSnapshot(anchor, Arrays.copyOf(states, count));
    }

    private static ObjList<WindowFunction> unwrapWindowFunctions(LiveViewInstance instance) {
        RecordCursorFactory factory = instance.getCompiledFactory();
        while (factory != null) {
            if (factory instanceof WindowRecordCursorFactory windowFactory) {
                return windowFactory.getWindowFunctions();
            }
            if (factory instanceof QueryProgress) {
                factory = factory.getBaseFactory();
                continue;
            }
            break;
        }
        throw new IllegalStateException("compiled factory does not contain a WindowRecordCursorFactory");
    }

    private static String timestamp(int second) {
        return "2026-01-01T00:00:" + (second < 10 ? "0" : "") + second + ".000000Z";
    }

    @Test
    public void testStartupAdoptsDurableTimelineWalFloorWithoutRestoringRoot() throws Exception {
        assertMemoryLeak(() -> {
            createView(false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                appendAndRefresh(job, 10, 1);
                appendAndRefresh(job, 20, 2);
            }

            final LiveViewInstance before = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(before);
            final long durableFloor = before.getCheckpointTimelineWalPurgeFloor();
            Assert.assertTrue(durableFloor >= 0);

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            final LiveViewInstance after = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(after);
            Assert.assertEquals(durableFloor, after.getCheckpointTimelineWalPurgeFloor());
            Assert.assertEquals(
                    "catalogue load must not restore mutable runtime before the refresh worker pins the generation",
                    Numbers.LONG_NULL,
                    after.getHeadCheckpointRestoreMicros()
            );
        });
    }

    @Test
    public void testRestartRestoresTimelineAndRebuildsOnlyCheckpointToFrontierGap() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 100);
        assertMemoryLeak(() -> {
            createView(false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                appendAndRefresh(job, 10, 1); // first cadence event always seals
                appendAndRefresh(job, 20, 2); // durable output beyond the root, no seal
            }

            final LiveViewInstance before = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(before);
            Assert.assertNotEquals(Numbers.LONG_NULL, before.getHeadCheckpointLvSeqTxn());

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            // Startup publishes the head from the timeline generation alone. The
            // root's own maxTs is still a placeholder here; only the first refresh
            // tick pins the generation and reads it.
            Assert.assertNotEquals(Numbers.LONG_NULL, reloaded.getHeadCheckpointLvSeqTxn());
            Assert.assertEquals(
                    "ACTIVE startup must not read a root before the worker pins the generation",
                    Numbers.LONG_NULL,
                    reloaded.getHeadCheckpointMaxTs()
            );

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // A fresh base notification drives the single-shot recovery,
                // then ordinary refresh continues from the reconciled boundary.
                appendAndRefresh(job, 30, 3);
                Assert.assertTrue(reloaded.isCheckpointRestoreSucceeded());
                Assert.assertEquals(3, reloaded.getLvRowsTotal());
                Assert.assertEquals(
                        "a valid timeline root plus (B,F] replay must not fall back to START FROM",
                        0,
                        reloaded.getO3BoundaryReplayRows()
                );
            }

            assertQuery("select ts, sym, s from lv order by ts")
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tsym\ts\n" +
                            "2026-01-01T00:00:10.000000Z\ta\t1.0\n" +
                            "2026-01-01T00:00:20.000000Z\ta\t3.0\n" +
                            "2026-01-01T00:00:30.000000Z\ta\t6.0\n");
        });
    }

    @Test
    public void testRestartRestoresATimelineWhoseFunctionsHoldNoState() throws Exception {
        // Every window function of this view is stateless - last_value over a frame ending at
        // the current row reads the argument off the row it was handed - so each root seals an
        // empty function set and a restore puts nothing back. What the root still carries is
        // the boundary itself, and that is what has to survive: the restart resumes from it
        // and replays only the gap above it rather than the whole view from START FROM.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                            "SELECT ts, sym, last_value(x) OVER (" +
                            "PARTITION BY sym ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" +
                            ") l FROM base"
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                appendAndRefresh(job, 10, 1); // first cadence event always seals
                appendAndRefresh(job, 20, 2); // durable output beyond the root, no seal
            }

            final LiveViewInstance before = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(before);
            Assert.assertNotEquals(Numbers.LONG_NULL, before.getHeadCheckpointLvSeqTxn());

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                appendAndRefresh(job, 30, 3);
                Assert.assertTrue(reloaded.isCheckpointRestoreSucceeded());
                Assert.assertEquals(3, reloaded.getLvRowsTotal());
                Assert.assertEquals(
                        "an empty state image is a valid root, not a missing one",
                        0,
                        reloaded.getO3BoundaryReplayRows()
                );
            }

            assertQuery("select ts, sym, l from lv order by ts")
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tsym\tl\n" +
                            "2026-01-01T00:00:10.000000Z\ta\t1\n" +
                            "2026-01-01T00:00:20.000000Z\ta\t2\n" +
                            "2026-01-01T00:00:30.000000Z\ta\t3\n");
        });
    }

    @Test
    public void testRestartExcludesApplyAheadO3BelowFrontierUntilOrdinaryClassification() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 100);
        assertMemoryLeak(() -> {
            createView(false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                appendAndRefresh(job, 10, 1); // timeline root B=10
                appendAndRefresh(job, 30, 2); // durable frontier F=30
            }

            // Base apply runs ahead of live-view materialization. Its timestamp
            // lies below F, so a recovery scan of the current applied base would
            // incorrectly incorporate it before the reconciled base seqTxn.
            execute("INSERT INTO base VALUES ('2026-01-01T00:00:20.000000Z', 'a', 5)");
            drainWalQueue();

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            Assert.assertTrue(reloaded.isCheckpointRestoreSucceeded());
            // Recovery must not swallow the apply-ahead txn: it stays above the
            // reconciled base boundary and is classified as ordinary O3 afterwards.
            // It resumes from the restored root rather than rebuilding from the
            // view's START FROM boundary - the restore publishes that root's real
            // maxTs, which is what makes the cheaper anchored arm eligible.
            Assert.assertTrue(
                    "the apply-ahead txn must enter the ordinary O3 path",
                    reloaded.getO3ResumeReplayRows() > 0
            );
            Assert.assertEquals(
                    "an anchored resume must not fall back to the O(view age) rebuild",
                    0,
                    reloaded.getO3BoundaryReplayRows()
            );
            assertQuery("select ts, sym, s from lv order by ts")
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tsym\ts\n" +
                            "2026-01-01T00:00:10.000000Z\ta\t1.0\n" +
                            "2026-01-01T00:00:20.000000Z\ta\t6.0\n" +
                            "2026-01-01T00:00:30.000000Z\ta\t8.0\n");
        });
    }

    private static final class RuntimeSnapshot {
        private final byte[] anchor;
        private final byte[][] functions;

        private RuntimeSnapshot(byte[] anchor, byte[][] functions) {
            this.anchor = anchor;
            this.functions = functions;
        }
    }

    /**
     * Wears a compiled function's checkpoint identity and key schema but reports the
     * next state layout version, which is the one shape that reaches the function
     * root's version check with everything ahead of it agreeing.
     */
    private static final class VersionDriftStub extends BaseWindowFunction {
        private final ColumnTypes keyColumnTypes;
        private final int stateFormatVersion;
        private boolean restoreBegun;

        private VersionDriftStub(WindowFunction compiled) {
            super(null);
            this.keyColumnTypes = compiled.getCheckpointKeyColumnTypes();
            this.stateFormatVersion = compiled.checkpointStateFormatVersion() + 1;
            setCheckpointCompilerMetadata(compiled.checkpointFunctionIdentity(), compiled.checkpointDependency());
        }

        @Override
        public int checkpointStateFormatVersion() {
            return stateFormatVersion;
        }

        @Override
        public ColumnTypes getCheckpointKeyColumnTypes() {
            return keyColumnTypes;
        }

        @Override
        public String getName() {
            return "version-drift";
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void onCheckpointRestoreBegin() {
            restoreBegun = true;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public boolean supportsCheckpointState() {
            return true;
        }
    }
}
