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
import io.questdb.cairo.lv.LiveViewCheckpointAnchorRoot;
import io.questdb.cairo.lv.LiveViewCheckpointDataSegmentReader;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewCheckpointWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LiveViewCheckpointTimelineSealTest extends AbstractLiveViewTest {

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCheckpointCadence() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_RETENTION_COUNT, 2);
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

                    final LiveViewCheckpointSegmentDirectory directory =
                            new LiveViewCheckpointSegmentDirectory(configuration);
                    try {
                        try (Path checkpointsDir = checkpointsDir(instance)) {
                            directory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
                        }
                        Assert.assertEquals(1, directory.size());
                        Assert.assertEquals(
                                "the retry must not reuse the final-name orphan's segment id",
                                expectedRetryDataSegmentId,
                                directory.getSegmentId(0)
                        );
                        Assert.assertEquals(1, directory.getReferenceCountAt(0));
                    } finally {
                        directory.close();
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
                Assert.assertEquals(2, instance.getRetainedCheckpointCount());
                try (
                        LiveViewCheckpointMetaStore store = openStore(instance);
                        LiveViewCheckpointGenerationPin pin = store.pin();
                        LiveViewCheckpointTimelineReader reader = openTimelineReader(instance);
                        LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                        LiveViewCheckpointAnchorRoot anchorRoot = new LiveViewCheckpointAnchorRoot(configuration);
                        LiveViewCheckpointDataSegmentReader dataReader =
                                new LiveViewCheckpointDataSegmentReader(configuration);
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
                    final LiveViewCheckpointStatePageRef anchorStateRef = new LiveViewCheckpointStatePageRef();
                    anchorRoot.getStatePageRef(anchorStateRef);
                    Assert.assertEquals(LiveViewCheckpointTimelineStoreWriter.ANCHOR_STATE_PAGE_KIND,
                            anchorStateRef.getPageKind());
                    final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
                    root.getFunctionDirectoryRef(functionDirectoryRef);
                    functions.of(checkpointsDir, functionDirectoryRef);
                    Assert.assertEquals(1, functions.size());

                    final LiveViewCheckpointSegmentDirectory directory =
                            new LiveViewCheckpointSegmentDirectory(configuration);
                    try {
                        directory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
                        Assert.assertEquals(3, directory.size());
                        for (int i = 0; i < directory.size(); i++) {
                            Assert.assertEquals(1, directory.getReferenceCountAt(i));
                        }
                        dataReader.of(
                                checkpointsDir,
                                anchorStateRef.getSegmentId(),
                                directory.getFileLength(anchorStateRef.getSegmentId())
                        );
                        dataReader.openPage(
                                anchorStateRef,
                                LiveViewCheckpointTimelineStoreWriter.ANCHOR_STATE_PAGE_KIND,
                                LiveViewCheckpointTimelineStoreWriter.RAW_CODEC,
                                0,
                                1,
                                Integer.MAX_VALUE
                        );
                        Assert.assertEquals(anchorStateRef.getDecodedLength(), dataReader.getPageStoredLength());
                    } finally {
                        directory.close();
                    }
                }
                assertNoRefreshFaults("lv");
            }
        });
    }

    private void appendAndRefresh(LiveViewRefreshJob job, int second, long value) throws Exception {
        setCurrentMicros(currentMicros + 200_000);
        execute("INSERT INTO base VALUES ('" + timestamp(second) + "', 'a', " + value + ")");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME);
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
                    "watermark adoption must not start the later runtime-root restore step",
                    Numbers.LONG_NULL,
                    after.getHeadCheckpointRestoreMicros()
            );
        });
    }
}
