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
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointKeyedReplay;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointOutputKeyDomain;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Coverage for the native-memory lifecycle of the seal's scratch buffers. The
 * timeline store writer encodes every whole-state image through one reusable
 * buffer, and the writer instance is shared across every view a refresh worker
 * seals, so the buffer must hand its capacity back when a seal completes: a
 * single view with an outlier state image must not pin that capacity for the
 * lifetime of the worker. The seals run through
 * {@link LiveViewCheckpointTimelineStoreWriter#append} with a stub scalar
 * function whose image bytes each case sizes exactly - no production function
 * varies its image length at will.
 */
public class LiveViewCheckpointSealScratchMemoryTest extends AbstractCairoTest {

    // Room under the RSS ceiling for what beginRepair allocates before it copies Q, and far
    // below the copy of a 16,384-key Q, whose slot table alone is 2 MiB.
    private static final long CAPTURE_RSS_SLACK_BYTES = 65_536;
    private static final long DEFINITION_TXN = 7;
    // Keys a failing freeze walk is given, half of which it freezes before it throws.
    private static final int FAILING_FREEZE_KEYS = 4_096;
    private static final long LIFECYCLE_IDENTITY = 201;
    private static final long LIFECYCLE_IDENTITY_A = 202;
    private static final long LIFECYCLE_IDENTITY_B = 203;
    private static final String LV_DIR = "lv_seal_scratch_memory";
    // Comfortably above every allocation the seal path retains by design, and
    // far below the state image, so the assertion separates "scratch released"
    // from "scratch retained" with no sensitivity to incidental allocations.
    private static final long RELEASED_TOLERANCE_BYTES = 1_048_576;
    private static final int STATE_IMAGE_BYTES = 8_388_608;
    private static final int WIDE_KEY_COLUMNS = 32;
    private static final int WIDE_KEY_DOMAIN = 4_096;

    @Before
    public void setUp() {
        super.setUp();
        createCheckpointLayout(LV_DIR);
    }

    @Test
    public void testPartitionMapPoolOwnerSurvivesPublicationFailureAndRetry() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    PartitionedStateStub stub = new PartitionedStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                stub.putState(11, 0x11);
                final int poolIdentity = writer.getPartitionMapObjectPoolIdentityForTest();
                seal(writer, stub, 1);
                Assert.assertEquals(poolIdentity, writer.getPartitionMapObjectPoolIdentityForTest());
                final int retainedNodeIdentity = writer.getFirstRetainedPartitionMapNodeIdentityForTest();
                Assert.assertNotEquals(0, retainedNodeIdentity);

                writer.setTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                try {
                    seal(writer, stub, 2);
                    Assert.fail("expected injected publication failure");
                } catch (CairoException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "test failure after live view checkpoint metadata publication"
                    );
                }
                Assert.assertEquals(poolIdentity, writer.getPartitionMapObjectPoolIdentityForTest());
                Assert.assertEquals(
                        retainedNodeIdentity,
                        writer.getFirstRetainedPartitionMapNodeIdentityForTest()
                );
                final int warmedObjectCount = writer.getRetainedPartitionMapObjectCountForTest();
                Assert.assertTrue(warmedObjectCount > 0);

                writer.setTestFailureStage(0);
                seal(writer, stub, 2);
                Assert.assertEquals(poolIdentity, writer.getPartitionMapObjectPoolIdentityForTest());
                Assert.assertEquals(
                        retainedNodeIdentity,
                        writer.getFirstRetainedPartitionMapNodeIdentityForTest()
                );
                Assert.assertEquals(warmedObjectCount, writer.getRetainedPartitionMapObjectCountForTest());

                seal(writer, stub, 3);
                Assert.assertEquals(poolIdentity, writer.getPartitionMapObjectPoolIdentityForTest());
                Assert.assertEquals(
                        retainedNodeIdentity,
                        writer.getFirstRetainedPartitionMapNodeIdentityForTest()
                );
                Assert.assertEquals(warmedObjectCount, writer.getRetainedPartitionMapObjectCountForTest());
            }
        });
    }

    @Test
    public void testAChainedCaptureJudgesEachBoundaryAgainstTheOneItStagedBelow() throws Exception {
        // A chained capture seeds boundary i's root from boundary i - 1's new root, so a key whose
        // image boundary i shares with boundary i - 1 is left out of boundary i's puts. What that
        // image is compared with has to be the entry boundary i - 1 staged, which the chain finds
        // in its own index over the capture's frozen keys. Here key 11 returns at boundary 1 to
        // the state the published predecessor holds, after boundary 0 moved it away: judged
        // against the predecessor instead, it would look unchanged, and boundary 0's state would
        // stand in boundary 1's root.
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireRefreshTracker();
            try (
                    PartitionedStateStub stub = new PartitionedStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration);
                    Path dir = new Path()
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                stub.putState(11, 0x55);
                stub.putState(12, 0x66);
                seal(writer, functions, LV_DIR, 1, LIFECYCLE_IDENTITY, null);
                stub.putState(11, 0x01);
                stub.putState(12, 0x02);
                for (int seq = 2; seq <= 4; seq++) {
                    seal(writer, functions, LV_DIR, seq, LIFECYCLE_IDENTITY, null);
                }

                checkpointsDir(dir);
                final ObjList<LiveViewCheckpointTimelineEntry> boundaries = new ObjList<>();
                try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                             writer.beginRepair(dir, null, tracker, true)) {
                    capture.collectBoundaries(1_500_000, 3_500_000, boundaries);
                    Assert.assertEquals(2, boundaries.size());
                    stub.putState(11, 0x77);
                    stub.putState(12, 0x88);
                    capture.capture(boundaries.getQuick(0), functions, null, 2);
                    stub.putState(11, 0x55);
                    capture.capture(boundaries.getQuick(1), functions, null, 3);
                    writer.publishRepair(capture, DEFINITION_TXN, 4, 4, 0, LIFECYCLE_IDENTITY, true, 3_500_000, 0);
                }

                assertPartitionState(dir, boundaries.getQuick(0), 11, 0x77);
                assertPartitionState(dir, boundaries.getQuick(0), 12, 0x88);
                assertPartitionState(dir, boundaries.getQuick(1), 11, 0x55);
                assertPartitionState(dir, boundaries.getQuick(1), 12, 0x88);
                Assert.assertEquals("the capture must release its tracker charge", 0, tracker.getUsed());
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testAppendChargesScratchToViewTrackerAndReturnsItClean() throws Exception {
        // The writer is shared across every view its worker seals, and each
        // view's tracker is pooled and recycled on a used == 0 guard, so a seal
        // must return the tracker with no charge left on it.
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireRefreshTracker();
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                stub.state = filled(STATE_IMAGE_BYTES, (byte) 0x5A);
                seal(writer, stub, 1, tracker);
                Assert.assertEquals("no charge may outlive the seal", 0, tracker.getUsed());
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testAppendFailsWhenScratchBreachesRefreshMemoryLimit() throws Exception {
        // The scratch is charged to the sealed view's refresh tracker, so the
        // configured per-view budget caps it: an image that does not fit fails
        // the seal at the allocation, and the failure path still releases the
        // scratch and returns the tracker clean.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 1_048_576);
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireRefreshTracker();
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                stub.state = filled(STATE_IMAGE_BYTES, (byte) 0x5A);
                final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                try {
                    seal(writer, stub, 1, tracker);
                    Assert.fail("expected a refresh memory limit breach");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    TestUtils.assertContains(e.getFlyweightMessage(), "LIVE_VIEW_REFRESH");
                }
                final long retained = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT) - baseline;
                Assert.assertTrue(
                        "a failed seal must release its scratch too, retained=" + retained,
                        retained < RELEASED_TOLERANCE_BYTES
                );
                Assert.assertEquals("no charge may outlive the seal", 0, tracker.getUsed());
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testAppendFreezesItsKeysOutsideTheRefreshMemoryLimit() throws Exception {
        // A seal's frozen keys and the partition index over them are native memory tagged
        // NATIVE_LIVE_VIEW_IN_MEM, which the process totals count, and the sealed view's
        // refresh tracker does not: they replaced heap arrays and a heap index that tracker
        // never saw, and a per-view limit sized before they went native has to keep
        // admitting the same seal. Here the keys alone are twice the per-view budget and the seal still fits
        // in it. Every state is unchanged from the root below and the seal is strictly
        // forward, so no build stages a single key: the tracker is charged for the freeze
        // scratch's two buffers alone. The seal still hands every key byte back.
        final long limit = 1_048_576;
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_REFRESH_MEMORY_LIMIT_BYTES, limit);
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireRefreshTracker();
            try (
                    PartitionedStateStub stub = new PartitionedStateStub(WIDE_KEY_COLUMNS);
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final int keyCount = 2 * WIDE_KEY_DOMAIN;
                Assert.assertTrue((long) keyCount * wideKeyBytes() > limit);
                putStates(stub, keyCount, 1);
                seal(writer, stub, 1);
                seal(writer, stub, 2);
                final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                try {
                    seal(writer, stub, 3, tracker);
                } catch (CairoException e) {
                    throw new AssertionError(
                            "frozen keys must stay outside the refresh memory limit: " + keyCount + " keys of "
                                    + wideKeyBytes() + " bytes each failed the seal: " + e.getFlyweightMessage(),
                            e
                    );
                }
                final long retained = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline;
                Assert.assertTrue(
                        "the seal must hand its frozen keys back, retained=" + retained,
                        retained < RELEASED_TOLERANCE_BYTES
                );
                Assert.assertEquals("no charge may outlive the seal", 0, tracker.getUsed());
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testAnOpenCaptureChargesTheViewsTrackerNothingPerFrozenKey() throws Exception {
        // A capture holds the keys it froze - and a chained one its two indexes over them -
        // until it publishes or is abandoned, possibly across refresh turns. They are native
        // memory tagged NATIVE_LIVE_VIEW_IN_MEM, and they stay off the repaired view's
        // refresh tracker, which counts what it counted while they were heap objects: the
        // freeze scratch's key and state buffers, sized by the widest key and image rather
        // than by how many keys the capture froze. So an open capture keeps the same charge
        // on the tracker for one key as for thousands, and none once it closes.
        assertMemoryLeak(() -> {
            int layout = 0;
            for (boolean isChained : new boolean[]{false, true}) {
                final long narrowCharge = openCaptureCharge(LV_DIR + '_' + layout++, 1, isChained);
                Assert.assertTrue(
                        "the freeze scratch's buffers must still charge the view's tracker [chained=" + isChained + ']',
                        narrowCharge > 0
                );
                final long wideCharge = openCaptureCharge(LV_DIR + '_' + layout++, WIDE_KEY_DOMAIN, isChained);
                Assert.assertEquals(
                        "an open capture's tracker charge must not grow with the keys it froze [chained=" + isChained + ']',
                        narrowCharge,
                        wideCharge
                );
            }
        });
    }

    @Test
    public void testASealThatFailsMidFreezeFreesItsPayloads() throws Exception {
        // The inline arm freezes each key's state image into the scratch's payload arena as
        // it walks, so a freeze that throws part-way unwinds with the arena holding every image
        // it froze so far. The append's own release is the only owner left to free them.
        assertMemoryLeak(() -> {
            try (
                    PartitionedStateStub stub = new PartitionedStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                putStates(stub, FAILING_FREEZE_KEYS, 1);
                seal(writer, stub, 1);
                putStates(stub, FAILING_FREEZE_KEYS, 2);
                stub.failAfterFreezes(FAILING_FREEZE_KEYS / 2);
                try {
                    seal(writer, stub, 2);
                    Assert.fail("the seal must fail mid-freeze");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "simulated live view checkpoint freeze failure");
                }
                Assert.assertEquals("the seal must have frozen half its keys", FAILING_FREEZE_KEYS / 2, stub.freezeCount);
                Assert.assertEquals(
                        "a failed seal must free the payloads it froze",
                        0,
                        writer.getRetainedFrozenPayloadBytesForTest()
                );
                // The writer stays usable, and the next seal publishes every key.
                seal(writer, stub, 3);
                Assert.assertEquals(FAILING_FREEZE_KEYS, writer.getLastBoundaryPartitionPuts());
                Assert.assertEquals(0, writer.getRetainedFrozenPayloadBytesForTest());
            }
        });
    }

    @Test
    public void testAChainedRepairCaptureThatFailsMidFreezeFreesItsPayloadsOnClose() throws Exception {
        assertRepairCaptureThatFailsMidFreezeFreesItsPayloadsOnClose(true);
    }

    @Test
    public void testARepairCaptureThatFailsMidFreezeFreesItsPayloadsOnClose() throws Exception {
        assertRepairCaptureThatFailsMidFreezeFreesItsPayloadsOnClose(false);
    }

    @Test
    public void testAnOpenCaptureSizesItsInlinePayloadArenaInOneAllocation() throws Exception {
        // The inline arm sizes the scratch's payload arena for every key the walk may image
        // before it images the first, from that walk's own key count, so the open capture
        // holds exactly the pages its records need: 5,000 eight-byte states take 16-byte
        // records, 80,000 bytes in 20 pages. An arena grown record by record would double its
        // way from one page to 32 instead.
        final int keyCount = 5_000;
        final long expectedBytes = 20 * 4_096;
        assertMemoryLeak(() -> {
            try (
                    PartitionedStateStub stub = new PartitionedStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration);
                    Path dir = new Path()
            ) {
                putStates(stub, keyCount, 1);
                seal(writer, stub, 1);
                checkpointsDir(dir);
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                             writer.beginRepair(dir, null, null, false)) {
                    final ObjList<LiveViewCheckpointTimelineEntry> boundaries = new ObjList<>();
                    capture.collectBoundaries(0, 2_000_000, boundaries);
                    Assert.assertEquals(1, boundaries.size());
                    Assert.assertEquals(0, writer.getRetainedFrozenPayloadBytesForTest());
                    final int freezesBefore = stub.freezeCount;
                    capture.capture(boundaries.getQuick(0), functions, null, 1);
                    Assert.assertEquals("the capture must image every key", keyCount, stub.freezeCount - freezesBefore);
                    Assert.assertEquals(
                            "the capture must size its payload arena for its " + keyCount + " inline states in one"
                                    + " allocation",
                            expectedBytes,
                            writer.getRetainedFrozenPayloadBytesForTest()
                    );
                }
            }
        });
    }

    @Test
    public void testClosingTheWriterBeforeItsParkedCapturesReleasesEveryFrozenKey() throws Exception {
        // A closing refresh worker frees its writer - and with it every freeze scratch the
        // writer pooled, leased ones included - before it discards the repairs it parked.
        // Each parked capture then releases a scratch its writer already closed, so that
        // release has to find the frozen keys and the partition index gone and free nothing
        // twice, and a chained capture's own indexes over those keys still have to go with it.
        assertMemoryLeak(() -> {
            final MemoryTracker trackerA = acquireRefreshTracker();
            final MemoryTracker trackerB = acquireRefreshTracker();
            try (
                    PartitionedStateStub partitionA = new PartitionedStateStub(WIDE_KEY_COLUMNS);
                    PartitionedStateStub partitionB = new PartitionedStateStub(WIDE_KEY_COLUMNS);
                    Path dirA = new Path();
                    Path dirB = new Path()
            ) {
                createCheckpointLayout(LV_DIR + "_a");
                createCheckpointLayout(LV_DIR + "_b");
                checkpointsDir(dirA, LV_DIR + "_a");
                checkpointsDir(dirB, LV_DIR + "_b");
                final ObjList<WindowFunction> functionsA = new ObjList<>();
                functionsA.add(partitionA);
                final ObjList<WindowFunction> functionsB = new ObjList<>();
                functionsB.add(partitionB);
                putStates(partitionA, WIDE_KEY_DOMAIN, 1);
                putStates(partitionB, WIDE_KEY_DOMAIN, 2);

                final LiveViewCheckpointTimelineStoreWriter writer =
                        new LiveViewCheckpointTimelineStoreWriter(configuration);
                LiveViewCheckpointTimelineStoreWriter.RepairCapture plain = null;
                LiveViewCheckpointTimelineStoreWriter.RepairCapture chained = null;
                try {
                    seal(writer, functionsA, LV_DIR + "_a", 1, LIFECYCLE_IDENTITY_A, null);
                    seal(writer, functionsA, LV_DIR + "_a", 2, LIFECYCLE_IDENTITY_A, null);
                    seal(writer, functionsB, LV_DIR + "_b", 1, LIFECYCLE_IDENTITY_B, null);
                    seal(writer, functionsB, LV_DIR + "_b", 2, LIFECYCLE_IDENTITY_B, null);

                    final ObjList<LiveViewCheckpointTimelineEntry> boundaries = new ObjList<>();
                    plain = writer.beginRepair(dirA, null, trackerA, false);
                    plain.collectBoundaries(0, 2_500_000, boundaries);
                    Assert.assertEquals(2, boundaries.size());
                    plain.capture(boundaries.getQuick(0), functionsA, null, 1);
                    plain.capture(boundaries.getQuick(1), functionsA, null, 2);

                    chained = writer.beginRepair(dirB, null, trackerB, true);
                    chained.collectBoundaries(0, 2_500_000, boundaries);
                    Assert.assertEquals(2, boundaries.size());
                    chained.capture(boundaries.getQuick(0), functionsB, null, 1);
                    chained.capture(boundaries.getQuick(1), functionsB, null, 2);

                    Assert.assertTrue("the plain capture must hold its frozen scratch", trackerA.getUsed() > 0);
                    Assert.assertTrue("the chained capture must hold its frozen scratch", trackerB.getUsed() > 0);
                    Assert.assertEquals(2, writer.getLeasedRepairScratchCountForTest());
                    Assert.assertTrue(
                            "the parked captures must hold the payloads they froze",
                            writer.getRetainedFrozenPayloadBytesForTest() > 0
                    );
                } finally {
                    // The worker's order: the writer first, the repairs it parked after.
                    writer.close();
                    Misc.free(plain);
                    Misc.free(chained);
                }
                Assert.assertEquals("the plain capture must return its tracker clean", 0, trackerA.getUsed());
                Assert.assertEquals("the chained capture must return its tracker clean", 0, trackerB.getUsed());
            } finally {
                trackerA.close();
                trackerB.close();
            }
        });
    }

    @Test
    public void testAppendReleasesStateScratchAfterSeal() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                stub.state = filled(STATE_IMAGE_BYTES, (byte) 0x5A);
                final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                seal(writer, stub, 1);
                final long retained = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT) - baseline;
                Assert.assertTrue(
                        "the seal must release its state scratch after publishing, retained=" + retained,
                        retained < RELEASED_TOLERANCE_BYTES
                );
            }
        });
    }

    @Test
    public void testAppendTrimsFrozenScratchAboveRetentionLimit() throws Exception {
        // The frozen holders and state arrays live on the Java heap, where neither the
        // refresh tracker nor the leak check sees them, and the writer lives as long as
        // its worker. One outlier seal must therefore hand its frozen graph back when it
        // ends, while a seal within the limit keeps reusing what it pooled.
        assertMemoryLeak(() -> {
            try (
                    PartitionedStateStub stub = new PartitionedStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                // One frozen key and one inline state array per key, which the limit counts
                // together, so this key set freezes and pools four times the limit, and its
                // ascending keys fill about twice as many partition-map nodes as that pool keeps.
                putStates(stub, 2 * LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_ENTRIES, 1);
                seal(writer, stub, 1);
                final int retainedAfterOutlier = writer.getRetainedFrozenObjectCountForTest();
                Assert.assertTrue(
                        "an outlier seal must not park its frozen graph on the writer, retained="
                                + retainedAfterOutlier,
                        retainedAfterOutlier <= LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_ENTRIES
                );
                final int retainedPartitionMapObjects = writer.getRetainedPartitionMapObjectCountForTest();
                Assert.assertTrue(
                        "an outlier seal must not park its partition-map nodes on the writer, retained="
                                + retainedPartitionMapObjects,
                        retainedPartitionMapObjects <= LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_PARTITION_MAP_OBJECTS
                );

                // The first small seal also removes every key the outlier root holds,
                // which is outlier work of its own; the two after it are the steady state.
                // Each seal moves every state, so each one rewrites its partition map.
                stub.clearStates();
                putStates(stub, 1_000, 2);
                seal(writer, stub, 2);
                putStates(stub, 1_000, 3);
                seal(writer, stub, 3);
                final int warmed = writer.getRetainedFrozenObjectCountForTest();
                Assert.assertTrue("a seal within the limit must keep its frozen graph pooled", warmed > 0);
                final int warmedPartitionMapObjects = writer.getRetainedPartitionMapObjectCountForTest();
                Assert.assertTrue(
                        "a seal within the limit must keep its partition-map nodes pooled",
                        warmedPartitionMapObjects > 0
                );
                putStates(stub, 1_000, 4);
                seal(writer, stub, 4);
                Assert.assertEquals(warmed, writer.getRetainedFrozenObjectCountForTest());
                Assert.assertEquals(warmedPartitionMapObjects, writer.getRetainedPartitionMapObjectCountForTest());
            }
        });
    }

    @Test
    public void testAppendParksNoWideKeyOnTheWriter() throws Exception {
        // Wide keys used to be heap images a few thousand of which pinned megabytes on the
        // writer between seals. A frozen key now lives in the freeze scratch's native arena,
        // and its state image in the scratch's payload arena, both of which the seal frees
        // when it ends, so a seal of wide keys leaves nothing of either on the writer, and
        // every native byte its keys and images took comes back.
        assertMemoryLeak(() -> {
            try (
                    PartitionedStateStub stub = new PartitionedStateStub(WIDE_KEY_COLUMNS);
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                // Every key images into WIDE_KEY_COLUMNS * 8 bytes, so these keys hold more
                // bytes than a key arena that outlives its operation may keep, while their
                // frozen keys and state images together stay inside the limit that counts them.
                final int keyCount = (int) (LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_KEY_BYTES
                        / wideKeyBytes()) + 1_024;
                Assert.assertTrue(2 * keyCount < LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_ENTRIES);
                putStates(stub, keyCount, 1);
                final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                seal(writer, stub, 1);
                Assert.assertEquals(
                        "a seal of wide keys must not keep its state images",
                        0,
                        writer.getRetainedFrozenPayloadBytesForTest()
                );
                final long retained = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline;
                Assert.assertTrue(
                        "the seal must hand its frozen keys back, retained=" + retained,
                        retained < RELEASED_TOLERANCE_BYTES
                );
            }
        });
    }

    @Test
    public void testFrozenScratchDropsTheSealedRuntimeWhenItsOperationEnds() throws Exception {
        // The writer is shared across every view its worker seals, so a frozen holder
        // that still names the function it froze keeps that view's runtime reachable
        // after DROP, until another seal happens to reuse the holder.
        assertMemoryLeak(() -> {
            try (
                    PartitionedStateStub partition = new PartitionedStateStub();
                    ScalarStateStub scalar = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration);
                    Path dir = new Path()
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(partition);
                functions.add(scalar);
                partition.putState(11, 0x11);
                scalar.state = filled(64, (byte) 0x21);
                seal(writer, functions, LV_DIR, 1, LIFECYCLE_IDENTITY, null);
                Assert.assertTrue(
                        "a finished seal must not keep the sealed functions reachable",
                        writer.isFrozenScratchRuntimeReferenceClearForTest()
                );
                seal(writer, functions, LV_DIR, 2, LIFECYCLE_IDENTITY, null);

                checkpointsDir(dir);
                try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                             writer.beginRepair(dir, null, null, false)) {
                    final ObjList<LiveViewCheckpointTimelineEntry> boundaries = new ObjList<>();
                    capture.collectBoundaries(0, 1_500_000, boundaries);
                    Assert.assertEquals(1, boundaries.size());
                    capture.capture(boundaries.getQuick(0), functions, null, 1);
                }
                Assert.assertTrue(
                        "a closed repair capture must not keep the replayed functions reachable",
                        writer.isFrozenScratchRuntimeReferenceClearForTest()
                );
            }
        });
    }

    @Test
    public void testRepairCaptureReleasesScratchOnAbandon() throws Exception {
        // A discarded capture is a temporary file and nothing else - and that
        // must hold for the freeze scratch too: the charge is visible on the
        // view's tracker while the capture is open and gone once it closes.
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireRefreshTracker();
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration);
                    Path dir = new Path()
            ) {
                stub.state = filled(64, (byte) 0x11);
                seal(writer, stub, 1);
                seal(writer, stub, 2);
                checkpointsDir(dir);
                final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                             writer.beginRepair(dir, null, tracker, false)) {
                    final ObjList<LiveViewCheckpointTimelineEntry> boundaries = new ObjList<>();
                    capture.collectBoundaries(0, 1_500_000, boundaries);
                    Assert.assertEquals(1, boundaries.size());
                    stub.state = filled(STATE_IMAGE_BYTES, (byte) 0x77);
                    final ObjList<WindowFunction> functions = new ObjList<>();
                    functions.add(stub);
                    capture.capture(boundaries.getQuick(0), functions, null, 1);
                    Assert.assertTrue(
                            "the open capture's scratch must be charged to the view's tracker",
                            tracker.getUsed() >= STATE_IMAGE_BYTES
                    );
                }
                final long retained = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT) - baseline;
                Assert.assertTrue(
                        "an abandoned capture must release its scratch, retained=" + retained,
                        retained < RELEASED_TOLERANCE_BYTES
                );
                Assert.assertEquals("no charge may outlive the capture", 0, tracker.getUsed());
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testRepairCaptureReleasesScratchOnPublish() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireRefreshTracker();
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration);
                    Path dir = new Path()
            ) {
                stub.state = filled(64, (byte) 0x11);
                seal(writer, stub, 1);
                seal(writer, stub, 2);
                checkpointsDir(dir);
                final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                             writer.beginRepair(dir, null, tracker, false)) {
                    final ObjList<LiveViewCheckpointTimelineEntry> boundaries = new ObjList<>();
                    capture.collectBoundaries(0, 1_500_000, boundaries);
                    Assert.assertEquals(1, boundaries.size());
                    stub.state = filled(STATE_IMAGE_BYTES, (byte) 0x77);
                    final ObjList<WindowFunction> functions = new ObjList<>();
                    functions.add(stub);
                    capture.capture(boundaries.getQuick(0), functions, null, 1);
                    final LiveViewCheckpointTimelineStoreWriter.RepairResult result = writer.publishRepair(
                            capture,
                            DEFINITION_TXN,
                            2,
                            2,
                            0,
                            LIFECYCLE_IDENTITY,
                            true,
                            1_500_000,
                            0
                    );
                    Assert.assertEquals(1, result.getRootsVersioned());
                }
                final long retained = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT) - baseline;
                Assert.assertTrue(
                        "a published capture must release its scratch, retained=" + retained,
                        retained < RELEASED_TOLERANCE_BYTES
                );
                Assert.assertEquals("no charge may outlive the capture", 0, tracker.getUsed());
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testAParkedCaptureKeepsItsOwnKeyDomainWhileTheReplayIsRearmed() throws Exception {
        // The hazard the capture's own copy of Q exists for. A keyed repair opens its
        // capture with the keyed replay's Q; the turn parks, the worker keeps its replay
        // instance, clears it and arms it again for the next segment it repairs, of this
        // view or any other. The re-arm writes a different key set over the very storage
        // the first one used, so a capture that shared it would image and replace the new
        // keys and leave the ones its replay actually recomputed as the old root wrote them.
        assertMemoryLeak(() -> {
            final ArrayColumnTypes checkpointKeyTypes = new ArrayColumnTypes();
            checkpointKeyTypes.add(ColumnType.STRING);
            final AccountSymbolTable symbols = new AccountSymbolTable(5);
            final IntList firstKeys = new IntList();
            firstKeys.add(1);
            firstKeys.add(2);
            final IntList secondKeys = new IntList();
            secondKeys.add(3);
            secondKeys.add(4);
            try (
                    StringPartitionedStateStub stub = new StringPartitionedStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration);
                    LiveViewCheckpointKeyedReplay replay = new LiveViewCheckpointKeyedReplay();
                    Path dir = new Path()
            ) {
                for (int account = 1; account <= 4; account++) {
                    stub.putState(symbols.valueOf(account), 0x10 + account);
                }
                seal(writer, stub, 1);
                seal(writer, stub, 2);
                checkpointsDir(dir);

                Assert.assertTrue(replay.arm(0, symbols, checkpointKeyTypes, firstKeys, false));
                final ObjList<LiveViewCheckpointTimelineEntry> boundaries = new ObjList<>();
                try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                             writer.beginRepair(dir, replay.getOutputKeys(), null, false)) {
                    replay.clear();
                    Assert.assertTrue(replay.arm(0, symbols, checkpointKeyTypes, secondKeys, false));

                    final LiveViewCheckpointOutputKeyDomain captured = capturedOutputKeys(capture);
                    Assert.assertEquals(2, captured.size());
                    Assert.assertTrue(LiveViewCheckpointTestKeys.contains(captured, stringKey("acct-1")));
                    Assert.assertTrue(LiveViewCheckpointTestKeys.contains(captured, stringKey("acct-2")));
                    Assert.assertFalse("the capture answers for the re-armed replay's key", LiveViewCheckpointTestKeys.contains(captured, stringKey("acct-3")));
                    Assert.assertFalse("the capture answers for the re-armed replay's key", LiveViewCheckpointTestKeys.contains(captured, stringKey("acct-4")));

                    capture.collectBoundaries(0, 1_500_000, boundaries);
                    Assert.assertEquals(1, boundaries.size());
                    for (int account = 1; account <= 4; account++) {
                        stub.putState(symbols.valueOf(account), 0x70 + account);
                    }
                    final ObjList<WindowFunction> functions = new ObjList<>();
                    functions.add(stub);
                    capture.capture(boundaries.getQuick(0), functions, null, 1);
                    writer.publishRepair(capture, DEFINITION_TXN, 2, 2, 0, LIFECYCLE_IDENTITY, true, 1_500_000, 0);
                }

                // The published boundary replaced exactly the keys the capture was opened
                // with and kept every other key as the old root wrote it.
                try (
                        StringPartitionedStateStub restored = new StringPartitionedStateStub();
                        LiveViewCheckpointTimelineStoreReader reader =
                                new LiveViewCheckpointTimelineStoreReader(configuration)
                ) {
                    final ObjList<WindowFunction> functions = new ObjList<>();
                    functions.add(restored);
                    reader.of(dir);
                    final LiveViewCheckpointTimelineEntry entry = boundaries.getQuick(0);
                    reader.restore(entry.maxTimestamp, entry.checkpointId, DEFINITION_TXN, functions, null);
                    Assert.assertEquals(0x71, restored.readState("acct-1"));
                    Assert.assertEquals(0x72, restored.readState("acct-2"));
                    Assert.assertEquals(0x13, restored.readState("acct-3"));
                    Assert.assertEquals(0x14, restored.readState("acct-4"));
                }
            }
        });
    }

    @Test
    public void testANarrowRepairCaptureCopiesOnlyItsOwnKeysAfterAWideOne() throws Exception {
        // The capture copies Q out of the worker's repair plan or keyed replay, and both are
        // reused from repair to repair, with clear() keeping the table the widest domain
        // grew. The copy must cost what Q itself needs: were it that table, every narrow
        // repair would hold the memory an earlier repair on the same worker, of any view,
        // grew. The copy stays off the view's refresh tracker, so a per-view budget far
        // below the wide table refuses no capture either way.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 262_144);
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireRefreshTracker();
            try (
                    PartitionedStateStub stub = new PartitionedStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration);
                    Path dir = new Path();
                    LiveViewCheckpointOutputKeyDomain freshKeys = new LiveViewCheckpointOutputKeyDomain();
                    LiveViewCheckpointOutputKeyDomain reusedKeys = new LiveViewCheckpointOutputKeyDomain()
            ) {
                stub.putState(11, 0x11);
                seal(writer, stub, 1);
                seal(writer, stub, 2);
                checkpointsDir(dir);
                freshKeys.beginKey().putLong(11);
                freshKeys.commitKey();
                final long freshCopyBytes = nativeBytesOfCapture(writer, dir, freshKeys, tracker);
                Assert.assertTrue(freshCopyBytes > 0);

                // Past the retention bound: a slot table several times the view's budget.
                // Then at most at it, which is what a bounded source keeps.
                for (int keyCount : new int[]{4 * WIDE_KEY_DOMAIN, 1_000}) {
                    for (long key = 0; key < keyCount; key++) {
                        reusedKeys.beginKey().putLong(key);
                        reusedKeys.commitKey();
                    }
                    reusedKeys.clear();
                    reusedKeys.beginKey().putLong(11);
                    reusedKeys.commitKey();
                    Assert.assertEquals(
                            "a narrow capture after a " + keyCount + "-key domain must copy what its own keys need",
                            freshCopyBytes,
                            nativeBytesOfCapture(writer, dir, reusedKeys, tracker)
                    );

                    // An empty Q after the same domain costs the capture nothing.
                    reusedKeys.clear();
                    Assert.assertEquals(
                            "an empty capture after a " + keyCount + "-key domain",
                            0,
                            nativeBytesOfCapture(writer, dir, reusedKeys, tracker)
                    );
                    reusedKeys.restoreInitialCapacity();
                }
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertEquals(0, writer.getLeasedRepairScratchCountForTest());
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testRepairCaptureKeepsItsKeyDomainCopyOffTheViewsTracker() throws Exception {
        // The capture's copy of Q is as wide as the key domain and lives as long as the
        // capture, parked turns included. It is native memory tagged NATIVE_LIVE_VIEW_IN_MEM,
        // and it stays off the repaired view's refresh tracker, as the heap copy it replaced
        // did, so a per-view limit sized for that copy keeps admitting the capture. It comes
        // back on the abandon and the publish path alike.
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireRefreshTracker();
            try (
                    PartitionedStateStub stub = new PartitionedStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration);
                    Path dir = new Path();
                    LiveViewCheckpointOutputKeyDomain keys = new LiveViewCheckpointOutputKeyDomain()
            ) {
                stub.putState(11, 0x11);
                seal(writer, stub, 1);
                seal(writer, stub, 2);
                checkpointsDir(dir);
                for (long key = 0; key < WIDE_KEY_DOMAIN; key++) {
                    keys.beginKey().putLong(key);
                    keys.commitKey();
                }
                for (int pass = 0; pass < 2; pass++) {
                    final boolean isPublished = pass == 1;
                    final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                    final long copied;
                    final long usedBeforeClose;
                    try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                                 writer.beginRepair(dir, keys, tracker, false)) {
                        copied = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline;
                        Assert.assertTrue("the capture must copy Q into memory of its own", copied > 0);
                        Assert.assertEquals("the capture's copy of Q must stay off the view's tracker", 0, tracker.getUsed());
                        Assert.assertEquals(WIDE_KEY_DOMAIN, capturedOutputKeys(capture).size());

                        final ObjList<LiveViewCheckpointTimelineEntry> boundaries = new ObjList<>();
                        capture.collectBoundaries(0, 1_500_000, boundaries);
                        Assert.assertEquals(1, boundaries.size());
                        stub.putState(11, 0x70 + pass);
                        final ObjList<WindowFunction> functions = new ObjList<>();
                        functions.add(stub);
                        capture.capture(boundaries.getQuick(0), functions, null, 1);
                        if (isPublished) {
                            writer.publishRepair(capture, DEFINITION_TXN, 2, 2, 0, LIFECYCLE_IDENTITY, true, 1_500_000, 0);
                        }
                        // Measured across the close alone: the publication decodes the old
                        // roots through the writer's own partition-map readers, whose node
                        // arenas are live-view memory the writer keeps between operations.
                        usedBeforeClose = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                    }
                    Assert.assertEquals("no charge may outlive the capture [published=" + isPublished + ']', 0, tracker.getUsed());
                    final long freed = usedBeforeClose - Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                    Assert.assertTrue(
                            "the capture must free its copy of Q [published=" + isPublished + ", copied=" + copied
                                    + ", freed=" + freed + ']',
                            freed >= copied
                    );
                    Assert.assertEquals(0, writer.getLeasedRepairScratchCountForTest());
                }
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testRepairCaptureThatCannotCopyItsKeyDomainReleasesEverything() throws Exception {
        // beginRepair copies Q after the capture holds its Paths and its scratch lease. A
        // copy the process cannot allocate must fail the capture and hand all of that back -
        // the scratch lease exactly once, since a second release would throw over the
        // original failure - leaving the writer able to open the next capture. The copy
        // stays off the view's refresh tracker, so the limit that refuses it is the RSS
        // ceiling: armed with room for everything a capture takes before its copy of Q, and
        // far too little for the copy.
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireRefreshTracker();
            try (
                    PartitionedStateStub stub = new PartitionedStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration);
                    Path dir = new Path();
                    LiveViewCheckpointOutputKeyDomain keys = new LiveViewCheckpointOutputKeyDomain()
            ) {
                stub.putState(11, 0x11);
                seal(writer, stub, 1);
                seal(writer, stub, 2);
                checkpointsDir(dir);
                // Its slot table alone is many times the room the ceiling leaves.
                for (long key = 0; key < 4 * WIDE_KEY_DOMAIN; key++) {
                    keys.beginKey().putLong(key);
                    keys.commitKey();
                }
                // The same ceiling admits a capture without Q, so what it refuses below is the copy.
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + CAPTURE_RSS_SLACK_BYTES);
                try (LiveViewCheckpointTimelineStoreWriter.RepairCapture ignore =
                             writer.beginRepair(dir, null, tracker, false)) {
                    Assert.assertEquals(1, writer.getLeasedRepairScratchCountForTest());
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
                final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + CAPTURE_RSS_SLACK_BYTES);
                try {
                    writer.beginRepair(dir, keys, tracker, false);
                    Assert.fail("expected the RSS ceiling to refuse the copy of Q");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "global RSS memory limit exceeded");
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
                Assert.assertEquals("no charge may outlive the failed capture", 0, tracker.getUsed());
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
                Assert.assertEquals("the failed capture must return its scratch lease", 0, writer.getLeasedRepairScratchCountForTest());

                // The writer is unharmed: the next capture leases the same scratch.
                try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                             writer.beginRepair(dir, null, tracker, false)) {
                    Assert.assertEquals(1, writer.getLeasedRepairScratchCountForTest());
                    final ObjList<LiveViewCheckpointTimelineEntry> boundaries = new ObjList<>();
                    capture.collectBoundaries(0, 1_500_000, boundaries);
                    Assert.assertEquals(1, boundaries.size());
                }
                Assert.assertEquals(0, writer.getLeasedRepairScratchCountForTest());
                Assert.assertEquals(0, tracker.getUsed());
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testTwoParkedRepairCapturesOwnFrozenScratchAndPublishDurably() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker trackerA = acquireRefreshTracker();
            final MemoryTracker trackerB = acquireRefreshTracker();
            try (
                    PartitionedStateStub partitionA = new PartitionedStateStub();
                    PartitionedStateStub partitionB = new PartitionedStateStub();
                    ScalarStateStub scalarA = new ScalarStateStub();
                    ScalarStateStub scalarB = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration);
                    Path dirA = new Path();
                    Path dirB = new Path()
            ) {
                createCheckpointLayout(LV_DIR + "_a");
                createCheckpointLayout(LV_DIR + "_b");
                checkpointsDir(dirA, LV_DIR + "_a");
                checkpointsDir(dirB, LV_DIR + "_b");
                final ObjList<WindowFunction> functionsA = new ObjList<>();
                functionsA.add(partitionA);
                functionsA.add(scalarA);
                final ObjList<WindowFunction> functionsB = new ObjList<>();
                functionsB.add(partitionB);
                functionsB.add(scalarB);

                partitionA.putState(11, 0x11);
                scalarA.state = filled(64, (byte) 0x21);
                partitionB.putState(11, 0x12);
                scalarB.state = filled(64, (byte) 0x22);
                seal(writer, functionsA, LV_DIR + "_a", 1, LIFECYCLE_IDENTITY_A, null);
                seal(writer, functionsA, LV_DIR + "_a", 2, LIFECYCLE_IDENTITY_A, null);
                seal(writer, functionsB, LV_DIR + "_b", 1, LIFECYCLE_IDENTITY_B, null);
                seal(writer, functionsB, LV_DIR + "_b", 2, LIFECYCLE_IDENTITY_B, null);

                final ObjList<LiveViewCheckpointTimelineEntry> boundariesA = new ObjList<>();
                final ObjList<LiveViewCheckpointTimelineEntry> boundariesB = new ObjList<>();
                try (LiveViewCheckpointTimelineStoreWriter.RepairCapture captureA =
                             writer.beginRepair(dirA, null, trackerA, false)) {
                    captureA.collectBoundaries(0, 1_500_000, boundariesA);
                    Assert.assertEquals(1, boundariesA.size());
                    partitionA.putState(11, 0x71);
                    scalarA.state = filled(64, (byte) 0x31);
                    captureA.capture(boundariesA.getQuick(0), functionsA, null, 1);

                    try (LiveViewCheckpointTimelineStoreWriter.RepairCapture captureB =
                                 writer.beginRepair(dirB, null, trackerB, false)) {
                        captureB.collectBoundaries(0, 1_500_000, boundariesB);
                        Assert.assertEquals(1, boundariesB.size());
                        partitionB.putState(11, 0x72);
                        scalarB.state = filled(64, (byte) 0x32);
                        captureB.capture(boundariesB.getQuick(0), functionsB, null, 1);

                        assertFrozenGraphsDoNotAlias(captureA, captureB);
                        Assert.assertTrue("capture A must retain its tracker-bound scratch", trackerA.getUsed() > 0);
                        Assert.assertTrue("capture B must retain its tracker-bound scratch", trackerB.getUsed() > 0);

                        writer.publishRepair(captureA, DEFINITION_TXN, 2, 2, 0, LIFECYCLE_IDENTITY_A, true, 1_500_000, 0);
                        writer.publishRepair(captureB, DEFINITION_TXN, 2, 2, 0, LIFECYCLE_IDENTITY_B, true, 1_500_000, 0);
                    }
                }

                assertRestoredState(dirA, boundariesA.getQuick(0), 0x71, (byte) 0x31);
                assertRestoredState(dirB, boundariesB.getQuick(0), 0x72, (byte) 0x32);
                Assert.assertEquals("capture A must release its tracker charge", 0, trackerA.getUsed());
                Assert.assertEquals("capture B must release its tracker charge", 0, trackerB.getUsed());
            } finally {
                trackerA.close();
                trackerB.close();
            }
        });
    }

    private static void assertFrozenGraphsDoNotAlias(
            LiveViewCheckpointTimelineStoreWriter.RepairCapture captureA,
            LiveViewCheckpointTimelineStoreWriter.RepairCapture captureB
    ) throws Exception {
        final Field boundariesField = captureA.getClass().getDeclaredField("boundaries");
        boundariesField.setAccessible(true);
        final Object boundaryA = ((ObjList<?>) boundariesField.get(captureA)).getQuick(0);
        final Object boundaryB = ((ObjList<?>) boundariesField.get(captureB)).getQuick(0);
        Assert.assertNotSame(boundaryA, boundaryB);

        final Field functionsField = boundaryA.getClass().getDeclaredField("functions");
        functionsField.setAccessible(true);
        final ObjList<?> frozenFunctionsA = (ObjList<?>) functionsField.get(boundaryA);
        final ObjList<?> frozenFunctionsB = (ObjList<?>) functionsField.get(boundaryB);
        final Object partitionFunctionA = frozenFunctionsA.getQuick(0);
        final Object partitionFunctionB = frozenFunctionsB.getQuick(0);
        final Field partitionsField = partitionFunctionA.getClass().getDeclaredField("partitions");
        partitionsField.setAccessible(true);
        final Object partitionA = ((ObjList<?>) partitionsField.get(partitionFunctionA)).getQuick(0);
        final Object partitionB = ((ObjList<?>) partitionsField.get(partitionFunctionB)).getQuick(0);
        Assert.assertNotSame("live captures must not share frozen holders", partitionA, partitionB);

        // A frozen partition names its key and its scalar by handles into its function's
        // scratch arenas, so two captures share key or scalar bytes exactly when their
        // functions share a scratch.
        final Field functionScratchField = partitionFunctionA.getClass().getDeclaredField("scratch");
        functionScratchField.setAccessible(true);
        final Object functionScratchA = functionScratchField.get(partitionFunctionA);
        final Object functionScratchB = functionScratchField.get(partitionFunctionB);
        Assert.assertNotSame("live captures must not share frozen keys", functionScratchA, functionScratchB);
        final Field frozenKeysField = functionScratchA.getClass().getDeclaredField("frozenKeys");
        frozenKeysField.setAccessible(true);
        Assert.assertNotSame(
                "live captures must not share key arenas",
                frozenKeysField.get(functionScratchA),
                frozenKeysField.get(functionScratchB)
        );
        final Field frozenPayloadsField = functionScratchA.getClass().getDeclaredField("frozenPayloads");
        frozenPayloadsField.setAccessible(true);
        Assert.assertNotSame(
                "live captures must not share payload arenas",
                frozenPayloadsField.get(functionScratchA),
                frozenPayloadsField.get(functionScratchB)
        );

        final Object scalarFunctionA = frozenFunctionsA.getQuick(1);
        final Object scalarFunctionB = frozenFunctionsB.getQuick(1);
        final Field scalarStateRefField = scalarFunctionA.getClass().getDeclaredField("scalarStateRef");
        scalarStateRefField.setAccessible(true);
        Assert.assertNotSame(
                "live captures must not share state-reference holders",
                scalarStateRefField.get(scalarFunctionA),
                scalarStateRefField.get(scalarFunctionB)
        );

        final Field scratchField = captureA.getClass().getDeclaredField("scratch");
        scratchField.setAccessible(true);
        Assert.assertNotSame(
                "each live capture must own a distinct scratch lease",
                scratchField.get(captureA),
                scratchField.get(captureB)
        );
    }

    private static void assertPartitionState(
            Path checkpointsDir,
            LiveViewCheckpointTimelineEntry entry,
            long key,
            long expectedState
    ) {
        try (
                PartitionedStateStub partition = new PartitionedStateStub();
                LiveViewCheckpointTimelineStoreReader reader =
                        new LiveViewCheckpointTimelineStoreReader(configuration)
        ) {
            final ObjList<WindowFunction> functions = new ObjList<>();
            functions.add(partition);
            reader.of(checkpointsDir);
            reader.restore(entry.maxTimestamp, entry.checkpointId, DEFINITION_TXN, functions, null);
            Assert.assertEquals(
                    "key " + key + " at boundary " + entry.maxTimestamp,
                    expectedState,
                    partition.readState(key)
            );
        }
    }

    private static void assertRestoredState(
            Path checkpointsDir,
            LiveViewCheckpointTimelineEntry entry,
            long expectedPartitionState,
            byte expectedScalarByte
    ) {
        try (
                PartitionedStateStub partition = new PartitionedStateStub();
                ScalarStateStub scalar = new ScalarStateStub();
                LiveViewCheckpointTimelineStoreReader reader =
                        new LiveViewCheckpointTimelineStoreReader(configuration)
        ) {
            scalar.state = new byte[64];
            final ObjList<WindowFunction> functions = new ObjList<>();
            functions.add(partition);
            functions.add(scalar);
            reader.of(checkpointsDir);
            reader.restore(entry.maxTimestamp, entry.checkpointId, DEFINITION_TXN, functions, null);
            Assert.assertEquals(expectedPartitionState, partition.readState(11));
            Assert.assertArrayEquals(filled(64, expectedScalarByte), scalar.state);
        }
    }

    private static LiveViewCheckpointOutputKeyDomain capturedOutputKeys(
            LiveViewCheckpointTimelineStoreWriter.RepairCapture capture
    ) throws Exception {
        final Field field = capture.getClass().getDeclaredField("outputKeys");
        field.setAccessible(true);
        return (LiveViewCheckpointOutputKeyDomain) field.get(capture);
    }

    private static Path checkpointsDir(Path path) {
        return checkpointsDir(path, LV_DIR);
    }

    private static Path checkpointsDir(Path path, String liveViewDir) {
        return path.of(configuration.getDbRoot()).concat(liveViewDir).concat("_checkpoints");
    }

    private static void createCheckpointLayout(String liveViewDir) {
        try (Path dir = new Path(); Path path = new Path()) {
            final FilesFacade ff = configuration.getFilesFacade();
            checkpointsDir(dir, liveViewDir);
            ff.mkdirs(LiveViewCheckpointLayout.metaDirPath(path, dir).slash(), configuration.getMkDirMode());
            ff.mkdirs(LiveViewCheckpointLayout.dataDirPath(path, dir).slash(), configuration.getMkDirMode());
        }
    }

    private static byte[] filled(int length, byte value) {
        final byte[] bytes = new byte[length];
        Arrays.fill(bytes, value);
        return bytes;
    }

    /**
     * Opens a capture of {@code keys} and closes it again.
     *
     * @return the live-view memory the open capture holds, which is its copy of Q alone
     */
    private static long nativeBytesOfCapture(
            LiveViewCheckpointTimelineStoreWriter writer,
            Path dir,
            LiveViewCheckpointOutputKeyDomain keys,
            MemoryTracker tracker
    ) {
        final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
        final long copyBytes;
        try (LiveViewCheckpointTimelineStoreWriter.RepairCapture ignore =
                     writer.beginRepair(dir, keys, tracker, false)) {
            copyBytes = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline;
            Assert.assertEquals("the capture's copy of Q must stay off the view's tracker", 0, tracker.getUsed());
        } catch (CairoException e) {
            throw new AssertionError(
                    "a capture of " + keys.size() + " keys was refused: " + e.getFlyweightMessage(),
                    e
            );
        }
        Assert.assertEquals(0, tracker.getUsed());
        return copyBytes;
    }

    /**
     * Seals two boundaries, captures the first into one repair and fails the capture of the
     * second half way through its freeze, then closes the capture. The capture's scratch
     * holds the first boundary's payloads and half of the second's when it closes, and the
     * close is what frees them and hands the scratch back.
     */
    private void assertRepairCaptureThatFailsMidFreezeFreesItsPayloadsOnClose(boolean isChained) throws Exception {
        assertMemoryLeak(() -> {
            try (
                    PartitionedStateStub stub = new PartitionedStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration);
                    Path dir = new Path()
            ) {
                putStates(stub, FAILING_FREEZE_KEYS, 1);
                seal(writer, stub, 1);
                putStates(stub, FAILING_FREEZE_KEYS, 2);
                seal(writer, stub, 2);
                checkpointsDir(dir);
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                             writer.beginRepair(dir, null, null, isChained)) {
                    final ObjList<LiveViewCheckpointTimelineEntry> boundaries = new ObjList<>();
                    capture.collectBoundaries(0, 2_500_000, boundaries);
                    Assert.assertEquals(2, boundaries.size());
                    capture.capture(boundaries.getQuick(0), functions, null, 1);
                    final long held = writer.getRetainedFrozenPayloadBytesForTest();
                    Assert.assertTrue("the open capture must hold the payloads it froze", held > 0);
                    // Complete, whether chained or not, so the second boundary re-images every key.
                    putStates(stub, FAILING_FREEZE_KEYS, 3);
                    stub.failAfterFreezes(FAILING_FREEZE_KEYS / 2);
                    try {
                        capture.capture(boundaries.getQuick(1), functions, null, 2);
                        Assert.fail("the capture must fail mid-freeze");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "simulated live view checkpoint freeze failure");
                    }
                    Assert.assertTrue(
                            "the failed capture must still hold what it froze until it closes",
                            writer.getRetainedFrozenPayloadBytesForTest() >= held
                    );
                    Assert.assertEquals(1, writer.getLeasedRepairScratchCountForTest());
                }
                Assert.assertEquals("a closed capture must hand its scratch back", 0, writer.getLeasedRepairScratchCountForTest());
                Assert.assertEquals(
                        "a closed capture must free the payloads it froze",
                        0,
                        writer.getRetainedFrozenPayloadBytesForTest()
                );
            }
        });
    }

    private static void putStates(PartitionedStateStub stub, int keyCount, long seq) {
        for (int key = 0; key < keyCount; key++) {
            stub.putState(key, seq * keyCount + key);
        }
    }

    /**
     * The STRING checkpoint key {@code LiveViewSnapshotKeyCodec} writes for {@code value}:
     * a character count, then the UTF-16 units, in native byte order.
     */
    private static byte[] stringKey(String value) {
        final ByteBuffer key = ByteBuffer.allocate(Integer.BYTES + value.length() * Character.BYTES)
                .order(ByteOrder.nativeOrder());
        key.putInt(value.length());
        for (int i = 0; i < value.length(); i++) {
            key.putChar(value.charAt(i));
        }
        return key.array();
    }

    /**
     * @return the bytes one {@code WIDE_KEY_COLUMNS}-column LONG key images into
     */
    private static long wideKeyBytes() {
        return (long) WIDE_KEY_COLUMNS * Long.BYTES;
    }

    private MemoryTracker acquireRefreshTracker() {
        return engine.getMemoryTrackerProvider().acquire(
                AllowAllSecurityContext.INSTANCE,
                1,
                MemoryTrackerWorkload.LIVE_VIEW_REFRESH
        );
    }

    /**
     * Seals two boundaries of {@code keyCount} wide keys into a layout of their own,
     * captures both into one repair and returns what the open capture charges the view's
     * refresh tracker.
     */
    private long openCaptureCharge(String liveViewDir, int keyCount, boolean isChained) {
        createCheckpointLayout(liveViewDir);
        final MemoryTracker tracker = acquireRefreshTracker();
        try (
                PartitionedStateStub stub = new PartitionedStateStub(WIDE_KEY_COLUMNS);
                LiveViewCheckpointTimelineStoreWriter writer =
                        new LiveViewCheckpointTimelineStoreWriter(configuration);
                Path dir = new Path()
        ) {
            final ObjList<WindowFunction> functions = new ObjList<>();
            functions.add(stub);
            putStates(stub, keyCount, 1);
            seal(writer, functions, liveViewDir, 1, LIFECYCLE_IDENTITY, null);
            seal(writer, functions, liveViewDir, 2, LIFECYCLE_IDENTITY, null);
            checkpointsDir(dir, liveViewDir);
            final long charge;
            try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture =
                         writer.beginRepair(dir, null, tracker, isChained)) {
                final ObjList<LiveViewCheckpointTimelineEntry> boundaries = new ObjList<>();
                capture.collectBoundaries(0, 2_500_000, boundaries);
                Assert.assertEquals(2, boundaries.size());
                final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                capture.capture(boundaries.getQuick(0), functions, null, 1);
                capture.capture(boundaries.getQuick(1), functions, null, 2);
                final long frozenKeyBytes = (long) keyCount * wideKeyBytes();
                final long held = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline;
                Assert.assertTrue(
                        "the open capture must hold its frozen keys as live-view memory [chained=" + isChained
                                + ", held=" + held + ", frozenKeyBytes=" + frozenKeyBytes + ']',
                        held >= frozenKeyBytes
                );
                charge = tracker.getUsed();
            }
            Assert.assertEquals("no charge may outlive the capture", 0, tracker.getUsed());
            return charge;
        } finally {
            tracker.close();
        }
    }

    private void seal(LiveViewCheckpointTimelineStoreWriter writer, WindowFunction function, long seq) {
        seal(writer, function, seq, null);
    }

    private void seal(
            LiveViewCheckpointTimelineStoreWriter writer,
            WindowFunction function,
            long seq,
            MemoryTracker memoryTracker
    ) {
        final ObjList<WindowFunction> functions = new ObjList<>();
        functions.add(function);
        seal(writer, functions, LV_DIR, seq, LIFECYCLE_IDENTITY, memoryTracker);
    }

    private void seal(
            LiveViewCheckpointTimelineStoreWriter writer,
            ObjList<WindowFunction> functions,
            String liveViewDir,
            long seq,
            long lifecycleIdentity,
            MemoryTracker memoryTracker
    ) {
        try (Path dir = new Path()) {
            checkpointsDir(dir, liveViewDir);
            writer.append(
                    dir,
                    functions,
                    null,
                    DEFINITION_TXN,
                    0,
                    seq,
                    seq,
                    0,
                    lifecycleIdentity,
                    true,
                    seq * 1_000_000L,
                    seq,
                    seq * 1_000_000L,
                    Numbers.LONG_NULL,
                    memoryTracker
            );
        }
    }

    /**
     * A one-key partitioned function that forces the production seal path through
     * the retained partition-map object pool.
     */
    private static final class PartitionedStateStub extends BaseWindowFunction {
        private final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        private final Map map;
        // How many more states freeze before the next one throws, or -1 to never throw.
        private int freezesBeforeFailure = -1;
        private int freezeCount;

        private PartitionedStateStub() {
            this(1);
        }

        /**
         * @param keyColumnCount LONG key columns, each holding the same key value, so a
         *                       case widens the frozen key image without changing the keys
         */
        private PartitionedStateStub(int keyColumnCount) {
            super(null);
            for (int i = 0; i < keyColumnCount; i++) {
                keyTypes.add(ColumnType.LONG);
            }
            map = new OrderedMap(
                    1024,
                    keyTypes,
                    new SingleColumnType(ColumnType.LONG),
                    16,
                    0.7,
                    Integer.MAX_VALUE
            );
            setCheckpointCompilerMetadata(
                    new LiveViewCheckpointFunctionIdentity(
                            "w0",
                            "partitioned_seal_scratch_stub()",
                            0,
                            "k",
                            "ts asc",
                            "partitioned-seal-scratch-stub-v1"
                    ),
                    new LiveViewCheckpointDependency(
                            LiveViewCheckpointContracts.DependencyKind.FIXED_ANCHOR_SEGMENT,
                            "k",
                            "ts asc",
                            0,
                            0,
                            0,
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
        public int checkpointStateFixedLength() {
            return Long.BYTES;
        }

        @Override
        public int checkpointStateFormatVersion() {
            return 1;
        }

        @Override
        public void close() {
            super.close();
            Misc.free(map);
        }

        @Override
        public void freezeCheckpointState(LiveViewStatePageWriter sink, MapValue value) {
            if (freezesBeforeFailure == 0) {
                freezesBeforeFailure = -1;
                throw CairoException.critical(0).put("simulated live view checkpoint freeze failure");
            }
            if (freezesBeforeFailure > 0) {
                freezesBeforeFailure--;
            }
            freezeCount++;
            sink.putLong(value.getLong(0));
        }

        @Override
        public ColumnTypes getCheckpointKeyColumnTypes() {
            return keyTypes;
        }

        @Override
        public int getCheckpointKeyStartIndex() {
            return 1;
        }

        @Override
        public String getName() {
            return "partitioned_seal_scratch_stub";
        }

        @Override
        public Map getPartitionMap() {
            return map;
        }

        @Override
        public void onCheckpointRestoreBegin() {
            map.clear();
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
            value.putLong(0, source.getLong(offset));
            return Long.BYTES;
        }

        @Override
        public boolean supportsCheckpointState() {
            return true;
        }

        private void clearStates() {
            map.clear();
        }

        /**
         * Arms the next freeze walk to throw once it has frozen {@code freezes} states, and
         * rewinds the freeze count, which then says how far the walk got.
         */
        private void failAfterFreezes(int freezes) {
            freezesBeforeFailure = freezes;
            freezeCount = 0;
        }

        private void putKey(MapKey mapKey, long key) {
            for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
                mapKey.putLong(key);
            }
        }

        private void putState(long key, long state) {
            final MapKey mapKey = map.withKey();
            putKey(mapKey, key);
            mapKey.createValue().putLong(0, state);
        }

        private long readState(long key) {
            final MapKey mapKey = map.withKey();
            putKey(mapKey, key);
            final MapValue value = mapKey.findValue();
            Assert.assertNotNull("restored map must hold key " + key, value);
            return value.getLong(0);
        }
    }

    /**
     * A scalar (map-less) whole-state function whose frozen image is exactly
     * {@link #state}, so a case controls the image's length per seal.
     */
    private static final class ScalarStateStub extends BaseWindowFunction {
        private byte[] state;

        private ScalarStateStub() {
            super(null);
            setCheckpointCompilerMetadata(
                    new LiveViewCheckpointFunctionIdentity(
                            "w0",
                            "seal_scratch_stub()",
                            0,
                            "",
                            "ts asc",
                            "seal-scratch-stub-v1"
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
            for (byte b : state) {
                sink.putByte(b);
            }
        }

        @Override
        public String getName() {
            return "seal_scratch_stub";
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
            for (int i = 0; i < state.length; i++) {
                state[i] = source.getByte(offset + i);
            }
            return state.length;
        }

        @Override
        public boolean supportsCheckpointState() {
            return true;
        }
    }

    /**
     * A symbol map naming key {@code n} {@code acct-n}, with every value built up front.
     */
    private static final class AccountSymbolTable implements StaticSymbolTable {
        private final ObjList<String> values = new ObjList<>();

        private AccountSymbolTable(int keyCount) {
            for (int key = 0; key < keyCount; key++) {
                values.add("acct-" + key);
            }
        }

        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCount() {
            return values.size();
        }

        @Override
        public int keyOf(CharSequence value) {
            if (value == null) {
                return SymbolTable.VALUE_IS_NULL;
            }
            for (int key = 0, n = values.size(); key < n; key++) {
                if (Chars.equals(values.getQuick(key), value)) {
                    return key;
                }
            }
            return SymbolTable.VALUE_NOT_FOUND;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return key > -1 && key < values.size() ? values.getQuick(key) : null;
        }
    }

    /**
     * {@link PartitionedStateStub} keyed by one STRING column, the checkpoint key a SYMBOL
     * partition column encodes to, so the keys it freezes are the ones a keyed replay's
     * {@code Q} names.
     */
    private static final class StringPartitionedStateStub extends BaseWindowFunction {
        private final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        private final Map map;

        private StringPartitionedStateStub() {
            super(null);
            keyTypes.add(ColumnType.STRING);
            map = new OrderedMap(
                    1024,
                    keyTypes,
                    new SingleColumnType(ColumnType.LONG),
                    16,
                    0.7,
                    Integer.MAX_VALUE
            );
            setCheckpointCompilerMetadata(
                    new LiveViewCheckpointFunctionIdentity(
                            "w0",
                            "string_partitioned_seal_scratch_stub()",
                            0,
                            "k",
                            "ts asc",
                            "string-partitioned-seal-scratch-stub-v1"
                    ),
                    new LiveViewCheckpointDependency(
                            LiveViewCheckpointContracts.DependencyKind.FIXED_ANCHOR_SEGMENT,
                            "k",
                            "ts asc",
                            0,
                            0,
                            0,
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
        public int checkpointStateFixedLength() {
            return Long.BYTES;
        }

        @Override
        public int checkpointStateFormatVersion() {
            return 1;
        }

        @Override
        public void close() {
            super.close();
            Misc.free(map);
        }

        @Override
        public void freezeCheckpointState(LiveViewStatePageWriter sink, MapValue value) {
            sink.putLong(value.getLong(0));
        }

        @Override
        public ColumnTypes getCheckpointKeyColumnTypes() {
            return keyTypes;
        }

        @Override
        public int getCheckpointKeyStartIndex() {
            return 1;
        }

        @Override
        public String getName() {
            return "string_partitioned_seal_scratch_stub";
        }

        @Override
        public Map getPartitionMap() {
            return map;
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void onCheckpointRestoreBegin() {
            map.clear();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public long restoreCheckpointState(LiveViewStatePageReader source, long offset, MapValue value) {
            value.putLong(0, source.getLong(offset));
            return Long.BYTES;
        }

        @Override
        public boolean supportsCheckpointState() {
            return true;
        }

        private void putState(CharSequence key, long state) {
            final MapKey mapKey = map.withKey();
            mapKey.putStr(key);
            final MapValue value = mapKey.createValue();
            value.putLong(0, state);
        }

        private long readState(CharSequence key) {
            final MapKey mapKey = map.withKey();
            mapKey.putStr(key);
            final MapValue value = mapKey.findValue();
            Assert.assertNotNull("restored map must hold key " + key, value);
            return value.getLong(0);
        }
    }
}
