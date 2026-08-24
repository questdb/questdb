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
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
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
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.FilesFacade;
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

    private static final long DEFINITION_TXN = 7;
    private static final long LIFECYCLE_IDENTITY = 201;
    private static final long LIFECYCLE_IDENTITY_A = 202;
    private static final long LIFECYCLE_IDENTITY_B = 203;
    private static final String LV_DIR = "lv_seal_scratch_memory";
    // Comfortably above every allocation the seal path retains by design, and
    // far below the state image, so the assertion separates "scratch released"
    // from "scratch retained" with no sensitivity to incidental allocations.
    private static final long RELEASED_TOLERANCE_BYTES = 1_048_576;
    private static final int STATE_IMAGE_BYTES = 8_388_608;

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

        final Field keyField = partitionA.getClass().getDeclaredField("key");
        final Field scalarStateField = partitionA.getClass().getDeclaredField("scalarState");
        keyField.setAccessible(true);
        scalarStateField.setAccessible(true);
        Assert.assertNotSame(
                "live captures must not share key arrays",
                keyField.get(partitionA),
                keyField.get(partitionB)
        );
        Assert.assertNotSame(
                "live captures must not share scalar-state arrays",
                scalarStateField.get(partitionA),
                scalarStateField.get(partitionB)
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

    private MemoryTracker acquireRefreshTracker() {
        return engine.getMemoryTrackerProvider().acquire(
                AllowAllSecurityContext.INSTANCE,
                1,
                MemoryTrackerWorkload.LIVE_VIEW_REFRESH
        );
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
        private static final ColumnTypes KEY_TYPES = new SingleColumnType(ColumnType.LONG);
        private final Map map = new OrderedMap(
                1024,
                KEY_TYPES,
                new SingleColumnType(ColumnType.LONG),
                16,
                0.7,
                8
        );

        private PartitionedStateStub() {
            super(null);
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
            sink.putLong(value.getLong(0));
        }

        @Override
        public ColumnTypes getCheckpointKeyColumnTypes() {
            return KEY_TYPES;
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

        private void putState(long key, long state) {
            final MapKey mapKey = map.withKey();
            mapKey.putLong(key);
            mapKey.createValue().putLong(0, state);
        }

        private long readState(long key) {
            final MapKey mapKey = map.withKey();
            mapKey.putLong(key);
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
}
