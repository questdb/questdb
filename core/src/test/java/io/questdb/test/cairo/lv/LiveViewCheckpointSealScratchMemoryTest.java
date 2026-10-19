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
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    private static final String LV_DIR = "lv_seal_scratch_memory";
    // Comfortably above every allocation the seal path retains by design, and
    // far below the state image, so the assertion separates "scratch released"
    // from "scratch retained" with no sensitivity to incidental allocations.
    private static final long RELEASED_TOLERANCE_BYTES = 1_048_576;
    private static final int STATE_IMAGE_BYTES = 8_388_608;

    @Before
    public void setUp() {
        super.setUp();
        try (Path dir = new Path(); Path path = new Path()) {
            final FilesFacade ff = configuration.getFilesFacade();
            checkpointsDir(dir);
            ff.mkdirs(LiveViewCheckpointLayout.metaDirPath(path, dir).slash(), configuration.getMkDirMode());
            ff.mkdirs(LiveViewCheckpointLayout.dataDirPath(path, dir).slash(), configuration.getMkDirMode());
        }
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
                             writer.beginRepair(dir, null, tracker)) {
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
                             writer.beginRepair(dir, null, tracker)) {
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

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
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

    private void seal(LiveViewCheckpointTimelineStoreWriter writer, ScalarStateStub stub, long seq) {
        seal(writer, stub, seq, null);
    }

    private void seal(
            LiveViewCheckpointTimelineStoreWriter writer,
            ScalarStateStub stub,
            long seq,
            MemoryTracker memoryTracker
    ) {
        try (Path dir = new Path()) {
            checkpointsDir(dir);
            final ObjList<WindowFunction> functions = new ObjList<>();
            functions.add(stub);
            writer.append(
                    dir,
                    functions,
                    null,
                    DEFINITION_TXN,
                    0,
                    seq,
                    seq,
                    0,
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
        public boolean supportsCheckpointState() {
            return true;
        }
    }
}
