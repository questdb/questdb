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
import io.questdb.cairo.lv.LiveViewAccumulatorDescriptor;
import io.questdb.cairo.lv.LiveViewAccumulatorProjection;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapEntry;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreReader;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRootBuilder;
import io.questdb.cairo.lv.LiveViewFunctionSnapshot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.lv.LiveViewWindowStateManifest;
import io.questdb.cairo.lv.LiveViewWindowStatePlan;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * Coverage for the fused window-state root: one persistent partition map holding an
 * anchored window's anchor value and every compatible accumulator component, in place of
 * the anchor root plus one function root per SELECT-list call.
 * <p>
 * Two things carry the format, and the cases are built around them.
 * <ul>
 *     <li><b>The manifest is the whole of the layout.</b> A fused entry carries no
 *     per-partition version and no component tags, so a decoder that disagrees with the
 *     manifest does not fail - it finds the total length it expects and reads the wrong
 *     fields out of it. Byte equality against the predecessor's manifest is therefore
 *     part of what a seal must prove before it may build on that predecessor's leaves,
 *     and part of what a restore must prove before it decodes a single entry.</li>
 *     <li><b>A window root is complete or absent.</b> A legacy predecessor - or one
 *     whose manifest moved - is converted whole on the next seal rather than key by
 *     key, and the converted head has to restore on its own afterwards, which is what
 *     the restart case proves.</li>
 * </ul>
 * The end-to-end cases drive the target shape: an anchored cumulative sum and count per
 * account, whose two calls deliberately do <b>not</b> share a counter - their arguments
 * differ - but do share one tree, one key and one 32-byte inline payload.
 */
public class LiveViewCheckpointWindowRootTest extends AbstractLiveViewTest {

    private static final int ANCHOR_BYTES = Long.BYTES;
    private static final int ANCHOR_VALUE_TYPE = ColumnType.TIMESTAMP_MICRO;
    private static final int COUNT_STATE_BYTES = Long.BYTES;
    private static final String LV_DIR = "lv_window_root";
    private static final int SUM_STATE_BYTES = Double.BYTES + Long.BYTES;
    // The target shape's fused payload: anchor + (sum, nonNullCount) + one counter.
    private static final int TARGET_PAYLOAD_BYTES = ANCHOR_BYTES + SUM_STATE_BYTES + COUNT_STATE_BYTES;

    @Before
    public void setUpCheckpointCadence() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
        try (Path path = new Path()) {
            directRootDir(path).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
            configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testACompleteSnapshotRemovesByOmissionAndAForwardOneDoesNot() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewWindowStateManifest manifest = countManifest();
            final LiveViewCheckpointPageRef first = new LiveViewCheckpointPageRef();
            buildDirectRoot(1, new LiveViewCheckpointPageRef(), manifest, true, builder -> {
                builder.putPartition(directKey(1), countPayload(10, 1), false);
                builder.putPartition(directKey(2), countPayload(20, 2), false);
                builder.putPartition(directKey(3), countPayload(30, 3), false);
            }, first);
            Assert.assertEquals(3, directEntryCount(first));

            // A forward freeze's puts are not the whole truth, so an untouched key keeps
            // the entry the predecessor wrote for it.
            final LiveViewCheckpointPageRef forward = new LiveViewCheckpointPageRef();
            buildDirectRoot(2, first, manifest, false, builder ->
                    builder.putPartition(directKey(2), countPayload(20, 22), false), forward);
            Assert.assertEquals(3, directEntryCount(forward));
            Assert.assertEquals(22, directCount(forward, directKey(2)));
            Assert.assertEquals(3, directCount(forward, directKey(3)));

            // A complete snapshot's are, so a key it did not name is gone. An unchanged
            // key still counts as named: it is live, it simply needs no mutation.
            final LiveViewCheckpointPageRef complete = new LiveViewCheckpointPageRef();
            buildDirectRoot(3, forward, manifest, true, builder -> {
                builder.putPartition(directKey(1), countPayload(10, 1), true);
                builder.putPartition(directKey(2), countPayload(20, 22), true);
            }, complete);
            Assert.assertEquals(2, directEntryCount(complete));
            Assert.assertEquals(1, directCount(complete, directKey(1)));
            Assert.assertEquals(22, directCount(complete, directKey(2)));
        });
    }

    @Test
    public void testAFusedEntryShapeIsValidatedBeforeItIsSliced() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();

            // The leaf holds no length of its own for an inlined payload, so the
            // manifest's total is the only width there is and anything else would slice
            // a component out of bytes something else wrote.
            entry.of(directKey(1), new byte[TARGET_PAYLOAD_BYTES - 1], new LiveViewCheckpointStatePageRef[0]);
            assertInvalid(
                    () -> LiveViewCheckpointWindowRoot.readWindowState(entry, TARGET_PAYLOAD_BYTES),
                    "window state entry scalar length invalid"
            );

            // Right length, but naming a page beside it: not the entry the manifest
            // describes, whatever the bytes read as.
            final LiveViewCheckpointStatePageRef ref = new LiveViewCheckpointStatePageRef();
            ref.of(1, 0, 8, 8, 0x41, 0, 1, 0);
            entry.of(directKey(1), new byte[TARGET_PAYLOAD_BYTES], new LiveViewCheckpointStatePageRef[]{ref});
            assertInvalid(
                    () -> LiveViewCheckpointWindowRoot.readWindowState(entry, TARGET_PAYLOAD_BYTES),
                    "window state entry must not reference a state page"
            );

            assertInvalid(
                    () -> LiveViewCheckpointWindowRoot.readAnchorValue(new byte[ANCHOR_BYTES - 1]),
                    "window state entry is too short for its anchor value"
            );
        });
    }

    @Test
    public void testALegacyHeadConvertsOnTheNextSealAndRestoresIndependently() throws Exception {
        assertMemoryLeak(() -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                // One row first, so the view has compiled its factory and this can reach
                // the window the runtime actually seals through.
                insertAccount(job, "2026-01-01T09:00:00.000000Z", "acct-1", 5.0);
                final LiveViewInstance instance = instance();
                final LiveViewWindow window = instance.getAnchorWindow();
                Assert.assertNotNull(window);
                final LiveViewWindowStatePlan plan = window.getCheckpointWindowStatePlan();
                Assert.assertNotNull("the target shape must compile a plan", plan);
                Assert.assertTrue(isFusedHead());

                // The upgrade in miniature, in both directions: a build with no plan to
                // persist seals the legacy shape, and the build that has one converts.
                // Neither may share a leaf with the other.
                Assert.assertFalse(window.bindCheckpointWindowStatePlan(null));
                insertAccount(job, "2026-01-01T09:00:10.000000Z", "acct-2", 7.0);
                Assert.assertFalse("a view with no plan seals the legacy shape", isFusedHead());
                Assert.assertEquals(2, headFunctionRootCount());
                assertHeadRestoresRuntime(instance);

                Assert.assertTrue(window.bindCheckpointWindowStatePlan(plan));
                insertAccount(job, "2026-01-01T09:00:20.000000Z", "acct-1", 11.0);
                Assert.assertTrue("the first seal above a legacy root converts it", isFusedHead());
                // A cadence seal freezes the whole live domain, so the conversion is
                // complete rather than trickling in over the keys the batch touched: both
                // accounts are in the fused tree, and neither function has a root left.
                Assert.assertEquals(0, headFunctionRootCount());
                Assert.assertEquals(2, headWindowEntryCount());

                // The second restore is the point: it proves the converted root is
                // independently readable rather than merely letting this process carry on
                // with the state it already had in memory.
                assertHeadRestoresRuntime(instance);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAPredecessorIsCompatibleOnlyWhenAllFourPartsMatch() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewWindowStateManifest manifest = countManifest();
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            buildDirectRoot(1, new LiveViewCheckpointPageRef(), manifest, true, builder ->
                    builder.putPartition(directKey(1), countPayload(10, 1), false), root);

            try (
                    LiveViewCheckpointWindowRootBuilder builder =
                            new LiveViewCheckpointWindowRootBuilder(configuration);
                    Path dir = new Path()
            ) {
                directRootDir(dir);
                Assert.assertTrue(builder.isCompatiblePredecessor(
                        dir, root, DIRECT_WINDOW_IDENTITY, ANCHOR_VALUE_TYPE, DIRECT_KEY_SCHEMA, manifest.getEncoded()
                ));
                Assert.assertFalse("a different window is a different root", builder.isCompatiblePredecessor(
                        dir, root, otherWindowIdentity(), ANCHOR_VALUE_TYPE, DIRECT_KEY_SCHEMA, manifest.getEncoded()
                ));
                Assert.assertFalse("a widened anchor changes how the LONG slot reads", builder.isCompatiblePredecessor(
                        dir, root, DIRECT_WINDOW_IDENTITY, ColumnType.LONG, DIRECT_KEY_SCHEMA, manifest.getEncoded()
                ));
                Assert.assertFalse("a re-keyed group addresses different state", builder.isCompatiblePredecessor(
                        dir, root, DIRECT_WINDOW_IDENTITY, ANCHOR_VALUE_TYPE, keySchema(ColumnType.LONG),
                        manifest.getEncoded()
                ));
                // The one nothing else covers. A component the manifest lays out
                // differently makes every existing leaf mean something else, and a
                // recompile can produce it without moving definitionTxn.
                Assert.assertFalse("a manifest that moved makes the leaves unreadable", builder.isCompatiblePredecessor(
                        dir, root, DIRECT_WINDOW_IDENTITY, ANCHOR_VALUE_TYPE, DIRECT_KEY_SCHEMA,
                        sumCountManifest().getEncoded()
                ));
                // And a legacy predecessor is simply not a window root at all.
                Assert.assertFalse(builder.isCompatiblePredecessor(
                        dir, new LiveViewCheckpointPageRef(), DIRECT_WINDOW_IDENTITY, ANCHOR_VALUE_TYPE,
                        DIRECT_KEY_SCHEMA, manifest.getEncoded()
                ));
            }
        });
    }

    @Test
    public void testAnOutOfOrderCorrectionResumesFromAFusedAnchorAndReSealsFused() throws Exception {
        assertMemoryLeak(() -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                for (int second = 10; second <= 60; second += 10) {
                    insertAccount(job, targetTimestamp(second), "acct-1", second / 10.0);
                }
                insertAccount(job, targetTimestamp(70), "acct-2", 9.0);
                Assert.assertTrue(isFusedHead());

                // An anchored view's repair is priced against a resume from the sealed
                // boundary below the change, and that resume always wins here: the
                // segment a daily anchor bounds a rebuild by starts at midnight, so it
                // reads more base rows than replaying the tail above the boundary does.
                // What the fused root has to survive is therefore the resume - a restore
                // from one of its own boundaries, the tail retired above it, and the
                // replay re-sealing on top.
                insertAccount(job, targetTimestamp(35), "acct-1", 100.0);
                assertRepairOutcome("anchor", "resume from anchor");

                Assert.assertTrue("the re-sealed head must still be a window root", isFusedHead());
                Assert.assertEquals(0, headFunctionRootCount());
                Assert.assertEquals(2, headWindowEntryCount());
                assertHeadRestoresRuntime(instance());
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAResidualFunctionKeepsItsOwnRootBesideTheFusedTree() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // count(*) counts rows rather than an argument's non-null values, so it has
            // no argument key and cannot join a count(x) component. "One B-tree per
            // window" is therefore one tree for the group plus its own root for this.
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no, sum(amt_txn) over w as s, count(*) over w as c "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertAccount(job, "2026-01-01T09:00:00.000000Z", "acct-1", 5.0);
                Assert.assertTrue(isFusedHead());
                Assert.assertEquals("count(*) keeps the root it has today", 1, headFunctionRootCount());
                Assert.assertEquals(ANCHOR_BYTES + SUM_STATE_BYTES, headWindowPayloadBytes());
                assertHeadRestoresRuntime(instance());
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testFusedHeadRestoresEveryProjectionFromOneTree() throws Exception {
        assertMemoryLeak(() -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                // Repeated accounts so the restore has to put several rows' worth of
                // accumulation back, and a null amount so the sum's counter and the
                // account counter disagree - which is the whole reason they are two
                // components rather than one.
                insertAccount(job, "2026-01-01T09:00:00.000000Z", "acct-1", 5.0);
                insertAccount(job, "2026-01-01T09:00:10.000000Z", "acct-2", 7.0);
                insertAccount(job, "2026-01-01T09:00:20.000000Z", "acct-1", 11.0);
                insertAccount(job, "2026-01-01T09:00:30.000000Z", "acct-1", null);
                // A bucket crossing, so at least one entry's anchor value moved and its
                // components were reset under it.
                insertAccount(job, "2026-01-02T09:00:00.000000Z", "acct-1", 3.0);

                final LiveViewInstance instance = instance();
                Assert.assertTrue(isFusedHead());
                Assert.assertEquals(TARGET_PAYLOAD_BYTES, headWindowPayloadBytes());
                Assert.assertEquals(2, headWindowEntryCount());
                Assert.assertEquals("both projections are in the fused tree", 0, headFunctionRootCount());
                assertHeadRestoresRuntime(instance);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testTheTargetShapeSealsOneFusedTreeWithNoDataSegment() throws Exception {
        assertMemoryLeak(() -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertAccount(job, "2026-01-01T09:00:00.000000Z", "acct-1", 5.0);
                insertAccount(job, "2026-01-01T09:00:10.000000Z", "acct-2", 7.0);

                final LiveViewInstance instance = instance();
                try (
                    LiveViewCheckpointMetaStore store = openStore(instance);
                    Path checkpointsDir = checkpointsDir(instance);
                    LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration);
                    LiveViewCheckpointPartitionMapReader partitions =
                            new LiveViewCheckpointPartitionMapReader(configuration)
                ) {
                    Assert.assertEquals(
                            "an all-inline seal writes no data page and commits no data segment",
                            0,
                            store.getSuperblock().dataBytes
                    );
                    final LiveViewCheckpointPageRef stateRootRef = headStateRootRef(instance);
                    Assert.assertTrue(windowRoot.ofIfWindowRoot(checkpointsDir, stateRootRef));
                    Assert.assertEquals(ANCHOR_VALUE_TYPE, windowRoot.getAnchorValueType());
                    Assert.assertEquals(TARGET_PAYLOAD_BYTES, windowRoot.getTotalInlineStateBytes());

                    final LiveViewWindowStatePlan plan = instance.getAnchorWindow().getCheckpointWindowStatePlan();
                    Assert.assertNotNull(plan);
                    Assert.assertArrayEquals(
                            "the persisted manifest is the compiled one, byte for byte",
                            plan.getManifest().getEncoded(),
                            windowRoot.getManifest()
                    );
                    Assert.assertTrue(plan.isSameWindowIdentity(windowRoot.getWindowIdentity()));

                    final LiveViewCheckpointPageRef mapRootRef = new LiveViewCheckpointPageRef();
                    windowRoot.getPartitionMapRootRef(mapRootRef);
                    partitions.of(checkpointsDir);
                    Assert.assertEquals(2, partitions.size(mapRootRef));
                    final double[] sums = new double[1];
                    final long[] counts = new long[1];
                    partitions.iterateAll(mapRootRef, entry -> {
                        final byte[] payload =
                                LiveViewCheckpointWindowRoot.readWindowState(entry, TARGET_PAYLOAD_BYTES);
                        Assert.assertEquals("a fused entry names no data page", 0, entry.getStatePageCount());
                        // Read through the plan's own offsets rather than hard-coded ones:
                        // the layout follows encoded component identity, and pinning it
                        // here would pin the ordering rule too.
                        for (int p = 0, n = plan.getProjectionCount(); p < n; p++) {
                            final LiveViewAccumulatorProjection projection = plan.getProjection(p);
                            if (projection.getKind() == LiveViewAccumulatorProjection.PROJECTION_SUM) {
                                sums[0] += Double.longBitsToDouble(
                                        readLongLe(payload, projection.getSumFieldOffset())
                                );
                            } else {
                                counts[0] += readLongLe(payload, projection.getNonNullCountFieldOffset());
                            }
                        }
                    });
                    Assert.assertEquals(12.0, sums[0], 0.0);
                    Assert.assertEquals("one row per account, both non-null", 2, counts[0]);
                }
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testWindowRootRoundTripsItsIdentityLayoutAndEntries() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewWindowStateManifest manifest = sumCountManifest();
            final LiveViewCheckpointPageRef rootRef = new LiveViewCheckpointPageRef();
            buildDirectRoot(1, new LiveViewCheckpointPageRef(), manifest, true, builder -> {
                builder.putPartition(directKey(1), sumCountPayload(100, 1.5, 3), false);
                builder.putPartition(directKey(2), sumCountPayload(200, -2.5, 7), false);
            }, rootRef);

            try (
                    LiveViewCheckpointWindowRoot root = new LiveViewCheckpointWindowRoot(configuration);
                    LiveViewCheckpointPartitionMapReader reader =
                            new LiveViewCheckpointPartitionMapReader(configuration);
                    Path dir = new Path()
            ) {
                directRootDir(dir);
                root.of(dir, rootRef);
                Assert.assertArrayEquals(DIRECT_WINDOW_IDENTITY, root.getWindowIdentity());
                Assert.assertArrayEquals(DIRECT_KEY_SCHEMA, root.getKeySchema());
                Assert.assertEquals(ANCHOR_VALUE_TYPE, root.getAnchorValueType());
                Assert.assertArrayEquals(manifest.getEncoded(), root.getManifest());
                Assert.assertEquals(ANCHOR_BYTES + SUM_STATE_BYTES, root.getTotalInlineStateBytes());

                final LiveViewCheckpointPageRef mapRootRef = new LiveViewCheckpointPageRef();
                root.getPartitionMapRootRef(mapRootRef);
                reader.of(dir);
                Assert.assertEquals(2, reader.size(mapRootRef));
                final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
                Assert.assertTrue(reader.find(mapRootRef, directKey(2), entry));
                final byte[] payload = LiveViewCheckpointWindowRoot.readWindowState(
                        entry,
                        root.getTotalInlineStateBytes()
                );
                Assert.assertEquals(200, LiveViewCheckpointWindowRoot.readAnchorValue(payload));
                Assert.assertEquals(-2.5, Double.longBitsToDouble(readLongLe(payload, ANCHOR_BYTES)), 0.0);
                Assert.assertEquals(7, readLongLe(payload, ANCHOR_BYTES + Double.BYTES));
            }
        });
    }

    private static final byte[] DIRECT_KEY_SCHEMA = keySchema(ColumnType.STRING);
    private static final byte[] DIRECT_WINDOW_IDENTITY =
            LiveViewWindowStatePlan.encodeWindowIdentity("w", "1:3:cod_acct_no;", "1:2:created_at:1;");

    private static void assertInvalid(ThrowingRunnable runnable, CharSequence message) {
        try {
            runnable.run();
            Assert.fail("expected a rejection containing: " + message);
        } catch (CairoException e) {
            Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }

    private static void buildDirectRoot(
            long segmentId,
            LiveViewCheckpointPageRef oldRootRef,
            LiveViewWindowStateManifest manifest,
            boolean isCompleteSnapshot,
            DirectBuild build,
            LiveViewCheckpointPageRef out
    ) {
        try (
                LiveViewCheckpointWindowRootBuilder builder =
                        new LiveViewCheckpointWindowRootBuilder(configuration);
                Path dir = new Path()
        ) {
            builder.of(
                    directRootDir(dir),
                    oldRootRef,
                    DIRECT_WINDOW_IDENTITY,
                    ANCHOR_VALUE_TYPE,
                    DIRECT_KEY_SCHEMA,
                    manifest.getEncoded(),
                    manifest.getTotalInlineStateBytes(),
                    isCompleteSnapshot,
                    null
            );
            build.run(builder);
            builder.build(segmentId, out);
        }
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static byte[] copyBytes(MemoryCARW memory) {
        final int length = (int) memory.getAppendOffset();
        final byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = memory.getByte(i);
        }
        return bytes;
    }

    private static LiveViewWindowStateManifest countManifest() {
        return manifestOf(component(LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT, 1, ColumnType.SYMBOL));
    }

    private static long directCount(LiveViewCheckpointPageRef rootRef, byte[] key) {
        try (
                LiveViewCheckpointWindowRoot root = new LiveViewCheckpointWindowRoot(configuration);
                LiveViewCheckpointPartitionMapReader reader =
                        new LiveViewCheckpointPartitionMapReader(configuration);
                Path dir = new Path()
        ) {
            directRootDir(dir);
            root.of(dir, rootRef);
            final LiveViewCheckpointPageRef mapRootRef = new LiveViewCheckpointPageRef();
            root.getPartitionMapRootRef(mapRootRef);
            reader.of(dir);
            final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
            Assert.assertTrue("the root must hold " + Arrays.toString(key), reader.find(mapRootRef, key, entry));
            return readLongLe(
                    LiveViewCheckpointWindowRoot.readWindowState(entry, root.getTotalInlineStateBytes()),
                    ANCHOR_BYTES
            );
        }
    }

    private static long directEntryCount(LiveViewCheckpointPageRef rootRef) {
        try (
                LiveViewCheckpointWindowRoot root = new LiveViewCheckpointWindowRoot(configuration);
                LiveViewCheckpointPartitionMapReader reader =
                        new LiveViewCheckpointPartitionMapReader(configuration);
                Path dir = new Path()
        ) {
            directRootDir(dir);
            root.of(dir, rootRef);
            final LiveViewCheckpointPageRef mapRootRef = new LiveViewCheckpointPageRef();
            root.getPartitionMapRootRef(mapRootRef);
            reader.of(dir);
            return reader.size(mapRootRef);
        }
    }

    private static byte[] directKey(int key) {
        return new byte[]{(byte) (key >>> 8), (byte) key};
    }

    private static Path directRootDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private static byte[] keySchema(int... columnTypes) {
        final byte[] schema = new byte[Integer.BYTES + columnTypes.length * Integer.BYTES];
        putIntBe(schema, 0, columnTypes.length);
        for (int i = 0; i < columnTypes.length; i++) {
            putIntBe(schema, Integer.BYTES + i * Integer.BYTES, columnTypes[i]);
        }
        return schema;
    }

    private static LiveViewAccumulatorDescriptor component(int family, int argumentColumnIndex, int argumentColumnType) {
        final LiveViewAccumulatorDescriptor component = LiveViewAccumulatorDescriptor.of(
                family,
                argumentColumnIndex,
                argumentColumnType
        );
        Assert.assertNotNull(component);
        return component;
    }

    private static LiveViewWindowStateManifest manifestOf(LiveViewAccumulatorDescriptor component) {
        final ObjList<LiveViewAccumulatorDescriptor> components = new ObjList<>();
        components.add(component);
        final IntList offsets = new IntList();
        offsets.add(ANCHOR_BYTES);
        return new LiveViewWindowStateManifest(
                components,
                offsets,
                LiveViewWindowStatePlan.ANCHOR_STATE_OFFSET,
                ANCHOR_BYTES,
                ANCHOR_BYTES + component.getStateLength()
        );
    }

    private static byte[] otherWindowIdentity() {
        return LiveViewWindowStatePlan.encodeWindowIdentity("w2", "1:3:cod_acct_no;", "1:2:created_at:1;");
    }

    private static void putIntBe(byte[] target, int offset, int value) {
        // ByteBuffer.putInt order, which is what LiveViewCheckpointMetadata encodes with.
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
    }

    private static long readLongLe(byte[] bytes, int offset) {
        long value = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            value |= (bytes[offset + i] & 0xFFL) << (8 * i);
        }
        return value;
    }

    private static LiveViewWindowStateManifest sumCountManifest() {
        return manifestOf(component(LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT, 2, ColumnType.DOUBLE));
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

    private static void writeLongLe(byte[] target, int offset, long value) {
        for (int i = 0; i < Long.BYTES; i++) {
            target[offset + i] = (byte) (value >>> (8 * i));
        }
    }

    private static byte[] countPayload(long anchorValue, long count) {
        final byte[] payload = new byte[ANCHOR_BYTES + COUNT_STATE_BYTES];
        writeLongLe(payload, 0, anchorValue);
        writeLongLe(payload, ANCHOR_BYTES, count);
        return payload;
    }

    private static byte[] sumCountPayload(long anchorValue, double sum, long count) {
        final byte[] payload = new byte[ANCHOR_BYTES + SUM_STATE_BYTES];
        writeLongLe(payload, 0, anchorValue);
        writeLongLe(payload, ANCHOR_BYTES, Double.doubleToRawLongBits(sum));
        writeLongLe(payload, ANCHOR_BYTES + Double.BYTES, count);
        return payload;
    }

    private static String targetTimestamp(int secondOfDay) {
        return String.format(
                "2026-01-01T%02d:%02d:%02d.000000Z",
                secondOfDay / 3600,
                (secondOfDay % 3600) / 60,
                secondOfDay % 60
        );
    }

    private void assertRepairOutcome(String plan, String disposition) throws Exception {
        assertQuery("SELECT checkpoint_repair_plan, checkpoint_repair_last_disposition FROM live_views()")
                .noLeakCheck().noRandomAccess()
                .returns("checkpoint_repair_plan\tcheckpoint_repair_last_disposition\n"
                        + plan + "\t" + disposition + "\n");
    }

    /**
     * Compares the view against a from-base recompute of the same window. ANCHOR is
     * live-view syntax, so the daily bucket is written out as an ordinary partition
     * term - which is exactly what an anchored window computes.
     */
    private void assertViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(select created_at, cod_acct_no, "
                        + "sum(amt_txn) over (partition by cod_acct_no, bucket order by created_at "
                        + "rows between unbounded preceding and current row) as cumulative_sum, "
                        + "count(cod_acct_no) over (partition by cod_acct_no, bucket order by created_at "
                        + "rows between unbounded preceding and current row) as cumulative_count "
                        + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)"
                        + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
    }

    /**
     * Snapshots the runtime, restores the published head over it and asserts the two
     * agree byte for byte - anchor map and every function map, grouped or residual.
     */
    private void assertHeadRestoresRuntime(LiveViewInstance instance) {
        final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);
        final LiveViewWindow window = instance.getAnchorWindow();
        final byte[][] expected = snapshotRuntime(functions, window);
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointTimelineStoreReader reader =
                        new LiveViewCheckpointTimelineStoreReader(configuration)
        ) {
            reader.of(checkpointsDir);
            reader.restoreLatest(instance.getLiveViewToken().getTableId(), functions, window);
        }
        final byte[][] actual = snapshotRuntime(functions, window);
        Assert.assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            Assert.assertArrayEquals("restored runtime state differs at index " + i, expected[i], actual[i]);
        }
    }

    private void createBaseTable() throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no symbol, amt_txn double) "
                + "timestamp(created_at) partition by hour wal");
    }

    private void createTargetView() throws Exception {
        createBaseTable();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, cod_acct_no, sum(amt_txn) over w as cumulative_sum, "
                + "count(cod_acct_no) over w as cumulative_count "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
    }

    private int headFunctionRootCount() {
        final LiveViewInstance instance = instance();
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                LiveViewCheckpointFunctionDirectory directory =
                        new LiveViewCheckpointFunctionDirectory(configuration)
        ) {
            headRoot(instance, checkpointsDir, root);
            final LiveViewCheckpointPageRef directoryRef = new LiveViewCheckpointPageRef();
            root.getFunctionDirectoryRef(directoryRef);
            directory.of(checkpointsDir, directoryRef);
            return directory.size();
        }
    }

    private void headRoot(LiveViewInstance instance, Path checkpointsDir, LiveViewCheckpointRoot root) {
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration)
        ) {
            timeline.of(checkpointsDir);
            final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
            Assert.assertTrue("the view must have sealed a boundary", timeline.last(pin.getTimelineRootRef(), entry));
            root.of(checkpointsDir, entry.rootRef);
        }
    }

    private LiveViewCheckpointPageRef headStateRootRef(LiveViewInstance instance) {
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration)
        ) {
            headRoot(instance, checkpointsDir, root);
            final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
            root.getStateRootRef(stateRootRef);
            Assert.assertFalse("an anchored view always has a state root", stateRootRef.isNull());
            return stateRootRef;
        }
    }

    private long headWindowEntryCount() {
        final LiveViewInstance instance = instance();
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration);
                LiveViewCheckpointPartitionMapReader partitions =
                        new LiveViewCheckpointPartitionMapReader(configuration)
        ) {
            Assert.assertTrue(windowRoot.ofIfWindowRoot(checkpointsDir, headStateRootRef(instance)));
            final LiveViewCheckpointPageRef mapRootRef = new LiveViewCheckpointPageRef();
            windowRoot.getPartitionMapRootRef(mapRootRef);
            partitions.of(checkpointsDir);
            return partitions.size(mapRootRef);
        }
    }

    private int headWindowPayloadBytes() {
        final LiveViewInstance instance = instance();
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration)
        ) {
            Assert.assertTrue(windowRoot.ofIfWindowRoot(checkpointsDir, headStateRootRef(instance)));
            return windowRoot.getTotalInlineStateBytes();
        }
    }

    private void insertAccount(LiveViewRefreshJob job, String timestamp, String account, Double amount) throws Exception {
        execute("insert into tx values ('" + timestamp + "', '" + account + "', "
                + (amount == null ? "null" : amount.toString()) + ")");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private LiveViewInstance instance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    private boolean isFusedHead() {
        final LiveViewInstance instance = instance();
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration)
        ) {
            return windowRoot.ofIfWindowRoot(checkpointsDir, headStateRootRef(instance));
        }
    }

    private LiveViewCheckpointMetaStore openStore(LiveViewInstance instance) {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        try (Path checkpointsDir = checkpointsDir(instance)) {
            store.of(checkpointsDir);
        }
        return store;
    }

    private byte[][] snapshotRuntime(ObjList<WindowFunction> functions, LiveViewWindow anchorWindow) {
        final byte[][] states = new byte[functions.size() + 1][];
        int count = 0;
        try (MemoryCARW sink = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            anchorWindow.snapshot(sink);
            states[count++] = copyBytes(sink);
        }
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
        return Arrays.copyOf(states, count);
    }

    @FunctionalInterface
    private interface DirectBuild {
        void run(LiveViewCheckpointWindowRootBuilder builder);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run();
    }
}
