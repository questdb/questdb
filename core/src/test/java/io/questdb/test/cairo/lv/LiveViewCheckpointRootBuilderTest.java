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
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRootBuilder;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapEntry;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointRootBuilder;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryWriter;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRootBuilder;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class LiveViewCheckpointRootBuilderTest extends AbstractCairoTest {

    private static final byte[] WINDOW_IDENTITY = "w0".getBytes(StandardCharsets.UTF_8);
    // The manifest of a window whose only durable component is the anchor value, which is
    // what a state root the builder only counts segments for needs to be.
    private static final byte[] WINDOW_MANIFEST = new byte[]{0};
    private static final byte[] AVG_ID = "avg(double):w0:0".getBytes(StandardCharsets.UTF_8);
    /**
     * Image bytes one identity pool of a builder or metadata reader may keep once its
     * operation ends: LiveViewCheckpointMetadata.MAX_RETAINED_IDENTITY_BYTES, which is
     * package-private to the checkpoint package.
     */
    private static final long IDENTITY_POOL_RETAINED_BYTES_LIMIT = 1_048_576;
    private static final String LV_DIR = "lv_root_builder";
    private static final int[] OUTLIER_IDENTITY_WIDTHS = {300_000, 400_000, 500_000};
    private static final int SMALL_IDENTITY_WIDTH = 64;
    private static final byte[] SUM_ID = "sum(long):w1:1".getBytes(StandardCharsets.UTF_8);

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            checkpointsDir(path).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
            configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testCheckpointRootsShareFunctionRootsAndReportExactSegments() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef avgRoot = buildInitialAvgRoot(20);
            final LiveViewCheckpointPageRef sumRoot = buildSumRoot(21);
            final LiveViewCheckpointPageRef windowRoot = buildWindowRoot(22);
            final LiveViewCheckpointPageRef checkpoint1 = new LiveViewCheckpointPageRef();
            final LongList referenced1 = new LongList();
            try (LiveViewCheckpointRootBuilder builder = new LiveViewCheckpointRootBuilder(configuration);
                 Path dir = new Path()) {
                builder.begin(checkpointsDir(dir), 7, 123_456, 42, windowRoot);
                builder.addFunction(sumRoot);
                builder.addFunction(avgRoot);
                builder.build(30, checkpoint1);
                builder.getReferencedSegmentIds(referenced1);
            }
            // Data segments 1, 2 and 4 hold the functions' state pages; 20, 21 and
            // 22 hold the function and window roots with their map pages, and 30
            // holds this root and its function directory.
            assertLongList(referenced1, 1, 2, 4, 20, 21, 22, 30);

            final LiveViewCheckpointPageRef avgDirectoryRef = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef sumDirectoryRef = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                 LiveViewCheckpointFunctionDirectory directory = new LiveViewCheckpointFunctionDirectory(configuration);
                 Path dir = new Path()) {
                root.of(checkpointsDir(dir), checkpoint1);
                Assert.assertEquals(7, root.getCheckpointId());
                Assert.assertEquals(123_456, root.getMaxTimestamp());
                Assert.assertEquals(42, root.getDefinitionTxn());
                Assert.assertEquals(7, root.getSegmentIdCount());
                final LiveViewCheckpointPageRef directoryRef = new LiveViewCheckpointPageRef();
                root.getFunctionDirectoryRef(directoryRef);
                directory.of(checkpointsDir(dir), directoryRef);
                Assert.assertEquals(2, directory.size());
                Assert.assertTrue(directory.find(AVG_ID, avgDirectoryRef));
                Assert.assertTrue(directory.find(SUM_ID, sumDirectoryRef));
                assertRefEquals(avgRoot, avgDirectoryRef);
                assertRefEquals(sumRoot, sumDirectoryRef);
            }

            final LiveViewCheckpointPageRef checkpoint2 = new LiveViewCheckpointPageRef();
            final LongList referenced2 = new LongList();
            try (LiveViewCheckpointRootBuilder builder = new LiveViewCheckpointRootBuilder(configuration);
                 Path dir = new Path()) {
                builder.begin(checkpointsDir(dir), 8, 223_456, 42, windowRoot);
                builder.addFunction(avgRoot);
                builder.addFunction(sumRoot);
                builder.build(31, checkpoint2);
                builder.getReferencedSegmentIds(referenced2);
            }
            // The second root reuses every subordinate root by reference, so only
            // the segment carrying the root itself differs.
            assertLongList(referenced2, 1, 2, 4, 20, 21, 22, 31);
            try (LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                 LiveViewCheckpointFunctionDirectory directory = new LiveViewCheckpointFunctionDirectory(configuration);
                 Path dir = new Path()) {
                root.of(checkpointsDir(dir), checkpoint2);
                final LiveViewCheckpointPageRef directoryRef = new LiveViewCheckpointPageRef();
                root.getFunctionDirectoryRef(directoryRef);
                directory.of(checkpointsDir(dir), directoryRef);
                final LiveViewCheckpointPageRef ref = new LiveViewCheckpointPageRef();
                Assert.assertTrue(directory.find(AVG_ID, ref));
                assertRefEquals(avgRoot, ref);
                Assert.assertTrue(directory.find(SUM_ID, ref));
                assertRefEquals(sumRoot, ref);
            }

            // Swapping the first root for the second is one reference transaction
            // over both halves of the closure: everything the two share keeps its
            // single reference, the segment only the old root named retires, and
            // the one only the new root names takes its place.
            try (LiveViewCheckpointSegmentDirectoryWriter segments = new LiveViewCheckpointSegmentDirectoryWriter(configuration);
                 Path dir = new Path()) {
                segments.of(checkpointsDir(dir));
                segments.begin(new LiveViewCheckpointPageRef());
                for (int i = 0; i < referenced1.size(); i++) {
                    final long segmentId = referenced1.getQuick(i);
                    segments.addSegment(segmentId, 100 + i, 1, kindOf(segmentId));
                }
                // The segment the second build wrote enters the catalogue with the
                // one reference its own root holds, so it is not counted again.
                segments.addSegment(31, 200, 1, LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_BOUNDARY);
                final LongList added = new LongList(referenced2);
                added.remove(31);
                segments.applyRootReferenceChanges(referenced1, added, 2);
                for (int i = 0; i < referenced1.size(); i++) {
                    final long segmentId = referenced1.getQuick(i);
                    Assert.assertEquals(segmentId == 30 ? 0 : 1, segments.getReferenceCount(segmentId));
                }
                Assert.assertEquals(2, segments.getRetireGeneration(30));
                Assert.assertEquals(1, segments.getReferenceCount(31));
            }
        });
    }

    @Test
    public void testFunctionBuilderUpdatesOnlyChangedPartitionOwnership() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef oldRoot = buildInitialAvgRoot(40);
            final LiveViewCheckpointPageRef newRoot = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointFunctionRootBuilder builder = new LiveViewCheckpointFunctionRootBuilder(configuration);
                 Path dir = new Path()) {
                builder.of(checkpointsDir(dir), oldRoot, AVG_ID, 1, new byte[]{1, 2});
                builder.setScalarStateRef(stateRef(3, 0));
                builder.putPartition(key(1), new byte[]{11}, new LiveViewCheckpointStatePageRef[]{stateRef(2, 24)});
                builder.removePartition(key(2));
                builder.build(41, newRoot);
            }

            try (LiveViewCheckpointFunctionRoot oldFunction = new LiveViewCheckpointFunctionRoot(configuration);
                 LiveViewCheckpointFunctionRoot newFunction = new LiveViewCheckpointFunctionRoot(configuration);
                 LiveViewCheckpointPartitionMapReader mapReader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                // Data segments 1 and 2 hold two state pages each; metadata segment
                // 40 holds the root page and the one map leaf under it.
                oldFunction.of(checkpointsDir(dir), oldRoot);
                Assert.assertEquals(3, oldFunction.getSegmentUseCountSize());
                Assert.assertEquals(1, oldFunction.getSegmentId(0));
                Assert.assertEquals(2, oldFunction.getSegmentUseCount(0));
                Assert.assertEquals(2, oldFunction.getSegmentId(1));
                Assert.assertEquals(2, oldFunction.getSegmentUseCount(1));
                Assert.assertEquals(40, oldFunction.getSegmentId(2));
                Assert.assertEquals(2, oldFunction.getSegmentUseCount(2));

                // The rewrite drops segment 1 entirely, keeps one page of 2, takes
                // the new scalar's segment 3, and moves both its own pages into 41 -
                // so 40 leaves the closure and can be reclaimed with the old root.
                newFunction.of(checkpointsDir(dir), newRoot);
                Assert.assertEquals(3, newFunction.getSegmentUseCountSize());
                Assert.assertEquals(2, newFunction.getSegmentId(0));
                Assert.assertEquals(1, newFunction.getSegmentUseCount(0));
                Assert.assertEquals(3, newFunction.getSegmentId(1));
                Assert.assertEquals(1, newFunction.getSegmentUseCount(1));
                Assert.assertEquals(41, newFunction.getSegmentId(2));
                Assert.assertEquals(2, newFunction.getSegmentUseCount(2));

                final LiveViewCheckpointPageRef oldMapRoot = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef newMapRoot = new LiveViewCheckpointPageRef();
                oldFunction.getPartitionMapRootRef(oldMapRoot);
                newFunction.getPartitionMapRootRef(newMapRoot);
                mapReader.of(checkpointsDir(dir));
                final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
                Assert.assertTrue(mapReader.find(oldMapRoot, key(1), entry));
                Assert.assertEquals(1, entry.getScalarState()[0]);
                Assert.assertTrue(mapReader.find(oldMapRoot, key(2), entry));
                Assert.assertTrue(mapReader.find(newMapRoot, key(1), entry));
                Assert.assertEquals(11, entry.getScalarState()[0]);
                Assert.assertEquals(2, entry.getStatePageRef(0).getSegmentId());
                Assert.assertFalse(mapReader.find(newMapRoot, key(2), entry));
            }
        });
    }

    @Test
    public void testIdentityPoolsKeepBoundedBytesAcrossDefinitions() throws Exception {
        assertMemoryLeak(() -> {
            try (LiveViewCheckpointRootBuilder builder = new LiveViewCheckpointRootBuilder(configuration);
                 LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                 LiveViewCheckpointFunctionDirectory directory = new LiveViewCheckpointFunctionDirectory(configuration);
                 LiveViewCheckpointFunctionRoot functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
                 LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration);
                 Path dir = new Path()) {
                // A refresh worker keeps one builder and one set of metadata readers for its
                // whole life and serves one definition after another through them. Each outlier
                // identity fits under the limit on its own, but together they exceed it, so a
                // shell that parked every width it has seen would keep 1,200,064 image bytes.
                long segmentId = 100;
                for (int i = 0; i < OUTLIER_IDENTITY_WIDTHS.length; i++) {
                    final byte[] identity = identity(OUTLIER_IDENTITY_WIDTHS[i], 'a' + i);
                    serveDefinition(builder, root, directory, functionRoot, windowRoot, dir, identity, segmentId);
                    segmentId += 3;
                }
                final byte[] smallIdentity = identity(SMALL_IDENTITY_WIDTH, 'z');
                serveDefinition(builder, root, directory, functionRoot, windowRoot, dir, smallIdentity, segmentId);
                segmentId += 3;

                final long[] retained = retainedIdentityPoolBytes(builder, directory, functionRoot, windowRoot);
                final String retainedText = " [rootBuilder=" + retained[0]
                        + ", rootBuilderFunctionRoot=" + retained[1]
                        + ", rootBuilderWindowRoot=" + retained[2]
                        + ", functionDirectory=" + retained[3]
                        + ", functionRoot=" + retained[4]
                        + ", windowRoot=" + retained[5]
                        + ", limit=" + IDENTITY_POOL_RETAINED_BYTES_LIMIT + ']';
                for (long bytes : retained) {
                    Assert.assertTrue(
                            "an identity pool kept the widths of definitions served before" + retainedText,
                            bytes <= IDENTITY_POOL_RETAINED_BYTES_LIMIT
                    );
                }
                // A detached builder stages nothing, so no slot of its identity list may keep an
                // image of a definition it built before.
                final ObjList<?> stagedIdentities = (ObjList<?>) readField(builder, "functionIdentities");
                for (int i = 0, n = stagedIdentities.size(); i < n; i++) {
                    Assert.assertNull("detached builder still names a staged identity at " + i, stagedIdentities.getQuick(i));
                }

                // The same definition again reuses every pooled image: a pool within its limit
                // keeps what it holds when the operation ends.
                final byte[] pooledFunctionIdentity = functionRoot.getFunctionIdentity();
                final byte[] pooledWindowIdentity = windowRoot.getWindowIdentity();
                serveDefinition(builder, root, directory, functionRoot, windowRoot, dir, smallIdentity, segmentId);
                Assert.assertSame(pooledFunctionIdentity, functionRoot.getFunctionIdentity());
                Assert.assertSame(pooledWindowIdentity, windowRoot.getWindowIdentity());
                final long[] reused = retainedIdentityPoolBytes(builder, directory, functionRoot, windowRoot);
                for (int i = 0; i < retained.length; i++) {
                    Assert.assertTrue("identity pool " + i + " must keep the images it lent" + retainedText, retained[i] > 0);
                    Assert.assertEquals("identity pool " + i + " must reuse the images it kept" + retainedText, retained[i], reused[i]);
                }
            }
        });
    }

    @Test
    public void testStructurallyCorruptRootPagesRejected() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef functionRoot = writeRaw(60, LiveViewCheckpointFunctionRoot.PAGE_KIND, mem -> {
                mem.putInt(1);
                mem.putInt(1);
                mem.putInt(1);
                mem.putInt(0);
                mem.putInt(2);
                new LiveViewCheckpointStatePageRef().clear().writeTo(mem);
                putNullMetaRef(mem);
                mem.putByte((byte) 'f');
                mem.putLong(5);
                mem.putLong(1);
                mem.putLong(4);
                mem.putLong(1);
            });
            try (LiveViewCheckpointFunctionRoot root = new LiveViewCheckpointFunctionRoot(configuration);
                 Path dir = new Path()) {
                assertInvalid(() -> root.of(checkpointsDir(dir), functionRoot), "segment catalogue invalid");
            }

            final LiveViewCheckpointPageRef directoryRoot = writeRaw(61, LiveViewCheckpointFunctionDirectory.PAGE_KIND, mem -> {
                mem.putInt(1);
                mem.putInt(2);
                putDirectoryEntry(mem, (byte) 'f');
                putDirectoryEntry(mem, (byte) 'f');
            });
            try (LiveViewCheckpointFunctionDirectory directory = new LiveViewCheckpointFunctionDirectory(configuration);
                 Path dir = new Path()) {
                assertInvalid(() -> directory.of(checkpointsDir(dir), directoryRoot), "not strictly increasing");
            }

            final LiveViewCheckpointPageRef checkpointRoot = writeRaw(62, LiveViewCheckpointRoot.PAGE_KIND, mem -> {
                mem.putInt(1);
                mem.putInt(2);
                mem.putLong(1);
                mem.putLong(100);
                mem.putLong(1);
                putNullMetaRef(mem);
                putFakeMetaRef(mem);
                mem.putLong(5);
                mem.putLong(4);
            });
            try (LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                 Path dir = new Path()) {
                assertInvalid(() -> root.of(checkpointsDir(dir), checkpointRoot), "not strictly increasing");
            }

            final LiveViewCheckpointPageRef oversizedDirectory = writeRaw(63, LiveViewCheckpointFunctionDirectory.PAGE_KIND, mem -> {
                mem.putInt(1);
                mem.putInt(1000);
            });
            try (LiveViewCheckpointFunctionDirectory directory = new LiveViewCheckpointFunctionDirectory(configuration);
                 Path dir = new Path()) {
                assertInvalid(() -> directory.of(checkpointsDir(dir), oversizedDirectory), "count exceeds payload");
            }
        });
    }

    private static void assertInvalid(ThrowingRunnable runnable, CharSequence message) {
        try {
            runnable.run();
            Assert.fail("expected corrupt root rejection");
        } catch (CairoException e) {
            Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }

    private static void assertLongList(LongList actual, long... expected) {
        Assert.assertEquals(expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(expected[i], actual.getQuick(i));
        }
    }

    private static void assertRefEquals(LiveViewCheckpointPageRef expected, LiveViewCheckpointPageRef actual) {
        Assert.assertEquals(expected.getSegmentId(), actual.getSegmentId());
        Assert.assertEquals(expected.getOffset(), actual.getOffset());
        Assert.assertEquals(expected.getLength(), actual.getLength());
    }

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private static byte[] identity(int width, int fill) {
        final byte[] identity = new byte[width];
        Arrays.fill(identity, (byte) fill);
        return identity;
    }

    private static byte[] key(int key) {
        return new byte[]{(byte) key};
    }

    /**
     * The two id spaces this test mints by hand: ids below 20 name the data
     * segments the state page references point at, the rest name the metadata
     * segments the builders wrote.
     */
    private static long kindOf(long segmentId) {
        return segmentId < 20
                ? LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_DATA
                : LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_BOUNDARY;
    }

    private static void putDirectoryEntry(MemoryA mem, byte identity) {
        mem.putInt(1);
        mem.putByte(identity);
        putFakeMetaRef(mem);
    }

    private static void putFakeMetaRef(MemoryA mem) {
        mem.putLong(99);
        mem.putLong(24);
        mem.putInt(LiveViewCheckpointLayout.PAGE_HEADER_SIZE);
    }

    private static void putNullMetaRef(MemoryA mem) {
        mem.putLong(-1);
        mem.putLong(0);
        mem.putInt(0);
    }

    private static Object readField(Object owner, String name) throws ReflectiveOperationException {
        final Field field = owner.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
    }

    /**
     * Image bytes each identity pool of the shells a worker keeps holds: the root builder's
     * own, those of the function and window roots it reads through, then those of the
     * standalone directory, function root and window root.
     */
    private static long[] retainedIdentityPoolBytes(
            LiveViewCheckpointRootBuilder builder,
            LiveViewCheckpointFunctionDirectory directory,
            LiveViewCheckpointFunctionRoot functionRoot,
            LiveViewCheckpointWindowRoot windowRoot
    ) throws ReflectiveOperationException {
        return new long[]{
                retainedPoolBytes(builder, "functionIdentityBytes"),
                retainedPoolBytes(readField(builder, "functionRoot"), "decodedBytes"),
                retainedPoolBytes(readField(builder, "windowRoot"), "decodedBytes"),
                retainedPoolBytes(directory, "identityBytes"),
                retainedPoolBytes(functionRoot, "decodedBytes"),
                retainedPoolBytes(windowRoot, "decodedBytes")
        };
    }

    /**
     * Image bytes the exact-width byte array pool in {@code owner}'s {@code poolField}
     * holds. {@code countRetainedBytesForTest()} sums the arrays of every width rather than
     * reading the pool's own tally.
     */
    private static long retainedPoolBytes(Object owner, String poolField) throws ReflectiveOperationException {
        final Object pool = readField(owner, poolField);
        final Method count = pool.getClass().getDeclaredMethod("countRetainedBytesForTest");
        count.setAccessible(true);
        return (long) count.invoke(pool);
    }

    private static LiveViewCheckpointStatePageRef stateRef(long segmentId, long offset) {
        return new LiveViewCheckpointStatePageRef().of(segmentId, offset, 8, 8, 0x31, 0, 1, 0);
    }

    private LiveViewCheckpointPageRef buildWindowRoot(long metadataSegmentId) {
        final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointWindowRootBuilder builder = new LiveViewCheckpointWindowRootBuilder(configuration);
             Path dir = new Path()) {
            builder.of(
                    checkpointsDir(dir),
                    new LiveViewCheckpointPageRef(),
                    WINDOW_IDENTITY,
                    ColumnType.TIMESTAMP_MICRO,
                    new byte[]{1, 0, 0, 0},
                    WINDOW_MANIFEST,
                    Long.BYTES,
                    true,
                    null
            );
            builder.putPartition(key(1), anchorState(111), false);
            builder.putPartition(key(2), anchorState(222), false);
            builder.build(metadataSegmentId, root);
        }
        return root;
    }

    /**
     * One window entry's payload for a manifest declaring no components beside the anchor:
     * the anchor value's eight little-endian bytes and nothing else.
     */
    private static byte[] anchorState(long anchorValue) {
        final byte[] state = new byte[Long.BYTES];
        for (int i = 0; i < Long.BYTES; i++) {
            state[i] = (byte) (anchorValue >>> (i * Byte.SIZE));
        }
        return state;
    }

    private LiveViewCheckpointPageRef buildInitialAvgRoot(long metadataSegmentId) {
        final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointFunctionRootBuilder builder = new LiveViewCheckpointFunctionRootBuilder(configuration);
             Path dir = new Path()) {
            builder.of(checkpointsDir(dir), new LiveViewCheckpointPageRef(), AVG_ID, 1, new byte[]{1, 2});
            builder.setScalarStateRef(stateRef(1, 0));
            builder.putPartition(key(1), new byte[]{1}, new LiveViewCheckpointStatePageRef[]{stateRef(1, 8), stateRef(2, 16)});
            builder.putPartition(key(2), new byte[]{2}, new LiveViewCheckpointStatePageRef[]{stateRef(2, 24)});
            builder.build(metadataSegmentId, root);
        }
        return root;
    }

    private LiveViewCheckpointPageRef buildSumRoot(long metadataSegmentId) {
        final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointFunctionRootBuilder builder = new LiveViewCheckpointFunctionRootBuilder(configuration);
             Path dir = new Path()) {
            builder.of(checkpointsDir(dir), new LiveViewCheckpointPageRef(), SUM_ID, 1, new byte[]{3});
            builder.putPartition(key(7), new byte[]{7}, new LiveViewCheckpointStatePageRef[]{stateRef(4, 0)});
            builder.build(metadataSegmentId, root);
        }
        return root;
    }

    /**
     * Serves one single-function definition the way a worker does: writes its window and
     * function roots, seals a checkpoint root over them through {@code builder}, then
     * restores through the long-lived readers - the directory the root names, the function
     * root in it and the window root. This method detaches every shell when its operation
     * ends, as the worker detaches its own. The window shares the function's identity width.
     */
    private void serveDefinition(
            LiveViewCheckpointRootBuilder builder,
            LiveViewCheckpointRoot root,
            LiveViewCheckpointFunctionDirectory directory,
            LiveViewCheckpointFunctionRoot functionRoot,
            LiveViewCheckpointWindowRoot windowRoot,
            Path dir,
            byte[] identity,
            long segmentId
    ) {
        final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointFunctionRootBuilder functionBuilder = new LiveViewCheckpointFunctionRootBuilder(configuration)) {
            functionBuilder.of(checkpointsDir(dir), new LiveViewCheckpointPageRef(), identity, 1, new byte[]{1, 2});
            functionBuilder.putPartition(key(1), new byte[]{1}, new LiveViewCheckpointStatePageRef[]{stateRef(1, 0)});
            functionBuilder.build(segmentId, functionRootRef);
        }
        final LiveViewCheckpointPageRef windowRootRef = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointWindowRootBuilder windowBuilder = new LiveViewCheckpointWindowRootBuilder(configuration)) {
            windowBuilder.of(
                    checkpointsDir(dir),
                    new LiveViewCheckpointPageRef(),
                    identity,
                    ColumnType.TIMESTAMP_MICRO,
                    new byte[]{1, 0, 0, 0},
                    WINDOW_MANIFEST,
                    Long.BYTES,
                    true,
                    null
            );
            windowBuilder.putPartition(key(1), anchorState(111), false);
            windowBuilder.build(segmentId + 1, windowRootRef);
        }

        final LiveViewCheckpointPageRef checkpointRef = new LiveViewCheckpointPageRef();
        builder.begin(checkpointsDir(dir), segmentId, 1_000, 1, windowRootRef);
        builder.addFunction(functionRootRef);
        builder.build(segmentId + 2, checkpointRef);
        builder.detach();

        final LiveViewCheckpointPageRef directoryRef = new LiveViewCheckpointPageRef();
        final LiveViewCheckpointPageRef foundRef = new LiveViewCheckpointPageRef();
        root.of(checkpointsDir(dir), checkpointRef);
        root.getFunctionDirectoryRef(directoryRef);
        directory.of(checkpointsDir(dir), directoryRef);
        Assert.assertEquals(1, directory.size());
        Assert.assertTrue(directory.find(identity, foundRef));
        assertRefEquals(functionRootRef, foundRef);
        directory.detach();
        root.detach();

        functionRoot.of(checkpointsDir(dir), functionRootRef);
        Assert.assertArrayEquals(identity, functionRoot.getFunctionIdentity());
        functionRoot.detach();

        windowRoot.of(checkpointsDir(dir), windowRootRef);
        Assert.assertArrayEquals(identity, windowRoot.getWindowIdentity());
        windowRoot.detach();
    }

    private LiveViewCheckpointPageRef writeRaw(long segmentId, int pageKind, PageWriter pageWriter) {
        final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointMetaSegmentWriter writer = new LiveViewCheckpointMetaSegmentWriter(configuration);
             Path dir = new Path()) {
            writer.of(checkpointsDir(dir), segmentId);
            pageWriter.write(writer.beginPage(pageKind));
            writer.endPage(root);
            writer.commit();
        }
        return root;
    }

    @FunctionalInterface
    private interface PageWriter {
        void write(MemoryA mem);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run();
    }
}
