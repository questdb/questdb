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
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.LongList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class LiveViewCheckpointRootBuilderTest extends AbstractCairoTest {

    private static final byte[] AVG_ID = "avg(double):w0:0".getBytes(StandardCharsets.UTF_8);
    private static final String LV_DIR = "lv_root_builder";
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
            final LongList anchorSegments = new LongList();
            anchorSegments.add(9);
            anchorSegments.add(4); // duplicate of SUM state; root catalogue must deduplicate it
            final LiveViewCheckpointPageRef fakeAnchorRoot = new LiveViewCheckpointPageRef().of(77, 24, 64);
            final LiveViewCheckpointPageRef checkpoint1 = new LiveViewCheckpointPageRef();
            final LongList referenced1 = new LongList();
            try (LiveViewCheckpointRootBuilder builder = new LiveViewCheckpointRootBuilder(configuration);
                 Path dir = new Path()) {
                builder.begin(checkpointsDir(dir), 7, 123_456, 42, fakeAnchorRoot, anchorSegments);
                builder.addFunction(sumRoot);
                builder.addFunction(avgRoot);
                builder.getReferencedSegmentIds(referenced1);
                builder.build(30, checkpoint1);
            }
            assertLongList(referenced1, 1, 2, 4, 9);

            final LiveViewCheckpointPageRef avgDirectoryRef = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef sumDirectoryRef = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                 LiveViewCheckpointFunctionDirectory directory = new LiveViewCheckpointFunctionDirectory(configuration);
                 Path dir = new Path()) {
                root.of(checkpointsDir(dir), checkpoint1);
                Assert.assertEquals(7, root.getCheckpointId());
                Assert.assertEquals(123_456, root.getMaxTimestamp());
                Assert.assertEquals(42, root.getDefinitionTxn());
                Assert.assertEquals(4, root.getSegmentIdCount());
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
                builder.begin(checkpointsDir(dir), 8, 223_456, 42, fakeAnchorRoot, anchorSegments);
                builder.addFunction(avgRoot);
                builder.addFunction(sumRoot);
                builder.getReferencedSegmentIds(referenced2);
                builder.build(31, checkpoint2);
            }
            assertLongList(referenced2, 1, 2, 4, 9);
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

            try (LiveViewCheckpointSegmentDirectory segments = new LiveViewCheckpointSegmentDirectory(configuration)) {
                for (int i = 0; i < referenced1.size(); i++) {
                    segments.addSegment(referenced1.getQuick(i), 100 + i, 1);
                }
                segments.applyRootReferenceChanges(referenced1, referenced2, 2);
                for (int i = 0; i < referenced1.size(); i++) {
                    Assert.assertEquals(1, segments.getReferenceCount(referenced1.getQuick(i)));
                }
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
                oldFunction.of(checkpointsDir(dir), oldRoot);
                Assert.assertEquals(2, oldFunction.getSegmentUseCountSize());
                Assert.assertEquals(1, oldFunction.getSegmentId(0));
                Assert.assertEquals(2, oldFunction.getSegmentUseCount(0));
                Assert.assertEquals(2, oldFunction.getSegmentId(1));
                Assert.assertEquals(2, oldFunction.getSegmentUseCount(1));

                newFunction.of(checkpointsDir(dir), newRoot);
                Assert.assertEquals(2, newFunction.getSegmentUseCountSize());
                Assert.assertEquals(2, newFunction.getSegmentId(0));
                Assert.assertEquals(1, newFunction.getSegmentUseCount(0));
                Assert.assertEquals(3, newFunction.getSegmentId(1));
                Assert.assertEquals(1, newFunction.getSegmentUseCount(1));

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

    private static byte[] key(int key) {
        return new byte[]{(byte) key};
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

    private static LiveViewCheckpointStatePageRef stateRef(long segmentId, long offset) {
        return new LiveViewCheckpointStatePageRef().of(segmentId, offset, 8, 8, 0x31, 0, 1, 0);
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
