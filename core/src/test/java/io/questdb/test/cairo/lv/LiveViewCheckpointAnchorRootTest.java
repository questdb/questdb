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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointAnchorRoot;
import io.questdb.cairo.lv.LiveViewCheckpointAnchorRootBuilder;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapEntry;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class LiveViewCheckpointAnchorRootTest extends AbstractCairoTest {

    private static final int ANCHOR_VALUE_TYPE = ColumnType.TIMESTAMP_MICRO;
    private static final byte[] EMPTY_KEY_SCHEMA = keySchema();
    private static final String LV_DIR = "lv_anchor_root";
    private static final byte[] SYMBOL_KEY_SCHEMA = keySchema(ColumnType.STRING);
    private static final byte[] WINDOW_NAME = "w".getBytes(StandardCharsets.UTF_8);

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            checkpointsDir(path).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
            configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testAdjacentAnchorRootsShareEntriesWhoseAnchorDidNotMove() throws Exception {
        assertMemoryLeak(() -> {
            final int keyCount = 300;
            final LiveViewCheckpointPageRef oldRoot = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointAnchorRootBuilder builder = new LiveViewCheckpointAnchorRootBuilder(configuration);
                 Path dir = new Path()) {
                builder.of(checkpointsDir(dir), new LiveViewCheckpointPageRef(), WINDOW_NAME, ANCHOR_VALUE_TYPE, SYMBOL_KEY_SCHEMA);
                for (int i = 0; i < keyCount; i++) {
                    builder.putPartition(key(i), 1_000 + i);
                }
                builder.build(1, oldRoot);
            }

            final LiveViewCheckpointPageRef oldMapRoot = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef[] oldChildren;
            try (LiveViewCheckpointAnchorRoot root = new LiveViewCheckpointAnchorRoot(configuration);
                 LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                root.of(checkpointsDir(dir), oldRoot);
                Assert.assertEquals(ANCHOR_VALUE_TYPE, root.getAnchorValueType());
                Assert.assertArrayEquals(WINDOW_NAME, root.getWindowName());
                Assert.assertArrayEquals(SYMBOL_KEY_SCHEMA, root.getKeySchema());
                root.getPartitionMapRootRef(oldMapRoot);
                reader.of(checkpointsDir(dir));
                Assert.assertEquals(keyCount, reader.size(oldMapRoot));
                final int childCount = reader.rootChildCount(oldMapRoot);
                Assert.assertTrue("the map must be deep enough for sharing to be observable", childCount > 1);
                oldChildren = new LiveViewCheckpointPageRef[childCount];
                for (int i = 0; i < childCount; i++) {
                    reader.rootChildRef(oldMapRoot, i, oldChildren[i] = new LiveViewCheckpointPageRef());
                }
            }

            // The next cadence event re-puts every live key, but only one anchor
            // value moved, so only that key's leaf path may be copied.
            final LiveViewCheckpointPageRef newRoot = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointAnchorRootBuilder builder = new LiveViewCheckpointAnchorRootBuilder(configuration);
                 Path dir = new Path()) {
                builder.of(checkpointsDir(dir), oldRoot, WINDOW_NAME, ANCHOR_VALUE_TYPE, SYMBOL_KEY_SCHEMA);
                for (int i = 0; i < keyCount; i++) {
                    builder.putPartition(key(i), i == 0 ? 999_999 : 1_000 + i);
                }
                builder.build(2, newRoot);
            }

            try (LiveViewCheckpointAnchorRoot root = new LiveViewCheckpointAnchorRoot(configuration);
                 LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                final LiveViewCheckpointPageRef newMapRoot = new LiveViewCheckpointPageRef();
                root.of(checkpointsDir(dir), newRoot);
                root.getPartitionMapRootRef(newMapRoot);
                reader.of(checkpointsDir(dir));
                Assert.assertEquals(keyCount, reader.size(newMapRoot));

                final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
                Assert.assertTrue(reader.find(oldMapRoot, key(0), entry));
                Assert.assertEquals(1_000, LiveViewCheckpointAnchorRoot.readAnchorValue(entry));
                Assert.assertTrue(reader.find(newMapRoot, key(0), entry));
                Assert.assertEquals(999_999, LiveViewCheckpointAnchorRoot.readAnchorValue(entry));

                Assert.assertEquals(oldChildren.length, reader.rootChildCount(newMapRoot));
                final LiveViewCheckpointPageRef child = new LiveViewCheckpointPageRef();
                int sharedChildren = 0;
                for (int i = 0; i < oldChildren.length; i++) {
                    reader.rootChildRef(newMapRoot, i, child);
                    if (oldChildren[i].getSegmentId() == child.getSegmentId()
                            && oldChildren[i].getOffset() == child.getOffset()
                            && oldChildren[i].getLength() == child.getLength()) {
                        sharedChildren++;
                    }
                }
                Assert.assertEquals(
                        "only the subtree holding the moved anchor may be rewritten",
                        oldChildren.length - 1,
                        sharedChildren
                );
            }
        });
    }

    @Test
    public void testAnchorEntryShapeIsValidatedBeforeTheValueIsRead() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
            entry.of(key(1), new byte[]{1, 2, 3}, new LiveViewCheckpointStatePageRef[0]);
            assertInvalid(
                    () -> LiveViewCheckpointAnchorRoot.readAnchorValue(entry),
                    "anchor entry scalar state length invalid"
            );

            entry.of(
                    key(1),
                    new byte[Long.BYTES],
                    new LiveViewCheckpointStatePageRef[]{new LiveViewCheckpointStatePageRef().of(1, 0, 8, 8, 0x41, 0, 1, 0)}
            );
            assertInvalid(
                    () -> LiveViewCheckpointAnchorRoot.readAnchorValue(entry),
                    "anchor entry must not reference a state page"
            );
        });
    }

    @Test
    public void testAnchorValueRoundTripsEveryBoundaryBitPattern() throws Exception {
        assertMemoryLeak(() -> {
            final long[] values = {0, 1, -1, Long.MIN_VALUE, Long.MAX_VALUE, Numbers.LONG_NULL};
            final LiveViewCheckpointPageRef rootRef = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointAnchorRootBuilder builder = new LiveViewCheckpointAnchorRootBuilder(configuration);
                 Path dir = new Path()) {
                builder.of(checkpointsDir(dir), new LiveViewCheckpointPageRef(), WINDOW_NAME, ColumnType.LONG, EMPTY_KEY_SCHEMA);
                for (int i = 0; i < values.length; i++) {
                    builder.putPartition(key(i), values[i]);
                }
                builder.build(20, rootRef);
            }
            try (LiveViewCheckpointAnchorRoot root = new LiveViewCheckpointAnchorRoot(configuration);
                 LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                root.of(checkpointsDir(dir), rootRef);
                final LiveViewCheckpointPageRef mapRoot = new LiveViewCheckpointPageRef();
                root.getPartitionMapRootRef(mapRoot);
                reader.of(checkpointsDir(dir));
                final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
                for (int i = 0; i < values.length; i++) {
                    Assert.assertTrue(reader.find(mapRoot, key(i), entry));
                    Assert.assertEquals(values[i], LiveViewCheckpointAnchorRoot.readAnchorValue(entry));
                }
            }
        });
    }

    @Test
    public void testEmptyAnchorMapPublishesRootWithoutMapPages() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef rootRef = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointAnchorRootBuilder builder = new LiveViewCheckpointAnchorRootBuilder(configuration);
                 Path dir = new Path()) {
                builder.of(checkpointsDir(dir), new LiveViewCheckpointPageRef(), WINDOW_NAME, ANCHOR_VALUE_TYPE, EMPTY_KEY_SCHEMA);
                builder.build(50, rootRef);
            }
            try (LiveViewCheckpointAnchorRoot root = new LiveViewCheckpointAnchorRoot(configuration);
                 LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                root.of(checkpointsDir(dir), rootRef);
                final LiveViewCheckpointPageRef mapRoot = new LiveViewCheckpointPageRef();
                root.getPartitionMapRootRef(mapRoot);
                Assert.assertTrue(mapRoot.isNull());
                reader.of(checkpointsDir(dir));
                Assert.assertEquals(0, reader.size(mapRoot));
            }
        });
    }

    @Test
    public void testFreezeDropsEntriesTheRuntimeNoLongerHoldsAndOldRootSurvives() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef oldRoot = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointAnchorRootBuilder builder = new LiveViewCheckpointAnchorRootBuilder(configuration);
                 Path dir = new Path()) {
                builder.of(checkpointsDir(dir), new LiveViewCheckpointPageRef(), WINDOW_NAME, ANCHOR_VALUE_TYPE, SYMBOL_KEY_SCHEMA);
                builder.putPartition(key(1), 11);
                builder.putPartition(key(2), 22);
                builder.putPartition(key(3), 33);
                builder.build(10, oldRoot);
            }

            // A frontier sweep evicted key 2 from the runtime map, so the next
            // freeze simply does not put it.
            final LiveViewCheckpointPageRef newRoot = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointAnchorRootBuilder builder = new LiveViewCheckpointAnchorRootBuilder(configuration);
                 Path dir = new Path()) {
                builder.of(checkpointsDir(dir), oldRoot, WINDOW_NAME, ANCHOR_VALUE_TYPE, SYMBOL_KEY_SCHEMA);
                builder.putPartition(key(1), 11);
                builder.putPartition(key(3), 44);
                builder.build(11, newRoot);
            }

            try (LiveViewCheckpointAnchorRoot root = new LiveViewCheckpointAnchorRoot(configuration);
                 LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir));
                final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
                final LiveViewCheckpointPageRef mapRoot = new LiveViewCheckpointPageRef();

                root.of(checkpointsDir(dir), oldRoot);
                root.getPartitionMapRootRef(mapRoot);
                Assert.assertEquals(3, reader.size(mapRoot));
                Assert.assertTrue(reader.find(mapRoot, key(2), entry));
                Assert.assertEquals(22, LiveViewCheckpointAnchorRoot.readAnchorValue(entry));
                Assert.assertTrue(reader.find(mapRoot, key(3), entry));
                Assert.assertEquals(33, LiveViewCheckpointAnchorRoot.readAnchorValue(entry));

                root.of(checkpointsDir(dir), newRoot);
                root.getPartitionMapRootRef(mapRoot);
                Assert.assertEquals(2, reader.size(mapRoot));
                Assert.assertFalse(reader.find(mapRoot, key(2), entry));
                Assert.assertTrue(reader.find(mapRoot, key(3), entry));
                Assert.assertEquals(44, LiveViewCheckpointAnchorRoot.readAnchorValue(entry));
            }
        });
    }

    @Test
    public void testIdentityMismatchAgainstTheOldRootIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef oldRoot = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointAnchorRootBuilder builder = new LiveViewCheckpointAnchorRootBuilder(configuration);
                 Path dir = new Path()) {
                builder.of(checkpointsDir(dir), new LiveViewCheckpointPageRef(), WINDOW_NAME, ANCHOR_VALUE_TYPE, SYMBOL_KEY_SCHEMA);
                builder.putPartition(key(1), 11);
                builder.build(30, oldRoot);
            }

            assertOldRootRejected(oldRoot, "other".getBytes(StandardCharsets.UTF_8), ANCHOR_VALUE_TYPE, SYMBOL_KEY_SCHEMA);
            assertOldRootRejected(oldRoot, WINDOW_NAME, ColumnType.LONG, SYMBOL_KEY_SCHEMA);
            assertOldRootRejected(oldRoot, WINDOW_NAME, ANCHOR_VALUE_TYPE, keySchema(ColumnType.INT));

            try (LiveViewCheckpointAnchorRootBuilder builder = new LiveViewCheckpointAnchorRootBuilder(configuration);
                 Path dir = new Path()) {
                try {
                    builder.of(checkpointsDir(dir), new LiveViewCheckpointPageRef(), new byte[0], ANCHOR_VALUE_TYPE, SYMBOL_KEY_SCHEMA);
                    Assert.fail("expected an empty window name rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "anchor window name or key schema invalid");
                }
            }
        });
    }

    @Test
    public void testMalformedAnchorRootPageIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef badVersion = writeRawRoot(60, mem -> {
                mem.putInt(7); // format version
                mem.putInt(ANCHOR_VALUE_TYPE);
                mem.putInt(WINDOW_NAME.length);
                mem.putInt(SYMBOL_KEY_SCHEMA.length);
                putNullMetaRef(mem);
                putBytes(mem, WINDOW_NAME);
                putBytes(mem, SYMBOL_KEY_SCHEMA);
            });
            final LiveViewCheckpointPageRef truncatedPayload = writeRawRoot(61, mem -> {
                mem.putInt(1);
                mem.putInt(ANCHOR_VALUE_TYPE);
                mem.putInt(WINDOW_NAME.length + 1); // one byte more than the page carries
                mem.putInt(SYMBOL_KEY_SCHEMA.length);
                putNullMetaRef(mem);
                putBytes(mem, WINDOW_NAME);
                putBytes(mem, SYMBOL_KEY_SCHEMA);
            });
            final LiveViewCheckpointPageRef emptyWindowName = writeRawRoot(62, mem -> {
                mem.putInt(1);
                mem.putInt(ANCHOR_VALUE_TYPE);
                mem.putInt(0);
                mem.putInt(SYMBOL_KEY_SCHEMA.length);
                putNullMetaRef(mem);
                putBytes(mem, SYMBOL_KEY_SCHEMA);
            });

            try (LiveViewCheckpointAnchorRoot root = new LiveViewCheckpointAnchorRoot(configuration);
                 Path dir = new Path()) {
                assertInvalid(() -> root.of(checkpointsDir(dir), badVersion), "anchor root format version mismatch");
                assertInvalid(() -> root.of(checkpointsDir(dir), truncatedPayload), "anchor root payload length mismatch");
                assertInvalid(() -> root.of(checkpointsDir(dir), emptyWindowName), "anchor root window name or key schema invalid");
            }
        });
    }

    private static void assertInvalid(ThrowingRunnable runnable, CharSequence message) {
        try {
            runnable.run();
            Assert.fail("expected corrupt anchor root rejection");
        } catch (CairoException e) {
            Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }

    private static void assertOldRootRejected(
            LiveViewCheckpointPageRef oldRoot,
            byte[] windowName,
            int anchorValueType,
            byte[] keySchema
    ) {
        try (LiveViewCheckpointAnchorRootBuilder builder = new LiveViewCheckpointAnchorRootBuilder(configuration);
             Path dir = new Path()) {
            builder.of(checkpointsDir(dir), oldRoot, windowName, anchorValueType, keySchema);
            Assert.fail("expected an anchor root identity rejection");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "anchor root identity or schema mismatch");
        }
    }

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private static byte[] key(int key) {
        return new byte[]{(byte) (key >>> 8), (byte) key};
    }

    private static byte[] keySchema(int... columnTypes) {
        final byte[] schema = new byte[Integer.BYTES + columnTypes.length * Integer.BYTES];
        putInt(schema, 0, columnTypes.length);
        for (int i = 0; i < columnTypes.length; i++) {
            putInt(schema, Integer.BYTES + i * Integer.BYTES, columnTypes[i]);
        }
        return schema;
    }

    private static void putBytes(MemoryA mem, byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            mem.putByte(bytes[i]);
        }
    }

    private static void putInt(byte[] target, int offset, int value) {
        // ByteBuffer.putInt order, which is what LiveViewCheckpointMetadata encodes with.
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
    }

    private static void putNullMetaRef(MemoryA mem) {
        mem.putLong(-1);
        mem.putLong(0);
        mem.putInt(0);
    }

    private static LiveViewCheckpointPageRef writeRawRoot(long segmentId, PageWriter pageWriter) {
        final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointMetaSegmentWriter writer = new LiveViewCheckpointMetaSegmentWriter(configuration);
             Path dir = new Path()) {
            writer.of(checkpointsDir(dir), segmentId);
            pageWriter.write(writer.beginPage(LiveViewCheckpointAnchorRoot.PAGE_KIND));
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
