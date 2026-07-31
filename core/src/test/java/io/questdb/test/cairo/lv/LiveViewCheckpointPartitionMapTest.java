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
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMap;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapEntry;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapWriter;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;

public class LiveViewCheckpointPartitionMapTest extends AbstractCairoTest {

    private static final String LV_DIR = "lv_partition_map";

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            checkpointsDir(path).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
            configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testBatchCopyOnWriteSharesUntouchedSubtreesAndOldRootSurvives() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef oldRoot = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPartitionMapWriter.Mutation[] initial = new LiveViewCheckpointPartitionMapWriter.Mutation[64];
            for (int i = 0; i < initial.length; i++) {
                initial[i] = put(i, i, i % 5);
            }
            try (LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 4, 4);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                writer.apply(new LiveViewCheckpointPageRef(), initial, initial.length, 1, oldRoot);
            }

            final LiveViewCheckpointPageRef[] oldChildren;
            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir));
                Assert.assertEquals(64, reader.size(oldRoot));
                final int childCount = reader.rootChildCount(oldRoot);
                Assert.assertTrue(childCount > 1);
                oldChildren = new LiveViewCheckpointPageRef[childCount];
                for (int i = 0; i < childCount; i++) {
                    reader.rootChildRef(oldRoot, i, oldChildren[i] = new LiveViewCheckpointPageRef());
                }
            }

            final LiveViewCheckpointPageRef newRoot = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 4, 4);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                final LiveViewCheckpointPartitionMapWriter.Mutation[] update = {put(0, 999, 9)};
                writer.apply(oldRoot, update, 1, 2, newRoot);
                Assert.assertTrue(writer.getLastSegmentPageCount() < 8);
            }

            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir));
                final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
                Assert.assertTrue(reader.find(oldRoot, key(0), entry));
                Assert.assertEquals(0, scalar(entry));
                Assert.assertEquals(0, entry.getStatePageRef(0).getSegmentId());
                Assert.assertTrue(reader.find(newRoot, key(0), entry));
                Assert.assertEquals(999, scalar(entry));
                Assert.assertEquals(9, entry.getStatePageRef(0).getSegmentId());

                Assert.assertEquals(oldChildren.length, reader.rootChildCount(newRoot));
                final LiveViewCheckpointPageRef child = new LiveViewCheckpointPageRef();
                for (int i = 1; i < oldChildren.length; i++) {
                    reader.rootChildRef(newRoot, i, child);
                    assertRefEquals(oldChildren[i], child);
                }
            }
        });
    }

    @Test
    public void testDeepCorruptionIsValidatedLazilyOnSelectedPath() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPartitionMapWriter.Mutation[] initial = new LiveViewCheckpointPartitionMapWriter.Mutation[64];
            for (int i = 0; i < initial.length; i++) {
                initial[i] = put(i, i, 1);
            }
            final LiveViewCheckpointPageRef firstChild = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 4, 4);
                 LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                writer.apply(new LiveViewCheckpointPageRef(), initial, initial.length, 10, root);
                reader.of(checkpointsDir(dir));
                Assert.assertTrue(reader.rootChildCount(root) > 1);
                reader.rootChildRef(root, 0, firstChild);
            }
            corruptPageChecksum(firstChild);

            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir));
                final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
                Assert.assertTrue(reader.find(root, key(63), entry));
                Assert.assertEquals(63, scalar(entry));
                try {
                    reader.find(root, key(0), entry);
                    Assert.fail("expected selected corrupt path to fail");
                } catch (CairoException e) {
                    Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                    TestUtils.assertContains(e.getFlyweightMessage(), "checksum mismatch");
                }
            }
        });
    }

    @Test
    public void testRandomBatchPropertyAgainstTreeMap() throws Exception {
        assertMemoryLeak(() -> {
            final TreeMap<Integer, Integer> expected = new TreeMap<>();
            final Rnd rnd = new Rnd();
            LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 5, 5);
                 LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                reader.of(checkpointsDir(dir));
                for (int generation = 1; generation <= 150; generation++) {
                    final boolean[] used = new boolean[80];
                    final int count = 1 + rnd.nextPositiveInt() % 6;
                    final LiveViewCheckpointPartitionMapWriter.Mutation[] mutations = new LiveViewCheckpointPartitionMapWriter.Mutation[count];
                    for (int i = 0; i < count; i++) {
                        int key;
                        do {
                            key = rnd.nextPositiveInt() % used.length;
                        } while (used[key]);
                        used[key] = true;
                        if ((rnd.nextInt() & 3) == 0) {
                            mutations[i] = new LiveViewCheckpointPartitionMapWriter.Mutation().remove(key(key));
                            expected.remove(key);
                        } else {
                            final int value = rnd.nextInt();
                            mutations[i] = put(key, value, key % 7);
                            expected.put(key, value);
                        }
                    }
                    final LiveViewCheckpointPageRef next = new LiveViewCheckpointPageRef();
                    writer.apply(root, mutations, count, 100 + generation, next);
                    root = copy(next);
                    Assert.assertEquals(expected.size(), reader.size(root));
                    final java.util.Iterator<Map.Entry<Integer, Integer>> iterator = expected.entrySet().iterator();
                    reader.iterateAll(root, entry -> {
                        Assert.assertTrue(iterator.hasNext());
                        final Map.Entry<Integer, Integer> expectedEntry = iterator.next();
                        Assert.assertEquals((int) expectedEntry.getKey(), intKey(entry.getKey()));
                        Assert.assertEquals((int) expectedEntry.getValue(), scalar(entry));
                    });
                    Assert.assertFalse(iterator.hasNext());
                }
            }
        });
    }

    @Test
    public void testRebindingDropsPagesOfAReMintedSegment() throws Exception {
        assertMemoryLeak(() -> {
            // The reader memoises the pages a lookup decoded, keyed on the segment and
            // offset they came from. A rebuilt timeline may mint a segment id a reader
            // already read, so detaching - which is what drops the mappings a retire,
            // repair or compaction is about to delete - has to drop the memo with them.
            final LiveViewCheckpointPageRef first = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                final LiveViewCheckpointPartitionMapWriter.Mutation[] initial = {put(1, 11, 0)};
                writer.apply(new LiveViewCheckpointPageRef(), initial, 1, 7, first);
            }

            final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir));
                Assert.assertTrue(reader.find(first, key(1), entry));
                Assert.assertEquals(11, scalar(entry));

                // Unmap before the id is re-minted: a published segment is immutable,
                // so the replacement arrives by rename, which a live mapping of the
                // name it replaces would block on Windows.
                reader.detach();

                // Production re-mints an id only after the retire, repair or
                // compaction that deleted the segment holding it, and the writer
                // refuses to publish over a name that still exists. Delete it here
                // for the same reason, so this rebuild is the one production
                // performs rather than a rename onto a live file - which POSIX
                // silently allows and Windows MoveFileW rejects outright.
                try (Path segment = new Path()) {
                    configuration.getFilesFacade().removeQuiet(
                            LiveViewCheckpointLayout.metaSegmentPath(segment, checkpointsDir(dir), 7).$()
                    );
                }

                final LiveViewCheckpointPageRef second = new LiveViewCheckpointPageRef();
                try (LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration)) {
                    writer.of(checkpointsDir(dir));
                    final LiveViewCheckpointPartitionMapWriter.Mutation[] replacement = {put(1, 22, 0)};
                    writer.apply(new LiveViewCheckpointPageRef(), replacement, 1, 7, second);
                }
                // Guards the guard: the replacement has to land where the memo keyed
                // the page it replaces, or a miss would hide a memo that never dropped.
                assertRefEquals(first, second);

                Assert.assertTrue(reader.find(second, key(1), entry));
                Assert.assertEquals(22, scalar(entry));
            }
        });
    }

    @Test
    public void testRepeatedLookupsDoNotOutliveTheirRoot() throws Exception {
        assertMemoryLeak(() -> {
            // A seal looks one root up once per partition, so the reader memoises the
            // pages a descent decoded rather than re-checksumming and re-decoding them
            // per lookup. Each lookup must still answer out of the page it asked for.
            final int keyCount = 64;
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPartitionMapWriter.Mutation[] initial =
                    new LiveViewCheckpointPartitionMapWriter.Mutation[keyCount];
            for (int i = 0; i < keyCount; i++) {
                initial[i] = put(i, i, i % 5);
            }
            // Narrow nodes, so one descent touches more pages than the memo holds and a
            // lookup that leaves the path evicts what it replaces.
            try (LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 2, 2);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                writer.apply(new LiveViewCheckpointPageRef(), initial, initial.length, 1, root);
            }

            final LiveViewCheckpointPageRef nextRoot = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 2, 2);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                final LiveViewCheckpointPartitionMapWriter.Mutation[] update = {put(0, 999, 9)};
                writer.apply(root, update, 1, 2, nextRoot);
            }

            final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir));
                for (int pass = 0; pass < 3; pass++) {
                    for (int i = 0; i < keyCount; i++) {
                        Assert.assertTrue(reader.find(root, key(i), entry));
                        Assert.assertEquals(i, scalar(entry));
                        Assert.assertEquals(i % 5, entry.getStatePageRef(0).getSegmentId());
                    }
                    Assert.assertFalse(reader.find(root, key(keyCount), entry));
                }
                // Two roots share every page the update left untouched, so a lookup
                // must answer with the root it names rather than with the pages the
                // lookup before it decoded.
                for (int pass = 0; pass < 3; pass++) {
                    Assert.assertTrue(reader.find(nextRoot, key(0), entry));
                    Assert.assertEquals(999, scalar(entry));
                    Assert.assertTrue(reader.find(root, key(0), entry));
                    Assert.assertEquals(0, scalar(entry));
                    Assert.assertTrue(reader.find(nextRoot, key(63), entry));
                    Assert.assertEquals(63, scalar(entry));
                }
            }
        });
    }

    @Test
    public void testStructurallyCorruptPagesRejected() throws Exception {
        assertMemoryLeak(() -> {
            assertRawPageRejected(300, LiveViewCheckpointPartitionMap.PAGE_KIND_LEAF, mem -> {
                mem.putInt(1);
                mem.putInt(1);
                mem.putInt(1);
                mem.putInt(0);
                mem.putInt(0);
            }, "truncated");
            assertRawPageRejected(301, LiveViewCheckpointPartitionMap.PAGE_KIND_LEAF, mem -> {
                mem.putInt(1);
                mem.putInt(2);
                putLeafEntry(mem, (byte) 2);
                putLeafEntry(mem, (byte) 1);
            }, "not strictly increasing");
            assertRawPageRejected(302, LiveViewCheckpointPartitionMap.PAGE_KIND_LEAF, mem -> {
                mem.putInt(1);
                mem.putInt(1);
                mem.putInt(1);
                mem.putInt(0);
                mem.putInt(1);
                mem.putByte((byte) 1);
                new LiveViewCheckpointStatePageRef().clear().writeTo(mem);
            }, "state page reference invalid");
            assertRawPageRejected(303, LiveViewCheckpointPartitionMap.PAGE_KIND_INTERNAL, mem -> {
                mem.putInt(1);
                mem.putInt(1);
                mem.putInt(1);
                mem.putByte((byte) 1);
                mem.putLong(-1);
                mem.putLong(0);
                mem.putInt(0);
            }, "metadata page reference invalid");
            assertRawPageRejected(304, LiveViewCheckpointPartitionMap.PAGE_KIND_LEAF, mem -> {
                mem.putInt(1);
                mem.putInt(1000);
            }, "count exceeds payload");
        });
    }

    private static void assertRefEquals(LiveViewCheckpointPageRef expected, LiveViewCheckpointPageRef actual) {
        Assert.assertEquals(expected.getSegmentId(), actual.getSegmentId());
        Assert.assertEquals(expected.getOffset(), actual.getOffset());
        Assert.assertEquals(expected.getLength(), actual.getLength());
    }

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private static LiveViewCheckpointPageRef copy(LiveViewCheckpointPageRef ref) {
        return new LiveViewCheckpointPageRef().of(ref.getSegmentId(), ref.getOffset(), ref.getLength());
    }

    private static int intKey(byte[] key) {
        return (key[0] & 0xff) << 24 | (key[1] & 0xff) << 16 | (key[2] & 0xff) << 8 | key[3] & 0xff;
    }

    private static byte[] key(int value) {
        return new byte[]{(byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value};
    }

    private static LiveViewCheckpointPartitionMapWriter.Mutation put(int key, int value, long segmentId) {
        final byte[] scalar = new byte[]{(byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value};
        final LiveViewCheckpointStatePageRef ref = new LiveViewCheckpointStatePageRef().of(
                segmentId, key * 8L, 8, 8, 0x31, 0, 1, 0
        );
        return new LiveViewCheckpointPartitionMapWriter.Mutation().put(key(key), scalar, new LiveViewCheckpointStatePageRef[]{ref});
    }

    private static void putLeafEntry(MemoryA mem, byte key) {
        mem.putInt(1);
        mem.putInt(0);
        mem.putInt(0);
        mem.putByte(key);
    }

    private static int scalar(LiveViewCheckpointPartitionMapEntry entry) {
        final byte[] value = entry.getScalarState();
        return (value[0] & 0xff) << 24 | (value[1] & 0xff) << 16 | (value[2] & 0xff) << 8 | value[3] & 0xff;
    }

    private void assertRawPageRejected(long segmentId, int pageKind, PageWriter pageWriter, CharSequence message) {
        final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointMetaSegmentWriter writer = new LiveViewCheckpointMetaSegmentWriter(configuration);
             Path dir = new Path()) {
            writer.of(checkpointsDir(dir), segmentId);
            pageWriter.write(writer.beginPage(pageKind));
            writer.endPage(root);
            writer.commit();
        }
        try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
             Path dir = new Path()) {
            reader.of(checkpointsDir(dir));
            try {
                reader.find(root, key(0), new LiveViewCheckpointPartitionMapEntry());
                Assert.fail("expected corrupt partition map rejection");
            } catch (CairoException e) {
                Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }
        }
    }

    private void corruptPageChecksum(LiveViewCheckpointPageRef ref) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path(); Path dir = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
            LiveViewCheckpointLayout.metaSegmentPath(path, checkpointsDir(dir), ref.getSegmentId());
            mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
            final long crcOffset = ref.getOffset() + LiveViewCheckpointLayout.PAGE_CRC_OFFSET;
            mem.putInt(crcOffset, mem.getInt(crcOffset) ^ 1);
        }
    }

    @FunctionalInterface
    private interface PageWriter {
        void write(MemoryA mem);
    }
}
