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
import io.questdb.cairo.lv.LiveViewCheckpointMutationArena;
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
    public void testObjectPoolReusedAcrossWriterLifetimesAndTreeShapeChanges() throws Exception {
        assertMemoryLeak(() -> {
            final int keyCount = 1_000;
            LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            int segmentId = 1;
            try (LiveViewCheckpointMutationArena initial = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter poolOwner =
                         new LiveViewCheckpointPartitionMapWriter(configuration, 4, 4);
                 Path dir = new Path()) {
                poolOwner.of(checkpointsDir(dir));
                for (int i = 0; i < keyCount; i++) {
                    put(initial, i, i, i % 5);
                }
                poolOwner.apply(root, initial, segmentId++, root);
                final int poolIdentity = poolOwner.getObjectPoolIdentityForTest();
                int warmedObjectCount = -1;

                for (int cycle = 0; cycle < 3; cycle++) {
                    final LiveViewCheckpointPageRef collapsedRoot = new LiveViewCheckpointPageRef();
                    try (LiveViewCheckpointMutationArena shrink = new LiveViewCheckpointMutationArena();
                         LiveViewCheckpointPartitionMapWriter writer =
                                 new LiveViewCheckpointPartitionMapWriter(configuration, 4, 4, poolOwner)) {
                        writer.of(checkpointsDir(dir));
                        for (int i = keyCount - 1; i > 0; i--) {
                            shrink.remove(key(i));
                        }
                        writer.apply(root, shrink, segmentId++, collapsedRoot);
                        Assert.assertEquals(poolIdentity, writer.getObjectPoolIdentityForTest());
                    }
                    root = copy(collapsedRoot);

                    final LiveViewCheckpointPageRef expandedRoot = new LiveViewCheckpointPageRef();
                    try (LiveViewCheckpointMutationArena expand = new LiveViewCheckpointMutationArena();
                         LiveViewCheckpointPartitionMapWriter writer =
                                 new LiveViewCheckpointPartitionMapWriter(configuration, 4, 4, poolOwner)) {
                        writer.of(checkpointsDir(dir));
                        for (int i = keyCount - 1; i >= 0; i--) {
                            put(expand, i, cycle * keyCount + i, i % 5);
                        }
                        writer.apply(root, expand, segmentId++, expandedRoot);
                        Assert.assertEquals(poolIdentity, writer.getObjectPoolIdentityForTest());
                    }
                    root = copy(expandedRoot);

                    final int retainedObjectCount = poolOwner.getRetainedObjectCountForTest();
                    Assert.assertTrue(retainedObjectCount > 0);
                    if (cycle == 0) {
                        warmedObjectCount = retainedObjectCount;
                    } else {
                        Assert.assertEquals(
                                "measurement publications must reuse the warmed node/ref high-water mark",
                                warmedObjectCount,
                                retainedObjectCount
                        );
                    }
                }

                try (LiveViewCheckpointPartitionMapReader reader =
                             new LiveViewCheckpointPartitionMapReader(configuration)) {
                    reader.of(checkpointsDir(dir));
                    Assert.assertEquals(keyCount, reader.size(root));
                    final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
                    Assert.assertTrue(reader.find(root, key(keyCount - 1), entry));
                    Assert.assertEquals(2 * keyCount + keyCount - 1, scalar(entry));
                }
            }
        });
    }

    @Test
    public void testBatchCopyOnWriteSharesUntouchedSubtreesAndOldRootSurvives() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef oldRoot = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointMutationArena initial = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 4, 4);
                 Path dir = new Path()) {
                for (int i = 0; i < 64; i++) {
                    put(initial, i, i, i % 5);
                }
                writer.of(checkpointsDir(dir));
                writer.apply(new LiveViewCheckpointPageRef(), initial, 1, oldRoot);
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
            try (LiveViewCheckpointMutationArena update = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 4, 4);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                put(update, 0, 999, 9);
                writer.apply(oldRoot, update, 2, newRoot);
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
            final LiveViewCheckpointPageRef firstChild = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointMutationArena initial = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 4, 4);
                 LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                for (int i = 0; i < 64; i++) {
                    put(initial, i, i, 1);
                }
                writer.of(checkpointsDir(dir));
                writer.apply(new LiveViewCheckpointPageRef(), initial, 10, root);
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
    public void testDeepDescentsAreMemoisedAtEveryLevel() throws Exception {
        assertMemoryLeak(() -> {
            // The memo has to hold a whole descent whatever the tree's depth. A
            // fixed-slot memo shallower than the tree evicts its own prefix mid
            // descent, which drops the hit rate to zero the moment a growing map
            // pushes the root down one more level - and shows up nowhere except in
            // seal duration. That is the shape of the ~2.4-million-partition cliff
            // that took the incremental checkpoint from 0.3s back to 4.5s.
            final int keyCount = 1024;
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            // Narrow nodes stand in for a large map: the depth is what matters. Four
            // rather than the minimum two, so a split hands each side more than one
            // child and the tree stays the shape a production capacity produces.
            try (LiveViewCheckpointMutationArena initial = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 4, 4);
                 Path dir = new Path()) {
                for (int i = 0; i < keyCount; i++) {
                    put(initial, i, i, i % 5);
                }
                writer.of(checkpointsDir(dir));
                writer.apply(new LiveViewCheckpointPageRef(), initial, 1, root);
            }

            final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir));
                Assert.assertTrue(reader.find(root, key(0), entry));
                final long depth = reader.getDecodedPageCount();
                Assert.assertTrue(
                        "the tree has to be deeper than the memo this guards, depth=" + depth,
                        depth > 4
                );

                // Repeating one lookup must decode nothing at all.
                for (int pass = 0; pass < 8; pass++) {
                    Assert.assertTrue(reader.find(root, key(0), entry));
                    Assert.assertEquals(0, scalar(entry));
                }
                Assert.assertEquals(depth, reader.getDecodedPageCount());

                // A seal walks the keys it touched in the order it touched them, so
                // an ascending sweep is the shape that matters: each page is decoded
                // as the sweep reaches it, and once.
                final long beforeSweep = reader.getDecodedPageCount();
                for (int i = 0; i < keyCount; i++) {
                    Assert.assertTrue(reader.find(root, key(i), entry));
                    Assert.assertEquals(i, scalar(entry));
                }
                final long sweepDecodes = reader.getDecodedPageCount() - beforeSweep;
                Assert.assertTrue(
                        "an ascending sweep decoded " + sweepDecodes + " pages, a memoless descent per key costs "
                                + keyCount * depth,
                        sweepDecodes < keyCount * depth / 4
                );
            }
        });
    }

    @Test
    public void testDeepDescentsPastMemoLimitUseScratchNode() throws Exception {
        assertMemoryLeak(() -> {
            final int keyCount = 256;
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            // The minimum capacity makes an ascending build degenerate into a
            // chain deeper than the bounded memo.
            try (LiveViewCheckpointMutationArena initial = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 2, 2);
                 Path dir = new Path()) {
                for (int i = 0; i < keyCount; i++) {
                    put(initial, i, i, i % 5);
                }
                writer.of(checkpointsDir(dir));
                writer.apply(new LiveViewCheckpointPageRef(), initial, 1, root);
            }

            final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir));
                Assert.assertTrue(reader.find(root, key(0), entry));
                Assert.assertEquals(0, scalar(entry));
                final long depth = reader.getDecodedPageCount();
                Assert.assertTrue("the tree must exceed the 64-level memo, depth=" + depth, depth > 64);

                // The cached prefix stays resident, while the suffix must be decoded
                // through the scratch node on every descent.
                final long beforeRepeat = reader.getDecodedPageCount();
                Assert.assertTrue(reader.find(root, key(0), entry));
                Assert.assertEquals(0, scalar(entry));
                Assert.assertTrue(reader.getDecodedPageCount() > beforeRepeat);

                for (int i = 0; i < keyCount; i++) {
                    Assert.assertTrue(reader.find(root, key(i), entry));
                    Assert.assertEquals(i, scalar(entry));
                    Assert.assertEquals(i % 5, entry.getStatePageRef(0).getSegmentId());
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
                 LiveViewCheckpointMutationArena mutations = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                reader.of(checkpointsDir(dir));
                for (int generation = 1; generation <= 150; generation++) {
                    final boolean[] used = new boolean[80];
                    final int count = 1 + rnd.nextPositiveInt() % 6;
                    mutations.clear();
                    for (int i = 0; i < count; i++) {
                        int key;
                        do {
                            key = rnd.nextPositiveInt() % used.length;
                        } while (used[key]);
                        used[key] = true;
                        if ((rnd.nextInt() & 3) == 0) {
                            mutations.remove(key(key));
                            expected.remove(key);
                        } else {
                            final int value = rnd.nextInt();
                            put(mutations, key, value, key % 7);
                            expected.put(key, value);
                        }
                    }
                    final LiveViewCheckpointPageRef next = new LiveViewCheckpointPageRef();
                    writer.apply(root, mutations, 100 + generation, next);
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
            try (LiveViewCheckpointMutationArena initial = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                put(initial, 1, 11, 0);
                writer.apply(new LiveViewCheckpointPageRef(), initial, 7, first);
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
                try (LiveViewCheckpointMutationArena replacement = new LiveViewCheckpointMutationArena();
                     LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration)) {
                    writer.of(checkpointsDir(dir));
                    put(replacement, 1, 22, 0);
                    writer.apply(new LiveViewCheckpointPageRef(), replacement, 7, second);
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
            // Narrow nodes, so a descent is many levels deep and a lookup that leaves
            // the path replaces the memo entry of every level it diverges at.
            try (LiveViewCheckpointMutationArena initial = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 2, 2);
                 Path dir = new Path()) {
                for (int i = 0; i < keyCount; i++) {
                    put(initial, i, i, i % 5);
                }
                writer.of(checkpointsDir(dir));
                writer.apply(new LiveViewCheckpointPageRef(), initial, 1, root);
            }

            final LiveViewCheckpointPageRef nextRoot = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointMutationArena update = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 2, 2);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                put(update, 0, 999, 9);
                writer.apply(root, update, 2, nextRoot);
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

    @Test
    public void testExistingInteriorMutationsSearchEachLeafOnce() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointMutationArena initial = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                for (int i = 0; i < 64; i++) {
                    put(initial, i, i, 7);
                }
                writer.apply(new LiveViewCheckpointPageRef(), initial, 1, root);
            }

            final LiveViewCheckpointPageRef updatedRoot = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointMutationArena updates = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                for (int i = 0; i < 63; i++) {
                    put(updates, i, 1_000 + i, 8);
                }
                updates.resetLowerBoundCountForTest();
                writer.apply(root, updates, 2, updatedRoot);
                Assert.assertEquals(
                        "each existing interior mutation must search its leaf once",
                        63,
                        updates.getLowerBoundCountForTest()
                );
            }

            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir));
                final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
                for (int i = 0; i < 63; i++) {
                    Assert.assertTrue(reader.find(updatedRoot, key(i), entry));
                    Assert.assertEquals(1_000 + i, scalar(entry));
                }
                Assert.assertTrue(reader.find(updatedRoot, key(63), entry));
                Assert.assertEquals(63, scalar(entry));
            }
        });
    }

    @Test
    public void testFlyweightReusesManyExactKeyWidthsInConstantTime() {
        final int widthCount = 4_096;
        final byte[] emptyBytes = new byte[0];
        final LiveViewCheckpointStatePageRef[] emptyRefs = new LiveViewCheckpointStatePageRef[0];
        final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
        final byte[][] retained = new byte[widthCount][];
        for (int charCount = 1; charCount <= widthCount; charCount++) {
            final int width = Integer.BYTES + Character.BYTES * charCount;
            entry.of(new byte[width], emptyBytes, emptyRefs);
            retained[charCount - 1] = entry.getKey();
        }

        entry.clear();
        Assert.assertEquals(0, entry.getKey().length);
        Assert.assertEquals(0, entry.getScalarState().length);
        Assert.assertEquals(0, entry.getStatePageCount());
        entry.resetWidthLookupCountForTest();
        for (int charCount = 1; charCount <= widthCount; charCount++) {
            final int width = Integer.BYTES + Character.BYTES * charCount;
            entry.of(new byte[width], emptyBytes, emptyRefs);
            Assert.assertSame(retained[charCount - 1], entry.getKey());
        }
        Assert.assertEquals(
                "each supported STRING width must perform one direct lookup",
                widthCount,
                entry.getWidthLookupCountForTest()
        );
    }

    @Test
    public void testFlyweightReusesScalarAndPageRefExactWidths() {
        final int widthCount = 128;
        final byte[] key = new byte[]{1};
        final byte[][] retainedScalars = new byte[widthCount][];
        final LiveViewCheckpointStatePageRef[] retainedRefs = new LiveViewCheckpointStatePageRef[widthCount];
        final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
        for (int width = 1; width <= widthCount; width++) {
            final LiveViewCheckpointStatePageRef[] refs = refs(width);
            entry.of(key, new byte[width], refs);
            retainedScalars[width - 1] = entry.getScalarState();
            retainedRefs[width - 1] = entry.getStatePageRef(width - 1);
        }

        entry.resetWidthLookupCountForTest();
        for (int width = 1; width <= widthCount; width++) {
            final LiveViewCheckpointStatePageRef[] refs = refs(width);
            entry.of(key, new byte[width], refs);
            Assert.assertSame(retainedScalars[width - 1], entry.getScalarState());
            Assert.assertSame(retainedRefs[width - 1], entry.getStatePageRef(width - 1));
            Assert.assertEquals(width, entry.getStatePageCount());
        }
        Assert.assertEquals(3 * widthCount, entry.getWidthLookupCountForTest());
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

    private static void put(LiveViewCheckpointMutationArena arena, int key, int value, long segmentId) {
        final byte[] scalar = new byte[]{(byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value};
        final LiveViewCheckpointStatePageRef ref = new LiveViewCheckpointStatePageRef().of(
                segmentId, key * 8L, 8, 8, 0x31, 0, 1, 0
        );
        arena.put(key(key), scalar, new LiveViewCheckpointStatePageRef[]{ref});
    }

    private static LiveViewCheckpointStatePageRef[] refs(int count) {
        final LiveViewCheckpointStatePageRef[] refs = new LiveViewCheckpointStatePageRef[count];
        for (int i = 0; i < count; i++) {
            refs[i] = new LiveViewCheckpointStatePageRef().of(i + 1, i * 8L, 8, 8, 0x31, 0, 1, i);
        }
        return refs;
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
