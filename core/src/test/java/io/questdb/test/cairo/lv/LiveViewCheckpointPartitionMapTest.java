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

import com.sun.management.ThreadMXBean;
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

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.LongSupplier;

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
        long retainedBytes = 0;
        for (int charCount = 1; charCount <= widthCount; charCount++) {
            entry.of(new byte[stringKeyWidth(charCount)], emptyBytes, emptyRefs);
            retained[charCount - 1] = entry.getKey();
            retainedBytes += stringKeyWidth(charCount);
        }
        Assert.assertEquals(16_797_696, retainedBytes);
        Assert.assertEquals(retainedBytes, entry.getRetainedBufferBytesForTest());

        entry.clear();
        Assert.assertEquals(0, entry.getKey().length);
        Assert.assertEquals(0, entry.getScalarState().length);
        Assert.assertEquals(0, entry.getStatePageCount());
        entry.resetWidthLookupCountForTest();
        for (int charCount = 1; charCount <= widthCount; charCount++) {
            entry.of(new byte[stringKeyWidth(charCount)], emptyBytes, emptyRefs);
            Assert.assertSame(retained[charCount - 1], entry.getKey());
        }
        Assert.assertEquals(
                "each supported STRING width must perform one direct lookup",
                widthCount,
                entry.getWidthLookupCountForTest()
        );

        // Every STRING width from 1 to 4,096 characters adds up to more than the key cache may
        // keep once its operation ends, so a trim drops them all.
        entry.trimWidthCaches();
        Assert.assertEquals(0, entry.getRetainedBufferBytesForTest());
        entry.of(new byte[stringKeyWidth(1)], emptyBytes, emptyRefs);
        Assert.assertNotSame("a width the trim dropped must get a fresh array", retained[0], entry.getKey());
    }

    @Test
    public void testFlyweightReusesScalarAndPageRefExactWidths() {
        final int widthCount = 400;
        final byte[] key = new byte[]{1};
        final byte[][] retainedScalars = new byte[widthCount][];
        final LiveViewCheckpointStatePageRef[] retainedRefs = new LiveViewCheckpointStatePageRef[widthCount];
        final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
        for (int width = 1; width <= widthCount; width++) {
            entry.of(key, new byte[width], refs(width));
            retainedScalars[width - 1] = entry.getScalarState();
            retainedRefs[width - 1] = entry.getStatePageRef(width - 1);
        }
        // Widths 1 to 400 add up to 80,200 scalar bytes and as many reference slots, within what
        // either cache keeps once its operation ends, so the trim keeps every width.
        Assert.assertEquals(80_200, entry.getRetainedStatePageRefCountForTest());
        Assert.assertEquals(key.length + 80_200, entry.getRetainedBufferBytesForTest());
        entry.trimWidthCaches();

        entry.resetWidthLookupCountForTest();
        for (int width = 1; width <= widthCount; width++) {
            entry.of(key, new byte[width], refs(width));
            Assert.assertSame(retainedScalars[width - 1], entry.getScalarState());
            Assert.assertSame(retainedRefs[width - 1], entry.getStatePageRef(width - 1));
            Assert.assertEquals(width, entry.getStatePageCount());
            Assert.assertEquals(width, entry.getStatePageRef(width - 1).getSegmentId());
        }
        Assert.assertEquals(3 * widthCount, entry.getWidthLookupCountForTest());
    }

    @Test
    public void testFlyweightTrimKeepsCachesWithinTheirRetentionLimits() {
        // Distinct key and scalar widths that add up to exactly 16,777,216 bytes, and distinct
        // reference counts that add up to exactly 262,144 slots: what the key, scalar and
        // reference caches of an entry may each keep once an operation ends.
        final int[] byteWidths = new int[17];
        long byteWidthSum = 0;
        for (int i = 0; i < 16; i++) {
            byteWidths[i] = 1_048_568 + i;
            byteWidthSum += byteWidths[i];
        }
        byteWidths[16] = 8;
        byteWidthSum += byteWidths[16];
        Assert.assertEquals(16_777_216, byteWidthSum);
        final int[] refCounts = {65_536, 65_535, 65_534, 65_533, 6};
        Assert.assertEquals(262_144, Arrays.stream(refCounts).asLongStream().sum());

        final byte[] emptyBytes = new byte[0];
        final LiveViewCheckpointStatePageRef[] emptyRefs = new LiveViewCheckpointStatePageRef[0];
        final byte[] oneMoreByte = new byte[]{1};
        final LiveViewCheckpointStatePageRef[] oneMoreRef = refs(1);
        final byte[][] bytes = new byte[byteWidths.length][];
        for (int i = 0; i < byteWidths.length; i++) {
            bytes[i] = new byte[byteWidths[i]];
            Arrays.fill(bytes[i], (byte) i);
        }
        final LiveViewCheckpointStatePageRef[][] refs = new LiveViewCheckpointStatePageRef[refCounts.length][];
        for (int i = 0; i < refCounts.length; i++) {
            refs[i] = refs(refCounts[i]);
        }

        final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
        final byte[][] cachedKeys = new byte[bytes.length][];
        final byte[][] cachedScalars = new byte[bytes.length][];
        final LiveViewCheckpointStatePageRef[] cachedRefs = new LiveViewCheckpointStatePageRef[refs.length];
        for (int i = 0; i < bytes.length; i++) {
            entry.of(bytes[i], bytes[i], emptyRefs);
            cachedKeys[i] = entry.getKey();
            cachedScalars[i] = entry.getScalarState();
        }
        for (int i = 0; i < refs.length; i++) {
            entry.of(emptyBytes, emptyBytes, refs[i]);
            cachedRefs[i] = entry.getStatePageRef(0);
        }
        Assert.assertEquals(2 * byteWidthSum, entry.getRetainedBufferBytesForTest());
        Assert.assertEquals(262_144, entry.getRetainedStatePageRefCountForTest());

        // Caches at their limits keep every width across a trim.
        entry.trimWidthCaches();
        Assert.assertEquals(2 * byteWidthSum, entry.getRetainedBufferBytesForTest());
        Assert.assertEquals(262_144, entry.getRetainedStatePageRefCountForTest());
        for (int i = 0; i < bytes.length; i++) {
            entry.of(bytes[i], bytes[i], emptyRefs);
            Assert.assertSame("a key cache at its limit must keep its widths [width=" + byteWidths[i] + ']', cachedKeys[i], entry.getKey());
            Assert.assertSame(cachedScalars[i], entry.getScalarState());
        }
        for (int i = 0; i < refs.length; i++) {
            entry.of(emptyBytes, emptyBytes, refs[i]);
            Assert.assertSame("a reference cache at its limit must keep its counts [count=" + refCounts[i] + ']', cachedRefs[i], entry.getStatePageRef(0));
        }

        // One more key byte takes the key cache past its limit: the trim drops every key width,
        // and the scalar and reference caches, each within its own limit, keep theirs.
        entry.of(oneMoreByte, emptyBytes, emptyRefs);
        entry.trimWidthCaches();
        Assert.assertEquals(byteWidthSum, entry.getRetainedBufferBytesForTest());
        Assert.assertEquals(262_144, entry.getRetainedStatePageRefCountForTest());
        for (int i = 0; i < bytes.length; i++) {
            entry.of(bytes[i], bytes[i], emptyRefs);
            Assert.assertNotSame("a key width the trim dropped must get a fresh array [width=" + byteWidths[i] + ']', cachedKeys[i], entry.getKey());
            Assert.assertArrayEquals(bytes[i], entry.getKey());
            Assert.assertSame(cachedScalars[i], entry.getScalarState());
            cachedKeys[i] = entry.getKey();
        }
        // The refill counts from zero, so the key cache sits at its limit again and a trim keeps it.
        entry.trimWidthCaches();
        for (int i = 0; i < bytes.length; i++) {
            entry.of(bytes[i], emptyBytes, emptyRefs);
            Assert.assertSame("a refilled key cache at its limit must keep its widths [width=" + byteWidths[i] + ']', cachedKeys[i], entry.getKey());
        }

        // The same for the scalar cache.
        entry.of(emptyBytes, oneMoreByte, emptyRefs);
        entry.trimWidthCaches();
        Assert.assertEquals(byteWidthSum, entry.getRetainedBufferBytesForTest());
        for (int i = 0; i < bytes.length; i++) {
            entry.of(bytes[i], bytes[i], emptyRefs);
            Assert.assertSame(cachedKeys[i], entry.getKey());
            Assert.assertNotSame("a scalar width the trim dropped must get a fresh array [width=" + byteWidths[i] + ']', cachedScalars[i], entry.getScalarState());
            Assert.assertArrayEquals(bytes[i], entry.getScalarState());
            cachedScalars[i] = entry.getScalarState();
        }
        entry.trimWidthCaches();
        for (int i = 0; i < bytes.length; i++) {
            entry.of(emptyBytes, bytes[i], emptyRefs);
            Assert.assertSame("a refilled scalar cache at its limit must keep its widths [width=" + byteWidths[i] + ']', cachedScalars[i], entry.getScalarState());
        }

        // The same for the reference cache.
        entry.of(emptyBytes, emptyBytes, oneMoreRef);
        entry.trimWidthCaches();
        Assert.assertEquals(0, entry.getRetainedStatePageRefCountForTest());
        Assert.assertEquals(2 * byteWidthSum, entry.getRetainedBufferBytesForTest());
        for (int i = 0; i < refs.length; i++) {
            entry.of(emptyBytes, emptyBytes, refs[i]);
            Assert.assertNotSame("a reference count the trim dropped must get fresh references [count=" + refCounts[i] + ']', cachedRefs[i], entry.getStatePageRef(0));
            Assert.assertEquals(refCounts[i], entry.getStatePageCount());
            Assert.assertEquals(refCounts[i], entry.getStatePageRef(refCounts[i] - 1).getSegmentId());
            cachedRefs[i] = entry.getStatePageRef(0);
        }
        entry.trimWidthCaches();
        for (int i = 0; i < refs.length; i++) {
            entry.of(emptyBytes, emptyBytes, refs[i]);
            Assert.assertSame("a refilled reference cache at its limit must keep its counts [count=" + refCounts[i] + ']', cachedRefs[i], entry.getStatePageRef(0));
        }
    }

    @Test
    public void testReaderDecodePoolsKeepEveryArrayWithinTheirRetentionLimits() throws Exception {
        assertMemoryLeak(() -> {
            // Single-leaf maps, each entry with its own key, scalar and references. Decoding
            // fullBytes needs 16 keys of 1,048,572 bytes and 16 scalars of 4 bytes, exactly
            // 16,777,216 image bytes; decoding fullRefs needs 4 arrays of 65,536 state page
            // references, exactly 262,144. That is what a node's byte pool and reference pool may
            // each keep once an operation ends. oneMoreByte adds one key of a width the byte
            // pool does not hold yet, 5 bytes, and oneMoreRef adds one reference. The 17 keys of
            // wideWidths each have their own width of about 1 MiB, 17,825,656 bytes in all.
            final long retainedBytesLimit = 16_777_216;
            final long retainedRefsLimit = 262_144;
            final int scalarWidth = Integer.BYTES;
            final int fullKeyCount = 16;
            final int fullKeyWidth = 1_048_572;
            final int fullRefEntryCount = 4;
            final int fullRefCount = 65_536;
            final int oneMoreKeyWidth = 5;
            final int wideWidthCount = 17;
            final int firstWideKeyWidth = 1_048_560;
            Assert.assertEquals(retainedBytesLimit, (long) fullKeyCount * (fullKeyWidth + scalarWidth));
            Assert.assertEquals(retainedRefsLimit, (long) fullRefEntryCount * fullRefCount);
            final LiveViewCheckpointPageRef fullBytes = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef oneMoreByte = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef fullRefs = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef oneMoreRef = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef wideWidths = new LiveViewCheckpointPageRef();
            try (Path dir = new Path()) {
                checkpointsDir(dir);
                writeDistinctEntries(dir, 1, 0, fullKeyCount, fullKeyWidth, 0, scalarWidth, 0, fullBytes);
                writeDistinctEntries(dir, 2, 100, 1, oneMoreKeyWidth, 0, scalarWidth, 0, oneMoreByte);
                writeDistinctEntries(dir, 3, 200, fullRefEntryCount, scalarWidth, 0, scalarWidth, fullRefCount, fullRefs);
                writeDistinctEntries(dir, 4, 300, 1, scalarWidth, 0, scalarWidth, 1, oneMoreRef);
                writeDistinctEntries(dir, 5, 400, wideWidthCount, firstWideKeyWidth, 1, scalarWidth, 0, wideWidths);
            }

            try (Path dir = new Path()) {
                checkpointsDir(dir);
                final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
                final int[] visited = new int[1];

                // Lookups decode into the reader's memo node and copy into the caller's entry, so
                // what the reader keeps is the memo node's pools alone.
                try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration)) {
                    assertDecodePoolRetention(
                            reader,
                            dir,
                            () -> {
                                for (int i = 0; i < fullKeyCount; i++) {
                                    Assert.assertTrue(reader.find(fullBytes, distinctBytes(i, fullKeyWidth), entry));
                                    assertDistinctWideEntry(i, fullKeyWidth, scalarWidth, entry);
                                }
                            },
                            () -> {
                                Assert.assertTrue(reader.find(oneMoreByte, distinctBytes(100, oneMoreKeyWidth), entry));
                                assertDistinctEntry(100, oneMoreKeyWidth, scalarWidth, 0, entry);
                            },
                            reader::getRetainedBufferBytesForTest,
                            retainedBytesLimit,
                            retainedBytesLimit + oneMoreKeyWidth,
                            0,
                            0
                    );
                    assertDecodePoolRetention(
                            reader,
                            dir,
                            () -> {
                                for (int i = 0; i < fullRefEntryCount; i++) {
                                    Assert.assertTrue(reader.find(fullRefs, distinctBytes(200 + i, scalarWidth), entry));
                                    assertDistinctEntry(200 + i, scalarWidth, scalarWidth, fullRefCount, entry);
                                }
                            },
                            () -> {
                                Assert.assertTrue(reader.find(oneMoreRef, distinctBytes(300, scalarWidth), entry));
                                assertDistinctEntry(300, scalarWidth, scalarWidth, 1, entry);
                            },
                            reader::getRetainedStatePageRefCountForTest,
                            retainedRefsLimit,
                            retainedRefsLimit + 1,
                            0,
                            0
                    );
                }

                // A root count decodes the root into the reader's navigation node.
                try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration)) {
                    assertDecodePoolRetention(
                            reader,
                            dir,
                            () -> Assert.assertEquals(0, reader.rootChildCount(fullBytes)),
                            () -> Assert.assertEquals(0, reader.rootChildCount(oneMoreByte)),
                            reader::getRetainedBufferBytesForTest,
                            retainedBytesLimit,
                            retainedBytesLimit + oneMoreKeyWidth,
                            0,
                            0
                    );
                }

                // Iteration decodes into a pooled node and copies every entry into the reader's
                // scratch entry, whose caches keep one array per width on top of the node's pools.
                try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration)) {
                    assertDecodePoolRetention(
                            reader,
                            dir,
                            () -> {
                                visited[0] = 0;
                                reader.iterateAll(fullBytes, visitedEntry -> assertDistinctWideEntry(visited[0]++, fullKeyWidth, scalarWidth, visitedEntry));
                                Assert.assertEquals(fullKeyCount, visited[0]);
                            },
                            () -> reader.iterateAll(oneMoreByte, visitedEntry -> assertDistinctEntry(100, oneMoreKeyWidth, scalarWidth, 0, visitedEntry)),
                            reader::getRetainedBufferBytesForTest,
                            retainedBytesLimit,
                            retainedBytesLimit + oneMoreKeyWidth,
                            fullKeyWidth + scalarWidth,
                            fullKeyWidth + scalarWidth + oneMoreKeyWidth
                    );
                    assertDecodePoolRetention(
                            reader,
                            dir,
                            () -> {
                                visited[0] = 0;
                                reader.iterateAll(fullRefs, visitedEntry -> assertDistinctEntry(200 + visited[0]++, scalarWidth, scalarWidth, fullRefCount, visitedEntry));
                                Assert.assertEquals(fullRefEntryCount, visited[0]);
                            },
                            () -> reader.iterateAll(oneMoreRef, visitedEntry -> assertDistinctEntry(300, scalarWidth, scalarWidth, 1, visitedEntry)),
                            reader::getRetainedStatePageRefCountForTest,
                            retainedRefsLimit,
                            retainedRefsLimit + 1,
                            fullRefCount,
                            fullRefCount + 1
                    );
                }

                // Keys of 17 widths take the scratch entry's key cache, and the pooled node's byte
                // pool, past their limits within one iteration, so detach() drops both and only the
                // scratch entry's 4-byte scalar width stays.
                try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration)) {
                    reader.of(dir);
                    visited[0] = 0;
                    reader.iterateAll(wideWidths, visitedEntry -> {
                        final int i = visited[0]++;
                        assertDistinctWideEntry(400 + i, firstWideKeyWidth + i, scalarWidth, visitedEntry);
                    });
                    Assert.assertEquals(wideWidthCount, visited[0]);
                    Assert.assertTrue(
                            "an iteration must keep every key width it copies [largestRetainedBytes="
                                    + reader.getLargestRetainedBufferBytesForTest() + ']',
                            reader.getLargestRetainedBufferBytesForTest() > retainedBytesLimit
                    );
                    reader.detach();
                    Assert.assertEquals(
                            "detaching must drop the scratch entry's key cache past its limit",
                            scalarWidth,
                            reader.getRetainedBufferBytesForTest()
                    );
                }
            }
        });
    }

    @Test
    public void testShuffledLookupsOverRingReferenceCountsAllocateNothingOnceWarm() throws Exception {
        assertMemoryLeak(() -> {
            // A ring-shaped function keeps two state pages per chunk and up to 256 chunks per key,
            // and keys that start at different times reach different chunk counts. Here 1,024 keys
            // hold every even reference count from 2 to 512, four keys per count, in an order
            // unrelated to key order, so every leaf holds its own mix of counts. A node's pool keeps,
            // per count, the most arrays of that count a single leaf needed, which for these leaves
            // adds up to more than 65,536 references. Each pass is one operation: shuffled lookups,
            // a full iteration, then detach(). Once the pools hold every count, a pass must not
            // allocate: an allocation-free pass measures a few hundred bytes, while fresh references
            // for every decoded entry cost hundreds of megabytes.
            final int keyCount = 1_024;
            final int warmUpPasses = 2;
            final int measuredPasses = 4;
            final long passAllocationLimitBytes = 16_384;
            final byte[][] keys = new byte[keyCount][];
            final int[] refCounts = new int[keyCount];
            final int[] order = new int[keyCount];
            for (int i = 0; i < keyCount; i++) {
                keys[i] = key(i);
                refCounts[i] = 2 * (1 + (i * 997 % keyCount) / 4);
                order[i] = i;
            }
            shuffle(order, new Rnd(42, 7));
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            try (Path dir = new Path();
                 LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration)) {
                writer.of(checkpointsDir(dir));
                for (int i = 0; i < keyCount; i++) {
                    arena.put(keys[i], keys[i], refs(refCounts[i]));
                }
                writer.apply(new LiveViewCheckpointPageRef(), arena, 1, root);
            }

            final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
            final long threadId = Thread.currentThread().threadId();
            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                checkpointsDir(dir);
                final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
                final int[] mismatchCount = new int[1];
                final int[] visitedCount = new int[1];
                final LiveViewCheckpointPartitionMapReader.Visitor visitor = visitedEntry -> {
                    final int i = scalar(visitedEntry);
                    if (i != visitedCount[0]++ || !hasRefs(visitedEntry, refCounts[i])) {
                        mismatchCount[0]++;
                    }
                };
                long lookupRefCount = 0;
                for (int pass = 0; pass < warmUpPasses + measuredPasses; pass++) {
                    final long decodedPagesBefore = reader.getDecodedPageCount();
                    final long allocatedBefore = threadMXBean.getThreadAllocatedBytes(threadId);
                    reader.of(dir);
                    for (int n = 0; n < keyCount; n++) {
                        final int i = order[n];
                        if (!reader.find(root, keys[i], entry) || scalar(entry) != i || !hasRefs(entry, refCounts[i])) {
                            mismatchCount[0]++;
                        }
                    }
                    if (pass == 0) {
                        lookupRefCount = reader.getRetainedStatePageRefCountForTest();
                    }
                    visitedCount[0] = 0;
                    reader.iterateAll(root, visitor);
                    reader.detach();
                    final long allocatedBytes = threadMXBean.getThreadAllocatedBytes(threadId) - allocatedBefore;
                    final long decodedPages = reader.getDecodedPageCount() - decodedPagesBefore;
                    Assert.assertEquals(0, mismatchCount[0]);
                    Assert.assertEquals(keyCount, visitedCount[0]);
                    Assert.assertTrue(
                            "shuffled lookups must keep decoding leaves [pass=" + pass + ", decodedPages=" + decodedPages + ']',
                            decodedPages > keyCount
                    );
                    if (pass >= warmUpPasses) {
                        Assert.assertTrue(
                                "a warmed-up pass must not allocate per decoded page [pass=" + pass
                                        + ", decodedPages=" + decodedPages + ", allocatedBytes=" + allocatedBytes
                                        + ", limit=" + passAllocationLimitBytes + ']',
                                allocatedBytes <= passAllocationLimitBytes
                        );
                    }
                }
                Assert.assertTrue(
                        "the lookups' decode pool must hold more than 65,536 references [refCount=" + lookupRefCount + ']',
                        lookupRefCount > 65_536
                );
            }
        });
    }

    @Test
    public void testShuffledLookupsOverVariableWidthKeysAllocateNothingOnceWarm() throws Exception {
        assertMemoryLeak(() -> {
            // A seal probes the root below it in the order it walks its own keys rather than in
            // key order, so nearly every lookup decodes another leaf, and with keys of 10 to 200
            // characters every leaf holds a different mix of widths. Each pass is one seal's
            // lookups between of() and detach(). Once the decode pools hold every width, a pass
            // must not allocate per decode: an allocation-free pass measures a few hundred bytes,
            // while one fresh array per decoded entry costs megabytes.
            final int keyCount = 4_096;
            final int minChars = 10;
            final int maxChars = 200;
            final int warmUpPasses = 2;
            final int measuredPasses = 4;
            final long passAllocationLimitBytes = 16_384;
            final Rnd rnd = new Rnd(42, 7);
            final byte[][] keys = new byte[keyCount][];
            final int[] order = new int[keyCount];
            for (int i = 0; i < keyCount; i++) {
                final byte[] key = new byte[stringKeyWidth(minChars + rnd.nextInt(maxChars - minChars + 1))];
                Arrays.fill(key, (byte) 'x');
                System.arraycopy(key(i), 0, key, 0, Integer.BYTES);
                keys[i] = key;
                order[i] = i;
            }
            for (int i = keyCount - 1; i > 0; i--) {
                final int j = rnd.nextInt(i + 1);
                final int swap = order[i];
                order[i] = order[j];
                order[j] = swap;
            }
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            try (Path dir = new Path();
                 LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration)) {
                writer.of(checkpointsDir(dir));
                for (int i = 0; i < keyCount; i++) {
                    arena.put(keys[i], key(i), refs(1));
                }
                writer.apply(new LiveViewCheckpointPageRef(), arena, 1, root);
            }

            final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
            final long threadId = Thread.currentThread().threadId();
            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                checkpointsDir(dir);
                final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
                for (int pass = 0; pass < warmUpPasses + measuredPasses; pass++) {
                    final long decodedPagesBefore = reader.getDecodedPageCount();
                    final long allocatedBefore = threadMXBean.getThreadAllocatedBytes(threadId);
                    reader.of(dir);
                    int mismatchCount = 0;
                    for (int n = 0; n < keyCount; n++) {
                        final int i = order[n];
                        if (!reader.find(root, keys[i], entry) || !Arrays.equals(keys[i], entry.getKey()) || scalar(entry) != i) {
                            mismatchCount++;
                        }
                    }
                    reader.detach();
                    final long allocatedBytes = threadMXBean.getThreadAllocatedBytes(threadId) - allocatedBefore;
                    final long decodedPages = reader.getDecodedPageCount() - decodedPagesBefore;
                    Assert.assertEquals(0, mismatchCount);
                    Assert.assertTrue(
                            "shuffled lookups must keep decoding leaves [pass=" + pass + ", decodedPages=" + decodedPages + ']',
                            decodedPages > keyCount
                    );
                    if (pass >= warmUpPasses) {
                        Assert.assertTrue(
                                "a warmed-up pass must not allocate per decoded page [pass=" + pass
                                        + ", decodedPages=" + decodedPages + ", allocatedBytes=" + allocatedBytes
                                        + ", limit=" + passAllocationLimitBytes + ']',
                                allocatedBytes <= passAllocationLimitBytes
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testShuffledLookupsOverWideKeyWidthsAllocateNothingOnceWarm() throws Exception {
        assertMemoryLeak(() -> {
            // 32 key widths of 4,096 to 6,080 bytes with 64 keys each. Keys of one width sort
            // together, as STRING keys of one length do, so a leaf holds up to 64 keys of a width
            // and a node's pool keeps at least 32 arrays of every width: more than 4 MiB of key
            // images. Each pass is one operation: lookups of every 16th key in shuffled order, which
            // cross every leaf, a full iteration, then detach(). Once the pools hold every width, a
            // pass must not allocate.
            final int widthCount = 32;
            final int keysPerWidth = 64;
            final int keyCount = widthCount * keysPerWidth;
            final int lookupStride = 16;
            final int warmUpPasses = 2;
            final int measuredPasses = 3;
            final long passAllocationLimitBytes = 16_384;
            final byte[][] keys = new byte[keyCount][];
            for (int w = 0; w < widthCount; w++) {
                for (int k = 0; k < keysPerWidth; k++) {
                    final int i = w * keysPerWidth + k;
                    final byte[] key = new byte[4_096 + 64 * w];
                    Arrays.fill(key, (byte) i);
                    System.arraycopy(key(w), 0, key, 0, Integer.BYTES);
                    System.arraycopy(key(k), 0, key, Integer.BYTES, Integer.BYTES);
                    keys[i] = key;
                }
            }
            final int lookupCount = keyCount / lookupStride;
            final int[] order = new int[lookupCount];
            for (int n = 0; n < lookupCount; n++) {
                order[n] = n * lookupStride;
            }
            shuffle(order, new Rnd(42, 7));
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            try (Path dir = new Path();
                 LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration)) {
                writer.of(checkpointsDir(dir));
                for (int i = 0; i < keyCount; i++) {
                    arena.put(keys[i], key(i), refs(1));
                }
                writer.apply(new LiveViewCheckpointPageRef(), arena, 1, root);
            }

            final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
            final long threadId = Thread.currentThread().threadId();
            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                checkpointsDir(dir);
                final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
                final int[] mismatchCount = new int[1];
                final int[] visitedCount = new int[1];
                final LiveViewCheckpointPartitionMapReader.Visitor visitor = visitedEntry -> {
                    final int i = visitedCount[0]++;
                    if (scalar(visitedEntry) != i || !Arrays.equals(keys[i], visitedEntry.getKey())) {
                        mismatchCount[0]++;
                    }
                };
                long largestLookupPoolBytes = 0;
                for (int pass = 0; pass < warmUpPasses + measuredPasses; pass++) {
                    final long decodedPagesBefore = reader.getDecodedPageCount();
                    final long allocatedBefore = threadMXBean.getThreadAllocatedBytes(threadId);
                    reader.of(dir);
                    for (int n = 0; n < lookupCount; n++) {
                        final int i = order[n];
                        if (!reader.find(root, keys[i], entry) || scalar(entry) != i || !Arrays.equals(keys[i], entry.getKey())) {
                            mismatchCount[0]++;
                        }
                    }
                    if (pass == 0) {
                        largestLookupPoolBytes = reader.getLargestRetainedBufferBytesForTest();
                    }
                    visitedCount[0] = 0;
                    reader.iterateAll(root, visitor);
                    reader.detach();
                    final long allocatedBytes = threadMXBean.getThreadAllocatedBytes(threadId) - allocatedBefore;
                    final long decodedPages = reader.getDecodedPageCount() - decodedPagesBefore;
                    Assert.assertEquals(0, mismatchCount[0]);
                    Assert.assertEquals(keyCount, visitedCount[0]);
                    Assert.assertTrue(
                            "shuffled lookups must keep decoding leaves [pass=" + pass + ", decodedPages=" + decodedPages + ']',
                            decodedPages > lookupCount
                    );
                    if (pass >= warmUpPasses) {
                        Assert.assertTrue(
                                "a warmed-up pass must not allocate per decoded page [pass=" + pass
                                        + ", decodedPages=" + decodedPages + ", allocatedBytes=" + allocatedBytes
                                        + ", limit=" + passAllocationLimitBytes + ']',
                                allocatedBytes <= passAllocationLimitBytes
                        );
                    }
                }
                Assert.assertTrue(
                        "the lookups' decode pool must hold more than 4 MiB of key images [bytes=" + largestLookupPoolBytes + ']',
                        largestLookupPoolBytes > 4_194_304
                );
            }
        });
    }

    // Checks one decode pool of reader against its retention limit across three operations:
    // filling the pool to exactly its limit keeps every array past detach(), one element more
    // drops them all at detach(), and a refill counts from zero, so the pool sits at its limit
    // again and keeps it. retained reads the pool together with what the reader keeps beside it,
    // which is residual before push runs and pushedResidual after.
    private static void assertDecodePoolRetention(
            LiveViewCheckpointPartitionMapReader reader,
            Path dir,
            Runnable fill,
            Runnable push,
            LongSupplier retained,
            long limit,
            long pushed,
            long residual,
            long pushedResidual
    ) {
        reader.of(dir);
        fill.run();
        Assert.assertEquals("an operation must keep every array it lends", limit + residual, retained.getAsLong());
        reader.detach();
        Assert.assertEquals("a pool at its limit must keep every array", limit + residual, retained.getAsLong());

        reader.of(dir);
        fill.run();
        push.run();
        Assert.assertEquals(
                "an operation must keep every array it lends past the limit too",
                pushed + pushedResidual,
                retained.getAsLong()
        );
        reader.detach();
        Assert.assertEquals("a pool past its limit must drop every array", pushedResidual, retained.getAsLong());

        reader.of(dir);
        fill.run();
        Assert.assertEquals(limit + pushedResidual, retained.getAsLong());
        reader.detach();
        Assert.assertEquals(
                "a refilled pool at its limit must keep every array",
                limit + pushedResidual,
                retained.getAsLong()
        );
    }

    // Checks an entry distinctBytes() and distinctRefs() wrote under id, down to every field of
    // every state page reference.
    private static void assertDistinctEntry(
            int id,
            int keyWidth,
            int scalarWidth,
            int refCount,
            LiveViewCheckpointPartitionMapEntry entry
    ) {
        Assert.assertArrayEquals("key [id=" + id + ']', distinctBytes(id, keyWidth), entry.getKey());
        Assert.assertArrayEquals("scalar [id=" + id + ']', distinctBytes(id, scalarWidth), entry.getScalarState());
        Assert.assertEquals("reference count [id=" + id + ']', refCount, entry.getStatePageCount());
        final LiveViewCheckpointStatePageRef[] expectedRefs = distinctRefs(id, refCount);
        for (int r = 0; r < refCount; r++) {
            final LiveViewCheckpointStatePageRef expected = expectedRefs[r];
            final LiveViewCheckpointStatePageRef actual = entry.getStatePageRef(r);
            final String message = "reference [id=" + id + ", ref=" + r + ']';
            Assert.assertEquals(message, expected.getSegmentId(), actual.getSegmentId());
            Assert.assertEquals(message, expected.getOffset(), actual.getOffset());
            Assert.assertEquals(message, expected.getStoredLength(), actual.getStoredLength());
            Assert.assertEquals(message, expected.getDecodedLength(), actual.getDecodedLength());
            Assert.assertEquals(message, expected.getPageKind(), actual.getPageKind());
            Assert.assertEquals(message, expected.getCodec(), actual.getCodec());
            Assert.assertEquals(message, expected.getRowCount(), actual.getRowCount());
            Assert.assertEquals(message, expected.getFlags(), actual.getFlags());
        }
    }

    // Checks an entry writeDistinctEntries() wrote under id without state page references, reading
    // only the bytes of a wide key that tell it apart from every other entry.
    private static void assertDistinctWideEntry(int id, int keyWidth, int scalarWidth, LiveViewCheckpointPartitionMapEntry entry) {
        final byte[] key = entry.getKey();
        Assert.assertEquals("key width [id=" + id + ']', keyWidth, key.length);
        Assert.assertEquals("key id [id=" + id + ']', id, intKey(key));
        Assert.assertEquals("key tail [id=" + id + ']', (byte) id, key[keyWidth - 1]);
        Assert.assertArrayEquals("scalar [id=" + id + ']', distinctBytes(id, scalarWidth), entry.getScalarState());
        Assert.assertEquals("reference count [id=" + id + ']', 0, entry.getStatePageCount());
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

    // Bytes only the entry under id holds at this width: the id leads, so keys sort by it, and
    // every later byte repeats the id's low byte.
    private static byte[] distinctBytes(int id, int width) {
        final byte[] bytes = new byte[width];
        Arrays.fill(bytes, (byte) id);
        System.arraycopy(key(id), 0, bytes, 0, Integer.BYTES);
        return bytes;
    }

    // References only the entry under id holds: every field but the page kind and codec
    // derives from the id or the reference's position.
    private static LiveViewCheckpointStatePageRef[] distinctRefs(int id, int count) {
        final LiveViewCheckpointStatePageRef[] refs = new LiveViewCheckpointStatePageRef[count];
        for (int r = 0; r < count; r++) {
            refs[r] = new LiveViewCheckpointStatePageRef().of(id + 1, id * 1_024L + r, 8 + r, 16 + id, 0x31, 0, 1 + r, id);
        }
        return refs;
    }

    // Tells whether entry holds count references, the last of which refs(count) wrote.
    private static boolean hasRefs(LiveViewCheckpointPartitionMapEntry entry, int count) {
        return entry.getStatePageCount() == count && entry.getStatePageRef(count - 1).getSegmentId() == count;
    }

    private static int intKey(byte[] key) {
        return (key[0] & 0xff) << 24 | (key[1] & 0xff) << 16 | (key[2] & 0xff) << 8 | key[3] & 0xff;
    }

    private static byte[] key(int value) {
        return new byte[]{(byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value};
    }

    private static int stringKeyWidth(int charCount) {
        return Integer.BYTES + Character.BYTES * charCount;
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

    private static void shuffle(int[] values, Rnd rnd) {
        for (int i = values.length - 1; i > 0; i--) {
            final int j = rnd.nextInt(i + 1);
            final int swap = values[i];
            values[i] = values[j];
            values[j] = swap;
        }
    }

    // Writes a single-leaf map of count entries under segmentId, the i-th one holding
    // distinctBytes() and distinctRefs() content under id firstId + i and a key of
    // firstKeyWidth + i * keyWidthStep bytes.
    private static void writeDistinctEntries(
            Path dir,
            long segmentId,
            int firstId,
            int count,
            int firstKeyWidth,
            int keyWidthStep,
            int scalarWidth,
            int refCount,
            LiveViewCheckpointPageRef root
    ) {
        final int capacity = Math.max(2, count);
        try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena();
             LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, capacity, capacity)) {
            writer.of(dir);
            for (int i = 0; i < count; i++) {
                final int id = firstId + i;
                arena.put(distinctBytes(id, firstKeyWidth + i * keyWidthStep), distinctBytes(id, scalarWidth), distinctRefs(id, refCount));
            }
            writer.apply(new LiveViewCheckpointPageRef(), arena, segmentId, root);
        }
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
