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
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
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
                            LiveViewCheckpointTestKeys.remove(shrink, key(i));
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
                    try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                        Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, root, key(keyCount - 1), entry));
                        Assert.assertEquals(2 * keyCount + keyCount - 1, scalar(entry));
                    }
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
                try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                    Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, oldRoot, key(0), entry));
                    Assert.assertEquals(0, scalar(entry));
                    Assert.assertEquals(0, entry.getStatePageRef(0).getSegmentId());
                    Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, newRoot, key(0), entry));
                    Assert.assertEquals(999, scalar(entry));
                    Assert.assertEquals(9, entry.getStatePageRef(0).getSegmentId());

                    Assert.assertEquals(oldChildren.length, reader.rootChildCount(newRoot));
                    final LiveViewCheckpointPageRef child = new LiveViewCheckpointPageRef();
                    for (int i = 1; i < oldChildren.length; i++) {
                        reader.rootChildRef(newRoot, i, child);
                        assertRefEquals(oldChildren[i], child);
                    }
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
                try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                    Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, root, key(63), entry));
                    Assert.assertEquals(63, scalar(entry));
                    try {
                        LiveViewCheckpointTestKeys.find(reader, root, key(0), entry);
                        Assert.fail("expected selected corrupt path to fail");
                    } catch (CairoException e) {
                        Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                        TestUtils.assertContains(e.getFlyweightMessage(), "checksum mismatch");
                    }
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

            try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                     Path dir = new Path()) {
                    reader.of(checkpointsDir(dir));
                    Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, root, key(0), entry));
                    final long depth = reader.getDecodedPageCount();
                    Assert.assertTrue(
                            "the tree has to be deeper than the memo this guards, depth=" + depth,
                            depth > 4
                    );

                    // Repeating one lookup must decode nothing at all.
                    for (int pass = 0; pass < 8; pass++) {
                        Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, root, key(0), entry));
                        Assert.assertEquals(0, scalar(entry));
                    }
                    Assert.assertEquals(depth, reader.getDecodedPageCount());

                    // A seal walks the keys it touched in the order it touched them, so
                    // an ascending sweep is the shape that matters: each page is decoded
                    // as the sweep reaches it, and once.
                    final long beforeSweep = reader.getDecodedPageCount();
                    for (int i = 0; i < keyCount; i++) {
                        Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, root, key(i), entry));
                        Assert.assertEquals(i, scalar(entry));
                    }
                    final long sweepDecodes = reader.getDecodedPageCount() - beforeSweep;
                    Assert.assertTrue(
                            "an ascending sweep decoded " + sweepDecodes + " pages, a memoless descent per key costs "
                                    + keyCount * depth,
                            sweepDecodes < keyCount * depth / 4
                    );
                }
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

            try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                     Path dir = new Path()) {
                    reader.of(checkpointsDir(dir));
                    Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, root, key(0), entry));
                    Assert.assertEquals(0, scalar(entry));
                    final long depth = reader.getDecodedPageCount();
                    Assert.assertTrue("the tree must exceed the 64-level memo, depth=" + depth, depth > 64);

                    // The cached prefix stays resident, while the suffix must be decoded
                    // through the scratch node on every descent.
                    final long beforeRepeat = reader.getDecodedPageCount();
                    Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, root, key(0), entry));
                    Assert.assertEquals(0, scalar(entry));
                    Assert.assertTrue(reader.getDecodedPageCount() > beforeRepeat);

                    for (int i = 0; i < keyCount; i++) {
                        Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, root, key(i), entry));
                        Assert.assertEquals(i, scalar(entry));
                        Assert.assertEquals(i % 5, entry.getStatePageRef(0).getSegmentId());
                    }
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
                            LiveViewCheckpointTestKeys.remove(mutations, key(key));
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
                        Assert.assertEquals((int) expectedEntry.getKey(), intKey(entry.copyKeyForTest()));
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

            try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                     Path dir = new Path()) {
                    reader.of(checkpointsDir(dir));
                    Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, first, key(1), entry));
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

                    Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, second, key(1), entry));
                    Assert.assertEquals(22, scalar(entry));
                }
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

            try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                     Path dir = new Path()) {
                    reader.of(checkpointsDir(dir));
                    for (int pass = 0; pass < 3; pass++) {
                        for (int i = 0; i < keyCount; i++) {
                            Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, root, key(i), entry));
                            Assert.assertEquals(i, scalar(entry));
                            Assert.assertEquals(i % 5, entry.getStatePageRef(0).getSegmentId());
                        }
                        Assert.assertFalse(LiveViewCheckpointTestKeys.find(reader, root, key(keyCount), entry));
                    }
                    // Two roots share every page the update left untouched, so a lookup
                    // must answer with the root it names rather than with the pages the
                    // lookup before it decoded.
                    for (int pass = 0; pass < 3; pass++) {
                        Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, nextRoot, key(0), entry));
                        Assert.assertEquals(999, scalar(entry));
                        Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, root, key(0), entry));
                        Assert.assertEquals(0, scalar(entry));
                        Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, nextRoot, key(63), entry));
                        Assert.assertEquals(63, scalar(entry));
                    }
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
                try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                    for (int i = 0; i < 63; i++) {
                        Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, updatedRoot, key(i), entry));
                        Assert.assertEquals(1_000 + i, scalar(entry));
                    }
                    Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, updatedRoot, key(63), entry));
                    Assert.assertEquals(63, scalar(entry));
                }
            }
        });
    }

    @Test
    public void testFlyweightKeepsOneNativeKeyBufferForEveryWidth() throws Exception {
        assertMemoryLeak(() -> {
            // Every STRING width from 1 to 4,096 characters. The entry copies each key into the
            // one native buffer it owns, which grows to the widest key and serves every
            // narrower one, so no width costs a heap array or a width lookup, and a trim within
            // the limit keeps the buffer.
            final int widthCount = 4_096;
            final int widestKey = stringKeyWidth(widthCount);
            final byte[] emptyBytes = new byte[0];
            final LiveViewCheckpointStatePageRef[] emptyRefs = new LiveViewCheckpointStatePageRef[0];
            final long source = Unsafe.malloc(widestKey, MemoryTag.NATIVE_DEFAULT);
            try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                for (int i = 0; i < widestKey; i++) {
                    Unsafe.putByte(source + i, (byte) (i * 31));
                }
                entry.of(source, widestKey, emptyBytes, emptyRefs);
                final long keyAddress = entry.getKeyAddress();
                final long capacity = entry.getKeyBufferCapacityForTest();
                Assert.assertTrue(
                        "the buffer holds the widest key [capacity=" + capacity + ']',
                        capacity >= widestKey && capacity < 2L * widestKey
                );

                final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
                final long threadId = Thread.currentThread().threadId();
                entry.resetWidthLookupCountForTest();
                int mismatchCount = 0;
                final long allocatedBefore = threadMXBean.getThreadAllocatedBytes(threadId);
                for (int charCount = widthCount; charCount >= 1; charCount--) {
                    final int width = stringKeyWidth(charCount);
                    entry.of(source, width, emptyBytes, emptyRefs);
                    if (entry.getKeyLength() != width
                            || entry.getKeyAddress() != keyAddress
                            || Unsafe.getByte(entry.getKeyAddress() + width - 1) != Unsafe.getByte(source + width - 1)) {
                        mismatchCount++;
                    }
                }
                final long allocatedBytes = threadMXBean.getThreadAllocatedBytes(threadId) - allocatedBefore;
                Assert.assertEquals(0, mismatchCount);
                Assert.assertEquals("a key costs no width lookup", 0, entry.getWidthLookupCountForTest());
                Assert.assertTrue(
                        "copying a key of any width must not allocate heap [allocatedBytes=" + allocatedBytes + ']',
                        allocatedBytes <= 16_384
                );
                Assert.assertEquals(capacity, entry.getKeyBufferCapacityForTest());

                entry.clear();
                Assert.assertEquals(0, entry.getKeyLength());
                Assert.assertEquals(0, entry.getScalarState().length);
                Assert.assertEquals(0, entry.getStatePageCount());
                entry.trimWidthCaches();
                Assert.assertEquals(
                        "a key buffer within its limit survives the trim",
                        capacity,
                        entry.getKeyBufferCapacityForTest()
                );
            } finally {
                Unsafe.free(source, widestKey, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFlyweightReusesScalarAndPageRefExactWidths() throws Exception {
        assertMemoryLeak(() -> {
            final int widthCount = 400;
            final byte[] key = new byte[]{1};
            final byte[][] retainedScalars = new byte[widthCount][];
            final LiveViewCheckpointStatePageRef[] retainedRefs = new LiveViewCheckpointStatePageRef[widthCount];
            try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                for (int width = 1; width <= widthCount; width++) {
                    LiveViewCheckpointTestKeys.of(entry, key, new byte[width], refs(width));
                    retainedScalars[width - 1] = entry.getScalarState();
                    retainedRefs[width - 1] = entry.getStatePageRef(width - 1);
                }
                // Widths 1 to 400 add up to 80,200 scalar bytes and as many reference slots, within
                // what either cache keeps once its operation ends, so the trim keeps every width.
                Assert.assertEquals(80_200, entry.getRetainedStatePageRefCountForTest());
                Assert.assertEquals(
                        entry.getKeyBufferCapacityForTest() + 80_200,
                        entry.getRetainedBufferBytesForTest()
                );
                entry.trimWidthCaches();

                entry.resetWidthLookupCountForTest();
                for (int width = 1; width <= widthCount; width++) {
                    LiveViewCheckpointTestKeys.of(entry, key, new byte[width], refs(width));
                    Assert.assertSame(retainedScalars[width - 1], entry.getScalarState());
                    Assert.assertSame(retainedRefs[width - 1], entry.getStatePageRef(width - 1));
                    Assert.assertEquals(width, entry.getStatePageCount());
                    Assert.assertEquals(width, entry.getStatePageRef(width - 1).getSegmentId());
                }
                // One scalar and one reference lookup per copy; the key takes none.
                Assert.assertEquals(2 * widthCount, entry.getWidthLookupCountForTest());
            }
        });
    }

    @Test
    public void testFlyweightTrimFreesAKeyBufferPastItsRetentionLimit() throws Exception {
        assertMemoryLeak(() -> {
            // A key buffer of exactly 16,777,216 bytes is what an entry may keep once its
            // operation ends; one byte more takes it past the limit, and the trim frees it.
            final long limit = LiveViewCheckpointPartitionMapEntry.MAX_RETAINED_BUFFER_BYTES;
            final byte[] emptyBytes = new byte[0];
            final LiveViewCheckpointStatePageRef[] emptyRefs = new LiveViewCheckpointStatePageRef[0];
            final byte[] scalar = {1, 2, 3, 4};
            final long sourceLength = limit + 1;
            final long source = Unsafe.malloc(sourceLength, MemoryTag.NATIVE_DEFAULT);
            try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                Vect.memset(source, sourceLength, 7);
                Unsafe.putByte(source + limit - 1, (byte) 9);
                entry.of(source, (int) limit, scalar, emptyRefs);
                Assert.assertEquals(limit, entry.getKeyBufferCapacityForTest());
                entry.trimWidthCaches();
                Assert.assertEquals("a key buffer at its limit survives the trim", limit, entry.getKeyBufferCapacityForTest());
                Assert.assertEquals(limit, entry.getKeyLength());
                Assert.assertEquals(9, Unsafe.getByte(entry.getKeyAddress() + limit - 1));

                entry.of(source, (int) sourceLength, scalar, emptyRefs);
                Assert.assertTrue(entry.getKeyBufferCapacityForTest() > limit);
                entry.trimWidthCaches();
                Assert.assertEquals("a key buffer past its limit is freed", 0, entry.getKeyBufferCapacityForTest());
                Assert.assertEquals("the freed buffer takes its key with it", 0, entry.getKeyLength());
                Assert.assertArrayEquals("the scalar state stays intact", scalar, entry.getScalarState());

                // The next copy allocates a buffer again.
                entry.of(source, 16, emptyBytes, emptyRefs);
                Assert.assertEquals(16, entry.getKeyLength());
                Assert.assertEquals(7, Unsafe.getByte(entry.getKeyAddress() + 15));
                Assert.assertTrue(entry.getKeyBufferCapacityForTest() > 0);
            } finally {
                Unsafe.free(source, sourceLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFlyweightTrimKeepsCachesWithinTheirRetentionLimits() throws Exception {
        // Distinct scalar widths that add up to exactly 16,777,216 bytes, and distinct reference
        // counts that add up to exactly 262,144 slots: what the scalar and reference caches of
        // an entry may each keep once an operation ends. The entry copies no key, so its key
        // buffer stays unallocated and holds none of the counted bytes.
        assertMemoryLeak(() -> {
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

            try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                final byte[][] cachedScalars = new byte[bytes.length][];
                final LiveViewCheckpointStatePageRef[] cachedRefs = new LiveViewCheckpointStatePageRef[refs.length];
                for (int i = 0; i < bytes.length; i++) {
                    LiveViewCheckpointTestKeys.of(entry, emptyBytes, bytes[i], emptyRefs);
                    cachedScalars[i] = entry.getScalarState();
                }
                for (int i = 0; i < refs.length; i++) {
                    LiveViewCheckpointTestKeys.of(entry, emptyBytes, emptyBytes, refs[i]);
                    cachedRefs[i] = entry.getStatePageRef(0);
                }
                Assert.assertEquals(0, entry.getKeyBufferCapacityForTest());
                Assert.assertEquals(byteWidthSum, entry.getRetainedBufferBytesForTest());
                Assert.assertEquals(262_144, entry.getRetainedStatePageRefCountForTest());

                // Caches at their limits keep every width across a trim.
                entry.trimWidthCaches();
                Assert.assertEquals(byteWidthSum, entry.getRetainedBufferBytesForTest());
                Assert.assertEquals(262_144, entry.getRetainedStatePageRefCountForTest());
                for (int i = 0; i < bytes.length; i++) {
                    LiveViewCheckpointTestKeys.of(entry, emptyBytes, bytes[i], emptyRefs);
                    Assert.assertSame("a scalar cache at its limit must keep its widths [width=" + byteWidths[i] + ']', cachedScalars[i], entry.getScalarState());
                }
                for (int i = 0; i < refs.length; i++) {
                    LiveViewCheckpointTestKeys.of(entry, emptyBytes, emptyBytes, refs[i]);
                    Assert.assertSame("a reference cache at its limit must keep its counts [count=" + refCounts[i] + ']', cachedRefs[i], entry.getStatePageRef(0));
                }

                // One more scalar byte takes the scalar cache past its limit: the trim drops every
                // scalar width, and the reference cache, within its own limit, keeps its counts.
                LiveViewCheckpointTestKeys.of(entry, emptyBytes, oneMoreByte, emptyRefs);
                entry.trimWidthCaches();
                Assert.assertEquals(0, entry.getRetainedBufferBytesForTest());
                Assert.assertEquals(262_144, entry.getRetainedStatePageRefCountForTest());
                for (int i = 0; i < bytes.length; i++) {
                    LiveViewCheckpointTestKeys.of(entry, emptyBytes, bytes[i], emptyRefs);
                    Assert.assertNotSame("a scalar width the trim dropped must get a fresh array [width=" + byteWidths[i] + ']', cachedScalars[i], entry.getScalarState());
                    Assert.assertArrayEquals(bytes[i], entry.getScalarState());
                    cachedScalars[i] = entry.getScalarState();
                }
                // The refill counts from zero, so the scalar cache sits at its limit again and a
                // trim keeps it.
                entry.trimWidthCaches();
                for (int i = 0; i < bytes.length; i++) {
                    LiveViewCheckpointTestKeys.of(entry, emptyBytes, bytes[i], emptyRefs);
                    Assert.assertSame("a refilled scalar cache at its limit must keep its widths [width=" + byteWidths[i] + ']', cachedScalars[i], entry.getScalarState());
                }

                // The same for the reference cache.
                LiveViewCheckpointTestKeys.of(entry, emptyBytes, emptyBytes, oneMoreRef);
                entry.trimWidthCaches();
                Assert.assertEquals(0, entry.getRetainedStatePageRefCountForTest());
                Assert.assertEquals(byteWidthSum, entry.getRetainedBufferBytesForTest());
                for (int i = 0; i < refs.length; i++) {
                    LiveViewCheckpointTestKeys.of(entry, emptyBytes, emptyBytes, refs[i]);
                    Assert.assertNotSame("a reference count the trim dropped must get fresh references [count=" + refCounts[i] + ']', cachedRefs[i], entry.getStatePageRef(0));
                    Assert.assertEquals(refCounts[i], entry.getStatePageCount());
                    Assert.assertEquals(refCounts[i], entry.getStatePageRef(refCounts[i] - 1).getSegmentId());
                    cachedRefs[i] = entry.getStatePageRef(0);
                }
                entry.trimWidthCaches();
                for (int i = 0; i < refs.length; i++) {
                    LiveViewCheckpointTestKeys.of(entry, emptyBytes, emptyBytes, refs[i]);
                    Assert.assertSame("a refilled reference cache at its limit must keep its counts [count=" + refCounts[i] + ']', cachedRefs[i], entry.getStatePageRef(0));
                }
            }
        });
    }

    @Test
    public void testLookupsCopyOutOfAMemoSlotThatIsReDecodedLater() throws Exception {
        assertMemoryLeak(() -> {
            // Minimum capacity, so the two keys sit in different leaves at the same depth and the
            // second lookup re-decodes the memo slot the first one read, reusing its arena.
            final int keyCount = 64;
            final int keyWidth = 40;
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointMutationArena initial = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 2, 2);
                 Path dir = new Path()) {
                for (int i = 0; i < keyCount; i++) {
                    LiveViewCheckpointTestKeys.put(initial, distinctBytes(i, keyWidth), key(i), refs(1));
                }
                writer.of(checkpointsDir(dir));
                writer.apply(new LiveViewCheckpointPageRef(), initial, 1, root);
            }

            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry();
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir));
                Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, root, distinctBytes(0, keyWidth), first));
                final long decodedBefore = reader.getDecodedPageCount();
                Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, root, distinctBytes(keyCount - 1, keyWidth), second));
                Assert.assertTrue(
                        "the second lookup must re-decode the leaf slot the first one read",
                        reader.getDecodedPageCount() > decodedBefore
                );
                Assert.assertArrayEquals(distinctBytes(0, keyWidth), first.copyKeyForTest());
                Assert.assertEquals(0, scalar(first));
                Assert.assertArrayEquals(distinctBytes(keyCount - 1, keyWidth), second.copyKeyForTest());
                Assert.assertEquals(keyCount - 1, scalar(second));

                // A walk hands every entry out through the reader's own scratch entry, so what a
                // lookup copied out stays put across it too.
                final int[] visited = {0};
                reader.iterateAll(root, entry -> visited[0]++);
                Assert.assertEquals(keyCount, visited[0]);
                Assert.assertArrayEquals(distinctBytes(0, keyWidth), first.copyKeyForTest());
                Assert.assertEquals(0, scalar(first));
            }
        });
    }

    @Test
    public void testReaderDetachFreesOnlyNodeArenasPastTheirRetentionLimit() throws Exception {
        assertMemoryLeak(() -> {
            // Single-leaf maps. A decoded page lives in the arena of the node that decoded it, so
            // an arena holds one page, and what it keeps is that page's size. The narrow map's
            // leaf holds 8 keys of 1,048,572 bytes, 8 MiB of images, within what a node may
            // keep once an operation ends; the wide map's leaf holds 17 keys of about 1 MiB each,
            // 17,825,656 bytes, past it. The reference map's leaf holds 3 entries of 65,536 state
            // page references each, 7.5 MiB of references, which the arena keeps as bytes.
            final long limit = LiveViewCheckpointPartitionMapReader.MAX_NODE_RETAINED_BYTES;
            final int scalarWidth = Integer.BYTES;
            final int narrowKeyCount = 8;
            final int narrowKeyWidth = 1_048_572;
            final int wideKeyCount = 17;
            final int firstWideKeyWidth = 1_048_560;
            final int refEntryCount = 3;
            final int refCount = 65_536;
            final LiveViewCheckpointPageRef narrow = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef wide = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef manyRefs = new LiveViewCheckpointPageRef();
            try (Path dir = new Path()) {
                checkpointsDir(dir);
                writeDistinctEntries(dir, 1, 0, narrowKeyCount, narrowKeyWidth, 0, scalarWidth, 0, narrow);
                writeDistinctEntries(dir, 2, 100, wideKeyCount, firstWideKeyWidth, 1, scalarWidth, 0, wide);
                writeDistinctEntries(dir, 3, 200, refEntryCount, scalarWidth, 0, scalarWidth, refCount, manyRefs);
            }

            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
                 Path dir = new Path()) {
                checkpointsDir(dir);
                // Lookups decode into the memo node and copy into the caller's entry.
                reader.of(dir);
                for (int i = 0; i < narrowKeyCount; i++) {
                    Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, narrow, distinctBytes(i, narrowKeyWidth), entry));
                    assertDistinctWideEntry(i, narrowKeyWidth, scalarWidth, entry);
                }
                final long narrowBytes = reader.getRetainedBufferBytesForTest();
                Assert.assertTrue(
                        "the memo node's arena holds the narrow page [bytes=" + narrowBytes + ']',
                        narrowBytes >= (long) narrowKeyCount * narrowKeyWidth && narrowBytes <= limit
                );
                reader.detach();
                Assert.assertEquals("an arena within its limit survives detach", narrowBytes, reader.getRetainedBufferBytesForTest());

                // References decode through the arena field for field.
                reader.of(dir);
                for (int i = 0; i < refEntryCount; i++) {
                    Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, manyRefs, distinctBytes(200 + i, scalarWidth), entry));
                    assertDistinctEntry(200 + i, scalarWidth, scalarWidth, refCount, entry);
                }
                reader.detach();
                Assert.assertTrue(
                        "an arena holding references within its limit survives detach [bytes="
                                + reader.getRetainedBufferBytesForTest() + ']',
                        reader.getRetainedBufferBytesForTest() >= (long) refEntryCount * refCount * LiveViewCheckpointStatePageRef.BYTES
                );

                reader.of(dir);
                for (int i = 0; i < wideKeyCount; i++) {
                    Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, wide, distinctBytes(100 + i, firstWideKeyWidth + i), entry));
                    assertDistinctWideEntry(100 + i, firstWideKeyWidth + i, scalarWidth, entry);
                }
                Assert.assertTrue(
                        "the memo node's arena must hold the whole wide page [bytes="
                                + reader.getLargestRetainedBufferBytesForTest() + ']',
                        reader.getLargestRetainedBufferBytesForTest() > limit
                );
                reader.detach();
                Assert.assertEquals("an arena past its limit is freed on detach", 0, reader.getRetainedBufferBytesForTest());

                // The next operation decodes into a fresh arena and still answers.
                reader.of(dir);
                Assert.assertTrue(LiveViewCheckpointTestKeys.find(reader, wide, distinctBytes(100, firstWideKeyWidth), entry));
                assertDistinctWideEntry(100, firstWideKeyWidth, scalarWidth, entry);
                reader.detach();
                Assert.assertEquals(0, reader.getRetainedBufferBytesForTest());

                // A walk decodes into a pooled node and copies every entry into the reader's scratch
                // entry, whose key buffer grows to the widest key, well within its own limit.
                reader.of(dir);
                final int[] visited = {0};
                reader.iterateAll(wide, visitedEntry -> {
                    final int i = visited[0]++;
                    assertDistinctWideEntry(100 + i, firstWideKeyWidth + i, scalarWidth, visitedEntry);
                });
                Assert.assertEquals(wideKeyCount, visited[0]);
                Assert.assertTrue(reader.getLargestRetainedBufferBytesForTest() > limit);
                reader.detach();
                final long walkBytes = reader.getRetainedBufferBytesForTest();
                Assert.assertTrue(
                        "detach must free the walk node's arena and keep only the scratch entry [bytes=" + walkBytes + ']',
                        walkBytes >= firstWideKeyWidth + wideKeyCount - 1 && walkBytes < 4L * firstWideKeyWidth
                );
            }
        });
    }

    @Test
    public void testShuffledLookupsOverRingReferenceCountsAllocateNothingOnceWarm() throws Exception {
        assertMemoryLeak(() -> {
            // A ring-shaped function keeps two state pages per chunk and up to 256 chunks per key,
            // and keys that start at different times reach different chunk counts. Here 1,024 keys
            // hold every even reference count from 2 to 512, four keys per count, in an order
            // unrelated to key order, so every leaf holds its own mix of counts. A decoded leaf keeps
            // its references in its node's native arena, and an entry keeps one reference array per
            // count it has copied, which for these keys adds up to more than 65,536 references. Each
            // pass is one operation: shuffled lookups, a full iteration, then detach(). Once the
            // entries hold every count, a pass must not allocate: an allocation-free pass measures a
            // few hundred bytes, while fresh references for every decoded entry cost hundreds of
            // megabytes.
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
                    LiveViewCheckpointTestKeys.put(arena, keys[i], keys[i], refs(refCounts[i]));
                }
                writer.apply(new LiveViewCheckpointPageRef(), arena, 1, root);
            }

            final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
            final long threadId = Thread.currentThread().threadId();
            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                checkpointsDir(dir);
                try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
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
                            if (!LiveViewCheckpointTestKeys.find(reader, root, keys[i], entry) || scalar(entry) != i || !hasRefs(entry, refCounts[i])) {
                                mismatchCount[0]++;
                            }
                        }
                        if (pass == 0) {
                            lookupRefCount = entry.getRetainedStatePageRefCountForTest();
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
                            "the lookups must have copied more than 65,536 references of distinct counts [refCount="
                                    + lookupRefCount + ']',
                            lookupRefCount > 65_536
                    );
                }
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
                    LiveViewCheckpointTestKeys.put(arena, keys[i], key(i), refs(1));
                }
                writer.apply(new LiveViewCheckpointPageRef(), arena, 1, root);
            }

            final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
            final long threadId = Thread.currentThread().threadId();
            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                checkpointsDir(dir);
                try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                    for (int pass = 0; pass < warmUpPasses + measuredPasses; pass++) {
                        final long decodedPagesBefore = reader.getDecodedPageCount();
                        final long allocatedBefore = threadMXBean.getThreadAllocatedBytes(threadId);
                        reader.of(dir);
                        int mismatchCount = 0;
                        for (int n = 0; n < keyCount; n++) {
                            final int i = order[n];
                            if (!LiveViewCheckpointTestKeys.find(reader, root, keys[i], entry) || !hasKey(entry, keys[i]) || scalar(entry) != i) {
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
            }
        });
    }

    @Test
    public void testShuffledLookupsOverWideKeyWidthsAllocateNothingOnceWarm() throws Exception {
        assertMemoryLeak(() -> {
            // 32 key widths of 4,096 to 6,080 bytes with 64 keys each. Keys of one width sort
            // together, as STRING keys of one length do, so a leaf holds up to 64 keys of a width,
            // which its node decodes into its native arena: more than 256 KiB of key images. Each
            // pass is one operation: lookups of every 16th key in shuffled order, which cross every
            // leaf, a full iteration, then detach(). Once the arenas have grown to the widest leaf, a
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
                    LiveViewCheckpointTestKeys.put(arena, keys[i], key(i), refs(1));
                }
                writer.apply(new LiveViewCheckpointPageRef(), arena, 1, root);
            }

            final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
            final long threadId = Thread.currentThread().threadId();
            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                checkpointsDir(dir);
                try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                    final int[] mismatchCount = new int[1];
                    final int[] visitedCount = new int[1];
                    final LiveViewCheckpointPartitionMapReader.Visitor visitor = visitedEntry -> {
                        final int i = visitedCount[0]++;
                        if (scalar(visitedEntry) != i || !hasKey(visitedEntry, keys[i])) {
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
                            if (!LiveViewCheckpointTestKeys.find(reader, root, keys[i], entry) || scalar(entry) != i || !hasKey(entry, keys[i])) {
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
                            "the lookups must have decoded leaves of 64 keys of at least 4 KiB [bytes="
                                    + largestLookupPoolBytes + ']',
                            largestLookupPoolBytes > 64L * 4_096
                    );
                }
            }
        });
    }

    @Test
    public void testWarmedLookupsAllocateNoHeapWhateverTheKeyWidths() throws Exception {
        assertMemoryLeak(() -> {
            // 1,024 keys of 1,024 distinct widths, 16,390 to 18,436 bytes, 17,825,792 bytes of key
            // images in all: more than one node may keep once an operation ends. A seal's lookups
            // run between of() and detach(), so a reader that images keys on the heap would pay
            // for every width again on every operation. Decoded keys live in native memory, so
            // 4,096 warmed lookups over four operations must allocate no heap at all; the bound
            // leaves room for the measurement itself and does not grow with the key count. The
            // odd operations probe with keys packed in one native block, the even ones with a
            // native copy of each key made for its probe.
            final int keyCount = 1_024;
            final int firstKeyWidth = 16_390;
            final int measuredPasses = 4;
            final long allocationLimitBytes = 16_384;
            final byte[][] keys = new byte[keyCount][];
            final int[] order = new int[keyCount];
            final long[] keyOffsets = new long[keyCount];
            long nativeKeysSize = 0;
            for (int i = 0; i < keyCount; i++) {
                keys[i] = distinctBytes(i, firstKeyWidth + 2 * i);
                order[i] = i;
                keyOffsets[i] = nativeKeysSize;
                nativeKeysSize += keys[i].length;
            }
            shuffle(order, new Rnd(42, 7));
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            try (Path dir = new Path();
                 LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration)) {
                writer.of(checkpointsDir(dir));
                for (int i = 0; i < keyCount; i++) {
                    LiveViewCheckpointTestKeys.put(arena, keys[i], key(i), refs(1));
                }
                writer.apply(new LiveViewCheckpointPageRef(), arena, 1, root);
            }

            final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
            final long threadId = Thread.currentThread().threadId();
            try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
                 Path dir = new Path()) {
                checkpointsDir(dir);
                try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                    int mismatchCount = 0;
                    // Warm-up: one whole operation, so every reusable shell exists.
                    reader.of(dir);
                    for (int n = 0; n < keyCount; n++) {
                        final int i = order[n];
                        if (!LiveViewCheckpointTestKeys.find(reader, root, keys[i], entry) || scalar(entry) != i) {
                            mismatchCount++;
                        }
                    }
                    reader.detach();
                    final long nativeKeys = Unsafe.malloc(nativeKeysSize, MemoryTag.NATIVE_DEFAULT);
                    final long allocatedBytes;
                    try {
                        for (int i = 0; i < keyCount; i++) {
                            for (int b = 0; b < keys[i].length; b++) {
                                Unsafe.putByte(nativeKeys + keyOffsets[i] + b, keys[i][b]);
                            }
                        }
                        final long allocatedBefore = threadMXBean.getThreadAllocatedBytes(threadId);
                        for (int pass = 0; pass < measuredPasses; pass++) {
                            final boolean isNativeProbe = (pass & 1) == 1;
                            reader.of(dir);
                            for (int n = 0; n < keyCount; n++) {
                                final int i = order[n];
                                final boolean isFound = isNativeProbe
                                        ? reader.find(root, nativeKeys + keyOffsets[i], keys[i].length, entry)
                                        : LiveViewCheckpointTestKeys.find(reader, root, keys[i], entry);
                                if (!isFound || scalar(entry) != i || !hasKey(entry, keys[i])) {
                                    mismatchCount++;
                                }
                            }
                            reader.detach();
                        }
                        allocatedBytes = threadMXBean.getThreadAllocatedBytes(threadId) - allocatedBefore;
                    } finally {
                        Unsafe.free(nativeKeys, nativeKeysSize, MemoryTag.NATIVE_DEFAULT);
                    }
                    Assert.assertEquals(0, mismatchCount);
                    Assert.assertTrue(
                            "warmed lookups must not allocate heap [probes=" + measuredPasses * keyCount
                                    + ", allocatedBytes=" + allocatedBytes + ", limit=" + allocationLimitBytes + ']',
                            allocatedBytes <= allocationLimitBytes
                    );
                }
            }
        });
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
        Assert.assertArrayEquals("key [id=" + id + ']', distinctBytes(id, keyWidth), entry.copyKeyForTest());
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
        final byte[] key = entry.copyKeyForTest();
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

    // Tells whether entry holds exactly the bytes of key, reading its native key in place, so
    // the measured lookups allocate nothing to check what they found.
    private static boolean hasKey(LiveViewCheckpointPartitionMapEntry entry, byte[] key) {
        if (entry.getKeyLength() != key.length) {
            return false;
        }
        final long address = entry.getKeyAddress();
        for (int i = 0; i < key.length; i++) {
            if (Unsafe.getByte(address + i) != key[i]) {
                return false;
            }
        }
        return true;
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
        LiveViewCheckpointTestKeys.put(arena, key(key), scalar, new LiveViewCheckpointStatePageRef[]{ref});
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
                LiveViewCheckpointTestKeys.put(arena, distinctBytes(id, firstKeyWidth + i * keyWidthStep), distinctBytes(id, scalarWidth), distinctRefs(id, refCount));
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
            try (LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry()) {
                LiveViewCheckpointTestKeys.find(reader, root, key(0), entry);
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
