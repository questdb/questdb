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
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointRingSeal;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryEntry;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryWriter;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.Zip;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.TreeMap;
import java.util.TreeSet;

public class LiveViewCheckpointSegmentDirectoryTest extends AbstractCairoTest {

    private static final String LV_DIR = "lv_segment_directory";

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            final FilesFacade ff = configuration.getFilesFacade();
            checkpointsDir(path).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
            ff.mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testAppendCostIsFlatAsTheCatalogueGrows() throws Exception {
        assertMemoryLeak(() -> {
            // The reason this structure replaced the flat page: a publication must
            // cost what it changed, not what the catalogue holds. Both catalogues
            // below take one new segment; the small one and the one a hundred times
            // larger must pay the same handful of pages for it.
            final long smallPages = appendCost(100, 1_000);
            final long largePages = appendCost(10_000, 2_000);
            Assert.assertTrue(
                    "one append wrote " + smallPages + " pages over 100 segments and "
                            + largePages + " over 10000",
                    largePages <= smallPages + 2
            );
        });
    }

    @Test
    public void testBoundedMetaStoreValidationFallsBackOnCorruptDirectory() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef generation1Root = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef generation2Root = new LiveViewCheckpointPageRef();
            try (
                    LiveViewCheckpointSegmentDirectoryWriter writer = openWriter();
                    LiveViewCheckpointMetaStore store = openStore()
            ) {
                writer.begin(generation1Root);
                writer.addSegment(100, 4096, 1); // data file deliberately absent
                writer.publish(1, generation1Root);
                publish(store, 1, generation1Root);

                writer.begin(generation1Root);
                writer.addSegment(101, 8192, 1);
                writer.publish(2, generation2Root);
                publish(store, 2, generation2Root);
            }
            corruptPageKind(generation2Root, 0x7fff_ffff);

            try (LiveViewCheckpointMetaStore store = openStore();
                 LiveViewCheckpointGenerationPin pin = store.pin()) {
                Assert.assertEquals(1, pin.getGeneration());
                Assert.assertEquals(1, store.getSuperblock().generation);
            }
        });
    }

    @Test
    public void testExactSharedRootReferenceAccountingRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointSegmentDirectoryWriter writer = openWriter()) {
                writer.begin(root);
                writer.addSegment(3, 30, 1);
                writer.addSegment(1, 10, 2);
                writer.addSegment(2, 20, 1);

                final LongList removed = new LongList();
                final LongList added = new LongList();
                removed.add(1);
                removed.add(999);
                try {
                    writer.applyRootReferenceChanges(removed, added, 6);
                    Assert.fail("expected unknown removed segment rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "unknown");
                }
                Assert.assertEquals(2, writer.getReferenceCount(1));
                Assert.assertEquals(1, writer.getReferenceCount(2));
                Assert.assertEquals(1, writer.getReferenceCount(3));

                removed.clear();
                added.add(-1);
                try {
                    writer.applyRootReferenceChanges(removed, added, 6);
                    Assert.fail("expected negative added segment rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "must be non-negative");
                }
                Assert.assertEquals(1, writer.getReferenceCount(3));

                added.clear();
                removed.add(1);
                removed.add(1); // two pages in one removed root still count once
                removed.add(2);
                removed.add(2);
                added.add(3);
                added.add(3);
                writer.applyRootReferenceChanges(removed, added, 7);

                Assert.assertEquals(1, writer.getReferenceCount(1));
                Assert.assertEquals(0, writer.getReferenceCount(2));
                Assert.assertEquals(7, writer.getRetireGeneration(2));
                Assert.assertEquals(2, writer.getReferenceCount(3));

                removed.clear();
                removed.add(3);
                added.clear();
                added.add(2);
                writer.applyRootReferenceChanges(removed, added, 8);
                Assert.assertEquals(1, writer.getReferenceCount(2));
                Assert.assertEquals(
                        LiveViewCheckpointSegmentDirectory.RETIRE_GENERATION_NONE,
                        writer.getRetireGeneration(2)
                );
                Assert.assertEquals(1, writer.getReferenceCount(3));
                writer.publish(4, root);
            }

            try (LiveViewCheckpointSegmentDirectoryReader restored = openReader(root)) {
                Assert.assertEquals(3, restored.size());
                Assert.assertEquals(10, restored.getFileLength(1));
                Assert.assertEquals(1, restored.getReferenceCount(1));
                Assert.assertEquals(1, restored.getReferenceCount(2));
                Assert.assertEquals(1, restored.getReferenceCount(3));
                Assert.assertEquals(60, restored.getReferencedBytes());
                Assert.assertEquals(0, restored.getObsoleteBytes());
                Assert.assertEquals(3, restored.lastSegmentId());
            }
        });
    }

    @Test
    public void testRandomReferenceAccountingProperty() throws Exception {
        assertMemoryLeak(() -> {
            // Node capacity four, so forty segments stand three levels tall and the
            // random walk keeps crossing node boundaries, splits and reuse.
            final int segmentCount = 40;
            final long[] counts = new long[segmentCount];
            final long[] retireGenerations = new long[segmentCount];
            final Rnd rnd = new Rnd();
            final LongList removed = new LongList();
            final LongList added = new LongList();
            final boolean[] touched = new boolean[segmentCount];
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            long metadataSegmentId = 0;

            try (LiveViewCheckpointSegmentDirectoryWriter writer = openWriter(4, 4)) {
                writer.begin(root);
                for (int i = 0; i < segmentCount; i++) {
                    counts[i] = 1 + rnd.nextPositiveInt() % 4;
                    retireGenerations[i] = LiveViewCheckpointSegmentDirectory.RETIRE_GENERATION_NONE;
                    writer.addSegment(i, 100 + i, counts[i]);
                }
                writer.publish(metadataSegmentId++, root);

                for (int generation = 1; generation <= 200; generation++) {
                    removed.clear();
                    added.clear();
                    java.util.Arrays.fill(touched, false);
                    final int removeAttempts = rnd.nextPositiveInt() % 8;
                    for (int i = 0; i < removeAttempts; i++) {
                        final int segment = rnd.nextPositiveInt() % segmentCount;
                        if (counts[segment] > 0) {
                            removed.add(segment);
                            removed.add(segment);
                            touched[segment] = true;
                        }
                    }
                    for (int i = 0; i < segmentCount; i++) {
                        if (touched[i]) {
                            counts[i]--;
                            if (counts[i] == 0) {
                                retireGenerations[i] = generation;
                            }
                        }
                    }

                    java.util.Arrays.fill(touched, false);
                    final int addAttempts = rnd.nextPositiveInt() % 8;
                    for (int i = 0; i < addAttempts; i++) {
                        final int segment = rnd.nextPositiveInt() % segmentCount;
                        added.add(segment);
                        added.add(segment);
                        touched[segment] = true;
                    }
                    for (int i = 0; i < segmentCount; i++) {
                        if (touched[i]) {
                            if (counts[i] == 0) {
                                retireGenerations[i] = LiveViewCheckpointSegmentDirectory.RETIRE_GENERATION_NONE;
                            }
                            counts[i]++;
                        }
                    }

                    writer.begin(root);
                    writer.applyRootReferenceChanges(removed, added, generation);
                    for (int i = 0; i < segmentCount; i++) {
                        Assert.assertEquals(counts[i], writer.getReferenceCount(i));
                        Assert.assertEquals(retireGenerations[i], writer.getRetireGeneration(i));
                    }
                    writer.publish(metadataSegmentId++, root);

                    // Every generation is read back through its own published root,
                    // so a splice that corrupted a reused subtree cannot survive to
                    // the end of the walk.
                    try (LiveViewCheckpointSegmentDirectoryReader restored = openReader(root)) {
                        Assert.assertEquals(segmentCount, restored.size());
                        for (int i = 0; i < segmentCount; i++) {
                            Assert.assertEquals(counts[i], restored.getReferenceCount(i));
                            Assert.assertEquals(retireGenerations[i], restored.getRetireGeneration(i));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testRandomizedInsertsAndUpdatesAgainstAnOracle() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd();
            final TreeMap<Long, Long> oracle = new TreeMap<>();
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            final LongList removed = new LongList();
            final LongList added = new LongList();
            final TreeSet<Long> touched = new TreeSet<>();
            long metadataSegmentId = 0;
            long nextSegmentId = 0;

            try (LiveViewCheckpointSegmentDirectoryWriter writer = openWriter(3, 3)) {
                for (int generation = 1; generation <= 120; generation++) {
                    writer.begin(root);
                    // A publication takes a few new segments and re-references a few
                    // existing ones, so inserts and in-place updates land in the same
                    // copy-on-write pass and can split a node the other one touched.
                    final int inserts = 1 + rnd.nextPositiveInt() % 5;
                    for (int i = 0; i < inserts; i++) {
                        final long segmentId = nextSegmentId++;
                        writer.addSegment(segmentId, 64 + segmentId, 1);
                        oracle.put(segmentId, 1L);
                    }
                    removed.clear();
                    added.clear();
                    touched.clear();
                    final int touches = rnd.nextPositiveInt() % 6;
                    for (int i = 0; i < touches; i++) {
                        touched.add(rnd.nextPositiveLong() % nextSegmentId);
                    }
                    // Repeated ids stand for one root naming a segment from several
                    // of its pages: the transaction counts them once.
                    for (long segmentId : touched) {
                        added.add(segmentId);
                        added.add(segmentId);
                        oracle.put(segmentId, oracle.get(segmentId) + 1);
                    }
                    if (added.size() > 0) {
                        writer.applyRootReferenceChanges(removed, added, generation);
                    }
                    writer.publish(metadataSegmentId++, root);

                    try (LiveViewCheckpointSegmentDirectoryReader reader = openReader(root)) {
                        final LongList seen = new LongList();
                        reader.iterateAll(entry -> {
                            seen.add(entry.segmentId);
                            Assert.assertEquals(64 + entry.segmentId, entry.fileLength);
                            Assert.assertEquals(
                                    (long) oracle.get(entry.segmentId),
                                    entry.referenceCount
                            );
                        });
                        Assert.assertEquals(oracle.size(), seen.size());
                        for (int i = 1; i < seen.size(); i++) {
                            Assert.assertTrue(
                                    "the scan must stay ordered",
                                    seen.getQuick(i - 1) < seen.getQuick(i)
                            );
                        }
                        Assert.assertEquals((long) oracle.lastKey(), reader.lastSegmentId());
                    }
                }
            }
        });
    }

    @Test
    public void testRepeatedLookupsDoNotOutliveTheirRoot() throws Exception {
        assertMemoryLeak(() -> {
            // One restore resolves the same handful of segment ids over and over, so
            // the reader memoises what the bound root answered. The memo has to key
            // on the id rather than on the slot it lands in, and a root published
            // after it may not be answered out of it.
            final long collidingStride = Numbers.ceilPow2(LiveViewCheckpointRingSeal.MAX_LIVE_CHUNKS);
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointSegmentDirectoryWriter writer = openWriter()) {
                writer.begin(root);
                writer.addSegment(5, 50, 1);
                writer.addSegment(5 + collidingStride, 70, 1);
                writer.publish(1, root);
            }

            final LiveViewCheckpointPageRef nextRoot = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointSegmentDirectoryWriter writer = openWriter()) {
                writer.begin(root);
                final LongList removed = new LongList();
                final LongList added = new LongList();
                added.add(5);
                writer.applyRootReferenceChanges(removed, added, 9);
                writer.publish(2, nextRoot);
            }

            final LiveViewCheckpointSegmentDirectoryEntry entry = new LiveViewCheckpointSegmentDirectoryEntry();
            try (LiveViewCheckpointSegmentDirectoryReader reader = openReader(root)) {
                // Two ids one cache span apart share a slot; each repeat must answer
                // with its own entry.
                for (int i = 0; i < 3; i++) {
                    Assert.assertEquals(50, reader.getFileLength(5));
                    Assert.assertEquals(70, reader.getFileLength(5 + collidingStride));
                    Assert.assertEquals(1, reader.getReferenceCount(5));
                    Assert.assertFalse(reader.find(6, entry));
                }

                try (Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), nextRoot);
                }
                Assert.assertEquals(2, reader.getReferenceCount(5));
                Assert.assertEquals(50, reader.getFileLength(5));

                reader.clear();
                Assert.assertFalse(reader.find(5, entry));
            }
        });
    }

    @Test
    public void testStructurallyCorruptDirectoriesRejected() throws Exception {
        assertMemoryLeak(() -> {
            assertRawDirectoryRejected(20, 0x66, mem -> mem.putInt(0), "page kind unknown");
            assertRawDirectoryRejected(21, LiveViewCheckpointSegmentDirectory.PAGE_KIND_LEAF, mem -> {
                mem.putInt(-1);
            }, "count negative");
            assertRawDirectoryRejected(22, LiveViewCheckpointSegmentDirectory.PAGE_KIND_LEAF, mem -> {
                mem.putInt(0);
                mem.putLong(99);
            }, "payload length mismatch");
            assertRawDirectoryRejected(23, LiveViewCheckpointSegmentDirectory.PAGE_KIND_LEAF, mem -> {
                mem.putInt(2);
                putEntry(mem, 5, 10, 1, -1);
                putEntry(mem, 5, 20, 1, -1);
            }, "ids not strictly increasing");
            assertRawDirectoryRejected(24, LiveViewCheckpointSegmentDirectory.PAGE_KIND_LEAF, mem -> {
                mem.putInt(1);
                putEntry(mem, 5, 10, 0, -1);
            }, "retirement state invalid");
            assertRawDirectoryRejected(25, LiveViewCheckpointSegmentDirectory.PAGE_KIND_LEAF, mem -> {
                mem.putInt(1);
                putEntry(mem, 5, 10, 1, 7);
            }, "retirement state invalid");
            assertRawDirectoryRejected(26, LiveViewCheckpointSegmentDirectory.PAGE_KIND_LEAF, mem -> {
                mem.putInt(1);
                putEntry(mem, 5, 0, 1, -1);
            }, "entry value invalid");
            assertRawDirectoryRejected(27, LiveViewCheckpointSegmentDirectory.PAGE_KIND_INTERNAL, mem -> {
                mem.putInt(2);
                putChild(mem, 9, 1, 0, 24);
                putChild(mem, 9, 1, 24, 24);
            }, "ids not strictly increasing");
        });
    }

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private static void copy(LiveViewCheckpointPageRef from, LiveViewCheckpointPageRef to) {
        to.of(from.getSegmentId(), from.getOffset(), from.getLength());
    }

    private static LiveViewCheckpointSegmentDirectoryReader openReader(LiveViewCheckpointPageRef root) {
        final LiveViewCheckpointSegmentDirectoryReader reader =
                new LiveViewCheckpointSegmentDirectoryReader(configuration);
        try (Path dir = new Path()) {
            reader.of(checkpointsDir(dir), root);
        }
        return reader;
    }

    private static LiveViewCheckpointSegmentDirectoryWriter openWriter() {
        return openWriter(64, 64);
    }

    private static LiveViewCheckpointSegmentDirectoryWriter openWriter(int leafCapacity, int internalCapacity) {
        final LiveViewCheckpointSegmentDirectoryWriter writer =
                new LiveViewCheckpointSegmentDirectoryWriter(configuration, leafCapacity, internalCapacity);
        try (Path dir = new Path()) {
            writer.of(checkpointsDir(dir));
        }
        return writer;
    }

    private static void putChild(MemoryA mem, long minSegmentId, long segmentId, long offset, int length) {
        mem.putLong(minSegmentId);
        mem.putLong(segmentId);
        mem.putLong(offset);
        mem.putInt(length);
    }

    private static void putEntry(MemoryA mem, long segmentId, long fileLength, long count, long retireGeneration) {
        mem.putLong(segmentId);
        mem.putLong(fileLength);
        mem.putLong(count);
        mem.putLong(retireGeneration);
    }

    /**
     * Publishes {@code segmentCount} segments, then measures what one further
     * segment costs: the pages the copy-on-write append writes into its own
     * metadata segment.
     */
    private long appendCost(int segmentCount, long metadataSegmentId) {
        final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointSegmentDirectoryWriter writer = openWriter()) {
            writer.begin(root);
            for (int i = 0; i < segmentCount; i++) {
                writer.addSegment(i, 100 + i, 1);
            }
            writer.publish(metadataSegmentId, root);

            writer.begin(root);
            writer.addSegment(segmentCount, 4096, 1);
            writer.publish(metadataSegmentId + 1, root);
            final int pages = writer.getLastSegmentPageCount();
            try (LiveViewCheckpointSegmentDirectoryReader reader = openReader(root)) {
                Assert.assertEquals(segmentCount + 1L, reader.size());
                Assert.assertEquals(4096, reader.getFileLength(segmentCount));
            }
            return pages;
        }
    }

    private void assertRawDirectoryRejected(
            long metadataSegmentId,
            int pageKind,
            PageBodyWriter bodyWriter,
            CharSequence expectedMessage
    ) {
        final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointMetaSegmentWriter writer = new LiveViewCheckpointMetaSegmentWriter(configuration);
             Path dir = new Path()) {
            writer.of(checkpointsDir(dir), metadataSegmentId);
            final MemoryA mem = writer.beginPage(pageKind);
            bodyWriter.write(mem);
            writer.endPage(root);
            writer.commit();
        }
        try (LiveViewCheckpointSegmentDirectoryReader directory =
                     new LiveViewCheckpointSegmentDirectoryReader(configuration);
             Path dir = new Path()) {
            try {
                directory.of(checkpointsDir(dir), root);
                Assert.fail("expected corrupt segment directory rejection");
            } catch (CairoException e) {
                Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
            }
        }
    }

    private void corruptPageKind(LiveViewCheckpointPageRef ref, int kind) {
        try (Path path = new Path(); Path dir = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
            LiveViewCheckpointLayout.metaSegmentPath(path, checkpointsDir(dir), ref.getSegmentId());
            mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
            mem.putInt(ref.getOffset() + LiveViewCheckpointLayout.PAGE_KIND_OFFSET, kind);
            final long crcStart = ref.getOffset() + LiveViewCheckpointLayout.PAGE_LENGTH_OFFSET;
            final int crc = Zip.crc32(
                    0,
                    mem.addressOf(crcStart),
                    ref.getLength() - LiveViewCheckpointLayout.PAGE_LENGTH_OFFSET
            );
            mem.putInt(ref.getOffset() + LiveViewCheckpointLayout.PAGE_CRC_OFFSET, crc);
        }
    }

    private LiveViewCheckpointMetaStore openStore() {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        try (Path dir = new Path()) {
            store.of(checkpointsDir(dir));
        }
        return store;
    }

    private void publish(LiveViewCheckpointMetaStore store, long generation, LiveViewCheckpointPageRef directoryRoot) {
        final LiveViewCheckpointSuperblock superblock = store.getSuperblock();
        superblock.generation = generation;
        superblock.timelineRootRef.clear();
        superblock.rowPositionDeltaRootRef.clear();
        copy(directoryRoot, superblock.segmentDirectoryRootRef);
        superblock.nextSegmentId = generation + 100;
        superblock.dataBytes = generation * 1000;
        store.publish();
    }

    @FunctionalInterface
    private interface PageBodyWriter {
        void write(MemoryA mem);
    }
}
