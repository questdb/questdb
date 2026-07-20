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
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Zip;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    public void testBoundedMetaStoreValidationFallsBackOnCorruptDirectory() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef generation2Root = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointSegmentDirectory directory = new LiveViewCheckpointSegmentDirectory(configuration);
                 LiveViewCheckpointMetaStore store = openStore()) {
                directory.addSegment(100, 4096, 1); // data file deliberately absent
                final LiveViewCheckpointPageRef generation1Root = writeDirectory(directory, 1);
                publish(store, 1, generation1Root);

                directory.addSegment(101, 8192, 1);
                copy(writeDirectory(directory, 2), generation2Root);
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
            final LiveViewCheckpointPageRef root;
            try (LiveViewCheckpointSegmentDirectory directory = new LiveViewCheckpointSegmentDirectory(configuration)) {
                directory.addSegment(3, 30, 1);
                directory.addSegment(1, 10, 2);
                directory.addSegment(2, 20, 1);

                final LongList removed = new LongList();
                final LongList added = new LongList();
                removed.add(1);
                removed.add(999);
                try {
                    directory.applyRootReferenceChanges(removed, added, 6);
                    Assert.fail("expected unknown removed segment rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "unknown");
                }
                Assert.assertEquals(2, directory.getReferenceCount(1));
                Assert.assertEquals(1, directory.getReferenceCount(2));
                Assert.assertEquals(1, directory.getReferenceCount(3));

                removed.clear();
                added.add(-1);
                try {
                    directory.applyRootReferenceChanges(removed, added, 6);
                    Assert.fail("expected negative added segment rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "must be non-negative");
                }
                Assert.assertEquals(1, directory.getReferenceCount(3));

                added.clear();
                removed.add(1);
                removed.add(1); // two pages in one removed root still count once
                removed.add(2);
                removed.add(2);
                added.add(3);
                added.add(3);
                directory.applyRootReferenceChanges(removed, added, 7);

                Assert.assertEquals(1, directory.getReferenceCount(1));
                Assert.assertEquals(0, directory.getReferenceCount(2));
                Assert.assertEquals(7, directory.getRetireGeneration(2));
                Assert.assertEquals(2, directory.getReferenceCount(3));
                Assert.assertEquals(40, directory.getReferencedBytes());
                Assert.assertEquals(20, directory.getObsoleteBytes());

                removed.clear();
                removed.add(3);
                added.clear();
                added.add(2);
                directory.applyRootReferenceChanges(removed, added, 8);
                Assert.assertEquals(1, directory.getReferenceCount(2));
                Assert.assertEquals(LiveViewCheckpointSegmentDirectory.RETIRE_GENERATION_NONE, directory.getRetireGeneration(2));
                Assert.assertEquals(1, directory.getReferenceCount(3));
                root = writeDirectory(directory, 4);
            }

            try (LiveViewCheckpointSegmentDirectory restored = new LiveViewCheckpointSegmentDirectory(configuration);
                 Path dir = new Path()) {
                restored.of(checkpointsDir(dir), root);
                Assert.assertEquals(3, restored.size());
                Assert.assertEquals(10, restored.getFileLength(1));
                Assert.assertEquals(1, restored.getReferenceCount(1));
                Assert.assertEquals(1, restored.getReferenceCount(2));
                Assert.assertEquals(1, restored.getReferenceCount(3));
                Assert.assertEquals(60, restored.getReferencedBytes());
                Assert.assertEquals(0, restored.getObsoleteBytes());
            }
        });
    }

    @Test
    public void testRandomReferenceAccountingProperty() throws Exception {
        assertMemoryLeak(() -> {
            final int segmentCount = 40;
            final long[] counts = new long[segmentCount];
            final long[] retireGenerations = new long[segmentCount];
            final Rnd rnd = new Rnd();
            final LongList removed = new LongList();
            final LongList added = new LongList();
            final boolean[] touched = new boolean[segmentCount];

            try (LiveViewCheckpointSegmentDirectory directory = new LiveViewCheckpointSegmentDirectory(configuration)) {
                for (int i = 0; i < segmentCount; i++) {
                    counts[i] = 1 + rnd.nextPositiveInt() % 4;
                    retireGenerations[i] = LiveViewCheckpointSegmentDirectory.RETIRE_GENERATION_NONE;
                    directory.addSegment(i, 100 + i, counts[i]);
                }

                for (int generation = 1; generation <= 500; generation++) {
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

                    directory.applyRootReferenceChanges(removed, added, generation);
                    for (int i = 0; i < segmentCount; i++) {
                        Assert.assertEquals(counts[i], directory.getReferenceCount(i));
                        Assert.assertEquals(retireGenerations[i], directory.getRetireGeneration(i));
                    }
                }

                final LiveViewCheckpointPageRef root = writeDirectory(directory, 9);
                try (LiveViewCheckpointSegmentDirectory restored = new LiveViewCheckpointSegmentDirectory(configuration);
                     Path dir = new Path()) {
                    restored.of(checkpointsDir(dir), root);
                    for (int i = 0; i < segmentCount; i++) {
                        Assert.assertEquals(counts[i], restored.getReferenceCount(i));
                        Assert.assertEquals(retireGenerations[i], restored.getRetireGeneration(i));
                    }
                }
            }
        });
    }

    @Test
    public void testStructurallyCorruptDirectoriesRejected() throws Exception {
        assertMemoryLeak(() -> {
            assertRawDirectoryRejected(20, 0x66, mem -> {
                mem.putInt(1);
                mem.putInt(0);
            }, "page kind unknown");
            assertRawDirectoryRejected(21, LiveViewCheckpointSegmentDirectory.PAGE_KIND, mem -> {
                mem.putInt(1);
                mem.putInt(-1);
            }, "count negative");
            assertRawDirectoryRejected(22, LiveViewCheckpointSegmentDirectory.PAGE_KIND, mem -> {
                mem.putInt(1);
                mem.putInt(0);
                mem.putLong(99);
            }, "payload length mismatch");
            assertRawDirectoryRejected(23, LiveViewCheckpointSegmentDirectory.PAGE_KIND, mem -> {
                mem.putInt(1);
                mem.putInt(2);
                putEntry(mem, 5, 10, 1, -1);
                putEntry(mem, 5, 20, 1, -1);
            }, "ids not strictly increasing");
            assertRawDirectoryRejected(24, LiveViewCheckpointSegmentDirectory.PAGE_KIND, mem -> {
                mem.putInt(1);
                mem.putInt(1);
                putEntry(mem, 5, 10, 0, -1);
            }, "retirement state invalid");
            assertRawDirectoryRejected(25, LiveViewCheckpointSegmentDirectory.PAGE_KIND, mem -> {
                mem.putInt(1);
                mem.putInt(1);
                putEntry(mem, 5, 10, 1, 7);
            }, "retirement state invalid");
        });
    }

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private static void copy(LiveViewCheckpointPageRef from, LiveViewCheckpointPageRef to) {
        to.of(from.getSegmentId(), from.getOffset(), from.getLength());
    }

    private static void putEntry(MemoryA mem, long segmentId, long fileLength, long count, long retireGeneration) {
        mem.putLong(segmentId);
        mem.putLong(fileLength);
        mem.putLong(count);
        mem.putLong(retireGeneration);
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
        try (LiveViewCheckpointSegmentDirectory directory = new LiveViewCheckpointSegmentDirectory(configuration);
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

    private LiveViewCheckpointPageRef writeDirectory(
            LiveViewCheckpointSegmentDirectory directory,
            long metadataSegmentId
    ) {
        final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointMetaSegmentWriter writer = new LiveViewCheckpointMetaSegmentWriter(configuration);
             Path dir = new Path()) {
            writer.of(checkpointsDir(dir), metadataSegmentId);
            directory.writeTo(writer, root);
            writer.commit();
        }
        return root;
    }

    @FunctionalInterface
    private interface PageBodyWriter {
        void write(MemoryA mem);
    }
}
