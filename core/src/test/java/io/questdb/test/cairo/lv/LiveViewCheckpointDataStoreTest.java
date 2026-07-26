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
import io.questdb.cairo.lv.LiveViewCheckpointDataSegmentReader;
import io.questdb.cairo.lv.LiveViewCheckpointDataSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointDataStore;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryWriter;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;

public class LiveViewCheckpointDataStoreTest extends AbstractCairoTest {

    private static final int PAGE_KIND = 0x41;
    private static final String LV_DIR = "lv_data_store";

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            final FilesFacade ff = configuration.getFilesFacade();
            checkpointsDir(path).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
            ff.mkdirs(path, configuration.getMkDirMode());
            checkpointsDir(path).concat(LiveViewCheckpointLayout.DATA_DIR_NAME).slash();
            ff.mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testAbandonedCollidingAndCorruptRepackCleanup() throws Exception {
        assertMemoryLeak(() -> {
            final DataSegment source = writeDataSegment(1, 11, 22);
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointMetaStore metaStore = openMetaStore();
                 LiveViewCheckpointDataStore dataStore = openDataStore(metaStore)) {
                directory.addSegment(1, source.fileLength, 1);
                directory.publish(metaStore, 1, 101);

                final ObjList<LiveViewCheckpointStatePageRef> sourceRefs = new ObjList<>();
                sourceRefs.add(source.refs.getQuick(0));
                final ObjList<LiveViewCheckpointStatePageRef> targetRefs = new ObjList<>();
                try (LiveViewCheckpointDataStore.Candidate candidate = dataStore.beginCandidate()) {
                    Assert.assertEquals(Integer.BYTES, candidate.repack(2, sourceRefs, targetRefs));
                    Assert.assertTrue(dataFileExists(2));
                    // Simulates a failure after final data rename but before the
                    // metadata generation commit point.
                }
                Assert.assertFalse(dataFileExists(2));
                Assert.assertTrue(dataFileExists(1));

                writeDataSegment(3, 33);
                try (LiveViewCheckpointDataStore.Candidate candidate = dataStore.beginCandidate()) {
                    try {
                        candidate.repack(3, sourceRefs, targetRefs);
                        Assert.fail("expected existing target rejection");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "already published");
                    }
                }
                Assert.assertTrue(dataFileExists(3));

                final LiveViewCheckpointStatePageRef sentinel = stateRef(99, 0, Integer.BYTES);
                targetRefs.clear();
                targetRefs.add(sentinel);
                sourceRefs.clear();
                sourceRefs.add(stateRef(1, source.fileLength, Integer.BYTES));
                try (LiveViewCheckpointDataStore.Candidate candidate = dataStore.beginCandidate()) {
                    try {
                        candidate.repack(4, sourceRefs, targetRefs);
                        Assert.fail("expected malformed source reference rejection");
                    } catch (CairoException e) {
                        Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                        TestUtils.assertContains(e.getFlyweightMessage(), "range out of bounds");
                    }
                    try {
                        candidate.markPublished();
                        Assert.fail("expected failed candidate publication rejection");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "candidate has failed");
                    }
                    // Output is all-or-nothing even though the target temp file
                    // was opened before source validation failed.
                    Assert.assertEquals(1, targetRefs.size());
                    Assert.assertSame(sentinel, targetRefs.getQuick(0));
                }
                Assert.assertFalse(dataFileExists(4));
                Assert.assertFalse(dataTmpFileExists(4));
            }
        });
    }

    @Test
    public void testCandidateOwnershipBlocksPurgeAcrossPublication() throws Exception {
        assertMemoryLeak(() -> {
            final DataSegment source = writeDataSegment(1, 55);
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointMetaStore metaStore = openMetaStore();
                 LiveViewCheckpointDataStore dataStore = openDataStore(metaStore)) {
                directory.addSegment(1, source.fileLength, 1);
                directory.publish(metaStore, 1, 101);
                final ObjList<LiveViewCheckpointStatePageRef> sourceRefs = new ObjList<>();
                sourceRefs.add(source.refs.getQuick(0));
                final ObjList<LiveViewCheckpointStatePageRef> targetRefs = new ObjList<>();

                try (LiveViewCheckpointDataStore.Candidate candidate = dataStore.beginCandidate()) {
                    final long targetLength = candidate.repack(2, sourceRefs, targetRefs);
                    directory.addSegment(2, targetLength, 1);
                    final LongList removed = new LongList();
                    removed.add(1);
                    final LongList added = new LongList();
                    added.add(2);
                    directory.applyRootReferenceChanges(removed, added, 2);
                    directory.publish(metaStore, 2, 102);
                    candidate.markPublished();
                    directory.publish(metaStore, 3, 103);

                    // Both slots now say the source is retired and there are no
                    // old reader pins. Candidate ownership alone protects it.
                    assertNoPurge(dataStore.purge());
                    Assert.assertTrue(dataFileExists(1));
                }

                final LiveViewCheckpointDataStore.PurgeResult purged = dataStore.purge();
                Assert.assertEquals(1, purged.getPurgedSegmentCount());
                Assert.assertFalse(dataFileExists(1));
                Assert.assertTrue(dataFileExists(2));
            }
        });
    }

    @Test
    public void testPurgeRetriesFailedUnlink() throws Exception {
        final boolean[] failNextRemove = {true};
        final TestFilesFacadeImpl failingFacade = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (failNextRemove[0]) {
                    failNextRemove[0] = false;
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(failingFacade, () -> {
            final DataSegment source = writeDataSegment(1, 77);
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointMetaStore metaStore = openMetaStore();
                 LiveViewCheckpointDataStore dataStore = openDataStore(metaStore)) {
                directory.addSegment(1, source.fileLength, 1);
                directory.publish(metaStore, 1, 101);
                directory.retire(1, 2);
                directory.publish(metaStore, 2, 102);
                directory.publish(metaStore, 3, 103); // overwrites generation 1 fallback

                final LiveViewCheckpointDataStore.PurgeResult failed = dataStore.purge();
                Assert.assertEquals(0, failed.getPurgedSegmentCount());
                Assert.assertEquals(1, failed.getFailedSegmentCount());
                Assert.assertTrue(dataFileExists(1));

                final LiveViewCheckpointDataStore.PurgeResult retried = dataStore.purge();
                Assert.assertEquals(1, retried.getPurgedSegmentCount());
                Assert.assertEquals(0, retried.getFailedSegmentCount());
                Assert.assertEquals(source.fileLength, retried.getPurgedBytes());
                Assert.assertFalse(dataFileExists(1));
            }
        });
    }

    @Test
    public void testRepackPreservesBytesSharingAndGenerationSafety() throws Exception {
        assertMemoryLeak(() -> {
            final DataSegment first = writeDataSegment(1, 11, 22);
            final DataSegment second = writeDataSegment(2, 33);
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointMetaStore metaStore = openMetaStore();
                 LiveViewCheckpointDataStore dataStore = openDataStore(metaStore)) {
                directory.addSegment(1, first.fileLength, 1);
                directory.addSegment(2, second.fileLength, 1);
                directory.publish(metaStore, 1, 101);

                final LiveViewCheckpointGenerationPin oldPin = metaStore.pin();
                try {
                    final ObjList<LiveViewCheckpointStatePageRef> sourceRefs = new ObjList<>();
                    sourceRefs.add(first.refs.getQuick(0));
                    sourceRefs.add(first.refs.getQuick(1));
                    sourceRefs.add(first.refs.getQuick(0)); // shared physical page
                    sourceRefs.add(second.refs.getQuick(0));
                    final ObjList<LiveViewCheckpointStatePageRef> targetRefs = new ObjList<>();
                    final long compactedLength;
                    try (LiveViewCheckpointDataStore.Candidate candidate = dataStore.beginCandidate()) {
                        compactedLength = candidate.repack(3, sourceRefs, targetRefs);
                        Assert.assertEquals(3L * Integer.BYTES, compactedLength);
                        Assert.assertEquals(4, targetRefs.size());
                        assertSamePhysicalPage(targetRefs.getQuick(0), targetRefs.getQuick(2));
                        Assert.assertEquals(11, readInt(3, compactedLength, targetRefs.getQuick(0)));
                        Assert.assertEquals(22, readInt(3, compactedLength, targetRefs.getQuick(1)));
                        Assert.assertEquals(33, readInt(3, compactedLength, targetRefs.getQuick(3)));

                        directory.addSegment(3, compactedLength, 1);
                        final LongList removed = new LongList();
                        removed.add(1);
                        removed.add(2);
                        final LongList added = new LongList();
                        added.add(3);
                        directory.applyRootReferenceChanges(removed, added, 2);
                        directory.publish(metaStore, 2, 102);
                        candidate.markPublished();
                    }

                    // Generation 1 is still the fallback slot, so no pin is
                    // required for this first protection gate.
                    assertNoPurge(dataStore.purge());
                    Assert.assertTrue(dataFileExists(1));
                    Assert.assertTrue(dataFileExists(2));

                    directory.publish(metaStore, 3, 103); // overwrite generation 1
                    // The fallback no longer needs the sources, but the reader
                    // that pinned generation 1 still does.
                    assertNoPurge(dataStore.purge());
                    Assert.assertEquals(11, readInt(1, first.fileLength, first.refs.getQuick(0)));
                    Assert.assertEquals(1, oldPin.getGeneration());
                } finally {
                    oldPin.close();
                }

                final LiveViewCheckpointDataStore.PurgeResult purged = dataStore.purge();
                Assert.assertEquals(2, purged.getPurgedSegmentCount());
                Assert.assertEquals(first.fileLength + second.fileLength, purged.getPurgedBytes());
                Assert.assertFalse(dataFileExists(1));
                Assert.assertFalse(dataFileExists(2));
                Assert.assertTrue(dataFileExists(3));
            }
        });
    }

    @Test
    public void testRetiredSegmentOutlivesItsOwnGenerationWithoutAnyReader() throws Exception {
        assertMemoryLeak(() -> {
            final DataSegment source = writeDataSegment(1, 88);
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointMetaStore metaStore = openMetaStore();
                 LiveViewCheckpointDataStore dataStore = openDataStore(metaStore)) {
                directory.addSegment(1, source.fileLength, 1);
                directory.publish(metaStore, 1, 101);
                directory.retire(1, 2);
                directory.publish(metaStore, 2, 102);

                // No reader holds a pin, yet the segment survives: generation 1
                // still occupies the other A/B slot and still references it, and
                // the purge's own pin still stands on the generation that retired
                // it. Retirement alone is not a licence to unlink.
                Assert.assertEquals(0, metaStore.getActivePinCount());
                assertNoPurge(dataStore.purge());
                Assert.assertTrue(dataFileExists(1));
                Assert.assertEquals(88, readInt(1, source.fileLength, source.refs.getQuick(0)));

                // Generation 3 overwrites the slot that held generation 1 and
                // moves the purge's own pin above the retire generation, so both
                // protections lapse together and the segment goes.
                directory.publish(metaStore, 3, 103);
                final LiveViewCheckpointDataStore.PurgeResult purged = dataStore.purge();
                Assert.assertEquals(1, purged.getPurgedSegmentCount());
                Assert.assertEquals(source.fileLength, purged.getPurgedBytes());
                Assert.assertFalse(dataFileExists(1));
            }
        });
    }

    private static void assertNoPurge(LiveViewCheckpointDataStore.PurgeResult result) {
        Assert.assertEquals(0, result.getPurgedSegmentCount());
        Assert.assertEquals(0, result.getFailedSegmentCount());
        Assert.assertEquals(0, result.getPurgedBytes());
    }

    private static void assertSamePhysicalPage(
            LiveViewCheckpointStatePageRef left,
            LiveViewCheckpointStatePageRef right
    ) {
        Assert.assertEquals(left.getSegmentId(), right.getSegmentId());
        Assert.assertEquals(left.getOffset(), right.getOffset());
        Assert.assertEquals(left.getStoredLength(), right.getStoredLength());
    }

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private static LiveViewCheckpointStatePageRef stateRef(long segmentId, long offset, int storedLength) {
        return new LiveViewCheckpointStatePageRef().of(
                segmentId,
                offset,
                storedLength,
                Integer.BYTES,
                PAGE_KIND,
                0,
                1,
                0
        );
    }

    private boolean dataFileExists(long segmentId) {
        try (Path path = new Path(); Path dir = new Path()) {
            LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir(dir), segmentId);
            return configuration.getFilesFacade().exists(path.$());
        }
    }

    private boolean dataTmpFileExists(long segmentId) {
        try (Path path = new Path(); Path dir = new Path()) {
            LiveViewCheckpointLayout.dataSegmentTmpPath(path, checkpointsDir(dir), segmentId);
            return configuration.getFilesFacade().exists(path.$());
        }
    }

    private LiveViewCheckpointDataStore openDataStore(LiveViewCheckpointMetaStore metaStore) {
        final LiveViewCheckpointDataStore store = new LiveViewCheckpointDataStore(configuration, metaStore);
        try (Path dir = new Path()) {
            store.of(checkpointsDir(dir));
        }
        return store;
    }

    private LiveViewCheckpointMetaStore openMetaStore() {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        try (Path dir = new Path()) {
            store.of(checkpointsDir(dir));
        }
        return store;
    }

    private int readInt(long segmentId, long fileLength, LiveViewCheckpointStatePageRef ref) {
        try (LiveViewCheckpointDataSegmentReader reader = new LiveViewCheckpointDataSegmentReader(configuration);
             Path dir = new Path()) {
            reader.of(checkpointsDir(dir), segmentId, fileLength);
            reader.openPage(ref, PAGE_KIND, 0, 0, 1, Integer.BYTES);
            final int value = reader.getInt(0);
            reader.assertFullyConsumed(Integer.BYTES, Integer.BYTES, 1);
            return value;
        }
    }

    private DataSegment writeDataSegment(long segmentId, int... values) {
        final ObjList<LiveViewCheckpointStatePageRef> refs = new ObjList<>();
        final long fileLength;
        try (LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
             Path dir = new Path()) {
            writer.of(checkpointsDir(dir), segmentId);
            for (int value : values) {
                final MemoryA mem = writer.beginPage();
                mem.putInt(value);
                final LiveViewCheckpointStatePageRef ref = new LiveViewCheckpointStatePageRef();
                writer.endPage(ref, Integer.BYTES, PAGE_KIND, 0, 1, 0);
                refs.add(ref);
            }
            fileLength = writer.commit();
        }
        return new DataSegment(fileLength, refs);
    }

    private static final class DataSegment {
        private final long fileLength;
        private final ObjList<LiveViewCheckpointStatePageRef> refs;

        private DataSegment(long fileLength, ObjList<LiveViewCheckpointStatePageRef> refs) {
            this.fileLength = fileLength;
            this.refs = refs;
        }
    }

    /**
     * One long-lived catalogue across a test: every publication starts a fresh
     * copy-on-write session against the root the previous one published, which is
     * how the production seal and repair paths drive the directory.
     */
    private final class Catalogue implements Closeable {

        private final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointSegmentDirectoryWriter writer =
                new LiveViewCheckpointSegmentDirectoryWriter(configuration);

        private Catalogue() {
            try (Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
            }
            writer.begin(root);
        }

        @Override
        public void close() {
            writer.close();
        }

        private void addSegment(long segmentId, long fileLength, long referenceCount) {
            writer.addSegment(segmentId, fileLength, referenceCount);
        }

        private void applyRootReferenceChanges(LongList removed, LongList added, long generation) {
            writer.applyRootReferenceChanges(removed, added, generation);
        }

        private void publish(LiveViewCheckpointMetaStore store, long generation, long metadataSegmentId) {
            writer.publish(metadataSegmentId, root);
            final LiveViewCheckpointSuperblock superblock = store.getSuperblock();
            superblock.generation = generation;
            superblock.timelineRootRef.clear();
            superblock.rowPositionDeltaRootRef.clear();
            superblock.segmentDirectoryRootRef.of(root.getSegmentId(), root.getOffset(), root.getLength());
            superblock.nextSegmentId = metadataSegmentId + 1;
            store.publish();
            writer.begin(root);
        }

        private void retire(long segmentId, long generation) {
            final LongList removed = new LongList();
            removed.add(segmentId);
            writer.applyRootReferenceChanges(removed, new LongList(), generation);
        }
    }
}
