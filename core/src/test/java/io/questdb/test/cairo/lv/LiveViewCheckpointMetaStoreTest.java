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
import io.questdb.cairo.lv.LiveViewCheckpointRowPositionDelta;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointRowPositionDeltaReader;
import io.questdb.cairo.lv.LiveViewCheckpointRowPositionDeltaWriter;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.lv.LiveViewCheckpointTimeline;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Zip;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end Phase-1 catalogue coverage. Unlike the component tests, these tests
 * combine immutable tree segments, A/B publication, bounded startup validation,
 * and generation pins. The crash cases stop after each durable boundary; the
 * corruption cases distinguish root-page fallback before exposure from lazy deep
 * path failure after a generation has been selected.
 */
public class LiveViewCheckpointMetaStoreTest extends AbstractCairoTest {

    private static final String LV_DIR = "lv_meta_store";

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
    public void testBothRootCorruptGenerationsAreRejected() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef generation1;
            final LiveViewCheckpointPageRef generation2;
            try (Harness h = new Harness(); LiveViewCheckpointMetaStore store = openStore()) {
                h.append(10, 1);
                generation1 = copy(h.timelineRoot);
                h.publish(store, 1);
                h.append(20, 2);
                generation2 = copy(h.timelineRoot);
                h.publish(store, 2);
            }
            corruptPage(generation1);
            corruptPage(generation2);

            try (LiveViewCheckpointMetaStore store = openStore()) {
                Assert.assertFalse(store.isValid());
                try {
                    store.pin();
                    Assert.fail("expected no generation to pin");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getFlyweightMessage().toString().contains("no published generation"));
                }
                // The checksum-valid slot generations remain an allocation floor
                // even though neither root can be exposed. Reusing generation 2
                // could otherwise reselect a corrupt peer slot after publication.
                store.getSuperblock().generation = 2;
                try {
                    store.publish();
                    Assert.fail("expected rejected-generation floor to be retained");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getFlyweightMessage().toString().contains("generation must advance"));
                }
            }
        });
    }

    @Test
    public void testCandidateRootIsValidatedBeforeSlotPublish() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(); LiveViewCheckpointMetaStore store = openStore()) {
                h.append(10, 1);
                h.publish(store, 1);

                final LiveViewCheckpointSuperblock sb = store.getSuperblock();
                sb.generation = 2;
                sb.timelineRootRef.of(999_999, 24, 64); // missing final segment
                try {
                    store.publish();
                    Assert.fail("expected candidate root validation failure");
                } catch (CairoException e) {
                    Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                }
                try (LiveViewCheckpointGenerationPin pin = store.pin()) {
                    Assert.assertEquals(1, pin.getGeneration());
                }
            }
            assertSelectedGeneration(1);
        });
    }

    @Test
    public void testCrashDuringMetadataWriteAndBeforeSlotPublishKeepsOldGeneration() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(); LiveViewCheckpointMetaStore store = openStore()) {
                h.append(10, 1);
                h.publish(store, 1);

                // Crash while writing metadata: only m.<id>.tmp exists.
                try (LiveViewCheckpointMetaSegmentWriter writer = new LiveViewCheckpointMetaSegmentWriter(configuration)) {
                    try (Path dir = new Path()) {
                        writer.of(checkpointsDir(dir), h.nextSegmentId++);
                    }
                    writer.beginPage(0x55).putLong(42);
                    writer.endPage(new LiveViewCheckpointPageRef());
                    // Deliberately do not commit/rename the temp segment.
                }

                // Crash after metadata rename but before the superblock commit:
                // append publishes a final immutable segment, but no slot names it.
                h.append(20, 2);
            }
            assertSelectedGeneration(1);
        });
    }

    @Test
    public void testDeepCorruptionIsDetectedLazilyWithoutStartupFallback() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef root;
            final LiveViewCheckpointPageRef deepChild = new LiveViewCheckpointPageRef();
            try (Harness h = new Harness(); LiveViewCheckpointMetaStore store = openStore()) {
                for (int i = 0; i < 60; i++) {
                    h.append(i * 10L, i);
                }
                root = copy(h.timelineRoot);
                h.publish(store, 1);
                try (LiveViewCheckpointTimelineReader reader = new LiveViewCheckpointTimelineReader(configuration);
                     Path dir = new Path()) {
                    reader.of(checkpointsDir(dir));
                    final int childCount = reader.rootChildCount(root);
                    Assert.assertTrue("expected internal root", childCount > 1);
                    for (int i = childCount - 1; i >= 0; i--) {
                        reader.rootChildRef(root, i, deepChild);
                        if (deepChild.getSegmentId() != root.getSegmentId()) {
                            break;
                        }
                    }
                    Assert.assertNotEquals(root.getSegmentId(), deepChild.getSegmentId());
                }
            }
            corruptPage(deepChild);

            try (LiveViewCheckpointMetaStore store = openStore();
                 LiveViewCheckpointGenerationPin pin = store.pin();
                 LiveViewCheckpointTimelineReader reader = new LiveViewCheckpointTimelineReader(configuration);
                 Path dir = new Path()) {
                // Startup validates only the root, so generation 1 is selected.
                Assert.assertEquals(1, pin.getGeneration());
                reader.of(checkpointsDir(dir));
                try {
                    reader.size(pin.getTimelineRootRef());
                    Assert.fail("expected lazy deep-page corruption failure");
                } catch (CairoException e) {
                    Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                }
            }
        });
    }

    @Test
    public void testNewestRowPositionRootCorruptionFallsBackBeforePin() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef corruptRoot;
            try (Harness h = new Harness(); LiveViewCheckpointMetaStore store = openStore()) {
                h.append(10, 1);
                h.suffixAdd(10, 1, 5);
                h.publish(store, 1);
                h.suffixAdd(20, 2, 7);
                corruptRoot = copy(h.rowPositionRoot);
                h.publish(store, 2);
            }
            corruptPageKind(corruptRoot);
            assertSelectedGeneration(1);
        });
    }

    @Test
    public void testNewestTimelineRootCorruptionFallsBackBeforePin() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef corruptRoot;
            try (Harness h = new Harness(); LiveViewCheckpointMetaStore store = openStore()) {
                h.append(10, 1);
                h.publish(store, 1);
                h.append(20, 2);
                corruptRoot = copy(h.timelineRoot);
                h.publish(store, 2);
            }
            corruptPageKind(corruptRoot);
            assertSelectedGeneration(1);
        });
    }

    @Test
    public void testPublishedGenerationsRoundTripAndPinsKeepTheirRoots() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(); LiveViewCheckpointMetaStore store = openStore()) {
                for (int i = 0; i < 20; i++) {
                    h.append(i * 10L, i);
                }
                h.suffixAdd(100, 10, 5);
                h.publish(store, 1);

                try (LiveViewCheckpointGenerationPin oldPin = store.pin()) {
                    h.append(200, 20);
                    h.suffixAdd(200, 20, 7);
                    h.publish(store, 2);
                    try (LiveViewCheckpointGenerationPin newPin = store.pin();
                         LiveViewCheckpointTimelineReader timelineReader = new LiveViewCheckpointTimelineReader(configuration);
                         LiveViewCheckpointRowPositionDeltaReader deltaReader = new LiveViewCheckpointRowPositionDeltaReader(configuration);
                         Path dir = new Path()) {
                        timelineReader.of(checkpointsDir(dir));
                        deltaReader.of(checkpointsDir(dir));

                        Assert.assertEquals(1, oldPin.getGeneration());
                        Assert.assertFalse(timelineReader.findExact(oldPin.getTimelineRootRef(), 200, 20, new LiveViewCheckpointTimelineEntry()));
                        Assert.assertEquals(5, deltaReader.prefixSum(oldPin.getRowPositionDeltaRootRef(), 200, 20));

                        Assert.assertEquals(2, newPin.getGeneration());
                        Assert.assertTrue(timelineReader.findExact(newPin.getTimelineRootRef(), 200, 20, new LiveViewCheckpointTimelineEntry()));
                        Assert.assertEquals(12, deltaReader.prefixSum(newPin.getRowPositionDeltaRootRef(), 200, 20));
                    }
                }
            }
            assertSelectedGeneration(2);
        });
    }

    @Test
    public void testRejectedPublicationLeavesThePinnableGenerationUntouched() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness(); LiveViewCheckpointMetaStore store = openStore()) {
                h.append(10, 1);
                h.publish(store, 1);
                final long walPurgeFloor = store.getWalPurgeFloor();

                // A second boundary reaches its durable metadata page, and then
                // the superblock refuses the publication: the generation does
                // not advance.
                h.append(20, 2);
                try {
                    h.publish(store, 1);
                    Assert.fail("expected non-advancing generation rejection");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getFlyweightMessage().toString().contains("generation must advance"));
                }

                // The slot write is the commit point, so a publication that
                // never reached it leaves every reader on the old generation.
                // The candidate root is a durable metadata page either way; what
                // must not happen is a pin naming it, because nothing has
                // committed the timeline it belongs to.
                try (LiveViewCheckpointGenerationPin pin = store.pin();
                     LiveViewCheckpointTimelineReader timelineReader = new LiveViewCheckpointTimelineReader(configuration);
                     Path dir = new Path()) {
                    timelineReader.of(checkpointsDir(dir));
                    Assert.assertEquals(1, pin.getGeneration());
                    final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
                    Assert.assertTrue(timelineReader.findExact(pin.getTimelineRootRef(), 10, 1, entry));
                    Assert.assertFalse(timelineReader.findExact(pin.getTimelineRootRef(), 20, 2, entry));
                }
                Assert.assertEquals(walPurgeFloor, store.getWalPurgeFloor());
            }
            assertSelectedGeneration(1);
        });
    }

    @Test
    public void testRestartValidationCostIsConstantAtOneAndOneHundredMillionLogicalEntries() throws Exception {
        final CountingMetaOpenFilesFacade ff = new CountingMetaOpenFilesFacade();
        assertMemoryLeak(ff, () -> {
            final LiveViewCheckpointPageRef timelineRoot = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef rowPositionRoot = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef segmentDirectoryRoot = new LiveViewCheckpointPageRef();

            // nextCheckpointId is the authoritative monotonic population upper bound.
            // The root pages deliberately name a missing deep child: bounded restart
            // must validate the roots and catalogue without following that child.
            writeLargeTimelineRoots(timelineRoot, rowPositionRoot, segmentDirectoryRoot);
            publishLargeTimelineGeneration(1, 1_000_000, timelineRoot, rowPositionRoot, segmentDirectoryRoot);
            assertBoundedRestart(ff, 1, 1_000_000);

            publishLargeTimelineGeneration(2, 100_000_000, timelineRoot, rowPositionRoot, segmentDirectoryRoot);
            assertBoundedRestart(ff, 2, 100_000_000);
        });
    }

    private static Path checkpointsDir(Path path) {
        path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
        return path;
    }

    private static LiveViewCheckpointPageRef copy(LiveViewCheckpointPageRef ref) {
        return new LiveViewCheckpointPageRef().of(ref.getSegmentId(), ref.getOffset(), ref.getLength());
    }

    private void assertSelectedGeneration(long expected) {
        try (LiveViewCheckpointMetaStore store = openStore(); LiveViewCheckpointGenerationPin pin = store.pin()) {
            Assert.assertEquals(expected, pin.getGeneration());
            Assert.assertEquals(expected, store.getSuperblock().generation);
        }
    }

    private void assertBoundedRestart(
            CountingMetaOpenFilesFacade ff,
            long expectedGeneration,
            long expectedLogicalEntryCount
    ) {
        ff.beginMeasurement();
        try (LiveViewCheckpointMetaStore store = openStore(); LiveViewCheckpointGenerationPin pin = store.pin()) {
            Assert.assertEquals(expectedGeneration, pin.getGeneration());
            Assert.assertEquals(expectedLogicalEntryCount, store.getSuperblock().nextCheckpointId);
        } finally {
            ff.endMeasurement();
        }
        Assert.assertEquals(
                "startup must open only the timeline, row-position, and segment-directory roots",
                3,
                ff.getMetaOpenCount()
        );
    }

    private void corruptPage(LiveViewCheckpointPageRef ref) {
        try (Path path = new Path(); Path dir = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
            LiveViewCheckpointLayout.metaSegmentPath(path, checkpointsDir(dir), ref.getSegmentId());
            mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
            final long offset = ref.getOffset() + LiveViewCheckpointLayout.PAGE_HEADER_SIZE;
            mem.putByte(offset, (byte) (mem.getByte(offset) ^ 1));
        }
    }

    private void corruptPageKind(LiveViewCheckpointPageRef ref) {
        try (Path path = new Path(); Path dir = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
            LiveViewCheckpointLayout.metaSegmentPath(path, checkpointsDir(dir), ref.getSegmentId());
            mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
            mem.putInt(ref.getOffset() + LiveViewCheckpointLayout.PAGE_KIND_OFFSET, 0x7FFF_FFFF);
            final long crcStart = ref.getOffset() + LiveViewCheckpointLayout.PAGE_LENGTH_OFFSET;
            final int crc = Zip.crc32(0, mem.addressOf(crcStart), ref.getLength() - LiveViewCheckpointLayout.PAGE_LENGTH_OFFSET);
            mem.putInt(ref.getOffset() + LiveViewCheckpointLayout.PAGE_CRC_OFFSET, crc);
        }
    }

    private void publishLargeTimelineGeneration(
            long generation,
            long logicalEntryCount,
            LiveViewCheckpointPageRef timelineRoot,
            LiveViewCheckpointPageRef rowPositionRoot,
            LiveViewCheckpointPageRef segmentDirectoryRoot
    ) {
        try (LiveViewCheckpointMetaStore store = openStore()) {
            final LiveViewCheckpointSuperblock sb = store.getSuperblock();
            sb.generation = generation;
            sb.definitionTxn = 10;
            sb.historyEpoch = 20;
            sb.normalizedBaseSeqTxn = generation;
            sb.coveredLvSeqTxn = generation;
            sb.nextCheckpointId = logicalEntryCount;
            sb.nextSegmentId = 1;
            sb.metadataBytes = 1;
            sb.dataBytes = 0;
            sb.timelineRootRef.of(timelineRoot.getSegmentId(), timelineRoot.getOffset(), timelineRoot.getLength());
            sb.rowPositionDeltaRootRef.of(rowPositionRoot.getSegmentId(), rowPositionRoot.getOffset(), rowPositionRoot.getLength());
            sb.segmentDirectoryRootRef.of(segmentDirectoryRoot.getSegmentId(), segmentDirectoryRoot.getOffset(), segmentDirectoryRoot.getLength());
            store.publish();
        }
    }

    private void writeLargeTimelineRoots(
            LiveViewCheckpointPageRef timelineRoot,
            LiveViewCheckpointPageRef rowPositionRoot,
            LiveViewCheckpointPageRef segmentDirectoryRoot
    ) {
        final long missingSegmentId = 999_999;
        try (LiveViewCheckpointMetaSegmentWriter writer = new LiveViewCheckpointMetaSegmentWriter(configuration);
             Path dir = new Path()) {
            writer.of(checkpointsDir(dir), 0);

            MemoryA page = writer.beginPage(LiveViewCheckpointTimeline.PAGE_KIND_INTERNAL);
            page.putInt(1);
            page.putLong(0);
            page.putLong(0);
            page.putLong(missingSegmentId);
            page.putLong(LiveViewCheckpointLayout.SEG_HEADER_SIZE);
            page.putInt(LiveViewCheckpointLayout.PAGE_HEADER_SIZE + Integer.BYTES);
            writer.endPage(timelineRoot);

            page = writer.beginPage(LiveViewCheckpointRowPositionDelta.PAGE_KIND_INTERNAL);
            page.putInt(1);
            page.putLong(0);
            page.putLong(0);
            page.putLong(0);
            page.putLong(missingSegmentId);
            page.putLong(LiveViewCheckpointLayout.SEG_HEADER_SIZE);
            page.putInt(LiveViewCheckpointLayout.PAGE_HEADER_SIZE + Integer.BYTES);
            writer.endPage(rowPositionRoot);

            page = writer.beginPage(LiveViewCheckpointSegmentDirectory.PAGE_KIND_LEAF);
            page.putInt(0);
            writer.endPage(segmentDirectoryRoot);
            writer.commit();
        }
    }

    private LiveViewCheckpointMetaStore openStore() {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        try (Path dir = new Path()) {
            store.of(checkpointsDir(dir));
        }
        return store;
    }

    private final class Harness implements AutoCloseable {
        private final LiveViewCheckpointPageRef rowPositionRoot = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointRowPositionDeltaWriter rowPositionWriter = new LiveViewCheckpointRowPositionDeltaWriter(configuration, 3, 3);
        private final LiveViewCheckpointPageRef timelineRoot = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointTimelineWriter timelineWriter = new LiveViewCheckpointTimelineWriter(configuration, 3, 3);
        private final LiveViewCheckpointPageRef tmpRoot = new LiveViewCheckpointPageRef();
        private long nextSegmentId;

        private Harness() {
            try (Path dir = new Path()) {
                timelineWriter.of(checkpointsDir(dir));
                rowPositionWriter.of(checkpointsDir(dir));
            }
        }

        private void append(long timestamp, long checkpointId) {
            final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
            entry.of(timestamp, checkpointId, checkpointId * 3, checkpointId * 11, checkpointId * 17);
            entry.rootRef.of(10_000 + checkpointId, checkpointId * 64, 60);
            timelineWriter.append(timelineRoot, entry, nextSegmentId++, tmpRoot);
            timelineRoot.of(tmpRoot.getSegmentId(), tmpRoot.getOffset(), tmpRoot.getLength());
        }

        @Override
        public void close() {
            rowPositionWriter.close();
            timelineWriter.close();
        }

        private void publish(LiveViewCheckpointMetaStore store, long generation) {
            final LiveViewCheckpointSuperblock sb = store.getSuperblock();
            sb.generation = generation;
            sb.definitionTxn = 10;
            sb.historyEpoch = 20;
            sb.normalizedBaseSeqTxn = generation * 100;
            sb.coveredLvSeqTxn = generation * 200;
            sb.nextCheckpointId = generation * 10;
            sb.nextSegmentId = nextSegmentId;
            sb.metadataBytes = generation * 1_000;
            sb.dataBytes = 0;
            sb.timelineRootRef.of(timelineRoot.getSegmentId(), timelineRoot.getOffset(), timelineRoot.getLength());
            sb.rowPositionDeltaRootRef.of(rowPositionRoot.getSegmentId(), rowPositionRoot.getOffset(), rowPositionRoot.getLength());
            sb.segmentDirectoryRootRef.clear();
            store.publish();
        }

        private void suffixAdd(long timestamp, long checkpointId, long delta) {
            rowPositionWriter.suffixAdd(rowPositionRoot, timestamp, checkpointId, delta, nextSegmentId++, tmpRoot);
            rowPositionRoot.of(tmpRoot.getSegmentId(), tmpRoot.getOffset(), tmpRoot.getLength());
        }
    }

    private static final class CountingMetaOpenFilesFacade extends TestFilesFacadeImpl {
        private boolean measuring;
        private int metaOpenCount;

        private void beginMeasurement() {
            metaOpenCount = 0;
            measuring = true;
        }

        private void endMeasurement() {
            measuring = false;
        }

        private int getMetaOpenCount() {
            return metaOpenCount;
        }

        @Override
        public long openRO(LPSZ name) {
            if (measuring
                    && Utf8s.containsAscii(name, LV_DIR)
                    && Utf8s.containsAscii(name, LiveViewCheckpointLayout.META_SEGMENT_PREFIX)) {
                metaOpenCount++;
            }
            return super.openRO(name);
        }
    }

    @Test
    public void testWalPurgeFloorIncludesPinnedGenerationOlderThanBothSlots() throws Exception {
        assertMemoryLeak(() -> {
            try (Harness harness = new Harness(); LiveViewCheckpointMetaStore store = openStore()) {
                harness.append(10, 1);
                harness.publish(store, 1);
                final LiveViewCheckpointGenerationPin oldPin = store.pin();
                try {
                    harness.append(20, 2);
                    harness.publish(store, 2);
                    harness.append(30, 3);
                    harness.publish(store, 3);

                    Assert.assertEquals(200, store.getSuperblock().getWalPurgeFloor());
                    Assert.assertEquals(
                            "the generation-1 reader still needs its base WAL after both slots advance",
                            100,
                            store.getWalPurgeFloor()
                    );
                } finally {
                    oldPin.close();
                }
                Assert.assertEquals(200, store.getWalPurgeFloor());
            }
        });
    }
}
