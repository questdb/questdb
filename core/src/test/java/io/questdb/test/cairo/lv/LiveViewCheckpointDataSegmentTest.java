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
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaSegmentReader;
import io.questdb.cairo.lv.LiveViewCheckpointMetaSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LiveViewCheckpointDataSegmentTest extends AbstractCairoTest {

    private static final int CODEC = 3;
    private static final int FLAGS = 0x5;
    private static final String LV_DIR = "lv_data_segment";
    private static final int PAGE_KIND = 0x31;

    @Before
    public void setUp() {
        super.setUp();
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            checkpointsDir(path).concat(LiveViewCheckpointLayout.DATA_DIR_NAME).slash();
            ff.mkdirs(path, configuration.getMkDirMode());
            checkpointsDir(path).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
            ff.mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testCrashTempAndFinalPublicationBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointStatePageRef ref = new LiveViewCheckpointStatePageRef();
            try (LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir), 7);
                writer.beginPage().putLong(11);
                writer.endPage(ref, Long.BYTES, PAGE_KIND, CODEC, 1, 0);
                // Simulate a crash during the data-write phase: close without rename.
            }
            try (Path path = new Path()) {
                Assert.assertTrue(configuration.getFilesFacade().exists(dataTmpPath(path, 7).$()));
                Assert.assertFalse(configuration.getFilesFacade().exists(dataPath(path, 7).$()));
            }

            // A retry may overwrite the stale temp and publishes only the exact
            // bytes written by the successful attempt.
            final WrittenPage page = writeLongPage(7, 22);
            Assert.assertEquals(Long.BYTES, page.fileLength);
            try (Path path = new Path()) {
                Assert.assertFalse(configuration.getFilesFacade().exists(dataTmpPath(path, 7).$()));
                Assert.assertTrue(configuration.getFilesFacade().exists(dataPath(path, 7).$()));
            }

            try (LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                 Path dir = new Path()) {
                try {
                    writer.of(checkpointsDir(dir), 7);
                    Assert.fail("expected immutable segment id reuse to fail");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "already published");
                }
            }
        });
    }

    @Test
    public void testExactFileLengthIsRequired() throws Exception {
        assertMemoryLeak(() -> {
            final WrittenPage page = writeLongPage(8, 42);
            try (LiveViewCheckpointDataSegmentReader reader = new LiveViewCheckpointDataSegmentReader(configuration);
                 Path dir = new Path()) {
                try {
                    reader.of(checkpointsDir(dir), 8, page.fileLength + 1);
                    Assert.fail("expected directory/file length mismatch");
                } catch (CairoException e) {
                    Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                    TestUtils.assertContains(e.getFlyweightMessage(), "data segment file length mismatch");
                }
            }

            // Truncation after metadata publication is rejected before mapping or
            // touching the referenced page.
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                final long fd = ff.openRW(dataPath(path, 8).$(), 0);
                try {
                    Assert.assertTrue(ff.truncate(fd, page.fileLength - 1));
                } finally {
                    ff.close(fd);
                }
            }
            try (LiveViewCheckpointDataSegmentReader reader = new LiveViewCheckpointDataSegmentReader(configuration);
                 Path dir = new Path()) {
                try {
                    reader.of(checkpointsDir(dir), 8, page.fileLength);
                    Assert.fail("expected truncated segment rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "data segment file length mismatch");
                }
            }
        });
    }

    @Test
    public void testMetadataReferenceAndPackedPagesRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointStatePageRef first = new LiveViewCheckpointStatePageRef();
            final LiveViewCheckpointStatePageRef second = new LiveViewCheckpointStatePageRef();
            final long fileLength;
            try (LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir), 3);
                writer.beginPage().putLong(0x1122_3344_5566_7788L);
                writer.endPage(first, 16, PAGE_KIND, CODEC, 2, FLAGS);
                final MemoryA payload = writer.beginPage();
                payload.putInt(17);
                payload.putInt(19);
                writer.endPage(second, 8, PAGE_KIND + 1, CODEC + 1, 2, 0);
                fileLength = writer.commit();
            }
            Assert.assertEquals(2L * Long.BYTES, fileLength);
            Assert.assertEquals(0, first.getOffset());
            Assert.assertEquals(Long.BYTES, second.getOffset());

            // StatePageRef is itself persisted only inside a checksummed metadata page.
            final LiveViewCheckpointPageRef metadataRef = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointMetaSegmentWriter writer = new LiveViewCheckpointMetaSegmentWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir), 5);
                final MemoryA metadata = writer.beginPage(0x61);
                first.writeTo(metadata);
                second.writeTo(metadata);
                writer.endPage(metadataRef);
                writer.commit();
            }

            final LiveViewCheckpointStatePageRef decodedFirst = new LiveViewCheckpointStatePageRef();
            final LiveViewCheckpointStatePageRef decodedSecond = new LiveViewCheckpointStatePageRef();
            try (LiveViewCheckpointMetaSegmentReader reader = new LiveViewCheckpointMetaSegmentReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir), 5);
                reader.openPage(metadataRef);
                decodedFirst.readFrom(reader, 0);
                decodedSecond.readFrom(reader, LiveViewCheckpointStatePageRef.BYTES);
            }

            try (LiveViewCheckpointDataSegmentReader reader = new LiveViewCheckpointDataSegmentReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir), 3, fileLength);
                reader.openPage(decodedFirst, PAGE_KIND, CODEC, FLAGS, 2, 16);
                Assert.assertEquals(0x1122_3344_5566_7788L, reader.getLong(0));
                reader.assertFullyConsumed(Long.BYTES, 16, 2);

                reader.openPage(decodedSecond, PAGE_KIND + 1, CODEC + 1, 0, 2, 8);
                Assert.assertEquals(17, reader.getInt(0));
                Assert.assertEquals(19, reader.getInt(Integer.BYTES));
                reader.assertFullyConsumed(2L * Integer.BYTES, 8, 2);
            }
        });
    }

    @Test
    public void testPayloadHasNoProductionChecksum() throws Exception {
        assertMemoryLeak(() -> {
            final WrittenPage page = writeLongPage(10, 0x0102_0304_0506_0708L);
            Assert.assertEquals(Long.BYTES, page.fileLength);
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                final long fd = ff.openRW(dataPath(path, 10).$(), 0);
                final long address = ff.mmap(fd, page.fileLength, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
                try {
                    final byte value = Unsafe.getUnsafe().getByte(address);
                    Unsafe.getUnsafe().putByte(address, (byte) (value ^ 0x7f));
                } finally {
                    ff.munmap(address, page.fileLength, MemoryTag.MMAP_DEFAULT);
                    ff.close(fd);
                }
            }
            try (LiveViewCheckpointDataSegmentReader reader = new LiveViewCheckpointDataSegmentReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir), 10, page.fileLength);
                reader.openPage(page.ref, PAGE_KIND, CODEC, FLAGS, 1, Long.BYTES);
                Assert.assertNotEquals(0x0102_0304_0506_0708L, reader.getLong(0));
            }
        });
    }

    @Test
    public void testStrictReferenceAndDecoderBounds() throws Exception {
        assertMemoryLeak(() -> {
            final WrittenPage page = writeLongPage(12, 99);
            try (LiveViewCheckpointDataSegmentReader reader = new LiveViewCheckpointDataSegmentReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir), 12, page.fileLength);
                assertPageRejected(reader, new LiveViewCheckpointStatePageRef(), "null data page reference");
                assertPageRejected(reader, copy(page.ref).of(13, 0, 8, 8, PAGE_KIND, CODEC, 1, FLAGS), "segment mismatch");
                assertPageRejected(reader, copy(page.ref).of(12, -1, 8, 8, PAGE_KIND, CODEC, 1, FLAGS), "range out of bounds");
                assertPageRejected(reader, copy(page.ref).of(12, Long.MAX_VALUE, 8, 8, PAGE_KIND, CODEC, 1, FLAGS), "range out of bounds");
                assertPageRejected(reader, copy(page.ref).of(12, 0, 0, 8, PAGE_KIND, CODEC, 1, FLAGS), "range out of bounds");
                assertPageRejected(reader, copy(page.ref).of(12, 0, 8, -1, PAGE_KIND, CODEC, 1, FLAGS), "decoded length");
                assertPageRejected(reader, copy(page.ref).of(12, 0, 8, 9, PAGE_KIND, CODEC, 1, FLAGS), "decoded length");
                assertPageRejected(reader, copy(page.ref).of(12, 0, 8, 8, PAGE_KIND + 1, CODEC, 1, FLAGS), "kind mismatch");
                assertPageRejected(reader, copy(page.ref).of(12, 0, 8, 8, PAGE_KIND, CODEC + 1, 1, FLAGS), "codec mismatch");
                assertPageRejected(reader, copy(page.ref).of(12, 0, 8, 8, PAGE_KIND, CODEC, 1, FLAGS | 8), "flags unsupported");
                assertPageRejected(reader, copy(page.ref).of(12, 0, 8, 8, PAGE_KIND, CODEC, -1, FLAGS), "row count");
                assertPageRejected(reader, copy(page.ref).of(12, 0, 8, 8, PAGE_KIND, CODEC, 2, FLAGS), "row count");

                reader.openPage(page.ref, PAGE_KIND, CODEC, FLAGS, 1, Long.BYTES);
                try {
                    reader.getLong(1);
                    Assert.fail("expected bounded field read rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "data page read out of bounds");
                }
                try {
                    reader.assertFullyConsumed(7, 8, 1);
                    Assert.fail("expected trailing encoded byte rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "consume reference exactly");
                }
                try {
                    reader.assertFullyConsumed(8, 7, 1);
                    Assert.fail("expected decoded length mismatch");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "consume reference exactly");
                }
                try {
                    reader.assertFullyConsumed(8, 8, 0);
                    Assert.fail("expected decoded row-count mismatch");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "consume reference exactly");
                }
            }
        });
    }

    private static LiveViewCheckpointStatePageRef copy(LiveViewCheckpointStatePageRef ref) {
        return new LiveViewCheckpointStatePageRef().of(
                ref.getSegmentId(),
                ref.getOffset(),
                ref.getStoredLength(),
                ref.getDecodedLength(),
                ref.getPageKind(),
                ref.getCodec(),
                ref.getRowCount(),
                ref.getFlags()
        );
    }

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private static Path dataPath(Path path, long segmentId) {
        try (Path dir = new Path()) {
            return LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir(dir), segmentId);
        }
    }

    private static Path dataTmpPath(Path path, long segmentId) {
        try (Path dir = new Path()) {
            return LiveViewCheckpointLayout.dataSegmentTmpPath(path, checkpointsDir(dir), segmentId);
        }
    }

    private static void assertPageRejected(
            LiveViewCheckpointDataSegmentReader reader,
            LiveViewCheckpointStatePageRef ref,
            CharSequence message
    ) {
        try {
            reader.openPage(ref, PAGE_KIND, CODEC, FLAGS, 1, Long.BYTES);
            Assert.fail("expected malformed state-page reference rejection");
        } catch (CairoException e) {
            Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }

    private WrittenPage writeLongPage(long segmentId, long value) {
        final WrittenPage result = new WrittenPage();
        try (LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
             Path dir = new Path()) {
            writer.of(checkpointsDir(dir), segmentId);
            writer.beginPage().putLong(value);
            writer.endPage(result.ref, Long.BYTES, PAGE_KIND, CODEC, 1, FLAGS);
            result.fileLength = writer.commit();
        }
        return result;
    }

    private static final class WrittenPage {
        private long fileLength;
        private final LiveViewCheckpointStatePageRef ref = new LiveViewCheckpointStatePageRef();
    }
}
