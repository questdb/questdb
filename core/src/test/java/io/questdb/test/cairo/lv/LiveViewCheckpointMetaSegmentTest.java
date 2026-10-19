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
import io.questdb.cairo.lv.LiveViewCheckpointMetaSegmentReader;
import io.questdb.cairo.lv.LiveViewCheckpointMetaSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Zip;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Standalone coverage for the immutable, per-page-checksummed metadata segment
 * store ({@code m.<segmentId>}) the versioned checkpoint timeline builds on.
 * <p>
 * A metadata segment packs many small pages, each with its own CRC32, so a
 * localized read validates one page in isolation. The contract cuts both ways: a
 * valid page must round-trip exactly, and every structural defect a crash or a
 * foreign writer can leave - a torn header, a bad page checksum, a length that
 * would run off the mapping - must be rejected before any unsafe read rather than
 * yielding wrong bytes.
 */
public class LiveViewCheckpointMetaSegmentTest extends AbstractCairoTest {

    private static final String LV_DIR = "lv_meta";

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            final FilesFacade ff = configuration.getFilesFacade();
            checkpointsDir(path);
            path.concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
            ff.mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testBadMagicRejected() throws Exception {
        assertMemoryLeak(() -> {
            writeSingleLongPage(4, 7, 0x1234L);
            rewriteHeaderRaw(4, 0xDEAD, LiveViewCheckpointLayout.SEG_FORMAT_VERSION, 4, 1, true);
            assertOpenRejected(4, "metadata segment magic mismatch");
        });
    }

    @Test
    public void testEmptyPageRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef ref = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointMetaSegmentWriter writer = new LiveViewCheckpointMetaSegmentWriter(configuration)) {
                try (Path dir = new Path()) {
                    writer.of(checkpointsDir(dir), 11);
                }
                writer.beginPage(99);
                writer.endPage(ref);
                writer.commit();
            }
            Assert.assertEquals(LiveViewCheckpointLayout.PAGE_HEADER_SIZE, ref.getLength());

            try (LiveViewCheckpointMetaSegmentReader reader = new LiveViewCheckpointMetaSegmentReader(configuration)) {
                try (Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), 11);
                }
                Assert.assertEquals(1, reader.getPageCount());
                reader.openPage(ref);
                Assert.assertEquals(99, reader.getPageKind());
                Assert.assertEquals(0, reader.getPagePayloadLength());
            }
        });
    }

    @Test
    public void testFieldReadOutOfBoundsRejected() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef ref = writeSingleLongPage(12, 1, 0xABCDL);
            try (LiveViewCheckpointMetaSegmentReader reader = new LiveViewCheckpointMetaSegmentReader(configuration)) {
                try (Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), 12);
                }
                reader.openPage(ref);
                Assert.assertEquals(0xABCDL, reader.getLong(0));
                // Payload is exactly 8 bytes; a long read at offset 1 runs off the end.
                try {
                    reader.getLong(1);
                    Assert.fail("expected an out-of-bounds field read to be rejected");
                } catch (CairoException e) {
                    Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                    TestUtils.assertContains(e.getFlyweightMessage(), "metadata page field read out of bounds");
                }
            }
        });
    }

    @Test
    public void testHeaderChecksumCorruptionRejected() throws Exception {
        assertMemoryLeak(() -> {
            writeSingleLongPage(5, 3, 0x99L);
            // Flip the page count without fixing the header CRC.
            rewriteHeaderRaw(5, LiveViewCheckpointLayout.SEG_MAGIC, LiveViewCheckpointLayout.SEG_FORMAT_VERSION, 5, 42, false);
            assertOpenRejected(5, "metadata segment header checksum mismatch");
        });
    }

    @Test
    public void testMultiPageRoundTripAndIteration() throws Exception {
        assertMemoryLeak(() -> {
            final int pageCount = 5;
            final long[] offsets = new long[pageCount];
            final int[] lengths = new int[pageCount];
            final LiveViewCheckpointPageRef ref = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointMetaSegmentWriter writer = new LiveViewCheckpointMetaSegmentWriter(configuration)) {
                try (Path dir = new Path()) {
                    writer.of(checkpointsDir(dir), 3);
                }
                for (int i = 0; i < pageCount; i++) {
                    final MemoryA payload = writer.beginPage(100 + i);
                    payload.putLong(1_000L + i);
                    payload.putInt(i);
                    writer.endPage(ref);
                    Assert.assertEquals(3, ref.getSegmentId());
                    offsets[i] = ref.getOffset();
                    lengths[i] = ref.getLength();
                }
                writer.commit();
            }

            try (LiveViewCheckpointMetaSegmentReader reader = new LiveViewCheckpointMetaSegmentReader(configuration)) {
                try (Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), 3);
                }
                Assert.assertEquals(pageCount, reader.getPageCount());

                // Random access by reference.
                for (int i = 0; i < pageCount; i++) {
                    ref.of(3, offsets[i], lengths[i]);
                    reader.openPage(ref);
                    Assert.assertEquals(100 + i, reader.getPageKind());
                    Assert.assertEquals(Long.BYTES + Integer.BYTES, reader.getPagePayloadLength());
                    Assert.assertEquals(1_000L + i, reader.getLong(0));
                    Assert.assertEquals(i, reader.getInt(Long.BYTES));
                }

                // Sequential walk from the first page offset must visit every page in order.
                long offset = reader.firstPageOffset();
                for (int i = 0; i < pageCount; i++) {
                    Assert.assertEquals(offsets[i], offset);
                    offset = reader.openPageAt(offset, -1);
                    Assert.assertEquals(100 + i, reader.getPageKind());
                    Assert.assertEquals(1_000L + i, reader.getLong(0));
                }
                Assert.assertEquals(reader.endOffset(), offset);
            }
        });
    }

    @Test
    public void testNegativePageLengthRejected() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef ref = writeSingleLongPage(9, 1, 0x1L);
            final long pageOffset = ref.getOffset();
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), segmentPath(path, 9).$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(pageOffset + LiveViewCheckpointLayout.PAGE_LENGTH_OFFSET, -1);
            }
            try (LiveViewCheckpointMetaSegmentReader reader = new LiveViewCheckpointMetaSegmentReader(configuration)) {
                try (Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), 9);
                }
                try {
                    reader.openPageAt(pageOffset, -1);
                    Assert.fail("expected a negative page length to be rejected");
                } catch (CairoException e) {
                    Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                    TestUtils.assertContains(e.getFlyweightMessage(), "metadata page payload length negative");
                }
            }
        });
    }

    @Test
    public void testPageChecksumCorruptionRejected() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef ref = writeSingleLongPage(6, 8, 0x5A5AL);
            final long payloadStart = ref.getOffset() + LiveViewCheckpointLayout.PAGE_HEADER_SIZE;
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), segmentPath(path, 6).$(), MemoryTag.MMAP_DEFAULT);
                // Corrupt one payload byte; the header and length still validate,
                // so only the per-page CRC catches it.
                mem.putByte(payloadStart, (byte) (mem.getByte(payloadStart) ^ 0xFF));
            }
            try (LiveViewCheckpointMetaSegmentReader reader = new LiveViewCheckpointMetaSegmentReader(configuration)) {
                try (Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), 6);
                }
                try {
                    reader.openPage(ref);
                    Assert.fail("expected a page checksum mismatch to be rejected");
                } catch (CairoException e) {
                    Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                    TestUtils.assertContains(e.getFlyweightMessage(), "metadata page checksum mismatch");
                }
            }
        });
    }

    @Test
    public void testPageLengthMismatchRejected() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef ref = writeSingleLongPage(10, 1, 0x2L);
            try (LiveViewCheckpointMetaSegmentReader reader = new LiveViewCheckpointMetaSegmentReader(configuration)) {
                try (Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), 10);
                }
                // A reference whose length disagrees with the page's own header
                // is a redirected/poisoned pointer and must be rejected.
                ref.of(10, ref.getOffset(), ref.getLength() + 8);
                try {
                    reader.openPage(ref);
                    Assert.fail("expected a page length mismatch to be rejected");
                } catch (CairoException e) {
                    Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                    TestUtils.assertContains(e.getFlyweightMessage(), "metadata page length mismatch");
                }
            }
        });
    }

    @Test
    public void testPageOffsetOutOfBoundsRejected() throws Exception {
        assertMemoryLeak(() -> {
            writeSingleLongPage(8, 1, 0x3L);
            try (LiveViewCheckpointMetaSegmentReader reader = new LiveViewCheckpointMetaSegmentReader(configuration)) {
                try (Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), 8);
                }
                // Below the header.
                assertOpenPageAtRejected(reader, 0, "metadata page offset out of bounds");
                // Past the end of the file.
                assertOpenPageAtRejected(reader, reader.endOffset(), "metadata page offset out of bounds");
            }
        });
    }

    @Test
    public void testPageRefCodecRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                checkpointsDir(path).concat("refcodec");
                mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
                mem.jumpTo(4 * LiveViewCheckpointPageRef.BYTES);

                final LiveViewCheckpointPageRef a = new LiveViewCheckpointPageRef().of(7, 4096, 128);
                final LiveViewCheckpointPageRef b = new LiveViewCheckpointPageRef(); // null

                a.writeTo(mem, 0);
                b.writeTo(mem, LiveViewCheckpointPageRef.BYTES);

                final LiveViewCheckpointPageRef out = new LiveViewCheckpointPageRef();
                out.readFrom(mem, 0);
                Assert.assertFalse(out.isNull());
                Assert.assertEquals(7, out.getSegmentId());
                Assert.assertEquals(4096, out.getOffset());
                Assert.assertEquals(128, out.getLength());

                out.readFrom(mem, LiveViewCheckpointPageRef.BYTES);
                Assert.assertTrue(out.isNull());
                Assert.assertEquals(LiveViewCheckpointPageRef.NULL_SEGMENT_ID, out.getSegmentId());
            }
        });
    }

    @Test
    public void testSegmentIdMismatchRejected() throws Exception {
        assertMemoryLeak(() -> {
            writeSingleLongPage(7, 1, 0x4L);
            // Rewrite the self-id to a different value with a valid CRC: the file
            // is intact but claims to be a different segment.
            rewriteHeaderRaw(7, LiveViewCheckpointLayout.SEG_MAGIC, LiveViewCheckpointLayout.SEG_FORMAT_VERSION, 99, 1, true);
            assertOpenRejected(7, "metadata segment id mismatch");
        });
    }

    @Test
    public void testSegmentRefMismatchRejected() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef ref = writeSingleLongPage(13, 1, 0x6L);
            try (LiveViewCheckpointMetaSegmentReader reader = new LiveViewCheckpointMetaSegmentReader(configuration)) {
                try (Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), 13);
                }
                ref.of(14, ref.getOffset(), ref.getLength());
                try {
                    reader.openPage(ref);
                    Assert.fail("expected a page reference segment mismatch to be rejected");
                } catch (CairoException e) {
                    Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                    TestUtils.assertContains(e.getFlyweightMessage(), "metadata page reference segment mismatch");
                }
            }
        });
    }

    @Test
    public void testVersionSkewRejected() throws Exception {
        assertMemoryLeak(() -> {
            writeSingleLongPage(15, 1, 0x7L);
            // Valid CRC over a newer format version: a genuine compatibility
            // difference, not corruption.
            rewriteHeaderRaw(15, LiveViewCheckpointLayout.SEG_MAGIC, LiveViewCheckpointLayout.SEG_FORMAT_VERSION + 1, 15, 1, true);
            assertOpenRejected(15, "metadata segment format version not supported");
        });
    }

    private static Path checkpointsDir(Path path) {
        path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
        return path;
    }

    private static Path segmentPath(Path path, long segmentId) {
        try (Path dir = new Path()) {
            return LiveViewCheckpointLayout.metaSegmentPath(path, checkpointsDir(dir), segmentId);
        }
    }

    private void assertOpenPageAtRejected(LiveViewCheckpointMetaSegmentReader reader, long offset, String reason) {
        try {
            reader.openPageAt(offset, -1);
            Assert.fail("expected openPageAt to be rejected: " + reason);
        } catch (CairoException e) {
            Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
            TestUtils.assertContains(e.getFlyweightMessage(), reason);
        }
    }

    private void assertOpenRejected(long segmentId, String reason) {
        try (LiveViewCheckpointMetaSegmentReader reader = new LiveViewCheckpointMetaSegmentReader(configuration)) {
            try (Path dir = new Path()) {
                reader.of(checkpointsDir(dir), segmentId);
            }
            Assert.fail("expected segment open to be rejected: " + reason);
        } catch (CairoException e) {
            Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
            TestUtils.assertContains(e.getFlyweightMessage(), reason);
        }
    }

    private void rewriteHeaderRaw(long segmentId, int magic, int version, long selfId, int pageCount, boolean fixCrc) {
        try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
            mem.smallFile(configuration.getFilesFacade(), segmentPath(path, segmentId).$(), MemoryTag.MMAP_DEFAULT);
            mem.putInt(LiveViewCheckpointLayout.SEG_MAGIC_OFFSET, magic);
            mem.putInt(LiveViewCheckpointLayout.SEG_FORMAT_VERSION_OFFSET, version);
            mem.putLong(LiveViewCheckpointLayout.SEG_ID_OFFSET, selfId);
            mem.putInt(LiveViewCheckpointLayout.SEG_PAGE_COUNT_OFFSET, pageCount);
            if (fixCrc) {
                final int crc = Zip.crc32(0, mem.addressOf(0), LiveViewCheckpointLayout.SEG_HEADER_CRC_COVERAGE);
                mem.putInt(LiveViewCheckpointLayout.SEG_HEADER_CRC_OFFSET, crc);
            }
        }
    }

    private LiveViewCheckpointPageRef writeSingleLongPage(long segmentId, int pageKind, long value) {
        final LiveViewCheckpointPageRef ref = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointMetaSegmentWriter writer = new LiveViewCheckpointMetaSegmentWriter(configuration)) {
            try (Path dir = new Path()) {
                writer.of(checkpointsDir(dir), segmentId);
            }
            final MemoryA payload = writer.beginPage(pageKind);
            payload.putLong(value);
            writer.endPage(ref);
            writer.commit();
        }
        return ref;
    }
}
