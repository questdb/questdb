/*+*****************************************************************************
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

package io.questdb.test.cairo.idx;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.idx.PostingIndexChainEntry;
import io.questdb.cairo.idx.PostingIndexChainHeader;
import io.questdb.cairo.idx.PostingIndexChainPicker;
import io.questdb.cairo.idx.PostingIndexKeyFileView;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

public class PostingIndexKeyFileViewTest extends AbstractCairoTest {
    private static final long REGION_BASE = PostingIndexUtils.V2_ENTRY_REGION_BASE;
    // A refill moves the window down by at least this much: it keeps WINDOW_TAIL bytes above the value it is for
    private static final long WINDOW_STEP = PostingIndexKeyFileView.WINDOW_SIZE - PostingIndexKeyFileView.WINDOW_TAIL - Long.BYTES;

    @Test
    public void testFullWalkReadsHeadOneValueAtATimeAndTheRestByWindow() throws Exception {
        // A walk down the whole chain reads the header pages and the head entry one value per pread: the 12 values
        // of PostingIndexChainHeader.readUnderSeqlock, then the LEN peek of findVisibleBySealTxn and the 10 values of
        // PostingIndexChainEntry.read for an entry without covering columns. The entries below the head take one
        // window fill per WINDOW_STEP, against one pread per value without the window.
        assertMemoryLeak(() -> {
            final CountingFf countingFf = new CountingFf();
            final PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
            final PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();
            try (Path path = new Path(); PostingIndexKeyFileView view = new PostingIndexKeyFileView()) {
                build(path, "pk_view_walk", "g40", 40, 127, 0);
                final long fd = countingFf.openRO(keyFile(path, "pk_view_walk", "g40"));
                try {
                    view.of(countingFf, fd, countingFf.length(fd));
                    countingFf.trackedFd = fd;
                    Assert.assertTrue(PostingIndexChainHeader.readUnderSeqlock(view, header));
                    view.setHeader(header);
                    // no entry names a negative sealTxn: the walk visits every entry
                    Assert.assertEquals(PostingIndexChainPicker.RESULT_NO_VISIBLE_ENTRY,
                            PostingIndexChainPicker.findVisibleBySealTxn(view, header, -7, 1, entry));
                } finally {
                    view.detach();
                    countingFf.close(fd);
                }
                Assert.assertEquals(12 + 11, countingFf.perValueReads);
                final long belowHead = header.headEntryOffset - header.regionBase;
                Assert.assertTrue("the walk must need more than one window", belowHead > PostingIndexKeyFileView.WINDOW_SIZE);
                Assert.assertTrue(countingFf.windowReads >= (belowHead + PostingIndexKeyFileView.WINDOW_SIZE - 1) / PostingIndexKeyFileView.WINDOW_SIZE);
                Assert.assertTrue(countingFf.windowReads <= 1 + (belowHead + WINDOW_STEP - 1) / WINDOW_STEP);
            }
        });
    }

    @Test
    public void testReadsInEveryZone() throws Exception {
        // A file whose bytes the test sets: the header pages, the entries below the head entry the header names,
        // and the head entry with what follows. Every value reads as the bytes at its offset hold it, negative ones
        // included, as through a mapping, and a value that the file ends before reads as -1.
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            final CountingFf countingFf = new CountingFf();
            final long fileSize = REGION_BASE + 4 * PostingIndexKeyFileView.WINDOW_SIZE + 2048;
            final long head = fileSize - 2048;
            final long buf = Unsafe.malloc(fileSize, MemoryTag.NATIVE_DEFAULT);
            try (Path path = new Path().of(root).concat("pk_view_zones.pk");
                 PostingIndexKeyFileView view = new PostingIndexKeyFileView()) {
                for (long k = 0; k < fileSize; k++) {
                    // the high bit stays clear, so that no value of the pattern is -1 or below
                    Unsafe.getUnsafe().putByte(buf + k, (byte) ((k * 31 + 7) & 0x7F));
                }
                Unsafe.getUnsafe().putLong(buf + 16, -5L);
                Unsafe.getUnsafe().putLong(buf + REGION_BASE + 64, -5L);
                Unsafe.getUnsafe().putInt(buf + REGION_BASE + 128, -7);
                Unsafe.getUnsafe().putLong(buf + head + 16, -5L);
                Unsafe.getUnsafe().putInt(buf + head + 32, -7);
                final long rw = ff.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(rw > -1);
                try {
                    Assert.assertEquals(fileSize, ff.write(rw, buf, fileSize, 0));
                } finally {
                    ff.close(rw);
                }

                final long fd = countingFf.openRO(path.$());
                Assert.assertTrue(fd > -1);
                countingFf.trackedFd = fd;
                try {
                    view.of(countingFf, fd, fileSize);
                    Assert.assertEquals(fd, view.getFd());
                    Assert.assertEquals(fileSize, view.size());

                    // until setHeader() runs, every value comes from a pread of its own
                    assertLong(view, countingFf, buf, REGION_BASE + 1024, 1, 0);

                    final PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
                    header.regionBase = REGION_BASE;
                    header.headEntryOffset = head;
                    view.setHeader(header);

                    // header pages: one pread per value
                    assertLong(view, countingFf, buf, 0, 1, 0);
                    assertLong(view, countingFf, buf, 16, 1, 0);
                    assertLong(view, countingFf, buf, REGION_BASE - Long.BYTES, 1, 0);
                    assertInt(view, countingFf, buf, 100, 1, 0);

                    // below the head entry: the first value fills the window and the values near it come from it
                    assertLong(view, countingFf, buf, head - Long.BYTES, 0, 1);
                    assertLong(view, countingFf, buf, head - 2 * Long.BYTES, 0, 0);
                    assertInt(view, countingFf, buf, head - Integer.BYTES, 0, 0);
                    // a refill keeps WINDOW_TAIL bytes above the value it is for: a long that starts four bytes before
                    // that end crosses it and fills the window again
                    final long low = REGION_BASE + PostingIndexKeyFileView.WINDOW_SIZE;
                    assertLong(view, countingFf, buf, low, 0, 1);
                    assertLong(view, countingFf, buf, low + Long.BYTES + PostingIndexKeyFileView.WINDOW_TAIL - 4, 0, 1);
                    // down the whole zone, as a walk goes
                    final int windowReads = countingFf.windowReads;
                    final int perValueReads = countingFf.perValueReads;
                    for (long offset = head - Long.BYTES; offset >= REGION_BASE; offset -= 4096) {
                        Assert.assertEquals("offset=" + offset, expectedLong(buf, offset), view.getLong(offset));
                    }
                    Assert.assertEquals(perValueReads, countingFf.perValueReads);
                    Assert.assertTrue(countingFf.windowReads - windowReads <= 1 + (head - REGION_BASE + WINDOW_STEP - 1) / WINDOW_STEP);
                    // back up after the walk reached the bottom
                    assertLong(view, countingFf, buf, head - Long.BYTES, 0, 1);
                    Assert.assertEquals(-5, view.getLong(REGION_BASE + 64));
                    Assert.assertEquals(-7, view.getInt(REGION_BASE + 128));

                    // a long that crosses into the head entry, and the head entry onwards: one pread per value
                    assertLong(view, countingFf, buf, head - 4, 1, 0);
                    assertLong(view, countingFf, buf, head, 1, 0);
                    assertLong(view, countingFf, buf, head + 16, 1, 0);
                    assertInt(view, countingFf, buf, head + 32, 1, 0);
                    assertLong(view, countingFf, buf, fileSize - Long.BYTES, 1, 0);
                    assertInt(view, countingFf, buf, fileSize - Integer.BYTES, 1, 0);

                    // the writer shrinks the file under the view
                    final long shrunkSize = REGION_BASE + 10_000;
                    final long rwShrink = ff.openRW(path.$(), CairoConfiguration.O_NONE);
                    try {
                        Assert.assertTrue(ff.truncate(rwShrink, shrunkSize));
                    } finally {
                        ff.close(rwShrink);
                    }
                    // the window lies past the new end: nothing to read
                    Assert.assertEquals(-1, view.getLong(head - Long.BYTES));
                    // values that cross the new end
                    Assert.assertEquals(-1, view.getLong(shrunkSize - 4));
                    Assert.assertEquals(-1, view.getInt(shrunkSize - 2));
                    // the refill read what the file still has
                    assertLong(view, countingFf, buf, shrunkSize - 1000, 0, 0);
                    // past the end, read one value at a time
                    Assert.assertEquals(-1, view.getLong(head));
                    Assert.assertEquals(-1, view.getInt(head + 4));
                    assertLong(view, countingFf, buf, 0, 1, 0);

                    view.detach();
                    Assert.assertEquals(-1, view.getFd());
                    Assert.assertEquals(0, view.size());

                    // the view reads the next file with the window it kept
                    view.of(countingFf, fd, shrunkSize);
                    view.setHeader(header);
                    assertLong(view, countingFf, buf, REGION_BASE + 9_000, 0, 1);
                } finally {
                    view.detach();
                    countingFf.close(fd);
                }
            } finally {
                Unsafe.free(buf, fileSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testUnsupportedAccessorsThrow() {
        try (PostingIndexKeyFileView view = new PostingIndexKeyFileView()) {
            Assert.assertThrows(UnsupportedOperationException.class, () -> view.addressOf(0));
            Assert.assertThrows(UnsupportedOperationException.class, view::addressHi);
            Assert.assertThrows(UnsupportedOperationException.class, () -> view.extend(1024));
            // the other accessors of the memory interfaces read through addressOf()
            Assert.assertThrows(UnsupportedOperationException.class, () -> view.getByte(0));
            Assert.assertEquals(-1, view.getFd());
        }
    }

    @Test
    public void testWalkMatchesMappedWalk() throws Exception {
        // The walk over a mapping, which the readers of the index use, is the reference: through the view, every
        // target sealTxn must get the same entry for every reader position. The chains are empty, fit one window,
        // need several windows, and carry a covering column.
        assertMemoryLeak(() -> {
            final long coverRows = 100 * 4;
            final long coverAddr = Unsafe.malloc(coverRows * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            try (Path path = new Path();
                 MemoryCMRImpl mapped = new MemoryCMRImpl();
                 PostingIndexKeyFileView view = new PostingIndexKeyFileView()) {
                Unsafe.setMemory(coverAddr, coverRows * Integer.BYTES, (byte) 0);
                assertWalksMatch(path, mapped, view, "empty", 0, 0, 0);
                assertWalksMatch(path, mapped, view, "s2", 2, 1, 0);
                assertWalksMatch(path, mapped, view, "s300", 300, 1, 0);
                assertWalksMatch(path, mapped, view, "g40", 40, 127, 0);
                assertWalksMatch(path, mapped, view, "cov100", 100, 1, coverAddr);
            } finally {
                Unsafe.free(coverAddr, coverRows * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void assertInt(PostingIndexKeyFileView view, CountingFf countingFf, long buf, long offset, int perValueReads, int windowReads) {
        final int perValueBefore = countingFf.perValueReads;
        final int windowBefore = countingFf.windowReads;
        Assert.assertEquals("offset=" + offset, Unsafe.getUnsafe().getInt(buf + offset), view.getInt(offset));
        Assert.assertEquals("per-value preads at offset=" + offset, perValueReads, countingFf.perValueReads - perValueBefore);
        Assert.assertEquals("window fills at offset=" + offset, windowReads, countingFf.windowReads - windowBefore);
    }

    private static void assertLong(PostingIndexKeyFileView view, CountingFf countingFf, long buf, long offset, int perValueReads, int windowReads) {
        final int perValueBefore = countingFf.perValueReads;
        final int windowBefore = countingFf.windowReads;
        Assert.assertEquals("offset=" + offset, expectedLong(buf, offset), view.getLong(offset));
        Assert.assertEquals("per-value preads at offset=" + offset, perValueReads, countingFf.perValueReads - perValueBefore);
        Assert.assertEquals("window fills at offset=" + offset, windowReads, countingFf.windowReads - windowBefore);
    }

    private static long expectedLong(long buf, long offset) {
        return Unsafe.getUnsafe().getLong(buf + offset);
    }

    private static LPSZ keyFile(Path path, String dir, String col) {
        return PostingIndexUtils.keyFileName(path.of(root).concat(dir), col, COLUMN_NAME_TXN_NONE);
    }

    private void assertWalksMatch(
            Path path,
            MemoryCMRImpl mapped,
            PostingIndexKeyFileView view,
            String col,
            int entries,
            int gensPerEntry,
            long coverAddr
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        final String dir = "pk_view_diff";
        build(path, dir, col, entries, gensPerEntry, coverAddr);
        final PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        final PostingIndexChainEntry.Snapshot mappedEntry = new PostingIndexChainEntry.Snapshot();
        final PostingIndexChainEntry.Snapshot viewEntry = new PostingIndexChainEntry.Snapshot();
        final long head = PostingIndexUtils.readSealTxnFromKeyFile(ff, keyFile(path, dir, col));
        int visible = 0;
        int notVisible = 0;
        for (long target = head; target >= head - entries - 1; target--) {
            for (long minPinnedTableTxn : new long[]{1, 10L * entries / 2 + 3, 10L * entries + 100}) {
                mapped.of(ff, keyFile(path, dir, col), ff.getMapPageSize(), -1, MemoryTag.MMAP_INDEX_READER, CairoConfiguration.O_NONE, -1);
                final int expected;
                try {
                    expected = PostingIndexChainPicker.findVisibleBySealTxn(mapped, target, minPinnedTableTxn, header, mappedEntry);
                } finally {
                    mapped.close();
                }
                final long fd = ff.openRO(keyFile(path, dir, col));
                Assert.assertTrue(fd > -1);
                final int actual;
                try {
                    view.of(ff, fd, ff.length(fd));
                    Assert.assertTrue(PostingIndexChainHeader.readUnderSeqlock(view, header));
                    view.setHeader(header);
                    actual = PostingIndexChainPicker.findVisibleBySealTxn(view, header, target, minPinnedTableTxn, viewEntry);
                } finally {
                    view.detach();
                    ff.close(fd);
                }
                final String what = col + ": target=" + target + ", minPinnedTableTxn=" + minPinnedTableTxn;
                Assert.assertEquals(what, expected, actual);
                if (expected == PostingIndexChainPicker.RESULT_OK) {
                    Assert.assertEquals(what, mappedEntry.sealTxn, viewEntry.sealTxn);
                    Assert.assertEquals(what, mappedEntry.txnAtSeal, viewEntry.txnAtSeal);
                    Assert.assertEquals(what, mappedEntry.offset, viewEntry.offset);
                    visible++;
                } else {
                    notVisible++;
                }
            }
        }
        if (entries == 0) {
            Assert.assertEquals(0, visible);
        } else if (entries > 1) {
            // both answers, so that the comparison proves something
            Assert.assertTrue(col, visible > 0 && notVisible > 0);
        }
    }

    private void build(Path path, String dir, String col, int entries, int gensPerEntry, long coverAddr) {
        path.of(root).concat(dir).slash();
        configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode());
        path.of(root).concat(dir);
        try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, col, COLUMN_NAME_TXN_NONE)) {
            if (coverAddr != 0) {
                writer.configureCovering(new long[]{coverAddr}, new long[]{0L}, new int[]{2}, new int[]{0}, new int[]{ColumnType.INT}, 1);
            }
            int row = 0;
            for (int e = 0; e < entries; e++) {
                for (int g = 0; g < gensPerEntry; g++) {
                    for (int i = 0; i < 4; i++, row++) {
                        writer.add(row % 4, row);
                    }
                    writer.setMaxValue(row - 1);
                    writer.commit();
                }
                // increasing txnAtSeal values, so that reader positions split the chain
                writer.setNextTxnAtSeal(10L * (e + 1));
                writer.seal();
            }
        }
    }

    // Counts the reads of one fd: one pread per value, or a window fill
    private static final class CountingFf extends TestFilesFacadeImpl {
        private int perValueReads;
        private long trackedFd = -1;
        private int windowReads;

        @Override
        public long read(long fd, long buf, long size, long offset) {
            if (fd == trackedFd) {
                windowReads++;
            }
            return super.read(fd, buf, size, offset);
        }

        @Override
        public int readNonNegativeInt(long fd, long offset) {
            if (fd == trackedFd) {
                perValueReads++;
            }
            return super.readNonNegativeInt(fd, offset);
        }

        @Override
        public long readNonNegativeLong(long fd, long offset) {
            if (fd == trackedFd) {
                perValueReads++;
            }
            return super.readNonNegativeLong(fd, offset);
        }
    }
}
