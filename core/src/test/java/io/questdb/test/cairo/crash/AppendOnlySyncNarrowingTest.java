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

package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.MemoryPMARImpl;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;

/**
 * Proves that append-only memories narrow the SYNC-mode msync to the written range, and that the contiguous
 * memory (CMARW) also skips the msync when nothing new is appended. A recording FilesFacade counts every msync
 * and records its address and length, against {@link MemoryCMARWImpl} (symbol char memory) and
 * {@link MemoryPMARImpl} (column data and aux vectors):
 * <ul>
 *   <li>(a) an append-only CMARW synced twice with no write between issues no msync on the second sync, and a
 *       sync after a write msyncs the narrowed length ({@code appendOffset}), not the full mapped extent;</li>
 *   <li>(b) an append-only PMAR synced after writing N bytes into the active page msyncs exactly N bytes, not
 *       the whole page window.</li>
 * </ul>
 * The controls, without {@code setAppendOnly}, still msync the full extent.
 */
public class AppendOnlySyncNarrowingTest {

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testAppendOnlyNarrowAndSkip() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE; // one OS page
            final RecordingFilesFacade ff = new RecordingFilesFacade();

            // (a) An append-only CMARW (symbol char memory) narrows on write and skips when nothing new is appended.
            try (Path path = new Path().of(temp.newFile("char.d").getAbsolutePath())) {
                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        ff, path.$(), pageSize, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    mem.setAppendOnly(true);
                    mem.jumpTo(0);

                    // write a known number of bytes, then sync
                    final int n1 = 100;
                    for (int i = 0; i < n1; i++) {
                        mem.putByte((byte) i);
                    }
                    final long ao1 = mem.getAppendOffset();
                    Assert.assertEquals(n1, ao1);

                    ff.clear();
                    mem.sync(false);
                    // Narrow: exactly one msync, over [pageAddress, appendOffset), not the full page extent.
                    Assert.assertEquals("appendOnly CMARW must issue exactly one narrowed msync",
                            1, ff.msyncCount);
                    Assert.assertEquals("CMARW msync length must be narrowed to appendOffset",
                            ao1, ff.lastMsyncLen);
                    Assert.assertTrue("narrowed length must be well below the full mapped extent",
                            ff.lastMsyncLen < pageSize);

                    // Skip: a second sync with no write in between issues no msync.
                    ff.clear();
                    mem.sync(false);
                    Assert.assertEquals("appendOnly CMARW must SKIP msync when nothing new appended",
                            0, ff.msyncCount);

                    // Append more, sync -> a single narrowed msync over the new, larger appendOffset.
                    final int n2 = 50;
                    for (int i = 0; i < n2; i++) {
                        mem.putByte((byte) i);
                    }
                    final long ao2 = mem.getAppendOffset();
                    ff.clear();
                    mem.sync(false);
                    Assert.assertEquals(1, ff.msyncCount);
                    Assert.assertEquals("CMARW msync length must track the new appendOffset",
                            ao2, ff.lastMsyncLen);
                }
            }

            // (a) control: a default CMARW, not append-only, msyncs the full mapped extent.
            try (Path path = new Path().of(temp.newFile("char_ctrl.d").getAbsolutePath())) {
                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        ff, path.$(), pageSize, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    // no setAppendOnly -> default false
                    mem.jumpTo(0);
                    for (int i = 0; i < 100; i++) {
                        mem.putByte((byte) i);
                    }
                    ff.clear();
                    mem.sync(false);
                    Assert.assertEquals(1, ff.msyncCount);
                    Assert.assertEquals("non-appendOnly CMARW must msync the full mapped extent",
                            pageSize, ff.lastMsyncLen);
                    // and a second sync with no write still full-syncs (no skip on the default path)
                    ff.clear();
                    mem.sync(false);
                    Assert.assertEquals(1, ff.msyncCount);
                    Assert.assertEquals(pageSize, ff.lastMsyncLen);
                }
            }

            // (b) An append-only PMAR (column data and aux vectors) only narrows: it msyncs the in-page written length.
            try (Path path = new Path().of(temp.newFile("col.d").getAbsolutePath())) {
                try (MemoryPMARImpl mem = new MemoryPMARImpl(
                        ff, path.$(), pageSize, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE)) {
                    mem.setAppendOnly(true);
                    mem.jumpTo(0);

                    final int n = 137; // bytes into the first (active) page
                    for (int i = 0; i < n; i++) {
                        mem.putByte((byte) i);
                    }
                    Assert.assertEquals(n, mem.getAppendOffset());

                    ff.clear();
                    mem.sync(false);
                    Assert.assertEquals("appendOnly PMAR must issue exactly one narrowed msync",
                            1, ff.msyncCount);
                    Assert.assertEquals("PMAR msync length must be the in-page written length",
                            n, ff.lastMsyncLen);
                    Assert.assertTrue("narrowed length must be well below the page window",
                            ff.lastMsyncLen < pageSize);
                }
            }

            // (b) control: a default PMAR, not append-only, msyncs the full page window.
            try (Path path = new Path().of(temp.newFile("col_ctrl.d").getAbsolutePath())) {
                try (MemoryPMARImpl mem = new MemoryPMARImpl(
                        ff, path.$(), pageSize, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE)) {
                    // no setAppendOnly -> default false
                    mem.jumpTo(0);
                    for (int i = 0; i < 137; i++) {
                        mem.putByte((byte) i);
                    }
                    ff.clear();
                    mem.sync(false);
                    Assert.assertEquals(1, ff.msyncCount);
                    Assert.assertEquals("non-appendOnly PMAR must msync the full page window",
                            pageSize, ff.lastMsyncLen);
                }
            }
        });
    }

    /**
     * The append-only skip must not survive a rewind of the append cursor.
     *
     * <p>{@code MemoryCMARWImpl.sync()} skips the msync when {@code appendOffset} and {@code size} are both
     * unchanged since the last sync. Across a rollback that does not mean nothing is dirty:
     * {@code WalWriter.rollback0()} -&gt; {@code setAppendPosition()} -&gt; {@code jumpTo(lower)} rewinds the
     * cursor, and the next commit re-appends different bytes over the same range. For a fixed-width column the
     * cursor lands on its old value, so both watermarks match again.
     *
     * <p>{@code jumpTo()} lowers {@code lastSyncedAppendOffset} on any backwards move, so the rewritten bytes
     * are msynced. The forward control checks that a {@code jumpTo} that does not move back keeps the skip.
     */
    @Test
    public void testAppendOnlySkipInvalidatedByRewind() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE;
            final RecordingFilesFacade ff = new RecordingFilesFacade();

            try (Path path = new Path().of(temp.newFile("rewind.d").getAbsolutePath())) {
                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        ff, path.$(), pageSize, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    mem.setAppendOnly(true);
                    mem.jumpTo(0);

                    // Commit 1: append 100 bytes and sync them.
                    for (int i = 0; i < 100; i++) {
                        mem.putByte((byte) 0xA);
                    }
                    Assert.assertEquals(100, mem.getAppendOffset());
                    ff.clear();
                    mem.sync(false);
                    Assert.assertEquals("baseline: first sync must msync", 1, ff.msyncCount);

                    // Baseline: with no write at all, the skip is in force.
                    ff.clear();
                    mem.sync(false);
                    Assert.assertEquals("baseline: idle sync must skip", 0, ff.msyncCount);

                    // Forward control: a jumpTo to the current offset keeps the watermark, so the skip
                    // still holds.
                    mem.jumpTo(100);
                    ff.clear();
                    mem.sync(false);
                    Assert.assertEquals("forward/no-op jumpTo must not defeat the skip", 0, ff.msyncCount);

                    // Rollback: rewind to 80 and re-append 20 different bytes. The cursor returns to 100
                    // and `size` is unchanged, so both match the watermarks of the last sync.
                    mem.jumpTo(80);
                    for (int i = 0; i < 20; i++) {
                        mem.putByte((byte) 0xB);
                    }
                    Assert.assertEquals("test setup: cursor must land back on the pre-rollback offset",
                            100, mem.getAppendOffset());

                    ff.clear();
                    mem.sync(false);
                    Assert.assertEquals(
                            "rewound-and-refilled range MUST be msync'd, not skipped",
                            1, ff.msyncCount);
                    Assert.assertTrue(
                            "the msync must cover the rewritten range [80, 100)",
                            ff.lastMsyncLen >= 100);

                    // And once that sync has landed, the skip is available again.
                    ff.clear();
                    mem.sync(false);
                    Assert.assertEquals("skip must be restored after the post-rollback sync",
                            0, ff.msyncCount);

                    // A deeper rewind that is only partly refilled must also msync: the watermark drops to
                    // 40 and the cursor ends at 60.
                    mem.jumpTo(40);
                    for (int i = 0; i < 20; i++) {
                        mem.putByte((byte) 0xC);
                    }
                    Assert.assertEquals(60, mem.getAppendOffset());
                    ff.clear();
                    mem.sync(false);
                    Assert.assertEquals("partial refill after a rewind must also msync", 1, ff.msyncCount);
                    Assert.assertEquals("msync must cover [0, appendOffset)", 60, ff.lastMsyncLen);
                }
            }

            // Control: a memory that is not append-only never skips, and the rewind hook does not change that.
            try (Path path = new Path().of(temp.newFile("rewind_ctrl.d").getAbsolutePath())) {
                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        ff, path.$(), pageSize, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    mem.jumpTo(0);
                    for (int i = 0; i < 100; i++) {
                        mem.putByte((byte) 0xA);
                    }
                    ff.clear();
                    mem.sync(false);
                    Assert.assertEquals(1, ff.msyncCount);
                    mem.jumpTo(80);
                    ff.clear();
                    mem.sync(false);
                    Assert.assertEquals("non-appendOnly path must be unaffected by the rewind hook",
                            1, ff.msyncCount);
                    Assert.assertEquals(pageSize, ff.lastMsyncLen);
                }
            }
        });
    }

    /**
     * FilesFacade that records every msync's (addr, len) and counts them.
     */
    private static final class RecordingFilesFacade extends TestFilesFacadeImpl {
        final List<long[]> msyncs = new ArrayList<>();
        long lastMsyncAddr = -1;
        long lastMsyncLen = -1;
        int msyncCount = 0;

        void clear() {
            msyncs.clear();
            msyncCount = 0;
            lastMsyncAddr = -1;
            lastMsyncLen = -1;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            super.msync(addr, len, async);
            msyncCount++;
            lastMsyncAddr = addr;
            lastMsyncLen = len;
            msyncs.add(new long[]{addr, len});
        }
    }
}
