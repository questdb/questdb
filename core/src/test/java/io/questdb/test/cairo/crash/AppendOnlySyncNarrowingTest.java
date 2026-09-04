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
 * Mechanism-level proof that append-only memories narrow the SYNC-mode msync to the written range,
 * and that the contiguous (CMARW) memory additionally SKIPs msync when nothing new was appended.
 *
 * <p>Uses a recording FilesFacade that counts every msync and records its (addr, len). This asserts
 * on the actual {@code sync()} behaviour of {@link MemoryCMARWImpl} (symbol char mem) and
 * {@link MemoryPMARImpl} (column data/aux vectors):
 * <ul>
 *   <li>(a) appendOnly CMARW synced twice with no write between → the 2nd sync issues NO msync (skip),
 *       and a CMARW sync after a write msyncs the NARROWED length (== appendOffset), not the full
 *       mapped extent.</li>
 *   <li>(b) appendOnly PMAR (NARROW-ONLY) synced after writing N bytes into the active page msyncs
 *       exactly N bytes (the in-page written length), not the whole page window.</li>
 * </ul>
 * The default-false control case (no setAppendOnly) is checked to still msync the full extent so the
 * non-appendOnly path is unchanged.
 */
public class AppendOnlySyncNarrowingTest {

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testAppendOnlyNarrowAndSkip() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE; // one OS page
            final RecordingFilesFacade ff = new RecordingFilesFacade();

            // ---------------------------------------------------------------------------------
            // (a) CMARW (symbol char mem): NARROW on write + SKIP when nothing new was appended.
            // ---------------------------------------------------------------------------------
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
                    // NARROW: exactly one msync, over [pageAddress, appendOffset), not the full page extent.
                    Assert.assertEquals("appendOnly CMARW must issue exactly one narrowed msync",
                            1, ff.msyncCount);
                    Assert.assertEquals("CMARW msync length must be narrowed to appendOffset",
                            ao1, ff.lastMsyncLen);
                    Assert.assertTrue("narrowed length must be well below the full mapped extent",
                            ff.lastMsyncLen < pageSize);

                    // SKIP: sync again with no write in between -> no msync at all.
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

            // ---------------------------------------------------------------------------------
            // (a-control) default (NOT appendOnly) CMARW must still msync the FULL mapped extent.
            // ---------------------------------------------------------------------------------
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

            // ---------------------------------------------------------------------------------
            // (b) PMAR (column data/aux vector): NARROW-ONLY — msync the in-page written length.
            // ---------------------------------------------------------------------------------
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

            // ---------------------------------------------------------------------------------
            // (b-control) default (NOT appendOnly) PMAR must still msync the full page window.
            // ---------------------------------------------------------------------------------
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
     * The append-only SKIP must NOT survive a REWIND of the append cursor.
     *
     * <p>{@code MemoryCMARWImpl.sync()} skips the msync when {@code appendOffset} and {@code size} are both
     * unchanged since the last sync. The naive reading of that condition ("nothing appended => nothing
     * dirty") is FALSE across a rollback: {@code WalWriter.rollback0()} -&gt; {@code setAppendPosition()} -&gt;
     * {@code jumpTo(lower)} rewinds the cursor and the next commit re-appends DIFFERENT bytes over the same
     * range. For a fixed-width column the cursor lands on exactly its old value, so both watermarks match
     * again and the rewritten bytes would never be msync'd.
     *
     * <p>{@code jumpTo()} closes the hole by lowering {@code lastSyncedAppendOffset} on any backwards move.
     * This test drives that exact sequence and asserts the msync happens; the FORWARD control below pins
     * that the fix does not over-invalidate and destroy the skip optimisation itself.
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

                    // FORWARD control: a jumpTo to the CURRENT offset must not disturb the watermark, so
                    // the skip must still hold. Guards against over-invalidating and killing the
                    // optimisation this fix is meant to preserve.
                    mem.jumpTo(100);
                    ff.clear();
                    mem.sync(false);
                    Assert.assertEquals("forward/no-op jumpTo must not defeat the skip", 0, ff.msyncCount);

                    // ROLLBACK: rewind to 80 and re-append 20 DIFFERENT bytes. The cursor returns to
                    // exactly 100 and `size` is unchanged -- the precise shape that used to skip.
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

                    // A deeper rewind that is only PARTLY refilled must also msync (watermark dropped to
                    // 40, cursor ends at 60 > 40).
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

            // Control: a NON-appendOnly memory never skips, so the rewind hook must not change it.
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
