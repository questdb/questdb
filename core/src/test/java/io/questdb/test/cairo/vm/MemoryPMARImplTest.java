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

package io.questdb.test.cairo.vm;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.MemoryPARWImpl;
import io.questdb.cairo.vm.MemoryPMARImpl;
import io.questdb.cairo.vm.api.MemoryM;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryPMARImplTest {
    private static final Log LOG = LogFactory.getLog(MemoryPMARImplTest.class);

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    /**
     * {@link MemoryPMARImpl#release(long)} msyncs completed pages per the commit mode threaded in via
     * {@code setCommitMode()}, not the instance-global mode, and falls back to the global configuration only for
     * {@link CommitMode#UNSET}. A {@code null} configuration (a NOSYNC global) isolates the threaded mode: with
     * SYNC threaded in, the completed pages are msync'd. UNSET and explicit NOSYNC skip the msync, ASYNC uses
     * {@code msync(async=true)}, and an adaptive {@code applyLazy} column skips it even with SYNC threaded in,
     * since the epoch and WAL replay make it durable.
     */
    @Test
    public void testReleaseHonorsThreadedCommitModeOverGlobalNosync() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE;
            // (a) SYNC threaded in, global NOSYNC (null config): completed pages msync.
            assertReleaseMsync(pageSize, CommitMode.SYNC, false, true, false);
            // (b) never threaded (UNSET) defers to the global mode, NOSYNC with a null config: no msync.
            assertReleaseMsync(pageSize, CommitMode.UNSET, false, false, false);
            // (c) NOSYNC threaded in: no msync.
            assertReleaseMsync(pageSize, CommitMode.NOSYNC, false, false, false);
            // (d) ASYNC threaded in: msync(async=true).
            assertReleaseMsync(pageSize, CommitMode.ASYNC, false, true, true);
            // (e) adaptive lazy-apply column still skips even with SYNC threaded in.
            assertReleaseMsync(pageSize, CommitMode.SYNC, true, false, false);
        });
    }

    // a jump to a page that cannot be mapped: it releases the mapped page and leaves the append offset
    private static void failJump(MemoryPMARImpl mem, AtomicBoolean isFailing, long appendOffset) {
        isFailing.set(true);
        try {
            mem.jumpTo(3 * Files.PAGE_SIZE);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
        } finally {
            isFailing.set(false);
        }
        Assert.assertEquals(appendOffset, mem.getAppendOffset());
        Assert.assertEquals(0, mem.getPageAddress(0));
    }

    private void assertReleaseMsync(
            long pageSize,
            int commitMode,
            boolean applyLazy,
            boolean expectMsync,
            boolean expectAsync
    ) throws Exception {
        final int[] msyncCount = {0};
        final boolean[] lastAsync = {false};
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public void msync(long addr, long len, boolean async) {
                msyncCount[0]++;
                lastAsync[0] = async;
                super.msync(addr, len, async);
            }
        };
        try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
            // null configuration => release()'s global fallback resolves to NOSYNC (a nosync instance).
            try (MemoryPMARImpl mem = new MemoryPMARImpl((CairoConfiguration) null)) {
                mem.of(ff, path.$(), pageSize, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE);
                mem.setApplyLazy(applyLazy);
                mem.setCommitMode(commitMode);
                // Cross >1 page boundary so completed pages are released (msync'd) during append; close()
                // then releases the final active page too.
                for (long i = 0, n = pageSize * 2 + 64; i < n; i += Long.BYTES) {
                    mem.putLong(i);
                }
                mem.close();
            }
        }
        final String ctx = " [commitMode=" + commitMode + ", applyLazy=" + applyLazy + ']';
        if (expectMsync) {
            Assert.assertTrue("expected >=1 release msync but got 0" + ctx, msyncCount[0] > 0);
            Assert.assertEquals("release msync async flag mismatch" + ctx, expectAsync, lastAsync[0]);
        } else {
            Assert.assertEquals("expected ZERO release msync" + ctx, 0, msyncCount[0]);
        }
    }

    @Test
    public void testFailedJumpKeepsAppendOffset() throws Exception {
        // A jump to another page that cannot map it releases the current page and stays where it was. A put
        // without a jump maps the page back and writes at the append offset, also an int across the page end.
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE;
            final AtomicBoolean isFailing = new AtomicBoolean();
            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public boolean allocate(long fd, long size) {
                    return !isFailing.get() && super.allocate(fd, size);
                }
            };
            try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
                try (MemoryPMARImpl mem = new MemoryPMARImpl(ff, path.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    for (long i = 0, n = pageSize / Long.BYTES - 1; i < n; i++) {
                        mem.putLong(i);
                    }
                    mem.putShort((short) 7);
                    Assert.assertEquals(pageSize - 6, mem.getAppendOffset());
                    failJump(mem, isFailing, pageSize - 6);
                    // mid-page: the put maps the page and writes at the append offset, not at the page start
                    mem.putShort((short) 8);
                    mem.putShort((short) 9);
                    Assert.assertEquals(pageSize - 2, mem.getAppendOffset());
                    failJump(mem, isFailing, pageSize - 2);
                    // 2 bytes before the page end: the int crosses into the second page
                    mem.putInt(0x0A0B0C0D);
                    mem.putByte((byte) 10);
                    Assert.assertEquals(pageSize + 3, mem.getAppendOffset());
                }
                try (MemoryCMRImpl file = new MemoryCMRImpl(ff, path.$(), pageSize + 3, MemoryTag.MMAP_DEFAULT)) {
                    Assert.assertEquals(pageSize + 3, file.size());
                    Assert.assertEquals(pageSize / Long.BYTES - 2, file.getLong(pageSize - 16));
                    Assert.assertEquals(7, file.getShort(pageSize - 8));
                    Assert.assertEquals(8, file.getShort(pageSize - 6));
                    Assert.assertEquals(9, file.getShort(pageSize - 4));
                    Assert.assertEquals(0x0A0B0C0D, file.getInt(pageSize - 2));
                    Assert.assertEquals(10, file.getByte(pageSize + 2));
                }
            }
        });
    }

    @Test
    public void testFailedPageSwitchKeepsAppendOffset() throws Exception {
        // The page switch releases the page before it maps the next one. When the mapping fails, nothing must
        // point into the released page: a cancel that jumps back into it, and the puts after it, map it again.
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE;
            final AtomicBoolean isFailing = new AtomicBoolean();
            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public boolean allocate(long fd, long size) {
                    return !isFailing.get() && super.allocate(fd, size);
                }
            };
            final long longsPerPage = pageSize / Long.BYTES;
            try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
                try (MemoryPMARImpl mem = new MemoryPMARImpl(ff, path.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    for (long i = 0; i < longsPerPage; i++) {
                        mem.putLong(i);
                    }
                    isFailing.set(true);
                    try {
                        mem.putLong(-1);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
                    }
                    Assert.assertEquals(pageSize, mem.getAppendOffset());
                    Assert.assertEquals(0, mem.getPageAddress(0));
                    // fails again, and still leaves the append offset
                    try {
                        mem.putLong(-1);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
                    }
                    Assert.assertEquals(pageSize, mem.getAppendOffset());
                    isFailing.set(false);
                    // a row cancel moves back into the released page
                    mem.jumpTo(pageSize - Long.BYTES);
                    mem.putLong(100);
                    mem.putLong(101);
                    Assert.assertEquals(pageSize + Long.BYTES, mem.getAppendOffset());
                }
                try (MemoryCMRImpl file = new MemoryCMRImpl(ff, path.$(), pageSize + Long.BYTES, MemoryTag.MMAP_DEFAULT)) {
                    Assert.assertEquals(pageSize + Long.BYTES, file.size());
                    Assert.assertEquals(longsPerPage - 2, file.getLong(pageSize - 2 * Long.BYTES));
                    Assert.assertEquals(100, file.getLong(pageSize - Long.BYTES));
                    Assert.assertEquals(101, file.getLong(pageSize));
                }
            }
        });
    }

    @Test
    public void testJumpChangesActivePage() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long pageSize = Files.PAGE_SIZE;
            ConcurrentLinkedQueue<Throwable> allErrors = new ConcurrentLinkedQueue<>();
            ObjList<Thread> threads = new ObjList<>();
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;

            for (int thread = 0; thread < 10; thread++) {
                Thread th = new Thread(() -> {

                    try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {

                        LOG.info().$(path).$();
                        try (MemoryPARWImpl mem = new MemoryPMARImpl(ff, path.$(), pageSize, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE)) {
                            long pos;

                            mem.jumpTo(0);
                            String value = "abcdef";
                            mem.putStr(value);
                            pos = mem.getAppendOffset();
                            Assert.assertEquals('f', mem.getChar(pos - 2));

                            mem.jumpTo(2 * pageSize);
                            mem.jumpTo(0);

                            Assert.assertEquals(0, mem.getAppendOffset());
                            Assert.assertEquals(0, mem.pageIndex(mem.getAppendOffset()));

                            long addr = ((MemoryM) mem).map(0, 4);
                            long pageAddress = mem.getPageAddress(0);

                            Assert.assertEquals(pageAddress, addr);
                        }
                    } catch (Throwable e) {
                        allErrors.add(e);
                    }
                });
                th.start();
                threads.add(th);
            }

            for (int i = 0, n = threads.size(); i < n; i++) {
                threads.getQuick(i).join();
            }

            if (!allErrors.isEmpty()) {
                throw new RuntimeException(allErrors.poll());
            }
        });
    }

    @Test
    public void testSyncMsyncsOnlyWhenNotAsync() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final AtomicInteger msyncs = new AtomicInteger();
            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public void msync(long addr, long len, boolean async) {
                    msyncs.incrementAndGet();
                    super.msync(addr, len, async);
                }
            };
            try (
                    Path path = new Path().of(temp.newFile().getAbsolutePath());
                    MemoryPMARImpl mem = new MemoryPMARImpl(ff, path.$(), Files.PAGE_SIZE, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)
            ) {
                mem.putLong(42);
                mem.sync(true);
                Assert.assertEquals(0, msyncs.get());
                mem.sync(false);
                Assert.assertEquals(1, msyncs.get());
            }
        });
    }
}
