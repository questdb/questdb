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
import io.questdb.cairo.CommitMode;
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

public class MemoryPMARImplTest {
    private static final Log LOG = LogFactory.getLog(MemoryPMARImplTest.class);

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    /**
     * MEDIUM review fix: {@link MemoryPMARImpl#release(long)} must msync completed/flipped pages according
     * to the per-table EFFECTIVE commit mode threaded via {@code setCommitMode()}, NOT the instance-global
     * mode. A {@code WITH commit_mode='sync'} column on a {@code nosync} instance would otherwise skip the
     * release msync and crash-lose committed rows. {@code release()} consults the global configuration only
     * as the {@link CommitMode#UNSET} fallback, so a {@code null} configuration (equivalent to a NOSYNC
     * global) lets the per-table override be proven in isolation: with a SYNC override the completed pages
     * must be msync'd even though the "global" resolves to NOSYNC.
     *
     * <p>RED before the fix (release read the global NOSYNC and skipped msync); GREEN after release()
     * prefers the threaded per-table mode. Negative controls pin: UNSET/explicit-NOSYNC skip, ASYNC uses
     * {@code msync(async=true)}, and an adaptive {@code applyLazy} column still skips even under a SYNC
     * override (its durability is the epoch + WAL roll-forward, not the release msync).
     */
    @Test
    public void testReleaseHonorsPerTableCommitModeOverGlobalNosync() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE;
            // (a) per-table SYNC override, "global" == NOSYNC (null config): completed pages MUST msync.
            assertReleaseMsync(pageSize, CommitMode.SYNC, false, true, false);
            // (b) negative control — no override (UNSET) defers to the (null==NOSYNC) global => NO msync.
            assertReleaseMsync(pageSize, CommitMode.UNSET, false, false, false);
            // (c) explicit NOSYNC override => NO msync.
            assertReleaseMsync(pageSize, CommitMode.NOSYNC, false, false, false);
            // (d) per-table ASYNC override => msync(async=true).
            assertReleaseMsync(pageSize, CommitMode.ASYNC, false, true, true);
            // (e) adaptive lazy-apply column still skips even with a SYNC override.
            assertReleaseMsync(pageSize, CommitMode.SYNC, true, false, false);
        });
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
}
