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
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MemoryCMARWImplTest {

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    /**
     * A closed instance must not keep the append bounds of the mapping it no longer owns.
     * {@code checkAndExtend()} short-circuits for any address at or below {@code lim}, so a
     * stale {@code lim} turns a use-after-close into a wild write through the unmapped
     * address instead of a loud failure.
     * <p>
     * Assertion order matters: the observational checks run first, so an unfixed build stops
     * before the append below can write through the stale address.
     */
    @Test
    public void testCloseResetsAppendBoundsSoPostCloseAppendFails() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            final long pageSize = Files.PAGE_SIZE;
            final long appendedBytes = 4 * pageSize;
            try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
                final MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        ff,
                        path.$(),
                        pageSize,
                        -1,
                        MemoryTag.MMAP_DEFAULT,
                        CairoConfiguration.O_NONE
                );
                try {
                    // append past the initial mapping so that extend0() runs at least once and
                    // both lim and appendAddress end up pointing deep into the grown mapping
                    for (long i = 0, n = appendedBytes / Long.BYTES; i < n; i++) {
                        mem.putLong(i);
                    }
                    Assert.assertEquals(appendedBytes, mem.getAppendOffset());
                    Assert.assertTrue(mem.size() >= appendedBytes);
                    Assert.assertTrue(mem.addressHi() > 0);
                    Assert.assertTrue(mem.getAppendAddress() > 0);
                } finally {
                    mem.close();
                }

                // 1. safe, purely observational state checks
                Assert.assertFalse(mem.isOpen());
                Assert.assertEquals(-1, mem.getFd());
                Assert.assertEquals(0, mem.size());
                Assert.assertEquals(0, mem.getPageAddress(0));
                Assert.assertEquals(0, mem.getAppendAddress());
                Assert.assertEquals(0, mem.addressHi());
                Assert.assertEquals(0, mem.getAppendOffset());
                // zero() memsets lim - pageAddress bytes from pageAddress, and close() nulls
                // pageAddress before this point, so a stale lim would make zero() memset lim
                // bytes starting at address 0. A clean close leaves that span empty.
                Assert.assertEquals(0, mem.addressHi() - mem.getPageAddress(0));

                // 2. only now the dangerous one: the append must fail loudly rather than write
                // through a stale address
                Throwable failure = null;
                try {
                    mem.putLong(42L);
                } catch (Throwable th) {
                    failure = th;
                }
                Assert.assertNotNull("append to closed memory silently succeeded", failure);
                // with -ea (the core module's surefire default) extend0() trips `assert size > 0`;
                // without it, extend0() reaches allocateDiskSpace() with the nulled files facade
                Assert.assertTrue(
                        "unexpected failure: " + failure,
                        failure instanceof AssertionError || failure instanceof NullPointerException
                );
                Assert.assertEquals(0, mem.getAppendOffset());

                // 3. the offset form of the same hazard, and the one that degenerates worst.
                // jumpTo() evaluates checkAndExtend(pageAddress + offset), so with pageAddress
                // nulled but lim left stale the check compares a bare offset against a stale
                // absolute address -- a comparison any realistic offset wins. jumpTo() then
                // returns silently, parks appendAddress at the raw offset, and hands the next
                // putLong(offset, value) that address to write through. A zeroed lim fails the
                // comparison for every positive offset, so extend0() runs and raises instead.
                Throwable jumpFailure = null;
                try {
                    mem.jumpTo(64);
                } catch (Throwable th) {
                    jumpFailure = th;
                }
                Assert.assertNotNull("jumpTo() on closed memory silently succeeded", jumpFailure);
                Assert.assertTrue(
                        "unexpected failure: " + jumpFailure,
                        jumpFailure instanceof AssertionError || jumpFailure instanceof NullPointerException
                );
                Assert.assertEquals(0, mem.getAppendAddress());
                Assert.assertEquals(0, mem.getAppendOffset());
            }
        });
    }

    /**
     * Closing an instance that was never opened, and closing an already closed one, both end
     * with the same clean state. This test does not pin the placement of the reset in
     * {@code close(boolean, byte)}: it passes with or without it, because neither path ever
     * mapped anything and the fields are already zero. What it does guard is that the reset
     * stays harmless on those two paths. The other tests in this class carry the red
     * evidence for the reset itself.
     */
    @Test
    public void testCloseWithoutOpenLeavesCleanState() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryCMARWImpl mem = new MemoryCMARWImpl();
            mem.close();
            Assert.assertEquals(0, mem.getAppendOffset());
            Assert.assertEquals(0, mem.getAppendAddress());
            Assert.assertEquals(0, mem.addressHi());

            mem.close();
            Assert.assertEquals(0, mem.getAppendOffset());
            Assert.assertEquals(0, mem.getAppendAddress());
            Assert.assertEquals(0, mem.addressHi());
        });
    }

    /**
     * {@code close(false)} must reset the append bounds exactly as {@code close(true)} does.
     * This is the limb the fix exists for: {@code extend0()} and {@code map0()} both call
     * {@code close(false)} when an mremap/mmap fault unwinds them, which is how a live
     * instance ends up with an unmapped file and stale bounds in the first place. The
     * happy-path callers that skip the truncate -- registry compaction, sequencer part
     * rollover, checkpoint -- land here too.
     */
    @Test
    public void testCloseWithoutTruncateResetsAppendBounds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            final long pageSize = Files.PAGE_SIZE;
            final long appendedBytes = 4 * pageSize;
            try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
                final MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        ff,
                        path.$(),
                        pageSize,
                        -1,
                        MemoryTag.MMAP_DEFAULT,
                        CairoConfiguration.O_NONE
                );
                try {
                    for (long i = 0, n = appendedBytes / Long.BYTES; i < n; i++) {
                        mem.putLong(i);
                    }
                    Assert.assertEquals(appendedBytes, mem.getAppendOffset());
                    Assert.assertTrue(mem.getAppendAddress() > 0);
                    Assert.assertTrue(mem.addressHi() > 0);
                } finally {
                    // the fault paths in extend0()/map0() unwind through this call, not close()
                    mem.close(false);
                }

                // observational checks first, so an unfixed build stops here rather than in the
                // jumpTo() below
                Assert.assertFalse(mem.isOpen());
                Assert.assertEquals(0, mem.size());
                Assert.assertEquals(0, mem.getPageAddress(0));
                Assert.assertEquals(0, mem.getAppendAddress());
                Assert.assertEquals(0, mem.addressHi());
                Assert.assertEquals(0, mem.getAppendOffset());

                Throwable failure = null;
                try {
                    mem.jumpTo(64);
                } catch (Throwable th) {
                    failure = th;
                }
                Assert.assertNotNull("jumpTo() on closed memory silently succeeded", failure);
                Assert.assertTrue(
                        "unexpected failure: " + failure,
                        failure instanceof AssertionError || failure instanceof NullPointerException
                );
                Assert.assertEquals(0, mem.getAppendOffset());
            }
        });
    }

    /**
     * A close in the middle of an open-append-close-reopen cycle must not leak the previous
     * mapping's bounds into the reopened instance.
     */
    @Test
    public void testReopenAfterCloseRestoresAppendBounds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            final long pageSize = Files.PAGE_SIZE;
            try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
                final MemoryCMARWImpl mem = new MemoryCMARWImpl();
                try {
                    mem.of(ff, path.$(), pageSize, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE, -1);
                    for (int i = 0; i < 1_000; i++) {
                        mem.putLong(i);
                    }
                    Assert.assertEquals(8_000, mem.getAppendOffset());
                    mem.close();
                    Assert.assertEquals(0, mem.getAppendOffset());

                    mem.of(ff, path.$(), pageSize, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE, -1);
                    Assert.assertTrue(mem.isOpen());
                    Assert.assertTrue(mem.getAppendAddress() > 0);
                    // map0() rebuilt lim from the new mapping rather than carrying the old one over
                    Assert.assertEquals(pageSize, mem.size());
                    Assert.assertEquals(mem.size(), mem.addressHi() - mem.getPageAddress(0));
                    Assert.assertEquals(0, mem.getAppendOffset());
                    mem.putLong(1L);
                    Assert.assertEquals(Long.BYTES, mem.getAppendOffset());
                } finally {
                    mem.close();
                }
            }
        });
    }
}
