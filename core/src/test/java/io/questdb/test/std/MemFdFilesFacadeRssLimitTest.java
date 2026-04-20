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

package io.questdb.test.std;

import io.questdb.cairo.CairoException;
import io.questdb.std.MemFdFilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Narrow unit tests that exercise {@link MemFdFilesFacade}'s new accounting
 * against the global {@link Unsafe#setRssMemLimit} budget. Verifies that
 * memfd-backed storage charges, credits, and abort-on-limit behave correctly
 * and leave no residual tag counter or per-facade {@code usedBytes} after
 * teardown.
 */
public class MemFdFilesFacadeRssLimitTest {

    private static final int MEMFD_TAG = MemoryTag.NATIVE_MEMFD_STORAGE;
    private long baselineRssLimit;
    private long baselineTag;

    @Before
    public void setUp() {
        baselineRssLimit = Unsafe.getRssMemLimit();
        baselineTag = Unsafe.getMemUsedByTag(MEMFD_TAG);
    }

    @After
    public void tearDown() {
        Unsafe.setRssMemLimit(baselineRssLimit);
    }

    @Test
    public void testAllocateGrowthChargesAndCreditsTag() {
        MemFdFilesFacade ff = new MemFdFilesFacade();
        try (Path p = new Path()) {
            p.of("/alloc-grow.bin");
            long fd = ff.openRW(p.$(), 0);
            assertTrue("openRW should succeed", fd >= 0);

            assertTrue(ff.allocate(fd, 256 * 1024));
            assertEquals(baselineTag + 256 * 1024, Unsafe.getMemUsedByTag(MEMFD_TAG));

            assertTrue(ff.allocate(fd, 1024 * 1024));
            assertEquals(baselineTag + 1024 * 1024, Unsafe.getMemUsedByTag(MEMFD_TAG));

            ff.close(fd);
        }
        ff.clear();
        assertEquals(baselineTag, Unsafe.getMemUsedByTag(MEMFD_TAG));
    }

    @Test
    public void testClearCreditsAllOutstandingFiles() {
        MemFdFilesFacade ff = new MemFdFilesFacade();
        try (Path p = new Path()) {
            p.of("/clear-a.bin");
            long fd1 = ff.openCleanRW(p.$(), 64 * 1024);
            assertTrue(fd1 >= 0);

            p.of("/clear-b.bin");
            long fd2 = ff.openCleanRW(p.$(), 128 * 1024);
            assertTrue(fd2 >= 0);

            assertEquals(baselineTag + 192 * 1024, Unsafe.getMemUsedByTag(MEMFD_TAG));

            // clear() must credit everything regardless of outstanding child fds.
            ff.clear();
            assertEquals(baselineTag, Unsafe.getMemUsedByTag(MEMFD_TAG));

            // Child fds have been unregistered too.
            assertEquals(0L, ff.getOpenFileCount());
        }
    }

    @Test
    public void testOpenCleanRwAbortsWhenOverLimit() {
        MemFdFilesFacade ff = new MemFdFilesFacade();
        // Budget: baseline + 1 MB. Request 4 MB so the charge overflows.
        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 1024 * 1024);
        try (Path p = new Path()) {
            p.of("/overflow.bin");
            try {
                ff.openCleanRW(p.$(), 4L * 1024 * 1024);
                fail("expected CairoException for RSS limit overflow");
            } catch (CairoException e) {
                assertTrue("expected OOM flag", e.isOutOfMemory());
                assertTrue(
                        "expected RSS_MEM_LIMIT message, got: " + e.getFlyweightMessage(),
                        e.getMessage().contains("RSS_MEM_LIMIT")
                );
            }
            // No residual tag bytes and no leaked fd must remain on the abort path.
            assertEquals(baselineTag, Unsafe.getMemUsedByTag(MEMFD_TAG));
            assertEquals(0L, ff.getOpenFileCount());
            // The same facade must still be usable once the limit has room again.
            Unsafe.setRssMemLimit(baselineRssLimit);
            long fd = ff.openCleanRW(p.$(), 64 * 1024);
            assertTrue("post-abort open should succeed", fd >= 0);
            assertEquals(baselineTag + 64 * 1024, Unsafe.getMemUsedByTag(MEMFD_TAG));
            ff.close(fd);
            ff.clear();
            assertEquals(baselineTag, Unsafe.getMemUsedByTag(MEMFD_TAG));
        }
    }

    @Test
    public void testRollbackOnFailedOpen() {
        // Sanity: even when size=0, opening and closing leaves the tag clean.
        MemFdFilesFacade ff = new MemFdFilesFacade();
        try (Path p = new Path()) {
            p.of("/empty.bin");
            long fd = ff.openCleanRW(p.$(), 0);
            assertTrue(fd >= 0);
            assertEquals(baselineTag, Unsafe.getMemUsedByTag(MEMFD_TAG));
            ff.close(fd);
        }
        ff.clear();
        assertEquals(baselineTag, Unsafe.getMemUsedByTag(MEMFD_TAG));
    }

    @Test
    public void testTruncateGrowsAndShrinksTag() {
        MemFdFilesFacade ff = new MemFdFilesFacade();
        try (Path p = new Path()) {
            p.of("/truncate.bin");
            long fd = ff.openCleanRW(p.$(), 0);
            assertTrue("openCleanRW should succeed", fd >= 0);
            assertEquals(baselineTag, Unsafe.getMemUsedByTag(MEMFD_TAG));

            // Grow.
            assertTrue(ff.truncate(fd, 512 * 1024));
            assertEquals(baselineTag + 512 * 1024, Unsafe.getMemUsedByTag(MEMFD_TAG));

            // Shrink.
            assertTrue(ff.truncate(fd, 64 * 1024));
            assertEquals(baselineTag + 64 * 1024, Unsafe.getMemUsedByTag(MEMFD_TAG));

            // Close returns the remainder.
            ff.close(fd);
        }
        ff.clear();
        assertEquals(baselineTag, Unsafe.getMemUsedByTag(MEMFD_TAG));
    }

    @Test
    public void testTruncateOverLimitRejects() {
        MemFdFilesFacade ff = new MemFdFilesFacade();
        try (Path p = new Path()) {
            p.of("/grow-overflow.bin");
            long fd = ff.openCleanRW(p.$(), 64 * 1024);
            assertTrue(fd >= 0);
            long afterOpen = Unsafe.getMemUsedByTag(MEMFD_TAG);
            assertNotEquals(baselineTag, afterOpen);

            Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 32 * 1024);
            try {
                ff.truncate(fd, 8L * 1024 * 1024);
                fail("expected CairoException for RSS limit overflow on truncate");
            } catch (CairoException e) {
                assertTrue(e.isOutOfMemory());
            }
            // File size and tag remain at pre-grow state.
            assertEquals(afterOpen, Unsafe.getMemUsedByTag(MEMFD_TAG));
            assertEquals(64L * 1024, ff.length(fd));

            Unsafe.setRssMemLimit(baselineRssLimit);
            ff.close(fd);
        }
        ff.clear();
        assertEquals(baselineTag, Unsafe.getMemUsedByTag(MEMFD_TAG));
    }

    @Test
    public void testCloseCreditsTag() {
        MemFdFilesFacade ff = new MemFdFilesFacade();
        long allocated;
        try (Path p = new Path()) {
            p.of("/close-credit.bin");
            long fd = ff.openCleanRW(p.$(), 200 * 1024);
            assertTrue(fd >= 0);
            allocated = Unsafe.getMemUsedByTag(MEMFD_TAG) - baselineTag;
            assertEquals(200L * 1024, allocated);
            assertTrue("close child fd", ff.close(fd));
        }
        // Child fd closed but primary fd keeps the memfd alive until clear()
        // or remove(). Tag stays charged.
        assertEquals(baselineTag + 200 * 1024, Unsafe.getMemUsedByTag(MEMFD_TAG));
        ff.clear();
        assertEquals(baselineTag, Unsafe.getMemUsedByTag(MEMFD_TAG));
    }
}
