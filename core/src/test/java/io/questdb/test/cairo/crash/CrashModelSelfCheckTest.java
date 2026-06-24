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
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The GATE for the SYNC-mode flush-batching safety net.
 * <p>
 * These tests PROVE that {@link CrashFaultFilesFacade}'s content + device-flush-batching model classifies
 * durable-vs-lossy sync schemes correctly, by writing known bytes through a real mmap'd region, running a
 * specific sync sequence, crashing, and asserting the content is exactly as durable (or as lost) as the OS
 * contract dictates. They must catch a model that is too LENIENT (tests 2, 4, 5) AND a model that is too
 * STRICT (tests 1, 3). A future commit will swap the product commit path to MS_ASYNC + sync_file_range + a
 * single batched flush; only because this model is proven here can that swap be trusted later.
 *
 * <p>Marker convention: 0xBB is a "new write" we are testing for survival; 0xAA is a previously-made-durable
 * value; an allocated-but-never-durable file reads back as 0x00 (lost). All sync ops go through {@code ff}
 * (the crash facade) so the model intercepts them; the bytes are written into a MAP_SHARED mmap whose dirty
 * pages share the page cache with the read() that the model uses to snapshot device-cache/durable content.
 */
public class CrashModelSelfCheckTest extends AbstractTest {

    private static final byte NEW = (byte) 0xBB;   // a fresh write under test
    private static final byte PRIOR = (byte) 0xAA; // a previously-durable write
    private static final int SIZE = 256;           // bytes per test file (a sub-page region is fine)

    /**
     * Test 1 — current scheme is durable.
     * write -> msync(MS_SYNC) -> crash -> content PRESENT.
     * Proves the model is not too strict: the per-file sync used by today's code survives.
     */
    @Test
    public void test1_currentSchemeDurable() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("model1").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d")) {
            Mapped m = mapAndFill(ff, a, NEW);
            ff.msync(m.addr, SIZE, false); // MS_SYNC: writeback + device flush
            unmapAndClose(ff, m);
            ff.crash(dir);
            assertAllBytes("test1: msync(MS_SYNC) content must survive crash", ff, a, NEW);
        }
    }

    /**
     * Test 2 — async-only is lost.
     * write A (durable via MS_SYNC) -> overwrite B -> msync(MS_ASYNC) only -> crash -> reverts to A.
     * Proves the model is not too lenient: MS_ASYNC reaches neither device cache nor a flush, so the
     * post-async write must NOT survive.
     */
    @Test
    public void test2_asyncOnlyLost() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("model2").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d")) {
            Mapped m = mapAndFill(ff, a, PRIOR);
            ff.msync(m.addr, SIZE, false);      // PRIOR is now durable
            fill(m.addr, NEW);                  // overwrite with the value under test
            ff.msync(m.addr, SIZE, true);       // MS_ASYNC only -> no device cache, no flush
            unmapAndClose(ff, m);
            ff.crash(dir);
            assertAllBytes("test2: MS_ASYNC-only write must be lost (reverts to last durable)", ff, a, PRIOR);
        }
    }

    /**
     * Test 3 — batched WITH wait is durable (the batching semantic).
     * A,B: write -> msync(MS_ASYNC) both -> sync_file_range(WRITE|WAIT_AFTER) both -> fdatasync(A) only ->
     * crash -> BOTH A and B content PRESENT.
     * Proves a SINGLE device flush promotes BOTH files' device-cache content — i.e. the optimization is
     * validatable: async msync makes pages visible, sync_file_range pushes them to the device cache, and
     * one fdatasync flushes the whole device cache.
     */
    @Test
    public void test3_batchedWithWaitDurable() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("model3").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d"); Path b = new Path().of(dir).concat("b.d")) {
            Mapped ma = mapAndFill(ff, a, NEW);
            Mapped mb = mapAndFill(ff, b, NEW);
            ff.msync(ma.addr, SIZE, true);  // MS_ASYNC -> pteFlushed, pages now visible to sync_file_range
            ff.msync(mb.addr, SIZE, true);
            final int flags = Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER;
            ff.syncFileRange(ma.fd, 0, SIZE, flags); // -> A device cache
            ff.syncFileRange(mb.fd, 0, SIZE, flags); // -> B device cache
            ff.fdatasync(ma.fd);                     // ONE flush -> promotes BOTH device caches to durable
            unmapAndClose(ff, ma);
            unmapAndClose(ff, mb);
            ff.crash(dir);
            assertAllBytes("test3: file A durable after batched flush", ff, a, NEW);
            assertAllBytes("test3: file B durable via the SAME batched flush (batching semantic)", ff, b, NEW);
        }
    }

    /**
     * Test 4 — batched WITHOUT the sync_file_range step is lost for the un-flushed file.
     * A,B: write -> msync(MS_ASYNC) both -> fdatasync(A) only (NO sync_file_range) -> crash ->
     * A PRESENT, B LOST.
     * Proves the model catches the unsafe shortcut: without sync_file_range, B's writeback never reached the
     * device cache, so the batched flush cannot make it durable.
     */
    @Test
    public void test4_batchedWithoutWaitLost() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("model4").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d"); Path b = new Path().of(dir).concat("b.d")) {
            Mapped ma = mapAndFill(ff, a, NEW);
            Mapped mb = mapAndFill(ff, b, NEW);
            ff.msync(ma.addr, SIZE, true);  // MS_ASYNC only
            ff.msync(mb.addr, SIZE, true);  // MS_ASYNC only -> B's bytes never pushed to the device cache
            ff.fdatasync(ma.fd);            // A snapshotted+flushed; B never reached the device cache
            unmapAndClose(ff, ma);
            unmapAndClose(ff, mb);
            ff.crash(dir);
            assertAllBytes("test4: file A reached the device cache (fdatasync) -> durable", ff, a, NEW);
            assertNoBytes("test4: file B never reached the device cache -> LOST", ff, b, NEW);
        }
    }

    /**
     * Test 5 — sync_file_range WITHOUT a preceding msync is a no-op (the footgun).
     * fresh C: write -> sync_file_range(WRITE|WAIT_AFTER) WITHOUT msync -> fdatasync(other file D) ->
     * crash -> C LOST.
     * Proves the model encodes the real Linux footgun: sync_file_range cannot see mmap-dirty pages that were
     * never msync'd into the page cache, so the batched flush leaves C's content un-persisted.
     */
    @Test
    public void test5_syncFileRangeWithoutMsyncNoop() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("model5").getAbsolutePath();
        try (Path c = new Path().of(dir).concat("c.d"); Path d = new Path().of(dir).concat("d.d")) {
            Mapped mc = mapAndFill(ff, c, NEW);     // mmap-dirty, but NEVER msync'd
            Mapped md = mapAndFill(ff, d, NEW);
            Assert.assertFalse("precondition: C must not be pteFlushed (no msync yet)", ff.isPteFlushed(c.toString()));
            ff.syncFileRange(mc.fd, 0, SIZE, Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER); // NO-OP for C
            ff.fdatasync(md.fd);                    // flush fires, but C's device cache was never advanced
            unmapAndClose(ff, mc);
            unmapAndClose(ff, md);
            ff.crash(dir);
            assertNoBytes("test5: sync_file_range without msync is a no-op -> C content LOST", ff, c, NEW);
        }
    }

    // === helpers ===

    private static final class Mapped {
        final long fd;
        final long addr;

        Mapped(long fd, long addr) {
            this.fd = fd;
            this.addr = addr;
        }
    }

    /** Open RW, allocate SIZE bytes, mmap MAP_RW via the facade, and fill the region with {@code value}. */
    private static Mapped mapAndFill(CrashFaultFilesFacade ff, Path path, byte value) {
        long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
        Assert.assertTrue("openRW failed, fd=" + fd, fd > 0);
        Assert.assertTrue("allocate failed", ff.allocate(fd, SIZE));
        long addr = ff.mmap(fd, SIZE, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
        Assert.assertNotEquals("mmap failed", -1L, addr);
        fill(addr, value);
        return new Mapped(fd, addr);
    }

    private static void fill(long addr, byte value) {
        Unsafe.getUnsafe().setMemory(addr, SIZE, value);
    }

    private static void unmapAndClose(CrashFaultFilesFacade ff, Mapped m) {
        ff.munmap(m.addr, SIZE, MemoryTag.MMAP_DEFAULT);
        ff.close(m.fd);
    }

    /** Reopen RO and assert every byte equals {@code expected}. */
    private static void assertAllBytes(String msg, CrashFaultFilesFacade ff, Path path, byte expected) {
        byte[] got = readBack(ff, path);
        for (int i = 0; i < SIZE; i++) {
            Assert.assertEquals(msg + " [byte " + i + "]", expected, got[i]);
        }
    }

    /** Reopen RO and assert NOT every byte equals {@code notExpected} (the write was lost / rolled back). */
    private static void assertNoBytes(String msg, CrashFaultFilesFacade ff, Path path, byte notExpected) {
        byte[] got = readBack(ff, path);
        boolean allEqual = true;
        for (int i = 0; i < SIZE; i++) {
            if (got[i] != notExpected) {
                allEqual = false;
                break;
            }
        }
        Assert.assertFalse(msg + " (file unexpectedly still holds the under-test bytes)", allEqual);
    }

    private static byte[] readBack(CrashFaultFilesFacade ff, Path path) {
        long rd = ff.openRO(path.$());
        Assert.assertTrue("reopen RO failed, fd=" + rd, rd > 0);
        long buf = Unsafe.malloc(SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long n = ff.read(rd, buf, SIZE, 0);
            byte[] out = new byte[SIZE];
            for (int i = 0; i < SIZE; i++) {
                // bytes past EOF (file rolled back shorter than SIZE) read as 0 -> treated as "not the marker".
                out[i] = i < n ? Unsafe.getUnsafe().getByte(buf + i) : 0;
            }
            return out;
        } finally {
            Unsafe.free(buf, SIZE, MemoryTag.NATIVE_DEFAULT);
            ff.close(rd);
        }
    }
}
