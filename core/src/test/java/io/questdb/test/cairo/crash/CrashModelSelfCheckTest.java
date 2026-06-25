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
 * contract dictates. They must catch a model that is too LENIENT (tests 2, 4, 5, 6, 7) AND a model that is
 * too STRICT (tests 1, 3, 8). A future commit will swap the product commit path to MS_ASYNC +
 * sync_file_range + a single batched flush; only because this model is proven here can that swap be trusted.
 *
 * <p><b>Tests 6–8</b> additionally pin the METADATA-JOURNALING dimension (data-at-device vs
 * metadata-journaled): data reaching the device cache via {@code sync_file_range} is NOT durable until a
 * JOURNAL COMMIT journals its (within-page) extent conversion. ST6 proves data-at-device alone is lost; ST7
 * proves that under per-inode journaling ({@code modelSharedJournal=false}, i.e. ext4 fast_commit) a
 * new-allocation file needs its OWN journal commit; ST8 proves the default shared journal (ext4 jbd2 / xfs)
 * makes one file's commit cover the others. ST7+ST8 are the executable statement of the dependency the
 * batched optimization's within-page durability rests on.
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

    /**
     * Test 6 — data-at-device WITHOUT a journal commit is lost (the metadata-journaling dimension).
     * fresh A: write -> msync(MS_ASYNC) -> sync_file_range(WRITE|WAIT_AFTER) -> NO journal commit at all ->
     * crash -> A LOST.
     * Proves data-at-device alone is NOT durable: the bytes reached the device cache, but the file is freshly
     * allocated, so its written-data extent conversion was never journaled (sync_file_range does not journal,
     * and nothing else flushed). On crash the unwritten extent reads back as zero.
     */
    @Test
    public void test6_dataAtDeviceWithoutJournalLost() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("model6").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d")) {
            Mapped m = mapAndFill(ff, a, NEW);
            ff.msync(m.addr, SIZE, true);  // MS_ASYNC -> pteFlushed (pages visible to sync_file_range)
            ff.syncFileRange(m.fd, 0, SIZE, Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER); // data-at-device
            // No msync(MS_SYNC), no fdatasync, no fsync anywhere -> the extent conversion is never journaled.
            Assert.assertEquals("precondition: data reached the device", SIZE, ff.syncedDataEndOf(a.toString()));
            Assert.assertEquals("precondition: but nothing is journaled", 0L, ff.journaledDataEndOf(a.toString()));
            unmapAndClose(ff, m);
            ff.crash(dir);
            assertNoBytes("test6: data-at-device without a journal commit must be LOST", ff, a, NEW);
        }
    }

    /**
     * Test 7 — THE KEY ONE: per-inode journaling exposes the new-allocation dependency.
     * {@code modelSharedJournal=false} (ext4 fast_commit world). A,B: write -> msync(MS_ASYNC) both ->
     * sync_file_range(WRITE|WAIT_AFTER) both -> fdatasync(A) ONLY -> crash -> A durable, B LOST.
     * Proves that without the shared journal, B's at-device new-allocation content is NOT made durable by
     * A's fdatasync: a per-inode commit journals only A's extent conversion, so B's written-data extent was
     * never journaled and reverts to zero on crash. New-allocation content needs the file's OWN journal commit.
     */
    @Test
    public void test7_perInodeJournalingNewAllocationLost() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        ff.modelSharedJournal = false; // model ext4 fast_commit: fsync(A) does NOT journal B's metadata
        final String dir = temp.newFolder("model7").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d"); Path b = new Path().of(dir).concat("b.d")) {
            Mapped ma = mapAndFill(ff, a, NEW);
            Mapped mb = mapAndFill(ff, b, NEW);
            ff.msync(ma.addr, SIZE, true);  // MS_ASYNC both
            ff.msync(mb.addr, SIZE, true);
            final int flags = Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER;
            ff.syncFileRange(ma.fd, 0, SIZE, flags); // A -> device cache
            ff.syncFileRange(mb.fd, 0, SIZE, flags); // B -> device cache
            ff.fdatasync(ma.fd);                     // journal-commits A ONLY (per-inode)
            Assert.assertEquals("A's extent metadata is journaled", SIZE, ff.journaledDataEndOf(a.toString()));
            Assert.assertEquals("B reached the device", SIZE, ff.syncedDataEndOf(b.toString()));
            Assert.assertEquals("but B's extent metadata is NOT journaled (per-inode)", 0L, ff.journaledDataEndOf(b.toString()));
            unmapAndClose(ff, ma);
            unmapAndClose(ff, mb);
            ff.crash(dir);
            assertAllBytes("test7: A has its OWN journal commit -> durable", ff, a, NEW);
            assertNoBytes("test7: B's new-allocation data needs B's own journal commit -> LOST without shared journal", ff, b, NEW);
        }
    }

    /**
     * Test 8 — contrast: the SHARED journal makes the new-allocation content durable.
     * Identical to ST7 but {@code modelSharedJournal=true} (default ext4 jbd2 / xfs). A,B: write ->
     * msync(MS_ASYNC) both -> sync_file_range both -> fdatasync(A) ONLY -> crash -> A AND B durable.
     * Documents the dependency's resolution: a single filesystem-wide journal commit (A's fdatasync) ALSO
     * journals B's pending extent conversion, so B's at-device data becomes durable without B's own fsync.
     * This is exactly the mechanism the batched optimization's {@code _cv} flush relies on.
     */
    @Test
    public void test8_sharedJournalNewAllocationDurable() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        ff.modelSharedJournal = true; // default; stated explicitly for contrast with ST7
        final String dir = temp.newFolder("model8").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d"); Path b = new Path().of(dir).concat("b.d")) {
            Mapped ma = mapAndFill(ff, a, NEW);
            Mapped mb = mapAndFill(ff, b, NEW);
            ff.msync(ma.addr, SIZE, true);
            ff.msync(mb.addr, SIZE, true);
            final int flags = Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER;
            ff.syncFileRange(ma.fd, 0, SIZE, flags);
            ff.syncFileRange(mb.fd, 0, SIZE, flags);
            ff.fdatasync(ma.fd); // shared journal: this commit journals BOTH A's and B's extent conversions
            Assert.assertEquals("shared journal also journals B", SIZE, ff.journaledDataEndOf(b.toString()));
            unmapAndClose(ff, ma);
            unmapAndClose(ff, mb);
            ff.crash(dir);
            assertAllBytes("test8: A durable", ff, a, NEW);
            assertAllBytes("test8: B durable via the SHARED journal commit (A's fdatasync covers B)", ff, b, NEW);
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
