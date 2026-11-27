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
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Self-check of {@link CrashFaultFilesFacade}'s content and device-flush model.
 * <p>
 * Each test writes known bytes through a real mmap'd region, runs a specific sync sequence, crashes, and asserts
 * the content is exactly as durable, or as lost, as the OS contract dictates. Together they catch a model that is
 * too lenient (a lost write survives) and one that is too strict (a durable write is lost).
 *
 * <p>Tests 6 to 8 cover data at the device versus journaled metadata: data that reached the device cache via
 * {@code sync_file_range} is not durable until a journal commit journals its within-page extent conversion.
 * Test 6 shows data-at-device alone is lost; test 7 shows that under per-inode journaling
 * ({@code modelSharedJournal=false}, ext4 fast_commit) a new-allocation file needs its own journal commit; test 8
 * shows the default shared journal (ext4 jbd2, xfs) makes one file's commit cover the others. Tests 7 and 8 state
 * the dependency the batched flush's within-page durability rests on.
 *
 * <p>Markers: 0xBB is a new write tested for survival; 0xAA is a previously durable value; an allocated but never
 * durable file reads back as 0x00 (lost). All sync ops go through {@code ff} (the crash facade) so the model
 * intercepts them; the bytes are written into a MAP_SHARED mmap whose dirty pages share the page cache with the
 * read() the model uses to snapshot device-cache and durable content.
 */
public class CrashModelSelfCheckTest extends AbstractTest {

    private static final byte NEW = (byte) 0xBB;   // a fresh write under test
    private static final byte PRIOR = (byte) 0xAA; // a previously-durable write
    private static final int SIZE = 256;           // bytes per test file (a sub-page region is fine)

    /**
     * Test 1: the per-file sync is durable.
     * write -> msync(MS_SYNC) -> crash -> content present.
     * The model is not too strict.
     */
    @Test
    public void test1_currentSchemeDurable() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("model1").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d")) {
            Mapped m = mapAndFill(ff, a, NEW);
            ff.msync(m.addr, SIZE, false); // MS_SYNC: writeback + device flush
            unmapAndClose(ff, m);
            ff.powerLoss();
            ff.crash(dir);
            assertAllBytes("test1: msync(MS_SYNC) content must survive crash", ff, a, NEW);
        }
    }

    /**
     * Test 2: async-only is lost.
     * write A (durable via MS_SYNC) -> overwrite B -> msync(MS_ASYNC) only -> crash -> reverts to A.
     * The model is not too lenient: MS_ASYNC reaches neither the device cache nor a flush, so the post-async write
     * does not survive.
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
            ff.powerLoss();
            ff.crash(dir);
            assertAllBytes("test2: MS_ASYNC-only write must be lost (reverts to last durable)", ff, a, PRIOR);
        }
    }

    /**
     * Test 3: batched with wait is durable.
     * A,B: write -> msync(MS_ASYNC) both -> sync_file_range(WRITE|WAIT_AFTER) both -> fdatasync(A) only ->
     * crash -> A and B content present.
     * One device flush promotes both files' device-cache content: async msync makes the pages visible,
     * sync_file_range pushes them to the device cache, and one fdatasync flushes the whole device cache.
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
            ff.powerLoss();
            ff.crash(dir);
            assertAllBytes("test3: file A durable after batched flush", ff, a, NEW);
            assertAllBytes("test3: file B durable via the SAME batched flush (batching semantic)", ff, b, NEW);
        }
    }

    /**
     * Test 4: batched without the sync_file_range step is lost for the unflushed file.
     * A,B: write -> msync(MS_ASYNC) both -> fdatasync(A) only (no sync_file_range) -> crash -> A present, B lost.
     * Without sync_file_range, B's writeback never reached the device cache, so the batched flush cannot make it
     * durable.
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
            ff.powerLoss();
            ff.crash(dir);
            assertAllBytes("test4: file A reached the device cache (fdatasync) -> durable", ff, a, NEW);
            assertNoBytes("test4: file B never reached the device cache -> LOST", ff, b, NEW);
        }
    }

    /**
     * Test 5: sync_file_range without a preceding msync is a no-op.
     * fresh C: write -> sync_file_range(WRITE|WAIT_AFTER) without msync -> fdatasync(other file D) -> crash -> C lost.
     * sync_file_range cannot see mmap-dirty pages that were never msync'd into the page cache, so the batched flush
     * leaves C's content unpersisted.
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
            ff.powerLoss();
            ff.crash(dir);
            assertNoBytes("test5: sync_file_range without msync is a no-op -> C content LOST", ff, c, NEW);
        }
    }

    /**
     * Test 6: data-at-device without a journal commit is lost.
     * fresh A: write -> msync(MS_ASYNC) -> sync_file_range(WRITE|WAIT_AFTER) -> no journal commit -> crash -> A lost.
     * The bytes reached the device cache, but the file is freshly allocated, so its written-data extent conversion
     * was never journaled (sync_file_range does not journal, and nothing else flushed). After the crash the
     * unwritten extent reads back as zero.
     */
    @Test
    public void test6_dataAtDeviceWithoutJournalLost() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("model6").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d")) {
            Mapped m = mapAndFill(ff, a, NEW);
            ff.msync(m.addr, SIZE, true);  // MS_ASYNC -> pteFlushed (pages visible to sync_file_range)
            ff.syncFileRange(m.fd, 0, SIZE, Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER); // data-at-device
            // No msync(MS_SYNC), no fdatasync, no fsync anywhere, so the extent conversion is never journaled.
            Assert.assertEquals("precondition: data reached the device", SIZE, ff.syncedDataEndOf(a.toString()));
            Assert.assertEquals("precondition: but nothing is journaled", 0L, ff.journaledDataEndOf(a.toString()));
            unmapAndClose(ff, m);
            ff.powerLoss();
            ff.crash(dir);
            assertNoBytes("test6: data-at-device without a journal commit must be LOST", ff, a, NEW);
        }
    }

    /**
     * Test 7: per-inode journaling exposes the new-allocation dependency.
     * {@code modelSharedJournal=false} (ext4 fast_commit). A,B: write -> msync(MS_ASYNC) both ->
     * sync_file_range(WRITE|WAIT_AFTER) both -> fdatasync(A) only -> crash -> A durable, B lost.
     * Without the shared journal, A's fdatasync does not make B's at-device new-allocation content durable: a
     * per-inode commit journals only A's extent conversion, so B's extent reverts to zero on crash. New-allocation
     * content needs the file's own journal commit.
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
            ff.powerLoss();
            ff.crash(dir);
            assertAllBytes("test7: A has its OWN journal commit -> durable", ff, a, NEW);
            assertNoBytes("test7: B's new-allocation data needs B's own journal commit -> LOST without shared journal", ff, b, NEW);
        }
    }

    /**
     * Test 8: the shared journal makes the new-allocation content durable.
     * Identical to test 7 but {@code modelSharedJournal=true} (default ext4 jbd2, xfs). A,B: write ->
     * msync(MS_ASYNC) both -> sync_file_range both -> fdatasync(A) only -> crash -> A and B durable.
     * A single filesystem-wide journal commit (A's fdatasync) also journals B's pending extent conversion, so B's
     * at-device data becomes durable without B's own fsync. The batched flush's {@code _cv} flush relies on this.
     */
    @Test
    public void test8_sharedJournalNewAllocationDurable() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        ff.modelSharedJournal = true; // default; stated explicitly for contrast with test 7
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
            ff.powerLoss();
            ff.crash(dir);
            assertAllBytes("test8: A durable", ff, a, NEW);
            assertAllBytes("test8: B durable via the SHARED journal commit (A's fdatasync covers B)", ff, b, NEW);
        }
    }

    /**
     * Test 9: syncfs journals every inode even under per-inode journaling.
     * The setup of test 7 ({@code modelSharedJournal=false}, where a single inode's {@code fdatasync} does not
     * journal another inode's extent conversions and B is lost), with {@code fdatasync(A)} replaced by one
     * {@code syncfs(A)}. A and B are both durable, because {@code syncfs(2)} writes back the whole filesystem and
     * journals every inode's pending extent conversions in one device flush, whatever the journal policy. The
     * batched SYNC commit relies on this to make all drained columns durable with one flush.
     */
    @Test
    public void test9_syncfsJournalsEveryoneUnderPerInodeJournaling() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        ff.modelSharedJournal = false; // ext4 fast_commit: a per-inode fdatasync(A) would not journal B (test 7)
        // The filesystem-wide syncfs of Linux, which tests pin off Linux too; CrashFaultFilesFacadeTest checks the
        // single-file one of macOS and Windows.
        ff.setSyncfsFileSystemWide(true);
        final String dir = temp.newFolder("model9").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d"); Path b = new Path().of(dir).concat("b.d")) {
            Mapped ma = mapAndFill(ff, a, NEW);
            Mapped mb = mapAndFill(ff, b, NEW);
            ff.msync(ma.addr, SIZE, true);  // MS_ASYNC both -> pteFlushed, pages visible to sync_file_range
            ff.msync(mb.addr, SIZE, true);
            final int flags = Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER;
            ff.syncFileRange(ma.fd, 0, SIZE, flags); // A -> device cache (NOT journaled)
            ff.syncFileRange(mb.fd, 0, SIZE, flags); // B -> device cache (NOT journaled)
            ff.syncfs(ma.fd); // WHOLE-FS journal commit + flush: journals BOTH A and B despite per-inode policy
            // If syncfs journaled only its own fd (like fdatasync under per-inode journaling), B's journaled end would
            // still be 0 here and the post-crash B-durable assertion below would fail.
            Assert.assertEquals("syncfs must journal B too (whole-fs commit)", SIZE, ff.journaledDataEndOf(b.toString()));
            Assert.assertEquals("syncfs journals A", SIZE, ff.journaledDataEndOf(a.toString()));
            unmapAndClose(ff, ma);
            unmapAndClose(ff, mb);
            ff.powerLoss();
            ff.crash(dir);
            assertAllBytes("test9: A durable via syncfs", ff, a, NEW);
            assertAllBytes("test9: B durable via syncfs's WHOLE-FILESYSTEM journal commit (even under per-inode journaling)", ff, b, NEW);
        }
    }

    /**
     * Test 10: syncfs also persists dirty pages after their mapping and fd are closed.
     * A is mmap-written and unmapped without msync/sync_file_range, so the facade's written-data watermark does not
     * know about the store. syncfs(B) still persists A's complete current image: the kernel keeps dirty MAP_SHARED
     * pages in the page cache after munmap, and syncfs writes them back filesystem-wide. This is the epoch case
     * where every column is already closed.
     */
    @Test
    public void test10_syncfsPersistsUnmappedDirtyPages() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        ff.modelSharedJournal = false;
        // The filesystem-wide syncfs of Linux, as in test 9.
        ff.setSyncfsFileSystemWide(true);
        final String dir = temp.newFolder("model10").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d"); Path b = new Path().of(dir).concat("b.d")) {
            Mapped ma = mapAndFill(ff, a, NEW);
            unmapAndClose(ff, ma); // deliberately no msync/sync_file_range

            Mapped mb = mapAndFill(ff, b, PRIOR);
            ff.syncfs(mb.fd); // any fd on the filesystem flushes A as well
            unmapAndClose(ff, mb);

            ff.powerLoss();

            ff.crash(dir);
            assertAllBytes("test10: syncfs must persist mmap-dirty A after munmap/close", ff, a, NEW);
        }
    }

    /**
     * Test 11: a ranged sync persists its range only.
     * Page 0 of A is written through its own mapping and released with msync(MS_ASYNC) + munmap, as a paged
     * WAL column releases a page. Page 1 is written through a second mapping and msync(MS_SYNC)'d, which on
     * Linux is a RANGE fsync: it writes back the dirty pages of that range only. After a crash page 1
     * survives and page 0 must not. A model that snapshots the whole file on a ranged sync is too lenient:
     * it would keep page 0 and hide a missing flush of every page released before the commit.
     */
    @Test
    public void test11_rangedMsyncPersistsItsRangeOnly() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        ff.modelSharedJournal = false;
        final String dir = temp.newFolder("model11").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d")) {
            final long fd = writeTwoPagesReleasingFirst(ff, a);
            final long page1 = ff.mmap(fd, Files.PAGE_SIZE, Files.PAGE_SIZE, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
            ff.msync(page1, Files.PAGE_SIZE, false); // MS_SYNC of page 1 only
            ff.munmap(page1, Files.PAGE_SIZE, MemoryTag.MMAP_DEFAULT);
            ff.close(fd);
            ff.powerLoss();
            ff.crash(dir);
            final byte[] got = readBack(ff, a, 2 * (int) Files.PAGE_SIZE);
            assertPage("test11: the MS_SYNC'd page 1 must survive", got, 1, true);
            assertPage("test11: page 0, released with MS_ASYNC and never synced, must be lost", got, 0, false);
        }
    }

    /**
     * Test 12: sync_file_range persists its range only (with a journal commit after it).
     * Same two pages; page 1 gets msync(MS_ASYNC) + sync_file_range over its own range, and a shared-journal
     * fdatasync of another file journals the extent. Page 1 survives; page 0, never written back, must not.
     */
    @Test
    public void test12_rangedSyncFileRangePersistsItsRangeOnly() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("model12").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d"); Path b = new Path().of(dir).concat("b.d")) {
            final long fd = writeTwoPagesReleasingFirst(ff, a);
            final long page1 = ff.mmap(fd, Files.PAGE_SIZE, Files.PAGE_SIZE, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
            ff.msync(page1, Files.PAGE_SIZE, true);
            ff.syncFileRange(fd, Files.PAGE_SIZE, Files.PAGE_SIZE, Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER);
            ff.munmap(page1, Files.PAGE_SIZE, MemoryTag.MMAP_DEFAULT);
            final Mapped mb = mapAndFill(ff, b, PRIOR);
            ff.fdatasync(mb.fd); // shared journal: journals A's drained extent too
            unmapAndClose(ff, mb);
            ff.close(fd);
            ff.powerLoss();
            ff.crash(dir);
            final byte[] got = readBack(ff, a, 2 * (int) Files.PAGE_SIZE);
            assertPage("test12: the drained page 1 must survive", got, 1, true);
            assertPage("test12: page 0, released with MS_ASYNC and never written back, must be lost", got, 0, false);
        }
    }

    /**
     * Test 13: fdatasync persists pages released before it.
     * Same two pages, page 1 left with msync(MS_ASYNC), then fdatasync(A): it writes back every dirty page of
     * the file, mapped or already unmapped, so both pages survive. This is the WAL commit barrier covering
     * the pages released during the transaction; a model failing it is too strict.
     */
    @Test
    public void test13_fdatasyncPersistsReleasedPages() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        ff.modelSharedJournal = false;
        final String dir = temp.newFolder("model13").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d")) {
            final long fd = writeTwoPagesReleasingFirst(ff, a);
            final long page1 = ff.mmap(fd, Files.PAGE_SIZE, Files.PAGE_SIZE, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
            ff.msync(page1, Files.PAGE_SIZE, true);
            ff.fdatasync(fd);
            ff.munmap(page1, Files.PAGE_SIZE, MemoryTag.MMAP_DEFAULT);
            ff.close(fd);
            ff.powerLoss();
            ff.crash(dir);
            final byte[] got = readBack(ff, a, 2 * (int) Files.PAGE_SIZE);
            assertPage("test13: page 0 must survive the file's fdatasync", got, 0, true);
            assertPage("test13: page 1 must survive the file's fdatasync", got, 1, true);
        }
    }

    /**
     * Test 14: fsync persists a store made through a mapping that was never msync'd.
     * A is written through a mapping and unmapped with no msync (as a null backfill writes a column through
     * its own mapping), then fsync(A). fsync writes back every dirty page of the file, so A survives. A model
     * that only counts msync'd or write()-n bytes as written is too strict: it drops A and blames a later,
     * unrelated flush for making it durable.
     */
    @Test
    public void test14_fsyncPersistsUnsyncedMappedStore() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        ff.modelSharedJournal = false;
        final String dir = temp.newFolder("model14").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d")) {
            final Mapped ma = mapAndFill(ff, a, NEW);
            ff.munmap(ma.addr, SIZE, MemoryTag.MMAP_DEFAULT); // deliberately no msync
            ff.fsync(ma.fd);
            ff.close(ma.fd);
            ff.powerLoss();
            ff.crash(dir);
            assertAllBytes("test14: fsync must persist a store made through an unsynced mapping", ff, a, NEW);
        }
    }

    /**
     * Test 15: a shrink frees the bytes past the new end for good.
     * A holds two durable pages of PRIOR. It shrinks (truncate to zero, truncate to one page, or a copy of an
     * empty file over it, which truncates with O_TRUNC) and grows back to two pages. A freed page 0 is written
     * again through a mapping and released with msync(MS_ASYNC); page 1 is written through a mapping and then
     * written back by a ranged msync(MS_SYNC), or by sync_file_range plus another file's fdatasync, or not
     * written back before another file's fdatasync. Each of them journals the shrink, so after a crash a freed
     * page reads zero or the new bytes, never PRIOR; a kept page still reads PRIOR, and a written-back page 1
     * reads NEW. A model that keeps the pre-shrink device image lets a ranged sync or a flush bring the freed
     * bytes back, and hides a missing flush whenever the new bytes equal the freed ones.
     */
    @Test
    public void test15_shrinkFreesBytesPastNewEnd() throws Exception {
        final String[] shrinks = {"truncate to zero", "truncate to one page", "copy an empty file over it"};
        final String[] syncs = {"msync(MS_SYNC) of page 1", "sync_file_range of page 1, then fdatasync(B)", "fdatasync(B) only"};
        final StringBuilder failures = new StringBuilder();
        // Windows copies onto no existing file (CopyFileW fails if it exists), so a copy cannot shrink one there.
        final int shrinkCount = Os.isWindows() ? shrinks.length - 1 : shrinks.length;
        for (int shrink = 0; shrink < shrinkCount; shrink++) {
            for (int sync = 0; sync < syncs.length; sync++) {
                final String what = "test15 [" + shrinks[shrink] + ", " + syncs[sync] + "]: ";
                final byte[] got = shrinkRewriteAndCrash(temp.newFolder("model15-" + shrink + "-" + sync).getAbsolutePath(), shrink, sync);
                if (shrink == 1 && !pageHolds(got, 0, PRIOR)) {
                    failures.append(what).append("page 0, kept by the shrink, lost its durable bytes\n");
                }
                if (shrink != 1 && pageHolds(got, 0, PRIOR)) {
                    failures.append(what).append("page 0 came back with the bytes the shrink freed\n");
                }
                if (pageHolds(got, 1, PRIOR)) {
                    failures.append(what).append("page 1 came back with the bytes the shrink freed\n");
                }
                if (sync < 2 && !pageHolds(got, 1, NEW)) {
                    failures.append(what).append("page 1, written back after the shrink, was lost\n");
                }
            }
        }
        Assert.assertEquals("", failures.toString());
    }

    /**
     * Test 16: nothing flushed after the power loss is durable.
     * A and B durable (PRIOR) -> powerLoss() -> one flushing operation -> crash -> A and B hold PRIOR, no new name.
     * A test must release the engine's handles before crash(), and those closes flush (a writer's close-time save
     * point runs syncfs). Every kind of flush the facade models is run after the mark: if any of them counts, the
     * rollback keeps what a real power loss would have lost.
     */
    @Test
    public void test16_nothingAfterThePowerLossIsDurable() throws Exception {
        final StringBuilder failures = new StringBuilder();
        int i = 0;
        for (Map.Entry<String, BiConsumer<CrashFaultFilesFacade, String>> flush : flushesAfterThePowerLoss().entrySet()) {
            final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
            final String dir = temp.newFolder("model16-" + i++).getAbsolutePath();
            try (Path a = new Path().of(dir).concat("a.d"); Path b = new Path().of(dir).concat("b.d")) {
                unmapAndClose(ff, mapAndFill(ff, a, PRIOR));
                unmapAndClose(ff, mapAndFill(ff, b, PRIOR));
                ff.markDurableBaseline(dir);
                ff.powerLoss();
                flush.getValue().accept(ff, dir);
                ff.crash(dir);
                if (!holdsOnly(dir, "a.d", PRIOR) || !holdsOnly(dir, "b.d", PRIOR)
                        || exists(dir, "c.d") || exists(dir, "d.d")) {
                    failures.append(flush.getKey()).append(" after the power loss became durable\n");
                }
            }
        }
        Assert.assertEquals("", failures.toString());
    }

    /**
     * Test 17: an armed crash is the power loss.
     * A durable (PRIOR) -> NEW, msync(MS_SYNC) is the armed op -> CrashSimulationError -> PRIOR2, msync(MS_SYNC) ->
     * crash -> A holds NEW. The armed op completes and the machine stops right after it, so the engine's work after
     * the error (a swallowed error's catch path, the harness releasing handles) does not count.
     */
    @Test
    public void test17_armedCrashIsThePowerLoss() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("model17").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d")) {
            unmapAndClose(ff, mapAndFill(ff, a, PRIOR));
            ff.markDurableBaseline(dir);
            final Mapped m = mapAndFill(ff, a, NEW);
            ff.armCrashAt(ff.durabilityOpCount() + 1);
            try {
                ff.msync(m.addr, SIZE, false);
                Assert.fail("expected CrashSimulationError");
            } catch (CrashSimulationError expected) {
                Assert.assertTrue(ff.isPowerLossMarked());
            }
            fill(m.addr, (byte) 0xCC);
            ff.msync(m.addr, SIZE, false);
            unmapAndClose(ff, m);
            ff.crash(dir);
            assertAllBytes("test17: the armed op is durable, the flush after it is not", ff, a, NEW);
        }
    }

    /**
     * Test 18: the rollback leaves nothing in the device cache.
     * A durable (PRIOR) -> NEW reaches the device cache (MS_ASYNC, sync_file_range), no device flush -> power loss,
     * crash -> B, fsync (a device flush) -> power loss, crash -> A holds PRIOR. NEW died in the first power loss; a
     * model that keeps A's device cache across it lets B's flush bring NEW back.
     */
    @Test
    public void test18_rollbackLeavesNoCachedBytes() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("model18").getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d"); Path b = new Path().of(dir).concat("b.d")) {
            unmapAndClose(ff, mapAndFill(ff, a, PRIOR));
            unmapAndClose(ff, mapAndFill(ff, b, PRIOR));
            ff.markDurableBaseline(dir);
            final Mapped ma = mapAndFill(ff, a, NEW);
            ff.msync(ma.addr, SIZE, true);
            ff.syncFileRange(ma.fd, 0, SIZE, Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER);
            unmapAndClose(ff, ma);
            ff.powerLoss();
            ff.crash(dir);
            assertAllBytes("test18: A's device-cache bytes died in the power loss", ff, a, PRIOR);
            rewriteAndSync(ff, dir, "b.d", (f, m) -> f.fsync(m.fd));
            ff.powerLoss();
            ff.crash(dir);
            assertAllBytes("test18: a later flush brought back bytes the first power loss destroyed", ff, a, PRIOR);
        }
    }

    /**
     * Test 19: crash() needs a power loss. Only the test knows where it is, so a rollback without a mark (or a kill
     * image) is refused instead of taking the instant of the call, after the handles were released.
     */
    @Test
    public void test19_crashNeedsAPowerLoss() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("model19").getAbsolutePath();
        ff.markDurableBaseline(dir);
        try {
            ff.crash(dir);
            Assert.fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("powerLoss()"));
        }
    }

    /**
     * Test 20: a power loss on one root keeps another root's later flushes.
     * Roots R1, R2 durable (PRIOR) -> power loss -> R2's A: NEW, msync(MS_SYNC) -> crash R1 -> power loss, crash R2 ->
     * R2's A holds NEW. Two engines on two roots model two machines; rolling back R1 must not undo R2's durability.
     */
    @Test
    public void test20_powerLossOnOneRootKeepsAnotherRootsFlushes() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String r1 = temp.newFolder("model20-r1").getAbsolutePath();
        final String r2 = temp.newFolder("model20-r2").getAbsolutePath();
        try (Path a1 = new Path().of(r1).concat("a.d"); Path a2 = new Path().of(r2).concat("a.d")) {
            unmapAndClose(ff, mapAndFill(ff, a1, PRIOR));
            unmapAndClose(ff, mapAndFill(ff, a2, PRIOR));
            ff.markDurableBaseline(r1);
            ff.markDurableBaseline(r2);
            ff.powerLoss();
            rewriteAndSync(ff, r2, "a.d", (f, m) -> f.msync(m.addr, SIZE, false));
            ff.crash(r1);
            ff.powerLoss();
            ff.crash(r2);
            assertAllBytes("test20: R1's rollback undid R2's flush", ff, a2, NEW);
        }
    }

    /**
     * Test 21: a path the rollback deleted starts empty when it is created again.
     * B durable (PRIOR) -> A in a new directory P gets NEW, then disappears one way -> power loss, crash: P is gone ->
     * P, A and C created again, never written, their entries made durable -> B, fsync (a device flush) -> power loss,
     * crash -> A and C hold no NEW. The engine creates the same partition, WAL directory, segment or column file again
     * after a restart; a model that keeps a deleted path's durable bytes, device cache or watermarks writes NEW back at
     * the next power loss, or lets an unrelated flush promote it.
     */
    @Test
    public void test21_pathDeletedByTheRollbackStartsEmpty() throws Exception {
        // The names created again become durable only through a directory fsync, and Windows opens no directory.
        Assume.assumeFalse("Windows has no directory handle to fsync", Os.isWindows());
        final Map<String, BiConsumer<CrashFaultFilesFacade, String>> ways = new LinkedHashMap<>();
        ways.put("durable bytes, entry never durable", (ff, p) -> {
            rewriteAndSync(ff, p, "a.d", (f, m) -> f.fsync(m.fd));
            ff.powerLoss();
        });
        ways.put("durable bytes, removed after the power loss", (ff, p) -> {
            rewriteAndSync(ff, p, "a.d", (f, m) -> f.fsync(m.fd));
            ff.powerLoss();
            try (Path a = new Path().of(p).concat("a.d")) {
                Assert.assertTrue(ff.removeQuiet(a.$()));
            }
        });
        ways.put("durable bytes, renamed after the power loss", (ff, p) -> {
            rewriteAndSync(ff, p, "a.d", (f, m) -> f.fsync(m.fd));
            ff.powerLoss();
            try (Path a = new Path().of(p).concat("a.d"); Path c = new Path().of(p).concat("c.d")) {
                Assert.assertEquals(Files.FILES_RENAME_OK, ff.rename(a.$(), c.$()));
            }
        });
        ways.put("bytes in the device cache only", (ff, p) -> {
            rewriteAndSync(ff, p, "a.d", (f, m) -> {
                f.msync(m.addr, SIZE, true);
                f.syncFileRange(m.fd, 0, SIZE, Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER);
            });
            ff.powerLoss();
        });
        ways.put("durable bytes, kill image taken before P existed", (ff, p) -> {
            // A kill rollback deletes P the same way, and the next power loss in the same facade lifetime must not
            // bring A's bytes back either.
            final String dir = Paths.get(p).getParent().toString();
            try {
                java.nio.file.Files.delete(Paths.get(p));
                ff.takeKillImage(dir);
                java.nio.file.Files.createDirectories(Paths.get(p));
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
            rewriteAndSync(ff, p, "a.d", (f, m) -> f.fsync(m.fd));
            // no power loss: the next crash() restores the kill image instead
        });
        final String[] names = {"a.d", "c.d"};
        final StringBuilder failures = new StringBuilder();
        int i = 0;
        for (Map.Entry<String, BiConsumer<CrashFaultFilesFacade, String>> way : ways.entrySet()) {
            final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
            final String dir = temp.newFolder("model21-" + i++).getAbsolutePath();
            final String p = Paths.get(dir, "p").toString();
            try (Path b = new Path().of(dir).concat("b.d")) {
                unmapAndClose(ff, mapAndFill(ff, b, PRIOR));
                ff.markDurableBaseline(dir);
                java.nio.file.Files.createDirectories(Paths.get(p));
                way.getValue().accept(ff, p);
                ff.crash(dir);
                Assert.assertFalse("precondition: the rollback deletes P", exists(dir, "p"));
                if (ff.trackedFileCountUnder(p) != 0) {
                    failures.append(way.getKey()).append(": the model still tracks files under the deleted P\n");
                }
                for (String name : names) {
                    final String file = Paths.get(p, name).toString();
                    if (ff.durableContentOf(file) != null || ff.syncedDataEndOf(file) != 0 || ff.journaledDataEndOf(file) != 0) {
                        failures.append(way.getKey()).append(": the model keeps state for the deleted ").append(name).append('\n');
                    }
                }
                java.nio.file.Files.createDirectories(Paths.get(p));
                for (String name : names) {
                    try (Path path = new Path().of(p).concat(name)) {
                        final long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
                        Assert.assertTrue("openRW failed, fd=" + fd, fd > 0);
                        Assert.assertTrue("allocate failed", ff.allocate(fd, SIZE));
                        ff.close(fd);
                    }
                }
                syncDirectory(ff, p);
                syncDirectory(ff, dir);
                rewriteAndSync(ff, dir, "b.d", (f, m) -> f.fsync(m.fd));
                ff.powerLoss();
                ff.crash(dir);
                for (String name : names) {
                    if (holdsAny(p, name, NEW)) {
                        failures.append(way.getKey()).append(": ").append(name)
                                .append(" created again holds bytes the first power loss destroyed\n");
                    }
                }
            }
        }
        Assert.assertEquals("", failures.toString());
    }

    /**
     * Test 22: between the power loss and the rollback, the model reports the frozen state.
     * A and B durable (PRIOR) -> powerLoss() -> one flushing operation -> durableContentOf each name -> crash -> each
     * name holds what was reported. A check that reads the model after the mark (after the releases) must not count a
     * flush the rollback throws away.
     */
    @Test
    public void test22_durableContentAfterThePowerLossIsWhatTheRollbackRestores() throws Exception {
        final String[] names = {"a.d", "b.d", "c.d", "d.d"};
        final StringBuilder failures = new StringBuilder();
        int i = 0;
        for (Map.Entry<String, BiConsumer<CrashFaultFilesFacade, String>> flush : flushesAfterThePowerLoss().entrySet()) {
            final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
            final String dir = temp.newFolder("model22-" + i++).getAbsolutePath();
            try (Path a = new Path().of(dir).concat("a.d"); Path b = new Path().of(dir).concat("b.d")) {
                unmapAndClose(ff, mapAndFill(ff, a, PRIOR));
                unmapAndClose(ff, mapAndFill(ff, b, PRIOR));
                ff.markDurableBaseline(dir);
                ff.powerLoss();
                flush.getValue().accept(ff, dir);
                final byte[][] reported = new byte[names.length][];
                for (int n = 0; n < names.length; n++) {
                    reported[n] = ff.durableContentOf(Paths.get(dir, names[n]).toString());
                }
                ff.crash(dir);
                for (int n = 0; n < names.length; n++) {
                    final byte[] restored = exists(dir, names[n]) ? java.nio.file.Files.readAllBytes(Paths.get(dir, names[n])) : null;
                    if (!Arrays.equals(reported[n], restored)) {
                        failures.append(flush.getKey()).append(": ").append(names[n])
                                .append(" reported other bytes than the rollback restored\n");
                    }
                }
            }
        }
        Assert.assertEquals("", failures.toString());
    }

    /**
     * Test 23: a handle from before the power loss cannot mutate after the rollback.
     * P/A durable (PRIOR), A open and mapped -> NEW in the mapping -> power loss, crash -> every mutation through
     * the pre-crash fd or mapping (write, truncate, allocate, msync, fsync, fdatasync, sync_file_range) is refused;
     * munmap and close are not; a fresh handle on the same path works -> A holds PRIOR. The rollback is the reboot,
     * and crash() ends in the baseline: a writer released after it would flush into files recovery reads, and the
     * model would count that as durable. A file directly under the root (an engine singleton's) is exempt.
     */
    @Test
    public void test23_aHandleFromBeforeThePowerLossCannotMutateAfterTheRollback() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        // The test carries a writable mapping across the rollback on purpose: the stale-handle fence is under test.
        ff.allowWritableMappingAcrossCrash();
        final String dir = temp.newFolder("model23").getAbsolutePath();
        final String p = Paths.get(dir, "p").toString();
        java.nio.file.Files.createDirectories(Paths.get(p));
        final long buf = Unsafe.malloc(SIZE, MemoryTag.NATIVE_DEFAULT);
        try (Path a = new Path().of(p).concat("a.d"); Path b = new Path().of(dir).concat("b.d")) {
            unmapAndClose(ff, mapAndFill(ff, a, PRIOR));
            unmapAndClose(ff, mapAndFill(ff, b, PRIOR));
            ff.markDurableBaseline(dir);
            final Mapped ma = mapAndFill(ff, a, NEW);
            final Mapped mb = mapAndFill(ff, b, NEW);
            ff.powerLoss();
            ff.crash(dir);

            final StringBuilder failures = new StringBuilder();
            expectRefused(failures, "write", () -> ff.write(ma.fd, buf, SIZE, 0));
            expectRefused(failures, "append", () -> ff.append(ma.fd, buf, SIZE));
            expectRefused(failures, "truncate", () -> ff.truncate(ma.fd, 0));
            expectRefused(failures, "allocate", () -> ff.allocate(ma.fd, 2 * SIZE));
            expectRefused(failures, "allocateRange", () -> ff.allocate(ma.fd, 0, 2 * SIZE));
            expectRefused(failures, "msync", () -> ff.msync(ma.addr, SIZE, false));
            expectRefused(failures, "msync-async", () -> ff.msync(ma.addr, SIZE, true));
            expectRefused(failures, "syncFileRange", () -> ff.syncFileRange(ma.fd, 0, SIZE, Files.SYNC_FILE_RANGE_WRITE));
            expectRefused(failures, "fdatasync", () -> ff.fdatasync(ma.fd));
            expectRefused(failures, "fsync", () -> ff.fsync(ma.fd));
            expectRefused(failures, "syncfs", () -> ff.syncfs(ma.fd));
            final boolean[] isClosed = {false};
            expectRefused(failures, "fsyncAndClose", () -> {
                ff.fsyncAndClose(ma.fd);
                isClosed[0] = true;
            });
            // an engine-root file's handle outlives the simulated restart
            ff.msync(mb.addr, SIZE, false);
            ff.fsync(mb.fd);
            unmapAndClose(ff, mb);
            // Releasing a stale handle is allowed. Keep the negative control's missing-fence assertion visible
            // even if fsyncAndClose above wrongly succeeded and already closed it.
            ff.munmap(ma.addr, SIZE, MemoryTag.MMAP_DEFAULT);
            if (!isClosed[0]) {
                ff.close(ma.fd);
            }
            Assert.assertEquals("", failures.toString());
            assertAllBytes("test23: a mutation through a pre-crash handle changed the rolled-back file", ff, a, PRIOR);

            // a fresh handle on the same path is not stale
            final Mapped fresh = mapAndFill(ff, a, NEW);
            ff.msync(fresh.addr, SIZE, false);
            unmapAndClose(ff, fresh);
            ff.powerLoss();
            ff.crash(dir);
            assertAllBytes("test23: a fresh handle after the rollback must work", ff, a, NEW);
        } finally {
            Unsafe.free(buf, SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Test 24: crash() refuses to roll back under a live writable mapping.
     * A durable (PRIOR) -> A mapped MAP_RW, NEW -> power loss -> crash refused, naming A -> A unmapped, a read-only
     * mapping of A and a writable one of a file directly under the root still live -> crash rolls back -> A holds
     * PRIOR. crash() rewrites A in place, so a store through the writable mapping would reach the rolled-back file
     * without any intercepted call. The refusal comes first, whether or not the owner would write after it.
     */
    @Test
    public void test24_crashRefusesALiveWritableMapping() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("model24").getAbsolutePath();
        final String p = Paths.get(dir, "p").toString();
        java.nio.file.Files.createDirectories(Paths.get(p));
        try (Path a = new Path().of(p).concat("a.d"); Path b = new Path().of(dir).concat("b.d")) {
            unmapAndClose(ff, mapAndFill(ff, a, PRIOR));
            ff.markDurableBaseline(dir);
            final Mapped ma = mapAndFill(ff, a, NEW);
            final Mapped mb = mapAndFill(ff, b, NEW);
            ff.powerLoss();
            try {
                ff.crash(dir);
                Assert.fail("expected IllegalStateException");
            } catch (IllegalStateException expected) {
                Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("a.d"));
                Assert.assertFalse(expected.getMessage(), expected.getMessage().contains("b.d"));
            }
            unmapAndClose(ff, ma);
            final long fd = ff.openRO(a.$());
            final long addr = ff.mmap(fd, SIZE, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
            try {
                ff.crash(dir);
            } finally {
                ff.munmap(addr, SIZE, MemoryTag.MMAP_DEFAULT);
                ff.close(fd);
                unmapAndClose(ff, mb);
            }
            assertAllBytes("test24: the rollback must restore the durable bytes", ff, a, PRIOR);
        }
    }

    private static final class Mapped {
        final long fd;
        final long addr;

        Mapped(long fd, long addr) {
            this.fd = fd;
            this.addr = addr;
        }
    }

    /**
     * Opens RW, allocates SIZE bytes, mmaps MAP_RW via the facade, and fills the region with {@code value}.
     */
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

    /**
     * Reopens RO and asserts every byte equals {@code expected}.
     */
    private static void assertAllBytes(String msg, CrashFaultFilesFacade ff, Path path, byte expected) {
        byte[] got = readBack(ff, path);
        for (int i = 0; i < SIZE; i++) {
            Assert.assertEquals(msg + " [byte " + i + "]", expected, got[i]);
        }
    }

    /**
     * Reopens RO and asserts not every byte equals {@code notExpected} (the write was lost or rolled back).
     */
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

    /**
     * Asserts that OS page {@code page} of {@code got} holds only {@code NEW} ({@code present}), or not only it.
     */
    private static void assertPage(String msg, byte[] got, int page, boolean present) {
        boolean allNew = true;
        for (int i = page * (int) Files.PAGE_SIZE, n = i + (int) Files.PAGE_SIZE; i < n; i++) {
            if (got[i] != NEW) {
                allNew = false;
                break;
            }
        }
        Assert.assertEquals(msg, present, allNew);
    }

    /**
     * Whether OS page {@code page} of {@code got} holds only {@code value}.
     */
    private static boolean pageHolds(byte[] got, int page, byte value) {
        for (int i = page * (int) Files.PAGE_SIZE, n = i + (int) Files.PAGE_SIZE; i < n; i++) {
            if (got[i] != value) {
                return false;
            }
        }
        return true;
    }

    private static byte[] readBack(CrashFaultFilesFacade ff, Path path) {
        return readBack(ff, path, SIZE);
    }

    private static byte[] readBack(CrashFaultFilesFacade ff, Path path, int size) {
        long rd = ff.openRO(path.$());
        Assert.assertTrue("reopen RO failed, fd=" + rd, rd > 0);
        long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long n = ff.read(rd, buf, size, 0);
            byte[] out = new byte[size];
            for (int i = 0; i < size; i++) {
                // Bytes past EOF (file rolled back shorter than size) read as 0, which is not the marker.
                out[i] = i < n ? Unsafe.getUnsafe().getByte(buf + i) : 0;
            }
            return out;
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
            ff.close(rd);
        }
    }

    /**
     * Test 15's sequence: two durable pages of PRIOR in A, a shrink, growth back to two pages, the freed
     * pages written again with NEW, the given write-back of page 1, then a crash. Returns A's first two pages.
     */
    private static byte[] shrinkRewriteAndCrash(String dir, int shrink, int sync) {
        final int page = (int) Files.PAGE_SIZE;
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        try (
                Path a = new Path().of(dir).concat("a.d");
                Path b = new Path().of(dir).concat("b.d");
                Path empty = new Path().of(dir).concat("empty.d")
        ) {
            final long fd = ff.openRW(a.$(), CairoConfiguration.O_NONE);
            Assert.assertTrue("openRW failed, fd=" + fd, fd > 0);
            Assert.assertTrue("allocate failed", ff.allocate(fd, 2L * page));
            final long buf = Unsafe.malloc(2L * page, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(buf, 2L * page, PRIOR);
                Assert.assertEquals(2L * page, ff.write(fd, buf, 2L * page, 0));
            } finally {
                Unsafe.free(buf, 2L * page, MemoryTag.NATIVE_DEFAULT);
            }
            ff.fdatasync(fd);

            switch (shrink) {
                case 0 -> Assert.assertTrue("truncate failed", ff.truncate(fd, 0));
                case 1 -> Assert.assertTrue("truncate failed", ff.truncate(fd, page));
                default -> {
                    Assert.assertTrue("touch failed", ff.touch(empty.$()));
                    Assert.assertEquals("copy failed", 0, ff.copy(empty.$(), a.$()));
                }
            }
            Assert.assertTrue("allocate failed", ff.allocate(fd, 2L * page));
            if (shrink != 1) {
                final long page0 = ff.mmap(fd, page, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
                Assert.assertNotEquals("mmap failed", -1L, page0);
                Unsafe.getUnsafe().setMemory(page0, page, NEW);
                ff.msync(page0, page, true);
                ff.munmap(page0, page, MemoryTag.MMAP_DEFAULT);
            }
            final long page1 = ff.mmap(fd, page, page, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
            Assert.assertNotEquals("mmap failed", -1L, page1);
            Unsafe.getUnsafe().setMemory(page1, page, NEW);
            if (sync == 0) {
                ff.msync(page1, page, false);
            } else {
                ff.msync(page1, page, true);
                if (sync == 1) {
                    ff.syncFileRange(fd, page, page, Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER);
                }
            }
            ff.munmap(page1, page, MemoryTag.MMAP_DEFAULT);
            if (sync > 0) {
                final Mapped mb = mapAndFill(ff, b, NEW);
                ff.fdatasync(mb.fd); // shared journal: journals A's shrink too
                unmapAndClose(ff, mb);
            }
            ff.close(fd);
            ff.powerLoss();
            ff.crash(dir);
            return readBack(ff, a, 2 * page);
        }
    }

    /**
     * Opens A, reserves two OS pages, writes page 0 through its own mapping and releases it as a paged WAL column
     * does (msync(MS_ASYNC), then munmap), then write page 1 through a second mapping and unmap it unsynced.
     * Returns the open fd; the caller maps page 1 again to sync it.
     */
    private static long writeTwoPagesReleasingFirst(CrashFaultFilesFacade ff, Path path) {
        final long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
        Assert.assertTrue("openRW failed, fd=" + fd, fd > 0);
        Assert.assertTrue("allocate failed", ff.allocate(fd, 2 * Files.PAGE_SIZE));
        final long page0 = ff.mmap(fd, Files.PAGE_SIZE, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
        Assert.assertNotEquals("mmap failed", -1L, page0);
        Unsafe.getUnsafe().setMemory(page0, Files.PAGE_SIZE, NEW);
        ff.msync(page0, Files.PAGE_SIZE, true);
        ff.munmap(page0, Files.PAGE_SIZE, MemoryTag.MMAP_DEFAULT);
        final long page1 = ff.mmap(fd, Files.PAGE_SIZE, Files.PAGE_SIZE, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
        Assert.assertNotEquals("mmap failed", -1L, page1);
        Unsafe.getUnsafe().setMemory(page1, Files.PAGE_SIZE, NEW);
        ff.munmap(page1, Files.PAGE_SIZE, MemoryTag.MMAP_DEFAULT);
        return fd;
    }

    /**
     * One operation per kind of flush the facade models, each run on a directory holding A and B; test 16 runs them
     * after the power loss.
     */
    private static Map<String, BiConsumer<CrashFaultFilesFacade, String>> flushesAfterThePowerLoss() {
        final Map<String, BiConsumer<CrashFaultFilesFacade, String>> flushes = new LinkedHashMap<>();
        flushes.put("fsync", (ff, dir) -> rewriteAndSync(ff, dir, "a.d", (f, m) -> f.fsync(m.fd)));
        flushes.put("fdatasync", (ff, dir) -> rewriteAndSync(ff, dir, "a.d", (f, m) -> f.fdatasync(m.fd)));
        flushes.put("msync(MS_SYNC)", (ff, dir) -> rewriteAndSync(ff, dir, "a.d", (f, m) -> f.msync(m.addr, SIZE, false)));
        flushes.put("syncfs", (ff, dir) -> rewriteAndSync(ff, dir, "a.d", (f, m) -> f.syncfs(m.fd)));
        flushes.put("fsyncAndClose", (ff, dir) -> {
            try (Path a = new Path().of(dir).concat("a.d")) {
                final Mapped m = mapAndFill(ff, a, NEW);
                ff.munmap(m.addr, SIZE, MemoryTag.MMAP_DEFAULT);
                ff.fsyncAndClose(m.fd);
            }
        });
        if (Os.isWindows()) {
            // Windows opens no directory, so it has no directory fsync to run; tests 16 and 22 check the rest.
            return flushes;
        }
        flushes.put("new file, directory fsync", (ff, dir) -> {
            rewriteAndSync(ff, dir, "c.d", (f, m) -> f.fsync(m.fd));
            syncDirectory(ff, dir);
        });
        flushes.put("unlink, directory fsync", (ff, dir) -> {
            try (Path b = new Path().of(dir).concat("b.d")) {
                Assert.assertTrue(ff.removeQuiet(b.$()));
            }
            syncDirectory(ff, dir);
        });
        flushes.put("rename, directory fsync", (ff, dir) -> {
            try (Path a = new Path().of(dir).concat("a.d"); Path d = new Path().of(dir).concat("d.d")) {
                Assert.assertEquals(Files.FILES_RENAME_OK, ff.rename(a.$(), d.$()));
            }
            syncDirectory(ff, dir);
        });
        return flushes;
    }

    private static void expectRefused(StringBuilder failures, String op, Runnable mutation) {
        try {
            mutation.run();
            failures.append(op).append(" through a pre-crash handle was not refused\n");
        } catch (IllegalStateException refused) {
            if (!refused.getMessage().contains("before the power loss")) {
                failures.append(op).append(": ").append(refused.getMessage()).append('\n');
            }
        }
    }

    private static boolean exists(String dir, String name) {
        return java.nio.file.Files.exists(Paths.get(dir, name));
    }

    /**
     * True if {@code dir/name} exists and holds {@code value} in any of its bytes.
     */
    private static boolean holdsAny(String dir, String name, byte value) throws IOException {
        if (!exists(dir, name)) {
            return false;
        }
        for (byte got : java.nio.file.Files.readAllBytes(Paths.get(dir, name))) {
            if (got == value) {
                return true;
            }
        }
        return false;
    }

    /**
     * True if {@code dir/name} exists and holds {@code value} in each of its first SIZE bytes.
     */
    private static boolean holdsOnly(String dir, String name, byte value) throws IOException {
        if (!exists(dir, name)) {
            return false;
        }
        final byte[] got = java.nio.file.Files.readAllBytes(Paths.get(dir, name));
        if (got.length < SIZE) {
            return false;
        }
        for (int i = 0; i < SIZE; i++) {
            if (got[i] != value) {
                return false;
            }
        }
        return true;
    }

    /**
     * Maps {@code dir/name}, fills it with NEW, runs {@code sync} with the mapping live, then unmaps and closes.
     */
    private static void rewriteAndSync(CrashFaultFilesFacade ff, String dir, String name, BiConsumer<CrashFaultFilesFacade, Mapped> sync) {
        try (Path path = new Path().of(dir).concat(name)) {
            final Mapped m = mapAndFill(ff, path, NEW);
            sync.accept(ff, m);
            unmapAndClose(ff, m);
        }
    }

    private static void syncDirectory(CrashFaultFilesFacade ff, String dir) {
        try (Path path = new Path().of(dir)) {
            final long fd = ff.openRO(path.$());
            Assert.assertTrue("openRO failed, fd=" + fd, fd > 0);
            try {
                ff.fsync(fd);
            } finally {
                ff.close(fd);
            }
        }
    }
}
