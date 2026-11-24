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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * The table writer maps a WAL transaction's columns from segment fds it cached for the
 * previous transaction. A failure part-way through that mapping must close every cached fd,
 * including those no column memory has taken yet. It must also leave the cache entries of
 * other segments in place.
 */
public class WalSegmentFdCacheFailureTest extends AbstractCairoTest {

    @Test
    public void testFailedMapFromCachedFdsClosesThem() throws Exception {
        // The fd of wal1/0/v.i, the aux file of VARCHAR column v. Its second mmap fails:
        // the first maps the segment for the plain commit, the second maps it from the
        // cached fds for the replace commit that follows in the same segment.
        final AtomicLong auxFd = new AtomicLong(-1);
        final AtomicInteger auxMaps = new AtomicInteger();
        final AtomicInteger faults = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (fd == auxFd.get() && auxMaps.incrementAndGet() == 2) {
                    faults.incrementAndGet();
                    return FilesFacade.MAP_FAILED;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRO(LPSZ name) {
                final long fd = super.openRO(name);
                if (Utf8s.containsAscii(name, "wal1") && Utf8s.endsWithAscii(name, "v.i")) {
                    auxFd.set(fd);
                }
                return fd;
            }
        };
        assertMemoryLeak(ff, () -> {
            // v fails; i and s come after it, so their cached fds are not taken yet
            execute("CREATE TABLE t (ts TIMESTAMP, v VARCHAR, i INT, s STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("t");
            try (WalWriter writer = engine.getWalWriter(token)) {
                appendRow(writer, "2024-01-01T01:00:00.000000Z", "a", 1);
                writer.commit();
                // A replace commit applies on its own, so the plain commit before it is not
                // the segment's last use in the batch and its fds stay cached for this one.
                appendRow(writer, "2024-01-01T02:00:00.000000Z", "b", 2);
                writer.commitWithParams(
                        MicrosTimestampDriver.floor("2024-01-01T02:00:00.000000Z"),
                        MicrosTimestampDriver.floor("2024-01-01T03:00:00.000000Z"),
                        WAL_DEDUP_MODE_REPLACE_RANGE
                );
            }
            drainWalQueue();
            Assert.assertEquals("fault must fire exactly once", 1, faults.get());
            Assert.assertTrue("t must suspend", engine.getTableSequencerAPI().isSuspended(token));

            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();
            Assert.assertFalse("t suspended", engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\ti\ts
                            2024-01-01T01:00:00.000000Z\ta\t1\ts1
                            2024-01-01T02:00:00.000000Z\tb\t2\ts2
                            """);
        });
    }

    /**
     * The other sub-case of a failed cached-fd replace map: a var-size column's aux maps cleanly but
     * reads corrupt offsets, so its data memory adopts the cached data fd and then {@code mapLazy}
     * asserts under -ea. The failed-map cleanup must not close that data fd again: the data memory now
     * owns it and {@code closeWalFiles()} closes it, so a second close in the catch makes
     * {@code Files.close()} throw "fd already closed", abort the rest of the cleanup, and leak the aux fd.
     *
     * <p>A plain commit caches wal1/0's fds; a replace commit re-maps the segment from them. The aux of
     * STRING column {@code str} maps cleanly for the plain commit but, on its second mmap (the replace
     * path), returns an all-0xFF buffer, so its offsets read as -1. {@code StringTypeDriver
     * .configureDataMemOM} then calls {@code dataMem.ofOffset(..., str.d fd, lo=-1, hi=-1)}; ofOffset
     * adopts the data fd and {@code mapLazy} asserts {@code hi >= 0}. {@code str} is not the last
     * column, so the trailing INT column's cached fd is never taken and the catch's close loop closes it.
     */
    @Test
    public void testCorruptCachedAuxClosesEachFdOnce() throws Exception {
        // wal1/0/str.i (aux) and wal1/0/str.d (data) fds. The aux's second mmap -- the replace path's
        // re-map from the cached fds -- returns a corrupt buffer; the first maps the plain commit.
        final AtomicLong strAuxFd = new AtomicLong(-1);
        final AtomicLong strDataFd = new AtomicLong(-1);
        final AtomicLong fakeAddr = new AtomicLong(0);
        final AtomicLong fakeLen = new AtomicLong(0);
        final AtomicInteger auxMaps = new AtomicInteger();
        final AtomicInteger faults = new AtomicInteger();
        // str.d (the data fd) is read only on the replace apply's cached-fd path, so its close count is
        // the clean double-close signal. The aux fd is opened and closed many times across sampling
        // and the apply, so assertMemoryLeak -- not a close count -- guards it against the leak the
        // double close triggers.
        final AtomicInteger strDataCloses = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean close(long fd) {
                if (fd != -1 && fd == strDataFd.get()) {
                    strDataCloses.incrementAndGet();
                }
                return super.close(fd);
            }

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (fd == strAuxFd.get() && auxMaps.incrementAndGet() == 2) {
                    // Substitute an all-0xFF buffer for the real aux mapping so its offsets read as -1
                    // and the data memory's mapLazy assert sees hi < 0. The malloc and the matching
                    // munmap below use the same NATIVE_DEFAULT tag and never touch an mmap tag, so
                    // native accounting stays balanced -- a leak at HEAD still shows in assertMemoryLeak.
                    faults.incrementAndGet();
                    final long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.getUnsafe().setMemory(buf, len, (byte) 0xFF);
                    fakeAddr.set(buf);
                    fakeLen.set(len);
                    return buf;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public void munmap(long address, long size, int memoryTag) {
                if (address != 0 && address == fakeAddr.get()) {
                    Unsafe.free(fakeAddr.get(), fakeLen.get(), MemoryTag.NATIVE_DEFAULT);
                    fakeAddr.set(0);
                    return;
                }
                super.munmap(address, size, memoryTag);
            }

            @Override
            public long openRO(LPSZ name) {
                final long fd = super.openRO(name);
                if (Utf8s.containsAscii(name, "wal1")) {
                    if (Utf8s.endsWithAscii(name, "str.i")) {
                        strAuxFd.set(fd);
                    } else if (Utf8s.endsWithAscii(name, "str.d")) {
                        strDataFd.set(fd);
                    }
                }
                return fd;
            }
        };
        assertMemoryLeak(ff, () -> {
            // str is var-size but not the last column: the trailing INT column's cached fd is never
            // taken, so the catch's fd-close loop must close it.
            execute("CREATE TABLE t (ts TIMESTAMP, str STRING, i INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("t");
            try (WalWriter writer = engine.getWalWriter(token)) {
                appendStrRow(writer, "2024-01-01T01:00:00.000000Z", "a", 1);
                writer.commit();
                // A replace commit applies on its own, so the plain commit before it is not the
                // segment's last use in the batch and its fds stay cached for this one.
                appendStrRow(writer, "2024-01-01T02:00:00.000000Z", "b", 2);
                writer.commitWithParams(
                        MicrosTimestampDriver.floor("2024-01-01T02:00:00.000000Z"),
                        MicrosTimestampDriver.floor("2024-01-01T03:00:00.000000Z"),
                        WAL_DEDUP_MODE_REPLACE_RANGE
                );
            }
            drainWalQueue();

            Assert.assertEquals("corruption must fire exactly once", 1, faults.get());
            Assert.assertTrue("t must suspend on the corrupt aux", engine.getTableSequencerAPI().isSuspended(token));
            // The data fd is closed exactly once. At HEAD the catch closes the data fd that dataMem
            // already adopted, so closeWalFiles() (and the enclosing map's cleanup) close it again --
            // Files.close() throws, aborts the cleanup, and leaks the aux fd, which assertMemoryLeak
            // then reports. With the fix only closeWalFiles() closes it, exactly once.
            Assert.assertEquals("str.d (data) fd must be closed exactly once", 1, strDataCloses.get());
            // The table suspends on the original corruption (mapLazy's assert), not on a double close.
            final String errorMessage = engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage();
            TestUtils.assertContains(errorMessage, "hi : -1 lo : -1");
            Assert.assertFalse("suspend reason must not be a double close: " + errorMessage,
                    errorMessage.contains("already closed"));

            // Once the corruption is gone the table resumes and reads both rows.
            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();
            Assert.assertFalse("t must resume", engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tstr\ti
                            2024-01-01T01:00:00.000000Z\ta\t1
                            2024-01-01T02:00:00.000000Z\tb\t2
                            """);
        });
    }

    /**
     * A failed re-map must remove only its own segment's cache entry. The fd cache is a linear-probing
     * hash map, and wal1/8's key collides with wal2/3's: with wal2/3's fds cached first, wal1/8's entry
     * sits in the next slot of the same probe chain. Removing wal2/3's entry after its re-map fails moves
     * wal1/8's into the freed slot, so a second removal at that slot would erase wal1/8's entry, and
     * nothing would ever close its fds: assertMemoryLeak would report them.
     */
    @Test
    public void testFailedMapFromCachedFdsRemovesOnlyItsOwnEntry() throws Exception {
        // One commit per apply, so the cache takes the fds in commit order: wal2/3's before wal1/8's.
        setProperty(PropertyKey.DEBUG_WAL_APPLY_MAX_TXN_BLOCK_SIZE, 1);
        final String failingSegmentDir = "wal2" + Files.SEPARATOR + "3" + Files.SEPARATOR;
        final String collidingSegmentDir = "wal1" + Files.SEPARATOR + "8" + Files.SEPARATOR;
        // The fd of wal2/3/v.i. Its second mmap fails: the first maps the segment for the plain commit,
        // the second maps it from the cached fds for the replace commit.
        final AtomicLong auxFd = new AtomicLong(-1);
        // The fds of that first and second mmap: one fd, unless the replace commit opened the file again.
        final AtomicLong firstAuxMapFd = new AtomicLong(-1);
        final AtomicLong failedAuxMapFd = new AtomicLong(-1);
        final AtomicInteger auxMaps = new AtomicInteger();
        final AtomicInteger faults = new AtomicInteger();
        // The wal1/8 fds that openRO returned and close has not closed yet, and their count at the fault.
        final LongHashSet collidingSegmentFds = new LongHashSet();
        final AtomicInteger collidingSegmentFdsAtFault = new AtomicInteger(-1);
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean close(long fd) {
                synchronized (collidingSegmentFds) {
                    collidingSegmentFds.remove(fd);
                }
                return super.close(fd);
            }

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (fd == auxFd.get()) {
                    final int auxMap = auxMaps.incrementAndGet();
                    if (auxMap == 1) {
                        firstAuxMapFd.set(fd);
                    } else if (auxMap == 2) {
                        faults.incrementAndGet();
                        failedAuxMapFd.set(fd);
                        synchronized (collidingSegmentFds) {
                            collidingSegmentFdsAtFault.set(collidingSegmentFds.size());
                        }
                        return FilesFacade.MAP_FAILED;
                    }
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRO(LPSZ name) {
                final long fd = super.openRO(name);
                if (Utf8s.containsAscii(name, failingSegmentDir) && Utf8s.endsWithAscii(name, "v.i")) {
                    auxFd.set(fd);
                } else if (fd != -1 && Utf8s.containsAscii(name, collidingSegmentDir)) {
                    synchronized (collidingSegmentFds) {
                        collidingSegmentFds.add(fd);
                    }
                }
                return fd;
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v VARCHAR, i INT, s STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("t");
            try (WalWriter wal1 = engine.getWalWriter(token); WalWriter wal2 = engine.getWalWriter(token)) {
                rollToSegment(wal1, 8);
                rollToSegment(wal2, 3);
                // The cache is a default-sized LongObjHashMap keyed by encodeLowHighInts(segmentId, walId).
                // Unless removing wal2/3's entry moves wal1/8's into its slot, this test cannot fail: pick
                // another pair of segments that collide.
                final long failingKey = Numbers.encodeLowHighInts(wal2.getSegmentId(), wal2.getWalId());
                final long collidingKey = Numbers.encodeLowHighInts(wal1.getSegmentId(), wal1.getWalId());
                final LongObjHashMap<String> cacheModel = new LongObjHashMap<>();
                cacheModel.put(failingKey, "wal2/3");
                cacheModel.put(collidingKey, "wal1/8");
                final int failingIndex = cacheModel.keyIndex(failingKey);
                cacheModel.removeAt(failingIndex);
                Assert.assertEquals("wal1/8 must collide with wal2/3", failingIndex, cacheModel.keyIndex(collidingKey));

                appendRow(wal2, "2024-01-01T01:00:00.000000Z", "a", 1);
                wal2.commit();
                appendRow(wal1, "2024-01-01T04:00:00.000000Z", "c", 3);
                wal1.commit();
                // Re-maps wal2/3 from its cached fds, while wal1/8's are cached too.
                appendRow(wal2, "2024-01-01T02:00:00.000000Z", "b", 2);
                wal2.commitWithParams(
                        MicrosTimestampDriver.floor("2024-01-01T02:00:00.000000Z"),
                        MicrosTimestampDriver.floor("2024-01-01T03:00:00.000000Z"),
                        WAL_DEDUP_MODE_REPLACE_RANGE
                );
                // A later commit to wal1/8 keeps its fds cached after its first commit applies.
                appendRow(wal1, "2024-01-01T05:00:00.000000Z", "d", 4);
                wal1.commit();
            }
            drainWalQueue();
            Assert.assertEquals("fault must fire exactly once", 1, faults.get());
            // The fault hit the re-map from the cached fds, not a fresh open of wal2/3/v.i: fds are
            // unique, so a re-open would map another fd.
            Assert.assertEquals("the replace commit must map wal2/3/v.i's cached fd", firstAuxMapFd.get(), failedAuxMapFd.get());
            // closeWalFiles(false, ...) kept the fds of wal1/8's first commit open in the cache, one per
            // column file: ts.d, v.d, v.i, i.d, s.d and s.i.
            Assert.assertEquals("wal1/8's fds must be cached at the fault", 6, collidingSegmentFdsAtFault.get());
            Assert.assertTrue("t must suspend", engine.getTableSequencerAPI().isSuspended(token));
            TestUtils.assertContains(engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage(), "could not mmap");
            // Both plain commits applied, so the mapping that failed was the replace commit's re-map.
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\ti\ts
                            2024-01-01T01:00:00.000000Z\ta\t1\ts1
                            2024-01-01T04:00:00.000000Z\tc\t3\ts3
                            """);

            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();
            Assert.assertFalse("t suspended", engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\ti\ts
                            2024-01-01T01:00:00.000000Z\ta\t1\ts1
                            2024-01-01T02:00:00.000000Z\tb\t2\ts2
                            2024-01-01T04:00:00.000000Z\tc\t3\ts3
                            2024-01-01T05:00:00.000000Z\td\t4\ts4
                            """);
        });
    }

    private static void appendRow(WalWriter writer, String ts, String v, int i) throws Exception {
        final TableWriter.Row row = writer.newRow(MicrosTimestampDriver.floor(ts));
        row.putVarchar(1, new Utf8String(v));
        row.putInt(2, i);
        row.putStr(3, "s" + i);
        row.append();
    }

    private static void appendStrRow(WalWriter writer, String ts, String str, int i) throws Exception {
        final TableWriter.Row row = writer.newRow(MicrosTimestampDriver.floor(ts));
        row.putStr(1, str);
        row.putInt(2, i);
        row.append();
    }

    private static void rollToSegment(WalWriter writer, int segmentId) {
        for (int i = writer.getSegmentId(); i < segmentId; i++) {
            writer.rollSegment();
        }
        Assert.assertEquals(segmentId, writer.getSegmentId());
    }
}
