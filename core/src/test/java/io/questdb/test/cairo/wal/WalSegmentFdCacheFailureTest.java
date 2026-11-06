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

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
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
 * including those no column memory has taken yet.
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
}
