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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.std.str.LPSZ;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The {@code data.parquet} barrier must be issued through a handle the platform will actually accept.
 * <p>
 * Both parquet producers -- the CONVERT path ({@code TableUtils.produceParquetFromNative}) and the O3
 * rewrite path ({@code O3PartitionJob}) -- fsync {@code data.parquet} before {@code _pm} is published, so
 * that a committed {@code _pm} can never name data-file bytes still sitting in the page cache. That barrier
 * is only worth having if it runs on every supported platform.
 * <p>
 * It did not. Both sites opened the file with {@code openRONoCache} and fsynced THAT fd. On POSIX an
 * {@code O_RDONLY} fd fsyncs fine, so every Linux CI leg was green; on Windows {@code fsync} is
 * {@code FlushFileBuffers}, which requires {@code GENERIC_WRITE} on the handle and returns
 * {@code ERROR_ACCESS_DENIED} (errno 5) for the {@code GENERIC_READ}-only handle {@code openRO} produces.
 * The barrier threw, the conversion was rolled back, and every retry threw again -- so under any commit
 * mode other than {@code nosync} a Windows instance could not produce parquet at all, which took Enterprise
 * cold storage (whose promotion path is exactly this conversion) down with it.
 * <p>
 * {@code Os.isWindows()} is a runtime constant that a test cannot fake, so this models the one platform
 * behaviour that differs: a barrier on a handle opened without write access FAILS. Directories are exempt
 * because Windows never fsyncs them at all -- every directory barrier in the engine is already behind an
 * {@code Os.isWindows()} / {@code isRestrictedFileSystem()} guard, and on Linux they legitimately go through
 * read-only handles.
 */
public class ParquetBarrierHandleAccessTest extends AbstractCairoTest {

    @Test
    public void testConvertToParquetBarrierUsesAWriteCapableHandle() throws Exception {
        final WindowsBarrierContractFacade ff = new WindowsBarrierContractFacade();
        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES (1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO x VALUES (2, '2024-06-11T00:00:00.000000Z')");

            ff.clear();
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10'");

            assertNoReadOnlyBarrier(ff);
            // The counter proves the code under test was reached: a conversion that silently skipped the
            // barrier would also report zero rejections.
            Assert.assertTrue(
                    "data.parquet was never fsynced, so this test would pass even with the barrier deleted"
                            + ff.debugDump(),
                    ff.barrierCount("data.parquet") > 0
            );
            assertQuery("x WHERE ts IN '2024-06-10'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("id\tts\n1\t2024-06-10T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testO3RewriteOfAParquetPartitionUsesAWriteCapableHandle() throws Exception {
        final WindowsBarrierContractFacade ff = new WindowsBarrierContractFacade();
        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES (1, '2024-06-10T00:00:10.000000Z')");
            execute("INSERT INTO x VALUES (3, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10'");

            ff.clear();
            // Lands INSIDE the parquet partition and before its existing row, so the partition is rewritten
            // through O3PartitionJob rather than appended to.
            execute("INSERT INTO x VALUES (2, '2024-06-10T00:00:05.000000Z')");

            assertNoReadOnlyBarrier(ff);
            Assert.assertTrue(
                    "the O3 rewrite never fsynced data.parquet, so this test would pass with the barrier deleted"
                            + ff.debugDump(),
                    ff.barrierCount("data.parquet") > 0
            );
            assertQuery("x WHERE ts IN '2024-06-10'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("id\tts\n2\t2024-06-10T00:00:05.000000Z\n1\t2024-06-10T00:00:10.000000Z\n");
        });
    }

    private static void assertNoReadOnlyBarrier(WindowsBarrierContractFacade ff) {
        if (ff.rejectedCount() > 0) {
            Assert.fail(
                    "a durability barrier was issued on a handle opened WITHOUT write access, which"
                            + " ERROR_ACCESS_DENIEDs on Windows: " + ff.rejectedPaths() + ff.debugDump()
            );
        }
    }

    /**
     * Models the one relevant Windows difference: {@code FlushFileBuffers} needs {@code GENERIC_WRITE}, so a
     * barrier on a read-only FILE handle fails with {@code ERROR_ACCESS_DENIED} (5). Read-only handles on
     * DIRECTORIES are not tracked -- Windows cannot fsync a directory at all and the engine already skips
     * those, whereas on Linux they are a legitimate, load-bearing barrier.
     * <p>
     * The failure is RECORDED rather than only thrown, so an assertion can name the offending path instead of
     * surfacing as a bare "[5] could not fsync [fd=...]" the way the Windows CI leg did.
     */
    private static class WindowsBarrierContractFacade extends TestFilesFacadeImpl {
        private final Map<String, int[]> barriers = new HashMap<>();
        private final Map<Long, String> fdToPath = new HashMap<>();
        private final Set<Long> readOnlyFileFds = new HashSet<>();
        private final Set<String> rejected = new HashSet<>();

        @Override
        public void barrierFsync(long fd) {
            enforceWriteAccess(fd);
            super.barrierFsync(fd);
        }

        public synchronized int barrierCount(String pathContains) {
            int total = 0;
            for (Map.Entry<String, int[]> e : barriers.entrySet()) {
                if (e.getKey().contains(pathContains)) {
                    total += e.getValue()[0];
                }
            }
            return total;
        }

        /**
         * Forget the counters but KEEP the fd table, which describes live open handles.
         */
        public synchronized void clear() {
            barriers.clear();
            rejected.clear();
        }

        @Override
        public boolean close(long fd) {
            forget(fd);
            return super.close(fd);
        }

        public synchronized String debugDump() {
            final StringBuilder sb = new StringBuilder("\nBARRIERS:\n");
            for (Map.Entry<String, int[]> e : barriers.entrySet()) {
                sb.append("  ").append(e.getKey()).append(" x").append(e.getValue()[0]).append('\n');
            }
            return sb.toString();
        }

        @Override
        public void fdatasync(long fd) {
            enforceWriteAccess(fd);
            super.fdatasync(fd);
        }

        @Override
        public void fsync(long fd) {
            enforceWriteAccess(fd);
            super.fsync(fd);
        }

        @Override
        public void fsyncAndClose(long fd) {
            try {
                enforceWriteAccess(fd);
            } catch (CairoException e) {
                // Mirror the real facade, which closes even when the barrier fails, so a rejection here
                // does not also leak the fd and turn this into a misleading memory-leak failure.
                super.close(fd);
                forget(fd);
                throw e;
            }
            forget(fd);
            super.fsyncAndClose(fd);
        }

        @Override
        public void fsyncDurable(long fd) {
            enforceWriteAccess(fd);
            super.fsyncDurable(fd);
        }

        @Override
        public long openCleanRW(LPSZ name, long size) {
            final long fd = super.openCleanRW(name, size);
            remember(fd, name, true);
            return fd;
        }

        @Override
        public long openRO(LPSZ name) {
            final long fd = super.openRO(name);
            remember(fd, name, false);
            return fd;
        }

        @Override
        public long openRONoCache(LPSZ name) {
            final long fd = super.openRONoCache(name);
            remember(fd, name, false);
            return fd;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            remember(fd, name, true);
            return fd;
        }

        @Override
        public long openRWNoCache(LPSZ name, int opts) {
            final long fd = super.openRWNoCache(name, opts);
            remember(fd, name, true);
            return fd;
        }

        public synchronized int rejectedCount() {
            return rejected.size();
        }

        public synchronized Set<String> rejectedPaths() {
            return new HashSet<>(rejected);
        }

        private synchronized void enforceWriteAccess(long fd) {
            final String path = fdToPath.get(fd);
            if (path != null) {
                bump(path);
            }
            if (readOnlyFileFds.contains(fd)) {
                rejected.add(path == null ? "<fd " + fd + '>' : path);
                throw CairoException.critical(5).put("could not fsync [fd=").put(fd).put(']');
            }
        }

        private synchronized void bump(String path) {
            final int[] c = barriers.get(path);
            if (c == null) {
                barriers.put(path, new int[]{1});
            } else {
                c[0]++;
            }
        }

        private synchronized void forget(long fd) {
            fdToPath.remove(fd);
            readOnlyFileFds.remove(fd);
        }

        private synchronized void remember(long fd, LPSZ name, boolean writable) {
            if (fd < 0) {
                return;
            }
            final String path = pathToString(name);
            fdToPath.put(fd, path);
            if (writable || isDirOrSoftLinkDir(name)) {
                readOnlyFileFds.remove(fd);
            } else {
                readOnlyFileFds.add(fd);
            }
        }

        /**
         * Decode by BYTES: {@code Path$PathLPSZ} does not override {@code toString()}, so the obvious
         * conversions yield an identity hash and every path lookup silently misses.
         */
        private static String pathToString(LPSZ name) {
            final int n = name.size();
            final StringBuilder sb = new StringBuilder(n);
            for (int i = 0; i < n; i++) {
                sb.append((char) (name.byteAt(i) & 0xFF));
            }
            return sb.toString();
        }
    }
}
