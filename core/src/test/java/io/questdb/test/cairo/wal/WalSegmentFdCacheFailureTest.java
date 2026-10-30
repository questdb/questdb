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
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
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

    private static void appendRow(WalWriter writer, String ts, String v, int i) throws Exception {
        final TableWriter.Row row = writer.newRow(MicrosTimestampDriver.floor(ts));
        row.putVarchar(1, new Utf8String(v));
        row.putInt(2, i);
        row.putStr(3, "s" + i);
        row.append();
    }
}
