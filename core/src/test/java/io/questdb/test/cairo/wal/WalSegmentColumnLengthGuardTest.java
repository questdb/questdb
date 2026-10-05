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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * The WAL apply job checks that each segment column file covers the committed row range before it maps the
 * file. TableWriterSegmentFileCache remembers the lengths it has seen, so the check stats a column file only
 * when the rows to apply reach past the length it already knows.
 */
public class WalSegmentColumnLengthGuardTest extends AbstractCairoTest {

    @Test
    public void testFreshWriterStatsAgainAndCatchesShortFile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, i INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken tableToken = engine.verifyTableName("t");
            execute("INSERT INTO t VALUES ('2022-02-24T00:00:00.000000Z', 1)");
            drainWalQueue();

            // Pending rows in the same segment, whose lengths the apply side has already seen.
            execute("INSERT INTO t VALUES ('2022-02-24T00:00:01.000000Z', 2), ('2022-02-24T00:00:02.000000Z', 3)");
            engine.releaseInactive();

            // Cut the pending rows off the column file, as an OS crash under nosync can. The writer that
            // learned the old length is gone, so its replacement must stat the file and refuse it.
            final FilesFacade ff = configuration.getFilesFacade();
            final Path path = Path.getThreadLocal(root).concat(tableToken).concat("wal1").concat("0").concat("i.d");
            final long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            try {
                Assert.assertTrue(ff.truncate(fd, Integer.BYTES));
            } finally {
                ff.close(fd);
            }

            drainWalQueue();
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\ti
                            2022-02-24T00:00:00.000000Z\t1
                            """);
        });
    }

    @Test
    public void testStatsOnlyWhenRowsOutgrowKnownLength() throws Exception {
        final ColumnStatCountingFilesFacade ff = new ColumnStatCountingFilesFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, i INT, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2022-02-24T00:00:00.000000Z', 0, 'a value too long to inline')");
            drainWalQueue();
            final int firstPassStats = ff.columnStatCount;
            // ts.d, i.d, v.d and v.i, each seen for the first time.
            Assert.assertEquals(4, firstPassStats);

            // Trickle ingestion: one transaction per apply pass, all inside the length already seen.
            for (int i = 1; i <= 10; i++) {
                execute("INSERT INTO t VALUES ('2022-02-24T00:00:" + (i < 10 ? "0" : "") + i + ".000000Z', " + i + ", 'a value too long to inline')");
                drainWalQueue();
            }
            Assert.assertEquals(firstPassStats, ff.columnStatCount);
            assertQuery("SELECT count(), sum(i) FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            11\t55
                            """);

            // A transaction big enough to reach past the preallocated length must stat again.
            execute("""
                    INSERT INTO t
                    SELECT '2022-02-25'::TIMESTAMP + x * 1_000_000, x::INT, 'a value too long to inline'
                    FROM long_sequence(200_000)
                    """);
            drainWalQueue();
            Assert.assertTrue(ff.columnStatCount > firstPassStats);
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            assertQuery("SELECT count() FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            200011
                            """);
        });
    }

    private static class ColumnStatCountingFilesFacade extends TestFilesFacadeImpl {
        private int columnStatCount;

        @Override
        public long length(LPSZ name) {
            if (Utf8s.containsAscii(name, "wal1") && (Utf8s.endsWithAscii(name, ".d") || Utf8s.endsWithAscii(name, ".i"))) {
                columnStatCount++;
            }
            return super.length(name);
        }
    }
}
