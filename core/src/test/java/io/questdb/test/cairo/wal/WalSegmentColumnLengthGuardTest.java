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
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * The WAL apply job checks that each segment column file covers the committed row range before it maps the
 * file. TableWriterSegmentFileCache remembers the lengths it has seen, so the check stats a column file only
 * when the rows to apply reach past the length it already knows. A var-size column's data file must cover the
 * end of the last row's data, which its last aux entry holds; a torn last entry must not be applied.
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

    @Test
    public void testTornLastAuxEntryIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            // The second applied-row count puts the pending rows at the start of an aux page of a 16-byte entry
            // column, so the entry before them is outside the aux mapping of their apply pass.
            final long[] appliedRowCounts = {1, Files.PAGE_SIZE / 16};
            final String[] tears = {"zero", "below", "past"};
            final StringBuilder notRefused = new StringBuilder();
            int table = 0;
            for (String type : new String[]{"STRING", "VARCHAR", "DOUBLE[]"}) {
                for (String tear : tears) {
                    for (long applied : appliedRowCounts) {
                        for (int pending = 1; pending < 3; pending++) {
                            try {
                                assertTornLastAuxEntryIsRefused("t" + table++, type, tear, applied, pending);
                            } catch (AssertionError e) {
                                notRefused.append(e.getMessage()).append('\n');
                            }
                        }
                    }
                }
            }
            Assert.assertEquals("", notRefused.toString());
        });
    }

    private static String insertRows(String tableName, String type, long lo, long count) {
        final String value = switch (type) {
            case "DOUBLE[]" -> "ARRAY[x::DOUBLE, 2.5]";
            case "STRING" -> "('a string value ' || x)::STRING";
            default -> "'a varchar value too long to inline ' || x";
        };
        return "INSERT INTO " + tableName + " SELECT '2022-02-24'::TIMESTAMP + (x + " + lo + ") * 1_000_000, " + value
                + " FROM long_sequence(" + count + ")";
    }

    private static long readLong(FileChannel channel, long position) throws IOException {
        final ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        Assert.assertEquals(Long.BYTES, channel.read(buf, position));
        return buf.getLong(0);
    }

    // Rewrites the data offset of the last row's aux entry (STRING: the data end after the last row), keeping
    // the rest of the entry: "zero" as a torn write leaves it, "below" under the previous row's data end, and
    // "past" beyond the end of the data file.
    private static void tearLastAuxEntry(String dir, String type, String tear, long rowCount) throws IOException {
        final long dataLength = Paths.get(dir, "c.d").toFile().length();
        try (FileChannel aux = FileChannel.open(Paths.get(dir, "c.i"), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            final boolean isString = "STRING".equals(type);
            // VARCHAR keeps the 48-bit offset in the top bytes of the entry's second long, ARRAY in the bottom
            // bytes of its first long; STRING keeps one long per row boundary.
            final long position = isString ? rowCount * Long.BYTES : (rowCount - 1) * 16 + ("VARCHAR".equals(type) ? Long.BYTES : 0);
            final int shift = "VARCHAR".equals(type) ? 16 : 0;
            final long mask = isString ? -1L : ((1L << 48) - 1) << shift;
            final long word = readLong(aux, position);
            final long offset = (word & mask) >>> shift;
            final long previousEnd = isString ? readLong(aux, position - Long.BYTES) : offset;
            final long torn = switch (tear) {
                case "zero" -> 0;
                case "below" -> previousEnd - 1;
                default -> dataLength + 1024;
            };
            final ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            buf.putLong(0, (word & ~mask) | (torn << shift));
            Assert.assertEquals(Long.BYTES, aux.write(buf, position));
        }
    }

    private void assertTornLastAuxEntryIsRefused(String tableName, String type, String tear, long applied, int pending) throws Exception {
        final String detail = "[type=" + type + ", tear=" + tear + ", applied=" + applied + ", pending=" + pending + ']';
        execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, c " + type + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        final TableToken tableToken = engine.verifyTableName(tableName);
        execute(insertRows(tableName, type, 0, applied));
        drainWalQueue();
        execute(insertRows(tableName, type, applied, pending));
        engine.releaseInactive();

        tearLastAuxEntry(Paths.get(root, tableToken.getDirName(), "wal1", "0").toString(), type, tear, applied + pending);

        drainWalQueue();
        final String error = engine.getTableSequencerAPI().getTxnTracker(tableToken).getErrorMessage();
        Assert.assertTrue("not suspended " + detail, engine.getTableSequencerAPI().isSuspended(tableToken));
        // Refused by the guard before the mapping, not by an assertion or a later check.
        Assert.assertTrue("not refused by the guard " + detail + ": " + error, error != null && error.startsWith("WAL segment column"));
        assertQuery("SELECT count() FROM " + tableName)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n" + applied + '\n');
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
