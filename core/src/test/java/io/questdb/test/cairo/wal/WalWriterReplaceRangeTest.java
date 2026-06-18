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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.NumericException;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT;
import static io.questdb.PropertyKey.CAIRO_WAL_MAX_LAG_SIZE;
import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

public class WalWriterReplaceRangeTest extends AbstractCairoTest {

    @Test
    public void testReplaceRangeAddsPartitionsAboveLastThenRebuilds() throws Exception {
        // Regression for a replace-mode O3 bug (reachable via live-view BACKFILL + O3).
        // Commit A is a REPLACE_RANGE [oldMax + 1us, +inf) that appends new partitions above the
        // previous last partition (2026-01-05), leaving it untouched. The open-ended high boundary
        // (Long.MAX_VALUE - 1) overflowed getCurrentPartitionMaxTimestamp, so partitionTimestampHi
        // stayed stale and finishO3Commit never switched the active columns to the new last
        // partition (2026-01-10). Commit B, a full rebuild (REPLACE_RANGE [min, +inf)), then
        // treated 2026-01-10 as the active partition, reused the stale 2026-01-05 descriptors, and
        // corrupted the view / suspended the table.
        assertMemoryLeak(() -> {
            execute("create table rg (id long, ts timestamp, v long) timestamp(ts) partition by DAY WAL");
            execute("create table expected (id long, ts timestamp, v long) timestamp(ts) partition by DAY WAL");
            TableToken rg = engine.verifyTableName("rg");
            TableToken expected = engine.verifyTableName("expected");

            // Existing data: days 2026-01-01..2026-01-05, last partition 2026-01-05 maxing at
            // exactly 2026-01-05T00:03:46.111248Z.
            for (int day = 0; day < 5; day++) {
                try (WalWriter ww = engine.getWalWriter(rg)) {
                    long[] tss = existingDay(day);
                    for (int r = 0; r < tss.length; r++) {
                        appendRow(ww, tss[r], day * 100L + r, (day * 100L + r) * 10);
                    }
                    ww.commit();
                }
                drainWalQueue();
            }

            // Commit A: replace [oldMax + 1us, +inf) with new partitions above the last: days 06,
            // 08, 09, 10 (gap at 07).
            long rangeLoA = MicrosTimestampDriver.floor("2026-01-05T00:03:46.111248Z") + 1;
            long[] newTss = newPartitionRows();
            try (WalWriter ww = engine.getWalWriter(rg)) {
                for (int i = 0; i < newTss.length; i++) {
                    appendRow(ww, newTss[i], 1000L + i, (1000L + i) * 10);
                }
                ww.commitWithParams(rangeLoA, Long.MAX_VALUE, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            // Commit B: full rebuild. Replace [min, +inf), re-emitting every row in ts order,
            // including a new 2026-01-07 row between commit A's partitions.
            try (WalWriter ww = engine.getWalWriter(rg)) {
                appendFinalDataset(ww);
                ww.commitWithParams(MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z"), Long.MAX_VALUE, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            // Oracle: the same final row set written with plain commits, no replace mode.
            try (WalWriter ww = engine.getWalWriter(expected)) {
                appendFinalDataset(ww);
                ww.commit();
            }
            drainWalQueue();

            Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(rg));
            assertSqlCursors("select id, ts, v from expected", "select id, ts, v from rg");
            assertSqlCursors(
                    "select count(*), min(ts), max(ts) from expected",
                    "select count(*), min(ts), max(ts) from rg"
            );
        });
    }

    // The complete final row set in ts order. Commit B and the oracle both write exactly this.
    private static void appendFinalDataset(WalWriter ww) throws NumericException {
        long id = 1;
        for (int day = 0; day < 5; day++) {
            for (long ts : existingDay(day)) {
                appendRow(ww, ts, id, id * 10);
                id++;
            }
        }
        long[] newTss = newPartitionRows();
        appendRow(ww, newTss[0], id++, 0); // 2026-01-06
        appendRow(ww, newTss[1], id++, 0); // 2026-01-06
        appendRow(ww, MicrosTimestampDriver.floor("2026-01-07T00:04:35.803821Z"), id++, 0);
        appendRow(ww, newTss[2], id++, 0); // 2026-01-08
        appendRow(ww, newTss[3], id++, 0); // 2026-01-09
        appendRow(ww, newTss[4], id++, 0); // 2026-01-10
        appendRow(ww, newTss[5], id, 0);   // 2026-01-10
    }

    private static void appendRow(WalWriter ww, long ts, long id, long v) {
        TableWriter.Row row = ww.newRow(ts);
        row.putLong(0, id);
        row.putLong(2, v);
        row.append();
    }

    // Existing rows for the 0-based day index. Days 0..3 hold five rows each; day 4 (2026-01-05)
    // holds six, the last at exactly 2026-01-05T00:03:46.111248Z (the commit-A range starts 1us
    // above it).
    private static long[] existingDay(int day) throws NumericException {
        long base = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        if (day < 4) {
            long dayBase = base + day * 86_400_000_000L;
            return new long[]{dayBase, dayBase + 1_000_000L, dayBase + 2_000_000L, dayBase + 3_000_000L, dayBase + 4_000_000L};
        }
        long d05 = MicrosTimestampDriver.floor("2026-01-05T00:03:32.239828Z");
        return new long[]{
                d05,
                d05 + 3_000_000L,
                d05 + 4_000_000L,
                d05 + 6_000_000L,
                d05 + 9_000_000L,
                MicrosTimestampDriver.floor("2026-01-05T00:03:46.111248Z"),
        };
    }

    // Commit-A rows: days 06, 08, 09, 10 (gap at 07), all above the 2026-01-05 max, in ts order.
    private static long[] newPartitionRows() throws NumericException {
        return new long[]{
                MicrosTimestampDriver.floor("2026-01-06T00:03:46.377496Z"),
                MicrosTimestampDriver.floor("2026-01-06T00:04:00.000000Z"),
                MicrosTimestampDriver.floor("2026-01-08T00:04:41.334009Z"),
                MicrosTimestampDriver.floor("2026-01-09T00:05:19.959452Z"),
                MicrosTimestampDriver.floor("2026-01-10T00:05:54.691962Z"),
                MicrosTimestampDriver.floor("2026-01-10T00:06:00.177392Z"),
        };
    }

    @Test
    public void testManyTransactionsSkippedWhenTruncateIfFound() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(CAIRO_WAL_MAX_LAG_SIZE, 1);

            execute("create table stress (id long, ts timestamp, value long) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("stress");

            long lastMinuteStart = MicrosTimestampDriver.floor("2022-02-24T23:59");

            for (int i = 0; i < 100; i++) {
                try (WalWriter ww = engine.getWalWriter(tableToken)) {
                    // Add a new row with the current replace value
                    TableWriter.Row row = ww.newRow(lastMinuteStart + 30_000_000); // Middle of the last minute
                    row.putLong(0, i);
                    row.putLong(2, i * 100);
                    row.append();
                    ww.commit();
                }
            }
            execute("truncate table stress");
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                // Add a new row with the current replace value
                TableWriter.Row row = ww.newRow(lastMinuteStart + 30_000_000); // Middle of the last minute
                row.putLong(0, 1001);
                row.putLong(2, (1001) * 100);
                row.append();
                ww.commit();
            }

            // Apply any remaining WAL entries
            drainWalQueue();

            // Verify the total count
            // Expected: rows before 23:00 + 3 rows in the replace ranges
            // Based on the test run, the original data has rows from 23:00 to 23:19:26 that get replaced
            assertQuery("select count(*) from stress")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
            try (TableReader rdr = engine.getReader(tableToken)) {
                var txn = rdr.getTxn();
                Assert.assertEquals(3, txn); // Expecting many transactions to be skipped
            }
        });
    }

    @Test
    public void testRemovesFirstPartitionNoRowsAdded() throws Exception {
        testRemovesFirstPartitionNoRowsAdded(true);
    }

    @Test
    public void testRemovesFirstPartitionNoRowsAddedNoRowsCommit() throws Exception {
        testRemovesFirstPartitionNoRowsAdded(false);
    }

    @Test
    public void testRemovesLastPartitionNoRowsAdded() throws Exception {
        testRemovesLastPartitionNoRowsAdded(false);
    }

    @Test
    public void testRemovesLastPartitionNoRowsAddedNoRowsCommit() throws Exception {
        testRemovesLastPartitionNoRowsAdded(true);
    }

    @Test
    public void testReplaceBetweenExisting() throws Exception {
        testReplaceRangeCommit("2022-02-24T16:25", "2022-02-24T16:25", "2022-02-24T16:26");
    }

    @Test
    public void testReplaceCommitAdds2PartitionsBeforeExisting() throws Exception {
        testReplaceCommitAdds2PartitionsBeforeExisting(false);
    }

    @Test
    public void testReplaceCommitAdds2PartitionsBeforeExistingNoRowsCommit() throws Exception {
        testReplaceCommitAdds2PartitionsBeforeExisting(true);
    }

    @Test
    public void testReplaceCommitNotOrdered() throws Exception {
        testReplaceCommitNotOrdered(false);
    }

    @Test
    public void testReplaceCommitNotOrderedNoRowsCommit() throws Exception {
        testReplaceCommitNotOrdered(true);
    }

    @Test
    public void testReplaceCommitRemoves2PartitionsAndAdds1() throws Exception {
        testReplaceCommitRemoves2PartitionsAndAdds1(false);
    }

    @Test
    public void testReplaceCommitRemoves2PartitionsAndAdds1NoRowsCommit() throws Exception {
        testReplaceCommitRemoves2PartitionsAndAdds1(true);
    }

    @Test
    public void testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother() throws Exception {
        testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother(false);
    }

    @Test
    public void testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother2() throws Exception {
        testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother2(false);
    }

    public void testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother2(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T21:31', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(20)");
            execute("insert into rg select x, timestamp_sequence('2022-02-28T21:31', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(20)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-25T02:36:07.769840Z", "2022-02-24T23:20:30", "2022-02-27T01:34:56.265527", tableToken, false, true, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    @Test
    public void testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother2NoRowsCommit() throws Exception {
        testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother2(true);
    }

    @Test
    public void testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnotherNoRowsCommit() throws Exception {
        testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother(true);
    }

    @Test
    public void testReplaceRangeBeforeFirstPartitionAndData() throws Exception {
        testReplaceRangeBeforeFirstPartitionAndData(false);
    }

    @Test
    public void testReplaceRangeBeforeFirstPartitionAndDataNoRowsCommit() throws Exception {
        testReplaceRangeBeforeFirstPartitionAndData(true);
    }

    @Test
    public void testReplaceRangeCommitDataRangeCommitErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");
            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");

            Utf8StringSink utf8StringSink = new Utf8StringSink();
            try {
                insertRowsWithRangeReplace(tableToken, utf8StringSink, "2022-02-24T17", "2022-02-24T18", "2022-02-25T18", true);
                Assert.fail("exception expected");
            } catch (CairoException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "Replace range low timestamp must be less than or equal to the minimum timestamp"
                );
            }

            try {
                insertRowsWithRangeReplace(tableToken, utf8StringSink, "2022-02-25T18", "2022-02-24T18", "2022-02-25T17:59:59.999999", true);
                Assert.fail("exception expected");
            } catch (CairoException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "Replace range high timestamp must be greater than the maximum timestamp"
                );
            }

            try {
                insertRowsWithRangeReplace(tableToken, utf8StringSink, "2022-02-25T18", "2022-02-25T18", "2022-02-25T17:59:59.999999", true);
                Assert.fail("exception expected");
            } catch (CairoException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "Replace range low timestamp must be less than replace range high timestamp"
                );
            }

            drainWalQueue();
            Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
        });
    }

    @Test
    public void testReplaceRangeDeletesMidFirstPartition() throws Exception {
        testReplaceRangeCommit(null, "2022-02-24T18:30", "2022-02-24T20:59:31", false, false, false);
    }

    @Test
    public void testReplaceRangeDeletesMidFirstPartitionNoRowsCommit() throws Exception {
        testReplaceRangeCommit(null, "2022-02-24T18:30", "2022-02-24T20:59:31", false, false, true);
    }

    @Test
    public void testReplaceRangeFirstPartitionNoImpact() throws Exception {
        testReplaceRangeCommit(null, "2022-02-24T23:59:30", "2022-02-24T23:59:31", false, false, false);
    }

    @Test
    public void testReplaceRangeFirstPartitionNoImpactNoRowsCommit() throws Exception {
        testReplaceRangeCommit(null, "2022-02-24T23:59:30", "2022-02-24T23:59:31", false, false, true);
    }

    @Test
    public void testReplaceRangeLastPartition() throws Exception {
        testReplaceRangeLastPartition(true, false);
    }

    @Test
    public void testReplaceRangeLastPartitionAppend() throws Exception {
        testReplaceRangeCommit("2022-02-25T16:25", "2022-02-25T16:00", "2022-02-25T17:00");
    }

    @Test
    public void testReplaceRangeLastPartitionAppendReplacesFullPartition() throws Exception {
        testReplaceRangeCommit("2022-02-25T16:25", "2022-02-25T00:00", "2022-02-25T23:59");
    }

    @Test
    public void testReplaceRangeLastPartitionNoRowsCommit() throws Exception {
        testReplaceRangeLastPartition(false, true);
    }

    @Test
    public void testReplaceRangeLastPartitionPrefix() throws Exception {
        testReplaceRangeCommit("2022-02-25T00:53", "2022-02-25T00:00", "2022-02-25T04");
    }

    @Test
    public void testReplaceRangeLastPartitionSuffix() throws Exception {
        testReplaceRangeCommit("2022-02-25T02:53", "2022-02-25T01:00", "2022-02-25T23");
    }

    @Test
    public void testReplaceRangeNotSupportedParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");
            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");

            execute("ALTER TABLE rg CONVERT PARTITION TO PARQUET LIST '2022-02-24'");
            drainWalQueue();

            insertRowsWithRangeReplace(tableToken, new Utf8StringSink(), "2022-02-24T17", "2022-02-14T17", "2022-02-25T18", true);
            drainWalQueue();

            Assert.assertTrue("table is suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
        });
    }

    @Test
    public void testReplaceRangeSingleCommitMiddleMerge() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(47)");

            drainWalQueue();

            assertQuery("select min(ts), max(ts), count(*) from rg")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min\tmax\tcount
                            2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t47
                            """);

            Utf8StringSink sink = new Utf8StringSink();

            insertRowsWithRangeReplace(tableToken, sink, "2022-02-24T14:45", "2022-02-24T12:45", "2022-02-24T23", true);
            drainWalQueue();

            assertQuery("select min(ts), max(ts), count(*) from rg")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min\tmax\tcount
                            2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t6
                            """);

            assertQuery("select * from rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\ty\ts\tv\tm
                            1\t2022-02-24T12:30:00.000000Z\t0\t1\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\ta
                            100\t2022-02-24T14:45:00.000000Z\t1000\thello\tw\tw
                            44\t2022-02-24T23:15:00.000000Z\t22\t44\t\uDAB1\uDC25J\uD969\uDF86gǢ\uDA97\uDEDC\t
                            45\t2022-02-24T23:30:00.000000Z\t22\t45\tHEZqUhE\ta
                            46\t2022-02-24T23:45:00.000000Z\t23\t46\tL^bE);P\tc
                            47\t2022-02-25T00:00:00.000000Z\t23\t47\t阇1(rոҊG\uD9A6\uDD42\uDB48\uDC78\tb
                            """);


            // Check _txn file makes sense, min, max timestamps, row counts etc.
            Assert.assertEquals(
                    """
                            {txn: 2, attachedPartitions: [
                            {ts: '2022-02-24T00:00:00.000Z', rowCount: 5},
                            {ts: '2022-02-25T00:00:00.000Z', rowCount: 1}
                            ], transientRowCount: 1, fixedRowCount: 5, \
                            minTimestamp: '2022-02-24T12:30:00.000Z', \
                            maxTimestamp: '2022-02-25T00:00:00.000Z', \
                            dataVersion: 0, structureVersion: 0, \
                            columnVersion: 0, truncateVersion: 0, seqTxn: 2, \
                            symbolColumnCount: 1, lagRowCount: 0, \
                            lagMinTimestamp: '294247-01-10T04:00:54.775Z', \
                            lagMaxTimestamp: '', lagTxnCount: 0, lagOrdered: true}""",
                    readTxnToString(tableToken, true, true)
            );
        });
    }

    @Test
    public void testReplaceRangeSingleCommitPrefixMerge() throws Exception {
        setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(47)");

            drainWalQueue();

            assertQuery("select min(ts), max(ts), count(*) from rg")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min\tmax\tcount
                            2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t47
                            """);

            Utf8StringSink sink = new Utf8StringSink();

            insertRowsWithRangeReplace(tableToken, sink, "2022-02-24T14:30", "2022-02-24", "2022-02-24T23", true);
            drainWalQueue();

            assertQuery("select min(ts), max(ts), count(*) from rg")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min\tmax\tcount
                            2022-02-24T14:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t5
                            """);

            assertQuery("select * from rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\ty\ts\tv\tm
                            100\t2022-02-24T14:30:00.000000Z\t1000\thello\tw\tw
                            44\t2022-02-24T23:15:00.000000Z\t22\t44\t\uDAB1\uDC25J\uD969\uDF86gǢ\uDA97\uDEDC\t
                            45\t2022-02-24T23:30:00.000000Z\t22\t45\tHEZqUhE\ta
                            46\t2022-02-24T23:45:00.000000Z\t23\t46\tL^bE);P\tc
                            47\t2022-02-25T00:00:00.000000Z\t23\t47\t阇1(rոҊG\uD9A6\uDD42\uDB48\uDC78\tb
                            """);


            // Check _txn file makes sense, min, max timestamps, row counts etc.
            Assert.assertEquals(
                    """
                            {txn: 2, attachedPartitions: [
                            {ts: '2022-02-24T00:00:00.000Z', rowCount: 4},
                            {ts: '2022-02-25T00:00:00.000Z', rowCount: 1}
                            ], transientRowCount: 1, fixedRowCount: 4, \
                            minTimestamp: '2022-02-24T14:30:00.000Z', \
                            maxTimestamp: '2022-02-25T00:00:00.000Z', \
                            dataVersion: 0, structureVersion: 0, \
                            columnVersion: 0, truncateVersion: 0, seqTxn: 2, \
                            symbolColumnCount: 1, lagRowCount: 0, \
                            lagMinTimestamp: '294247-01-10T04:00:54.775Z', \
                            lagMaxTimestamp: '', lagTxnCount: 0, lagOrdered: true}""",
                    readTxnToString(tableToken, true, true)
            );
        });
    }

    @Test
    public void testReplaceRangeSingleCommitSuffixMerge() throws Exception {
        setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(47)");

            drainWalQueue();

            assertQuery("select min(ts), max(ts), count(*) from rg")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min\tmax\tcount
                            2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t47
                            """);

            Utf8StringSink sink = new Utf8StringSink();

            insertRowsWithRangeReplace(tableToken, sink, "2022-02-24T14:45", "2022-02-24T12:45", "2022-02-24T23:59:59.999999", true);
            drainWalQueue();

            assertQuery("select min(ts), max(ts), count(*) from rg")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min\tmax\tcount
                            2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t3
                            """);

            assertQuery("select * from rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\ty\ts\tv\tm
                            1\t2022-02-24T12:30:00.000000Z\t0\t1\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\ta
                            100\t2022-02-24T14:45:00.000000Z\t1000\thello\tw\tw
                            47\t2022-02-25T00:00:00.000000Z\t23\t47\t阇1(rոҊG\uD9A6\uDD42\uDB48\uDC78\tb
                            """);

            // Check _txn file makes sense, min, max timestamps, row counts etc.
            Assert.assertEquals(
                    """
                            {txn: 2, attachedPartitions: [
                            {ts: '2022-02-24T00:00:00.000Z', rowCount: 2},
                            {ts: '2022-02-25T00:00:00.000Z', rowCount: 1}
                            ], transientRowCount: 1, fixedRowCount: 2, \
                            minTimestamp: '2022-02-24T12:30:00.000Z', \
                            maxTimestamp: '2022-02-25T00:00:00.000Z', \
                            dataVersion: 0, structureVersion: 0, \
                            columnVersion: 0, truncateVersion: 0, seqTxn: 2, \
                            symbolColumnCount: 1, lagRowCount: 0, \
                            lagMinTimestamp: '294247-01-10T04:00:54.775Z', \
                            lagMaxTimestamp: '', lagTxnCount: 0, lagOrdered: true}""",
                    readTxnToString(tableToken, true, true)
            );
        });
    }

    @Test
    public void testReplaceRangeTwoPartitionsDataInFirst() throws Exception {
        testReplaceRangeCommit("2022-02-24T16:25", "2022-02-24T16:25", "2022-02-25T01:00");
    }

    @Test
    public void testReplaceRangeTwoPartitionsDataInSecond() throws Exception {
        testReplaceRangeCommit("2022-02-25T01:00", "2022-02-24T16:25", "2022-02-25T01:00");
    }

    @Test
    public void testReplaceRangeWithColumnAddedInMiddleOfPartition() throws Exception {
        assertMemoryLeak(() -> {
            final int partitionRowCount = 500;

            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            // Insert 1000 rows to fill one partition
            execute("insert into rg select x, timestamp_sequence('2022-02-24T01:00', 86 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(" + partitionRowCount + ")");
            drainWalQueue();

            // Add both fixed size (int) and variable size (string) columns after the partition is full
            // This sets column_top = 1000 for both columns
            execute("alter table rg add column c4 int");
            execute("alter table rg add column c5 string");
            drainWalQueue();

            // Now perform a replace operation on ALL 1000 rows (the entire area where column data doesn't exist)
            // This should trigger the column top fix for both fixed and variable size columns
            Utf8StringSink sink = new Utf8StringSink();
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                // Add one row with new column values for the replace operation
                long ts = MicrosTimestampDriver.floor("2022-02-24T09");
                TableWriter.Row row = ww.newRow(ts);
                row.putInt(0, 999);
                row.putLong(2, 9999);
                row.putStr(3, "replaced");
                sink.clear();
                sink.put("replaced_varchar");
                row.putVarchar(4, sink);
                row.putSym(5, "replaced_sym");
                row.putInt(6, 42); // c4 - fixed size column
                row.putStr(7, "replaced_string"); // c5 - variable size column
                row.append();

                // Replace the entire partition range
                long rangeStart = MicrosTimestampDriver.floor("2022-02-24T00");
                long rangeEnd = MicrosTimestampDriver.floor("2022-02-24T10");
                ww.commitWithParams(rangeStart, rangeEnd, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            // Verify the row content
            assertQuery("select * from rg limit 10")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\ty\ts\tv\tm\tc4\tc5
                            999\t2022-02-24T09:00:00.000000Z\t9999\treplaced\treplaced_varchar\treplaced_sym\t42\treplaced_string
                            378\t2022-02-24T10:00:22.000000Z\t189\t378\tNUZ[\tc\tnull\t
                            379\t2022-02-24T10:01:48.000000Z\t189\t379\t@xbR>i@s\ta\tnull\t
                            380\t2022-02-24T10:03:14.000000Z\t190\t380\t;WS\tc\tnull\t
                            381\t2022-02-24T10:04:40.000000Z\t190\t381\tV1IF \t\tnull\t
                            382\t2022-02-24T10:06:06.000000Z\t191\t382\t14wddTh&))\tb\tnull\t
                            383\t2022-02-24T10:07:32.000000Z\t191\t383\t#<Y达\u197F亙ጾ燇Ȉc\ta\tnull\t
                            384\t2022-02-24T10:08:58.000000Z\t192\t384\t\uDB9E\uDD3D\uF29Ec+ɫwՊ毷걭\ta\tnull\t
                            385\t2022-02-24T10:10:24.000000Z\t192\t385\tsoMv* !Em>\ta\tnull\t
                            386\t2022-02-24T10:11:50.000000Z\t193\t386\t@1oq.w%V\tb\tnull\t
                            """);
        });
    }

    @Test
    public void testReplaceTruncatesAllData() throws Exception {
        testReplaceTruncatesAllData(false);
    }

    @Test
    public void testReplaceTruncatesAllDataAndAddsNewBeforeExisting() throws Exception {
        testReplaceTruncatesAllDataAndAddsNewBeforeExisting(false);
    }

    @Test
    public void testReplaceTruncatesAllDataAndAddsNewBeforeExistingNoRowsCommit() throws Exception {
        testReplaceTruncatesAllDataAndAddsNewBeforeExisting(true);
    }

    @Test
    public void testReplaceTruncatesAllDataNoRowsCommit() throws Exception {
        testReplaceTruncatesAllData(true);
    }

    @Test
    public void testSkipDoesNotCrossSymbolConversion() throws Exception {
        // When a future replace-range txn justifies skipping earlier data txns, the scan must
        // not cross a structural (non-data) txn such as ALTER COLUMN ... TYPE SYMBOL. The
        // conversion seeds its symbol map from the table rows that exist at apply time; skipping
        // the pre-conversion rows under a large lookahead changes which strings get registered
        // first, silently diverging the symbol key assignment from a low-lookahead applier.
        assertMemoryLeak(() -> {
            // Build t1 with a tiny lag window (lookahead=1 + maxLagSize=1) so the apply job
            // sees only the current txn and cannot look ahead to the future replace-range.
            node1.setProperty(CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 1);
            node1.setProperty(CAIRO_WAL_MAX_LAG_SIZE, 1);
            execute("create table rg_conv_small (id long, ts timestamp, s string) timestamp(ts) partition by DAY WAL");
            TableToken tt1 = engine.verifyTableName("rg_conv_small");

            // Three separate data commits with distinct strings (each is a separate sequencer txn).
            // Use separate WalWriter instances to ensure separate sequencer txns.
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T01:00"));
                row.putLong(0, 1);
                row.putStr(2, "AAA");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T02:00"));
                row.putLong(0, 2);
                row.putStr(2, "BBB");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T03:00"));
                row.putLong(0, 3);
                row.putStr(2, "CCC");
                row.append();
                ww.commit();
            }

            // Structural txn: convert s from STRING to SYMBOL
            execute("alter table rg_conv_small alter column s type symbol");

            // Post-conversion data commits via SQL INSERT (each INSERT is a separate sequencer txn).
            // The first-encounter order differs from the pre-conversion writes: CCC, AAA, DDD.
            execute("insert into rg_conv_small values (4, '2022-02-24T10:00:00.000000Z', 'CCC')");
            execute("insert into rg_conv_small values (5, '2022-02-24T11:00:00.000000Z', 'AAA')");
            execute("insert into rg_conv_small values (6, '2022-02-24T12:00:00.000000Z', 'DDD')");

            // Replace-range covering exactly the first three rows (01:00..03:00 window)
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00:00");
                long rangeHi = MicrosTimestampDriver.floor("2022-02-24T04:00");
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            // Build t2 with a large lookahead so all future txns are visible and the
            // replace-range can justify skipping across the ALTER txn (the bug path).
            node1.setProperty(CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 10_000);
            node1.setProperty(CAIRO_WAL_MAX_LAG_SIZE, 1024 * 1024 * 1024);
            execute("create table rg_conv_large (id long, ts timestamp, s string) timestamp(ts) partition by DAY WAL");
            TableToken tt2 = engine.verifyTableName("rg_conv_large");

            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T01:00"));
                row.putLong(0, 1);
                row.putStr(2, "AAA");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T02:00"));
                row.putLong(0, 2);
                row.putStr(2, "BBB");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T03:00"));
                row.putLong(0, 3);
                row.putStr(2, "CCC");
                row.append();
                ww.commit();
            }
            execute("alter table rg_conv_large alter column s type symbol");
            execute("insert into rg_conv_large values (4, '2022-02-24T10:00:00.000000Z', 'CCC')");
            execute("insert into rg_conv_large values (5, '2022-02-24T11:00:00.000000Z', 'AAA')");
            execute("insert into rg_conv_large values (6, '2022-02-24T12:00:00.000000Z', 'DDD')");
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00:00");
                long rangeHi = MicrosTimestampDriver.floor("2022-02-24T04:00");
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            // (a) Query contents must agree
            assertQuery("select id, s from rg_conv_small order by id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\ts\n4\tCCC\n5\tAAA\n6\tDDD\n");
            assertQuery("select id, s from rg_conv_large order by id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\ts\n4\tCCC\n5\tAAA\n6\tDDD\n");

            // (b) Symbol maps must be identical: same count, same key->string mapping.
            // Pre-fix, t1 seeds the map from the pre-conversion rows (AAA=0, BBB=1, CCC=2)
            // while t2 skips them and starts with CCC=0, AAA=1, DDD=2.
            try (TableReader r1 = engine.getReader(tt1); TableReader r2 = engine.getReader(tt2)) {
                int colIdx1 = r1.getMetadata().getColumnIndex("s");
                int colIdx2 = r2.getMetadata().getColumnIndex("s");
                SymbolMapReader sm1 = r1.getSymbolMapReader(colIdx1);
                SymbolMapReader sm2 = r2.getSymbolMapReader(colIdx2);
                int count = sm1.getSymbolCount();
                Assert.assertEquals("symbol counts differ", count, sm2.getSymbolCount());
                for (int k = 0; k < count; k++) {
                    Assert.assertEquals(
                            "symbol key " + k + " differs",
                            String.valueOf(sm1.valueOf(k)),
                            String.valueOf(sm2.valueOf(k))
                    );
                }
            }
        });
    }

    @Test
    public void testSkipDoesNotCrossTruncate() throws Exception {
        // When a future replace-range txn justifies skipping earlier data txns, the inner scan
        // must not jump straight to TRUNCATE across a non-data structural txn. A soft truncate
        // keeps symbol maps in place, so registering symbols in different orders (depending on
        // which rows were visible when the map was seeded) would make the map applier-dependent.
        assertMemoryLeak(() -> {
            // Build t1 with a tiny lag window so the apply job cannot look ahead to the
            // future replace-range and does not skip any txns.
            node1.setProperty(CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 1);
            node1.setProperty(CAIRO_WAL_MAX_LAG_SIZE, 1);
            execute("create table rg_trunc_small (id long, ts timestamp, s symbol) timestamp(ts) partition by DAY WAL");
            TableToken tt1 = engine.verifyTableName("rg_trunc_small");

            long t1 = MicrosTimestampDriver.floor("2022-02-24T01:00");
            long t2 = MicrosTimestampDriver.floor("2022-02-24T02:00");
            long t3 = MicrosTimestampDriver.floor("2022-02-24T03:00");
            long t4 = MicrosTimestampDriver.floor("2022-02-24T10:00");
            long t5 = MicrosTimestampDriver.floor("2022-02-24T11:00");
            long t6 = MicrosTimestampDriver.floor("2022-02-24T12:00");

            // Three separate data commits before the truncate
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(t1);
                row.putLong(0, 1);
                row.putSym(2, "AAA");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(t2);
                row.putLong(0, 2);
                row.putSym(2, "BBB");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(t3);
                row.putLong(0, 3);
                row.putSym(2, "CCC");
                row.append();
                ww.commit();
            }

            // Structural txn: TRUNCATE (soft-truncate keeps symbol maps)
            execute("truncate table rg_trunc_small");

            // More data commits after the truncate, same symbols in a different first-encounter order
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(t4);
                row.putLong(0, 4);
                row.putSym(2, "CCC");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(t5);
                row.putLong(0, 5);
                row.putSym(2, "AAA");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(t6);
                row.putLong(0, 6);
                row.putSym(2, "DDD");
                row.append();
                ww.commit();
            }

            // Replace-range covering the first batch of rows
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00:00");
                long rangeHi = MicrosTimestampDriver.floor("2022-02-24T04:00");
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            // Build t2 with a large lag window so all future txns are visible and the
            // replace-range can justify skipping across the TRUNCATE txn (the bug path).
            node1.setProperty(CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 10_000);
            node1.setProperty(CAIRO_WAL_MAX_LAG_SIZE, 1024 * 1024 * 1024);
            execute("create table rg_trunc_large (id long, ts timestamp, s symbol) timestamp(ts) partition by DAY WAL");
            TableToken tt2 = engine.verifyTableName("rg_trunc_large");

            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(t1);
                row.putLong(0, 1);
                row.putSym(2, "AAA");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(t2);
                row.putLong(0, 2);
                row.putSym(2, "BBB");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(t3);
                row.putLong(0, 3);
                row.putSym(2, "CCC");
                row.append();
                ww.commit();
            }
            execute("truncate table rg_trunc_large");
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(t4);
                row.putLong(0, 4);
                row.putSym(2, "CCC");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(t5);
                row.putLong(0, 5);
                row.putSym(2, "AAA");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(t6);
                row.putLong(0, 6);
                row.putSym(2, "DDD");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00:00");
                long rangeHi = MicrosTimestampDriver.floor("2022-02-24T04:00");
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            // (a) Query contents must agree
            assertQuery("select id, s from rg_trunc_small order by id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\ts\n4\tCCC\n5\tAAA\n6\tDDD\n");
            assertQuery("select id, s from rg_trunc_large order by id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\ts\n4\tCCC\n5\tAAA\n6\tDDD\n");

            // (b) Symbol maps must be identical.
            try (TableReader r1 = engine.getReader(tt1); TableReader r2 = engine.getReader(tt2)) {
                int colIdx1 = r1.getMetadata().getColumnIndex("s");
                int colIdx2 = r2.getMetadata().getColumnIndex("s");
                SymbolMapReader sm1 = r1.getSymbolMapReader(colIdx1);
                SymbolMapReader sm2 = r2.getSymbolMapReader(colIdx2);
                int count = sm1.getSymbolCount();
                Assert.assertEquals("symbol counts differ", count, sm2.getSymbolCount());
                for (int k = 0; k < count; k++) {
                    Assert.assertEquals(
                            "symbol key " + k + " differs",
                            String.valueOf(sm1.valueOf(k)),
                            String.valueOf(sm2.valueOf(k))
                    );
                }
            }
        });
    }

    @Test
    public void testStressReplaceLastMinuteRepeatedly() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);

            execute("create table stress (id long, ts timestamp, value long) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("stress");

            // Insert 100k rows spanning 1 day, so that partition is reasonably big to rewrite
            execute("insert into stress select x, cast('2022-02-24T00:00' as timestamp) + (x / 60) * 840000 * 60, x * 10 from long_sequence(100000)");
            drainWalQueue();

            // Verify initial state
            assertQuery("select count(*) from stress")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n100000\n");

            // Get the timestamp of the last minute (last row timestamp)
            long lastMinuteStart = MicrosTimestampDriver.floor("2022-02-24T23:59");
            long lastMinuteEnd = lastMinuteStart + 60 * 1_000_000; // 1 minute in microseconds

            // Perform 1,000 replace commits, each replacing the last minute with a new value
            long replaceValue = 1000L;
            long lastValue = 0;
            for (int i = 0; i < 1_000; i++) {
                try (WalWriter ww = engine.getWalWriter(tableToken)) {
                    // Add a new row with the current replace value
                    TableWriter.Row row = ww.newRow(lastMinuteStart + 30_000_000); // Middle of the last minute
                    row.putLong(0, replaceValue + i);
                    row.putLong(2, (replaceValue + i) * 100);
                    row.append();

                    // Commit with replace range for the last minute
                    ww.commitWithParams(lastMinuteStart, lastMinuteEnd, WAL_DEDUP_MODE_REPLACE_RANGE);
                    lastValue = (replaceValue + i) * 100;
                }
            }

            // Apply WAL
            drainWalQueue();

            // Verify the final state
            assertQuery("select count(*) from stress")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n100001\n");

            // Check the results of the last minute
            assertQuery("select count(*) from stress where ts >= '2022-02-24T23:59' and ts < '2022-02-25T00:00'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");

            // Verify the value is from the last replace operation
            assertQuery("select value from stress where ts >= '2022-02-24T23:59' and ts < '2022-02-25T00:00'")
                    .noLeakCheck()
                    .returns("value\n" + lastValue + "\n");
        });
    }

    @Test
    public void testStressReplaceLastMinuteRepeatedlyWithSkippableTransactions() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);

            execute("create table stress (id long, ts timestamp, value long) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("stress");

            // Insert 100k rows spanning 1 day, so that partition is reasonably big to rewrite
            execute("insert into stress select x, cast('2022-02-24T00:00' as timestamp) + (x / 60) * 840000 * 60, x * 10 from long_sequence(100000)");
            drainWalQueue();

            // Verify initial state
            assertQuery("select count(*) from stress")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n100000\n");

            // Base timestamp ranges - we'll vary these to create overlapping and non-overlapping replace ranges
            long baseHour = MicrosTimestampDriver.floor("2022-02-24T23:00");
            long minuteInMicros = 60 * 1_000_000L;

            // Perform 1000 replace commits with randomly varying replace ranges
            // Some transactions will replace the same time range (and can be skipped)
            // Others will replace different ranges (and cannot be skipped)
            long replaceValue = 1000L;
            java.util.Random rnd = new java.util.Random(42); // Fixed seed for reproducibility
            long[] lastValues = new long[3];

            for (int i = 0; i < 1_000; i++) {
                try (WalWriter ww = engine.getWalWriter(tableToken)) {
                    // Randomly choose one of 3 different time ranges to replace
                    // This creates overlaps where ~33% of transactions can be skipped
                    int rangeSelector = rnd.nextInt(3);
                    long rangeStart = baseHour + rangeSelector * 20 * minuteInMicros; // 23:00, 23:20, or 23:40
                    long rangeEnd = rangeStart + 20 * minuteInMicros; // 20 minute ranges

                    // Insert a row within the replace range
                    long insertTs = rangeStart + 10 * minuteInMicros; // Middle of the range
                    TableWriter.Row row = ww.newRow(insertTs);
                    row.putLong(0, replaceValue + i);
                    long currentValue = (replaceValue + i) * 100;
                    row.putLong(2, currentValue);
                    row.append();

                    // Commit with replace range
                    ww.commitWithParams(rangeStart, rangeEnd, WAL_DEDUP_MODE_REPLACE_RANGE);
                    lastValues[rangeSelector] = currentValue;
                }

                // Drain periodically to allow some batching and transaction skipping
                if (i % 10 == 9) {
                    drainWalQueue();
                }
            }

            // Apply any remaining WAL entries
            drainWalQueue();

            assertQuery("select value, ts from stress where ts >= '2022-02-24T23:00'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("value\tts\n" +
                            lastValues[0] + "\t2022-02-24T23:10:00.000000Z\n" +
                            lastValues[1] + "\t2022-02-24T23:30:00.000000Z\n" +
                            lastValues[2] + "\t2022-02-24T23:50:00.000000Z\n");
        });
    }

    @Test
    public void testStressReplaceWithInterleavedSkipsAndApplies() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);

            execute("create table stress (id long, ts timestamp, value long) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("stress");

            // Insert 100k rows spanning 1 day
            execute("insert into stress select x, cast('2022-02-24T00:00' as timestamp) + (x / 60) * 840000 * 60, x * 10 from long_sequence(100000)");
            drainWalQueue();

            // Verify initial state
            assertQuery("select count(*) from stress")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n100000\n");

            long baseHour = MicrosTimestampDriver.floor("2022-02-24T23:00");
            long minuteInMicros = 60 * 1_000_000L;

            // Create a pattern that forces: skip -> apply -> skip -> apply within a single batch
            // Pattern: A, A, B, C, C, D, A, C, E, F (where A-F are different time ranges)
            // Within this batch:
            // - Txns 0-1 (A,A) -> skipped (replaced by txn 6 which is the last A)
            // - Txn 2 (B) -> APPLIED (only B, not replaced by any later transaction)
            // - Txns 3-4 (C,C) -> skipped (replaced by txn 7 which is the last C)
            // - Txn 5 (D) -> APPLIED (only D, not replaced)
            // - Txn 6 (A) -> APPLIED (last A)
            // - Txn 7 (C) -> APPLIED (last C)
            // - Txn 8 (E) -> APPLIED (only E)
            // - Txn 9 (F) -> APPLIED (only F)
            // This creates: skip(0-1) -> APPLY(2) -> skip(3-4) -> APPLY(5) -> APPLY(6-9)
            // We use 6 different ranges (0-5) to ensure some are unique and must be applied
            int[] pattern = {0, 0, 1, 2, 2, 3, 0, 2, 4, 5}; // 0-5 represent 6 different time ranges
            long replaceValue = 1000L;
            long[] lastValues = new long[6];

            for (int batch = 0; batch < 10; batch++) {
                for (int i = 0; i < pattern.length; i++) {
                    try (WalWriter ww = engine.getWalWriter(tableToken)) {
                        int rangeSelector = pattern[i];
                        // Create 6 different 10-minute ranges to spread transactions across more ranges
                        long rangeStart = baseHour + rangeSelector * 10 * minuteInMicros; // 23:00, 23:10, 23:20, 23:30, 23:40, 23:50
                        long rangeEnd = rangeStart + 10 * minuteInMicros; // 10 minute ranges

                        // Insert a row within the replace range
                        long insertTs = rangeStart + 5 * minuteInMicros; // Middle of the range
                        TableWriter.Row row = ww.newRow(insertTs);
                        row.putLong(0, replaceValue);
                        row.putLong(2, replaceValue * 100);
                        row.append();

                        // Commit with replace range
                        ww.commitWithParams(rangeStart, rangeEnd, WAL_DEDUP_MODE_REPLACE_RANGE);
                        lastValues[rangeSelector] = replaceValue * 100;
                        replaceValue++;
                    }
                }
                // Drain after each complete pattern to allow skipping within the batch
                drainWalQueue();
            }

            // Verify we have exactly one row in each of the six 10-minute ranges
            assertQuery("select value, ts from stress where ts >= '2022-02-24T23:00'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("value\tts\n" +
                            lastValues[0] + "\t2022-02-24T23:05:00.000000Z\n" +
                            lastValues[1] + "\t2022-02-24T23:15:00.000000Z\n" +
                            lastValues[2] + "\t2022-02-24T23:25:00.000000Z\n" +
                            lastValues[3] + "\t2022-02-24T23:35:00.000000Z\n" +
                            lastValues[4] + "\t2022-02-24T23:45:00.000000Z\n" +
                            lastValues[5] + "\t2022-02-24T23:55:00.000000Z\n");
        });
    }

    private static void commitNoRowsWithRangeReplace(
            TableToken tableToken,
            String rangeStartStr,
            String rangeEndStr
    ) throws NumericException {
        try (WalWriter ww = engine.getWalWriter(tableToken)) {
            long rangeStart = MicrosTimestampDriver.floor(rangeStartStr);
            long rangeEnd = MicrosTimestampDriver.floor(rangeEndStr) + 1;
            ww.commitWithParams(rangeStart, rangeEnd, WAL_DEDUP_MODE_REPLACE_RANGE);
        }
    }

    private static void insertRowsWithRangeReplace(
            TableToken tableToken,
            Utf8StringSink sink,
            String tsStr,
            String rangeStartStr,
            String rangeEndStr,
            boolean commitWithRangeReplace
    ) throws NumericException {
        try (WalWriter ww = engine.getWalWriter(tableToken)) {
            if (tsStr != null) {
                int i = 0;
                String[] sybmols = new String[]{"w", "d", "a", "b", "c"};
                for (String tsStrPart : tsStr.split(",")) {
                    long ts = MicrosTimestampDriver.floor(tsStrPart);
                    TableWriter.Row row = ww.newRow(ts);
                    row.putInt(0, 100);
                    row.putLong(2, 1000);
                    row.putStr(3, "hello");
                    sink.clear();
                    sink.put("w");
                    row.putVarchar(4, sink);
                    row.putSym(5, sybmols[i % sybmols.length]);
                    row.append();
                }
            }

            if (commitWithRangeReplace) {
                long rangeStart = MicrosTimestampDriver.floor(rangeStartStr);
                long rangeEnd = MicrosTimestampDriver.floor(rangeEndStr) + 1;
                ww.commitWithParams(rangeStart, rangeEnd, WAL_DEDUP_MODE_REPLACE_RANGE);
            } else {
                ww.commit();
            }
        }
    }

    private void insertRowWithReplaceRange(
            String tsStr,
            String rangeStartStr,
            String rangeEndStr,
            TableToken tableToken,
            boolean compareTxns,
            boolean compareTruncateVersion,
            String tableName,
            String expectedTableName,
            boolean compareTxnDetails,
            boolean generateNoRowsCommit
    ) throws SqlException, NumericException {
        execute("create table " + expectedTableName + " as (select * from " + tableName + " where ts not between '" + rangeStartStr + "' and '" + rangeEndStr + "') timestamp(ts) partition by DAY WAL");

        Utf8StringSink sink = new Utf8StringSink();

        if (generateNoRowsCommit) {
            commitNoRowsWithRangeReplace(tableToken, rangeStartStr, rangeEndStr);
        }
        insertRowsWithRangeReplace(tableToken, sink, tsStr, rangeStartStr, rangeEndStr, true);
        drainWalQueue();

        var ttExpected = engine.verifyTableName(expectedTableName);
        insertRowsWithRangeReplace(ttExpected, sink, tsStr, rangeStartStr, rangeEndStr, false);
        drainWalQueue();

        Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(tableToken));

        if (compareTxnDetails) {
            Assert.assertEquals(
                    readTxnToString(ttExpected, compareTxns, compareTruncateVersion),
                    readTxnToString(tableToken, compareTxns, compareTruncateVersion)
            );
        }

        assertSqlCursors(expectedTableName, tableName);
        assertSqlCursors("select count(*), min(ts), max(ts) from " + expectedTableName, "select count(*), min(ts), max(ts) from " + tableName);
    }

    private void runReplaceCase(String tsStr, String rangeStartStr, String rangeEndStr, boolean compareTxns, boolean compareTruncateVersion, String tableName, String expectedTableName, boolean compareTxnDetails, boolean generateNoRowsCommit) throws SqlException, NumericException {
        execute("create table " + tableName + " (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
        TableToken tableToken = engine.verifyTableName(tableName);

        execute("insert into " + tableName + " select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(100)");
        drainWalQueue();
        insertRowWithReplaceRange(tsStr, rangeStartStr, rangeEndStr, tableToken, compareTxns, compareTruncateVersion, tableName, expectedTableName, compareTxnDetails, generateNoRowsCommit);
    }

    private void testRemovesFirstPartitionNoRowsAdded(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            insertRowWithReplaceRange(null, "2022-02-19T17", "2022-02-25T01", tableToken, false, true, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testRemovesLastPartitionNoRowsAdded(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(200)");
            drainWalQueue();

            insertRowWithReplaceRange(null, "2022-02-26", "2022-02-27", tableToken, false, true, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceCommitAdds2PartitionsBeforeExisting(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-20T17,2022-02-21T17", "2022-02-20T17", "2022-02-21T18", tableToken, !generateNoRowsCommit, !generateNoRowsCommit, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceCommitNotOrdered(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-21T17,2022-02-20T17", "2022-02-20T17", "2022-02-21T18", tableToken, !generateNoRowsCommit, !generateNoRowsCommit, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceCommitRemoves2PartitionsAndAdds1(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-26T17", "2022-02-24T17", "2022-02-28T02", tableToken, !generateNoRowsCommit, !generateNoRowsCommit, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T00:31', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(20)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-25T02:36:07.769840Z", "2022-02-24T23:20:30", "2022-02-27T01:34:56.265527", tableToken, !generateNoRowsCommit, !generateNoRowsCommit, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceRangeBeforeFirstPartitionAndData(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-20T17", "2022-02-19T17", "2022-02-21T18", tableToken, !generateNoRowsCommit, !generateNoRowsCommit, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceRangeCommit(String tsStr, String rangeStartStr, String rangeEndStr, boolean compareTxns, boolean compareTruncateVersion, boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            runReplaceCase(tsStr, rangeStartStr, rangeEndStr, compareTxns, compareTruncateVersion, "rg", "expected", true, generateNoRowsCommit);

            // Run the same test with aggressive partition split
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);

            runReplaceCase(tsStr, rangeStartStr, rangeEndStr, false, false, "rg_split", "expected_split", false, generateNoRowsCommit);
        });
    }

    private void testReplaceRangeCommit(String tsStr, String rangeStartStr, String rangeEndStr) throws Exception {
        testReplaceRangeCommit(tsStr, rangeStartStr, rangeEndStr, true, true, false);
    }

    private void testReplaceRangeLastPartition(boolean compareTruncateVersion, boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T00:31', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(20)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-24T17", "2022-02-19T17", "2022-02-28T18", tableToken, false, compareTruncateVersion, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceTruncatesAllData(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(200)");
            drainWalQueue();

            insertRowWithReplaceRange(null, "2022-02-20", "2022-02-27", tableToken, false, false, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceTruncatesAllDataAndAddsNewBeforeExisting(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(200)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-21,2022-02-21T01", "2022-02-21", "2022-02-27", tableToken, false, false, "rg", "expected", true, generateNoRowsCommit);
        });
    }
}
