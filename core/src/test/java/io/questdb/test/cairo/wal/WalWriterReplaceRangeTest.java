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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
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

import static io.questdb.PropertyKey.CAIRO_WAL_MAX_LAG_SIZE;
import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

public class WalWriterReplaceRangeTest extends AbstractCairoTest {

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
            assertSql("count\n1\n", "select count(*) from stress");
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

            assertSql("""
                    min\tmax\tcount
                    2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t47
                    """, "select min(ts), max(ts), count(*) from rg");

            Utf8StringSink sink = new Utf8StringSink();

            insertRowsWithRangeReplace(tableToken, sink, "2022-02-24T14:45", "2022-02-24T12:45", "2022-02-24T23", true);
            drainWalQueue();

            assertSql("""
                    min\tmax\tcount
                    2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t6
                    """, "select min(ts), max(ts), count(*) from rg");

            assertSql("""
                            id\tts\ty\ts\tv\tm
                            1\t2022-02-24T12:30:00.000000Z\t0\t1\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\ta
                            100\t2022-02-24T14:45:00.000000Z\t1000\thello\tw\tw
                            44\t2022-02-24T23:15:00.000000Z\t22\t44\t\uDAB1\uDC25J\uD969\uDF86gǢ\uDA97\uDEDC\t
                            45\t2022-02-24T23:30:00.000000Z\t22\t45\tHEZqUhE\ta
                            46\t2022-02-24T23:45:00.000000Z\t23\t46\tL^bE);P\tc
                            47\t2022-02-25T00:00:00.000000Z\t23\t47\t阇1(rոҊG\uD9A6\uDD42\uDB48\uDC78\tb
                            """,
                    "select * from rg"
            );


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

            assertSql("""
                    min\tmax\tcount
                    2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t47
                    """, "select min(ts), max(ts), count(*) from rg");

            Utf8StringSink sink = new Utf8StringSink();

            insertRowsWithRangeReplace(tableToken, sink, "2022-02-24T14:30", "2022-02-24", "2022-02-24T23", true);
            drainWalQueue();

            assertSql("""
                    min\tmax\tcount
                    2022-02-24T14:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t5
                    """, "select min(ts), max(ts), count(*) from rg");

            assertSql("""
                            id\tts\ty\ts\tv\tm
                            100\t2022-02-24T14:30:00.000000Z\t1000\thello\tw\tw
                            44\t2022-02-24T23:15:00.000000Z\t22\t44\t\uDAB1\uDC25J\uD969\uDF86gǢ\uDA97\uDEDC\t
                            45\t2022-02-24T23:30:00.000000Z\t22\t45\tHEZqUhE\ta
                            46\t2022-02-24T23:45:00.000000Z\t23\t46\tL^bE);P\tc
                            47\t2022-02-25T00:00:00.000000Z\t23\t47\t阇1(rոҊG\uD9A6\uDD42\uDB48\uDC78\tb
                            """,
                    "select * from rg"
            );


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

            assertSql("""
                    min\tmax\tcount
                    2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t47
                    """, "select min(ts), max(ts), count(*) from rg");

            Utf8StringSink sink = new Utf8StringSink();

            insertRowsWithRangeReplace(tableToken, sink, "2022-02-24T14:45", "2022-02-24T12:45", "2022-02-24T23:59:59.999999", true);
            drainWalQueue();

            assertSql("""
                    min\tmax\tcount
                    2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t3
                    """, "select min(ts), max(ts), count(*) from rg");

            assertSql("""
                            id\tts\ty\ts\tv\tm
                            1\t2022-02-24T12:30:00.000000Z\t0\t1\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\ta
                            100\t2022-02-24T14:45:00.000000Z\t1000\thello\tw\tw
                            47\t2022-02-25T00:00:00.000000Z\t23\t47\t阇1(rոҊG\uD9A6\uDD42\uDB48\uDC78\tb
                            """,
                    "select * from rg"
            );

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
            assertSql("""
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
                            """,
                    "select * from rg limit 10");
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
    public void testStressReplaceLastMinuteRepeatedly() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);

            execute("create table stress (id long, ts timestamp, value long) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("stress");

            // Insert 100k rows spanning 1 day, so that partition is reasonably big to rewrite
            execute("insert into stress select x, cast('2022-02-24T00:00' as timestamp) + (x / 60) * 840000 * 60, x * 10 from long_sequence(100000)");
            drainWalQueue();

            // Verify initial state
            assertSql("count\n100000\n", "select count(*) from stress");

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
            assertSql("count\n100001\n", "select count(*) from stress");

            // Check the results of the last minute
            assertSql("count\n1\n", "select count(*) from stress where ts >= '2022-02-24T23:59' and ts < '2022-02-25T00:00'");

            // Verify the value is from the last replace operation
            assertSql("value\n" + lastValue + "\n", "select value from stress where ts >= '2022-02-24T23:59' and ts < '2022-02-25T00:00'");
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
            assertSql("count\n100000\n", "select count(*) from stress");

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

            assertSql("value\tts\n" +
                            lastValues[0] + "\t2022-02-24T23:10:00.000000Z\n" +
                            lastValues[1] + "\t2022-02-24T23:30:00.000000Z\n" +
                            lastValues[2] + "\t2022-02-24T23:50:00.000000Z\n",
                    "select value, ts from stress where ts >= '2022-02-24T23:00'"
            );
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
            assertSql("count\n100000\n", "select count(*) from stress");

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
            assertSql("value\tts\n" +
                            lastValues[0] + "\t2022-02-24T23:05:00.000000Z\n" +
                            lastValues[1] + "\t2022-02-24T23:15:00.000000Z\n" +
                            lastValues[2] + "\t2022-02-24T23:25:00.000000Z\n" +
                            lastValues[3] + "\t2022-02-24T23:35:00.000000Z\n" +
                            lastValues[4] + "\t2022-02-24T23:45:00.000000Z\n" +
                            lastValues[5] + "\t2022-02-24T23:55:00.000000Z\n",
                    "select value, ts from stress where ts >= '2022-02-24T23:00'"
            );
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
