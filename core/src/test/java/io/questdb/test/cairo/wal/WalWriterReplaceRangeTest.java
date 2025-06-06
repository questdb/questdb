/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

public class WalWriterReplaceRangeTest extends AbstractCairoTest {
    @Test
    public void testRemovesFirstPartitionNoRowsAdded() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            insertRowWithReplaceRange(null, "2022-02-19T17", "2022-02-25T01", tableToken, false, true, "rg", "expected", true);
        });
    }

    @Test
    public void testRemovesLastPartitionNoRowsAdded() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(200)");
            drainWalQueue();

            insertRowWithReplaceRange(null, "2022-02-26", "2022-02-27", tableToken, false, true, "rg", "expected", true);
        });
    }

    @Test
    public void testReplaceBetweenExisting() throws Exception {
        testReplaceRangeCommit("2022-02-24T16:25", "2022-02-24T16:25", "2022-02-24T16:26");
    }

    @Test
    public void testReplaceCommitAdds2PartitionsBeforeExisting() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-20T17,2022-02-21T17", "2022-02-20T17", "2022-02-21T18", tableToken, true, true, "rg", "expected", true);
        });
    }

    @Test
    public void testReplaceCommitNotOrdered() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-21T17,2022-02-20T17", "2022-02-20T17", "2022-02-21T18", tableToken, true, true, "rg", "expected", true);
        });
    }

    @Test
    public void testReplaceCommitRemoves2PartitionsAndAdds1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-26T17", "2022-02-24T17", "2022-02-28T02", tableToken, true, true, "rg", "expected", true);
        });
    }

    @Test
    public void testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T00:31', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(20)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-25T02:36:07.769840Z", "2022-02-24T23:20:30", "2022-02-27T01:34:56.265527", tableToken, true, true, "rg", "expected", true);
        });
    }

    @Test
    public void testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T21:31', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(20)");
            execute("insert into rg select x, timestamp_sequence('2022-02-28T21:31', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(20)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-25T02:36:07.769840Z", "2022-02-24T23:20:30", "2022-02-27T01:34:56.265527", tableToken, false, true, "rg", "expected", true);
        });
    }

    @Test
    public void testReplaceRangeBeforeFirstPartitionAndData() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-20T17", "2022-02-19T17", "2022-02-21T18", tableToken, true, true, "rg", "expected", true);
        });
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
        testReplaceRangeCommit(null, "2022-02-24T18:30", "2022-02-24T20:59:31", false, false);
    }

    @Test
    public void testReplaceRangeFirstPartitionNoImpact() throws Exception {
        testReplaceRangeCommit(null, "2022-02-24T23:59:30", "2022-02-24T23:59:31", false, false);
    }

    @Test
    public void testReplaceRangeLastPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T00:31', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(20)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-24T17", "2022-02-19T17", "2022-02-28T18", tableToken, false, true, "rg", "expected", true);
        });
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
    public void testReplaceRangeLastPartitionPrefix() throws Exception {
        testReplaceRangeCommit("2022-02-25T00:53", "2022-02-25T00:00", "2022-02-25T04");
    }

    @Test
    public void testReplaceRangeLastPartitionSuffix() throws Exception {
        testReplaceRangeCommit("2022-02-25T02:53", "2022-02-25T01:00", "2022-02-25T23");
    }

    @Test
    public void testReplaceRangeSingleCommitMiddleMerge() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(47)");

            drainWalQueue();

            assertSql("min\tmax\tcount\n" +
                    "2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t47\n", "select min(ts), max(ts), count(*) from rg");

            Utf8StringSink sink = new Utf8StringSink();

            insertRowsWithRangeReplace(tableToken, sink, "2022-02-24T14:45", "2022-02-24T12:45", "2022-02-24T23", true);
            drainWalQueue();

            assertSql("min\tmax\tcount\n" +
                    "2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t6\n", "select min(ts), max(ts), count(*) from rg");

            assertSql("id\tts\ty\ts\tv\tm\n" +
                            "1\t2022-02-24T12:30:00.000000Z\t0\t1\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\ta\n" +
                            "100\t2022-02-24T14:45:00.000000Z\t1000\thello\tw\tw\n" +
                            "44\t2022-02-24T23:15:00.000000Z\t22\t44\t\uDAB1\uDC25J\uD969\uDF86gǢ\uDA97\uDEDC\t\n" +
                            "45\t2022-02-24T23:30:00.000000Z\t22\t45\tHEZqUhE\ta\n" +
                            "46\t2022-02-24T23:45:00.000000Z\t23\t46\tL^bE);P\tc\n" +
                            "47\t2022-02-25T00:00:00.000000Z\t23\t47\t阇1(rոҊG\uD9A6\uDD42\uDB48\uDC78\tb\n",
                    "select * from rg"
            );


            // Check _txn file makes sense, min, max timestamps, row counts etc.
            Assert.assertEquals(
                    "{txn: 2, attachedPartitions: [\n" +
                            "{ts: '2022-02-24T00:00:00.000Z', rowCount: 5},\n" +
                            "{ts: '2022-02-25T00:00:00.000Z', rowCount: 1}\n" +
                            "], transientRowCount: 1, fixedRowCount: 5, " +
                            "minTimestamp: '2022-02-24T12:30:00.000Z', " +
                            "maxTimestamp: '2022-02-25T00:00:00.000Z', " +
                            "dataVersion: 0, structureVersion: 0, " +
                            "columnVersion: 0, truncateVersion: 0, seqTxn: 2, " +
                            "symbolColumnCount: 1, lagRowCount: 0, " +
                            "lagMinTimestamp: '294247-01-10T04:00:54.775Z', " +
                            "lagMaxTimestamp: '', lagTxnCount: 0, lagOrdered: true}",
                    readTxnToSTring(tableToken, true, true)
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

            assertSql("min\tmax\tcount\n" +
                    "2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t47\n", "select min(ts), max(ts), count(*) from rg");

            Utf8StringSink sink = new Utf8StringSink();

            insertRowsWithRangeReplace(tableToken, sink, "2022-02-24T14:30", "2022-02-24", "2022-02-24T23", true);
            drainWalQueue();

            assertSql("min\tmax\tcount\n" +
                    "2022-02-24T14:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t5\n", "select min(ts), max(ts), count(*) from rg");

            assertSql("id\tts\ty\ts\tv\tm\n" +
                            "100\t2022-02-24T14:30:00.000000Z\t1000\thello\tw\tw\n" +
                            "44\t2022-02-24T23:15:00.000000Z\t22\t44\t\uDAB1\uDC25J\uD969\uDF86gǢ\uDA97\uDEDC\t\n" +
                            "45\t2022-02-24T23:30:00.000000Z\t22\t45\tHEZqUhE\ta\n" +
                            "46\t2022-02-24T23:45:00.000000Z\t23\t46\tL^bE);P\tc\n" +
                            "47\t2022-02-25T00:00:00.000000Z\t23\t47\t阇1(rոҊG\uD9A6\uDD42\uDB48\uDC78\tb\n",
                    "select * from rg"
            );


            // Check _txn file makes sense, min, max timestamps, row counts etc.
            Assert.assertEquals(
                    "{txn: 2, attachedPartitions: [\n" +
                            "{ts: '2022-02-24T00:00:00.000Z', rowCount: 4},\n" +
                            "{ts: '2022-02-25T00:00:00.000Z', rowCount: 1}\n" +
                            "], transientRowCount: 1, fixedRowCount: 4, " +
                            "minTimestamp: '2022-02-24T14:30:00.000Z', " +
                            "maxTimestamp: '2022-02-25T00:00:00.000Z', " +
                            "dataVersion: 0, structureVersion: 0, " +
                            "columnVersion: 0, truncateVersion: 0, seqTxn: 2, " +
                            "symbolColumnCount: 1, lagRowCount: 0, " +
                            "lagMinTimestamp: '294247-01-10T04:00:54.775Z', " +
                            "lagMaxTimestamp: '', lagTxnCount: 0, lagOrdered: true}",
                    readTxnToSTring(tableToken, true, true)
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

            assertSql("min\tmax\tcount\n" +
                    "2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t47\n", "select min(ts), max(ts), count(*) from rg");

            Utf8StringSink sink = new Utf8StringSink();

            insertRowsWithRangeReplace(tableToken, sink, "2022-02-24T14:45", "2022-02-24T12:45", "2022-02-24T23:59:59.999999", true);
            drainWalQueue();

            assertSql("min\tmax\tcount\n" +
                    "2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t3\n", "select min(ts), max(ts), count(*) from rg");

            assertSql("id\tts\ty\ts\tv\tm\n" +
                            "1\t2022-02-24T12:30:00.000000Z\t0\t1\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\ta\n" +
                            "100\t2022-02-24T14:45:00.000000Z\t1000\thello\tw\tw\n" +
                            "47\t2022-02-25T00:00:00.000000Z\t23\t47\t阇1(rոҊG\uD9A6\uDD42\uDB48\uDC78\tb\n",
                    "select * from rg"
            );

            // Check _txn file makes sense, min, max timestamps, row counts etc.
            Assert.assertEquals(
                    "{txn: 2, attachedPartitions: [\n" +
                            "{ts: '2022-02-24T00:00:00.000Z', rowCount: 2},\n" +
                            "{ts: '2022-02-25T00:00:00.000Z', rowCount: 1}\n" +
                            "], transientRowCount: 1, fixedRowCount: 2, " +
                            "minTimestamp: '2022-02-24T12:30:00.000Z', " +
                            "maxTimestamp: '2022-02-25T00:00:00.000Z', " +
                            "dataVersion: 0, structureVersion: 0, " +
                            "columnVersion: 0, truncateVersion: 0, seqTxn: 2, " +
                            "symbolColumnCount: 1, lagRowCount: 0, " +
                            "lagMinTimestamp: '294247-01-10T04:00:54.775Z', " +
                            "lagMaxTimestamp: '', lagTxnCount: 0, lagOrdered: true}",
                    readTxnToSTring(tableToken, true, true)
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
    public void testReplaceTruncatesAllData() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(200)");
            drainWalQueue();

            insertRowWithReplaceRange(null, "2022-02-20", "2022-02-27", tableToken, false, false, "rg", "expected", true);
        });
    }

    @Test
    public void testReplaceTruncatesAllDataAndAddsNewBeforeExisting() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(200)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-21,2022-02-21T01", "2022-02-21", "2022-02-27", tableToken, false, false, "rg", "expected", true);
        });
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
                    long ts = IntervalUtils.parseFloorPartialTimestamp(tsStrPart);
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
                long rangeStart = IntervalUtils.parseFloorPartialTimestamp(rangeStartStr);
                long rangeEnd = IntervalUtils.parseFloorPartialTimestamp(rangeEndStr) + 1;
                ww.commitWithParams(rangeStart, rangeEnd, WAL_DEDUP_MODE_REPLACE_RANGE);
            } else {
                ww.commit();
            }
        }
    }

    private static String readTxnToSTring(TableToken tt, boolean compareTxns, boolean compareTruncateVersion) {
        try (TxReader rdr = new TxReader(engine.getConfiguration().getFilesFacade())) {
            Path tempPath = Path.getThreadLocal(root);
            rdr.ofRO(tempPath.concat(tt).concat(TableUtils.TXN_FILE_NAME).$(), PartitionBy.DAY);
            rdr.unsafeLoadAll();

            return txnToString(rdr, compareTxns, compareTruncateVersion);
        }
    }

    private static String txnToString(TxReader txReader, boolean compareTxns, boolean compareTruncateVersion) {
        // Used for debugging, don't use Misc.getThreadLocalSink() to not mess with other debugging values
        StringSink sink = Misc.getThreadLocalSink();
        sink.put("{");
        if (compareTxns) {
            sink.put("txn: ").put(txReader.getTxn());
        }
        sink.put(", attachedPartitions: [");
        for (int i = 0; i < txReader.getPartitionCount(); i++) {
            long timestamp = txReader.getPartitionTimestampByIndex(i);
            long rowCount = txReader.getPartitionRowCountByTimestamp(timestamp);

            if (i - 1 == txReader.getPartitionCount()) {
                rowCount = txReader.getTransientRowCount();
            }

            long parquetSize = txReader.getPartitionParquetFileSize(i);

            if (i > 0) {
                sink.put(",");
            }
            sink.put("\n{ts: '");
            TimestampFormatUtils.appendDateTime(sink, timestamp);
            sink.put("', rowCount: ").put(rowCount);
            // Do not print name txn, it can be different in expected and actual table

            if (txReader.isPartitionParquet(i)) {
                sink.put(", parquetSize: ").put(parquetSize);
            }
            if (txReader.isPartitionReadOnly(i)) {
                sink.put(", readOnly=true");
            }
            sink.put("}");
        }
        sink.put("\n], transientRowCount: ").put(txReader.getTransientRowCount());
        sink.put(", fixedRowCount: ").put(txReader.getFixedRowCount());
        sink.put(", minTimestamp: '");
        TimestampFormatUtils.appendDateTime(sink, txReader.getMinTimestamp());
        sink.put("', maxTimestamp: '");
        TimestampFormatUtils.appendDateTime(sink, txReader.getMaxTimestamp());
        if (compareTruncateVersion) {
            sink.put("', dataVersion: ").put(txReader.getDataVersion());
        }
        sink.put(", structureVersion: ").put(txReader.getColumnStructureVersion());
        sink.put(", columnVersion: ").put(txReader.getColumnVersion());
        if (compareTruncateVersion) {
            sink.put(", truncateVersion: ").put(txReader.getTruncateVersion());
        }

        if (compareTxns) {
            sink.put(", seqTxn: ").put(txReader.getSeqTxn());
        }
        sink.put(", symbolColumnCount: ").put(txReader.getSymbolColumnCount());
        sink.put(", lagRowCount: ").put(txReader.getLagRowCount());
        sink.put(", lagMinTimestamp: '");
        TimestampFormatUtils.appendDateTime(sink, txReader.getLagMinTimestamp());
        sink.put("', lagMaxTimestamp: '");
        TimestampFormatUtils.appendDateTime(sink, txReader.getLagMaxTimestamp());
        sink.put("', lagTxnCount: ").put(txReader.getLagRowCount());
        sink.put(", lagOrdered: ").put(txReader.isLagOrdered());
        sink.put("}");
        return sink.toString();
    }

    private void insertRowWithReplaceRange(String tsStr, String rangeStartStr, String rangeEndStr, TableToken tableToken, boolean compareTxns, boolean compareTruncateVersion, String tableName, String expectedTableName, boolean compareTxnDetails) throws SqlException, NumericException {
        execute("create table " + expectedTableName + " as (select * from " + tableName + " where ts not between '" + rangeStartStr + "' and '" + rangeEndStr + "') timestamp(ts) partition by DAY WAL");

        Utf8StringSink sink = new Utf8StringSink();

        insertRowsWithRangeReplace(tableToken, sink, tsStr, rangeStartStr, rangeEndStr, true);
        drainWalQueue();

        var ttExpected = engine.verifyTableName(expectedTableName);
        insertRowsWithRangeReplace(ttExpected, sink, tsStr, rangeStartStr, rangeEndStr, false);
        drainWalQueue();

        Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(tableToken));

        if (compareTxnDetails) {
            Assert.assertEquals(
                    readTxnToSTring(ttExpected, compareTxns, compareTruncateVersion),
                    readTxnToSTring(tableToken, compareTxns, compareTruncateVersion)
            );
        }

        assertSqlCursors(expectedTableName, tableName);
        assertSqlCursors("select count(*), min(ts), max(ts) from " + expectedTableName, "select count(*), min(ts), max(ts) from " + tableName);
    }

    private void runReplaceCase(String tsStr, String rangeStartStr, String rangeEndStr, boolean compareTxns, boolean compareTruncateVersion, String tableName, String expectedTableName, boolean compareTxnDetails) throws SqlException, NumericException {
        execute("create table " + tableName + " (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
        TableToken tableToken = engine.verifyTableName(tableName);

        execute("insert into " + tableName + " select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(100)");
        drainWalQueue();
        insertRowWithReplaceRange(tsStr, rangeStartStr, rangeEndStr, tableToken, compareTxns, compareTruncateVersion, tableName, expectedTableName, compareTxnDetails);
    }

    private void testReplaceRangeCommit(String tsStr, String rangeStartStr, String rangeEndStr, boolean compareTxns, boolean compareTruncateVersion) throws Exception {
        assertMemoryLeak(() -> {
            runReplaceCase(tsStr, rangeStartStr, rangeEndStr, compareTxns, compareTruncateVersion, "rg", "expected", true);

            // Run the same test with aggressive partition split
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);

            runReplaceCase(tsStr, rangeStartStr, rangeEndStr, false, false, "rg_split", "expected_split", false);
        });
    }

    private void testReplaceRangeCommit(String tsStr, String rangeStartStr, String rangeEndStr) throws Exception {
        testReplaceRangeCommit(tsStr, rangeStartStr, rangeEndStr, true, true);
    }
}
