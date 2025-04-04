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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.wal.SymbolMapDiff;
import io.questdb.cairo.wal.SymbolMapDiffEntry;
import io.questdb.cairo.wal.WalDataRecord;
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.DirectBinarySequence;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.questdb.cairo.wal.WalUtils.*;
import static org.junit.Assert.*;

public class WalWriterReplaceRangeTest extends AbstractCairoTest {
    @Test
    public void testReplaceRangeLastPartitionAppend() throws Exception {
        testReplaceRangeCommit("2022-02-25T16:25", "2022-02-25T16:00", "2022-02-25T17:00");
    }

    @Test
    public void testReplaceBetweenExisting() throws Exception {
        testReplaceRangeCommit("2022-02-24T16:25", "2022-02-24T16:25", "2022-02-24T16:26");
    }

    @Test
    public void testReplaceRangeTwoPartitions() throws Exception {
        testReplaceRangeCommit("2022-02-24T16:25", "2022-02-24T16:25", "2022-02-25T01:00");
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

            insertOneRowWithRangeReplace(tableToken, sink, "2022-02-24T14:45", "2022-02-24T12:45", "2022-02-24T23", true);
            drainWalQueue();

            assertSql("min\tmax\tcount\n" +
                    "2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t6\n", "select min(ts), max(ts), count(*) from rg");

            assertSql("id\tts\ty\ts\tv\tm\n" +
                            "1\t2022-02-24T12:30:00.000000Z\t0\t1\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\ta\n" +
                            "100\t2022-02-24T14:45:00.000000Z\t1000\thello\tw\ta\n" +
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
                    readTxnToSTring(tableToken)
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

            insertOneRowWithRangeReplace(tableToken, sink, "2022-02-24T14:30", "2022-02-24", "2022-02-24T23", true);
            drainWalQueue();

            assertSql("min\tmax\tcount\n" +
                    "2022-02-24T14:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t5\n", "select min(ts), max(ts), count(*) from rg");

            assertSql("id\tts\ty\ts\tv\tm\n" +
                            "100\t2022-02-24T14:30:00.000000Z\t1000\thello\tw\ta\n" +
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
                            "minTimestamp: '2022-02-24T12:30:00.000Z', " +
                            "maxTimestamp: '2022-02-25T00:00:00.000Z', " +
                            "dataVersion: 0, structureVersion: 0, " +
                            "columnVersion: 0, truncateVersion: 0, seqTxn: 2, " +
                            "symbolColumnCount: 1, lagRowCount: 0, " +
                            "lagMinTimestamp: '294247-01-10T04:00:54.775Z', " +
                            "lagMaxTimestamp: '', lagTxnCount: 0, lagOrdered: true}",
                    readTxnToSTring(tableToken)
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

            insertOneRowWithRangeReplace(tableToken, sink, "2022-02-24T14:45", "2022-02-24T12:45", "2022-02-24T23:59:59.999999", true);
            drainWalQueue();

            assertSql("min\tmax\tcount\n" +
                    "2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t3\n", "select min(ts), max(ts), count(*) from rg");

            assertSql("id\tts\ty\ts\tv\tm\n" +
                            "1\t2022-02-24T12:30:00.000000Z\t0\t1\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\ta\n" +
                            "100\t2022-02-24T14:45:00.000000Z\t1000\thello\tw\ta\n" +
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
                    readTxnToSTring(tableToken)
            );
        });
    }

    private static void insertOneRowWithRangeReplace(
            TableToken tableToken,
            Utf8StringSink sink,
            String tsStr,
            String rangeStartStr,
            String rangeEndStr,
            boolean commitWithRangeReplace
    ) throws NumericException {
        try (WalWriter ww = engine.getWalWriter(tableToken)) {
            long ts = IntervalUtils.parseFloorPartialTimestamp(tsStr);
            TableWriter.Row row = ww.newRow(ts);
            row.putInt(0, 100);
            row.putLong(2, 1000);
            row.putStr(3, "hello");
            sink.clear();
            sink.put("w");
            row.putVarchar(4, sink);
            row.putSym(5, "a");
            row.append();

            if (commitWithRangeReplace) {
                long rangeStart = IntervalUtils.parseFloorPartialTimestamp(rangeStartStr);
                long rangeEnd = IntervalUtils.parseFloorPartialTimestamp(rangeEndStr);
                ww.commitWitParams(rangeStart, rangeEnd, WAL_DEDUP_MODE_REPLACE_RANGE);
            } else {
                ww.commit();
            }
        }
    }

    private static String readTxnToSTring(TableToken tt) {
        try (TxReader rdr = new TxReader(engine.getConfiguration().getFilesFacade())) {
            Path tempPath = Path.getThreadLocal(root);
            rdr.ofRO(tempPath.concat(tt).concat(TableUtils.TXN_FILE_NAME).$(), PartitionBy.DAY);
            rdr.unsafeLoadAll();

            return txnToString(rdr);
        }
    }

    private static String txnToString(TxReader txReader) {
        // Used for debugging, don't use Misc.getThreadLocalSink() to not mess with other debugging values
        StringSink sink = Misc.getThreadLocalSink();
        sink.put("{");
        sink.put("txn: ").put(txReader.getTxn());
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
        sink.put("', dataVersion: ").put(txReader.getDataVersion());
        sink.put(", structureVersion: ").put(txReader.getColumnStructureVersion());
        sink.put(", columnVersion: ").put(txReader.getColumnVersion());
        sink.put(", truncateVersion: ").put(txReader.getTruncateVersion());
        sink.put(", seqTxn: ").put(txReader.getSeqTxn());
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

    private void testReplaceRangeCommit(String tsStr, String rangeStartStr, String rangeEndStr) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(100)");
            drainWalQueue();

            execute("create table expected as (select * from rg where ts not between '" + rangeStartStr + "' and '" + rangeEndStr + "') timestamp(ts) partition by DAY WAL");

            assertSql("min\tmax\tcount\n" +
                    "2022-02-24T12:30:00.000000Z\t2022-02-25T13:15:00.000000Z\t100\n", "select min(ts), max(ts), count(*) from rg");

            Utf8StringSink sink = new Utf8StringSink();
            insertOneRowWithRangeReplace(tableToken, sink, tsStr, rangeStartStr, rangeEndStr, true);
            drainWalQueue();

            var ttExpected = engine.verifyTableName("expected");
            insertOneRowWithRangeReplace(ttExpected, sink, tsStr, rangeStartStr, rangeEndStr, false);
            drainWalQueue();

            assertSqlCursors("expected", "rg");
            assertSqlCursors("select count(*), min(ts), max(ts) from expected", "select count(*), min(ts), max(ts) from rg");

            Assert.assertEquals(
                    readTxnToSTring(ttExpected),
                    readTxnToSTring(tableToken)
            );
        });
    }

}
