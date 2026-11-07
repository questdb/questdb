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

package io.questdb.test.cairo.parquet;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.o3.AbstractO3Test;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionGenerator;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class O3ParquetPartitionFuzzTest extends AbstractO3Test {
    @Test
    public void testParquetWriteFuzz() throws Exception {
        // native y never splits a partition: a split leaves the prefix piece with its column tops, where a
        // merge that rewrites the partition, as the parquet merge of x does, ends a top past the merge point
        partitionO3SplitThreshold = Long.MAX_VALUE;
        executeWithPool(0, this::testFuzz0);
    }

    // Adds ab BOOLEAN, al BYTE, af SHORT and at CHAR to x and y, and commits 1..3 rows with non-zero values in
    // them into one row group of x's parquet partition, at or after addColumnTs where one allows it. Returns the
    // row groups of the rewritten partition that hold none of these rows and whose al chunk reports no null: the
    // rewrite copied them with the ended top of al as 0, not as NULL
    private static int endTopsOfColumnsAddedAfterConversion(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            Rnd rnd,
            long partitionTs,
            long addColumnTs
    ) throws SqlException {
        final LongList o3Timestamps = new LongList();
        try (TableReader reader = engine.getReader("x")) {
            final int partitionIndex = reader.getPartitionIndexByTimestamp(partitionTs);
            reader.openPartition(partitionIndex);
            final ParquetMetaFileReader meta = reader.getAndInitParquetPartitionDecoder(partitionIndex).metadata();
            final int tsIndex = meta.getColumnIndex("ts");
            final LongList candidateRanges = new LongList();
            for (long bound : new long[]{addColumnTs, Long.MIN_VALUE}) {
                for (int rowGroup = 0, n = meta.getRowGroupCount(); rowGroup < n; rowGroup++) {
                    final long lo = Math.max(bound, meta.getRowGroupMinTimestamp(rowGroup, tsIndex));
                    final long hi = meta.getRowGroupMaxTimestamp(rowGroup, tsIndex);
                    if (lo < hi) {
                        candidateRanges.add(lo, hi);
                    }
                }
                if (candidateRanges.size() > 0) {
                    break;
                }
            }
            Assert.assertTrue("parquet partition has no row group to merge into", candidateRanges.size() > 0);
            final int range = rnd.nextInt(candidateRanges.size() / 2);
            final long lo = candidateRanges.getQuick(2 * range);
            final long hi = candidateRanges.getQuick(2 * range + 1);
            for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                // strictly before the last row of the row group: the merge never appends
                o3Timestamps.add(lo + rnd.nextLong(hi - lo));
            }
            LOG.info().$("O3 rows into a row group [lo=").$(lo).$(", hi=").$(hi).$(", count=").$(o3Timestamps.size()).I$();
        }
        final byte[] byteValues = new byte[o3Timestamps.size()];
        final short[] shortValues = new short[o3Timestamps.size()];
        final char[] charValues = new char[o3Timestamps.size()];
        for (int i = 0, n = o3Timestamps.size(); i < n; i++) {
            byteValues[i] = (byte) (1 + rnd.nextInt(100));
            shortValues[i] = (short) (1 + rnd.nextInt(1000));
            charValues[i] = (char) ('A' + rnd.nextInt(26));
        }
        for (String table : new String[]{"x", "y"}) {
            engine.execute("alter table " + table + " add column ab boolean, al byte, af short, at char", sqlExecutionContext);
            try (TableWriter w = TestUtils.getWriter(engine, table)) {
                final int iIndex = w.getMetadata().getColumnIndex("i");
                final int abIndex = w.getMetadata().getColumnIndex("ab");
                final int alIndex = w.getMetadata().getColumnIndex("al");
                final int afIndex = w.getMetadata().getColumnIndex("af");
                final int atIndex = w.getMetadata().getColumnIndex("at");
                for (int i = 0, n = o3Timestamps.size(); i < n; i++) {
                    TableWriter.Row row = w.newRow(o3Timestamps.getQuick(i));
                    row.putInt(iIndex, 3_000_000 + i);
                    row.putBool(abIndex, true);
                    row.putByte(alIndex, byteValues[i]);
                    row.putShort(afIndex, shortValues[i]);
                    row.putChar(atIndex, charValues[i]);
                    row.append();
                }
                w.commit();
            }
        }
        int copiedZeroRowGroupCount = 0;
        try (TableReader reader = engine.getReader("x")) {
            final int partitionIndex = reader.getPartitionIndexByTimestamp(partitionTs);
            reader.openPartition(partitionIndex);
            final ParquetMetaFileReader meta = reader.getAndInitParquetPartitionDecoder(partitionIndex).metadata();
            final int tsIndex = meta.getColumnIndex("ts");
            final int alIndex = meta.getColumnIndex("al");
            Assert.assertTrue("the rewrite did not add al to the parquet file", alIndex >= 0);
            for (int rowGroup = 0, n = meta.getRowGroupCount(); rowGroup < n; rowGroup++) {
                final long min = meta.getRowGroupMinTimestamp(rowGroup, tsIndex);
                final long max = meta.getRowGroupMaxTimestamp(rowGroup, tsIndex);
                boolean hasO3Row = false;
                for (int i = 0, k = o3Timestamps.size(); i < k; i++) {
                    final long ts = o3Timestamps.getQuick(i);
                    hasO3Row |= ts >= min && ts <= max;
                }
                if (!hasO3Row && meta.hasChunkNullCount(rowGroup, alIndex) && meta.getChunkNullCount(rowGroup, alIndex) == 0) {
                    copiedZeroRowGroupCount++;
                }
            }
        }
        return copiedZeroRowGroupCount;
    }

    private static void replayTransactions(Rnd rnd, CairoEngine engine, TableWriter w, ObjList<FuzzTransaction> transactions) {
        for (int i = 0, n = transactions.size(); i < n; i++) {
            FuzzTransaction tx = transactions.getQuick(i);
            ObjList<FuzzTransactionOperation> ops = tx.operationList;
            for (int j = 0, k = ops.size(); j < k; j++) {
                ops.getQuick(j).apply(rnd, engine, w, -1, null);
            }
            w.ic();
        }
    }

    private static void testFuzz00(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName,
            Rnd rnd
    ) throws SqlException {
        long microsBetweenRows = 1000000L;
        int nTotalRows = 120_000;
        // the rows of x, with BOOLEAN, BYTE, SHORT and CHAR columns that x gets later, see below
        String sql = "create table src as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 0) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L, " + microsBetweenRows + "L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t," +
                " rnd_varchar(6, 16, 2) v1," +
                " rnd_varchar(1, 1, 1) v2," +
                " rnd_boolean() nb," +
                " rnd_byte() nl," +
                " rnd_short() nf," +
                " rnd_char() nt," +
                " from long_sequence(" + nTotalRows + ")" +
                ") timestamp (ts) partition by HOUR";
        engine.execute(sql, sqlExecutionContext);

        final int partitionIndex;
        final long partitionTs;
        StringSink stringSink = new StringSink();
        TimestampDriver timestampDriver;
        try (TableReader srcReader = engine.getReader("src")) {
            timestampDriver = ColumnType.getTimestampDriver(srcReader.getMetadata().getTimestampType());
            partitionIndex = rnd.nextInt(srcReader.getPartitionCount() - 1);
            partitionTs = srcReader.getPartitionTimestampByIndex(partitionIndex);
            PartitionBy.setSinkForPartition(stringSink, srcReader.getMetadata().getTimestampType(), srcReader.getPartitionedBy(), partitionTs);
        }
        // x and y add the no-sentinel columns at a random row of the partition that x converts to
        // parquet: the columns have a column top inside it
        final long addColumnTs = partitionTs + timestampDriver.fromMicros(rnd.nextInt(3600) * microsBetweenRows);
        LOG.info().$("no-sentinel columns added at [ts=").$(addColumnTs).I$();
        final String columnsBeforeAdd = "i, sym, amt, timestamp, b, c, d, e, f, g, ik, j, ts, l, m, n, t, v1, v2";
        for (String table : new String[]{"x", "y"}) {
            engine.execute("create table " + table + " as (select " + columnsBeforeAdd + " from src where ts < " + addColumnTs
                    + ") timestamp (ts) partition by HOUR", sqlExecutionContext);
            engine.execute("alter table " + table + " add column nb boolean, nl byte, nf short, nt char", sqlExecutionContext);
            engine.execute("insert into " + table + " select * from src where ts >= " + addColumnTs, sqlExecutionContext);
        }
        TestUtils.assertEquals(compiler, sqlExecutionContext, "y", "x");
        engine.execute("alter table x convert partition to parquet list '" + stringSink + "'", sqlExecutionContext);

        long minTs = partitionTs - timestampDriver.fromMicros(3600000000L);
        long maxTs = partitionTs + timestampDriver.fromMicros(2 * 3600000000L);

        int txCount = Math.max(1, rnd.nextInt(10));
        int rowCount = Math.max(1, txCount * rnd.nextInt(20) * 100);
        try (
                TableWriter xw = TestUtils.getWriter(engine, "x");
                TableRecordMetadata sequencerMetadata = engine.getLegacyMetadata(xw.getTableToken());
                TableWriter yw = TestUtils.getWriter(engine, "y")
        ) {

            ObjList<FuzzTransaction> transactions = FuzzTransactionGenerator.generateSet(
                    nTotalRows,
                    sequencerMetadata,
                    xw.getMetadata(),
                    rnd,
                    minTs,
                    maxTs,
                    rowCount,
                    txCount,
                    true,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    rnd.nextDouble(),
                    0,
                    0.0,
                    0.0,
                    0.0,
                    0,
                    0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    5,
                    new String[]{"ABC", "CDE", "XYZ"},
                    0,
                    0.3,
                    0.0, 0.0
            );

            try {
                Rnd rnd2 = new Rnd(rnd.nextLong(), rnd.nextLong());
                replayTransactions(rnd2, engine, yw, transactions);
                yw.commit();

                Rnd rnd1 = new Rnd(rnd.nextLong(), rnd.nextLong());
                replayTransactions(rnd1, engine, xw, transactions);
                xw.commit();

                TestUtils.assertEqualsExactOrder(compiler, sqlExecutionContext, "y", "x");
            } finally {
                Misc.freeObjListAndClear(transactions);
            }

            // Ties: the first commit puts one or two rows on each whole second of the partition from
            // addColumnTs on, which already holds one, so the row groups it merges split runs of equal
            // timestamps at their boundaries; the second commit adds a row on every k-th of those seconds,
            // some of them at such a boundary. Native y puts each O3 row after every stored row of its
            // timestamp, x must as well. The commits start at addColumnTs: the prefix that keeps its
            // position takes the whole column top, which stays, as in y
            final long secondTicks = timestampDriver.fromMicros(1_000_000L);
            final long partitionHi = partitionTs + timestampDriver.fromMicros(3_600_000_000L);
            final int secondCount = (int) ((partitionHi - addColumnTs) / secondTicks);
            final int[] firstCommitRowCounts = new int[secondCount];
            for (int i = 0; i < secondCount; i++) {
                firstCommitRowCounts[i] = 1 + rnd.nextInt(2);
            }
            final int tieStep = 1 + rnd.nextInt(3);
            LOG.info().$("tie commits [secondCount=").$(secondCount).$(", tieStep=").$(tieStep).I$();
            for (TableWriter w : new TableWriter[]{xw, yw}) {
                final int iIndex = w.getMetadata().getColumnIndex("i");
                int value = 1_000_000;
                for (int i = 0; i < secondCount; i++) {
                    for (int j = 0; j < firstCommitRowCounts[i]; j++) {
                        TableWriter.Row row = w.newRow(addColumnTs + i * secondTicks);
                        row.putInt(iIndex, value++);
                        row.append();
                    }
                }
                w.commit();
                value = 2_000_000;
                for (int i = 0; i < secondCount; i += tieStep) {
                    TableWriter.Row row = w.newRow(addColumnTs + i * secondTicks);
                    row.putInt(iIndex, value++);
                    row.append();
                }
                w.commit();
            }
            TestUtils.assertEqualsExactOrder(compiler, sqlExecutionContext, "y", "x");
        }

        // x and y add BOOLEAN, BYTE, SHORT and CHAR columns that the parquet file of x lacks, then an O3
        // batch merges into one row group of the partition at or after addColumnTs. The merge ends the top of
        // the added columns, which spans the whole partition, and native y materializes it as 0/false: the
        // rewrite that the schema change forces must write the row groups it copies with 0/false as well
        final int copiedZeroRowGroupCount = endTopsOfColumnsAddedAfterConversion(engine, sqlExecutionContext, rnd, partitionTs, addColumnTs);
        LOG.info().$("row groups copied with the ended top of an absent column [count=").$(copiedZeroRowGroupCount).I$();
        Assert.assertTrue("rewrite copied no row group with the ended top of an absent column", copiedZeroRowGroupCount > 0);
        TestUtils.assertEqualsExactOrder(compiler, sqlExecutionContext, "y", "x");

        // The O3 commits keep the column top of the no-sentinel columns as native does: the top rows
        // read NULL after a change to a type with NULL, lazily on the parquet partition (nb, nl) and
        // after CONVERT TO NATIVE (nf, nt)
        for (String table : new String[]{"x", "y"}) {
            engine.execute("alter table " + table + " alter column nb type symbol", sqlExecutionContext);
            engine.execute("alter table " + table + " alter column nl type int", sqlExecutionContext);
            engine.execute("alter table " + table + " alter column ab type varchar", sqlExecutionContext);
            engine.execute("alter table " + table + " alter column al type int", sqlExecutionContext);
            engine.execute("alter table " + table + " alter column af type double", sqlExecutionContext);
            engine.execute("alter table " + table + " alter column at type varchar", sqlExecutionContext);
        }
        TestUtils.assertEqualsExactOrder(compiler, sqlExecutionContext, "y", "x");
        engine.execute("alter table x convert partition to native list '" + stringSink + "'", sqlExecutionContext);
        for (String table : new String[]{"x", "y"}) {
            engine.execute("alter table " + table + " alter column nf type int", sqlExecutionContext);
            engine.execute("alter table " + table + " alter column nt type symbol", sqlExecutionContext);
        }
        TestUtils.assertEqualsExactOrder(compiler, sqlExecutionContext, "y", "x");
    }

    private void testFuzz0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        testFuzz00(engine, compiler, sqlExecutionContext, timestampTypeName, TestUtils.generateRandom(LOG));
    }
}
