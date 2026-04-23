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
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.o3.AbstractO3Test;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionGenerator;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class O3ParquetPartitionFuzzTest extends AbstractO3Test {
    @Test
    public void testParquetWriteFuzz() throws Exception {
        executeWithPool(0, this::testFuzz0);
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
        // create initial table "x"
        String sql = "create table x as (" +
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
                " from long_sequence(" + nTotalRows + ")" +
                ") timestamp (ts) partition by HOUR";

        engine.execute(sql, sqlExecutionContext);
        engine.execute("create table y as (select * from x) timestamp (ts) partition by HOUR", sqlExecutionContext);
        TestUtils.assertEquals(compiler, sqlExecutionContext, "y", "x");

        final int partitionIndex;
        final long partitionTs;
        StringSink stringSink = new StringSink();
        TimestampDriver timestampDriver;
        try (TableReader xr = engine.getReader("x")) {
            timestampDriver = ColumnType.getTimestampDriver(xr.getMetadata().getTimestampType());
            partitionIndex = rnd.nextInt(xr.getPartitionCount() - 1);
            partitionTs = xr.getPartitionTimestampByIndex(partitionIndex);
            int partitionBy = xr.getPartitionedBy();
            PartitionBy.setSinkForPartition(stringSink, xr.getMetadata().getTimestampType(), partitionBy, partitionTs);
            engine.execute("alter table x convert partition to parquet list '" + stringSink + "'", sqlExecutionContext);
        }

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
                    0.3
            );

            try {
                Rnd rnd2 = new Rnd(rnd.nextLong(), rnd.nextLong());
                replayTransactions(rnd2, engine, yw, transactions);
                yw.commit();

                Rnd rnd1 = new Rnd(rnd.nextLong(), rnd.nextLong());
                replayTransactions(rnd1, engine, xw, transactions);
                xw.commit();

                TestUtils.assertEquals(compiler, sqlExecutionContext, "y", "x");
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        }
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
