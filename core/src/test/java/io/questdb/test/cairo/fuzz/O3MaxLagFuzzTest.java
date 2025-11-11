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

package io.questdb.test.cairo.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.cairo.o3.AbstractO3Test;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionGenerator;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class O3MaxLagFuzzTest extends AbstractO3Test {
    private final static Log LOG = LogFactory.getLog(O3MaxLagFuzzTest.class);

    @Test
    public void testFuzzParallel() throws Exception {
        executeWithPool(2, this::testFuzz0);
    }

    @Test
    public void testRollbackFuzzParallel() throws Exception {
        executeWithPool(2, this::testRollbackFuzz);
    }

    @Test
    public void testRollbackRegression1() throws Exception {
        executeWithPool(0, this::testRollbackRegression1);
    }

    @Test
    public void testRollbackRegression2() throws Exception {
        executeWithPool(0, this::testRollbackRegression2);
    }

    private static void replayTransactions(Rnd rnd, CairoEngine engine, TableWriter w, ObjList<FuzzTransaction> transactions, int virtualTimestampIndex) {
        for (int i = 0, n = transactions.size(); i < n; i++) {
            FuzzTransaction tx = transactions.getQuick(i);
            ObjList<FuzzTransactionOperation> ops = tx.operationList;
            for (int j = 0, k = ops.size(); j < k; j++) {
                ops.getQuick(j).apply(rnd, engine, w, virtualTimestampIndex, null);
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
    ) throws SqlException, NumericException {
        long microsBetweenRows = 1000000L;
        int nTotalRows = 12000;
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
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(0L," + microsBetweenRows + "L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t," +
                " rnd_varchar(6, 16, 2) v1," +
                " rnd_varchar(1, 1, 1) v2," +
                " from long_sequence(" + nTotalRows + ")" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        engine.execute("create table y as (select * from x order by t)", sqlExecutionContext);
        engine.execute("create table z as (select * from x order by t)", sqlExecutionContext);

        TestUtils.assertEquals(compiler, sqlExecutionContext, "y order by ts", "x");


        try (
                TableWriter w = TestUtils.getWriter(engine, "x");
                TableRecordMetadata sequencerMetadata = engine.getLegacyMetadata(w.getTableToken());
                TableWriter w2 = TestUtils.getWriter(engine, "y")
        ) {
            TimestampDriver driver = ColumnType.getTimestampDriver(w.getTimestampType());
            long minTs = driver.parseFloorLiteral("2022-11-11T14:28:00.000000Z");
            long maxTs = driver.parseFloorLiteral("2022-11-12T14:28:00.000000Z");
            int txCount = Math.max(1, rnd.nextInt(50));
            int rowCount = Math.min(2_000_000, Math.max(1, txCount * rnd.nextInt(200) * 1000));

            ObjList<FuzzTransaction> transactions = FuzzTransactionGenerator.generateSet(
                    nTotalRows,
                    sequencerMetadata,
                    w.getMetadata(),
                    rnd,
                    minTs,
                    maxTs,
                    rowCount,
                    txCount,
                    true,
                    rnd.nextDouble(),
                    rnd.nextDouble(),
                    rnd.nextDouble(),
                    rnd.nextDouble(),
                    rnd.nextDouble(),
                    rnd.nextDouble(),
                    rnd.nextDouble(),
                    rnd.nextDouble(),
                    // do not generate truncate on windows because it cannot be executed
                    // successfully due to readers being open
                    rnd.nextDouble(),
                    0,
                    0.0,
                    Os.type == Os.WINDOWS ? 0 : 0.2, // insert only
                    0,
                    0.0,
                    0.0,
                    0.0,
                    5,
                    new String[]{"ABC", "CDE", "XYZ"},
                    0
            );

            try {
                Rnd rnd1 = new Rnd();
                replayTransactions(rnd1, engine, w, transactions, -1);
                w.commit();

                Rnd rnd2 = new Rnd();
                replayTransactions(rnd2, engine, w2, transactions, w.getMetadata().getTimestampIndex());
                w2.commit();

                TestUtils.assertEquals(compiler, sqlExecutionContext, "y order by ts", "x");
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        }
    }

    private void runRollbackRegression(
            CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName,
            int nTotalRows, long microsBetweenRows, double fraction
    ) throws SqlException {

        // table "x" is in order
        String sql = "create table x as (" +
                "select" +
                " rnd_short(10,1024) f," +
                " timestamp_sequence(0L," + microsBetweenRows + "L)::" + timestampTypeName + " ts," +
                " from long_sequence(" + nTotalRows + ")" +
                ") timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);
        // table "z" is out of order - reshuffled "x"
        engine.execute("create table z as (select * from x order by f)", sqlExecutionContext);
        // table "y" is our target table, where we exercise O3 and rollbacks
        engine.execute("create table y as (select * from x where 1 <> 1) timestamp(ts) partition by day", sqlExecutionContext);
        try (TableWriter w = TestUtils.getWriter(engine, "y")) {
            insertUncommitted(compiler, sqlExecutionContext, "z limit " + (int) (nTotalRows * fraction), w);
            w.ic();
            final long o3Uncommitted = w.getO3RowCount();
            long expectedRowCount = w.size() - o3Uncommitted;
            w.rollback();
            Assert.assertEquals(expectedRowCount, w.size());
            TestUtils.assertEquals(
                    compiler,
                    sqlExecutionContext,
                    "(z limit " + (int) (nTotalRows * fraction) + ") order by ts limit " + expectedRowCount,
                    "y"
            );
            // insert remaining data (that we did not try to insert yet)
            insertUncommitted(compiler, sqlExecutionContext, "z limit " + (int) (nTotalRows * fraction) + ", " + nTotalRows, w);
            w.ic();
            // insert data that we rolled back
            insertUncommitted(compiler, sqlExecutionContext, "(z limit " + (int) (nTotalRows * fraction) + ") order by ts limit -" + o3Uncommitted, w);
            w.ic();
            w.commit();
        }
        assertXY(compiler, sqlExecutionContext);
    }

    private void testFuzz0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException, NumericException {
        testFuzz00(engine, compiler, sqlExecutionContext, timestampTypeName, TestUtils.generateRandom(LOG));
    }

    private void testRollbackFuzz(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int nTotalRows = rnd.nextInt(79000);
        final long microsBetweenRows = rnd.nextLong(3090985);
        final double fraction = rnd.nextDouble();
        testRollbackFuzz0(engine, compiler, sqlExecutionContext, nTotalRows, (int) (nTotalRows * fraction), microsBetweenRows);
    }

    private void testRollbackFuzz0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            int nTotalRows,
            int lim,
            long microsBetweenRows
    ) throws SqlException {
        // table "x" is in order
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(0L," + microsBetweenRows + "L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t," +
                " rnd_varchar(5, 16, 2) v1," +
                " rnd_varchar(1, 1, 1) v2," +
                " from long_sequence(" + nTotalRows + ")" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);
        // table "z" is out of order - reshuffled "x"
        engine.execute("create table z as (select * from x order by f)", sqlExecutionContext);
        // table "z" is our target table, where we exercise O3 and rollbacks
        engine.execute("create table y as (select * from x where 1 <> 1) timestamp(ts) partition by day", sqlExecutionContext);

        try (TableWriter w = TestUtils.getWriter(engine, "y")) {

            insertUncommitted(compiler, sqlExecutionContext, "z limit " + lim, w);

            w.ic();

            final long o3Uncommitted = w.getO3RowCount();

            long expectedRowCount = w.size() - o3Uncommitted;

            w.rollback();

            Assert.assertEquals(expectedRowCount, w.size());

            TestUtils.assertEquals(
                    compiler,
                    sqlExecutionContext,
                    "(z limit " + lim + ") order by ts limit " + (lim - o3Uncommitted),
                    "y"
            );

            // insert remaining data (that we did not try to insert yet)
            insertUncommitted(compiler, sqlExecutionContext, "z limit " + lim + ", " + nTotalRows, w);
            w.ic();

            // insert data that we rolled back
            insertUncommitted(compiler, sqlExecutionContext, "(z limit " + lim + ") order by ts limit -" + o3Uncommitted, w);
            w.ic();

            w.commit();
        }
        assertXY(compiler, sqlExecutionContext);
    }

    private void testRollbackRegression1(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
        // We used this commented-out code to find values that make this and the other regression test fail:
//        final Rnd rnd = TestUtils.generateRandom(LOG);
//        final int nTotalRows = rnd.nextInt(79000);
//        final long microsBetweenRows = rnd.nextLong(3090985);
//        final double fraction = rnd.nextDouble();
//        System.out.printf("*+*+*+*+ nTotalRows %,d microsBetweenRows %,d fraction %.2f\n", nTotalRows, microsBetweenRows, fraction);

        final int nTotalRows = 51_555;
        final long microsBetweenRows = 2_267_870;
        final double fraction = 0.8;
        runRollbackRegression(engine, compiler, sqlExecutionContext, timestampTypeName, nTotalRows, microsBetweenRows, fraction);
    }

    private void testRollbackRegression2(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
        final int nTotalRows = 10_795;
        final long microsBetweenRows = 1_970_536;
        final double fraction = 0.09;
        runRollbackRegression(engine, compiler, sqlExecutionContext, timestampTypeName, nTotalRows, microsBetweenRows, fraction);
    }
}
