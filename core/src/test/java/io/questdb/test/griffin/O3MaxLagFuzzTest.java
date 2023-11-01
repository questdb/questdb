/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
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

    private static void replayTransactions(Rnd rnd, CairoEngine engine, TableWriter w, ObjList<FuzzTransaction> transactions, int virtualTimestampIndex) {
        for (int i = 0, n = transactions.size(); i < n; i++) {
            FuzzTransaction tx = transactions.getQuick(i);
            ObjList<FuzzTransactionOperation> ops = tx.operationList;
            for (int j = 0, k = ops.size(); j < k; j++) {
                ops.getQuick(j).apply(rnd, engine, w, virtualTimestampIndex);
            }
            w.ic();
        }
    }

    private static void testFuzz00(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
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
                " rnd_char() t" +
                " from long_sequence(" + nTotalRows + ")" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        compiler.compile("create table y as (select * from x order by t)", sqlExecutionContext);
        compiler.compile("create table z as (select * from x order by t)", sqlExecutionContext);

        TestUtils.assertEquals(compiler, sqlExecutionContext, "y order by ts", "x");

        long minTs = TimestampFormatUtils.parseTimestamp("2022-11-11T14:28:00.000000Z");
        long maxTs = TimestampFormatUtils.parseTimestamp("2022-11-12T14:28:00.000000Z");
        int txCount = Math.max(1, rnd.nextInt(50));
        int rowCount = Math.max(1, txCount * rnd.nextInt(200) * 1000);
        try (
                TableWriter w = TestUtils.getWriter(engine, "x");
                TableWriter w2 = TestUtils.getWriter(engine, "y")
        ) {
            ObjList<FuzzTransaction> transactions = FuzzTransactionGenerator.generateSet(
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
                    Os.type == Os.WINDOWS ? 0 : 0.2, // insert only
                    0,
                    5,
                    new String[]{"ABC", "CDE", "XYZ"},
                    0,
                    0
            );

            Rnd rnd1 = new Rnd();
            replayTransactions(rnd1, engine, w, transactions, -1);
            w.commit();

            Rnd rnd2 = new Rnd();
            replayTransactions(rnd2, engine, w2, transactions, w.getMetadata().getTimestampIndex());
            w2.commit();

            TestUtils.assertEquals(compiler, sqlExecutionContext, "y order by ts", "x");
        }
    }

    private void testFuzz0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, NumericException {
        testFuzz00(engine, compiler, sqlExecutionContext, TestUtils.generateRandom(LOG));
    }


    private void testRollbackFuzz(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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
                " rnd_char() t" +
                " from long_sequence(" + nTotalRows + ")" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);
        // table "z" is out of order - reshuffled "x"
        compiler.compile("create table z as (select * from x order by f)", sqlExecutionContext);
        // table "z" is our target table, where we exercise O3 and rollbacks
        compiler.compile("create table y as (select * from x where 1 <> 1) timestamp(ts) partition by day", sqlExecutionContext);

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
}
