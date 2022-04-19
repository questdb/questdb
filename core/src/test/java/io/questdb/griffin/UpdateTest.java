/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.cairo.security.CairoSecurityContextImpl;
import io.questdb.cairo.sql.ReaderOutOfDateException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.mp.SCSequence;
import io.questdb.std.Misc;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.util.concurrent.CyclicBarrier;

public class UpdateTest extends AbstractGriffinTest {
    private static UpdateExecution updateExecution;
    private final SCSequence eventSubSequence = new SCSequence();

    @BeforeClass
    public static void setUpUpdates() {
        // Make updateExecution static to test re-usabbility of the object
        updateExecution = new UpdateExecution(configuration, engine.getMessageBus());
    }

    @AfterClass
    public static void tearDownUpdate() {
        updateExecution = Misc.free(updateExecution);
    }

    @Test
    public void testInsertAfterUpdate() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) v," + " cast(x as int) x," + " cast(x as int) z" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = 1");

            assertSql("up", "ts\tv\tx\tz\n" + "1970-01-01T00:00:00.000000Z\t1\t1\t1\n" + "1970-01-01T00:00:01.000000Z\t2\t1\t2\n" + "1970-01-01T00:00:02.000000Z\t3\t1\t3\n" + "1970-01-01T00:00:03.000000Z\t4\t1\t4\n" + "1970-01-01T00:00:04.000000Z\t5\t1\t5\n");

            executeUpdate("UPDATE up SET z = 2");

            assertSql("up", "ts\tv\tx\tz\n" + "1970-01-01T00:00:00.000000Z\t1\t1\t2\n" + "1970-01-01T00:00:01.000000Z\t2\t1\t2\n" + "1970-01-01T00:00:02.000000Z\t3\t1\t2\n" + "1970-01-01T00:00:03.000000Z\t4\t1\t2\n" + "1970-01-01T00:00:04.000000Z\t5\t1\t2\n");

            executeUpdate("UPDATE up SET v = 33");

            assertSql("up", "ts\tv\tx\tz\n" + "1970-01-01T00:00:00.000000Z\t33\t1\t2\n" + "1970-01-01T00:00:01.000000Z\t33\t1\t2\n" + "1970-01-01T00:00:02.000000Z\t33\t1\t2\n" + "1970-01-01T00:00:03.000000Z\t33\t1\t2\n" + "1970-01-01T00:00:04.000000Z\t33\t1\t2\n");

            compile("INSERT INTO up VALUES('1970-01-01T00:00:05.000000Z', 10.0, 10.0, 10.0)");
            compile("INSERT INTO up VALUES('1970-01-01T00:00:06.000000Z', 100.0, 100.0, 100.0)");

            assertSql("up", "ts\tv\tx\tz\n" + "1970-01-01T00:00:00.000000Z\t33\t1\t2\n" + "1970-01-01T00:00:01.000000Z\t33\t1\t2\n" + "1970-01-01T00:00:02.000000Z\t33\t1\t2\n" + "1970-01-01T00:00:03.000000Z\t33\t1\t2\n" + "1970-01-01T00:00:04.000000Z\t33\t1\t2\n" + "1970-01-01T00:00:05.000000Z\t10\t10\t10\n" + "1970-01-01T00:00:06.000000Z\t100\t100\t100\n");
        });
    }

    @Test
    public void testNoRowsUpdated() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) v," + " cast(x as int) x," + " cast(x as int) z" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = 1 WHERE x > 10");

            assertSql("up", "ts\tv\tx\tz\n" + "1970-01-01T00:00:00.000000Z\t1\t1\t1\n" + "1970-01-01T00:00:01.000000Z\t2\t2\t2\n" + "1970-01-01T00:00:02.000000Z\t3\t3\t3\n" + "1970-01-01T00:00:03.000000Z\t4\t4\t4\n" + "1970-01-01T00:00:04.000000Z\t5\t5\t5\n");
        });
    }

    @Test
    public void testSymbolsIndexed_UpdateNull() throws Exception {
        testSymbols_UpdateNull(true);
    }

    @Test
    public void testSymbolsIndexed_UpdateWithExistingValue() throws Exception {
        testSymbol_UpdateWithExistingValue(true);
    }

    @Test
    public void testSymbolsIndexed_UpdateWithNewValue() throws Exception {
        testSymbols_UpdateWithNewValue(true);
    }

    @Test
    public void testSymbols_UpdateNull() throws Exception {
        testSymbols_UpdateNull(false);
    }

    @Test
    public void testSymbols_UpdateWithExistingValue() throws Exception {
        testSymbol_UpdateWithExistingValue(false);
    }

    @Test
    public void testSymbols_UpdateWithNewValue() throws Exception {
        testSymbols_UpdateWithNewValue(false);
    }

    @Test
    public void testUpdate2ColumnsWith2TableJoinInWithClause() throws Exception {
        assertMemoryLeak(() -> {
            createTablesToJoin("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " rnd_symbol('a', 'b', null) s," + " x," + " x + 1 as y" + " from long_sequence(5))" + " timestamp(ts) partition by DAY");

            executeUpdate("WITH jn AS (select down1.y + down2.y AS sm, down1.s, down2.y FROM down1 JOIN down2 ON down1.s = down2.s)" + "UPDATE up SET x = sm, y = jn.y" + " FROM jn " + " WHERE up.s = jn.s");

            assertSql("up", "ts\ts\tx\ty\n" + "1970-01-01T00:00:00.000000Z\ta\t101\t100\n" + "1970-01-01T00:00:01.000000Z\ta\t101\t100\n" + "1970-01-01T00:00:02.000000Z\tb\t303\t300\n" + "1970-01-01T00:00:03.000000Z\t\t505\t500\n" + "1970-01-01T00:00:04.000000Z\t\t505\t500\n");
        });
    }

    @Test
    public void testUpdateAddedColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table testUpdateAddedColumn as" + " (select timestamp_sequence(0, 6*60*60*1000000L) ts," + " cast(x - 1 as int) x" + " from long_sequence(10))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            // Bump table version
            compile("alter table testUpdateAddedColumn add column y long", sqlExecutionContext);
            executeUpdate("UPDATE testUpdateAddedColumn SET y = x + 1 WHERE ts between '1970-01-01T12' and '1970-01-02T12'");

            assertSql("testUpdateAddedColumn", "ts\tx\ty\n" + "1970-01-01T00:00:00.000000Z\t0\tNaN\n" + "1970-01-01T06:00:00.000000Z\t1\tNaN\n" + "1970-01-01T12:00:00.000000Z\t2\t3\n" + "1970-01-01T18:00:00.000000Z\t3\t4\n" + "1970-01-02T00:00:00.000000Z\t4\t5\n" + "1970-01-02T06:00:00.000000Z\t5\t6\n" + "1970-01-02T12:00:00.000000Z\t6\t7\n" + "1970-01-02T18:00:00.000000Z\t7\tNaN\n" + "1970-01-03T00:00:00.000000Z\t8\tNaN\n" + "1970-01-03T06:00:00.000000Z\t9\tNaN\n");

            compile("alter table testUpdateAddedColumn drop column y");
            compile("alter table testUpdateAddedColumn add column y int");
            executeUpdate("UPDATE testUpdateAddedColumn SET y = COALESCE(y, x + 2) WHERE x%2 = 0");

            assertSql("testUpdateAddedColumn", "ts\tx\ty\n" + "1970-01-01T00:00:00.000000Z\t0\t2\n" + "1970-01-01T06:00:00.000000Z\t1\tNaN\n" + "1970-01-01T12:00:00.000000Z\t2\t4\n" + "1970-01-01T18:00:00.000000Z\t3\tNaN\n" + "1970-01-02T00:00:00.000000Z\t4\t6\n" + "1970-01-02T06:00:00.000000Z\t5\tNaN\n" + "1970-01-02T12:00:00.000000Z\t6\t8\n" + "1970-01-02T18:00:00.000000Z\t7\tNaN\n" + "1970-01-03T00:00:00.000000Z\t8\t10\n" + "1970-01-03T06:00:00.000000Z\t9\tNaN\n");

            compile("alter table testUpdateAddedColumn drop column x");
            executeUpdate("UPDATE testUpdateAddedColumn SET y = COALESCE(y, 1)");

            assertSql("testUpdateAddedColumn", "ts\ty\n" + "1970-01-01T00:00:00.000000Z\t2\n" + "1970-01-01T06:00:00.000000Z\t1\n" + "1970-01-01T12:00:00.000000Z\t4\n" + "1970-01-01T18:00:00.000000Z\t1\n" + "1970-01-02T00:00:00.000000Z\t6\n" + "1970-01-02T06:00:00.000000Z\t1\n" + "1970-01-02T12:00:00.000000Z\t8\n" + "1970-01-02T18:00:00.000000Z\t1\n" + "1970-01-03T00:00:00.000000Z\t10\n" + "1970-01-03T06:00:00.000000Z\t1\n");
        });
    }

    @Test
    public void testUpdateAsyncMode() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5))" + " timestamp(ts)", sqlExecutionContext);

            CyclicBarrier barrier = new CyclicBarrier(2);

            final Thread th = new Thread(() -> {
                try {
                    TableWriter tableWriter = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "up", "test");
                    barrier.await(); // table is locked
                    barrier.await(); // update is on writer async cmd queue
                    tableWriter.tick();
                    tableWriter.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail();
                }
            });
            th.start();

            barrier.await(); // table is locked
            try (QueryFuture queryFuture = executeUpdateGetFuture("UPDATE up SET x = 123 WHERE x > 1 and x < 4")) {
                Assert.assertEquals(QueryFuture.QUERY_NO_RESPONSE, queryFuture.getStatus());
                Assert.assertEquals(0, queryFuture.getAffectedRowsCount());
                barrier.await(); // update is on writer async cmd queue
                queryFuture.await(10000000); // 10 seconds timeout
                Assert.assertEquals(QueryFuture.QUERY_COMPLETE, queryFuture.getStatus());
                Assert.assertEquals(2, queryFuture.getAffectedRowsCount());
            }
            th.join();

            assertSql("up", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t1\n" + "1970-01-01T00:00:01.000000Z\t123\n" + "1970-01-01T00:00:02.000000Z\t123\n" + "1970-01-01T00:00:03.000000Z\t4\n" + "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateBinaryColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," + " rnd_bin(10, 20, 2) as bin1," + " x as lng2" + " from long_sequence(10)" + " )" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET bin1 = cast(null as binary) WHERE ts > '1970-01-01T08' and lng2 % 2 = 1");

            assertSql("up", "ts\tbin1\tlng2\n" + "1970-01-01T00:00:00.000000Z\t00000000 41 1d 15 55 8a 17 fa d8 cc 14 ce f1 59 88 c4 91\n" + "00000010 3b 72 db f3\t1\n" + "1970-01-01T06:00:00.000000Z\t00000000 c7 88 de a0 79 3c 77 15 68 61 26 af 19 c4 95 94\n" + "00000010 36 53\t2\n" + "1970-01-01T12:00:00.000000Z\t\t3\n" + "1970-01-01T18:00:00.000000Z\t\t4\n" + "1970-01-02T00:00:00.000000Z\t\t5\n" + "1970-01-02T06:00:00.000000Z\t00000000 08 a1 1e 38 8d 1b 9e f4 c8 39 09 fe d8\t6\n" + "1970-01-02T12:00:00.000000Z\t\t7\n" + "1970-01-02T18:00:00.000000Z\t00000000 78 b5 b9 11 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b\n" + "00000010 b1 3e e3 f1\t8\n" + "1970-01-03T00:00:00.000000Z\t\t9\n" + "1970-01-03T06:00:00.000000Z\t00000000 9c 1d 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3\t10\n");
        });
    }

    @Test
    public void testUpdateBinaryColumnWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," + " rnd_bin(10, 20, 0) as bin1," + " x as lng2" + " from long_sequence(10)" + " )" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            compile("alter table up add column bin2 binary", sqlExecutionContext);
            compile("insert into up select * from " + " (select timestamp_sequence(6*100000000000L, 6 * 60 * 60 * 1000000L) ts," + " rnd_bin(10, 20, 0) as bin1," + " x + 10 as lng2," + " rnd_bin(10, 20, 0) as bin2" + " from long_sequence(5))", sqlExecutionContext);
            executeUpdate("UPDATE up SET bin1 = cast(null as binary), bin2 = cast(null as binary) WHERE lng2 in (6,8,10,12,14)");

            assertSql("up", "ts\tbin1\tlng2\tbin2\n" + "1970-01-01T00:00:00.000000Z\t00000000 41 1d 15 55 8a 17 fa d8 cc 14 ce f1 59 88 c4 91\n" + "00000010 3b 72 db f3\t1\t\n" + "1970-01-01T06:00:00.000000Z\t00000000 c7 88 de a0 79 3c 77 15 68 61 26 af 19 c4 95 94\n" + "00000010 36 53\t2\t\n" + "1970-01-01T12:00:00.000000Z\t00000000 59 7e 3b 08 a1 1e 38 8d 1b 9e f4 c8 39 09 fe\t3\t\n" + "1970-01-01T18:00:00.000000Z\t00000000 30 78 36 6a 32 de e4 7c d2 35 07 42 fc 31 79\t4\t\n" + "1970-01-02T00:00:00.000000Z\t00000000 81 2b 93 4d 1a 8e 78 b5 b9 11 53 d0\t5\t\n" + "1970-01-02T06:00:00.000000Z\t\t6\t\n" + "1970-01-02T12:00:00.000000Z\t00000000 ac 37 c8 cd 82 89 2b 4d 5f f6 46\t7\t\n" + "1970-01-02T18:00:00.000000Z\t\t8\t\n" + "1970-01-03T00:00:00.000000Z\t00000000 d2 85 7f a5 b8 7b 4a 9d 46 7c 8d dd 93 e6 d0\t9\t\n" + "1970-01-03T06:00:00.000000Z\t\t10\t\n" + "1970-01-07T22:40:00.000000Z\t00000000 a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a e7\n" + "00000010 0c 89\t11\t00000000 63 b7 c2 9f 29 8e 29 5e 69 c6 eb ea c3 c9 73 93\n" + "00000010 46 fe\n" + "1970-01-08T04:40:00.000000Z\t\t12\t\n" + "1970-01-08T10:40:00.000000Z\t00000000 e0 b0 e9 98 f7 67 62 28 60 b0 ec 0b 92\t13\t00000000 24 bc 2e 60 6a 1c 0b 20 a2 86 89 37 11 2c\n" + "1970-01-08T16:40:00.000000Z\t\t14\t\n" + "1970-01-08T22:40:00.000000Z\t00000000 e4 35 e4 3a dc 5c 65 ff 27 67 77\t15\t00000000 52 d0 29 26 c5 aa da 18 ce 5f b2\n");
        });
    }

    @Test
    public void testUpdateColumNameCaseInsensitive() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) x" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET X = null WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("up", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t1\n" + "1970-01-01T00:00:01.000000Z\t2\n" + "1970-01-01T00:00:02.000000Z\tNaN\n" + "1970-01-01T00:00:03.000000Z\tNaN\n" + "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateColumnsTypeMismatch() throws Exception {
        assertMemoryLeak(() -> {
            createTablesToJoin("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " rnd_symbol('a', 'b', null) s," + " x," + " x + 1 as y" + " from long_sequence(5))" + " timestamp(ts) partition by DAY");

            executeUpdateFails("WITH jn AS (select down1.y + down2.y AS sm, down1.s, down2.y " + "                         FROM down1 JOIN down2 ON down1.s = down2.s" + ")" + "UPDATE up SET s = sm, y = jn.y" + " FROM jn " + " WHERE jn.s = up.s", 147, "inconvertible types: LONG -> SYMBOL");
        });
    }

    @Test
    public void testUpdateDifferentColumnTypes() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) xint," + " cast(x as long) xlong," + " cast(x as double) xdouble," + " cast(x as short) xshort," + " cast(x as byte) xbyte," + " cast(x as char) xchar," + " cast(x as date) xdate," + " cast(x as float) xfloat," + " cast(x as timestamp) xts, " + " cast(x as boolean) xbool," + " cast(x as long256) xl256" + " from long_sequence(2))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            // All combinations to update xint
            executeUpdateFails("UPDATE up SET xint = xdouble", 21, "inconvertible types: DOUBLE -> INT [from=, to=xint]");
            executeUpdateFails("UPDATE up SET xint = xlong", 21, "inconvertible types: LONG -> INT [from=, to=xint]");
            executeUpdateFails("UPDATE up SET xshort = xlong", 23, "inconvertible types: LONG -> SHORT [from=, to=xshort]");
            executeUpdateFails("UPDATE up SET xchar = xlong", 22, "inconvertible types: LONG -> CHAR [from=, to=xchar]");
            executeUpdateFails("UPDATE up SET xbyte = xlong", 22, "inconvertible types: LONG -> BYTE [from=, to=xbyte]");
            executeUpdateFails("UPDATE up SET xlong = xl256", 22, "inconvertible types: LONG256 -> LONG [from=, to=xlong]");
            executeUpdateFails("UPDATE up SET xl256 = xlong", 22, "inconvertible types: LONG -> LONG256 [from=, to=xl256]");
            executeUpdateFails("UPDATE up SET xchar = xlong", 22, "inconvertible types: LONG -> CHAR [from=, to=xchar]");

            String expected = "ts\txint\txlong\txdouble\txshort\txbyte\txchar\txdate\txfloat\txts\txbool\txl256\n" + "1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t1\t1\t\u0001\t1970-01-01T00:00:00.001Z\t1.0000\t1970-01-01T00:00:00.000001Z\ttrue\t0x01\n" + "1970-01-01T00:00:01.000000Z\t2\t2\t2.0\t2\t2\t\u0002\t1970-01-01T00:00:00.002Z\t2.0000\t1970-01-01T00:00:00.000002Z\tfalse\t0x02\n";

            executeUpdate("UPDATE up SET xint=xshort");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xint=xshort WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xlong=xshort");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xlong=xshort WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xlong=xchar");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xlong=xchar WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xfloat=xint");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xfloat=xint WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xdouble=xfloat");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xdouble=xfloat WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xdouble=xlong");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xdouble=xlong WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xshort=xbyte");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xshort=xbyte WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xshort=xchar");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xshort=xchar WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xchar=xshort");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xchar=xshort WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xint=xchar");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xint=xchar WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xdouble=xlong");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xdouble=xlong WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xlong=xts");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xlong=xts WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xdate=xlong");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xdate=xlong WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xts=xdate");
            // above call modified data from micro to milli. Revert the data back
            executeUpdate("UPDATE up SET xts=xlong");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xts=xlong WHERE ts='1970-01-01'");
            assertSql("up", expected);

            // Update all at once
            executeUpdate("UPDATE up SET xint=xshort, xfloat=xint, xdouble=xfloat, xshort=xbyte, xlong=xts, xts=xlong");
            assertSql("up", expected);

            // Update without conversion
            executeUpdate("UPDATE up" + " SET xint=up2.xint," + " xfloat=up2.xfloat," + " xdouble=up2.xdouble," + " xshort=up2.xshort," + " xlong=up2.xlong," + " xts=up2.xts, " + " xchar=up2.xchar, " + " xbool=up2.xbool, " + " xbyte=up2.xbyte " + " FROM up up2 " + " WHERE up.ts = up2.ts AND up.ts = '1970-01-01'");
            assertSql("up", expected);
        });
    }

    @Test
    public void testUpdateGeoHashColumnToLowerPrecision() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " rnd_geohash(5) g1c," + " rnd_geohash(15) g3c," + " rnd_geohash(25) g5c," + " rnd_geohash(35) g7c" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up " + "SET " + "g1c = cast('questdb' as geohash(7c)), " + "g3c = cast('questdb' as geohash(7c)), " + "g5c = cast('questdb' as geohash(7c)), " + "g7c = cast('questdb' as geohash(7c)) " + "WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("up", "ts\tg1c\tg3c\tg5c\tg7c\n" + "1970-01-01T00:00:00.000000Z\t9\t46s\tjnw97\tzfuqd3b\n" + "1970-01-01T00:00:01.000000Z\th\twh4\ts2z2f\t1cjjwk6\n" + "1970-01-01T00:00:02.000000Z\tq\tque\tquest\tquestdb\n" + "1970-01-01T00:00:03.000000Z\tq\tque\tquest\tquestdb\n" + "1970-01-01T00:00:04.000000Z\tx\t76u\tq0s5w\ts2vqs1b\n");
        });
    }

    @Test
    public void testUpdateGeoHashColumnToLowerPrecision2() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " rnd_geohash(5) g1c," + " rnd_geohash(15) g3c," + " rnd_geohash(25) g5c," + " rnd_geohash(35) g7c" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up " + "SET " + "g1c = g7c, " + "g3c = g7c, " + "g5c = g7c, " + "g7c = g7c " + "WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("up", "ts\tg1c\tg3c\tg5c\tg7c\n" + "1970-01-01T00:00:00.000000Z\t9\t46s\tjnw97\tzfuqd3b\n" + "1970-01-01T00:00:01.000000Z\th\twh4\ts2z2f\t1cjjwk6\n" + "1970-01-01T00:00:02.000000Z\tq\tq4s\tq4s2x\tq4s2xyt\n" + "1970-01-01T00:00:03.000000Z\tb\tbuy\tbuyv3\tbuyv3pv\n" + "1970-01-01T00:00:04.000000Z\tx\t76u\tq0s5w\ts2vqs1b\n");
        });
    }

    @Test
    public void testUpdateGeohashToStringLiteral() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " rnd_geohash(15) as geo3," + " rnd_geohash(25) as geo5 " + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET geo3 = 'questdb', geo5 = 'questdb' WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("up", "ts\tgeo3\tgeo5\n" + "1970-01-01T00:00:00.000000Z\t9v1\t46swg\n" + "1970-01-01T00:00:01.000000Z\tjnw\tzfuqd\n" + "1970-01-01T00:00:02.000000Z\tque\tquest\n" + "1970-01-01T00:00:03.000000Z\tque\tquest\n" + "1970-01-01T00:00:04.000000Z\tmmt\t71ftm\n");
        });
    }

    @Test
    public void testUpdateIdentical() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) x" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = x WHERE x > 1 and x < 4");

            assertSql("up", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t1\n" + "1970-01-01T00:00:01.000000Z\t2\n" + "1970-01-01T00:00:02.000000Z\t3\n" + "1970-01-01T00:00:03.000000Z\t4\n" + "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateMultiPartitionEmptyColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 25000000000) ts," + " cast(x as int) x" + " from long_sequence(10))" + " timestamp(ts) partition by DAY", sqlExecutionContext);
            compile("alter table up add column y long", sqlExecutionContext);

            executeUpdate("UPDATE up SET y = 42 where x = 2 or x = 4 or x = 6 or x = 8 or x = 13");

            assertSql("up", "ts\tx\ty\n" + "1970-01-01T00:00:00.000000Z\t1\tNaN\n" + "1970-01-01T06:56:40.000000Z\t2\t42\n" + "1970-01-01T13:53:20.000000Z\t3\tNaN\n" + "1970-01-01T20:50:00.000000Z\t4\t42\n" + "1970-01-02T03:46:40.000000Z\t5\tNaN\n" + "1970-01-02T10:43:20.000000Z\t6\t42\n" + "1970-01-02T17:40:00.000000Z\t7\tNaN\n" + "1970-01-03T00:36:40.000000Z\t8\t42\n" + "1970-01-03T07:33:20.000000Z\t9\tNaN\n" + "1970-01-03T14:30:00.000000Z\t10\tNaN\n");
        });
    }

    @Test
    public void testUpdateMultiPartitionedTableSamePartitionManyFrames() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tml = new TableModel(configuration, "up", PartitionBy.DAY)) {
                tml.col("xint", ColumnType.INT).col("xsym", ColumnType.SYMBOL).indexed(true, 256).timestamp("ts");
                createPopulateTable(tml, 10, "2020-01-01", 2);
            }

            executeUpdate("UPDATE up SET xint = -1000 WHERE ts in '2020-01-01T00;6h;12h;24'");
            assertSql("up", "xint\txsym\tts\n" + "-1000\tCPSW\t2020-01-01T04:47:59.900000Z\n" + "2\tHYRX\t2020-01-01T09:35:59.800000Z\n" + "-1000\t\t2020-01-01T14:23:59.700000Z\n" + "4\tVTJW\t2020-01-01T19:11:59.600000Z\n" + "5\tPEHN\t2020-01-01T23:59:59.500000Z\n" + "-1000\t\t2020-01-02T04:47:59.400000Z\n" + "7\tVTJW\t2020-01-02T09:35:59.300000Z\n" + "-1000\t\t2020-01-02T14:23:59.200000Z\n" + "9\tCPSW\t2020-01-02T19:11:59.100000Z\n" + "10\t\t2020-01-02T23:59:59.000000Z\n");

            executeUpdate("UPDATE up SET xint = -1000 WHERE ts in '2020-01-01T06;6h;12h;24' and xint > 7");
            assertSql("up", "xint\txsym\tts\n" + "-1000\tCPSW\t2020-01-01T04:47:59.900000Z\n" + "2\tHYRX\t2020-01-01T09:35:59.800000Z\n" + "-1000\t\t2020-01-01T14:23:59.700000Z\n" + "4\tVTJW\t2020-01-01T19:11:59.600000Z\n" + "5\tPEHN\t2020-01-01T23:59:59.500000Z\n" + "-1000\t\t2020-01-02T04:47:59.400000Z\n" + "7\tVTJW\t2020-01-02T09:35:59.300000Z\n" + "-1000\t\t2020-01-02T14:23:59.200000Z\n" + "-1000\tCPSW\t2020-01-02T19:11:59.100000Z\n" + "-1000\t\t2020-01-02T23:59:59.000000Z\n");
        });
    }

    @Test
    public void testUpdateMultiPartitionsWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 25000000000) ts," + " cast(x as int) x" + " from long_sequence(10))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            // Bump table version
            compile("alter table up add column y long", sqlExecutionContext);
            compile("insert into up select * from " + " (select timestamp_sequence(250000000000, 25000000000) ts," + " cast(x as int) + 10 as x," + " cast(x as long) * 10 as y" + " from long_sequence(10))", sqlExecutionContext);

            executeUpdate("UPDATE up SET y = 42 where x = 2 or x = 4 or x = 9 or x = 6 or x = 13 or x = 20");

            assertSql("up", "ts\tx\ty\n" + "1970-01-01T00:00:00.000000Z\t1\tNaN\n" + "1970-01-01T06:56:40.000000Z\t2\t42\n" + "1970-01-01T13:53:20.000000Z\t3\tNaN\n" + "1970-01-01T20:50:00.000000Z\t4\t42\n" + "1970-01-02T03:46:40.000000Z\t5\tNaN\n" + "1970-01-02T10:43:20.000000Z\t6\t42\n" + "1970-01-02T17:40:00.000000Z\t7\tNaN\n" + "1970-01-03T00:36:40.000000Z\t8\tNaN\n" + "1970-01-03T07:33:20.000000Z\t9\t42\n" + "1970-01-03T14:30:00.000000Z\t10\tNaN\n" + "1970-01-03T21:26:40.000000Z\t11\t10\n" + "1970-01-04T04:23:20.000000Z\t12\t20\n" + "1970-01-04T11:20:00.000000Z\t13\t42\n" + "1970-01-04T18:16:40.000000Z\t14\t40\n" + "1970-01-05T01:13:20.000000Z\t15\t50\n" + "1970-01-05T08:10:00.000000Z\t16\t60\n" + "1970-01-05T15:06:40.000000Z\t17\t70\n" + "1970-01-05T22:03:20.000000Z\t18\t80\n" + "1970-01-06T05:00:00.000000Z\t19\t90\n" + "1970-01-06T11:56:40.000000Z\t20\t42\n");
        });
    }

    @Test
    public void testUpdateMultipartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tml = new TableModel(configuration, "up", PartitionBy.DAY)) {
                tml.col("xint", ColumnType.INT).col("xsym", ColumnType.SYMBOL).indexed(true, 256).timestamp("ts");
                createPopulateTable(tml, 5, "2020-01-01", 2);
            }

            executeUpdate("UPDATE up SET xint = -1000 WHERE ts > '2020-01-02T14'");
            assertSql("up", "xint\txsym\tts\n" + "1\tCPSW\t2020-01-01T09:35:59.800000Z\n" + "2\tHYRX\t2020-01-01T19:11:59.600000Z\n" + "3\t\t2020-01-02T04:47:59.400000Z\n" + "-1000\tVTJW\t2020-01-02T14:23:59.200000Z\n" +  // Updated
                    "-1000\tPEHN\t2020-01-02T23:59:59.000000Z\n"    // Updated
            );

            executeUpdate("UPDATE up SET xint = -2000 WHERE ts > '2020-01-02T14' AND xsym = 'VTJW'");
            assertSql("up", "xint\txsym\tts\n" + "1\tCPSW\t2020-01-01T09:35:59.800000Z\n" + "2\tHYRX\t2020-01-01T19:11:59.600000Z\n" + "3\t\t2020-01-02T04:47:59.400000Z\n" + "-2000\tVTJW\t2020-01-02T14:23:59.200000Z\n" +  // Updated
                    "-1000\tPEHN\t2020-01-02T23:59:59.000000Z\n");
        });
    }

    @Test
    public void testUpdateNoFilter() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) x" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = 1");

            assertSql("up", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t1\n" + "1970-01-01T00:00:01.000000Z\t1\n" + "1970-01-01T00:00:02.000000Z\t1\n" + "1970-01-01T00:00:03.000000Z\t1\n" + "1970-01-01T00:00:04.000000Z\t1\n");
        });
    }

    @Test
    public void testUpdateNoFilterOnAlteredTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) x" + " from long_sequence(1))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            CompiledQuery cc = compiler.compile("UPDATE up SET x = 1", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.UPDATE, cc.getType());

            try (UpdateStatement updateStatement = cc.getUpdateStatement()) {
                // Bump table version
                compile("alter table up add column y long", sqlExecutionContext);
                compile("alter table up drop column y", sqlExecutionContext);

                applyUpdate(updateStatement);
                Assert.fail();
            } catch (ReaderOutOfDateException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "table='up'");
            }
        });
    }

    @Test
    public void testUpdateNonPartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5))" + " timestamp(ts)", sqlExecutionContext);

            try (QueryFuture queryFuture = executeUpdateGetFuture("UPDATE up SET x = 123 WHERE x > 1 and x < 5")) {
                Assert.assertEquals(QueryFuture.QUERY_COMPLETE, queryFuture.getStatus());
                Assert.assertEquals(3, queryFuture.getAffectedRowsCount());
            }

            assertSql("up", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t1\n" + "1970-01-01T00:00:01.000000Z\t123\n" + "1970-01-01T00:00:02.000000Z\t123\n" + "1970-01-01T00:00:03.000000Z\t123\n" + "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateOnAlteredTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) x" + " from long_sequence(1))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            // Bump table version
            compile("alter table up add column y long", sqlExecutionContext);
            compile("alter table up drop column y", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = 1");

            assertSql("up", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t1\n");
        });
    }

    @Test
    public void testUpdateReadonlyFails() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) x" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            SqlExecutionContext roExecutionContext = new SqlExecutionContextImpl(engine, 1).with(new CairoSecurityContextImpl(false), bindVariableService, null, -1, null);

            try {
                compile("UPDATE up SET x = x WHERE x > 1 and x < 4", roExecutionContext);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "permission denied");
            }
        });
    }

    @Test
    public void testUpdateSinglePartitionColumnTopAndAroundDense() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x - 1 as int) x" + " from long_sequence(10))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            // Bump table version
            compile("alter table up add column y long", sqlExecutionContext);
            compile("insert into up select * from " + " (select timestamp_sequence(100000000, 1000000) ts," + " cast(x - 1 as int) + 10 as x," + " cast(x * 10 as long) as y" + " from long_sequence(5))", sqlExecutionContext);

            executeUpdate("UPDATE up SET y = 42 where x = 9 or x = 10 or x = 11");

            assertSql("up", "ts\tx\ty\n" + "1970-01-01T00:00:00.000000Z\t0\tNaN\n" + "1970-01-01T00:00:01.000000Z\t1\tNaN\n" + "1970-01-01T00:00:02.000000Z\t2\tNaN\n" + "1970-01-01T00:00:03.000000Z\t3\tNaN\n" + "1970-01-01T00:00:04.000000Z\t4\tNaN\n" + "1970-01-01T00:00:05.000000Z\t5\tNaN\n" + "1970-01-01T00:00:06.000000Z\t6\tNaN\n" + "1970-01-01T00:00:07.000000Z\t7\tNaN\n" + "1970-01-01T00:00:08.000000Z\t8\tNaN\n" + "1970-01-01T00:00:09.000000Z\t9\t42\n" + "1970-01-01T00:01:40.000000Z\t10\t42\n" + "1970-01-01T00:01:41.000000Z\t11\t42\n" + "1970-01-01T00:01:42.000000Z\t12\t30\n" + "1970-01-01T00:01:43.000000Z\t13\t40\n" + "1970-01-01T00:01:44.000000Z\t14\t50\n");
        });
    }

    @Test
    public void testUpdateSinglePartitionColumnTopAndAroundSparse() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x - 1 as int) x" + " from long_sequence(10))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            // Bump table version
            compile("alter table up add column y long", sqlExecutionContext);
            compile("insert into up select * from " + " (select timestamp_sequence(100000000, 1000000) ts," + " cast(x - 1 as int) + 10 as x," + " cast(x * 10 as long) as y" + " from long_sequence(5))", sqlExecutionContext);

            executeUpdate("UPDATE up SET y = 42 where x = 5 or x = 7 or x = 10 or x = 13 or x = 14");

            assertSql("up", "ts\tx\ty\n" + "1970-01-01T00:00:00.000000Z\t0\tNaN\n" + "1970-01-01T00:00:01.000000Z\t1\tNaN\n" + "1970-01-01T00:00:02.000000Z\t2\tNaN\n" + "1970-01-01T00:00:03.000000Z\t3\tNaN\n" + "1970-01-01T00:00:04.000000Z\t4\tNaN\n" + "1970-01-01T00:00:05.000000Z\t5\t42\n" + "1970-01-01T00:00:06.000000Z\t6\tNaN\n" + "1970-01-01T00:00:07.000000Z\t7\t42\n" + "1970-01-01T00:00:08.000000Z\t8\tNaN\n" + "1970-01-01T00:00:09.000000Z\t9\tNaN\n" + "1970-01-01T00:01:40.000000Z\t10\t42\n" + "1970-01-01T00:01:41.000000Z\t11\t20\n" + "1970-01-01T00:01:42.000000Z\t12\t30\n" + "1970-01-01T00:01:43.000000Z\t13\t42\n" + "1970-01-01T00:01:44.000000Z\t14\t42\n");
        });
    }

    @Test
    public void testUpdateSinglePartitionEmptyColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 100000000) ts," + " cast(x as int) x" + " from long_sequence(10))" + " timestamp(ts) partition by DAY", sqlExecutionContext);
            compile("alter table up add column y long", sqlExecutionContext);

            executeUpdate("UPDATE up SET y = 42 where x = 2 or x = 4 or x = 6 or x = 8 or x = 13");

            assertSql("up", "ts\tx\ty\n" + "1970-01-01T00:00:00.000000Z\t1\tNaN\n" + "1970-01-01T00:01:40.000000Z\t2\t42\n" + "1970-01-01T00:03:20.000000Z\t3\tNaN\n" + "1970-01-01T00:05:00.000000Z\t4\t42\n" + "1970-01-01T00:06:40.000000Z\t5\tNaN\n" + "1970-01-01T00:08:20.000000Z\t6\t42\n" + "1970-01-01T00:10:00.000000Z\t7\tNaN\n" + "1970-01-01T00:11:40.000000Z\t8\t42\n" + "1970-01-01T00:13:20.000000Z\t9\tNaN\n" + "1970-01-01T00:15:00.000000Z\t10\tNaN\n");
        });
    }

    @Test
    public void testUpdateSinglePartitionGapAroundColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x - 1 as int) x" + " from long_sequence(10))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            // Bump table version
            compile("alter table up add column y long", sqlExecutionContext);
            compile("insert into up select * from " + " (select timestamp_sequence(100000000, 1000000) ts," + " cast(x - 1 as int) + 10 as x," + " cast(x * 10 as long) as y" + " from long_sequence(5))", sqlExecutionContext);

            executeUpdate("UPDATE up SET y = 42 where x = 6 or x = 8 or x = 12 or x = 14");

            assertSql("up", "ts\tx\ty\n" + "1970-01-01T00:00:00.000000Z\t0\tNaN\n" + "1970-01-01T00:00:01.000000Z\t1\tNaN\n" + "1970-01-01T00:00:02.000000Z\t2\tNaN\n" + "1970-01-01T00:00:03.000000Z\t3\tNaN\n" + "1970-01-01T00:00:04.000000Z\t4\tNaN\n" + "1970-01-01T00:00:05.000000Z\t5\tNaN\n" + "1970-01-01T00:00:06.000000Z\t6\t42\n" + "1970-01-01T00:00:07.000000Z\t7\tNaN\n" + "1970-01-01T00:00:08.000000Z\t8\t42\n" + "1970-01-01T00:00:09.000000Z\t9\tNaN\n" + "1970-01-01T00:01:40.000000Z\t10\t10\n" + "1970-01-01T00:01:41.000000Z\t11\t20\n" + "1970-01-01T00:01:42.000000Z\t12\t42\n" + "1970-01-01T00:01:43.000000Z\t13\t40\n" + "1970-01-01T00:01:44.000000Z\t14\t42\n");
        });
    }

    @Test
    public void testUpdateStringAndFixedColumnPageSize() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," + " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," + " x as lng2" + " from long_sequence(10)" + " )" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET str1 = concat('questdb', str1), lng2 = -1 WHERE ts > '1970-01-01T08' and lng2 % 2 = 1");

            assertSql("up", "ts\tstr1\tlng2\n" + "1970-01-01T00:00:00.000000Z\t15\t1\n" + "1970-01-01T06:00:00.000000Z\t15\t2\n" + "1970-01-01T12:00:00.000000Z\tquestdb\t-1\n" + "1970-01-01T18:00:00.000000Z\t1\t4\n" + "1970-01-02T00:00:00.000000Z\tquestdb1\t-1\n" + "1970-01-02T06:00:00.000000Z\t1\t6\n" + "1970-01-02T12:00:00.000000Z\tquestdb190232\t-1\n" + "1970-01-02T18:00:00.000000Z\t\t8\n" + "1970-01-03T00:00:00.000000Z\tquestdb15\t-1\n" + "1970-01-03T06:00:00.000000Z\t\t10\n");
        });
    }

    @Test
    public void testUpdateStringColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," + " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," + " x as lng2" + " from long_sequence(10)" + " )" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET str1 = 'questdb' WHERE ts > '1970-01-01T08' and lng2 % 2 = 1");

            assertSql("up", "ts\tstr1\tlng2\n" + "1970-01-01T00:00:00.000000Z\t15\t1\n" + "1970-01-01T06:00:00.000000Z\t15\t2\n" + "1970-01-01T12:00:00.000000Z\tquestdb\t3\n" + "1970-01-01T18:00:00.000000Z\t1\t4\n" + "1970-01-02T00:00:00.000000Z\tquestdb\t5\n" + "1970-01-02T06:00:00.000000Z\t1\t6\n" + "1970-01-02T12:00:00.000000Z\tquestdb\t7\n" + "1970-01-02T18:00:00.000000Z\t\t8\n" + "1970-01-03T00:00:00.000000Z\tquestdb\t9\n" + "1970-01-03T06:00:00.000000Z\t\t10\n");
        });
    }

    @Test
    public void testUpdateStringColumnPageSize() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000L) ts," + " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," + " x as lng2" + " from long_sequence(100000)" + " )" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET str1 = 'questdb' WHERE ts between '1970-01-01T08' and '1970-01-01T12' and lng2 % 2 = 1");
            assertSql("select count() from up where str1 = 'questdb'", "count\n" + "7201\n");
            assertSql("select count() from up where ts between '1970-01-01T08' and '1970-01-01T12' and lng2 % 2 = 1", "count\n" + "7201\n");
        });
    }

    @Test
    public void testUpdateStringColumnUpdate1Value() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 6 * 30 * 1000000L) ts," + " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," + " x as lng2" + " from long_sequence(10)" + " )" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            compile("alter table up add column str2 string", sqlExecutionContext);

            compile("insert into up select * from " + " (select timestamp_sequence('1970-01-01T00:30', 6 * 60 * 1000000L) ts," + " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," + " x + 10 as lng2," + " rnd_str('15', null, '190232', 'rdgb', '', '1') as str2" + " from long_sequence(10))", sqlExecutionContext);

            executeUpdate("UPDATE up SET str1 = 'questdb2', str2 = 'questdb2' WHERE ts = '1970-01-01T01:18:00.000000Z'");

            assertSql("up", "ts\tstr1\tlng2\tstr2\n" + "1970-01-01T00:00:00.000000Z\t15\t1\t\n" + "1970-01-01T00:03:00.000000Z\t15\t2\t\n" + "1970-01-01T00:06:00.000000Z\t\t3\t\n" + "1970-01-01T00:09:00.000000Z\t1\t4\t\n" + "1970-01-01T00:12:00.000000Z\t1\t5\t\n" + "1970-01-01T00:15:00.000000Z\t1\t6\t\n" + "1970-01-01T00:18:00.000000Z\t190232\t7\t\n" + "1970-01-01T00:21:00.000000Z\t\t8\t\n" + "1970-01-01T00:24:00.000000Z\t15\t9\t\n" + "1970-01-01T00:27:00.000000Z\t\t10\t\n" + "1970-01-01T00:30:00.000000Z\t\t11\t190232\n" + "1970-01-01T00:36:00.000000Z\t\t12\t\n" + "1970-01-01T00:42:00.000000Z\t\t13\t15\n" + "1970-01-01T00:48:00.000000Z\t15\t14\t\n" + "1970-01-01T00:54:00.000000Z\trdgb\t15\t\n" + "1970-01-01T01:00:00.000000Z\t\t16\trdgb\n" + "1970-01-01T01:06:00.000000Z\t15\t17\t1\n" + "1970-01-01T01:12:00.000000Z\trdgb\t18\t\n" + "1970-01-01T01:18:00.000000Z\tquestdb2\t19\tquestdb2\n" + "1970-01-01T01:24:00.000000Z\t\t20\t15\n");
        });
    }

    @Test
    public void testUpdateStringColumnWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," + " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," + " x as lng2" + " from long_sequence(10)" + " )" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            compile("alter table up add column str2 string", sqlExecutionContext);
            compile("insert into up select * from " + " (select timestamp_sequence(6*100000000000L, 6 * 60 * 60 * 1000000L) ts," + " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," + " x + 10 as lng2," + " rnd_str('15', null, '190232', 'rdgb', '', '1') as str2" + " from long_sequence(5))", sqlExecutionContext);

            executeUpdate("UPDATE up SET str1 = 'questdb1', str2 = 'questdb2' WHERE lng2 in (6, 8, 10, 12, 14)");

            assertSql("up", "ts\tstr1\tlng2\tstr2\n" + "1970-01-01T00:00:00.000000Z\t15\t1\t\n" + "1970-01-01T06:00:00.000000Z\t15\t2\t\n" + "1970-01-01T12:00:00.000000Z\t\t3\t\n" + "1970-01-01T18:00:00.000000Z\t1\t4\t\n" + "1970-01-02T00:00:00.000000Z\t1\t5\t\n" + "1970-01-02T06:00:00.000000Z\tquestdb1\t6\tquestdb2\n" + "1970-01-02T12:00:00.000000Z\t190232\t7\t\n" + "1970-01-02T18:00:00.000000Z\tquestdb1\t8\tquestdb2\n" + "1970-01-03T00:00:00.000000Z\t15\t9\t\n" + "1970-01-03T06:00:00.000000Z\tquestdb1\t10\tquestdb2\n" + "1970-01-07T22:40:00.000000Z\t\t11\t190232\n" + "1970-01-08T04:40:00.000000Z\tquestdb1\t12\tquestdb2\n" + "1970-01-08T10:40:00.000000Z\t\t13\t15\n" + "1970-01-08T16:40:00.000000Z\tquestdb1\t14\tquestdb2\n" + "1970-01-08T22:40:00.000000Z\trdgb\t15\t\n");
        });
    }

    @Test
    public void testUpdateSymbolWithNotEqualsInWhere() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5)), index(symCol)" + " timestamp(ts)", sqlExecutionContext);

            try (QueryFuture queryFuture = executeUpdateGetFuture("UPDATE up SET symCol = 'VTJ' WHERE symCol != 'WCP'")) {
                Assert.assertEquals(QueryFuture.QUERY_COMPLETE, queryFuture.getStatus());
                Assert.assertEquals(2, queryFuture.getAffectedRowsCount());
            }

            assertSql("up", "symCol\tts\tx\n" + "WCP\t1970-01-01T00:00:00.000000Z\t1\n" + "WCP\t1970-01-01T00:00:01.000000Z\t2\n" + "WCP\t1970-01-01T00:00:02.000000Z\t3\n" + "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" + "VTJ\t1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateTableNameContainsSpace() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table \"віт ер\" as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) x" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE \"віт ер\" SET X = null WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("\"віт ер\"", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t1\n" + "1970-01-01T00:00:01.000000Z\t2\n" + "1970-01-01T00:00:02.000000Z\tNaN\n" + "1970-01-01T00:00:03.000000Z\tNaN\n" + "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateTableWithoutDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) x" + " from long_sequence(5))", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = 12");

            assertSql("up", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t12\n" + "1970-01-01T00:00:01.000000Z\t12\n" + "1970-01-01T00:00:02.000000Z\t12\n" + "1970-01-01T00:00:03.000000Z\t12\n" + "1970-01-01T00:00:04.000000Z\t12\n");
        });
    }

    @Test
    public void testUpdateTimestampFails() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) x" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdateFails("UPDATE up SET ts = 1", 14, "Designated timestamp column cannot be updated");
        });
    }

    @Test
    public void testUpdateTimestampToStringLiteral() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " timestamp_sequence(0, 1000000) ts1" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET ts1 = '1970-02-01' WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("up", "ts\tts1\n" + "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\n" + "1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000000Z\n" + "1970-01-01T00:00:02.000000Z\t1970-02-01T00:00:00.000000Z\n" + "1970-01-01T00:00:03.000000Z\t1970-02-01T00:00:00.000000Z\n" + "1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:04.000000Z\n");
        });
    }

    @Test
    public void testUpdateTimestampToSymbolLiteral() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " timestamp_sequence(0, 1000000) ts1, " + " cast(to_str(timestamp_sequence(1000000, 1000000), 'yyyy-MM-ddTHH:mm:ss.SSSz') as symbol) as sym" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET ts1 = sym WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("up", "ts\tts1\tsym\n" + "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:01.000Z\n" + "1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:02.000Z\n" + "1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:03.000Z\n" + "1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:04.000Z\n" + "1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:05.000Z\n");
        });
    }

    @Test
    public void testUpdateToBindVar() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) x" + " from long_sequence(2))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            sqlExecutionContext.getBindVariableService().setInt(0, 100);
            executeUpdate("UPDATE up SET x = $1 WHERE x > 1 and x < 4");

            assertSql("up", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t1\n" + "1970-01-01T00:00:01.000000Z\t100\n");
        });
    }

    @Test
    public void testUpdateToNull() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) x" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = null WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("up", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t1\n" + "1970-01-01T00:00:01.000000Z\t2\n" + "1970-01-01T00:00:02.000000Z\tNaN\n" + "1970-01-01T00:00:03.000000Z\tNaN\n" + "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateUnsupportedKeyword() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5)), index(symCol)" + " timestamp(ts)", sqlExecutionContext);

            assertSql("up", "symCol\tts\tx\n" + "WCP\t1970-01-01T00:00:00.000000Z\t1\n" + "WCP\t1970-01-01T00:00:01.000000Z\t2\n" + "WCP\t1970-01-01T00:00:02.000000Z\t3\n" + "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" + "\t1970-01-01T00:00:04.000000Z\t5\n");

            compiler.compile("create table t2 as" + " (select rnd_symbol(3,3,3,3) as symCol2, timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5)), index(symCol2)" + " timestamp(ts)", sqlExecutionContext);

            assertSql("t2", "symCol2\tts\tx\n" + "XUX\t1970-01-01T00:00:00.000000Z\t1\n" + "IBB\t1970-01-01T00:00:01.000000Z\t2\n" + "IBB\t1970-01-01T00:00:02.000000Z\t3\n" + "GZS\t1970-01-01T00:00:03.000000Z\t4\n" + "\t1970-01-01T00:00:04.000000Z\t5\n");

            executeUpdateFails("UPDATE up SET symCol = 'VTJ' JOIN t2 ON up.x = t2.x", 29, "FROM, WHERE or EOF expected");
        });
    }

    @Test
    public void testUpdateWith2TableJoinInWithClause() throws Exception {
        assertMemoryLeak(() -> {

            createTablesToJoin("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " rnd_symbol('a', 'b', 'c', null) s," + " x" + " from long_sequence(5))" + " timestamp(ts) partition by DAY");

            executeUpdate("WITH jn AS (select down1.y + down2.y AS sm, down1.s FROM down1 JOIN down2 ON down1.s = down2.s)" + "UPDATE up SET x = sm" + " FROM jn " + " WHERE up.s = jn.s");

            assertSql("up", "ts\ts\tx\n" + "1970-01-01T00:00:00.000000Z\ta\t101\n" + "1970-01-01T00:00:01.000000Z\tc\t2\n" + "1970-01-01T00:00:02.000000Z\tb\t303\n" + "1970-01-01T00:00:03.000000Z\t\t505\n" + "1970-01-01T00:00:04.000000Z\tb\t303\n");
        });
    }

    @Test
    public void testUpdateWithBindVarInWhere() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) x" + " from long_sequence(2))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            sqlExecutionContext.getBindVariableService().setInt(0, 2);
            executeUpdate("UPDATE up SET x = 100 WHERE x < $1");

            assertSql("up", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t100\n" + "1970-01-01T00:00:01.000000Z\t2\n");
        });
    }

    @Test
    public void testUpdateWithFilterAndFunction() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) x," + " x as y" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET y = 10L * x WHERE x > 1 and x < 4");

            assertSql("up", "ts\tx\ty\n" + "1970-01-01T00:00:00.000000Z\t1\t1\n" + "1970-01-01T00:00:01.000000Z\t2\t20\n" + "1970-01-01T00:00:02.000000Z\t3\t30\n" + "1970-01-01T00:00:03.000000Z\t4\t4\n" + "1970-01-01T00:00:04.000000Z\t5\t5\n");
        });
    }

    @Test
    public void testUpdateWithFilterAndFunctionValueUpcast() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " cast(x as int) x," + " x as y" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET y = 10 * x WHERE x > 1 and x < 4");

            assertSql("up", "ts\tx\ty\n" + "1970-01-01T00:00:00.000000Z\t1\t1\n" + "1970-01-01T00:00:01.000000Z\t2\t20\n" + "1970-01-01T00:00:02.000000Z\t3\t30\n" + "1970-01-01T00:00:03.000000Z\t4\t4\n" + "1970-01-01T00:00:04.000000Z\t5\t5\n");
        });
    }

    @Test
    public void testUpdateWithFullCrossJoin() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            compiler.compile("create table down as" + " (select x * 100 as y," + " timestamp_sequence(0, 1000000) ts" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = y" + " FROM down " + " WHERE up.x < 4;");

            assertSql("up", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t100\n" + "1970-01-01T00:00:01.000000Z\t100\n" + "1970-01-01T00:00:02.000000Z\t100\n" + "1970-01-01T00:00:03.000000Z\t4\n" + "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateWithJoin() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            compiler.compile("create table down as" + " (select timestamp_sequence(0, 1000000) ts," + " x * 100 as y" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = y + x" + " FROM down " + " WHERE up.ts = down.ts and x > 1 and x < 4");

            assertSql("up", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t1\n" + "1970-01-01T00:00:01.000000Z\t202\n" + "1970-01-01T00:00:02.000000Z\t303\n" + "1970-01-01T00:00:03.000000Z\t4\n" + "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateWithJoinAndPostJoinFilter() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            compiler.compile("create table down as" + " (select x * 100 as y," + " timestamp_sequence(0, 1000000) ts" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = y" + " FROM down " + " WHERE up.ts = down.ts and up.x < down.y and up.x < 4 and down.y > 100;");

            assertSql("up", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t1\n" + "1970-01-01T00:00:01.000000Z\t200\n" + "1970-01-01T00:00:02.000000Z\t300\n" + "1970-01-01T00:00:03.000000Z\t4\n" + "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateWithJoinNoVirtual() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            compiler.compile("create table down as" + " (select timestamp_sequence(0, 1000000) ts," + " x * 100 as y" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = y" + " FROM down " + " WHERE up.ts = down.ts and x < 4");

            assertSql("up", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t100\n" + "1970-01-01T00:00:01.000000Z\t200\n" + "1970-01-01T00:00:02.000000Z\t300\n" + "1970-01-01T00:00:03.000000Z\t4\n" + "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateWithJoinNotEquals() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select timestamp_sequence(0, 1000000) ts," + " x * 100 as x" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            compiler.compile("create table down as" + " (select x * 50 as y," + " timestamp_sequence(0, 1000000) ts" + " from long_sequence(5))" + " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = y + 1" + " FROM down " + " WHERE up.x < down.y;");

            assertSql("up", "ts\tx\n" + "1970-01-01T00:00:00.000000Z\t151\n" + "1970-01-01T00:00:01.000000Z\t251\n" + "1970-01-01T00:00:02.000000Z\t300\n" + "1970-01-01T00:00:03.000000Z\t400\n" + "1970-01-01T00:00:04.000000Z\t500\n");
        });
    }

    @Test
    public void testUpdateWithJoinUnsupported() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5)), index(symCol)" + " timestamp(ts)", sqlExecutionContext);

            assertSql("up", "symCol\tts\tx\n" + "WCP\t1970-01-01T00:00:00.000000Z\t1\n" + "WCP\t1970-01-01T00:00:01.000000Z\t2\n" + "WCP\t1970-01-01T00:00:02.000000Z\t3\n" + "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" + "\t1970-01-01T00:00:04.000000Z\t5\n");

            compiler.compile("create table t2 as" + " (select rnd_symbol(3,3,3,3) as symCol2, timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5)), index(symCol2)" + " timestamp(ts)", sqlExecutionContext);

            assertSql("t2", "symCol2\tts\tx\n" + "XUX\t1970-01-01T00:00:00.000000Z\t1\n" + "IBB\t1970-01-01T00:00:01.000000Z\t2\n" + "IBB\t1970-01-01T00:00:02.000000Z\t3\n" + "GZS\t1970-01-01T00:00:03.000000Z\t4\n" + "\t1970-01-01T00:00:04.000000Z\t5\n");

            executeUpdateFails("UPDATE up SET symCol = 'VTJ' FROM t2 CROSS JOIN up ON up.x = t2.x", 37, "JOIN is not supported on UPDATE statement");
        });
    }

    @Test
    public void testUpdateWithLatestOnUnsupported() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5)), index(symCol)" + " timestamp(ts)", sqlExecutionContext);

            assertSql("up", "symCol\tts\tx\n" + "WCP\t1970-01-01T00:00:00.000000Z\t1\n" + "WCP\t1970-01-01T00:00:01.000000Z\t2\n" + "WCP\t1970-01-01T00:00:02.000000Z\t3\n" + "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" + "\t1970-01-01T00:00:04.000000Z\t5\n");

            executeUpdateFails("UPDATE up SET symCol = 'ABC' LATEST ON ts PARTITION BY symCol", 29, "FROM, WHERE or EOF expected");
        });
    }

    @Test
    public void testUpdateWithSubSelectUnsupported() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5)), index(symCol)" + " timestamp(ts)", sqlExecutionContext);

            assertSql("up", "symCol\tts\tx\n" + "WCP\t1970-01-01T00:00:00.000000Z\t1\n" + "WCP\t1970-01-01T00:00:01.000000Z\t2\n" + "WCP\t1970-01-01T00:00:02.000000Z\t3\n" + "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" + "\t1970-01-01T00:00:04.000000Z\t5\n");

            compiler.compile("create table t2 as" + " (select rnd_symbol(3,3,3,3) as symCol2, timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5)), index(symCol2)" + " timestamp(ts)", sqlExecutionContext);

            assertSql("t2", "symCol2\tts\tx\n" + "XUX\t1970-01-01T00:00:00.000000Z\t1\n" + "IBB\t1970-01-01T00:00:01.000000Z\t2\n" + "IBB\t1970-01-01T00:00:02.000000Z\t3\n" + "GZS\t1970-01-01T00:00:03.000000Z\t4\n" + "\t1970-01-01T00:00:04.000000Z\t5\n");

            executeUpdateFails("UPDATE up SET symCol = (select symCol2 from t2 where x = 4)", 24, "query is not allowed here");
        });
    }

    @Test
    public void testUpdateWithSymbolJoin() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5)), index(symCol)" + " timestamp(ts)", sqlExecutionContext);

            assertSql("up", "symCol\tts\tx\n" + "WCP\t1970-01-01T00:00:00.000000Z\t1\n" + "WCP\t1970-01-01T00:00:01.000000Z\t2\n" + "WCP\t1970-01-01T00:00:02.000000Z\t3\n" + "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" + "\t1970-01-01T00:00:04.000000Z\t5\n");

            compiler.compile("create table t2 as" + " (select rnd_symbol(3,3,3,3) as symCol2, timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5)), index(symCol2)" + " timestamp(ts)", sqlExecutionContext);

            assertSql("t2", "symCol2\tts\tx\n" + "XUX\t1970-01-01T00:00:00.000000Z\t1\n" + "IBB\t1970-01-01T00:00:01.000000Z\t2\n" + "IBB\t1970-01-01T00:00:02.000000Z\t3\n" + "GZS\t1970-01-01T00:00:03.000000Z\t4\n" + "\t1970-01-01T00:00:04.000000Z\t5\n");

            try (QueryFuture queryFuture = executeUpdateGetFuture("UPDATE up SET symCol = 'VTJ' FROM t2 WHERE up.symCol = t2.symCol2")) {
                Assert.assertEquals(QueryFuture.QUERY_COMPLETE, queryFuture.getStatus());
                Assert.assertEquals(1, queryFuture.getAffectedRowsCount());
            }

            assertSql("up", "symCol\tts\tx\n" + "WCP\t1970-01-01T00:00:00.000000Z\t1\n" + "WCP\t1970-01-01T00:00:01.000000Z\t2\n" + "WCP\t1970-01-01T00:00:02.000000Z\t3\n" + "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" + "VTJ\t1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    private void applyUpdate(UpdateStatement updateStatement) throws SqlException, TableStructureChangesException {
        try (TableWriter tableWriter = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), updateStatement.getTableName(), "UPDATE")) {
            updateStatement.apply(tableWriter);
        }
    }

    private void createTablesToJoin(String createTableSql) throws SqlException {
        compiler.compile(createTableSql, sqlExecutionContext);

        compiler.compile("create table down1 (s symbol index, y int)", sqlExecutionContext);
        executeInsert("insert into down1 values ('a', 1)");
        executeInsert("insert into down1 values ('a', 2)");
        executeInsert("insert into down1 values ('b', 3)");
        executeInsert("insert into down1 values ('b', 4)");
        executeInsert("insert into down1 values (null, 5)");
        executeInsert("insert into down1 values (null, 6)");

        compiler.compile("create table  down2 (s symbol index, y long)", sqlExecutionContext);
        executeInsert("insert into down2 values ('a', 100)");
        executeInsert("insert into down2 values ('b', 300)");
        executeInsert("insert into down2 values (null, 500)");

        // Check what will be in JOIN between down1 and down2
        assertSql("select down1.y + down2.y AS sm, down1.s FROM down1 JOIN down2 ON down1.s = down2.s", "sm\ts\n" + "101\ta\n" + "102\ta\n" + "303\tb\n" + "304\tb\n" + "505\t\n" + "506\t\n");
    }

    private void executeUpdate(String query) throws SqlException {
        CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
        Assert.assertEquals(CompiledQuery.UPDATE, cc.getType());
        try (QueryFuture qf = cc.execute(eventSubSequence)) {
            qf.await();
        }
    }

    private void executeUpdateFails(String sql, int position, String reason) {
        try {
            executeUpdate(sql);
            Assert.fail();
        } catch (SqlException exception) {
            TestUtils.assertContains(exception.getFlyweightMessage(), reason);
            Assert.assertEquals(position, exception.getPosition());
        }
    }

    private QueryFuture executeUpdateGetFuture(String query) throws SqlException {
        CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
        Assert.assertEquals(CompiledQuery.UPDATE, cc.getType());
        return cc.execute(eventSubSequence);
    }

    private void testSymbol_UpdateWithExistingValue(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5))" + (indexed ? ", index(symCol)" : "") + " timestamp(ts)", sqlExecutionContext);

            executeUpdate("update up set symCol = 'VTJ' where symCol = 'WCP'");
            assertSql("up", "symCol\tts\tx\n" + "VTJ\t1970-01-01T00:00:00.000000Z\t1\n" + "VTJ\t1970-01-01T00:00:01.000000Z\t2\n" + "VTJ\t1970-01-01T00:00:02.000000Z\t3\n" + "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" + "\t1970-01-01T00:00:04.000000Z\t5\n");

            try (RecordCursorFactory factory = compiler.compile("up where symCol = 'VTJ'", sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(indexed, cursor.isUsingIndex());
                    TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, TestUtils.printer);
                }
            }
            TestUtils.assertEquals("symCol\tts\tx\n" + "VTJ\t1970-01-01T00:00:00.000000Z\t1\n" + "VTJ\t1970-01-01T00:00:01.000000Z\t2\n" + "VTJ\t1970-01-01T00:00:02.000000Z\t3\n" + "VTJ\t1970-01-01T00:00:03.000000Z\t4\n", sink);

            assertSql("up where symCol = 'WCP'", "symCol\tts\tx\n");
        });
    }

    private void testSymbols_UpdateNull(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5))" + (indexed ? ", index(symCol)" : "") + " timestamp(ts)", sqlExecutionContext);

            executeUpdate("update up set symCol = 'ABC' where symCol is null");
            assertSql("up", "symCol\tts\tx\n" + "WCP\t1970-01-01T00:00:00.000000Z\t1\n" + "WCP\t1970-01-01T00:00:01.000000Z\t2\n" + "WCP\t1970-01-01T00:00:02.000000Z\t3\n" + "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" + "ABC\t1970-01-01T00:00:04.000000Z\t5\n");

            assertSql("up where symCol = 'ABC'", "symCol\tts\tx\n" + "ABC\t1970-01-01T00:00:04.000000Z\t5\n");
            assertSql("up where symCol is null", "symCol\tts\tx\n");
        });
    }

    private void testSymbols_UpdateWithNewValue(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" + " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," + " x" + " from long_sequence(5))" + (indexed ? ", index(symCol)" : "") + " timestamp(ts)", sqlExecutionContext);

            executeUpdate("update up set symCol = 'ABC' where symCol = 'WCP'");
            assertSql("up", "symCol\tts\tx\n" + "ABC\t1970-01-01T00:00:00.000000Z\t1\n" + "ABC\t1970-01-01T00:00:01.000000Z\t2\n" + "ABC\t1970-01-01T00:00:02.000000Z\t3\n" + "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" + "\t1970-01-01T00:00:04.000000Z\t5\n");

            try (RecordCursorFactory factory = compiler.compile("up where symCol = 'ABC'", sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(indexed, cursor.isUsingIndex());
                    TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, TestUtils.printer);
                }
            }
            TestUtils.assertEquals("symCol\tts\tx\n" + "ABC\t1970-01-01T00:00:00.000000Z\t1\n" + "ABC\t1970-01-01T00:00:01.000000Z\t2\n" + "ABC\t1970-01-01T00:00:02.000000Z\t3\n", sink);

            assertSql("up where symCol = 'WCP'", "symCol\tts\tx\n");
        });
    }
}
