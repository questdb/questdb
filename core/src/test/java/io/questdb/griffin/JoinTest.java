/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JoinTest extends AbstractGriffinTest {
    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testAsOfCorrectness() throws Exception {
        assertMemoryLeak(() -> {

            compiler.compile(
                    "create table orders (sym SYMBOL, amount DOUBLE, side BYTE, timestamp TIMESTAMP) timestamp(timestamp)",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table quotes (sym SYMBOL, bid DOUBLE, ask DOUBLE, timestamp TIMESTAMP) timestamp(timestamp)",
                    sqlExecutionContext
            );

            try (
                    TableWriter orders = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "orders", "testing");
                    TableWriter quotes = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "quotes", "testing")
            ) {
                TableWriter.Row rOrders;
                TableWriter.Row rQuotes;

                // quote googl @ 10:00:02
                rQuotes = quotes.newRow(TimestampFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000000Z"));
                rQuotes.putSym(0, "googl");
                rQuotes.putDouble(1, 100.2);
                rQuotes.putDouble(2, 100.3);
                rQuotes.append();

                // quote msft @ 10.00.02.000001
                rQuotes = quotes.newRow(TimestampFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000001Z"));
                rQuotes.putSym(0, "msft");
                rQuotes.putDouble(1, 185.9);
                rQuotes.putDouble(2, 187.3);
                rQuotes.append();

                // quote msft @ 10.00.02.000002
                rQuotes = quotes.newRow(TimestampFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000002Z"));
                rQuotes.putSym(0, "msft");
                rQuotes.putDouble(1, 186.1);
                rQuotes.putDouble(2, 187.8);
                rQuotes.append();

                // order googl @ 10.00.03
                rOrders = orders.newRow(TimestampFormatUtils.parseUTCTimestamp("2018-11-02T10:00:03.000000Z"));
                rOrders.putSym(0, "googl");
                rOrders.putDouble(1, 2000);
                rOrders.putByte(2, (byte) '1');
                rOrders.append();

                // quote msft @ 10.00.03.000001
                rQuotes = quotes.newRow(TimestampFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000002Z"));
                rQuotes.putSym(0, "msft");
                rQuotes.putDouble(1, 183.4);
                rQuotes.putDouble(2, 185.9);
                rQuotes.append();

                rOrders = orders.newRow(TimestampFormatUtils.parseUTCTimestamp("2018-11-02T10:00:04.000000Z"));
                rOrders.putSym(0, "msft");
                rOrders.putDouble(1, 150);
                rOrders.putByte(2, (byte) '1');
                rOrders.append();

                // order googl @ 10.00.05
                rOrders = orders.newRow(TimestampFormatUtils.parseUTCTimestamp("2018-11-02T10:00:05.000000Z"));
                rOrders.putSym(0, "googl");
                rOrders.putDouble(1, 3000);
                rOrders.putByte(2, (byte) '2');
                rOrders.append();

                quotes.commit();
                orders.commit();
            }
        });

        assertQuery(
                "sym\tamount\tside\ttimestamp\tsym1\tbid\task\ttimestamp1\n" +
                        "googl\t2000.0\t49\t2018-11-02T10:00:03.000000Z\tgoogl\t100.2\t100.3\t2018-11-02T10:00:02.000000Z\n" +
                        "msft\t150.0\t49\t2018-11-02T10:00:04.000000Z\tmsft\t183.4\t185.9\t2018-11-02T10:00:02.000002Z\n" +
                        "googl\t3000.0\t50\t2018-11-02T10:00:05.000000Z\tgoogl\t100.2\t100.3\t2018-11-02T10:00:02.000000Z\n",
                "select * from orders asof join quotes on(sym)",
                null,
                "timestamp",
                false,
                true,
                true
        );
    }

    @Test
    public void testAsOfFullFat() throws Exception {
        testFullFat(this::testAsOfJoin);
    }

    @Test
    public void testJoinAliasBug() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table x (xid int, a int, b int)", sqlExecutionContext);
            compiler.compile("create table y (yid int, a int, b int)", sqlExecutionContext);
            compiler.compile(
                    "select tx.a, tx.b from x as tx left join y as ty on xid = yid where tx.a = 1 or tx.b=2",
                    sqlExecutionContext
            ).getRecordCursorFactory().close();
            compiler.compile(
                    "select tx.a, tx.b from x as tx left join y as ty on xid = yid where ty.a = 1 or ty.b=2",
                    sqlExecutionContext
            ).getRecordCursorFactory().close();
        });
    }

    @Test
    public void testAsOfFullFatJoinOnStr() throws Exception {
        testFullFat(() -> assertMemoryLeak(() -> {
            final String query = "select x.i, x.c, y.c, x.amt, price, x.timestamp, y.timestamp, y.m from x asof join y on y.c = x.c";

            compiler.compile(
                    "create table x as (" +
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
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            try {
                compiler.compile(query, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(73, e.getPosition());
                Assert.assertTrue(Chars.contains(e.getMessage(), "right side column 'm' is of unsupported type"));
            }
        }));
    }

    @Test
    public void testAsOfFullFatJoinOnStrNoVar() throws Exception {
        testFullFat(this::testAsOfJoinOnStrNoVar);
    }

    @Test
    public void testAsOfFullFatJoinOnStrSubSelect() throws Exception {
        testFullFat(() -> assertMemoryLeak(() -> {
            final String query = "select x.i, x.c, y.c, x.amt, price, x.timestamp, y.timestamp from x asof join (select c, price, timestamp from y) y on y.c = x.c";

            final String expected = "i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "1\tXYZ\t\t50.938\tNaN\t2018-01-01T00:12:00.000000Z\t\n" +
                    "2\tABC\tABC\t42.281\t0.537\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                    "3\tABC\tABC\t17.371\t0.673\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                    "4\tXYZ\tXYZ\t44.805\t0.116\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                    "5\t\t\t42.956\t0.47700000000000004\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "6\tCDE\tCDE\t82.59700000000001\t0.24\t2018-01-01T01:12:00.000000Z\t2018-01-01T00:40:00.000000Z\n" +
                    "7\tCDE\tCDE\t98.59100000000001\t0.24\t2018-01-01T01:24:00.000000Z\t2018-01-01T00:40:00.000000Z\n" +
                    "8\tABC\tABC\t57.086\t0.59\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:58:00.000000Z\n" +
                    "9\t\t\t81.44200000000001\t0.47700000000000004\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "10\tXYZ\tXYZ\t3.973\t0.867\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:50:00.000000Z\n";

            compiler.compile(
                    "create table x as (" +
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
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            compiler.compile(
                    "insert into x select * from " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str('ABC', 'CDE', null, 'KZZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "insert into y select * from " +
                            "(select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str('ABC', 'CDE', null, 'KZZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQuery("i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1\n" +
                            "1\tXYZ\t\t50.938\tNaN\t2018-01-01T00:12:00.000000Z\t\n" +
                            "2\tABC\tABC\t42.281\t0.537\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                            "3\tABC\tABC\t17.371\t0.673\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                            "4\tXYZ\tXYZ\t44.805\t0.116\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                            "5\t\t\t42.956\t0.47700000000000004\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                            "6\tCDE\tCDE\t82.59700000000001\t0.212\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:12:00.000000Z\n" +
                            "7\tCDE\tCDE\t98.59100000000001\t0.28200000000000003\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:22:00.000000Z\n" +
                            "8\tABC\tABC\t57.086\t0.453\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:16:00.000000Z\n" +
                            "9\t\t\t81.44200000000001\t0.624\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:34:00.000000Z\n" +
                            "10\tXYZ\tXYZ\t3.973\t0.867\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:50:00.000000Z\n" +
                            "11\t\t\t85.019\t0.624\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:34:00.000000Z\n" +
                            "12\tKZZ\tKZZ\t85.49\t0.528\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "13\tCDE\tCDE\t27.493000000000002\t0.401\t2018-01-01T02:36:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "14\tCDE\tCDE\t39.244\t0.401\t2018-01-01T02:48:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "15\tABC\tABC\t55.152\t0.775\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:54:00.000000Z\n" +
                            "16\tKZZ\tKZZ\t3.224\t0.528\t2018-01-01T03:12:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "17\t\t\t6.368\t0.624\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:34:00.000000Z\n" +
                            "18\tCDE\tCDE\t18.305\t0.401\t2018-01-01T03:36:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "19\tABC\tABC\t16.378\t0.775\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:54:00.000000Z\n" +
                            "20\t\t\t4.773\t0.624\t2018-01-01T04:00:00.000000Z\t2018-01-01T01:34:00.000000Z\n",
                    query,
                    "timestamp",
                    false,
                    true
            );

        }));
    }

    @Test
    public void testAsOfJoin() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.sym2 = x.sym";

            final String expected = "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "1\tmsft\t22.463\tNaN\t2018-01-01T00:12:00.000000Z\t\n" +
                    "2\tgoogl\t29.92\t0.885\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                    "3\tmsft\t65.086\t0.5660000000000001\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                    "4\tibm\t98.563\t0.405\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:34:00.000000Z\n" +
                    "5\tmsft\t50.938\t0.545\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                    "6\tibm\t76.11\t0.9540000000000001\t2018-01-01T01:12:00.000000Z\t2018-01-01T00:56:00.000000Z\n" +
                    "7\tmsft\t55.992000000000004\t0.545\t2018-01-01T01:24:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                    "8\tibm\t23.905\t0.9540000000000001\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:56:00.000000Z\n" +
                    "9\tgoogl\t67.786\t0.198\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "10\tgoogl\t38.54\t0.198\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n";

            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table y as (" +
                            "select cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            compiler.compile(
                    "insert into x select * from (" +
                            "select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            compiler.compile(
                    "insert into y select * from (" +
                            "select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQuery("i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                            "1\tmsft\t22.463\tNaN\t2018-01-01T00:12:00.000000Z\t\n" +
                            "2\tgoogl\t29.92\t0.885\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                            "3\tmsft\t65.086\t0.5660000000000001\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                            "4\tibm\t98.563\t0.405\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:34:00.000000Z\n" +
                            "5\tmsft\t50.938\t0.545\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                            "6\tibm\t76.11\t0.337\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:12:00.000000Z\n" +
                            "7\tmsft\t55.992000000000004\t0.226\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:16:00.000000Z\n" +
                            "8\tibm\t23.905\t0.767\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:36:00.000000Z\n" +
                            "9\tgoogl\t67.786\t0.101\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:48:00.000000Z\n" +
                            "10\tgoogl\t38.54\t0.6900000000000001\t2018-01-01T02:00:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "11\tmsft\t68.069\t0.051000000000000004\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                            "12\tmsft\t24.008\t0.051000000000000004\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                            "13\tgoogl\t94.559\t0.6900000000000001\t2018-01-01T02:36:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "14\tibm\t62.474000000000004\t0.068\t2018-01-01T02:48:00.000000Z\t2018-01-01T01:40:00.000000Z\n" +
                            "15\tmsft\t39.017\t0.051000000000000004\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                            "16\tgoogl\t10.643\t0.6900000000000001\t2018-01-01T03:12:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "17\tmsft\t7.246\t0.051000000000000004\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                            "18\tmsft\t36.798\t0.051000000000000004\t2018-01-01T03:36:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                            "19\tmsft\t66.98\t0.051000000000000004\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                            "20\tgoogl\t26.369\t0.6900000000000001\t2018-01-01T04:00:00.000000Z\t2018-01-01T02:00:00.000000Z\n",
                    query,
                    "timestamp",
                    false,
                    true
            );
        });
    }

    @Test
    public void testAsOfJoinAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.sym2 = x.sym";

            final String expected = "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "1\tmsft\t50.938\t0.152\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:10:00.000000Z\n" +
                    "2\tgoogl\t42.281\t0.42\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:18:00.000000Z\n" +
                    "3\tgoogl\t17.371\t0.911\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:30:00.000000Z\n" +
                    "4\tibm\t14.831\t0.54\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                    "5\tgoogl\t86.772\t0.911\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:30:00.000000Z\n" +
                    "6\tmsft\t29.659\t0.08700000000000001\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "7\tgoogl\t7.594\t0.911\t2018-01-01T01:24:00.000000Z\t2018-01-01T00:30:00.000000Z\n" +
                    "8\tibm\t54.253\t0.383\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:56:00.000000Z\n" +
                    "9\tmsft\t62.26\t0.08700000000000001\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "10\tmsft\t50.908\t0.08700000000000001\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n";

            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            compiler.compile(
                    "insert into x select * from " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "insert into y select * from " +
                            "(select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQuery("i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                            "1\tmsft\t50.938\t0.152\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:10:00.000000Z\n" +
                            "2\tgoogl\t42.281\t0.42\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:18:00.000000Z\n" +
                            "3\tgoogl\t17.371\t0.911\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:30:00.000000Z\n" +
                            "4\tibm\t14.831\t0.54\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.911\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:30:00.000000Z\n" +
                            "6\tmsft\t29.659\t0.061\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:06:00.000000Z\n" +
                            "7\tgoogl\t7.594\t0.222\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:14:00.000000Z\n" +
                            "8\tibm\t54.253\t0.47700000000000004\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:36:00.000000Z\n" +
                            "9\tmsft\t62.26\t0.724\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:48:00.000000Z\n" +
                            "10\tmsft\t50.908\t0.209\t2018-01-01T02:00:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "11\tgoogl\t27.493000000000002\t0.26\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "12\tgoogl\t39.244\t0.26\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "13\tgoogl\t56.985\t0.26\t2018-01-01T02:36:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "14\tmsft\t49.758\t0.209\t2018-01-01T02:48:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "15\tmsft\t49.108000000000004\t0.209\t2018-01-01T03:00:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "16\tmsft\t0.132\t0.209\t2018-01-01T03:12:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "17\tibm\t80.48\t0.732\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "18\tmsft\t57.556000000000004\t0.209\t2018-01-01T03:36:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "19\tgoogl\t34.25\t0.26\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "20\tgoogl\t2.6750000000000003\t0.26\t2018-01-01T04:00:00.000000Z\t2018-01-01T01:58:00.000000Z\n",
                    query,
                    "timestamp",
                    false,
                    true
            );
        });
    }

    @Test
    public void testAsOfJoinAllTypesFullFat() throws Exception {
        testFullFat(this::testAsOfJoinNoStrings);
    }

    @Test
    public void testAsOfJoinNoKey() throws Exception {
        assertMemoryLeak(() -> {
            final String query =
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y";

            final String expected = "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "1\tmsft\t50.938\t0.523\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:12:00.000000Z\n" +
                    "2\tgoogl\t42.281\t0.044\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                    "3\tgoogl\t17.371\t0.915\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                    "4\tibm\t14.831\t0.005\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:48:00.000000Z\n" +
                    "5\tgoogl\t86.772\t0.092\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "6\tmsft\t29.659\t0.092\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "7\tgoogl\t7.594\t0.092\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "8\tibm\t54.253\t0.092\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "9\tmsft\t62.26\t0.092\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "10\tmsft\t50.908\t0.092\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n";

            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            compiler.compile(
                    "insert into x select * from " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "insert into y select * from " +
                            "(select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQuery("i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                            "1\tmsft\t50.938\t0.523\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:12:00.000000Z\n" +
                            "2\tgoogl\t42.281\t0.044\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                            "3\tgoogl\t17.371\t0.915\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                            "4\tibm\t14.831\t0.005\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:48:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.092\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                            "6\tmsft\t29.659\t0.544\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:12:00.000000Z\n" +
                            "7\tgoogl\t7.594\t0.036000000000000004\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:24:00.000000Z\n" +
                            "8\tibm\t54.253\t0.544\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:36:00.000000Z\n" +
                            "9\tmsft\t62.26\t0.683\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:48:00.000000Z\n" +
                            "10\tmsft\t50.908\t0.148\t2018-01-01T02:00:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "11\tmsft\t25.604\t0.148\t2018-01-01T02:12:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "12\tgoogl\t89.22\t0.148\t2018-01-01T02:24:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "13\tgoogl\t64.536\t0.148\t2018-01-01T02:36:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "14\tibm\t33.0\t0.148\t2018-01-01T02:48:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "15\tmsft\t67.285\t0.148\t2018-01-01T03:00:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "16\tgoogl\t17.31\t0.148\t2018-01-01T03:12:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "17\tibm\t23.957\t0.148\t2018-01-01T03:24:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "18\tibm\t60.678000000000004\t0.148\t2018-01-01T03:36:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "19\tmsft\t4.727\t0.148\t2018-01-01T03:48:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "20\tgoogl\t26.222\t0.148\t2018-01-01T04:00:00.000000Z\t2018-01-01T02:00:00.000000Z\n",
                    query,
                    "timestamp",
                    false,
                    true
            );
        });
    }

    @Test
    public void testAsOfJoinNoKey3MMaster1MSlave() throws Exception {
        assertMemoryLeak(() -> {
            final String query =
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y";

            final String expected = "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "1\tmsft\t50.938\t0.181\t2018-01-01T00:00:00.000000Z\t2018-01-01T00:00:00.000000Z\n" +
                    "2\tgoogl\t42.281\t0.181\t2018-01-01T00:01:00.000000Z\t2018-01-01T00:00:00.000000Z\n" +
                    "3\tgoogl\t17.371\t0.181\t2018-01-01T00:02:00.000000Z\t2018-01-01T00:00:00.000000Z\n" +
                    "4\tibm\t14.831\t0.27\t2018-01-01T00:03:00.000000Z\t2018-01-01T00:03:00.000000Z\n" +
                    "5\tgoogl\t86.772\t0.27\t2018-01-01T00:04:00.000000Z\t2018-01-01T00:03:00.000000Z\n" +
                    "6\tmsft\t29.659\t0.27\t2018-01-01T00:05:00.000000Z\t2018-01-01T00:03:00.000000Z\n" +
                    "7\tgoogl\t7.594\t0.47300000000000003\t2018-01-01T00:06:00.000000Z\t2018-01-01T00:06:00.000000Z\n" +
                    "8\tibm\t54.253\t0.47300000000000003\t2018-01-01T00:07:00.000000Z\t2018-01-01T00:06:00.000000Z\n" +
                    "9\tmsft\t62.26\t0.47300000000000003\t2018-01-01T00:08:00.000000Z\t2018-01-01T00:06:00.000000Z\n" +
                    "10\tmsft\t50.908\t0.179\t2018-01-01T00:09:00.000000Z\t2018-01-01T00:09:00.000000Z\n" +
                    "11\tmsft\t57.79\t0.179\t2018-01-01T00:10:00.000000Z\t2018-01-01T00:09:00.000000Z\n" +
                    "12\tmsft\t66.121\t0.179\t2018-01-01T00:11:00.000000Z\t2018-01-01T00:09:00.000000Z\n" +
                    "13\tibm\t70.398\t0.6\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:12:00.000000Z\n" +
                    "14\tgoogl\t65.066\t0.6\t2018-01-01T00:13:00.000000Z\t2018-01-01T00:12:00.000000Z\n" +
                    "15\tmsft\t40.863\t0.6\t2018-01-01T00:14:00.000000Z\t2018-01-01T00:12:00.000000Z\n" +
                    "16\tgoogl\t83.861\t0.47800000000000004\t2018-01-01T00:15:00.000000Z\t2018-01-01T00:15:00.000000Z\n" +
                    "17\tibm\t28.627\t0.47800000000000004\t2018-01-01T00:16:00.000000Z\t2018-01-01T00:15:00.000000Z\n" +
                    "18\tibm\t93.163\t0.47800000000000004\t2018-01-01T00:17:00.000000Z\t2018-01-01T00:15:00.000000Z\n" +
                    "19\tibm\t15.121\t0.34900000000000003\t2018-01-01T00:18:00.000000Z\t2018-01-01T00:18:00.000000Z\n" +
                    "20\tgoogl\t62.401\t0.34900000000000003\t2018-01-01T00:19:00.000000Z\t2018-01-01T00:18:00.000000Z\n" +
                    "21\tmsft\t59.651\t0.34900000000000003\t2018-01-01T00:20:00.000000Z\t2018-01-01T00:18:00.000000Z\n" +
                    "22\tgoogl\t70.205\t0.221\t2018-01-01T00:21:00.000000Z\t2018-01-01T00:21:00.000000Z\n" +
                    "23\tibm\t57.257\t0.221\t2018-01-01T00:22:00.000000Z\t2018-01-01T00:21:00.000000Z\n" +
                    "24\tmsft\t23.846\t0.221\t2018-01-01T00:23:00.000000Z\t2018-01-01T00:21:00.000000Z\n" +
                    "25\tmsft\t91.83500000000001\t0.47200000000000003\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                    "26\tibm\t33.0\t0.47200000000000003\t2018-01-01T00:25:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                    "27\tmsft\t67.285\t0.47200000000000003\t2018-01-01T00:26:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                    "28\tgoogl\t17.31\t0.675\t2018-01-01T00:27:00.000000Z\t2018-01-01T00:27:00.000000Z\n" +
                    "29\tibm\t23.957\t0.675\t2018-01-01T00:28:00.000000Z\t2018-01-01T00:27:00.000000Z\n" +
                    "30\tibm\t60.678000000000004\t0.675\t2018-01-01T00:29:00.000000Z\t2018-01-01T00:27:00.000000Z\n";

            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x-1) * 60000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(30)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x-1) * 180000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, "timestamp", true);
        });
    }

    @Test
    public void testAsOfJoinNoKeyEmptySlave() throws Exception {
        assertMemoryLeak(() -> {
            final String query =
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y";

            final String expected = "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "1\tmsft\t50.938\tNaN\t2018-01-01T00:00:00.000000Z\t\n" +
                    "2\tgoogl\t42.281\tNaN\t2018-01-01T00:01:00.000000Z\t\n" +
                    "3\tgoogl\t17.371\tNaN\t2018-01-01T00:02:00.000000Z\t\n" +
                    "4\tibm\t14.831\tNaN\t2018-01-01T00:03:00.000000Z\t\n" +
                    "5\tgoogl\t86.772\tNaN\t2018-01-01T00:04:00.000000Z\t\n" +
                    "6\tmsft\t29.659\tNaN\t2018-01-01T00:05:00.000000Z\t\n" +
                    "7\tgoogl\t7.594\tNaN\t2018-01-01T00:06:00.000000Z\t\n" +
                    "8\tibm\t54.253\tNaN\t2018-01-01T00:07:00.000000Z\t\n" +
                    "9\tmsft\t62.26\tNaN\t2018-01-01T00:08:00.000000Z\t\n" +
                    "10\tmsft\t50.908\tNaN\t2018-01-01T00:09:00.000000Z\t\n" +
                    "11\tmsft\t57.79\tNaN\t2018-01-01T00:10:00.000000Z\t\n" +
                    "12\tmsft\t66.121\tNaN\t2018-01-01T00:11:00.000000Z\t\n" +
                    "13\tibm\t70.398\tNaN\t2018-01-01T00:12:00.000000Z\t\n" +
                    "14\tgoogl\t65.066\tNaN\t2018-01-01T00:13:00.000000Z\t\n" +
                    "15\tmsft\t40.863\tNaN\t2018-01-01T00:14:00.000000Z\t\n" +
                    "16\tgoogl\t83.861\tNaN\t2018-01-01T00:15:00.000000Z\t\n" +
                    "17\tibm\t28.627\tNaN\t2018-01-01T00:16:00.000000Z\t\n" +
                    "18\tibm\t93.163\tNaN\t2018-01-01T00:17:00.000000Z\t\n" +
                    "19\tibm\t15.121\tNaN\t2018-01-01T00:18:00.000000Z\t\n" +
                    "20\tgoogl\t62.401\tNaN\t2018-01-01T00:19:00.000000Z\t\n" +
                    "21\tmsft\t59.651\tNaN\t2018-01-01T00:20:00.000000Z\t\n" +
                    "22\tgoogl\t70.205\tNaN\t2018-01-01T00:21:00.000000Z\t\n" +
                    "23\tibm\t57.257\tNaN\t2018-01-01T00:22:00.000000Z\t\n" +
                    "24\tmsft\t23.846\tNaN\t2018-01-01T00:23:00.000000Z\t\n" +
                    "25\tmsft\t91.83500000000001\tNaN\t2018-01-01T00:24:00.000000Z\t\n" +
                    "26\tibm\t33.0\tNaN\t2018-01-01T00:25:00.000000Z\t\n" +
                    "27\tmsft\t67.285\tNaN\t2018-01-01T00:26:00.000000Z\t\n" +
                    "28\tgoogl\t17.31\tNaN\t2018-01-01T00:27:00.000000Z\t\n" +
                    "29\tibm\t23.957\tNaN\t2018-01-01T00:28:00.000000Z\t\n" +
                    "30\tibm\t60.678000000000004\tNaN\t2018-01-01T00:29:00.000000Z\t\n";

            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x-1) * 60000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(30)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01-01 00:15', 'yyyy-MM-dd HH:mm') + (x-1) * 180000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l" +
                            " from long_sequence(0)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, "timestamp", true);
        });
    }

    @Test
    public void testAsOfJoinNoKeyPartialBottomOverlap() throws Exception {
        assertMemoryLeak(() -> {
            final String query =
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y";

            final String expected = "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "1\tmsft\t50.938\tNaN\t2018-01-01T00:00:00.000000Z\t\n" +
                    "2\tgoogl\t42.281\tNaN\t2018-01-01T00:01:00.000000Z\t\n" +
                    "3\tgoogl\t17.371\tNaN\t2018-01-01T00:02:00.000000Z\t\n" +
                    "4\tibm\t14.831\tNaN\t2018-01-01T00:03:00.000000Z\t\n" +
                    "5\tgoogl\t86.772\tNaN\t2018-01-01T00:04:00.000000Z\t\n" +
                    "6\tmsft\t29.659\tNaN\t2018-01-01T00:05:00.000000Z\t\n" +
                    "7\tgoogl\t7.594\tNaN\t2018-01-01T00:06:00.000000Z\t\n" +
                    "8\tibm\t54.253\tNaN\t2018-01-01T00:07:00.000000Z\t\n" +
                    "9\tmsft\t62.26\tNaN\t2018-01-01T00:08:00.000000Z\t\n" +
                    "10\tmsft\t50.908\tNaN\t2018-01-01T00:09:00.000000Z\t\n" +
                    "11\tmsft\t57.79\tNaN\t2018-01-01T00:10:00.000000Z\t\n" +
                    "12\tmsft\t66.121\tNaN\t2018-01-01T00:11:00.000000Z\t\n" +
                    "13\tibm\t70.398\tNaN\t2018-01-01T00:12:00.000000Z\t\n" +
                    "14\tgoogl\t65.066\tNaN\t2018-01-01T00:13:00.000000Z\t\n" +
                    "15\tmsft\t40.863\tNaN\t2018-01-01T00:14:00.000000Z\t\n" +
                    "16\tgoogl\t83.861\t0.181\t2018-01-01T00:15:00.000000Z\t2018-01-01T00:15:00.000000Z\n" +
                    "17\tibm\t28.627\t0.181\t2018-01-01T00:16:00.000000Z\t2018-01-01T00:15:00.000000Z\n" +
                    "18\tibm\t93.163\t0.181\t2018-01-01T00:17:00.000000Z\t2018-01-01T00:15:00.000000Z\n" +
                    "19\tibm\t15.121\t0.27\t2018-01-01T00:18:00.000000Z\t2018-01-01T00:18:00.000000Z\n" +
                    "20\tgoogl\t62.401\t0.27\t2018-01-01T00:19:00.000000Z\t2018-01-01T00:18:00.000000Z\n" +
                    "21\tmsft\t59.651\t0.27\t2018-01-01T00:20:00.000000Z\t2018-01-01T00:18:00.000000Z\n" +
                    "22\tgoogl\t70.205\t0.47300000000000003\t2018-01-01T00:21:00.000000Z\t2018-01-01T00:21:00.000000Z\n" +
                    "23\tibm\t57.257\t0.47300000000000003\t2018-01-01T00:22:00.000000Z\t2018-01-01T00:21:00.000000Z\n" +
                    "24\tmsft\t23.846\t0.47300000000000003\t2018-01-01T00:23:00.000000Z\t2018-01-01T00:21:00.000000Z\n" +
                    "25\tmsft\t91.83500000000001\t0.179\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                    "26\tibm\t33.0\t0.179\t2018-01-01T00:25:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                    "27\tmsft\t67.285\t0.179\t2018-01-01T00:26:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                    "28\tgoogl\t17.31\t0.6\t2018-01-01T00:27:00.000000Z\t2018-01-01T00:27:00.000000Z\n" +
                    "29\tibm\t23.957\t0.6\t2018-01-01T00:28:00.000000Z\t2018-01-01T00:27:00.000000Z\n" +
                    "30\tibm\t60.678000000000004\t0.6\t2018-01-01T00:29:00.000000Z\t2018-01-01T00:27:00.000000Z\n";

            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x-1) * 60000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(30)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01-01 00:15', 'yyyy-MM-dd HH:mm') + (x-1) * 180000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, "timestamp", true);
        });
    }

    @Test
    public void testAsOfJoinNoKeySlaveAllBelow() throws Exception {
        assertMemoryLeak(() -> {
            final String query =
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y";

            final String expected = "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "1\tmsft\t50.938\tNaN\t2018-01-01T00:00:00.000000Z\t\n" +
                    "2\tgoogl\t42.281\tNaN\t2018-01-01T00:01:00.000000Z\t\n" +
                    "3\tgoogl\t17.371\tNaN\t2018-01-01T00:02:00.000000Z\t\n" +
                    "4\tibm\t14.831\tNaN\t2018-01-01T00:03:00.000000Z\t\n" +
                    "5\tgoogl\t86.772\tNaN\t2018-01-01T00:04:00.000000Z\t\n" +
                    "6\tmsft\t29.659\tNaN\t2018-01-01T00:05:00.000000Z\t\n" +
                    "7\tgoogl\t7.594\tNaN\t2018-01-01T00:06:00.000000Z\t\n" +
                    "8\tibm\t54.253\tNaN\t2018-01-01T00:07:00.000000Z\t\n" +
                    "9\tmsft\t62.26\tNaN\t2018-01-01T00:08:00.000000Z\t\n" +
                    "10\tmsft\t50.908\tNaN\t2018-01-01T00:09:00.000000Z\t\n" +
                    "11\tmsft\t57.79\tNaN\t2018-01-01T00:10:00.000000Z\t\n" +
                    "12\tmsft\t66.121\tNaN\t2018-01-01T00:11:00.000000Z\t\n" +
                    "13\tibm\t70.398\tNaN\t2018-01-01T00:12:00.000000Z\t\n" +
                    "14\tgoogl\t65.066\tNaN\t2018-01-01T00:13:00.000000Z\t\n" +
                    "15\tmsft\t40.863\tNaN\t2018-01-01T00:14:00.000000Z\t\n" +
                    "16\tgoogl\t83.861\tNaN\t2018-01-01T00:15:00.000000Z\t\n" +
                    "17\tibm\t28.627\tNaN\t2018-01-01T00:16:00.000000Z\t\n" +
                    "18\tibm\t93.163\tNaN\t2018-01-01T00:17:00.000000Z\t\n" +
                    "19\tibm\t15.121\tNaN\t2018-01-01T00:18:00.000000Z\t\n" +
                    "20\tgoogl\t62.401\tNaN\t2018-01-01T00:19:00.000000Z\t\n" +
                    "21\tmsft\t59.651\tNaN\t2018-01-01T00:20:00.000000Z\t\n" +
                    "22\tgoogl\t70.205\tNaN\t2018-01-01T00:21:00.000000Z\t\n" +
                    "23\tibm\t57.257\tNaN\t2018-01-01T00:22:00.000000Z\t\n" +
                    "24\tmsft\t23.846\tNaN\t2018-01-01T00:23:00.000000Z\t\n" +
                    "25\tmsft\t91.83500000000001\tNaN\t2018-01-01T00:24:00.000000Z\t\n" +
                    "26\tibm\t33.0\tNaN\t2018-01-01T00:25:00.000000Z\t\n" +
                    "27\tmsft\t67.285\tNaN\t2018-01-01T00:26:00.000000Z\t\n" +
                    "28\tgoogl\t17.31\tNaN\t2018-01-01T00:27:00.000000Z\t\n" +
                    "29\tibm\t23.957\tNaN\t2018-01-01T00:28:00.000000Z\t\n" +
                    "30\tibm\t60.678000000000004\tNaN\t2018-01-01T00:29:00.000000Z\t\n";

            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x-1) * 60000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(30)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01-01 03:00', 'yyyy-MM-dd HH:mm') + (x-1) * 180000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, "timestamp", true);
        });
    }

    @Test
    public void testAsOfJoinNoLeftTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.sym2 = x.sym";
            compiler.compile("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10))", sqlExecutionContext);
            compiler.compile("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)", sqlExecutionContext);

            try {
                compiler.compile(query, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(65, e.getPosition());
                Assert.assertTrue(Chars.contains(e.getFlyweightMessage(), "left"));
            }
        });
    }

    @Test
    public void testAsOfJoinNoRightTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.sym2 = x.sym";
            compiler.compile("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)", sqlExecutionContext);
            compiler.compile("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30))", sqlExecutionContext);

            try {
                compiler.compile(query, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(65, e.getPosition());
                Assert.assertTrue(Chars.contains(e.getFlyweightMessage(), "right"));
            }
        });
    }

    @Test
    public void testAsOfJoinNoSelect() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "x asof join y on y.sym2 = x.sym";

            final String expected = "i\tsym\tamt\ttimestamp\ti1\tsym2\tprice\ttimestamp1\n" +
                    "1\tmsft\t22.463\t2018-01-01T00:12:00.000000Z\tNaN\t\tNaN\t\n" +
                    "2\tgoogl\t29.92\t2018-01-01T00:24:00.000000Z\t12\tgoogl\t0.885\t2018-01-01T00:24:00.000000Z\n" +
                    "3\tmsft\t65.086\t2018-01-01T00:36:00.000000Z\t18\tmsft\t0.5660000000000001\t2018-01-01T00:36:00.000000Z\n" +
                    "4\tibm\t98.563\t2018-01-01T00:48:00.000000Z\t17\tibm\t0.405\t2018-01-01T00:34:00.000000Z\n" +
                    "5\tmsft\t50.938\t2018-01-01T01:00:00.000000Z\t23\tmsft\t0.545\t2018-01-01T00:46:00.000000Z\n" +
                    "6\tibm\t76.11\t2018-01-01T01:12:00.000000Z\t28\tibm\t0.9540000000000001\t2018-01-01T00:56:00.000000Z\n" +
                    "7\tmsft\t55.992000000000004\t2018-01-01T01:24:00.000000Z\t23\tmsft\t0.545\t2018-01-01T00:46:00.000000Z\n" +
                    "8\tibm\t23.905\t2018-01-01T01:36:00.000000Z\t28\tibm\t0.9540000000000001\t2018-01-01T00:56:00.000000Z\n" +
                    "9\tgoogl\t67.786\t2018-01-01T01:48:00.000000Z\t30\tgoogl\t0.198\t2018-01-01T01:00:00.000000Z\n" +
                    "10\tgoogl\t38.54\t2018-01-01T02:00:00.000000Z\t30\tgoogl\t0.198\t2018-01-01T01:00:00.000000Z\n";

            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table y as (" +
                            "select cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            compiler.compile(
                    "insert into x select * from (" +
                            "select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            compiler.compile(
                    "insert into y select * from (" +
                            "select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQuery("i\tsym\tamt\ttimestamp\ti1\tsym2\tprice\ttimestamp1\n" +
                            "1\tmsft\t22.463\t2018-01-01T00:12:00.000000Z\tNaN\t\tNaN\t\n" +
                            "2\tgoogl\t29.92\t2018-01-01T00:24:00.000000Z\t12\tgoogl\t0.885\t2018-01-01T00:24:00.000000Z\n" +
                            "3\tmsft\t65.086\t2018-01-01T00:36:00.000000Z\t18\tmsft\t0.5660000000000001\t2018-01-01T00:36:00.000000Z\n" +
                            "4\tibm\t98.563\t2018-01-01T00:48:00.000000Z\t17\tibm\t0.405\t2018-01-01T00:34:00.000000Z\n" +
                            "5\tmsft\t50.938\t2018-01-01T01:00:00.000000Z\t23\tmsft\t0.545\t2018-01-01T00:46:00.000000Z\n" +
                            "6\tibm\t76.11\t2018-01-01T01:12:00.000000Z\t36\tibm\t0.337\t2018-01-01T01:12:00.000000Z\n" +
                            "7\tmsft\t55.992000000000004\t2018-01-01T01:24:00.000000Z\t38\tmsft\t0.226\t2018-01-01T01:16:00.000000Z\n" +
                            "8\tibm\t23.905\t2018-01-01T01:36:00.000000Z\t48\tibm\t0.767\t2018-01-01T01:36:00.000000Z\n" +
                            "9\tgoogl\t67.786\t2018-01-01T01:48:00.000000Z\t54\tgoogl\t0.101\t2018-01-01T01:48:00.000000Z\n" +
                            "10\tgoogl\t38.54\t2018-01-01T02:00:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z\n" +
                            "11\tmsft\t68.069\t2018-01-01T02:12:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z\n" +
                            "12\tmsft\t24.008\t2018-01-01T02:24:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z\n" +
                            "13\tgoogl\t94.559\t2018-01-01T02:36:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z\n" +
                            "14\tibm\t62.474000000000004\t2018-01-01T02:48:00.000000Z\t50\tibm\t0.068\t2018-01-01T01:40:00.000000Z\n" +
                            "15\tmsft\t39.017\t2018-01-01T03:00:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z\n" +
                            "16\tgoogl\t10.643\t2018-01-01T03:12:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z\n" +
                            "17\tmsft\t7.246\t2018-01-01T03:24:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z\n" +
                            "18\tmsft\t36.798\t2018-01-01T03:36:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z\n" +
                            "19\tmsft\t66.98\t2018-01-01T03:48:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z\n" +
                            "20\tgoogl\t26.369\t2018-01-01T04:00:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z\n",
                    query,
                    "timestamp",
                    false,
                    true
            );
        });
    }

    @Test
    public void testAsOfJoinNoStrings() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.sym2 = x.sym";

            final String expected = "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "1\tmsft\t50.938\t0.523\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:12:00.000000Z\n" +
                    "2\tgoogl\t42.281\t0.215\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:18:00.000000Z\n" +
                    "3\tgoogl\t17.371\t0.915\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                    "4\tibm\t14.831\t0.404\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:42:00.000000Z\n" +
                    "5\tgoogl\t86.772\t0.092\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "6\tmsft\t29.659\t0.537\t2018-01-01T01:12:00.000000Z\t2018-01-01T00:54:00.000000Z\n" +
                    "7\tgoogl\t7.594\t0.092\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "8\tibm\t54.253\t0.404\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:42:00.000000Z\n" +
                    "9\tmsft\t62.26\t0.537\t2018-01-01T01:48:00.000000Z\t2018-01-01T00:54:00.000000Z\n" +
                    "10\tmsft\t50.908\t0.537\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:54:00.000000Z\n";

            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            compiler.compile(
                    "insert into x select * from " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "insert into y select * from " +
                            "(select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQuery("i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                            "1\tmsft\t50.938\t0.523\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:12:00.000000Z\n" +
                            "2\tgoogl\t42.281\t0.215\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:18:00.000000Z\n" +
                            "3\tgoogl\t17.371\t0.915\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                            "4\tibm\t14.831\t0.404\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:42:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.092\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                            "6\tmsft\t29.659\t0.098\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:08:00.000000Z\n" +
                            "7\tgoogl\t7.594\t0.036000000000000004\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:24:00.000000Z\n" +
                            "8\tibm\t54.253\t0.74\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:20:00.000000Z\n" +
                            "9\tmsft\t62.26\t0.032\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:32:00.000000Z\n" +
                            "10\tmsft\t50.908\t0.912\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "11\tmsft\t25.604\t0.912\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "12\tgoogl\t89.22\t0.148\t2018-01-01T02:24:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "13\tgoogl\t64.536\t0.148\t2018-01-01T02:36:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "14\tibm\t33.0\t0.388\t2018-01-01T02:48:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "15\tmsft\t67.285\t0.912\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "16\tgoogl\t17.31\t0.148\t2018-01-01T03:12:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "17\tibm\t23.957\t0.388\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "18\tibm\t60.678000000000004\t0.388\t2018-01-01T03:36:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "19\tmsft\t4.727\t0.912\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "20\tgoogl\t26.222\t0.148\t2018-01-01T04:00:00.000000Z\t2018-01-01T02:00:00.000000Z\n",
                    query,
                    "timestamp",
                    false,
                    true
            );
        });
    }

    @Test
    public void testAsOfJoinNoTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "(x timestamp(timestamp)) x asof join (y timestamp(timestamp)) y on y.sym2 = x.sym";

            final String expected = "i\tsym\tamt\ttimestamp\ti1\tsym2\tprice\ttimestamp1\n" +
                    "1\tmsft\t22.463\t2018-01-01T00:12:00.000000Z\tNaN\t\tNaN\t\n" +
                    "2\tgoogl\t29.92\t2018-01-01T00:24:00.000000Z\t12\tgoogl\t0.885\t2018-01-01T00:24:00.000000Z\n" +
                    "3\tmsft\t65.086\t2018-01-01T00:36:00.000000Z\t18\tmsft\t0.5660000000000001\t2018-01-01T00:36:00.000000Z\n" +
                    "4\tibm\t98.563\t2018-01-01T00:48:00.000000Z\t17\tibm\t0.405\t2018-01-01T00:34:00.000000Z\n" +
                    "5\tmsft\t50.938\t2018-01-01T01:00:00.000000Z\t23\tmsft\t0.545\t2018-01-01T00:46:00.000000Z\n" +
                    "6\tibm\t76.11\t2018-01-01T01:12:00.000000Z\t28\tibm\t0.9540000000000001\t2018-01-01T00:56:00.000000Z\n" +
                    "7\tmsft\t55.992000000000004\t2018-01-01T01:24:00.000000Z\t23\tmsft\t0.545\t2018-01-01T00:46:00.000000Z\n" +
                    "8\tibm\t23.905\t2018-01-01T01:36:00.000000Z\t28\tibm\t0.9540000000000001\t2018-01-01T00:56:00.000000Z\n" +
                    "9\tgoogl\t67.786\t2018-01-01T01:48:00.000000Z\t30\tgoogl\t0.198\t2018-01-01T01:00:00.000000Z\n" +
                    "10\tgoogl\t38.54\t2018-01-01T02:00:00.000000Z\t30\tgoogl\t0.198\t2018-01-01T01:00:00.000000Z\n";

            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ")",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table y as (" +
                            "select cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ")",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            compiler.compile(
                    "insert into x select * from (" +
                            "select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            compiler.compile(
                    "insert into y select * from (" +
                            "select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQuery("i\tsym\tamt\ttimestamp\ti1\tsym2\tprice\ttimestamp1\n" +
                            "1\tmsft\t22.463\t2018-01-01T00:12:00.000000Z\tNaN\t\tNaN\t\n" +
                            "2\tgoogl\t29.92\t2018-01-01T00:24:00.000000Z\t12\tgoogl\t0.885\t2018-01-01T00:24:00.000000Z\n" +
                            "3\tmsft\t65.086\t2018-01-01T00:36:00.000000Z\t18\tmsft\t0.5660000000000001\t2018-01-01T00:36:00.000000Z\n" +
                            "4\tibm\t98.563\t2018-01-01T00:48:00.000000Z\t17\tibm\t0.405\t2018-01-01T00:34:00.000000Z\n" +
                            "5\tmsft\t50.938\t2018-01-01T01:00:00.000000Z\t23\tmsft\t0.545\t2018-01-01T00:46:00.000000Z\n" +
                            "6\tibm\t76.11\t2018-01-01T01:12:00.000000Z\t36\tibm\t0.337\t2018-01-01T01:12:00.000000Z\n" +
                            "7\tmsft\t55.992000000000004\t2018-01-01T01:24:00.000000Z\t38\tmsft\t0.226\t2018-01-01T01:16:00.000000Z\n" +
                            "8\tibm\t23.905\t2018-01-01T01:36:00.000000Z\t48\tibm\t0.767\t2018-01-01T01:36:00.000000Z\n" +
                            "9\tgoogl\t67.786\t2018-01-01T01:48:00.000000Z\t54\tgoogl\t0.101\t2018-01-01T01:48:00.000000Z\n" +
                            "10\tgoogl\t38.54\t2018-01-01T02:00:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z\n" +
                            "11\tmsft\t68.069\t2018-01-01T02:12:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z\n" +
                            "12\tmsft\t24.008\t2018-01-01T02:24:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z\n" +
                            "13\tgoogl\t94.559\t2018-01-01T02:36:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z\n" +
                            "14\tibm\t62.474000000000004\t2018-01-01T02:48:00.000000Z\t50\tibm\t0.068\t2018-01-01T01:40:00.000000Z\n" +
                            "15\tmsft\t39.017\t2018-01-01T03:00:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z\n" +
                            "16\tgoogl\t10.643\t2018-01-01T03:12:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z\n" +
                            "17\tmsft\t7.246\t2018-01-01T03:24:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z\n" +
                            "18\tmsft\t36.798\t2018-01-01T03:36:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z\n" +
                            "19\tmsft\t66.98\t2018-01-01T03:48:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z\n" +
                            "20\tgoogl\t26.369\t2018-01-01T04:00:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z\n",
                    query,
                    "timestamp",
                    false,
                    true
            );
        });
    }

    @Test
    public void testAsOfJoinOnStr() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.c, y.c, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.c = x.c";

            final String expected = "i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "1\tXYZ\t\t50.938\tNaN\t2018-01-01T00:12:00.000000Z\t\n" +
                    "2\tABC\tABC\t42.281\t0.537\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                    "3\tABC\tABC\t17.371\t0.673\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                    "4\tXYZ\tXYZ\t44.805\t0.116\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                    "5\t\t\t42.956\t0.47700000000000004\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "6\tCDE\tCDE\t82.59700000000001\t0.24\t2018-01-01T01:12:00.000000Z\t2018-01-01T00:40:00.000000Z\n" +
                    "7\tCDE\tCDE\t98.59100000000001\t0.24\t2018-01-01T01:24:00.000000Z\t2018-01-01T00:40:00.000000Z\n" +
                    "8\tABC\tABC\t57.086\t0.59\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:58:00.000000Z\n" +
                    "9\t\t\t81.44200000000001\t0.47700000000000004\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "10\tXYZ\tXYZ\t3.973\t0.867\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:50:00.000000Z\n";

            compiler.compile(
                    "create table x as (" +
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
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            compiler.compile(
                    "insert into x select * from " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str('ABC', 'CDE', null, 'KZZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "insert into y select * from " +
                            "(select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str('ABC', 'CDE', null, 'KZZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQuery("i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1\n" +
                            "1\tXYZ\t\t50.938\tNaN\t2018-01-01T00:12:00.000000Z\t\n" +
                            "2\tABC\tABC\t42.281\t0.537\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                            "3\tABC\tABC\t17.371\t0.673\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                            "4\tXYZ\tXYZ\t44.805\t0.116\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                            "5\t\t\t42.956\t0.47700000000000004\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                            "6\tCDE\tCDE\t82.59700000000001\t0.212\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:12:00.000000Z\n" +
                            "7\tCDE\tCDE\t98.59100000000001\t0.28200000000000003\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:22:00.000000Z\n" +
                            "8\tABC\tABC\t57.086\t0.453\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:16:00.000000Z\n" +
                            "9\t\t\t81.44200000000001\t0.624\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:34:00.000000Z\n" +
                            "10\tXYZ\tXYZ\t3.973\t0.867\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:50:00.000000Z\n" +
                            "11\t\t\t85.019\t0.624\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:34:00.000000Z\n" +
                            "12\tKZZ\tKZZ\t85.49\t0.528\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "13\tCDE\tCDE\t27.493000000000002\t0.401\t2018-01-01T02:36:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "14\tCDE\tCDE\t39.244\t0.401\t2018-01-01T02:48:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "15\tABC\tABC\t55.152\t0.775\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:54:00.000000Z\n" +
                            "16\tKZZ\tKZZ\t3.224\t0.528\t2018-01-01T03:12:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "17\t\t\t6.368\t0.624\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:34:00.000000Z\n" +
                            "18\tCDE\tCDE\t18.305\t0.401\t2018-01-01T03:36:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "19\tABC\tABC\t16.378\t0.775\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:54:00.000000Z\n" +
                            "20\t\t\t4.773\t0.624\t2018-01-01T04:00:00.000000Z\t2018-01-01T01:34:00.000000Z\n",
                    query,
                    "timestamp",
                    false,
                    true
            );
        });
    }

    @Test
    public void testAsOfJoinOnStrNoVar() throws Exception {
        // there are no variable length columns in slave table other than the one we join on
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.c, y.c, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.c = x.c";

            final String expected = "i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "1\tXYZ\tXYZ\t50.938\t0.294\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:10:00.000000Z\n" +
                    "2\tABC\tABC\t42.281\t0.167\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:22:00.000000Z\n" +
                    "3\tABC\tABC\t17.371\t0.167\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:22:00.000000Z\n" +
                    "4\tXYZ\tXYZ\t44.805\t0.79\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                    "5\t\t\t42.956\t0.28800000000000003\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:48:00.000000Z\n" +
                    "6\tCDE\tCDE\t82.59700000000001\t0.8200000000000001\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "7\tCDE\tCDE\t98.59100000000001\t0.8200000000000001\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "8\tABC\tABC\t57.086\t0.319\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:38:00.000000Z\n" +
                    "9\t\t\t81.44200000000001\t0.28800000000000003\t2018-01-01T01:48:00.000000Z\t2018-01-01T00:48:00.000000Z\n" +
                    "10\tXYZ\tXYZ\t3.973\t0.16\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:52:00.000000Z\n";

            compiler.compile(
                    "create table x as (" +
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
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            compiler.compile(
                    "insert into x select * from " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str('ABC', 'CDE', null, 'KZZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "insert into y select * from " +
                            "(select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str('ABC', 'CDE', null, 'KZZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQuery("i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1\n" +
                            "1\tXYZ\tXYZ\t50.938\t0.294\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:10:00.000000Z\n" +
                            "2\tABC\tABC\t42.281\t0.167\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:22:00.000000Z\n" +
                            "3\tABC\tABC\t17.371\t0.167\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:22:00.000000Z\n" +
                            "4\tXYZ\tXYZ\t44.805\t0.79\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                            "5\t\t\t42.956\t0.28800000000000003\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:48:00.000000Z\n" +
                            "6\tCDE\tCDE\t82.59700000000001\t0.19\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:06:00.000000Z\n" +
                            "7\tCDE\tCDE\t98.59100000000001\t0.201\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:20:00.000000Z\n" +
                            "8\tABC\tABC\t57.086\t0.359\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:24:00.000000Z\n" +
                            "9\t\t\t81.44200000000001\t0.92\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:48:00.000000Z\n" +
                            "10\tXYZ\tXYZ\t3.973\t0.16\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:52:00.000000Z\n" +
                            "11\tABC\tABC\t22.372\t0.359\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:24:00.000000Z\n" +
                            "12\tABC\tABC\t48.423\t0.359\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:24:00.000000Z\n" +
                            "13\tKZZ\tKZZ\t74.174\t0.853\t2018-01-01T02:36:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "14\t\t\t87.184\t0.46900000000000003\t2018-01-01T02:48:00.000000Z\t2018-01-01T01:52:00.000000Z\n" +
                            "15\tABC\tABC\t66.993\t0.359\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:24:00.000000Z\n" +
                            "16\tABC\tABC\t19.968\t0.359\t2018-01-01T03:12:00.000000Z\t2018-01-01T01:24:00.000000Z\n" +
                            "17\tABC\tABC\t34.368\t0.359\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:24:00.000000Z\n" +
                            "18\t\t\t1.869\t0.46900000000000003\t2018-01-01T03:36:00.000000Z\t2018-01-01T01:52:00.000000Z\n" +
                            "19\tABC\tABC\t85.427\t0.359\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:24:00.000000Z\n" +
                            "20\tABC\tABC\t54.586\t0.359\t2018-01-01T04:00:00.000000Z\t2018-01-01T01:24:00.000000Z\n",
                    query,
                    "timestamp",
                    false,
                    true
            );
        });
    }

    @Test
    public void testAsOfJoinSlaveSymbol() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, sym2, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.sym2 = x.sym";

            final String expected = "i\tsym\tsym2\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "1\tmsft\t\t22.463\tNaN\t2018-01-01T00:12:00.000000Z\t\n" +
                    "2\tgoogl\tgoogl\t29.92\t0.885\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                    "3\tmsft\tmsft\t65.086\t0.5660000000000001\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                    "4\tibm\tibm\t98.563\t0.405\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:34:00.000000Z\n" +
                    "5\tmsft\tmsft\t50.938\t0.545\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                    "6\tibm\tibm\t76.11\t0.9540000000000001\t2018-01-01T01:12:00.000000Z\t2018-01-01T00:56:00.000000Z\n" +
                    "7\tmsft\tmsft\t55.992000000000004\t0.545\t2018-01-01T01:24:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                    "8\tibm\tibm\t23.905\t0.9540000000000001\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:56:00.000000Z\n" +
                    "9\tgoogl\tgoogl\t67.786\t0.198\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "10\tgoogl\tgoogl\t38.54\t0.198\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n";

            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            compiler.compile("insert into x select * from (select cast(x + 10 as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)", sqlExecutionContext);
            compiler.compile("insert into y select * from (select cast(x + 30 as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)", sqlExecutionContext);

            assertQuery("i\tsym\tsym2\tamt\tprice\ttimestamp\ttimestamp1\n" +
                            "1\tmsft\t\t22.463\tNaN\t2018-01-01T00:12:00.000000Z\t\n" +
                            "2\tgoogl\tgoogl\t29.92\t0.885\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z\n" +
                            "3\tmsft\tmsft\t65.086\t0.5660000000000001\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                            "4\tibm\tibm\t98.563\t0.405\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:34:00.000000Z\n" +
                            "5\tmsft\tmsft\t50.938\t0.545\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                            "6\tibm\tibm\t76.11\t0.337\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:12:00.000000Z\n" +
                            "7\tmsft\tmsft\t55.992000000000004\t0.226\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:16:00.000000Z\n" +
                            "8\tibm\tibm\t23.905\t0.767\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:36:00.000000Z\n" +
                            "9\tgoogl\tgoogl\t67.786\t0.101\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:48:00.000000Z\n" +
                            "10\tgoogl\tgoogl\t38.54\t0.6900000000000001\t2018-01-01T02:00:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "11\tmsft\tmsft\t68.069\t0.051000000000000004\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                            "12\tmsft\tmsft\t24.008\t0.051000000000000004\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                            "13\tgoogl\tgoogl\t94.559\t0.6900000000000001\t2018-01-01T02:36:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "14\tibm\tibm\t62.474000000000004\t0.068\t2018-01-01T02:48:00.000000Z\t2018-01-01T01:40:00.000000Z\n" +
                            "15\tmsft\tmsft\t39.017\t0.051000000000000004\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                            "16\tgoogl\tgoogl\t10.643\t0.6900000000000001\t2018-01-01T03:12:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "17\tmsft\tmsft\t7.246\t0.051000000000000004\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                            "18\tmsft\tmsft\t36.798\t0.051000000000000004\t2018-01-01T03:36:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                            "19\tmsft\tmsft\t66.98\t0.051000000000000004\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                            "20\tgoogl\tgoogl\t26.369\t0.6900000000000001\t2018-01-01T04:00:00.000000Z\t2018-01-01T02:00:00.000000Z\n",
                    query,
                    "timestamp",
                    false,
                    true
            );
        });
    }

    @Test
    public void testAsOfSlaveSymbolFullFat() throws Exception {
        testFullFat(this::testAsOfJoinSlaveSymbol);
    }

    @Test
    public void testCrossJoinAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tkk1\ta1\tb1\tc1\td1\te1\tf1\tg1\ti1\tj1\tk1\tl1\tm1\tn1\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t1196016669\tfalse\tW\t0.8828228366697741\t0.7230\t845\t2015-08-26T10:57:26.275Z\tOOZZ\t9029468389542245059\t1970-01-01T00:00:00.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t183633043\ttrue\tB\t0.9441658975532605\t0.3457\t459\t2015-12-23T11:21:02.321Z\t\t-3289070757475856942\t1970-01-01T00:16:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                    "00000010 38 e1\tVLTOVLJ\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t-101516094\tfalse\tG\t0.9820662735672192\t0.5357\t792\t2015-12-04T15:38:03.249Z\tVDZJ\t5703149806881083206\t1970-01-01T00:33:20.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\n" +
                    "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t1\t1196016669\tfalse\tW\t0.8828228366697741\t0.7230\t845\t2015-08-26T10:57:26.275Z\tOOZZ\t9029468389542245059\t1970-01-01T00:00:00.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV\n" +
                    "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t1\t183633043\ttrue\tB\t0.9441658975532605\t0.3457\t459\t2015-12-23T11:21:02.321Z\t\t-3289070757475856942\t1970-01-01T00:16:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                    "00000010 38 e1\tVLTOVLJ\n" +
                    "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t1\t-101516094\tfalse\tG\t0.9820662735672192\t0.5357\t792\t2015-12-04T15:38:03.249Z\tVDZJ\t5703149806881083206\t1970-01-01T00:33:20.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\n";

            compiler.compile(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_int() a," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(2))",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table y as (select" +
                            " cast((x-1)/4 + 1 as int) kk," +
                            " rnd_int() a," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(3))",
                    sqlExecutionContext
            );

            // filter is applied to final join result
            assertQuery(expected, "select * from x cross join y", null, false, true);
        });
    }

    @Test
    public void testCrossJoinNoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "kk\ta\tb\tc\td\te\tf\tg\ti\tj\tl\tm\tn\tkk1\ta1\tb1\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t1196016669\tfalse\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t183633043\ttrue\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t-101516094\tfalse\n" +
                    "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t1\t1196016669\tfalse\n" +
                    "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t1\t183633043\ttrue\n" +
                    "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t1\t-101516094\tfalse\n";
            compiler.compile(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_int() a," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(2)) timestamp(k)",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table y as (select" +
                            " cast((x-1)/4 + 1 as int) kk," +
                            " rnd_int() a," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(3))",
                    sqlExecutionContext
            );

            // filter is applied to final join result
            assertQuery(expected, "select x.kk, x.a, x.b, x.c, x.d, x.e, x.f, x.g, x.i, x.j, x.l, x.m, x.n, y.kk, y.a, y.b from x cross join y", null, false, true);
        });
    }

    @Test
    public void testCrossTripleOverflow() throws Exception {
        assertMemoryLeak(() -> {
            final CompiledQuery cq = compiler.compile("select * from long_sequence(1000000000) a cross join long_sequence(1000000000) b cross join long_sequence(1000000000) c", sqlExecutionContext);
            final RecordCursorFactory factory = cq.getRecordCursorFactory();
            try {
                Assert.assertNotNull(factory);
                sink.clear();
                printer.printHeader(factory.getMetadata(), sink);
                TestUtils.assertEquals("x\tx1\tx2\n", sink);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(Long.MAX_VALUE, cursor.size());
                }
            } finally {
                Misc.free(factory);
            }
        });
    }

    @Test
    public void testCrossJoinTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tkk1\ta1\tb1\tc1\td1\te1\tf1\tg1\ti1\tj1\tk1\tl1\tm1\tn1\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t1196016669\tfalse\tW\t0.8828228366697741\t0.7230\t845\t2015-08-26T10:57:26.275Z\tOOZZ\t9029468389542245059\t1970-01-01T00:00:00.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t183633043\ttrue\tB\t0.9441658975532605\t0.3457\t459\t2015-12-23T11:21:02.321Z\t\t-3289070757475856942\t1970-01-01T00:16:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                    "00000010 38 e1\tVLTOVLJ\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t-101516094\tfalse\tG\t0.9820662735672192\t0.5357\t792\t2015-12-04T15:38:03.249Z\tVDZJ\t5703149806881083206\t1970-01-01T00:33:20.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\n" +
                    "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t1\t1196016669\tfalse\tW\t0.8828228366697741\t0.7230\t845\t2015-08-26T10:57:26.275Z\tOOZZ\t9029468389542245059\t1970-01-01T00:00:00.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV\n" +
                    "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t1\t183633043\ttrue\tB\t0.9441658975532605\t0.3457\t459\t2015-12-23T11:21:02.321Z\t\t-3289070757475856942\t1970-01-01T00:16:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                    "00000010 38 e1\tVLTOVLJ\n" +
                    "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t1\t-101516094\tfalse\tG\t0.9820662735672192\t0.5357\t792\t2015-12-04T15:38:03.249Z\tVDZJ\t5703149806881083206\t1970-01-01T00:33:20.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\n";

            compiler.compile(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_int() a," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(2)) timestamp(k)",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table y as (select" +
                            " cast((x-1)/4 + 1 as int) kk," +
                            " rnd_int() a," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(3))",
                    sqlExecutionContext
            );

            // filter is applied to final join result
            assertQuery(expected, "select * from x cross join y", "k", false, true);
        });
    }

    @Test
    public void testJoinConstantFalse() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "c\ta\tb\tcolumn\n";
            compiler.compile("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(10))", sqlExecutionContext);
            compiler.compile("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))", sqlExecutionContext);

            // master records should be filtered out because slave records missing
            assertQuery(expected, "select x.c, x.a, b, a+b from x join y on y.m = x.c and 1 > 10", null, false, true);
        });
    }

    @Test
    public void testJoinConstantFalseFF() throws Exception {
        testFullFat(this::testJoinConstantFalse);
    }

    @Test
    public void testJoinConstantTrue() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "c\ta\tb\n" +
                    "2\t568\t16\n" +
                    "2\t568\t72\n" +
                    "4\t371\t14\n" +
                    "4\t371\t3\n" +
                    "6\t439\t81\n" +
                    "6\t439\t12\n" +
                    "8\t521\t16\n" +
                    "8\t521\t97\n" +
                    "10\t598\t5\n" +
                    "10\t598\t74\n";

            compiler.compile("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(10))", sqlExecutionContext);
            compiler.compile("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))", sqlExecutionContext);

            // master records should be filtered out because slave records missing
            assertQuery(expected, "select x.c, x.a, b from x join y on y.m = x.c and 1 < 10", null);
        });
    }

    @Test
    public void testJoinConstantTrueFF() throws Exception {
        testFullFat(this::testJoinConstantTrue);
    }

    @Test
    public void testJoinInner() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "c\ta\tb\td\tcolumn\n" +
                    "1\t120\t39\t0\t-39\n" +
                    "1\t120\t39\t50\t11\n" +
                    "1\t120\t42\t0\t-42\n" +
                    "1\t120\t42\t50\t8\n" +
                    "1\t120\t71\t0\t-71\n" +
                    "1\t120\t71\t50\t-21\n" +
                    "1\t120\t6\t0\t-6\n" +
                    "1\t120\t6\t50\t44\n" +
                    "2\t568\t48\t968\t920\n" +
                    "2\t568\t48\t55\t7\n" +
                    "2\t568\t16\t968\t952\n" +
                    "2\t568\t16\t55\t39\n" +
                    "2\t568\t72\t968\t896\n" +
                    "2\t568\t72\t55\t-17\n" +
                    "2\t568\t14\t968\t954\n" +
                    "2\t568\t14\t55\t41\n" +
                    "3\t333\t3\t964\t961\n" +
                    "3\t333\t3\t305\t302\n" +
                    "3\t333\t81\t964\t883\n" +
                    "3\t333\t81\t305\t224\n" +
                    "3\t333\t12\t964\t952\n" +
                    "3\t333\t12\t305\t293\n" +
                    "3\t333\t16\t964\t948\n" +
                    "3\t333\t16\t305\t289\n" +
                    "4\t371\t97\t171\t74\n" +
                    "4\t371\t97\t104\t7\n" +
                    "4\t371\t5\t171\t166\n" +
                    "4\t371\t5\t104\t99\n" +
                    "4\t371\t74\t171\t97\n" +
                    "4\t371\t74\t104\t30\n" +
                    "4\t371\t67\t171\t104\n" +
                    "4\t371\t67\t104\t37\n" +
                    "5\t251\t47\t279\t232\n" +
                    "5\t251\t47\t198\t151\n" +
                    "5\t251\t44\t279\t235\n" +
                    "5\t251\t44\t198\t154\n" +
                    "5\t251\t97\t279\t182\n" +
                    "5\t251\t97\t198\t101\n" +
                    "5\t251\t7\t279\t272\n" +
                    "5\t251\t7\t198\t191\n";

            compiler.compile("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x ts from long_sequence(5)) timestamp(ts)", sqlExecutionContext);
            compiler.compile("create table y as (select cast((x-1)/4 + 1 as int) c, abs(rnd_int() % 100) b from long_sequence(20))", sqlExecutionContext);
            compiler.compile("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))", sqlExecutionContext);

            assertQuery(expected, "select z.c, x.a, b, d, d-b from x join y on(c) join z on (c)", null);
        });
    }

    @Test
    public void testJoinInnerAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tkk1\ta1\tb1\tc1\td1\te1\tf1\tg1\ti1\tj1\tk1\tl1\tm1\tn1\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t1389971928\tfalse\tH\t0.5992548493051852\t0.6456\t632\t2015-01-23T07:09:43.557Z\tPHRI\t-5103414617212558357\t1970-01-01T00:00:00.000000Z\t25\t00000000 6a 71 34 e0 b0 e9 98 f7 67 62 28 60 b0 ec\tLUOHNZH\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t-210935524\tfalse\tL\tNaN\t0.0516\t285\t\tPHRI\t3527911398466283309\t1970-01-01T00:16:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                    "00000010 ea 4e ea 8b\tHTWNWIFFLRBROMNX\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t1180113884\tfalse\tZ\t0.04173263630897883\t0.5677\t16\t2015-11-23T00:35:00.838Z\t\t5953039264407551685\t1970-01-01T00:33:20.000000Z\t24\t00000000 ce 5f b2 8b 5c 54 90 25 c2 20 ff 70 3a c7 8a b3\n" +
                    "00000010 14 cd 47\t\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t2067844108\tfalse\tF\t0.08909442703907178\t0.8439\t111\t2015-11-01T18:55:38.528Z\t\t8798087869168938593\t1970-01-01T00:50:00.000000Z\t15\t00000000 93 e5 57 a5 db a1 76 1c 1c 26 fb 2e 42 fa f5 6e\n" +
                    "00000010 8f 80 e3 54\tLPBNHG\n" +
                    "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t2\t-950108024\tfalse\tC\t0.4729022357373792\t0.7665\t179\t2015-02-08T12:28:36.066Z\t\t7036584259400395476\t1970-01-01T01:06:40.000000Z\t38\t00000000 49 40 44 49 96 cf 2b b3 71 a7 d5\tIGQZVKHT\n" +
                    "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t2\t-779364310\tfalse\t\t0.29150980082006395\tNaN\t277\t2015-02-20T01:54:36.644Z\tPZIM\t-4036499202601723677\t1970-01-01T01:23:20.000000Z\t23\t00000000 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e 44\tNFKPEVMCGFNW\n" +
                    "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t2\t495047580\ttrue\tD\t0.1402258042231984\t0.1105\t433\t2015-09-01T17:07:49.293Z\tPZIM\t-8768558643112932333\t1970-01-01T01:40:00.000000Z\t31\t00000000 4b af 8f 89 df 35 8f da fe 33 98 80 85 20 53 3b\n" +
                    "00000010 51 9d 5d\tENNEBQQEM\n" +
                    "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t2\t-1763054372\tfalse\tX\tNaN\t0.9998\t184\t2015-05-16T03:27:28.517Z\tPHRI\t-8441475391834338900\t1970-01-01T01:56:40.000000Z\t13\t00000000 47 3c e1 72 3b 9d ef c4 4a c9 cf fb 9d 63 ca 94\n" +
                    "00000010 00 6b dd 18\tHGGIWH\n" +
                    "3\t161592763\ttrue\tZ\t0.18769708157331322\t0.1638\t137\t2015-03-12T05:14:11.462Z\t\t7522482991756933150\t1970-01-01T00:33:20.000000Z\t43\t00000000 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3 b3 59\n" +
                    "00000010 8e e5 61 2f\tQOLYXWC\t3\t1159512064\ttrue\tH\t0.8124306844969832\t0.0033\t432\t2015-09-12T17:45:31.519Z\tPZIM\t7964539812331152681\t1970-01-01T02:13:20.000000Z\t8\t\tWLEVMLKC\n" +
                    "3\t161592763\ttrue\tZ\t0.18769708157331322\t0.1638\t137\t2015-03-12T05:14:11.462Z\t\t7522482991756933150\t1970-01-01T00:33:20.000000Z\t43\t00000000 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3 b3 59\n" +
                    "00000010 8e e5 61 2f\tQOLYXWC\t3\t-1751905058\tfalse\tV\t0.8977957942059742\t0.1897\t262\t2015-06-14T03:59:52.156Z\tPZIM\t8231256356538221412\t1970-01-01T02:30:00.000000Z\t13\t\tXFSUWPNXH\n" +
                    "3\t161592763\ttrue\tZ\t0.18769708157331322\t0.1638\t137\t2015-03-12T05:14:11.462Z\t\t7522482991756933150\t1970-01-01T00:33:20.000000Z\t43\t00000000 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3 b3 59\n" +
                    "00000010 8e e5 61 2f\tQOLYXWC\t3\t882350590\ttrue\tZ\tNaN\t0.0331\t575\t2015-08-28T02:22:07.682Z\tPZIM\t-6342128731155487317\t1970-01-01T02:46:40.000000Z\t26\t00000000 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a 0c\tVFDBZW\n" +
                    "3\t161592763\ttrue\tZ\t0.18769708157331322\t0.1638\t137\t2015-03-12T05:14:11.462Z\t\t7522482991756933150\t1970-01-01T00:33:20.000000Z\t43\t00000000 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3 b3 59\n" +
                    "00000010 8e e5 61 2f\tQOLYXWC\t3\t450540087\tfalse\t\tNaN\t0.1354\t932\t\t\t-6426355179359373684\t1970-01-01T03:03:20.000000Z\t30\t\tKVSBEGM\n" +
                    "4\t-1172180184\tfalse\tS\t0.5891216483879789\t0.2820\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\t4\t815018557\tfalse\t\t0.07383464174908916\t0.8791\t187\t\tMFMB\t8725895078168602870\t1970-01-01T03:20:00.000000Z\t36\t\tVLOMPBETTTKRIV\n" +
                    "4\t-1172180184\tfalse\tS\t0.5891216483879789\t0.2820\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\t4\t-682294338\ttrue\tG\t0.9153044839960652\t0.7943\t646\t2015-11-20T14:44:35.439Z\t\t8432832362817764490\t1970-01-01T03:36:40.000000Z\t38\t\tBOSEPGIUQZHEISQH\n" +
                    "4\t-1172180184\tfalse\tS\t0.5891216483879789\t0.2820\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\t4\t-2099411412\ttrue\t\tNaN\tNaN\t119\t2015-09-08T05:51:33.432Z\tMFMB\t8196152051414471878\t1970-01-01T03:53:20.000000Z\t17\t00000000 05 2b 73 51 cf c3 7e c0 1d 6c a9 65 81 ad 79 87\tYWXBBZVRLPT\n" +
                    "4\t-1172180184\tfalse\tS\t0.5891216483879789\t0.2820\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\t4\t-267213623\ttrue\tG\t0.5221781467839528\t0.6246\t263\t2015-07-07T21:30:05.180Z\tNZZR\t6868735889622839219\t1970-01-01T04:10:00.000000Z\t31\t00000000 78 09 1c 5d 88 f5 52 fd 36 02 50\t\n" +
                    "5\t-2088317486\tfalse\tU\t0.7446000371089992\tNaN\t651\t2015-07-18T10:50:24.009Z\tVTJW\t3446015290144635451\t1970-01-01T01:06:40.000000Z\t8\t00000000 92 fe 69 38 e1 77 9a e7 0c 89 14 58\tUMLGLHMLLEOY\t5\t350233248\tfalse\tT\tNaN\tNaN\t542\t2015-10-10T12:23:35.567Z\tMFMB\t7638330131199319038\t1970-01-01T04:26:40.000000Z\t27\t00000000 fd a9 d7 0e 39 5a 28 ed 97 99\tVMKPYV\n" +
                    "5\t-2088317486\tfalse\tU\t0.7446000371089992\tNaN\t651\t2015-07-18T10:50:24.009Z\tVTJW\t3446015290144635451\t1970-01-01T01:06:40.000000Z\t8\t00000000 92 fe 69 38 e1 77 9a e7 0c 89 14 58\tUMLGLHMLLEOY\t5\t1911638855\tfalse\tK\tNaN\t0.3505\t384\t2015-05-09T06:21:47.768Z\t\t-6966377555709737822\t1970-01-01T04:43:20.000000Z\t3\t\t\n" +
                    "5\t-2088317486\tfalse\tU\t0.7446000371089992\tNaN\t651\t2015-07-18T10:50:24.009Z\tVTJW\t3446015290144635451\t1970-01-01T01:06:40.000000Z\t8\t00000000 92 fe 69 38 e1 77 9a e7 0c 89 14 58\tUMLGLHMLLEOY\t5\t-958065826\ttrue\tU\t0.3448217091983955\t0.5708\t1001\t2015-11-04T17:03:03.434Z\tPZIM\t-2022828060719876991\t1970-01-01T05:00:00.000000Z\t42\t00000000 22 35 3b 1c 9c 1d 5c c1 5d 2d 44 ea 00 81 c4 19\n" +
                    "00000010 a1 ec 74 f8\tIFDYPDKOEZBRQ\n" +
                    "5\t-2088317486\tfalse\tU\t0.7446000371089992\tNaN\t651\t2015-07-18T10:50:24.009Z\tVTJW\t3446015290144635451\t1970-01-01T01:06:40.000000Z\t8\t00000000 92 fe 69 38 e1 77 9a e7 0c 89 14 58\tUMLGLHMLLEOY\t5\t77821642\tfalse\tG\t0.22122747948030208\t0.4873\t322\t2015-10-22T18:19:01.452Z\tNZZR\t-4117907293110263427\t1970-01-01T05:16:40.000000Z\t28\t00000000 25 42 67 78 47 b3 80 69 b9 14 d6 fc ee 03 22 81\n" +
                    "00000010 b8 06\tQSPZPBHLNEJ\n";

            compiler.compile(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_int() a," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(5))",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table y as (select" +
                            " cast((x-1)/4 + 1 as int) kk," +
                            " rnd_int() a," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(20))",
                    sqlExecutionContext
            );

            // filter is applied to final join result
            assertQuery(expected, "select * from x join y on (kk)", null);
        });
    }

    @Test
    public void testJoinInnerAllTypesFF() throws Exception {
        testFullFat(this::testJoinInnerAllTypes);
    }

    @Test
    public void testJoinInnerDifferentColumnNames() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "c\ta\tb\td\tcolumn\n" +
                    "1\t120\t39\t0\t-39\n" +
                    "1\t120\t39\t50\t11\n" +
                    "1\t120\t42\t0\t-42\n" +
                    "1\t120\t42\t50\t8\n" +
                    "1\t120\t71\t0\t-71\n" +
                    "1\t120\t71\t50\t-21\n" +
                    "1\t120\t6\t0\t-6\n" +
                    "1\t120\t6\t50\t44\n" +
                    "2\t568\t48\t968\t920\n" +
                    "2\t568\t48\t55\t7\n" +
                    "2\t568\t16\t968\t952\n" +
                    "2\t568\t16\t55\t39\n" +
                    "2\t568\t72\t968\t896\n" +
                    "2\t568\t72\t55\t-17\n" +
                    "2\t568\t14\t968\t954\n" +
                    "2\t568\t14\t55\t41\n" +
                    "3\t333\t3\t964\t961\n" +
                    "3\t333\t3\t305\t302\n" +
                    "3\t333\t81\t964\t883\n" +
                    "3\t333\t81\t305\t224\n" +
                    "3\t333\t12\t964\t952\n" +
                    "3\t333\t12\t305\t293\n" +
                    "3\t333\t16\t964\t948\n" +
                    "3\t333\t16\t305\t289\n" +
                    "4\t371\t97\t171\t74\n" +
                    "4\t371\t97\t104\t7\n" +
                    "4\t371\t5\t171\t166\n" +
                    "4\t371\t5\t104\t99\n" +
                    "4\t371\t74\t171\t97\n" +
                    "4\t371\t74\t104\t30\n" +
                    "4\t371\t67\t171\t104\n" +
                    "4\t371\t67\t104\t37\n" +
                    "5\t251\t47\t279\t232\n" +
                    "5\t251\t47\t198\t151\n" +
                    "5\t251\t44\t279\t235\n" +
                    "5\t251\t44\t198\t154\n" +
                    "5\t251\t97\t279\t182\n" +
                    "5\t251\t97\t198\t101\n" +
                    "5\t251\t7\t279\t272\n" +
                    "5\t251\t7\t198\t191\n";

            compiler.compile("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))", sqlExecutionContext);
            compiler.compile("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))", sqlExecutionContext);
            compiler.compile("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))", sqlExecutionContext);

            assertQuery(expected, "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c)", null);
        });
    }

    @Test
    public void testJoinInnerDifferentColumnNamesFF() throws Exception {
        testFullFat(this::testJoinInnerDifferentColumnNames);
    }

    @Test
    public void testJoinInnerFF() throws Exception {
        testFullFat(this::testJoinInner);
    }

    @Test
    public void testJoinInnerInnerFilter() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "c\ta\tb\td\tcolumn\n" +
                    "1\t120\t6\t0\t-6\n" +
                    "1\t120\t6\t50\t44\n" +
                    "2\t568\t16\t968\t952\n" +
                    "2\t568\t16\t55\t39\n" +
                    "2\t568\t14\t968\t954\n" +
                    "2\t568\t14\t55\t41\n" +
                    "3\t333\t3\t964\t961\n" +
                    "3\t333\t3\t305\t302\n" +
                    "3\t333\t12\t964\t952\n" +
                    "3\t333\t12\t305\t293\n" +
                    "3\t333\t16\t964\t948\n" +
                    "3\t333\t16\t305\t289\n" +
                    "4\t371\t5\t171\t166\n" +
                    "4\t371\t5\t104\t99\n" +
                    "5\t251\t7\t279\t272\n" +
                    "5\t251\t7\t198\t191\n";

            compiler.compile("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))", sqlExecutionContext);
            compiler.compile("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))", sqlExecutionContext);
            compiler.compile("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(16))", sqlExecutionContext);

            // filter is applied to intermediate join result
            assertQueryAndCache(expected, "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where y.b < 20", null, false);

            compiler.compile("insert into x select cast(x+6 as int) c, abs(rnd_int() % 650) a from long_sequence(3)", sqlExecutionContext);
            compiler.compile("insert into y select cast((x+19)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(16)", sqlExecutionContext);
            compiler.compile("insert into z select cast((x+15)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(2)", sqlExecutionContext);

            assertQuery(expected +
                            "7\t253\t14\t228\t214\n" +
                            "7\t253\t14\t723\t709\n" +
                            "8\t431\t0\t348\t348\n" +
                            "8\t431\t0\t790\t790\n" +
                            "9\t100\t19\t667\t648\n" +
                            "9\t100\t19\t456\t437\n" +
                            "9\t100\t8\t667\t659\n" +
                            "9\t100\t8\t456\t448\n",
                    "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where y.b < 20",
                    null);

        });
    }

    @Test
    public void testJoinInnerInnerFilterFF() throws Exception {
        testFullFat(this::testJoinInnerInnerFilter);
    }

    @Test
    public void testJoinInnerLastFilter() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "c\ta\tb\td\tcolumn\n" +
                    "2\t568\t48\t968\t920\n" +
                    "2\t568\t16\t968\t952\n" +
                    "2\t568\t72\t968\t896\n" +
                    "2\t568\t14\t968\t954\n" +
                    "3\t333\t3\t964\t961\n" +
                    "3\t333\t3\t305\t302\n" +
                    "3\t333\t81\t964\t883\n" +
                    "3\t333\t81\t305\t224\n" +
                    "3\t333\t12\t964\t952\n" +
                    "3\t333\t12\t305\t293\n" +
                    "3\t333\t16\t964\t948\n" +
                    "3\t333\t16\t305\t289\n" +
                    "4\t371\t5\t171\t166\n" +
                    "4\t371\t67\t171\t104\n" +
                    "5\t251\t47\t279\t232\n" +
                    "5\t251\t47\t198\t151\n" +
                    "5\t251\t44\t279\t235\n" +
                    "5\t251\t44\t198\t154\n" +
                    "5\t251\t97\t279\t182\n" +
                    "5\t251\t97\t198\t101\n" +
                    "5\t251\t7\t279\t272\n" +
                    "5\t251\t7\t198\t191\n";

            compiler.compile("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))", sqlExecutionContext);
            compiler.compile("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))", sqlExecutionContext);
            compiler.compile("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))", sqlExecutionContext);

            // filter is applied to final join result
            assertQuery(
                    expected,
                    "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where d-b > 100",
                    null
            );
        });
    }

    @Test
    public void testJoinInnerLastFilterFF() throws Exception {
        testFullFat(this::testJoinInnerLastFilter);
    }

    @Test
    public void testJoinInnerLong256AndChar() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "kk\ta\tb\tkk1\ta1\tb1\n" +
                    "1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\n" +
                    "1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0xc718ab5cbb3fd261c1bf6c24be53876861b1a0b0a559551538b73d329210d277\tY\n" +
                    "1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\n" +
                    "1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0x8a538661f350d0b46f06560981acb5496adc00ebd29fdd5373dee145497c5436\tH\n" +
                    "2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\n" +
                    "2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x58dfd08eeb9cc39ecec82869edec121bc2593f82b430328d84a09f29df637e38\tB\n" +
                    "2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x4c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39bccb30ed7795ebc8\tJ\n" +
                    "2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x10bb226eb4243e3683b91ec970b04e788a50f7ff7f6ed3305705e75fe328fa9d\tE\n" +
                    "3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0xbacd57f41b59057caa237cfb02a208e494cfe42988a633de738bab883dc7e332\tU\n" +
                    "3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\n" +
                    "3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0x3ad08d6037d3ce8155c06051ee52138b655f87a3a21d575f610f69efe063fe79\tS\n" +
                    "3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0x4cd64b0b0a344f8e6698c6c186b7571a9cba3ef59083484d98c2d832d83de993\tR\n" +
                    "4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x9a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d645277ee62a5a6e9fb\tZ\n" +
                    "4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x9b27eba5e9cfa1e29660300cea7db540954a62eca44acb2d71660a9b0890a2f0\tJ\n" +
                    "4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0xc736a8b67656c4f159d574d2ff5fb1e3687a84abb7bfac3ebedf29efb28cdcb1\tC\n" +
                    "5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\n" +
                    "5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0xba37e200ad5b17cdada00dc8b85c1bc8a5f80be4b45bf437492990e1a29afcac\tG\n" +
                    "5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0x37b4f6e41fbfd55f587274e3ab1ebd4d6cecb916a1ad092b997918f622d62989\tS\n" +
                    "5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0x3c5d8a6969daa0b37d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46\tE\n";

            compiler.compile(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_long256() a," +
                            " rnd_char() b " +
                            " from long_sequence(5))",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table y as (select" +
                            " cast((x-1)/4 + 1 as int) kk," +
                            " rnd_long256() a," +
                            " rnd_char() b " +
                            " from long_sequence(20))",
                    sqlExecutionContext
            );

            // filter is applied to final join result
            assertQuery(expected, "select * from x join y on (kk)", null);
        });
    }

    @Test
    public void testJoinInnerLong256AndCharAndOrder() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "kk\ta\tb\tkk1\ta1\tb1\n" +
                    "3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0xbacd57f41b59057caa237cfb02a208e494cfe42988a633de738bab883dc7e332\tU\n" +
                    "3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\n" +
                    "3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0x3ad08d6037d3ce8155c06051ee52138b655f87a3a21d575f610f69efe063fe79\tS\n" +
                    "3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0x4cd64b0b0a344f8e6698c6c186b7571a9cba3ef59083484d98c2d832d83de993\tR\n" +
                    "1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\n" +
                    "1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0x8a538661f350d0b46f06560981acb5496adc00ebd29fdd5373dee145497c5436\tH\n" +
                    "1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0xc718ab5cbb3fd261c1bf6c24be53876861b1a0b0a559551538b73d329210d277\tY\n" +
                    "1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\n" +
                    "2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\n" +
                    "2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x10bb226eb4243e3683b91ec970b04e788a50f7ff7f6ed3305705e75fe328fa9d\tE\n" +
                    "2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x4c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39bccb30ed7795ebc8\tJ\n" +
                    "2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x58dfd08eeb9cc39ecec82869edec121bc2593f82b430328d84a09f29df637e38\tB\n" +
                    "4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x9a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d645277ee62a5a6e9fb\tZ\n" +
                    "4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x9b27eba5e9cfa1e29660300cea7db540954a62eca44acb2d71660a9b0890a2f0\tJ\n" +
                    "4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0xc736a8b67656c4f159d574d2ff5fb1e3687a84abb7bfac3ebedf29efb28cdcb1\tC\n" +
                    "4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0xba37e200ad5b17cdada00dc8b85c1bc8a5f80be4b45bf437492990e1a29afcac\tG\n" +
                    "5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\n" +
                    "5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0x37b4f6e41fbfd55f587274e3ab1ebd4d6cecb916a1ad092b997918f622d62989\tS\n" +
                    "5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0x3c5d8a6969daa0b37d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46\tE\n";

            compiler.compile(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_long256() a," +
                            " rnd_char() b " +
                            " from long_sequence(5))",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table y as (select" +
                            " cast((x-1)/4 + 1 as int) kk," +
                            " rnd_long256() a," +
                            " rnd_char() b " +
                            " from long_sequence(20))",
                    sqlExecutionContext
            );

            // filter is applied to final join result
            assertQuery(expected, "select * from x join y on (kk) order by x.a, x.b, y.a", null, true);
        });
    }

    @Test
    public void testJoinInnerNoSlaveRecords() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "c\ta\tb\n" +
                    "2\t568\t16\n" +
                    "2\t568\t72\n" +
                    "4\t371\t14\n" +
                    "4\t371\t3\n" +
                    "6\t439\t81\n" +
                    "6\t439\t12\n" +
                    "8\t521\t16\n" +
                    "8\t521\t97\n" +
                    "10\t598\t5\n" +
                    "10\t598\t74\n";

            compiler.compile("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(10))", sqlExecutionContext);
            compiler.compile("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))", sqlExecutionContext);

            assertQueryAndCache(expected, "select x.c, x.a, b from x join y on y.m = x.c", null, false);

            compiler.compile("insert into x select cast(x+10 as int) c, abs(rnd_int() % 650) a from long_sequence(4)", sqlExecutionContext);
            compiler.compile("insert into y select x, cast(2*((x-1+10)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(6)", sqlExecutionContext);

            assertQuery(expected +
                            "12\t347\t7\n" +
                            "12\t347\t0\n" +
                            "14\t197\t50\n" +
                            "14\t197\t68\n",
                    "select x.c, x.a, b from x join y on y.m = x.c",
                    null);
        });
    }

    @Test
    public void testJoinInnerNoSlaveRecordsFF() throws Exception {
        testFullFat(this::testJoinInnerNoSlaveRecords);
    }

    @Test
    public void testJoinInnerOnSymbol() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "xc\tzc\tyc\ta\tb\td\tcolumn\n" +
                    "A\tA\tA\t568\t12\t319\t307\n" +
                    "A\tA\tA\t568\t12\t456\t444\n" +
                    "A\tA\tA\t568\t12\t263\t251\n" +
                    "A\tA\tA\t568\t74\t319\t245\n" +
                    "A\tA\tA\t568\t74\t456\t382\n" +
                    "A\tA\tA\t568\t74\t263\t189\n" +
                    "A\tA\tA\t568\t71\t319\t248\n" +
                    "A\tA\tA\t568\t71\t456\t385\n" +
                    "A\tA\tA\t568\t71\t263\t192\n" +
                    "A\tA\tA\t568\t54\t319\t265\n" +
                    "A\tA\tA\t568\t54\t456\t402\n" +
                    "A\tA\tA\t568\t54\t263\t209\n" +
                    "B\tB\tB\t371\t72\t842\t770\n" +
                    "B\tB\tB\t371\t72\t703\t631\n" +
                    "B\tB\tB\t371\t72\t933\t861\n" +
                    "B\tB\tB\t371\t72\t667\t595\n" +
                    "B\tB\tB\t371\t72\t467\t395\n" +
                    "B\tB\tB\t371\t97\t842\t745\n" +
                    "B\tB\tB\t371\t97\t703\t606\n" +
                    "B\tB\tB\t371\t97\t933\t836\n" +
                    "B\tB\tB\t371\t97\t667\t570\n" +
                    "B\tB\tB\t371\t97\t467\t370\n" +
                    "B\tB\tB\t371\t97\t842\t745\n" +
                    "B\tB\tB\t371\t97\t703\t606\n" +
                    "B\tB\tB\t371\t97\t933\t836\n" +
                    "B\tB\tB\t371\t97\t667\t570\n" +
                    "B\tB\tB\t371\t97\t467\t370\n" +
                    "B\tB\tB\t371\t79\t842\t763\n" +
                    "B\tB\tB\t371\t79\t703\t624\n" +
                    "B\tB\tB\t371\t79\t933\t854\n" +
                    "B\tB\tB\t371\t79\t667\t588\n" +
                    "B\tB\tB\t371\t79\t467\t388\n" +
                    "B\tB\tB\t439\t72\t842\t770\n" +
                    "B\tB\tB\t439\t72\t703\t631\n" +
                    "B\tB\tB\t439\t72\t933\t861\n" +
                    "B\tB\tB\t439\t72\t667\t595\n" +
                    "B\tB\tB\t439\t72\t467\t395\n" +
                    "B\tB\tB\t439\t97\t842\t745\n" +
                    "B\tB\tB\t439\t97\t703\t606\n" +
                    "B\tB\tB\t439\t97\t933\t836\n" +
                    "B\tB\tB\t439\t97\t667\t570\n" +
                    "B\tB\tB\t439\t97\t467\t370\n" +
                    "B\tB\tB\t439\t97\t842\t745\n" +
                    "B\tB\tB\t439\t97\t703\t606\n" +
                    "B\tB\tB\t439\t97\t933\t836\n" +
                    "B\tB\tB\t439\t97\t667\t570\n" +
                    "B\tB\tB\t439\t97\t467\t370\n" +
                    "B\tB\tB\t439\t79\t842\t763\n" +
                    "B\tB\tB\t439\t79\t703\t624\n" +
                    "B\tB\tB\t439\t79\t933\t854\n" +
                    "B\tB\tB\t439\t79\t667\t588\n" +
                    "B\tB\tB\t439\t79\t467\t388\n" +
                    "\t\t\t521\t3\t8\t5\n" +
                    "\t\t\t521\t3\t2\t-1\n" +
                    "\t\t\t521\t3\t540\t537\n" +
                    "\t\t\t521\t3\t908\t905\n" +
                    "\t\t\t521\t68\t8\t-60\n" +
                    "\t\t\t521\t68\t2\t-66\n" +
                    "\t\t\t521\t68\t540\t472\n" +
                    "\t\t\t521\t68\t908\t840\n" +
                    "\t\t\t521\t69\t8\t-61\n" +
                    "\t\t\t521\t69\t2\t-67\n" +
                    "\t\t\t521\t69\t540\t471\n" +
                    "\t\t\t521\t69\t908\t839\n" +
                    "\t\t\t521\t53\t8\t-45\n" +
                    "\t\t\t521\t53\t2\t-51\n" +
                    "\t\t\t521\t53\t540\t487\n" +
                    "\t\t\t521\t53\t908\t855\n" +
                    "\t\t\t598\t3\t8\t5\n" +
                    "\t\t\t598\t3\t2\t-1\n" +
                    "\t\t\t598\t3\t540\t537\n" +
                    "\t\t\t598\t3\t908\t905\n" +
                    "\t\t\t598\t68\t8\t-60\n" +
                    "\t\t\t598\t68\t2\t-66\n" +
                    "\t\t\t598\t68\t540\t472\n" +
                    "\t\t\t598\t68\t908\t840\n" +
                    "\t\t\t598\t69\t8\t-61\n" +
                    "\t\t\t598\t69\t2\t-67\n" +
                    "\t\t\t598\t69\t540\t471\n" +
                    "\t\t\t598\t69\t908\t839\n" +
                    "\t\t\t598\t53\t8\t-45\n" +
                    "\t\t\t598\t53\t2\t-51\n" +
                    "\t\t\t598\t53\t540\t487\n" +
                    "\t\t\t598\t53\t908\t855\n";

            compiler.compile("create table x as (select rnd_symbol('A','B',null,'D') c, abs(rnd_int() % 650) a from long_sequence(5))", sqlExecutionContext);
            compiler.compile("create table y as (select rnd_symbol('B','A',null,'D') m, abs(rnd_int() % 100) b from long_sequence(20))", sqlExecutionContext);
            compiler.compile("create table z as (select rnd_symbol('D','B',null,'A') c, abs(rnd_int() % 1000) d from long_sequence(16))", sqlExecutionContext);

            // filter is applied to intermediate join result
            assertQueryAndCache(expected, "select x.c xc, z.c zc, y.m yc, x.a, b, d, d-b from x join y on y.m = x.c join z on (c)", null, false);

            compiler.compile("insert into x select rnd_symbol('L','K','P') c, abs(rnd_int() % 650) a from long_sequence(3)", sqlExecutionContext);
            compiler.compile("insert into y select rnd_symbol('P','L','K') m, abs(rnd_int() % 100) b from long_sequence(6)", sqlExecutionContext);
            compiler.compile("insert into z select rnd_symbol('K','P','L') c, abs(rnd_int() % 1000) d from long_sequence(6)", sqlExecutionContext);

            assertQuery(expected +
                            "L\tL\tL\t148\t38\t121\t83\n" +
                            "L\tL\tL\t148\t52\t121\t69\n",
                    "select x.c xc, z.c zc, y.m yc, x.a, b, d, d-b from x join y on y.m = x.c join z on (c)",
                    null);

        });
    }

    @Test
    public void testJoinInnerOnSymbolFF() throws Exception {
        testFullFat(this::testJoinInnerOnSymbol);
    }

    @Test
    public void testJoinInnerPostJoinFilter() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "c\ta\tb\td\tcolumn\n" +
                    "1\t120\t39\t0\t159\n" +
                    "1\t120\t39\t50\t159\n" +
                    "1\t120\t42\t0\t162\n" +
                    "1\t120\t42\t50\t162\n" +
                    "1\t120\t71\t0\t191\n" +
                    "1\t120\t71\t50\t191\n" +
                    "1\t120\t6\t0\t126\n" +
                    "1\t120\t6\t50\t126\n" +
                    "5\t251\t47\t279\t298\n" +
                    "5\t251\t47\t198\t298\n" +
                    "5\t251\t44\t279\t295\n" +
                    "5\t251\t44\t198\t295\n" +
                    "5\t251\t7\t279\t258\n" +
                    "5\t251\t7\t198\t258\n";

            compiler.compile("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))", sqlExecutionContext);
            compiler.compile("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))", sqlExecutionContext);
            compiler.compile("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(16))", sqlExecutionContext);

            // filter is applied to intermediate join result
            assertQueryAndCache(expected, "select z.c, x.a, b, d, a+b from x join y on y.m = x.c join z on (c) where a+b < 300", null, false);

            compiler.compile("insert into x select cast(x+6 as int) c, abs(rnd_int() % 650) a from long_sequence(3)", sqlExecutionContext);
            compiler.compile("insert into y select cast((x+19)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(16)", sqlExecutionContext);
            compiler.compile("insert into z select cast((x+15)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(2)", sqlExecutionContext);

            assertQuery(expected +
                            "7\t253\t35\t228\t288\n" +
                            "7\t253\t35\t723\t288\n" +
                            "7\t253\t14\t228\t267\n" +
                            "7\t253\t14\t723\t267\n" +
                            "9\t100\t63\t667\t163\n" +
                            "9\t100\t63\t456\t163\n" +
                            "9\t100\t19\t667\t119\n" +
                            "9\t100\t19\t456\t119\n" +
                            "9\t100\t38\t667\t138\n" +
                            "9\t100\t38\t456\t138\n" +
                            "9\t100\t8\t667\t108\n" +
                            "9\t100\t8\t456\t108\n",
                    "select z.c, x.a, b, d, a+b from x join y on y.m = x.c join z on (c) where a+b < 300",
                    null);

        });
    }

    @Test
    public void testJoinInnerPostJoinFilterFF() throws Exception {
        testFullFat(this::testJoinInnerPostJoinFilter);
    }

    @Test
    public void testJoinInnerTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "c\ta\tb\td\tcolumn\tts\n" +
                    "1\t120\t39\t0\t-39\t2018-03-01T00:00:00.000001Z\n" +
                    "1\t120\t39\t50\t11\t2018-03-01T00:00:00.000001Z\n" +
                    "1\t120\t42\t0\t-42\t2018-03-01T00:00:00.000001Z\n" +
                    "1\t120\t42\t50\t8\t2018-03-01T00:00:00.000001Z\n" +
                    "1\t120\t71\t0\t-71\t2018-03-01T00:00:00.000001Z\n" +
                    "1\t120\t71\t50\t-21\t2018-03-01T00:00:00.000001Z\n" +
                    "1\t120\t6\t0\t-6\t2018-03-01T00:00:00.000001Z\n" +
                    "1\t120\t6\t50\t44\t2018-03-01T00:00:00.000001Z\n" +
                    "2\t568\t48\t968\t920\t2018-03-01T00:00:00.000002Z\n" +
                    "2\t568\t48\t55\t7\t2018-03-01T00:00:00.000002Z\n" +
                    "2\t568\t16\t968\t952\t2018-03-01T00:00:00.000002Z\n" +
                    "2\t568\t16\t55\t39\t2018-03-01T00:00:00.000002Z\n" +
                    "2\t568\t72\t968\t896\t2018-03-01T00:00:00.000002Z\n" +
                    "2\t568\t72\t55\t-17\t2018-03-01T00:00:00.000002Z\n" +
                    "2\t568\t14\t968\t954\t2018-03-01T00:00:00.000002Z\n" +
                    "2\t568\t14\t55\t41\t2018-03-01T00:00:00.000002Z\n" +
                    "3\t333\t3\t964\t961\t2018-03-01T00:00:00.000003Z\n" +
                    "3\t333\t3\t305\t302\t2018-03-01T00:00:00.000003Z\n" +
                    "3\t333\t81\t964\t883\t2018-03-01T00:00:00.000003Z\n" +
                    "3\t333\t81\t305\t224\t2018-03-01T00:00:00.000003Z\n" +
                    "3\t333\t12\t964\t952\t2018-03-01T00:00:00.000003Z\n" +
                    "3\t333\t12\t305\t293\t2018-03-01T00:00:00.000003Z\n" +
                    "3\t333\t16\t964\t948\t2018-03-01T00:00:00.000003Z\n" +
                    "3\t333\t16\t305\t289\t2018-03-01T00:00:00.000003Z\n" +
                    "4\t371\t97\t171\t74\t2018-03-01T00:00:00.000004Z\n" +
                    "4\t371\t97\t104\t7\t2018-03-01T00:00:00.000004Z\n" +
                    "4\t371\t5\t171\t166\t2018-03-01T00:00:00.000004Z\n" +
                    "4\t371\t5\t104\t99\t2018-03-01T00:00:00.000004Z\n" +
                    "4\t371\t74\t171\t97\t2018-03-01T00:00:00.000004Z\n" +
                    "4\t371\t74\t104\t30\t2018-03-01T00:00:00.000004Z\n" +
                    "4\t371\t67\t171\t104\t2018-03-01T00:00:00.000004Z\n" +
                    "4\t371\t67\t104\t37\t2018-03-01T00:00:00.000004Z\n" +
                    "5\t251\t47\t279\t232\t2018-03-01T00:00:00.000005Z\n" +
                    "5\t251\t47\t198\t151\t2018-03-01T00:00:00.000005Z\n" +
                    "5\t251\t44\t279\t235\t2018-03-01T00:00:00.000005Z\n" +
                    "5\t251\t44\t198\t154\t2018-03-01T00:00:00.000005Z\n" +
                    "5\t251\t97\t279\t182\t2018-03-01T00:00:00.000005Z\n" +
                    "5\t251\t97\t198\t101\t2018-03-01T00:00:00.000005Z\n" +
                    "5\t251\t7\t279\t272\t2018-03-01T00:00:00.000005Z\n" +
                    "5\t251\t7\t198\t191\t2018-03-01T00:00:00.000005Z\n";

            compiler.compile("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x ts from long_sequence(5)) timestamp(ts)", sqlExecutionContext);
            compiler.compile("create table y as (select cast((x-1)/4 + 1 as int) c, abs(rnd_int() % 100) b from long_sequence(20))", sqlExecutionContext);
            compiler.compile("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))", sqlExecutionContext);

            assertQuery(expected, "select z.c, x.a, b, d, d-b, ts from x join y on(c) join z on (c)", "ts");
        });
    }

    @Test
    public void testJoinOnLong256() throws Exception {
        testFullFat(() -> assertMemoryLeak(() -> {
            final String query = "select x.i, y.i, x.hash from x join x y on y.hash = x.hash";

            final String expected = "i\ti1\thash\n" +
                    "1\t1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\n" +
                    "2\t2\t0xb5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa65572a215ba0462ad15\n" +
                    "3\t3\t0x322a2198864beb14797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fa\n";

            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_long256() hash" +
                            " from long_sequence(3)" +
                            ")",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, null, false);
        }));
    }

    @Test
    public void testJoinOuterAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tkk1\ta1\tb1\tc1\td1\te1\tf1\tg1\ti1\tj1\tk1\tl1\tm1\tn1\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tNaN\tNaN\tfalse\t\tNaN\tNaN\t0\t\t\tNaN\t\t0\t\t\n" +
                    "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t2\t415709351\tfalse\tM\t0.5626370294064983\t0.7653\t712\t\tGGLN\t6235849401126045090\t1970-01-01T00:00:00.000000Z\t36\t00000000 62 e1 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43\tIZJSVTNP\n" +
                    "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t2\t1704158532\tfalse\tN\t0.43493246663794993\t0.9612\t344\t2015-09-09T21:39:05.530Z\tHHIU\t-4645139889518544281\t1970-01-01T00:16:40.000000Z\t47\t\tGGIJYDV\n" +
                    "3\t161592763\ttrue\tZ\t0.18769708157331322\t0.1638\t137\t2015-03-12T05:14:11.462Z\t\t7522482991756933150\t1970-01-01T00:33:20.000000Z\t43\t00000000 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3 b3 59\n" +
                    "00000010 8e e5 61 2f\tQOLYXWC\tNaN\tNaN\tfalse\t\tNaN\tNaN\t0\t\t\tNaN\t\t0\t\t\n" +
                    "4\t-1172180184\tfalse\tS\t0.5891216483879789\t0.2820\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\t4\t325316\tfalse\tG\t0.27068535446692277\t0.0031\t809\t2015-02-24T12:10:43.199Z\t\t-4990885278588247665\t1970-01-01T00:33:20.000000Z\t8\t00000000 98 80 85 20 53 3b 51 9d 5d 28 ac 02 2e fe\tQQEMXDKXEJCTIZ\n" +
                    "4\t-1172180184\tfalse\tS\t0.5891216483879789\t0.2820\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\t4\t263487884\ttrue\t\tNaN\t0.9483\t59\t2015-01-20T06:18:18.583Z\t\t-5873213601796545477\t1970-01-01T00:50:00.000000Z\t26\t00000000 4a c9 cf fb 9d 63 ca 94 00 6b dd\tHHGGIWH\n" +
                    "5\t-2088317486\tfalse\tU\t0.7446000371089992\tNaN\t651\t2015-07-18T10:50:24.009Z\tVTJW\t3446015290144635451\t1970-01-01T01:06:40.000000Z\t8\t00000000 92 fe 69 38 e1 77 9a e7 0c 89 14 58\tUMLGLHMLLEOY\tNaN\tNaN\tfalse\t\tNaN\tNaN\t0\t\t\tNaN\t\t0\t\t\n" +
                    "6\t1431425139\tfalse\t\t0.30716667810043663\t0.4275\t181\t2015-07-26T11:59:20.003Z\t\t-8546113611224784332\t1970-01-01T01:23:20.000000Z\t11\t00000000 d8 57 91 88 28 a5 18 93 bd 0b\tJOXPKRGIIHYH\t6\t1159512064\ttrue\tH\t0.8124306844969832\t0.0033\t432\t2015-09-12T17:45:31.519Z\tHHIU\t7964539812331152681\t1970-01-01T01:06:40.000000Z\t8\t\tWLEVMLKC\n" +
                    "6\t1431425139\tfalse\t\t0.30716667810043663\t0.4275\t181\t2015-07-26T11:59:20.003Z\t\t-8546113611224784332\t1970-01-01T01:23:20.000000Z\t11\t00000000 d8 57 91 88 28 a5 18 93 bd 0b\tJOXPKRGIIHYH\t6\t-1751905058\tfalse\tV\t0.8977957942059742\t0.1897\t262\t2015-06-14T03:59:52.156Z\tHHIU\t8231256356538221412\t1970-01-01T01:23:20.000000Z\t13\t\tXFSUWPNXH\n" +
                    "7\t-2077041000\ttrue\tM\t0.7340656260730631\t0.5026\t345\t2015-02-16T05:23:30.407Z\t\t-8534688874718947140\t1970-01-01T01:40:00.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE\tNaN\tNaN\tfalse\t\tNaN\tNaN\t0\t\t\tNaN\t\t0\t\t\n" +
                    "8\t-1234141625\tfalse\tC\t0.06381657870188628\t0.7606\t397\t2015-02-14T21:43:16.924Z\tHYRX\t-8888027247206813045\t1970-01-01T01:56:40.000000Z\t10\t00000000 b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4\tUIZULIGYVFZFK\t8\t882350590\ttrue\tZ\tNaN\t0.0331\t575\t2015-08-28T02:22:07.682Z\tHHIU\t-6342128731155487317\t1970-01-01T01:40:00.000000Z\t26\t00000000 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a 0c\tVFDBZW\n" +
                    "8\t-1234141625\tfalse\tC\t0.06381657870188628\t0.7606\t397\t2015-02-14T21:43:16.924Z\tHYRX\t-8888027247206813045\t1970-01-01T01:56:40.000000Z\t10\t00000000 b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4\tUIZULIGYVFZFK\t8\t450540087\tfalse\t\tNaN\t0.1354\t932\t\t\t-6426355179359373684\t1970-01-01T01:56:40.000000Z\t30\t\tKVSBEGM\n" +
                    "9\t976011946\ttrue\tU\t0.24001459007748394\t0.9292\t379\t\tVTJW\t3820631780839257855\t1970-01-01T02:13:20.000000Z\t12\t00000000 8a b3 14 cd 47 0b 0c 39 12 f7 05 10 f4\tGMXUKLGMXSLUQDYO\tNaN\tNaN\tfalse\t\tNaN\tNaN\t0\t\t\tNaN\t\t0\t\t\n" +
                    "10\t-1915752164\tfalse\tI\t0.8786111112537701\t0.9966\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:30:00.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t\t10\t815018557\tfalse\t\t0.07383464174908916\t0.8791\t187\t\tYRZL\t8725895078168602870\t1970-01-01T02:13:20.000000Z\t36\t\tVLOMPBETTTKRIV\n" +
                    "10\t-1915752164\tfalse\tI\t0.8786111112537701\t0.9966\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:30:00.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t\t10\t-682294338\ttrue\tG\t0.9153044839960652\t0.7943\t646\t2015-11-20T14:44:35.439Z\t\t8432832362817764490\t1970-01-01T02:30:00.000000Z\t38\t\tBOSEPGIUQZHEISQH\n";

            compiler.compile(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_int() a," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10))",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table y as (select" +
                            " cast(2*((x-1)/2) as int)+2 kk," +
                            " rnd_int() a," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10))",
                    sqlExecutionContext
            );

            // filter is applied to final join result
            assertQuery(
                    expected,
                    "select * from x outer join y on (kk)",
                    null
            );
        });
    }

    @Test
    public void testJoinOuterAllTypesFF() throws Exception {
        testFullFat(this::testJoinOuterAllTypes);
    }

    @Test
    public void testJoinOuterLong256AndChar() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "kk\ta\tb\tkk1\ta1\tb1\n" +
                    "1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\tNaN\t\t\n" +
                    "2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x58dfd08eeb9cc39ecec82869edec121bc2593f82b430328d84a09f29df637e38\tB\n" +
                    "2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x4c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39bccb30ed7795ebc8\tJ\n" +
                    "3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\tNaN\t\t\n" +
                    "4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x10bb226eb4243e3683b91ec970b04e788a50f7ff7f6ed3305705e75fe328fa9d\tE\n" +
                    "4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0xbacd57f41b59057caa237cfb02a208e494cfe42988a633de738bab883dc7e332\tU\n" +
                    "5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\tNaN\t\t\n" +
                    "6\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t6\t0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\n" +
                    "6\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t6\t0x3ad08d6037d3ce8155c06051ee52138b655f87a3a21d575f610f69efe063fe79\tS\n" +
                    "7\t0xc718ab5cbb3fd261c1bf6c24be53876861b1a0b0a559551538b73d329210d277\tY\tNaN\t\t\n" +
                    "8\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t8\t0x4cd64b0b0a344f8e6698c6c186b7571a9cba3ef59083484d98c2d832d83de993\tR\n" +
                    "8\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t8\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "9\t0x8a538661f350d0b46f06560981acb5496adc00ebd29fdd5373dee145497c5436\tH\tNaN\t\t\n" +
                    "10\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t10\t0x9a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d645277ee62a5a6e9fb\tZ\n" +
                    "10\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t10\t0x9b27eba5e9cfa1e29660300cea7db540954a62eca44acb2d71660a9b0890a2f0\tJ\n";

            compiler.compile(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_long256() a," +
                            " rnd_char() b" +
                            " from long_sequence(10))",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table y as (select" +
                            " cast(2*((x-1)/2) as int)+2 kk," +
                            " rnd_long256() a," +
                            " rnd_char() b" +
                            " from long_sequence(10))",
                    sqlExecutionContext
            );

            // filter is applied to final join result
            assertQuery(
                    expected,
                    "select * from x outer join y on (kk)",
                    null
            );
        });
    }

    @Test
    public void testJoinOuterLong256AndCharAndOrder() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "kk\ta\tb\tkk1\ta1\tb1\n" +
                    "8\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t8\t0x4cd64b0b0a344f8e6698c6c186b7571a9cba3ef59083484d98c2d832d83de993\tR\n" +
                    "8\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t8\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\tNaN\t\t\n" +
                    "4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0xbacd57f41b59057caa237cfb02a208e494cfe42988a633de738bab883dc7e332\tU\n" +
                    "4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x10bb226eb4243e3683b91ec970b04e788a50f7ff7f6ed3305705e75fe328fa9d\tE\n" +
                    "2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x4c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39bccb30ed7795ebc8\tJ\n" +
                    "2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x58dfd08eeb9cc39ecec82869edec121bc2593f82b430328d84a09f29df637e38\tB\n" +
                    "7\t0xc718ab5cbb3fd261c1bf6c24be53876861b1a0b0a559551538b73d329210d277\tY\tNaN\t\t\n" +
                    "1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\tNaN\t\t\n" +
                    "10\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t10\t0x9a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d645277ee62a5a6e9fb\tZ\n" +
                    "10\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t10\t0x9b27eba5e9cfa1e29660300cea7db540954a62eca44acb2d71660a9b0890a2f0\tJ\n" +
                    "3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\tNaN\t\t\n" +
                    "9\t0x8a538661f350d0b46f06560981acb5496adc00ebd29fdd5373dee145497c5436\tH\tNaN\t\t\n" +
                    "6\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t6\t0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\n" +
                    "6\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t6\t0x3ad08d6037d3ce8155c06051ee52138b655f87a3a21d575f610f69efe063fe79\tS\n";

            compiler.compile(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_long256() a," +
                            " rnd_char() b" +
                            " from long_sequence(10))",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table y as (select" +
                            " cast(2*((x-1)/2) as int)+2 kk," +
                            " rnd_long256() a," +
                            " rnd_char() b" +
                            " from long_sequence(10))",
                    sqlExecutionContext
            );

            // filter is applied to final join result
            assertQuery(
                    expected,
                    "select * from x outer join y on (kk) order by x.a desc, y.a",
                    null,
                    true
            );
        });
    }

    @Test
    public void testJoinOuterNoSlaveRecords() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "c\ta\tb\n" +
                    "1\t120\tNaN\n" +
                    "2\t568\t16\n" +
                    "2\t568\t72\n" +
                    "3\t333\tNaN\n" +
                    "4\t371\t14\n" +
                    "4\t371\t3\n" +
                    "5\t251\tNaN\n" +
                    "6\t439\t81\n" +
                    "6\t439\t12\n" +
                    "7\t42\tNaN\n" +
                    "8\t521\t16\n" +
                    "8\t521\t97\n" +
                    "9\t356\tNaN\n" +
                    "10\t598\t5\n" +
                    "10\t598\t74\n";

            compiler.compile("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x ts from long_sequence(10)) timestamp(ts)", sqlExecutionContext);
            compiler.compile("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))", sqlExecutionContext);

            // master records should be filtered out because slave records missing
            assertQueryAndCache(expected, "select x.c, x.a, b from x outer join y on y.m = x.c", null, false);

            compiler.compile("insert into x select * from (select cast(x+10 as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x + 10 ts from long_sequence(4)) timestamp(ts)", sqlExecutionContext);
            compiler.compile("insert into y select x, cast(2*((x-1+10)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(6)", sqlExecutionContext);

            assertQuery(expected +
                            "11\t467\tNaN\n" +
                            "12\t347\t7\n" +
                            "12\t347\t0\n" +
                            "13\t244\tNaN\n" +
                            "14\t197\t50\n" +
                            "14\t197\t68\n",
                    "select x.c, x.a, b from x outer join y on y.m = x.c",
                    null
            );
        });
    }

    @Test
    public void testJoinOuterNoSlaveRecordsFF() throws Exception {
        testFullFat(this::testJoinOuterNoSlaveRecords);
    }

    @Test
    public void testJoinOuterTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.c, x.a, b, ts from x outer join y on y.m = x.c";
            final String expected = "c\ta\tb\tts\n" +
                    "1\t120\tNaN\t2018-03-01T00:00:00.000001Z\n" +
                    "2\t568\t16\t2018-03-01T00:00:00.000002Z\n" +
                    "2\t568\t72\t2018-03-01T00:00:00.000002Z\n" +
                    "3\t333\tNaN\t2018-03-01T00:00:00.000003Z\n" +
                    "4\t371\t14\t2018-03-01T00:00:00.000004Z\n" +
                    "4\t371\t3\t2018-03-01T00:00:00.000004Z\n" +
                    "5\t251\tNaN\t2018-03-01T00:00:00.000005Z\n" +
                    "6\t439\t81\t2018-03-01T00:00:00.000006Z\n" +
                    "6\t439\t12\t2018-03-01T00:00:00.000006Z\n" +
                    "7\t42\tNaN\t2018-03-01T00:00:00.000007Z\n" +
                    "8\t521\t16\t2018-03-01T00:00:00.000008Z\n" +
                    "8\t521\t97\t2018-03-01T00:00:00.000008Z\n" +
                    "9\t356\tNaN\t2018-03-01T00:00:00.000009Z\n" +
                    "10\t598\t5\t2018-03-01T00:00:00.000010Z\n" +
                    "10\t598\t74\t2018-03-01T00:00:00.000010Z\n";

            compiler.compile("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x ts from long_sequence(10)) timestamp(ts)", sqlExecutionContext);
            compiler.compile("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))", sqlExecutionContext);

            // master records should be filtered out because slave records missing
            assertQueryAndCache(expected, query, "ts", false);

            compiler.compile("insert into x select * from (select cast(x+10 as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x + 10 ts from long_sequence(4)) timestamp(ts)", sqlExecutionContext);
            compiler.compile("insert into y select x, cast(2*((x-1+10)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(6)", sqlExecutionContext);

            assertQuery(expected +
                            "11\t467\tNaN\t2018-03-01T00:00:00.000011Z\n" +
                            "12\t347\t7\t2018-03-01T00:00:00.000012Z\n" +
                            "12\t347\t0\t2018-03-01T00:00:00.000012Z\n" +
                            "13\t244\tNaN\t2018-03-01T00:00:00.000013Z\n" +
                            "14\t197\t50\t2018-03-01T00:00:00.000014Z\n" +
                            "14\t197\t68\t2018-03-01T00:00:00.000014Z\n",
                    query,
                    "ts"
            );
        });
    }

    @Test
    public void testSpliceCorrectness() throws Exception {
        assertMemoryLeak(() -> {

            compiler.compile("create table orders (sym SYMBOL, amount DOUBLE, side BYTE, timestamp TIMESTAMP) timestamp(timestamp)", sqlExecutionContext);

            compiler.compile(
                    "create table quotes (sym SYMBOL, bid DOUBLE, ask DOUBLE, timestamp TIMESTAMP) timestamp(timestamp)", sqlExecutionContext);

            try (
                    TableWriter orders = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "orders", "testing");
                    TableWriter quotes = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "quotes", "testing")
            ) {
                TableWriter.Row rOrders;
                TableWriter.Row rQuotes;

                // quote googl @ 10:00:02
                rQuotes = quotes.newRow(TimestampFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000000Z"));
                rQuotes.putSym(0, "googl");
                rQuotes.putDouble(1, 100.2);
                rQuotes.putDouble(2, 100.3);
                rQuotes.append();

                // quote msft @ 10.00.02.000001
                rQuotes = quotes.newRow(TimestampFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000001Z"));
                rQuotes.putSym(0, "msft");
                rQuotes.putDouble(1, 185.9);
                rQuotes.putDouble(2, 187.3);
                rQuotes.append();

                // quote msft @ 10.00.02.000002
                rQuotes = quotes.newRow(TimestampFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000002Z"));
                rQuotes.putSym(0, "msft");
                rQuotes.putDouble(1, 186.1);
                rQuotes.putDouble(2, 187.8);
                rQuotes.append();

                // order googl @ 10.00.03
                rOrders = orders.newRow(TimestampFormatUtils.parseUTCTimestamp("2018-11-02T10:00:03.000000Z"));
                rOrders.putSym(0, "googl");
                rOrders.putDouble(1, 2000);
                rOrders.putByte(2, (byte) '1');
                rOrders.append();

                // quote msft @ 10.00.03.000001
                rQuotes = quotes.newRow(TimestampFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000002Z"));
                rQuotes.putSym(0, "msft");
                rQuotes.putDouble(1, 183.4);
                rQuotes.putDouble(2, 185.9);
                rQuotes.append();

                // order msft @ 10:00:04
                rOrders = orders.newRow(TimestampFormatUtils.parseUTCTimestamp("2018-11-02T10:00:04.000000Z"));
                rOrders.putSym(0, "msft");
                rOrders.putDouble(1, 150);
                rOrders.putByte(2, (byte) '1');
                rOrders.append();

                // order googl @ 10.00.05
                rOrders = orders.newRow(TimestampFormatUtils.parseUTCTimestamp("2018-11-02T10:00:05.000000Z"));
                rOrders.putSym(0, "googl");
                rOrders.putDouble(1, 3000);
                rOrders.putByte(2, (byte) '2');
                rOrders.append();

                quotes.commit();
                orders.commit();
            }
        });

        assertQuery(
                "sym\tamount\tside\ttimestamp\tsym1\tbid\task\ttimestamp1\n" +
                        "\tNaN\t0\t\tgoogl\t100.2\t100.3\t2018-11-02T10:00:02.000000Z\n" +
                        "\tNaN\t0\t\tmsft\t185.9\t187.3\t2018-11-02T10:00:02.000001Z\n" +
                        "\tNaN\t0\t\tmsft\t186.1\t187.8\t2018-11-02T10:00:02.000002Z\n" +
                        "\tNaN\t0\t\tmsft\t183.4\t185.9\t2018-11-02T10:00:02.000002Z\n" +
                        "googl\t2000.0\t49\t2018-11-02T10:00:03.000000Z\tgoogl\t100.2\t100.3\t2018-11-02T10:00:02.000000Z\n" +
                        "msft\t150.0\t49\t2018-11-02T10:00:04.000000Z\tmsft\t183.4\t185.9\t2018-11-02T10:00:02.000002Z\n" +
                        "googl\t3000.0\t50\t2018-11-02T10:00:05.000000Z\tgoogl\t100.2\t100.3\t2018-11-02T10:00:02.000000Z\n",
                "select * from orders splice join quotes on(sym)",
                null,
                null,
                false
        );
    }

    @Test
    public void testSpliceJoinAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x splice join y on y.sym2 = x.sym";

            final String expected = "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "NaN\t\tNaN\t0.032\t\t2018-01-01T00:02:00.000000Z\n" +
                    "NaN\t\tNaN\t0.043000000000000003\t\t2018-01-01T00:04:00.000000Z\n" +
                    "NaN\t\tNaN\t0.986\t\t2018-01-01T00:06:00.000000Z\n" +
                    "NaN\t\tNaN\t0.139\t\t2018-01-01T00:08:00.000000Z\n" +
                    "NaN\t\tNaN\t0.152\t\t2018-01-01T00:10:00.000000Z\n" +
                    "1\tmsft\t50.938\t0.043000000000000003\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:04:00.000000Z\n" +
                    "NaN\t\tNaN\t0.707\t\t2018-01-01T00:14:00.000000Z\n" +
                    "NaN\t\tNaN\t0.937\t\t2018-01-01T00:16:00.000000Z\n" +
                    "NaN\t\tNaN\t0.42\t\t2018-01-01T00:18:00.000000Z\n" +
                    "NaN\t\tNaN\t0.8300000000000001\t\t2018-01-01T00:20:00.000000Z\n" +
                    "NaN\t\tNaN\t0.392\t\t2018-01-01T00:22:00.000000Z\n" +
                    "2\tgoogl\t42.281\t0.937\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:16:00.000000Z\n" +
                    "NaN\t\tNaN\t0.834\t\t2018-01-01T00:26:00.000000Z\n" +
                    "NaN\t\tNaN\t0.47900000000000004\t\t2018-01-01T00:28:00.000000Z\n" +
                    "2\tgoogl\t42.281\t0.911\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:30:00.000000Z\n" +
                    "NaN\t\tNaN\t0.9410000000000001\t\t2018-01-01T00:32:00.000000Z\n" +
                    "NaN\t\tNaN\t0.736\t\t2018-01-01T00:34:00.000000Z\n" +
                    "3\tgoogl\t17.371\t0.42\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:18:00.000000Z\n" +
                    "NaN\t\tNaN\t0.437\t\t2018-01-01T00:38:00.000000Z\n" +
                    "NaN\t\tNaN\t0.109\t\t2018-01-01T00:40:00.000000Z\n" +
                    "NaN\t\tNaN\t0.84\t\t2018-01-01T00:42:00.000000Z\n" +
                    "NaN\t\tNaN\t0.252\t\t2018-01-01T00:44:00.000000Z\n" +
                    "NaN\t\tNaN\t0.54\t\t2018-01-01T00:46:00.000000Z\n" +
                    "4\tibm\t14.831\t0.252\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:44:00.000000Z\n" +
                    "NaN\t\tNaN\t0.621\t\t2018-01-01T00:50:00.000000Z\n" +
                    "NaN\t\tNaN\t0.963\t\t2018-01-01T00:52:00.000000Z\n" +
                    "NaN\t\tNaN\t0.359\t\t2018-01-01T00:54:00.000000Z\n" +
                    "NaN\t\tNaN\t0.383\t\t2018-01-01T00:56:00.000000Z\n" +
                    "NaN\t\tNaN\t0.009000000000000001\t\t2018-01-01T00:58:00.000000Z\n" +
                    "5\tgoogl\t86.772\t0.42\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:18:00.000000Z\n" +
                    "6\tmsft\t29.659\t0.08700000000000001\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "7\tgoogl\t7.594\t0.911\t2018-01-01T01:24:00.000000Z\t2018-01-01T00:30:00.000000Z\n" +
                    "8\tibm\t54.253\t0.383\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:56:00.000000Z\n" +
                    "9\tmsft\t62.26\t0.08700000000000001\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "10\tmsft\t50.908\t0.08700000000000001\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n";

            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, null, false);

            compiler.compile(
                    "insert into x select * from " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "insert into y select * from " +
                            "(select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQuery("i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                            "NaN\t\tNaN\t0.032\t\t2018-01-01T00:02:00.000000Z\n" +
                            "NaN\t\tNaN\t0.043000000000000003\t\t2018-01-01T00:04:00.000000Z\n" +
                            "NaN\t\tNaN\t0.986\t\t2018-01-01T00:06:00.000000Z\n" +
                            "NaN\t\tNaN\t0.139\t\t2018-01-01T00:08:00.000000Z\n" +
                            "NaN\t\tNaN\t0.152\t\t2018-01-01T00:10:00.000000Z\n" +
                            "1\tmsft\t50.938\t0.043000000000000003\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:04:00.000000Z\n" +
                            "NaN\t\tNaN\t0.707\t\t2018-01-01T00:14:00.000000Z\n" +
                            "NaN\t\tNaN\t0.937\t\t2018-01-01T00:16:00.000000Z\n" +
                            "NaN\t\tNaN\t0.42\t\t2018-01-01T00:18:00.000000Z\n" +
                            "NaN\t\tNaN\t0.8300000000000001\t\t2018-01-01T00:20:00.000000Z\n" +
                            "NaN\t\tNaN\t0.392\t\t2018-01-01T00:22:00.000000Z\n" +
                            "2\tgoogl\t42.281\t0.937\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:16:00.000000Z\n" +
                            "NaN\t\tNaN\t0.834\t\t2018-01-01T00:26:00.000000Z\n" +
                            "NaN\t\tNaN\t0.47900000000000004\t\t2018-01-01T00:28:00.000000Z\n" +
                            "2\tgoogl\t42.281\t0.911\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:30:00.000000Z\n" +
                            "NaN\t\tNaN\t0.9410000000000001\t\t2018-01-01T00:32:00.000000Z\n" +
                            "NaN\t\tNaN\t0.736\t\t2018-01-01T00:34:00.000000Z\n" +
                            "3\tgoogl\t17.371\t0.42\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:18:00.000000Z\n" +
                            "NaN\t\tNaN\t0.437\t\t2018-01-01T00:38:00.000000Z\n" +
                            "NaN\t\tNaN\t0.109\t\t2018-01-01T00:40:00.000000Z\n" +
                            "NaN\t\tNaN\t0.84\t\t2018-01-01T00:42:00.000000Z\n" +
                            "NaN\t\tNaN\t0.252\t\t2018-01-01T00:44:00.000000Z\n" +
                            "NaN\t\tNaN\t0.54\t\t2018-01-01T00:46:00.000000Z\n" +
                            "4\tibm\t14.831\t0.252\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:44:00.000000Z\n" +
                            "NaN\t\tNaN\t0.621\t\t2018-01-01T00:50:00.000000Z\n" +
                            "NaN\t\tNaN\t0.963\t\t2018-01-01T00:52:00.000000Z\n" +
                            "NaN\t\tNaN\t0.359\t\t2018-01-01T00:54:00.000000Z\n" +
                            "NaN\t\tNaN\t0.383\t\t2018-01-01T00:56:00.000000Z\n" +
                            "NaN\t\tNaN\t0.009000000000000001\t\t2018-01-01T00:58:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.42\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:18:00.000000Z\n" +
                            "3\tgoogl\t17.371\t0.687\t2018-01-01T00:36:00.000000Z\t2018-01-01T01:02:00.000000Z\n" +
                            "NaN\t\tNaN\t0.215\t\t2018-01-01T01:04:00.000000Z\n" +
                            "1\tmsft\t50.938\t0.061\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:06:00.000000Z\n" +
                            "NaN\t\tNaN\t0.554\t\t2018-01-01T01:08:00.000000Z\n" +
                            "3\tgoogl\t17.371\t0.332\t2018-01-01T00:36:00.000000Z\t2018-01-01T01:10:00.000000Z\n" +
                            "6\tmsft\t29.659\t0.08700000000000001\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.222\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:14:00.000000Z\n" +
                            "1\tmsft\t50.938\t0.305\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:16:00.000000Z\n" +
                            "NaN\t\tNaN\t0.403\t\t2018-01-01T01:18:00.000000Z\n" +
                            "1\tmsft\t50.938\t0.323\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:20:00.000000Z\n" +
                            "1\tmsft\t50.938\t0.297\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:22:00.000000Z\n" +
                            "7\tgoogl\t7.594\t0.332\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:10:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.372\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:26:00.000000Z\n" +
                            "1\tmsft\t50.938\t0.446\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:28:00.000000Z\n" +
                            "4\tibm\t14.831\t0.231\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:30:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.23900000000000002\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:32:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.067\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:34:00.000000Z\n" +
                            "8\tibm\t54.253\t0.47700000000000004\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:36:00.000000Z\n" +
                            "4\tibm\t14.831\t0.877\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:38:00.000000Z\n" +
                            "6\tmsft\t29.659\t0.432\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:40:00.000000Z\n" +
                            "4\tibm\t14.831\t0.67\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:42:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.264\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:44:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.782\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:46:00.000000Z\n" +
                            "9\tmsft\t62.26\t0.724\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:48:00.000000Z\n" +
                            "4\tibm\t14.831\t0.252\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                            "4\tibm\t14.831\t0.6960000000000001\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:52:00.000000Z\n" +
                            "4\tibm\t14.831\t0.904\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:54:00.000000Z\n" +
                            "4\tibm\t14.831\t0.732\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.26\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "10\tmsft\t50.908\t0.209\t2018-01-01T02:00:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "11\tgoogl\t27.493000000000002\t0.26\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "12\tgoogl\t39.244\t0.26\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "13\tgoogl\t56.985\t0.26\t2018-01-01T02:36:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "14\tmsft\t49.758\t0.209\t2018-01-01T02:48:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "15\tmsft\t49.108000000000004\t0.209\t2018-01-01T03:00:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "16\tmsft\t0.132\t0.209\t2018-01-01T03:12:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "17\tibm\t80.48\t0.732\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "18\tmsft\t57.556000000000004\t0.209\t2018-01-01T03:36:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "19\tgoogl\t34.25\t0.26\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "20\tgoogl\t2.6750000000000003\t0.26\t2018-01-01T04:00:00.000000Z\t2018-01-01T01:58:00.000000Z\n",
                    query,
                    null);
        });
    }

    @Test
    public void testSpliceJoinNoLeftTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x splice join y on y.sym2 = x.sym";
            compiler.compile("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10))", sqlExecutionContext);
            compiler.compile("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)", sqlExecutionContext);

            try {
                compiler.compile(query, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(65, e.getPosition());
                Assert.assertTrue(Chars.contains(e.getFlyweightMessage(), "left"));
            }
        });
    }

    @Test
    public void testSpliceJoinNoRightTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x splice join y on y.sym2 = x.sym";
            compiler.compile("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)", sqlExecutionContext);
            compiler.compile("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30))", sqlExecutionContext);

            try {
                compiler.compile(query, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(65, e.getPosition());
                Assert.assertTrue(Chars.contains(e.getFlyweightMessage(), "right"));
            }
        });
    }

    @Test
    public void testSpliceJoinNoStrings() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x splice join y on y.sym2 = x.sym";

            final String expected = "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "NaN\t\tNaN\t0.032\t\t2018-01-01T00:02:00.000000Z\n" +
                    "NaN\t\tNaN\t0.113\t\t2018-01-01T00:04:00.000000Z\n" +
                    "NaN\t\tNaN\t0.11\t\t2018-01-01T00:06:00.000000Z\n" +
                    "NaN\t\tNaN\t0.21\t\t2018-01-01T00:08:00.000000Z\n" +
                    "NaN\t\tNaN\t0.934\t\t2018-01-01T00:10:00.000000Z\n" +
                    "1\tmsft\t50.938\t0.523\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:12:00.000000Z\n" +
                    "NaN\t\tNaN\t0.846\t\t2018-01-01T00:14:00.000000Z\n" +
                    "NaN\t\tNaN\t0.605\t\t2018-01-01T00:16:00.000000Z\n" +
                    "NaN\t\tNaN\t0.215\t\t2018-01-01T00:18:00.000000Z\n" +
                    "NaN\t\tNaN\t0.223\t\t2018-01-01T00:20:00.000000Z\n" +
                    "NaN\t\tNaN\t0.781\t\t2018-01-01T00:22:00.000000Z\n" +
                    "2\tgoogl\t42.281\t0.605\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:16:00.000000Z\n" +
                    "NaN\t\tNaN\t0.108\t\t2018-01-01T00:26:00.000000Z\n" +
                    "NaN\t\tNaN\t0.91\t\t2018-01-01T00:28:00.000000Z\n" +
                    "2\tgoogl\t42.281\t0.373\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:30:00.000000Z\n" +
                    "NaN\t\tNaN\t0.024\t\t2018-01-01T00:32:00.000000Z\n" +
                    "2\tgoogl\t42.281\t0.301\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:34:00.000000Z\n" +
                    "3\tgoogl\t17.371\t0.915\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                    "2\tgoogl\t42.281\t0.419\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:38:00.000000Z\n" +
                    "NaN\t\tNaN\t0.864\t\t2018-01-01T00:40:00.000000Z\n" +
                    "NaN\t\tNaN\t0.404\t\t2018-01-01T00:42:00.000000Z\n" +
                    "NaN\t\tNaN\t0.982\t\t2018-01-01T00:44:00.000000Z\n" +
                    "NaN\t\tNaN\t0.586\t\t2018-01-01T00:46:00.000000Z\n" +
                    "4\tibm\t14.831\t0.91\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:28:00.000000Z\n" +
                    "NaN\t\tNaN\t0.539\t\t2018-01-01T00:50:00.000000Z\n" +
                    "3\tgoogl\t17.371\t0.989\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:52:00.000000Z\n" +
                    "NaN\t\tNaN\t0.537\t\t2018-01-01T00:54:00.000000Z\n" +
                    "3\tgoogl\t17.371\t0.5710000000000001\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:56:00.000000Z\n" +
                    "3\tgoogl\t17.371\t0.76\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:58:00.000000Z\n" +
                    "5\tgoogl\t86.772\t0.092\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "6\tmsft\t29.659\t0.537\t2018-01-01T01:12:00.000000Z\t2018-01-01T00:54:00.000000Z\n" +
                    "7\tgoogl\t7.594\t0.092\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "8\tibm\t54.253\t0.404\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:42:00.000000Z\n" +
                    "9\tmsft\t62.26\t0.537\t2018-01-01T01:48:00.000000Z\t2018-01-01T00:54:00.000000Z\n" +
                    "10\tmsft\t50.908\t0.537\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:54:00.000000Z\n";

            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQueryAndCache(expected, query, null, false);

            compiler.compile(
                    "insert into x select * from " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );
            compiler.compile(
                    "insert into y select * from " +
                            "(select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)",
                    sqlExecutionContext
            );

            assertQuery("i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                            "NaN\t\tNaN\t0.032\t\t2018-01-01T00:02:00.000000Z\n" +
                            "NaN\t\tNaN\t0.113\t\t2018-01-01T00:04:00.000000Z\n" +
                            "NaN\t\tNaN\t0.11\t\t2018-01-01T00:06:00.000000Z\n" +
                            "NaN\t\tNaN\t0.21\t\t2018-01-01T00:08:00.000000Z\n" +
                            "NaN\t\tNaN\t0.934\t\t2018-01-01T00:10:00.000000Z\n" +
                            "1\tmsft\t50.938\t0.523\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:12:00.000000Z\n" +
                            "NaN\t\tNaN\t0.846\t\t2018-01-01T00:14:00.000000Z\n" +
                            "NaN\t\tNaN\t0.605\t\t2018-01-01T00:16:00.000000Z\n" +
                            "NaN\t\tNaN\t0.215\t\t2018-01-01T00:18:00.000000Z\n" +
                            "NaN\t\tNaN\t0.223\t\t2018-01-01T00:20:00.000000Z\n" +
                            "NaN\t\tNaN\t0.781\t\t2018-01-01T00:22:00.000000Z\n" +
                            "2\tgoogl\t42.281\t0.605\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:16:00.000000Z\n" +
                            "NaN\t\tNaN\t0.108\t\t2018-01-01T00:26:00.000000Z\n" +
                            "NaN\t\tNaN\t0.91\t\t2018-01-01T00:28:00.000000Z\n" +
                            "2\tgoogl\t42.281\t0.373\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:30:00.000000Z\n" +
                            "NaN\t\tNaN\t0.024\t\t2018-01-01T00:32:00.000000Z\n" +
                            "2\tgoogl\t42.281\t0.301\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:34:00.000000Z\n" +
                            "3\tgoogl\t17.371\t0.915\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                            "2\tgoogl\t42.281\t0.419\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:38:00.000000Z\n" +
                            "NaN\t\tNaN\t0.864\t\t2018-01-01T00:40:00.000000Z\n" +
                            "NaN\t\tNaN\t0.404\t\t2018-01-01T00:42:00.000000Z\n" +
                            "NaN\t\tNaN\t0.982\t\t2018-01-01T00:44:00.000000Z\n" +
                            "NaN\t\tNaN\t0.586\t\t2018-01-01T00:46:00.000000Z\n" +
                            "4\tibm\t14.831\t0.91\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:28:00.000000Z\n" +
                            "NaN\t\tNaN\t0.539\t\t2018-01-01T00:50:00.000000Z\n" +
                            "3\tgoogl\t17.371\t0.989\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:52:00.000000Z\n" +
                            "NaN\t\tNaN\t0.537\t\t2018-01-01T00:54:00.000000Z\n" +
                            "3\tgoogl\t17.371\t0.5710000000000001\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:56:00.000000Z\n" +
                            "3\tgoogl\t17.371\t0.76\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:58:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.092\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                            "NaN\t\tNaN\t0.252\t\t2018-01-01T01:02:00.000000Z\n" +
                            "3\tgoogl\t17.371\t0.122\t2018-01-01T00:36:00.000000Z\t2018-01-01T01:04:00.000000Z\n" +
                            "1\tmsft\t50.938\t0.962\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:06:00.000000Z\n" +
                            "1\tmsft\t50.938\t0.098\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:08:00.000000Z\n" +
                            "NaN\t\tNaN\t0.705\t\t2018-01-01T01:10:00.000000Z\n" +
                            "6\tmsft\t29.659\t0.962\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:06:00.000000Z\n" +
                            "NaN\t\tNaN\t0.489\t\t2018-01-01T01:14:00.000000Z\n" +
                            "1\tmsft\t50.938\t0.105\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:16:00.000000Z\n" +
                            "NaN\t\tNaN\t0.892\t\t2018-01-01T01:18:00.000000Z\n" +
                            "NaN\t\tNaN\t0.74\t\t2018-01-01T01:20:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.38\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:22:00.000000Z\n" +
                            "7\tgoogl\t7.594\t0.036000000000000004\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:24:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.395\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:26:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.882\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:28:00.000000Z\n" +
                            "1\tmsft\t50.938\t0.301\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:30:00.000000Z\n" +
                            "1\tmsft\t50.938\t0.032\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:32:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.308\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:34:00.000000Z\n" +
                            "8\tibm\t54.253\t0.892\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:18:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.667\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:38:00.000000Z\n" +
                            "4\tibm\t14.831\t0.594\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:40:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.08700000000000001\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:42:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.855\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:44:00.000000Z\n" +
                            "5\tgoogl\t86.772\t0.786\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:46:00.000000Z\n" +
                            "9\tmsft\t62.26\t0.301\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:30:00.000000Z\n" +
                            "4\tibm\t14.831\t0.644\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                            "4\tibm\t14.831\t0.55\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:52:00.000000Z\n" +
                            "9\tmsft\t62.26\t0.434\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:54:00.000000Z\n" +
                            "4\tibm\t14.831\t0.388\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "9\tmsft\t62.26\t0.912\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "10\tmsft\t50.908\t0.434\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:54:00.000000Z\n" +
                            "11\tmsft\t25.604\t0.912\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "12\tgoogl\t89.22\t0.148\t2018-01-01T02:24:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "13\tgoogl\t64.536\t0.148\t2018-01-01T02:36:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "14\tibm\t33.0\t0.388\t2018-01-01T02:48:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "15\tmsft\t67.285\t0.912\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "16\tgoogl\t17.31\t0.148\t2018-01-01T03:12:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "17\tibm\t23.957\t0.388\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "18\tibm\t60.678000000000004\t0.388\t2018-01-01T03:36:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "19\tmsft\t4.727\t0.912\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "20\tgoogl\t26.222\t0.148\t2018-01-01T04:00:00.000000Z\t2018-01-01T02:00:00.000000Z\n",
                    query,
                    null);
        });
    }

    @Test
    public void testTypeMismatch() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table x as (select x c, abs(rnd_int() % 650) a from long_sequence(5))", sqlExecutionContext);
            compiler.compile("create table y as (select cast((x-1)/4 + 1 as int) c, abs(rnd_int() % 100) b from long_sequence(20))", sqlExecutionContext);
            compiler.compile("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))", sqlExecutionContext);

            try {
                compiler.compile("select z.c, x.a, b, d, d-b from x join y on(c) join z on (c)", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(44, e.getPosition());
            }
        });
    }

    @Test
    public void testSelectAliasTest() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table contact_events as (" +
                    "  select rnd_symbol(4,4,4,2) _id, " +
                    "    rnd_symbol(4,4,4,2) contactid, " +
                    "    CAST(x as Timestamp) timestamp, " +
                    "    rnd_symbol(4,4,4,2) groupId " +
                    "  from long_sequence(50)) " +
                    "timestamp(timestamp)", sqlExecutionContext);
            compiler.compile("create table contacts as (" +
                    "  select rnd_symbol(4,4,4,2) _id, " +
                    "    CAST(x as Timestamp) timestamp, " +
                    "    rnd_symbol(4,4,4,2) notRealType " +
                    "  from long_sequence(50)) " +
                    "timestamp(timestamp)", sqlExecutionContext);

            assertQuery("id\n",
                    "with\n" +
                            "eventlist as (select * from contact_events latest by _id order by timestamp)\n" +
                            ",contactlist as (select * from contacts latest by _id order by timestamp)\n" +
                            ",c as (select distinct contactid from eventlist where groupId = 'ykom80aRN5AwUcuRp4LJ' except select distinct _id as contactId from contactlist where notRealType = 'bot')\n" +
                            "select\n" +
                            "c.contactId as id\n" +
                            "from\n" +
                            "c\n" +
                            "join contactlist on c.contactid = contactlist._id\n",
                    null, false, false, true);
        });
    }

    @Test
    public void testAsofJoin() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table trips as (" +
                    "  select rnd_double() fare_amount, " +
                    "    CAST(x as Timestamp) pickup_datetime " +
                    "  from long_sequence(5)) " +
                    "timestamp(pickup_datetime)", sqlExecutionContext);

            compiler.compile("create table weather as (" +
                    "  select rnd_double() tempF, " +
                    "    rnd_int() windDir, " +
                    "    cast(x as TIMESTAMP) timestamp " +
                    "  from long_sequence(5)) " +
                    "timestamp(timestamp)", sqlExecutionContext);

            assertQuery("pickup_datetime\tfare_amount\ttempF\twindDir\n" +
                            "1970-01-01T00:00:00.000001Z\t0.6607777894187332\t0.6508594025855301\t-1436881714\n" +
                            "1970-01-01T00:00:00.000002Z\t0.2246301342497259\t0.7905675319675964\t1545253512\n" +
                            "1970-01-01T00:00:00.000003Z\t0.08486964232560668\t0.22452340856088226\t-409854405\n" +
                            "1970-01-01T00:00:00.000004Z\t0.299199045961845\t0.3491070363730514\t1904508147\n" +
                            "1970-01-01T00:00:00.000005Z\t0.20447441837877756\t0.7611029514995744\t1125579207\n",
                    "SELECT pickup_datetime, fare_amount, tempF, windDir \n" +
                            "FROM (trips WHERE pickup_datetime IN '1970-01-01') \n" +
                            "ASOF JOIN weather",
                    "pickup_datetime", false, false, true);
        });
    }


    @Test
    public void testTypeMismatchFF() throws Exception {
        testFullFat(this::testTypeMismatch);
    }

    private void testFullFat(TestMethod method) throws Exception {
        compiler.setFullSatJoins(true);
        try {
            method.run();
        } finally {
            compiler.setFullSatJoins(false);
        }
    }

    @FunctionalInterface
    private interface TestMethod {
        void run() throws Exception;
    }
}