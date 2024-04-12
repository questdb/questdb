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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class JoinTest extends AbstractCairoTest {

    @Test
    public void test2686() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table table_1 (\n" +
                    "          ts timestamp,\n" +
                    "          name string,\n" +
                    "          age int,\n" +
                    "          member boolean\n" +
                    "        ) timestamp(ts) PARTITION by month");

            insert("insert into table_1 values ( '2022-10-25T01:00:00.000000Z', 'alice',  60, True )");
            insert("insert into table_1 values ( '2022-10-25T02:00:00.000000Z', 'peter',  58, False )");
            insert("insert into table_1 values ( '2022-10-25T03:00:00.000000Z', 'david',  21, True )");

            compile("create table table_2 (\n" +
                    "          ts timestamp,\n" +
                    "          name string,\n" +
                    "          age int,\n" +
                    "          address string\n" +
                    "        ) timestamp(ts) PARTITION by month");

            insert("insert into table_2 values ( '2022-10-25T01:00:00.000000Z', 'alice',  60,  '1 Glebe St' )");
            insert("insert into table_2 values ( '2022-10-25T02:00:00.000000Z', 'peter',  58, '1 Broon St' )");
        });

        // query "2"
        assertQuery("name\tage\tmember\taddress\tts\n" +
                        "alice\t60\ttrue\t1 Glebe St\t2022-10-25T01:00:00.000000Z\n" +
                        "peter\t58\tfalse\t1 Broon St\t2022-10-25T02:00:00.000000Z\n" +
                        "david\t21\ttrue\t\t2022-10-25T03:00:00.000000Z\n",
                "select a.name, a.age, a.member, b.address, a.ts\n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b \n" +
                        "   on a.ts = b.ts ", null, "ts", false, false);

        // query "3"
        assertQuery("name\tage\taddress\tts\tdateadd\tdateadd1\n" +
                        "alice\t60\t1 Glebe St\t2022-10-25T01:00:00.000000Z\t2022-10-25T00:59:00.000000Z\t2022-10-25T01:01:00.000000Z\n" +
                        "peter\t58\t1 Broon St\t2022-10-25T02:00:00.000000Z\t2022-10-25T01:59:00.000000Z\t2022-10-25T02:01:00.000000Z\n" +
                        "david\t21\t\t2022-10-25T03:00:00.000000Z\t\t\n",
                "select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts) \n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b \n" +
                        "   on a.ts between dateadd('m', -1, b.ts)  and dateadd('m', 1, b.ts) ", null, "ts", false, false);

        // query "4" - same as "3" but between is replaced with >= and <=
        assertQuery("name\tage\taddress\tts\tdateadd\tdateadd1\n" +
                        "alice\t60\t1 Glebe St\t2022-10-25T01:00:00.000000Z\t2022-10-25T00:59:00.000000Z\t2022-10-25T01:01:00.000000Z\n" +
                        "peter\t58\t1 Broon St\t2022-10-25T02:00:00.000000Z\t2022-10-25T01:59:00.000000Z\t2022-10-25T02:01:00.000000Z\n" +
                        "david\t21\t\t2022-10-25T03:00:00.000000Z\t\t\n",
                "select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts) \n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b \n" +
                        "   on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts)", null, "ts", false, false);
    }

    @Test
    public void testAsOfCorrectness() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table orders (sym SYMBOL, amount DOUBLE, side BYTE, timestamp TIMESTAMP) timestamp(timestamp)");
            ddl("create table quotes (sym SYMBOL, bid DOUBLE, ask DOUBLE, timestamp TIMESTAMP) timestamp(timestamp)");

            try (
                    TableWriter orders = getWriter("orders");
                    TableWriter quotes = getWriter("quotes")
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
                true
        );
    }

    @Test
    public void testAsOfFullFat() throws Exception {
        testFullFat(this::testAsOfJoin0);
    }

    @Test
    public void testAsOfFullFatJoinOnStr() throws Exception {
        assertMemoryLeak(() -> {

            ddl(
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
                            ") timestamp (timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
            );

            assertException(
                    "select x.i, x.c, y.c, x.amt, price, x.timestamp, y.timestamp, y.m from x asof join y on y.c = x.c",
                    73,
                    "right side column 'm' is of unsupported type",
                    true
            );
        });
    }

    @Test
    public void testAsOfFullFatJoinOnStrNoVar() throws Exception {
        testFullFat(this::testAsOfJoinOnStrNoVar0);
    }

    @Test
    public void testAsOfFullFatJoinOnStrSubSelect() throws Exception {
        assertMemoryLeak(() -> {
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

            ddl(
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
                            ") timestamp (timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
            );

            assertQueryAndCacheFullFat(expected, query, "timestamp", false, true);

            insert(
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
                            ") timestamp(timestamp)"
            );
            insert(
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
                            ") timestamp(timestamp)"
            );

            assertQueryFullFat(
                    "i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1\n" +
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
                    true,
                    true
            );

        });
    }

    @Test
    public void testAsOfFullFatJoinOnVarCharNoVar() throws Exception {
        testFullFat(this::testAsOfJoinOnVarcharNoVar0);
    }

    @Test
    public void testAsOfJoin() throws Exception {
        testAsOfJoin0(false);
    }

    @Test
    public void testAsOfJoinAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.sym2 = x.sym";

            final String expected = "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "1\tmsft\t50.938\tNaN\t2018-01-01T00:12:00.000000Z\t\n" +
                    "2\tmsft\t56.594\t0.503\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:20:00.000000Z\n" +
                    "3\tmsft\t5.359\t0.503\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:20:00.000000Z\n" +
                    "4\tmsft\t48.353\t0.055\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:48:00.000000Z\n" +
                    "5\tibm\t95.82000000000001\t0.192\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "6\tibm\t5.134\t0.192\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "7\tgoogl\t54.5\t0.273\t2018-01-01T01:24:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                    "8\tibm\t73.384\t0.192\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "9\tgoogl\t41.37\t0.273\t2018-01-01T01:48:00.000000Z\t2018-01-01T00:36:00.000000Z\n" +
                    "10\tmsft\t32.425\t0.49\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:54:00.000000Z\n";

            ddl(
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
                            " rnd_str(5,16,2) n," +
                            " rnd_varchar(5,16,2) vch" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)"
            );
            ddl(
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
                            " rnd_str(5,16,2) n," +
                            " rnd_varchar(5,16,2) vch" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)"
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            ddl(
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
                            " rnd_str(5,16,2) n," +
                            " rnd_varchar(5,16,2) vch" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)"
            );
            ddl(
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
                            " rnd_str(5,16,2) n," +
                            " rnd_varchar(5,16,2) vch" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)"
            );

            assertQuery("i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                            "1\tmsft\t50.938\tNaN\t2018-01-01T00:12:00.000000Z\t\n" +
                            "2\tmsft\t56.594\t0.503\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:20:00.000000Z\n" +
                            "3\tmsft\t5.359\t0.503\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:20:00.000000Z\n" +
                            "4\tmsft\t48.353\t0.055\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:48:00.000000Z\n" +
                            "5\tibm\t95.82000000000001\t0.192\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                            "6\tibm\t5.134\t0.6980000000000001\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:12:00.000000Z\n" +
                            "7\tgoogl\t54.5\t0.274\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:20:00.000000Z\n" +
                            "8\tibm\t73.384\t0.707\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:34:00.000000Z\n" +
                            "9\tgoogl\t41.37\t0.218\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:44:00.000000Z\n" +
                            "10\tmsft\t32.425\t0.974\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "11\tmsft\t93.60300000000001\t0.974\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "12\tmsft\t83.539\t0.974\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "13\tmsft\t99.315\t0.974\t2018-01-01T02:36:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "14\tibm\t81.634\t0.617\t2018-01-01T02:48:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "15\tibm\t75.197\t0.617\t2018-01-01T03:00:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                            "16\tmsft\t52.129\t0.974\t2018-01-01T03:12:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "17\tgoogl\t53.897\t0.434\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "18\tgoogl\t52.817\t0.434\t2018-01-01T03:36:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                            "19\tmsft\t91.873\t0.974\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:56:00.000000Z\n" +
                            "20\tgoogl\t7.509\t0.434\t2018-01-01T04:00:00.000000Z\t2018-01-01T01:58:00.000000Z\n",
                    query,
                    "timestamp",
                    false,
                    true
            );
        });
    }

    @Test
    public void testAsOfJoinAllTypesFullFat() throws Exception {
        testFullFat(this::testAsOfJoinNoStrings0);
    }

    @Test
    public void testAsOfJoinLeftTimestampDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            ddl("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertException(
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from (x order by timestamp desc) x asof join y on y.sym2 = x.sym",
                    93,
                    "left"
            );
        });
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

            ddl(
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
                            ") timestamp (timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            ddl(
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
                            ") timestamp(timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
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

            ddl(
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
                            ") timestamp (timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
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

            ddl(
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
                            ") timestamp (timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
            );

            assertQueryAndCache(expected, query, "timestamp", true);
        });
    }

    @Test
    public void testAsOfJoinNoKeyNoLeaks() throws Exception {
        testJoinForCursorLeaks("with crj as (select x, ts from xx latest by x) select xx.x from xx asof join crj", false);
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

            ddl(
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
                            ") timestamp (timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
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

            ddl(
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
                            ") timestamp (timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
            );

            assertQueryAndCache(expected, query, "timestamp", true);
        });
    }

    @Test
    public void testAsOfJoinNoLeftTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10))");
            ddl("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");

            assertException(
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.sym2 = x.sym",
                    65,
                    "left"
            );
        });
    }

    @Test
    public void testAsOfJoinNoRightTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.sym2 = x.sym";
            ddl("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            ddl("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30))");
            assertException(
                    query,
                    65,
                    "right"
            );
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

            ddl(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)"
            );

            ddl(
                    "create table y as (" +
                            "select cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)"
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            ddl(
                    "insert into x select * from (" +
                            "select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)"
            );

            ddl(
                    "insert into y select * from (" +
                            "select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)"
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
        testAsOfJoinNoStrings0(false);
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

            ddl(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ")"
            );

            ddl(
                    "create table y as (" +
                            "select cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ")"
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            ddl(
                    "insert into x select * from (" +
                            "select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)"
            );

            ddl(
                    "insert into y select * from (" +
                            "select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)"
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

            ddl(
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
                            ") timestamp (timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            ddl(
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
                            ") timestamp(timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
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
        testAsOfJoinOnStrNoVar0(false);
    }

    @Test
    public void testAsOfJoinOnVarcharNoVar() throws Exception {
        testAsOfJoinOnVarcharNoVar0(false);
    }

    @Test
    public void testAsOfJoinRecordNoLeaks() throws Exception {
        testJoinForCursorLeaks("with crj as (select x, ts from xx latest by x) select xx.x from xx asof join crj on xx.x = crj.x ", false);
    }

    @Test
    public void testAsOfJoinRecordNoLeaks2() throws Exception {
        testJoinForCursorLeaks("with crj as (select x, ts from xx latest by x) select xx.x from xx asof join crj on xx.x = crj.x ", false);
    }

    @Test
    public void testAsOfJoinRightTimestampDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            ddl("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertException(
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join (y order by timestamp desc) y on y.sym2 = x.sym",
                    65,
                    "right"
            );
        });
    }

    @Test
    public void testAsOfJoinSlaveSymbol() throws Exception {
        testAsOfJoinSlaveSymbol0(false);
    }

    @Test
    public void testAsOfSlaveSymbolFullFat() throws Exception {
        testFullFat(this::testAsOfJoinSlaveSymbol0);
    }

    @Test
    public void testAsofJoin() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trips as (" +
                    "  select rnd_double() fare_amount, " +
                    "    CAST(x as Timestamp) pickup_datetime " +
                    "  from long_sequence(5)) " +
                    "timestamp(pickup_datetime)");

            ddl("create table weather as (" +
                    "  select rnd_double() tempF, " +
                    "    rnd_int() windDir, " +
                    "    cast(x as TIMESTAMP) timestamp " +
                    "  from long_sequence(5)) " +
                    "timestamp(timestamp)");

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
    public void testAsofJoinWithComplexConditionFails1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            compile("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 asof join t2 on l1=l2+5", "unsupported ASOF join expression [expr='l1 = l2 + 5']", 35);
        });
    }

    @Test
    public void testAsofJoinWithComplexConditionFails2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            compile("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 asof join t2 on l1>l2", "unsupported ASOF join expression [expr='l1 > l2']", 35);
        });
    }

    @Test
    public void testAsofJoinWithComplexConditionFails3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            compile("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 asof join t2 on l1=abs(l2)", "unsupported ASOF join expression [expr='l1 = abs(l2)']", 35);
        });
    }

    @Test
    public void testCrossJoinAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tvch\tkk1\ta1\tb1\tc1\td1\te1\tf1\tg1\ti1\tj1\tk1\tl1\tm1\tn1\tvch1\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t1245795385\tfalse\tV\t0.03993124821273464\tNaN\t358\t2015-03-03T13:26:27.587Z\t\t-6912707344119330199\t1970-01-01T00:00:00.000000Z\t14\t00000000 9a e7 0c 89 14 58 63 b7 c2 9f 29 8e 29 5e 69\tEOYPHRIPZIMNZZ\t\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t684778036\tfalse\tB\t0.7445998836567925\t0.2826\t189\t2015-08-21T15:46:32.624Z\t\t3084117448873356811\t1970-01-01T00:16:40.000000Z\t7\t\tXPKRGIIHY\t碨Ь\uDA23\uDF64m\uDA30\uDEE0\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t-2017314910\ttrue\tL\t0.750281471677565\t0.6591\t815\t2015-09-22T19:25:53.894Z\tUQSR\t-6782883555378798844\t1970-01-01T00:33:20.000000Z\t22\t00000000 8f 23 3f ae 7c 9f 77 04 e9 0c ea 4e ea 8b f5 0f\n" +
                    "00000010 2d\tNWIFFLRBROMNX\tߧE}$\uDA43\uDFF0-㔍x钷\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t1\t1245795385\tfalse\tV\t0.03993124821273464\tNaN\t358\t2015-03-03T13:26:27.587Z\t\t-6912707344119330199\t1970-01-01T00:00:00.000000Z\t14\t00000000 9a e7 0c 89 14 58 63 b7 c2 9f 29 8e 29 5e 69\tEOYPHRIPZIMNZZ\t\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t1\t684778036\tfalse\tB\t0.7445998836567925\t0.2826\t189\t2015-08-21T15:46:32.624Z\t\t3084117448873356811\t1970-01-01T00:16:40.000000Z\t7\t\tXPKRGIIHY\t碨Ь\uDA23\uDF64m\uDA30\uDEE0\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t1\t-2017314910\ttrue\tL\t0.750281471677565\t0.6591\t815\t2015-09-22T19:25:53.894Z\tUQSR\t-6782883555378798844\t1970-01-01T00:33:20.000000Z\t22\t00000000 8f 23 3f ae 7c 9f 77 04 e9 0c ea 4e ea 8b f5 0f\n" +
                    "00000010 2d\tNWIFFLRBROMNX\tߧE}$\uDA43\uDFF0-㔍x钷\n";

            ddl(
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
                            " rnd_str(5,16,2) n," +
                            " rnd_varchar(5,16,2) vch" +
                            " from long_sequence(2))"
            );

            ddl(
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
                            " rnd_str(5,16,2) n," +
                            " rnd_varchar(5,16,2) vch" +
                            " from long_sequence(3))"
            );

            // filter is applied to final join result
            assertQuery(expected, "select * from x cross join y", null, false, true);
        });
    }

    @Test
    public void testCrossJoinCount() throws Exception {
        assertMemoryLeak(() -> {
            // 1 partition
            compile("create table TabA ( " +
                    "          ts timestamp, " +
                    "          x long " +
                    "        ) timestamp(ts) PARTITION by month");

            // 3 partitions
            compile("create table TabB ( " +
                    "          ts timestamp, " +
                    "          x long " +
                    "        ) timestamp(ts) PARTITION by hour");

            // 0 partitions
            compile("create table TabC ( " +
                    "          ts timestamp, " +
                    "          x long " +
                    "        ) timestamp(ts) PARTITION by year");

            insert("insert into TabA select x::timestamp, x/6 from long_sequence(10)");
            insert("insert into TabB select (x*15L*60L*1000000L)::timestamp, x/6 from long_sequence(10)");

            //join with empty table
            String selectWithEmpty = "(" +
                    "select * from TabA " +
                    "cross join TabC )";
            assertSkipToAndCalculateSize(selectWithEmpty, 0);

            //async filter
            String selectWithFilter = "(" +
                    "select * from TabA " +
                    "cross join TabB " +
                    "where TabA.x = 0 " +
                    "and TabB.x = 1 )";
            assertSkipToAndCalculateSize(selectWithFilter, 25);

            //async filter with limit
            String selectWithFilterWithLimit = "( select * from " +
                    "(select * from TabA where x = 0 limit 3) " +
                    "cross join " +
                    "(select * from TabB where x = 1 limit 3) )";
            assertSkipToAndCalculateSize(selectWithFilterWithLimit, 9);

            // fwd data frame
            String selectWithFwdFrame = "(select * from TabA " +
                    "cross join TabB )";
            assertSkipToAndCalculateSize(selectWithFwdFrame, 100);

            // bwd data frame
            String selectWithBwdFrame = "(select * from " +
                    "(select * from TabA order by ts desc) " +
                    "cross join " +
                    "(select * from TabB order by ts desc) )";
            assertSkipToAndCalculateSize(selectWithBwdFrame, 100);

            String selectWithIntervalFwdFrame = "( select * from " +
                    "(select * from TabA where ts > 1) " +
                    "cross join " +
                    "(select * from TabB where ts > 15L*60L*1000000L) )";
            assertSkipToAndCalculateSize(selectWithIntervalFwdFrame, 81);

            // bwd data frame
            String selectWithIntervalBwdFrame = "( select * from " +
                    "(select * from TabA where ts > 1 order by ts desc ) " +
                    "cross join " +
                    "(select * from TabB where ts > 15L*60L*1000000L order by ts desc) )";
            assertSkipToAndCalculateSize(selectWithIntervalBwdFrame, 81);
        });
    }

    @Test
    public void testCrossJoinNoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "kk\ta\tb\tc\td\te\tf\tg\ti\tj\tl\tm\tn\tvch\tkk1\ta1\tb1\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t1245795385\tfalse\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t684778036\tfalse\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t-2017314910\ttrue\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t1\t1245795385\tfalse\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t1\t684778036\tfalse\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t1\t-2017314910\ttrue\n";
            ddl(
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
                            " rnd_str(5,16,2) n," +
                            " rnd_varchar(5,16,2) vch" +
                            " from long_sequence(2)) timestamp(k)"
            );

            ddl(
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
                            " rnd_str(5,16,2) n," +
                            " rnd_varchar(5,16,2) vch" +
                            " from long_sequence(3))"
            );

            // filter is applied to final join result
            assertQuery(expected, "select x.kk, x.a, x.b, x.c, x.d, x.e, x.f, x.g, x.i, x.j, x.l, x.m, x.n, x.vch, y.kk, y.a, y.b from x cross join y", null, false, true);
        });
    }

    @Test
    public void testCrossJoinTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tvch\tkk1\ta1\tb1\tc1\td1\te1\tf1\tg1\ti1\tj1\tk1\tl1\tm1\tn1\tvch1\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t1245795385\tfalse\tV\t0.03993124821273464\tNaN\t358\t2015-03-03T13:26:27.587Z\t\t-6912707344119330199\t1970-01-01T00:00:00.000000Z\t14\t00000000 9a e7 0c 89 14 58 63 b7 c2 9f 29 8e 29 5e 69\tEOYPHRIPZIMNZZ\t\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t684778036\tfalse\tB\t0.7445998836567925\t0.2826\t189\t2015-08-21T15:46:32.624Z\t\t3084117448873356811\t1970-01-01T00:16:40.000000Z\t7\t\tXPKRGIIHY\t碨Ь\uDA23\uDF64m\uDA30\uDEE0\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t-2017314910\ttrue\tL\t0.750281471677565\t0.6591\t815\t2015-09-22T19:25:53.894Z\tUQSR\t-6782883555378798844\t1970-01-01T00:33:20.000000Z\t22\t00000000 8f 23 3f ae 7c 9f 77 04 e9 0c ea 4e ea 8b f5 0f\n" +
                    "00000010 2d\tNWIFFLRBROMNX\tߧE}$\uDA43\uDFF0-㔍x钷\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t1\t1245795385\tfalse\tV\t0.03993124821273464\tNaN\t358\t2015-03-03T13:26:27.587Z\t\t-6912707344119330199\t1970-01-01T00:00:00.000000Z\t14\t00000000 9a e7 0c 89 14 58 63 b7 c2 9f 29 8e 29 5e 69\tEOYPHRIPZIMNZZ\t\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t1\t684778036\tfalse\tB\t0.7445998836567925\t0.2826\t189\t2015-08-21T15:46:32.624Z\t\t3084117448873356811\t1970-01-01T00:16:40.000000Z\t7\t\tXPKRGIIHY\t碨Ь\uDA23\uDF64m\uDA30\uDEE0\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t1\t-2017314910\ttrue\tL\t0.750281471677565\t0.6591\t815\t2015-09-22T19:25:53.894Z\tUQSR\t-6782883555378798844\t1970-01-01T00:33:20.000000Z\t22\t00000000 8f 23 3f ae 7c 9f 77 04 e9 0c ea 4e ea 8b f5 0f\n" +
                    "00000010 2d\tNWIFFLRBROMNX\tߧE}$\uDA43\uDFF0-㔍x钷\n";

            ddl(
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
                            " rnd_str(5,16,2) n, " +
                            " rnd_varchar(5,16,2) vch" +
                            " from long_sequence(2)) timestamp(k)"
            );

            ddl(
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
                            " rnd_str(5,16,2) n," +
                            " rnd_varchar(5,16,2) vch" +
                            " from long_sequence(3))"
            );

            // filter is applied to final join result
            assertQuery(expected, "select * from x cross join y", "k", false, true);
        });
    }

    @Test
    public void testCrossTripleOverflow() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select("select * from long_sequence(1000000000) a cross join long_sequence(1000000000) b cross join long_sequence(1000000000) c")) {
                Assert.assertNotNull(factory);
                sink.clear();
                CursorPrinter.println(factory.getMetadata(), sink);
                TestUtils.assertEquals("x\tx1\tx2\n", sink);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(Long.MAX_VALUE, cursor.size());
                }
            }
        });
    }

    @Test
    public void testHashJoinLightdNoLeaks() throws Exception {
        testJoinForCursorLeaks("with crj as (select * from xx latest by x) select xx.x from xx join crj on xx.x = crj.x ", false);
    }

    @Test
    public void testHashJoinRecordNoLeaks() throws Exception {
        testJoinForCursorLeaks("with crj as (select first(x) x, first(ts) ts from xx latest by x) select xx.x from xx join crj on xx.x = crj.x ", false);
    }

    @Test
    public void testJoinAliasBug() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (xid int, a int, b int)");
            ddl("create table y (yid int, a int, b int)");
            select("select tx.a, tx.b from x as tx left join y as ty on xid = yid where tx.a = 1 or tx.b=2").close();
            select("select tx.a, tx.b from x as tx left join y as ty on xid = yid where ty.a = 1 or ty.b=2").close();
        });
    }

    @Test
    public void testJoinColumnPropagationIntoJoinModel() throws Exception {
        ddl(
                "CREATE TABLE trades (" +
                        "  symbol SYMBOL," +
                        "  price DOUBLE," +
                        "  amount DOUBLE," +
                        "  timestamp TIMESTAMP " +
                        ") timestamp (timestamp) PARTITION BY DAY;"
        );

        insert("insert into trades values ( 'ETH-USD', 2, 2, '2023-05-29T13:15:00.000000Z') ");

        for (String joinType : Arrays.asList("LEFT JOIN", "LT JOIN", "ASOF JOIN", "SPLICE JOIN")) {
            testJoinColumnPropagationIntoJoinModel0(joinType, false);
        }
        testJoinColumnPropagationIntoJoinModel0("JOIN", true);
    }

    @Test
    public void testJoinConstantFalse() throws Exception {
        testJoinConstantFalse0(false);
    }

    @Test
    public void testJoinConstantFalseFF() throws Exception {
        testFullFat(this::testJoinConstantFalse0);
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(10))");
            ddl("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))");

            // master records should be filtered out because slave records missing
            assertQuery(expected, "select x.c, x.a, b from x join y on y.m = x.c and 1 < 10", null, false, true);
        });
    }

    @Test
    public void testJoinConstantTrueFF() throws Exception {
        testFullFat(this::testJoinConstantTrue0);
    }

    @Test
    public void testJoinContextIsolationInIntersect() throws Exception {
        assertMemoryLeak(() -> {
            ddl(
                    "CREATE TABLE t (\n" +
                            "  created timestamp,\n" +
                            "  event short,\n" +
                            "  origin short\n" +
                            ") TIMESTAMP(created) PARTITION BY DAY;"
            );
            insert("INSERT INTO t VALUES ('2023-09-21T10:00:00.000000Z', 1, 1);");

            // The important aspects here are T2.created = '2003-09-21T10:00:00.000000Z'
            // in the first query and T2.created = T3.created in the second one. Due to this,
            // transitive filters pass was mistakenly mutating where clause in the second query.
            final String query1 = "SELECT count(1)\n" +
                    "FROM t as T1 CROSS JOIN t as T2\n" +
                    "WHERE T2.created > now() and T2.created = '2003-09-21T10:00:00.000000Z'";
            final String query2 = "SELECT count(1)\n" +
                    "FROM t as T1 JOIN t as T2 on T1.created = T2.created JOIN t as T3 ON T2.created = T3.created\n" +
                    "WHERE T3.created < now()";

            assertQuery("count\n0\n", query1, null, false, true);
            assertQuery("count\n1\n", query2, null, false, true);

            assertQuery(
                    "count\n",
                    query1 + " INTERSECT " + query2,
                    null,
                    false,
                    false
            );
        });
    }

    @Test
    public void testJoinContextIsolationInUnion() throws Exception {
        assertMemoryLeak(() -> {
            ddl(
                    "CREATE TABLE t (\n" +
                            "  created timestamp,\n" +
                            "  event short,\n" +
                            "  origin short\n" +
                            ") TIMESTAMP(created) PARTITION BY DAY;"
            );
            insert("INSERT INTO t VALUES ('2023-09-21T10:00:00.000000Z', 1, 1);");
            insert("INSERT INTO t VALUES ('2023-09-21T11:00:00.000000Z', 1, 1);");

            // The important aspects here are T1.event = 1.0
            // in the first query and T1.event = T2.event in the second one. Due to this,
            // transitive filters pass was mistakenly mutating where clause in the second query.
            final String query1 = "SELECT count(1)\n" +
                    "FROM t as T1 JOIN t as T2 ON T1.created = T2.created\n" +
                    "WHERE T1.event = 1.0";
            final String query2 = "SELECT count(1)\n" +
                    "FROM t as T1 JOIN t as T2 ON T1.event = T2.event";

            assertQuery("count\n2\n", query1, null, false, true);
            assertQuery("count\n4\n", query2, null, false, true);

            assertQuery(
                    "count\n" +
                            "2\n" +
                            "4\n",
                    query1 + " UNION " + query2,
                    null,
                    false,
                    false
            );
        });
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x ts from long_sequence(5)) timestamp(ts)");
            ddl("create table y as (select cast((x-1)/4 + 1 as int) c, abs(rnd_int() % 100) b from long_sequence(20))");
            ddl("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))");

            assertQuery(
                    expected,
                    "select z.c, x.a, b, d, d-b from x join y on(c) join z on (c)",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testJoinInnerAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tvch\tkk1\ta1\tb1\tc1\td1\te1\tf1\tg1\ti1\tj1\tk1\tl1\tm1\tn1\tvch1\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t-1915752164\tfalse\tI\t0.8786111112537701\t0.9966\t403\t2015-08-19T00:36:24.375Z\tKLGM\t-8506266080452644687\t1970-01-01T00:00:00.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t\t\uDBC5\uDF7B\uDBE5\uDD8F.vmOa\uDA76\uDDD4*\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t-1871096754\tfalse\tP\t0.4740684604688953\t0.0181\t148\t2015-10-12T03:40:03.380Z\tQDYO\t-8490120737538725244\t1970-01-01T00:16:40.000000Z\t2\t00000000 44 11 e2 a3 24 4e 44 a8 0d fe 27 ec 53 13 5d\tGFNWGRMDGG\t\uE044|'ꋯɟ\uF6BE\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t1247654963\ttrue\tY\t0.7842455970681089\t0.3162\t584\t2015-06-24T18:10:25.514Z\t\t-2317221228709139922\t1970-01-01T00:33:20.000000Z\t29\t00000000 94 5f ec d3 dc f8 43 b2 e3 75 62 60 af 6d 8c d8\n" +
                    "00000010 ac c8\tZQSNPXMKJSMKIXEY\t\uDA8B\uDFC4︵Ƀ^DU?$s_*\uDBAE\uDF56\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t-1535701773\ttrue\tV\t0.32824342042623134\t0.7826\t299\t2015-07-22T20:23:51.186Z\tKLGM\t1363805506825579405\t1970-01-01T00:50:00.000000Z\t17\t00000000 fe b9 52 dd 4d f3 f9 76 f6 85 ab a3 ab ee 6d 54\n" +
                    "00000010 75 10\tWLNLR\tw雑\uDB9C\uDCE1\uD9F6\uDF3D횝\uDB9A\uDF230pጙ锿\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t2\t233699709\ttrue\tV\t0.7727320382377867\t0.7708\t846\t2015-10-03T22:53:17.989Z\t\t8336821503223023075\t1970-01-01T01:06:40.000000Z\t18\t00000000 8a b0 35 d8 ab 3f a1 f5 4b ea 01 c9\tZHEISQHNO\t\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t2\t1939793032\tfalse\tI\t0.5765797240495835\t0.4913\t833\t2015-07-31T03:51:57.870Z\t\t-8646234590100970125\t1970-01-01T01:23:20.000000Z\t5\t00000000 7e c0 1d 6c a9 65 81 ad 79 87 fc 92 83 fc 88 f3\n" +
                    "00000010 32 27\tLPTYXYGYFUXCD\t<'ָ읈ҽ\uDA01\uDE60E죢魷ꞛ#DQ헙\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t2\t1361673432\ttrue\tM\t0.16979644136429572\t0.2812\t59\t2015-02-16T09:18:55.746Z\t\t-6746433171163409313\t1970-01-01T01:40:00.000000Z\t4\t\tDDCIHCNP\t\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t2\t-1169915830\ttrue\tP\tNaN\t0.0589\t359\t2015-05-26T17:24:24.749Z\t\t-7350430133595690521\t1970-01-01T01:56:40.000000Z\t14\t00000000 35 3b 1c 9c 1d 5c c1 5d 2d 44 ea 00 81 c4 19 a1\n" +
                    "00000010 ec\tSMIFDYPDK\t\n" +
                    "3\t844704299\ttrue\t\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:33:20.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                    "00000010 e7 0c 89\tLJUMLGLHMLLEO\t+\u0093ً\uDAF5\uDE17qRӽ-\t3\t-1505690678\tfalse\tR\t0.09854153834719315\t0.2329\t82\t2015-06-03T01:01:00.230Z\tQDYO\t-7725099828175109832\t1970-01-01T02:13:20.000000Z\t27\t\tZUPVQFULMER\tּ\uDB48\uDC78{ϸ\uD9F4\uDFB9\uDA0A\uDC7A\uDA76\uDC87>\uD8F0\uDF66Ҫb\uDBB1\uDEA3\n" +
                    "3\t844704299\ttrue\t\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:33:20.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                    "00000010 e7 0c 89\tLJUMLGLHMLLEO\t+\u0093ً\uDAF5\uDE17qRӽ-\t3\t600986867\tfalse\tM\t0.19823647700531244\tNaN\t557\t2015-01-30T03:27:34.392Z\t\t5324839128380055812\t1970-01-01T02:30:00.000000Z\t25\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a\n" +
                    "00000010 42 71 a3 7a\tDNZNLCNGZTOY\t\u07B3\uDA8F\uDC319믓˫ᡙ\uDBEC\uDE3B櫑߸!>\uD9F3\uDFD5a~=V\n" +
                    "3\t844704299\ttrue\t\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:33:20.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                    "00000010 e7 0c 89\tLJUMLGLHMLLEO\t+\u0093ً\uDAF5\uDE17qRӽ-\t3\t100444418\tfalse\tK\t0.28400807705010733\t0.5784\t1015\t2015-05-21T09:22:31.780Z\tXSLU\t-2052253029650705565\t1970-01-01T02:46:40.000000Z\t18\t00000000 4b b7 e2 7f ab 6e 23 03 dd c7 d6\tDRHFBCZI\t緧\uD9FF\uDE64ì˵\"&LB\n" +
                    "3\t844704299\ttrue\t\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:33:20.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                    "00000010 e7 0c 89\tLJUMLGLHMLLEO\t+\u0093ً\uDAF5\uDE17qRӽ-\t3\t-97222559\ttrue\tI\t0.1810605823104886\t0.9209\t917\t2015-08-05T02:39:14.093Z\tXSLU\t7768501691006807692\t1970-01-01T03:03:20.000000Z\t3\t00000000 c0 55 12 44 dc 4b c0 d9 1c 71 cf 5a 8f 21 06\tMQEHDHQH\tܹ\uDA38\uDDD7̊E+%륤\n" +
                    "4\t-2053564939\tfalse\tX\t0.7381752894013154\t0.7365\t448\t2015-08-30T11:34:00.859Z\tVTJW\t-7336930007738575369\t1970-01-01T00:50:00.000000Z\t15\t\tSMPGL\t\t4\t-494568109\ttrue\tU\tNaN\t0.7958\t469\t2015-05-24T04:44:08.006Z\tQDYO\t-6162343549945721271\t1970-01-01T03:20:00.000000Z\t7\t\tUKVZI\t٦ہ\uDAB6\uDF33\uDB00\uDF8AϿ˄礏ɍ\uDB2C\uDD55\uD904\uDFA0Uڥ[<\n" +
                    "4\t-2053564939\tfalse\tX\t0.7381752894013154\t0.7365\t448\t2015-08-30T11:34:00.859Z\tVTJW\t-7336930007738575369\t1970-01-01T00:50:00.000000Z\t15\t\tSMPGL\t\t4\t-670048539\ttrue\tU\t0.6747913655036517\t0.4895\t295\t\tGMXU\t6609063415252900609\t1970-01-01T03:36:40.000000Z\t28\t\tZIZKM\t\uD94E\uDE17䎡R3à䋿Ϛ\uDA6D\uDD37텎,ۃ)ǟ\n" +
                    "4\t-2053564939\tfalse\tX\t0.7381752894013154\t0.7365\t448\t2015-08-30T11:34:00.859Z\tVTJW\t-7336930007738575369\t1970-01-01T00:50:00.000000Z\t15\t\tSMPGL\t\t4\t1355454157\tfalse\t\t0.9359965050658042\t0.2008\t881\t2015-11-27T19:42:33.675Z\tXSLU\t-7991782124465626462\t1970-01-01T03:53:20.000000Z\t27\t00000000 a8 2b 8d 35 c5 85 9a 00 4a a1 06 7e 3f 4e 27 42\tHBQJP\t\uD98E\uDF30-\uE605\uDB23\uDFB3Le楣DB\uDAAD\uDE0A\uE916G[ꫭ\n" +
                    "4\t-2053564939\tfalse\tX\t0.7381752894013154\t0.7365\t448\t2015-08-30T11:34:00.859Z\tVTJW\t-7336930007738575369\t1970-01-01T00:50:00.000000Z\t15\t\tSMPGL\t\t4\t737515614\tfalse\tP\t0.9879118064160076\t0.0189\t14\t2015-08-02T08:50:10.325Z\t\t-4200011690995273529\t1970-01-01T04:10:00.000000Z\t20\t00000000 54 02 9f c2 37 98 60 bb 38 d1 36\tQBORDTQ\tz\uDA0E\uDFE4yƹC\n" +
                    "5\t-1726426588\ttrue\tZ\t0.7700798090070919\t0.4416\t158\t2015-04-21T07:10:57.197Z\tVTJW\t6121305147479698964\t1970-01-01T01:06:40.000000Z\t19\t00000000 84 52 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04\tVELLK\t䳉\uDB5F\uDE9AKJܜߧE}$\uDA43\uDFF0-㔍x钷Mͱ\t5\t1697668849\ttrue\tM\tNaN\t0.4132\t890\t2015-07-15T10:46:51.266Z\tXSLU\t6581120496001202966\t1970-01-01T04:26:40.000000Z\t13\t00000000 f4 03 ed c9 2a 4e 91 c5 e4 39 b2 dd 0d a7 bb\tDJIGSYLXGYTEQC\tږ胵zݒ邍\n" +
                    "5\t-1726426588\ttrue\tZ\t0.7700798090070919\t0.4416\t158\t2015-04-21T07:10:57.197Z\tVTJW\t6121305147479698964\t1970-01-01T01:06:40.000000Z\t19\t00000000 84 52 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04\tVELLK\t䳉\uDB5F\uDE9AKJܜߧE}$\uDA43\uDFF0-㔍x钷Mͱ\t5\t-1803543488\tfalse\tU\t0.06138053193106652\t0.5984\t108\t2015-04-07T16:45:54.013Z\t\t-5925564127232580585\t1970-01-01T04:43:20.000000Z\t15\t00000000 cf 5c 24 32 1e 3d ba 5f e3 91 7b ae d1 be 82 5f\n" +
                    "00000010 87 9e 79 1c\t\t\uD9FB\uDE6C\uDA85\uDF29䚭ϸ\uD9A8\uDFFB\n" +
                    "5\t-1726426588\ttrue\tZ\t0.7700798090070919\t0.4416\t158\t2015-04-21T07:10:57.197Z\tVTJW\t6121305147479698964\t1970-01-01T01:06:40.000000Z\t19\t00000000 84 52 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04\tVELLK\t䳉\uDB5F\uDE9AKJܜߧE}$\uDA43\uDFF0-㔍x钷Mͱ\t5\t-706221546\ttrue\tD\t0.2829857902567392\t0.6340\t242\t2015-08-02T20:54:21.297Z\tKLGM\t-5510843329132792373\t1970-01-01T05:00:00.000000Z\t34\t00000000 48 10 64 65 32 e1 a2 d4 70 b2\tLRBIDSTDTFBYHSH\t폓\uDA22\uDD8AV\uDBB1\uDE4BG&ُܵ\n" +
                    "5\t-1726426588\ttrue\tZ\t0.7700798090070919\t0.4416\t158\t2015-04-21T07:10:57.197Z\tVTJW\t6121305147479698964\t1970-01-01T01:06:40.000000Z\t19\t00000000 84 52 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04\tVELLK\t䳉\uDB5F\uDE9AKJܜߧE}$\uDA43\uDFF0-㔍x钷Mͱ\t5\t-548641641\tfalse\tT\tNaN\t0.1390\t852\t2015-01-13T12:34:15.149Z\tXSLU\t-8876530977637141165\t1970-01-01T05:16:40.000000Z\t3\t\tDCMUMK\tT\uDBA0\uDD2Fmτ⻱[N`亲m\n";

            ddl(
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
                            " rnd_str(5,16,2) n, " +
                            " rnd_varchar(5,16,2) vch" +
                            " from long_sequence(5))"
            );

            ddl(
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
                            " rnd_str(5,16,2) n," +
                            " rnd_varchar(5,16,2) vch" +
                            " from long_sequence(20))"
            );

            // filter is applied to final join result
            assertQuery(expected, "select * from x join y on (kk)", null, false, true);
        });
    }

    @Test
    public void testJoinInnerAllTypesFF() throws Exception {
        testFullFat(this::testJoinInnerAllTypes0);
    }

    @Test
    public void testJoinInnerConstantFilterWithNonBooleanExpressionFails() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE IF NOT EXISTS x (ts timestamp, event short) TIMESTAMP(ts);");

            assertFailure(
                    "SELECT count(*) FROM x AS a INNER JOIN x AS b ON a.event = b.event WHERE now()",
                    "boolean expression expected",
                    73
            );
        });
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))");
            ddl("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))");
            ddl("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))");

            assertQuery(expected, "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c)", null, false, true);
        });
    }

    @Test
    public void testJoinInnerDifferentColumnNamesFF() throws Exception {
        testFullFat(this::testJoinInnerDifferentColumnNames0);
    }

    @Test
    public void testJoinInnerFF() throws Exception {
        testFullFat(this::testJoinInner0);
    }

    @Test
    public void testJoinInnerFunctionInJoinExpression() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE IF NOT EXISTS x (ts timestamp, event short) TIMESTAMP(ts);");
            insert("INSERT INTO x VALUES (now(), 42)");
            assertQuery("count\n" +
                    "1\n", "SELECT count(*) FROM x AS a INNER JOIN x AS b ON a.event = b.event WHERE now() = now()", null, false, true);
        });
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))");
            ddl("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))");
            ddl("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(16))");

            // filter is applied to intermediate join result
            assertQueryAndCache(expected, "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where y.b < 20", null, true);

            insert("insert into x select cast(x+6 as int) c, abs(rnd_int() % 650) a from long_sequence(3)");
            insert("insert into y select cast((x+19)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(16)");
            insert("insert into z select cast((x+15)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(2)");

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
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testJoinInnerInnerFilterFF() throws Exception {
        testFullFat(this::testJoinInnerInnerFilter0);
    }

    @Test
    public void testJoinInnerLastFilter() throws Exception {
        testJoinInnerLastFilter0(false);
    }

    @Test
    public void testJoinInnerLastFilterFF() throws Exception {
        testFullFat(this::testJoinInnerLastFilter0);
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

            ddl(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_long256() a," +
                            " rnd_char() b " +
                            " from long_sequence(5))"
            );

            ddl(
                    "create table y as (select" +
                            " cast((x-1)/4 + 1 as int) kk," +
                            " rnd_long256() a," +
                            " rnd_char() b " +
                            " from long_sequence(20))"
            );

            // filter is applied to final join result
            assertQuery(expected, "select * from x join y on (kk)", null, false, true);
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

            ddl(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_long256() a," +
                            " rnd_char() b " +
                            " from long_sequence(5))"
            );

            ddl(
                    "create table y as (select" +
                            " cast((x-1)/4 + 1 as int) kk," +
                            " rnd_long256() a," +
                            " rnd_char() b " +
                            " from long_sequence(20))"
            );

            // filter is applied to final join result
            assertQuery(expected, "select * from x join y on (kk) order by x.a, x.b, y.a", null, true, true);
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(10))");
            ddl("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))");

            assertQueryAndCache(expected, "select x.c, x.a, b from x join y on y.m = x.c", null, true);

            insert("insert into x select cast(x+10 as int) c, abs(rnd_int() % 650) a from long_sequence(4)");
            insert("insert into y select x, cast(2*((x-1+10)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(6)");

            assertQuery(expected +
                            "12\t347\t7\n" +
                            "12\t347\t0\n" +
                            "14\t197\t50\n" +
                            "14\t197\t68\n",
                    "select x.c, x.a, b from x join y on y.m = x.c",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testJoinInnerNoSlaveRecordsFF() throws Exception {
        testFullFat(this::testJoinInnerNoSlaveRecords0);
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

            ddl("create table x as (select rnd_symbol('A','B',null,'D') c, abs(rnd_int() % 650) a from long_sequence(5))");
            ddl("create table y as (select rnd_symbol('B','A',null,'D') m, abs(rnd_int() % 100) b from long_sequence(20))");
            ddl("create table z as (select rnd_symbol('D','B',null,'A') c, abs(rnd_int() % 1000) d from long_sequence(16))");

            // filter is applied to intermediate join result
            assertQueryAndCache(expected, "select x.c xc, z.c zc, y.m yc, x.a, b, d, d-b from x join y on y.m = x.c join z on (c)", null, true);

            ddl("insert into x select rnd_symbol('L','K','P') c, abs(rnd_int() % 650) a from long_sequence(3)");
            ddl("insert into y select rnd_symbol('P','L','K') m, abs(rnd_int() % 100) b from long_sequence(6)");
            ddl("insert into z select rnd_symbol('K','P','L') c, abs(rnd_int() % 1000) d from long_sequence(6)");

            assertQuery(expected +
                            "L\tL\tL\t148\t38\t121\t83\n" +
                            "L\tL\tL\t148\t52\t121\t69\n",
                    "select x.c xc, z.c zc, y.m yc, x.a, b, d, d-b from x join y on y.m = x.c join z on (c)",
                    null,
                    false,
                    true
            );

        });
    }

    @Test
    public void testJoinInnerOnSymbolFF() throws Exception {
        testFullFat(this::testJoinInnerOnSymbol0);
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))");
            ddl("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))");
            ddl("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(16))");

            // filter is applied to intermediate join result
            assertQueryAndCache(expected, "select z.c, x.a, b, d, a+b from x join y on y.m = x.c join z on (c) where a+b < 300", null, true);

            insert("insert into x select cast(x+6 as int) c, abs(rnd_int() % 650) a from long_sequence(3)");
            insert("insert into y select cast((x+19)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(16)");
            insert("insert into z select cast((x+15)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(2)");

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
                    null,
                    false,
                    true
            );

        });
    }

    @Test
    public void testJoinInnerPostJoinFilterFF() throws Exception {
        testFullFat(this::testJoinInnerPostJoinFilter0);
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x ts from long_sequence(5)) timestamp(ts)");
            ddl("create table y as (select cast((x-1)/4 + 1 as int) c, abs(rnd_int() % 100) b from long_sequence(20))");
            ddl("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))");

            assertQuery(expected, "select z.c, x.a, b, d, d-b, ts from x join y on(c) join z on (c)", "ts", false, true);
        });
    }

    @Test
    public void testJoinOfTablesWithReservedWordsColNames() throws SqlException {
        ddl(
                "create table x as (" +
                        "select" +
                        " x as i, " +
                        " x*2 as \"in\", " +
                        " x*3 as \"from\" " +
                        " from long_sequence(3)" +
                        ")"
        );

        assertSql("in\tfrom\n" +
                "2\t3\n" +
                "4\t6\n" +
                "6\t9\n", "select \"in\", \"from\" from x"
        );

        assertSql("in\tfrom\tin1\tfrom1\n" +
                "2\t3\t2\t3\n" +
                "4\t6\t4\t6\n" +
                "6\t9\t6\t9\n", "select x.\"in\", x.\"from\", x1.\"in\", x1.\"from\" " +
                "from x " +
                "join x as x1 on x.i = x1.i"
        );

        assertSql("i\tin\tfrom\ti1\tin1\tfrom1\tcolumn\n" +
                "1\t2\t3\t1\t2\t3\t5\n" +
                "2\t4\t6\t2\t4\t6\t10\n" +
                "3\t6\t9\t3\t6\t9\t15\n", "select *, x.\"in\" + x1.\"from\" " +
                "from x " +
                "join x as x1 on x.i = x1.i"
        );
    }

    @Test
    public void testJoinOnGeohash() throws Exception {
        assertMemoryLeak(() -> {
            ddl(
                    "create table t1 as (select " +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(2c)) geo2," +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(8c)) geo8," +
                            "x," +
                            "timestamp_sequence(0, 1000000) ts " +
                            "from long_sequence(10)) timestamp(ts)"
            );
            ddl(
                    "create table t2 as (select " +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(2c)) geo2," +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(8c)) geo8," +
                            "x," +
                            "timestamp_sequence(0, 1000000) ts " +
                            "from long_sequence(2)) timestamp(ts)"
            );


            String expected = "geo4\tgeo1\tgeo2\tgeo8\tx\tts\tgeo41\tgeo11\tgeo21\tgeo81\tx1\tts1\n" +
                    "ques\tq\t12\t\t1\t1970-01-01T00:00:00.000000Z\t\t\t\t\tNaN\t\n" +
                    "3456\t3\t34\t\t2\t1970-01-01T00:00:01.000000Z\t3456\tq\t12\t\t1\t1970-01-01T00:00:00.000000Z\n" +
                    "ques\t1\t12\t\t3\t1970-01-01T00:00:02.000000Z\t\t\t\t\tNaN\t\n" +
                    "1234\t1\t12\t\t4\t1970-01-01T00:00:03.000000Z\t1234\t3\t12\t\t2\t1970-01-01T00:00:01.000000Z\n" +
                    "ques\t1\tqu\t\t5\t1970-01-01T00:00:04.000000Z\t\t\t\t\tNaN\t\n" +
                    "1234\tq\tqu\t\t6\t1970-01-01T00:00:05.000000Z\t1234\t3\t12\t\t2\t1970-01-01T00:00:01.000000Z\n" +
                    "ques\t1\t34\t\t7\t1970-01-01T00:00:06.000000Z\t\t\t\t\tNaN\t\n" +
                    "1234\tq\t34\t\t8\t1970-01-01T00:00:07.000000Z\t1234\t3\t12\t\t2\t1970-01-01T00:00:01.000000Z\n" +
                    "3456\t3\tqu\t\t9\t1970-01-01T00:00:08.000000Z\t3456\tq\t12\t\t1\t1970-01-01T00:00:00.000000Z\n" +
                    "3456\tq\t12\t\t10\t1970-01-01T00:00:09.000000Z\t3456\tq\t12\t\t1\t1970-01-01T00:00:00.000000Z\n";

            String sql = "with g1 as (select distinct * from t1)," +
                    "g2 as (select distinct * from t2)" +
                    "select * from g1 lt join g2 on g1.geo4 = g2.geo4";

            assertSql(sql, expected, true);
            assertSql(expected, sql);
        });
    }

    @Test
    public void testJoinOnGeohashNonExactPrecisionNotAllowed() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t1 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "x," +
                    "timestamp_sequence(0, 1000000) ts " +
                    "from long_sequence(10)) timestamp(ts)");
            ddl("create table t2 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "x," +
                    "timestamp_sequence(0, 1000000) ts " +
                    "from long_sequence(2)) timestamp(ts)");

            String sql = "with g1 as (select distinct * from t1)," +
                    "g2 as (select distinct * from t2)" +
                    "select * from g1 lt join g2 on g1.geo4 = g2.geo1";

            try {
                assertSql("", sql);
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "join column type mismatch");
            }
        });
    }

    @Test
    public void testJoinOnLong256() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, y.i, x.hash from x join x y on y.hash = x.hash";

            final String expected = "i\ti1\thash\n" +
                    "1\t1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\n" +
                    "2\t2\t0xb5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa65572a215ba0462ad15\n" +
                    "3\t3\t0x322a2198864beb14797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fa\n";

            ddl(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_long256() hash" +
                            " from long_sequence(3)" +
                            ")"
            );

            assertQueryAndCache(expected, query, null, true);
        });
    }

    @Test
    public void testJoinOnUUID() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, y.i, x.uuid " +
                    "from x " +
                    "join x y on y.uuid = x.uuid";

            final String expected = "i\ti1\tuuid\n" +
                    "1\t1\t0010cde8-12ce-40ee-8010-a928bb8b9650\n" +
                    "2\t2\t9f9b2131-d49f-4d1d-ab81-39815c50d341\n" +
                    "3\t3\t7bcd48d8-c77a-4655-b2a2-15ba0462ad15\n";

            ddl(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_uuid4() uuid" +
                            " from long_sequence(3)" +
                            ")"
            );

            assertQueryAndCache(expected, query, null, true);
        });
    }

    @Test
    public void testJoinOuterAllTypes() throws Exception {
        testJoinOuterAllTypes0(false);
    }

    @Test
    public void testJoinOuterAllTypesFF() throws Exception {
        testFullFat(this::testJoinOuterAllTypes0);
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

            ddl(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_long256() a," +
                            " rnd_char() b" +
                            " from long_sequence(10))"
            );

            ddl(
                    "create table y as (select" +
                            " cast(2*((x-1)/2) as int)+2 kk," +
                            " rnd_long256() a," +
                            " rnd_char() b" +
                            " from long_sequence(10))"
            );

            // filter is applied to final join result
            assertQuery(
                    expected,
                    "select * from x left join y on (kk)",
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

            ddl(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_long256() a," +
                            " rnd_char() b" +
                            " from long_sequence(10))"
            );

            ddl(
                    "create table y as (select" +
                            " cast(2*((x-1)/2) as int)+2 kk," +
                            " rnd_long256() a," +
                            " rnd_char() b" +
                            " from long_sequence(10))"
            );

            // filter is applied to final join result
            assertQuery(
                    expected,
                    "select * from x left join y on (kk) order by x.a desc, y.a",
                    null,
                    true
            );
        });
    }

    @Test
    public void testJoinOuterNoSlaveRecords() throws Exception {
        testJoinOuterNoSlaveRecords0(false);
    }

    @Test
    public void testJoinOuterNoSlaveRecordsFF() throws Exception {
        testFullFat(this::testJoinOuterNoSlaveRecords0);
    }

    @Test
    public void testJoinOuterTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.c, x.a, b, ts from x left join y on y.m = x.c";
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x ts from long_sequence(10)) timestamp(ts)");
            ddl("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))");

            // master records should be filtered out because slave records missing
            assertQueryAndCache(expected, query, "ts", false);

            ddl("insert into x select * from (select cast(x+10 as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x + 10 ts from long_sequence(4)) timestamp(ts)");
            ddl("insert into y select x, cast(2*((x-1+10)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(6)");

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
    public void testJoinWithGeohash() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "with x1 as (select distinct * from x)," +
                    "y1 as (select distinct * from y) " +
                    "select g1, gg1, gg2, gg4, gg8, x1.k " +
                    "from x1 " +
                    "join y1 on y1.kk = x1.k";

            final String expected = "g1\tgg1\tgg2\tgg4\tgg8\tk\n" +
                    "9v1s\t1\twh4\ts2z2\t10011100111100101000010010010000010001010\t1\n" +
                    "46sw\tq\t71f\tfsnj\t11010111111011100000110010000111111101101\t2\n" +
                    "jnw9\tb\tjj5\tksu7\t11101100011100010000100111000111100000001\t3\n" +
                    "zfuq\ts\t76u\tq0s5\t11110001011010001010010100000110110100010\t4\n" +
                    "hp4m\ty\tp1d\tp2n3\t10111100100011101101110001110010111011001\t5\n";


            ddl(
                    "create table x as (select" +
                            " cast(x as int) k, " +
                            " rnd_geohash(20) g1" +
                            " from long_sequence(5))"
            );

            ddl(
                    "create table y as (select" +
                            " cast(x as int) kk," +
                            " rnd_geohash(15) gg2," +
                            " rnd_geohash(20) gg4," +
                            " rnd_geohash(5) gg1," +
                            " rnd_geohash(41) gg8" +
                            " from long_sequence(20))"
            );

            assertSql(query, expected, true);
            assertSql(expected, query);
        });
    }

    @Test
    public void testJoinWithGeohash2() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "with x1 as (select distinct * from x)," +
                    "y1 as (select distinct * from y) " +
                    "select g1, gg1, gg2, gg4, gg8, x1.k " +
                    "from x1 " +
                    "lt join y1 on x1.l = y1.l";

            final String expected = "g1\tgg1\tgg2\tgg4\tgg8\tk\n" +
                    "9v1s\t\t\t\t\t1970-01-01T00:00:00.000001Z\n" +
                    "46sw\t1\twh4\ts2z2\t10011100111100101000010010010000010001010\t1970-01-01T00:00:00.000002Z\n" +
                    "jnw9\tq\t71f\tfsnj\t11010111111011100000110010000111111101101\t1970-01-01T00:00:00.000003Z\n" +
                    "zfuq\tb\tjj5\tksu7\t11101100011100010000100111000111100000001\t1970-01-01T00:00:00.000004Z\n" +
                    "hp4m\ts\t76u\tq0s5\t11110001011010001010010100000110110100010\t1970-01-01T00:00:00.000005Z\n";

            ddl(
                    "create table x as (select" +
                            " 1 as l, " +
                            " cast(x as timestamp) k, " +
                            " rnd_geohash(20) g1" +
                            " from long_sequence(5)) timestamp(k)"
            );

            ddl(
                    "create table y as (select" +
                            " 1 as l, " +
                            " cast(x as timestamp) kk," +
                            " rnd_geohash(15) gg2," +
                            " rnd_geohash(20) gg4," +
                            " rnd_geohash(5) gg1," +
                            " rnd_geohash(41) gg8" +
                            " from long_sequence(20))  timestamp(kk)"
            );

            assertSql(query, expected, true);
            assertSql(expected, query);
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int)");
            insert("insert into t1 values (1), (2), (3), (4), (5);");
            compile("create table t2 (j int)");
            insert("insert into t2 values (5), (4), (3), (2), (1);");

            assertHashJoinSql("select * from t1 left join t2 on i = j and abs(i) > 3",
                    "i\tj\n" +
                            "1\tNaN\n" +
                            "2\tNaN\n" +
                            "3\tNaN\n" +
                            "4\t4\n" +
                            "5\t5\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition10() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s1 string)");
            insert("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            compile("create table t2 (j int, s2 string)");
            insert("insert into t2 values (1,'a'), (5,'e'), (2, 'b'), (4, 'd'), (3,'c');");

            assertHashJoinSql("select * from t1 left join t2 on j = i and s2 = s1",
                    "i\ts1\tj\ts2\n" +
                            "1\ta\t1\ta\n" +
                            "2\tb\t2\tb\n" +
                            "3\tc\t3\tc\n" +
                            "4\td\t4\td\n" +
                            "5\te\t5\te\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition11() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s1 string)");
            insert("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            compile("create table t2 (j int, s2 string)");
            insert("insert into t2 values (1, 'a'), (5, 'e'), (2, 'b'), (4, 'd'), (3, 'c');");

            assertHashJoinSql("select * from t1 left join t2 on j = i and (s1 ~ 'a' or s2 ~ 'c')",
                    "i\ts1\tj\ts2\n" +
                            "1\ta\t1\ta\n" +
                            "2\tb\tNaN\t\n" +
                            "3\tc\t3\tc\n" +
                            "4\td\tNaN\t\n" +
                            "5\te\tNaN\t\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition12() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s1 string)");
            insert("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            compile("create table t2 (j int, s2 string)");
            insert("insert into t2 values (1,'a'), (1,'e'), (2, 'b'), (2, 'd'), (3,'c');");

            assertHashJoinSql("select * from t1 left join t2 on j = i and (s1 ~ '[abde]')",
                    "i\ts1\tj\ts2\n" +
                            "1\ta\t1\ta\n" +
                            "1\ta\t1\te\n" +
                            "2\tb\t2\tb\n" +
                            "2\tb\t2\td\n" +
                            "3\tc\tNaN\t\n" +
                            "4\td\tNaN\t\n" +
                            "5\te\tNaN\t\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition13() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s1 string)");
            insert("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            compile("create table t2 (j int, s2 string)");

            assertHashJoinSql("select * from t1 left join t2 on j = i and (s1 ~ '[abde]')",
                    "i\ts1\tj\ts2\n" +
                            "1\ta\tNaN\t\n" +
                            "2\tb\tNaN\t\n" +
                            "3\tc\tNaN\t\n" +
                            "4\td\tNaN\t\n" +
                            "5\te\tNaN\t\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition14() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s1 string)");
            compile("create table t2 (j int, s2 string)");
            insert("insert into t2 values (1,'a'), (1,'e'), (2, 'b'), (2, 'd'), (3,'c');");

            assertHashJoinSql("select * from t1 left join t2 on j = i and (s1 ~ '[abde]')",
                    "i\ts1\tj\ts2\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionConditionVarchar13() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s1 varchar)");
            insert("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            compile("create table t2 (j int, s2 varchar)");

            assertHashJoinSql("select * from t1 left join t2 on j = i and (s1 ~ '[abde]')",
                    "i\ts1\tj\ts2\n" +
                            "1\ta\tNaN\t\n" +
                            "2\tb\tNaN\t\n" +
                            "3\tc\tNaN\t\n" +
                            "4\td\tNaN\t\n" +
                            "5\te\tNaN\t\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionConditionVarchar14() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s1 varchar)");
            compile("create table t2 (j int, s2 varchar)");
            insert("insert into t2 values (1,'a'), (1,'e'), (2, 'b'), (2, 'd'), (3,'c');");

            assertHashJoinSql("select * from t1 left join t2 on j = i and (s1 ~ '[abde]')",
                    "i\ts1\tj\ts2\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition15() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s1 string)");
            compile("create table t2 (j int, s2 string)");

            assertHashJoinSql("select * from t1 left join t2 on j = i and (s1 ~ '[abde]')",
                    "i\ts1\tj\ts2\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition16() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s1 string)");
            insert("insert into t1 values (1, 'a'), (2, 'b');");
            compile("create table t2 (j int, s2 string)");
            insert("insert into t2 values (1,'a'), (1,'f'), (1, 'g'), (1, 'd'), (3,'c');");

            assertHashJoinSql("select * from t1 left join t2 on j = i and (s2 ~ '[abde]')",
                    "i\ts1\tj\ts2\n" +
                            "1\ta\t1\ta\n" +
                            "1\ta\t1\td\n" +
                            "2\tb\tNaN\t\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition17() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s1 string, ts1 timestamp) timestamp(ts1)");
            insert("insert into t1 values (1, 'a', 1), (2, 'b', 2);");
            compile("create table t2 (j int, s2 string, ts2 timestamp) timestamp(ts2) ");
            insert("insert into t2 values (1,'a', 1), (1,'f', 2), (1, 'g', 3), (1, 'd', 4), (3,'c', 5);");

            assertHashJoinSql("select * from t1 left join t2 on j = i and (s2 ~ '[abde]') order by ts1 desc",
                    "i\ts1\tts1\tj\ts2\tts2\n" +
                            "2\tb\t1970-01-01T00:00:00.000002Z\tNaN\t\t\n" +
                            "1\ta\t1970-01-01T00:00:00.000001Z\t1\ta\t1970-01-01T00:00:00.000001Z\n" +
                            "1\ta\t1970-01-01T00:00:00.000001Z\t1\td\t1970-01-01T00:00:00.000004Z\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition18() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s1 symbol)");
            insert("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            compile("create table t2 (j int, s2 symbol)");
            insert("insert into t2 values (1, 'a'), (5, 'e'), (2, 'b'), (4, 'd'), (3, 'c');");

            assertHashJoinSql("select * from t1 left join t2 on j = i and (s1 ~ 'a' or s2 ~ 'c')",
                    "i\ts1\tj\ts2\n" +
                            "1\ta\t1\ta\n" +
                            "2\tb\tNaN\t\n" +
                            "3\tc\t3\tc\n" +
                            "4\td\tNaN\t\n" +
                            "5\te\tNaN\t\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int)");
            insert("insert into t1 values (1), (2), (3), (4), (5);");
            compile("create table t2 (j int)");
            insert("insert into t2 values (5), (4), (3), (2), (1);");

            assertHashJoinSql("select * from t1 left join t2 on i = j and abs(i) > 5",
                    "i\tj\n" +
                            "1\tNaN\n" +
                            "2\tNaN\n" +
                            "3\tNaN\n" +
                            "4\tNaN\n" +
                            "5\tNaN\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int)");
            insert("insert into t1 values (1), (2), (3), (4), (5);");
            compile("create table t2 (j int)");
            insert("insert into t2 values (5), (4), (3), (2), (1);");

            assertHashJoinSql("select * from t1 left join t2 on i = j and abs(i) = 3",
                    "i\tj\n" +
                            "1\tNaN\n" +
                            "2\tNaN\n" +
                            "3\t3\n" +
                            "4\tNaN\n" +
                            "5\tNaN\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition4() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int)");
            insert("insert into t1 values (1), (2), (3), (4), (5);");
            compile("create table t2 (j int)");
            insert("insert into t2 values (1), (5), (2), (4), (3);");

            assertHashJoinSql("select * from t1 left join t2 on i = j and abs(i) <= 0",
                    "i\tj\n" +
                            "1\tNaN\n" +
                            "2\tNaN\n" +
                            "3\tNaN\n" +
                            "4\tNaN\n" +
                            "5\tNaN\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition5() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int)");
            insert("insert into t1 values (1), (2), (3), (4), (5);");
            compile("create table t2 (j int)");
            insert("insert into t2 values (1), (5), (2), (4), (3);");

            assertHashJoinSql("select * from t1 left join t2 on j = i and abs(i)*abs(j) >= 4 and i*j <= 9",
                    "i\tj\n" +
                            "1\tNaN\n" +
                            "2\t2\n" +
                            "3\t3\n" +
                            "4\tNaN\n" +
                            "5\tNaN\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition6() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int)");
            insert("insert into t1 values (1), (2), (3), (4), (5);");
            compile("create table t2 (j int)");
            insert("insert into t2 values (1), (5), (2), (4), (3);");

            assertHashJoinSql("select * from t1 left join t2 on j = i and (j = 2 or i = 4)",
                    "i\tj\n" +
                            "1\tNaN\n" +
                            "2\t2\n" +
                            "3\tNaN\n" +
                            "4\t4\n" +
                            "5\tNaN\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition7() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int)");
            insert("insert into t1 values (1), (2), (3), (-4), (5);");
            compile("create table t2 (j int)");
            insert("insert into t2 values (1), (5), (-2), (-4), (3);");

            assertHashJoinSql("select * from t1 left join t2 on j = i and (abs(j) = 2 or abs(i) = 4)",
                    "i\tj\n" +
                            "1\tNaN\n" +
                            "2\tNaN\n" +
                            "3\tNaN\n" +
                            "-4\t-4\n" +
                            "5\tNaN\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition8() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int)");
            insert("insert into t1 values (1), (2), (3), (4), (5);");
            compile("create table t2 (j int, s2 string)");
            insert("insert into t2 values (1,'a'), (5,'e'), (-2, 'b'), (4, 'd'), (3,'c');");

            assertHashJoinSql("select * from t1 left join t2 on j = i and s2 = 'a'",
                    "i\tj\ts2\n" +
                            "1\t1\ta\n" +
                            "2\tNaN\t\n" +
                            "3\tNaN\t\n" +
                            "4\tNaN\t\n" +
                            "5\tNaN\t\n");
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition9() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int)");
            insert("insert into t1 values (1), (2), (3), (4), (5);");
            compile("create table t2 (j int, s2 string)");
            insert("insert into t2 values (1,'a'), (5,'e'), (-2, 'b'), (4, 'd'), (3,'c');");

            assertHashJoinSql("select * from t1 left join t2 on j = i and s2 ~ '[ad]'",
                    "i\tj\ts2\n" +
                            "1\t1\ta\n" +
                            "2\tNaN\t\n" +
                            "3\tNaN\t\n" +
                            "4\t4\td\n" +
                            "5\tNaN\t\n");
        });
    }

    @Test
    public void testLeftHashJoinWithWhere1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s1 string)");
            insert("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            compile("create table t2 (j int, s2 string)");
            insert("insert into t2 values (5, 'e'), (3, 'c'), (2, 'b'), (4, 'd'), (1, 'a');");

            assertHashJoinSql("select * from t1 left join t2 on j = i and i = 1 where 1 = 1",
                    "i\ts1\tj\ts2\n" +
                            "1\ta\t1\ta\n" +
                            "2\tb\tNaN\t\n" +
                            "3\tc\tNaN\t\n" +
                            "4\td\tNaN\t\n" +
                            "5\te\tNaN\t\n");
        });
    }

    @Test
    public void testLeftHashJoinWithWhere2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s1 string)");
            insert("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            compile("create table t2 (j int, s2 string)");
            insert("insert into t2 values (5, 'e'), (3, 'c'), (2, 'b'), (4, 'd'), (1, 'a');");

            assertHashJoinSql("select * from t1 left join t2 on j = i and j = 1 where 1 = 1",
                    "i\ts1\tj\ts2\n" +
                            "1\ta\t1\ta\n" +
                            "2\tb\tNaN\t\n" +
                            "3\tc\tNaN\t\n" +
                            "4\td\tNaN\t\n" +
                            "5\te\tNaN\t\n");
        });
    }

    @Test
    public void testLeftHashJoinWithWhere3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s1 string)");
            insert("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            compile("create table t2 (j int, s2 string)");
            insert("insert into t2 values (5, 'e'), (3, 'c'), (2, 'b'), (4, 'd'), (1, 'a');");

            assertHashJoinSql("select * from t1 left join t2 on j = i where j = 1",
                    "i\ts1\tj\ts2\n" +
                            "1\ta\t1\ta\n");
        });
    }

    @Test
    public void testLeftHashJoinWithWhere4() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s1 string)");
            insert("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            compile("create table t2 (j int, s2 string)");
            insert("insert into t2 values (5, 'e'), (3, 'c'), (2, 'b'), (1, 'a');");

            assertHashJoinSql("select * from t1 left join t2 on j = i where j = 1 or j = null",
                    "i\ts1\tj\ts2\n" +
                            "1\ta\t1\ta\n" +
                            "4\td\tNaN\t\n");
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition0() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int);");
            compile("create table t2 as (select x+10 j from long_sequence(3))");

            String query = "select * from t1 left join t2 on t1.i+10 = t2.j";

            assertSql("i\tj\n", query);
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 as (select x i from long_sequence(5))");
            compile("create table t2 as (select x+10 j from long_sequence(3))");

            assertSql("i\tj\n" +
                    "1\t11\n" +
                    "2\t12\n" +
                    "3\t13\n" +
                    "4\tNaN\n" +
                    "5\tNaN\n", "select * from t1 left join t2 on t1.i+10 = t2.j"
            );
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 as (select x i from long_sequence(5))");
            compile("create table t2 as (select x-3 j from long_sequence(3))");//-2,-1,0

            assertSql("i\tj\n" +
                    "1\t-1\n" +
                    "2\t-2\n" +
                    "3\tNaN\n" +
                    "4\tNaN\n" +
                    "5\tNaN\n", "select * from t1 left join t2 on t1.i = - t2.j"
            );
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int)");
            insert("insert into t1 values (1), (-2), (3), (-4), (5);");
            compile("create table t2 (j int)");
            insert("insert into t2 values (-1), (-2), (3), (0), (-5);");

            String query = "select * from t1 left join t2 on abs(t1.i) = abs(t2.j)";

            assertSql("i\tj\n" +
                    "1\t-1\n" +
                    "-2\t-2\n" +
                    "3\t3\n" +
                    "-4\tNaN\n" +
                    "5\t-5\n", query
            );
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition4() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int)");
            insert("insert into t1 values (1), (2), (3), (4), (5);");
            compile("create table t2 (j int)");
            insert("insert into t2 values (-1), (-2), (-3), (-4), (-5);");

            assertSql("i\tj\n" +
                    "1\tNaN\n" +
                    "2\tNaN\n" +
                    "3\tNaN\n" +
                    "4\t-4\n" +
                    "5\t-5\n", "select * from t1 left join t2 on case when i < 4 then 0 else i end = abs(j)"
            );
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition5() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int)");
            insert("insert into t1 values (1), (2), (3), (4), (5);");
            compile("create table t2 (j int)");
            insert("insert into t2 values (-5), (-4), (-3), (-2), (-1);");

            assertSql("i\tj\n" +
                    "1\tNaN\n" +
                    "2\tNaN\n" +
                    "3\tNaN\n" +
                    "4\tNaN\n" +
                    "5\t-5\n" +
                    "5\t-4\n" +
                    "5\t-3\n" +
                    "5\t-2\n" +
                    "5\t-1\n", "select * from t1 left join t2 on i > 4  "
            );
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition6() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int)");
            insert("insert into t1 values (1), (2), (3), (4), (5);");
            compile("create table t2 (j int)");
            insert("insert into t2 values (-5), (-4), (-3), (-2), (-1);");

            assertSql("i\tj\n" +
                    "1\tNaN\n" +
                    "2\tNaN\n" +
                    "3\tNaN\n" +
                    "4\tNaN\n" +
                    "5\t-5\n" +
                    "5\t-4\n", "select * from t1 left join t2 on i > 4 and j < -3 "
            );
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition7() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int)");
            insert("insert into t1 values (1), (2), (3), (4), (5);");
            compile("create table t2 (j int)");
            insert("insert into t2 values (-5), (-4), (-3), (-2), (-1);");

            assertSql("i\tj\n" +
                    "1\t-4\n" +
                    "1\t-3\n" +
                    "1\t-2\n" +
                    "1\t-1\n" +
                    "2\t-2\n" +
                    "2\t-1\n" +
                    "3\t-1\n" +
                    "4\t-1\n" +
                    "5\tNaN\n", "select * from t1 left join t2 on i*j >= -4 "
            );
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition8() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int)");
            insert("insert into t1 values (1), (2), (3), (4), (5);");
            compile("create table t2 (j int)");
            insert("insert into t2 values (-5), (-4), (-3), (-2), (-1);");

            assertSql("i\tj\n" +
                    "1\t-1\n" +
                    "2\t-2\n" +
                    "3\tNaN\n" +
                    "4\tNaN\n" +
                    "5\tNaN\n", "select * from t1 left join t2 on abs(i) = abs(j) and abs(i*j) <= 4"
            );
        });
    }

    @Test
    public void testLeftJoinOnFunctionConditionWith3Tables() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 as (select x i from long_sequence(5))");
            compile("create table t2 as (select x+10 j from long_sequence(3))");
            compile("create table t3 as (select x+1 k from long_sequence(3))");

            String query = "select * from t1 left join (select * from t2 left join t3 on t2.j-1 = t3.k) tx on t1.i+10 = tx.j";

            assertSql("i\tj\tk\n" +
                    "1\t11\tNaN\n" +
                    "2\t12\tNaN\n" +
                    "3\t13\tNaN\n" +
                    "4\tNaN\tNaN\n" +
                    "5\tNaN\tNaN\n", query
            );
        });
    }

    @Test
    public void testLeftJoinWithConstantFalseFilter() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 as (select x i from long_sequence(3))");
            compile("create table t2 as (select x+10 j from long_sequence(3))");

            String query = "select * from t1 left join t2 on i=j and abs(1) = 0";

            assertSql("i\tj\n" +
                    "1\tNaN\n" +
                    "2\tNaN\n" +
                    "3\tNaN\n", query);
        });
    }

    @Test
    public void testLtJoinLeftTimestampDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            ddl("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertException(
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from (x order by timestamp desc) x lt join y on y.sym2 = x.sym",
                    93,
                    "left"
            );
        });
    }

    @Test
    public void testLtJoinNoKeyNoLeaks() throws Exception {
        testJoinForCursorLeaks("with crj as (select x, ts from xx latest by x) select xx.x from xx lt join crj ", false);
    }

    @Test
    public void testLtJoinNoLeftTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10))");
            ddl("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertException(
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x lt join y on y.sym2 = x.sym",
                    65,
                    "left"
            );
        });
    }

    @Test
    public void testLtJoinNoRightTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            ddl("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30))");
            assertException(
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x lt join y on y.sym2 = x.sym",
                    65,
                    "right"
            );
        });
    }

    @Test
    public void testLtJoinRecordNoLeaks() throws Exception {
        testJoinForCursorLeaks("with crj as (select x, ts from xx latest by x) select xx.x from xx lt join crj on xx.x = crj.x ", false);
    }

    @Test
    public void testLtJoinRecordNoLeaks2() throws Exception {
        testJoinForCursorLeaks("with crj as (select x, ts from xx latest by x) select xx.x from xx lt join crj on xx.x = crj.x ", true);
    }

    @Test
    public void testLtJoinRightTimestampDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            ddl("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertException(
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x lt join (y order by timestamp desc) y on y.sym2 = x.sym",
                    65,
                    "right"
            );
        });
    }

    @Test
    public void testLtJoinWithComplexConditionFails1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            compile("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 lt join t2 on l1=l2+5", "unsupported LT join expression [expr='l1 = l2 + 5']", 33);
        });
    }

    @Test
    public void testLtJoinWithComplexConditionFails2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            compile("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 lt join t2 on l1>l2", "unsupported LT join expression [expr='l1 > l2']", 33);
        });
    }

    @Test
    public void testLtJoinWithComplexConditionFails3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            compile("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 lt join t2 on l1=abs(l2)", "unsupported LT join expression [expr='l1 = abs(l2)']", 33);
        });
    }

    @Test
    public void testLtJoinWithCondition01() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            compile("insert into t1 select x, x::timestamp from long_sequence(3)");
            compile("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");
            compile("insert into t2 select x, x::timestamp from long_sequence(3)");

            assertSql("l1\tts1\tl2\tts2\n" +
                    "1\t1970-01-01T00:00:00.000001Z\tNaN\t\n" +
                    "2\t1970-01-01T00:00:00.000002Z\tNaN\t\n" +
                    "3\t1970-01-01T00:00:00.000003Z\tNaN\t\n", "select * from t1 lt join t2 on l1=l2"
            );
        });
    }

    @Test
    public void testLtJoinWithoutCondition() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            insert("insert into t1 select x, x::timestamp from long_sequence(3)");
            compile("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");
            insert("insert into t2 select x, x::timestamp from long_sequence(3)");

            assertSql("l1\tts1\tl2\tts2\n" +
                    "1\t1970-01-01T00:00:00.000001Z\tNaN\t\n" +
                    "2\t1970-01-01T00:00:00.000002Z\t1\t1970-01-01T00:00:00.000001Z\n" +
                    "3\t1970-01-01T00:00:00.000003Z\t2\t1970-01-01T00:00:00.000002Z\n", "select * from t1 lt join t2"
            );
        });
    }

    @Test
    public void testLtJoinWithoutCondition2() throws Exception {
        // Here we test case when all slave records have newer timestamps than what's in the master table.
        assertMemoryLeak(() -> {
            compile("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            insert("insert into t1 select x, x::timestamp from long_sequence(3)");
            compile("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");
            insert("insert into t2 select x, (x + 1000000)::timestamp from long_sequence(3)");

            assertSql("l1\tts1\tl2\tts2\n" +
                    "1\t1970-01-01T00:00:00.000001Z\tNaN\t\n" +
                    "2\t1970-01-01T00:00:00.000002Z\tNaN\t\n" +
                    "3\t1970-01-01T00:00:00.000003Z\tNaN\t\n", "select * from t1 lt join t2"
            );
        });
    }

    @Test
    public void testMultipleJoinsWithTopLevelSelect() throws Exception {
        ddl(
                "CREATE TABLE train ( " +
                        "  id INT, " +
                        "  date timestamp, " +
                        "  store_nbr INT, " +
                        "  family SYMBOL, " +
                        "  sales DOUBLE " +
                        ") timestamp (date) PARTITION BY YEAR"
        );

        insert("insert into train values (1, '2015-05-31T00:00:00', 1, 'A', 1.0 )");

        String query = "WITH train_lim as (select id, date, store_nbr, family, sales from train where date < '2017-07-16' AND date > '2012-12-29') " +
                "SELECT s.id  " +
                "FROM train_lim s " +
                "#JOIN_TYPE# JOIN " +
                "( " +
                "    SELECT * FROM train_lim   " +
                "    #JOIN_TYPE# JOIN  " +
                "    ( " +
                "        SELECT * FROM train_lim  " +
                "    ) ON (store_nbr, family) " +
                ") ON (store_nbr, family)";

        assertRepeatedJoinQuery(query, "LT", true);
        assertRepeatedJoinQuery(query, "ASOF", true);
        assertRepeatedJoinQuery(query, "INNER", true);
        assertRepeatedJoinQuery(query, "LEFT", false);
    }

    @Test
    public void testNestedCrossJoinCount() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t(c0 timestamp, c1 int, c2 int);\n");
            insert("insert into t values('2023-09-21T10:00:00.000000Z',1,1);\n");
            insert("insert into t values('2023-09-21T10:00:00.000000Z',1,1);\n");

            assertSql("count\n0\n",
                    "select count(*) " +
                            "from t as t1 " +
                            "join t as t2 on t1.c0<t2.c0 " +
                            "cross join t as t3");

            assertSql("count\n0\n",
                    "select count(*) " +
                            "from t as t3 " +
                            "cross join t as t1 " +
                            "join t as t2 on t1.c0<t2.c0 ");

            assertSql("count\n0\n",
                    "select count(*) " +
                            "from t as t3 " +
                            "cross join t as t2 " +
                            "join t as t1 on t1.c0<t2.c0 ");
        });

    }

    @Test
    public void testSelectAliasTest() throws Exception {
        assertMemoryLeak(() -> {
            ddl(
                    "create table contact_events as (" +
                            "  select rnd_symbol(4,4,4,2) _id, " +
                            "    rnd_symbol(4,4,4,2) contactid, " +
                            "    CAST(x as Timestamp) timestamp, " +
                            "    rnd_symbol(4,4,4,2) groupId " +
                            "  from long_sequence(50)) " +
                            "timestamp(timestamp)"
            );
            ddl(
                    "create table contacts as (" +
                            "  select rnd_symbol(4,4,4,2) _id, " +
                            "    CAST(x as Timestamp) timestamp, " +
                            "    rnd_symbol(4,4,4,2) notRealType " +
                            "  from long_sequence(50)) " +
                            "timestamp(timestamp)"
            );

            assertQuery("id\n",
                    "with\n" +
                            "eventlist as (select * from contact_events latest on timestamp partition by _id order by timestamp)\n" +
                            ",contactlist as (select * from contacts latest on timestamp partition by _id order by timestamp)\n" +
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
    public void testSpliceCorrectness() throws Exception {
        assertMemoryLeak(() -> {

            ddl("create table orders (sym SYMBOL, amount DOUBLE, side BYTE, timestamp TIMESTAMP) timestamp(timestamp)");

            ddl("create table quotes (sym SYMBOL, bid DOUBLE, ask DOUBLE, timestamp TIMESTAMP) timestamp(timestamp)");

            try (
                    TableWriter orders = getWriter("orders");
                    TableWriter quotes = getWriter("quotes")
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

            ddl(
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
                            ") timestamp (timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
            );

            assertQueryAndCache(expected, query, null, false);

            ddl(
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
                            ") timestamp(timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
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
    public void testSpliceJoinFailsBecauseSubqueryDoesntSupportRandomAccess() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE trade (\n" +
                    "  ts TIMESTAMP,\n" +
                    "  instrument SYMBOL,\n" +
                    "  price DOUBLE,\n" +
                    "  qty DOUBLE\n" +
                    ") timestamp (ts) PARTITION BY MONTH", sqlExecutionContext);

            assertFailure("SELECT *\n" +
                    "FROM \n" +
                    "(\n" +
                    "  SELECT ts, SUM(price * qty) / SUM(qty) vwap\n" +
                    "  FROM trade\n" +
                    "  WHERE instrument = 'A'\n" +
                    "  SAMPLE by 5m ALIGN TO FIRST OBSERVATION\n" +
                    ") \n" +
                    "SPLICE JOIN trade ", "left side of splice join doesn't support random access", 146);

            assertFailure("SELECT *\n" +
                    "FROM trade " +
                    "SPLICE JOIN " +
                    "(\n" +
                    "  SELECT ts, SUM(price * qty) / SUM(qty) vwap\n" +
                    "  FROM trade\n" +
                    "  WHERE instrument = 'A'\n" +
                    "  SAMPLE BY 5m ALIGN TO FIRST OBSERVATION\n" +
                    ") \n", "right side of splice join doesn't support random access", 20);
        });
    }

    @Test
    public void testSpliceJoinFailsInFullFatMode() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE trade (\n" +
                    "  ts TIMESTAMP,\n" +
                    "  instrument SYMBOL,\n" +
                    "  price DOUBLE,\n" +
                    "  qty DOUBLE\n" +
                    ") timestamp (ts) PARTITION BY MONTH");

            assertException(
                    "SELECT *" +
                            "FROM trade t1 " +
                            "SPLICE JOIN trade t2",
                    22,
                    "splice join doesn't support full fat mode",
                    true
            );
        });
    }

    @Test
    public void testSpliceJoinLeftTimestampDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            ddl("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertException(
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from (x order by timestamp desc) x splice join y on y.sym2 = x.sym",
                    93,
                    "left"
            );
        });
    }

    @Test
    public void testSpliceJoinNoLeftTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10))");
            ddl("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertException(
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x splice join y on y.sym2 = x.sym",
                    65,
                    "left"
            );
        });
    }

    @Test
    public void testSpliceJoinNoRightTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            ddl("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30))");
            assertException(
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x splice join y on y.sym2 = x.sym",
                    65,
                    "right"
            );
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

            ddl(
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
                            ") timestamp (timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
            );

            assertQueryAndCache(expected, query, null, false);

            ddl(
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
                            ") timestamp(timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
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
    public void testSpliceJoinRecordNoLeaks() throws Exception {
        testJoinForCursorLeaks("with crj as (select x, ts from xx latest by x) select xx.x from xx splice join crj on xx.x = crj.x ", false);
    }

    @Test
    public void testSpliceJoinRightTimestampDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            ddl("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertException(
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x splice join (y order by timestamp desc) y on y.sym2 = x.sym",
                    65,
                    "right"
            );
        });
    }

    @Test
    public void testSpliceJoinWithComplexConditionFails1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            compile("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 splice join t2 on l1=l2+5", "unsupported SPLICE join expression [expr='l1 = l2 + 5']", 37);
        });
    }

    @Test
    public void testSpliceJoinWithComplexConditionFails2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            compile("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 splice join t2 on l1>l2", "unsupported SPLICE join expression [expr='l1 > l2']", 37);
        });
    }

    @Test
    public void testSpliceJoinWithComplexConditionFails3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            compile("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 splice join t2 on l1=abs(l2)", "unsupported SPLICE join expression [expr='l1 = abs(l2)']", 37);
        });
    }

    @Test
    public void testSpliceOfJoinAliasDuplication() throws Exception {
        assertMemoryLeak(() -> {
            // ASKS
            ddl("create table asks(ask int, ts timestamp) timestamp(ts) partition by none");
            insert("insert into asks values(100, 0)");
            insert("insert into asks values(101, 2);");
            insert("insert into asks values(102, 4);");

            // BIDS
            ddl("create table bids(bid int, ts timestamp) timestamp(ts) partition by none");
            insert("insert into bids values(101, 1);");
            insert("insert into bids values(102, 3);");
            insert("insert into bids values(103, 5);");

            String query =
                    "select \n" +
                            "    b.timebid timebid,\n" +
                            "    a.timeask timeask, \n" +
                            "    b.b b, \n" +
                            "    a.a a\n" +
                            "from (select b.bid b, b.ts timebid from bids b) b \n" +
                            "    splice join\n" +
                            "(select a.ask a, a.ts timeask from asks a) a\n" +
                            "WHERE (b.timebid != a.timeask);";

            String expected = "timebid\ttimeask\tb\ta\n" +
                    "\t1970-01-01T00:00:00.000000Z\tNaN\t100\n" +
                    "1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000000Z\t101\t100\n" +
                    "1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000002Z\t101\t101\n" +
                    "1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000002Z\t102\t101\n" +
                    "1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000004Z\t102\t102\n" +
                    "1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000004Z\t103\t102\n";

            printSqlResult(expected, query, null, false, false);
        });
    }

    @Test
    public void testTypeMismatch() throws Exception {
        testTypeMismatch0(false);
    }

    @Test
    public void testTypeMismatchFF() throws Exception {
        testFullFat(this::testTypeMismatch0);
    }

    @Test
    public void testUnionAllCount() throws Exception {
        assertMemoryLeak(() -> {
            // 1 partition
            compile("create table TabA ( " +
                    "          ts timestamp, " +
                    "          x long " +
                    "        ) timestamp(ts) PARTITION by month");

            // 3 partitions
            compile("create table TabB ( " +
                    "          ts timestamp, " +
                    "          x long " +
                    "        ) timestamp(ts) PARTITION by hour");

            // 0 partitions
            compile("create table TabC ( " +
                    "          ts timestamp, " +
                    "          x long " +
                    "        ) timestamp(ts) PARTITION by year");

            insert("insert into TabA select x::timestamp, x/6 from long_sequence(10)");
            insert("insert into TabB select (x*15L*60L*1000000L)::timestamp, x/6 from long_sequence(10)");

            //async filter
            String selectWithFilter = "(select * from TabA where x = 0 " +
                    "union all " +
                    "select * from TabB where x = 1 " +
                    "union all " +
                    "select * from taBC where x = 0 )";
            assertSkipToAndCalculateSize(selectWithFilter, 10);

            //async filter with limit
            String selectWithFilterAndLimit = "( " +
                    "selecT * from " +
                    "(select * from TabA where x = 0 limit 3) " +
                    "union all " +
                    "(select * from TabB where x = 1 limit 3) " +
                    "union all " +
                    "(select * from taBC where x = 0 limit 1) )";
            assertSkipToAndCalculateSize(selectWithFilterAndLimit, 6);

            // fwd data frame
            String selectWithFwdFrame = "(select * from TabA union all select * from TabB union all select * from TabC)";
            assertSkipToAndCalculateSize(selectWithFwdFrame, 20);

            // bwd data frame
            String selectWithBwdFrame = "(select * from " +
                    "(select * from TabA order by ts desc) " +
                    "union all " +
                    "(select * from TabB order by ts desc) " +
                    "union all (select * from tabC order by ts desc) )";
            assertSkipToAndCalculateSize(selectWithBwdFrame, 20);

            // interval fwd data frame
            String selectWithIntervalFwdFrame = "(" +
                    "(select * from TabA where ts > 1) " +
                    "union all " +
                    "(select * from TabB where ts > 15L*60L*1000000L) " +
                    "union all " +
                    "(select * from TabC where ts > 1))";
            assertSkipToAndCalculateSize(selectWithIntervalFwdFrame, 18);

            String selectWithIntervalBwdFrame = "(" +
                    "(select * from TabA where ts > 1 order by ts desc) " +
                    "union all " +
                    "(select * from TabB where ts > 15L*60L*1000000L order by ts desc) " +
                    "union all " +
                    "(select * from TabC where ts > 1 order by ts desc))";
            assertSkipToAndCalculateSize(selectWithIntervalBwdFrame, 18);
        });
    }

    @Test
    public void testUnionAllCursorLeaks() throws Exception {
        testJoinForCursorLeaks("with crj as (select x, ts from xx latest by x) select x from xx union all select x from crj", false);
    }

    @Test
    public void testUnionCursorLeaks() throws Exception {
        testJoinForCursorLeaks("with crj as (select x, ts from xx latest by x) select x from xx union select x from crj", false);
    }

    @Test
    public void testSymbolVarcharJoin() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table xy2 as (select rnd_varchar(1,3,1) a from long_sequence(10000))");
            compile("create table xy3 as (select a::symbol a, rnd_int() b from xy2);");
            assertSql("a\tb\ta1\n" +
                    "핕\t1978723740\t핕\n" +
                    "K|\t-1755013015\tK|\n" +
                    "J1\t-1542807447\tJ1\n" +
                    "ӄǈ\t-311197613\tӄǈ\n" +
                    "Lg\t864972949\tLg\n", "xy3 join xy2 on (a) limit 5");
        });
    }

    @Test
    public void testStringSymbolVarcharJoins() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t1 (i int, s string, b symbol)");
            insert("insert into t1 values (1, 'a', 'a'), (2, 'b', 'b'), (3, 'c', 'c'), (4, 'd', 'd'), (5, 'e', 'e');");
            compile("create table t2 (j int, v varchar)");
            insert("insert into t2 values (5, 'e'), (3, 'c'), (2, 'b'), (4, 'd'), (1, 'a');");

            final String expected = "i\ts\tb\tj\tv\n" +
                    "1\ta\ta\t1\ta\n" +
                    "2\tb\tb\t2\tb\n" +
                    "3\tc\tc\t3\tc\n" +
                    "4\td\td\t4\td\n" +
                    "5\te\te\t5\te\n";

            assertSql(expected, "select i, s, b, j, v from t1 inner join t2 on s = v");
            assertSql(expected, "select i, s, b, j, v from t1 inner join t2 on b = v");
            assertSql(expected, "select i, s, b, j, v from t1 left join t2 on s = v");
            assertSql(expected, "select i, s, b, j, v from t1 left join t2 on b = v");

            final String expected2 = "i\ts\tb\tj\tv\n" +
                    "1\ta\ta\t5\te\n" +
                    "1\ta\ta\t3\tc\n" +
                    "1\ta\ta\t2\tb\n" +
                    "1\ta\ta\t4\td\n" +
                    "1\ta\ta\t1\ta\n" +
                    "2\tb\tb\t5\te\n" +
                    "2\tb\tb\t3\tc\n" +
                    "2\tb\tb\t2\tb\n" +
                    "2\tb\tb\t4\td\n" +
                    "2\tb\tb\t1\ta\n" +
                    "3\tc\tc\t5\te\n" +
                    "3\tc\tc\t3\tc\n" +
                    "3\tc\tc\t2\tb\n" +
                    "3\tc\tc\t4\td\n" +
                    "3\tc\tc\t1\ta\n" +
                    "4\td\td\t5\te\n" +
                    "4\td\td\t3\tc\n" +
                    "4\td\td\t2\tb\n" +
                    "4\td\td\t4\td\n" +
                    "4\td\td\t1\ta\n" +
                    "5\te\te\t5\te\n" +
                    "5\te\te\t3\tc\n" +
                    "5\te\te\t2\tb\n" +
                    "5\te\te\t4\td\n" +
                    "5\te\te\t1\ta\n";

            assertSql(expected2, "select i, s, b, j, v from t1 cross join t2");
        });
    }

    private void assertFailure(String query, String expectedMessage, int position) {
        try {
            ddl(query, sqlExecutionContext);
            Assert.fail("query '" + query + "' should have failed with '" + expectedMessage + "' message!");
        } catch (SqlException | ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
            Assert.assertEquals(Chars.toString(query), position, e.getPosition());
        }
    }

    private void assertHashJoinSql(String query, String expected) throws SqlException {
        assertSql(expected, query);
        printSql(query, true);
        TestUtils.assertEquals("full fat join", expected, sink);
    }

    private void assertRepeatedJoinQuery(String query, String left, boolean expectSize) throws SqlException {
        assertQuery("id\n1\n", query.replace("#JOIN_TYPE#", left), null, false, expectSize);
    }

    private void assertSkipToAndCalculateSize(String select, int size) throws SqlException {
        assertSql("count\n" + size + "\n", "select count(*) from " + select);

        RecordCursor.Counter counter = new RecordCursor.Counter();

        try (RecordCursorFactory factory = select(select)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                Assert.assertEquals(size, counter.get());

                for (int i = 0; i < size + 2; i++) {
                    cursor.toTop();
                    counter.set(i);
                    cursor.skipRows(counter);

                    Assert.assertEquals(Math.max(i - size, 0), counter.get());

                    counter.clear();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(Math.max(size - i, 0), counter.get());

                    cursor.toTop();
                    for (int j = 0; j < i; j++) {
                        if (!cursor.hasNext()) {
                            break;
                        }
                    }

                    counter.clear();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(Math.max(size - i, 0), counter.get());
                }
            }
        }
    }

    private void testAsOfJoin0(boolean fullFatJoin) throws Exception {
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

            ddl(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)"
            );

            ddl(
                    "create table y as (" +
                            "select cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)"
            );

            assertQueryAndCacheFullFat(expected, query, "timestamp", false, true);

            ddl(
                    "insert into x select * from (" +
                            "select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)"
            );

            ddl(
                    "insert into y select * from (" +
                            "select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)"
            );

            assertQueryFullFat(
                    "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
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
                    true,
                    fullFatJoin
            );
        });
    }

    private void testAsOfJoinNoStrings0(boolean fullFatJoin) throws Exception {
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

            ddl(
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
                            ") timestamp (timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
            );

            assertQueryAndCacheFullFat(expected, query, "timestamp", false, true);

            insert(
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
                            ") timestamp(timestamp)"
            );
            insert(
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
                            ") timestamp(timestamp)"
            );

            assertQueryFullFat(
                    "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
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
                    true,
                    fullFatJoin
            );
        });
    }

    private void testAsOfJoinOnStrNoVar0(boolean fullFatJoin) throws Exception {
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

            ddl(
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
                            ") timestamp (timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            ddl(
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
                            ") timestamp(timestamp)"
            );
            ddl(
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
                            ") timestamp(timestamp)"
            );

            assertQueryFullFat(
                    "i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1\n" +
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
                    true,
                    fullFatJoin
            );
        });
    }

    private void testAsOfJoinOnVarcharNoVar0(boolean fullFatJoin) throws Exception {
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

            ddl(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_varchar('ABC', 'CDE', null, 'XYZ') c," +
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
                            ") timestamp (timestamp)"
            );
            ddl(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_varchar('ABC', 'CDE', null, 'XYZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)"
            );

            assertQueryAndCache(expected, query, "timestamp", true);

            ddl(
                    "insert into x select * from " +
                            "(select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_varchar('ABC', 'CDE', null, 'KZZ') c," +
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
                            ") timestamp(timestamp)"
            );
            ddl(
                    "insert into y select * from " +
                            "(select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_varchar('ABC', 'CDE', null, 'KZZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)"
            );

            assertQueryFullFat(
                    "i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1\n" +
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
                    true,
                    fullFatJoin
            );
        });
    }

    private void testAsOfJoinSlaveSymbol0(boolean fullFatJoin) throws Exception {
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

            ddl(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)"
            );
            ddl(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)"
            );

            assertQueryAndCacheFullFat(expected, query, "timestamp", false, true);

            ddl("insert into x select * from (select cast(x + 10 as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            ddl("insert into y select * from (select cast(x + 30 as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");

            assertQueryFullFat("i\tsym\tsym2\tamt\tprice\ttimestamp\ttimestamp1\n" +
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
                    true,
                    fullFatJoin
            );
        });
    }

    private void testFullFat(TestMethod method) throws Exception {
        method.run(true);
    }

    private void testJoinColumnPropagationIntoJoinModel0(String joinType, boolean expectSize) throws SqlException {
        String query = ("SELECT amount, price1\n" +
                "FROM\n" +
                "(\n" +
                "  SELECT *\n" +
                "  FROM trades b \n" +
                "  #JOIN_TYPE# \n" +
                "  (\n" +
                "    SELECT * \n" +
                "    FROM trades \n" +
                "    WHERE price > 1\n" +
                "      AND symbol = 'ETH-USD'\n" +
                "  ) a ON #JOIN_CLAUSE#\n" +
                "  WHERE b.amount > 1\n" +
                "    AND b.symbol = 'ETH-USD'\n" +
                ")").replace("#JOIN_TYPE#", joinType);
        String expected = "LT JOIN".equals(joinType) ? "amount\tprice1\n2.0\tNaN\n" : "amount\tprice1\n2.0\t2.0\n";

        assertQuery(expected, query.replace("#JOIN_CLAUSE#", "symbol"), null, false, expectSize);
        assertQuery(expected, query.replace("#JOIN_CLAUSE#", "a.symbol = b.symbol"), null, false, expectSize);
        assertQuery(expected, query.replace("#JOIN_CLAUSE#", "a.symbol = b.symbol and a.price = b.price"), null, false, expectSize);
        assertQuery(expected, query.replace("#JOIN_CLAUSE#", "b.symbol = a.symbol and a.timestamp = b.timestamp"), null, false, expectSize);
    }

    private void testJoinConstantFalse0(boolean fullFatJoin) throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "c\ta\tb\tcolumn\n";
            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(10))");
            ddl("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))");

            // master records should be filtered out because slave records missing
            assertQueryFullFat(
                    expected,
                    "select x.c, x.a, b, a+b from x join y on y.m = x.c and 1 > 10",
                    null,
                    false,
                    true,
                    fullFatJoin
            );
        });
    }

    private void testJoinConstantTrue0(boolean fullFatJoin) throws Exception {
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(10))");
            ddl("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))");

            // master records should be filtered out because slave records missing
            assertQueryFullFat(
                    expected,
                    "select x.c, x.a, b from x join y on y.m = x.c and 1 < 10",
                    null,
                    false,
                    true,
                    fullFatJoin
            );
        });
    }

    private void testJoinForCursorLeaks(String sql, boolean fullFatJoins) throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger counter = new AtomicInteger();
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRO(LPSZ name) {
                    if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "ts.d") && counter.incrementAndGet() == 1) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            ddl("create table xx as (" +
                    "select x," +
                    " timestamp_sequence(0, 1000) ts" +
                    " from long_sequence(100000)) timestamp (ts)");

            try {
                assertException(sql, sqlExecutionContext, fullFatJoins);
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-only");
                TestUtils.assertContains(ex.getFlyweightMessage(), "ts.d");
            }
        });
    }

    private void testJoinInner0(boolean fullFatJoin) throws Exception {
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x ts from long_sequence(5)) timestamp(ts)");
            ddl("create table y as (select cast((x-1)/4 + 1 as int) c, abs(rnd_int() % 100) b from long_sequence(20))");
            ddl("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))");

            assertQueryFullFat(
                    expected,
                    "select z.c, x.a, b, d, d-b from x join y on(c) join z on (c)",
                    null,
                    false,
                    true,
                    fullFatJoin
            );
        });
    }

    private void testJoinInnerAllTypes0(boolean fullFatJoin) throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tvch\tkk1\ta1\tb1\tc1\td1\te1\tf1\tg1\ti1\tj1\tk1\tl1\tm1\tn1\tvch1\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t-1915752164\tfalse\tI\t0.8786111112537701\t0.9966\t403\t2015-08-19T00:36:24.375Z\tKLGM\t-8506266080452644687\t1970-01-01T00:00:00.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t\t\uDBC5\uDF7B\uDBE5\uDD8F.vmOa\uDA76\uDDD4*\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t-1871096754\tfalse\tP\t0.4740684604688953\t0.0181\t148\t2015-10-12T03:40:03.380Z\tQDYO\t-8490120737538725244\t1970-01-01T00:16:40.000000Z\t2\t00000000 44 11 e2 a3 24 4e 44 a8 0d fe 27 ec 53 13 5d\tGFNWGRMDGG\t\uE044|'ꋯɟ\uF6BE\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t1247654963\ttrue\tY\t0.7842455970681089\t0.3162\t584\t2015-06-24T18:10:25.514Z\t\t-2317221228709139922\t1970-01-01T00:33:20.000000Z\t29\t00000000 94 5f ec d3 dc f8 43 b2 e3 75 62 60 af 6d 8c d8\n" +
                    "00000010 ac c8\tZQSNPXMKJSMKIXEY\t\uDA8B\uDFC4︵Ƀ^DU?$s_*\uDBAE\uDF56\n" +
                    "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tԈ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t-1535701773\ttrue\tV\t0.32824342042623134\t0.7826\t299\t2015-07-22T20:23:51.186Z\tKLGM\t1363805506825579405\t1970-01-01T00:50:00.000000Z\t17\t00000000 fe b9 52 dd 4d f3 f9 76 f6 85 ab a3 ab ee 6d 54\n" +
                    "00000010 75 10\tWLNLR\tw雑\uDB9C\uDCE1\uD9F6\uDF3D횝\uDB9A\uDF230pጙ锿\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t2\t233699709\ttrue\tV\t0.7727320382377867\t0.7708\t846\t2015-10-03T22:53:17.989Z\t\t8336821503223023075\t1970-01-01T01:06:40.000000Z\t18\t00000000 8a b0 35 d8 ab 3f a1 f5 4b ea 01 c9\tZHEISQHNO\t\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t2\t1939793032\tfalse\tI\t0.5765797240495835\t0.4913\t833\t2015-07-31T03:51:57.870Z\t\t-8646234590100970125\t1970-01-01T01:23:20.000000Z\t5\t00000000 7e c0 1d 6c a9 65 81 ad 79 87 fc 92 83 fc 88 f3\n" +
                    "00000010 32 27\tLPTYXYGYFUXCD\t<'ָ읈ҽ\uDA01\uDE60E죢魷ꞛ#DQ헙\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t2\t1361673432\ttrue\tM\t0.16979644136429572\t0.2812\t59\t2015-02-16T09:18:55.746Z\t\t-6746433171163409313\t1970-01-01T01:40:00.000000Z\t4\t\tDDCIHCNP\t\n" +
                    "2\t-1787109293\ttrue\tG\tNaN\t0.8001\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\t\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\t2\t-1169915830\ttrue\tP\tNaN\t0.0589\t359\t2015-05-26T17:24:24.749Z\t\t-7350430133595690521\t1970-01-01T01:56:40.000000Z\t14\t00000000 35 3b 1c 9c 1d 5c c1 5d 2d 44 ea 00 81 c4 19 a1\n" +
                    "00000010 ec\tSMIFDYPDK\t\n" +
                    "3\t844704299\ttrue\t\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:33:20.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                    "00000010 e7 0c 89\tLJUMLGLHMLLEO\t+\u0093ً\uDAF5\uDE17qRӽ-\t3\t-1505690678\tfalse\tR\t0.09854153834719315\t0.2329\t82\t2015-06-03T01:01:00.230Z\tQDYO\t-7725099828175109832\t1970-01-01T02:13:20.000000Z\t27\t\tZUPVQFULMER\tּ\uDB48\uDC78{ϸ\uD9F4\uDFB9\uDA0A\uDC7A\uDA76\uDC87>\uD8F0\uDF66Ҫb\uDBB1\uDEA3\n" +
                    "3\t844704299\ttrue\t\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:33:20.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                    "00000010 e7 0c 89\tLJUMLGLHMLLEO\t+\u0093ً\uDAF5\uDE17qRӽ-\t3\t600986867\tfalse\tM\t0.19823647700531244\tNaN\t557\t2015-01-30T03:27:34.392Z\t\t5324839128380055812\t1970-01-01T02:30:00.000000Z\t25\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a\n" +
                    "00000010 42 71 a3 7a\tDNZNLCNGZTOY\t\u07B3\uDA8F\uDC319믓˫ᡙ\uDBEC\uDE3B櫑߸!>\uD9F3\uDFD5a~=V\n" +
                    "3\t844704299\ttrue\t\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:33:20.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                    "00000010 e7 0c 89\tLJUMLGLHMLLEO\t+\u0093ً\uDAF5\uDE17qRӽ-\t3\t100444418\tfalse\tK\t0.28400807705010733\t0.5784\t1015\t2015-05-21T09:22:31.780Z\tXSLU\t-2052253029650705565\t1970-01-01T02:46:40.000000Z\t18\t00000000 4b b7 e2 7f ab 6e 23 03 dd c7 d6\tDRHFBCZI\t緧\uD9FF\uDE64ì˵\"&LB\n" +
                    "3\t844704299\ttrue\t\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:33:20.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                    "00000010 e7 0c 89\tLJUMLGLHMLLEO\t+\u0093ً\uDAF5\uDE17qRӽ-\t3\t-97222559\ttrue\tI\t0.1810605823104886\t0.9209\t917\t2015-08-05T02:39:14.093Z\tXSLU\t7768501691006807692\t1970-01-01T03:03:20.000000Z\t3\t00000000 c0 55 12 44 dc 4b c0 d9 1c 71 cf 5a 8f 21 06\tMQEHDHQH\tܹ\uDA38\uDDD7̊E+%륤\n" +
                    "4\t-2053564939\tfalse\tX\t0.7381752894013154\t0.7365\t448\t2015-08-30T11:34:00.859Z\tVTJW\t-7336930007738575369\t1970-01-01T00:50:00.000000Z\t15\t\tSMPGL\t\t4\t-494568109\ttrue\tU\tNaN\t0.7958\t469\t2015-05-24T04:44:08.006Z\tQDYO\t-6162343549945721271\t1970-01-01T03:20:00.000000Z\t7\t\tUKVZI\t٦ہ\uDAB6\uDF33\uDB00\uDF8AϿ˄礏ɍ\uDB2C\uDD55\uD904\uDFA0Uڥ[<\n" +
                    "4\t-2053564939\tfalse\tX\t0.7381752894013154\t0.7365\t448\t2015-08-30T11:34:00.859Z\tVTJW\t-7336930007738575369\t1970-01-01T00:50:00.000000Z\t15\t\tSMPGL\t\t4\t-670048539\ttrue\tU\t0.6747913655036517\t0.4895\t295\t\tGMXU\t6609063415252900609\t1970-01-01T03:36:40.000000Z\t28\t\tZIZKM\t\uD94E\uDE17䎡R3à䋿Ϛ\uDA6D\uDD37텎,ۃ)ǟ\n" +
                    "4\t-2053564939\tfalse\tX\t0.7381752894013154\t0.7365\t448\t2015-08-30T11:34:00.859Z\tVTJW\t-7336930007738575369\t1970-01-01T00:50:00.000000Z\t15\t\tSMPGL\t\t4\t1355454157\tfalse\t\t0.9359965050658042\t0.2008\t881\t2015-11-27T19:42:33.675Z\tXSLU\t-7991782124465626462\t1970-01-01T03:53:20.000000Z\t27\t00000000 a8 2b 8d 35 c5 85 9a 00 4a a1 06 7e 3f 4e 27 42\tHBQJP\t\uD98E\uDF30-\uE605\uDB23\uDFB3Le楣DB\uDAAD\uDE0A\uE916G[ꫭ\n" +
                    "4\t-2053564939\tfalse\tX\t0.7381752894013154\t0.7365\t448\t2015-08-30T11:34:00.859Z\tVTJW\t-7336930007738575369\t1970-01-01T00:50:00.000000Z\t15\t\tSMPGL\t\t4\t737515614\tfalse\tP\t0.9879118064160076\t0.0189\t14\t2015-08-02T08:50:10.325Z\t\t-4200011690995273529\t1970-01-01T04:10:00.000000Z\t20\t00000000 54 02 9f c2 37 98 60 bb 38 d1 36\tQBORDTQ\tz\uDA0E\uDFE4yƹC\n" +
                    "5\t-1726426588\ttrue\tZ\t0.7700798090070919\t0.4416\t158\t2015-04-21T07:10:57.197Z\tVTJW\t6121305147479698964\t1970-01-01T01:06:40.000000Z\t19\t00000000 84 52 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04\tVELLK\t䳉\uDB5F\uDE9AKJܜߧE}$\uDA43\uDFF0-㔍x钷Mͱ\t5\t1697668849\ttrue\tM\tNaN\t0.4132\t890\t2015-07-15T10:46:51.266Z\tXSLU\t6581120496001202966\t1970-01-01T04:26:40.000000Z\t13\t00000000 f4 03 ed c9 2a 4e 91 c5 e4 39 b2 dd 0d a7 bb\tDJIGSYLXGYTEQC\tږ胵zݒ邍\n" +
                    "5\t-1726426588\ttrue\tZ\t0.7700798090070919\t0.4416\t158\t2015-04-21T07:10:57.197Z\tVTJW\t6121305147479698964\t1970-01-01T01:06:40.000000Z\t19\t00000000 84 52 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04\tVELLK\t䳉\uDB5F\uDE9AKJܜߧE}$\uDA43\uDFF0-㔍x钷Mͱ\t5\t-1803543488\tfalse\tU\t0.06138053193106652\t0.5984\t108\t2015-04-07T16:45:54.013Z\t\t-5925564127232580585\t1970-01-01T04:43:20.000000Z\t15\t00000000 cf 5c 24 32 1e 3d ba 5f e3 91 7b ae d1 be 82 5f\n" +
                    "00000010 87 9e 79 1c\t\t\uD9FB\uDE6C\uDA85\uDF29䚭ϸ\uD9A8\uDFFB\n" +
                    "5\t-1726426588\ttrue\tZ\t0.7700798090070919\t0.4416\t158\t2015-04-21T07:10:57.197Z\tVTJW\t6121305147479698964\t1970-01-01T01:06:40.000000Z\t19\t00000000 84 52 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04\tVELLK\t䳉\uDB5F\uDE9AKJܜߧE}$\uDA43\uDFF0-㔍x钷Mͱ\t5\t-706221546\ttrue\tD\t0.2829857902567392\t0.6340\t242\t2015-08-02T20:54:21.297Z\tKLGM\t-5510843329132792373\t1970-01-01T05:00:00.000000Z\t34\t00000000 48 10 64 65 32 e1 a2 d4 70 b2\tLRBIDSTDTFBYHSH\t폓\uDA22\uDD8AV\uDBB1\uDE4BG&ُܵ\n" +
                    "5\t-1726426588\ttrue\tZ\t0.7700798090070919\t0.4416\t158\t2015-04-21T07:10:57.197Z\tVTJW\t6121305147479698964\t1970-01-01T01:06:40.000000Z\t19\t00000000 84 52 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04\tVELLK\t䳉\uDB5F\uDE9AKJܜߧE}$\uDA43\uDFF0-㔍x钷Mͱ\t5\t-548641641\tfalse\tT\tNaN\t0.1390\t852\t2015-01-13T12:34:15.149Z\tXSLU\t-8876530977637141165\t1970-01-01T05:16:40.000000Z\t3\t\tDCMUMK\tT\uDBA0\uDD2Fmτ⻱[N`亲m\n";

            ddl(
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
                            " rnd_str(5,16,2) n," +
                            " rnd_varchar(5,16,2) vch" +
                            " from long_sequence(5))"
            );

            ddl(
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
                            " rnd_str(5,16,2) n," +
                            " rnd_varchar(5,16,2) vch" +
                            " from long_sequence(20))"
            );

            // filter is applied to final join result
            assertQueryFullFat(
                    expected,
                    "select * from x join y on (kk)",
                    null,
                    false,
                    true,
                    fullFatJoin
            );
        });
    }

    private void testJoinInnerDifferentColumnNames0(boolean fullFatJoin) throws Exception {
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))");
            ddl("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))");
            ddl("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))");
            assertQueryFullFat(
                    expected,
                    "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c)",
                    null,
                    false,
                    true,
                    fullFatJoin
            );
        });
    }

    private void testJoinInnerInnerFilter0(boolean fullFatJoin) throws Exception {
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))");
            ddl("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))");
            ddl("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(16))");

            // filter is applied to intermediate join result
            assertQueryAndCacheFullFat(
                    expected,
                    "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where y.b < 20",
                    null,
                    false,
                    true
            );

            ddl("insert into x select cast(x+6 as int) c, abs(rnd_int() % 650) a from long_sequence(3)");
            ddl("insert into y select cast((x+19)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(16)");
            ddl("insert into z select cast((x+15)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(2)");

            assertQueryFullFat(
                    expected +
                            "7\t253\t14\t228\t214\n" +
                            "7\t253\t14\t723\t709\n" +
                            "8\t431\t0\t348\t348\n" +
                            "8\t431\t0\t790\t790\n" +
                            "9\t100\t19\t667\t648\n" +
                            "9\t100\t19\t456\t437\n" +
                            "9\t100\t8\t667\t659\n" +
                            "9\t100\t8\t456\t448\n",
                    "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where y.b < 20",
                    null,
                    false,
                    true,
                    fullFatJoin
            );
        });
    }

    private void testJoinInnerLastFilter0(boolean fullFatJoin) throws Exception {
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))");
            ddl("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))");
            ddl("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))");

            // filter is applied to final join result
            assertQueryFullFat(
                    expected,
                    "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where d-b > 100",
                    null,
                    false,
                    false,
                    fullFatJoin
            );
        });
    }

    private void testJoinInnerNoSlaveRecords0(boolean fullFatJoin) throws Exception {
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(10))");
            ddl("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))");

            assertQueryAndCache(
                    expected,
                    "select x.c, x.a, b from x join y on y.m = x.c",
                    null,
                    true
            );

            insert("insert into x select cast(x+10 as int) c, abs(rnd_int() % 650) a from long_sequence(4)");
            insert("insert into y select x, cast(2*((x-1+10)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(6)");

            assertQuery(
                    expected +
                            "12\t347\t7\n" +
                            "12\t347\t0\n" +
                            "14\t197\t50\n" +
                            "14\t197\t68\n",
                    "select x.c, x.a, b from x join y on y.m = x.c",
                    null,
                    false,
                    false,
                    fullFatJoin
            );
        });
    }

    private void testJoinInnerOnSymbol0(boolean fullFatJoin) throws Exception {
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

            ddl("create table x as (select rnd_symbol('A','B',null,'D') c, abs(rnd_int() % 650) a from long_sequence(5))");
            ddl("create table y as (select rnd_symbol('B','A',null,'D') m, abs(rnd_int() % 100) b from long_sequence(20))");
            ddl("create table z as (select rnd_symbol('D','B',null,'A') c, abs(rnd_int() % 1000) d from long_sequence(16))");

            // filter is applied to intermediate join result
            assertQueryAndCacheFullFat(
                    expected,
                    "select x.c xc, z.c zc, y.m yc, x.a, b, d, d-b from x join y on y.m = x.c join z on (c)",
                    null,
                    false,
                    true
            );

            insert("insert into x select rnd_symbol('L','K','P') c, abs(rnd_int() % 650) a from long_sequence(3)");
            insert("insert into y select rnd_symbol('P','L','K') m, abs(rnd_int() % 100) b from long_sequence(6)");
            insert("insert into z select rnd_symbol('K','P','L') c, abs(rnd_int() % 1000) d from long_sequence(6)");

            assertQueryFullFat(
                    expected +
                            "L\tL\tL\t148\t38\t121\t83\n" +
                            "L\tL\tL\t148\t52\t121\t69\n",
                    "select x.c xc, z.c zc, y.m yc, x.a, b, d, d-b from x join y on y.m = x.c join z on (c)",
                    null,
                    false,
                    true,
                    fullFatJoin
            );

        });
    }

    private void testJoinInnerPostJoinFilter0(boolean fullFatJoin) throws Exception {
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))");
            ddl("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))");
            ddl("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(16))");

            // filter is applied to intermediate join result
            assertQueryAndCacheFullFat(
                    expected,
                    "select z.c, x.a, b, d, a+b from x join y on y.m = x.c join z on (c) where a+b < 300",
                    null,
                    false,
                    true
            );

            insert("insert into x select cast(x+6 as int) c, abs(rnd_int() % 650) a from long_sequence(3)");
            insert("insert into y select cast((x+19)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(16)");
            insert("insert into z select cast((x+15)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(2)");

            assertQueryFullFat(
                    expected +
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
                    null,
                    false,
                    true,
                    fullFatJoin
            );
        });
    }

    private void testJoinOuterAllTypes0(boolean fullFatJoins) throws Exception {
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

            ddl(
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
                            " from long_sequence(10))"
            );

            ddl(
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
                            " from long_sequence(10))"
            );

            // filter is applied to final join result
            assertQueryFullFat(
                    expected,
                    "select * from x left join y on (kk)",
                    null,
                    false,
                    false,
                    fullFatJoins
            );
        });
    }

    private void testJoinOuterNoSlaveRecords0(boolean fullFatJoins) throws Exception {
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

            ddl("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x ts from long_sequence(10)) timestamp(ts)");
            ddl("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))");

            // master records should be filtered out because slave records missing
            assertQueryAndCache(expected, "select x.c, x.a, b from x left join y on y.m = x.c", null, false);

            insert("insert into x select * from (select cast(x+10 as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x + 10 ts from long_sequence(4)) timestamp(ts)");
            insert("insert into y select x, cast(2*((x-1+10)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(6)");

            assertQueryFullFat(
                    expected +
                            "11\t467\tNaN\n" +
                            "12\t347\t7\n" +
                            "12\t347\t0\n" +
                            "13\t244\tNaN\n" +
                            "14\t197\t50\n" +
                            "14\t197\t68\n",
                    "select x.c, x.a, b from x left join y on y.m = x.c",
                    null,
                    false,
                    false,
                    fullFatJoins
            );
        });
    }

    private void testTypeMismatch0(boolean fullFatJoins) throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select x c, abs(rnd_int() % 650) a from long_sequence(5))");
            ddl("create table y as (select cast((x-1)/4 + 1 as int) c, abs(rnd_int() % 100) b from long_sequence(20))");
            ddl("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))");
            assertException(
                    "select z.c, x.a, b, d, d-b from x join y on(c) join z on (c)",
                    44,
                    "join column type mismatch",
                    fullFatJoins
            );
        });
    }

    @FunctionalInterface
    private interface TestMethod {
        void run(boolean fullFatJoin) throws Exception;
    }
}
