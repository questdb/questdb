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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
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
            execute("""
                    create table table_1 (
                              ts timestamp,
                              name string,
                              age int,
                              member boolean
                    ) timestamp(ts) PARTITION by month
                    """);

            execute("insert into table_1 values ( '2022-10-25T01:00:00.000000Z', 'alice',  60, True )");
            execute("insert into table_1 values ( '2022-10-25T02:00:00.000000Z', 'peter',  58, False )");
            execute("insert into table_1 values ( '2022-10-25T03:00:00.000000Z', 'david',  21, True )");

            execute("""
                    create table table_2 (
                              ts timestamp,
                              name string,
                              age int,
                              address string
                    ) timestamp(ts) PARTITION by month
                    """);

            execute("insert into table_2 values ( '2022-10-25T01:00:00.000000Z', 'alice',  60,  '1 Glebe St' )");
            execute("insert into table_2 values ( '2022-10-25T02:00:00.000000Z', 'peter',  58, '1 Broon St' )");
            execute("insert into table_2 values ( '2022-10-25T04:00:00.000000Z', 'tom',  24, '1 Houston St' )");

            // query "2"
            assertQuery("""
                     select a.name, a.age, a.member, b.address, a.ts
                     from table_1 as a
                     left join table_2 as b
                        on a.ts = b.ts\
                    """)
                    .noLeakCheck()
                    .ddl(null)
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            name\tage\tmember\taddress\tts
                            alice\t60\ttrue\t1 Glebe St\t2022-10-25T01:00:00.000000Z
                            peter\t58\tfalse\t1 Broon St\t2022-10-25T02:00:00.000000Z
                            david\t21\ttrue\t\t2022-10-25T03:00:00.000000Z
                            """);

            assertQuery("""
                     select a.name, a.age, a.member, b.address, a.ts
                     from table_2 as b
                     right join table_1 as a
                        on a.ts = b.ts
                    """)
                    .noLeakCheck()
                    .ddl(null)
                    .noRandomAccess()
                    .returns("""
                            name\tage\tmember\taddress\tts
                            alice\t60\ttrue\t1 Glebe St\t2022-10-25T01:00:00.000000Z
                            peter\t58\tfalse\t1 Broon St\t2022-10-25T02:00:00.000000Z
                            david\t21\ttrue\t\t2022-10-25T03:00:00.000000Z
                            """);

            assertQuery("""
                     select a.name, a.age, a.member, b.address, a.ts
                     from table_2 as b
                     full join table_1 as a
                        on a.ts = b.ts
                    """)
                    .noLeakCheck()
                    .ddl(null)
                    .noRandomAccess()
                    .returns("""
                            name\tage\tmember\taddress\tts
                            alice\t60\ttrue\t1 Glebe St\t2022-10-25T01:00:00.000000Z
                            peter\t58\tfalse\t1 Broon St\t2022-10-25T02:00:00.000000Z
                            \tnull\tfalse\t1 Houston St\t
                            david\t21\ttrue\t\t2022-10-25T03:00:00.000000Z
                            """);

            // query "3"
            assertQuery("""
                    select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)
                    from table_1 as a
                    left join table_2 as b
                    on a.ts between dateadd('m', -1, b.ts)  and dateadd('m', 1, b.ts)
                    """)
                    .noLeakCheck()
                    .ddl(null)
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            name\tage\taddress\tts\tdateadd\tdateadd1
                            alice\t60\t1 Glebe St\t2022-10-25T01:00:00.000000Z\t2022-10-25T00:59:00.000000Z\t2022-10-25T01:01:00.000000Z
                            peter\t58\t1 Broon St\t2022-10-25T02:00:00.000000Z\t2022-10-25T01:59:00.000000Z\t2022-10-25T02:01:00.000000Z
                            david\t21\t\t2022-10-25T03:00:00.000000Z\t\t
                            """);

            assertQuery("""
                    select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)
                    from table_2 as b
                    right join table_1 as a
                       on a.ts between dateadd('m', -1, b.ts)  and dateadd('m', 1, b.ts)
                    """)
                    .noLeakCheck()
                    .ddl(null)
                    .noRandomAccess()
                    .returns("""
                            name\tage\taddress\tts\tdateadd\tdateadd1
                            alice\t60\t1 Glebe St\t2022-10-25T01:00:00.000000Z\t2022-10-25T00:59:00.000000Z\t2022-10-25T01:01:00.000000Z
                            peter\t58\t1 Broon St\t2022-10-25T02:00:00.000000Z\t2022-10-25T01:59:00.000000Z\t2022-10-25T02:01:00.000000Z
                            david\t21\t\t2022-10-25T03:00:00.000000Z\t\t
                            """);

            assertQuery("""
                    select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)
                    from table_1 as a
                    full join table_2 as b
                       on a.ts between dateadd('m', -1, b.ts)  and dateadd('m', 1, b.ts)
                    """)
                    .noLeakCheck()
                    .ddl(null)
                    .noRandomAccess()
                    .returns("""
                            name\tage\taddress\tts\tdateadd\tdateadd1
                            alice\t60\t1 Glebe St\t2022-10-25T01:00:00.000000Z\t2022-10-25T00:59:00.000000Z\t2022-10-25T01:01:00.000000Z
                            peter\t58\t1 Broon St\t2022-10-25T02:00:00.000000Z\t2022-10-25T01:59:00.000000Z\t2022-10-25T02:01:00.000000Z
                            david\t21\t\t2022-10-25T03:00:00.000000Z\t\t
                            \tnull\t1 Houston St\t\t2022-10-25T03:59:00.000000Z\t2022-10-25T04:01:00.000000Z
                            """);

            // query "4" - same as "3" but between is replaced with >= and <=
            assertQuery("""
                    select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)
                    from table_1 as a
                    left join table_2 as b
                       on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts)
                    """)
                    .noLeakCheck()
                    .ddl(null)
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            name\tage\taddress\tts\tdateadd\tdateadd1
                            alice\t60\t1 Glebe St\t2022-10-25T01:00:00.000000Z\t2022-10-25T00:59:00.000000Z\t2022-10-25T01:01:00.000000Z
                            peter\t58\t1 Broon St\t2022-10-25T02:00:00.000000Z\t2022-10-25T01:59:00.000000Z\t2022-10-25T02:01:00.000000Z
                            david\t21\t\t2022-10-25T03:00:00.000000Z\t\t
                            """);

            assertQuery("""
                    select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)
                    from table_2 as b
                    right join table_1 as a
                       on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts)
                    """)
                    .noLeakCheck()
                    .ddl(null)
                    .noRandomAccess()
                    .returns("""
                            name\tage\taddress\tts\tdateadd\tdateadd1
                            alice\t60\t1 Glebe St\t2022-10-25T01:00:00.000000Z\t2022-10-25T00:59:00.000000Z\t2022-10-25T01:01:00.000000Z
                            peter\t58\t1 Broon St\t2022-10-25T02:00:00.000000Z\t2022-10-25T01:59:00.000000Z\t2022-10-25T02:01:00.000000Z
                            david\t21\t\t2022-10-25T03:00:00.000000Z\t\t
                            """);


            assertQuery("""
                    select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)
                    from table_2 as b
                    full join table_1 as a
                       on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts)
                    """)
                    .noLeakCheck()
                    .ddl(null)
                    .noRandomAccess()
                    .returns("""
                            name\tage\taddress\tts\tdateadd\tdateadd1
                            alice\t60\t1 Glebe St\t2022-10-25T01:00:00.000000Z\t2022-10-25T00:59:00.000000Z\t2022-10-25T01:01:00.000000Z
                            peter\t58\t1 Broon St\t2022-10-25T02:00:00.000000Z\t2022-10-25T01:59:00.000000Z\t2022-10-25T02:01:00.000000Z
                            \tnull\t1 Houston St\t\t2022-10-25T03:59:00.000000Z\t2022-10-25T04:01:00.000000Z
                            david\t21\t\t2022-10-25T03:00:00.000000Z\t\t
                            """);
        });
    }

    @Test
    public void testAsOfCorrectness() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table orders (sym SYMBOL, amount DOUBLE, side BYTE, timestamp TIMESTAMP) timestamp(timestamp)");
            execute("create table quotes (sym SYMBOL, bid DOUBLE, ask DOUBLE, timestamp TIMESTAMP) timestamp(timestamp)");

            try (
                    TableWriter orders = getWriter("orders");
                    TableWriter quotes = getWriter("quotes")
            ) {
                TableWriter.Row rOrders;
                TableWriter.Row rQuotes;

                // quote googl @ 10:00:02
                rQuotes = quotes.newRow(MicrosFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000000Z"));
                rQuotes.putSym(0, "googl");
                rQuotes.putDouble(1, 100.2);
                rQuotes.putDouble(2, 100.3);
                rQuotes.append();

                // quote msft @ 10.00.02.000001
                rQuotes = quotes.newRow(MicrosFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000001Z"));
                rQuotes.putSym(0, "msft");
                rQuotes.putDouble(1, 185.9);
                rQuotes.putDouble(2, 187.3);
                rQuotes.append();

                // quote msft @ 10.00.02.000002
                rQuotes = quotes.newRow(MicrosFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000002Z"));
                rQuotes.putSym(0, "msft");
                rQuotes.putDouble(1, 186.1);
                rQuotes.putDouble(2, 187.8);
                rQuotes.append();

                // order googl @ 10.00.03
                rOrders = orders.newRow(MicrosFormatUtils.parseUTCTimestamp("2018-11-02T10:00:03.000000Z"));
                rOrders.putSym(0, "googl");
                rOrders.putDouble(1, 2000);
                rOrders.putByte(2, (byte) '1');
                rOrders.append();

                // quote msft @ 10.00.03.000001
                rQuotes = quotes.newRow(MicrosFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000002Z"));
                rQuotes.putSym(0, "msft");
                rQuotes.putDouble(1, 183.4);
                rQuotes.putDouble(2, 185.9);
                rQuotes.append();

                rOrders = orders.newRow(MicrosFormatUtils.parseUTCTimestamp("2018-11-02T10:00:04.000000Z"));
                rOrders.putSym(0, "msft");
                rOrders.putDouble(1, 150);
                rOrders.putByte(2, (byte) '1');
                rOrders.append();

                // order googl @ 10.00.05
                rOrders = orders.newRow(MicrosFormatUtils.parseUTCTimestamp("2018-11-02T10:00:05.000000Z"));
                rOrders.putSym(0, "googl");
                rOrders.putDouble(1, 3000);
                rOrders.putByte(2, (byte) '2');
                rOrders.append();

                quotes.commit();
                orders.commit();
            }

            assertQuery("select * from orders asof join quotes on(sym)")
                    .noLeakCheck()
                    .ddl(null)
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            sym\tamount\tside\ttimestamp\tsym1\tbid\task\ttimestamp1
                            googl\t2000.0\t49\t2018-11-02T10:00:03.000000Z\tgoogl\t100.2\t100.3\t2018-11-02T10:00:02.000000Z
                            msft\t150.0\t49\t2018-11-02T10:00:04.000000Z\tmsft\t183.4\t185.9\t2018-11-02T10:00:02.000002Z
                            googl\t3000.0\t50\t2018-11-02T10:00:05.000000Z\tgoogl\t100.2\t100.3\t2018-11-02T10:00:02.000000Z
                            """);
        });
    }

    @Test
    public void testAsOfFullFat() throws Exception {
        testFullFat(this::testAsOfJoin0);
    }

    @Test
    public void testAsOfFullFatJoinOnStr() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym,
                               round(rnd_double(0)*100, 3) amt,
                               to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp,
                               rnd_boolean() b,
                               rnd_str('ABC', 'CDE', null, 'XYZ') c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n
                            from long_sequence(10)
                            ) timestamp (timestamp)
                            """
            );
            execute(
                    """
                            create table y as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym2,
                               round(rnd_double(0), 3) price,
                               to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp,
                               rnd_boolean() b,
                               rnd_str('ABC', 'CDE', null, 'XYZ') c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n
                            from long_sequence(30)
                            ) timestamp(timestamp)
                            """
            );

            assertQuery("select x.i, x.c, y.c, x.amt, price, x.timestamp, y.timestamp, y.m from x asof join y on y.c = x.c")
                    .fullFatJoins()
                    .noLeakCheck()
                    .fails(73, "right side column 'm' is of unsupported type");
        });
    }

    @Test
    public void testAsOfFullFatJoinOnStrNoVar() throws Exception {
        testFullFat(this::testAsOfJoinOnStrNoVar0);
    }

    @Test
    public void testAsOfFullFatJoinOnStrSubSelect() throws Exception {
        assertMemoryLeak(() -> {
            final String query = """
                    select
                        x.i,
                        x.c,
                        y.c,
                        x.amt,
                        price,
                        x.timestamp,
                        y.timestamp
                    from x asof join (
                        select c, price, timestamp from y
                    ) y on y.c = x.c
                    """;

            final String expected = """
                    i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1
                    1\tXYZ\t\t50.938\tnull\t2018-01-01T00:12:00.000000Z\t
                    2\tABC\tABC\t42.281\t0.537\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z
                    3\tABC\tABC\t17.371\t0.673\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                    4\tXYZ\tXYZ\t44.805\t0.116\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z
                    5\t\t\t42.956\t0.47700000000000004\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z
                    6\tCDE\tCDE\t82.59700000000001\t0.24\t2018-01-01T01:12:00.000000Z\t2018-01-01T00:40:00.000000Z
                    7\tCDE\tCDE\t98.59100000000001\t0.24\t2018-01-01T01:24:00.000000Z\t2018-01-01T00:40:00.000000Z
                    8\tABC\tABC\t57.086\t0.59\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:58:00.000000Z
                    9\t\t\t81.44200000000001\t0.47700000000000004\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:00:00.000000Z
                    10\tXYZ\tXYZ\t3.973\t0.867\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:50:00.000000Z
                    """;

            execute(
                    """
                            create table x as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym,
                               round(rnd_double(0)*100, 3) amt,
                               to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp,
                               rnd_boolean() b,
                               rnd_str('ABC', 'CDE', null, 'XYZ') c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n
                            from long_sequence(10)
                            ) timestamp (timestamp)
                            """
            );
            execute(
                    """
                            create table y as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym2,
                               round(rnd_double(0), 3) price,
                               to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp,
                               rnd_boolean() b,
                               rnd_str('ABC', 'CDE', null, 'XYZ') c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n
                            from long_sequence(30)
                            ) timestamp(timestamp)
                            """
            );

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            execute(
                    """
                            insert into x select * from
                            (select
                               cast(x + 10 as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym,
                               round(rnd_double(0)*100, 3) amt,
                               to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp,
                               rnd_boolean() b,
                               rnd_str('ABC', 'CDE', null, 'KZZ') c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n
                            from long_sequence(10)
                            ) timestamp(timestamp)
                            """
            );
            execute(
                    """
                            insert into y select * from
                            (select
                               cast(x + 30 as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym2,
                               round(rnd_double(0), 3) price,
                               to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp,
                               rnd_boolean() b,
                               rnd_str('ABC', 'CDE', null, 'KZZ') c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n
                            from long_sequence(30)
                            ) timestamp(timestamp)
                            """
            );

            assertQuery(query)
                    .noLeakCheck()
                    .fullFatJoins()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1
                            1\tXYZ\t\t50.938\tnull\t2018-01-01T00:12:00.000000Z\t
                            2\tABC\tABC\t42.281\t0.537\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z
                            3\tABC\tABC\t17.371\t0.673\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                            4\tXYZ\tXYZ\t44.805\t0.116\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z
                            5\t\t\t42.956\t0.47700000000000004\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z
                            6\tCDE\tCDE\t82.59700000000001\t0.212\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:12:00.000000Z
                            7\tCDE\tCDE\t98.59100000000001\t0.28200000000000003\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:22:00.000000Z
                            8\tABC\tABC\t57.086\t0.453\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:16:00.000000Z
                            9\t\t\t81.44200000000001\t0.624\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:34:00.000000Z
                            10\tXYZ\tXYZ\t3.973\t0.867\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:50:00.000000Z
                            11\t\t\t85.019\t0.624\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:34:00.000000Z
                            12\tKZZ\tKZZ\t85.49\t0.528\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:56:00.000000Z
                            13\tCDE\tCDE\t27.493000000000002\t0.401\t2018-01-01T02:36:00.000000Z\t2018-01-01T02:00:00.000000Z
                            14\tCDE\tCDE\t39.244\t0.401\t2018-01-01T02:48:00.000000Z\t2018-01-01T02:00:00.000000Z
                            15\tABC\tABC\t55.152\t0.775\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:54:00.000000Z
                            16\tKZZ\tKZZ\t3.224\t0.528\t2018-01-01T03:12:00.000000Z\t2018-01-01T01:56:00.000000Z
                            17\t\t\t6.368\t0.624\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:34:00.000000Z
                            18\tCDE\tCDE\t18.305\t0.401\t2018-01-01T03:36:00.000000Z\t2018-01-01T02:00:00.000000Z
                            19\tABC\tABC\t16.378\t0.775\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:54:00.000000Z
                            20\t\t\t4.773\t0.624\t2018-01-01T04:00:00.000000Z\t2018-01-01T01:34:00.000000Z
                            """);

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

            final String expected = """
                    i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                    1\tmsft\t50.938\t0.198\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:10:00.000000Z
                    2\tmsft\t5.048\t0.049\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:22:00.000000Z
                    3\tmsft\t5.359\t0.652\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                    4\tgoogl\t72.032\t0.131\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:40:00.000000Z
                    5\tgoogl\t63.35\t0.897\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:56:00.000000Z
                    6\tmsft\t43.493\t0.395\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:00:00.000000Z
                    7\tgoogl\t0.533\t0.897\t2018-01-01T01:24:00.000000Z\t2018-01-01T00:56:00.000000Z
                    8\tibm\t52.517\t0.994\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:58:00.000000Z
                    9\tgoogl\t30.062\t0.897\t2018-01-01T01:48:00.000000Z\t2018-01-01T00:56:00.000000Z
                    10\tgoogl\t40.39\t0.897\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:56:00.000000Z
                    """;

            execute(
                    """
                            create table x as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym,
                               round(rnd_double(0)*100, 3) amt,
                               to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp,
                               rnd_boolean() b,
                               rnd_str(1,1,2) c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n,
                               rnd_varchar(5,16,2) vch
                             from long_sequence(10)
                            ) timestamp (timestamp)
                            """
            );
            execute(
                    """
                            create table y as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym2,
                               round(rnd_double(0), 3) price,
                               to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp,
                               rnd_boolean() b,
                               rnd_str(1,1,2) c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n,
                               rnd_varchar(5,16,2) vch
                            from long_sequence(30)
                            ) timestamp(timestamp)
                            """
            );

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            execute(
                    """
                            insert into x select * from
                            (select
                               cast(x + 10 as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym,
                               round(rnd_double(0)*100, 3) amt,
                               to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp,
                               rnd_boolean() b,
                               rnd_str(1,1,2) c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n,
                               rnd_varchar(5,16,2) vch
                            from long_sequence(10)
                            ) timestamp(timestamp)
                            """
            );
            execute(
                    """
                            insert into y select * from
                            (select
                               cast(x + 30 as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym2,
                               round(rnd_double(0), 3) price,
                               to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp,
                               rnd_boolean() b,
                               rnd_str(1,1,2) c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n,
                               rnd_varchar(5,16,2) vch
                            from long_sequence(30)
                            ) timestamp(timestamp)
                            """
            );

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                            1\tmsft\t50.938\t0.198\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:10:00.000000Z
                            2\tmsft\t5.048\t0.049\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:22:00.000000Z
                            3\tmsft\t5.359\t0.652\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                            4\tgoogl\t72.032\t0.131\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:40:00.000000Z
                            5\tgoogl\t63.35\t0.897\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:56:00.000000Z
                            6\tmsft\t43.493\t0.44\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:04:00.000000Z
                            7\tgoogl\t0.533\t0.34700000000000003\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:20:00.000000Z
                            8\tibm\t52.517\t0.377\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:36:00.000000Z
                            9\tgoogl\t30.062\t0.274\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:46:00.000000Z
                            10\tgoogl\t40.39\t0.968\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:58:00.000000Z
                            11\tmsft\t35.82\t0.11\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:52:00.000000Z
                            12\tmsft\t55.255\t0.11\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:52:00.000000Z
                            13\tgoogl\t26.438\t0.968\t2018-01-01T02:36:00.000000Z\t2018-01-01T01:58:00.000000Z
                            14\tmsft\t21.467\t0.11\t2018-01-01T02:48:00.000000Z\t2018-01-01T01:52:00.000000Z
                            15\tibm\t83.642\t0.556\t2018-01-01T03:00:00.000000Z\t2018-01-01T02:00:00.000000Z
                            16\tgoogl\t2.523\t0.968\t2018-01-01T03:12:00.000000Z\t2018-01-01T01:58:00.000000Z
                            17\tgoogl\t63.464\t0.968\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:58:00.000000Z
                            18\tibm\t98.293\t0.556\t2018-01-01T03:36:00.000000Z\t2018-01-01T02:00:00.000000Z
                            19\tmsft\t90.087\t0.11\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:52:00.000000Z
                            20\tibm\t59.437000000000005\t0.556\t2018-01-01T04:00:00.000000Z\t2018-01-01T02:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAsOfJoinAllTypesFullFat() throws Exception {
        testFullFat(this::testAsOfJoinNoStrings0);
    }

    @Test
    public void testAsOfJoinLeftTimestampDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            execute("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertQuery("select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from (x order by timestamp desc) x asof join y on y.sym2 = x.sym")
                    .noLeakCheck()
                    .fails(93, "left");
        });
    }

    @Test
    public void testAsOfJoinNoKey() throws Exception {
        assertMemoryLeak(() -> {
            final String query =
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y";

            final String expected = """
                    i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                    1\tmsft\t50.938\t0.523\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:12:00.000000Z
                    2\tgoogl\t42.281\t0.044\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z
                    3\tgoogl\t17.371\t0.915\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                    4\tibm\t14.831\t0.005\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:48:00.000000Z
                    5\tgoogl\t86.772\t0.092\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z
                    6\tmsft\t29.659\t0.092\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:00:00.000000Z
                    7\tgoogl\t7.594\t0.092\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:00:00.000000Z
                    8\tibm\t54.253\t0.092\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:00:00.000000Z
                    9\tmsft\t62.26\t0.092\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:00:00.000000Z
                    10\tmsft\t50.908\t0.092\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:00:00.000000Z
                    """;

            execute(
                    """
                            create table x as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym,
                               round(rnd_double(0)*100, 3) amt,
                               to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp,
                               rnd_boolean() b,
                               rnd_str(1,1,2) c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n
                            from long_sequence(10)
                            ) timestamp (timestamp)
                            """
            );
            execute(
                    """
                            create table y as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym2,
                               round(rnd_double(0), 3) price,
                               to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp,
                               rnd_boolean() b,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l
                            from long_sequence(30)
                            ) timestamp(timestamp)
                            """
            );

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            execute(
                    """
                            insert into x select * from
                            (select
                               cast(x + 10 as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym,
                               round(rnd_double(0)*100, 3) amt,
                               to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp,
                               rnd_boolean() b,
                               rnd_str(1,1,2) c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n
                            from long_sequence(10)
                            ) timestamp(timestamp)
                            """
            );
            execute(
                    """
                            insert into y select * from
                            (select
                               cast(x + 30 as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym2,
                               round(rnd_double(0), 3) price,
                               to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp,
                               rnd_boolean() b,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l
                            from long_sequence(30)
                            ) timestamp(timestamp)
                            """
            );

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                            1\tmsft\t50.938\t0.523\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:12:00.000000Z
                            2\tgoogl\t42.281\t0.044\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z
                            3\tgoogl\t17.371\t0.915\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                            4\tibm\t14.831\t0.005\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:48:00.000000Z
                            5\tgoogl\t86.772\t0.092\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z
                            6\tmsft\t29.659\t0.544\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:12:00.000000Z
                            7\tgoogl\t7.594\t0.036000000000000004\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:24:00.000000Z
                            8\tibm\t54.253\t0.544\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:36:00.000000Z
                            9\tmsft\t62.26\t0.683\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:48:00.000000Z
                            10\tmsft\t50.908\t0.148\t2018-01-01T02:00:00.000000Z\t2018-01-01T02:00:00.000000Z
                            11\tmsft\t25.604\t0.148\t2018-01-01T02:12:00.000000Z\t2018-01-01T02:00:00.000000Z
                            12\tgoogl\t89.22\t0.148\t2018-01-01T02:24:00.000000Z\t2018-01-01T02:00:00.000000Z
                            13\tgoogl\t64.536\t0.148\t2018-01-01T02:36:00.000000Z\t2018-01-01T02:00:00.000000Z
                            14\tibm\t33.0\t0.148\t2018-01-01T02:48:00.000000Z\t2018-01-01T02:00:00.000000Z
                            15\tmsft\t67.285\t0.148\t2018-01-01T03:00:00.000000Z\t2018-01-01T02:00:00.000000Z
                            16\tgoogl\t17.31\t0.148\t2018-01-01T03:12:00.000000Z\t2018-01-01T02:00:00.000000Z
                            17\tibm\t23.957\t0.148\t2018-01-01T03:24:00.000000Z\t2018-01-01T02:00:00.000000Z
                            18\tibm\t60.678000000000004\t0.148\t2018-01-01T03:36:00.000000Z\t2018-01-01T02:00:00.000000Z
                            19\tmsft\t4.727\t0.148\t2018-01-01T03:48:00.000000Z\t2018-01-01T02:00:00.000000Z
                            20\tgoogl\t26.222\t0.148\t2018-01-01T04:00:00.000000Z\t2018-01-01T02:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAsOfJoinNoKey3MMaster1MSlave() throws Exception {
        assertMemoryLeak(() -> {
            final String query =
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y";

            final String expected = """
                    i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                    1\tmsft\t50.938\t0.181\t2018-01-01T00:00:00.000000Z\t2018-01-01T00:00:00.000000Z
                    2\tgoogl\t42.281\t0.181\t2018-01-01T00:01:00.000000Z\t2018-01-01T00:00:00.000000Z
                    3\tgoogl\t17.371\t0.181\t2018-01-01T00:02:00.000000Z\t2018-01-01T00:00:00.000000Z
                    4\tibm\t14.831\t0.27\t2018-01-01T00:03:00.000000Z\t2018-01-01T00:03:00.000000Z
                    5\tgoogl\t86.772\t0.27\t2018-01-01T00:04:00.000000Z\t2018-01-01T00:03:00.000000Z
                    6\tmsft\t29.659\t0.27\t2018-01-01T00:05:00.000000Z\t2018-01-01T00:03:00.000000Z
                    7\tgoogl\t7.594\t0.47300000000000003\t2018-01-01T00:06:00.000000Z\t2018-01-01T00:06:00.000000Z
                    8\tibm\t54.253\t0.47300000000000003\t2018-01-01T00:07:00.000000Z\t2018-01-01T00:06:00.000000Z
                    9\tmsft\t62.26\t0.47300000000000003\t2018-01-01T00:08:00.000000Z\t2018-01-01T00:06:00.000000Z
                    10\tmsft\t50.908\t0.179\t2018-01-01T00:09:00.000000Z\t2018-01-01T00:09:00.000000Z
                    11\tmsft\t57.79\t0.179\t2018-01-01T00:10:00.000000Z\t2018-01-01T00:09:00.000000Z
                    12\tmsft\t66.121\t0.179\t2018-01-01T00:11:00.000000Z\t2018-01-01T00:09:00.000000Z
                    13\tibm\t70.398\t0.6\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:12:00.000000Z
                    14\tgoogl\t65.066\t0.6\t2018-01-01T00:13:00.000000Z\t2018-01-01T00:12:00.000000Z
                    15\tmsft\t40.863\t0.6\t2018-01-01T00:14:00.000000Z\t2018-01-01T00:12:00.000000Z
                    16\tgoogl\t83.861\t0.47800000000000004\t2018-01-01T00:15:00.000000Z\t2018-01-01T00:15:00.000000Z
                    17\tibm\t28.627\t0.47800000000000004\t2018-01-01T00:16:00.000000Z\t2018-01-01T00:15:00.000000Z
                    18\tibm\t93.163\t0.47800000000000004\t2018-01-01T00:17:00.000000Z\t2018-01-01T00:15:00.000000Z
                    19\tibm\t15.121\t0.34900000000000003\t2018-01-01T00:18:00.000000Z\t2018-01-01T00:18:00.000000Z
                    20\tgoogl\t62.401\t0.34900000000000003\t2018-01-01T00:19:00.000000Z\t2018-01-01T00:18:00.000000Z
                    21\tmsft\t59.651\t0.34900000000000003\t2018-01-01T00:20:00.000000Z\t2018-01-01T00:18:00.000000Z
                    22\tgoogl\t70.205\t0.221\t2018-01-01T00:21:00.000000Z\t2018-01-01T00:21:00.000000Z
                    23\tibm\t57.257\t0.221\t2018-01-01T00:22:00.000000Z\t2018-01-01T00:21:00.000000Z
                    24\tmsft\t23.846\t0.221\t2018-01-01T00:23:00.000000Z\t2018-01-01T00:21:00.000000Z
                    25\tmsft\t91.83500000000001\t0.47200000000000003\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z
                    26\tibm\t33.0\t0.47200000000000003\t2018-01-01T00:25:00.000000Z\t2018-01-01T00:24:00.000000Z
                    27\tmsft\t67.285\t0.47200000000000003\t2018-01-01T00:26:00.000000Z\t2018-01-01T00:24:00.000000Z
                    28\tgoogl\t17.31\t0.675\t2018-01-01T00:27:00.000000Z\t2018-01-01T00:27:00.000000Z
                    29\tibm\t23.957\t0.675\t2018-01-01T00:28:00.000000Z\t2018-01-01T00:27:00.000000Z
                    30\tibm\t60.678000000000004\t0.675\t2018-01-01T00:29:00.000000Z\t2018-01-01T00:27:00.000000Z
                    """;

            execute(
                    """
                            create table x as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym,
                               round(rnd_double(0)*100, 3) amt,
                               to_timestamp('2018-01', 'yyyy-MM') + (x-1) * 60000000 timestamp,
                               rnd_boolean() b,
                               rnd_str(1,1,2) c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n
                            from long_sequence(30)
                            ) timestamp (timestamp)
                            """
            );
            execute(
                    """
                            create table y as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym2,
                               round(rnd_double(0), 3) price,
                               to_timestamp('2018-01', 'yyyy-MM') + (x-1) * 180000000 timestamp,
                               rnd_boolean() b,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l
                            from long_sequence(10)
                            ) timestamp(timestamp)
                            """
            );

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testAsOfJoinNoKeyEmptySlave() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y";

            final String expected = """
                    i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                    1\tmsft\t50.938\tnull\t2018-01-01T00:00:00.000000Z\t
                    2\tgoogl\t42.281\tnull\t2018-01-01T00:01:00.000000Z\t
                    3\tgoogl\t17.371\tnull\t2018-01-01T00:02:00.000000Z\t
                    4\tibm\t14.831\tnull\t2018-01-01T00:03:00.000000Z\t
                    5\tgoogl\t86.772\tnull\t2018-01-01T00:04:00.000000Z\t
                    6\tmsft\t29.659\tnull\t2018-01-01T00:05:00.000000Z\t
                    7\tgoogl\t7.594\tnull\t2018-01-01T00:06:00.000000Z\t
                    8\tibm\t54.253\tnull\t2018-01-01T00:07:00.000000Z\t
                    9\tmsft\t62.26\tnull\t2018-01-01T00:08:00.000000Z\t
                    10\tmsft\t50.908\tnull\t2018-01-01T00:09:00.000000Z\t
                    11\tmsft\t57.79\tnull\t2018-01-01T00:10:00.000000Z\t
                    12\tmsft\t66.121\tnull\t2018-01-01T00:11:00.000000Z\t
                    13\tibm\t70.398\tnull\t2018-01-01T00:12:00.000000Z\t
                    14\tgoogl\t65.066\tnull\t2018-01-01T00:13:00.000000Z\t
                    15\tmsft\t40.863\tnull\t2018-01-01T00:14:00.000000Z\t
                    16\tgoogl\t83.861\tnull\t2018-01-01T00:15:00.000000Z\t
                    17\tibm\t28.627\tnull\t2018-01-01T00:16:00.000000Z\t
                    18\tibm\t93.163\tnull\t2018-01-01T00:17:00.000000Z\t
                    19\tibm\t15.121\tnull\t2018-01-01T00:18:00.000000Z\t
                    20\tgoogl\t62.401\tnull\t2018-01-01T00:19:00.000000Z\t
                    21\tmsft\t59.651\tnull\t2018-01-01T00:20:00.000000Z\t
                    22\tgoogl\t70.205\tnull\t2018-01-01T00:21:00.000000Z\t
                    23\tibm\t57.257\tnull\t2018-01-01T00:22:00.000000Z\t
                    24\tmsft\t23.846\tnull\t2018-01-01T00:23:00.000000Z\t
                    25\tmsft\t91.83500000000001\tnull\t2018-01-01T00:24:00.000000Z\t
                    26\tibm\t33.0\tnull\t2018-01-01T00:25:00.000000Z\t
                    27\tmsft\t67.285\tnull\t2018-01-01T00:26:00.000000Z\t
                    28\tgoogl\t17.31\tnull\t2018-01-01T00:27:00.000000Z\t
                    29\tibm\t23.957\tnull\t2018-01-01T00:28:00.000000Z\t
                    30\tibm\t60.678000000000004\tnull\t2018-01-01T00:29:00.000000Z\t
                    """;

            execute(
                    """
                            create table x as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym,
                               round(rnd_double(0)*100, 3) amt,
                               to_timestamp('2018-01', 'yyyy-MM') + (x-1) * 60000000 timestamp,
                               rnd_boolean() b,
                               rnd_str(1,1,2) c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n
                            from long_sequence(30)
                            ) timestamp (timestamp)
                            """
            );
            execute(
                    """
                            create table y as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym2,
                               round(rnd_double(0), 3) price,
                               to_timestamp('2018-01-01 00:15', 'yyyy-MM-dd HH:mm') + (x-1) * 180000000 timestamp,
                               rnd_boolean() b,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l
                            from long_sequence(0)
                            ) timestamp(timestamp)
                            """
            );

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
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

            final String expected = """
                    i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                    1\tmsft\t50.938\tnull\t2018-01-01T00:00:00.000000Z\t
                    2\tgoogl\t42.281\tnull\t2018-01-01T00:01:00.000000Z\t
                    3\tgoogl\t17.371\tnull\t2018-01-01T00:02:00.000000Z\t
                    4\tibm\t14.831\tnull\t2018-01-01T00:03:00.000000Z\t
                    5\tgoogl\t86.772\tnull\t2018-01-01T00:04:00.000000Z\t
                    6\tmsft\t29.659\tnull\t2018-01-01T00:05:00.000000Z\t
                    7\tgoogl\t7.594\tnull\t2018-01-01T00:06:00.000000Z\t
                    8\tibm\t54.253\tnull\t2018-01-01T00:07:00.000000Z\t
                    9\tmsft\t62.26\tnull\t2018-01-01T00:08:00.000000Z\t
                    10\tmsft\t50.908\tnull\t2018-01-01T00:09:00.000000Z\t
                    11\tmsft\t57.79\tnull\t2018-01-01T00:10:00.000000Z\t
                    12\tmsft\t66.121\tnull\t2018-01-01T00:11:00.000000Z\t
                    13\tibm\t70.398\tnull\t2018-01-01T00:12:00.000000Z\t
                    14\tgoogl\t65.066\tnull\t2018-01-01T00:13:00.000000Z\t
                    15\tmsft\t40.863\tnull\t2018-01-01T00:14:00.000000Z\t
                    16\tgoogl\t83.861\t0.181\t2018-01-01T00:15:00.000000Z\t2018-01-01T00:15:00.000000Z
                    17\tibm\t28.627\t0.181\t2018-01-01T00:16:00.000000Z\t2018-01-01T00:15:00.000000Z
                    18\tibm\t93.163\t0.181\t2018-01-01T00:17:00.000000Z\t2018-01-01T00:15:00.000000Z
                    19\tibm\t15.121\t0.27\t2018-01-01T00:18:00.000000Z\t2018-01-01T00:18:00.000000Z
                    20\tgoogl\t62.401\t0.27\t2018-01-01T00:19:00.000000Z\t2018-01-01T00:18:00.000000Z
                    21\tmsft\t59.651\t0.27\t2018-01-01T00:20:00.000000Z\t2018-01-01T00:18:00.000000Z
                    22\tgoogl\t70.205\t0.47300000000000003\t2018-01-01T00:21:00.000000Z\t2018-01-01T00:21:00.000000Z
                    23\tibm\t57.257\t0.47300000000000003\t2018-01-01T00:22:00.000000Z\t2018-01-01T00:21:00.000000Z
                    24\tmsft\t23.846\t0.47300000000000003\t2018-01-01T00:23:00.000000Z\t2018-01-01T00:21:00.000000Z
                    25\tmsft\t91.83500000000001\t0.179\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z
                    26\tibm\t33.0\t0.179\t2018-01-01T00:25:00.000000Z\t2018-01-01T00:24:00.000000Z
                    27\tmsft\t67.285\t0.179\t2018-01-01T00:26:00.000000Z\t2018-01-01T00:24:00.000000Z
                    28\tgoogl\t17.31\t0.6\t2018-01-01T00:27:00.000000Z\t2018-01-01T00:27:00.000000Z
                    29\tibm\t23.957\t0.6\t2018-01-01T00:28:00.000000Z\t2018-01-01T00:27:00.000000Z
                    30\tibm\t60.678000000000004\t0.6\t2018-01-01T00:29:00.000000Z\t2018-01-01T00:27:00.000000Z
                    """;

            execute(
                    """
                            create table x as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym,
                               round(rnd_double(0)*100, 3) amt,
                               to_timestamp('2018-01', 'yyyy-MM') + (x-1) * 60000000 timestamp,
                               rnd_boolean() b,
                               rnd_str(1,1,2) c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n
                            from long_sequence(30)
                            ) timestamp (timestamp)
                            """
            );
            execute(
                    """
                            create table y as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym2,
                               round(rnd_double(0), 3) price,
                               to_timestamp('2018-01-01 00:15', 'yyyy-MM-dd HH:mm') + (x-1) * 180000000 timestamp,
                               rnd_boolean() b,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l
                            from long_sequence(10)
                            ) timestamp(timestamp)
                            """
            );

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testAsOfJoinNoKeySlaveAllBelow() throws Exception {
        assertMemoryLeak(() -> {
            final String query =
                    "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y";

            final String expected = """
                    i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                    1\tmsft\t50.938\tnull\t2018-01-01T00:00:00.000000Z\t
                    2\tgoogl\t42.281\tnull\t2018-01-01T00:01:00.000000Z\t
                    3\tgoogl\t17.371\tnull\t2018-01-01T00:02:00.000000Z\t
                    4\tibm\t14.831\tnull\t2018-01-01T00:03:00.000000Z\t
                    5\tgoogl\t86.772\tnull\t2018-01-01T00:04:00.000000Z\t
                    6\tmsft\t29.659\tnull\t2018-01-01T00:05:00.000000Z\t
                    7\tgoogl\t7.594\tnull\t2018-01-01T00:06:00.000000Z\t
                    8\tibm\t54.253\tnull\t2018-01-01T00:07:00.000000Z\t
                    9\tmsft\t62.26\tnull\t2018-01-01T00:08:00.000000Z\t
                    10\tmsft\t50.908\tnull\t2018-01-01T00:09:00.000000Z\t
                    11\tmsft\t57.79\tnull\t2018-01-01T00:10:00.000000Z\t
                    12\tmsft\t66.121\tnull\t2018-01-01T00:11:00.000000Z\t
                    13\tibm\t70.398\tnull\t2018-01-01T00:12:00.000000Z\t
                    14\tgoogl\t65.066\tnull\t2018-01-01T00:13:00.000000Z\t
                    15\tmsft\t40.863\tnull\t2018-01-01T00:14:00.000000Z\t
                    16\tgoogl\t83.861\tnull\t2018-01-01T00:15:00.000000Z\t
                    17\tibm\t28.627\tnull\t2018-01-01T00:16:00.000000Z\t
                    18\tibm\t93.163\tnull\t2018-01-01T00:17:00.000000Z\t
                    19\tibm\t15.121\tnull\t2018-01-01T00:18:00.000000Z\t
                    20\tgoogl\t62.401\tnull\t2018-01-01T00:19:00.000000Z\t
                    21\tmsft\t59.651\tnull\t2018-01-01T00:20:00.000000Z\t
                    22\tgoogl\t70.205\tnull\t2018-01-01T00:21:00.000000Z\t
                    23\tibm\t57.257\tnull\t2018-01-01T00:22:00.000000Z\t
                    24\tmsft\t23.846\tnull\t2018-01-01T00:23:00.000000Z\t
                    25\tmsft\t91.83500000000001\tnull\t2018-01-01T00:24:00.000000Z\t
                    26\tibm\t33.0\tnull\t2018-01-01T00:25:00.000000Z\t
                    27\tmsft\t67.285\tnull\t2018-01-01T00:26:00.000000Z\t
                    28\tgoogl\t17.31\tnull\t2018-01-01T00:27:00.000000Z\t
                    29\tibm\t23.957\tnull\t2018-01-01T00:28:00.000000Z\t
                    30\tibm\t60.678000000000004\tnull\t2018-01-01T00:29:00.000000Z\t
                    """;

            execute(
                    """
                            create table x as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym,
                               round(rnd_double(0)*100, 3) amt,
                               to_timestamp('2018-01', 'yyyy-MM') + (x-1) * 60000000 timestamp,
                               rnd_boolean() b,
                               rnd_str(1,1,2) c,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l,
                               rnd_bin(10, 20, 2) m,
                               rnd_str(5,16,2) n
                            from long_sequence(30)
                            ) timestamp (timestamp)
                            """
            );
            execute(
                    """
                            create table y as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym2,
                               round(rnd_double(0), 3) price,
                               to_timestamp('2018-01-01 03:00', 'yyyy-MM-dd HH:mm') + (x-1) * 180000000 timestamp,
                               rnd_boolean() b,
                               rnd_double(2) d,
                               rnd_float(2) e,
                               rnd_short(10,1024) f,
                               rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g,
                               rnd_symbol(4,4,4,2) ik,
                               rnd_long() j,
                               timestamp_sequence(0, 1000000000) k,
                               rnd_byte(2,50) l
                            from long_sequence(10)
                            ) timestamp(timestamp)
                            """
            );

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testAsOfJoinNoLeftTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10))");
            execute("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertQuery("select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.sym2 = x.sym")
                    .noLeakCheck()
                    .fails(65, "left");
        });
    }

    @Test
    public void testAsOfJoinNoRightTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.sym2 = x.sym";
            execute("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            execute("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30))");
            assertQuery(query)
                    .noLeakCheck()
                    .fails(65, "right");
        });
    }

    @Test
    public void testAsOfJoinNoSelect() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "x asof join y on y.sym2 = x.sym";

            final String expected = """
                    i\tsym\tamt\ttimestamp\ti1\tsym2\tprice\ttimestamp1
                    1\tmsft\t22.463\t2018-01-01T00:12:00.000000Z\tnull\t\tnull\t
                    2\tgoogl\t29.92\t2018-01-01T00:24:00.000000Z\t12\tgoogl\t0.885\t2018-01-01T00:24:00.000000Z
                    3\tmsft\t65.086\t2018-01-01T00:36:00.000000Z\t18\tmsft\t0.5660000000000001\t2018-01-01T00:36:00.000000Z
                    4\tibm\t98.563\t2018-01-01T00:48:00.000000Z\t17\tibm\t0.405\t2018-01-01T00:34:00.000000Z
                    5\tmsft\t50.938\t2018-01-01T01:00:00.000000Z\t23\tmsft\t0.545\t2018-01-01T00:46:00.000000Z
                    6\tibm\t76.11\t2018-01-01T01:12:00.000000Z\t28\tibm\t0.9540000000000001\t2018-01-01T00:56:00.000000Z
                    7\tmsft\t55.992000000000004\t2018-01-01T01:24:00.000000Z\t23\tmsft\t0.545\t2018-01-01T00:46:00.000000Z
                    8\tibm\t23.905\t2018-01-01T01:36:00.000000Z\t28\tibm\t0.9540000000000001\t2018-01-01T00:56:00.000000Z
                    9\tgoogl\t67.786\t2018-01-01T01:48:00.000000Z\t30\tgoogl\t0.198\t2018-01-01T01:00:00.000000Z
                    10\tgoogl\t38.54\t2018-01-01T02:00:00.000000Z\t30\tgoogl\t0.198\t2018-01-01T01:00:00.000000Z
                    """;

            execute(
                    """
                            create table x as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym,
                               round(rnd_double(0)*100, 3) amt,
                               to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp
                            from long_sequence(10)
                            ) timestamp (timestamp)
                            """
            );

            execute(
                    """
                            create table y as (
                            select cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym2,
                               round(rnd_double(0), 3) price,
                               to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp
                            from long_sequence(30)
                            ) timestamp(timestamp)
                            """
            );

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            execute(
                    """
                            insert into x select * from (
                            select
                               cast(x + 10 as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym,
                               round(rnd_double(0)*100, 3) amt,
                               to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp
                            from long_sequence(10)
                            ) timestamp(timestamp)
                            """
            );

            execute(
                    """
                            insert into y select * from (
                            select
                               cast(x + 30 as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym2,
                               round(rnd_double(0), 3) price,
                               to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp
                            from long_sequence(30)
                            ) timestamp(timestamp)
                            """
            );

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            i\tsym\tamt\ttimestamp\ti1\tsym2\tprice\ttimestamp1
                            1\tmsft\t22.463\t2018-01-01T00:12:00.000000Z\tnull\t\tnull\t
                            2\tgoogl\t29.92\t2018-01-01T00:24:00.000000Z\t12\tgoogl\t0.885\t2018-01-01T00:24:00.000000Z
                            3\tmsft\t65.086\t2018-01-01T00:36:00.000000Z\t18\tmsft\t0.5660000000000001\t2018-01-01T00:36:00.000000Z
                            4\tibm\t98.563\t2018-01-01T00:48:00.000000Z\t17\tibm\t0.405\t2018-01-01T00:34:00.000000Z
                            5\tmsft\t50.938\t2018-01-01T01:00:00.000000Z\t23\tmsft\t0.545\t2018-01-01T00:46:00.000000Z
                            6\tibm\t76.11\t2018-01-01T01:12:00.000000Z\t36\tibm\t0.337\t2018-01-01T01:12:00.000000Z
                            7\tmsft\t55.992000000000004\t2018-01-01T01:24:00.000000Z\t38\tmsft\t0.226\t2018-01-01T01:16:00.000000Z
                            8\tibm\t23.905\t2018-01-01T01:36:00.000000Z\t48\tibm\t0.767\t2018-01-01T01:36:00.000000Z
                            9\tgoogl\t67.786\t2018-01-01T01:48:00.000000Z\t54\tgoogl\t0.101\t2018-01-01T01:48:00.000000Z
                            10\tgoogl\t38.54\t2018-01-01T02:00:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z
                            11\tmsft\t68.069\t2018-01-01T02:12:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z
                            12\tmsft\t24.008\t2018-01-01T02:24:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z
                            13\tgoogl\t94.559\t2018-01-01T02:36:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z
                            14\tibm\t62.474000000000004\t2018-01-01T02:48:00.000000Z\t50\tibm\t0.068\t2018-01-01T01:40:00.000000Z
                            15\tmsft\t39.017\t2018-01-01T03:00:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z
                            16\tgoogl\t10.643\t2018-01-01T03:12:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z
                            17\tmsft\t7.246\t2018-01-01T03:24:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z
                            18\tmsft\t36.798\t2018-01-01T03:36:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z
                            19\tmsft\t66.98\t2018-01-01T03:48:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z
                            20\tgoogl\t26.369\t2018-01-01T04:00:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z
                            """);
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

            final String expected = """
                    i\tsym\tamt\ttimestamp\ti1\tsym2\tprice\ttimestamp1
                    1\tmsft\t22.463\t2018-01-01T00:12:00.000000Z\tnull\t\tnull\t
                    2\tgoogl\t29.92\t2018-01-01T00:24:00.000000Z\t12\tgoogl\t0.885\t2018-01-01T00:24:00.000000Z
                    3\tmsft\t65.086\t2018-01-01T00:36:00.000000Z\t18\tmsft\t0.5660000000000001\t2018-01-01T00:36:00.000000Z
                    4\tibm\t98.563\t2018-01-01T00:48:00.000000Z\t17\tibm\t0.405\t2018-01-01T00:34:00.000000Z
                    5\tmsft\t50.938\t2018-01-01T01:00:00.000000Z\t23\tmsft\t0.545\t2018-01-01T00:46:00.000000Z
                    6\tibm\t76.11\t2018-01-01T01:12:00.000000Z\t28\tibm\t0.9540000000000001\t2018-01-01T00:56:00.000000Z
                    7\tmsft\t55.992000000000004\t2018-01-01T01:24:00.000000Z\t23\tmsft\t0.545\t2018-01-01T00:46:00.000000Z
                    8\tibm\t23.905\t2018-01-01T01:36:00.000000Z\t28\tibm\t0.9540000000000001\t2018-01-01T00:56:00.000000Z
                    9\tgoogl\t67.786\t2018-01-01T01:48:00.000000Z\t30\tgoogl\t0.198\t2018-01-01T01:00:00.000000Z
                    10\tgoogl\t38.54\t2018-01-01T02:00:00.000000Z\t30\tgoogl\t0.198\t2018-01-01T01:00:00.000000Z
                    """;

            execute(
                    """
                            create table x as (
                            select
                               cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym,
                               round(rnd_double(0)*100, 3) amt,
                               to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp
                            from long_sequence(10)
                            )
                            """
            );

            execute(
                    """
                            create table y as (
                            select cast(x as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym2,
                               round(rnd_double(0), 3) price,
                               to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp
                            from long_sequence(30)
                            )
                            """
            );

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            execute(
                    """
                            insert into x select * from (
                            select
                               cast(x + 10 as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym,
                               round(rnd_double(0)*100, 3) amt,
                               to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp
                            from long_sequence(10)
                            ) timestamp(timestamp)
                            """
            );

            execute(
                    """
                            insert into y select * from (
                            select
                               cast(x + 30 as int) i,
                               rnd_symbol('msft','ibm', 'googl') sym2,
                               round(rnd_double(0), 3) price,
                               to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp
                            from long_sequence(30)
                            ) timestamp(timestamp)
                            """
            );

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            i\tsym\tamt\ttimestamp\ti1\tsym2\tprice\ttimestamp1
                            1\tmsft\t22.463\t2018-01-01T00:12:00.000000Z\tnull\t\tnull\t
                            2\tgoogl\t29.92\t2018-01-01T00:24:00.000000Z\t12\tgoogl\t0.885\t2018-01-01T00:24:00.000000Z
                            3\tmsft\t65.086\t2018-01-01T00:36:00.000000Z\t18\tmsft\t0.5660000000000001\t2018-01-01T00:36:00.000000Z
                            4\tibm\t98.563\t2018-01-01T00:48:00.000000Z\t17\tibm\t0.405\t2018-01-01T00:34:00.000000Z
                            5\tmsft\t50.938\t2018-01-01T01:00:00.000000Z\t23\tmsft\t0.545\t2018-01-01T00:46:00.000000Z
                            6\tibm\t76.11\t2018-01-01T01:12:00.000000Z\t36\tibm\t0.337\t2018-01-01T01:12:00.000000Z
                            7\tmsft\t55.992000000000004\t2018-01-01T01:24:00.000000Z\t38\tmsft\t0.226\t2018-01-01T01:16:00.000000Z
                            8\tibm\t23.905\t2018-01-01T01:36:00.000000Z\t48\tibm\t0.767\t2018-01-01T01:36:00.000000Z
                            9\tgoogl\t67.786\t2018-01-01T01:48:00.000000Z\t54\tgoogl\t0.101\t2018-01-01T01:48:00.000000Z
                            10\tgoogl\t38.54\t2018-01-01T02:00:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z
                            11\tmsft\t68.069\t2018-01-01T02:12:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z
                            12\tmsft\t24.008\t2018-01-01T02:24:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z
                            13\tgoogl\t94.559\t2018-01-01T02:36:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z
                            14\tibm\t62.474000000000004\t2018-01-01T02:48:00.000000Z\t50\tibm\t0.068\t2018-01-01T01:40:00.000000Z
                            15\tmsft\t39.017\t2018-01-01T03:00:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z
                            16\tgoogl\t10.643\t2018-01-01T03:12:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z
                            17\tmsft\t7.246\t2018-01-01T03:24:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z
                            18\tmsft\t36.798\t2018-01-01T03:36:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z
                            19\tmsft\t66.98\t2018-01-01T03:48:00.000000Z\t55\tmsft\t0.051000000000000004\t2018-01-01T01:50:00.000000Z
                            20\tgoogl\t26.369\t2018-01-01T04:00:00.000000Z\t60\tgoogl\t0.6900000000000001\t2018-01-01T02:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAsOfJoinOnStr() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.c, y.c, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.c = x.c";

            final String expected = """
                    i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1
                    1\tXYZ\t\t50.938\tnull\t2018-01-01T00:12:00.000000Z\t
                    2\tABC\tABC\t42.281\t0.537\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z
                    3\tABC\tABC\t17.371\t0.673\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                    4\tXYZ\tXYZ\t44.805\t0.116\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z
                    5\t\t\t42.956\t0.47700000000000004\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z
                    6\tCDE\tCDE\t82.59700000000001\t0.24\t2018-01-01T01:12:00.000000Z\t2018-01-01T00:40:00.000000Z
                    7\tCDE\tCDE\t98.59100000000001\t0.24\t2018-01-01T01:24:00.000000Z\t2018-01-01T00:40:00.000000Z
                    8\tABC\tABC\t57.086\t0.59\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:58:00.000000Z
                    9\t\t\t81.44200000000001\t0.47700000000000004\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:00:00.000000Z
                    10\tXYZ\tXYZ\t3.973\t0.867\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:50:00.000000Z
                    """;

            execute(
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
            execute(
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

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            execute(
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
            execute(
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

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1
                            1\tXYZ\t\t50.938\tnull\t2018-01-01T00:12:00.000000Z\t
                            2\tABC\tABC\t42.281\t0.537\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z
                            3\tABC\tABC\t17.371\t0.673\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                            4\tXYZ\tXYZ\t44.805\t0.116\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z
                            5\t\t\t42.956\t0.47700000000000004\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z
                            6\tCDE\tCDE\t82.59700000000001\t0.212\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:12:00.000000Z
                            7\tCDE\tCDE\t98.59100000000001\t0.28200000000000003\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:22:00.000000Z
                            8\tABC\tABC\t57.086\t0.453\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:16:00.000000Z
                            9\t\t\t81.44200000000001\t0.624\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:34:00.000000Z
                            10\tXYZ\tXYZ\t3.973\t0.867\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:50:00.000000Z
                            11\t\t\t85.019\t0.624\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:34:00.000000Z
                            12\tKZZ\tKZZ\t85.49\t0.528\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:56:00.000000Z
                            13\tCDE\tCDE\t27.493000000000002\t0.401\t2018-01-01T02:36:00.000000Z\t2018-01-01T02:00:00.000000Z
                            14\tCDE\tCDE\t39.244\t0.401\t2018-01-01T02:48:00.000000Z\t2018-01-01T02:00:00.000000Z
                            15\tABC\tABC\t55.152\t0.775\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:54:00.000000Z
                            16\tKZZ\tKZZ\t3.224\t0.528\t2018-01-01T03:12:00.000000Z\t2018-01-01T01:56:00.000000Z
                            17\t\t\t6.368\t0.624\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:34:00.000000Z
                            18\tCDE\tCDE\t18.305\t0.401\t2018-01-01T03:36:00.000000Z\t2018-01-01T02:00:00.000000Z
                            19\tABC\tABC\t16.378\t0.775\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:54:00.000000Z
                            20\t\t\t4.773\t0.624\t2018-01-01T04:00:00.000000Z\t2018-01-01T01:34:00.000000Z
                            """);
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
            execute("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            execute("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertQuery("select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join (y order by timestamp desc) y on y.sym2 = x.sym")
                    .noLeakCheck()
                    .fails(65, "right");
        });
    }

    @Test
    public void testAsOfJoinSlaveOmitsMasterKeyColumnName() throws Exception {
        // q.v is a VARCHAR column that nothing references; only the master's key column t.v shares
        // its name. A full-fat ASOF or LT join rejects a var-size slave column, so the slave must not
        // project q.v for the master's key name.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, v VARCHAR, px INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE quotes (sym SYMBOL, v VARCHAR, bid INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE instruments (code VARCHAR, name VARCHAR)");
            execute("""
                    INSERT INTO trades VALUES
                        ('A', 'A', 1, '2024-01-01T00:00:10.000000Z'),
                        ('B', 'B', 2, '2024-01-01T00:00:20.000000Z'),
                        ('C', 'A', 3, '2024-01-01T00:00:30.000000Z'),
                        ('D', 'C', 4, '2024-01-01T00:00:40.000000Z')
                    """);
            execute("""
                    INSERT INTO quotes VALUES
                        ('A', 'x', 10, '2024-01-01T00:00:05.000000Z'),
                        ('B', 'y', 20, '2024-01-01T00:00:06.000000Z'),
                        ('A', 'z', 30, '2024-01-01T00:00:30.000000Z'),
                        ('C', 'w', 40, '2024-01-01T00:00:45.000000Z')
                    """);
            execute("INSERT INTO instruments VALUES ('A', 'alpha'), ('B', 'beta')");
            // UNION ALL gives the view no random access, so ASOF and LT joins run full-fat on it
            execute("CREATE VIEW uq AS (SELECT * FROM (SELECT * FROM quotes UNION ALL SELECT * FROM quotes WHERE bid < 0) TIMESTAMP(ts))");

            // the default compiler on a view slave, and the full-fat compiler flag on a table slave
            final String[] slaves = {"uq", "quotes"};
            final boolean[] isFullFatFlags = {false, true};
            for (int s = 0; s < slaves.length; s++) {
                final String slave = slaves[s];
                final boolean isFullFat = isFullFatFlags[s];

                // the inner join implies q.sym = i.code from q.sym = t.v, and the key keeps both equalities
                final String innerFrom = " FROM trades t JOIN instruments i ON i.code = t.v ";
                final String innerOn = " q ON q.sym = t.v AND q.sym = i.code";
                assertQuery("SELECT t.ts, t.px, i.name, q.sym, q.bid" + innerFrom + "ASOF JOIN " + slave + innerOn)
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .noRandomAccess()
                        .expectSize(isFullFat)
                        .timestamp("ts")
                        .returns("""
                                ts\tpx\tname\tsym\tbid
                                2024-01-01T00:00:10.000000Z\t1\talpha\tA\t10
                                2024-01-01T00:00:20.000000Z\t2\tbeta\tB\t20
                                2024-01-01T00:00:30.000000Z\t3\talpha\tA\t30
                                """);
                assertQuery("SELECT t.ts, t.px, i.name, q.sym, q.bid" + innerFrom + "LT JOIN " + slave + innerOn)
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .noRandomAccess()
                        .expectSize(isFullFat)
                        .timestamp("ts")
                        .returns("""
                                ts\tpx\tname\tsym\tbid
                                2024-01-01T00:00:10.000000Z\t1\talpha\tA\t10
                                2024-01-01T00:00:20.000000Z\t2\tbeta\tB\t20
                                2024-01-01T00:00:30.000000Z\t3\talpha\tA\t10
                                """);

                // a single key pair
                assertQuery("SELECT t.ts, t.px, q.sym, q.bid FROM trades t ASOF JOIN " + slave + " q ON q.sym = t.v")
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .noRandomAccess()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tpx\tsym\tbid
                                2024-01-01T00:00:10.000000Z\t1\tA\t10
                                2024-01-01T00:00:20.000000Z\t2\tB\t20
                                2024-01-01T00:00:30.000000Z\t3\tA\t30
                                2024-01-01T00:00:40.000000Z\t4\t\tnull
                                """);
                assertQuery("SELECT t.ts, t.px, q.sym, q.bid FROM trades t LT JOIN " + slave + " q ON q.sym = t.v")
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .noRandomAccess()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tpx\tsym\tbid
                                2024-01-01T00:00:10.000000Z\t1\tA\t10
                                2024-01-01T00:00:20.000000Z\t2\tB\t20
                                2024-01-01T00:00:30.000000Z\t3\tA\t10
                                2024-01-01T00:00:40.000000Z\t4\t\tnull
                                """);
            }

            // a column that the query references still fails as before
            assertQuery("SELECT t.ts, q.v FROM trades t ASOF JOIN uq q ON q.sym = t.v")
                    .noLeakCheck()
                    .fails(31, "right side column 'v' is of unsupported type");
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
            execute("create table trips as (" +
                    "  select rnd_double() fare_amount, " +
                    "    CAST(x as Timestamp) pickup_datetime " +
                    "  from long_sequence(5)) " +
                    "timestamp(pickup_datetime)");

            execute("create table weather as (" +
                    "  select rnd_double() tempF, " +
                    "    rnd_int() windDir, " +
                    "    cast(x as TIMESTAMP) timestamp " +
                    "  from long_sequence(5)) " +
                    "timestamp(timestamp)");

            assertQuery("""
                    SELECT pickup_datetime, fare_amount, tempF, windDir\s
                    FROM (trips WHERE pickup_datetime IN '1970-01-01')\s
                    ASOF JOIN weather""")
                    .noLeakCheck()
                    .timestamp("pickup_datetime")
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            pickup_datetime\tfare_amount\ttempF\twindDir
                            1970-01-01T00:00:00.000001Z\t0.6607777894187332\t0.6508594025855301\t-1436881714
                            1970-01-01T00:00:00.000002Z\t0.2246301342497259\t0.7905675319675964\t1545253512
                            1970-01-01T00:00:00.000003Z\t0.08486964232560668\t0.22452340856088226\t-409854405
                            1970-01-01T00:00:00.000004Z\t0.299199045961845\t0.3491070363730514\t1904508147
                            1970-01-01T00:00:00.000005Z\t0.20447441837877756\t0.7611029514995744\t1125579207
                            """);
        });
    }

    @Test
    public void testAsofJoinWithComplexConditionFails1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            execute("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 asof join t2 on l1=l2+5", "unsupported ASOF join expression [expr='l1 = l2 + 5']", 35);
        });
    }

    @Test
    public void testAsofJoinWithComplexConditionFails2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            execute("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 asof join t2 on l1>l2", "unsupported ASOF join expression [expr='l1 > l2']", 35);
        });
    }

    @Test
    public void testAsofJoinWithComplexConditionFails3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            execute("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 asof join t2 on l1=abs(l2)", "unsupported ASOF join expression [expr='l1 = abs(l2)']", 35);
        });
    }

    @Test
    public void testAsofJoinWithComplexConditionFails4() throws Exception {
        // Same-table equality on the slave side (l2 = m2) is now routed to the
        // outer-join expression clause and surfaced as an unsupported-expression
        // error, instead of being silently dropped.
        assertMemoryLeak(() -> {
            execute("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            execute("create table t2 (l2 long, m2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 asof join t2 on l1=l2 and l2=m2", "unsupported ASOF join expression [expr='l2 = m2']", 45);
        });
    }

    @Test
    public void testBarrierJoinedMasterFilterStaysPostJoin() throws Exception {
        // The filter references a single table (u1), but u1 is itself the slave of a LEFT join, so
        // assignFilters cannot push it into u1's sub-query and routes it to the multi-reference
        // else-branch. A later RIGHT join NULL-extends both u0 and u1 for the unmatched u2 key 2;
        // the predicate must be held back past it. Anchoring at the LEFT join (where u1 arrives)
        // leaked that NULL-master row, returning 2 rows instead of 1.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u0 (k INT)");
            execute("INSERT INTO u0 VALUES (1)");
            execute("CREATE TABLE u1 (k INT, x INT)");
            execute("INSERT INTO u1 VALUES (1, 1)");
            execute("CREATE TABLE u2 (k INT)");
            execute("INSERT INTO u2 VALUES (1), (2)");

            final String expected = "k\tx\tk1\n1\t1\t1\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                final String literal = "SELECT u0.k, u1.x, u2.k FROM u0 LEFT JOIN u1 ON u0.k = u1.k " + joinType + " JOIN u2 ON u2.k = u0.k WHERE u1.x = 1";
                bindVariableService.clear();
                assertQuery(literal)
                        .noLeakCheck()
                        .noRandomAccess()
                        .withPlanContaining("Filter filter: u1.x=1")
                        .returns(expected);

                final String bind = "SELECT u0.k, u1.x, u2.k FROM u0 LEFT JOIN u1 ON u0.k = u1.k " + joinType + " JOIN u2 ON u2.k = u0.k WHERE u1.x = :v::INT";
                bindVariableService.clear();
                bindVariableService.setInt("v", 1);
                assertQuery(bind).noLeakCheck().noRandomAccess().returns(expected);
            }
        });
    }

    @Test
    public void testColumnEqColumnMasterFilterStaysPostJoin() throws Exception {
        // A same-table column comparison (a.c1 = a.c2) is single-table, so it used to be pushed
        // into the master sub-query. RIGHT/FULL OUTER NULL-extend the master: pushing it emptied
        // the master (its only row fails c1=c2), pairing each slave row with a NULL master and
        // leaking 2 rows. As a post-join filter the full join keeps the matched (1,2) row, which
        // c1=c2 drops, and the unmatched NULL-master row, which c1=c2 keeps because NULL=NULL is
        // true here, leaving exactly one (null,null) row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (c1 INT, c2 INT, k INT)");
            execute("INSERT INTO m VALUES (1, 2, 10)");
            execute("CREATE TABLE s (k INT)");
            execute("INSERT INTO s VALUES (10), (20)");

            final String expected = "c1\tc2\nnull\tnull\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT m.c1, m.c2 FROM m " + joinType + " JOIN s ON m.k = s.k WHERE m.c1 = m.c2")
                        .noLeakCheck()
                        .noRandomAccess()
                        .withPlanContaining("Filter filter: m.c1=m.c2")
                        .returns(expected);
            }
        });
    }

    @Test
    public void testColumnEqColumnOuterJoinedTableStaysPostJoin() throws Exception {
        // Variant of testColumnEqColumnMasterFilterStaysPostJoin where the table the predicate
        // references (a) is itself reached via an outer join, then NULL-extended by a SECOND outer
        // join. analyseEquals routes a same-table equality whose table is barrier-joined to a
        // model-order post-join anchor at that table's own join -- below the later FULL/RIGHT OUTER,
        // which then synthesizes NULL-master rows that bypass the filter, leaking (null,null,1) on
        // top of the legitimate (null,null,3). Held above the outer join, the matched (1,2) row is
        // dropped by c1=c2 and only the (null,null,3) row survives because null=null is true for INT.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (k INT)");
            execute("INSERT INTO t0 VALUES (1)");
            execute("CREATE TABLE a (c1 INT, c2 INT, k INT)");
            execute("INSERT INTO a VALUES (1, 2, 1)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (3)");

            final String expected = "c1\tc2\tk\nnull\tnull\t3\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT a.c1, a.c2, t2.k FROM t0 RIGHT JOIN a ON t0.k = a.k " + joinType + " JOIN t2 ON a.k = t2.k WHERE a.c1 = a.c2")
                        .noLeakCheck()
                        .noRandomAccess()
                        .withPlanContaining("Filter filter: a.c1=a.c2")
                        .returns(expected);
            }
        });
    }

    @Test
    public void testColumnEqColumnFilterAfterNonEquiOuterJoinPushesDown() throws Exception {
        // Companion to testColumnEqColumnMasterFilterStaysPostJoin: there the col=col WHERE sits on the
        // directly NULL-extended master; here it sits on an INNER-joined table (c) that the non-equi
        // RIGHT/FULL OUTER cannot NULL-extend, because constrainRightAndFullJoinOrder keeps that join
        // at its SQL position and c joins its output. assignFilters therefore pushes c.c1 = c.c2 into
        // c's scan. Master ran the outer join after c, NULL-extended c and leaked a (null,50,null,null)
        // row -- 1 row for 0; the deferral via hasNonEquiNullingJoin kept the filter post-join to hide
        // it. The matched (100,50,7,8) row fails c1=c2, so the result is empty either way.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, k INT)");
            execute("INSERT INTO a VALUES (100, 1)");
            execute("CREATE TABLE b (y INT)");
            execute("INSERT INTO b VALUES (50)");
            execute("CREATE TABLE c (k INT, c1 INT, c2 INT)");
            execute("INSERT INTO c VALUES (1, 7, 8)");

            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT a.x, b.y, c.c1, c.c2 FROM a " + joinType + " JOIN b ON a.x > b.y JOIN c ON c.k = a.k WHERE c.c1 = c.c2")
                        .noLeakCheck()
                        .noRandomAccess()
                        .withPlanContaining("filter: c1=c2")
                        .withPlanNotContaining("Filter filter: c.c1=c.c2")
                        .returns("x\ty\tc1\tc2\n");
            }
        });
    }

    @Test
    public void testColumnEqColumnFilterAfterNonEquiOuterJoinPushesDownSymbol() throws Exception {
        // SYMBOL variant of testColumnEqColumnFilterAfterNonEquiOuterJoinPushesDown. b now holds a row
        // that finds no a row. In SQL order the non-equi outer join NULL-extends it and the inner join
        // on c.k = a.k drops it, so only the matched (10,5,foo,foo) row survives; c.v = c.w keeps it.
        // Master joined c first, NULL-extended it for that b row and returned it as (null,100,,), which
        // SYMBOL NULL=NULL kept once item D fixed the key comparison.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1)");
            execute("CREATE TABLE b (y INT)");
            execute("INSERT INTO b VALUES (5), (100)");
            execute("CREATE TABLE c (k INT, v SYMBOL, w SYMBOL)");
            execute("INSERT INTO c VALUES (1, 'foo', 'foo')");

            final String expected = "x\ty\tv\tw\n10\t5\tfoo\tfoo\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT a.x, b.y, c.v, c.w FROM a " + joinType + " JOIN b ON a.x > b.y JOIN c ON c.k = a.k WHERE c.v = c.w ORDER BY b.y")
                        .noLeakCheck()
                        .withPlanContaining("filter: v=w")
                        .withPlanNotContaining("Filter filter: c.v=c.w")
                        .returns(expected);
            }
        });
    }

    @Test
    public void testCrossJoinAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tvch\tkk1\ta1\tb1\tc1\td1\te1\tf1\tg1\ti1\tj1\tk1\tl1\tm1\tn1\tvch1
                    1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t-1966408995\tfalse\tQ\tnull\t0.9441659\t95\t2015-01-04T19:58:55.654Z\tHOLN\t-5024542231726589509\t1970-01-01T00:00:00.000000Z\t39\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\tOJIPHZEPIHVL\t4xL?49Mqqpk-Z
                    1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t387510473\ttrue\tR\t0.30716667810043663\t0.4274704\t181\t2015-07-26T11:59:20.003Z\t\t-8546113611224784332\t1970-01-01T00:16:40.000000Z\t11\t00000000 d8 57 91 88 28 a5 18 93 bd 0b\tJOXPKRGIIHYH\t-Ь\uDA23\uDF64m\uDA30\uDEE01
                    1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t-1810676855\tfalse\tG\t0.06846631555382798\t0.0436064\t970\t2015-06-17T01:06:20.599Z\t\t6405448934035934123\t1970-01-01T00:33:20.000000Z\t22\t00000000 23 3f ae 7c 9f 77 04 e9 0c ea 4e ea 8b f5 0f 2d
                    00000010 b3 14 33\tFFLRBROMNXKUIZ\t}$\uDA43\uDFF0-㔍x
                    2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t1\t-1966408995\tfalse\tQ\tnull\t0.9441659\t95\t2015-01-04T19:58:55.654Z\tHOLN\t-5024542231726589509\t1970-01-01T00:00:00.000000Z\t39\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\tOJIPHZEPIHVL\t4xL?49Mqqpk-Z
                    2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t1\t387510473\ttrue\tR\t0.30716667810043663\t0.4274704\t181\t2015-07-26T11:59:20.003Z\t\t-8546113611224784332\t1970-01-01T00:16:40.000000Z\t11\t00000000 d8 57 91 88 28 a5 18 93 bd 0b\tJOXPKRGIIHYH\t-Ь\uDA23\uDF64m\uDA30\uDEE01
                    2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t1\t-1810676855\tfalse\tG\t0.06846631555382798\t0.0436064\t970\t2015-06-17T01:06:20.599Z\t\t6405448934035934123\t1970-01-01T00:33:20.000000Z\t22\t00000000 23 3f ae 7c 9f 77 04 e9 0c ea 4e ea 8b f5 0f 2d
                    00000010 b3 14 33\tFFLRBROMNXKUIZ\t}$\uDA43\uDFF0-㔍x
                    """;

            execute(
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

            execute(
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
            assertQuery("select * from x cross join y")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testCrossJoinCount() throws Exception {
        assertMemoryLeak(() -> {
            // 1 partition
            execute("create table TabA ( " +
                    "          ts timestamp, " +
                    "          x long " +
                    "        ) timestamp(ts) PARTITION by month");

            // 3 partitions
            execute("create table TabB ( " +
                    "          ts timestamp, " +
                    "          x long " +
                    "        ) timestamp(ts) PARTITION by hour");

            // 0 partitions
            execute("create table TabC ( " +
                    "          ts timestamp, " +
                    "          x long " +
                    "        ) timestamp(ts) PARTITION by year");

            execute("insert into TabA select x::timestamp, x/6 from long_sequence(10)");
            execute("insert into TabB select (x*15L*60L*1000000L)::timestamp, x/6 from long_sequence(10)");

            //join with empty table
            String selectWithEmpty = "(" +
                    "select * from TabA " +
                    "cross join TabC )";
            assertSkipToAndCalculateSize(selectWithEmpty, 0);

            // async filter
            String selectWithFilter = "(" +
                    "select * from TabA " +
                    "cross join TabB " +
                    "where TabA.x = 0 " +
                    "and TabB.x = 1 )";
            assertSkipToAndCalculateSize(selectWithFilter, 25);

            // async filter with limit
            String selectWithFilterWithLimit = "( select * from " +
                    "(select * from TabA where x = 0 limit 3) " +
                    "cross join " +
                    "(select * from TabB where x = 1 limit 3) )";
            assertSkipToAndCalculateSize(selectWithFilterWithLimit, 9);

            // fwd page frame
            String selectWithFwdFrame = "(select * from TabA " +
                    "cross join TabB )";
            assertSkipToAndCalculateSize(selectWithFwdFrame, 100);

            // bwd page frame
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

            // bwd page frame
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
            final String expected = """
                    kk\ta\tb\tc\td\te\tf\tg\ti\tj\tl\tm\tn\tvch\tkk1\ta1\tb1
                    1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t-1966408995\tfalse
                    1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t387510473\ttrue
                    1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t-1810676855\tfalse
                    2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t1\t-1966408995\tfalse
                    2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t1\t387510473\ttrue
                    2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t1\t-1810676855\tfalse
                    """;
            execute(
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

            execute(
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
            assertQuery("select x.kk, x.a, x.b, x.c, x.d, x.e, x.f, x.g, x.i, x.j, x.l, x.m, x.n, x.vch, y.kk, y.a, y.b from x cross join y")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testCrossJoinSkipRowsIsReentrant() throws Exception {
        // Regression test: CrossJoinRecordCursor.skipRows() used to be correct only when called from a
        // master-row boundary. A second skipRows() call (e.g. the one a wrapping LIMIT cursor issues from
        // calculateSize()) re-skipped the already-consumed master cursor and silently dropped the
        // remaining rows of the partially iterated master row. A single master row is the cleanest
        // trigger: after the first skip consumes it, the second skip would find the master exhausted and
        // skip nothing. The original failure (testOrderByAdviceWorksWithCrossJoin1a) was seed-dependent;
        // the exhaustive skip split below reproduces it deterministically.
        assertMemoryLeak(() -> {
            final long[][] shapes = {{1, 9}, {3, 4}, {1, 1}, {5, 1}};
            for (int s = 0; s < shapes.length; s++) {
                final long masterRows = shapes[s][0];
                final long slaveRows = shapes[s][1];
                final long total = masterRows * slaveRows;
                final String query = "select * from long_sequence(" + masterRows
                        + ") a cross join long_sequence(" + slaveRows + ") b";
                try (RecordCursorFactory factory = select(query)) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        // Split the skip across two skipRows() calls so the second one lands mid-stream.
                        for (long skip1 = 0; skip1 <= total; skip1++) {
                            for (long skip2 = 0; skip2 <= total - skip1; skip2++) {
                                cursor.toTop();
                                counter.set(skip1);
                                cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);
                                Assert.assertEquals("first skip should fully apply", 0, counter.get());
                                counter.set(skip2);
                                cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);
                                Assert.assertEquals("second skip should fully apply", 0, counter.get());
                                long remaining = 0;
                                while (cursor.hasNext()) {
                                    remaining++;
                                }
                                Assert.assertEquals(
                                        query + " skip1=" + skip1 + " skip2=" + skip2,
                                        total - skip1 - skip2, remaining);
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testCrossJoinTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tvch\tkk1\ta1\tb1\tc1\td1\te1\tf1\tg1\ti1\tj1\tk1\tl1\tm1\tn1\tvch1
                    1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t-1966408995\tfalse\tQ\tnull\t0.9441659\t95\t2015-01-04T19:58:55.654Z\tHOLN\t-5024542231726589509\t1970-01-01T00:00:00.000000Z\t39\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\tOJIPHZEPIHVL\t4xL?49Mqqpk-Z
                    1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t387510473\ttrue\tR\t0.30716667810043663\t0.4274704\t181\t2015-07-26T11:59:20.003Z\t\t-8546113611224784332\t1970-01-01T00:16:40.000000Z\t11\t00000000 d8 57 91 88 28 a5 18 93 bd 0b\tJOXPKRGIIHYH\t-Ь\uDA23\uDF64m\uDA30\uDEE01
                    1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t-1810676855\tfalse\tG\t0.06846631555382798\t0.0436064\t970\t2015-06-17T01:06:20.599Z\t\t6405448934035934123\t1970-01-01T00:33:20.000000Z\t22\t00000000 23 3f ae 7c 9f 77 04 e9 0c ea 4e ea 8b f5 0f 2d
                    00000010 b3 14 33\tFFLRBROMNXKUIZ\t}$\uDA43\uDFF0-㔍x
                    2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t1\t-1966408995\tfalse\tQ\tnull\t0.9441659\t95\t2015-01-04T19:58:55.654Z\tHOLN\t-5024542231726589509\t1970-01-01T00:00:00.000000Z\t39\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\tOJIPHZEPIHVL\t4xL?49Mqqpk-Z
                    2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t1\t387510473\ttrue\tR\t0.30716667810043663\t0.4274704\t181\t2015-07-26T11:59:20.003Z\t\t-8546113611224784332\t1970-01-01T00:16:40.000000Z\t11\t00000000 d8 57 91 88 28 a5 18 93 bd 0b\tJOXPKRGIIHYH\t-Ь\uDA23\uDF64m\uDA30\uDEE01
                    2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t1\t-1810676855\tfalse\tG\t0.06846631555382798\t0.0436064\t970\t2015-06-17T01:06:20.599Z\t\t6405448934035934123\t1970-01-01T00:33:20.000000Z\t22\t00000000 23 3f ae 7c 9f 77 04 e9 0c ea 4e ea 8b f5 0f 2d
                    00000010 b3 14 33\tFFLRBROMNXKUIZ\t}$\uDA43\uDFF0-㔍x
                    """;

            execute(
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

            execute(
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
            assertQuery("select * from x cross join y")
                    .noLeakCheck()
                    .timestamp("k")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testCrossJoinWithMultiColumnQualifiedJoinKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (event INT, origin INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, 1, '2024-01-01T00:00:00.000000Z'), (2, 2, '2024-01-02T00:00:00.000000Z')");
            assertQuery("SELECT T1.origin, count(*) " +
                    "FROM t T1 " +
                    "CROSS JOIN t T2 " +
                    "CROSS JOIN t T3 " +
                    "JOIN t T4 ON T3.event = T4.event AND T3.origin = T4.origin " +
                    "GROUP BY T1.origin " +
                    "ORDER BY T1.origin")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            origin\tcount
                            1\t4
                            2\t4
                            """);
        });
    }

    @Test
    public void testCrossJoinedMasterFilterStaysPostJoin() throws Exception {
        // t0 is cross-joined into the RIGHT join's master, so that join NULL-extends it for the t2 row
        // that finds no t1 row, and WHERE t0.c = 1 drops that row post-join. Master appended the
        // cross-joined t0 after the RIGHT join, which left t0 un-nulled, pushed the filter into its
        // scan and returned an extra (1,null,2) row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (c INT)");
            execute("INSERT INTO t0 VALUES (1)");
            execute("CREATE TABLE t1 (k INT)");
            execute("INSERT INTO t1 VALUES (1)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (2)");
            assertQuery("SELECT t0.c, t1.k, t2.k FROM t0 CROSS JOIN t1 RIGHT JOIN t2 ON t2.k = t1.k WHERE t0.c = 1 ORDER BY t2.k")
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: t0.c=1")
                    .returns("c\tk\tk1\n1\t1\t1\n");
        });
    }

    @Test
    public void testFirstTableKeepsTimestampOrderOfChain() throws Exception {
        // The join chain streams rows in the order of the table that drives it, and ASOF JOIN matches
        // on that table's timestamps. The implied equalities make m a candidate to take over
        // m.b = t3.a, which would run it after t1: ASOF JOIN would then match on t1's timestamps,
        // and first()/last() would read m's rows in t1's order. A first table with a designated
        // timestamp never takes over an equality, so m drives the chain. The sub-query form declares
        // no timestamp, and a first sub-query never takes over an equality either, so it keeps the
        // same order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT, c INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE t1 (a INT, b INT, c INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE t2 (a INT, b INT, c INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE t3 (a INT, b INT, c INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE t4 (a INT, b INT, c INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t0 VALUES
                    (10, 1, 100, '1970-01-01T00:00:10Z'),
                    (20, 2, 200, '1970-01-01T00:00:20Z'),
                    (30, 1, 300, '1970-01-01T00:00:30Z')
                    """);
            execute("""
                    INSERT INTO t1 VALUES
                    (11, 1, 101, '1970-01-01T00:00:11Z'),
                    (21, 2, 201, '1970-01-01T00:00:21Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (12, 102, 1, '1970-01-01T00:00:12Z'),
                    (22, 202, 2, '1970-01-01T00:00:22Z')
                    """);
            execute("""
                    INSERT INTO t3 VALUES
                    (1, 1, 1, '1970-01-01T00:00:01Z'),
                    (1, 2, 2, '1970-01-01T00:00:01Z'),
                    (2, 2, 2, '1970-01-01T00:00:02Z'),
                    (2, 1, 2, '1970-01-01T00:00:02Z'),
                    (2, 2, 1, '1970-01-01T00:00:02Z')
                    """);
            execute("""
                    INSERT INTO t4 VALUES
                    (1, 1, 104, '1970-01-01T00:00:01Z'),
                    (1, 2, 304, '1970-01-01T00:00:01Z'),
                    (2, 2, 204, '1970-01-01T00:00:02Z'),
                    (2, 1, 404, '1970-01-01T00:00:02Z')
                    """);

            for (String master : new String[]{"t0 m", "(SELECT a, b, c, ts FROM t0 WHERE a > 0) m"}) {
                final String from = " FROM " + master + " CROSS JOIN t1 CROSS JOIN t2 CROSS JOIN t3 JOIN t4 ON m.b = t3.a"
                        + " AND t1.b = t3.c AND t3.c = t4.b AND t4.b = t2.c AND t4.a = t2.c AND t4.a = t3.a"
                        + " AND t3.b = t4.a AND t1.b = t4.a";
                assertQuery("SELECT first(m.a), last(m.a)" + from)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                first\tlast
                                10\t30
                                """);
                assertQuery("SELECT m.a, m.ts, z.a za" + from + " ASOF JOIN t1 z")
                        .noLeakCheck()
                        .noRandomAccess()
                        .timestamp("ts")
                        .returns("""
                                a\tts\tza
                                10\t1970-01-01T00:00:10.000000Z\tnull
                                20\t1970-01-01T00:00:20.000000Z\t11
                                30\t1970-01-01T00:00:30.000000Z\t21
                                """);
            }
        });
    }

    @Test
    public void testFirstTableSlaveProjectsItsJoinKey() throws Exception {
        // The equalities put eight columns in one class. t0 is a plain table without a designated
        // timestamp, so the reorder may make it a slave: it joins on t0.b = t3.a, the only reference
        // to t0.b, so t0's scan must project b although the query does not select it. Every split of
        // the class adds rows on this data, so a plan that drops an equality returns a different result.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT, c INT)");
            execute("CREATE TABLE t1 (a INT, b INT, c INT)");
            execute("CREATE TABLE t2 (a INT, b INT, c INT)");
            execute("CREATE TABLE t3 (a INT, b INT, c INT)");
            execute("CREATE TABLE t4 (a INT, b INT, c INT)");
            execute("INSERT INTO t0 VALUES (10, 1, 100), (20, 2, 200), (30, 1, 300)");
            execute("INSERT INTO t1 VALUES (11, 1, 101), (21, 2, 201)");
            execute("INSERT INTO t2 VALUES (12, 102, 1), (22, 202, 2)");
            execute("INSERT INTO t3 VALUES (1, 1, 1), (2, 2, 2), (1, 2, 2), (2, 1, 2), (2, 2, 1)");
            execute("INSERT INTO t4 VALUES (1, 1, 104), (2, 2, 204), (1, 2, 304), (2, 1, 404)");

            final String equalities = "t0.b = t3.a AND t1.b = t3.c AND t3.c = t4.b AND t4.b = t2.c"
                    + " AND t4.a = t2.c AND t4.a = t3.a AND t3.b = t4.a AND t1.b = t4.a";
            for (String from : new String[]{
                    " FROM t0, t1, t2, t3, t4 WHERE " + equalities,
                    " FROM t0 CROSS JOIN t1 CROSS JOIN t2 CROSS JOIN t3 JOIN t4 ON " + equalities
            }) {
                assertQuery("SELECT t0.a t0a, t0.c t0c, t1.a t1a, t2.a t2a, t3.b t3b, t4.c t4c" + from + " ORDER BY t0a")
                        .noLeakCheck()
                        .withPlanContaining("condition: t0.b=t3.a")
                        .returns("""
                                t0a\tt0c\tt1a\tt2a\tt3b\tt4c
                                10\t100\t11\t12\t1\t104
                                20\t200\t21\t22\t2\t204
                                30\t300\t11\t12\t1\t104
                                """);
                assertQuery("SELECT count(*)" + from)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("condition: t0.b=t3.a")
                        .returns("""
                                count
                                3
                                """);
            }
        });
    }

    @Test
    public void testFirstTableSlaveProjectsMasterJoinKey() throws Exception {
        // t0 is a plain table without a designated timestamp, so the reorder may run it after t3: t0
        // takes over t3.a = t0.b from t3's ON clause, and t3's scan must project a, which only t0's
        // join context references. The second query writes out the equality that t3's ON clause
        // implies and yields the same plan. Every split of each column class adds rows on this data,
        // so a plan that drops an equality returns a different result.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT, c INT)");
            execute("CREATE TABLE t1 (a INT, b INT, c INT)");
            execute("CREATE TABLE t3 (a INT, b INT, c INT)");
            execute("CREATE TABLE t4 (a INT, b INT, c INT)");
            execute("CREATE TABLE t5 (a INT, b INT, c INT)");
            execute("CREATE TABLE t6 (a INT, b INT, c INT)");
            execute("CREATE TABLE t7 (a INT, b INT, c INT)");
            execute("INSERT INTO t0 VALUES (1, 1, 1), (2, 2, 1), (3, 1, 2)");
            execute("INSERT INTO t1 VALUES (1, 1, 7), (2, 2, 8)");
            execute("INSERT INTO t3 VALUES (1, 9, 1), (2, 9, 2), (1, 8, 2)");
            execute("INSERT INTO t4 VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30)");
            execute("INSERT INTO t5 VALUES (1, 1, 1), (2, 2, 2)");
            execute("INSERT INTO t6 VALUES (1, 1, 1), (2, 2, 2)");
            execute("INSERT INTO t7 VALUES (1, 5, 5), (2, 6, 6)");

            for (String t1Join : new String[]{"CROSS JOIN t1 JOIN t3 ON t3.c = t5.c AND", "JOIN t1 ON t1.b = t5.c JOIN t3 ON"}) {
                final String from = " FROM t0 JOIN t4 ON t4.b = t0.b AND t4.a = t0.c CROSS JOIN t5 JOIN t7 ON t7.a = t4.b "
                        + t1Join + " t3.c = t1.b AND t3.a = t0.b CROSS JOIN t6";
                assertQuery("SELECT t0.a t0a, t4.c t4c, t7.b t7b, t3.b t3b, t6.a t6a" + from + " ORDER BY t0a, t4c, t7b, t3b, t6a")
                        .noLeakCheck()
                        .withPlanContaining("condition: t0.b=t3.a")
                        .returns("""
                                t0a\tt4c\tt7b\tt3b\tt6a
                                1\t10\t5\t8\t1
                                1\t10\t5\t8\t2
                                1\t10\t5\t9\t1
                                1\t10\t5\t9\t2
                                2\t20\t6\t9\t1
                                2\t20\t6\t9\t2
                                3\t30\t5\t8\t1
                                3\t30\t5\t8\t2
                                3\t30\t5\t9\t1
                                3\t30\t5\t9\t2
                                """);
                assertQuery("SELECT count(*)" + from)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("condition: t0.b=t3.a")
                        .returns("""
                                count
                                10
                                """);
            }
        });
    }

    @Test
    public void testFirstTableTakesOverEqualityOnlyWithoutTimestamp() throws Exception {
        // The reorder finds no order without a cross join that starts at u0, only one that starts at
        // u1 and lets u0 take over u3.a = u0.a, so u0 joins last. u0 is a plain table without a
        // designated timestamp, so the reorder allows that. The s tables hold the same rows with
        // designated timestamps, and s1 lists them in reverse. There the first table keeps driving the
        // chain, which reaches s1 through hash joins with s3 and s2, so the rows follow s0's
        // timestamps rather than s1's order. Every dropped equality turns the 3 rows into 9 on this data.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u0 (a INT, b INT, x INT)");
            execute("CREATE TABLE u1 (a INT, b INT, x INT)");
            execute("CREATE TABLE u2 (a INT, b INT, x INT)");
            execute("CREATE TABLE u3 (a INT, b INT, x INT)");
            execute("INSERT INTO u0 VALUES (1, 0, 0), (2, 0, 0), (3, 0, 0)");
            execute("INSERT INTO u1 VALUES (0, 0, 3), (0, 0, 2), (0, 0, 1)");
            execute("INSERT INTO u2 VALUES (0, 1, 1), (0, 2, 2), (0, 3, 3)");
            execute("INSERT INTO u3 VALUES (1, 1, 0), (2, 2, 0), (3, 3, 0)");

            for (String from : new String[]{
                    " FROM u0, u1, u2, u3 WHERE u2.x = u1.x AND u3.a = u0.a AND u3.b = u2.b",
                    " FROM u0 CROSS JOIN u1 JOIN u2 ON u2.x = u1.x JOIN u3 ON u3.a = u0.a AND u3.b = u2.b"
            }) {
                assertQuery("SELECT u0.a, u1.x, u2.b, u3.b b3" + from + " ORDER BY u0.a")
                        .noLeakCheck()
                        .withPlanContaining("condition: u0.a=u3.a", "condition: u3.b=u2.b", "condition: u2.x=u1.x")
                        .withPlanNotContaining("Cross Join")
                        .returns("""
                                a\tx\tb\tb3
                                1\t1\t1\t1
                                2\t2\t2\t2
                                3\t3\t3\t3
                                """);
                assertQuery("SELECT count(*)" + from)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("condition: u0.a=u3.a", "condition: u3.b=u2.b", "condition: u2.x=u1.x")
                        .withPlanNotContaining("Cross Join")
                        .returns("""
                                count
                                3
                                """);
                assertQuery("SELECT count(*) FROM (SELECT *" + from + ")")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("condition: u0.a=u3.a", "condition: u3.b=u2.b", "condition: u2.x=u1.x")
                        .withPlanNotContaining("Cross Join")
                        .returns("""
                                count
                                3
                                """);
            }

            execute("CREATE TABLE s0 (a INT, b INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE s1 (a INT, b INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE s2 (a INT, b INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE s3 (a INT, b INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO s0 VALUES
                    (1, 0, 0, '1970-01-01T00:00:01Z'),
                    (2, 0, 0, '1970-01-01T00:00:02Z'),
                    (3, 0, 0, '1970-01-01T00:00:03Z')
                    """);
            execute("""
                    INSERT INTO s1 VALUES
                    (0, 0, 3, '1970-01-01T00:00:01Z'),
                    (0, 0, 2, '1970-01-01T00:00:02Z'),
                    (0, 0, 1, '1970-01-01T00:00:03Z')
                    """);
            execute("""
                    INSERT INTO s2 VALUES
                    (0, 1, 1, '1970-01-01T00:00:01Z'),
                    (0, 2, 2, '1970-01-01T00:00:02Z'),
                    (0, 3, 3, '1970-01-01T00:00:03Z')
                    """);
            execute("""
                    INSERT INTO s3 VALUES
                    (1, 1, 0, '1970-01-01T00:00:01Z'),
                    (2, 2, 0, '1970-01-01T00:00:02Z'),
                    (3, 3, 0, '1970-01-01T00:00:03Z')
                    """);

            final String from = " FROM s0, s1, s2, s3 WHERE s2.x = s1.x AND s3.a = s0.a AND s3.b = s2.b";
            assertQuery("SELECT s0.a, s0.ts, s1.x" + from)
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .withPlanContaining("condition: s3.a=s0.a", "condition: s2.b=s3.b", "condition: s1.x=s2.x")
                    .withPlanNotContaining("Cross Join")
                    .returns("""
                            a\tts\tx
                            1\t1970-01-01T00:00:01.000000Z\t1
                            2\t1970-01-01T00:00:02.000000Z\t2
                            3\t1970-01-01T00:00:03.000000Z\t3
                            """);
            assertQuery("SELECT s0.a, s0.ts, s1.x" + from + " LIMIT -1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanNotContaining("Cross Join")
                    .returns("""
                            a\tts\tx
                            3\t1970-01-01T00:00:03.000000Z\t3
                            """);
            assertQuery("SELECT first(s0.a), last(s0.a), count(*)" + from)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanNotContaining("Cross Join")
                    .returns("""
                            first\tlast\tcount
                            1\t3\t3
                            """);
        });
    }

    @Test
    public void testFirstTableWithTimestampReachesBranchingJoinGraphThroughHashJoins() throws Exception {
        // No equality links g1 to g0. g2 and g3 hold equalities with g1 alone, and g4 cannot hand
        // g4.x = g1.x over to g1, because g4 depends on g2, which depends on g1. The reorder used to
        // cross join g1 right after g0. The chain now starts from g4, the only table that an equality
        // links to g0: g4 hands its equalities with g1 and g2 over to them, so g1 joins g4, g2 joins
        // g1 and g4 on both of its keys, and g3 joins g1. The row of g0 that matches two rows of g4
        // comes out three times, and g4.z = g2.z drops the chain of the third row of g0.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE g0 (k INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE g1 (x INT, y INT, w INT, v INT)");
            execute("CREATE TABLE g2 (y INT, z INT, v INT)");
            execute("CREATE TABLE g3 (w INT, v INT)");
            execute("CREATE TABLE g4 (k INT, x INT, z INT, v INT)");
            execute("""
                    INSERT INTO g0 VALUES
                    (1, 101, '1970-01-01T00:00:01Z'),
                    (2, 102, '1970-01-01T00:00:02Z'),
                    (3, 103, '1970-01-01T00:00:03Z')
                    """);
            execute("INSERT INTO g1 VALUES (10, 20, 30, 201), (11, 21, 31, 202), (12, 22, 32, 203)");
            execute("INSERT INTO g2 VALUES (21, 41, 301), (20, 40, 302), (22, 99, 303)");
            execute("INSERT INTO g3 VALUES (31, 401), (30, 402), (30, 403), (39, 404)");
            execute("INSERT INTO g4 VALUES (2, 11, 41, 501), (1, 10, 40, 502), (3, 12, 42, 503), (1, 11, 41, 504)");

            final String from = " FROM g0, g1, g2, g3, g4"
                    + " WHERE g4.k = g0.k AND g4.x = g1.x AND g2.y = g1.y AND g4.z = g2.z AND g3.w = g1.w";
            assertQuery("SELECT g0.v, g0.ts" + from)
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .withPlanContaining(
                            "condition: g4.k=g0.k",
                            "condition: g1.x=g4.x",
                            "condition: g2.y=g1.y and g2.z=g4.z",
                            "condition: g3.w=g1.w"
                    )
                    .withPlanNotContaining("Cross Join")
                    .returns("""
                            v\tts
                            101\t1970-01-01T00:00:01.000000Z
                            101\t1970-01-01T00:00:01.000000Z
                            101\t1970-01-01T00:00:01.000000Z
                            102\t1970-01-01T00:00:02.000000Z
                            """);
            assertQuery("SELECT g0.v, g0.ts, g1.v v1, g2.v v2, g3.v v3, g4.v v4" + from + " ORDER BY g0.ts, v4, v3")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanNotContaining("Cross Join")
                    .returns("""
                            v\tts\tv1\tv2\tv3\tv4
                            101\t1970-01-01T00:00:01.000000Z\t201\t302\t402\t502
                            101\t1970-01-01T00:00:01.000000Z\t201\t302\t403\t502
                            101\t1970-01-01T00:00:01.000000Z\t202\t301\t401\t504
                            102\t1970-01-01T00:00:02.000000Z\t202\t301\t401\t501
                            """);
            assertQuery("SELECT first(g0.ts) first_ts, last(g0.ts) last_ts, count(*) cnt, sum(g3.v) total" + from)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanNotContaining("Cross Join")
                    .returns("""
                            first_ts\tlast_ts\tcnt\ttotal
                            1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:02.000000Z\t4\t1607
                            """);
        });
    }

    @Test
    public void testFirstTableWithTimestampReachesStarSchemaThroughHashJoins() throws Exception {
        // The query lists the fact table first and the dimensions outermost-first, so no equality
        // links countries to a table before it, and venues holds no other equality that would let
        // countries take over v.country = c.id. trades has a designated timestamp, so it drives the
        // chain and never takes over i.id = t.instrument. The reorder reaches the dimensions from
        // trades instead: instruments joins trades, venues joins instruments and countries joins
        // venues, all through hash joins. Rows keep the order of trades, and a NULL key matches
        // nothing on either side of the chain.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (instrument INT, px INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE countries (id INT, name SYMBOL)");
            execute("CREATE TABLE venues (id INT, country INT)");
            execute("CREATE TABLE instruments (id INT, venue INT)");
            execute("""
                    INSERT INTO trades VALUES
                    (3, 100, '1970-01-01T00:00:01Z'),
                    (1, 101, '1970-01-01T00:00:02Z'),
                    (9, 102, '1970-01-01T00:00:03Z'),
                    (2, 103, '1970-01-01T00:00:04Z'),
                    (3, 104, '1970-01-01T00:00:05Z'),
                    (4, 105, '1970-01-01T00:00:06Z'),
                    (NULL, 106, '1970-01-01T00:00:07Z'),
                    (6, 107, '1970-01-01T00:00:08Z')
                    """);
            execute("INSERT INTO countries VALUES (2, 'de'), (1, 'uk'), (3, 'fr')");
            execute("INSERT INTO venues VALUES (20, 1), (10, 2), (30, 7), (40, NULL)");
            execute("INSERT INTO instruments VALUES (4, 30), (3, 20), (2, 20), (1, 10), (5, 10), (6, 40)");

            final String from = " FROM trades t, countries c, venues v, instruments i"
                    + " WHERE v.country = c.id AND i.venue = v.id AND i.id = t.instrument";
            assertQuery("SELECT t.px, t.ts, c.name, v.id vid, i.id iid" + from)
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .withPlan("""
                            SelectedRecord
                                Hash Join Light
                                  condition: c.id=v.country
                                    Hash Join Light
                                      condition: v.id=i.venue
                                        Hash Join Light
                                          condition: i.id=t.instrument
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: trades
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: instruments
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: venues
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: countries
                            """)
                    .returns("""
                            px\tts\tname\tvid\tiid
                            100\t1970-01-01T00:00:01.000000Z\tuk\t20\t3
                            101\t1970-01-01T00:00:02.000000Z\tde\t10\t1
                            103\t1970-01-01T00:00:04.000000Z\tuk\t20\t2
                            104\t1970-01-01T00:00:05.000000Z\tuk\t20\t3
                            """);
            assertQuery("SELECT t.px, t.ts, c.name" + from + " LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanNotContaining("Cross Join")
                    .returns("""
                            px\tts\tname
                            103\t1970-01-01T00:00:04.000000Z\tuk
                            104\t1970-01-01T00:00:05.000000Z\tuk
                            """);
            assertQuery("SELECT c.name, count(*) cnt, sum(t.px) total, first(t.px) first_px, last(t.px) last_px" + from + " ORDER BY name")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining("Cross Join")
                    .returns("""
                            name\tcnt\ttotal\tfirst_px\tlast_px
                            de\t1\t101\t101\t101
                            uk\t3\t307\t100\t104
                            """);
        });
    }

    @Test
    public void testFirstTableWithTimestampReachesTablesThroughHashJoins() throws Exception {
        // The first item has a designated timestamp, or is a sub-query or CTE, so it drives the join
        // chain and never takes over an equality. No equality links s1 to an item before it, and s2
        // holds no other equality that would let s1 take over s2.x = s1.x, so the reorder used to
        // cross join s1 right after the first item. The chain now reaches s1 through hash joins: s3
        // joins the first item, s2 joins s3 and s1 joins s2. s1 lists its rows against the order of
        // s0, so a chain that followed s1 would return the rows in another order. Some rows of s0
        // match two rows, and one matches none, so a dropped equality changes the row count.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s0 (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE s1 (x INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE s2 (b INT, x INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE s3 (a INT, b INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE q (v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO s0 VALUES
                    (1, 101, '1970-01-01T00:00:01Z'),
                    (2, 102, '1970-01-01T00:00:02Z'),
                    (3, 103, '1970-01-01T00:00:03Z'),
                    (4, 104, '1970-01-01T00:00:04Z'),
                    (2, 105, '1970-01-01T00:00:05Z')
                    """);
            execute("""
                    INSERT INTO s1 VALUES
                    (300, 11, '1970-01-01T00:00:01Z'),
                    (210, 12, '1970-01-01T00:00:02Z'),
                    (200, 13, '1970-01-01T00:00:03Z'),
                    (100, 14, '1970-01-01T00:00:04Z'),
                    (100, 15, '1970-01-01T00:00:05Z'),
                    (999, 16, '1970-01-01T00:00:06Z')
                    """);
            execute("""
                    INSERT INTO s2 VALUES
                    (10, 100, 21, '1970-01-01T00:00:01Z'),
                    (20, 200, 22, '1970-01-01T00:00:02Z'),
                    (21, 210, 23, '1970-01-01T00:00:03Z'),
                    (30, 300, 24, '1970-01-01T00:00:04Z'),
                    (40, 400, 25, '1970-01-01T00:00:05Z')
                    """);
            execute("""
                    INSERT INTO s3 VALUES
                    (3, 30, 31, '1970-01-01T00:00:01Z'),
                    (2, 20, 32, '1970-01-01T00:00:02Z'),
                    (1, 10, 33, '1970-01-01T00:00:03Z'),
                    (2, 21, 34, '1970-01-01T00:00:04Z'),
                    (5, 50, 35, '1970-01-01T00:00:05Z'),
                    (1, 40, 36, '1970-01-01T00:00:06Z')
                    """);
            execute("""
                    INSERT INTO q VALUES
                    (901, '1970-01-01T00:00:02Z'),
                    (902, '1970-01-01T00:00:04Z'),
                    (903, '1970-01-01T00:00:06Z')
                    """);

            final String where = " WHERE s2.x = s1.x AND s3.a = m.a AND s3.b = s2.b";
            assertQuery("SELECT m.v, m.ts FROM s0 m, s1, s2, s3" + where)
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Hash Join Light
                                  condition: s1.x=s2.x
                                    Hash Join Light
                                      condition: s2.b=s3.b
                                        Hash Join Light
                                          condition: s3.a=m.a
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: s0
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: s3
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: s2
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: s1
                            """);
            assertFirstTableReachesTablesThroughHashJoins("", "s0 m, s1, s2, s3", where);
            assertFirstTableReachesTablesThroughHashJoins("", "s0 m CROSS JOIN s1 JOIN s2 ON s2.x = s1.x JOIN s3 ON s3.a = m.a AND s3.b = s2.b", "");
            assertFirstTableReachesTablesThroughHashJoins("", "(SELECT a, v, ts FROM s0 WHERE v > 0) m, s1, s2, s3", where);
            assertFirstTableReachesTablesThroughHashJoins("WITH m AS (SELECT a, v, ts FROM s0 WHERE v > 0) ", "m, s1, s2, s3", where);
        });
    }

    @Test
    public void testForwardRefOuterJoinOnClauseFails() throws Exception {
        // A RIGHT/FULL OUTER ON that references a table joined later has no meaning in SQL: the join's
        // master is the prefix before it, which does not contain c. constrainRightAndFullJoinOrder
        // keeps the join at that position, so code generation compiles the ON against metadata without
        // c and rejects the reference, as PostgreSQL, MySQL and DuckDB do. Master homogenized such a
        // join to a CROSS variant, appended it after c, and ran the query as (a JOIN c) RIGHT JOIN b.
        // LEFT OUTER keeps that reorder -- it NULL-extends only its own slave -- and still compiles.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT, k2 INT, av INT)");
            execute("INSERT INTO a VALUES (1, 100, 11)");
            execute("CREATE TABLE b (k INT, bv INT)");
            execute("INSERT INTO b VALUES (1, 21), (9, 29)");
            execute("CREATE TABLE c (k INT, k2 INT, v INT)");
            execute("INSERT INTO c VALUES (1, 100, 1)");

            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                final String sql = "SELECT a.av, b.k bk, b.bv, c.v cv FROM a " + joinType
                        + " JOIN b ON b.k = c.k JOIN c ON c.k2 = a.k2 WHERE c.v = 1 ORDER BY bk";
                assertQuery(sql)
                        .noLeakCheck()
                        .fails(sql.indexOf("c.k JOIN"), "Invalid column: c.k");
            }

            assertQuery("SELECT a.av, b.k bk, b.bv, c.v cv FROM a LEFT OUTER JOIN b ON b.k = c.k JOIN c ON c.k2 = a.k2 WHERE c.v = 1 ORDER BY bk")
                    .noLeakCheck()
                    .returns("av\tbk\tbv\tcv\n11\t1\t21\t1\n");
        });
    }

    @Test
    public void testFirstTableWithTimestampStopsChainAtLeftJoin() throws Exception {
        // s0 drives the chain, and no equality before the LEFT JOIN links it to s1 or s2, so a cross join
        // stays. The LEFT JOIN's ON clause links s4 to s0 and s2, but the chain must not reach across the
        // LEFT JOIN: anchoring s4 to s0 would make s4.b = s2.b an inner hash join key and drop the rows
        // that the LEFT JOIN NULL-extends.
        assertMemoryLeak(() -> {
            createFirstTableBarrierTables();
            assertQuery("SELECT s0.v v0, s0.ts, s1.v v1, s2.v v2, s4.v v4 FROM s0, s1, s2 LEFT JOIN s4 ON s4.a = s0.a AND s4.b = s2.b WHERE s2.x = s1.x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .withPlanContaining("Cross Join", "condition: s4.b=s2.b and s4.a=s0.a")
                    .returns("""
                            v0\tts\tv1\tv2\tv4
                            100\t2024-01-01T10:00:00.000000Z\t200\t300\t400
                            100\t2024-01-01T10:00:00.000000Z\t201\t301\tnull
                            101\t2024-01-01T11:00:00.000000Z\t200\t300\tnull
                            101\t2024-01-01T11:00:00.000000Z\t201\t301\tnull
                            """);
        });
    }

    @Test
    public void testFirstTableWithTimestampStopsChainAtLtJoin() throws Exception {
        // s0 drives the chain, and no equality before the LT JOIN links it to s1 or s2, so a cross join
        // stays. The LT JOIN's keys link s4 to s0 and s2, but the chain must not reach across the LT
        // JOIN: anchoring s4 to s0 would make s4.b = s2.b an inner hash join key and drop the rows
        // without an earlier s4 row.
        assertMemoryLeak(() -> {
            createFirstTableBarrierTables();
            assertQuery("SELECT s0.v v0, s0.ts, s1.v v1, s2.v v2, s4.v v4 FROM s0 CROSS JOIN s1 JOIN s2 ON s2.x = s1.x LT JOIN s4 ON (s4.a = s0.a AND s4.b = s2.b)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .withPlanContaining("Cross Join", "condition: s4.b=s2.b and s4.a=s0.a")
                    .returns("""
                            v0\tts\tv1\tv2\tv4
                            100\t2024-01-01T10:00:00.000000Z\t200\t300\t400
                            100\t2024-01-01T10:00:00.000000Z\t201\t301\tnull
                            101\t2024-01-01T11:00:00.000000Z\t200\t300\tnull
                            101\t2024-01-01T11:00:00.000000Z\t201\t301\tnull
                            """);
        });
    }

    @Test
    public void testFirstTableWithoutEqualityAsOfJoinMatchesItsTimestamp() throws Exception {
        // ASOF and LT JOIN over the join match on its designated timestamp, which is l.ts, not r.rts
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            final String rows = """
                    ts\trts\tdts\tv
                    2020-01-01T00:10:00.000000Z\t2020-01-01T00:20:00.000000Z\t2020-01-01T00:00:00.000000Z\t1
                    2020-01-01T00:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t2020-01-01T00:00:00.000000Z\t1
                    2020-01-01T00:10:00.000000Z\t2020-01-01T03:20:00.000000Z\t2020-01-01T00:00:00.000000Z\t1
                    2020-01-01T02:10:00.000000Z\t2020-01-01T00:20:00.000000Z\t2020-01-01T02:00:00.000000Z\t4
                    2020-01-01T02:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t2020-01-01T02:00:00.000000Z\t4
                    2020-01-01T02:10:00.000000Z\t2020-01-01T03:20:00.000000Z\t2020-01-01T02:00:00.000000Z\t4
                    """;
            for (String join : new String[]{"ASOF", "LT"}) {
                assertQuery("SELECT x.ts, x.rts, d.dts, d.v FROM (SELECT l.ts, r.rts FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts) x "
                        + join + " JOIN td d")
                        .noLeakCheck()
                        .noRandomAccess()
                        .timestamp("ts")
                        .returns(rows);
            }
        });
    }

    @Test
    public void testFirstTableWithoutEqualityAsOfSlaveFollowsItsTimestamp() throws Exception {
        // as the slave of an ASOF JOIN, the join yields for each td row the last of its rows whose l.ts
        // is not later than td's timestamp
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            assertQuery("SELECT d.dts, x.ts, x.rts FROM td d ASOF JOIN (SELECT l.ts, r.rts FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts) x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("dts")
                    .returns("""
                            dts\tts\trts
                            2020-01-01T00:00:00.000000Z\t\t
                            2020-01-01T00:15:00.000000Z\t2020-01-01T00:10:00.000000Z\t2020-01-01T03:20:00.000000Z
                            2020-01-01T01:30:00.000000Z\t2020-01-01T00:10:00.000000Z\t2020-01-01T03:20:00.000000Z
                            2020-01-01T02:00:00.000000Z\t2020-01-01T00:10:00.000000Z\t2020-01-01T03:20:00.000000Z
                            2020-01-01T03:00:00.000000Z\t2020-01-01T02:10:00.000000Z\t2020-01-01T03:20:00.000000Z
                            """);
        });
    }

    @Test
    public void testFirstTableWithoutEqualityCreateTableKeepsPlanOfSelect() throws Exception {
        // CREATE TABLE AS SELECT, like INSERT AS SELECT, keeps the plan that runs ta last at every level of
        // its SELECT. The SELECT streams tb2's rows and declares r.rts, so the new table takes r.rts as its
        // designated timestamp, and a non-partitioned table with TIMESTAMP(rts) receives its rows in order.
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            final String join = " FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts";
            execute("CREATE TABLE tj AS (SELECT l.ts lts, r.rts, r.c1" + join + ")");
            execute("CREATE TABLE tj_rts AS (SELECT l.ts lts, r.rts, r.c1" + join + ") TIMESTAMP(rts)");
            execute("CREATE TABLE tj_none AS (SELECT l.ts lts, r.rts, r.c1" + join + ") TIMESTAMP(rts) PARTITION BY NONE");
            assertQuery("SELECT table_name, designatedTimestamp FROM tables() WHERE table_name LIKE 'tj%' ORDER BY table_name")
                    .noLeakCheck()
                    .returns("""
                            table_name\tdesignatedTimestamp
                            tj\trts
                            tj_none\trts
                            tj_rts\trts
                            """);
            for (String table : new String[]{"tj", "tj_rts", "tj_none"}) {
                assertQuery("SELECT * FROM " + table)
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("rts")
                        .returns("""
                                lts\trts\tc1
                                2020-01-01T00:10:00.000000Z\t2020-01-01T00:20:00.000000Z\t10
                                2020-01-01T02:10:00.000000Z\t2020-01-01T00:20:00.000000Z\t10
                                2020-01-01T00:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t20
                                2020-01-01T02:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t20
                                2020-01-01T00:10:00.000000Z\t2020-01-01T03:20:00.000000Z\t30
                                2020-01-01T02:10:00.000000Z\t2020-01-01T03:20:00.000000Z\t30
                                """);
            }
            // the SELECT reads no column of ta
            execute("CREATE TABLE tj_chain AS (SELECT r.rts, r.c1" + join + ") TIMESTAMP(rts)");
            assertQuery("SELECT * FROM tj_chain")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("rts")
                    .returns("""
                            rts\tc1
                            2020-01-01T00:20:00.000000Z\t10
                            2020-01-01T00:20:00.000000Z\t10
                            2020-01-01T01:20:00.000000Z\t20
                            2020-01-01T01:20:00.000000Z\t20
                            2020-01-01T03:20:00.000000Z\t30
                            2020-01-01T03:20:00.000000Z\t30
                            """);
            // the sub-queries of the SELECT keep that plan as well: the ASOF JOIN in the IN set matches
            // on r.rts, and the set {10, 20, 30} holds '10' and '20' but not '7'
            execute("CREATE TABLE tsym (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tsym VALUES ('10', '2020-01-01T00:00'), ('20', '2020-01-01T01:00'), ('7', '2020-01-01T02:00')");
            execute("CREATE TABLE tj_in AS (SELECT * FROM tsym WHERE s IN (SELECT x.c1::SYMBOL FROM (SELECT l.ts, r.c1" + join + ") x ASOF JOIN td d))");
            assertQuery("SELECT * FROM tj_in")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            s\tts
                            10\t2020-01-01T00:00:00.000000Z
                            20\t2020-01-01T01:00:00.000000Z
                            """);
            // EXPLAIN shows the plan that CREATE TABLE AS SELECT runs
            assertQuery("CREATE TABLE tj_plan AS (SELECT l.ts lts, r.rts, r.c1" + join + ")")
                    .noLeakCheck()
                    .assertsPlan("""
                            Create table: tj_plan
                                SelectedRecord
                                    Cross Join
                                        Hash Join Light
                                          condition: n.cts=r.rts
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tb2
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tc2
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: ta
                            """);
        });
    }

    @Test
    public void testFirstTableWithoutEqualityDeclaresItsTimestamp() throws Exception {
        // the join streams ta's rows, joined to tb2 and tc2 by a cross join and a hash join, and declares
        // l.ts as its designated timestamp
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            final String rows = """
                    lts\trts\tc1
                    2020-01-01T00:10:00.000000Z\t2020-01-01T00:20:00.000000Z\t10
                    2020-01-01T00:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t20
                    2020-01-01T00:10:00.000000Z\t2020-01-01T03:20:00.000000Z\t30
                    2020-01-01T02:10:00.000000Z\t2020-01-01T00:20:00.000000Z\t10
                    2020-01-01T02:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t20
                    2020-01-01T02:10:00.000000Z\t2020-01-01T03:20:00.000000Z\t30
                    """;
            final String plan = """
                    SelectedRecord
                        Hash Join Light
                          condition: n.cts=r.rts
                            Cross Join
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: ta
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tb2
                            Hash
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tc2
                    """;
            for (String from : new String[]{
                    " FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts",
                    " FROM ta l, tb2 r, tc2 n WHERE n.cts = r.rts"
            }) {
                assertQuery("SELECT l.ts lts, r.rts, r.c1" + from)
                        .noLeakCheck()
                        .noRandomAccess()
                        .timestamp("lts")
                        .withPlan(plan)
                        .returns(rows);
            }
        });
    }

    @Test
    public void testFirstTableWithoutEqualityExplicitTimestampOfChainKeepsItsOrder() throws Exception {
        // TIMESTAMP(rts) declares the order of tb2's timestamp, so the join keeps the plan that streams
        // tb2's rows. The filter of the time series joins keeps (02:10, 00:20), (02:10, 01:20) and
        // (00:10, 03:20), one row per rts value.
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            final String join = "SELECT l.ts lts, r.rts, r.c1 FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts";
            final String filtered = "((" + join + " WHERE (l.v = 9 AND r.c1 < 30) OR (l.v = 1 AND r.c1 = 30)) TIMESTAMP(rts)) x";
            assertQuery("SELECT rts, c1 FROM (" + join + ") TIMESTAMP(rts)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("rts")
                    .returns("""
                            rts\tc1
                            2020-01-01T00:20:00.000000Z\t10
                            2020-01-01T00:20:00.000000Z\t10
                            2020-01-01T01:20:00.000000Z\t20
                            2020-01-01T01:20:00.000000Z\t20
                            2020-01-01T03:20:00.000000Z\t30
                            2020-01-01T03:20:00.000000Z\t30
                            """);
            // for each td row, the last row whose rts is not later than td's timestamp
            assertQuery("SELECT d.dts, x.lts, x.rts, x.c1 FROM td d ASOF JOIN " + filtered)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("dts")
                    .returns("""
                            dts\tlts\trts\tc1
                            2020-01-01T00:00:00.000000Z\t\t\tnull
                            2020-01-01T00:15:00.000000Z\t\t\tnull
                            2020-01-01T01:30:00.000000Z\t2020-01-01T02:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t20
                            2020-01-01T02:00:00.000000Z\t2020-01-01T02:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t20
                            2020-01-01T03:00:00.000000Z\t2020-01-01T02:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t20
                            """);
            // for each row, the last td row whose timestamp is not later than rts
            assertQuery("SELECT x.lts, x.rts, d.dts, d.v FROM " + filtered + " ASOF JOIN td d")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("rts")
                    .returns("""
                            lts\trts\tdts\tv
                            2020-01-01T02:10:00.000000Z\t2020-01-01T00:20:00.000000Z\t2020-01-01T00:15:00.000000Z\t2
                            2020-01-01T02:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t2020-01-01T00:15:00.000000Z\t2
                            2020-01-01T00:10:00.000000Z\t2020-01-01T03:20:00.000000Z\t2020-01-01T03:00:00.000000Z\t5
                            """);
            // the window around 00:20 holds td's 00:00 and 00:15; the one around 01:20 holds 01:30 and
            // 02:00, plus 00:15 as the prevailing row; the one around 03:20 holds 03:00, plus 02:00
            assertQuery("SELECT x.rts, sum(d.v) s FROM ((" + join + ") TIMESTAMP(rts)) x"
                    + " WINDOW JOIN td d RANGE BETWEEN 1 hour PRECEDING AND 1 hour FOLLOWING")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("rts")
                    .returns("""
                            rts\ts
                            2020-01-01T00:20:00.000000Z\t3
                            2020-01-01T00:20:00.000000Z\t3
                            2020-01-01T01:20:00.000000Z\t9
                            2020-01-01T01:20:00.000000Z\t9
                            2020-01-01T03:20:00.000000Z\t9
                            2020-01-01T03:20:00.000000Z\t9
                            """);
        });
    }

    @Test
    public void testFirstTableWithoutEqualityExplicitTimestampOfChainSampleBy() throws Exception {
        // TIMESTAMP(rts) declares the order of tb2's timestamp, so every join below the clause keeps the
        // plan that runs ta last and streams tb2's rows: also where the sub-query reads its own rows in
        // order, through a window function or LIMIT, and where the join is the first model of another
        // join. Each rts value holds two rows.
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            final String join = "SELECT l.ts lts, r.rts, r.c1 FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts";
            final String buckets = """
                    c\ts
                    2\t20
                    2\t40
                    2\t60
                    """;
            assertQuery("SELECT count() c, sum(c1) s FROM (" + join + ") TIMESTAMP(rts) SAMPLE BY 1h ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(buckets);
            assertQuery("WITH w AS (" + join + ") SELECT count() c, sum(c1) s FROM w TIMESTAMP(rts) SAMPLE BY 1h ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(buckets);
            assertQuery("SELECT count() c, sum(c1) s FROM ((SELECT l.ts lts, r.rts, r.c1, row_number() OVER () rn"
                    + " FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts) TIMESTAMP(rts)) SAMPLE BY 1h ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(buckets);
            // x.rts comes from tb2 inside x, the first model of the outer join
            assertQuery("SELECT count() c, sum(c1) s FROM ((SELECT x.rts, x.c1 FROM (" + join + ") x CROSS JOIN long_sequence(1) z)"
                    + " TIMESTAMP(rts)) SAMPLE BY 1h ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(buckets);
            assertQuery("SELECT count() c, sum(c1) s FROM ((SELECT x.rts, x.c1, d.v FROM (" + join + ") x ASOF JOIN td d)"
                    + " TIMESTAMP(rts)) SAMPLE BY 1h ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(buckets);
            assertQuery("SELECT rts, count() c, sum(c1) s FROM ((" + join + " LIMIT 100) TIMESTAMP(rts)) SAMPLE BY 1h ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("rts")
                    .returns("""
                            rts\tc\ts
                            2020-01-01T00:20:00.000000Z\t2\t20
                            2020-01-01T01:20:00.000000Z\t2\t40
                            2020-01-01T03:20:00.000000Z\t2\t60
                            """);
            // SAMPLE BY over a view does not become a GROUP BY, so it buckets the rows in their order
            execute("CREATE VIEW vjr AS (SELECT * FROM (" + join + ") TIMESTAMP(rts))");
            assertQuery("SELECT rts, count() c, sum(c1) s FROM vjr SAMPLE BY 1h")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("rts")
                    .returns("""
                            rts\tc\ts
                            2020-01-01T00:00:00.000000Z\t2\t20
                            2020-01-01T01:00:00.000000Z\t2\t40
                            2020-01-01T03:00:00.000000Z\t2\t60
                            """);
            // the same tables, all timestamped on ts: only the alias of r.ts tells it from sa's timestamp
            execute("CREATE TABLE sa (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE sb (ts TIMESTAMP, v INT, c1 INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE sc (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO sa SELECT * FROM ta");
            execute("INSERT INTO sb SELECT * FROM tb2");
            execute("INSERT INTO sc SELECT * FROM tc2");
            assertQuery("SELECT count() c, sum(c1) s FROM (SELECT l.ts lts, r.ts rts, r.c1 FROM sa l CROSS JOIN sb r JOIN sc n ON n.ts = r.ts)"
                    + " TIMESTAMP(rts) SAMPLE BY 1h ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(buckets);
        });
    }

    @Test
    public void testFirstTableWithoutEqualityExplicitTimestampOfFirstTableSampleBy() throws Exception {
        // TIMESTAMP(lts) declares the order of ta's designated timestamp, which the join keeps when ta
        // drives it, wherever the clause sits: each l.ts bucket holds one ta row joined to three chain rows
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            final String buckets = """
                    c\ts
                    3\t60
                    3\t60
                    """;
            for (String query : new String[]{
                    // the first item is a sub-query
                    "SELECT count() c, sum(c1) s FROM (SELECT l.ts lts, r.c1 FROM (SELECT * FROM ta) l"
                            + " CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts) TIMESTAMP(lts)",
                    // the first item is a CTE, and the projection does not qualify the column
                    "WITH w AS (SELECT * FROM ta) SELECT count() c, sum(c1) s FROM (SELECT ts lts, c1 FROM w l"
                            + " CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts) TIMESTAMP(lts)",
                    // the join is the first model of another join
                    "SELECT count() c, sum(c1) s FROM ((SELECT x.lts, x.c1 FROM (SELECT l.ts lts, r.c1 FROM ta l"
                            + " CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts) x CROSS JOIN long_sequence(1) z) TIMESTAMP(lts))"
            }) {
                assertQuery(query + " SAMPLE BY 1h ALIGN TO FIRST OBSERVATION")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns(buckets);
            }
            execute("CREATE VIEW vjl AS (SELECT * FROM (SELECT l.ts lts, r.c1 FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts) TIMESTAMP(lts))");
            assertQuery("SELECT lts, count() c, sum(c1) s FROM vjl SAMPLE BY 1h")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("lts")
                    .returns("""
                            lts\tc\ts
                            2020-01-01T00:00:00.000000Z\t3\t60
                            2020-01-01T02:00:00.000000Z\t3\t60
                            """);
        });
    }

    @Test
    public void testFirstTableWithoutEqualityFirstLastFollowItsOrder() throws Exception {
        // the filter keeps (l.v = 1, r.c1 = 30) and (l.v = 9, r.c1 = 10), in this order of ta's rows
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            assertQuery("SELECT first(l.v) fl, last(l.v) ll, first(r.c1) fr, last(r.c1) lr FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts"
                    + " WHERE (l.v = 1 AND r.c1 = 30) OR (l.v = 9 AND r.c1 = 10)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            fl\tll\tfr\tlr
                            1\t9\t30\t10
                            """);
        });
    }

    @Test
    public void testFirstTableWithoutEqualityInsertKeepsTargetTimestamp() throws Exception {
        // INSERT AS SELECT keeps the plan that runs ta last at every level of its SELECT, so the SELECT
        // declares r.rts, which feeds the target's designated timestamp.
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            final String join = " FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts";
            final String rows = """
                    lts\trts\tc1
                    2020-01-01T00:10:00.000000Z\t2020-01-01T00:20:00.000000Z\t10
                    2020-01-01T02:10:00.000000Z\t2020-01-01T00:20:00.000000Z\t10
                    2020-01-01T00:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t20
                    2020-01-01T02:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t20
                    2020-01-01T00:10:00.000000Z\t2020-01-01T03:20:00.000000Z\t30
                    2020-01-01T02:10:00.000000Z\t2020-01-01T03:20:00.000000Z\t30
                    """;
            execute("CREATE TABLE ins_rts (lts TIMESTAMP, rts TIMESTAMP, c1 INT) TIMESTAMP(rts) PARTITION BY DAY");
            execute("INSERT INTO ins_rts SELECT l.ts lts, r.rts, r.c1" + join);
            assertQuery("SELECT lts, rts, c1 FROM ins_rts ORDER BY rts, lts")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("rts")
                    .returns(rows);
            execute("CREATE TABLE ins_rts0 (rts TIMESTAMP, lts TIMESTAMP, c1 INT) TIMESTAMP(rts) PARTITION BY DAY");
            execute("INSERT INTO ins_rts0 SELECT r.rts, l.ts lts, r.c1" + join);
            assertQuery("SELECT lts, rts, c1 FROM ins_rts0 ORDER BY rts, lts")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("rts")
                    .returns(rows);
            execute("CREATE TABLE ins_wal (lts TIMESTAMP, rts TIMESTAMP, c1 INT) TIMESTAMP(rts) PARTITION BY DAY WAL");
            execute("INSERT INTO ins_wal SELECT l.ts lts, r.rts, r.c1" + join);
            drainWalQueue();
            assertQuery("SELECT lts, rts, c1 FROM ins_wal ORDER BY rts, lts")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("rts")
                    .returns(rows);
            // LIMIT, a window function and an ASOF JOIN in the SELECT keep that plan as well, and none of
            // them changes which rows reach the target here
            final String[] selects = {
                    "SELECT l.ts lts, r.rts, r.c1" + join + " LIMIT 100",
                    "SELECT lts, rts, c1 FROM (SELECT l.ts lts, r.rts, r.c1, row_number() OVER () rn" + join + ") WHERE rn > 0",
                    "SELECT x.lts, x.rts, x.c1 FROM (SELECT l.ts lts, r.rts, r.c1" + join + ") x ASOF JOIN td d"
            };
            for (int i = 0; i < selects.length; i++) {
                execute("CREATE TABLE ins_rts" + (i + 1) + " (lts TIMESTAMP, rts TIMESTAMP, c1 INT) TIMESTAMP(rts) PARTITION BY DAY");
                execute("INSERT INTO ins_rts" + (i + 1) + " " + selects[i]);
                assertQuery("SELECT lts, rts, c1 FROM ins_rts" + (i + 1) + " ORDER BY rts, lts")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("rts")
                        .returns(rows);
            }
            // the sub-queries of the SELECT keep that plan as well. The join streams tb2's rows, so LIMIT 1, 2
            // takes the row of l.ts 02:10, and the filter keeps the rows of ta's second row
            execute("CREATE TABLE ins_scalar (lts TIMESTAMP, rts TIMESTAMP, c1 INT) TIMESTAMP(rts) PARTITION BY DAY");
            execute("INSERT INTO ins_scalar SELECT l.ts lts, r.rts, r.c1" + join + " WHERE l.ts >= (SELECT l.ts" + join + " LIMIT 1, 2)");
            assertQuery("SELECT * FROM ins_scalar")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("rts")
                    .returns("""
                            lts\trts\tc1
                            2020-01-01T02:10:00.000000Z\t2020-01-01T00:20:00.000000Z\t10
                            2020-01-01T02:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t20
                            2020-01-01T02:10:00.000000Z\t2020-01-01T03:20:00.000000Z\t30
                            """);
            // the ASOF JOIN in the IN set matches on r.rts, and the set {10, 20, 30} holds '10' and '20'
            // but not '7'
            execute("CREATE TABLE tsym (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tsym VALUES ('10', '2020-01-01T00:00'), ('20', '2020-01-01T01:00'), ('7', '2020-01-01T02:00')");
            execute("CREATE TABLE ins_in (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO ins_in SELECT * FROM tsym WHERE s IN (SELECT x.c1::SYMBOL FROM (SELECT l.ts, r.c1" + join + ") x ASOF JOIN td d)");
            assertQuery("SELECT * FROM ins_in")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            s\tts
                            10\t2020-01-01T00:00:00.000000Z
                            20\t2020-01-01T01:00:00.000000Z
                            """);
            // a target whose designated timestamp takes l.ts rejects the SELECT, which declares r.rts, as it
            // does on a revision that never keeps ta first
            execute("CREATE TABLE ins_lts (lts TIMESTAMP, rts TIMESTAMP, c1 INT) TIMESTAMP(lts) PARTITION BY DAY");
            assertExceptionNoLeakCheck(
                    "INSERT INTO ins_lts SELECT l.ts lts, r.rts, r.c1" + join,
                    12,
                    "designated timestamp of existing table (0) does not match designated timestamp in select query (1)"
            );
        });
    }

    @Test
    public void testFirstTableWithoutEqualityKeepsPlanOfInSubQuery() throws Exception {
        // The order of an IN set's rows cannot change the result, so the sub-query keeps the plan that
        // runs ta last.
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            execute("CREATE TABLE tsym (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tsym VALUES ('10', '2020-01-01T00:00'), ('20', '2020-01-01T01:00'), ('7', '2020-01-01T02:00')");
            final String subQuery = "(SELECT r.c1::SYMBOL FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts)";
            assertQuery("SELECT count() FROM tsym WHERE s IN " + subQuery)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            Count
                                Async Filter workers: 1
                                  filter: s in cursor\s
                                    VirtualRecord
                                      functions: [c1::symbol]
                                        SelectedRecord
                                            Cross Join
                                                Hash Join Light
                                                  condition: n.cts=r.rts
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: tb2
                                                    Hash
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: tc2
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: ta [state-shared]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tsym
                            """)
                    .returns("""
                            count
                            2
                            """);
            assertQuery("SELECT count() FROM tsym WHERE s NOT IN " + subQuery)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            Count
                                Async Filter workers: 1
                                  filter: not (s in cursor\s
                                    VirtualRecord
                                      functions: [c1::symbol]
                                        SelectedRecord
                                            Cross Join
                                                Hash Join Light
                                                  condition: n.cts=r.rts
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: tb2
                                                    Hash
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: tc2
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: ta [state-shared])
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tsym
                            """)
                    .returns("""
                            count
                            1
                            """);
            // the set of an IN predicate in a GROUP BY or ORDER BY expression keeps the plan as well:
            // '10' and '20' are in the set, '7' is not
            assertQuery("SELECT count() c FROM tsym GROUP BY s IN " + subQuery + " ORDER BY c")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort light
                              keys: [c]
                                VirtualRecord
                                  functions: [c]
                                    Async Group By workers: 1
                                      keys: [column]
                                      keyFunctions: [s in cursor\s
                                        VirtualRecord
                                          functions: [c1::symbol]
                                            SelectedRecord
                                                Cross Join
                                                    Hash Join Light
                                                      condition: n.cts=r.rts
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: tb2
                                                        Hash
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: tc2
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: ta [state-shared]]
                                      values: [count(*)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tsym
                            """)
                    .returns("""
                            c
                            1
                            2
                            """);
            // the same set in a column that the GROUP BY names by its alias
            assertQuery("SELECT s IN " + subQuery + " k, count() c FROM tsym GROUP BY k ORDER BY k")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort light
                              keys: [k]
                                VirtualRecord
                                  functions: [k,c]
                                    Async Group By workers: 1
                                      keys: [k]
                                      keyFunctions: [s in cursor\s
                                        VirtualRecord
                                          functions: [c1::symbol]
                                            SelectedRecord
                                                Cross Join
                                                    Hash Join Light
                                                      condition: n.cts=r.rts
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: tb2
                                                        Hash
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: tc2
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: ta [state-shared]]
                                      values: [count(*)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tsym
                            """)
                    .returns("""
                            k\tc
                            false\t1
                            true\t2
                            """);
            assertQuery("SELECT s FROM tsym ORDER BY s IN " + subQuery + ", s")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [column, s]
                                    VirtualRecord
                                      functions: [s,s in cursor\s
                                        VirtualRecord
                                          functions: [c1::symbol]
                                            SelectedRecord
                                                Cross Join
                                                    Hash Join Light
                                                      condition: n.cts=r.rts
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: tb2
                                                        Hash
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: tc2
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: ta]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tsym
                            """)
                    .returns("""
                            s
                            7
                            10
                            20
                            """);
        });
    }

    @Test
    public void testFirstTableWithoutEqualityKeepsPlanWhenOrderIsUnobserved() throws Exception {
        // Where nothing reads the order of the join's rows, the plan keeps the cross join with ta last,
        // which multiplies fewer rows than a chain that starts at ta.
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            final String from = " FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts";
            assertQuery("SELECT count() c, sum(r.c1) s" + from)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            GroupBy vectorized: false
                              values: [count(*),sum(c1)]
                                SelectedRecord
                                    Cross Join
                                        Hash Join Light
                                          condition: n.cts=r.rts
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tb2
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tc2
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: ta
                            """)
                    .returns("""
                            c\ts
                            6\t120
                            """);
            assertQuery("SELECT l.v, count() c" + from + " ORDER BY l.v")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort light
                              keys: [v]
                                GroupBy vectorized: false
                                  keys: [v]
                                  values: [count(*)]
                                    SelectedRecord
                                        Cross Join
                                            Hash Join Light
                                              condition: n.cts=r.rts
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tb2
                                                Hash
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: tc2
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: ta
                            """)
                    .returns("""
                            v\tc
                            1\t3
                            9\t3
                            """);
            // SAMPLE BY with the calendar alignment becomes a GROUP BY over timestamp_floor(l.ts)
            assertQuery("SELECT count() c, sum(r.c1) s" + from + " SAMPLE BY 1h")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [ts]
                                    GroupBy vectorized: false
                                      keys: [ts]
                                      values: [count(*),sum(c1)]
                                        SelectedRecord
                                            Cross Join
                                                Hash Join Light
                                                  condition: n.cts=r.rts
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: tb2
                                                    Hash
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: tc2
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: ta
                            """)
                    .returns("""
                            c\ts
                            3\t60
                            3\t60
                            """);
            assertQuery("SELECT l.v, r.c1" + from + " ORDER BY r.c1, l.v")
                    .noLeakCheck()
                    .withPlan("""
                            Encode sort
                              keys: [c1, v]
                                SelectedRecord
                                    Cross Join
                                        Hash Join Light
                                          condition: n.cts=r.rts
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tb2
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tc2
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: ta
                            """)
                    .returns("""
                            v\tc1
                            1\t10
                            9\t10
                            1\t20
                            9\t20
                            1\t30
                            9\t30
                            """);
        });
    }

    @Test
    public void testFirstTableWithoutEqualityLimitFollowsItsOrder() throws Exception {
        // the first item of the last query lists ta's rows in reverse, ORDER BY v DESC
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            final String from = " FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts";
            assertQuery("SELECT l.ts lts, r.rts, r.c1" + from + " LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("lts")
                    .returns("""
                            lts\trts\tc1
                            2020-01-01T02:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t20
                            2020-01-01T02:10:00.000000Z\t2020-01-01T03:20:00.000000Z\t30
                            """);
            assertQuery("SELECT l.ts lts, r.rts, r.c1" + from + " LIMIT 2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("lts")
                    .returns("""
                            lts\trts\tc1
                            2020-01-01T00:10:00.000000Z\t2020-01-01T00:20:00.000000Z\t10
                            2020-01-01T00:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t20
                            """);
            assertQuery("WITH l AS (SELECT * FROM ta) SELECT l.ts lts, r.rts FROM l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("lts")
                    .returns("""
                            lts\trts
                            2020-01-01T02:10:00.000000Z\t2020-01-01T01:20:00.000000Z
                            2020-01-01T02:10:00.000000Z\t2020-01-01T03:20:00.000000Z
                            """);
            assertQuery("SELECT l.v, r.c1 FROM (SELECT * FROM ta ORDER BY v DESC) l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            v\tc1
                            1\t20
                            1\t30
                            """);
        });
    }

    @Test
    public void testFirstTableWithoutEqualitySampleByAlignedToFirstObservation() throws Exception {
        // this SAMPLE BY does not become a GROUP BY: it buckets the join's rows on the join's designated
        // timestamp, in the order of the rows, so the buckets start at l.ts 00:10 and 02:10
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            final String buckets = """
                    c\ts
                    3\t60
                    3\t60
                    """;
            for (String from : new String[]{
                    " FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts",
                    " FROM ta l, tb2 r, tc2 n WHERE n.cts = r.rts",
                    " FROM (SELECT * FROM ta) l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts"
            }) {
                assertQuery("SELECT count() c, sum(r.c1) s" + from + " SAMPLE BY 1h ALIGN TO FIRST OBSERVATION")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns(buckets);
            }
            assertQuery("SELECT count() c, sum(c1) s FROM (SELECT l.ts, r.c1 FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts)"
                    + " TIMESTAMP(ts) SAMPLE BY 1h ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(buckets);
            assertQuery("SELECT l.ts, count() c, sum(r.c1) s FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts"
                    + " SAMPLE BY 1h ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tc\ts
                            2020-01-01T00:10:00.000000Z\t3\t60
                            2020-01-01T02:10:00.000000Z\t3\t60
                            """);
        });
    }

    @Test
    public void testFirstTableWithoutEqualitySampleByFillLinear() throws Exception {
        // SAMPLE BY FILL(LINEAR) does not become a GROUP BY either: the l.ts buckets are the calendar
        // hours 00:00 and 02:00, and the fill interpolates 01:00 between its equal neighbours
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            final String from = " FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts";
            assertQuery("SELECT count() c, sum(r.c1) s" + from + " SAMPLE BY 1h FILL(LINEAR)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            c\ts
                            3\t60
                            3\t60
                            3\t60
                            """);
            assertQuery("SELECT l.ts, count() c, sum(r.c1) s" + from + " SAMPLE BY 1h FILL(LINEAR)")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tc\ts
                            2020-01-01T00:00:00.000000Z\t3\t60
                            2020-01-01T01:00:00.000000Z\t3\t60
                            2020-01-01T02:00:00.000000Z\t3\t60
                            """);
        });
    }

    @Test
    public void testFirstTableWithoutEqualityWindowFunctionFollowsItsOrder() throws Exception {
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            assertQuery("SELECT l.ts lts, r.rts, row_number() OVER () rn FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("lts")
                    .returns("""
                            lts\trts\trn
                            2020-01-01T00:10:00.000000Z\t2020-01-01T00:20:00.000000Z\t1
                            2020-01-01T00:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t2
                            2020-01-01T00:10:00.000000Z\t2020-01-01T03:20:00.000000Z\t3
                            2020-01-01T02:10:00.000000Z\t2020-01-01T00:20:00.000000Z\t4
                            2020-01-01T02:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t5
                            2020-01-01T02:10:00.000000Z\t2020-01-01T03:20:00.000000Z\t6
                            """);
        });
    }

    @Test
    public void testFirstTableWithoutEqualityWindowJoinMatchesItsTimestamp() throws Exception {
        // the window around 00:10 holds td's 00:00 and 00:15; the one around 02:10 holds 01:30, 02:00
        // and 03:00, plus 00:15 as the prevailing row
        assertMemoryLeak(() -> {
            createFirstTableWithoutEqualityTables();
            assertQuery("SELECT x.ts, x.rts, sum(d.v) s FROM (SELECT l.ts, r.rts FROM ta l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts) x"
                    + " WINDOW JOIN td d RANGE BETWEEN 1 hour PRECEDING AND 1 hour FOLLOWING")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\trts\ts
                            2020-01-01T00:10:00.000000Z\t2020-01-01T00:20:00.000000Z\t3
                            2020-01-01T00:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t3
                            2020-01-01T00:10:00.000000Z\t2020-01-01T03:20:00.000000Z\t3
                            2020-01-01T02:10:00.000000Z\t2020-01-01T00:20:00.000000Z\t14
                            2020-01-01T02:10:00.000000Z\t2020-01-01T01:20:00.000000Z\t14
                            2020-01-01T02:10:00.000000Z\t2020-01-01T03:20:00.000000Z\t14
                            """);
        });
    }

    @Test
    public void testFullFatTemporalJoinRejectsIntervalSlaveColumn() throws Exception {
        // A full-fat ASOF/LT join materializes the slave's non-key columns into the map
        // value via RecordValueSinkFactory. MapValue cannot hold an INTERVAL (there is no
        // putInterval), and INTERVAL is not comparable so it is never a join key, so a
        // projected interval slave column reached the factory's default branch and threw a
        // bare UnsupportedOperationException at compile time. The full-fat guard now rejects
        // any slave column the value sink cannot store up front with a user-facing
        // SqlException, matching the graceful array rejection. INTERVAL is expression-only
        // (no stored column), so an interval slave column only comes from a sub-query
        // projection with no random access - always full-fat, always rejected (unlike a
        // var-size array, which still has a light path that returns the row).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s (k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES (1, '1970-01-01T00:00:00.000002Z')");
            execute("INSERT INTO s VALUES (1, '1970-01-01T00:00:00.000001Z')");

            for (String join : new String[]{"LT", "ASOF"}) {
                final String sql = "SELECT a.ts, b.iv FROM m a " + join
                        + " JOIN (SELECT k, ts, rnd_interval() iv FROM s) b ON a.k = b.k";
                // The position points at the join keyword; the prefix "SELECT a.ts, b.iv
                // FROM m a " is identical for LT and ASOF, so the keyword starts at 27.
                assertExceptionNoLeakCheck(sql, 27, "right side column 'iv' is of unsupported type", true);
            }
        });
    }

    @Test
    public void testFullFatTemporalJoinSinksUuidAndLong256SlaveColumns() throws Exception {
        // A full-fat ASOF/LT join materializes the slave's non-key columns into the map
        // value via RecordValueSinkFactory. That factory had no case for UUID/LONG128 or
        // LONG256, so its default branch threw a bare UnsupportedOperationException at
        // compile time - even though MapValue can store both (putLong128/putLong256) and
        // the light (random-access) path reads them straight from the slave cursor. The
        // value sink now handles them, so the full-fat path returns the same rows as the
        // light path. A var-size slave column (e.g. an array) still rejects with a
        // user-facing SqlException, since the map value cannot hold a variable-length type.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (k INT, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s (k INT, sym SYMBOL, u UUID, l LONG256, arr DOUBLE[], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES " +
                    "(1, 'a', '1970-01-01T00:00:00.000002Z'), " +
                    "(1, 'a', '1970-01-01T00:00:00.000004Z'), " +
                    "(2, 'a', '1970-01-01T00:00:00.000006Z')");
            execute("INSERT INTO s VALUES " +
                    "(1, 'a', '11111111-1111-1111-1111-111111111111', '0x01', ARRAY[1.0], '1970-01-01T00:00:00.000001Z'), " +
                    "(1, 'a', '22222222-2222-2222-2222-222222222222', '0x02', ARRAY[2.0], '1970-01-01T00:00:00.000003Z')");

            // The last master row (k=2) has no slave key, so the map lookup misses and both the UUID
            // and LONG256 must NULL-extend - the sentinel path most likely to mishandle these
            // fixed-size types. The full-fat and light paths must still agree on it.
            final String expected = "ts\tu\tl\n" +
                    "1970-01-01T00:00:00.000002Z\t11111111-1111-1111-1111-111111111111\t0x01\n" +
                    "1970-01-01T00:00:00.000004Z\t22222222-2222-2222-2222-222222222222\t0x02\n" +
                    "1970-01-01T00:00:00.000006Z\t\t\n";

            for (String join : new String[]{"LT", "ASOF"}) {
                final String sql = "SELECT a.ts, b.u, b.l FROM m a " + join + " JOIN s b ON a.k = b.k AND a.sym = b.sym";
                // Full-fat path: the slave UUID and LONG256 are sunk into the map value
                // (the path the storage shadow takes when the slave has no random access).
                assertQuery(sql)
                        .noLeakCheck()
                        .fullFatJoins()
                        .timestamp("ts")
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
                // Light path: the same rows read from the slave cursor.
                assertQuery(sql)
                        .noLeakCheck()
                        .timestamp("ts")
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);

                // A var-size slave column (the array) still cannot enter the map value, so
                // the full-fat path rejects it with a user-facing SqlException rather than
                // a bare runtime exception.
                final String arraySql = "SELECT a.ts, b.arr FROM m a " + join + " JOIN s b ON a.k = b.k AND a.sym = b.sym";
                // The position points at the join keyword; the prefix "SELECT a.ts, b.arr
                // FROM m a " is identical for LT and ASOF, so the keyword starts at 28.
                assertExceptionNoLeakCheck(arraySql, 28, "right side column 'arr' is of unsupported type", true);
            }
        });
    }

    @Test
    public void testFullJoinChainKeepsUnmatchedRowsOfSharedKey() throws Exception {
        // t0 has fewer rows than t1, so the first FULL join hashes t0 on t0.a, and no t0 row passes
        // its filter. Only the t0 row (null, 1, null) matches t4 rows, and it shares its NULL key with
        // (null, null, 1), so the join must emit every unmatched t0 row of a key, not the first one.
        assertQuery("""
                SELECT t0.a c0a, t0.b c0b, t0.c c0c, t1.a c1a, t2.a c2a, t3.b c3b, t4.a c4a, t4.b c4b, t4.c c4c
                FROM t0
                FULL JOIN t1 ON t1.c > t0.b AND t1.c = t0.a AND t1.c > t0.a
                FULL JOIN t2 ON t2.c > t0.c AND t2.b > t1.a AND t2.a = t0.b
                FULL JOIN t3 ON t0.b = t3.b AND t3.b = t1.a
                RIGHT JOIN t4 ON t4.c > t0.b
                ORDER BY c4a, c4b, c4c, c0a
                """)
                .ddl(
                        "CREATE TABLE t0 (a INT, b INT, c INT)",
                        "CREATE TABLE t1 (a INT, b INT, c INT)",
                        "CREATE TABLE t2 (a INT, b INT, c INT)",
                        "CREATE TABLE t3 (a INT, b INT, c INT)",
                        "CREATE TABLE t4 (a INT, b INT, c INT)",
                        """
                                INSERT INTO t0 VALUES
                                    (2, null, null),
                                    (2, 2, 2),
                                    (null, 1, null),
                                    (null, null, 1)
                                """,
                        """
                                INSERT INTO t1 VALUES
                                    (3, 2, 2),
                                    (null, 2, 1),
                                    (2, 1, 2),
                                    (null, 3, null),
                                    (2, null, null)
                                """,
                        """
                                INSERT INTO t2 VALUES
                                    (2, 2, 2),
                                    (1, 1, 2),
                                    (3, null, null),
                                    (null, 3, 1),
                                    (2, 2, 1)
                                """,
                        """
                                INSERT INTO t3 VALUES
                                    (null, 3, 1),
                                    (2, 2, 2),
                                    (1, 3, 2)
                                """,
                        """
                                INSERT INTO t4 VALUES
                                    (2, 2, 2),
                                    (null, 2, 3),
                                    (2, 3, 2)
                                """
                )
                .returns("""
                        c0a\tc0b\tc0c\tc1a\tc2a\tc3b\tc4a\tc4b\tc4c
                        null\t1\tnull\tnull\tnull\tnull\tnull\t2\t3
                        2\t2\t2\tnull\tnull\tnull\tnull\t2\t3
                        null\t1\tnull\tnull\tnull\tnull\t2\t2\t2
                        null\t1\tnull\tnull\tnull\tnull\t2\t3\t2
                        """);
    }

    @Test
    public void testFullJoinFilterKeepsUnmatchedRowsOfSharedKey() throws Exception {
        // A FULL join with a filter emits its unmatched build rows after the probe. The light join
        // builds on the input with fewer rows, the left one in "t0 FULL JOIN t1", and must emit every
        // unmatched row of a key, NULL keys included.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("CREATE TABLE n0 (a INT, b INT)");
            execute("CREATE TABLE t1 (a INT, c INT)");
            execute("CREATE TABLE t2 (b INT)");
            execute("INSERT INTO t0 VALUES (7, 1), (7, 2)");
            execute("INSERT INTO n0 VALUES (null, 1), (null, 2)");
            execute("INSERT INTO t1 VALUES (1, 1), (2, 2), (3, 3)");
            execute("INSERT INTO t2 VALUES (5)");

            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String from : new String[]{"t0 FULL JOIN t1 ON t1.a = t0.a", "t1 FULL JOIN t0 ON t0.a = t1.a"}) {
                    assertQuery("SELECT t0.a c0a, t0.b c0b, t1.a c1a, t1.c c1c FROM " + from + " AND t1.c > t0.b ORDER BY c0a, c0b, c1a")
                            .noLeakCheck()
                            .fullFatJoins(isFullFatJoin)
                            .returns("""
                                    c0a\tc0b\tc1a\tc1c
                                    null\tnull\t1\t1
                                    null\tnull\t2\t2
                                    null\tnull\t3\t3
                                    7\t1\tnull\tnull
                                    7\t2\tnull\tnull
                                    """);
                }
                assertQuery("SELECT n0.a c0a, n0.b c0b, t1.a c1a, t1.c c1c FROM n0 FULL JOIN t1 ON t1.a = n0.a AND t1.c > n0.b ORDER BY c0b, c1a")
                        .noLeakCheck()
                        .fullFatJoins(isFullFatJoin)
                        .returns("""
                                c0a\tc0b\tc1a\tc1c
                                null\tnull\t1\t1
                                null\tnull\t2\t2
                                null\tnull\t3\t3
                                null\t1\tnull\tnull
                                null\t2\tnull\tnull
                                """);
                // t2 keeps both of its equalities, so the t0 FULL JOIN t1 join keeps the single key t1.a = t0.a
                assertQuery("""
                        SELECT t0.a c0a, t0.b c0b, t1.a c1a, t1.c c1c, t2.b c2b
                        FROM t0
                        FULL JOIN t1 ON t1.a = t0.a AND t1.c > t0.b
                        FULL JOIN t2 ON t0.b = t2.b AND t2.b = t1.c
                        ORDER BY c0a, c0b, c1a
                        """)
                        .noLeakCheck()
                        .fullFatJoins(isFullFatJoin)
                        .returns("""
                                c0a\tc0b\tc1a\tc1c\tc2b
                                null\tnull\tnull\tnull\t5
                                null\tnull\t1\t1\tnull
                                null\tnull\t2\t2\tnull
                                null\tnull\t3\t3\tnull
                                7\t1\tnull\tnull\tnull
                                7\t2\tnull\tnull\tnull
                                """);
            }
        });
    }

    @Test
    public void testFullJoinFilterSkipsMatchedRowsOfSharedKey() throws Exception {
        // t0 has fewer rows than t1, so the light FULL join builds on t0 and probes t1. The probe row
        // (7, 3) matches t0's (7, 2) and (7, 1) but not (7, 6) or (7, 5), which share their key. After
        // the probe, the join emits exactly the unmatched t0 rows, whatever the filter returns for a
        // NULL t1 side. The second pass of returns() rewinds the join through toTop().
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT)");
            execute("CREATE TABLE t1 (a INT, c INT)");
            execute("INSERT INTO t0 VALUES (7, 5), (7, 1), (7, 6), (7, 2)");
            execute("INSERT INTO t1 VALUES (7, 3), (1, 1), (2, 2), (3, 3), (4, 4)");

            for (String filter : new String[]{"t1.c > t0.b", "(t1.c > t0.b OR t1.c IS NULL)"}) {
                assertQuery("SELECT t0.a c0a, t0.b c0b, t1.a c1a, t1.c c1c FROM t0 FULL JOIN t1 ON t1.a = t0.a AND " + filter)
                        .noLeakCheck()
                        .noRandomAccess()
                        .withPlanContaining("Hash Full Outer Join Light")
                        .returns("""
                                c0a\tc0b\tc1a\tc1c
                                7\t2\t7\t3
                                7\t1\t7\t3
                                null\tnull\t1\t1
                                null\tnull\t2\t2
                                null\tnull\t3\t3
                                null\tnull\t4\t4
                                7\t6\tnull\tnull
                                7\t5\tnull\tnull
                                """);
            }
        });
    }

    @Test
    public void testFullJoinOnRegexPreservesBothUnmatchedSides() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x STRING)");
            execute("INSERT INTO a VALUES ('a')");
            execute("CREATE TABLE b (x STRING)");
            execute("INSERT INTO b VALUES ('a')");
            execute("CREATE TABLE c (x STRING)");
            execute("INSERT INTO c VALUES ('z')");

            final String fullJoinSql = """
                    SELECT a.x AS ax, b.x AS bx, c.x AS cx
                    FROM a
                    JOIN b ON b.x = a.x
                    FULL JOIN c ON c.x = a.x AND a.x ~ '^z$'
                    ORDER BY c.x, a.x
                    """;
            final String fullJoinExpected = """
                    ax\tbx\tcx
                    a\ta\t
                    \t\tz
                    """;
            final String rightJoinSql = fullJoinSql.replace("FULL JOIN", "RIGHT JOIN");
            final String rightJoinExpected = "ax\tbx\tcx\n\t\tz\n";
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                assertQuery(fullJoinSql)
                        .noLeakCheck()
                        .fullFatJoins(isFullFatJoin)
                        .returns(fullJoinExpected);
                assertQuery(rightJoinSql)
                        .noLeakCheck()
                        .fullFatJoins(isFullFatJoin)
                        .returns(rightJoinExpected);
            }
        });
    }

    @Test
    public void testHashJoinLightdNoLeaks() throws Exception {
        testJoinForCursorLeaks("with crj as (select * from xx latest by x) select xx.x from xx join crj on xx.x = crj.x ", false);
    }

    @Test
    public void testHashJoinOfSmallerMasterChainKeepsTimestampOrder() throws Exception {
        // x0 has fewer rows than x1, so the first join would build its hash table on x0 and stream x1.
        // The chain then emits rows in x1's order while it claims x0's timestamp. The projection casts
        // x0.ts: a designated timestamp makes the assertion ask for the scan direction first, and that
        // call alone keeps the join from swapping its inputs.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT, a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (k INT, a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t3 (k INT, a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (0, 1, 1, '1970-01-01T01:00:00Z'),
                    (2, 2, NULL, '1970-01-01T02:00:00Z'),
                    (1, 2, NULL, '1970-01-01T04:00:00Z'),
                    (1, 1, 2, '1970-01-01T04:00:00Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (0, 1, 1, '1970-01-01T02:00:00Z'),
                    (2, 2, 2, '1970-01-01T03:00:00Z'),
                    (1, 2, 1, '1970-01-01T03:00:00Z'),
                    (2, 1, 2, '1970-01-01T05:00:00Z'),
                    (NULL, 0, 1, '1970-01-01T05:00:00Z'),
                    (0, 2, 0, '1970-01-01T05:00:00Z')
                    """);
            execute("""
                    INSERT INTO t3 VALUES
                    (0, 1, 2, '1970-01-01T01:00:00Z'),
                    (0, 1, 1, '1970-01-01T02:00:00Z'),
                    (1, 2, 1, '1970-01-01T03:00:00Z')
                    """);

            final String query = """
                    SELECT x0.ts::STRING ts, x0.k, x0.a, x0.b, x1.k, x1.a, x1.b, x2.k, x2.a, x2.b, x3.k, x3.a, x3.b
                    FROM t3 x0
                    JOIN t2 x1 ON x1.k = x0.k
                    JOIN t2 x2 ON x2.k = 2 AND x2.a = x0.b
                    LEFT JOIN t1 x3 ON x3.a = x0.k AND x3.a = 2 AND x3.a = x1.b
                    """;
            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts\tk\ta\tb\tk1\ta1\tb1\tk2\ta2\tb2\tk3\ta3\tb3
                            1970-01-01T01:00:00.000000Z\t0\t1\t2\t0\t2\t0\t2\t2\t2\tnull\tnull\tnull
                            1970-01-01T01:00:00.000000Z\t0\t1\t2\t0\t1\t1\t2\t2\t2\tnull\tnull\tnull
                            1970-01-01T02:00:00.000000Z\t0\t1\t1\t0\t2\t0\t2\t1\t2\tnull\tnull\tnull
                            1970-01-01T02:00:00.000000Z\t0\t1\t1\t0\t1\t1\t2\t1\t2\tnull\tnull\tnull
                            1970-01-01T03:00:00.000000Z\t1\t2\t1\t1\t2\t1\t2\t1\t2\tnull\tnull\tnull
                            """);
            assertQuery(query + " LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tk\ta\tb\tk1\ta1\tb1\tk2\ta2\tb2\tk3\ta3\tb3
                            1970-01-01T02:00:00.000000Z\t0\t1\t1\t0\t1\t1\t2\t1\t2\tnull\tnull\tnull
                            1970-01-01T03:00:00.000000Z\t1\t2\t1\t1\t2\t1\t2\t1\t2\tnull\tnull\tnull
                            """);
        });
    }

    @Test
    public void testHashJoinOfSmallerMasterFilteredChainKeepsTimestampOrder() throws Exception {
        // x0 has fewer rows than x1. LIMIT 3 must return the first three rows by x0.ts, all at 00:00.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (k INT, a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t1 (k INT, a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (k INT, a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t0 VALUES
                    (NULL, 2, 0, '1970-01-01T00:00:00Z'),
                    (1, 2, 2, '1970-01-01T01:00:00Z'),
                    (2, NULL, 1, '1970-01-01T03:00:00Z'),
                    (1, 1, 2, '1970-01-01T04:00:00Z'),
                    (1, NULL, 0, '1970-01-01T06:00:00Z'),
                    (2, 2, NULL, '1970-01-01T06:00:00Z')
                    """);
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 1, 1, '1970-01-01T00:00:00Z'),
                    (0, 1, 0, '1970-01-01T00:00:00Z'),
                    (0, NULL, 0, '1970-01-01T02:00:00Z'),
                    (2, 2, 2, '1970-01-01T03:00:00Z'),
                    (1, 1, 0, '1970-01-01T05:00:00Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (NULL, 1, 2, '1970-01-01T01:00:00Z'),
                    (1, 1, 1, '1970-01-01T01:00:00Z'),
                    (0, 1, 1, '1970-01-01T01:00:00Z'),
                    (NULL, 2, 0, '1970-01-01T03:00:00Z'),
                    (1, 2, 0, '1970-01-01T05:00:00Z'),
                    (NULL, 2, 0, '1970-01-01T05:00:00Z')
                    """);

            assertQuery("""
                    SELECT x0.ts::STRING ts, x0.k, x0.a, x0.b, x1.k, x1.a, x1.b, x2.k, x2.a, x2.b, x3.k, x3.a, x3.b
                    FROM t1 x0
                    JOIN t0 x1 ON x1.a = x0.k
                    JOIN t0 x2 ON x2.b = x1.b
                    LEFT JOIN t2 x3 ON x3.k = x0.k AND x3.k = x0.a
                    WHERE x0.k = x3.k
                    LIMIT 3
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts\tk\ta\tb\tk1\ta1\tb1\tk2\ta2\tb2\tk3\ta3\tb3
                            1970-01-01T00:00:00.000000Z\t1\t1\t1\t1\t1\t2\t1\t1\t2\t1\t2\t0
                            1970-01-01T00:00:00.000000Z\t1\t1\t1\t1\t1\t2\t1\t1\t2\t1\t1\t1
                            1970-01-01T00:00:00.000000Z\t1\t1\t1\t1\t1\t2\t1\t2\t2\t1\t2\t0
                            """);
        });
    }

    @Test
    public void testHashJoinOfSmallerMasterKeepsTimestampOrder() throws Exception {
        // a has fewer rows than b, so the join would build its hash table on a and stream b. The output
        // would then follow b's order while the join claims a's designated timestamp, and LIMIT would
        // pick the wrong rows. Most projections cast a.ts: a designated timestamp makes the assertion
        // ask for the scan direction first, and that call alone keeps the join from swapping its inputs.
        // The master key 5 has no slave row, and the slave keys 4 and 6 have no master row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE b (k INT, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO a VALUES
                    (1, '1970-01-01T01:00:00Z'),
                    (NULL, '1970-01-01T01:30:00Z'),
                    (2, '1970-01-01T02:00:00Z'),
                    (5, '1970-01-01T02:30:00Z'),
                    (3, '1970-01-01T03:00:00Z')
                    """);
            execute("""
                    INSERT INTO b VALUES
                    (3, 30, '1970-01-01T00:00:00Z'),
                    (2, 20, '1970-01-01T01:00:00Z'),
                    (NULL, 50, '1970-01-01T01:30:00Z'),
                    (1, 10, '1970-01-01T02:00:00Z'),
                    (4, 40, '1970-01-01T03:00:00Z'),
                    (6, 60, '1970-01-01T04:00:00Z')
                    """);

            final String join = " FROM a JOIN b ON a.k = b.k";
            final String expected = """
                    ts\tk\tv
                    1970-01-01T01:00:00.000000Z\t1\t10
                    1970-01-01T01:30:00.000000Z\tnull\t50
                    1970-01-01T02:00:00.000000Z\t2\t20
                    1970-01-01T03:00:00.000000Z\t3\t30
                    """;
            // the full-fat join never swaps its inputs, so it pins the same result
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                assertQuery("SELECT a.ts::STRING ts, a.k, b.v" + join)
                        .noLeakCheck()
                        .fullFatJoins(isFullFatJoin)
                        .noRandomAccess()
                        .expectSize(isFullFatJoin)
                        .returns(expected);
            }
            assertQuery("SELECT a.ts::STRING ts, a.k, b.v" + join + " LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tk\tv
                            1970-01-01T02:00:00.000000Z\t2\t20
                            1970-01-01T03:00:00.000000Z\t3\t30
                            """);
            assertQuery("SELECT a.ts::STRING ts, a.k, b.v" + join + " LIMIT 1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts\tk\tv
                            1970-01-01T01:00:00.000000Z\t1\t10
                            """);
            assertQuery("WITH j AS (SELECT a.ts, a.k, b.v" + join + ") SELECT ts::STRING ts, k, v FROM j LIMIT -1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tk\tv
                            1970-01-01T03:00:00.000000Z\t3\t30
                            """);
            assertQuery("SELECT a.ts::STRING ts, a.k, b.v, row_number() OVER () rn" + join)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts\tk\tv\trn
                            1970-01-01T01:00:00.000000Z\t1\t10\t1
                            1970-01-01T01:30:00.000000Z\tnull\t50\t2
                            1970-01-01T02:00:00.000000Z\t2\t20\t3
                            1970-01-01T03:00:00.000000Z\t3\t30\t4
                            """);
            assertQuery("SELECT a.ts, a.k, b.v" + join + " LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tk\tv
                            1970-01-01T02:00:00.000000Z\t2\t20
                            1970-01-01T03:00:00.000000Z\t3\t30
                            """);
        });
    }

    @Test
    public void testHashJoinOfSmallerMasterKeepsTimestampOrderOnSymbolKey() throws Exception {
        // The symbol key join translates b's symbol keys into a's. The master key q has no slave row,
        // and the slave keys z and w have no master row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE b (s SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO a VALUES
                    ('x', '1970-01-01T01:00:00Z'),
                    (NULL, '1970-01-01T01:30:00Z'),
                    ('y', '1970-01-01T02:00:00Z'),
                    ('q', '1970-01-01T02:30:00Z')
                    """);
            execute("""
                    INSERT INTO b VALUES
                    ('z', 1, '1970-01-01T00:00:00Z'),
                    ('y', 2, '1970-01-01T00:30:00Z'),
                    (NULL, 3, '1970-01-01T01:00:00Z'),
                    ('x', 4, '1970-01-01T02:00:00Z'),
                    ('w', 5, '1970-01-01T03:00:00Z')
                    """);

            // the full-fat join never swaps its inputs, so it pins the same result
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                assertQuery("SELECT a.ts::STRING ts, a.s, b.v FROM a JOIN b ON a.s = b.s")
                        .noLeakCheck()
                        .fullFatJoins(isFullFatJoin)
                        .noRandomAccess()
                        .expectSize(isFullFatJoin)
                        .returns("""
                                ts\ts\tv
                                1970-01-01T01:00:00.000000Z\tx\t4
                                1970-01-01T01:30:00.000000Z\t\t3
                                1970-01-01T02:00:00.000000Z\ty\t2
                                """);
            }
            assertQuery("SELECT a.ts::STRING ts, a.s, b.v FROM a JOIN b ON a.s = b.s LIMIT 1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Hash Join Light", "symbolKeyJoin: true")
                    .returns("""
                            ts\ts\tv
                            1970-01-01T01:00:00.000000Z\tx\t4
                            """);
        });
    }

    @Test
    public void testHashJoinRecordNoLeaks() throws Exception {
        testJoinForCursorLeaks("with crj as (select first(x) x, first(ts) ts from xx latest by x) select xx.x from xx join crj on xx.x = crj.x ", false);
    }

    @Test
    public void testImpliedEqualityFilterOnSubQueryKeepsJoinKey() throws Exception {
        // c's ON clause implies b.k = b.y, a filter of b. When b is a sub-query or a view, the filter
        // moves into it and takes the sub-query's own column names. c's join key keeps one of the two
        // equalities, and it must still refer to b's column: a bare y resolves to a.y or fails to
        // resolve.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (k INT, x INT, y INT, v INT)");
            execute("CREATE TABLE tb (k INT, x INT, y INT, v INT)");
            execute("CREATE TABLE tc (k INT, v INT)");
            execute("""
                    INSERT INTO ta VALUES
                        (1, 0, 1, 101),
                        (2, 0, 2, 102),
                        (null, 1, null, 103),
                        (3, 1, 1, 104),
                        (2, 1, 2, 105)
                    """);
            execute("""
                    INSERT INTO tb VALUES
                        (1, 0, 1, 201),
                        (2, 1, 0, 202),
                        (null, 1, null, 203),
                        (2, 0, 2, 204),
                        (3, 1, 3, 205)
                    """);
            execute("""
                    INSERT INTO tc VALUES
                        (1, 301),
                        (2, 302),
                        (null, 303),
                        (3, 304),
                        (7, 305)
                    """);
            execute("CREATE VIEW vb AS (SELECT k, x, y, v FROM tb)");

            final String innerJoinRows = """
                    av\tbv\tcv
                    101\t201\t301
                    102\t201\t301
                    101\t204\t302
                    102\t204\t302
                    103\t203\t303
                    104\t203\t303
                    105\t203\t303
                    103\t205\t304
                    104\t205\t304
                    105\t205\t304
                    """;
            for (String[] b : new String[][]{
                    {"(SELECT k, x, y, v FROM tb)", "c.k = b.k AND c.k = b.y", "condition: c.k=b.y"},
                    {"vb", "c.k = b.k AND c.k = b.y", "condition: c.k=b.y"},
                    {"(SELECT * FROM tb)", "c.k = b.k AND c.k = b.y", "condition: c.k=b.y"},
                    {"(SELECT k, x, y, max(v) v FROM tb GROUP BY k, x, y)", "c.k = b.k AND c.k = b.y", "condition: c.k=b.y"},
                    {"(SELECT k kk, x, y yy, v FROM tb)", "c.k = b.kk AND c.k = b.yy", "condition: c.k=b.yy"}
            }) {
                // the RIGHT join also returns c's row 305, which no (a, b) pair matches
                assertQuery("SELECT a.v av, b.v bv, c.v cv FROM ta a JOIN " + b[0] + " b ON a.x = b.x RIGHT JOIN tc c ON " + b[1] + " ORDER BY cv, av, bv")
                        .noLeakCheck()
                        .withPlanContaining(b[2], "filter: k=y")
                        .returns(innerJoinRows + "null\tnull\t305\n");
                assertQuery("SELECT a.v av, b.v bv, c.v cv FROM ta a JOIN " + b[0] + " b ON a.x = b.x JOIN tc c ON " + b[1] + " ORDER BY cv, av, bv")
                        .noLeakCheck()
                        .withPlanContaining(b[2], "filter: k=y")
                        .returns(innerJoinRows);
            }

            assertQuery("SELECT count(*) FROM ta a JOIN (SELECT k, x, y, v FROM tb) b ON a.x = b.x RIGHT JOIN tc c ON c.k = b.k AND c.k = b.y")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("condition: c.k=b.y", "filter: k=y")
                    .returns("""
                            count
                            11
                            """);

            // a cross-joined b: every a row pairs with the b rows that c matches
            for (String b : new String[]{"(SELECT k, x, y, v FROM tb)", "(SELECT * FROM tb)"}) {
                assertQuery("SELECT count(*) FROM ta a CROSS JOIN " + b + " b RIGHT JOIN tc c ON c.k = b.k AND c.k = b.y")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("condition: c.k=b.y", "filter: k=y")
                        .returns("""
                                count
                                21
                                """);
            }

            // c's ON clause implies b.k = a.y, a join key of b, which b's b.y = a.y then turns into the
            // filter b.k = b.y
            for (String b : new String[]{"(SELECT * FROM tb)", "(SELECT k, x, y, max(v) v FROM tb GROUP BY k, x, y)"}) {
                assertQuery("SELECT a.v av, b.v bv, c.v cv FROM ta a JOIN " + b + " b ON b.y = a.y AND a.k = b.y RIGHT JOIN tc c ON c.k = a.y AND b.k = c.k ORDER BY cv, av, bv")
                        .noLeakCheck()
                        .withPlanContaining("condition: c.k=b.k", "filter: y=k")
                        .returns("""
                                av\tbv\tcv
                                101\t201\t301
                                102\t204\t302
                                105\t204\t302
                                103\t203\t303
                                null\tnull\t304
                                null\tnull\t305
                                """);
            }
        });
    }

    @Test
    public void testImpliedEqualityImpliesAnother() throws Exception {
        // The last ON clause of each query holds equalities that share a column, so they imply
        // equalities of earlier tables. Merging an implied equality into the join context of its
        // table can imply yet another one, and the plan must keep that one too.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, c INT)");
            execute("CREATE TABLE t1 (a INT, b INT, c INT)");
            execute("CREATE TABLE t2 (a INT, b INT, c INT)");
            execute("CREATE TABLE t3 (a INT, c INT)");
            execute("INSERT INTO t0 VALUES (1, 1), (2, 1), (2, 2)");
            execute("INSERT INTO t1 VALUES (1, 1, 1), (2, 1, 2), (2, 2, 3)");
            execute("INSERT INTO t2 VALUES (1, 1, 1), (2, 2, 2), (1, 2, 3)");
            execute("INSERT INTO t3 VALUES (1, 1), (2, 2)");

            // t3 implies t2.b = t1.a, which t2's t2.b = t0.a turns into t1.a = t0.a; without it, the
            // t0 row (1, 1) also joins the t1 row (2, 1, 2)
            for (String t3On : new String[]{
                    "t2.a = t3.c AND t2.b = t3.c AND t3.c = t1.a",
                    "t2.b = t3.c AND t3.c = t1.a AND t2.a = t3.c",
                    "t3.c = t1.a AND t2.b = t3.c AND t2.a = t3.c"
            }) {
                final String from = " FROM t0 CROSS JOIN t1 JOIN t2 ON t1.b = t2.b AND t0.a = t2.b JOIN t3 ON " + t3On;
                assertQuery("SELECT t0.a t0a, t0.c t0c, t1.a t1a, t1.b t1b, t2.a t2a, t2.b t2b, t3.c t3c" + from + " ORDER BY t0a, t0c")
                        .noLeakCheck()
                        .returns("""
                                t0a\tt0c\tt1a\tt1b\tt2a\tt2b\tt3c
                                1\t1\t1\t1\t1\t1\t1
                                2\t1\t2\t2\t2\t2\t2
                                2\t2\t2\t2\t2\t2\t2
                                """);
                assertQuery("SELECT count(*)" + from)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                3
                                """);
            }

            // t3 implies t2.c = t0.a and t2.c = t1.c, which together imply t1.c = t0.a; without it,
            // the plan cross joins t0 or t1 to the other tables
            for (String on : new String[]{
                    "t3.c = t0.a AND t2.c = t3.c AND t3.c = t1.c",
                    "t3.c = t1.c AND t2.c = t3.c AND t3.c = t0.a"
            }) {
                for (String from : new String[]{
                        " FROM t0 CROSS JOIN t1 CROSS JOIN t2 JOIN t3 ON " + on,
                        " FROM t0, t1, t2, t3 WHERE " + on
                }) {
                    assertQuery("SELECT t0.a t0a, t0.c t0c, t1.c t1c, t2.c t2c, t3.c t3c" + from + " ORDER BY t0a, t0c")
                            .noLeakCheck()
                            .returns("""
                                    t0a\tt0c\tt1c\tt2c\tt3c
                                    1\t1\t1\t1\t1
                                    2\t1\t2\t2\t2
                                    2\t2\t2\t2\t2
                                    """);
                    assertQuery("SELECT count(*)" + from)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    count
                                    3
                                    """);
                }
            }

            // t3 implies t2.c = t1.c, which t2's t2.c = t0.a turns into t1.c = t0.a; without it, the
            // t0 row (2, 1) also joins
            for (String t3On : new String[]{"t3.a = t1.c AND t3.a = t2.c", "t3.a = t2.c AND t3.a = t1.c"}) {
                final String from = " FROM t0 JOIN t1 ON t0.c = t1.c JOIN t2 ON t0.a = t2.c JOIN t3 ON " + t3On;
                assertQuery("SELECT t0.a t0a, t0.c t0c, t1.c t1c, t2.c t2c, t3.a t3a" + from + " ORDER BY t0a, t0c")
                        .noLeakCheck()
                        .returns("""
                                t0a\tt0c\tt1c\tt2c\tt3a
                                1\t1\t1\t1\t1
                                2\t2\t2\t2\t2
                                """);
                assertQuery("SELECT count(*)" + from)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                2
                                """);
            }
        });
    }

    @Test
    public void testImpliedEqualityKeyOnSubQueryKeepsJoinKey() throws Exception {
        // c's ON clause implies b.y = a.k, a join key of b, and a.k = 2 then implies the filter b.y = 2.
        // When b is a sub-query or a view, the filter moves into it and takes the sub-query's own column
        // names. c's join key keeps c.k = b.y, and it must still refer to b's column: a bare y fails to
        // resolve, and tb's w, which b renames to y, resolves to a.w.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (k INT, x INT, y INT, w INT, v INT)");
            execute("CREATE TABLE tb (k INT, x INT, y INT, w INT, v INT)");
            execute("CREATE TABLE tc (k INT, v INT)");
            execute("""
                    INSERT INTO ta VALUES
                        (1, 0, 1, 3, 101),
                        (2, 0, 2, 2, 102),
                        (null, 1, null, 1, 103),
                        (3, 1, 1, 2, 104),
                        (2, 1, 2, null, 105)
                    """);
            execute("""
                    INSERT INTO tb VALUES
                        (1, 0, 1, 2, 201),
                        (2, 1, 0, 2, 202),
                        (null, 1, null, 3, 203),
                        (2, 0, 2, null, 204),
                        (3, 1, 3, 1, 205),
                        (2, 1, 2, 2, 206)
                    """);
            execute("""
                    INSERT INTO tc VALUES
                        (1, 301),
                        (2, 302),
                        (null, 303),
                        (3, 304),
                        (7, 305)
                    """);
            execute("CREATE VIEW vb AS (SELECT k, x, w y, v FROM tb)");

            // b's y is tb's y
            final String yRows = """
                    av\tbv\tcv
                    102\t204\t302
                    105\t206\t302
                    """;
            final String yRightJoinRows = """
                    av\tbv\tcv
                    null\tnull\t301
                    102\t204\t302
                    105\t206\t302
                    null\tnull\t303
                    null\tnull\t304
                    null\tnull\t305
                    """;
            // b's y is tb's w
            final String wRows = """
                    av\tbv\tcv
                    102\t201\t302
                    105\t202\t302
                    105\t206\t302
                    """;
            final String wRightJoinRows = """
                    av\tbv\tcv
                    null\tnull\t301
                    102\t201\t302
                    105\t202\t302
                    105\t206\t302
                    null\tnull\t303
                    null\tnull\t304
                    null\tnull\t305
                    """;
            for (String[] b : new String[][]{
                    {"(SELECT k, x, y, v FROM tb)", yRows, yRightJoinRows},
                    {"(SELECT k, x, w y, v FROM tb)", wRows, wRightJoinRows},
                    {"vb", wRows, wRightJoinRows}
            }) {
                assertQuery("SELECT a.v av, b.v bv, c.v cv FROM ta a JOIN " + b[0] + " b ON a.x = b.x JOIN tc c ON c.k = a.k AND c.k = b.y WHERE a.k = 2 ORDER BY av, bv")
                        .noLeakCheck()
                        .withPlanContaining("condition: c.k=b.y")
                        .returns(b[1]);
                // the WHERE clause drops the rows that the RIGHT join NULL-extends
                assertQuery("SELECT a.v av, b.v bv, c.v cv FROM ta a JOIN " + b[0] + " b ON a.x = b.x RIGHT JOIN tc c ON c.k = a.k AND c.k = b.y WHERE a.k = 2 ORDER BY av, bv")
                        .noLeakCheck()
                        .withPlanContaining("condition: c.k=b.y")
                        .returns(b[1]);
                // an ON conjunct keeps them
                assertQuery("SELECT a.v av, b.v bv, c.v cv FROM ta a JOIN " + b[0] + " b ON a.x = b.x AND a.k = 2 RIGHT JOIN tc c ON c.k = a.k AND c.k = b.y ORDER BY cv, av, bv")
                        .noLeakCheck()
                        .withPlanContaining("condition: c.k=b.y")
                        .returns(b[2]);
            }
        });
    }

    @Test
    public void testImpliedEqualityOfFirstTableStaysBeforeOuterJoin() throws Exception {
        // s's ON clause implies m.a = m.c, a filter of the first table, which the plan applies after
        // the last join. The outer join r then still matches (m, s) rows that the filter removes
        // afterwards, instead of NULL-extending r's rows. So s keeps both of its equalities.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (a INT, c INT)");
            execute("CREATE TABLE s (a INT)");
            execute("CREATE TABLE r (c INT)");
            execute("INSERT INTO m VALUES (1, 3)");
            execute("INSERT INTO s VALUES (3)");
            execute("INSERT INTO r VALUES (3), (5)");

            // no (m, s) row matches both equalities, so the outer join NULL-extends both r rows
            for (String outerJoin : new String[]{"RIGHT", "FULL"}) {
                for (String sOn : new String[]{"s.a = m.a AND s.a = m.c", "s.a = m.c AND s.a = m.a"}) {
                    final String from = " FROM m JOIN s ON " + sOn + " " + outerJoin + " JOIN r ON r.c = s.a";
                    assertQuery("SELECT m.a ma, s.a sa, r.c rc" + from + " ORDER BY rc")
                            .noLeakCheck()
                            .returns("""
                                    ma\tsa\trc
                                    null\tnull\t3
                                    null\tnull\t5
                                    """);
                    assertQuery("SELECT count(*)" + from)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    count
                                    2
                                    """);
                }
            }
        });
    }

    @Test
    public void testImpliedEqualityStaysOutOfAsOfJoin() throws Exception {
        // As a key or a filter of te, an equality that d's ON clause implies changes which te row the
        // ASOF or LT join picks. Two equalities of the ASOF join itself only match rows, so they imply
        // no filter of its master.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (ts TIMESTAMP, k INT, m INT) TIMESTAMP(ts)");
            execute("CREATE TABLE te (ts TIMESTAMP, k INT, m INT, n INT) TIMESTAMP(ts)");
            execute("CREATE TABLE tc (k INT, m INT)");
            execute("CREATE TABLE d (m INT)");
            execute("""
                    INSERT INTO ta VALUES
                        ('2024-01-01T00:00:10.000000Z', 1, 1),
                        ('2024-01-01T00:00:20.000000Z', 1, 7)
                    """);
            execute("""
                    INSERT INTO te VALUES
                        ('2024-01-01T00:00:01.000000Z', 1, 1, 1),
                        ('2024-01-01T00:00:02.000000Z', 1, 7, 8)
                    """);
            execute("INSERT INTO tc VALUES (1, 1), (1, 7)");
            execute("INSERT INTO d VALUES (1), (7), (8)");

            // both ta rows pick te's (7, 8) row, which only the second ta row matches in d
            for (String teJoin : new String[]{"ASOF JOIN te ON te.k = ta.k", "ASOF JOIN te", "LT JOIN te ON te.k = ta.k"}) {
                for (String dOn : new String[]{"d.m = ta.m AND d.m = te.m", "d.m = te.m AND d.m = ta.m"}) {
                    assertQuery("SELECT ta.m tam, te.m tem, d.m dm FROM ta " + teJoin + " JOIN d ON " + dOn)
                            .noLeakCheck()
                            .noRandomAccess()
                            .returns("""
                                    tam\ttem\tdm
                                    7\t7\t7
                                    """);
                }
                assertQuery("SELECT count(*) FROM ta " + teJoin + " JOIN d ON d.m = te.m AND d.m = te.n")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                0
                                """);
            }

            // every (ta, tc) row stays, and only those with ta.m = tc.m find a te row
            for (boolean isFullFat : new boolean[]{false, true}) {
                assertQuery("SELECT ta.m tam, tc.m tcm, te.m tem FROM ta JOIN tc ON tc.k = ta.k ASOF JOIN te ON te.m = ta.m AND te.m = tc.m ORDER BY tam, tcm")
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .expectSize(isFullFat)
                        .returns("""
                                tam\ttcm\ttem
                                1\t1\t1
                                1\t7\tnull
                                7\t1\tnull
                                7\t7\t7
                                """);
            }
        });
    }

    @Test
    public void testImpliedEqualityStaysOutOfFullFatAsOfJoin() throws Exception {
        // Both equalities of q's ON clause stay in the ASOF or LT key, so the key holds q.sym twice.
        // The full-fat join, which runs for a slave without random access, exposes q.sym once.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, px INT, alt SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE quotes (sym SYMBOL, bid INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE instruments (sym SYMBOL, name VARCHAR)");
            execute("CREATE TABLE t3 (s1 SYMBOL, s2 SYMBOL, k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE q3 (s SYMBOL, k INT, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE ty (a1 INT, a2 INT, b LONG, c LONG, px INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE qy (a INT, b LONG, c LONG, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO trades VALUES
                        ('A', 1, 'A', '2024-01-01T00:00:10.000000Z'),
                        ('B', 2, 'A', '2024-01-01T00:00:20.000000Z'),
                        ('A', 3, 'B', '2024-01-01T00:00:30.000000Z'),
                        ('C', 4, 'C', '2024-01-01T00:00:35.000000Z')
                    """);
            execute("""
                    INSERT INTO quotes VALUES
                        ('A', 10, '2024-01-01T00:00:05.000000Z'),
                        ('B', 20, '2024-01-01T00:00:06.000000Z'),
                        ('A', 11, '2024-01-01T00:00:25.000000Z'),
                        ('A', 12, '2024-01-01T00:00:30.000000Z'),
                        ('C', 30, '2024-01-01T00:00:35.000000Z'),
                        ('B', 21, '2024-01-01T00:00:36.000000Z')
                    """);
            execute("INSERT INTO instruments VALUES ('A', 'alpha'), ('B', 'beta'), ('C', 'gamma')");
            execute("""
                    INSERT INTO t3 VALUES
                        ('A', 'A', 1, '2024-01-01T00:00:10.000000Z'),
                        ('A', 'B', 1, '2024-01-01T00:00:20.000000Z'),
                        ('B', 'B', 2, '2024-01-01T00:00:30.000000Z'),
                        ('A', 'A', 2, '2024-01-01T00:00:40.000000Z')
                    """);
            execute("""
                    INSERT INTO q3 VALUES
                        ('A', 1, 100, '2024-01-01T00:00:01.000000Z'),
                        ('A', 2, 200, '2024-01-01T00:00:02.000000Z'),
                        ('B', 2, 300, '2024-01-01T00:00:03.000000Z'),
                        ('B', 1, 400, '2024-01-01T00:00:04.000000Z'),
                        ('A', 1, 101, '2024-01-01T00:00:35.000000Z')
                    """);
            execute("""
                    INSERT INTO ty VALUES
                        (1, 1, 10, 100, 1, '2024-01-01T00:00:10.000000Z'),
                        (2, 2, 20, 200, 2, '2024-01-01T00:00:20.000000Z'),
                        (1, 2, 10, 100, 3, '2024-01-01T00:00:30.000000Z')
                    """);
            execute("""
                    INSERT INTO qy VALUES
                        (1, 10, 100, 7, '2024-01-01T00:00:01.000000Z'),
                        (2, 20, 200, 8, '2024-01-01T00:00:02.000000Z')
                    """);
            // UNION ALL gives the views no random access, so ASOF and LT joins run full-fat on them
            execute("CREATE VIEW uq AS (SELECT * FROM (SELECT * FROM quotes UNION ALL SELECT * FROM quotes WHERE bid < 0) TIMESTAMP(ts))");
            execute("CREATE VIEW uq3 AS (SELECT * FROM (SELECT * FROM q3 UNION ALL SELECT * FROM q3 WHERE v < 0) TIMESTAMP(ts))");
            execute("CREATE VIEW uqy AS (SELECT * FROM (SELECT * FROM qy UNION ALL SELECT * FROM qy WHERE v < 0) TIMESTAMP(ts))");

            // the default compiler on a view slave, and the full-fat compiler flag on a table slave
            final String[] slaves = {"uq", "quotes"};
            final String[] slaves3 = {"uq3", "q3"};
            final String[] slavesY = {"uqy", "qy"};
            final boolean[] isFullFatFlags = {false, true};
            for (int s = 0; s < slaves.length; s++) {
                final String slave = slaves[s];
                final boolean isFullFat = isFullFatFlags[s];

                // the inner join implies q.sym = i.sym from q.sym = t.sym
                final String innerFrom = " FROM trades t JOIN instruments i ON i.sym = t.sym ";
                final String innerOn = " q ON q.sym = t.sym AND q.sym = i.sym";
                assertQuery("SELECT t.ts, t.sym, i.name, t.px, q.sym qsym, q.bid" + innerFrom + "ASOF JOIN " + slave + innerOn)
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .noRandomAccess()
                        .expectSize(isFullFat)
                        .timestamp("ts")
                        .returns("""
                                ts\tsym\tname\tpx\tqsym\tbid
                                2024-01-01T00:00:10.000000Z\tA\talpha\t1\tA\t10
                                2024-01-01T00:00:20.000000Z\tB\tbeta\t2\tB\t20
                                2024-01-01T00:00:30.000000Z\tA\talpha\t3\tA\t12
                                2024-01-01T00:00:35.000000Z\tC\tgamma\t4\tC\t30
                                """);
                assertQuery("SELECT t.ts, t.sym, i.name, t.px, q.sym qsym, q.bid" + innerFrom + "LT JOIN " + slave + innerOn)
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .noRandomAccess()
                        .expectSize(isFullFat)
                        .timestamp("ts")
                        .returns("""
                                ts\tsym\tname\tpx\tqsym\tbid
                                2024-01-01T00:00:10.000000Z\tA\talpha\t1\tA\t10
                                2024-01-01T00:00:20.000000Z\tB\tbeta\t2\tB\t20
                                2024-01-01T00:00:30.000000Z\tA\talpha\t3\tA\t11
                                2024-01-01T00:00:35.000000Z\tC\tgamma\t4\t\tnull
                                """);
                assertQuery("SELECT *" + innerFrom + "ASOF JOIN " + slave + innerOn)
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .noRandomAccess()
                        .expectSize(isFullFat)
                        .timestamp("ts")
                        .returns("""
                                sym\tpx\talt\tts\tsym1\tname\tsym2\tbid\tts1
                                A\t1\tA\t2024-01-01T00:00:10.000000Z\tA\talpha\tA\t10\t2024-01-01T00:00:05.000000Z
                                B\t2\tA\t2024-01-01T00:00:20.000000Z\tB\tbeta\tB\t20\t2024-01-01T00:00:06.000000Z
                                A\t3\tB\t2024-01-01T00:00:30.000000Z\tA\talpha\tA\t12\t2024-01-01T00:00:30.000000Z
                                C\t4\tC\t2024-01-01T00:00:35.000000Z\tC\tgamma\tC\t30\t2024-01-01T00:00:35.000000Z
                                """);

                // nothing ties t.sym to t.alt, so only rows with t.sym = t.alt can find a q row
                final String outerOn = " q ON q.sym = t.sym AND q.sym = t.alt";
                assertQuery("SELECT t.ts, t.sym, t.alt, t.px, q.sym qsym, q.bid FROM trades t ASOF JOIN " + slave + outerOn)
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .noRandomAccess()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tsym\talt\tpx\tqsym\tbid
                                2024-01-01T00:00:10.000000Z\tA\tA\t1\tA\t10
                                2024-01-01T00:00:20.000000Z\tB\tA\t2\t\tnull
                                2024-01-01T00:00:30.000000Z\tA\tB\t3\t\tnull
                                2024-01-01T00:00:35.000000Z\tC\tC\t4\tC\t30
                                """);
                assertQuery("SELECT t.ts, t.sym, t.alt, t.px, q.sym qsym, q.bid FROM trades t LT JOIN " + slave + outerOn)
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .noRandomAccess()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tsym\talt\tpx\tqsym\tbid
                                2024-01-01T00:00:10.000000Z\tA\tA\t1\tA\t10
                                2024-01-01T00:00:20.000000Z\tB\tA\t2\t\tnull
                                2024-01-01T00:00:30.000000Z\tA\tB\t3\t\tnull
                                2024-01-01T00:00:35.000000Z\tC\tC\t4\t\tnull
                                """);

                // a key that holds q.s at both ends, and one that holds it first and second
                final String slave3 = slaves3[s];
                for (String on : new String[]{
                        " q ON q.s = t.s1 AND q.k = t.k AND q.s = t.s2",
                        " q ON q.k = t.k AND q.s = t.s1 AND q.s = t.s2"
                }) {
                    for (String join : new String[]{" ASOF JOIN ", " LT JOIN "}) {
                        assertQuery("SELECT * FROM t3 t" + join + slave3 + on)
                                .noLeakCheck()
                                .fullFatJoins(isFullFat)
                                .noRandomAccess()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        s1\ts2\tk\tts\ts\tk1\tv\tts1
                                        A\tA\t1\t2024-01-01T00:00:10.000000Z\tA\t1\t100\t2024-01-01T00:00:01.000000Z
                                        A\tB\t1\t2024-01-01T00:00:20.000000Z\t\tnull\tnull\t
                                        B\tB\t2\t2024-01-01T00:00:30.000000Z\tB\t2\t300\t2024-01-01T00:00:03.000000Z
                                        A\tA\t2\t2024-01-01T00:00:40.000000Z\tA\t2\t200\t2024-01-01T00:00:02.000000Z
                                        """);
                    }
                }

                // the key (a, a, b, c) becomes (a, b, c, a), and the map key types move with the
                // columns, so the exposed LONG columns b and c read from their own offsets
                for (String join : new String[]{" ASOF JOIN ", " LT JOIN "}) {
                    assertQuery("SELECT t.px, q.a, q.b, q.c, q.v FROM ty t" + join + slavesY[s] + " q ON q.c = t.c AND q.b = t.b AND q.a = t.a2 AND q.a = t.a1")
                            .noLeakCheck()
                            .fullFatJoins(isFullFat)
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    px\ta\tb\tc\tv
                                    1\t1\t10\t100\t7
                                    2\t2\t20\t200\t8
                                    3\tnull\tnull\tnull\tnull
                                    """);
                }
            }
        });
    }

    @Test
    public void testImpliedEqualityStaysOutOfOuterJoinKey() throws Exception {
        // d's ON clause implies c.m = a.m. As a key of the outer join c, that equality NULL-extends
        // the rows it fails instead of dropping them, and a NULL-extended row can pass the equality
        // that d keeps. So d keeps both of its equalities.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, k INT, m INT)");
            execute("CREATE TABLE b (y INT)");
            execute("CREATE TABLE c (k INT, m INT)");
            execute("CREATE TABLE d (m INT)");
            execute("INSERT INTO a VALUES (5, 1, 1), (5, 2, 1)");
            execute("INSERT INTO b VALUES (1)");
            execute("INSERT INTO c VALUES (1, 1), (2, 7)");
            execute("INSERT INTO d VALUES (1), (7)");

            // a's (5, 2, 1) row and c's (2, 7) row match each other, but no d row matches both
            for (String prefix : new String[]{"a JOIN b ON a.x > b.y", "a"}) {
                for (String outerJoin : new String[]{"LEFT", "RIGHT", "FULL"}) {
                    for (String dOn : new String[]{"d.m = a.m AND d.m = c.m", "d.m = c.m AND d.m = a.m"}) {
                        final String from = " FROM " + prefix + " " + outerJoin + " JOIN c ON c.k = a.k JOIN d ON " + dOn;
                        assertQuery("SELECT a.m am, c.m cm, d.m dm" + from)
                                .noLeakCheck()
                                .noRandomAccess()
                                .returns("""
                                        am\tcm\tdm
                                        1\t1\t1
                                        """);
                        assertQuery("SELECT count(*)" + from)
                                .noLeakCheck()
                                .noRandomAccess()
                                .expectSize()
                                .returns("""
                                        count
                                        1
                                        """);
                    }
                }
            }
        });
    }

    @Test
    public void testImpliedEqualityStaysOutOfTableBeforeOuterJoin() throws Exception {
        // s's ON clause implies t1.a = t1.b, a filter of t1, which joins before the outer join r. The
        // filter would remove t1's only row, so the outer join would NULL-extend r's row, and s's NULL
        // row would match the NULL t1 column of the equality that s keeps. Without the filter, r's row
        // matches, and s drops the joined row. So s keeps both of its equalities.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (k INT)");
            execute("CREATE TABLE t1 (k INT, a INT, b INT)");
            execute("CREATE TABLE r (x INT)");
            execute("CREATE TABLE s (v INT)");
            execute("INSERT INTO t0 VALUES (1)");
            execute("INSERT INTO t1 VALUES (1, 1, 2)");
            execute("INSERT INTO r VALUES (1)");
            execute("INSERT INTO s VALUES (null)");

            for (String outerJoin : new String[]{"RIGHT", "FULL"}) {
                for (String sOn : new String[]{"s.v = t1.a AND s.v = t1.b", "s.v = t1.b AND s.v = t1.a"}) {
                    final String from = " FROM t0 JOIN t1 ON t1.k = t0.k " + outerJoin + " JOIN r ON r.x = t0.k JOIN s ON " + sOn;
                    assertQuery("SELECT t0.k, t1.a, t1.b, r.x, s.v" + from)
                            .noLeakCheck()
                            .noRandomAccess()
                            .returns("""
                                    k\ta\tb\tx\tv
                                    """);
                    assertQuery("SELECT count(*)" + from)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    count
                                    0
                                    """);
                }
            }
        });
    }

    @Test
    public void testInSubQueryWithJoinOnClause() throws Exception {
        // A JOIN nested in a lambda IN sub-query (e.g. "x IN (SELECT ... JOIN ... ON ...)",
        // HORIZON JOIN as first reported) used to drain the shared parser arg stack and consume
        // the IN operand, crashing with an NPE in WhereClauseParser.analyzeIn. The sub-query must
        // compile and filter correctly regardless of join type or how the ON clause is written.
        // ON-clause sub-queries are unsupported and must reject with "query is not allowed here"
        // at every nesting depth: the top level already did, while on master the nested case
        // returned a misleading "Column name expected" (the ON drain consumed the enclosing operand).
        assertMemoryLeak(() -> {
            execute("create table trades (symbol symbol, ts timestamp) timestamp(ts) partition by day");
            execute("create table src (symbol symbol, ts timestamp) timestamp(ts) partition by day");
            execute("create table ref (symbol symbol, ts timestamp) timestamp(ts) partition by day");
            execute("insert into trades values ('A', '2020-01-01T00:00:00.000000Z'), ('B', '2020-01-02T00:00:00.000000Z'), ('C', '2020-01-03T00:00:00.000000Z')");
            execute("insert into src values ('A', '2020-01-01T00:00:00.000000Z'), ('B', '2020-01-02T00:00:00.000000Z')");
            execute("insert into ref values ('A', '2020-01-01T00:00:00.000000Z'), ('B', '2020-01-02T00:00:00.000000Z')");

            final String expected = "symbol\tts\n" +
                    "A\t2020-01-01T00:00:00.000000Z\n" +
                    "B\t2020-01-02T00:00:00.000000Z\n";

            // HORIZON JOIN with shorthand ON (col) -- the exact shape from the bug report
            assertQuery(
                    "select * from trades where symbol in " +
                            "(select s.symbol from src s horizon join ref r on (symbol) range from -30s to 30s step 5s as h)"
            ).noLeakCheck().timestamp("ts").returns(expected);

            // explicit equality ON -- used to fail with "Column name expected"
            assertQuery(
                    "select * from trades where symbol in " +
                            "(select s.symbol from src s horizon join ref r on s.symbol = r.symbol range from -30s to 30s step 5s as h)"
            ).noLeakCheck().timestamp("ts").returns(expected);

            // a second join type with shorthand ON, to cover the shared ON-clause parse path
            // (ASOF and the INNER/LEFT/... family share the same ON-drain code in parseJoin)
            assertQuery(
                    "select * from trades where symbol in (select s.symbol from src s asof join ref r on (symbol))"
            ).noLeakCheck().timestamp("ts").returns(expected);

            // INNER join is the common real-world shape and enters the ON case via the direct arm
            // (not the ASOF/HORIZON fall-through). The hash join in the lambda still filters trades
            // down to A, B, but its slave row chain (eagerly sized to the join page size, >64 KiB) is
            // held by the IN sub-query factory until factory close, past the outer cursor close, so
            // skip assertQuery's post-close memory-usage check (which flags the still-owned RSS as a leak).
            assertQuery(
                    "select * from trades where symbol in (select s.symbol from src s join ref r on s.symbol = r.symbol)"
            ).noLeakCheck().noMemoryUsageCheck().timestamp("ts").returns(expected);

            // multi-column shorthand ON (a, b) inside the lambda exercises the list-of-columns drain
            // arm (parseJoin's default case), distinct from the single-column ON (col) above; the same
            // hash-join slave-chain retention applies, hence noMemoryUsageCheck()
            assertQuery(
                    "select * from trades where symbol in (select s.symbol from src s join ref r on (symbol, ts))"
            ).noLeakCheck().noMemoryUsageCheck().timestamp("ts").returns(expected);

            // NOT IN exercises the same parse path with a negated operator -- expect only C
            assertQuery(
                    "select * from trades where symbol not in " +
                            "(select s.symbol from src s horizon join ref r on (symbol) range from -30s to 30s step 5s as h)"
            ).noLeakCheck().timestamp("ts").returns(
                    "symbol\tts\n" +
                            "C\t2020-01-03T00:00:00.000000Z\n"
            );

            // A scalar sub-query operand (not just IN/NOT IN) hits the same shared arg stack: the "="
            // left-hand side stays on the stack while the inner join's ON clause is parsed. ASOF is a
            // merge join (<64 KiB RSS), so assertQuery works here -- max(s.ts) over the join is the
            // last src timestamp, 2020-01-02, matching trades row B.
            assertQuery(
                    "select * from trades where ts = " +
                            "(select max(s.ts) from src s asof join ref r on (symbol))"
            ).noLeakCheck().timestamp("ts").returns(
                    "symbol\tts\n" +
                            "B\t2020-01-02T00:00:00.000000Z\n"
            );

            // the same for a scalar ">" with an INNER (hash) join; skip the memory-usage check for the
            // >64 KiB RSS reason above. min(s.ts) over the join is 2020-01-01, so trades after it is B, C.
            assertQuery(
                    "select * from trades where ts > " +
                            "(select min(s.ts) from src s join ref r on s.symbol = r.symbol)"
            ).noLeakCheck().noMemoryUsageCheck().timestamp("ts").returns(
                    "symbol\tts\n" +
                            "B\t2020-01-02T00:00:00.000000Z\n" +
                            "C\t2020-01-03T00:00:00.000000Z\n"
            );

            // ON-clause sub-queries stay unsupported when nested, just like at top level. On master
            // the nested "IN sub-query in ON" form returned a misleading "Column name expected" ...
            assertExceptionNoLeakCheck(
                    "select * from trades where symbol in " +
                            "(select s.symbol from src s join ref r on s.symbol in (select symbol from trades))",
                    92,
                    "query is not allowed here",
                    sqlExecutionContext
            );
            // ... and a bare sub-query as the ON criteria returned the same "Column name expected".
            assertExceptionNoLeakCheck(
                    "select * from trades where symbol in " +
                            "(select s.symbol from src s join ref r on (select symbol from trades))",
                    80,
                    "query is not allowed here",
                    sqlExecutionContext
            );
            // The same rejection must hold two lambda levels deep. The ON-clause reject fires inside
            // a parseExpr frame whose scope-stack bottom was raised by the outer lambdas; without the
            // scope-stack clamp this fix adds, the error-unwind would restore that stale bottom over an
            // already-cleared stack and surface an internal "Tried to set bottom beyond the top of the
            // stack" IllegalStateException instead of the positioned SqlException.
            assertExceptionNoLeakCheck(
                    "select * from trades where symbol in " +
                            "(select symbol from src where symbol in " +
                            "(select x.symbol from src x join ref y on x.symbol in (select symbol from trades)))",
                    132,
                    "query is not allowed here",
                    sqlExecutionContext
            );
            assertExceptionNoLeakCheck(
                    "select * from trades where symbol in " +
                            "(select symbol from src where symbol in " +
                            "(select x.symbol from src x join ref y on (select symbol from trades)))",
                    120,
                    "query is not allowed here",
                    sqlExecutionContext
            );

            // A rejected ON-clause sub-query must leave the shared parser state clean: the error
            // path unwinds through reset() and popArgStackBottom(), so the SAME pooled compiler
            // compiles the next, valid query without carrying over corrupted arg-stack state.
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try {
                    CairoEngine.select(
                            compiler,
                            "select * from trades where symbol in " +
                                    "(select s.symbol from src s join ref r on s.symbol in (select symbol from trades))",
                            sqlExecutionContext
                    ).close();
                    Assert.fail("nested ON-clause sub-query must be rejected");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "query is not allowed here");
                }
                assertQuery(
                        "select * from trades where symbol in (select s.symbol from src s asof join ref r on (symbol))"
                ).withCompiler(compiler).noLeakCheck().timestamp("ts").returns(expected);
            }
        });
    }

    @Test
    public void testInnerJoinEqualityNotTakenOverByOuterJoin() throws Exception {
        // reorderTables moves an equality from a later inner join into a model that has no join
        // context, to join that model on a key instead of crossing it. An outer or UNNEST join must
        // not be such a target: code generation reads only the ON clause of a nested-loop outer join
        // and only the array of an UNNEST join, so the equality would vanish. Master dropped
        // f.k = b.y from the plan and returned rows that violate it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1)");
            execute("CREATE TABLE b (y INT)");
            execute("INSERT INTO b VALUES (5), (100)");
            execute("CREATE TABLE f (k INT, c1 INT)");
            execute("INSERT INTO f VALUES (100, 10), (1, 10)");

            // (a LEFT JOIN b ON a.x < b.y) keeps (10,100); only f (100,10) matches both conjuncts
            assertQuery("SELECT a.x, b.y, f.k FROM a LEFT JOIN b ON a.x < b.y JOIN f ON f.k = b.y AND f.c1 = a.x ORDER BY f.k")
                    .noLeakCheck()
                    .withPlanContaining("f.k=b.y")
                    .returns("x\ty\tk\n10\t100\t100\n");

            // (a RIGHT JOIN b ON a.x > b.y) keeps (10,5) and the NULL-extended (null,100); the first
            // fails f.k = b.y, the second fails f.c1 = a.x
            assertQuery("SELECT a.x, b.y, f.k FROM a RIGHT JOIN b ON a.x > b.y JOIN f ON f.k = b.y AND f.c1 = a.x ORDER BY f.k")
                    .noLeakCheck()
                    .withPlanContaining("f.k=b.y")
                    .returns("x\ty\tk\n");

            execute("CREATE TABLE g (id INT, arr DOUBLE[])");
            execute("INSERT INTO g VALUES (10, ARRAY[1.0, 100.0])");
            execute("CREATE TABLE h (k DOUBLE, c1 INT)");
            execute("INSERT INTO h VALUES (100.0, 10), (1.0, 10), (5.0, 10)");
            assertQuery("SELECT g.id, u.val, h.k FROM g CROSS JOIN UNNEST(g.arr) u(val) JOIN h ON h.k = u.val AND h.c1 = g.id ORDER BY h.k")
                    .noLeakCheck()
                    .withPlanContaining("h.k=u.val")
                    .returns("""
                            id\tval\tk
                            10\t1.0\t1.0
                            10\t100.0\t100.0
                            """);
        });
    }

    @Test
    public void testInnerJoinOnConjunctOfAsOfSlaveStaysAboveOuterJoin() throws Exception {
        // The ASOF or LT join of ta runs before the non-equi outer join of tr, which it follows in SQL.
        // n's ON clause compares ta and tm, which both join before the outer join then. As a filter of
        // the ASOF join, the conjunct drops tm's (1) row, so the outer join NULL-extends tr's (1) row,
        // and the NULL-extended row never meets the conjunct. So the conjunct stays a filter of n's join.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tm (k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tr (k INT)");
            execute("CREATE TABLE ta (a INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE n (v INT)");
            execute("""
                    INSERT INTO tm VALUES
                        (1, '2024-01-01T01:00:00.000000Z'),
                        (2, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO tr VALUES
                        (1),
                        (2)
                    """);
            execute("INSERT INTO ta VALUES (2, '2024-01-01T00:30:00.000000Z')");
            execute("INSERT INTO n VALUES (null)");

            // both tm rows pick ta's row, and only tm's (2) row passes the conjunct
            for (String outerJoin : new String[]{"RIGHT", "FULL"}) {
                for (String taJoin : new String[]{"ASOF", "LT"}) {
                    for (String nOn : new String[]{"ta.a = tm.k", "ta.a <= tm.k"}) {
                        final String from = " FROM tm " + outerJoin + " JOIN tr ON tr.k <= tm.k AND tr.k >= tm.k "
                                + taJoin + " JOIN ta JOIN n ON " + nOn;
                        assertQuery("SELECT tm.k mk, tr.k rk, ta.a, n.v" + from + " ORDER BY rk")
                                .noLeakCheck()
                                .returns("""
                                        mk\trk\ta\tv
                                        2\t2\t2\tnull
                                        """);
                        assertQuery("SELECT count(*)" + from)
                                .noLeakCheck()
                                .noRandomAccess()
                                .expectSize()
                                .returns("""
                                        count
                                        1
                                        """);
                    }
                }
            }
        });
    }

    @Test
    public void testInnerJoinOnConjunctPushesPastNullingJoin() throws Exception {
        // An inner-join ON conjunct that references only the master (m.c = 1, m.c > 0, abs(m.c) = 1)
        // gates the inner join, which runs before the downstream RIGHT/FULL OUTER join that NULL-extends
        // the master. It must push down into the master scan, not stay as a post-join filter - otherwise
        // the unmatched (NULL-master) slave rows the outer join synthesizes get dropped. Regression: the
        // master-nulling guard used to intercept these ON conjuncts as if they were WHERE predicates.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (k INT, c INT)");
            execute("INSERT INTO m VALUES (1, 1)");
            execute("CREATE TABLE x (k INT)");
            execute("INSERT INTO x VALUES (1)");
            execute("CREATE TABLE s (k INT)");
            execute("INSERT INTO s VALUES (1), (2), (3)");

            final String expected = "sk\tmk\tmc\n1\t1\t1\n2\tnull\tnull\n3\tnull\tnull\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                for (String onConjunct : new String[]{"m.c = 1", "m.c > 0", "abs(m.c) = 1"}) {
                    assertQuery("SELECT s.k sk, m.k mk, m.c mc FROM m JOIN x ON x.k = m.k AND " + onConjunct
                            + " " + joinType + " JOIN s ON s.k = x.k ORDER BY sk")
                            .noLeakCheck()
                            .returns(expected);
                }
            }
        });
    }

    @Test
    public void testInnerJoinOnEqualityKeepsSpliceJoinMaster() throws Exception {
        // The master of the SPLICE join is the cross join of x0 and x1, which has no random access, so
        // every query fails to compile. n's ON clause holds an equality of x0 and x1. Nothing keeps a
        // SPLICE join at its SQL position, so if the equality left x1's join for n's, x1 would become a
        // pure cross join that runs after the SPLICE join, which would then splice x0 alone and return
        // rows. A later RIGHT or FULL OUTER join keeps x0 and x1 before the SPLICE join either way.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (k INT, a INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE t1 (k INT, a INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE sp (k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE n (k INT)");

            for (String from : new String[]{
                    "t0 x0 CROSS JOIN t1 x1 SPLICE JOIN sp JOIN n ON x1.a = x0.k",
                    "t0 x0 CROSS JOIN t1 x1 SPLICE JOIN sp ON sp.k = x0.k JOIN n ON x1.a = x0.k",
                    "t0 x0 CROSS JOIN t1 x1 CROSS JOIN n x2 SPLICE JOIN sp JOIN n ON x1.a = x0.k AND x2.k = x1.k",
                    "t0 x0 CROSS JOIN t1 x1 SPLICE JOIN sp JOIN n ON x1.a = x0.k JOIN n n2 ON n2.k = n.k",
                    "t0 x0 CROSS JOIN t1 x1 SPLICE JOIN sp RIGHT JOIN n r ON r.k = sp.k JOIN n ON x1.a = x0.k",
                    "t0 x0 CROSS JOIN t1 x1 SPLICE JOIN sp FULL JOIN n r ON r.k = sp.k JOIN n ON x1.a = x0.k"
            }) {
                final String sql = "SELECT x0.k, x1.a, sp.k FROM " + from;
                assertQuery(sql)
                        .noLeakCheck()
                        .fails(sql.indexOf("SPLICE"), "left side of splice join doesn't support random access");
            }
        });
    }

    @Test
    public void testInnerJoinOnEqualityStaysAboveOuterJoin() throws Exception {
        // x4's ON clause holds x1.a = x0.k, an equality of two tables that join before the outer join
        // x3. As a key or a filter of x1, it drops the only (x0, x1) row, so the outer join
        // NULL-extends x3's row, and the NULL-extended row never meets the equality, while x4's other
        // keys match NULL to NULL. So the equality stays a filter of x4's join, which drops every row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (k INT, a INT, b INT)");
            execute("CREATE TABLE t1 (k INT, a INT, b INT)");
            execute("CREATE TABLE t2 (k INT, a INT, b INT)");
            execute("CREATE TABLE t3 (k INT, a INT, b INT)");
            execute("CREATE TABLE t4 (k INT, a INT, b INT)");
            execute("INSERT INTO t0 VALUES (1, 2, 5)");
            execute("INSERT INTO t1 VALUES (null, null, null)");
            execute("INSERT INTO t2 VALUES (1, 2, 5)");
            execute("INSERT INTO t3 VALUES (1, 2, 5)");
            execute("INSERT INTO t4 VALUES (1, 2, 5)");

            for (String outerJoin : new String[]{"RIGHT", "FULL"}) {
                assertQuery("SELECT x0.k, x1.a, x1.b, x2.b, x3.k, x4.a FROM t0 x0 CROSS JOIN t2 x1 CROSS JOIN t3 x2 "
                        + outerJoin + " JOIN t4 x3 ON x1.k = 1 JOIN t1 x4 ON x4.a = x1.b AND x4.a = x2.b AND x1.a = x0.k")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                k\ta\tb\tb1\tk1\ta1
                                """);
                for (String x3On : new String[]{"x1.k = x3.k", "x1.k = 1"}) {
                    assertQuery("SELECT x0.k, x1.a, x1.b, x3.k FROM t0 x0 CROSS JOIN t2 x1 "
                            + outerJoin + " JOIN t4 x3 ON " + x3On + " JOIN t1 x4 ON x1.a = x0.k")
                            .noLeakCheck()
                            .noRandomAccess()
                            .returns("""
                                    k\ta\tb\tk1
                                    """);
                }
            }
        });
    }

    @Test
    public void testInnerJoinOnFilterStaysAboveOuterJoin() throws Exception {
        // n's ON clause compares s and m, which both join before the outer join r. As a filter of s,
        // the conjunct drops the (2, 3, 7) row of (m, s), so the outer join NULL-extends r's (3) row,
        // and the NULL-extended row never meets the conjunct. So the conjunct stays a filter of n's join.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (k INT)");
            execute("CREATE TABLE s (k INT, a INT)");
            execute("CREATE TABLE r (k INT)");
            execute("CREATE TABLE n (v INT)");
            execute("INSERT INTO m VALUES (2)");
            execute("""
                    INSERT INTO s VALUES
                        (1, 2),
                        (3, 7)
                    """);
            execute("""
                    INSERT INTO r VALUES
                        (1),
                        (3)
                    """);
            execute("INSERT INTO n VALUES (null)");

            // r matches both (m, s) rows, and only the (2, 1, 2) row passes the conjunct
            for (String outerJoin : new String[]{"RIGHT", "FULL"}) {
                for (String rOn : new String[]{"r.k = s.k", "r.k <= s.k"}) {
                    for (String nOn : new String[]{"s.a = m.k", "s.a <= m.k", "s.a + 0 = m.k"}) {
                        final String from = " FROM m CROSS JOIN s " + outerJoin + " JOIN r ON " + rOn + " JOIN n ON " + nOn;
                        assertQuery("SELECT m.k mk, s.k sk, s.a sa, r.k rk, n.v" + from + " ORDER BY rk")
                                .noLeakCheck()
                                .returns("""
                                        mk\tsk\tsa\trk\tv
                                        2\t1\t2\t1\tnull
                                        """);
                        assertQuery("SELECT count(*)" + from)
                                .noLeakCheck()
                                .noRandomAccess()
                                .expectSize()
                                .returns("""
                                        count
                                        1
                                        """);
                    }
                }
            }
        });
    }

    @Test
    public void testJoinAliasBug() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (xid int, a int, b int)");
            execute("create table y (yid int, a int, b int)");
            select("select tx.a, tx.b from x as tx left join y as ty on xid = yid where tx.a = 1 or tx.b=2").close();
            select("select tx.a, tx.b from x as tx left join y as ty on xid = yid where ty.a = 1 or ty.b=2").close();
            select("select tx.a, tx.b from x as tx right join y as ty on xid = yid where tx.a = 1 or tx.b=2").close();
            select("select tx.a, tx.b from x as tx right join y as ty on xid = yid where ty.a = 1 or ty.b=2").close();
            select("select tx.a, tx.b from x as tx full join y as ty on xid = yid where tx.a = 1 or tx.b=2").close();
            select("select tx.a, tx.b from x as tx full join y as ty on xid = yid where ty.a = 1 or ty.b=2").close();
        });
    }

    @Test
    public void testJoinByInterval() throws Exception {
        assertMemoryLeak(() -> assertQuery("select * from (" +
                "  (select interval(100000,200000) i, 'foo' s) a " +
                "  join " +
                "  (select interval(100000,200000) i, 'bar' s) b " +
                "  on a.i = b.i " +
                ")")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        i\ts\ti1\ts1
                        ('1970-01-01T00:00:00.100Z', '1970-01-01T00:00:00.200Z')\tfoo\t('1970-01-01T00:00:00.100Z', '1970-01-01T00:00:00.200Z')\tbar
                        """));
    }

    @Test
    public void testJoinColumnPropagationIntoJoinModel() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (" +
                            "  symbol SYMBOL," +
                            "  price DOUBLE," +
                            "  amount DOUBLE," +
                            "  timestamp TIMESTAMP " +
                            ") timestamp (timestamp) PARTITION BY DAY;"
            );

            execute("insert into trades values ( 'ETH-USD', 2, 2, '2023-05-29T13:15:00.000000Z') ");

            for (String joinType : Arrays.asList("LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "LT JOIN", "ASOF JOIN", "SPLICE JOIN")) {
                testJoinColumnPropagationIntoJoinModel0(joinType);
            }
            testJoinColumnPropagationIntoJoinModel0("JOIN");
        });
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
            final String expected = """
                    c\ta\tb
                    2\t568\t16
                    2\t568\t72
                    4\t371\t3
                    4\t371\t14
                    6\t439\t12
                    6\t439\t81
                    8\t521\t16
                    8\t521\t97
                    10\t598\t5
                    10\t598\t74
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(10))");
            execute("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))");

            // master records should be filtered out because slave records missing
            assertQuery("select x.c, x.a, b from x join y on y.m = x.c and 1 < 10 order by c, a, b")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testJoinConstantTrueFF() throws Exception {
        testFullFat(this::testJoinConstantTrue0);
    }

    @Test
    public void testJoinContextIsolationInIntersect() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE t (
                              created timestamp,
                              event short,
                              origin short
                            ) TIMESTAMP(created) PARTITION BY DAY;"""
            );
            execute("INSERT INTO t VALUES ('2023-09-21T10:00:00.000000Z', 1, 1);");

            // The important aspects here are T2.created = '2003-09-21T10:00:00.000000Z'
            // in the first query and T2.created = T3.created in the second one. Due to this,
            // transitive filters pass was mistakenly mutating where clause in the second query.
            final String query1 = """
                    SELECT count(1)
                    FROM t as T1 CROSS JOIN t as T2
                    WHERE T2.created > now() and T2.created = '2003-09-21T10:00:00.000000Z'""";
            final String query2 = """
                    SELECT count(1)
                    FROM t as T1 JOIN t as T2 on T1.created = T2.created JOIN t as T3 ON T2.created = T3.created
                    WHERE T3.created < now()""";

            assertQuery(query1)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n0\n");
            assertQuery(query2)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n1\n");

            assertQuery(query1 + " INTERSECT " + query2)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("count\n");
        });
    }

    @Test
    public void testJoinContextIsolationInLambdaConstCondition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (akey SYMBOL INDEX, av STRING)");
            execute("CREATE TABLE tb2 (akey SYMBOL INDEX, bv STRING)");
            execute("CREATE TABLE tc (ckey SYMBOL INDEX, cv STRING)");
            execute("INSERT INTO ta VALUES ('x', 'ax'), ('y', 'ay')");
            execute("INSERT INTO tb2 VALUES ('x', 'bx'), ('y', 'by')");
            execute("INSERT INTO tc VALUES ('x', 'cx'), ('y', 'cy')");

            // optimiseExpressionModels optimises the IN-lambda before the enclosing
            // query's join pass. The lambda's join pass collects akey='x' into the
            // transitive-filter const maps; the enclosing pass must not read that
            // stale entry and derive ckey='x' on tc, which would drop the 'y' row.
            assertQuery(
                    """
                            SELECT a.akey, a.av, c.cv
                            FROM ta a
                            JOIN tc c ON c.ckey = a.akey
                            WHERE a.akey IN (SELECT t1.akey FROM ta t1 CROSS JOIN tb2 t2 WHERE t1.akey = t2.akey AND t1.akey = 'x')
                               OR a.av = 'ay'
                            ORDER BY av"""
            )
                    .noLeakCheck()
                    // a join inside an IN (SELECT ...) lambda retains ~131 KiB of factory
                    // memory until factory close, tripping the 64 KiB post-close RSS check
                    // for any such query; the test-end leak check still guards real leaks
                    .noMemoryUsageCheck()
                    .returns("""
                            akey\tav\tcv
                            x\tax\tcx
                            y\tay\tcy
                            """);
        });
    }

    @Test
    public void testJoinContextIsolationInLambdaPushedPredicate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (akey SYMBOL INDEX, av STRING)");
            execute("CREATE TABLE tb2 (akey SYMBOL INDEX, bv STRING)");
            execute("CREATE TABLE tc (ckey SYMBOL INDEX, cv STRING)");
            execute("INSERT INTO ta VALUES ('x', 'ax'), ('y', 'ay')");
            execute("INSERT INTO tb2 VALUES ('x', 'bx'), ('y', 'by')");
            execute("INSERT INTO tc VALUES ('x', 'cx'), ('y', 'cy')");
            execute("CREATE VIEW v1 AS (SELECT t1.akey AS k, t1.av FROM ta t1 CROSS JOIN tb2 t2 WHERE t1.akey = t2.akey)");

            // moveWhereInsideSubQueries pushes k='x' into the view's join inside the
            // IN-lambda and re-derives transitive filters from the pushed predicate.
            // The const-map entry it writes must not survive into the enclosing
            // query's join pass, or tc picks up a derived ckey='x' filter and the
            // 'y' row disappears.
            assertQuery(
                    """
                            SELECT a.akey, a.av, c.cv
                            FROM ta a
                            JOIN tc c ON c.ckey = a.akey
                            WHERE a.akey IN (SELECT k FROM v1 WHERE k = 'x')
                               OR a.av = 'ay'
                            ORDER BY av"""
            )
                    .noLeakCheck()
                    // a join inside an IN (SELECT ...) lambda retains ~131 KiB of factory
                    // memory until factory close, tripping the 64 KiB post-close RSS check
                    // for any such query; the test-end leak check still guards real leaks
                    .noMemoryUsageCheck()
                    .returns("""
                            akey\tav\tcv
                            x\tax\tcx
                            y\tay\tcy
                            """);
        });
    }

    @Test
    public void testJoinContextIsolationInUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE t (
                              created timestamp,
                              event short,
                              origin short
                            ) TIMESTAMP(created) PARTITION BY DAY;"""
            );
            execute("INSERT INTO t VALUES ('2023-09-21T10:00:00.000000Z', 1, 1);");
            execute("INSERT INTO t VALUES ('2023-09-21T11:00:00.000000Z', 1, 1);");

            // The important aspects here are T1.event = 1.0
            // in the first query and T1.event = T2.event in the second one. Due to this,
            // transitive filters pass was mistakenly mutating where clause in the second query.
            final String query1 = """
                    SELECT count(1)
                    FROM t as T1 JOIN t as T2 ON T1.created = T2.created
                    WHERE T1.event = 1.0""";
            final String query2 = "SELECT count(1)\n" +
                    "FROM t as T1 JOIN t as T2 ON T1.event = T2.event";

            assertQuery(query1)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n2\n");
            assertQuery(query2)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n4\n");

            assertQuery(query1 + " UNION " + query2)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            count
                            2
                            4
                            """);
        });
    }

    @Test
    public void testJoinContextIsolationInUnionWithFactTokens() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s1a (k INT, x STRING)");
            execute("INSERT INTO s1a VALUES (1, 'seed')");
            execute("CREATE TABLE s1b (k INT, y STRING)");
            execute("INSERT INTO s1b VALUES (1, 'one')");
            execute("CREATE TABLE r2a (x STRING)");
            execute("INSERT INTO r2a VALUES ('foo')");
            execute("CREATE TABLE r2b (x STRING)");
            execute("INSERT INTO r2b VALUES ('foo')");
            execute("CREATE TABLE r2c (x STRING)");
            execute("INSERT INTO r2c VALUES ('foo'), ('z')");

            assertQuery("""
                    SELECT count(*) AS total, count(ax) AS non_null_ax
                    FROM (
                        SELECT a.x AS ax, b.y AS bx, NULL::STRING AS cx
                        FROM s1a a
                        JOIN s1b b ON b.k = a.k
                        WHERE a.x = 'seed'
                        UNION ALL
                        SELECT a.x AS ax, b.x AS bx, c.x AS cx
                        FROM r2a a
                        JOIN r2b b ON b.x = a.x AND a.x ~ '^foo$'
                        RIGHT JOIN r2c c ON c.x = a.x
                    )
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("total\tnon_null_ax\n3\t2\n");
        });
    }

    @Test
    public void testJoinContextIsolationInUnionWithModelOnOrigins() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u1a (k INT, v INT)");
            execute("INSERT INTO u1a VALUES (1, 1)");
            execute("CREATE TABLE u1b (k INT)");
            execute("INSERT INTO u1b VALUES (1)");
            execute("CREATE TABLE u1c (k INT)");
            execute("INSERT INTO u1c VALUES (1)");
            execute("CREATE TABLE u1d (k INT)");
            execute("INSERT INTO u1d VALUES (1)");
            execute("CREATE TABLE u2a (k INT)");
            execute("INSERT INTO u2a VALUES (1)");
            execute("CREATE TABLE u2b (k INT)");
            execute("INSERT INTO u2b VALUES (1)");
            execute("CREATE TABLE u2c (k INT)");
            execute("INSERT INTO u2c VALUES (1)");

            assertQuery("""
                    SELECT count(*) AS row_count FROM (
                        SELECT a.k
                        FROM u1a a
                        JOIN u1b b ON b.k = a.k
                        JOIN u1c c ON c.k = a.k
                        JOIN u1d g ON g.k = a.k AND a.v > 0
                        UNION ALL
                        SELECT d.k
                        FROM u2a d
                        RIGHT JOIN u2b e ON e.k >= d.k
                        JOIN u2c f ON f.k = d.k
                    )
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("row_count\n2\n");
        });
    }

    @Test
    public void testJoinInner() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    c\ta\tb\td\tcolumn
                    1\t120\t6\t0\t-6
                    1\t120\t6\t50\t44
                    1\t120\t39\t0\t-39
                    1\t120\t39\t50\t11
                    1\t120\t42\t0\t-42
                    1\t120\t42\t50\t8
                    1\t120\t71\t0\t-71
                    1\t120\t71\t50\t-21
                    2\t568\t14\t55\t41
                    2\t568\t14\t968\t954
                    2\t568\t16\t55\t39
                    2\t568\t16\t968\t952
                    2\t568\t48\t55\t7
                    2\t568\t48\t968\t920
                    2\t568\t72\t55\t-17
                    2\t568\t72\t968\t896
                    3\t333\t3\t305\t302
                    3\t333\t3\t964\t961
                    3\t333\t12\t305\t293
                    3\t333\t12\t964\t952
                    3\t333\t16\t305\t289
                    3\t333\t16\t964\t948
                    3\t333\t81\t305\t224
                    3\t333\t81\t964\t883
                    4\t371\t5\t104\t99
                    4\t371\t5\t171\t166
                    4\t371\t67\t104\t37
                    4\t371\t67\t171\t104
                    4\t371\t74\t104\t30
                    4\t371\t74\t171\t97
                    4\t371\t97\t104\t7
                    4\t371\t97\t171\t74
                    5\t251\t7\t198\t191
                    5\t251\t7\t279\t272
                    5\t251\t44\t198\t154
                    5\t251\t44\t279\t235
                    5\t251\t47\t198\t151
                    5\t251\t47\t279\t232
                    5\t251\t97\t198\t101
                    5\t251\t97\t279\t182
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x ts from long_sequence(5)) timestamp(ts)");
            execute("create table y as (select cast((x-1)/4 + 1 as int) c, abs(rnd_int() % 100) b from long_sequence(20))");
            execute("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))");

            assertQuery("select z.c, x.a, b, d, d-b from x join y on(c) join z on (c) order by z.c, b, d")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testJoinInnerAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (select" +
                            " x id," +
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

            execute(
                    "create table y as (select" +
                            " x id," +
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

            final String expected = """
                    id\tkk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tvch\tid1\tkk1\ta1\tb1\tc1\td1\te1\tf1\tg1\ti1\tj1\tk1\tl1\tm1\tn1\tvch1
                    1\t1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t1\t1746137611\ttrue\tL\t0.18852800970933203\t0.62260014\t777\t2015-08-19T06:10:07.386Z\t\t-7228768303272348606\t1970-01-01T00:00:00.000000Z\t15\t\tTNPHFL\tg>)5{l5J\\d;f7u
                    1\t1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t2\t1\t1350645064\tfalse\tH\t0.2394591643144588\t0.90679234\t399\t\tMQNT\t8321277364671502705\t1970-01-01T00:16:40.000000Z\t50\t00000000 11 96 37 08 dd 98 ef 54 88 2a a2 ad e7\tVFGPPRGSXBH\t7^\uDBF8\uDD28\uDB37\uDC95Qǜbȶ\u05EC˟'ꋯɟ\uF6BE腠
                    1\t1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t3\t1\t1373528915\ttrue\tW\t0.38509066982448115\tnull\t658\t2015-12-24T01:28:12.922Z\tJCKF\t-7745861463408011425\t1970-01-01T00:33:20.000000Z\t43\t\tKXEJCTIZKYFLU\tһτ鏻Ê띘Ѷ>͓\uDA8B\uDFC4︵Ƀ^
                    1\t1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t4\t1\t1120609071\ttrue\t\tnull\t0.13890666\t984\t2015-04-30T08:35:52.508Z\tOGMX\t-6929866925584807039\t1970-01-01T00:50:00.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\t\t$c~{=T@Xz
                    2\t2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t5\t2\t-1583707719\tfalse\tO\t0.03314618075579956\t0.838306\t711\t2015-10-17T09:06:19.735Z\tMQNT\t3396017735551392340\t1970-01-01T01:06:40.000000Z\t28\t00000000 4c 0e 8f f1 0c c5 60 b7 d1 5a 0c e9 db 51\tBZWNIJEEHRUG\t
                    2\t2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t6\t2\t-2016176825\ttrue\tT\tnull\t0.23567414\t813\t2015-12-27T00:19:42.415Z\tMQNT\t3464609208866088600\t1970-01-01T01:23:20.000000Z\t49\t\tFNUHNR\t\\0zpA
                    2\t2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t7\t2\t1947808961\ttrue\tE\t0.7783351753890267\t0.33046818\t725\t2015-12-22T01:44:08.182Z\t\t8809114770260886433\t1970-01-01T01:40:00.000000Z\t43\t00000000 92 a3 9b e3 cb c2 64 8a b0 35\tBOSEPGIUQZHEISQH\t"k[JYtuW/
                    2\t2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t8\t2\t1271828924\tfalse\t\tnull\t0.43757588\t397\t2015-02-06T00:08:58.203Z\tUKLG\t6903369264246740332\t1970-01-01T01:56:40.000000Z\t50\t00000000 ad 79 87 fc 92 83 fc 88 f3 32\tRLPTY\t芊,\uD931\uDF48ҽ\uDA01\uDE60E죢魷
                    3\t3\t-1172180184\tfalse\tS\t0.5891216483879789\t0.28200203\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:33:20.000000Z\t27\t\tIQBZXIOVIKJS\t\uDAB2\uDF79軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t9\t3\t-481534978\tfalse\tI\t0.21224614178286005\tnull\t169\t2015-11-10T00:58:54.194Z\tMQNT\t-6128888161808465767\t1970-01-01T02:13:20.000000Z\t14\t\tKPYVGP\t>XzlGEYDcSIJLy
                    3\t3\t-1172180184\tfalse\tS\t0.5891216483879789\t0.28200203\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:33:20.000000Z\t27\t\tIQBZXIOVIKJS\t\uDAB2\uDF79軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t10\t3\t-1169915830\ttrue\tP\tnull\t0.058909357\t359\t2015-05-26T17:24:24.749Z\t\t-7350430133595690521\t1970-01-01T02:30:00.000000Z\t14\t00000000 35 3b 1c 9c 1d 5c c1 5d 2d 44 ea 00 81 c4 19 a1
                    00000010 ec\tSMIFDYPDK\t
                    3\t3\t-1172180184\tfalse\tS\t0.5891216483879789\t0.28200203\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:33:20.000000Z\t27\t\tIQBZXIOVIKJS\t\uDAB2\uDF79軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t11\t3\t-1505690678\tfalse\tR\t0.09854153834719315\t0.23285526\t82\t2015-06-03T01:01:00.230Z\tUKLG\t-7725099828175109832\t1970-01-01T02:46:40.000000Z\t27\t\tZUPVQFULMER\tM\uDB48\uDC78{ϸ\uD9F4\uDFB9\uDA0A\uDC7A\uDA76\uDC87>\uD8F0\uDF66Ҫb\uDBB1\uDEA3
                    3\t3\t-1172180184\tfalse\tS\t0.5891216483879789\t0.28200203\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:33:20.000000Z\t27\t\tIQBZXIOVIKJS\t\uDAB2\uDF79軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t12\t3\t600986867\tfalse\tM\t0.19823647700531244\tnull\t557\t2015-01-30T03:27:34.392Z\t\t5324839128380055812\t1970-01-01T03:03:20.000000Z\t25\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a
                    00000010 42 71 a3 7a\tDNZNLCNGZTOY\t1\uDA8F\uDC319믓˫ᡙ\uDBEC\uDE3B櫑߸!>\uD9F3\uDFD5a~=V
                    4\t4\t862447505\ttrue\tV\t0.2711532808184136\t0.48524046\t556\t2015-12-06T14:13:54.132Z\tPEHN\t2387397055355257412\t1970-01-01T00:50:00.000000Z\t5\t00000000 34 e0 b0 e9 98 f7 67 62 28 60 b0 ec 0b 92\tOHNZHZ\t1CW#k1.xo\t13\t4\t100444418\tfalse\tK\t0.28400807705010733\t0.5784462\t1015\t2015-05-21T09:22:31.780Z\tOGMX\t-2052253029650705565\t1970-01-01T03:20:00.000000Z\t18\t00000000 4b b7 e2 7f ab 6e 23 03 dd c7 d6\tDRHFBCZI\tB8^嘢\uD952\uDF63^寻&
                    4\t4\t862447505\ttrue\tV\t0.2711532808184136\t0.48524046\t556\t2015-12-06T14:13:54.132Z\tPEHN\t2387397055355257412\t1970-01-01T00:50:00.000000Z\t5\t00000000 34 e0 b0 e9 98 f7 67 62 28 60 b0 ec 0b 92\tOHNZHZ\t1CW#k1.xo\t14\t4\t473980\ttrue\tK\t0.7066431848881077\tnull\t486\t2015-04-18T21:58:29.097Z\t\t-8829329332761013903\t1970-01-01T03:36:40.000000Z\t27\t00000000 40 4e 8c 47 84 e9 c0 55 12 44 dc\tQCMZCCYVBDMQE\t:\uDACD\uDD7D%륤\uD8F4\uDC67YͥɈ\uDAB6\uDF33\uDB00\uDF8AϿ˄礏ɍ\uDB2C\uDD55\uD904\uDFA0
                    4\t4\t862447505\ttrue\tV\t0.2711532808184136\t0.48524046\t556\t2015-12-06T14:13:54.132Z\tPEHN\t2387397055355257412\t1970-01-01T00:50:00.000000Z\t5\t00000000 34 e0 b0 e9 98 f7 67 62 28 60 b0 ec 0b 92\tOHNZHZ\t1CW#k1.xo\t15\t4\t-45671426\tfalse\tG\t0.8825940193001498\tnull\t405\t2015-02-23T23:20:35.948Z\tOGMX\t1708771870007419078\t1970-01-01T03:53:20.000000Z\t40\t\tUIOXLQLUUZIZ\t
                    4\t4\t862447505\ttrue\tV\t0.2711532808184136\t0.48524046\t556\t2015-12-06T14:13:54.132Z\tPEHN\t2387397055355257412\t1970-01-01T00:50:00.000000Z\t5\t00000000 34 e0 b0 e9 98 f7 67 62 28 60 b0 ec 0b 92\tOHNZHZ\t1CW#k1.xo\t16\t4\t-1917313611\tfalse\tK\t0.1855717716409928\t0.69262904\t766\t2015-11-01T03:24:58.178Z\tMQNT\t-5387461693978657124\t1970-01-01T04:10:00.000000Z\t18\t\tGYDEQNNGKFDONP\t7?TPa,m9=
                    5\t5\t-903066492\tfalse\tZ\t0.7260468106076399\t0.722936\t393\t2015-04-04T13:16:46.517Z\tPEHN\t-4058426794463997577\t1970-01-01T01:06:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ\t}$\uDA43\uDFF0-㔍x\t17\t5\t-642526996\ttrue\tG\t0.38014703172702147\tnull\t251\t2015-05-22T02:07:31.345Z\tOGMX\t7509515980141386401\t1970-01-01T04:26:40.000000Z\t21\t00000000 c2 a2 b4 8e 99 a8 2b 8d 35 c5 85 9a\tTKIBWFC\t fF.R
                    5\t5\t-903066492\tfalse\tZ\t0.7260468106076399\t0.722936\t393\t2015-04-04T13:16:46.517Z\tPEHN\t-4058426794463997577\t1970-01-01T01:06:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ\t}$\uDA43\uDFF0-㔍x\t18\t5\t671650197\ttrue\tC\t0.2977278793266547\t0.4953196\t454\t2015-06-27T19:24:50.416Z\t\t-8775249844552344320\t1970-01-01T04:43:20.000000Z\t25\t00000000 77 91 b2 de 58 45 d0 1b 58 be 33 92\t\tC\uDB4E\uDC43\uDAAD\uDE0A\uE916G[ꫭ\uDA99\uDC83\uD8F9\uDF14߂ؠ葶\u2433\uEE49
                    5\t5\t-903066492\tfalse\tZ\t0.7260468106076399\t0.722936\t393\t2015-04-04T13:16:46.517Z\tPEHN\t-4058426794463997577\t1970-01-01T01:06:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ\t}$\uDA43\uDFF0-㔍x\t19\t5\t-671347440\tfalse\tC\t0.6455308455173533\t0.5938364\t64\t2015-04-01T22:42:30.344Z\tOGMX\t7356286536462170873\t1970-01-01T05:00:00.000000Z\t47\t00000000 92 08 f1 96 7f a0 cf 00 74 7c 32 16 38 00\tZDYHD\t❍\uDB17\uDC72쬉반+Eږ胵zݒ邍\uF7F86H
                    5\t5\t-903066492\tfalse\tZ\t0.7260468106076399\t0.722936\t393\t2015-04-04T13:16:46.517Z\tPEHN\t-4058426794463997577\t1970-01-01T01:06:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ\t}$\uDA43\uDFF0-㔍x\t20\t5\t-2033189695\tfalse\tK\t0.1672705743728916\t0.28764933\t271\t2015-03-17T09:46:55.817Z\tOGMX\t-7429841700499010243\t1970-01-01T05:16:40.000000Z\t14\t\tSWHLSWPF\tJ\uD9FB\uDE6C\uDA85\uDF29䚭ϸ\uD9A8\uDFFBi⟃2
                    """;

            // filter is applied to final join result
            assertQuery("select * from x join y on (kk) order by x.id, y.id")
                    .noLeakCheck()
                    .returns(expected);
            // add no-op filter, so that x size estimate is not available anymore
            assertQuery("select * from (x where kk > -1) x join y on (kk) order by x.id, y.id")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testJoinInnerAllTypesFF() throws Exception {
        testFullFat(this::testJoinInnerAllTypes0);
    }

    @Test
    public void testJoinInnerConstantFilterWithNonBooleanExpressionFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE IF NOT EXISTS x (ts timestamp, event short) TIMESTAMP(ts);");

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
            final String expected = """
                    c\ta\tb\td\tcolumn
                    1\t120\t71\t0\t-71
                    1\t120\t42\t0\t-42
                    1\t120\t39\t0\t-39
                    1\t120\t71\t50\t-21
                    1\t120\t6\t0\t-6
                    1\t120\t42\t50\t8
                    1\t120\t39\t50\t11
                    1\t120\t6\t50\t44
                    2\t568\t72\t55\t-17
                    2\t568\t48\t55\t7
                    2\t568\t16\t55\t39
                    2\t568\t14\t55\t41
                    2\t568\t72\t968\t896
                    2\t568\t48\t968\t920
                    2\t568\t16\t968\t952
                    2\t568\t14\t968\t954
                    3\t333\t81\t305\t224
                    3\t333\t16\t305\t289
                    3\t333\t12\t305\t293
                    3\t333\t3\t305\t302
                    3\t333\t81\t964\t883
                    3\t333\t16\t964\t948
                    3\t333\t12\t964\t952
                    3\t333\t3\t964\t961
                    4\t371\t97\t104\t7
                    4\t371\t74\t104\t30
                    4\t371\t67\t104\t37
                    4\t371\t97\t171\t74
                    4\t371\t74\t171\t97
                    4\t371\t5\t104\t99
                    4\t371\t67\t171\t104
                    4\t371\t5\t171\t166
                    5\t251\t97\t198\t101
                    5\t251\t47\t198\t151
                    5\t251\t44\t198\t154
                    5\t251\t97\t279\t182
                    5\t251\t7\t198\t191
                    5\t251\t47\t279\t232
                    5\t251\t44\t279\t235
                    5\t251\t7\t279\t272
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))");
            execute("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))");
            execute("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))");

            assertQuery("select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) order by z.c, d-b")
                    .noLeakCheck()
                    .returns(expected);
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
            execute("CREATE TABLE IF NOT EXISTS x (ts timestamp, event short) TIMESTAMP(ts);");
            execute("INSERT INTO x VALUES (now(), 42)");
            assertQuery("SELECT count(*) FROM x AS a INNER JOIN x AS b ON a.event = b.event WHERE now() = now()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            1
                            """);
        });
    }

    @Test
    public void testJoinInnerInnerFilter() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    c\ta\tb\td\tcolumn
                    1\t120\t6\t0\t-6
                    1\t120\t6\t50\t44
                    2\t568\t16\t55\t39
                    2\t568\t14\t55\t41
                    2\t568\t16\t968\t952
                    2\t568\t14\t968\t954
                    3\t333\t16\t305\t289
                    3\t333\t12\t305\t293
                    3\t333\t3\t305\t302
                    3\t333\t16\t964\t948
                    3\t333\t12\t964\t952
                    3\t333\t3\t964\t961
                    4\t371\t5\t104\t99
                    4\t371\t5\t171\t166
                    5\t251\t7\t198\t191
                    5\t251\t7\t279\t272
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))");
            execute("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))");
            execute("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(16))");

            // filter is applied to intermediate join result
            assertQuery("select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where y.b < 20 order by z.c, d-b")
                    .noLeakCheck()
                    .returns(expected);

            execute("insert into x select cast(x+6 as int) c, abs(rnd_int() % 650) a from long_sequence(3)");
            execute("insert into y select cast((x+19)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(16)");
            execute("insert into z select cast((x+15)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(2)");

            assertQuery("select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where y.b < 20 order by z.c, d-b")
                    .noLeakCheck()
                    .returns(expected +
                            "7\t253\t14\t228\t214\n" +
                            "7\t253\t14\t723\t709\n" +
                            "8\t431\t0\t348\t348\n" +
                            "8\t431\t0\t790\t790\n" +
                            "9\t100\t19\t456\t437\n" +
                            "9\t100\t8\t456\t448\n" +
                            "9\t100\t19\t667\t648\n" +
                            "9\t100\t8\t667\t659\n");
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
            final String expected = """
                    kk\ta\tb\tkk1\ta1\tb1
                    1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0x8a538661f350d0b46f06560981acb5496adc00ebd29fdd5373dee145497c5436\tH
                    1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP
                    1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0xc718ab5cbb3fd261c1bf6c24be53876861b1a0b0a559551538b73d329210d277\tY
                    1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY
                    2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x58dfd08eeb9cc39ecec82869edec121bc2593f82b430328d84a09f29df637e38\tB
                    2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x10bb226eb4243e3683b91ec970b04e788a50f7ff7f6ed3305705e75fe328fa9d\tE
                    2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x4c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39bccb30ed7795ebc8\tJ
                    2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP
                    3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG
                    3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0x4cd64b0b0a344f8e6698c6c186b7571a9cba3ef59083484d98c2d832d83de993\tR
                    3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0x3ad08d6037d3ce8155c06051ee52138b655f87a3a21d575f610f69efe063fe79\tS
                    3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0xbacd57f41b59057caa237cfb02a208e494cfe42988a633de738bab883dc7e332\tU
                    4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0xc736a8b67656c4f159d574d2ff5fb1e3687a84abb7bfac3ebedf29efb28cdcb1\tC
                    4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x9b27eba5e9cfa1e29660300cea7db540954a62eca44acb2d71660a9b0890a2f0\tJ
                    4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM
                    4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x9a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d645277ee62a5a6e9fb\tZ
                    5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0x3c5d8a6969daa0b37d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46\tE
                    5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0xba37e200ad5b17cdada00dc8b85c1bc8a5f80be4b45bf437492990e1a29afcac\tG
                    5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI
                    5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0x37b4f6e41fbfd55f587274e3ab1ebd4d6cecb916a1ad092b997918f622d62989\tS
                    """;

            execute(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_long256() a," +
                            " rnd_char() b " +
                            " from long_sequence(5))"
            );

            execute(
                    "create table y as (select" +
                            " cast((x-1)/4 + 1 as int) kk," +
                            " rnd_long256() a," +
                            " rnd_char() b " +
                            " from long_sequence(20))"
            );

            // filter is applied to final join result
            assertQuery("select * from x join y on (kk) order by kk, b1")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testJoinInnerLong256AndCharAndOrder() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    kk\ta\tb\tkk1\ta1\tb1
                    4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM
                    4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x9a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d645277ee62a5a6e9fb\tZ
                    4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x9b27eba5e9cfa1e29660300cea7db540954a62eca44acb2d71660a9b0890a2f0\tJ
                    4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0xc736a8b67656c4f159d574d2ff5fb1e3687a84abb7bfac3ebedf29efb28cdcb1\tC
                    5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI
                    5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0x37b4f6e41fbfd55f587274e3ab1ebd4d6cecb916a1ad092b997918f622d62989\tS
                    5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0x3c5d8a6969daa0b37d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46\tE
                    5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t5\t0xba37e200ad5b17cdada00dc8b85c1bc8a5f80be4b45bf437492990e1a29afcac\tG
                    3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG
                    3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0x3ad08d6037d3ce8155c06051ee52138b655f87a3a21d575f610f69efe063fe79\tS
                    3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0x4cd64b0b0a344f8e6698c6c186b7571a9cba3ef59083484d98c2d832d83de993\tR
                    3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t3\t0xbacd57f41b59057caa237cfb02a208e494cfe42988a633de738bab883dc7e332\tU
                    1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY
                    1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP
                    1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0x8a538661f350d0b46f06560981acb5496adc00ebd29fdd5373dee145497c5436\tH
                    1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1\t0xc718ab5cbb3fd261c1bf6c24be53876861b1a0b0a559551538b73d329210d277\tY
                    2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x10bb226eb4243e3683b91ec970b04e788a50f7ff7f6ed3305705e75fe328fa9d\tE
                    2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x4c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39bccb30ed7795ebc8\tJ
                    2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x58dfd08eeb9cc39ecec82869edec121bc2593f82b430328d84a09f29df637e38\tB
                    2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP
                    """;

            execute(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_long256() a," +
                            " rnd_char() b " +
                            " from long_sequence(5))"
            );

            execute(
                    "create table y as (select" +
                            " cast((x-1)/4 + 1 as int) kk," +
                            " rnd_long256() a," +
                            " rnd_char() b " +
                            " from long_sequence(20))"
            );

            // filter is applied to final join result
            assertQuery("select * from x join y on (kk) order by x.a, x.b, y.a")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testJoinInnerNoSlaveRecords() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    c\ta\tb
                    2\t568\t16
                    2\t568\t72
                    4\t371\t3
                    4\t371\t14
                    6\t439\t12
                    6\t439\t81
                    8\t521\t16
                    8\t521\t97
                    10\t598\t5
                    10\t598\t74
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(10))");
            execute("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))");

            assertQuery("select x.c, x.a, b from x join y on y.m = x.c order by 1,2,3")
                    .noLeakCheck()
                    .returns(expected);

            execute("insert into x select cast(x+10 as int) c, abs(rnd_int() % 650) a from long_sequence(4)");
            execute("insert into y select x, cast(2*((x-1+10)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(6)");

            assertQuery("select x.c, x.a, b from x join y on y.m = x.c order by 1,2,3")
                    .noLeakCheck()
                    .returns(expected +
                            "12\t347\t0\n" +
                            "12\t347\t7\n" +
                            "14\t197\t50\n" +
                            "14\t197\t68\n");
        });
    }

    @Test
    public void testJoinInnerNoSlaveRecordsFF() throws Exception {
        testFullFat(this::testJoinInnerNoSlaveRecords0);
    }

    @Test
    public void testJoinInnerOnComputedRegexDoesNotPublishColumnFact() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (id INT, x STRING, y STRING)");
            execute("INSERT INTO a VALUES (1, 'foo', 'bar'), (2, 'FOO', 'zzz'), (3, null, 'foo')");
            execute("CREATE TABLE b (id INT, x STRING, y STRING)");
            execute("INSERT INTO b VALUES (1, 'foo', 'bar'), (2, 'FOO', 'zzz'), (3, null, 'foo')");

            final String concatOnSql = """
                    SELECT a.id
                    FROM a
                    JOIN b ON b.id = a.id AND b.y = a.y AND (a.x || a.y) ~ '^foobar$'
                    ORDER BY a.id
                    """;
            final String concatWhereSql = """
                    SELECT a.id
                    FROM a
                    JOIN b ON b.id = a.id AND b.y = a.y
                    WHERE (a.x || a.y) ~ '^foobar$'
                    ORDER BY a.id
                    """;
            final String coalesceSql = """
                    SELECT a.id
                    FROM a
                    JOIN b ON b.id = a.id AND b.x = a.x AND coalesce(a.x, 'foo') ~ '^foo$'
                    ORDER BY a.id
                    """;
            final String lowerSql = """
                    SELECT a.id
                    FROM a
                    JOIN b ON b.id = a.id AND b.x = a.x AND lower(a.x) ~ '^foo$'
                    ORDER BY a.id
                    """;
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                assertQuery(concatOnSql)
                        .noLeakCheck()
                        .expectSize(isFullFatJoin)
                        .fullFatJoins(isFullFatJoin)
                        .sizeMayVary(!isFullFatJoin)
                        .returns("id\n1\n");
                assertQuery(concatWhereSql)
                        .noLeakCheck()
                        .expectSize(isFullFatJoin)
                        .fullFatJoins(isFullFatJoin)
                        .sizeMayVary(!isFullFatJoin)
                        .returns("id\n1\n");
                assertQuery(coalesceSql)
                        .noLeakCheck()
                        .expectSize(isFullFatJoin)
                        .fullFatJoins(isFullFatJoin)
                        .sizeMayVary(!isFullFatJoin)
                        .returns("id\n1\n3\n");
                assertQuery(lowerSql)
                        .noLeakCheck()
                        .expectSize(isFullFatJoin)
                        .fullFatJoins(isFullFatJoin)
                        .sizeMayVary(!isFullFatJoin)
                        .returns("id\n1\n2\n");
            }

            final String directRegexSql = """
                    SELECT a.id
                    FROM a
                    JOIN b ON b.id = a.id AND b.x = a.x AND a.x ~ '^foo$'
                    ORDER BY a.id
                    """;
            assertQuery(directRegexSql)
                    .noLeakCheck()
                    .sizeMayVary()
                    .withPlanContaining(
                            "                Async Filter workers: 1\n"
                                    + "                  filter: x ~ ^foo$\n"
                                    + "                    PageFrame\n"
                                    + "                        Row forward scan\n"
                                    + "                        Frame forward scan on: b"
                    )
                    .returns("id\n1\n");
            assertQuery(directRegexSql)
                    .noLeakCheck()
                    .expectSize()
                    .fullFatJoins()
                    .returns("id\n1\n");
        });
    }

    @Test
    public void testJoinInnerOnConstAfterNullingJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT, x INT)");
            execute("INSERT INTO a VALUES (1, 5)");
            execute("CREATE TABLE b (x INT)");
            execute("INSERT INTO b VALUES (null)");
            execute("CREATE TABLE c (k INT)");
            execute("INSERT INTO c VALUES (2)");

            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String joinType : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                    for (String predicate : new String[]{"a.x = 5", "5 = a.x"}) {
                        assertQuery("""
                                SELECT a.x AS ax, c.k AS ck, b.x AS bx
                                FROM a
                                %s c ON c.k = a.k
                                JOIN b ON b.x = a.x AND %s
                                """.formatted(joinType, predicate))
                                .noLeakCheck()
                                .fullFatJoins(isFullFatJoin)
                                .noRandomAccess()
                                .returns("ax\tck\tbx\n");
                    }
                }
            }
        });
    }

    @Test
    public void testJoinInnerOnConstAfterNullingJoinDoesNotPruneEarlierPeer() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT, x INT)");
            execute("INSERT INTO a VALUES (1, 4)");
            execute("CREATE TABLE b (x INT)");
            execute("INSERT INTO b VALUES (NULL)");
            execute("CREATE TABLE c (k INT)");
            execute("INSERT INTO c VALUES (1), (2)");
            execute("CREATE TABLE d (x INT)");
            execute("INSERT INTO d VALUES (4)");

            final String expectedNull = "k\tax\tdx\tbx\n2\tnull\tnull\tnull\n";
            final String expectedValue = "k\tax\tdx\tbx\n1\t4\t4\t4\n";
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String joinType : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                    for (String predicate : new String[]{
                            "a.x = :v",
                            ":v::INT = a.x",
                            "a.x = (:v + 0)",
                            "(:v + 0) = a.x"
                    }) {
                        final String sql = """
                                SELECT c.k, a.x AS ax, d.x AS dx, b.x AS bx
                                FROM a
                                JOIN d ON d.x = a.x
                                %s c ON c.k = a.k
                                JOIN b ON b.x = a.x AND %s
                                ORDER BY c.k
                                """.formatted(joinType, predicate);
                        bindVariableService.clear();
                        bindVariableService.setInt("v", Numbers.INT_NULL);
                        assertQuery(sql)
                                .noLeakCheck()
                                .fullFatJoins(isFullFatJoin)
                                .returns(expectedNull);

                        execute("INSERT INTO b VALUES (4)");
                        bindVariableService.setInt("v", 4);
                        assertQuery(sql)
                                .noLeakCheck()
                                .fullFatJoins(isFullFatJoin)
                                .returns(expectedValue);
                        execute("TRUNCATE TABLE b");
                        execute("INSERT INTO b VALUES (NULL)");
                    }
                }
            }

            execute("INSERT INTO b VALUES (4)");
            for (String predicate : new String[]{
                    "a.x = 4",
                    "4 = a.x",
                    "a.x = (2 + 2)",
                    "(2 + 2) = a.x"
            }) {
                assertQuery("""
                        SELECT c.k, a.x AS ax, d.x AS dx, b.x AS bx
                        FROM a
                        JOIN d ON d.x = a.x
                        RIGHT JOIN c ON c.k = a.k
                        JOIN b ON b.x = a.x AND %s
                        ORDER BY c.k
                        """.formatted(predicate))
                        .noLeakCheck()
                        .withPlanContaining(
                                "                        Hash\n"
                                        + "                            Async JIT Filter workers: 1\n"
                                        + "                              filter: x=4\n"
                                        + "                                PageFrame\n"
                                        + "                                    Row forward scan\n"
                                        + "                                    Frame forward scan on: d"
                        )
                        .returns(expectedValue);
            }

            execute("CREATE TABLE ab (k INT, x BOOLEAN)");
            execute("INSERT INTO ab VALUES (1, TRUE)");
            execute("CREATE TABLE bb (x BOOLEAN)");
            execute("INSERT INTO bb VALUES (FALSE)");
            execute("CREATE TABLE db (x BOOLEAN)");
            execute("INSERT INTO db VALUES (TRUE)");
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String joinType : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                    assertQuery("""
                            SELECT c.k, ab.x AS ax, db.x AS dx, bb.x AS bx
                            FROM ab
                            JOIN db ON db.x = ab.x
                            %s c ON c.k = ab.k
                            JOIN bb ON bb.x = ab.x AND ab.x = FALSE
                            ORDER BY c.k
                            """.formatted(joinType))
                            .noLeakCheck()
                            .fullFatJoins(isFullFatJoin)
                            .returns("k\tax\tdx\tbx\n2\tfalse\tfalse\tfalse\n");
                }
            }

            execute("CREATE TABLE aby (k INT, x BYTE)");
            execute("INSERT INTO aby VALUES (1, 1::BYTE)");
            execute("CREATE TABLE bby (x BYTE)");
            execute("INSERT INTO bby VALUES (0::BYTE)");
            execute("CREATE TABLE dby (x BYTE)");
            execute("INSERT INTO dby VALUES (1::BYTE)");
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String joinType : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                    assertQuery("""
                            SELECT c.k, aby.x AS ax, dby.x AS dx, bby.x AS bx
                            FROM aby
                            JOIN dby ON dby.x = aby.x
                            %s c ON c.k = aby.k
                            JOIN bby ON bby.x = aby.x AND aby.x = 0::BYTE
                            ORDER BY c.k
                            """.formatted(joinType))
                            .noLeakCheck()
                            .fullFatJoins(isFullFatJoin)
                            .returns("k\tax\tdx\tbx\n2\t0\t0\t0\n");
                }
            }

            execute("CREATE TABLE ach (k INT, x CHAR)");
            execute("INSERT INTO ach VALUES (1, 'A')");
            execute("CREATE TABLE bch (x CHAR)");
            execute("INSERT INTO bch VALUES (''::CHAR)");
            execute("CREATE TABLE dch (x CHAR)");
            execute("INSERT INTO dch VALUES ('A')");
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String joinType : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                    assertQuery("""
                            SELECT c.k, a.x AS ax, d.x AS dx, b.x AS bx
                            FROM ach a
                            JOIN dch d ON d.x = a.x
                            %s c ON c.k = a.k
                            JOIN bch b ON b.x = a.x AND a.x = ''::CHAR
                            ORDER BY c.k
                            """.formatted(joinType))
                            .noLeakCheck()
                            .fullFatJoins(isFullFatJoin)
                            .returns("k\tax\tdx\tbx\n2\t\t\t\n");
                }
            }

            execute("CREATE TABLE ai (k INT, x IPv4)");
            execute("INSERT INTO ai VALUES (1, '1.1.1.1')");
            execute("CREATE TABLE bi (x IPv4)");
            execute("INSERT INTO bi VALUES (NULL)");
            execute("CREATE TABLE di (x IPv4)");
            execute("INSERT INTO di VALUES ('1.1.1.1')");
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String joinType : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                    assertQuery("""
                            SELECT c.k, ai.x AS ax, di.x AS dx, bi.x AS bx
                            FROM ai
                            JOIN di ON di.x = ai.x
                            %s c ON c.k = ai.k
                            JOIN bi ON bi.x = ai.x AND ai.x = '0.0.0.0'
                            ORDER BY c.k
                            """.formatted(joinType))
                            .noLeakCheck()
                            .fullFatJoins(isFullFatJoin)
                            .returns("k\tax\tdx\tbx\n2\t\t\t\n");
                }
            }

            execute("INSERT INTO bi VALUES ('1.1.1.1')");
            assertQuery("""
                    SELECT c.k, ai.x AS ax, di.x AS dx, bi.x AS bx
                    FROM ai
                    JOIN di ON di.x = ai.x
                    RIGHT JOIN c ON c.k = ai.k
                    JOIN bi ON bi.x = ai.x AND ai.x = '1.1.1.1'
                    ORDER BY c.k
                    """)
                    .noLeakCheck()
                    .withPlanContaining("filter: x='1.1.1.1'", "Frame forward scan on: di")
                    .returns("k\tax\tdx\tbx\n1\t1.1.1.1\t1.1.1.1\t1.1.1.1\n");

            assertQuery("""
                    SELECT * FROM (
                        SELECT c.k, ai.x AS ax, di.x AS dx, bi.x AS bx
                        FROM ai
                        JOIN di ON di.x = ai.x
                        RIGHT JOIN c ON c.k = ai.k
                        JOIN bi ON bi.x = ai.x
                    ) WHERE ax = '0.0.0.0'
                    ORDER BY k
                    """)
                    .noLeakCheck()
                    .returns("k\tax\tdx\tbx\n2\t\t\t\n");

            execute("CREATE TABLE au (k INT, x UUID)");
            execute("INSERT INTO au VALUES (1, '00000000-0000-0000-0000-000000000001')");
            execute("CREATE TABLE bu (x UUID)");
            execute("INSERT INTO bu VALUES ('00000000-0000-0000-0000-000000000001')");
            execute("CREATE TABLE du (x UUID)");
            execute("INSERT INTO du VALUES ('00000000-0000-0000-0000-000000000001')");
            assertQuery("""
                    SELECT c.k, au.x AS ax, du.x AS dx, bu.x AS bx
                    FROM au
                    JOIN du ON du.x = au.x
                    RIGHT JOIN c ON c.k = au.k
                    JOIN bu ON bu.x = au.x AND au.x = '00000000-0000-0000-0000-000000000001'
                    ORDER BY c.k
                    """)
                    .noLeakCheck()
                    .returns("k\tax\tdx\tbx\n1\t00000000-0000-0000-0000-000000000001\t00000000-0000-0000-0000-000000000001\t00000000-0000-0000-0000-000000000001\n");
        });
    }

    @Test
    public void testJoinInnerOnConstBeforeLaterNullingJoinUsesIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pa AS (SELECT 'foo'::SYMBOL AS x, 1::INT AS y FROM long_sequence(1))");
            execute("""
                    CREATE TABLE pb AS (
                        SELECT (CASE WHEN x = 1 THEN 'foo' ELSE 'bar' END)::SYMBOL AS x
                        FROM long_sequence(4)
                    ), INDEX(x)
                    """);
            execute("CREATE TABLE pc AS (SELECT 1::INT AS y FROM long_sequence(1))");

            for (String joinType : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                assertQuery("""
                        SELECT count(*) AS row_count
                        FROM pa
                        JOIN pb ON pb.x = pa.x AND pa.x = 'foo'
                        %s pc ON pc.y = pa.y
                        """.formatted(joinType))
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining(
                                "Hash Join Light",
                                "condition: pb.x=pa.x",
                                "DeferredSingleSymbolFilterPageFrame",
                                "Index forward scan on: x",
                                "Frame forward scan on: pb"
                        )
                        .returns("row_count\n1\n");
            }
        });
    }

    @Test
    public void testJoinInnerOnConstBeforeNullingJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, ts TIMESTAMP, av INT)");
            execute("INSERT INTO a VALUES (5, 5, 10)");
            execute("CREATE TABLE b (x INT, bv INT)");
            execute("INSERT INTO b VALUES (5, 20)");
            execute("CREATE TABLE c (x INT, ts TIMESTAMP, cv INT)");
            execute("INSERT INTO c VALUES (5, 5, 50), (6, 6, 60)");

            final String expected = "av\tbv\tcv\n10\t20\t50\nnull\tnull\t60\n";
            final String nullBindExpected = "av\tbv\tcv\nnull\tnull\t50\nnull\tnull\t60\n";
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String joinType : Arrays.asList("RIGHT JOIN", "FULL JOIN")) {
                    final String intFrom = " FROM a JOIN b ON b.x = a.x AND %s "
                            + joinType + " c ON c.x = a.x ORDER BY c.cv";
                    bindVariableService.clear();
                    bindVariableService.setInt("v", 5);
                    for (String predicate : Arrays.asList(
                            "a.x = :v::INT",
                            ":v::INT = a.x",
                            "a.x = (:v + 0)",
                            "(:v + 0) = a.x"
                    )) {
                        assertQuery("SELECT a.av, b.bv, c.cv" + intFrom.formatted(predicate))
                                .noLeakCheck()
                                .fullFatJoins(isFullFatJoin)
                                .returns(expected);
                    }

                    bindVariableService.setInt("v", Numbers.INT_NULL);
                    assertQuery("SELECT a.av, b.bv, c.cv" + intFrom.formatted("a.x = :v::INT"))
                            .noLeakCheck()
                            .fullFatJoins(isFullFatJoin)
                            .returns(nullBindExpected);

                    sqlExecutionContext.setNowAndFixClock(5, ColumnType.TIMESTAMP_MICRO);
                    final String timestampFrom = " FROM a JOIN b ON b.x = a.x AND %s "
                            + joinType + " c ON c.ts = a.ts ORDER BY c.cv";
                    for (String predicate : Arrays.asList("a.ts = now()", "now() = a.ts")) {
                        assertQuery("SELECT a.av, b.bv, c.cv" + timestampFrom.formatted(predicate))
                                .noLeakCheck()
                                .fullFatJoins(isFullFatJoin)
                                .returns(expected);
                    }
                }
            }
        });
    }

    @Test
    public void testJoinInnerOnConstBeforeReorderedNonEquiNullingJoinUsesIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE anchor (z INT, y INT)");
            execute("INSERT INTO anchor VALUES (1, 10)");
            execute("CREATE TABLE preserved (y INT)");
            execute("INSERT INTO preserved VALUES (5)");
            execute("CREATE TABLE pa_ne AS (SELECT 'foo'::SYMBOL AS x, 1::INT AS z FROM long_sequence(1))");
            execute("""
                    CREATE TABLE pb_ne AS (
                        SELECT (CASE WHEN x = 1 THEN 'foo' ELSE 'bar' END)::SYMBOL AS x
                        FROM long_sequence(4)
                    ), INDEX(x)
                    """);

            for (String joinType : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                assertQuery("""
                        SELECT count(*) AS row_count
                        FROM anchor a
                        %s preserved n ON a.y > n.y
                        JOIN pa_ne ON pa_ne.z = a.z
                        JOIN pb_ne ON pb_ne.x = pa_ne.x AND 'foo' = pa_ne.x
                        """.formatted(joinType))
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining(
                                joinType.equals("RIGHT JOIN") ? "Nested Loop Right Join" : "Nested Loop Full Join",
                                "condition: pb_ne.x=pa_ne.x",
                                "DeferredSingleSymbolFilterPageFrame",
                                "Index forward scan on: x",
                                "Frame forward scan on: pb_ne"
                        )
                        .returns("row_count\n1\n");
            }
        });
    }

    @Test
    public void testJoinInnerOnConstBetweenStackedNullingJoins() throws Exception {
        // qd runs between two joins that NULL-extend qa. The nearest qc boundary must block the
        // MODEL_ON constant from pruning qd; using only the outermost qe boundary loses qd's NULL row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE qa (k INT, x INT)");
            execute("INSERT INTO qa VALUES (1, 5)");
            execute("CREATE TABLE qb (k INT)");
            execute("INSERT INTO qb VALUES (1)");
            execute("CREATE TABLE qc (k INT)");
            execute("INSERT INTO qc VALUES (1), (2)");
            execute("CREATE TABLE qd (x INT, ck INT, v INT)");
            execute("INSERT INTO qd VALUES (5, 1, 10), (NULL, 2, 20)");
            execute("CREATE TABLE qe (k INT)");
            execute("INSERT INTO qe VALUES (1), (2)");

            final String expected = """
                    ck\tdv\tek
                    1\t10\t1
                    2\t20\t2
                    """;
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String firstNullingJoin : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                    for (String secondNullingJoin : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                        assertQuery("""
                                SELECT qc.k AS ck, qd.v AS dv, qe.k AS ek
                                FROM qa
                                JOIN qb ON qb.k = qa.k AND qa.x = 5
                                %s qc ON qc.k = qa.k
                                JOIN qd ON qd.x = qa.x AND qd.ck = qc.k
                                %s qe ON qe.k = qc.k
                                ORDER BY qe.k
                                """.formatted(firstNullingJoin, secondNullingJoin))
                                .noLeakCheck()
                                .fullFatJoins(isFullFatJoin)
                                .returns(expected);
                    }
                }
            }
        });
    }

    @Test
    public void testJoinInnerOnConstDoesNotReplaceWhereConstBeforeNullingJoinUsesIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT 'foo'::SYMBOL AS x FROM long_sequence(1))");
            execute("CREATE TABLE b AS (SELECT 'foo'::SYMBOL AS x FROM long_sequence(1))");
            execute("""
                    CREATE TABLE c AS (
                        SELECT (CASE WHEN x = 1 THEN 'foo' ELSE 'bar' END)::SYMBOL AS x
                        FROM long_sequence(4)
                    ), INDEX(x)
                    """);

            for (String joinType : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                final String sql = """
                        SELECT count(*) AS row_count
                        FROM a
                        JOIN b ON b.x = a.x AND a.x = 'foo'
                        %s c ON c.x = a.x
                        WHERE a.x = 'foo'
                        """.formatted(joinType);
                assertQuery(sql)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining(
                                "DeferredSingleSymbolFilterPageFrame",
                                "Index forward scan on: x",
                                "Frame forward scan on: c"
                        )
                        .returns("row_count\n1\n");
                // QueryAssertion intentionally disallows plan assertions with a custom compiler,
                // so exercise the full-fat path with the complete data assertion battery.
                assertQuery(sql)
                        .noLeakCheck()
                        .fullFatJoins()
                        .noRandomAccess()
                        .expectSize()
                        .returns("row_count\n1\n");
            }
        });
    }

    @Test
    public void testJoinInnerOnConstFromOlderModelUsesIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE fa (k INT, x SYMBOL INDEX)");
            execute("INSERT INTO fa VALUES (1, 'foo')");
            execute("CREATE TABLE fb (k INT)");
            execute("INSERT INTO fb VALUES (1)");
            execute("CREATE TABLE fc (k INT, x SYMBOL)");
            execute("INSERT INTO fc VALUES (1, 'bar')");
            execute("CREATE TABLE fd (k INT)");
            execute("INSERT INTO fd VALUES (1)");
            execute("CREATE TABLE ft (x SYMBOL INDEX)");
            execute("INSERT INTO ft VALUES ('foo'), ('zzz')");

            final String sql = """
                    SELECT count(*) AS row_count
                    FROM fa
                    JOIN fb ON fb.k = fa.k AND fa.x = 'foo'
                    CROSS JOIN fc
                    JOIN fd ON fd.k = fc.k AND fc.x = 'bar'
                    JOIN ft ON ft.x = fa.x
                    """;
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining(
                            "            DeferredSingleSymbolFilterPageFrame\n"
                                    + "                Index forward scan on: x\n"
                                    + "                  filter: x=1\n"
                                    + "                Frame forward scan on: ft"
                    )
                    .returns("row_count\n1\n");
            assertQuery(sql)
                    .noLeakCheck()
                    .fullFatJoins()
                    .noRandomAccess()
                    .expectSize()
                    .returns("row_count\n1\n");
        });
    }

    @Test
    public void testJoinInnerOnDerivedTablePredicateAfterNullingJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (dummy INT)");
            execute("INSERT INTO o VALUES (1)");
            execute("CREATE TABLE b (k INT, y INT)");
            execute("INSERT INTO b VALUES (1, -1)");
            execute("CREATE TABLE n (k INT)");
            execute("INSERT INTO n VALUES (1)");

            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String joinType : new String[]{"RIGHT", "FULL"}) {
                    assertQuery("""
                            SELECT o.dummy, v.y, v.nk
                            FROM o
                            JOIN (
                                SELECT b.y, n.k AS nk
                                FROM b
                                %s JOIN n ON n.k = b.k
                            ) v ON v.y > 0
                            """.formatted(joinType))
                            .noLeakCheck()
                            .fullFatJoins(isFullFatJoin)
                            .noRandomAccess()
                            .returns("dummy\ty\tnk\n");
                }
            }
        });
    }

    @Test
    public void testJoinInnerOnLiteralPushedIntoViewUsesIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE outer_t (ok SYMBOL)");
            execute("INSERT INTO outer_t VALUES ('x')");
            execute("CREATE TABLE ta (akey SYMBOL INDEX, av INT)");
            execute("INSERT INTO ta VALUES ('x', 10)");
            execute("CREATE TABLE tb (bkey SYMBOL INDEX, bv INT)");
            execute("INSERT INTO tb VALUES ('x', 20), ('y', 21)");
            execute("CREATE TABLE tc (ckey SYMBOL INDEX, cv INT)");
            execute("INSERT INTO tc VALUES ('x', 30), ('y', 31)");
            execute("CREATE TABLE tn (nkey SYMBOL INDEX, nv INT)");
            execute("INSERT INTO tn VALUES ('x', 40), ('z', 41)");
            execute("""
                    CREATE VIEW v AS (
                        SELECT b.bkey AS k, a.av, b.bv, c.cv, n.nv
                        FROM ta a
                        JOIN tb b ON b.bkey = a.akey
                        RIGHT JOIN tn n ON n.nkey = b.bkey
                        JOIN tc c ON c.ckey = b.bkey
                    )
                    """);

            assertQuery("""
                    SELECT count(*) AS row_count
                    FROM outer_t o
                    JOIN v ON v.k = o.ok AND v.k = 'x'
                    """)
                    .expectSize()
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining(
                            "condition: c.ckey=b.bkey",
                            "DeferredSingleSymbolFilterPageFrame",
                            "Index forward scan on: ckey",
                            "Frame forward scan on: tc"
                    )
                    .returns("row_count\n1\n");
        });
    }

    @Test
    public void testJoinInnerOnNullAcceptingGateAfterNullingJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT, x INT)");
            execute("INSERT INTO a VALUES (1, 5)");
            execute("CREATE TABLE c (k INT)");
            execute("INSERT INTO c VALUES (1)");
            execute("CREATE TABLE b (k INT)");
            execute("INSERT INTO b VALUES (1)");

            bindVariableService.clear();
            bindVariableService.setInt("v", Numbers.INT_NULL);
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String nullingJoin : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                    for (String predicate : new String[]{"a.x IS NULL", "a.x = :v::INT"}) {
                        assertQuery("""
                                SELECT a.x AS ax, c.k AS ck, b.k AS bk
                                FROM a
                                %s c ON c.k = a.k
                                JOIN b ON b.k = c.k AND %s
                                """.formatted(nullingJoin, predicate))
                                .noLeakCheck()
                                .fullFatJoins(isFullFatJoin)
                                .noRandomAccess()
                                .returns("ax\tck\tbk\n");
                    }
                }
            }
        });
    }

    @Test
    public void testJoinInnerOnNullAcceptingGateAfterReorderedNonEquiNullingJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT, x INT)");
            execute("INSERT INTO a VALUES (1, 10)");
            execute("CREATE TABLE c (k INT)");
            execute("INSERT INTO c VALUES (1)");
            execute("CREATE TABLE b (k INT)");
            execute("INSERT INTO b VALUES (1)");

            bindVariableService.clear();
            bindVariableService.setInt("v", Numbers.INT_NULL);
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String nullingJoin : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                    for (String predicate : new String[]{"a.x IS NULL", "a.x = :v::INT"}) {
                        assertQuery("""
                                SELECT a.k AS ak, c.k AS ck, b.k AS bk
                                FROM a
                                %s c ON c.k >= a.k
                                JOIN b ON b.k = a.k AND %s
                                """.formatted(nullingJoin, predicate))
                                .noLeakCheck()
                                .fullFatJoins(isFullFatJoin)
                                .noRandomAccess()
                                .returns("ak\tck\tbk\n");
                    }
                }
            }
        });
    }

    @Test
    public void testJoinInnerOnNullAcceptingGateAfterReorderedNonEquiNullingJoinWithSourceAfterBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT)");
            execute("INSERT INTO a VALUES (1)");
            execute("CREATE TABLE c (k INT)");
            execute("INSERT INTO c VALUES (1)");
            execute("CREATE TABLE p (k INT, x INT)");
            execute("INSERT INTO p VALUES (1, 10)");
            execute("CREATE TABLE b (k INT)");
            execute("INSERT INTO b VALUES (1)");

            bindVariableService.clear();
            bindVariableService.setInt("v", Numbers.INT_NULL);
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String nullingJoin : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                    for (String predicate : new String[]{"p.x IS NULL", "p.x = :v::INT"}) {
                        assertQuery("""
                                SELECT a.k AS ak, c.k AS ck, p.k AS pk, b.k AS bk
                                FROM a
                                %s c ON c.k >= a.k
                                CROSS JOIN p
                                JOIN b ON b.k = p.k AND %s
                                """.formatted(nullingJoin, predicate))
                                .noLeakCheck()
                                .fullFatJoins(isFullFatJoin)
                                .noRandomAccess()
                                .returns("ak\tck\tpk\tbk\n");
                    }
                }
            }
        });
    }

    @Test
    public void testJoinInnerOnRegexAfterConstBeforeNullingJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x STRING, av INT)");
            execute("INSERT INTO a VALUES ('5', 10)");
            execute("CREATE TABLE b (x STRING, bv INT)");
            execute("INSERT INTO b VALUES ('5', 20)");
            execute("CREATE TABLE c (x STRING, cv INT)");
            execute("INSERT INTO c VALUES ('5', 50), (null, 60)");

            assertQuery("""
                    SELECT a.av, b.bv, c.cv
                    FROM a
                    JOIN b ON b.x = a.x AND a.x = '5' AND a.x ~ '^z$'
                    RIGHT JOIN c ON c.x = a.x
                    ORDER BY c.cv
                    """)
                    .noLeakCheck()
                    .returns("av\tbv\tcv\nnull\tnull\t50\nnull\tnull\t60\n");
        });
    }

    @Test
    public void testJoinInnerOnRegexBeforeConstBeforeNullingJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x STRING, av INT)");
            execute("INSERT INTO a VALUES ('5', 10)");
            execute("CREATE TABLE b (x STRING, bv INT)");
            execute("INSERT INTO b VALUES ('5', 20)");
            execute("CREATE TABLE c (x STRING, cv INT)");
            execute("INSERT INTO c VALUES ('5', 50), (null, 60)");

            final String expected = "av\tbv\tcv\nnull\tnull\t50\nnull\tnull\t60\n";
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String joinType : Arrays.asList("RIGHT JOIN", "FULL JOIN")) {
                    for (String equality : Arrays.asList("a.x = '5'", "'5' = a.x")) {
                        assertQuery("""
                                SELECT a.av, b.bv, c.cv
                                FROM a
                                JOIN b ON b.x = a.x AND a.x ~ '^z$' AND %s
                                %s c ON c.x = a.x
                                ORDER BY c.cv
                                """.formatted(equality, joinType))
                                .noLeakCheck()
                                .fullFatJoins(isFullFatJoin)
                                .returns(expected);
                    }
                }
            }
        });
    }

    @Test
    public void testJoinInnerOnSingleModelPredicateAfterEarlierJoinBarrier() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE z (k INT)");
            execute("INSERT INTO z VALUES (1)");
            execute("CREATE TABLE a (k INT, x INT, y INT)");
            execute("INSERT INTO a VALUES (1, 5, 4)");
            execute("CREATE TABLE c (k INT)");
            execute("INSERT INTO c VALUES (2)");
            execute("CREATE TABLE b (x INT)");
            execute("INSERT INTO b VALUES (NULL)");

            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String nullingJoin : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                    for (String predicate : new String[]{
                            "a.x > 0",
                            "a.x = 5",
                            "5 = a.x",
                            "(a.x > 0 OR a.y > 0)",
                            "a.x > a.y"
                    }) {
                        assertQuery("""
                                SELECT z.k AS zk, a.x AS ax, c.k AS ck, b.x AS bx
                                FROM z
                                LEFT JOIN a ON a.k = z.k
                                %s c ON c.k = z.k
                                JOIN b ON b.x = a.x AND %s
                                """.formatted(nullingJoin, predicate))
                                .noLeakCheck()
                                .fullFatJoins(isFullFatJoin)
                                .noRandomAccess()
                                .returns("zk\tax\tck\tbx\n");
                    }
                }
            }
        });
    }

    @Test
    public void testJoinInnerOnSymbol() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    xc\tzc\tyc\ta\tb\td\tcolumn
                    \t\t\t598\t3\t2\t-1
                    \t\t\t521\t3\t2\t-1
                    \t\t\t598\t68\t2\t-66
                    \t\t\t521\t68\t2\t-66
                    \t\t\t598\t69\t2\t-67
                    \t\t\t521\t69\t2\t-67
                    \t\t\t598\t53\t2\t-51
                    \t\t\t521\t53\t2\t-51
                    \t\t\t598\t3\t8\t5
                    \t\t\t521\t3\t8\t5
                    \t\t\t598\t68\t8\t-60
                    \t\t\t521\t68\t8\t-60
                    \t\t\t598\t69\t8\t-61
                    \t\t\t521\t69\t8\t-61
                    \t\t\t598\t53\t8\t-45
                    \t\t\t521\t53\t8\t-45
                    \t\t\t598\t3\t540\t537
                    \t\t\t521\t3\t540\t537
                    \t\t\t598\t68\t540\t472
                    \t\t\t521\t68\t540\t472
                    \t\t\t598\t69\t540\t471
                    \t\t\t521\t69\t540\t471
                    \t\t\t598\t53\t540\t487
                    \t\t\t521\t53\t540\t487
                    \t\t\t598\t3\t908\t905
                    \t\t\t521\t3\t908\t905
                    \t\t\t598\t68\t908\t840
                    \t\t\t521\t68\t908\t840
                    \t\t\t598\t69\t908\t839
                    \t\t\t521\t69\t908\t839
                    \t\t\t598\t53\t908\t855
                    \t\t\t521\t53\t908\t855
                    A\tA\tA\t568\t12\t263\t251
                    A\tA\tA\t568\t74\t263\t189
                    A\tA\tA\t568\t71\t263\t192
                    A\tA\tA\t568\t54\t263\t209
                    A\tA\tA\t568\t12\t319\t307
                    A\tA\tA\t568\t74\t319\t245
                    A\tA\tA\t568\t71\t319\t248
                    A\tA\tA\t568\t54\t319\t265
                    A\tA\tA\t568\t12\t456\t444
                    A\tA\tA\t568\t74\t456\t382
                    A\tA\tA\t568\t71\t456\t385
                    A\tA\tA\t568\t54\t456\t402
                    B\tB\tB\t439\t72\t467\t395
                    B\tB\tB\t371\t72\t467\t395
                    B\tB\tB\t439\t97\t467\t370
                    B\tB\tB\t371\t97\t467\t370
                    B\tB\tB\t439\t97\t467\t370
                    B\tB\tB\t371\t97\t467\t370
                    B\tB\tB\t439\t79\t467\t388
                    B\tB\tB\t371\t79\t467\t388
                    B\tB\tB\t439\t72\t667\t595
                    B\tB\tB\t371\t72\t667\t595
                    B\tB\tB\t439\t97\t667\t570
                    B\tB\tB\t371\t97\t667\t570
                    B\tB\tB\t439\t97\t667\t570
                    B\tB\tB\t371\t97\t667\t570
                    B\tB\tB\t439\t79\t667\t588
                    B\tB\tB\t371\t79\t667\t588
                    B\tB\tB\t439\t72\t703\t631
                    B\tB\tB\t371\t72\t703\t631
                    B\tB\tB\t439\t97\t703\t606
                    B\tB\tB\t371\t97\t703\t606
                    B\tB\tB\t439\t97\t703\t606
                    B\tB\tB\t371\t97\t703\t606
                    B\tB\tB\t439\t79\t703\t624
                    B\tB\tB\t371\t79\t703\t624
                    B\tB\tB\t439\t72\t842\t770
                    B\tB\tB\t371\t72\t842\t770
                    B\tB\tB\t439\t97\t842\t745
                    B\tB\tB\t371\t97\t842\t745
                    B\tB\tB\t439\t97\t842\t745
                    B\tB\tB\t371\t97\t842\t745
                    B\tB\tB\t439\t79\t842\t763
                    B\tB\tB\t371\t79\t842\t763
                    B\tB\tB\t439\t72\t933\t861
                    B\tB\tB\t371\t72\t933\t861
                    B\tB\tB\t439\t97\t933\t836
                    B\tB\tB\t371\t97\t933\t836
                    B\tB\tB\t439\t97\t933\t836
                    B\tB\tB\t371\t97\t933\t836
                    B\tB\tB\t439\t79\t933\t854
                    B\tB\tB\t371\t79\t933\t854
                    """;

            execute("create table x as (select rnd_symbol('A','B',null,'D') c, abs(rnd_int() % 650) a from long_sequence(5))");
            execute("create table y as (select rnd_symbol('B','A',null,'D') m, abs(rnd_int() % 100) b from long_sequence(20))");
            execute("create table z as (select rnd_symbol('D','B',null,'A') c, abs(rnd_int() % 1000) d from long_sequence(16))");

            // filter is applied to intermediate join result
            assertQuery("select x.c xc, z.c zc, y.m yc, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) order by x.c, d")
                    .noLeakCheck()
                    .returns(expected);

            execute("insert into x select rnd_symbol('L','K','P') c, abs(rnd_int() % 650) a from long_sequence(3)");
            execute("insert into y select rnd_symbol('P','L','K') m, abs(rnd_int() % 100) b from long_sequence(6)");
            execute("insert into z select rnd_symbol('K','P','L') c, abs(rnd_int() % 1000) d from long_sequence(6)");

            assertQuery("select x.c xc, z.c zc, y.m yc, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) order by x.c, d")
                    .noLeakCheck()
                    .returns(expected +
                            "L\tL\tL\t148\t38\t121\t83\n" +
                            "L\tL\tL\t148\t52\t121\t69\n");
        });
    }

    @Test
    public void testJoinInnerOnSymbolFF() throws Exception {
        testFullFat(this::testJoinInnerOnSymbol0);
    }

    @Test
    public void testJoinInnerOnUnaryComplementFailsWithSqlException() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (id INT)");
            execute("INSERT INTO a VALUES (-2)");
            execute("CREATE TABLE b (id INT)");
            execute("INSERT INTO b VALUES (1)");

            assertFailure(
                    "SELECT a.id FROM a JOIN b ON ~a.id",
                    "boolean expression expected",
                    29
            );
            assertQuery("SELECT a.id FROM a JOIN b ON (~a.id) > 0")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("id\n-2\n");
        });
    }

    @Test
    public void testJoinInnerPostJoinAndConstFilter() throws Exception {
        // Regression test for https://github.com/questdb/questdb/issues/6762
        // When WHERE has both a column-referencing condition (postJoinWhereClause)
        // and a non-column, non-constant condition (constWhereClause like NOW() = NOW()),
        // the optimizer merges them into a single postJoinWhereClause so the code
        // generator applies one filter instead of nesting FilteredRecordCursorFactory.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z'),
                    (3, '2024-01-03T00:00:00.000000Z')
                    """);
            assertQuery("SELECT T1.val, T2.val FROM t T1 " +
                    "INNER JOIN t T2 ON T1.ts < T2.ts " +
                    "WHERE T1.val > 0 AND NOW() = NOW()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val\tval1
                            1\t2
                            1\t3
                            2\t3
                            """);
        });
    }

    @Test
    public void testJoinInnerPostJoinAndMixedConstFilter() throws Exception {
        // When constWhereClause mixes compile-time and non-compile-time terms
        // (e.g. false AND NOW() = NOW()), the optimizer splits them: false stays
        // as constWhereClause and the code generator folds it to EmptyTableRecordCursorFactory.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, '2024-01-01T00:00:00.000000Z')");
            assertQuery("SELECT T1.val, T2.val FROM t T1 " +
                    "INNER JOIN t T2 ON T1.ts < T2.ts " +
                    "WHERE T1.val > 0 AND 1 > 10 AND NOW() = NOW()")
                    .noLeakCheck()
                    .expectSize()
                    .returns("val\tval1\n");
        });
    }

    @Test
    public void testJoinInnerPostJoinAndMixedConstTrueFilter() throws Exception {
        // When constWhereClause has true AND NOW() = NOW(), the optimizer merges
        // NOW() = NOW() into postJoinWhereClause and the code generator folds
        // the remaining constant true away.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z')
                    """);
            String query = "SELECT T1.val, T2.val FROM t T1 " +
                    "INNER JOIN t T2 ON T1.ts < T2.ts " +
                    "WHERE T1.val > 0 AND 1 < 10 AND NOW() = NOW()";
            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val\tval1
                            1\t2
                            """);
            // Verify: no Empty table (1 < 10 folded as constant true), and
            // now()=now() merged from constWhereClause into a post-join filter.
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Filter filter: (T1.ts<T2.ts and now()=now())
                                    Cross Join
                                        Async JIT Filter workers: 1
                                          filter: 0<val
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testJoinInnerPostJoinFilter() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    c\ta\tb\td\tcolumn
                    1\t120\t39\t0\t159
                    1\t120\t42\t0\t162
                    1\t120\t71\t0\t191
                    1\t120\t6\t0\t126
                    1\t120\t39\t50\t159
                    1\t120\t42\t50\t162
                    1\t120\t71\t50\t191
                    1\t120\t6\t50\t126
                    5\t251\t47\t198\t298
                    5\t251\t44\t198\t295
                    5\t251\t7\t198\t258
                    5\t251\t47\t279\t298
                    5\t251\t44\t279\t295
                    5\t251\t7\t279\t258
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))");
            execute("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))");
            execute("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(16))");

            // filter is applied to intermediate join result
            assertQuery("select z.c, x.a, b, d, a+b from x join y on y.m = x.c join z on (c) where a+b < 300 order by z.c, d")
                    .noLeakCheck()
                    .returns(expected);

            execute("insert into x select cast(x+6 as int) c, abs(rnd_int() % 650) a from long_sequence(3)");
            execute("insert into y select cast((x+19)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(16)");
            execute("insert into z select cast((x+15)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(2)");

            assertQuery("select z.c, x.a, b, d, a+b from x join y on y.m = x.c join z on (c) where a+b < 300 order by z.c, d")
                    .noLeakCheck()
                    .returns(expected +
                            "7\t253\t35\t228\t288\n" +
                            "7\t253\t14\t228\t267\n" +
                            "7\t253\t35\t723\t288\n" +
                            "7\t253\t14\t723\t267\n" +
                            "9\t100\t63\t456\t163\n" +
                            "9\t100\t19\t456\t119\n" +
                            "9\t100\t38\t456\t138\n" +
                            "9\t100\t8\t456\t108\n" +
                            "9\t100\t63\t667\t163\n" +
                            "9\t100\t19\t667\t119\n" +
                            "9\t100\t38\t667\t138\n" +
                            "9\t100\t8\t667\t108\n");

        });
    }

    @Test
    public void testJoinInnerPostJoinFilterFF() throws Exception {
        testFullFat(this::testJoinInnerPostJoinFilter0);
    }

    @Test
    public void testJoinInnerPostJoinMultipleJoinsFilter() throws Exception {
        // Tests multi-way join with post-join WHERE conditions referencing
        // columns from different join pairs.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t3 (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (10, '2024-01-02T00:00:00.000000Z'),
                    (20, '2024-01-03T00:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t3 VALUES
                    (100, '2024-01-03T00:00:00.000000Z'),
                    (200, '2024-01-04T00:00:00.000000Z')
                    """);
            assertQuery("SELECT a.val, b.val, c.val FROM t1 a " +
                    "INNER JOIN t2 b ON a.ts < b.ts " +
                    "INNER JOIN t3 c ON b.ts < c.ts " +
                    "WHERE a.val + b.val > 5 AND b.val + c.val > 50")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val\tval1\tval2
                            1\t10\t100
                            1\t10\t200
                            1\t20\t200
                            2\t20\t200
                            """);
        });
    }

    @Test
    public void testJoinInnerTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    c\ta\tb\td\tcolumn\tts
                    1\t120\t6\t50\t44\t2018-03-01T00:00:00.000001Z
                    1\t120\t6\t0\t-6\t2018-03-01T00:00:00.000001Z
                    1\t120\t39\t50\t11\t2018-03-01T00:00:00.000001Z
                    1\t120\t39\t0\t-39\t2018-03-01T00:00:00.000001Z
                    1\t120\t42\t50\t8\t2018-03-01T00:00:00.000001Z
                    1\t120\t42\t0\t-42\t2018-03-01T00:00:00.000001Z
                    1\t120\t71\t50\t-21\t2018-03-01T00:00:00.000001Z
                    1\t120\t71\t0\t-71\t2018-03-01T00:00:00.000001Z
                    2\t568\t14\t55\t41\t2018-03-01T00:00:00.000002Z
                    2\t568\t14\t968\t954\t2018-03-01T00:00:00.000002Z
                    2\t568\t16\t55\t39\t2018-03-01T00:00:00.000002Z
                    2\t568\t16\t968\t952\t2018-03-01T00:00:00.000002Z
                    2\t568\t48\t55\t7\t2018-03-01T00:00:00.000002Z
                    2\t568\t48\t968\t920\t2018-03-01T00:00:00.000002Z
                    2\t568\t72\t55\t-17\t2018-03-01T00:00:00.000002Z
                    2\t568\t72\t968\t896\t2018-03-01T00:00:00.000002Z
                    3\t333\t3\t305\t302\t2018-03-01T00:00:00.000003Z
                    3\t333\t3\t964\t961\t2018-03-01T00:00:00.000003Z
                    3\t333\t12\t305\t293\t2018-03-01T00:00:00.000003Z
                    3\t333\t12\t964\t952\t2018-03-01T00:00:00.000003Z
                    3\t333\t16\t305\t289\t2018-03-01T00:00:00.000003Z
                    3\t333\t16\t964\t948\t2018-03-01T00:00:00.000003Z
                    3\t333\t81\t305\t224\t2018-03-01T00:00:00.000003Z
                    3\t333\t81\t964\t883\t2018-03-01T00:00:00.000003Z
                    4\t371\t5\t104\t99\t2018-03-01T00:00:00.000004Z
                    4\t371\t5\t171\t166\t2018-03-01T00:00:00.000004Z
                    4\t371\t67\t104\t37\t2018-03-01T00:00:00.000004Z
                    4\t371\t67\t171\t104\t2018-03-01T00:00:00.000004Z
                    4\t371\t74\t104\t30\t2018-03-01T00:00:00.000004Z
                    4\t371\t74\t171\t97\t2018-03-01T00:00:00.000004Z
                    4\t371\t97\t104\t7\t2018-03-01T00:00:00.000004Z
                    4\t371\t97\t171\t74\t2018-03-01T00:00:00.000004Z
                    5\t251\t7\t198\t191\t2018-03-01T00:00:00.000005Z
                    5\t251\t7\t279\t272\t2018-03-01T00:00:00.000005Z
                    5\t251\t44\t198\t154\t2018-03-01T00:00:00.000005Z
                    5\t251\t44\t279\t235\t2018-03-01T00:00:00.000005Z
                    5\t251\t47\t198\t151\t2018-03-01T00:00:00.000005Z
                    5\t251\t47\t279\t232\t2018-03-01T00:00:00.000005Z
                    5\t251\t97\t198\t101\t2018-03-01T00:00:00.000005Z
                    5\t251\t97\t279\t182\t2018-03-01T00:00:00.000005Z
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x ts from long_sequence(5)) timestamp(ts)");
            execute("create table y as (select cast((x-1)/4 + 1 as int) c, abs(rnd_int() % 100) b from long_sequence(20))");
            execute("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))");

            assertQuery("select z.c, x.a, b, d, d-b, ts from x join y on(c) join z on (c) order by z.c, b")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testJoinKeysOfReorderedSecondTable() throws Exception {
        // The plan starts with t0 and joins t6 before t1, so t1's join runs over the join of t0 and
        // t6, not over t0 alone. Its key t1.c = t6.b must resolve against that join, where the bare
        // column name b is ambiguous.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT, c INT)");
            execute("CREATE TABLE t1 (a INT, b INT, c INT)");
            execute("CREATE TABLE t6 (a INT, b INT, c INT)");
            execute("CREATE TABLE t2 (a INT, b INT, c INT)");
            execute("INSERT INTO t0 VALUES (1, 1, 1)");
            execute("INSERT INTO t1 VALUES (3, 5, 1)");
            execute("INSERT INTO t6 VALUES (1, 1, 1)");
            execute("INSERT INTO t2 VALUES (9, 1, 2), (9, 1, 1)");

            // only t2's (9, 1, 1) row matches t2.c = t0.b
            final String from = " FROM t0 JOIN t1 ON t1.a = 3 JOIN t6 ON t6.a = t1.c AND t6.c = t0.c AND t6.b = t1.c"
                    + " JOIN t2 ON t2.b = t6.b AND t2.c = t0.b";
            assertQuery("SELECT t0.b t0b, t1.c t1c, t6.b t6b, t2.c t2c" + from)
                    .noLeakCheck()
                    .noRandomAccess()
                    // t6 joins t0, which leads the plan, and t1 joins after t6
                    .withPlanContaining("condition: t6.c=t0.c", "condition: t1.c=t6.b")
                    .returns("""
                            t0b\tt1c\tt6b\tt2c
                            1\t1\t1\t1
                            """);
            assertQuery("SELECT count(*)" + from)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            1
                            """);
        });
    }

    @Test
    public void testJoinMultiLevelViewWithDifferentColumnNames() throws Exception {
        // reproducer for: InvalidColumnException when joining a table with a
        // multi-level view where the ON clause uses different column names on
        // each side (t.c1 = v.max). Requires: (1) multi-level view chain with
        // a JOIN inside, (2) different column names in the outer join ON clause,
        // and (3) a WHERE clause on the master table's join column.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c1 INT, c2 INT)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
            execute("CREATE VIEW v1 AS (SELECT c2, max(c1) FROM t GROUP BY c2)");
            execute("CREATE VIEW v2 AS (SELECT v1.max, v1.c2 FROM t t0 LEFT JOIN v1 ON t0.c1 = v1.max)");

            assertQuery("SELECT v2.c2 FROM t t0 JOIN v2 ON t0.c1 = v2.max WHERE t0.c1 = 1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c2
                            10
                            """);
        });
    }

    @Test
    public void testJoinOfTablesWithReservedWordsColNames() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "select" +
                            " x as i, " +
                            " x*2 as \"in\", " +
                            " x*3 as \"from\" " +
                            " from long_sequence(3)" +
                            ")"
            );

            assertQuery("select \"in\", \"from\" from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            in\tfrom
                            2\t3
                            4\t6
                            6\t9
                            """);

            assertQuery("select x.\"in\", x.\"from\", x1.\"in\", x1.\"from\" " +
                    "from x " +
                    "join x as x1 on x.i = x1.i")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            in\tfrom\tin1\tfrom1
                            2\t3\t2\t3
                            4\t6\t4\t6
                            6\t9\t6\t9
                            """);

            assertQuery("select *, x.\"in\" + x1.\"from\" " +
                    "from x " +
                    "join x as x1 on x.i = x1.i")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tin\tfrom\ti1\tin1\tfrom1\tcolumn
                            1\t2\t3\t1\t2\t3\t5
                            2\t4\t6\t2\t4\t6\t10
                            3\t6\t9\t3\t6\t9\t15
                            """);
        });
    }

    @Test
    public void testJoinOnAllDecimal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table t1 (
                        val int,
                        dec8 decimal(2, 0),
                        dec16 decimal(4, 1),
                        dec32 decimal(9, 2),
                        dec64 decimal(18, 3),
                        dec128 decimal(38, 4),
                        dec256 decimal(76, 5),
                        ts timestamp
                    ) timestamp(ts)
                    """);
            execute("""
                    insert into t1 values
                    (1, 1m, 1.2m, 12.34m, 123.456m, 1234.5678m, 12345.6789m, '1970-01-01T00:00:00.000000Z'),
                    (2, 2m, 2.3m, 23.45m, 234.567m, 2345.6789m, 23456.78901m, '1970-01-01T00:00:01.000000Z'),
                    (3, 3m, 3.4m, 34.56m, 345.678m, 3456.789m, 34567.89012m, '1970-01-01T00:00:02.000000Z'),
                    (4, 4m, 4.5m, 45.67m, 456.789m, 4567.8901m, 45678.90123m, '1970-01-01T00:00:03.000000Z')
                    """);

            execute("""
                    create table t2 (
                        val int,
                        dec8 decimal(2, 0),
                        dec16 decimal(4, 1),
                        dec32 decimal(9, 2),
                        dec64 decimal(18, 3),
                        dec128 decimal(38, 4),
                        dec256 decimal(76, 5),
                        ts timestamp
                    ) timestamp(ts)
                    """);
            execute("""
                    insert into t2 values
                    (0, 5m, 5.6m, 56.78m, 567.89m, 5678.9012m, 56789.01234m, '1970-01-01T00:00:00.000000Z'),
                    (1, 6m, 6.7m, 67.89m, 678.901m, 6789.0123m, 67890.12345m, '1970-01-01T00:00:01.000000Z'),
                    (3, 7m, 7.8m, 78.9m, 789.012m, 7890.1234m, 78901.23456m, '1970-01-01T00:00:02.000000Z'),
                    (5, 8m, 8.9m, 89.01m, 890.123m, 8901.2345m, 89012.34567m, '1970-01-01T00:00:03.000000Z')
                    """);

            String expected = """
                    val\tdec8\tdec16\tdec32\tdec64\tdec128\tdec256\tts\tval1\tdec81\tdec161\tdec321\tdec641\tdec1281\tdec2561\tts1
                    1\t1\t1.2\t12.34\t123.456\t1234.5678\t12345.67890\t1970-01-01T00:00:00.000000Z\t1\t6\t6.7\t67.89\t678.901\t6789.0123\t67890.12345\t1970-01-01T00:00:01.000000Z
                    3\t3\t3.4\t34.56\t345.678\t3456.7890\t34567.89012\t1970-01-01T00:00:02.000000Z\t3\t7\t7.8\t78.90\t789.012\t7890.1234\t78901.23456\t1970-01-01T00:00:02.000000Z
                    """;

            String sql = "select * from t1 join t2 on t1.val = t2.val";

            assertQuery(sql)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testJoinOnClauseDeclaredColumnShorthand() throws Exception {
        // A declared variable bound to a bare column may be used as a shorthand join column, exactly
        // like the inline column it expands to. "ON (@c)" with "@c := symbol" behaves like
        // "ON (symbol)" -> "src.symbol = ref.symbol"; the variable is expanded before the join-column
        // dispatch instead of leaking a raw "@c" literal as "Invalid column: s.@c".
        assertMemoryLeak(() -> {
            execute("create table src (symbol symbol, ts timestamp) timestamp(ts) partition by day");
            execute("create table ref (symbol symbol, ts timestamp) timestamp(ts) partition by day");
            execute("insert into src values ('A', '2020-01-01T00:00:00.000000Z'), ('B', '2020-01-02T00:00:00.000000Z')");
            execute("insert into ref values ('A', '2020-01-01T00:00:00.000000Z'), ('B', '2020-01-02T00:00:00.000000Z')");

            final String expected = "symbol\n" +
                    "A\n" +
                    "B\n";

            // single-column shorthand with parentheses
            assertQuery(
                    "declare @c := symbol " +
                            "select s.symbol from src s join ref r on (@c) order by s.symbol"
            ).noLeakCheck().returns(expected);
            // single-column shorthand without parentheses
            assertQuery(
                    "declare @c := symbol " +
                            "select s.symbol from src s join ref r on @c order by s.symbol"
            ).noLeakCheck().returns(expected);
            // multi-column shorthand mixing a declared column var with a plain column
            assertQuery(
                    "declare @c := symbol " +
                            "select s.symbol from src s join ref r on (@c, ts) order by s.symbol"
            ).noLeakCheck().returns(expected);
            // baseline: the equivalent inline shorthand must produce the same result
            assertQuery(
                    "select s.symbol from src s join ref r on (symbol) order by s.symbol"
            ).noLeakCheck().returns(expected);
        });
    }

    @Test
    public void testJoinOnClauseRejectsDeclaredSubQuery() throws Exception {
        // ON-clause sub-queries are unsupported and rejected during expression parsing. A declared
        // variable is a literal at parse time and only expands to its definition later, in
        // rewriteKnownStatements, so a variable bound to a sub-query (e.g. "@q := (SELECT ...)" used
        // as "ON x IN @q") used to slip past the parse-time block and compile to surprising cross-join
        // semantics -- the very footgun the literal rejection prevents. The declared form must now
        // reject with "query is not allowed here", just like the literal one, at every nesting depth
        // and in every ON-clause position: operator forms, single-column shorthand "ON (@q)", and
        // multi-column shorthand "ON (@q, ts)" alike.
        assertMemoryLeak(() -> {
            execute("create table trades (symbol symbol, ts timestamp) timestamp(ts) partition by day");
            execute("create table src (symbol symbol, ts timestamp) timestamp(ts) partition by day");
            execute("create table ref (symbol symbol, ts timestamp) timestamp(ts) partition by day");
            execute("insert into src values ('A', '2020-01-01T00:00:00.000000Z'), ('B', '2020-01-02T00:00:00.000000Z')");
            execute("insert into ref values ('A', '2020-01-01T00:00:00.000000Z'), ('B', '2020-01-02T00:00:00.000000Z')");

            // declared sub-query in the ON clause of a join nested in an IN sub-query
            assertExceptionNoLeakCheck(
                    "select * from trades where symbol in " +
                            "(declare @q := (select symbol from trades) " +
                            "select s.symbol from src s join ref r on s.symbol in @q)",
                    53,
                    "query is not allowed here",
                    sqlExecutionContext
            );
            // the same shape at the top level (a pre-existing bypass, now also rejected)
            assertExceptionNoLeakCheck(
                    "declare @q := (select symbol from trades) " +
                            "select s.symbol from src s join ref r on s.symbol in @q",
                    15,
                    "query is not allowed here",
                    sqlExecutionContext
            );
            // a scalar operator with a declared sub-query operand hits the same rewrite path
            assertExceptionNoLeakCheck(
                    "declare @q := (select max(symbol) from trades) " +
                            "select s.symbol from src s join ref r on s.symbol = @q",
                    15,
                    "query is not allowed here",
                    sqlExecutionContext
            );
            // bare single-column shorthand "ON (@q)" -- declared var expands to a sub-query and is
            // rejected, instead of leaking a raw "@q" literal as "Invalid column: s.@q"
            assertExceptionNoLeakCheck(
                    "declare @q := (select symbol from trades) " +
                            "select s.symbol from src s join ref r on (@q)",
                    15,
                    "query is not allowed here",
                    sqlExecutionContext
            );
            // bare single-column shorthand without parentheses "ON @q"
            assertExceptionNoLeakCheck(
                    "declare @q := (select symbol from trades) " +
                            "select s.symbol from src s join ref r on @q",
                    15,
                    "query is not allowed here",
                    sqlExecutionContext
            );
            // multi-column shorthand "ON (@q, ts)" -- the column-list branch rejects the sub-query too
            assertExceptionNoLeakCheck(
                    "declare @q := (select symbol from trades) " +
                            "select s.symbol from src s join ref r on (@q, ts)",
                    15,
                    "query is not allowed here",
                    sqlExecutionContext
            );
            // single-column shorthand nested in an IN sub-query, to prove the reject holds at depth
            assertExceptionNoLeakCheck(
                    "select * from trades where symbol in " +
                            "(declare @q := (select symbol from trades) " +
                            "select s.symbol from src s join ref r on (@q))",
                    53,
                    "query is not allowed here",
                    sqlExecutionContext
            );

            // A declared variable bound to a column (not a sub-query) in the ON clause is valid and
            // must still compile and run -- the reject only fires on sub-query nodes.
            assertQuery(
                    "declare @x := s.symbol, @y := r.symbol " +
                            "select s.symbol from src s join ref r on @x = @y"
            ).noLeakCheck().noRandomAccess().returns(
                    "symbol\n" +
                            "A\n" +
                            "B\n"
            );

            // A rejected declared ON-clause sub-query must leave the shared parser state clean: the
            // SAME pooled compiler compiles the next, valid query without carrying over corrupted state.
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try {
                    CairoEngine.select(
                            compiler,
                            "declare @q := (select symbol from trades) " +
                                    "select s.symbol from src s join ref r on s.symbol in @q",
                            sqlExecutionContext
                    ).close();
                    Assert.fail("declared ON-clause sub-query must be rejected");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "query is not allowed here");
                }
                assertQuery(
                        "select s.symbol from src s join ref r on s.symbol = r.symbol"
                ).withCompiler(compiler).noLeakCheck().noRandomAccess().returns(
                        "symbol\n" +
                                "A\n" +
                                "B\n"
                );
            }
        });
    }

    @Test
    public void testJoinOnDecimalFailureMixedScale() throws Exception {
        // We don't support implicit casting between different decimals during join resolution
        assertMemoryLeak(() -> {
            execute("create table t1 (dec decimal(4, 2), ts timestamp) timestamp(ts)");
            execute("create table t2 (dec decimal(8, 4), ts timestamp) timestamp(ts)");

            try {
                assertQuery("select * from t1 join t2 on t1.dec = t2.dec")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("");
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "join column type mismatch");
            }
        });
    }

    @Test
    public void testJoinOnDecimalKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (dec decimal(4, 2), ts timestamp) timestamp(ts)");
            execute("""
                    insert into t1 (dec, ts) values
                    (1.1m, '1970-01-01T00:00:00.000000Z'),
                    (1.2m, '1970-01-01T00:00:01.000000Z'),
                    (1.3m, '1970-01-01T00:00:02.000000Z'),
                    (1.4m, '1970-01-01T00:00:03.000000Z')
                    """);

            execute("create table t2 (dec decimal(4, 2), ts timestamp) timestamp(ts)");
            execute("""
                    insert into t2 (dec, ts) values
                    (1.5m, '1970-01-01T00:00:04.000000Z'),
                    (1.4m, '1970-01-01T00:00:05.000000Z'),
                    (1.2m, '1970-01-01T00:00:06.000000Z'),
                    (1.1m, '1970-01-01T00:00:07.000000Z')
                    """);

            String expected = """
                    dec\tts\tdec1\tts1
                    1.10\t1970-01-01T00:00:00.000000Z\t1.10\t1970-01-01T00:00:07.000000Z
                    1.20\t1970-01-01T00:00:01.000000Z\t1.20\t1970-01-01T00:00:06.000000Z
                    1.40\t1970-01-01T00:00:03.000000Z\t1.40\t1970-01-01T00:00:05.000000Z
                    """;

            String sql = "select * from t1 join t2 on t1.dec = t2.dec";

            assertQuery(sql)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testJoinOnDecimalKeyMixedScales() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (dec decimal(4, 2), ts timestamp) timestamp(ts)");
            execute("""
                    insert into t1 (dec, ts) values
                    (1.1m, '1970-01-01T00:00:00.000000Z'),
                    (1.2m, '1970-01-01T00:00:01.000000Z'),
                    (1.3m, '1970-01-01T00:00:02.000000Z'),
                    (1.41m, '1970-01-01T00:00:03.000000Z')
                    """);

            execute("create table t2 (dec decimal(8, 4), ts timestamp) timestamp(ts)");
            execute("""
                    insert into t2 (dec, ts) values
                    (1.5432m, '1970-01-01T00:00:04.000000Z'),
                    (1.41m, '1970-01-01T00:00:05.000000Z'),
                    (1.200m, '1970-01-01T00:00:06.000000Z'),
                    (1.1000m, '1970-01-01T00:00:07.000000Z')
                    """);

            String expected = """
                    dec\tts\tdec1\tts1
                    1.10\t1970-01-01T00:00:00.000000Z\t1.1000\t1970-01-01T00:00:07.000000Z
                    1.20\t1970-01-01T00:00:01.000000Z\t1.2000\t1970-01-01T00:00:06.000000Z
                    1.41\t1970-01-01T00:00:03.000000Z\t1.4100\t1970-01-01T00:00:05.000000Z
                    """;

            String sql = "select * from t1 join t2 on cast(t1.dec as decimal(8, 4)) = t2.dec";

            assertQuery(sql)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testJoinOnGeohash() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table t1 as (select " +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(2c)) geo2," +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(8c)) geo8," +
                            "x," +
                            "timestamp_sequence(0, 1000000) ts " +
                            "from long_sequence(10)) timestamp(ts)"
            );
            execute(
                    "create table t2 as (select " +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(2c)) geo2," +
                            "cast(rnd_str('quest', '1234', '3456') as geohash(8c)) geo8," +
                            "x," +
                            "timestamp_sequence(0, 1000000) ts " +
                            "from long_sequence(2)) timestamp(ts)"
            );

            String expected = """
                    geo4\tgeo1\tgeo2\tgeo8\tx\tts\tgeo41\tgeo11\tgeo21\tgeo81\tx1\tts1
                    ques\tq\t12\t\t1\t1970-01-01T00:00:00.000000Z\t\t\t\t\tnull\t
                    3456\t3\t34\t\t2\t1970-01-01T00:00:01.000000Z\t3456\tq\t12\t\t1\t1970-01-01T00:00:00.000000Z
                    ques\t1\t12\t\t3\t1970-01-01T00:00:02.000000Z\t\t\t\t\tnull\t
                    1234\t1\t12\t\t4\t1970-01-01T00:00:03.000000Z\t1234\t3\t12\t\t2\t1970-01-01T00:00:01.000000Z
                    ques\t1\tqu\t\t5\t1970-01-01T00:00:04.000000Z\t\t\t\t\tnull\t
                    1234\tq\tqu\t\t6\t1970-01-01T00:00:05.000000Z\t1234\t3\t12\t\t2\t1970-01-01T00:00:01.000000Z
                    ques\t1\t34\t\t7\t1970-01-01T00:00:06.000000Z\t\t\t\t\tnull\t
                    1234\tq\t34\t\t8\t1970-01-01T00:00:07.000000Z\t1234\t3\t12\t\t2\t1970-01-01T00:00:01.000000Z
                    3456\t3\tqu\t\t9\t1970-01-01T00:00:08.000000Z\t3456\tq\t12\t\t1\t1970-01-01T00:00:00.000000Z
                    3456\tq\t12\t\t10\t1970-01-01T00:00:09.000000Z\t3456\tq\t12\t\t1\t1970-01-01T00:00:00.000000Z
                    """;

            String sql = "with g1 as (select distinct * from t1 order by ts)," +
                    "g2 as (select distinct * from t2 order by ts)" +
                    "select * from g1 lt join g2 on g1.geo4 = g2.geo4";

            assertQuery(sql)
                    .noLeakCheck()
                    .fullFatJoins()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            assertQuery(sql)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testJoinOnGeohashNonExactPrecisionNotAllowed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "x," +
                    "timestamp_sequence(0, 1000000) ts " +
                    "from long_sequence(10)) timestamp(ts)");
            execute("create table t2 as (select " +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(4c)) geo4," +
                    "cast(rnd_str('quest', '1234', '3456') as geohash(1c)) geo1," +
                    "x," +
                    "timestamp_sequence(0, 1000000) ts " +
                    "from long_sequence(2)) timestamp(ts)");

            String sql = "with g1 as (select distinct * from t1 order by ts)," +
                    "g2 as (select distinct * from t2 order by ts)" +
                    "select * from g1 lt join g2 on g1.geo4 = g2.geo1";

            try {
                assertQuery(sql)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("");
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

            final String expected = """
                    i\ti1\thash
                    1\t1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650
                    2\t2\t0xb5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa65572a215ba0462ad15
                    3\t3\t0x322a2198864beb14797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fa
                    """;

            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_long256() hash" +
                            " from long_sequence(3)" +
                            ")"
            );

            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testJoinOnUUID() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, y.i, x.uuid " +
                    "from x " +
                    "join x y on y.uuid = x.uuid";

            final String expected = """
                    i\ti1\tuuid
                    1\t1\t0010cde8-12ce-40ee-8010-a928bb8b9650
                    2\t2\t9f9b2131-d49f-4d1d-ab81-39815c50d341
                    3\t3\t7bcd48d8-c77a-4655-b2a2-15ba0462ad15
                    """;

            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_uuid4() uuid" +
                            " from long_sequence(3)" +
                            ")"
            );

            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
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
    public void testJoinOuterBooleanProjectedFilterPreservesRightRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE l (x BOOLEAN)");
            execute("CREATE TABLE r (x BOOLEAN)");
            execute("INSERT INTO r VALUES (FALSE), (TRUE)");

            final String expected = "ax\trx\nfalse\tfalse\nfalse\ttrue\n";
            for (boolean isFullFatJoin : new boolean[]{false, true}) {
                for (String joinType : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                    assertQuery("""
                            SELECT ax, rx
                            FROM (
                                SELECT l.x AS ax, r.x AS rx
                                FROM l
                                %s r ON r.x = l.x
                            )
                            WHERE ax = (1 = 2)
                            ORDER BY rx
                            """.formatted(joinType))
                            .noLeakCheck()
                            .fullFatJoins(isFullFatJoin)
                            .returns(expected);
                }
            }
        });
    }

    @Test
    public void testJoinOuterLong256AndChar() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    kk\ta\tb\tkk1\ta1\tb1
                    1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\tnull\t\t
                    2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x4c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39bccb30ed7795ebc8\tJ
                    2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x58dfd08eeb9cc39ecec82869edec121bc2593f82b430328d84a09f29df637e38\tB
                    3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\tnull\t\t
                    4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0xbacd57f41b59057caa237cfb02a208e494cfe42988a633de738bab883dc7e332\tU
                    4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x10bb226eb4243e3683b91ec970b04e788a50f7ff7f6ed3305705e75fe328fa9d\tE
                    5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\tnull\t\t
                    6\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t6\t0x3ad08d6037d3ce8155c06051ee52138b655f87a3a21d575f610f69efe063fe79\tS
                    6\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t6\t0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG
                    7\t0xc718ab5cbb3fd261c1bf6c24be53876861b1a0b0a559551538b73d329210d277\tY\tnull\t\t
                    8\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t8\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM
                    8\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t8\t0x4cd64b0b0a344f8e6698c6c186b7571a9cba3ef59083484d98c2d832d83de993\tR
                    9\t0x8a538661f350d0b46f06560981acb5496adc00ebd29fdd5373dee145497c5436\tH\tnull\t\t
                    10\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t10\t0x9b27eba5e9cfa1e29660300cea7db540954a62eca44acb2d71660a9b0890a2f0\tJ
                    10\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t10\t0x9a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d645277ee62a5a6e9fb\tZ
                    """;

            execute(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_long256() a," +
                            " rnd_char() b" +
                            " from long_sequence(10))"
            );

            execute(
                    "create table y as (select" +
                            " cast(2*((x-1)/2) as int)+2 kk," +
                            " rnd_long256() a," +
                            " rnd_char() b" +
                            " from long_sequence(10))"
            );

            // filter is applied to final join result
            assertQuery("select * from x left join y on (kk) order by kk,a")
                    .noLeakCheck()
                    .returns(expected);

            assertQuery("select x.*, y.* from y right join x on (kk) order by kk,a")
                    .noLeakCheck()
                    .returns("""
                            kk\ta\tb\tkk1\ta1\tb1
                            1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\tnull\t\t
                            2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x58dfd08eeb9cc39ecec82869edec121bc2593f82b430328d84a09f29df637e38\tB
                            2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x4c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39bccb30ed7795ebc8\tJ
                            3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\tnull\t\t
                            4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x10bb226eb4243e3683b91ec970b04e788a50f7ff7f6ed3305705e75fe328fa9d\tE
                            4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0xbacd57f41b59057caa237cfb02a208e494cfe42988a633de738bab883dc7e332\tU
                            5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\tnull\t\t
                            6\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t6\t0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG
                            6\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t6\t0x3ad08d6037d3ce8155c06051ee52138b655f87a3a21d575f610f69efe063fe79\tS
                            7\t0xc718ab5cbb3fd261c1bf6c24be53876861b1a0b0a559551538b73d329210d277\tY\tnull\t\t
                            8\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t8\t0x4cd64b0b0a344f8e6698c6c186b7571a9cba3ef59083484d98c2d832d83de993\tR
                            8\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t8\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM
                            9\t0x8a538661f350d0b46f06560981acb5496adc00ebd29fdd5373dee145497c5436\tH\tnull\t\t
                            10\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t10\t0x9a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d645277ee62a5a6e9fb\tZ
                            10\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t10\t0x9b27eba5e9cfa1e29660300cea7db540954a62eca44acb2d71660a9b0890a2f0\tJ
                            """);

            assertQuery("select x.*, y.* from y full join x on (kk) order by kk,a")
                    .noLeakCheck()
                    .returns("""
                            kk\ta\tb\tkk1\ta1\tb1
                            1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\tnull\t\t
                            2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x58dfd08eeb9cc39ecec82869edec121bc2593f82b430328d84a09f29df637e38\tB
                            2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x4c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39bccb30ed7795ebc8\tJ
                            3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\tnull\t\t
                            4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x10bb226eb4243e3683b91ec970b04e788a50f7ff7f6ed3305705e75fe328fa9d\tE
                            4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0xbacd57f41b59057caa237cfb02a208e494cfe42988a633de738bab883dc7e332\tU
                            5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\tnull\t\t
                            6\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t6\t0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG
                            6\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t6\t0x3ad08d6037d3ce8155c06051ee52138b655f87a3a21d575f610f69efe063fe79\tS
                            7\t0xc718ab5cbb3fd261c1bf6c24be53876861b1a0b0a559551538b73d329210d277\tY\tnull\t\t
                            8\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t8\t0x4cd64b0b0a344f8e6698c6c186b7571a9cba3ef59083484d98c2d832d83de993\tR
                            8\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t8\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM
                            9\t0x8a538661f350d0b46f06560981acb5496adc00ebd29fdd5373dee145497c5436\tH\tnull\t\t
                            10\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t10\t0x9a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d645277ee62a5a6e9fb\tZ
                            10\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t10\t0x9b27eba5e9cfa1e29660300cea7db540954a62eca44acb2d71660a9b0890a2f0\tJ
                            """);
        });
    }

    @Test
    public void testJoinOuterLong256AndCharAndOrder() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    kk\ta\tb\tkk1\ta1\tb1
                    2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x4c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39bccb30ed7795ebc8\tJ
                    2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x58dfd08eeb9cc39ecec82869edec121bc2593f82b430328d84a09f29df637e38\tB
                    7\t0xc718ab5cbb3fd261c1bf6c24be53876861b1a0b0a559551538b73d329210d277\tY\tnull\t\t
                    1\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\tnull\t\t
                    10\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t10\t0x9a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d645277ee62a5a6e9fb\tZ
                    10\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t10\t0x9b27eba5e9cfa1e29660300cea7db540954a62eca44acb2d71660a9b0890a2f0\tJ
                    3\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\tnull\t\t
                    9\t0x8a538661f350d0b46f06560981acb5496adc00ebd29fdd5373dee145497c5436\tH\tnull\t\t
                    6\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t6\t0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG
                    6\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t6\t0x3ad08d6037d3ce8155c06051ee52138b655f87a3a21d575f610f69efe063fe79\tS
                    8\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t8\t0x4cd64b0b0a344f8e6698c6c186b7571a9cba3ef59083484d98c2d832d83de993\tR
                    8\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t8\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM
                    5\t0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\tnull\t\t
                    4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x10bb226eb4243e3683b91ec970b04e788a50f7ff7f6ed3305705e75fe328fa9d\tE
                    4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0xbacd57f41b59057caa237cfb02a208e494cfe42988a633de738bab883dc7e332\tU
                    """;

            execute(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_long256() a," +
                            " rnd_char() b" +
                            " from long_sequence(10))"
            );

            execute(
                    "create table y as (select" +
                            " cast(2*((x-1)/2) as int)+2 kk," +
                            " rnd_long256() a," +
                            " rnd_char() b" +
                            " from long_sequence(10))"
            );

            // filter is applied to final join result
            assertQuery("select * from x left join y on (kk) order by x.a desc, y.a")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x right join y on (kk) order by x.a desc, y.a")
                    .noLeakCheck()
                    .returns("""
                            kk\ta\tb\tkk1\ta1\tb1
                            2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x4c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39bccb30ed7795ebc8\tJ
                            2\t0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t2\t0x58dfd08eeb9cc39ecec82869edec121bc2593f82b430328d84a09f29df637e38\tB
                            10\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t10\t0x9a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d645277ee62a5a6e9fb\tZ
                            10\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t10\t0x9b27eba5e9cfa1e29660300cea7db540954a62eca44acb2d71660a9b0890a2f0\tJ
                            6\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t6\t0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG
                            6\t0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t6\t0x3ad08d6037d3ce8155c06051ee52138b655f87a3a21d575f610f69efe063fe79\tS
                            8\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t8\t0x4cd64b0b0a344f8e6698c6c186b7571a9cba3ef59083484d98c2d832d83de993\tR
                            8\t0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t8\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM
                            4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0x10bb226eb4243e3683b91ec970b04e788a50f7ff7f6ed3305705e75fe328fa9d\tE
                            4\t0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t4\t0xbacd57f41b59057caa237cfb02a208e494cfe42988a633de738bab883dc7e332\tU
                            """);
            assertQuery("select * from x full join y on (kk) order by x.a desc, y.a")
                    .noLeakCheck()
                    .returns(expected);
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
            final String leftJoin = "select x.c, x.a, b, ts from x left join y on y.m = x.c order by x.c, x.a, b";
            final String rightJoin = "select x.c, x.a, b, ts from y right join x on y.m = x.c order by x.c, x.a, b";
            final String fullJoin = "select x.c, x.a, b, ts from x full join y on y.m = x.c order by x.c, x.a, b";
            String expected = """
                    c\ta\tb\tts
                    1\t120\tnull\t2018-03-01T00:00:00.000001Z
                    2\t568\t16\t2018-03-01T00:00:00.000002Z
                    2\t568\t72\t2018-03-01T00:00:00.000002Z
                    3\t333\tnull\t2018-03-01T00:00:00.000003Z
                    4\t371\t3\t2018-03-01T00:00:00.000004Z
                    4\t371\t14\t2018-03-01T00:00:00.000004Z
                    5\t251\tnull\t2018-03-01T00:00:00.000005Z
                    6\t439\t12\t2018-03-01T00:00:00.000006Z
                    6\t439\t81\t2018-03-01T00:00:00.000006Z
                    7\t42\tnull\t2018-03-01T00:00:00.000007Z
                    8\t521\t16\t2018-03-01T00:00:00.000008Z
                    8\t521\t97\t2018-03-01T00:00:00.000008Z
                    9\t356\tnull\t2018-03-01T00:00:00.000009Z
                    10\t598\t5\t2018-03-01T00:00:00.000010Z
                    10\t598\t74\t2018-03-01T00:00:00.000010Z
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x ts from long_sequence(10)) timestamp(ts)");
            execute("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))");

            // master records should be filtered out because slave records missing
            assertQuery(leftJoin)
                    .noLeakCheck()
                    .returns(expected);
            assertQuery(rightJoin)
                    .noLeakCheck()
                    .returns(expected);
            assertQuery(fullJoin)
                    .noLeakCheck()
                    .returns(expected);


            execute("insert into x select * from (select cast(x+10 as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x + 10 ts from long_sequence(4)) timestamp(ts)");
            execute("insert into y select x, cast(2*((x-1+10)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(6)");

            expected = expected +
                    "11\t467\tnull\t2018-03-01T00:00:00.000011Z\n" +
                    "12\t347\t0\t2018-03-01T00:00:00.000012Z\n" +
                    "12\t347\t7\t2018-03-01T00:00:00.000012Z\n" +
                    "13\t244\tnull\t2018-03-01T00:00:00.000013Z\n" +
                    "14\t197\t50\t2018-03-01T00:00:00.000014Z\n" +
                    "14\t197\t68\t2018-03-01T00:00:00.000014Z\n";
            assertQuery(leftJoin)
                    .noLeakCheck()
                    .returns(expected);
            assertQuery(rightJoin)
                    .noLeakCheck()
                    .returns(expected);
            assertQuery(fullJoin)
                    .noLeakCheck()
                    .returns("c\ta\tb\tts\n" +
                            "null\tnull\t55\t\n" +
                            "null\tnull\t64\t\n" +
                            expected.replace("c\ta\tb\tts\n", ""));
        });
    }

    @Test
    public void testJoinWithGeohash() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "with x1 as (select distinct * from x)," +
                    "y1 as (select distinct * from y) " +
                    "select g1, gg1, gg2, gg4, gg8, x1.k " +
                    "from x1 " +
                    "join y1 on y1.kk = x1.k" +
                    " order by 6";

            final String expected = """
                    g1\tgg1\tgg2\tgg4\tgg8\tk
                    9v1s\t1\twh4\ts2z2\t10011100111100101000010010010000010001010\t1
                    46sw\tq\t71f\tfsnj\t11010111111011100000110010000111111101101\t2
                    jnw9\tb\tjj5\tksu7\t11101100011100010000100111000111100000001\t3
                    zfuq\ts\t76u\tq0s5\t11110001011010001010010100000110110100010\t4
                    hp4m\ty\tp1d\tp2n3\t10111100100011101101110001110010111011001\t5
                    """;


            execute(
                    "create table x as (select" +
                            " cast(x as int) k, " +
                            " rnd_geohash(20) g1" +
                            " from long_sequence(5))"
            );

            execute(
                    "create table y as (select" +
                            " cast(x as int) kk," +
                            " rnd_geohash(15) gg2," +
                            " rnd_geohash(20) gg4," +
                            " rnd_geohash(5) gg1," +
                            " rnd_geohash(41) gg8" +
                            " from long_sequence(20))"
            );

            assertQuery(query)
                    .noLeakCheck()
                    .returns(expected);
            assertQuery(query)
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testJoinWithGeohash2() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "with x1 as (select distinct * from x order by k)," +
                    "y1 as (select distinct * from y order by kk) " +
                    "select g1, gg1, gg2, gg4, gg8, x1.k " +
                    "from x1 " +
                    "lt join y1 on x1.l = y1.l";

            final String expected = """
                    g1\tgg1\tgg2\tgg4\tgg8\tk
                    9v1s\t\t\t\t\t1970-01-01T00:00:00.000001Z
                    46sw\t1\twh4\ts2z2\t10011100111100101000010010010000010001010\t1970-01-01T00:00:00.000002Z
                    jnw9\tq\t71f\tfsnj\t11010111111011100000110010000111111101101\t1970-01-01T00:00:00.000003Z
                    zfuq\tb\tjj5\tksu7\t11101100011100010000100111000111100000001\t1970-01-01T00:00:00.000004Z
                    hp4m\ts\t76u\tq0s5\t11110001011010001010010100000110110100010\t1970-01-01T00:00:00.000005Z
                    """;

            execute(
                    "create table x as (select" +
                            " 1 as l, " +
                            " cast(x as timestamp) k, " +
                            " rnd_geohash(20) g1" +
                            " from long_sequence(5)) timestamp(k)"
            );

            execute(
                    "create table y as (select" +
                            " 1 as l, " +
                            " cast(x as timestamp) kk," +
                            " rnd_geohash(15) gg2," +
                            " rnd_geohash(20) gg4," +
                            " rnd_geohash(5) gg1," +
                            " rnd_geohash(41) gg8" +
                            " from long_sequence(20))  timestamp(kk)"
            );

            assertQuery(query)
                    .noLeakCheck()
                    .fullFatJoins()
                    .timestamp("k")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("k")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testJoiningSubqueriesWithDuplicateDottedColumnNames() throws Exception {
        // Two join sides project the same dotted alias; wildcard deduplication must keep the
        // second name quote-protected so it also strips to a clean display name ("a.b" / "a.b1",
        // not "a.b" / "a.b"1 with leaked quotes).
        assertMemoryLeak(() -> assertQuery("""
                SELECT * FROM (SELECT 1 AS "a.b") t1 CROSS JOIN (SELECT 2 AS "a.b") t2
                """)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("""
                        a.b	a.b1
                        1	2
                        """));
    }

    @Test
    public void testJoiningSubqueriesWithQualifiedDuplicateDottedColumnRefs() throws Exception {
        // Regression: two join sides expose the same dotted alias referenced by its qualified name
        // (t1."a.b", t2."a.b"), NOT via a wildcard. The composed reference is aliased through
        // createColumnAlias's prefixed path; its column part is quote-protected, so the dedup suffix
        // must land INSIDE the quotes ("a.b1") and strip to a clean a.b / a.b1 - not "a.b"1, which
        // leaks the protective quotes into result set metadata. Fails without the composed-reference
        // fix in SqlUtil.createColumnAlias.
        assertMemoryLeak(() -> assertQuery("""
                SELECT t1."a.b", t2."a.b" FROM (SELECT 1 AS "a.b") t1 CROSS JOIN (SELECT 2 AS "a.b") t2
                """)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("""
                        a.b	a.b1
                        1	2
                        """));
    }

    @Test
    public void testJoiningSubqueryWithDotInColumnName() throws Exception {
        assertMemoryLeak(() -> assertQuery("""
                SELECT * FROM (SELECT x as "foo.bar" FROM long_sequence(5))
                LEFT JOIN (select 1) ON true;
                """)
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        foo.bar	1
                        1	1
                        2	1
                        3	1
                        4	1
                        5	1
                        """));
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int)");
            execute("insert into t1 values (1), (2), (3), (4), (5);");
            execute("create table t2 (j int)");
            execute("insert into t2 values (5), (4), (3), (2), (1);");

            assertHashJoinSql(
                    "select * from t1 left join t2 on i = j and abs(i) > 3",
                    """
                            i\tj
                            1\tnull
                            2\tnull
                            3\tnull
                            4\t4
                            5\t5
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select t1.*, t2.* from t2 right join t1 on i = j and abs(i) > 3 order by i, j",
                    """
                            i\tj
                            1\tnull
                            2\tnull
                            3\tnull
                            4\t4
                            5\t5
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on i = j and abs(i) > 3 order by i, j",
                    """
                            i\tj
                            null\t1
                            null\t2
                            null\t3
                            1\tnull
                            2\tnull
                            3\tnull
                            4\t4
                            5\t5
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition10() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s1 string)");
            execute("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            execute("create table t2 (j int, s2 string)");
            execute("insert into t2 values (1,'a'), (5,'e'), (2, 'b'), (4, 'd'), (3,'c');");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and s2 = s1",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            2\tb\t2\tb
                            3\tc\t3\tc
                            4\td\t4\td
                            5\te\t5\te
                            """
            );
            assertHashJoinSql(
                    "select t1.*, t2.* from t2 right join t1 on j = i and s2 = s1",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            5\te\t5\te
                            2\tb\t2\tb
                            4\td\t4\td
                            3\tc\t3\tc
                            """
            );
            assertHashJoinSql(
                    "select * from t1 full join t2 on j = i and s2 = s1",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            2\tb\t2\tb
                            3\tc\t3\tc
                            4\td\t4\td
                            5\te\t5\te
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition11() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s1 string)");
            execute("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            execute("create table t2 (j int, s2 string)");
            execute("insert into t2 values (1, 'a'), (5, 'e'), (2, 'b'), (4, 'd'), (3, 'c');");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and (s1 ~ 'a' or s2 ~ 'c')",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            2\tb\tnull\t
                            3\tc\t3\tc
                            4\td\tnull\t
                            5\te\tnull\t
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select t1.*, t2.* from t2 right join t1 on j = i and (s1 ~ 'a' or s2 ~ 'c') order by i, s1, j, s2",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            2\tb\tnull\t
                            3\tc\t3\tc
                            4\td\tnull\t
                            5\te\tnull\t
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on j = i and (s1 ~ 'a' or s2 ~ 'c') order by i, s1, j, s2",
                    """
                            i\ts1\tj\ts2
                            null\t\t2\tb
                            null\t\t4\td
                            null\t\t5\te
                            1\ta\t1\ta
                            2\tb\tnull\t
                            3\tc\t3\tc
                            4\td\tnull\t
                            5\te\tnull\t
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition13() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s1 string)");
            execute("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            execute("create table t2 (j int, s2 string)");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and (s1 ~ '[abde]')",
                    """
                            i\ts1\tj\ts2
                            1\ta\tnull\t
                            2\tb\tnull\t
                            3\tc\tnull\t
                            4\td\tnull\t
                            5\te\tnull\t
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select t1.*, t2.* from t2 right join t1 on j = i and (s1 ~ '[abde]') order by i, s1, j, s2",
                    """
                            i\ts1\tj\ts2
                            1\ta\tnull\t
                            2\tb\tnull\t
                            3\tc\tnull\t
                            4\td\tnull\t
                            5\te\tnull\t
                            """
            );
            assertHashJoinSql(
                    "select * from t1 full join t2 on j = i and (s1 ~ '[abde]')",
                    """
                            i\ts1\tj\ts2
                            1\ta\tnull\t
                            2\tb\tnull\t
                            3\tc\tnull\t
                            4\td\tnull\t
                            5\te\tnull\t
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition14() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s1 string)");
            execute("create table t2 (j int, s2 string)");
            execute("insert into t2 values (1,'a'), (1,'e'), (2, 'b'), (2, 'd'), (3,'c');");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and (s1 ~ '[abde]')",
                    "i\ts1\tj\ts2\n"
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 right join t2 on j = i and (s1 ~ '[abde]') order by j, s2",
                    """
                            i\ts1\tj\ts2
                            null\t\t1\ta
                            null\t\t1\te
                            null\t\t2\tb
                            null\t\t2\td
                            null\t\t3\tc
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on j = i and (s1 ~ '[abde]') order by j, s2",
                    """
                            i\ts1\tj\ts2
                            null\t\t1\ta
                            null\t\t1\te
                            null\t\t2\tb
                            null\t\t2\td
                            null\t\t3\tc
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition15() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s1 string)");
            execute("create table t2 (j int, s2 string)");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and (s1 ~ '[abde]')",
                    "i\ts1\tj\ts2\n"
            );
            assertHashJoinSql(
                    "select * from t1 right join t2 on j = i and (s1 ~ '[abde]')",
                    "i\ts1\tj\ts2\n"
            );
            assertHashJoinSql(
                    "select * from t1 full join t2 on j = i and (s1 ~ '[abde]')",
                    "i\ts1\tj\ts2\n"
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition16() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s1 string)");
            execute("insert into t1 values (1, 'a'), (2, 'b');");
            execute("create table t2 (j int, s2 string)");
            execute("insert into t2 values (1,'a'), (1,'f'), (1, 'g'), (1, 'd'), (3,'c');");

            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 left join t2 on j = i and (s2 ~ '[abde]') order by i, s2",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            1\ta\t1\td
                            2\tb\tnull\t
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 right join t2 on j = i and (s2 ~ '[abde]') order by i, s2",
                    """
                            i\ts1\tj\ts2
                            null\t\t3\tc
                            null\t\t1\tf
                            null\t\t1\tg
                            1\ta\t1\ta
                            1\ta\t1\td
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on j = i and (s2 ~ '[abde]') order by i, s2",
                    """
                            i\ts1\tj\ts2
                            null\t\t3\tc
                            null\t\t1\tf
                            null\t\t1\tg
                            1\ta\t1\ta
                            1\ta\t1\td
                            2\tb\tnull\t
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition17() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s1 symbol, ts1 timestamp) timestamp(ts1)");
            execute("insert into t1 values (1, 'a', 1), (2, 'b', 2);");
            execute("create table t2 (j int, s2 symbol, ts2 timestamp) timestamp(ts2) ");
            execute("insert into t2 values (1,'a', 1), (1,'f', 2), (1, 'g', 3), (1, 'd', 4), (3,'c', 5);");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and (s2 ~ '[abde]') order by ts1 desc, s2",
                    """
                            i\ts1\tts1\tj\ts2\tts2
                            2\tb\t1970-01-01T00:00:00.000002Z\tnull\t\t
                            1\ta\t1970-01-01T00:00:00.000001Z\t1\ta\t1970-01-01T00:00:00.000001Z
                            1\ta\t1970-01-01T00:00:00.000001Z\t1\td\t1970-01-01T00:00:00.000004Z
                            """,
                    "ts1", true, true
            );

            assertHashJoinSql(
                    "select * from t1 right join t2 on j = i and (s2 ~ '[abde]') order by ts1 desc, s2",
                    """
                            i\ts1\tts1\tj\ts2\tts2
                            1\ta\t1970-01-01T00:00:00.000001Z\t1\ta\t1970-01-01T00:00:00.000001Z
                            1\ta\t1970-01-01T00:00:00.000001Z\t1\td\t1970-01-01T00:00:00.000004Z
                            null\t\t\t3\tc\t1970-01-01T00:00:00.000005Z
                            null\t\t\t1\tf\t1970-01-01T00:00:00.000002Z
                            null\t\t\t1\tg\t1970-01-01T00:00:00.000003Z
                            """,
                    "ts1", true, true
            );
            assertHashJoinSql(
                    "select * from t1 full join t2 on j = i and (s2 ~ '[abde]') order by ts1 desc, s2",
                    """
                            i\ts1\tts1\tj\ts2\tts2
                            2\tb\t1970-01-01T00:00:00.000002Z\tnull\t\t
                            1\ta\t1970-01-01T00:00:00.000001Z\t1\ta\t1970-01-01T00:00:00.000001Z
                            1\ta\t1970-01-01T00:00:00.000001Z\t1\td\t1970-01-01T00:00:00.000004Z
                            null\t\t\t3\tc\t1970-01-01T00:00:00.000005Z
                            null\t\t\t1\tf\t1970-01-01T00:00:00.000002Z
                            null\t\t\t1\tg\t1970-01-01T00:00:00.000003Z
                            """,
                    "ts1", true, true
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition18() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s1 symbol)");
            execute("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            execute("create table t2 (j int, s2 symbol)");
            execute("insert into t2 values (1, 'a'), (5, 'e'), (2, 'b'), (4, 'd'), (3, 'c');");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and (s1 ~ 'a' or s2 ~ 'c')",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            2\tb\tnull\t
                            3\tc\t3\tc
                            4\td\tnull\t
                            5\te\tnull\t
                            """,
                    null,
                    false,
                    false
            );
            assertHashJoinSql(
                    "select * from t1 right join t2 on j = i and (s1 ~ 'a' or s2 ~ 'c') order by i, s1, j, s2",
                    """
                            i\ts1\tj\ts2
                            null\t\t2\tb
                            null\t\t4\td
                            null\t\t5\te
                            1\ta\t1\ta
                            3\tc\t3\tc
                            """,
                    null,
                    false,
                    true
            );
            assertHashJoinSql(
                    "select * from t1 full join t2 on j = i and (s1 ~ 'a' or s2 ~ 'c') order by i, s1, j, s2",
                    """
                            i\ts1\tj\ts2
                            null\t\t2\tb
                            null\t\t4\td
                            null\t\t5\te
                            1\ta\t1\ta
                            2\tb\tnull\t
                            3\tc\t3\tc
                            4\td\tnull\t
                            5\te\tnull\t
                            """,
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int)");
            execute("insert into t1 values (1), (2), (3), (4), (5);");
            execute("create table t2 (j int)");
            execute("insert into t2 values (5), (4), (3), (2), (1);");

            assertHashJoinSql(
                    "select * from t1 left join t2 on i = j and abs(i) > 5",
                    """
                            i\tj
                            1\tnull
                            2\tnull
                            3\tnull
                            4\tnull
                            5\tnull
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 right join t2 on i = j and abs(i) > 5 order by i, j",
                    """
                            i\tj
                            null\t1
                            null\t2
                            null\t3
                            null\t4
                            null\t5
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on i = j and abs(i) > 5 order by i, j",
                    """
                            i\tj
                            null\t1
                            null\t2
                            null\t3
                            null\t4
                            null\t5
                            1\tnull
                            2\tnull
                            3\tnull
                            4\tnull
                            5\tnull
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int)");
            execute("insert into t1 values (1), (2), (3), (4), (5);");
            execute("create table t2 (j int)");
            execute("insert into t2 values (5), (4), (3), (2), (1);");

            assertHashJoinSql(
                    "select * from t1 left join t2 on i = j and abs(i) = 3",
                    """
                            i\tj
                            1\tnull
                            2\tnull
                            3\t3
                            4\tnull
                            5\tnull
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 right join t2 on i = j and abs(i) = 3 order by i, j",
                    """
                            i\tj
                            null\t1
                            null\t2
                            null\t4
                            null\t5
                            3\t3
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on i = j and abs(i) = 3 order by i, j",
                    """
                            i\tj
                            null\t1
                            null\t2
                            null\t4
                            null\t5
                            1\tnull
                            2\tnull
                            3\t3
                            4\tnull
                            5\tnull
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int)");
            execute("insert into t1 values (1), (2), (3), (4), (5);");
            execute("create table t2 (j int)");
            execute("insert into t2 values (1), (5), (2), (4), (3);");

            assertHashJoinSql(
                    "select * from t1 left join t2 on i = j and abs(i) <= 0",
                    """
                            i\tj
                            1\tnull
                            2\tnull
                            3\tnull
                            4\tnull
                            5\tnull
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 right join t2 on i = j and abs(i) <= 0 order by i, j",
                    """
                            i\tj
                            null\t1
                            null\t2
                            null\t3
                            null\t4
                            null\t5
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on i = j and abs(i) <= 0 order by i, j",
                    """
                            i\tj
                            null\t1
                            null\t2
                            null\t3
                            null\t4
                            null\t5
                            1\tnull
                            2\tnull
                            3\tnull
                            4\tnull
                            5\tnull
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int)");
            execute("insert into t1 values (1), (2), (3), (4), (5);");
            execute("create table t2 (j int)");
            execute("insert into t2 values (1), (5), (2), (4), (3);");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and abs(i)*abs(j) >= 4 and i*j <= 9",
                    """
                            i\tj
                            1\tnull
                            2\t2
                            3\t3
                            4\tnull
                            5\tnull
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 right join t2 on j = i and abs(i)*abs(j) >= 4 and i*j <= 9 order by i, j",
                    """
                            i\tj
                            null\t1
                            null\t4
                            null\t5
                            2\t2
                            3\t3
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on j = i and abs(i)*abs(j) >= 4 and i*j <= 9 order by i, j",
                    """
                            i\tj
                            null\t1
                            null\t4
                            null\t5
                            1\tnull
                            2\t2
                            3\t3
                            4\tnull
                            5\tnull
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition6() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int)");
            execute("insert into t1 values (1), (2), (3), (4), (5);");
            execute("create table t2 (j int)");
            execute("insert into t2 values (1), (5), (2), (4), (3);");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and (j = 2 or i = 4)",
                    """
                            i\tj
                            1\tnull
                            2\t2
                            3\tnull
                            4\t4
                            5\tnull
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 right join t2 on j = i and (j = 2 or i = 4) order by i, j",
                    """
                            i\tj
                            null\t1
                            null\t3
                            null\t5
                            2\t2
                            4\t4
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on j = i and (j = 2 or i = 4) order by i, j",
                    """
                            i\tj
                            null\t1
                            null\t3
                            null\t5
                            1\tnull
                            2\t2
                            3\tnull
                            4\t4
                            5\tnull
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition7() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int)");
            execute("insert into t1 values (1), (2), (3), (-4), (5);");
            execute("create table t2 (j int)");
            execute("insert into t2 values (1), (5), (-2), (-4), (3);");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and (abs(j) = 2 or abs(i) = 4)",
                    """
                            i\tj
                            1\tnull
                            2\tnull
                            3\tnull
                            -4\t-4
                            5\tnull
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 right join t2 on j = i and (abs(j) = 2 or abs(i) = 4) order by i, j",
                    """
                            i\tj
                            null\t-2
                            null\t1
                            null\t3
                            null\t5
                            -4\t-4
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on j = i and (abs(j) = 2 or abs(i) = 4) order by i, j",
                    """
                            i\tj
                            null\t-2
                            null\t1
                            null\t3
                            null\t5
                            -4\t-4
                            1\tnull
                            2\tnull
                            3\tnull
                            5\tnull
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition8() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int)");
            execute("insert into t1 values (1), (2), (3), (4), (5);");
            execute("create table t2 (j int, s2 string)");
            execute("insert into t2 values (1,'a'), (5,'e'), (-2, 'b'), (4, 'd'), (3,'c');");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and s2 = 'a'",
                    """
                            i\tj\ts2
                            1\t1\ta
                            2\tnull\t
                            3\tnull\t
                            4\tnull\t
                            5\tnull\t
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 right join t2 on j = i and s2 = 'a' order by i, j, s2",
                    """
                            i\tj\ts2
                            null\t-2\tb
                            null\t3\tc
                            null\t4\td
                            null\t5\te
                            1\t1\ta
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on j = i and s2 = 'a' order by i, j, s2",
                    """
                            i\tj\ts2
                            null\t-2\tb
                            null\t3\tc
                            null\t4\td
                            null\t5\te
                            1\t1\ta
                            2\tnull\t
                            3\tnull\t
                            4\tnull\t
                            5\tnull\t
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionCondition9() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int)");
            execute("insert into t1 values (1), (2), (3), (4), (5);");
            execute("create table t2 (j int, s2 string)");
            execute("insert into t2 values (1,'a'), (5,'e'), (-2, 'b'), (4, 'd'), (3,'c');");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and s2 ~ '[ad]'",
                    """
                            i\tj\ts2
                            1\t1\ta
                            2\tnull\t
                            3\tnull\t
                            4\t4\td
                            5\tnull\t
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 right join t2 on j = i and s2 ~ '[ad]' order by i, j, s2",
                    """
                            i\tj\ts2
                            null\t-2\tb
                            null\t3\tc
                            null\t5\te
                            1\t1\ta
                            4\t4\td
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on j = i and s2 ~ '[ad]' order by i, j, s2",
                    """
                            i\tj\ts2
                            null\t-2\tb
                            null\t3\tc
                            null\t5\te
                            1\t1\ta
                            2\tnull\t
                            3\tnull\t
                            4\t4\td
                            5\tnull\t
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionConditionVarchar13() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s1 varchar)");
            execute("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            execute("create table t2 (j int, s2 varchar)");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and (s1 ~ '[abde]')",
                    """
                            i\ts1\tj\ts2
                            1\ta\tnull\t
                            2\tb\tnull\t
                            3\tc\tnull\t
                            4\td\tnull\t
                            5\te\tnull\t
                            """
            );
            assertHashJoinSql(
                    "select * from t1 right join t2 on j = i and (s1 ~ '[abde]')",
                    "i\ts1\tj\ts2\n"
            );
            assertHashJoinSql(
                    "select * from t1 full join t2 on j = i and (s1 ~ '[abde]')",
                    """
                            i\ts1\tj\ts2
                            1\ta\tnull\t
                            2\tb\tnull\t
                            3\tc\tnull\t
                            4\td\tnull\t
                            5\te\tnull\t
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinOnFunctionConditionVarchar14() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s1 varchar)");
            execute("create table t2 (j int, s2 varchar)");
            execute("insert into t2 values (1,'a'), (1,'e'), (2, 'b'), (2, 'd'), (3,'c');");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and (s1 ~ '[abde]')",
                    "i\ts1\tj\ts2\n"
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 right join t2 on j = i and (s1 ~ '[abde]') order by j, s2",
                    """
                            i\ts1\tj\ts2
                            null\t\t1\ta
                            null\t\t1\te
                            null\t\t2\tb
                            null\t\t2\td
                            null\t\t3\tc
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on j = i and (s1 ~ '[abde]') order by j, s2",
                    """
                            i\ts1\tj\ts2
                            null\t\t1\ta
                            null\t\t1\te
                            null\t\t2\tb
                            null\t\t2\td
                            null\t\t3\tc
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinWithWhere1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s1 string)");
            execute("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            execute("create table t2 (j int, s2 string)");
            execute("insert into t2 values (5, 'e'), (3, 'c'), (2, 'b'), (4, 'd'), (1, 'a');");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and i = 1 where 1 = 1",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            2\tb\tnull\t
                            3\tc\tnull\t
                            4\td\tnull\t
                            5\te\tnull\t
                            """
            );

            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 right join t2 on j = i and i = 1 where 1 = 1 order by i, s1, j, s2",
                    """
                            i\ts1\tj\ts2
                            null\t\t2\tb
                            null\t\t3\tc
                            null\t\t4\td
                            null\t\t5\te
                            1\ta\t1\ta
                            """
            );

            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on j = i and i = 1 where 1 = 1 order by i, s1, j, s2",
                    """
                            i\ts1\tj\ts2
                            null\t\t2\tb
                            null\t\t3\tc
                            null\t\t4\td
                            null\t\t5\te
                            1\ta\t1\ta
                            2\tb\tnull\t
                            3\tc\tnull\t
                            4\td\tnull\t
                            5\te\tnull\t
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinWithWhere2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s1 string)");
            execute("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            execute("create table t2 (j int, s2 string)");
            execute("insert into t2 values (5, 'e'), (3, 'c'), (2, 'b'), (4, 'd'), (1, 'a');");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i and j = 1 where 1 = 1",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            2\tb\tnull\t
                            3\tc\tnull\t
                            4\td\tnull\t
                            5\te\tnull\t
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 right join t2 on j = i and j = 1 where 1 = 1 order by i, s1, j, s2",
                    """
                            i\ts1\tj\ts2
                            null\t\t2\tb
                            null\t\t3\tc
                            null\t\t4\td
                            null\t\t5\te
                            1\ta\t1\ta
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on j = i and j = 1 where 1 = 1 order by i, s1, j, s2",
                    """
                            i\ts1\tj\ts2
                            null\t\t2\tb
                            null\t\t3\tc
                            null\t\t4\td
                            null\t\t5\te
                            1\ta\t1\ta
                            2\tb\tnull\t
                            3\tc\tnull\t
                            4\td\tnull\t
                            5\te\tnull\t
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinWithWhere3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s1 string)");
            execute("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            execute("create table t2 (j int, s2 string)");
            execute("insert into t2 values (5, 'e'), (3, 'c'), (2, 'b'), (4, 'd'), (1, 'a');");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i where j = 1",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            """
            );
            assertHashJoinSql(
                    "select * from t1 right join t2 on j = i where j = 1",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            """
            );
            assertHashJoinSql(
                    "select * from t1 full join t2 on j = i where j = 1",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            """
            );
        });
    }

    @Test
    public void testLeftHashJoinWithWhere4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s1 string)");
            execute("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            execute("create table t2 (j int, s2 string)");
            execute("insert into t2 values (5, 'e'), (3, 'c'), (2, 'b'), (1, 'a');");

            assertHashJoinSql(
                    "select * from t1 left join t2 on j = i where j = 1 or j = null",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            4\td\tnull\t
                            """
            );
            assertHashJoinSql(
                    "select * from t1 right join t2 on j = i where j = 1 or j = null",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            """
            );
            assertHashJoinSql(
                    "select * from t1 full join t2 on j = i where j = 1 or j = null",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            4\td\tnull\t
                            """
            );
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition0() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int);");
            execute("create table t2 as (select x+10 j from long_sequence(3))");

            assertQuery("select * from t1 left join t2 on t1.i+10 = t2.j")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("i\tj\n");
            assertQuery("select * from t1 right join t2 on t1.i+10 = t2.j")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            null\t11
                            null\t12
                            null\t13
                            """);
            assertQuery("select * from t1 full join t2 on t1.i+10 = t2.j")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            null\t11
                            null\t12
                            null\t13
                            """);
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select x i from long_sequence(5))");
            execute("create table t2 as (select x+10 j from long_sequence(3))");

            assertQuery("select * from t1 left join t2 on t1.i+10 = t2.j")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\t11
                            2\t12
                            3\t13
                            4\tnull
                            5\tnull
                            """);
            assertQuery("select * from t1 right join t2 on t1.i+10 = t2.j")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\t11
                            2\t12
                            3\t13
                            """);
            assertQuery("select * from t1 full join t2 on t1.i+10 = t2.j")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\t11
                            2\t12
                            3\t13
                            4\tnull
                            5\tnull
                            """);
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select x i from long_sequence(5))");
            execute("create table t2 as (select x-3 j from long_sequence(3))");//-2,-1,0

            assertQuery("select * from t1 left join t2 on t1.i = - t2.j")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\t-1
                            2\t-2
                            3\tnull
                            4\tnull
                            5\tnull
                            """);

            assertQuery("select * from t1 right join t2 on t1.i = - t2.j")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            2\t-2
                            1\t-1
                            null\t0
                            """);

            assertQuery("select * from t1 full join t2 on t1.i = - t2.j")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\t-1
                            2\t-2
                            3\tnull
                            4\tnull
                            5\tnull
                            null\t0
                            """);
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int)");
            execute("insert into t1 values (1), (-2), (3), (-4), (5);");
            execute("create table t2 (j int)");
            execute("insert into t2 values (-1), (-2), (3), (0), (-5);");

            assertQuery("select * from t1 left join t2 on abs(t1.i) = abs(t2.j)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\t-1
                            -2\t-2
                            3\t3
                            -4\tnull
                            5\t-5
                            """);
            assertQuery("select * from t1 right join t2 on abs(t1.i) = abs(t2.j)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\t-1
                            -2\t-2
                            3\t3
                            null\t0
                            5\t-5
                            """);
            assertQuery("select * from t1 right join t2 on abs(t1.i) = abs(t2.j)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\t-1
                            -2\t-2
                            3\t3
                            null\t0
                            5\t-5
                            """);
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int)");
            execute("insert into t1 values (1), (2), (3), (4), (5);");
            execute("create table t2 (j int)");
            execute("insert into t2 values (-1), (-2), (-3), (-4), (-5);");

            assertQuery("select * from t1 left join t2 on case when i < 4 then 0 else i end = abs(j)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\tnull
                            2\tnull
                            3\tnull
                            4\t-4
                            5\t-5
                            """);
            assertQuery("select * from t1 right join t2 on case when i < 4 then 0 else i end = abs(j)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            null\t-1
                            null\t-2
                            null\t-3
                            4\t-4
                            5\t-5
                            """);
            assertQuery("select * from t1 full join t2 on case when i < 4 then 0 else i end = abs(j)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\tnull
                            2\tnull
                            3\tnull
                            4\t-4
                            5\t-5
                            null\t-1
                            null\t-2
                            null\t-3
                            """);
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int)");
            execute("insert into t1 values (1), (2), (3), (4), (5);");
            execute("create table t2 (j int)");
            execute("insert into t2 values (-5), (-4), (-3), (-2), (-1);");

            assertQuery("select * from t1 left join t2 on i > 4  ")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\tnull
                            2\tnull
                            3\tnull
                            4\tnull
                            5\t-5
                            5\t-4
                            5\t-3
                            5\t-2
                            5\t-1
                            """);
            assertQuery("select * from t1 right join t2 on i > 4  ")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            5\t-5
                            5\t-4
                            5\t-3
                            5\t-2
                            5\t-1
                            """);
            assertQuery("select * from t1 full join t2 on i > 4  ")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\tnull
                            2\tnull
                            3\tnull
                            4\tnull
                            5\t-5
                            5\t-4
                            5\t-3
                            5\t-2
                            5\t-1
                            """);
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition6() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int)");
            execute("insert into t1 values (1), (2), (3), (4), (5);");
            execute("create table t2 (j int)");
            execute("insert into t2 values (-5), (-4), (-3), (-2), (-1);");

            assertQuery("select * from t1 left join t2 on i > 4 and j < -3 ")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\tnull
                            2\tnull
                            3\tnull
                            4\tnull
                            5\t-5
                            5\t-4
                            """);
            assertQuery("select * from t1 right join t2 on i > 4 and j < -3 ")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            5\t-5
                            5\t-4
                            null\t-3
                            null\t-2
                            null\t-1
                            """);
            assertQuery("select * from t1 full join t2 on i > 4 and j < -3 ")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\tnull
                            2\tnull
                            3\tnull
                            4\tnull
                            5\t-5
                            5\t-4
                            null\t-3
                            null\t-2
                            null\t-1
                            """);
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition7() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int)");
            execute("insert into t1 values (1), (2), (3), (4), (5);");
            execute("create table t2 (j int)");
            execute("insert into t2 values (-5), (-4), (-3), (-2), (-1);");

            assertQuery("select * from t1 left join t2 on i*j >= -4 ")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\t-4
                            1\t-3
                            1\t-2
                            1\t-1
                            2\t-2
                            2\t-1
                            3\t-1
                            4\t-1
                            5\tnull
                            """);
            assertQuery("select * from t1 right join t2 on i*j >= -4 ")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            null\t-5
                            1\t-4
                            1\t-3
                            1\t-2
                            2\t-2
                            1\t-1
                            2\t-1
                            3\t-1
                            4\t-1
                            """);
            assertQuery("select * from t1 full join t2 on i*j >= -4 ")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\t-4
                            1\t-3
                            1\t-2
                            1\t-1
                            2\t-2
                            2\t-1
                            3\t-1
                            4\t-1
                            5\tnull
                            null\t-5
                            """);
        });
    }

    @Test
    public void testLeftJoinOnFunctionCondition8() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int)");
            execute("insert into t1 values (1), (2), (3), (4), (5);");
            execute("create table t2 (j int)");
            execute("insert into t2 values (-5), (-4), (-3), (-2), (-1);");

            assertQuery("select * from t1 left join t2 on abs(i) = abs(j) and abs(i*j) <= 4")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\t-1
                            2\t-2
                            3\tnull
                            4\tnull
                            5\tnull
                            """);
            assertQuery("select * from t1 right join t2 on abs(i) = abs(j) and abs(i*j) <= 4")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            null\t-5
                            null\t-4
                            null\t-3
                            2\t-2
                            1\t-1
                            """);
            assertQuery("select * from t1 right join t2 on abs(i) = abs(j) and abs(i*j) <= 4")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            null\t-5
                            null\t-4
                            null\t-3
                            2\t-2
                            1\t-1
                            """);
        });
    }

    @Test
    public void testLeftJoinOnFunctionConditionWith3Tables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select x i from long_sequence(5))");
            execute("create table t2 as (select x+10 j from long_sequence(3))");
            execute("create table t3 as (select x+1 k from long_sequence(3))");

            assertQuery("select * from t1 left join (select * from t2 left join t3 on t2.j-1 = t3.k) tx on t1.i+10 = tx.j")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj\tk
                            1\t11\tnull
                            2\t12\tnull
                            3\t13\tnull
                            4\tnull\tnull
                            5\tnull\tnull
                            """);
            assertQuery("select * from t1 right join (select * from t2 right join t3 on t2.j-1 = t3.k) tx on t1.i+10 = tx.j")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj\tk
                            null\tnull\t2
                            null\tnull\t3
                            null\tnull\t4
                            """);
            assertQuery("select * from t1 right join (select * from t2 right join t3 on t2.j-1 = t3.k) tx on t1.i+10 = tx.j")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj\tk
                            null\tnull\t2
                            null\tnull\t3
                            null\tnull\t4
                            """);
        });
    }

    @Test
    public void testLeftJoinOnPredicateMasterOnly() throws Exception {
        // Same-table equality on the master side (x.a = x.b) inside a LEFT/RIGHT/FULL OUTER ON
        // clause must be honoured: rows where x.a != x.b cannot match any slave row.
        // The optimiser previously dropped the predicate silently, joining all x rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, a INT, b INT)");
            execute("INSERT INTO x VALUES (1, 1, 1), (2, 1, 2)");
            execute("CREATE TABLE y (id INT)");
            execute("INSERT INTO y VALUES (1), (2)");

            assertQuery("SELECT x.id, x.a, x.b, y.id FROM x LEFT JOIN y ON x.id = y.id AND x.a = x.b ORDER BY x.id")
                    .noLeakCheck()
                    .returns("""
                            id\ta\tb\tid1
                            1\t1\t1\t1
                            2\t1\t2\tnull
                            """);
            assertQuery("SELECT x.id, x.a, x.b, y.id FROM x RIGHT JOIN y ON x.id = y.id AND x.a = x.b ORDER BY y.id")
                    .noLeakCheck()
                    .returns("""
                            id\ta\tb\tid1
                            1\t1\t1\t1
                            null\tnull\tnull\t2
                            """);
            assertQuery("SELECT x.id, x.a, x.b, y.id FROM x FULL JOIN y ON x.id = y.id AND x.a = x.b ORDER BY x.id, y.id")
                    .noLeakCheck()
                    .returns("""
                            id\ta\tb\tid1
                            null\tnull\tnull\t2
                            1\t1\t1\t1
                            2\t1\t2\tnull
                            """);
        });
    }

    @Test
    public void testLeftJoinOnPredicateSlaveOnly() throws Exception {
        // Same-table equality on the slave side (y.a = y.b) inside a LEFT/RIGHT/FULL OUTER ON
        // clause must be honoured: slave rows where y.a != y.b cannot match the master.
        // The optimiser previously dropped the predicate silently, leaving every y row eligible.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT)");
            execute("INSERT INTO x VALUES (1), (2)");
            execute("CREATE TABLE y (id INT, a INT, b INT)");
            execute("INSERT INTO y VALUES (1, 1, 1), (1, 1, 2), (3, 5, 5)");

            assertQuery("SELECT x.id, y.id, y.a, y.b FROM x LEFT JOIN y ON x.id = y.id AND y.a = y.b ORDER BY x.id")
                    .noLeakCheck()
                    .returns("""
                            id\tid1\ta\tb
                            1\t1\t1\t1
                            2\tnull\tnull\tnull
                            """);
            assertQuery("SELECT x.id, y.id, y.a, y.b FROM x RIGHT JOIN y ON x.id = y.id AND y.a = y.b ORDER BY x.id, y.id, y.a, y.b")
                    .noLeakCheck()
                    .returns("""
                            id\tid1\ta\tb
                            null\t1\t1\t2
                            null\t3\t5\t5
                            1\t1\t1\t1
                            """);
            assertQuery("SELECT x.id, y.id, y.a, y.b FROM x FULL JOIN y ON x.id = y.id AND y.a = y.b ORDER BY x.id, y.id")
                    .noLeakCheck()
                    .returns("""
                            id\tid1\ta\tb
                            null\t1\t1\t2
                            null\t3\t5\t5
                            1\t1\t1\t1
                            2\tnull\tnull\tnull
                            """);
        });
    }

    @Test
    public void testLeftJoinWithConstantFalseFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select x i from long_sequence(3))");
            execute("create table t2 as (select x+10 j from long_sequence(3))");

            assertQuery("select * from t1 join t2 on i=j and abs(1) = 0")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("i\tj\n");
            assertQuery("select * from t1 left join t2 on i=j and abs(1) = 0")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tj
                            1\tnull
                            2\tnull
                            3\tnull
                            """);
            assertQuery("select * from t1 right join t2 on i=j and abs(1) = 0 order by i, j")
                    .noLeakCheck()
                    .returns("""
                            i\tj
                            null\t11
                            null\t12
                            null\t13
                            """);
            assertQuery("select * from t1 full join t2 on i=j and abs(1) = 0 order by i, j")
                    .noLeakCheck()
                    .returns("""
                            i\tj
                            null\t11
                            null\t12
                            null\t13
                            1\tnull
                            2\tnull
                            3\tnull
                            """);
        });
    }

    @Test
    public void testLeftJoinWithNestedAliases() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table dim_apTemperature as (" +
                            "  select x::int id," +
                            "         rnd_str('a','b','c') as category," +
                            "         rnd_float() aparent_temperature" +
                            "  from long_sequence(10)" +
                            ");"
            );
            execute(
                    "create table fact_table as (" +
                            "  select x::int id_aparent_temperature," +
                            "         (x * 120000000)::timestamp date_time," +
                            "         rnd_float() radiation," +
                            "         rnd_float() energy_power" +
                            "  from long_sequence(10)" +
                            ");"
            );

            String query = """
                    SELECT
                      "dim_ap_temperature".category "dim_ap_temperature__category",
                      timestamp_floor('d', to_timezone("fact_table".date_time, 'UTC')) "fact_table__date_time_day"
                    FROM
                      fact_table AS "fact_table"
                      LEFT JOIN dim_apTemperature AS "dim_ap_temperature" ON "fact_table".id_aparent_temperature = "dim_ap_temperature".id
                    LIMIT 3;""";
            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            dim_ap_temperature__category\tfact_table__date_time_day
                            a\t1970-01-01T00:00:00.000000Z
                            b\t1970-01-01T00:00:00.000000Z
                            c\t1970-01-01T00:00:00.000000Z
                            """);
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: 3 skip-rows-max: 0 take-rows-max: 3
                                VirtualRecord
                                  functions: [dim_ap_temperature__category,timestamp_floor('day',to_timezone(date_time))]
                                    SelectedRecord
                                        Hash Left Outer Join Light
                                          condition: dim_ap_temperature.id=fact_table.id_aparent_temperature
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: fact_table
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: dim_apTemperature
                            """);

            query = """
                    SELECT
                      "dim_ap_temperature".category "dim_ap_temperature__category",
                      timestamp_floor('d', to_timezone("fact_table".date_time, 'UTC')) "fact_table__date_time_day"
                    FROM
                      fact_table AS "fact_table"
                      RIGHT JOIN dim_apTemperature AS "dim_ap_temperature" ON "fact_table".id_aparent_temperature = "dim_ap_temperature".id
                    LIMIT 3;""";
            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            dim_ap_temperature__category\tfact_table__date_time_day
                            a\t1970-01-01T00:00:00.000000Z
                            b\t1970-01-01T00:00:00.000000Z
                            c\t1970-01-01T00:00:00.000000Z
                            """);
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: 3 skip-rows-max: 0 take-rows-max: 3
                                VirtualRecord
                                  functions: [dim_ap_temperature__category,timestamp_floor('day',to_timezone(date_time))]
                                    SelectedRecord
                                        Hash Right Outer Join Light
                                          condition: dim_ap_temperature.id=fact_table.id_aparent_temperature
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: fact_table
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: dim_apTemperature
                            """);

            query = """
                    SELECT
                      "dim_ap_temperature".category "dim_ap_temperature__category",
                      timestamp_floor('d', to_timezone("fact_table".date_time, 'UTC')) "fact_table__date_time_day"
                    FROM
                      fact_table AS "fact_table"
                      FULL JOIN dim_apTemperature AS "dim_ap_temperature" ON "fact_table".id_aparent_temperature = "dim_ap_temperature".id
                    LIMIT 3;""";
            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            dim_ap_temperature__category\tfact_table__date_time_day
                            a\t1970-01-01T00:00:00.000000Z
                            b\t1970-01-01T00:00:00.000000Z
                            c\t1970-01-01T00:00:00.000000Z
                            """);
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: 3 skip-rows-max: 0 take-rows-max: 3
                                VirtualRecord
                                  functions: [dim_ap_temperature__category,timestamp_floor('day',to_timezone(date_time))]
                                    SelectedRecord
                                        Hash Full Outer Join Light
                                          condition: dim_ap_temperature.id=fact_table.id_aparent_temperature
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: fact_table
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: dim_apTemperature
                            """);
        });
    }

    @Test
    public void testLtJoinLeftTimestampDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            execute("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertQuery("select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from (x order by timestamp desc) x lt join y on y.sym2 = x.sym")
                    .noLeakCheck()
                    .fails(93, "left");
        });
    }

    @Test
    public void testLtJoinNoKeyNoLeaks() throws Exception {
        testJoinForCursorLeaks("with crj as (select x, ts from xx latest by x) select xx.x from xx lt join crj ", false);
    }

    @Test
    public void testLtJoinNoLeftTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10))");
            execute("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertQuery("select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x lt join y on y.sym2 = x.sym")
                    .noLeakCheck()
                    .fails(65, "left");
        });
    }

    @Test
    public void testLtJoinNoRightTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            execute("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30))");
            assertQuery("select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x lt join y on y.sym2 = x.sym")
                    .noLeakCheck()
                    .fails(65, "right");
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
            execute("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            execute("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertQuery("select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x lt join (y order by timestamp desc) y on y.sym2 = x.sym")
                    .fails(65, "right");
        });
    }

    @Test
    public void testLtJoinWithComplexConditionFails1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            execute("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 lt join t2 on l1=l2+5", "unsupported LT join expression [expr='l1 = l2 + 5']", 33);
        });
    }

    @Test
    public void testLtJoinWithComplexConditionFails2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            execute("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 lt join t2 on l1>l2", "unsupported LT join expression [expr='l1 > l2']", 33);
        });
    }

    @Test
    public void testLtJoinWithComplexConditionFails3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            execute("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 lt join t2 on l1=abs(l2)", "unsupported LT join expression [expr='l1 = abs(l2)']", 33);
        });
    }

    @Test
    public void testLtJoinWithComplexConditionFails4() throws Exception {
        // Same-table equality on the slave side (l2 = m2) is now routed to the
        // outer-join expression clause and surfaced as an unsupported-expression
        // error, instead of being silently dropped.
        assertMemoryLeak(() -> {
            execute("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            execute("create table t2 (l2 long, m2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 lt join t2 on l1=l2 and l2=m2", "unsupported LT join expression [expr='l2 = m2']", 43);
        });
    }

    @Test
    public void testLtJoinWithCondition01() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            execute("insert into t1 select x, x::timestamp from long_sequence(3)");
            execute("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");
            execute("insert into t2 select x, x::timestamp from long_sequence(3)");

            assertQuery("select * from t1 lt join t2 on l1=l2")
                    .noLeakCheck()
                    .timestamp("ts1")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            l1\tts1\tl2\tts2
                            1\t1970-01-01T00:00:00.000001Z\tnull\t
                            2\t1970-01-01T00:00:00.000002Z\tnull\t
                            3\t1970-01-01T00:00:00.000003Z\tnull\t
                            """);
        });
    }

    @Test
    public void testLtJoinWithoutCondition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            execute("insert into t1 select x, x::timestamp from long_sequence(3)");
            execute("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");
            execute("insert into t2 select x, x::timestamp from long_sequence(3)");

            assertQuery("select * from t1 lt join t2")
                    .noLeakCheck()
                    .timestamp("ts1")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            l1\tts1\tl2\tts2
                            1\t1970-01-01T00:00:00.000001Z\tnull\t
                            2\t1970-01-01T00:00:00.000002Z\t1\t1970-01-01T00:00:00.000001Z
                            3\t1970-01-01T00:00:00.000003Z\t2\t1970-01-01T00:00:00.000002Z
                            """);
        });
    }

    @Test
    public void testLtJoinWithoutCondition2() throws Exception {
        // Here we test case when all slave records have newer timestamps than what's in the master table.
        assertMemoryLeak(() -> {
            execute("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            execute("insert into t1 select x, x::timestamp from long_sequence(3)");
            execute("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");
            execute("insert into t2 select x, (x + 1000000)::timestamp from long_sequence(3)");

            assertQuery("select * from t1 lt join t2")
                    .noLeakCheck()
                    .timestamp("ts1")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            l1\tts1\tl2\tts2
                            1\t1970-01-01T00:00:00.000001Z\tnull\t
                            2\t1970-01-01T00:00:00.000002Z\tnull\t
                            3\t1970-01-01T00:00:00.000003Z\tnull\t
                            """);
        });
    }

    @Test
    public void testMarkoutCrossJoinCount() throws Exception {
        execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
        // Insert 10 master rows with 1-second spacing
        for (int i = 1; i <= 10; i++) {
            execute("INSERT INTO orders VALUES (" + i + ", " + (i * 1_000_000_000L) + ")");
        }
        // 100-row sequence of offsets creates 1000 total rows
        String sql = """
                WITH offsets AS (
                    SELECT 1_000_000 * (x-1) usec_offs
                    FROM long_sequence(100)
                )
                SELECT /*+ markout_horizon(orders offsets) */ id, order_ts + usec_offs AS ts
                FROM orders CROSS JOIN offsets
                ORDER BY order_ts + usec_offs
                """;
        assertSkipToAndCalculateSize(sql, 1000);
    }

    @Test
    public void testMasterFilterAnchorsAtLastNullingJoinInOrder() throws Exception {
        // wm is NULL-extended by two joins: a homogenized CROSS_RIGHT (non-equi ON) and a RIGHT join.
        // doReorderTables appends the context-less CROSS_RIGHT last, so it executes AFTER the RIGHT join.
        // The master WHERE wm.c = 1 must anchor at that last-executing nulling join; anchoring by model
        // index placed it below the CROSS_RIGHT, which then re-synthesized a NULL-master row that leaked.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wm (x INT, q INT, c INT)");
            execute("INSERT INTO wm VALUES (1, 1, 9)");
            execute("CREATE TABLE ws1 (y INT)");
            execute("INSERT INTO ws1 VALUES (100)");
            execute("CREATE TABLE ws2 (q INT)");
            execute("INSERT INTO ws2 VALUES (2)");
            assertQuery("SELECT * FROM wm RIGHT JOIN ws1 ON wm.x > ws1.y RIGHT JOIN ws2 ON ws2.q = wm.q WHERE wm.c = 1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Filter filter: wm.c=1")
                    .returns("x\tq\tc\ty\tq1\n");
        });
    }

    @Test
    public void testMasterFilterPushesTransitiveSlaveConstForLiteral() throws Exception {
        // Parity with the regex path: a master-side equality predicate on a NULL-extending join
        // stays a post-join filter (removing NULL-master rows), but a non-null literal constant is
        // still propagated to the slave through the join key, so the slave is pre-filtered. The
        // push is result-neutral (matched rows have b.sym = a.sym, and the post-join filter already
        // drops the NULL-master rows). A bind variable is deliberately NOT propagated, because it can
        // be NULL at runtime and `null = null` is TRUE; the literal and bind forms must still agree.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (sym SYMBOL, c1 INT)");
            execute("INSERT INTO m VALUES ('s2', 100), ('x', 200)");
            execute("CREATE TABLE s (sym SYMBOL, v INT)");
            execute("INSERT INTO s VALUES ('s2', 10), ('x', 50), ('zz', 99)");

            final String expected = "e0\te1\ns2\t100\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                final String literal = "SELECT a.sym AS e0, a.c1 AS e1 FROM m a " + joinType + " JOIN s b ON a.sym = b.sym WHERE a.sym = 's2'";
                final String bind = "SELECT a.sym AS e0, a.c1 AS e1 FROM m a " + joinType + " JOIN s b ON a.sym = b.sym WHERE a.sym = :sym::SYMBOL";

                // Literal: the post-join filter (a.sym) stays, and the transitive slave filter (sym)
                // is pushed into the slave sub-query.
                bindVariableService.clear();
                assertQuery(literal).noLeakCheck().noRandomAccess().withPlanContaining("filter: sym='s2'").returns(expected);

                // Bind: no transitive push, but the result must match the literal form.
                bindVariableService.clear();
                bindVariableService.setStr("sym", "s2");
                assertQuery(bind).noLeakCheck().noRandomAccess().returns(expected);
            }
        });
    }

    @Test
    public void testMultiTableEqualityMasterFilterStaysPostJoin() throws Exception {
        // Companion to testMultiTableMasterFilterStaysPostJoin, which uses an INEQUALITY (t0.a < t1.b)
        // that analyseEquals routes straight to assignFilters. An EQUALITY across two master tables
        // (t0.a = t1.b) instead folds into the inner join's keys, so it was applied BEFORE the later
        // RIGHT/FULL OUTER NULL-extends t0 and t1 for the unmatched t2 key 2. With the equality folded,
        // the inner t0/t1 join is empty (1 != 5), so every t2 row became a NULL-master row and the
        // filter never ran -- leaking (null,null,1) on top of the legitimate (null,null,2), 2 rows for
        // 1. Held post-join the inner join keeps (1,5,1), which t0.a=t1.b drops, and the outer join's
        // (null,null,2) survives because null=null is true for INT.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, k INT)");
            execute("INSERT INTO t0 VALUES (1, 1)");
            execute("CREATE TABLE t1 (b INT, k INT)");
            execute("INSERT INTO t1 VALUES (5, 1)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (2)");

            final String expected = "a\tb\tk\nnull\tnull\t2\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT t0.a, t1.b, t2.k FROM t0 JOIN t1 ON t0.k = t1.k " + joinType + " JOIN t2 ON t2.k = t1.k WHERE t0.a = t1.b")
                        .noLeakCheck()
                        .noRandomAccess()
                        .withPlanContaining("Filter filter: t0.a=t1.b")
                        .returns(expected);
            }
        });
    }

    @Test
    public void testMultiTableEqualityOuterJoinedTableStaysPostJoin() throws Exception {
        // Variant of testMultiTableEqualityMasterFilterStaysPostJoin where the HIGHER table of the
        // equality (t1) is itself reached via an outer join, then NULL-extended by a SECOND outer
        // join. analyseEquals routes a two-table equality whose higher table is barrier-joined to a
        // model-order post-join anchor at that table's own join -- below the later FULL/RIGHT OUTER,
        // leaking (null,null,1) on top of the legitimate (null,null,3). Held above the outer join,
        // the matched (1,5) row fails 1=5 and only (null,null,3) survives because null=null is true.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, k INT)");
            execute("INSERT INTO t0 VALUES (1, 1)");
            execute("CREATE TABLE t1 (b INT, k INT)");
            execute("INSERT INTO t1 VALUES (5, 1), (9, 2)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (3)");

            final String expected = "a\tb\tk\nnull\tnull\t3\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT t0.a, t1.b, t2.k FROM t0 RIGHT JOIN t1 ON t0.k = t1.k " + joinType + " JOIN t2 ON t1.k = t2.k WHERE t0.a = t1.b")
                        .noLeakCheck()
                        .noRandomAccess()
                        .withPlanContaining("Filter filter: t0.a=t1.b")
                        .returns(expected);
            }
        });
    }

    @Test
    public void testMultiTableEqualityDeferredFilterStaysPostJoin() throws Exception {
        // Covers the hasNonEquiNullingJoin arm of the two-table equality deferral;
        // testMultiTableEqualityMasterFilterStaysPostJoin covers the masterNullingJoinIndex arm.
        // The WHERE equality (c.c1 = d.d1) is across two INNER-joined tables and the level holds a
        // non-equi RIGHT/FULL OUTER, so analyseEquals defers it to assignFilters, which keeps it
        // post-join. Folding it into the c/d inner join would empty that subtree (7 != 8), and on
        // master, where the outer join ran after c and d, the join then paired its slave row with
        // NULL c/d and leaked (null,50,null,null) -- 1 row for 0. constrainRightAndFullJoinOrder
        // keeps the outer join at its SQL position, so the inner joins now drop that row anyway, and
        // the deferral is what still keeps the equality out of the c/d join.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, k INT)");
            execute("INSERT INTO a VALUES (100, 1)");
            execute("CREATE TABLE b (y INT)");
            execute("INSERT INTO b VALUES (50)");
            execute("CREATE TABLE c (k INT, c1 INT)");
            execute("INSERT INTO c VALUES (1, 7)");
            execute("CREATE TABLE d (k INT, d1 INT)");
            execute("INSERT INTO d VALUES (1, 8)");

            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT a.x, b.y, c.c1, d.d1 FROM a " + joinType + " JOIN b ON a.x > b.y JOIN c ON c.k = a.k JOIN d ON d.k = a.k WHERE c.c1 = d.d1")
                        .noLeakCheck()
                        .noRandomAccess()
                        .withPlanContaining("Filter filter: c.c1=d.d1")
                        .returns("x\ty\tc1\td1\n");
            }
        });
    }

    @Test
    public void testMultiTableMasterFilterStaysPostJoin() throws Exception {
        // A WHERE predicate that references TWO master tables (t0.a < t1.b) reaches assignFilters'
        // multi-reference else-branch, which anchored it at the inner join where both tables arrive.
        // A later RIGHT/FULL OUTER join NULL-extends t0 and t1 for the unmatched t2 key 2; the filter
        // must stay above that join. Anchoring below it leaked the (null,null,2) row -- 2 rows for 1.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, k INT)");
            execute("INSERT INTO t0 VALUES (1, 1)");
            execute("CREATE TABLE t1 (b INT, k INT)");
            execute("INSERT INTO t1 VALUES (5, 1)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (2)");

            final String expected = "a\tb\tk\n1\t5\t1\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT t0.a, t1.b, t2.k FROM t0 JOIN t1 ON t0.k = t1.k " + joinType + " JOIN t2 ON t2.k = t1.k WHERE t0.a < t1.b")
                        .noLeakCheck()
                        .noRandomAccess()
                        .withPlanContaining("Filter filter: t0.a<t1.b")
                        .returns(expected);
            }
        });
    }

    @Test
    public void testMultipleJoinsWithTopLevelSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE train ( " +
                            "  id INT, " +
                            "  date timestamp, " +
                            "  store_nbr INT, " +
                            "  family SYMBOL, " +
                            "  sales DOUBLE " +
                            ") timestamp (date) PARTITION BY YEAR"
            );

            execute("insert into train values (1, '2015-05-31T00:00:00', 1, 'A', 1.0 )");

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

            assertRepeatedJoinQuery(query, "LT", false);
            assertRepeatedJoinQuery(query, "ASOF", false);
            assertRepeatedJoinQuery(query, "INNER", true);
            assertRepeatedJoinQuery(query, "LEFT", false);
        });
    }

    @Test
    public void testNestedCrossJoinCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t(c0 timestamp, c1 int, c2 int);\n");
            execute("insert into t values('2023-09-21T10:00:00.000000Z',1,1);\n");
            execute("insert into t values('2023-09-21T10:00:00.000000Z',1,1);\n");

            assertQuery("select count(*) " +
                    "from t as t1 " +
                    "join t as t2 on t1.c0<t2.c0 " +
                    "cross join t as t3")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n0\n");

            assertQuery("select count(*) " +
                    "from t as t3 " +
                    "cross join t as t1 " +
                    "join t as t2 on t1.c0<t2.c0 ")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n0\n");

            assertQuery("select count(*) " +
                    "from t as t3 " +
                    "cross join t as t2 " +
                    "join t as t1 on t1.c0<t2.c0 ")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n0\n");
        });

    }

    @Test
    public void testNonEquiOuterJoinMasterFilterStaysPostJoin() throws Exception {
        // A RIGHT/FULL OUTER join with a NON-equi ON clause carries no JoinContext, so
        // homogenizeCrossJoins (which runs before assignFilters) rewrites it to
        // JOIN_CROSS_RIGHT/JOIN_CROSS_FULL. Those CROSS variants still NULL-extend the
        // master (NestedLoopRight/FullJoin), so a master-only WHERE must stay a post-join
        // filter. With the predicate pushed into the master sub-query the unmatched
        // (NULL-master) slave rows leaked. i=5 is a non-NULL predicate, so the result must
        // equal the INNER ground truth (5|-5, 5|-4).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (i INT)");
            execute("INSERT INTO t1 VALUES (1),(2),(3),(4),(5)");
            execute("CREATE TABLE t2 (j INT)");
            execute("INSERT INTO t2 VALUES (-5),(-4),(-3),(-2),(-1)");

            final String expected = "i\tj\n5\t-5\n5\t-4\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                final String literal = "SELECT i, j FROM t1 " + joinType + " JOIN t2 ON i > 4 AND j < -3 WHERE i = 5 ORDER BY j";

                // i=5 must stay above the join as a post-join Filter, not be pushed into t1.
                bindVariableService.clear();
                assertQuery(literal).noLeakCheck().withPlanContaining("Filter filter: t1.i=5").returns(expected);

                final String bind = "SELECT i, j FROM t1 " + joinType + " JOIN t2 ON i > 4 AND j < -3 WHERE i = :v::INT ORDER BY j";
                bindVariableService.clear();
                bindVariableService.setInt("v", 5);
                // Bind-variable form must produce the identical result under the full assertion battery.
                assertQuery(bind).noLeakCheck().returns(expected);
            }
        });
    }

    @Test
    public void testNonEquiOuterJoinLaterTableFilterPushesDown() throws Exception {
        // Companion to testNonEquiOuterJoinMasterFilterStaysPostJoin, which filters the directly
        // NULL-extended master that masterNullingJoinIndex catches in model order. Here the WHERE
        // predicate (c.v = 1) is on an INNER-joined table that the non-equi RIGHT/FULL OUTER above it
        // cannot NULL-extend: constrainRightAndFullJoinOrder keeps that join at its SQL position, so c
        // joins its output and assignFilters pushes the predicate into c's scan. Master ran the join
        // after c and returned an extra (null,100,null) row -- 2 rows for 1.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1)");
            execute("CREATE TABLE b (y INT)");
            execute("INSERT INTO b VALUES (5), (100)");
            execute("CREATE TABLE c (k INT, v INT)");
            execute("INSERT INTO c VALUES (1, 1)");

            final String expected = "x\ty\tv\n10\t5\t1\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                final String literal = "SELECT a.x, b.y, c.v FROM a " + joinType + " JOIN b ON a.x > b.y JOIN c ON c.k = a.k WHERE c.v = 1 ORDER BY b.y";
                bindVariableService.clear();
                assertQuery(literal).noLeakCheck()
                        .withPlanContaining("filter: v=1")
                        .withPlanNotContaining("Filter filter: c.v=1")
                        .returns(expected);

                final String bind = "SELECT a.x, b.y, c.v FROM a " + joinType + " JOIN b ON a.x > b.y JOIN c ON c.k = a.k WHERE c.v = :v::INT ORDER BY b.y";
                bindVariableService.clear();
                bindVariableService.setInt("v", 1);
                assertQuery(bind).noLeakCheck().returns(expected);
            }
        });
    }

    @Test
    public void testNonEquiOuterJoinThenAsOfJoin() throws Exception {
        // An ASOF join keeps every master row, so it commutes with a non-equi RIGHT/FULL OUTER join and
        // still joins ta ahead of it; the outer join's NULL-master row stays NULL for td either way.
        // Written after the outer join, ASOF would reject its output for lacking a designated
        // timestamp. A CROSS JOIN that no later join depends on already follows the outer join, so it
        // does not pin the outer join ahead of the ASOF join either.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (ts TIMESTAMP, x INT, k INT) TIMESTAMP(ts)");
            execute("INSERT INTO ta VALUES ('2024-01-01T00:00:02.000000Z', 10, 1)");
            execute("CREATE TABLE b (y INT)");
            execute("INSERT INTO b VALUES (5), (100)");
            execute("CREATE TABLE x2 (k INT)");
            execute("INSERT INTO x2 VALUES (7), (8)");
            execute("CREATE TABLE td (ts TIMESTAMP, k INT, v SYMBOL) TIMESTAMP(ts)");
            execute("INSERT INTO td VALUES ('2024-01-01T00:00:01.000000Z', 1, 'asof1')");

            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT ta.x, b.y, td.v FROM ta " + joinType + " JOIN b ON ta.x > b.y ASOF JOIN td ON (k) ORDER BY b.y")
                        .noLeakCheck()
                        .returns("""
                                x\ty\tv
                                10\t5\tasof1
                                null\t100\t
                                """);
                assertQuery("SELECT ta.x, b.y, x2.k, td.v FROM ta " + joinType + " JOIN b ON ta.x > b.y CROSS JOIN x2 ASOF JOIN td ON (k) ORDER BY b.y, x2.k")
                        .noLeakCheck()
                        .returns("""
                                x\ty\tk\tv
                                10\t5\t7\tasof1
                                10\t5\t8\tasof1
                                null\t100\t7\t
                                null\t100\t8\t
                                """);
            }
        });
    }

    @Test
    public void testNonEquiOuterJoinThenAsOfJoinKeyedOnEarlierAsOfJoin() throws Exception {
        // The keyless ASOF or LT join of u runs ahead of the non-equi RIGHT/FULL OUTER join, so the join
        // of s, keyed on u's slave, can run ahead of it too: both match on m's timestamp, and the outer
        // join NULL-extends both slaves. The join of s used to stay after the outer join, which failed
        // the query for lack of a designated timestamp, unless the trailing join of u2 let the implied
        // dependency of s on m release s ahead of the outer join.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO m VALUES
                        (1, '2024-01-01T03:00:00.000000Z'),
                        (2, '2024-01-01T04:00:00.000000Z')
                    """);
            execute("CREATE TABLE r (k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO r VALUES
                        (0, '2024-01-01T01:00:00.000000Z'),
                        (5, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("CREATE TABLE s (k INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO s VALUES
                        (7, 70, '2024-01-01T02:00:00.000000Z'),
                        (8, 80, '2024-01-01T02:30:00.000000Z'),
                        (7, 71, '2024-01-01T03:30:00.000000Z')
                    """);
            execute("CREATE TABLE u (v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO u VALUES
                        (7, '2024-01-01T00:30:00.000000Z'),
                        (8, '2024-01-01T03:15:00.000000Z')
                    """);
            execute("CREATE TABLE u2 (v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO u2 VALUES (9, '2024-01-01T00:40:00.000000Z')");

            for (String outerJoin : new String[]{"RIGHT", "FULL"}) {
                for (String uJoin : new String[]{"ASOF", "LT"}) {
                    for (String sJoin : new String[]{"ASOF", "LT"}) {
                        final String from = " FROM m " + outerJoin + " JOIN r ON r.k < m.k " + uJoin + " JOIN u "
                                + sJoin + " JOIN s ON s.k = u.v";
                        assertQuery("SELECT m.k, r.k, u.v, s.v" + from + " ORDER BY m.k, r.k")
                                .noLeakCheck()
                                .returns("""
                                        k\tk1\tv\tv1
                                        null\t5\tnull\tnull
                                        1\t0\t7\t70
                                        2\t0\t8\t80
                                        """);
                        assertQuery("SELECT m.k, r.k, u.v, s.v, u2.v" + from + " " + sJoin + " JOIN u2 ORDER BY m.k, r.k")
                                .noLeakCheck()
                                .returns("""
                                        k\tk1\tv\tv1\tv2
                                        null\t5\tnull\tnull\tnull
                                        1\t0\t7\t70\t9
                                        2\t0\t8\t80\t9
                                        """);
                    }
                }
            }
        });
    }

    @Test
    public void testNonEquiOuterJoinThenAsOfJoinKeyedOnOuterJoinSlaveFails() throws Exception {
        // An ASOF or LT join keyed on r, the slave of a non-equi RIGHT/FULL OUTER join, cannot run ahead
        // of the outer join, and the outer join's output has no designated timestamp, so the query fails.
        // The keyless join of u runs ahead of the outer join and holds it back. The implied dependency of
        // s on m, which is not a join-context parent of s, used to release s meanwhile: s then joined m
        // alone and matched s.k against m.k. A key that reads only m lets s run ahead too.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO m VALUES (1, '2024-01-01T03:00:00.000000Z')");
            execute("CREATE TABLE r (k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO r VALUES
                        (0, '2024-01-01T01:00:00.000000Z'),
                        (5, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("CREATE TABLE s (k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO s VALUES (1, '2024-01-01T02:00:00.000000Z')");
            execute("CREATE TABLE u (v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO u VALUES (7, '2024-01-01T00:30:00.000000Z')");

            for (String outerJoin : new String[]{"RIGHT", "FULL"}) {
                for (String sJoin : new String[]{"ASOF", "LT"}) {
                    for (String uJoin : new String[]{"ASOF", "LT"}) {
                        final String head = "SELECT m.k, r.k, s.k, u.v FROM m " + outerJoin + " JOIN r ON r.k < m.k "
                                + sJoin + " JOIN s ON s.k = ";
                        final String sql = head + "r.k " + uJoin + " JOIN u";
                        assertQuery(sql)
                                .noLeakCheck()
                                .fails(sql.indexOf(sJoin + " JOIN s"), "left side of time series join has no timestamp");
                        assertQuery(head + "m.k " + uJoin + " JOIN u ORDER BY r.k")
                                .noLeakCheck()
                                .returns("""
                                        k\tk1\tk2\tv
                                        1\t0\t1\t7
                                        null\t5\tnull\tnull
                                        """);
                    }
                }
            }
        });
    }

    @Test
    public void testNonEquiOuterJoinThenAsOfJoinSlaveFilter() throws Exception {
        // constrainRightAndFullJoinOrder runs the ASOF or LT join ahead of the non-equi RIGHT/FULL
        // OUTER join, which then NULL-extends td. WHERE td.v = 'asof1' must drop the outer join's
        // NULL-master row for b.y = 100 instead of filtering td below the outer join, and FULL keeps
        // its unmatched master row (x = 1), which finds td. The equi ON clause leaves WHERE
        // predicates to eager model-order pushdown, so that spelling keeps the SQL order, where the
        // outer join's output has no designated timestamp.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (ts TIMESTAMP, x INT, k INT) TIMESTAMP(ts)");
            execute("INSERT INTO ta VALUES ('2024-01-01T00:00:02.000000Z', 10, 1), ('2024-01-01T00:00:03.000000Z', 1, 1)");
            execute("CREATE TABLE b (y INT, k INT)");
            execute("INSERT INTO b VALUES (5, 1), (100, 2)");
            execute("CREATE TABLE td (ts TIMESTAMP, k INT, v SYMBOL) TIMESTAMP(ts)");
            execute("INSERT INTO td VALUES ('2024-01-01T00:00:01.000000Z', 1, 'asof1')");

            for (String timeSeriesJoin : new String[]{"ASOF", "LT"}) {
                final String tail = " JOIN b ON ta.x > b.y " + timeSeriesJoin + " JOIN td ON (k) ";
                assertQuery("SELECT ta.x, b.y, td.v FROM ta RIGHT" + tail + "WHERE td.v = 'asof1' ORDER BY b.y, ta.x")
                        .noLeakCheck()
                        .returns("""
                                x\ty\tv
                                10\t5\tasof1
                                """);
                assertQuery("SELECT ta.x, b.y, td.v FROM ta FULL" + tail + "WHERE td.v = 'asof1' ORDER BY b.y, ta.x")
                        .noLeakCheck()
                        .returns("""
                                x\ty\tv
                                1\tnull\tasof1
                                10\t5\tasof1
                                """);
                assertQuery("SELECT ta.x, b.y, td.v FROM ta FULL" + tail + "WHERE td.v = null ORDER BY b.y, ta.x")
                        .noLeakCheck()
                        .returns("""
                                x\ty\tv
                                null\t100\t
                                """);
            }

            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                final String sql = "SELECT ta.x, b.y, td.v FROM ta " + joinType + " JOIN b ON ta.k = b.k ASOF JOIN td ON (k)";
                assertQuery(sql)
                        .noLeakCheck()
                        .fails(sql.indexOf("ASOF"), "left side of time series join has no timestamp");
            }
        });
    }

    @Test
    public void testNonEquiOuterJoinThenCrossJoinMultipliesNullExtendedRows() throws Exception {
        // CROSS JOIN x2 pairs every row of the non-equi RIGHT/FULL OUTER join with every x2 row,
        // including the NULL-master row for the unmatched b.y = 100. A later join on x2 made x2 a
        // dependency root that doReorderTables used to order before the outer join, which then
        // NULL-extended x2 as well and left a single (null,100,null,) row instead of one per x2 row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1)");
            execute("CREATE TABLE b (y INT)");
            execute("INSERT INTO b VALUES (5), (100)");
            execute("CREATE TABLE x2 (k INT)");
            execute("INSERT INTO x2 VALUES (7), (8)");
            execute("CREATE TABLE d (k INT, w SYMBOL)");
            execute("INSERT INTO d VALUES (7, 'd7')");

            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT a.x, b.y, x2.k, d.w FROM a " + joinType + " JOIN b ON a.x > b.y CROSS JOIN x2 LEFT JOIN d ON d.k = x2.k ORDER BY b.y, x2.k")
                        .noLeakCheck()
                        .returns("""
                                x\ty\tk\tw
                                10\t5\t7\td7
                                10\t5\t8\t
                                null\t100\t7\td7
                                null\t100\t8\t
                                """);
                assertQuery("SELECT a.x, b.y, x2.k, d.w FROM a " + joinType + " JOIN b ON a.x > b.y CROSS JOIN x2 JOIN d ON d.k = x2.k ORDER BY b.y, x2.k")
                        .noLeakCheck()
                        .returns("""
                                x\ty\tk\tw
                                10\t5\t7\td7
                                null\t100\t7\td7
                                """);
            }
        });
    }

    @Test
    public void testNonEquiOuterJoinThenInnerJoinDropsNullExtendedRows() throws Exception {
        // Joins associate left to right, so JOIN c ON c.k = a.k consumes the output of the non-equi
        // RIGHT/FULL OUTER join. The outer join NULL-extends a for the unmatched b.y = 100; that row's
        // a.k is NULL, so it matches no c row and the INNER join must drop it. The FULL join's
        // unmatched master row (a.x = 1) keeps a.k = 2, matches c, and must survive. The non-equi join
        // carries no JoinContext, so it homogenizes to a CROSS variant that reorderTables used to append
        // after c; that plan NULL-extended the already joined c and leaked a (null,100,) row. The
        // parenthesised spelling means the same join order, and the t0 variant puts two tables before
        // the outer join.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (k INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("CREATE TABLE a (x INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1), (1, 2)");
            execute("CREATE TABLE b (y INT)");
            execute("INSERT INTO b VALUES (5), (100)");
            execute("CREATE TABLE c (k INT, v SYMBOL)");
            execute("INSERT INTO c VALUES (1, 'foo'), (2, 'bar')");

            final String expectedRight = """
                    x\ty\tv
                    10\t5\tfoo
                    """;
            final String expectedFull = """
                    x\ty\tv
                    1\tnull\tbar
                    10\t5\tfoo
                    """;
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                final String expected = "RIGHT OUTER".equals(joinType) ? expectedRight : expectedFull;
                assertQuery("SELECT a.x, b.y, c.v FROM a " + joinType + " JOIN b ON a.x > b.y JOIN c ON c.k = a.k ORDER BY a.x")
                        .noLeakCheck()
                        .returns(expected);
                assertQuery("SELECT a.x, b.y, c.v FROM (a " + joinType + " JOIN b ON a.x > b.y) JOIN c ON c.k = a.k ORDER BY a.x")
                        .noLeakCheck()
                        .returns(expected);
                assertQuery("SELECT a.x, b.y, c.v FROM t0 JOIN a ON a.k = t0.k " + joinType + " JOIN b ON a.x > b.y JOIN c ON c.k = t0.k ORDER BY a.x")
                        .noLeakCheck()
                        .returns(expected);
            }
        });
    }

    @Test
    public void testNonEquiOuterJoinThenInnerJoinOnBothSides() throws Exception {
        // JOIN cj ON cj.k = a.k AND cj.j = b.y keys on both sides of the non-equi RIGHT/FULL OUTER join.
        // reorderTables used to let the context-free outer join take over cj.j = b.y from cj's join
        // key; the nested-loop outer join ignores join keys, so the condition vanished and the
        // (10,5,j7) row survived next to the matching (10,5,j5).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1)");
            execute("CREATE TABLE b (y INT)");
            execute("INSERT INTO b VALUES (5), (100)");
            execute("CREATE TABLE cj (k INT, j INT, v SYMBOL)");
            execute("INSERT INTO cj VALUES (1, 5, 'j5'), (1, 7, 'j7')");

            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT a.x, b.y, cj.v FROM a " + joinType + " JOIN b ON a.x > b.y JOIN cj ON cj.k = a.k AND cj.j = b.y")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("x\ty\tv\n10\t5\tj5\n");
            }
        });
    }

    @Test
    public void testNonEquiOuterJoinThenInnerJoinWithColumnEqColumnFilter() throws Exception {
        // Filter variant of testNonEquiOuterJoinThenInnerJoinDropsNullExtendedRows. The INNER join on
        // c.k = a.k drops the outer join's NULL-master row for b.y = 100 before WHERE runs, so only the
        // matched (10,5) row reaches c.v = c.w. The filter cannot hide a wrongly NULL-extended c row,
        // because NULL = NULL holds for INT and SYMBOL alike. The (2, NULL, NULL) rows match no a row.
        // In c_sym they make the SYMBOL dictionaries hold NULL, so a leaked row passes the SYMBOL
        // filter whether or not SYMBOL equality checks the dictionary for NULL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1)");
            execute("CREATE TABLE b (y INT)");
            execute("INSERT INTO b VALUES (5), (100)");
            execute("CREATE TABLE c_int (k INT, v INT, w INT)");
            execute("INSERT INTO c_int VALUES (1, 7, 7), (2, NULL, NULL)");
            execute("CREATE TABLE c_sym (k INT, v SYMBOL, w SYMBOL)");
            execute("INSERT INTO c_sym VALUES (1, 'foo', 'foo'), (2, NULL, NULL)");

            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT a.x, b.y, c.v, c.w FROM a " + joinType + " JOIN b ON a.x > b.y JOIN c_int c ON c.k = a.k WHERE c.v = c.w")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("x\ty\tv\tw\n10\t5\t7\t7\n");
                assertQuery("SELECT a.x, b.y, c.v, c.w FROM a " + joinType + " JOIN b ON a.x > b.y JOIN c_sym c ON c.k = a.k WHERE c.v = c.w")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("x\ty\tv\tw\n10\t5\tfoo\tfoo\n");
            }
        });
    }

    @Test
    public void testNonEquiOuterJoinThenLeftJoinBetweenAsOfJoins() throws Exception {
        // The keyless ASOF or LT joins of u and u2 run ahead of the non-equi RIGHT/FULL OUTER join, and
        // the LEFT join of s, which reads r, runs after it. The LEFT join's ON clause also makes s depend
        // on u, the model before it, which is not a join-context parent of s. That dependency used to
        // release s while the join of u2 still held the outer join back, so s joined ahead of r: the
        // equality matched s.k against m.k, and the non-equi ON clause failed to resolve r.k.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO m VALUES (1, '2024-01-01T03:00:00.000000Z')");
            execute("CREATE TABLE r (k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO r VALUES
                        (0, '2024-01-01T01:00:00.000000Z'),
                        (5, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("CREATE TABLE s (k INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO s VALUES (1, 1, '2024-01-01T02:00:00.000000Z')");
            execute("CREATE TABLE u (v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO u VALUES (7, '2024-01-01T00:30:00.000000Z')");
            execute("CREATE TABLE u2 (v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO u2 VALUES (8, '2024-01-01T00:40:00.000000Z')");

            for (String outerJoin : new String[]{"RIGHT", "FULL"}) {
                for (String timeSeriesJoin : new String[]{"ASOF", "LT"}) {
                    final String head = "SELECT m.k, r.k, s.k, u.v, u2.v FROM m " + outerJoin + " JOIN r ON r.k < m.k "
                            + timeSeriesJoin + " JOIN u LEFT JOIN s ON ";
                    final String tail = " " + timeSeriesJoin + " JOIN u2 ORDER BY r.k";
                    assertQuery(head + "s.k = r.k AND s.v > 0" + tail)
                            .noLeakCheck()
                            .returns("""
                                    k\tk1\tk2\tv\tv1
                                    1\t0\tnull\t7\t8
                                    null\t5\tnull\tnull\tnull
                                    """);
                    assertQuery(head + "s.k > r.k" + tail)
                            .noLeakCheck()
                            .returns("""
                                    k\tk1\tk2\tv\tv1
                                    1\t0\t1\t7\t8
                                    null\t5\tnull\tnull\tnull
                                    """);
                }
            }
        });
    }

    @Test
    public void testNonEquiOuterJoinThenOuterJoin() throws Exception {
        // A later equi RIGHT/FULL OUTER JOIN x2 ON x2.k = a.k consumes the non-equi RIGHT/FULL OUTER
        // join's output, where no a.k matches x2: RIGHT keeps only the NULL-extended x2 rows, FULL also
        // keeps both earlier rows. doReorderTables used to join x2 first, so the non-equi join dropped
        // the x2 rows and returned its own b rows instead.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1)");
            execute("CREATE TABLE b (y INT)");
            execute("INSERT INTO b VALUES (5), (100)");
            execute("CREATE TABLE x2 (k INT)");
            execute("INSERT INTO x2 VALUES (7), (8)");

            final String expectedRight = """
                    x\ty\tk
                    null\tnull\t7
                    null\tnull\t8
                    """;
            final String expectedFull = """
                    x\ty\tk
                    null\tnull\t7
                    null\tnull\t8
                    10\t5\tnull
                    null\t100\tnull
                    """;
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT a.x, b.y, x2.k FROM a " + joinType + " JOIN b ON a.x > b.y RIGHT JOIN x2 ON x2.k = a.k ORDER BY b.y, x2.k")
                        .noLeakCheck()
                        .returns(expectedRight);
                assertQuery("SELECT a.x, b.y, x2.k FROM a " + joinType + " JOIN b ON a.x > b.y FULL JOIN x2 ON x2.k = a.k ORDER BY b.y, x2.k")
                        .noLeakCheck()
                        .returns(expectedFull);
            }
        });
    }

    @Test
    public void testOuterConstFilterDoesNotLeakIntoNestedNullingJoin() throws Exception {
        // The outer query filters tc.k = 2 and the nested subquery RIGHT JOINs ta a to tb b under a
        // master-only WHERE a.k = 1. That WHERE references the NULL-extended master, so the
        // master-nulling guard keeps it post-join and skips registering a's constant in the
        // constNameTo* maps. Those maps are instance state keyed by bare column name; without a clear
        // between join-model levels the outer query's stale "k -> 2" entry survived into the nested
        // model, and addTransitiveFilters injected b.k = 2 into the nested slave, dropping the matching
        // row (0 rows instead of 1). optimiseJoins now clears the maps before recursing into nested
        // and union models, so the foreign constant no longer leaks.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tc (q INT, k INT)");
            execute("INSERT INTO tc VALUES (1, 2)");
            execute("CREATE TABLE ta (k INT)");
            execute("INSERT INTO ta VALUES (1)");
            execute("CREATE TABLE tb (k INT, v INT)");
            execute("INSERT INTO tb VALUES (1, 10)");

            assertQuery("SELECT * FROM tc " +
                    "JOIN (SELECT a.k k1, b.v FROM ta a RIGHT JOIN tb b ON a.k = b.k WHERE a.k = 1) x " +
                    "  ON tc.q = x.k1 " +
                    "WHERE tc.k = 2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("q\tk\tk1\tv\n1\t2\t1\t10\n");
        });
    }

    @Test
    public void testOuterConstFilterDoesNotLeakIntoNestedNullingJoinDifferentColumn() throws Exception {
        // Keeps the per-level clearConstNameMaps() load-bearing: unlike the sibling test where the
        // nested WHERE a.k = 1 re-registers "k" and masks a missing clear, here the nested master
        // WHERE filters a DIFFERENT column (a.j = 1) while the join key still reuses "k". Without the
        // clear the outer "k -> 2" survives and addTransitiveFilters injects a foreign b.k = 2 into
        // the nested slave, dropping the matching row (0 rows instead of 1).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tc (q INT, k INT)");
            execute("INSERT INTO tc VALUES (1, 2)");
            execute("CREATE TABLE ta (j INT, k INT)");
            execute("INSERT INTO ta VALUES (1, 1)");
            execute("CREATE TABLE tb (k INT, v INT)");
            execute("INSERT INTO tb VALUES (1, 10)");

            assertQuery("SELECT * FROM tc " +
                    "JOIN (SELECT a.k k1, b.v FROM ta a RIGHT JOIN tb b ON a.k = b.k WHERE a.j = 1) x " +
                    "  ON tc.q = x.k1 " +
                    "WHERE tc.k = 2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("q\tk\tk1\tv\n1\t2\t1\t10\n");
        });
    }

    @Test
    public void testOuterHashJoinOnFunctionCondition12() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s1 string)");
            execute("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");
            execute("create table t2 (j int, s2 string)");
            execute("insert into t2 values (1,'a'), (1,'e'), (2, 'b'), (2, 'd'), (3,'c');");

            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 left join t2 on j = i and (s1 ~ '[abde]') order by i, s2",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            1\ta\t1\te
                            2\tb\t2\tb
                            2\tb\t2\td
                            3\tc\tnull\t
                            4\td\tnull\t
                            5\te\tnull\t
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select t1.*, t2.* from t2 right join t1 on j = i and (s1 ~ '[abde]') order by i, s2",
                    """
                            i\ts1\tj\ts2
                            1\ta\t1\ta
                            1\ta\t1\te
                            2\tb\t2\tb
                            2\tb\t2\td
                            3\tc\tnull\t
                            4\td\tnull\t
                            5\te\tnull\t
                            """
            );
            assertHashJoinSqlWithRandomAccess(
                    "select * from t1 full join t2 on j = i and (s1 ~ '[abde]') order by i, s2",
                    """
                            i\ts1\tj\ts2
                            null\t\t3\tc
                            1\ta\t1\ta
                            1\ta\t1\te
                            2\tb\t2\tb
                            2\tb\t2\td
                            3\tc\tnull\t
                            4\td\tnull\t
                            5\te\tnull\t
                            """
            );
        });
    }

    @Test
    public void testOuterJoinEqualitiesImplyNothing() throws Exception {
        // Two equalities of an outer join that share a column only match rows. The equality they
        // imply must not filter a table whose rows the outer join keeps: the master of a LEFT or
        // FULL join, or the slave of a RIGHT or FULL join.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE oa (k INT, m INT)");
            execute("CREATE TABLE oc (k INT, m INT)");
            execute("CREATE TABLE od (m INT)");
            execute("INSERT INTO oa VALUES (1, 1)");
            execute("INSERT INTO oc VALUES (1, 1), (1, 7)");
            execute("INSERT INTO od VALUES (1), (7)");
            execute("CREATE TABLE sa (m INT)");
            execute("CREATE TABLE sd (m INT, n INT)");
            execute("INSERT INTO sa VALUES (1)");
            execute("INSERT INTO sd VALUES (1, 1), (2, 3)");
            execute("CREATE TABLE pa (m INT, n INT)");
            execute("CREATE TABLE pd (m INT)");
            execute("INSERT INTO pa VALUES (1, 1), (2, 3)");
            execute("INSERT INTO pd VALUES (1), (2), (3)");

            // implies oc.m = oa.m, which does not hold for the (oa, oc (1, 7)) row
            final String oFrom = " FROM oa JOIN oc ON oc.k = oa.k %s JOIN od ON od.m = oa.m AND od.m = oc.m";
            final String oSelect = "SELECT oa.m oam, oc.m ocm, od.m odm";
            assertQuery(oSelect + oFrom.formatted("LEFT") + " ORDER BY ocm")
                    .noLeakCheck()
                    .returns("""
                            oam\tocm\todm
                            1\t1\t1
                            1\t7\tnull
                            """);
            assertQuery(oSelect + oFrom.formatted("FULL") + " ORDER BY ocm, odm")
                    .noLeakCheck()
                    .returns("""
                            oam\tocm\todm
                            null\tnull\t7
                            1\t1\t1
                            1\t7\tnull
                            """);
            assertQuery(oSelect + oFrom.formatted("RIGHT") + " ORDER BY ocm, odm")
                    .noLeakCheck()
                    .returns("""
                            oam\tocm\todm
                            null\tnull\t7
                            1\t1\t1
                            """);
            assertQuery("SELECT count(*)" + oFrom.formatted("LEFT"))
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            2
                            """);

            // implies sd.m = sd.n, which does not hold for sd's (2, 3) row
            for (String outerJoin : new String[]{"RIGHT", "FULL"}) {
                assertQuery("SELECT sa.m sam, sd.m sdm, sd.n sdn FROM sa " + outerJoin + " JOIN sd ON sd.m = sa.m AND sd.n = sa.m ORDER BY sdm")
                        .noLeakCheck()
                        .returns("""
                                sam\tsdm\tsdn
                                1\t1\t1
                                null\t2\t3
                                """);
            }

            // implies pa.m = pa.n, which does not hold for pa's (2, 3) row
            final String pFrom = "SELECT pa.m pam, pa.n pan, pd.m pdm FROM pa %s JOIN pd ON pd.m = pa.m AND pd.m = pa.n ORDER BY pam, pdm";
            assertQuery(pFrom.formatted("LEFT"))
                    .noLeakCheck()
                    .returns("""
                            pam\tpan\tpdm
                            1\t1\t1
                            2\t3\tnull
                            """);
            assertQuery(pFrom.formatted("FULL"))
                    .noLeakCheck()
                    .returns("""
                            pam\tpan\tpdm
                            null\tnull\t2
                            null\tnull\t3
                            1\t1\t1
                            2\t3\tnull
                            """);
            assertQuery(pFrom.formatted("RIGHT"))
                    .noLeakCheck()
                    .returns("""
                            pam\tpan\tpdm
                            null\tnull\t2
                            null\tnull\t3
                            1\t1\t1
                            """);
        });
    }

    @Test
    public void testOuterJoinEqualitiesOnCrossJoinedTable() throws Exception {
        // x is cross-joined, and the FULL join r keeps all of its equalities, so x has no join key of its
        // own. The plan joins x after y, on y's equality y.c = x.c, and that key must resolve against the
        // join of a and y. The RIGHT join r gives x the keys that its equalities imply instead.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (b INT, c INT)");
            execute("CREATE TABLE x (b INT, c INT)");
            execute("CREATE TABLE y (b INT, c INT)");
            execute("CREATE TABLE r (a INT)");
            execute("CREATE TABLE z (a INT)");
            execute("INSERT INTO a VALUES (1, 1)");
            execute("INSERT INTO x VALUES (1, 5)");
            execute("INSERT INTO y VALUES (1, 5)");
            execute("INSERT INTO r VALUES (1), (2)");
            execute("INSERT INTO z VALUES (1)");

            // r's (2) row matches no (a, x, y) row, so the outer join NULL-extends it and z drops it
            final String prefix = " FROM a CROSS JOIN x JOIN y ON y.b = a.b AND y.c = x.c ";
            for (String outerJoin : new String[]{"RIGHT", "FULL"}) {
                final String from = prefix + outerJoin + " JOIN r ON r.a = a.c AND r.a = x.b AND r.a = a.b JOIN z ON z.a = a.b";
                assertQuery("SELECT a.b ab, r.a ra, z.a za" + from)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                ab\tra\tza
                                1\t1\t1
                                """);
                assertQuery("SELECT count(*)" + from)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                1
                                """);
            }

            // without z, the NULL-extended r row stays
            final String rightFrom = prefix + "RIGHT JOIN r ON r.a = a.c AND r.a = x.b";
            assertQuery("SELECT a.b ab, r.a ra" + rightFrom + " ORDER BY ra")
                    .noLeakCheck()
                    .returns("""
                            ab\tra
                            1\t1
                            null\t2
                            """);
            assertQuery("SELECT count(*)" + rightFrom)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            2
                            """);
        });
    }

    @Test
    public void testOuterJoinKeepsCrossJoinedPrefixBeforeIt() throws Exception {
        // A RIGHT/FULL OUTER join takes its whole SQL prefix as the master, cross join included, so its
        // unmatched slave rows carry one NULL-extended (a, x) pair. Master had doReorderTables append a
        // prefix model without join-context parents after the ordered ones, which put the cross join
        // after the outer join: with an empty x every unmatched row disappeared, and with two x rows
        // each unmatched row came back twice.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1)");
            execute("CREATE TABLE b (y INT)");
            execute("INSERT INTO b VALUES (5), (100)");
            execute("CREATE TABLE x0 (k INT)");
            execute("CREATE TABLE x2 (k INT)");
            execute("INSERT INTO x2 VALUES (1), (2)");

            // a.k = 1 matches no b row, so every b row is NULL-extended; with FULL the unmatched master
            // rows come back too, one per (a, x) pair.
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                final boolean isFull = joinType.startsWith("FULL");
                assertQuery("SELECT a.x, x0.k xk, b.y FROM a CROSS JOIN x0 " + joinType + " JOIN b ON b.y = a.k ORDER BY b.y")
                        .noLeakCheck()
                        .returns("""
                                x\txk\ty
                                null\tnull\t5
                                null\tnull\t100
                                """);
                assertQuery("SELECT a.x, x2.k xk, b.y FROM a CROSS JOIN x2 " + joinType + " JOIN b ON b.y = a.k ORDER BY b.y, xk")
                        .noLeakCheck()
                        .returns(isFull
                                ? """
                                x\txk\ty
                                10\t1\tnull
                                10\t2\tnull
                                null\tnull\t5
                                null\tnull\t100
                                """
                                : """
                                x\txk\ty
                                null\tnull\t5
                                null\tnull\t100
                                """);
            }

            // the same with a non-equi ON clause, which homogenizes to a CROSS_RIGHT/CROSS_FULL
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT a.x, x0.k xk, b.y FROM a CROSS JOIN x0 " + joinType + " JOIN b ON a.x > b.y ORDER BY b.y")
                        .noLeakCheck()
                        .returns("""
                                x\txk\ty
                                null\tnull\t5
                                null\tnull\t100
                                """);
                assertQuery("SELECT a.x, x2.k xk, b.y FROM a CROSS JOIN x2 " + joinType + " JOIN b ON a.x > b.y ORDER BY b.y, xk")
                        .noLeakCheck()
                        .returns("""
                                x\txk\ty
                                10\t1\t5
                                10\t2\t5
                                null\tnull\t100
                                """);
            }
        });
    }

    @Test
    public void testOuterJoinKeepsEarlierInnerJoinEquality() throws Exception {
        // b's ON clause joins it to both a and x. a must not take over b.k = a.k when that closes a
        // cycle of join contexts, or the plan drops the equality and returns b's (2, 1) row too.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (i INT, k INT, v INT)");
            execute("CREATE TABLE x (i INT, j INT)");
            execute("CREATE TABLE b (k INT, j INT)");
            execute("CREATE TABLE c (v INT, k INT)");
            execute("CREATE TABLE d (k INT)");
            execute("CREATE TABLE e (z INT)");
            execute("INSERT INTO a VALUES (1, 1, 5)");
            execute("INSERT INTO x VALUES (1, 1)");
            execute("INSERT INTO b VALUES (1, 1), (2, 1)");
            execute("INSERT INTO c VALUES (1, 7)");
            execute("INSERT INTO d VALUES (7)");
            execute("INSERT INTO e VALUES (1)");

            // (a JOIN x JOIN b) keeps only b's (1, 1) row, which c's row matches and d's row follows
            for (String outerJoin : new String[]{"RIGHT", "FULL"}) {
                for (String bOn : new String[]{"b.k = a.k AND b.j = x.j", "b.j = x.j AND b.k = a.k"}) {
                    final String from = "FROM a JOIN x ON a.i = x.i JOIN b ON " + bOn + " " + outerJoin + " JOIN c ON a.v > c.v";
                    for (String laterJoins : new String[]{
                            " JOIN d ON d.k = c.k",
                            " JOIN d ON d.k = 7",
                            " CROSS JOIN e JOIN d ON d.k = c.k AND e.z = 1",
                            "",
                            " LEFT JOIN d ON d.k = c.k",
                            " CROSS JOIN d"
                    }) {
                        assertQuery("SELECT a.k ak, b.k bk " + from + laterJoins)
                                .noLeakCheck()
                                .noRandomAccess()
                                .withPlanContaining("b.k=a.k")
                                .returns("""
                                        ak\tbk
                                        1\t1
                                        """);
                    }
                    assertQuery("SELECT count(*) " + from + " JOIN d ON d.k = c.k")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .withPlanContaining("b.k=a.k")
                            .returns("""
                                    count
                                    1
                                    """);
                }
            }

            // the same take-over without an outer join, in the pass that spares e
            assertQuery("SELECT a.k ak, b.k bk FROM a JOIN x ON a.i = x.i JOIN b ON b.k = a.k AND b.j = x.j CROSS JOIN e")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("b.k=a.k")
                    .returns("""
                            ak\tbk
                            1\t1
                            """);
        });
    }

    @Test
    public void testOuterJoinKeepsEqualitiesTakenOverByCrossJoin() throws Exception {
        // y has no equality of its own, so it takes over both g.z = y.z and h.z = y.z. The second
        // take-over must add to the join context that holds the first one, not replace it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT, v INT)");
            execute("CREATE TABLE p (k INT)");
            execute("CREATE TABLE c (v INT)");
            execute("CREATE TABLE y (z INT)");
            execute("CREATE TABLE g (k INT, z INT)");
            execute("CREATE TABLE u (m INT)");
            execute("CREATE TABLE h (m INT, z INT)");
            execute("INSERT INTO a VALUES (1, 5)");
            execute("INSERT INTO p VALUES (1)");
            execute("INSERT INTO c VALUES (1)");
            execute("INSERT INTO y VALUES (1)");
            execute("INSERT INTO g VALUES (1, 1), (1, 2)");
            execute("INSERT INTO u VALUES (1)");
            execute("INSERT INTO h VALUES (1, 1), (1, 2)");

            for (String outerJoin : new String[]{"RIGHT", "FULL"}) {
                final String from = " FROM a JOIN p ON p.k = a.k " + outerJoin + " JOIN c ON a.v > c.v CROSS JOIN y"
                        + " JOIN g ON g.k = a.k AND g.z = y.z JOIN u ON u.m = 1 JOIN h ON h.m = u.m AND h.z = y.z";
                assertQuery("SELECT a.k, y.z, g.z gz, h.z hz" + from)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                k\tz\tgz\thz
                                1\t1\t1\t1
                                """);
                assertQuery("SELECT count(*)" + from)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                1
                                """);
            }
        });
    }

    @Test
    public void testOuterJoinKeepsJoinsOnImpliedEquality() throws Exception {
        // e's ON clause implies b.k = a.k, which gives the cross-joined b the parent a. The plan must
        // still join b and e after the non-equi outer join. The LEFT-joined b takes no implied
        // equality, so e keeps both of its equalities.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT, v INT)");
            execute("CREATE TABLE c (v INT)");
            execute("CREATE TABLE b (k INT, j INT)");
            execute("CREATE TABLE e (k INT)");
            execute("INSERT INTO a VALUES (1, 5)");
            execute("INSERT INTO c VALUES (1), (9)");
            execute("INSERT INTO b VALUES (1, 1), (2, 1)");
            execute("INSERT INTO e VALUES (1)");

            // c's (9) row matches no a row, so e drops it along with the b (2, 1) row
            for (String outerJoin : new String[]{"RIGHT", "FULL"}) {
                for (String bJoin : new String[]{"CROSS JOIN b", "LEFT JOIN b ON a.v > b.j"}) {
                    final String from = " FROM a " + outerJoin + " JOIN c ON a.v > c.v " + bJoin
                            + " JOIN e ON e.k = a.k AND e.k = b.k";
                    assertQuery("SELECT a.k, c.v, b.k bk, e.k ek" + from)
                            .noLeakCheck()
                            .noRandomAccess()
                            .returns("""
                                    k\tv\tbk\tek
                                    1\t1\t1\t1
                                    """);
                    assertQuery("SELECT count(*)" + from)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    count
                                    1
                                    """);
                }
            }
        });
    }

    @Test
    public void testOuterJoinKeepsLaterInnerJoins() throws Exception {
        // d's ON clause holds no equality, so d takes over e.k = d.k, but e also follows d through f.
        // The take-over must not close the join-context cycle d -> f -> e -> d, or the plan drops
        // d, f and e.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT, v INT)");
            execute("CREATE TABLE y (z INT)");
            execute("CREATE TABLE b (k INT, j INT)");
            execute("CREATE TABLE c (v INT)");
            execute("CREATE TABLE d (k INT)");
            execute("CREATE TABLE e (k INT, m INT)");
            execute("CREATE TABLE f (k INT, m INT)");
            execute("CREATE TABLE f2 (k INT, m INT)");
            execute("INSERT INTO a VALUES (1, 5)");
            execute("INSERT INTO y VALUES (1)");
            execute("INSERT INTO b VALUES (1, 1), (2, 1)");
            execute("INSERT INTO c VALUES (1)");
            execute("INSERT INTO d VALUES (7)");
            execute("INSERT INTO e VALUES (7, 1), (7, 1), (7, 2)");
            execute("INSERT INTO f VALUES (7, 1)");
            execute("INSERT INTO f2 VALUES (8, 1)");

            // each prefix yields one row with c, which d and f match once each and e matches twice
            for (String prefix : new String[]{
                    "a RIGHT JOIN c ON a.v > c.v",
                    "a FULL JOIN c ON a.v > c.v",
                    "a CROSS JOIN y JOIN b ON b.k = a.k AND b.j = y.z RIGHT JOIN c ON a.v > c.v",
                    "a LEFT JOIN c ON a.v > c.v",
                    "a RIGHT JOIN c ON c.v = a.k",
                    "a CROSS JOIN c"
            }) {
                for (String dJoin : new String[]{" JOIN d ON d.k = 7", " CROSS JOIN d"}) {
                    final String from = " FROM " + prefix + dJoin;
                    assertQuery("SELECT a.k, c.v, d.k dk, f.m fm, e.m em" + from + " JOIN f ON f.k = d.k JOIN e ON e.k = d.k AND e.m = f.m")
                            .noLeakCheck()
                            .noRandomAccess()
                            .returns("""
                                    k\tv\tdk\tfm\tem
                                    1\t1\t7\t1\t1
                                    1\t1\t7\t1\t1
                                    """);
                    assertQuery("SELECT count(*)" + from + " JOIN f ON f.k = d.k JOIN e ON e.k = d.k AND e.m = f.m")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    count
                                    2
                                    """);
                    // f2 matches no d row, so no row survives
                    assertQuery("SELECT count(*)" + from + " JOIN f2 ON f2.k = d.k JOIN e ON e.k = d.k AND e.m = f2.m")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    count
                                    0
                                    """);
                    assertQuery("SELECT sum(e.m) s, count(*) n" + from + " JOIN f ON f.k = d.k JOIN e ON e.k = d.k AND e.m = f.m")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    s\tn
                                    2\t2
                                    """);
                }
            }
        });
    }

    @Test
    public void testOuterJoinKeepsSqlPositionBeforeLaterJoins() throws Exception {
        // (a RIGHT JOIN b ON a.x > b.y) JOIN c ON c.k = a.k: the b row that finds no a row is
        // NULL-extended, and the inner join drops it, because c.k = NULL matches no c row. Master
        // homogenized the non-equi outer join to a CROSS variant that doReorderTables appended after c,
        // so the join NULL-extended c instead and returned an extra (null,100,null) row. Whether that
        // row survived depended on the WHERE clause, which is how the bug stayed hidden: c.v = c.w
        // dropped it until item D fixed SYMBOL equality on null-extended rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1)");
            execute("CREATE TABLE b (y INT)");
            execute("INSERT INTO b VALUES (5), (100)");
            execute("CREATE TABLE c (k INT, v SYMBOL, w SYMBOL, c1 INT, c2 INT, s STRING)");
            execute("INSERT INTO c VALUES (1, 'foo', 'foo', 7, 7, 'foo')");

            final String expected = "x\ty\tk\n10\t5\t1\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER", "LEFT OUTER"}) {
                final String on = joinType.startsWith("LEFT") ? "a.x < b.y" : "a.x > b.y";
                for (String where : new String[]{"", " WHERE c.c1 = c.c2", " WHERE c.v = c.w", " WHERE c.s = c.s", " WHERE c.k IS NOT NULL"}) {
                    assertQuery("SELECT a.x, b.y, c.k FROM a " + joinType + " JOIN b ON " + on
                            + " JOIN c ON c.k = a.k" + where + " ORDER BY b.y")
                            .noLeakCheck()
                            .returns(joinType.startsWith("LEFT") ? "x\ty\tk\n10\t100\t1\n" : expected);
                    // the bracketed spelling forces the SQL join order and must agree
                    assertQuery("SELECT ab.x, ab.y, c.k FROM (SELECT a.x, a.k, b.y FROM a " + joinType
                            + " JOIN b ON " + on + ") ab JOIN c ON c.k = ab.k" + where + " ORDER BY ab.y")
                            .noLeakCheck()
                            .returns(joinType.startsWith("LEFT") ? "x\ty\tk\n10\t100\t1\n" : expected);
                }
            }
        });
    }

    @Test
    public void testOuterJoinKeepsSqlPositionInJoinChain() throws Exception {
        // Every join after a RIGHT/FULL OUTER joins its output, whatever its own type. Master ran the
        // equi joins first and the non-equi outer join last, which NULL-extended their tables.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1)");
            execute("CREATE TABLE b (y INT)");
            execute("INSERT INTO b VALUES (5), (100)");
            execute("CREATE TABLE c (k INT)");
            execute("INSERT INTO c VALUES (1)");
            execute("CREATE TABLE d (z INT)");
            execute("INSERT INTO d VALUES (3), (4)");
            execute("CREATE TABLE e (k INT, c1 INT)");
            execute("INSERT INTO e VALUES (1, 7), (100, 8), (NULL, 9)");

            // a later equi RIGHT join: its own unmatched rows null-extend the first join's output
            assertQuery("SELECT a.x, b.y, e.k, e.c1 FROM a RIGHT JOIN b ON a.x > b.y RIGHT JOIN e ON e.k = a.k ORDER BY e.c1")
                    .noLeakCheck()
                    .returns("""
                            x\ty\tk\tc1
                            10\t5\t1\t7
                            null\tnull\t100\t8
                            null\t100\tnull\t9
                            """);

            // a later equi LEFT join: NULL=NULL matches e's NULL key for the NULL-extended row
            assertQuery("SELECT a.x, b.y, e.k, e.c1 FROM a RIGHT JOIN b ON a.x > b.y LEFT JOIN e ON e.k = a.k ORDER BY b.y")
                    .noLeakCheck()
                    .returns("""
                            x\ty\tk\tc1
                            10\t5\t1\t7
                            null\t100\tnull\t9
                            """);

            // an inner join between two non-equi outer joins
            assertQuery("SELECT a.x, b.y, c.k, d.z FROM a RIGHT JOIN b ON a.x > b.y JOIN c ON c.k = a.k RIGHT JOIN d ON d.z < b.y ORDER BY d.z, b.y")
                    .noLeakCheck()
                    .returns("""
                            x\ty\tk\tz
                            10\t5\t1\t3
                            10\t5\t1\t4
                            """);

            // a non-equi LEFT join before an equi RIGHT join: the RIGHT join must not move before it
            assertQuery("SELECT a.x, b.y, e.k, e.c1 FROM a LEFT JOIN b ON a.x != b.y RIGHT JOIN e ON e.k = a.k ORDER BY e.c1, b.y")
                    .noLeakCheck()
                    .returns("""
                            x\ty\tk\tc1
                            10\t5\t1\t7
                            10\t100\t1\t7
                            null\tnull\t100\t8
                            null\tnull\tnull\t9
                            """);
        });
    }

    @Test
    public void testOuterJoinMasterFilterKeepsMatchedRow() throws Exception {
        // Companion to testOuterJoinMasterFilterStaysPostJoin: that test proves the NULL-master
        // rows are removed; this one proves a genuine master match survives, so the post-join
        // filter does not over-filter. The master holds a matching 's2' row (joined to the
        // slave's 's2') plus an unrelated 'x' row, and the slave holds an unmatched 'zzz' row
        // that becomes a NULL-master row. WHERE a.sym = 's2' must keep the matched (s2, 300)
        // row and drop the NULL-master 'zzz' row for both RIGHT and FULL OUTER, with the
        // constant on either side of the equality (the two analyseEquals branches). The
        // predicate stays a post-join Filter; pushing it into the master sub-query would also
        // propagate 's2' to the slave for the literal form, so its leak is only visible in the
        // bind-variable form, which the plan assertion and the bind arm both guard against.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (sym SYMBOL, c1 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES ('x', 200, 4), ('s2', 300, 6)");
            execute("CREATE TABLE s (sym SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES ('s2', 10, 1), ('zzz', 99, 7)");

            final String expected = "e0\te1\ns2\t300\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                // Constant on the RHS of the equality (a.sym = 's2').
                final String rhs = "SELECT a.sym AS e0, a.c1 AS e1 FROM m a " + joinType + " JOIN s b ON a.sym = b.sym WHERE a.sym = 's2'";
                bindVariableService.clear();
                assertQuery(rhs).noLeakCheck().noRandomAccess().withPlanContaining("Filter filter: a.sym='s2'").returns(expected);

                // Constant on the LHS of the equality ('s2' = a.sym), the mirror analyseEquals branch.
                final String lhs = "SELECT a.sym AS e0, a.c1 AS e1 FROM m a " + joinType + " JOIN s b ON a.sym = b.sym WHERE 's2' = a.sym";
                bindVariableService.clear();
                assertQuery(lhs).noLeakCheck().noRandomAccess().withPlanContaining("Filter filter: a.sym='s2'").returns(expected);

                // Bind-variable form must produce the identical result under the full assertion battery.
                final String bind = "SELECT a.sym AS e0, a.c1 AS e1 FROM m a " + joinType + " JOIN s b ON a.sym = b.sym WHERE a.sym = :sym::SYMBOL";
                bindVariableService.clear();
                bindVariableService.setStr("sym", "s2");
                assertQuery(bind).noLeakCheck().noRandomAccess().returns(expected);
            }
        });
    }

    @Test
    public void testOuterJoinMasterFilterStaysPostJoin() throws Exception {
        // Regression for a query-fuzzer bind-variable divergence. RIGHT and FULL OUTER joins
        // NULL-extend the master (left) table, so a WHERE predicate that references only the
        // master used to be pushed into the master sub-query, leaving the unmatched right
        // rows (with a NULL master) unfiltered. Here the master has no 's2' row, so the
        // slave's 's2' row has no match; WHERE a.sym = 's2' must return no rows because the
        // master symbol is NULL. The literal form leaked one such row and the bind-variable
        // form two; both now correctly return nothing because the predicate stays post-join.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (sym SYMBOL, c1 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES ('x', 200, 4)");
            execute("CREATE TABLE s (sym SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES ('s2', 10, 1), ('x', 50, 5)");

            final String empty = "e0\te1\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                final String literal = "SELECT a.sym AS e0, a.c1 AS e1 FROM m a " + joinType + " JOIN s b ON a.sym = b.sym WHERE a.sym = 's2'";
                final String bind = "SELECT a.sym AS e0, a.c1 AS e1 FROM m a " + joinType + " JOIN s b ON a.sym = b.sym WHERE a.sym = :sym::SYMBOL";

                bindVariableService.clear();
                assertQuery(literal).noLeakCheck().noRandomAccess().returns(empty);

                // Bind-variable form must produce the identical result under the full assertion battery.
                bindVariableService.clear();
                bindVariableService.setStr("sym", "s2");
                assertQuery(bind).noLeakCheck().noRandomAccess().returns(empty);
            }
        });
    }

    @Test
    public void testRepeatedKeyColumnWithMixedStringTypesAsOfJoin() throws Exception {
        // Both equalities of q's ON clause stay in the ASOF or LT key, so the key holds q's column
        // twice, and its two partners have different types. Both pairs have to write the column in
        // one encoding, or no key matches and every row comes back NULL-extended.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL, v VARCHAR, s STRING, px INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE q (sym SYMBOL, bid INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE qs (s STRING, bid INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE iv (code VARCHAR, name VARCHAR)");
            execute("CREATE TABLE istr (code STRING, name VARCHAR)");
            execute("""
                    INSERT INTO t VALUES
                        ('A', 'A', 'A', 1, '2024-01-01T00:00:10.000000Z'),
                        ('B', 'A', 'B', 2, '2024-01-01T00:00:20.000000Z'),
                        ('A', 'B', 'A', 3, '2024-01-01T00:00:30.000000Z'),
                        ('C', 'C', 'X', 4, '2024-01-01T00:00:40.000000Z')
                    """);
            // B comes first, so q's symbol keys differ from t's
            execute("""
                    INSERT INTO q VALUES
                        ('B', 20, '2024-01-01T00:00:06.000000Z'),
                        ('A', 10, '2024-01-01T00:00:05.000000Z'),
                        ('A', 11, '2024-01-01T00:00:25.000000Z'),
                        ('A', 12, '2024-01-01T00:00:30.000000Z'),
                        ('C', 30, '2024-01-01T00:00:35.000000Z'),
                        ('D', 40, '2024-01-01T00:00:45.000000Z')
                    """);
            execute("""
                    INSERT INTO qs VALUES
                        ('A', 10, '2024-01-01T00:00:05.000000Z'),
                        ('B', 20, '2024-01-01T00:00:06.000000Z'),
                        ('A', 11, '2024-01-01T00:00:25.000000Z'),
                        ('A', 12, '2024-01-01T00:00:30.000000Z'),
                        ('C', 30, '2024-01-01T00:00:35.000000Z')
                    """);
            execute("""
                    INSERT INTO iv VALUES
                        ('A', 'alpha'),
                        ('B', 'beta'),
                        ('C', 'gamma')
                    """);
            execute("""
                    INSERT INTO istr VALUES
                        ('A', 'alpha'),
                        ('B', 'beta'),
                        ('C', 'gamma')
                    """);
            // UNION ALL gives the views no random access, so ASOF and LT joins run full-fat on them
            execute("CREATE VIEW uq AS (SELECT * FROM (SELECT * FROM q UNION ALL SELECT * FROM q WHERE bid < 0) TIMESTAMP(ts))");
            execute("CREATE VIEW uqs AS (SELECT * FROM (SELECT * FROM qs UNION ALL SELECT * FROM qs WHERE bid < 0) TIMESTAMP(ts))");

            // the light join on a table slave, the full-fat join on a view slave, and the full-fat
            // compiler flag on a table slave
            final String[] slaves = {"q", "uq", "q"};
            final String[] stringSlaves = {"qs", "uqs", "qs"};
            final boolean[] isFullFatFlags = {false, false, true};
            for (int s = 0; s < slaves.length; s++) {
                final String slave = slaves[s];
                final boolean isFullFat = isFullFatFlags[s];

                // the inner join ties t.sym to i.code, a VARCHAR or a STRING
                for (String instruments : new String[]{"iv", "istr"}) {
                    for (String on : new String[]{"q.sym = t.sym AND q.sym = i.code", "q.sym = i.code AND q.sym = t.sym"}) {
                        final String from = " FROM t JOIN " + instruments + " i ON i.code = t.sym ";
                        final String select = "SELECT t.ts, t.sym, i.name, q.sym qsym, q.bid";
                        assertQuery(select + from + "ASOF JOIN " + slave + " q ON " + on)
                                .noLeakCheck()
                                .fullFatJoins(isFullFat)
                                .noRandomAccess()
                                .expectSize(isFullFat)
                                .timestamp("ts")
                                .returns("""
                                        ts\tsym\tname\tqsym\tbid
                                        2024-01-01T00:00:10.000000Z\tA\talpha\tA\t10
                                        2024-01-01T00:00:20.000000Z\tB\tbeta\tB\t20
                                        2024-01-01T00:00:30.000000Z\tA\talpha\tA\t12
                                        2024-01-01T00:00:40.000000Z\tC\tgamma\tC\t30
                                        """);
                        assertQuery(select + from + "LT JOIN " + slave + " q ON " + on)
                                .noLeakCheck()
                                .fullFatJoins(isFullFat)
                                .noRandomAccess()
                                .expectSize(isFullFat)
                                .timestamp("ts")
                                .returns("""
                                        ts\tsym\tname\tqsym\tbid
                                        2024-01-01T00:00:10.000000Z\tA\talpha\tA\t10
                                        2024-01-01T00:00:20.000000Z\tB\tbeta\tB\t20
                                        2024-01-01T00:00:30.000000Z\tA\talpha\tA\t11
                                        2024-01-01T00:00:40.000000Z\tC\tgamma\tC\t30
                                        """);
                    }
                }

                // nothing ties t.sym to t.v, so only rows with t.sym = t.v can find a q row
                for (String join : new String[]{"ASOF", "LT"}) {
                    assertQuery("SELECT t.ts, t.sym, t.v, q.sym qsym, q.bid FROM t " + join + " JOIN " + slave + " q ON q.sym = t.sym AND q.sym = t.v")
                            .noLeakCheck()
                            .fullFatJoins(isFullFat)
                            .noRandomAccess()
                            .expectSize()
                            .timestamp("ts")
                            .returns("""
                                    ts\tsym\tv\tqsym\tbid
                                    2024-01-01T00:00:10.000000Z\tA\tA\tA\t10
                                    2024-01-01T00:00:20.000000Z\tB\tA\t\tnull
                                    2024-01-01T00:00:30.000000Z\tA\tB\t\tnull
                                    2024-01-01T00:00:40.000000Z\tC\tC\tC\t30
                                    """);
                }
                assertQuery("SELECT t.ts, t.sym, t.s, q.sym qsym, q.bid FROM t ASOF JOIN " + slave + " q ON q.sym = t.sym AND q.sym = t.s")
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .noRandomAccess()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tsym\ts\tqsym\tbid
                                2024-01-01T00:00:10.000000Z\tA\tA\tA\t10
                                2024-01-01T00:00:20.000000Z\tB\tB\tB\t20
                                2024-01-01T00:00:30.000000Z\tA\tA\tA\t12
                                2024-01-01T00:00:40.000000Z\tC\tX\t\tnull
                                """);

                // a STRING slave column with a STRING and a VARCHAR partner, and with a VARCHAR
                // partner alone; the full-fat join reads the column from the key, which holds a VARCHAR
                final String stringSlave = stringSlaves[s];
                assertQuery("SELECT t.ts, t.s, t.v, q.s qs, q.bid FROM t ASOF JOIN " + stringSlave + " q ON q.s = t.s AND q.s = t.v")
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .noRandomAccess()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\ts\tv\tqs\tbid
                                2024-01-01T00:00:10.000000Z\tA\tA\tA\t10
                                2024-01-01T00:00:20.000000Z\tB\tA\t\tnull
                                2024-01-01T00:00:30.000000Z\tA\tB\t\tnull
                                2024-01-01T00:00:40.000000Z\tX\tC\t\tnull
                                """);
                assertQuery("SELECT t.ts, t.v, q.s qs, q.bid FROM t ASOF JOIN " + stringSlave + " q ON q.s = t.v")
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .noRandomAccess()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tv\tqs\tbid
                                2024-01-01T00:00:10.000000Z\tA\tA\t10
                                2024-01-01T00:00:20.000000Z\tA\tA\t10
                                2024-01-01T00:00:30.000000Z\tB\tB\t20
                                2024-01-01T00:00:40.000000Z\tC\tC\t30
                                """);
            }
        });
    }

    @Test
    public void testRepeatedKeyColumnWithMixedStringTypesHashJoin() throws Exception {
        // The hash join key holds one column twice, with partners of different types or with SYMBOL
        // partners from different dictionaries. Both pairs have to write the column in one encoding,
        // and a translated SYMBOL key belongs to one dictionary, so such pairs compare as text.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL, s STRING, px INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE q (sym SYMBOL, bid INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE qv (sym SYMBOL, v VARCHAR, bid INT)");
            execute("CREATE TABLE iv (code VARCHAR, name VARCHAR)");
            execute("CREATE TABLE istr (code STRING, name VARCHAR)");
            execute("CREATE TABLE isym (code SYMBOL, name VARCHAR)");
            execute("""
                    INSERT INTO t VALUES
                        ('A', 'A', 1, '2024-01-01T00:00:10.000000Z'),
                        ('B', 'B', 2, '2024-01-01T00:00:20.000000Z'),
                        ('A', 'A', 3, '2024-01-01T00:00:30.000000Z'),
                        ('C', 'X', 4, '2024-01-01T00:00:40.000000Z')
                    """);
            // the three SYMBOL columns assign their keys in different orders
            execute("""
                    INSERT INTO q VALUES
                        ('B', 20, '2024-01-01T00:00:06.000000Z'),
                        ('A', 10, '2024-01-01T00:00:05.000000Z'),
                        ('A', 11, '2024-01-01T00:00:25.000000Z'),
                        ('A', 12, '2024-01-01T00:00:30.000000Z'),
                        ('C', 30, '2024-01-01T00:00:35.000000Z'),
                        ('D', 40, '2024-01-01T00:00:45.000000Z')
                    """);
            execute("""
                    INSERT INTO qv VALUES
                        ('B', 'B', 20),
                        ('A', 'A', 10),
                        ('A', 'X', 11),
                        ('C', 'C', 30),
                        ('D', 'D', 40)
                    """);
            execute("""
                    INSERT INTO iv VALUES
                        ('A', 'alpha'),
                        ('B', 'beta'),
                        ('C', 'gamma')
                    """);
            execute("""
                    INSERT INTO istr VALUES
                        ('A', 'alpha'),
                        ('B', 'beta'),
                        ('C', 'gamma')
                    """);
            execute("""
                    INSERT INTO isym VALUES
                        ('C', 'gamma'),
                        ('B', 'beta'),
                        ('A', 'alpha')
                    """);

            for (boolean isFullFat : new boolean[]{false, true}) {
                // the inner join ties t.sym to i.code, a VARCHAR, a STRING or a SYMBOL
                for (String instruments : new String[]{"iv", "istr", "isym"}) {
                    final String from = " FROM t JOIN " + instruments + " i ON i.code = t.sym ";
                    assertQuery("SELECT t.px, i.name, q.sym qsym, q.bid" + from + "LEFT JOIN q ON q.sym = t.sym AND q.sym = i.code ORDER BY t.px, q.bid")
                            .noLeakCheck()
                            .fullFatJoins(isFullFat)
                            .returns("""
                                    px\tname\tqsym\tbid
                                    1\talpha\tA\t10
                                    1\talpha\tA\t11
                                    1\talpha\tA\t12
                                    2\tbeta\tB\t20
                                    3\talpha\tA\t10
                                    3\talpha\tA\t11
                                    3\talpha\tA\t12
                                    4\tgamma\tC\t30
                                    """);
                    assertQuery("SELECT t.px, i.name, q.sym qsym, q.bid" + from + "FULL JOIN q ON q.sym = t.sym AND q.sym = i.code ORDER BY t.px, q.bid")
                            .noLeakCheck()
                            .fullFatJoins(isFullFat)
                            .returns("""
                                    px\tname\tqsym\tbid
                                    null\t\tD\t40
                                    1\talpha\tA\t10
                                    1\talpha\tA\t11
                                    1\talpha\tA\t12
                                    2\tbeta\tB\t20
                                    3\talpha\tA\t10
                                    3\talpha\tA\t11
                                    3\talpha\tA\t12
                                    4\tgamma\tC\t30
                                    """);
                    // i is an outer join, so q keeps both of its equalities as an inner join too
                    assertQuery("SELECT t.px, i.name, q.sym qsym, q.bid FROM t LEFT JOIN " + instruments
                            + " i ON i.code = t.sym JOIN q ON q.sym = t.sym AND q.sym = i.code ORDER BY t.px, q.bid")
                            .noLeakCheck()
                            .fullFatJoins(isFullFat)
                            .expectSize(isFullFat)
                            .returns("""
                                    px\tname\tqsym\tbid
                                    1\talpha\tA\t10
                                    1\talpha\tA\t11
                                    1\talpha\tA\t12
                                    2\tbeta\tB\t20
                                    3\talpha\tA\t10
                                    3\talpha\tA\t11
                                    3\talpha\tA\t12
                                    4\tgamma\tC\t30
                                    """);
                }

                // nothing ties t.sym to t.s, so only rows with t.sym = t.s can find a q row
                assertQuery("SELECT t.px, t.sym, t.s, q.sym qsym, q.bid FROM t LEFT JOIN q ON q.sym = t.sym AND q.sym = t.s ORDER BY t.px, q.bid")
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .returns("""
                                px\tsym\ts\tqsym\tbid
                                1\tA\tA\tA\t10
                                1\tA\tA\tA\t11
                                1\tA\tA\tA\t12
                                2\tB\tB\tB\t20
                                3\tA\tA\tA\t10
                                3\tA\tA\tA\t11
                                3\tA\tA\tA\t12
                                4\tC\tX\t\tnull
                                """);

                // the key holds the master column t.sym twice, with a SYMBOL and a VARCHAR partner
                for (String join : new String[]{"RIGHT", "FULL"}) {
                    assertQuery("SELECT t.px, qv.sym qsym, qv.v, qv.bid FROM t " + join + " JOIN qv ON qv.sym = t.sym AND qv.v = t.sym ORDER BY qv.bid, t.px")
                            .noLeakCheck()
                            .fullFatJoins(isFullFat)
                            .returns("""
                                    px\tqsym\tv\tbid
                                    1\tA\tA\t10
                                    3\tA\tA\t10
                                    null\tA\tX\t11
                                    2\tB\tB\t20
                                    4\tC\tC\t30
                                    null\tD\tD\t40
                                    """);
                }

                // qv.v and qv.sym share t.s, and qv.sym also pairs with t.sym, so the VARCHAR of the
                // first pair reaches the last pair only through the middle one
                for (String on : new String[]{"qv.v = t.s AND qv.sym = t.s AND qv.sym = t.sym", "qv.sym = t.s AND qv.v = t.s AND qv.sym = t.sym"}) {
                    final String select = "SELECT t.px, qv.sym qsym, qv.v, qv.bid FROM t ";
                    assertQuery(select + "RIGHT JOIN qv ON " + on + " ORDER BY qv.bid, t.px")
                            .noLeakCheck()
                            .fullFatJoins(isFullFat)
                            .returns("""
                                    px\tqsym\tv\tbid
                                    1\tA\tA\t10
                                    3\tA\tA\t10
                                    null\tA\tX\t11
                                    2\tB\tB\t20
                                    null\tC\tC\t30
                                    null\tD\tD\t40
                                    """);
                    assertQuery(select + "FULL JOIN qv ON " + on + " ORDER BY qv.bid, t.px")
                            .noLeakCheck()
                            .fullFatJoins(isFullFat)
                            .returns("""
                                    px\tqsym\tv\tbid
                                    4\t\t\tnull
                                    1\tA\tA\t10
                                    3\tA\tA\t10
                                    null\tA\tX\t11
                                    2\tB\tB\t20
                                    null\tC\tC\t30
                                    null\tD\tD\t40
                                    """);
                }
            }
        });
    }

    @Test
    public void testRepeatedKeyColumnWithMixedTimestampUnits() throws Exception {
        // The key holds q.k twice, paired with a TIMESTAMP and a TIMESTAMP_NS column. Both pairs have
        // to write q.k in nanos. The full-fat join reads q.k from the key, so it exposes q.k in nanos.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k TIMESTAMP, kn TIMESTAMP_NS, px INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE ti (kn TIMESTAMP_NS, name VARCHAR)");
            execute("CREATE TABLE q (k TIMESTAMP, bid INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2020-01-01T00:00:01.000000Z', '2020-01-01T00:00:01.000000000Z', 1, '2024-01-01T00:00:10.000000Z'),
                        ('2020-01-01T00:00:02.000000Z', '2020-01-01T00:00:02.000000000Z', 2, '2024-01-01T00:00:20.000000Z'),
                        ('2020-01-01T00:00:01.000000Z', '2020-01-01T00:00:03.000000000Z', 3, '2024-01-01T00:00:30.000000Z'),
                        ('2020-01-01T00:00:03.000000Z', '2020-01-01T00:00:03.000000000Z', 4, '2024-01-01T00:00:40.000000Z')
                    """);
            execute("""
                    INSERT INTO ti VALUES
                        ('2020-01-01T00:00:01.000000000Z', 'one'),
                        ('2020-01-01T00:00:02.000000000Z', 'two'),
                        ('2020-01-01T00:00:03.000000000Z', 'three')
                    """);
            execute("""
                    INSERT INTO q VALUES
                        ('2020-01-01T00:00:01.000000Z', 10, '2024-01-01T00:00:05.000000Z'),
                        ('2020-01-01T00:00:02.000000Z', 20, '2024-01-01T00:00:06.000000Z'),
                        ('2020-01-01T00:00:01.000000Z', 11, '2024-01-01T00:00:25.000000Z'),
                        ('2020-01-01T00:00:01.000000Z', 12, '2024-01-01T00:00:30.000000Z'),
                        ('2020-01-01T00:00:03.000000Z', 30, '2024-01-01T00:00:35.000000Z')
                    """);
            execute("CREATE VIEW uq AS (SELECT * FROM (SELECT * FROM q UNION ALL SELECT * FROM q WHERE bid < 0) TIMESTAMP(ts))");

            final String[] slaves = {"q", "uq", "q"};
            final boolean[] isFullFatFlags = {false, false, true};
            for (int s = 0; s < slaves.length; s++) {
                final String slave = slaves[s];
                final boolean isFullFat = isFullFatFlags[s];
                final boolean isKeyExposed = s > 0;
                final String k1 = isKeyExposed ? "2020-01-01T00:00:01.000000000Z" : "2020-01-01T00:00:01.000000Z";
                final String k2 = isKeyExposed ? "2020-01-01T00:00:02.000000000Z" : "2020-01-01T00:00:02.000000Z";
                final String k3 = isKeyExposed ? "2020-01-01T00:00:03.000000000Z" : "2020-01-01T00:00:03.000000Z";

                // the inner join ties t.k to i.kn
                for (String on : new String[]{"q.k = t.k AND q.k = i.kn", "q.k = i.kn AND q.k = t.k"}) {
                    final String from = " FROM t JOIN ti i ON i.kn = t.k ";
                    assertQuery("SELECT t.ts, t.px, i.name, q.k qk, q.bid" + from + "ASOF JOIN " + slave + " q ON " + on)
                            .noLeakCheck()
                            .fullFatJoins(isFullFat)
                            .noRandomAccess()
                            .expectSize(isFullFat)
                            .timestamp("ts")
                            .returns("ts\tpx\tname\tqk\tbid\n" +
                                    "2024-01-01T00:00:10.000000Z\t1\tone\t" + k1 + "\t10\n" +
                                    "2024-01-01T00:00:20.000000Z\t2\ttwo\t" + k2 + "\t20\n" +
                                    "2024-01-01T00:00:30.000000Z\t3\tone\t" + k1 + "\t12\n" +
                                    "2024-01-01T00:00:40.000000Z\t4\tthree\t" + k3 + "\t30\n");
                    assertQuery("SELECT t.ts, t.px, i.name, q.k qk, q.bid" + from + "LT JOIN " + slave + " q ON " + on)
                            .noLeakCheck()
                            .fullFatJoins(isFullFat)
                            .noRandomAccess()
                            .expectSize(isFullFat)
                            .timestamp("ts")
                            .returns("ts\tpx\tname\tqk\tbid\n" +
                                    "2024-01-01T00:00:10.000000Z\t1\tone\t" + k1 + "\t10\n" +
                                    "2024-01-01T00:00:20.000000Z\t2\ttwo\t" + k2 + "\t20\n" +
                                    "2024-01-01T00:00:30.000000Z\t3\tone\t" + k1 + "\t11\n" +
                                    "2024-01-01T00:00:40.000000Z\t4\tthree\t" + k3 + "\t30\n");
                }

                // nothing ties t.k to t.kn, so only rows with t.k = t.kn can find a q row
                assertQuery("SELECT t.ts, t.px, q.k qk, q.bid FROM t ASOF JOIN " + slave + " q ON q.k = t.k AND q.k = t.kn")
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .noRandomAccess()
                        .expectSize()
                        .timestamp("ts")
                        .returns("ts\tpx\tqk\tbid\n" +
                                "2024-01-01T00:00:10.000000Z\t1\t" + k1 + "\t10\n" +
                                "2024-01-01T00:00:20.000000Z\t2\t" + k2 + "\t20\n" +
                                "2024-01-01T00:00:30.000000Z\t3\t\tnull\n" +
                                "2024-01-01T00:00:40.000000Z\t4\t" + k3 + "\t30\n");
                // one TIMESTAMP_NS partner alone
                assertQuery("SELECT t.ts, t.px, q.k qk, q.bid FROM t ASOF JOIN " + slave + " q ON q.k = t.kn")
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .noRandomAccess()
                        .expectSize()
                        .timestamp("ts")
                        .returns("ts\tpx\tqk\tbid\n" +
                                "2024-01-01T00:00:10.000000Z\t1\t" + k1 + "\t10\n" +
                                "2024-01-01T00:00:20.000000Z\t2\t" + k2 + "\t20\n" +
                                "2024-01-01T00:00:30.000000Z\t3\t\tnull\n" +
                                "2024-01-01T00:00:40.000000Z\t4\t" + k3 + "\t30\n");
            }

            for (boolean isFullFat : new boolean[]{false, true}) {
                assertQuery("SELECT t.px, i.name, q.bid FROM t JOIN ti i ON i.kn = t.k LEFT JOIN q ON q.k = t.k AND q.k = i.kn ORDER BY t.px, q.bid")
                        .noLeakCheck()
                        .fullFatJoins(isFullFat)
                        .returns("""
                                px\tname\tbid
                                1\tone\t10
                                1\tone\t11
                                1\tone\t12
                                2\ttwo\t20
                                3\tone\t10
                                3\tone\t11
                                3\tone\t12
                                4\tthree\t30
                                """);
            }
        });
    }

    @Test
    public void testRepeatedKeyColumnWithMixedTimestampUnitsAndNullKeys() throws Exception {
        // A TIMESTAMP key column with a TIMESTAMP_NS partner writes its keys in nanos. The widening keeps
        // NULL as NULL, so a NULL key matches NULL keys, as keys of one unit do, and never the epoch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (px INT, k TIMESTAMP, kn TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE ik (id INT, code TIMESTAMP)");
            execute("CREATE TABLE q (bid INT, kk TIMESTAMP, kkn TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1, '2020-01-01T00:00:01.000000Z', '2020-01-01T00:00:01.000000000Z', '2024-01-01T00:00:10.000000Z'),
                        (2, NULL, NULL, '2024-01-01T00:00:20.000000Z'),
                        (3, '1970-01-01T00:00:00.000000Z', '1970-01-01T00:00:00.000000000Z', '2024-01-01T00:00:30.000000Z')
                    """);
            execute("INSERT INTO ik VALUES (1, '2020-01-01T00:00:01.000000Z')");
            // the NULL row follows the epoch row, so a NULL key read as the epoch would find it
            execute("""
                    INSERT INTO q VALUES
                        (10, '2020-01-01T00:00:01.000000Z', '2020-01-01T00:00:01.000000000Z', '2024-01-01T00:00:02.000000Z'),
                        (30, '1970-01-01T00:00:00.000000Z', '1970-01-01T00:00:00.000000000Z', '2024-01-01T00:00:04.000000Z'),
                        (20, NULL, NULL, '2024-01-01T00:00:06.000000Z')
                    """);
            execute("CREATE VIEW uq AS (SELECT * FROM (SELECT * FROM q UNION ALL SELECT * FROM q WHERE bid < 0) TIMESTAMP(ts))");

            // HORIZON JOIN builds its own key sinks, from the same encodings
            for (String on : new String[]{"q.kk = t.kn", "q.kkn = t.k", "q.kk = t.kn AND q.kk = t.k"}) {
                assertQuery("SELECT t.px, sum(q.bid) bid FROM t HORIZON JOIN q ON (" + on + ") LIST (0) AS h ORDER BY t.px")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                px\tbid
                                1\t10
                                2\t20
                                3\t30
                                """);
            }

            // the chunked sink falls back to the single-method one for keys this narrow
            for (int sinkType : new int[]{RecordSinkFactory.SINK_TYPE_SINGLE_METHOD, RecordSinkFactory.SINK_TYPE_LOOPING}) {
                setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, sinkType);
                final String[] slaves = {"q", "uq", "q"};
                final boolean[] isFullFatFlags = {false, false, true};
                for (int s = 0; s < slaves.length; s++) {
                    final String slave = slaves[s];
                    final boolean isFullFat = isFullFatFlags[s];
                    final boolean isKeyExposed = s > 0;
                    final String k1 = isKeyExposed ? "2020-01-01T00:00:01.000000000Z" : "2020-01-01T00:00:01.000000Z";
                    final String k0 = isKeyExposed ? "1970-01-01T00:00:00.000000000Z" : "1970-01-01T00:00:00.000000Z";
                    for (String join : new String[]{"ASOF", "LT"}) {
                        // q.kk repeats with a TIMESTAMP_NS partner and a TIMESTAMP one that the LEFT join
                        // can leave NULL: px 2 has NULL keys only, px 3 has the epoch and a NULL
                        assertQuery("SELECT t.ts, t.px, i.id, q.kk qk, q.bid FROM t LEFT JOIN ik i ON i.code = t.kn "
                                + join + " JOIN " + slave + " q ON q.kk = t.kn AND q.kk = i.code")
                                .noLeakCheck()
                                .fullFatJoins(isFullFat)
                                .noRandomAccess()
                                .timestamp("ts")
                                .returns("ts\tpx\tid\tqk\tbid\n" +
                                        "2024-01-01T00:00:10.000000Z\t1\t1\t" + k1 + "\t10\n" +
                                        "2024-01-01T00:00:20.000000Z\t2\tnull\t\t20\n" +
                                        "2024-01-01T00:00:30.000000Z\t3\tnull\t\tnull\n");
                        // one pair, with the slave key widened
                        assertQuery("SELECT t.ts, t.px, q.kk qk, q.bid FROM t " + join + " JOIN " + slave + " q ON q.kk = t.kn")
                                .noLeakCheck()
                                .fullFatJoins(isFullFat)
                                .noRandomAccess()
                                .expectSize()
                                .timestamp("ts")
                                .returns("ts\tpx\tqk\tbid\n" +
                                        "2024-01-01T00:00:10.000000Z\t1\t" + k1 + "\t10\n" +
                                        "2024-01-01T00:00:20.000000Z\t2\t\t20\n" +
                                        "2024-01-01T00:00:30.000000Z\t3\t" + k0 + "\t30\n");
                        // one pair, with the master key widened
                        assertQuery("SELECT t.ts, t.px, q.kkn qk, q.bid FROM t " + join + " JOIN " + slave + " q ON q.kkn = t.k")
                                .noLeakCheck()
                                .fullFatJoins(isFullFat)
                                .noRandomAccess()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tpx\tqk\tbid
                                        2024-01-01T00:00:10.000000Z\t1\t2020-01-01T00:00:01.000000000Z\t10
                                        2024-01-01T00:00:20.000000Z\t2\t\t20
                                        2024-01-01T00:00:30.000000Z\t3\t1970-01-01T00:00:00.000000000Z\t30
                                        """);
                    }
                }

                for (boolean isFullFat : new boolean[]{false, true}) {
                    assertQuery("SELECT t.px, i.id, q.bid FROM t LEFT JOIN ik i ON i.code = t.kn LEFT JOIN q ON q.kk = t.kn AND q.kk = i.code ORDER BY t.px")
                            .noLeakCheck()
                            .fullFatJoins(isFullFat)
                            .returns("""
                                    px\tid\tbid
                                    1\t1\t10
                                    2\tnull\t20
                                    3\tnull\tnull
                                    """);
                    // q.kkn repeats under a RIGHT join, which keeps both pairs
                    assertQuery("SELECT t.px, q.bid FROM t RIGHT JOIN q ON q.kkn = t.k AND q.kkn = t.kn ORDER BY q.bid")
                            .noLeakCheck()
                            .fullFatJoins(isFullFat)
                            .returns("""
                                    px\tbid
                                    1\t10
                                    2\t20
                                    3\t30
                                    """);
                    // one pair: a NULL key matches the NULL row, not the epoch row
                    for (String on : new String[]{"q.kkn = t.k", "q.kk = t.kn"}) {
                        assertQuery("SELECT t.px, q.bid FROM t LEFT JOIN q ON " + on + " ORDER BY t.px")
                                .noLeakCheck()
                                .fullFatJoins(isFullFat)
                                .returns("""
                                        px\tbid
                                        1\t10
                                        2\t20
                                        3\t30
                                        """);
                    }
                }

                // the fused hash join GROUP BY builds its key sinks with the same factory
                for (String on : new String[]{"q.kkn = t.k", "q.kk = t.kn"}) {
                    assertQuery("SELECT t.px, count(q.bid) n, sum(q.bid) s FROM t LEFT JOIN q ON " + on + " ORDER BY t.px")
                            .noLeakCheck()
                            .expectSize()
                            .withPlanContaining("Async Hash Join Group By")
                            .returns("""
                                    px\tn\ts
                                    1\t1\t10
                                    2\t1\t20
                                    3\t1\t30
                                    """);
                }
            }
        });
    }

    @Test
    public void testRightJoinImpliedEqualityImpliesAnother() throws Exception {
        // t4's ON clause implies t3.a = t2.c and t3.a = t1.c, two equalities of t3. Merged into t3's
        // join context, they imply t2.c = t1.c in turn, which the plan must keep: without it, t2 joins
        // without a key, and every t2 row matches.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT, c INT)");
            execute("CREATE TABLE t1 (a INT, b INT, c INT)");
            execute("CREATE TABLE t2 (a INT, b INT, c INT)");
            execute("CREATE TABLE t3 (a INT, b INT, c INT)");
            execute("CREATE TABLE t4 (a INT, b INT, c INT)");
            execute("INSERT INTO t0 VALUES (null, 1, 2), (1, 2, 2)");
            execute("INSERT INTO t1 VALUES (3, null, 1), (2, 1, 1), (3, null, 3)");
            execute("INSERT INTO t2 VALUES (2, 2, 3), (2, null, null), (1, null, 3), (1, 2, 1), (1, 2, 2)");
            execute("INSERT INTO t3 VALUES (1, null, null), (2, 1, 1), (1, 1, 2), (null, 3, 1), (1, 2, 2), (null, 1, 2)");
            execute("INSERT INTO t4 VALUES (1, 2, 3), (3, 2, 3), (1, null, 2), (2, null, 2), (2, null, 3), (2, 3, 1)");

            final String from = """
                     FROM t0
                    RIGHT JOIN t1 ON t0.a = t1.c AND t1.b = t0.c
                    CROSS JOIN t2
                    JOIN t3 ON t3.a = t1.c AND t3.a = t2.a AND t3.b = 1 AND t3.a = t1.b
                    RIGHT JOIN t4 ON t2.c = t4.a AND t3.a = t4.a AND t4.a = t1.c
                    """;
            assertQuery("SELECT t1.a c1a, t2.c c2c, t3.a c3a, t3.b c3b, t4.a c4a, t4.b c4b, t4.c c4c" + from + "ORDER BY c4a, c4c")
                    .noLeakCheck()
                    .returns("""
                            c1a\tc2c\tc3a\tc3b\tc4a\tc4b\tc4c
                            2\t1\t1\t1\t1\tnull\t2
                            2\t1\t1\t1\t1\t2\t3
                            null\tnull\tnull\tnull\t2\t3\t1
                            null\tnull\tnull\tnull\t2\tnull\t2
                            null\tnull\tnull\tnull\t2\tnull\t3
                            null\tnull\tnull\tnull\t3\t2\t3
                            """);
            assertQuery("SELECT count(*)" + from)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            6
                            """);

            // the same chain of implied equalities, with t1, t2 and t3 cross-joined
            for (String t4On : new String[]{"t4.a = t2.c AND t4.a = t3.a AND t4.a = t1.c", "t2.c = t4.a AND t3.a = t4.a AND t4.a = t1.c"}) {
                assertQuery("SELECT count(*) FROM t1 CROSS JOIN t2 CROSS JOIN t3 RIGHT JOIN t4 ON " + t4On)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                16
                                """);
            }
        });
    }

    @Test
    public void testRightJoinImpliedEqualityKeysEarlierInnerJoin() throws Exception {
        // c's ON clause implies b.k = a.k. A RIGHT join drops the master rows that it does not match,
        // as an inner join does, so the implied equality may filter its master: it keys the join of
        // a and b, which then pairs only the rows that c can match, instead of every pair that shares
        // x. NULL keys match each other, so the (103, 203) pair still matches c's NULL row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT, x INT, v INT)");
            execute("CREATE TABLE b (k INT, x INT, y INT, v INT)");
            execute("CREATE TABLE c (k INT, v INT)");
            execute("CREATE TABLE d (x INT, v INT)");
            execute("INSERT INTO a VALUES (1, 0, 101), (2, 0, 102), (null, 1, 103), (3, 1, 104)");
            execute("INSERT INTO b VALUES (1, 0, 1, 201), (2, 1, 0, 202), (null, 1, null, 203), (2, 0, 2, 204)");
            execute("INSERT INTO c VALUES (1, 301), (2, 302), (null, 303), (3, 304)");
            execute("INSERT INTO d VALUES (0, 401), (5, 402)");

            for (String cOn : new String[]{"c.k = a.k AND c.k = b.k", "c.k = b.k AND c.k = a.k"}) {
                final String from = " FROM a JOIN b ON a.x = b.x RIGHT JOIN c ON " + cOn;
                assertQuery("SELECT a.v av, b.v bv, c.v cv" + from + " ORDER BY cv")
                        .noLeakCheck()
                        .withPlanContaining("condition: b.k=a.k and b.x=a.x")
                        .returns("""
                                av\tbv\tcv
                                101\t201\t301
                                102\t204\t302
                                103\t203\t303
                                null\tnull\t304
                                """);
                assertQuery("SELECT count(*)" + from)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("condition: b.k=a.k and b.x=a.x")
                        .returns("""
                                count
                                4
                                """);
            }

            // the implied equality turns the cross join into a hash join
            assertQuery("SELECT a.v av, b.v bv, c.v cv FROM a CROSS JOIN b RIGHT JOIN c ON c.k = a.k AND c.k = b.k ORDER BY cv, bv")
                    .noLeakCheck()
                    .withPlanContaining("condition: b.k=a.k")
                    .withPlanNotContaining("Cross Join")
                    .returns("""
                            av\tbv\tcv
                            101\t201\t301
                            102\t202\t302
                            102\t204\t302
                            103\t203\t303
                            null\tnull\t304
                            """);

            // an implied equality of b's own columns filters b's rows before the join
            assertQuery("SELECT a.v av, b.v bv, c.v cv FROM a JOIN b ON a.x = b.x RIGHT JOIN c ON c.k = b.k AND c.k = b.y ORDER BY cv, av, bv")
                    .noLeakCheck()
                    .withPlanContaining("filter: k=y")
                    .returns("""
                            av\tbv\tcv
                            101\t201\t301
                            102\t201\t301
                            101\t204\t302
                            102\t204\t302
                            103\t203\t303
                            104\t203\t303
                            null\tnull\t304
                            """);

            // a LEFT join keeps every row of its master, so the filter may also run before it
            assertQuery("SELECT a.v av, b.v bv, d.v dv, c.v cv FROM a JOIN b ON a.x = b.x LEFT JOIN d ON d.x = b.x RIGHT JOIN c ON c.k = a.k AND c.k = b.k ORDER BY cv")
                    .noLeakCheck()
                    .withPlanContaining("condition: b.k=a.k and b.x=a.x")
                    .returns("""
                            av\tbv\tdv\tcv
                            101\t201\t401\t301
                            102\t204\t401\t302
                            103\t203\tnull\t303
                            null\tnull\tnull\t304
                            """);

            // a later join sees the same rows
            assertQuery("SELECT a.v av, b.v bv, c.v cv, d.v dv FROM a JOIN b ON a.x = b.x RIGHT JOIN c ON c.k = a.k AND c.k = b.k LEFT JOIN d ON d.x = a.x ORDER BY cv")
                    .noLeakCheck()
                    .withPlanContaining("condition: b.k=a.k and b.x=a.x")
                    .returns("""
                            av\tbv\tcv\tdv
                            101\t201\t301\t401
                            102\t204\t302\t401
                            103\t203\t303\tnull
                            null\tnull\t304\tnull
                            """);
        });
    }

    @Test
    public void testRightJoinImpliedEqualityStaysOutOfOuterJoins() throws Exception {
        // c's ON clause implies an equality of its master side, which the plan may use only where it
        // drops the rows that it fails, as the RIGHT join does. It must not key the outer join of b,
        // filter the rows that a later RIGHT or FULL join NULL-extends, or filter the first table,
        // whose filter runs after the last join. Each query returns other rows when c keeps only one
        // of its equalities.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT, x INT, y INT, v INT)");
            execute("CREATE TABLE b (k INT, x INT, v INT)");
            execute("CREATE TABLE c (k INT, v INT)");
            execute("CREATE TABLE d (x INT, v INT)");
            execute("INSERT INTO a VALUES (null, 0, 1, 101), (1, 2, null, 102), (1, 2, null, 103)");
            execute("INSERT INTO b VALUES (0, 2, 201)");
            execute("INSERT INTO c VALUES (1, 301), (0, 302), (null, 303)");
            execute("INSERT INTO d VALUES (2, 401), (1, 402)");

            // no (a, b) pair has b.k = a.k; filtered by it, the pairs could not match d's (2, 401) row,
            // which the outer join would then NULL-extend for c's NULL row to match
            for (String outerJoin : new String[]{"RIGHT", "FULL"}) {
                for (String cOn : new String[]{"c.k = a.k AND c.k = b.k", "c.k = b.k AND c.k = a.k"}) {
                    assertQuery("SELECT a.v av, b.v bv, d.v dv, c.v cv FROM a JOIN b ON a.x = b.x " + outerJoin + " JOIN d ON d.x = b.x RIGHT JOIN c ON " + cOn + " ORDER BY cv")
                            .noLeakCheck()
                            .returns("""
                                    av\tbv\tdv\tcv
                                    null\tnull\tnull\t301
                                    null\tnull\tnull\t302
                                    null\tnull\t402\t303
                                    """);
                }
            }

            // no a row matches both equalities; filtered after the join by a.k = a.y, the a rows that
            // match one of them would take c's rows with them instead of leaving them NULL-extended
            for (String cOn : new String[]{"c.k = a.k AND c.k = a.y", "c.k = a.y AND c.k = a.k"}) {
                assertQuery("SELECT a.v av, c.v cv FROM a RIGHT JOIN c ON " + cOn + " ORDER BY cv")
                        .noLeakCheck()
                        .returns("""
                                av\tcv
                                null\t301
                                null\t302
                                null\t303
                                """);
            }

            // as a key of b's outer join, b.k = a.k would NULL-extend the rows that it fails
            for (String outerJoin : new String[]{"LEFT", "RIGHT", "FULL"}) {
                for (String cOn : new String[]{"c.k = a.k AND c.k = b.k", "c.k = b.k AND c.k = a.k"}) {
                    assertQuery("SELECT a.v av, b.v bv, c.v cv FROM a " + outerJoin + " JOIN b ON a.x = b.x RIGHT JOIN c ON " + cOn + " ORDER BY cv")
                            .noLeakCheck()
                            .returns("RIGHT".equals(outerJoin)
                                    ? """
                                    av\tbv\tcv
                                    null\tnull\t301
                                    null\tnull\t302
                                    null\tnull\t303
                                    """
                                    : """
                                    av\tbv\tcv
                                    null\tnull\t301
                                    null\tnull\t302
                                    101\tnull\t303
                                    """);
                }
            }
        });
    }

    @Test
    public void testRightJoinPreservedSideFilterRunsBeforeJoin() throws Exception {
        // A RIGHT join keeps every row of its right side, so a WHERE predicate that reads only that
        // side selects the same rows whether it filters the side's scan or the join's output. The
        // optimizer pushes it into the scan, as it does for the master of a LEFT join, unless a later
        // join NULL-extends the side. A predicate on the NULL-extended left side stays above the join.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE b (k INT, x INT, y INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE c (k INT, w INT)");
            execute("""
                    INSERT INTO a VALUES
                        (1, 10, '2024-01-01T00:00:00.000000Z'),
                        (2, 20, '2024-01-02T00:00:00.000000Z'),
                        (5, 50, '2024-01-03T00:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO b VALUES
                        (1, 100, 100, '2024-01-01T00:00:00.000000Z'),
                        (2, -1, 7, '2024-01-02T00:00:00.000000Z'),
                        (3, 300, 300, '2024-01-03T00:00:00.000000Z'),
                        (4, 400, 1, '2024-01-04T00:00:00.000000Z')
                    """);
            execute("INSERT INTO c VALUES (1, 1_000), (3, 3_000), (6, 6_000)");

            final String select = "SELECT a.k ak, a.v, b.k bk, b.x FROM a RIGHT JOIN b ON a.k = b.k WHERE ";
            final String positive = """
                    ak\tv\tbk\tx
                    1\t10\t1\t100
                    null\tnull\t3\t300
                    null\tnull\t4\t400
                    """;
            assertQuery(select + "b.x > 0 ORDER BY bk")
                    .noLeakCheck()
                    .withPlanContaining("filter: 0<x")
                    .withPlanNotContaining("Filter filter: 0<b.x")
                    .returns(positive);
            // A predicate on the designated timestamp narrows the scan to an interval.
            assertQuery(select + "b.ts >= '2024-01-03' ORDER BY bk")
                    .noLeakCheck()
                    .withPlanContaining("Interval forward scan on: b")
                    .returns("""
                            ak\tv\tbk\tx
                            null\tnull\t3\t300
                            null\tnull\t4\t400
                            """);
            // An equality of two columns of the side takes its own route through the optimizer.
            assertQuery(select + "b.x = b.y ORDER BY bk")
                    .noLeakCheck()
                    .withPlanContaining("filter: x=y")
                    .withPlanNotContaining("Filter filter: b.x=b.y")
                    .returns("""
                            ak\tv\tbk\tx
                            1\t10\t1\t100
                            null\tnull\t3\t300
                            """);
            // So does a column compared with a constant.
            assertQuery(select + "b.x = 400")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("filter: x=400")
                    .withPlanNotContaining("Filter filter: b.x=400")
                    .returns("""
                            ak\tv\tbk\tx
                            null\tnull\t4\t400
                            """);
            // An outer query's WHERE clause reaches the join model after the join's own.
            assertQuery("SELECT * FROM (SELECT a.k ak, a.v, b.k bk, b.x FROM a RIGHT JOIN b ON a.k = b.k) WHERE x > 0 ORDER BY bk")
                    .noLeakCheck()
                    .withPlanContaining("filter: 0<x")
                    .withPlanNotContaining("Filter filter: 0<b.x")
                    .returns(positive);
            // The left side is NULL-extended, so its predicate stays above the join.
            assertQuery(select + "a.v > 0 ORDER BY bk")
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: 0<a.v")
                    .returns("""
                            ak\tv\tbk\tx
                            1\t10\t1\t100
                            2\t20\t2\t-1
                            """);
            // A FULL join NULL-extends both sides, so neither side's predicate filters its scan.
            assertQuery("SELECT a.k ak, a.v, b.k bk, b.x FROM a FULL JOIN b ON a.k = b.k WHERE b.x > 0 ORDER BY bk")
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: 0<b.x")
                    .returns(positive);

            // A later LEFT or INNER join keeps every row of b that it matches, so the predicate
            // still filters b's scan.
            final String chain = "SELECT a.k ak, b.k bk, b.x, c.k ck FROM a RIGHT JOIN b ON a.k = b.k ";
            assertQuery(chain + "LEFT JOIN c ON c.k = b.k WHERE b.x > 0 ORDER BY bk")
                    .noLeakCheck()
                    .withPlanContaining("filter: 0<x")
                    .withPlanNotContaining("Filter filter: 0<b.x")
                    .returns("""
                            ak\tbk\tx\tck
                            1\t1\t100\t1
                            null\t3\t300\t3
                            null\t4\t400\tnull
                            """);
            assertQuery(chain + "JOIN c ON c.k = b.k WHERE b.x > 0 ORDER BY bk")
                    .noLeakCheck()
                    .withPlanContaining("filter: 0<x")
                    .withPlanNotContaining("Filter filter: 0<b.x")
                    .returns("""
                            ak\tbk\tx\tck
                            1\t1\t100\t1
                            null\t3\t300\t3
                            """);
            // A later RIGHT or FULL join NULL-extends b for the c row that matches nothing, and the
            // predicate must drop that row. Pushed into b's scan, it would keep the (null,null,null,6)
            // row.
            assertQuery(chain + "RIGHT JOIN c ON c.k = b.k WHERE b.x > 0 ORDER BY ck")
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: 0<b.x")
                    .returns("""
                            ak\tbk\tx\tck
                            1\t1\t100\t1
                            null\t3\t300\t3
                            """);
            assertQuery(chain + "FULL JOIN c ON c.k = b.k WHERE b.x > 0 ORDER BY bk")
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: 0<b.x")
                    .returns("""
                            ak\tbk\tx\tck
                            1\t1\t100\t1
                            null\t3\t300\t3
                            null\t4\t400\tnull
                            """);
        });
    }

    @Test
    public void testSelectAliasTest() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table contact_events as (" +
                            "  select rnd_symbol(4,4,4,2) _id, " +
                            "    rnd_symbol(4,4,4,2) contactid, " +
                            "    CAST(x as Timestamp) timestamp, " +
                            "    rnd_symbol(4,4,4,2) groupId " +
                            "  from long_sequence(50)) " +
                            "timestamp(timestamp)"
            );
            execute(
                    "create table contacts as (" +
                            "  select rnd_symbol(4,4,4,2) _id, " +
                            "    CAST(x as Timestamp) timestamp, " +
                            "    rnd_symbol(4,4,4,2) notRealType " +
                            "  from long_sequence(50)) " +
                            "timestamp(timestamp)"
            );

            assertQuery("""
                    with
                    eventlist as (select * from contact_events latest on timestamp partition by _id order by timestamp)
                    ,contactlist as (select * from contacts latest on timestamp partition by _id order by timestamp)
                    ,c as (select distinct contactid from eventlist where groupId = 'ykom80aRN5AwUcuRp4LJ' except select distinct _id as contactId from contactlist where notRealType = 'bot')
                    select
                    c.contactId as id
                    from
                    c
                    join contactlist on c.contactid = contactlist._id
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("id\n");
        });
    }

    @Test
    public void testSelfJoinOnSymbolKey1() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (pair SYMBOL, ts TIMESTAMP, price INT) TIMESTAMP(ts) PARTITION BY DAY");

            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC-USD', '2000-01-01T00:00:00.000000Z', 1)," +
                            "('BTC-USD', '2001-01-01T00:00:01.000000Z', 2)," +
                            "('ETH-USD', '2001-01-01T00:00:00.000000Z', 3)," +
                            "('ETH-USD', '2001-01-01T00:00:01.000000Z', 4)"
            );

            String query = "SELECT * FROM trades t1 JOIN trades t2 ON (pair) ORDER BY pair, price";
            String expected = """
                    pair\tts\tprice\tpair1\tts1\tprice1
                    BTC-USD\t2000-01-01T00:00:00.000000Z\t1\tBTC-USD\t2001-01-01T00:00:01.000000Z\t2
                    BTC-USD\t2000-01-01T00:00:00.000000Z\t1\tBTC-USD\t2000-01-01T00:00:00.000000Z\t1
                    BTC-USD\t2001-01-01T00:00:01.000000Z\t2\tBTC-USD\t2001-01-01T00:00:01.000000Z\t2
                    BTC-USD\t2001-01-01T00:00:01.000000Z\t2\tBTC-USD\t2000-01-01T00:00:00.000000Z\t1
                    ETH-USD\t2001-01-01T00:00:00.000000Z\t3\tETH-USD\t2001-01-01T00:00:01.000000Z\t4
                    ETH-USD\t2001-01-01T00:00:00.000000Z\t3\tETH-USD\t2001-01-01T00:00:00.000000Z\t3
                    ETH-USD\t2001-01-01T00:00:01.000000Z\t4\tETH-USD\t2001-01-01T00:00:01.000000Z\t4
                    ETH-USD\t2001-01-01T00:00:01.000000Z\t4\tETH-USD\t2001-01-01T00:00:00.000000Z\t3
                    """;
            assertQuery(query)
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testSelfJoinOnSymbolKey2() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (pair SYMBOL, ts TIMESTAMP, price INT) TIMESTAMP(ts) PARTITION BY DAY");

            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC-USD', '2000-01-01T00:00:00.000000Z', 1)," +
                            "('BTC-USD', '2001-01-01T00:00:01.000000Z', 2)," +
                            "('ETH-USD', '2001-01-01T00:00:00.000000Z', 3)," +
                            "('ETH-USD', '2001-01-01T00:00:01.000000Z', 4)"
            );

            String query = "SELECT * FROM (select pair p1, ts, price from trades) t1 " +
                    "JOIN (select ts, price, pair p2 from trades) t2 ON t1.p1 = t2.p2 " +
                    "ORDER BY p1, price, price1";
            String expected = """
                    p1\tts\tprice\tts1\tprice1\tp2
                    BTC-USD\t2000-01-01T00:00:00.000000Z\t1\t2000-01-01T00:00:00.000000Z\t1\tBTC-USD
                    BTC-USD\t2000-01-01T00:00:00.000000Z\t1\t2001-01-01T00:00:01.000000Z\t2\tBTC-USD
                    BTC-USD\t2001-01-01T00:00:01.000000Z\t2\t2000-01-01T00:00:00.000000Z\t1\tBTC-USD
                    BTC-USD\t2001-01-01T00:00:01.000000Z\t2\t2001-01-01T00:00:01.000000Z\t2\tBTC-USD
                    ETH-USD\t2001-01-01T00:00:00.000000Z\t3\t2001-01-01T00:00:00.000000Z\t3\tETH-USD
                    ETH-USD\t2001-01-01T00:00:00.000000Z\t3\t2001-01-01T00:00:01.000000Z\t4\tETH-USD
                    ETH-USD\t2001-01-01T00:00:01.000000Z\t4\t2001-01-01T00:00:00.000000Z\t3\tETH-USD
                    ETH-USD\t2001-01-01T00:00:01.000000Z\t4\t2001-01-01T00:00:01.000000Z\t4\tETH-USD
                    """;
            assertQuery(query)
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testSelfJoinOnSymbolKey3() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (pair SYMBOL, side SYMBOL, ts TIMESTAMP, price INT) TIMESTAMP(ts) PARTITION BY DAY");

            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC-USD', 'sell', '2000-01-01T00:00:00.000000Z', 1)," +
                            "('BTC-USD', 'buy', '2001-01-01T00:00:01.000000Z', 2)," +
                            "('ETH-USD', 'sell', '2001-01-01T00:00:00.000000Z', 4)," +
                            "('ETH-USD', 'buy', '2001-01-01T00:00:01.000000Z', 5)"
            );

            String query = "SELECT * FROM trades t1 JOIN trades t2 ON(pair, side)";
            String expected = """
                    pair\tside\tts\tprice\tpair1\tside1\tts1\tprice1
                    BTC-USD\tsell\t2000-01-01T00:00:00.000000Z\t1\tBTC-USD\tsell\t2000-01-01T00:00:00.000000Z\t1
                    ETH-USD\tsell\t2001-01-01T00:00:00.000000Z\t4\tETH-USD\tsell\t2001-01-01T00:00:00.000000Z\t4
                    ETH-USD\tbuy\t2001-01-01T00:00:01.000000Z\t5\tETH-USD\tbuy\t2001-01-01T00:00:01.000000Z\t5
                    BTC-USD\tbuy\t2001-01-01T00:00:01.000000Z\t2\tBTC-USD\tbuy\t2001-01-01T00:00:01.000000Z\t2
                    """;
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testSelfJoinOnSymbolKey4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym1 SYMBOL, sym2 SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            execute(
                    "INSERT INTO x VALUES " +
                            "('1', '2', '2000-01-01T00:00:00.000000Z')," +
                            "('3', '4', '2000-01-01T00:00:00.000000Z')," +
                            "('1', '1', '2000-01-01T00:00:00.000000Z')," +
                            "('2', '2', '2000-01-01T00:00:00.000000Z')," +
                            "('4', '3', '2000-01-01T00:00:00.000000Z')"
            );

            String query = "SELECT * FROM (select sym1 s, ts from x) x1 " +
                    "INNER JOIN (select sym2 s, ts from x) x2 ON(s)";
            String expected = """
                    s\tts\ts1\tts1
                    1\t2000-01-01T00:00:00.000000Z\t1\t2000-01-01T00:00:00.000000Z
                    3\t2000-01-01T00:00:00.000000Z\t3\t2000-01-01T00:00:00.000000Z
                    1\t2000-01-01T00:00:00.000000Z\t1\t2000-01-01T00:00:00.000000Z
                    2\t2000-01-01T00:00:00.000000Z\t2\t2000-01-01T00:00:00.000000Z
                    2\t2000-01-01T00:00:00.000000Z\t2\t2000-01-01T00:00:00.000000Z
                    4\t2000-01-01T00:00:00.000000Z\t4\t2000-01-01T00:00:00.000000Z
                    """;
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testSelfJoinOnSymbolKey5() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (pair SYMBOL, ts TIMESTAMP, price INT) TIMESTAMP(ts) PARTITION BY DAY");

            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC-USD', '2000-01-01T00:00:00.000000Z', 1)," +
                            "('BTC-USD', '2001-01-01T00:00:01.000000Z', 2)," +
                            "('ETH-USD', '2001-01-01T00:00:02.000000Z', 3)"
            );

            String leftJoinQuery = "SELECT * FROM (select * from trades where pair = 'BTC-USD') t1 " +
                    "LEFT JOIN (select * from trades where pair = 'BTC-USD' and price > 1) t2 ON(pair)";
            String rightJoinQuery = "SELECT * FROM (select * from trades where pair = 'BTC-USD') t1 " +
                    "RIGHT JOIN (select * from trades where pair = 'BTC-USD' and price > 1) t2 ON(pair)";
            String fullJoinQuery = "SELECT * FROM (select * from trades where pair = 'BTC-USD') t1 " +
                    "FULL JOIN (select * from trades where pair = 'BTC-USD' and price > 1) t2 ON(pair)";
            String expected = """
                    pair\tts\tprice\tpair1\tts1\tprice1
                    BTC-USD\t2000-01-01T00:00:00.000000Z\t1\tBTC-USD\t2001-01-01T00:00:01.000000Z\t2
                    BTC-USD\t2001-01-01T00:00:01.000000Z\t2\tBTC-USD\t2001-01-01T00:00:01.000000Z\t2
                    """;
            assertQuery(leftJoinQuery)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(expected);
            assertQuery(rightJoinQuery)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
            assertQuery(fullJoinQuery)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testSpliceColumnEqColumnMasterFilterStaysPostJoin() throws Exception {
        // SPLICE variant of testColumnEqColumnMasterFilterStaysPostJoin: a same-table column
        // comparison (m.c1 = m.c2) is single-table, so it used to be pushed into the master
        // sub-query. SPLICE NULL-extends the master (slave-only timestamps emit NULL-master rows),
        // so pushing it emptied the master and paired each slave timestamp with a NULL master,
        // leaking a row per slave-only timestamp. Held post-join the splice keeps the prevailing
        // master row, which c1=c2 drops, and the single pre-master NULL-master row, which c1=c2
        // keeps because null=null is true for INT, leaving exactly one (null,null) row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (c1 INT, c2 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES (1, 2, 2)");
            execute("CREATE TABLE s (sv INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // s leads (ts=1) and trails (ts=3) the single master row at ts=2; the ts=1 row is a
            // pre-master NULL-master splice row, the ts=2/ts=3 rows carry the prevailing (1,2) master.
            execute("INSERT INTO s VALUES (10, 1), (20, 3)");

            assertQuery("SELECT m.c1, m.c2 FROM m SPLICE JOIN s WHERE m.c1 = m.c2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Filter filter: m.c1=m.c2")
                    .returns("c1\tc2\nnull\tnull\n");
        });
    }

    @Test
    public void testSpliceConstOnLhsMasterFilterStaysPostJoin() throws Exception {
        // Const-on-LHS variant of testSpliceJoinMasterFilterProjectsSlaveColumn: the equality is
        // written 'A' = m.k, so analyseEquals routes it through the case-0 (constant on the left)
        // branch rather than case-1. That branch registers the literal const for the transitive
        // slave prune, but addTransitiveFilters must still skip the push for SPLICE: SPLICE is a
        // temporal prevailing join, so pruning the slave to key 'A' shifts which slave row prevails
        // at each master timestamp and diverges the literal from the bind form. The master-side
        // predicate stays a post-join filter and the slave column is projected to surface a diverging
        // prevailing value if the const were wrongly pushed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (k SYMBOL, mv INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES ('A',1,1),('B',2,2),('A',3,5)");
            execute("CREATE TABLE s (k SYMBOL, sv INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // The B-key slave rows (99@1, 88@4) prevail at master timestamps for key A; pruning them
            // would change the prevailing slave value, so the literal and bind forms would diverge.
            execute("INSERT INTO s VALUES ('A',10,0),('B',99,1),('A',20,3),('B',88,4),('A',30,6)");

            final String expected = """
                    k\tmv\tsv
                    A\t1\tnull
                    A\t1\t20
                    A\t3\t20
                    A\t3\t30
                    """;

            // Literal form: the predicate stays a post-join Filter over a full slave scan (no
            // 'A'=k pushed into the slave sub-query). The plan normalizes 'A'=m.k to m.k='A'.
            bindVariableService.clear();
            assertQuery("SELECT m.k, m.mv, s.sv FROM m SPLICE JOIN s ON m.k = s.k WHERE 'A' = m.k ORDER BY m.mv, s.sv")
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: m.k='A'")
                    .returns(expected);

            // Bind-variable form must produce the identical result.
            bindVariableService.clear();
            bindVariableService.setStr("v", "A");
            assertQuery("SELECT m.k, m.mv, s.sv FROM m SPLICE JOIN s ON m.k = s.k WHERE :v::SYMBOL = m.k ORDER BY m.mv, s.sv")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testSpliceCorrectness() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table orders (sym SYMBOL, amount DOUBLE, side BYTE, timestamp TIMESTAMP) timestamp(timestamp)");
            execute("create table quotes (sym SYMBOL, bid DOUBLE, ask DOUBLE, timestamp TIMESTAMP) timestamp(timestamp)");

            try (
                    TableWriter orders = getWriter("orders");
                    TableWriter quotes = getWriter("quotes")
            ) {
                TableWriter.Row rOrders;
                TableWriter.Row rQuotes;

                // quote googl @ 10:00:02
                rQuotes = quotes.newRow(MicrosFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000000Z"));
                rQuotes.putSym(0, "googl");
                rQuotes.putDouble(1, 100.2);
                rQuotes.putDouble(2, 100.3);
                rQuotes.append();

                // quote msft @ 10.00.02.000001
                rQuotes = quotes.newRow(MicrosFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000001Z"));
                rQuotes.putSym(0, "msft");
                rQuotes.putDouble(1, 185.9);
                rQuotes.putDouble(2, 187.3);
                rQuotes.append();

                // quote msft @ 10.00.02.000002
                rQuotes = quotes.newRow(MicrosFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000002Z"));
                rQuotes.putSym(0, "msft");
                rQuotes.putDouble(1, 186.1);
                rQuotes.putDouble(2, 187.8);
                rQuotes.append();

                // order googl @ 10.00.03
                rOrders = orders.newRow(MicrosFormatUtils.parseUTCTimestamp("2018-11-02T10:00:03.000000Z"));
                rOrders.putSym(0, "googl");
                rOrders.putDouble(1, 2000);
                rOrders.putByte(2, (byte) '1');
                rOrders.append();

                // quote msft @ 10.00.03.000001
                rQuotes = quotes.newRow(MicrosFormatUtils.parseUTCTimestamp("2018-11-02T10:00:02.000002Z"));
                rQuotes.putSym(0, "msft");
                rQuotes.putDouble(1, 183.4);
                rQuotes.putDouble(2, 185.9);
                rQuotes.append();

                // order msft @ 10:00:04
                rOrders = orders.newRow(MicrosFormatUtils.parseUTCTimestamp("2018-11-02T10:00:04.000000Z"));
                rOrders.putSym(0, "msft");
                rOrders.putDouble(1, 150);
                rOrders.putByte(2, (byte) '1');
                rOrders.append();

                // order googl @ 10.00.05
                rOrders = orders.newRow(MicrosFormatUtils.parseUTCTimestamp("2018-11-02T10:00:05.000000Z"));
                rOrders.putSym(0, "googl");
                rOrders.putDouble(1, 3000);
                rOrders.putByte(2, (byte) '2');
                rOrders.append();

                quotes.commit();
                orders.commit();
            }

            assertQuery("select * from orders splice join quotes on(sym)")
                    .noLeakCheck()
                    .ddl(null)
                    .noRandomAccess()
                    .returns("""
                            sym\tamount\tside\ttimestamp\tsym1\tbid\task\ttimestamp1
                            \tnull\t0\t\tgoogl\t100.2\t100.3\t2018-11-02T10:00:02.000000Z
                            \tnull\t0\t\tmsft\t185.9\t187.3\t2018-11-02T10:00:02.000001Z
                            \tnull\t0\t\tmsft\t186.1\t187.8\t2018-11-02T10:00:02.000002Z
                            \tnull\t0\t\tmsft\t183.4\t185.9\t2018-11-02T10:00:02.000002Z
                            googl\t2000.0\t49\t2018-11-02T10:00:03.000000Z\tgoogl\t100.2\t100.3\t2018-11-02T10:00:02.000000Z
                            msft\t150.0\t49\t2018-11-02T10:00:04.000000Z\tmsft\t183.4\t185.9\t2018-11-02T10:00:02.000002Z
                            googl\t3000.0\t50\t2018-11-02T10:00:05.000000Z\tgoogl\t100.2\t100.3\t2018-11-02T10:00:02.000000Z
                            """);
        });
    }

    @Test
    public void testSpliceJoinAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x splice join y on y.sym2 = x.sym";

            final String expected = """
                    i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                    null\t\tnull\t0.032\t\t2018-01-01T00:02:00.000000Z
                    null\t\tnull\t0.043000000000000003\t\t2018-01-01T00:04:00.000000Z
                    null\t\tnull\t0.986\t\t2018-01-01T00:06:00.000000Z
                    null\t\tnull\t0.139\t\t2018-01-01T00:08:00.000000Z
                    null\t\tnull\t0.152\t\t2018-01-01T00:10:00.000000Z
                    1\tmsft\t50.938\t0.043000000000000003\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:04:00.000000Z
                    null\t\tnull\t0.707\t\t2018-01-01T00:14:00.000000Z
                    null\t\tnull\t0.937\t\t2018-01-01T00:16:00.000000Z
                    null\t\tnull\t0.42\t\t2018-01-01T00:18:00.000000Z
                    null\t\tnull\t0.8300000000000001\t\t2018-01-01T00:20:00.000000Z
                    null\t\tnull\t0.392\t\t2018-01-01T00:22:00.000000Z
                    2\tgoogl\t42.281\t0.937\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:16:00.000000Z
                    null\t\tnull\t0.834\t\t2018-01-01T00:26:00.000000Z
                    null\t\tnull\t0.47900000000000004\t\t2018-01-01T00:28:00.000000Z
                    2\tgoogl\t42.281\t0.911\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:30:00.000000Z
                    null\t\tnull\t0.9410000000000001\t\t2018-01-01T00:32:00.000000Z
                    null\t\tnull\t0.736\t\t2018-01-01T00:34:00.000000Z
                    3\tgoogl\t17.371\t0.42\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:18:00.000000Z
                    null\t\tnull\t0.437\t\t2018-01-01T00:38:00.000000Z
                    null\t\tnull\t0.109\t\t2018-01-01T00:40:00.000000Z
                    null\t\tnull\t0.84\t\t2018-01-01T00:42:00.000000Z
                    null\t\tnull\t0.252\t\t2018-01-01T00:44:00.000000Z
                    null\t\tnull\t0.54\t\t2018-01-01T00:46:00.000000Z
                    4\tibm\t14.831\t0.252\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:44:00.000000Z
                    null\t\tnull\t0.621\t\t2018-01-01T00:50:00.000000Z
                    null\t\tnull\t0.963\t\t2018-01-01T00:52:00.000000Z
                    null\t\tnull\t0.359\t\t2018-01-01T00:54:00.000000Z
                    null\t\tnull\t0.383\t\t2018-01-01T00:56:00.000000Z
                    null\t\tnull\t0.009000000000000001\t\t2018-01-01T00:58:00.000000Z
                    5\tgoogl\t86.772\t0.42\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:18:00.000000Z
                    6\tmsft\t29.659\t0.08700000000000001\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:00:00.000000Z
                    7\tgoogl\t7.594\t0.911\t2018-01-01T01:24:00.000000Z\t2018-01-01T00:30:00.000000Z
                    8\tibm\t54.253\t0.383\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:56:00.000000Z
                    9\tmsft\t62.26\t0.08700000000000001\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:00:00.000000Z
                    10\tmsft\t50.908\t0.08700000000000001\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:00:00.000000Z
                    """;

            execute(
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
            execute(
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

            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);

            execute(
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
            execute(
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

            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                            null\t\tnull\t0.032\t\t2018-01-01T00:02:00.000000Z
                            null\t\tnull\t0.043000000000000003\t\t2018-01-01T00:04:00.000000Z
                            null\t\tnull\t0.986\t\t2018-01-01T00:06:00.000000Z
                            null\t\tnull\t0.139\t\t2018-01-01T00:08:00.000000Z
                            null\t\tnull\t0.152\t\t2018-01-01T00:10:00.000000Z
                            1\tmsft\t50.938\t0.043000000000000003\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:04:00.000000Z
                            null\t\tnull\t0.707\t\t2018-01-01T00:14:00.000000Z
                            null\t\tnull\t0.937\t\t2018-01-01T00:16:00.000000Z
                            null\t\tnull\t0.42\t\t2018-01-01T00:18:00.000000Z
                            null\t\tnull\t0.8300000000000001\t\t2018-01-01T00:20:00.000000Z
                            null\t\tnull\t0.392\t\t2018-01-01T00:22:00.000000Z
                            2\tgoogl\t42.281\t0.937\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:16:00.000000Z
                            null\t\tnull\t0.834\t\t2018-01-01T00:26:00.000000Z
                            null\t\tnull\t0.47900000000000004\t\t2018-01-01T00:28:00.000000Z
                            2\tgoogl\t42.281\t0.911\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:30:00.000000Z
                            null\t\tnull\t0.9410000000000001\t\t2018-01-01T00:32:00.000000Z
                            null\t\tnull\t0.736\t\t2018-01-01T00:34:00.000000Z
                            3\tgoogl\t17.371\t0.42\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:18:00.000000Z
                            null\t\tnull\t0.437\t\t2018-01-01T00:38:00.000000Z
                            null\t\tnull\t0.109\t\t2018-01-01T00:40:00.000000Z
                            null\t\tnull\t0.84\t\t2018-01-01T00:42:00.000000Z
                            null\t\tnull\t0.252\t\t2018-01-01T00:44:00.000000Z
                            null\t\tnull\t0.54\t\t2018-01-01T00:46:00.000000Z
                            4\tibm\t14.831\t0.252\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:44:00.000000Z
                            null\t\tnull\t0.621\t\t2018-01-01T00:50:00.000000Z
                            null\t\tnull\t0.963\t\t2018-01-01T00:52:00.000000Z
                            null\t\tnull\t0.359\t\t2018-01-01T00:54:00.000000Z
                            null\t\tnull\t0.383\t\t2018-01-01T00:56:00.000000Z
                            null\t\tnull\t0.009000000000000001\t\t2018-01-01T00:58:00.000000Z
                            5\tgoogl\t86.772\t0.42\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:18:00.000000Z
                            3\tgoogl\t17.371\t0.687\t2018-01-01T00:36:00.000000Z\t2018-01-01T01:02:00.000000Z
                            null\t\tnull\t0.215\t\t2018-01-01T01:04:00.000000Z
                            1\tmsft\t50.938\t0.061\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:06:00.000000Z
                            null\t\tnull\t0.554\t\t2018-01-01T01:08:00.000000Z
                            3\tgoogl\t17.371\t0.332\t2018-01-01T00:36:00.000000Z\t2018-01-01T01:10:00.000000Z
                            6\tmsft\t29.659\t0.08700000000000001\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:00:00.000000Z
                            5\tgoogl\t86.772\t0.222\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:14:00.000000Z
                            1\tmsft\t50.938\t0.305\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:16:00.000000Z
                            null\t\tnull\t0.403\t\t2018-01-01T01:18:00.000000Z
                            1\tmsft\t50.938\t0.323\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:20:00.000000Z
                            1\tmsft\t50.938\t0.297\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:22:00.000000Z
                            7\tgoogl\t7.594\t0.332\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:10:00.000000Z
                            5\tgoogl\t86.772\t0.372\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:26:00.000000Z
                            1\tmsft\t50.938\t0.446\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:28:00.000000Z
                            4\tibm\t14.831\t0.231\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:30:00.000000Z
                            5\tgoogl\t86.772\t0.23900000000000002\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:32:00.000000Z
                            5\tgoogl\t86.772\t0.067\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:34:00.000000Z
                            8\tibm\t54.253\t0.47700000000000004\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:36:00.000000Z
                            4\tibm\t14.831\t0.877\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:38:00.000000Z
                            6\tmsft\t29.659\t0.432\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:40:00.000000Z
                            4\tibm\t14.831\t0.67\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:42:00.000000Z
                            5\tgoogl\t86.772\t0.264\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:44:00.000000Z
                            5\tgoogl\t86.772\t0.782\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:46:00.000000Z
                            9\tmsft\t62.26\t0.724\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:48:00.000000Z
                            4\tibm\t14.831\t0.252\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:50:00.000000Z
                            4\tibm\t14.831\t0.6960000000000001\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:52:00.000000Z
                            4\tibm\t14.831\t0.904\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:54:00.000000Z
                            4\tibm\t14.831\t0.732\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:56:00.000000Z
                            5\tgoogl\t86.772\t0.26\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:58:00.000000Z
                            10\tmsft\t50.908\t0.209\t2018-01-01T02:00:00.000000Z\t2018-01-01T02:00:00.000000Z
                            11\tgoogl\t27.493000000000002\t0.26\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:58:00.000000Z
                            12\tgoogl\t39.244\t0.26\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:58:00.000000Z
                            13\tgoogl\t56.985\t0.26\t2018-01-01T02:36:00.000000Z\t2018-01-01T01:58:00.000000Z
                            14\tmsft\t49.758\t0.209\t2018-01-01T02:48:00.000000Z\t2018-01-01T02:00:00.000000Z
                            15\tmsft\t49.108000000000004\t0.209\t2018-01-01T03:00:00.000000Z\t2018-01-01T02:00:00.000000Z
                            16\tmsft\t0.132\t0.209\t2018-01-01T03:12:00.000000Z\t2018-01-01T02:00:00.000000Z
                            17\tibm\t80.48\t0.732\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:56:00.000000Z
                            18\tmsft\t57.556000000000004\t0.209\t2018-01-01T03:36:00.000000Z\t2018-01-01T02:00:00.000000Z
                            19\tgoogl\t34.25\t0.26\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:58:00.000000Z
                            20\tgoogl\t2.6750000000000003\t0.26\t2018-01-01T04:00:00.000000Z\t2018-01-01T01:58:00.000000Z
                            """);
        });
    }

    @Test
    public void testSpliceJoinAsMasterOfSecondJoin() throws Exception {
        // A splice join as the left (master) side of a following join used to leak the root
        // table alias into the second join's metadata, crashing on a fully qualified column
        // name. The splice result already carries qualified names, so the alias must be cleared.
        assertMemoryLeak(() -> {
            execute("create table m (k symbol, mv int, ts timestamp) timestamp(ts) partition by day");
            execute("create table s (k symbol, sv int, ts timestamp) timestamp(ts) partition by day");
            execute("create table r (k symbol, rv int, ts timestamp) timestamp(ts) partition by day");
            execute("insert into m values ('a', 1, '2020-01-01T00:00:00.000000Z')");
            execute("insert into s values ('a', 2, '2020-01-01T00:00:00.000000Z')");
            execute("insert into r values ('a', 3, '2020-01-01T00:00:00.000000Z'), ('b', 4, '2020-01-01T00:00:01.000000Z')");

            assertQuery("select m.k, s.sv, r.rv from m splice join s on m.k = s.k right join r on r.k = m.k")
                    .noLeakCheck()
                    .ddl(null)
                    .noRandomAccess()
                    .returns("k\tsv\trv\na\t2\t3\n\tnull\t4\n");
            assertQuery("select m.k, s.sv, r.rv from m splice join s on m.k = s.k full join r on r.k = m.k")
                    .noLeakCheck()
                    .ddl(null)
                    .noRandomAccess()
                    .returns("k\tsv\trv\na\t2\t3\n\tnull\t4\n");
            assertQuery("select m.k, s.sv, r.rv from m splice join s on m.k = s.k inner join r on r.k = m.k")
                    .noLeakCheck()
                    .ddl(null)
                    .noRandomAccess()
                    .returns("k\tsv\trv\na\t2\t3\n");
        });
    }

    @Test
    public void testSpliceJoinFailsBecauseSubqueryDoesntSupportRandomAccess() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE trade (
                      ts TIMESTAMP,
                      instrument SYMBOL,
                      price DOUBLE,
                      qty DOUBLE
                    ) timestamp (ts) PARTITION BY MONTH""", sqlExecutionContext);

            assertFailure("""
                    SELECT *
                    FROM\s
                    (
                      SELECT ts, SUM(price * qty) / SUM(qty) vwap
                      FROM trade
                      WHERE instrument = 'A'
                      SAMPLE by 5m ALIGN TO FIRST OBSERVATION
                    )\s
                    SPLICE JOIN trade\s""", "left side of splice join doesn't support random access", 146);

            assertFailure("""
                    SELECT *
                    FROM trade \
                    SPLICE JOIN \
                    (
                      SELECT ts, SUM(price * qty) / SUM(qty) vwap
                      FROM trade
                      WHERE instrument = 'A'
                      SAMPLE BY 5m ALIGN TO FIRST OBSERVATION
                    )\s
                    """, "right side of splice join doesn't support random access", 20);
        });
    }

    @Test
    public void testSpliceJoinFailsInFullFatMode() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE trade (
                      ts TIMESTAMP,
                      instrument SYMBOL,
                      price DOUBLE,
                      qty DOUBLE
                    ) timestamp (ts) PARTITION BY MONTH""");

            assertQuery("SELECT *" +
                    "FROM trade t1 " +
                    "SPLICE JOIN trade t2")
                    .fullFatJoins()
                    .noLeakCheck()
                    .fails(22, "splice join doesn't support full fat mode");
        });
    }

    @Test
    public void testSpliceJoinFoldedFalseMasterFilterProducesNoRows() throws Exception {
        // A master-only WHERE on a SPLICE join that folds to FALSE: ((a.c0 + null))::TIMESTAMP
        // is NULL for every row (AddIntFunctionFactory short-circuits the null), so the
        // comparison is NULL, hence FALSE, throughout. SPLICE NULL-extends the master, so the
        // predicate stays a post-join Filter rather than being pushed into the master
        // sub-query (which would empty the master and pair each slave row with a NULL master,
        // leaking one row per slave). A WHERE that is always FALSE therefore produces no rows,
        // and the literal and bind-variable forms must agree. The literal folds to a post-join
        // "Filter filter: false" over a full Splice Join (no Empty table substitution).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (c0 SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE t0 (c0 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t1 VALUES (1::SHORT, '2024-01-01T00:00:00.000000Z'), " +
                    "(2::SHORT, '2024-01-02T00:00:00.000000Z'), " +
                    "(3::SHORT, '2024-01-03T00:00:00.000000Z')");
            execute("INSERT INTO t0 VALUES (10, '2024-01-01T00:00:00.000000Z'), " +
                    "(20, '2024-01-02T00:00:00.000000Z'), " +
                    "(30, '2024-01-03T00:00:00.000000Z')");
            drainWalQueue();

            final String expected = "e0\te1\n";
            final String literalSql = "SELECT (a.c0)::STRING AS e0, true AS e1 " +
                    "FROM t1 a SPLICE JOIN t0 b " +
                    "WHERE ((a.c0 + null))::TIMESTAMP < '2024-03-06T20:54:00.000000Z'::TIMESTAMP";
            bindVariableService.clear();
            assertQuery(literalSql).noLeakCheck().noRandomAccess().withPlanContaining("Filter filter: false").returns(expected);

            // Bind-variable form evaluates the same NULL/FALSE predicate per row and must agree.
            bindVariableService.clear();
            bindVariableService.setStr("b1", "2024-03-06T20:54:00.000000Z");
            assertQuery("SELECT (a.c0)::STRING AS e0, true AS e1 " +
                    "FROM t1 a SPLICE JOIN t0 b " +
                    "WHERE ((a.c0 + null))::TIMESTAMP < :b1::TIMESTAMP")
                    .noLeakCheck().noRandomAccess().returns(expected);
        });
    }

    @Test
    public void testSpliceJoinIndexedSymbolMasterWithOrderByPreservesTimestamp() throws Exception {
        // Regression for a query-fuzzer divergence: a SPLICE JOIN whose master
        // table has an indexed SYMBOL column and an interval WHERE on ts, with
        // the outer query ordering by that indexed symbol, used to compile the
        // master as SortedSymbolIndexRecordCursorFactory. That factory emits
        // rows in symbol order and zeroes the timestamp index, so the SPLICE
        // join validation either threw "left side of time series join has no
        // timestamp" or, with the timestamp restored, would have fed
        // sym-ordered input into a merge that assumes ts order. The codegen
        // now skips the symbol-index sort path when the parent join requires
        // a timestamp on the master.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x_idx (sym SYMBOL INDEX, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE y_tab (sym SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x_idx SELECT 'a', x::DOUBLE, ('2024-01-01T00:00:00'::TIMESTAMP + x * 1_000_000) FROM long_sequence(3)");
            execute("INSERT INTO y_tab SELECT 'b', x::DOUBLE, ('2024-01-01T00:00:00'::TIMESTAMP + x * 1_000_000) FROM long_sequence(3)");
            assertQuery("SELECT b.val AS e0, b.sym AS e1, a.sym AS e2 FROM x_idx a SPLICE JOIN y_tab b WHERE a.ts IN '2024-01-01' ORDER BY 3")
                    .noLeakCheck()
                    .ddl(null)
                    .returns("""
                            e0\te1\te2
                            1.0\tb\ta
                            2.0\tb\ta
                            3.0\tb\ta
                            """);
        });
    }

    @Test
    public void testSpliceJoinLeftTimestampDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            execute("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertQuery("select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from (x order by timestamp desc) x splice join y on y.sym2 = x.sym")
                    .noLeakCheck()
                    .fails(93, "left");
        });
    }

    @Test
    public void testSpliceJoinMasterFilterProjectsSlaveColumn() throws Exception {
        // Regression: the transitive slave-const prune that is result-neutral for RIGHT/FULL OUTER
        // set joins is NOT neutral for SPLICE. SPLICE is a temporal prevailing join, so removing
        // slave rows of other keys (pushing s.k = 'A' into the slave) shifts which slave row
        // prevails at a master timestamp. The master-side literal predicate stays a post-join filter,
        // but the const must NOT be pushed into the slave; addTransitiveFilters skips SPLICE. The
        // bug only surfaces when a SLAVE column is projected: testSpliceJoinMasterFilterStaysPostJoin
        // projects master columns only, hiding the diverging slave value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (k SYMBOL, mv INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES ('A',1,1),('B',2,2),('A',3,5)");
            execute("CREATE TABLE s (k SYMBOL, sv INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // The B-key slave rows (99@1, 88@4) prevail at master timestamps for key A; pruning them
            // would change the prevailing slave value, so the literal and bind forms would diverge.
            execute("INSERT INTO s VALUES ('A',10,0),('B',99,1),('A',20,3),('B',88,4),('A',30,6)");

            final String expected = """
                    k\tmv\tsv
                    A\t1\tnull
                    A\t1\t20
                    A\t3\t20
                    A\t3\t30
                    """;

            // Literal form: the predicate stays a post-join Filter over a full slave scan (no
            // filter: k='A' pushed into the slave sub-query).
            bindVariableService.clear();
            assertQuery("SELECT m.k, m.mv, s.sv FROM m SPLICE JOIN s ON m.k = s.k WHERE m.k = 'A' ORDER BY m.mv, s.sv")
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: m.k='A'")
                    .returns(expected);

            // Bind-variable form must produce the identical result.
            bindVariableService.clear();
            bindVariableService.setStr("v", "A");
            assertQuery("SELECT m.k, m.mv, s.sv FROM m SPLICE JOIN s ON m.k = s.k WHERE m.k = :v::SYMBOL ORDER BY m.mv, s.sv")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testSpliceJoinMasterFilterStaysPostJoin() throws Exception {
        // Regression for a query-fuzzer bind-variable divergence. A WHERE predicate that
        // references only the master (left) table of a SPLICE join used to be pushed into
        // the master sub-query. SPLICE is a full outer temporal join, so it emits rows in
        // which the master columns are all NULL (slave-only timestamps); pushing the
        // predicate left those NULL-master rows unfiltered, and for the literal form it was
        // also propagated to the slave through the join key, so the literal and
        // bind-variable forms of the same query diverged (here 3 vs 4 rows). The predicate
        // now stays a post-join filter, so both forms agree and NULL-master rows are removed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (sym SYMBOL, c1 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES ('s2', 100, 2), ('s2', 200, 4)");
            execute("CREATE TABLE s (sym SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // s2@1 leads the first master row (would be a NULL-master splice row); x@3 never
            // has a master match (another NULL-master splice row). Both must be filtered out.
            execute("INSERT INTO s VALUES ('s2', 10, 1), ('x', 50, 3)");

            final String query = "SELECT a.sym AS e0, a.c1 AS e1 FROM m a SPLICE JOIN s b ON (sym) WHERE a.sym = 's2' ORDER BY e1";
            final String expected = """
                    e0\te1
                    s2\t100
                    s2\t200
                    """;

            // Literal form: correct result and a post-join Filter over a full master scan
            // (no predicate pushed into the master sub-query).
            bindVariableService.clear();
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: a.sym='s2'")
                    .returns(expected);

            // Bind-variable form must produce the identical result under the full assertion battery.
            bindVariableService.clear();
            bindVariableService.setStr("sym", "s2");
            assertQuery("SELECT a.sym AS e0, a.c1 AS e1 FROM m a SPLICE JOIN s b ON (sym) WHERE a.sym = :sym::SYMBOL ORDER BY e1")
                    .noLeakCheck().returns(expected);
        });
    }

    @Test
    public void testSpliceJoinNoLeftTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10))");
            execute("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertQuery("select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x splice join y on y.sym2 = x.sym")
                    .noLeakCheck()
                    .fails(65, "left");
        });
    }

    @Test
    public void testSpliceJoinNoRightTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            execute("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30))");
            assertQuery("select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x splice join y on y.sym2 = x.sym")
                    .noLeakCheck()
                    .fails(65, "right");
        });
    }

    @Test
    public void testSpliceJoinNoStrings() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x splice join y on y.sym2 = x.sym";

            final String expected = """
                    i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                    null\t\tnull\t0.032\t\t2018-01-01T00:02:00.000000Z
                    null\t\tnull\t0.113\t\t2018-01-01T00:04:00.000000Z
                    null\t\tnull\t0.11\t\t2018-01-01T00:06:00.000000Z
                    null\t\tnull\t0.21\t\t2018-01-01T00:08:00.000000Z
                    null\t\tnull\t0.934\t\t2018-01-01T00:10:00.000000Z
                    1\tmsft\t50.938\t0.523\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:12:00.000000Z
                    null\t\tnull\t0.846\t\t2018-01-01T00:14:00.000000Z
                    null\t\tnull\t0.605\t\t2018-01-01T00:16:00.000000Z
                    null\t\tnull\t0.215\t\t2018-01-01T00:18:00.000000Z
                    null\t\tnull\t0.223\t\t2018-01-01T00:20:00.000000Z
                    null\t\tnull\t0.781\t\t2018-01-01T00:22:00.000000Z
                    2\tgoogl\t42.281\t0.605\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:16:00.000000Z
                    null\t\tnull\t0.108\t\t2018-01-01T00:26:00.000000Z
                    null\t\tnull\t0.91\t\t2018-01-01T00:28:00.000000Z
                    2\tgoogl\t42.281\t0.373\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:30:00.000000Z
                    null\t\tnull\t0.024\t\t2018-01-01T00:32:00.000000Z
                    2\tgoogl\t42.281\t0.301\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:34:00.000000Z
                    3\tgoogl\t17.371\t0.915\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                    2\tgoogl\t42.281\t0.419\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:38:00.000000Z
                    null\t\tnull\t0.864\t\t2018-01-01T00:40:00.000000Z
                    null\t\tnull\t0.404\t\t2018-01-01T00:42:00.000000Z
                    null\t\tnull\t0.982\t\t2018-01-01T00:44:00.000000Z
                    null\t\tnull\t0.586\t\t2018-01-01T00:46:00.000000Z
                    4\tibm\t14.831\t0.91\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:28:00.000000Z
                    null\t\tnull\t0.539\t\t2018-01-01T00:50:00.000000Z
                    3\tgoogl\t17.371\t0.989\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:52:00.000000Z
                    null\t\tnull\t0.537\t\t2018-01-01T00:54:00.000000Z
                    3\tgoogl\t17.371\t0.5710000000000001\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:56:00.000000Z
                    3\tgoogl\t17.371\t0.76\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:58:00.000000Z
                    5\tgoogl\t86.772\t0.092\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z
                    6\tmsft\t29.659\t0.537\t2018-01-01T01:12:00.000000Z\t2018-01-01T00:54:00.000000Z
                    7\tgoogl\t7.594\t0.092\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:00:00.000000Z
                    8\tibm\t54.253\t0.404\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:42:00.000000Z
                    9\tmsft\t62.26\t0.537\t2018-01-01T01:48:00.000000Z\t2018-01-01T00:54:00.000000Z
                    10\tmsft\t50.908\t0.537\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:54:00.000000Z
                    """;

            execute(
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
            execute(
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

            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);

            execute(
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
            execute(
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

            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                            null\t\tnull\t0.032\t\t2018-01-01T00:02:00.000000Z
                            null\t\tnull\t0.113\t\t2018-01-01T00:04:00.000000Z
                            null\t\tnull\t0.11\t\t2018-01-01T00:06:00.000000Z
                            null\t\tnull\t0.21\t\t2018-01-01T00:08:00.000000Z
                            null\t\tnull\t0.934\t\t2018-01-01T00:10:00.000000Z
                            1\tmsft\t50.938\t0.523\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:12:00.000000Z
                            null\t\tnull\t0.846\t\t2018-01-01T00:14:00.000000Z
                            null\t\tnull\t0.605\t\t2018-01-01T00:16:00.000000Z
                            null\t\tnull\t0.215\t\t2018-01-01T00:18:00.000000Z
                            null\t\tnull\t0.223\t\t2018-01-01T00:20:00.000000Z
                            null\t\tnull\t0.781\t\t2018-01-01T00:22:00.000000Z
                            2\tgoogl\t42.281\t0.605\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:16:00.000000Z
                            null\t\tnull\t0.108\t\t2018-01-01T00:26:00.000000Z
                            null\t\tnull\t0.91\t\t2018-01-01T00:28:00.000000Z
                            2\tgoogl\t42.281\t0.373\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:30:00.000000Z
                            null\t\tnull\t0.024\t\t2018-01-01T00:32:00.000000Z
                            2\tgoogl\t42.281\t0.301\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:34:00.000000Z
                            3\tgoogl\t17.371\t0.915\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                            2\tgoogl\t42.281\t0.419\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:38:00.000000Z
                            null\t\tnull\t0.864\t\t2018-01-01T00:40:00.000000Z
                            null\t\tnull\t0.404\t\t2018-01-01T00:42:00.000000Z
                            null\t\tnull\t0.982\t\t2018-01-01T00:44:00.000000Z
                            null\t\tnull\t0.586\t\t2018-01-01T00:46:00.000000Z
                            4\tibm\t14.831\t0.91\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:28:00.000000Z
                            null\t\tnull\t0.539\t\t2018-01-01T00:50:00.000000Z
                            3\tgoogl\t17.371\t0.989\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:52:00.000000Z
                            null\t\tnull\t0.537\t\t2018-01-01T00:54:00.000000Z
                            3\tgoogl\t17.371\t0.5710000000000001\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:56:00.000000Z
                            3\tgoogl\t17.371\t0.76\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:58:00.000000Z
                            5\tgoogl\t86.772\t0.092\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z
                            null\t\tnull\t0.252\t\t2018-01-01T01:02:00.000000Z
                            3\tgoogl\t17.371\t0.122\t2018-01-01T00:36:00.000000Z\t2018-01-01T01:04:00.000000Z
                            1\tmsft\t50.938\t0.962\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:06:00.000000Z
                            1\tmsft\t50.938\t0.098\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:08:00.000000Z
                            null\t\tnull\t0.705\t\t2018-01-01T01:10:00.000000Z
                            6\tmsft\t29.659\t0.962\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:06:00.000000Z
                            null\t\tnull\t0.489\t\t2018-01-01T01:14:00.000000Z
                            1\tmsft\t50.938\t0.105\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:16:00.000000Z
                            null\t\tnull\t0.892\t\t2018-01-01T01:18:00.000000Z
                            null\t\tnull\t0.74\t\t2018-01-01T01:20:00.000000Z
                            5\tgoogl\t86.772\t0.38\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:22:00.000000Z
                            7\tgoogl\t7.594\t0.036000000000000004\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:24:00.000000Z
                            5\tgoogl\t86.772\t0.395\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:26:00.000000Z
                            5\tgoogl\t86.772\t0.882\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:28:00.000000Z
                            1\tmsft\t50.938\t0.301\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:30:00.000000Z
                            1\tmsft\t50.938\t0.032\t2018-01-01T00:12:00.000000Z\t2018-01-01T01:32:00.000000Z
                            5\tgoogl\t86.772\t0.308\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:34:00.000000Z
                            8\tibm\t54.253\t0.892\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:18:00.000000Z
                            5\tgoogl\t86.772\t0.667\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:38:00.000000Z
                            4\tibm\t14.831\t0.594\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:40:00.000000Z
                            5\tgoogl\t86.772\t0.08700000000000001\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:42:00.000000Z
                            5\tgoogl\t86.772\t0.855\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:44:00.000000Z
                            5\tgoogl\t86.772\t0.786\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:46:00.000000Z
                            9\tmsft\t62.26\t0.301\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:30:00.000000Z
                            4\tibm\t14.831\t0.644\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:50:00.000000Z
                            4\tibm\t14.831\t0.55\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:52:00.000000Z
                            9\tmsft\t62.26\t0.434\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:54:00.000000Z
                            4\tibm\t14.831\t0.388\t2018-01-01T00:48:00.000000Z\t2018-01-01T01:56:00.000000Z
                            9\tmsft\t62.26\t0.912\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:58:00.000000Z
                            10\tmsft\t50.908\t0.434\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:54:00.000000Z
                            11\tmsft\t25.604\t0.912\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:58:00.000000Z
                            12\tgoogl\t89.22\t0.148\t2018-01-01T02:24:00.000000Z\t2018-01-01T02:00:00.000000Z
                            13\tgoogl\t64.536\t0.148\t2018-01-01T02:36:00.000000Z\t2018-01-01T02:00:00.000000Z
                            14\tibm\t33.0\t0.388\t2018-01-01T02:48:00.000000Z\t2018-01-01T01:56:00.000000Z
                            15\tmsft\t67.285\t0.912\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:58:00.000000Z
                            16\tgoogl\t17.31\t0.148\t2018-01-01T03:12:00.000000Z\t2018-01-01T02:00:00.000000Z
                            17\tibm\t23.957\t0.388\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:56:00.000000Z
                            18\tibm\t60.678000000000004\t0.388\t2018-01-01T03:36:00.000000Z\t2018-01-01T01:56:00.000000Z
                            19\tmsft\t4.727\t0.912\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:58:00.000000Z
                            20\tgoogl\t26.222\t0.148\t2018-01-01T04:00:00.000000Z\t2018-01-01T02:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testSpliceJoinRecordNoLeaks() throws Exception {
        testJoinForCursorLeaks("with crj as (select x, ts from xx latest by x) select xx.x from xx splice join crj on xx.x = crj.x ", false);
    }

    @Test
    public void testSpliceJoinRightTimestampDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            execute("create table y as (select cast(x as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");
            assertQuery("select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x splice join (y order by timestamp desc) y on y.sym2 = x.sym")
                    .noLeakCheck()
                    .fails(65, "right");
        });
    }

    @Test
    public void testSpliceJoinWithComplexConditionFails1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            execute("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 splice join t2 on l1=l2+5", "unsupported SPLICE join expression [expr='l1 = l2 + 5']", 37);
        });
    }

    @Test
    public void testSpliceJoinWithComplexConditionFails2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            execute("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 splice join t2 on l1>l2", "unsupported SPLICE join expression [expr='l1 > l2']", 37);
        });
    }

    @Test
    public void testSpliceJoinWithComplexConditionFails3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            execute("create table t2 (l2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 splice join t2 on l1=abs(l2)", "unsupported SPLICE join expression [expr='l1 = abs(l2)']", 37);
        });
    }

    @Test
    public void testSpliceJoinWithComplexConditionFails4() throws Exception {
        // Same-table equality on the slave side (l2 = m2) is now routed to the
        // outer-join expression clause and surfaced as an unsupported-expression
        // error, instead of being silently dropped.
        assertMemoryLeak(() -> {
            execute("create table t1 (l1 long, ts1 timestamp) timestamp(ts1) partition by year");
            execute("create table t2 (l2 long, m2 long, ts2 timestamp) timestamp(ts2) partition by year");

            assertFailure("select * from t1 splice join t2 on l1=l2 and l2=m2", "unsupported SPLICE join expression [expr='l2 = m2']", 47);
        });
    }

    @Test
    public void testSpliceOfJoinAliasDuplication() throws Exception {
        assertMemoryLeak(() -> {
            // ASKS
            execute("create table asks(ask int, ts timestamp) timestamp(ts) partition by none");
            execute("insert into asks values(100, 0)");
            execute("insert into asks values(101, 2);");
            execute("insert into asks values(102, 4);");

            // BIDS
            execute("create table bids(bid int, ts timestamp) timestamp(ts) partition by none");
            execute("insert into bids values(101, 1);");
            execute("insert into bids values(102, 3);");
            execute("insert into bids values(103, 5);");

            String query =
                    """
                            select\s
                                b.timebid timebid,
                                a.timeask timeask,\s
                                b.b b,\s
                                a.a a
                            from (select b.bid b, b.ts timebid from bids b) b\s
                                splice join
                            (select a.ask a, a.ts timeask from asks a) a
                            WHERE (b.timebid != a.timeask);""";

            String expected = """
                    timebid\ttimeask\tb\ta
                    \t1970-01-01T00:00:00.000000Z\tnull\t100
                    1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000000Z\t101\t100
                    1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000002Z\t101\t101
                    1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000002Z\t102\t101
                    1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000004Z\t102\t102
                    1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000004Z\t103\t102
                    """;

            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testSpliceOperatorMasterFilterStaysPostJoin() throws Exception {
        // SPLICE variant of testOperatorMasterFilterStaysPostJoin: assignFilters routes a non-folded
        // operator predicate (m.c1 < 100) on a NULL-extending master to a post-join filter; the only
        // existing SPLICE master-filter test for a live operator is the folded-FALSE case. The master
        // row (c1=50) passes the filter, so pushing the predicate into the master leaves it unchanged,
        // but it also strips the post-join filter, leaking the pre-master NULL-master splice row that
        // c1<100 must drop (NULL<100 is NULL/false). Held post-join, only the two prevailing-master
        // rows survive.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (c1 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES (50, 2)");
            execute("CREATE TABLE s (sv INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // s@1 leads the single master row (a NULL-master splice row); s@3 trails it (prevailing
            // master 50). The leading row must be filtered out, the two prevailing rows kept.
            execute("INSERT INTO s VALUES (10, 1), (20, 3)");

            assertQuery("SELECT m.c1 FROM m SPLICE JOIN s WHERE m.c1 < 100")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Filter filter: m.c1<100")
                    .returns("c1\n50\n50\n");
        });
    }

    @Test
    public void testSpliceSelfJoinSymbolAndStringKeyIndexCollision() throws Exception {
        // A self-join compares a.sym = b.sym as raw symbol keys and does not set its bits, but
        // processJoinContext() sets the bits for a.side = b.side_str on both sides of one shared BitSet.
        // The projection puts b.side_str at slave column 1, so the stray bit makes the master sink write
        // a.sym (master column 1) as a string while the slave sink writes b.sym as an int.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE book (ts TIMESTAMP, sym SYMBOL, side SYMBOL, side_str STRING, qty LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO book VALUES
                        ('2024-01-01T00:00:01.000000Z', 'AAPL', 'buy', 'buy', 1),
                        ('2024-01-01T00:00:02.000000Z', 'MSFT', 'sell', 'sell', 2),
                        ('2024-01-01T00:00:03.000000Z', 'AAPL', 'sell', 'sell', 3)""");
            // every row matches itself, so each timestamp yields one row with both sides
            assertQuery("SELECT a.ts, a.sym, b.qty FROM book a SPLICE JOIN book b ON a.sym = b.sym AND a.side = b.side_str")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Splice Join")
                    .returns("""
                            ts\tsym\tqty
                            2024-01-01T00:00:01.000000Z\tAAPL\t1
                            2024-01-01T00:00:02.000000Z\tMSFT\t2
                            2024-01-01T00:00:03.000000Z\tAAPL\t3
                            """);
        });
    }

    @Test
    public void testStackedNullingJoinsMasterFilterStaysPostJoin() throws Exception {
        // Two stacked nulling joins both NULL-extend the master mm. masterNullingJoinIndex must
        // anchor the master-only WHERE to the OUTERMOST nulling join (the ..s2 join), not the inner
        // one: a filter applied after only the inner join would be re-exposed to the NULL-master rows
        // synthesized by the outer join. Here the inner join (mm..s1) matches on k=1, so mm.col
        // survives it; the outer join (..s2) then NULL-extends the master for the unmatched s2 key 2.
        // WHERE mm.col = 1 must drop that NULL-master row, leaving exactly one row. Anchoring to the
        // inner join instead would leak it (two rows). Both RIGHT and FULL OUTER null the master, so
        // every combination of the two join slots must behave the same, for literal and bind forms.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE mm (k INT, col INT)");
            execute("INSERT INTO mm VALUES (1, 1)");
            execute("CREATE TABLE s1 (k INT)");
            execute("INSERT INTO s1 VALUES (1)");
            execute("CREATE TABLE s2 (k INT)");
            execute("INSERT INTO s2 VALUES (1), (2)");

            final String expected = "col\n1\n";
            for (String inner : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                for (String outer : new String[]{"RIGHT JOIN", "FULL JOIN"}) {
                    final String from = " FROM mm " + inner + " s1 ON mm.k = s1.k " + outer + " s2 ON s1.k = s2.k WHERE mm.col ";

                    bindVariableService.clear();
                    assertQuery("SELECT mm.col" + from + "= 1")
                            .noLeakCheck()
                            .noRandomAccess()
                            .withPlanContaining("Filter filter: mm.col=1")
                            .returns(expected);

                    // Bind-variable form must produce the identical result under the full battery.
                    bindVariableService.clear();
                    bindVariableService.setInt("v", 1);
                    assertQuery("SELECT mm.col" + from + "= :v::INT")
                            .noLeakCheck()
                            .noRandomAccess()
                            .returns(expected);
                }
            }
        });
    }

    @Test
    public void testStringSymbolVarcharJoins() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (i int, s string, b symbol)");
            execute("insert into t1 values (1, 'a', 'a'), (2, 'b', 'b'), (3, 'c', 'c'), (4, 'd', 'd'), (5, 'e', 'e');");
            execute("create table t2 (j int, v varchar)");
            execute("insert into t2 values (7, 'g'), (6, 'f'), (5, 'e'), (3, 'c'), (2, 'b'), (4, 'd'), (1, 'a');");

            final String expected = """
                    i\ts\tb\tj\tv
                    1\ta\ta\t1\ta
                    2\tb\tb\t2\tb
                    3\tc\tc\t3\tc
                    4\td\td\t4\td
                    5\te\te\t5\te
                    """;
            final String rightJoinExpected = """
                    i\ts\tb\tj\tv
                    1\ta\ta\t1\ta
                    2\tb\tb\t2\tb
                    3\tc\tc\t3\tc
                    4\td\td\t4\td
                    5\te\te\t5\te
                    null\t\t\t6\tf
                    null\t\t\t7\tg
                    """;
            final String fullJoinExpected = """
                    i\ts\tb\tj\tv
                    null\t\t\t7\tg
                    null\t\t\t6\tf
                    1\ta\ta\t1\ta
                    2\tb\tb\t2\tb
                    3\tc\tc\t3\tc
                    4\td\td\t4\td
                    5\te\te\t5\te
                    """;

            assertQuery("select i, s, b, j, v from t1 inner join t2 on s = v order by i")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select i, s, b, j, v from t1 inner join t2 on b = v order by i")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select i, s, b, j, v from t1 left join t2 on s = v")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
            assertQuery("select i, s, b, j, v from t1 left join t2 on b = v")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);

            assertQuery("select i, s, b, j, v from t1 right join t2 on s = v")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(rightJoinExpected);
            assertQuery("select i, s, b, j, v from t1 right join t2 on b = v")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(rightJoinExpected);

            assertQuery("select i, s, b, j, v from t1 full join t2 on s = v order by i")
                    .noLeakCheck()
                    .returns(fullJoinExpected);
            assertQuery("select i, s, b, j, v from t1 full join t2 on b = v order by i")
                    .noLeakCheck()
                    .returns(fullJoinExpected);

            final String expected2 = """
                    i\ts\tb\tj\tv
                    1\ta\ta\t7\tg
                    1\ta\ta\t6\tf
                    1\ta\ta\t5\te
                    1\ta\ta\t3\tc
                    1\ta\ta\t2\tb
                    1\ta\ta\t4\td
                    1\ta\ta\t1\ta
                    2\tb\tb\t7\tg
                    2\tb\tb\t6\tf
                    2\tb\tb\t5\te
                    2\tb\tb\t3\tc
                    2\tb\tb\t2\tb
                    2\tb\tb\t4\td
                    2\tb\tb\t1\ta
                    3\tc\tc\t7\tg
                    3\tc\tc\t6\tf
                    3\tc\tc\t5\te
                    3\tc\tc\t3\tc
                    3\tc\tc\t2\tb
                    3\tc\tc\t4\td
                    3\tc\tc\t1\ta
                    4\td\td\t7\tg
                    4\td\td\t6\tf
                    4\td\td\t5\te
                    4\td\td\t3\tc
                    4\td\td\t2\tb
                    4\td\td\t4\td
                    4\td\td\t1\ta
                    5\te\te\t7\tg
                    5\te\te\t6\tf
                    5\te\te\t5\te
                    5\te\te\t3\tc
                    5\te\te\t2\tb
                    5\te\te\t4\td
                    5\te\te\t1\ta
                    """;

            assertQuery("select i, s, b, j, v from t1 cross join t2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected2);
        });
    }

    @Test
    public void testSymbolStringJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xy2 as (select rnd_str(1,3,1) a from long_sequence(1000))");
            execute("create table xy3 as (select a::symbol a, rnd_int() b from xy2);");
            assertQuery("xy3 join xy2 on (a) order by a desc, b limit 5")
                    .noLeakCheck()
                    .returns("""
                            a\tb\ta1
                            ZY\t-2057990897\tZY
                            ZW\t-1719808959\tZW
                            ZW\t-1719808959\tZW
                            ZW\t-1067292175\tZW
                            ZW\t-1067292175\tZW
                            """);
            assertQuery("xy2 join xy3 on (a) order by a desc, b limit 5")
                    .noLeakCheck()
                    .returns("""
                            a\ta1\tb
                            ZY\tZY\t-2057990897
                            ZW\tZW\t-1719808959
                            ZW\tZW\t-1719808959
                            ZW\tZW\t-1067292175
                            ZW\tZW\t-1067292175
                            """);
        });
    }

    @Test
    public void testSymbolVarcharJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xy2 as (select rnd_varchar(1,3,1) a from long_sequence(1000))");
            execute("create table xy3 as (select a::symbol a, rnd_int() b from xy2);");
            assertQuery("xy3 join xy2 on (a) order by a desc, b limit 5")
                    .noLeakCheck()
                    .returns("""
                            a\tb\ta1
                            סּ\uDA07\uDD7B\uDBD1\uDCF9\t393942866\tסּ\uDA07\uDD7B\uDBD1\uDCF9
                            櫓\t2125240559\t櫓
                            \uF8F2\t-1552484280\t\uF8F2
                            \uEF20X\t1327628680\t\uEF20X
                            \uED0D|\uDB08\uDCF3\t-890115527\t\uED0D|\uDB08\uDCF3
                            """);
            assertQuery("xy2 join xy3 on (a) order by a desc, b limit 5")
                    .noLeakCheck()
                    .returns("""
                            a\ta1\tb
                            \uDBE9\uDC70,䜉\t\uDBE9\uDC70,䜉\t1756786531
                            \uDBD8\uDD33\uDB58\uDFC4\t\uDBD8\uDD33\uDB58\uDFC4\t-1759183734
                            \uDBB2\uDE2Eӿ\uDAF8\uDD66\t\uDBB2\uDE2Eӿ\uDAF8\uDD66\t2059419445
                            \uDBAE\uDD12ɜ|\t\uDBAE\uDD12ɜ|\t-2013119811
                            \uDBAD\uDCF1푻䑫\t\uDBAD\uDCF1푻䑫\t-681264014
                            """);
        });
    }

    @Test
    public void testThreeTableMasterFilterStaysPostJoin() throws Exception {
        // A WHERE predicate that references THREE master tables (t0.a + t1.b + t2.c > 0), wrapped in a
        // sub-query so moveWhereInsideSubQueries re-anchors it. The multi-table branch there routes
        // through lastNullingJoinAfterReferencedTables, whose loop over the referenced indexes only
        // iterated over two entries in every other test. A later RIGHT/FULL join NULL-extends t0, t1
        // and t2 for the unmatched t3 key 2; the predicate must stay above that join. Anchoring at the
        // highest referenced model index (t2's inner join) would leak the (null,null,null,2) row -- 2
        // rows for 1. The matched row (1+2+3=6 > 0) survives; the NULL-master row (null+...>0 is
        // NULL/false) is dropped.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, k INT)");
            execute("INSERT INTO t0 VALUES (1, 1)");
            execute("CREATE TABLE t1 (b INT, k INT)");
            execute("INSERT INTO t1 VALUES (2, 1)");
            execute("CREATE TABLE t2 (c INT, k INT)");
            execute("INSERT INTO t2 VALUES (3, 1)");
            execute("CREATE TABLE t3 (k INT)");
            execute("INSERT INTO t3 VALUES (1), (2)");

            final String expected = "a\tb\tc\tk\n1\t2\t3\t1\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT a, b, c, k FROM (SELECT t0.a a, t1.b b, t2.c c, t3.k k " +
                        "FROM t0 JOIN t1 ON t0.k = t1.k JOIN t2 ON t1.k = t2.k " + joinType + " JOIN t3 ON t3.k = t2.k) " +
                        "WHERE a + b + c > 0")
                        .noLeakCheck()
                        .noRandomAccess()
                        .withPlanContaining("Filter filter: 0<t0.a+t1.b+t2.c")
                        .returns(expected);
            }
        });
    }

    @Test
    public void testTransitiveFiltersOnRenamingSubQuery() throws Exception {
        // t1.k = 1 and t2's join key t2.k = t1.k imply the filter t2.k = 1, and the outer k1 = 1, which
        // the outer query pushes into the join, implies it again. Both filters move into t2, which
        // swaps tb's k and y, so each must take tb's column name exactly once: t2.k is tb's y, and a
        // second rename turns it into tb's k. tc's ON clause implies the key, or t2's ON clause states it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (k INT, x INT, v INT)");
            execute("CREATE TABLE tb (k INT, y INT, x INT, v INT)");
            execute("CREATE TABLE tc (k INT, v INT)");
            execute("""
                    INSERT INTO t1 VALUES
                        (1, 0, 11),
                        (2, 0, 12),
                        (null, 1, 13),
                        (1, 1, 14)
                    """);
            execute("""
                    INSERT INTO tb VALUES
                        (1, 5, 0, 101),
                        (5, 1, 0, 102),
                        (1, 1, 1, 103),
                        (2, 7, 0, 104),
                        (1, 1, 0, 105)
                    """);
            execute("""
                    INSERT INTO tc VALUES
                        (1, 301),
                        (2, 302),
                        (5, 303),
                        (null, 304)
                    """);
            execute("""
                    CREATE VIEW vr AS (
                        SELECT t1.k k1, t2.v v2, tc.v cv
                        FROM t1
                        JOIN (SELECT y k, k y, x, v FROM tb) t2 ON t2.x = t1.x AND t1.k = 1
                        RIGHT JOIN tc ON tc.k = t2.k AND tc.k = t1.k
                    )
                    """);
            execute("""
                    CREATE VIEW vi AS (
                        SELECT t1.k k1, t2.v v2, tc.v cv
                        FROM t1
                        JOIN (SELECT y k, k y, x, v FROM tb) t2 ON t2.x = t1.x AND t1.k = 1
                        JOIN tc ON tc.k = t2.k AND tc.k = t1.k
                    )
                    """);

            final String t2 = "(SELECT y k, k y, x, v FROM tb) t2";
            final String t2Renamed = "(SELECT y kk, k yy, x, v FROM tb) t2";
            for (String[] q : new String[][]{
                    {"vr", "t2.k"},
                    {"(SELECT t1.k k1, t2.v v2, tc.v cv FROM t1 JOIN " + t2 + " ON t2.x = t1.x AND t1.k = 1 RIGHT JOIN tc ON tc.k = t2.k AND tc.k = t1.k)", "t2.k"},
                    {"(SELECT t1.k k1, t2.v v2, tc.v cv FROM t1 JOIN " + t2 + " ON t2.x = t1.x RIGHT JOIN tc ON tc.k = t2.k AND tc.k = t1.k WHERE t1.k = 1)", "t2.k"},
                    {"(SELECT t1.k k1, t2.v v2, tc.v cv FROM t1 JOIN " + t2Renamed + " ON t2.x = t1.x AND t1.k = 1 RIGHT JOIN tc ON tc.k = t2.kk AND tc.k = t1.k)", "t2.kk"},
                    {"vi", "t2.k"},
                    {"(SELECT t1.k k1, t2.v v2, tc.v cv FROM t1 JOIN " + t2 + " ON t2.x = t1.x AND t1.k = 1 JOIN tc ON tc.k = t2.k AND tc.k = t1.k)", "t2.k"},
                    {"(SELECT t1.k k1, t2.v v2, tc.v cv FROM t1 JOIN " + t2Renamed + " ON t2.x = t1.x AND t1.k = 1 JOIN tc ON tc.k = t2.kk AND tc.k = t1.k)", "t2.kk"}
            }) {
                assertQuery("SELECT v2, cv FROM " + q[0] + " WHERE k1 = 1 ORDER BY v2")
                        .noLeakCheck()
                        .withPlanContaining("condition: " + q[1] + "=t1.k and t2.x=t1.x", "filter: (y=1 and y=1)")
                        .returns("""
                                v2\tcv
                                102\t301
                                103\t301
                                105\t301
                                """);
            }

            assertQuery("SELECT v2, x2 FROM (SELECT t1.k k1, t2.v v2, t2.x x2 FROM t1 JOIN " + t2 + " ON t2.k = t1.k WHERE t1.k = 1) WHERE k1 = 1 ORDER BY v2, x2")
                    .noLeakCheck()
                    .withPlanContaining("condition: t2.k=t1.k", "filter: (y=1 and y=1)")
                    .returns("""
                            v2\tx2
                            102\t0
                            102\t0
                            103\t1
                            103\t1
                            105\t0
                            105\t0
                            """);
        });
    }

    @Test
    public void testTransitiveFiltersOnRightJoinedSubQuery() throws Exception {
        // a.a = 2 and the join key c.cc = a.a imply the filter c.cc = 2, and the outer aa = 2, which the
        // outer query pushes into the join, implies it again. Both filters move into the sub-query c,
        // so each must rename c.cc to t2's c exactly once; a second rename fails the query.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, b INT, c INT, v INT)");
            execute("CREATE TABLE t1 (a INT, b INT, c INT, v INT)");
            execute("CREATE TABLE t2 (a INT, b INT, c INT, v INT)");
            execute("""
                    INSERT INTO t0 VALUES
                        (2, 0, 2, 101),
                        (2, 0, 1, 102),
                        (3, 0, 3, 103),
                        (null, 0, null, 104)
                    """);
            execute("""
                    INSERT INTO t1 VALUES
                        (0, 2, 0, 201),
                        (0, 1, 0, 202),
                        (0, 3, 0, 203),
                        (0, 2, 0, 204)
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                        (0, 0, 2, 301),
                        (0, 0, 3, 302),
                        (0, 0, null, 303),
                        (0, 0, 2, 304)
                    """);

            final String b = "(SELECT a aa, b bb, c cc, v FROM t1) b";
            final String c = "(SELECT a aa, b bb, c cc, v FROM t2) c";
            assertQuery("SELECT aa, ac, cv FROM (SELECT a.a aa, a.c ac, c.v cv FROM t0 a RIGHT JOIN " + c + " ON c.cc = a.a WHERE a.a = 2) WHERE aa = 2 ORDER BY ac, cv")
                    .noLeakCheck()
                    .withPlanContaining("condition: c.cc=a.a", "filter: (c=2 and c=2)")
                    .returns("""
                            aa\tac\tcv
                            2\t1\t301
                            2\t1\t304
                            2\t2\t301
                            2\t2\t304
                            """);
            // the RIGHT join keeps both equalities as its keys
            assertQuery("SELECT aa, ac, cv FROM (SELECT a.a aa, a.c ac, c.v cv FROM t0 a RIGHT JOIN " + c + " ON c.cc = a.a AND c.cc = a.c WHERE a.a = 2) WHERE aa = 2 ORDER BY cv")
                    .noLeakCheck()
                    .withPlanContaining("condition: c.cc=a.c and c.cc=a.a", "filter: (c=2 and c=2)")
                    .returns("""
                            aa\tac\tcv
                            2\t2\t301
                            2\t2\t304
                            """);
            // the RIGHT join's ON clause implies b.bb = a.a, a join key of b, so both filters move into b
            assertQuery("SELECT aa, ac, bv, cv FROM (SELECT a.a aa, a.c ac, b.v bv, c.v cv FROM t0 a JOIN " + b + " ON b.bb = a.c RIGHT JOIN " + c + " ON c.cc = a.a AND c.cc = b.bb AND c.cc = a.c WHERE a.a = 2) WHERE aa = 2 ORDER BY bv, cv")
                    .noLeakCheck()
                    .withPlanContaining("condition: c.cc=a.c", "filter: (b=2 and b=2)")
                    .returns("""
                            aa\tac\tbv\tcv
                            2\t2\t201\t301
                            2\t2\t201\t304
                            2\t2\t204\t301
                            2\t2\t204\t304
                            """);
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
            execute("create table TabA ( " +
                    "          ts timestamp, " +
                    "          x long " +
                    "        ) timestamp(ts) PARTITION by month");

            // 3 partitions
            execute("create table TabB ( " +
                    "          ts timestamp, " +
                    "          x long " +
                    "        ) timestamp(ts) PARTITION by hour");

            // 0 partitions
            execute("create table TabC ( " +
                    "          ts timestamp, " +
                    "          x long " +
                    "        ) timestamp(ts) PARTITION by year");

            execute("insert into TabA select x::timestamp, x/6 from long_sequence(10)");
            execute("insert into TabB select (x*15L*60L*1000000L)::timestamp, x/6 from long_sequence(10)");

            // async filter
            String selectWithFilter = "(select * from TabA where x = 0 " +
                    "union all " +
                    "select * from TabB where x = 1 " +
                    "union all " +
                    "select * from taBC where x = 0 )";
            assertSkipToAndCalculateSize(selectWithFilter, 10);

            // async filter with limit
            String selectWithFilterAndLimit = "( " +
                    "selecT * from " +
                    "(select * from TabA where x = 0 limit 3) " +
                    "union all " +
                    "(select * from TabB where x = 1 limit 3) " +
                    "union all " +
                    "(select * from taBC where x = 0 limit 1) )";
            assertSkipToAndCalculateSize(selectWithFilterAndLimit, 6);

            // fwd page frame
            String selectWithFwdFrame = "(select * from TabA union all select * from TabB union all select * from TabC)";
            assertSkipToAndCalculateSize(selectWithFwdFrame, 20);

            // bwd page frame
            String selectWithBwdFrame = "(select * from " +
                    "(select * from TabA order by ts desc) " +
                    "union all " +
                    "(select * from TabB order by ts desc) " +
                    "union all (select * from tabC order by ts desc) )";
            assertSkipToAndCalculateSize(selectWithBwdFrame, 20);

            // interval fwd page frame
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
    public void testWrappedBarrierSlaveMasterFilterStaysPostJoin() throws Exception {
        // LEAK-B: a single-table predicate (b.w + b.m > 0) references only b, which is the SLAVE of
        // the inner RIGHT join AND is NULL-extended by the later c RIGHT join. Because the join is
        // wrapped in a sub-query, the predicate routes through moveWhereInsideSubQueries' barrier
        // branch, which anchored it at b's own join -- below the c nulling join. The unmatched c key
        // 2 produces a NULL-master row that the predicate must drop; anchoring below the c join leaked
        // it (2 rows for 1). The non-wrapped form already stays post-join via assignFilters.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT)");
            execute("INSERT INTO a VALUES (1)");
            execute("CREATE TABLE b (k INT, w INT, m INT)");
            execute("INSERT INTO b VALUES (1, 5, 5)");
            execute("CREATE TABLE c (k INT, x INT)");
            execute("INSERT INTO c VALUES (1, 9), (2, 99)");

            final String expected = "ak\tbk\tbw\tbm\tck\tcx\n1\t1\t5\t5\t1\t9\n";
            assertQuery("SELECT * FROM (SELECT a.k ak, b.k bk, b.w bw, b.m bm, c.k ck, c.x cx " +
                    "FROM a RIGHT JOIN b ON a.k = b.k RIGHT JOIN c ON b.k = c.k) WHERE bw + bm > 0")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Filter filter: 0<b.w+b.m")
                    .returns(expected);
        });
    }

    @Test
    public void testWrappedMultiTableMasterFilterStaysPostJoin() throws Exception {
        // LEAK-A: companion to testMultiTableMasterFilterStaysPostJoin, but the join is wrapped in a
        // sub-query. After moveWhereInsideSubQueries inlines the outer predicate into the join model,
        // the rewritten t0.a < t1.b references two master tables and routes through the
        // distinctIndexes>1 branch instead of assignFilters. A later RIGHT/FULL join NULL-extends t0
        // and t1 for the unmatched t2 key 2; the filter must stay above that join. Anchoring at the
        // highest referenced model index (t1's inner join) leaked the (null,null,2) row -- 2 for 1.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT, k INT)");
            execute("INSERT INTO t0 VALUES (1, 1)");
            execute("CREATE TABLE t1 (b INT, k INT)");
            execute("INSERT INTO t1 VALUES (5, 1)");
            execute("CREATE TABLE t2 (k INT)");
            execute("INSERT INTO t2 VALUES (1), (2)");

            final String expected = "a\tb\tk\n1\t5\t1\n";
            for (String joinType : new String[]{"RIGHT OUTER", "FULL OUTER"}) {
                assertQuery("SELECT a, b, k FROM (SELECT t0.a a, t1.b b, t2.k k " +
                        "FROM t0 JOIN t1 ON t0.k = t1.k " + joinType + " JOIN t2 ON t2.k = t1.k) WHERE a < b")
                        .noLeakCheck()
                        .noRandomAccess()
                        .withPlanContaining("Filter filter: t0.a<t1.b")
                        .returns(expected);
            }
        });
    }

    @Test
    public void testWrappedSubQueryMasterFilterStaysPostJoin() throws Exception {
        // The join is wrapped in a sub-query and the master predicate sits on the outer model, so
        // it reaches moveWhereInsideSubQueries instead of analyseEquals. The same master-nulling
        // guard must apply: RIGHT/FULL/SPLICE all NULL-extend the master, and the master has no
        // 's2' row, so every output row is NULL-master and WHERE a = 's2' must return nothing.
        // Pushing the predicate into the master sub-query emptied it and leaked 2 NULL-master rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (sym SYMBOL, c1 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES ('x', 200, 1)");
            execute("CREATE TABLE s (sym SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES ('s2', 10, 2), ('s3', 50, 3)");

            final String empty = "a\tb\n";
            for (String join : new String[]{
                    "m RIGHT JOIN s ON m.sym = s.sym",
                    "m FULL JOIN s ON m.sym = s.sym",
                    "m SPLICE JOIN s ON (sym)"}) {
                assertQuery("SELECT a, b FROM (SELECT m.sym a, m.c1 b FROM " + join + ") WHERE a = 's2'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .withPlanContaining("Filter filter: m.sym='s2'")
                        .returns(empty);
            }
        });
    }

    private void assertFailure(String query, String expectedMessage, int position) {
        try {
            execute(query, sqlExecutionContext);
            Assert.fail("query '" + query + "' should have failed with '" + expectedMessage + "' message!");
        } catch (SqlException | ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
            Assert.assertEquals(Chars.toString(query), position, e.getPosition());
        }
    }

    // Asserts the queries of testFirstTableWithTimestampReachesTablesThroughHashJoins() for one form of
    // the FROM clause: an optional WITH prefix, the joined items, and an optional WHERE clause.
    private void assertFirstTableReachesTablesThroughHashJoins(String with, String items, String where) throws Exception {
        final String from = " FROM " + items + where;
        assertQuery(with + "SELECT m.v, m.ts" + from)
                .noLeakCheck()
                .noRandomAccess()
                .timestamp("ts")
                .withPlanContaining("condition: s3.a=m.a", "condition: s2.b=s3.b", "condition: s1.x=s2.x")
                .withPlanNotContaining("Cross Join")
                .returns("""
                        v\tts
                        101\t1970-01-01T00:00:01.000000Z
                        101\t1970-01-01T00:00:01.000000Z
                        102\t1970-01-01T00:00:02.000000Z
                        102\t1970-01-01T00:00:02.000000Z
                        103\t1970-01-01T00:00:03.000000Z
                        105\t1970-01-01T00:00:05.000000Z
                        105\t1970-01-01T00:00:05.000000Z
                        """);
        assertQuery(with + "SELECT m.v, m.ts, s3.v v3, s2.v v2, s1.v v1" + from + " ORDER BY m.ts, v3, v1")
                .noLeakCheck()
                .timestamp("ts")
                .withPlanNotContaining("Cross Join")
                .returns("""
                        v\tts\tv3\tv2\tv1
                        101\t1970-01-01T00:00:01.000000Z\t33\t21\t14
                        101\t1970-01-01T00:00:01.000000Z\t33\t21\t15
                        102\t1970-01-01T00:00:02.000000Z\t32\t22\t13
                        102\t1970-01-01T00:00:02.000000Z\t34\t23\t12
                        103\t1970-01-01T00:00:03.000000Z\t31\t24\t11
                        105\t1970-01-01T00:00:05.000000Z\t32\t22\t13
                        105\t1970-01-01T00:00:05.000000Z\t34\t23\t12
                        """);
        assertQuery(with + "SELECT m.v, m.ts" + from + " LIMIT -3")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .timestamp("ts")
                .withPlanNotContaining("Cross Join")
                .returns("""
                        v\tts
                        103\t1970-01-01T00:00:03.000000Z
                        105\t1970-01-01T00:00:05.000000Z
                        105\t1970-01-01T00:00:05.000000Z
                        """);
        assertQuery(with + "SELECT first(m.v), last(m.v), count(*)" + from)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .withPlanNotContaining("Cross Join")
                .returns("""
                        first\tlast\tcount
                        101\t105\t7
                        """);
        assertQuery(with + "SELECT m.ts, count(*) cnt, first(m.v) first_v, last(m.v) last_v" + from + " SAMPLE BY 2s ALIGN TO FIRST OBSERVATION")
                .noLeakCheck()
                .noRandomAccess()
                .timestamp("ts")
                .withPlanNotContaining("Cross Join")
                .returns("""
                        ts\tcnt\tfirst_v\tlast_v
                        1970-01-01T00:00:01.000000Z\t4\t101\t102
                        1970-01-01T00:00:03.000000Z\t1\t103\t103
                        1970-01-01T00:00:05.000000Z\t2\t105\t105
                        """);
        // the ASOF JOIN matches on the timestamps of the first item
        assertQuery(with + "SELECT m.v, m.ts, q.v qv FROM " + items + " ASOF JOIN q" + where)
                .noLeakCheck()
                .noRandomAccess()
                .timestamp("ts")
                .withPlanNotContaining("Cross Join")
                .returns("""
                        v\tts\tqv
                        101\t1970-01-01T00:00:01.000000Z\tnull
                        101\t1970-01-01T00:00:01.000000Z\tnull
                        102\t1970-01-01T00:00:02.000000Z\t901
                        102\t1970-01-01T00:00:02.000000Z\t901
                        103\t1970-01-01T00:00:03.000000Z\t901
                        105\t1970-01-01T00:00:05.000000Z\t902
                        105\t1970-01-01T00:00:05.000000Z\t902
                        """);
    }

    private void assertHashJoinSql(String query, String expected) throws Exception {
        assertHashJoinSql(query, expected, null, false, false);
    }

    private void assertHashJoinSql(String query, String expected, String tsColumn, boolean tsDescending, boolean supportRandom) throws Exception {
        var qa = assertQuery(query)
                .noLeakCheck()
                .fullFatJoins();
        if (tsColumn != null) {
            if (tsDescending) {
                qa.timestampDesc(tsColumn);
            } else {
                qa.timestampAsc(tsColumn);
            }
        }
        qa.supportsRandomAccess(supportRandom)
                .returns(expected);
        printSql(query, true);
        TestUtils.assertEquals("full fat join", expected, sink);
    }

    private void assertHashJoinSqlWithRandomAccess(String query, String expected) throws Exception {
        assertHashJoinSql(query, expected, null, false, true);
    }

    private void assertRepeatedJoinQuery(String query, String left, boolean expectSize) throws Exception {
        assertQuery(query.replace("#JOIN_TYPE#", left))
                .noLeakCheck()
                .noRandomAccess()
                .expectSize(expectSize)
                .returns("id\n1\n");
    }

    private void assertSkipToAndCalculateSize(String select, int size) throws Exception {
        assertQuery("select count(*) from (" + select + ")")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n" + size + "\n");

        RecordCursor.Counter counter = new RecordCursor.Counter();

        try (RecordCursorFactory factory = select(select)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                Assert.assertEquals(size, counter.get());

                for (int i = 0; i < size + 2; i++) {
                    cursor.toTop();
                    counter.set(i);
                    cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);

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

    // Tables of testFirstTableWithTimestampStopsChainAt*(): s0 has a designated timestamp, each s1 row
    // joins one s2 row on x, and s4 matches the first s0 and s2 rows on a and b, at a time before s0's.
    private void createFirstTableBarrierTables() throws SqlException {
        execute("CREATE TABLE s0 (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE s1 (x INT, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE s2 (b INT, x INT, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE s4 (a INT, b INT, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO s0 VALUES (1, 100, '2024-01-01T10:00'), (2, 101, '2024-01-01T11:00')");
        execute("INSERT INTO s1 VALUES (1, 200, '2024-01-01T00:00'), (2, 201, '2024-01-01T01:00')");
        execute("INSERT INTO s2 VALUES (1, 1, 300, '2024-01-01T00:00'), (2, 2, 301, '2024-01-01T01:00')");
        execute("INSERT INTO s4 VALUES (1, 1, 400, '2024-01-01T09:00')");
    }

    // Tables of testFirstTableWithoutEquality*(). No equality links the first FROM item ta to tb2 and
    // tc2, which join on their timestamps, so a cross join connects ta to the chain. When the result
    // depends on the order of the join's rows, ta drives the chain: the join declares l.ts as its
    // designated timestamp and streams ta's rows, ta-major and tb2-minor. A plan that ran ta last, as
    // the slave of the cross join, would stream tb2's rows and declare r.rts instead. td is the other
    // side of time series joins.
    private void createFirstTableWithoutEqualityTables() throws SqlException {
        execute("CREATE TABLE ta (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE tb2 (rts TIMESTAMP, v INT, c1 INT) TIMESTAMP(rts) PARTITION BY DAY");
        execute("CREATE TABLE tc2 (cts TIMESTAMP, v INT) TIMESTAMP(cts) PARTITION BY DAY");
        execute("CREATE TABLE td (dts TIMESTAMP, v INT) TIMESTAMP(dts) PARTITION BY DAY");
        execute("INSERT INTO ta VALUES ('2020-01-01T00:10', 1), ('2020-01-01T02:10', 9)");
        execute("""
                INSERT INTO tb2 VALUES
                ('2020-01-01T00:20', 5, 10),
                ('2020-01-01T01:20', 0, 20),
                ('2020-01-01T03:20', 100, 30)
                """);
        execute("""
                INSERT INTO tc2 VALUES
                ('2020-01-01T00:20', 5),
                ('2020-01-01T01:20', 0),
                ('2020-01-01T03:20', 100)
                """);
        execute("""
                INSERT INTO td VALUES
                ('2020-01-01T00:00', 1),
                ('2020-01-01T00:15', 2),
                ('2020-01-01T01:30', 3),
                ('2020-01-01T02:00', 4),
                ('2020-01-01T03:00', 5)
                """);
    }

    private void testAsOfJoin0(boolean fullFatJoin) throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.sym2 = x.sym";

            final String expected = """
                    i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                    1\tmsft\t22.463\tnull\t2018-01-01T00:12:00.000000Z\t
                    2\tgoogl\t29.92\t0.885\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z
                    3\tmsft\t65.086\t0.5660000000000001\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                    4\tibm\t98.563\t0.405\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:34:00.000000Z
                    5\tmsft\t50.938\t0.545\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:46:00.000000Z
                    6\tibm\t76.11\t0.9540000000000001\t2018-01-01T01:12:00.000000Z\t2018-01-01T00:56:00.000000Z
                    7\tmsft\t55.992000000000004\t0.545\t2018-01-01T01:24:00.000000Z\t2018-01-01T00:46:00.000000Z
                    8\tibm\t23.905\t0.9540000000000001\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:56:00.000000Z
                    9\tgoogl\t67.786\t0.198\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:00:00.000000Z
                    10\tgoogl\t38.54\t0.198\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:00:00.000000Z
                    """;

            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)"
            );

            execute(
                    "create table y as (" +
                            "select cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)"
            );

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            execute(
                    "insert into x select * from (" +
                            "select" +
                            " cast(x + 10 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ") timestamp(timestamp)"
            );

            execute(
                    "insert into y select * from (" +
                            "select" +
                            " cast(x + 30 as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)"
            );

            assertQuery(query)
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoin)
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                            1\tmsft\t22.463\tnull\t2018-01-01T00:12:00.000000Z\t
                            2\tgoogl\t29.92\t0.885\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z
                            3\tmsft\t65.086\t0.5660000000000001\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                            4\tibm\t98.563\t0.405\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:34:00.000000Z
                            5\tmsft\t50.938\t0.545\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:46:00.000000Z
                            6\tibm\t76.11\t0.337\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:12:00.000000Z
                            7\tmsft\t55.992000000000004\t0.226\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:16:00.000000Z
                            8\tibm\t23.905\t0.767\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:36:00.000000Z
                            9\tgoogl\t67.786\t0.101\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:48:00.000000Z
                            10\tgoogl\t38.54\t0.6900000000000001\t2018-01-01T02:00:00.000000Z\t2018-01-01T02:00:00.000000Z
                            11\tmsft\t68.069\t0.051000000000000004\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:50:00.000000Z
                            12\tmsft\t24.008\t0.051000000000000004\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:50:00.000000Z
                            13\tgoogl\t94.559\t0.6900000000000001\t2018-01-01T02:36:00.000000Z\t2018-01-01T02:00:00.000000Z
                            14\tibm\t62.474000000000004\t0.068\t2018-01-01T02:48:00.000000Z\t2018-01-01T01:40:00.000000Z
                            15\tmsft\t39.017\t0.051000000000000004\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:50:00.000000Z
                            16\tgoogl\t10.643\t0.6900000000000001\t2018-01-01T03:12:00.000000Z\t2018-01-01T02:00:00.000000Z
                            17\tmsft\t7.246\t0.051000000000000004\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:50:00.000000Z
                            18\tmsft\t36.798\t0.051000000000000004\t2018-01-01T03:36:00.000000Z\t2018-01-01T01:50:00.000000Z
                            19\tmsft\t66.98\t0.051000000000000004\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:50:00.000000Z
                            20\tgoogl\t26.369\t0.6900000000000001\t2018-01-01T04:00:00.000000Z\t2018-01-01T02:00:00.000000Z
                            """);
        });
    }

    private void testAsOfJoinNoStrings0(boolean fullFatJoin) throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.sym2 = x.sym";

            final String expected = """
                    i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                    1\tmsft\t50.938\t0.523\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:12:00.000000Z
                    2\tgoogl\t42.281\t0.215\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:18:00.000000Z
                    3\tgoogl\t17.371\t0.915\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                    4\tibm\t14.831\t0.404\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:42:00.000000Z
                    5\tgoogl\t86.772\t0.092\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z
                    6\tmsft\t29.659\t0.537\t2018-01-01T01:12:00.000000Z\t2018-01-01T00:54:00.000000Z
                    7\tgoogl\t7.594\t0.092\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:00:00.000000Z
                    8\tibm\t54.253\t0.404\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:42:00.000000Z
                    9\tmsft\t62.26\t0.537\t2018-01-01T01:48:00.000000Z\t2018-01-01T00:54:00.000000Z
                    10\tmsft\t50.908\t0.537\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:54:00.000000Z
                    """;

            execute(
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
            execute(
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

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            execute(
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
            execute(
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

            assertQuery(query)
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoin)
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            i\tsym\tamt\tprice\ttimestamp\ttimestamp1
                            1\tmsft\t50.938\t0.523\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:12:00.000000Z
                            2\tgoogl\t42.281\t0.215\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:18:00.000000Z
                            3\tgoogl\t17.371\t0.915\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                            4\tibm\t14.831\t0.404\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:42:00.000000Z
                            5\tgoogl\t86.772\t0.092\t2018-01-01T01:00:00.000000Z\t2018-01-01T01:00:00.000000Z
                            6\tmsft\t29.659\t0.098\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:08:00.000000Z
                            7\tgoogl\t7.594\t0.036000000000000004\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:24:00.000000Z
                            8\tibm\t54.253\t0.74\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:20:00.000000Z
                            9\tmsft\t62.26\t0.032\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:32:00.000000Z
                            10\tmsft\t50.908\t0.912\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:58:00.000000Z
                            11\tmsft\t25.604\t0.912\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:58:00.000000Z
                            12\tgoogl\t89.22\t0.148\t2018-01-01T02:24:00.000000Z\t2018-01-01T02:00:00.000000Z
                            13\tgoogl\t64.536\t0.148\t2018-01-01T02:36:00.000000Z\t2018-01-01T02:00:00.000000Z
                            14\tibm\t33.0\t0.388\t2018-01-01T02:48:00.000000Z\t2018-01-01T01:56:00.000000Z
                            15\tmsft\t67.285\t0.912\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:58:00.000000Z
                            16\tgoogl\t17.31\t0.148\t2018-01-01T03:12:00.000000Z\t2018-01-01T02:00:00.000000Z
                            17\tibm\t23.957\t0.388\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:56:00.000000Z
                            18\tibm\t60.678000000000004\t0.388\t2018-01-01T03:36:00.000000Z\t2018-01-01T01:56:00.000000Z
                            19\tmsft\t4.727\t0.912\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:58:00.000000Z
                            20\tgoogl\t26.222\t0.148\t2018-01-01T04:00:00.000000Z\t2018-01-01T02:00:00.000000Z
                            """);
        });
    }

    private void testAsOfJoinOnStrNoVar0(boolean fullFatJoin) throws Exception {
        // there are no variable length columns in slave table other than the one we join on
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.c, y.c, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.c = x.c";

            final String expected = """
                    i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1
                    1\tXYZ\tXYZ\t50.938\t0.294\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:10:00.000000Z
                    2\tABC\tABC\t42.281\t0.167\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:22:00.000000Z
                    3\tABC\tABC\t17.371\t0.167\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:22:00.000000Z
                    4\tXYZ\tXYZ\t44.805\t0.79\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z
                    5\t\t\t42.956\t0.28800000000000003\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:48:00.000000Z
                    6\tCDE\tCDE\t82.59700000000001\t0.8200000000000001\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:00:00.000000Z
                    7\tCDE\tCDE\t98.59100000000001\t0.8200000000000001\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:00:00.000000Z
                    8\tABC\tABC\t57.086\t0.319\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:38:00.000000Z
                    9\t\t\t81.44200000000001\t0.28800000000000003\t2018-01-01T01:48:00.000000Z\t2018-01-01T00:48:00.000000Z
                    10\tXYZ\tXYZ\t3.973\t0.16\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:52:00.000000Z
                    """;

            execute(
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
            execute(
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

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            execute(
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
            execute(
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

            assertQuery(query)
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoin)
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1
                            1\tXYZ\tXYZ\t50.938\t0.294\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:10:00.000000Z
                            2\tABC\tABC\t42.281\t0.167\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:22:00.000000Z
                            3\tABC\tABC\t17.371\t0.167\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:22:00.000000Z
                            4\tXYZ\tXYZ\t44.805\t0.79\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z
                            5\t\t\t42.956\t0.28800000000000003\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:48:00.000000Z
                            6\tCDE\tCDE\t82.59700000000001\t0.19\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:06:00.000000Z
                            7\tCDE\tCDE\t98.59100000000001\t0.201\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:20:00.000000Z
                            8\tABC\tABC\t57.086\t0.359\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:24:00.000000Z
                            9\t\t\t81.44200000000001\t0.92\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:48:00.000000Z
                            10\tXYZ\tXYZ\t3.973\t0.16\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:52:00.000000Z
                            11\tABC\tABC\t22.372\t0.359\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:24:00.000000Z
                            12\tABC\tABC\t48.423\t0.359\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:24:00.000000Z
                            13\tKZZ\tKZZ\t74.174\t0.853\t2018-01-01T02:36:00.000000Z\t2018-01-01T01:56:00.000000Z
                            14\t\t\t87.184\t0.46900000000000003\t2018-01-01T02:48:00.000000Z\t2018-01-01T01:52:00.000000Z
                            15\tABC\tABC\t66.993\t0.359\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:24:00.000000Z
                            16\tABC\tABC\t19.968\t0.359\t2018-01-01T03:12:00.000000Z\t2018-01-01T01:24:00.000000Z
                            17\tABC\tABC\t34.368\t0.359\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:24:00.000000Z
                            18\t\t\t1.869\t0.46900000000000003\t2018-01-01T03:36:00.000000Z\t2018-01-01T01:52:00.000000Z
                            19\tABC\tABC\t85.427\t0.359\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:24:00.000000Z
                            20\tABC\tABC\t54.586\t0.359\t2018-01-01T04:00:00.000000Z\t2018-01-01T01:24:00.000000Z
                            """);
        });
    }

    private void testAsOfJoinOnVarcharNoVar0(boolean fullFatJoin) throws Exception {
        // there are no variable length columns in slave table other than the one we join on
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.c, y.c, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.c = x.c";

            final String expected = """
                    i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1
                    1\tXYZ\tXYZ\t50.938\t0.294\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:10:00.000000Z
                    2\tABC\tABC\t42.281\t0.167\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:22:00.000000Z
                    3\tABC\tABC\t17.371\t0.167\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:22:00.000000Z
                    4\tXYZ\tXYZ\t44.805\t0.79\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z
                    5\t\t\t42.956\t0.28800000000000003\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:48:00.000000Z
                    6\tCDE\tCDE\t82.59700000000001\t0.8200000000000001\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:00:00.000000Z
                    7\tCDE\tCDE\t98.59100000000001\t0.8200000000000001\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:00:00.000000Z
                    8\tABC\tABC\t57.086\t0.319\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:38:00.000000Z
                    9\t\t\t81.44200000000001\t0.28800000000000003\t2018-01-01T01:48:00.000000Z\t2018-01-01T00:48:00.000000Z
                    10\tXYZ\tXYZ\t3.973\t0.16\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:52:00.000000Z
                    """;

            execute(
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
            execute(
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

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            execute(
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
            execute(
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

            assertQuery(query)
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoin)
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            i\tc\tc1\tamt\tprice\ttimestamp\ttimestamp1
                            1\tXYZ\tXYZ\t50.938\t0.294\t2018-01-01T00:12:00.000000Z\t2018-01-01T00:10:00.000000Z
                            2\tABC\tABC\t42.281\t0.167\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:22:00.000000Z
                            3\tABC\tABC\t17.371\t0.167\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:22:00.000000Z
                            4\tXYZ\tXYZ\t44.805\t0.79\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:46:00.000000Z
                            5\t\t\t42.956\t0.28800000000000003\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:48:00.000000Z
                            6\tCDE\tCDE\t82.59700000000001\t0.19\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:06:00.000000Z
                            7\tCDE\tCDE\t98.59100000000001\t0.201\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:20:00.000000Z
                            8\tABC\tABC\t57.086\t0.359\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:24:00.000000Z
                            9\t\t\t81.44200000000001\t0.92\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:48:00.000000Z
                            10\tXYZ\tXYZ\t3.973\t0.16\t2018-01-01T02:00:00.000000Z\t2018-01-01T00:52:00.000000Z
                            11\tABC\tABC\t22.372\t0.359\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:24:00.000000Z
                            12\tABC\tABC\t48.423\t0.359\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:24:00.000000Z
                            13\tKZZ\tKZZ\t74.174\t0.853\t2018-01-01T02:36:00.000000Z\t2018-01-01T01:56:00.000000Z
                            14\t\t\t87.184\t0.46900000000000003\t2018-01-01T02:48:00.000000Z\t2018-01-01T01:52:00.000000Z
                            15\tABC\tABC\t66.993\t0.359\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:24:00.000000Z
                            16\tABC\tABC\t19.968\t0.359\t2018-01-01T03:12:00.000000Z\t2018-01-01T01:24:00.000000Z
                            17\tABC\tABC\t34.368\t0.359\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:24:00.000000Z
                            18\t\t\t1.869\t0.46900000000000003\t2018-01-01T03:36:00.000000Z\t2018-01-01T01:52:00.000000Z
                            19\tABC\tABC\t85.427\t0.359\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:24:00.000000Z
                            20\tABC\tABC\t54.586\t0.359\t2018-01-01T04:00:00.000000Z\t2018-01-01T01:24:00.000000Z
                            """);
        });
    }

    private void testAsOfJoinSlaveSymbol0(boolean fullFatJoin) throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, sym2, x.amt, price, x.timestamp, y.timestamp from x asof join y on y.sym2 = x.sym";

            final String expected = """
                    i\tsym\tsym2\tamt\tprice\ttimestamp\ttimestamp1
                    1\tmsft\t\t22.463\tnull\t2018-01-01T00:12:00.000000Z\t
                    2\tgoogl\tgoogl\t29.92\t0.885\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z
                    3\tmsft\tmsft\t65.086\t0.5660000000000001\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                    4\tibm\tibm\t98.563\t0.405\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:34:00.000000Z
                    5\tmsft\tmsft\t50.938\t0.545\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:46:00.000000Z
                    6\tibm\tibm\t76.11\t0.9540000000000001\t2018-01-01T01:12:00.000000Z\t2018-01-01T00:56:00.000000Z
                    7\tmsft\tmsft\t55.992000000000004\t0.545\t2018-01-01T01:24:00.000000Z\t2018-01-01T00:46:00.000000Z
                    8\tibm\tibm\t23.905\t0.9540000000000001\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:56:00.000000Z
                    9\tgoogl\tgoogl\t67.786\t0.198\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:00:00.000000Z
                    10\tgoogl\tgoogl\t38.54\t0.198\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:00:00.000000Z
                    """;

            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)"
            );
            execute(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym2," +
                            " round(rnd_double(0), 3) price," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)"
            );

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            execute("insert into x select * from (select cast(x + 10 as int) i, rnd_symbol('msft','ibm', 'googl') sym, round(rnd_double(0)*100, 3) amt, to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp from long_sequence(10)) timestamp(timestamp)");
            execute("insert into y select * from (select cast(x + 30 as int) i, rnd_symbol('msft','ibm', 'googl') sym2, round(rnd_double(0), 3) price, to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp from long_sequence(30)) timestamp(timestamp)");

            assertQuery(query)
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoin)
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            i\tsym\tsym2\tamt\tprice\ttimestamp\ttimestamp1
                            1\tmsft\t\t22.463\tnull\t2018-01-01T00:12:00.000000Z\t
                            2\tgoogl\tgoogl\t29.92\t0.885\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:24:00.000000Z
                            3\tmsft\tmsft\t65.086\t0.5660000000000001\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:36:00.000000Z
                            4\tibm\tibm\t98.563\t0.405\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:34:00.000000Z
                            5\tmsft\tmsft\t50.938\t0.545\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:46:00.000000Z
                            6\tibm\tibm\t76.11\t0.337\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:12:00.000000Z
                            7\tmsft\tmsft\t55.992000000000004\t0.226\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:16:00.000000Z
                            8\tibm\tibm\t23.905\t0.767\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:36:00.000000Z
                            9\tgoogl\tgoogl\t67.786\t0.101\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:48:00.000000Z
                            10\tgoogl\tgoogl\t38.54\t0.6900000000000001\t2018-01-01T02:00:00.000000Z\t2018-01-01T02:00:00.000000Z
                            11\tmsft\tmsft\t68.069\t0.051000000000000004\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:50:00.000000Z
                            12\tmsft\tmsft\t24.008\t0.051000000000000004\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:50:00.000000Z
                            13\tgoogl\tgoogl\t94.559\t0.6900000000000001\t2018-01-01T02:36:00.000000Z\t2018-01-01T02:00:00.000000Z
                            14\tibm\tibm\t62.474000000000004\t0.068\t2018-01-01T02:48:00.000000Z\t2018-01-01T01:40:00.000000Z
                            15\tmsft\tmsft\t39.017\t0.051000000000000004\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:50:00.000000Z
                            16\tgoogl\tgoogl\t10.643\t0.6900000000000001\t2018-01-01T03:12:00.000000Z\t2018-01-01T02:00:00.000000Z
                            17\tmsft\tmsft\t7.246\t0.051000000000000004\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:50:00.000000Z
                            18\tmsft\tmsft\t36.798\t0.051000000000000004\t2018-01-01T03:36:00.000000Z\t2018-01-01T01:50:00.000000Z
                            19\tmsft\tmsft\t66.98\t0.051000000000000004\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:50:00.000000Z
                            20\tgoogl\tgoogl\t26.369\t0.6900000000000001\t2018-01-01T04:00:00.000000Z\t2018-01-01T02:00:00.000000Z
                            """);
        });
    }

    private void testFullFat(TestMethod method) throws Exception {
        method.run(true);
    }

    private void testJoinColumnPropagationIntoJoinModel0(String joinType) throws Exception {
        String query = ("""
                SELECT amount, price1
                FROM
                (
                  SELECT *
                  FROM trades b
                  #JOIN_TYPE#
                  (
                    SELECT *
                    FROM trades
                    WHERE price > 1
                      AND symbol = 'ETH-USD'
                  ) a ON #JOIN_CLAUSE#
                  WHERE b.amount > 1
                    AND b.symbol = 'ETH-USD'
                )""").replace("#JOIN_TYPE#", joinType);
        String expected = "LT JOIN".equals(joinType) ? "amount\tprice1\n2.0\tnull\n" : "amount\tprice1\n2.0\t2.0\n";

        assertQuery(query.replace("#JOIN_CLAUSE#", "symbol"))
                .noLeakCheck()
                .noRandomAccess()
                .returns(expected);
        assertQuery(query.replace("#JOIN_CLAUSE#", "a.symbol = b.symbol"))
                .noLeakCheck()
                .noRandomAccess()
                .returns(expected);
        assertQuery(query.replace("#JOIN_CLAUSE#", "a.symbol = b.symbol and a.price = b.price"))
                .noLeakCheck()
                .noRandomAccess()
                .returns(expected);
        if (!joinType.contains("LT") && !joinType.contains("ASOF")) {
            assertQuery(query.replace("#JOIN_CLAUSE#", "b.symbol = a.symbol and a.timestamp = b.timestamp"))
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
        }
    }

    private void testJoinConstantFalse0(boolean fullFatJoin) throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "c\ta\tb\tcolumn\n";
            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(10))");
            execute("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))");

            // master records should be filtered out because slave records missing
            assertQuery("select x.c, x.a, b, a+b from x join y on y.m = x.c and 1 > 10")
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoin)
                    .expectSize()
                    .returns(expected);
        });
    }

    private void testJoinConstantTrue0(boolean fullFatJoin) throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    c\ta\tb
                    2\t568\t16
                    2\t568\t72
                    4\t371\t14
                    4\t371\t3
                    6\t439\t81
                    6\t439\t12
                    8\t521\t16
                    8\t521\t97
                    10\t598\t5
                    10\t598\t74
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(10))");
            execute("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))");

            // master records should be filtered out because slave records missing
            assertQuery("select x.c, x.a, b from x join y on y.m = x.c and 1 < 10")
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoin)
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    private void testJoinForCursorLeaks(String sql, boolean fullFatJoins) throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger counter = new AtomicInteger();
            ff = new TestFilesFacadeImpl() {
                @Override
                public int errno() {
                    // return "Too many open files" to avoid conflicting with ERRNO_FILE_DOES_NOT_EXIST.
                    return 4;
                }

                @Override
                public long openRO(LPSZ name) {
                    // x.d is the first column file opened because the active columns
                    // optimization skips ts.d when it is not in the query's column set
                    if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "x.d") && counter.incrementAndGet() == 1) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            execute("create table xx as (" +
                    "select x," +
                    " timestamp_sequence(0, 1000) ts" +
                    " from long_sequence(100000)) timestamp (ts)");

            try {
                assertExceptionNoLeakCheck(sql, sqlExecutionContext, fullFatJoins);
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-only");
                TestUtils.assertContains(ex.getFlyweightMessage(), "x.d");
            }
        });
    }

    private void testJoinInner0(boolean fullFatJoin) throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    c\ta\tb\td\tcolumn
                    1\t120\t39\t0\t-39
                    1\t120\t39\t50\t11
                    1\t120\t42\t0\t-42
                    1\t120\t42\t50\t8
                    1\t120\t71\t0\t-71
                    1\t120\t71\t50\t-21
                    1\t120\t6\t0\t-6
                    1\t120\t6\t50\t44
                    2\t568\t48\t968\t920
                    2\t568\t48\t55\t7
                    2\t568\t16\t968\t952
                    2\t568\t16\t55\t39
                    2\t568\t72\t968\t896
                    2\t568\t72\t55\t-17
                    2\t568\t14\t968\t954
                    2\t568\t14\t55\t41
                    3\t333\t3\t964\t961
                    3\t333\t3\t305\t302
                    3\t333\t81\t964\t883
                    3\t333\t81\t305\t224
                    3\t333\t12\t964\t952
                    3\t333\t12\t305\t293
                    3\t333\t16\t964\t948
                    3\t333\t16\t305\t289
                    4\t371\t97\t171\t74
                    4\t371\t97\t104\t7
                    4\t371\t5\t171\t166
                    4\t371\t5\t104\t99
                    4\t371\t74\t171\t97
                    4\t371\t74\t104\t30
                    4\t371\t67\t171\t104
                    4\t371\t67\t104\t37
                    5\t251\t47\t279\t232
                    5\t251\t47\t198\t151
                    5\t251\t44\t279\t235
                    5\t251\t44\t198\t154
                    5\t251\t97\t279\t182
                    5\t251\t97\t198\t101
                    5\t251\t7\t279\t272
                    5\t251\t7\t198\t191
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x ts from long_sequence(5)) timestamp(ts)");
            execute("create table y as (select cast((x-1)/4 + 1 as int) c, abs(rnd_int() % 100) b from long_sequence(20))");
            execute("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))");

            assertQuery("select z.c, x.a, b, d, d-b from x join y on(c) join z on (c)")
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoin)
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    private void testJoinInnerAllTypes0(boolean fullFatJoin) throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tvch\tkk1\ta1\tb1\tc1\td1\te1\tf1\tg1\ti1\tj1\tk1\tl1\tm1\tn1\tvch1
                    1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t1746137611\ttrue\tL\t0.18852800970933203\t0.62260014\t777\t2015-08-19T06:10:07.386Z\t\t-7228768303272348606\t1970-01-01T00:00:00.000000Z\t15\t\tTNPHFL\tg>)5{l5J\\d;f7u
                    1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t1350645064\tfalse\tH\t0.2394591643144588\t0.90679234\t399\t\tMQNT\t8321277364671502705\t1970-01-01T00:16:40.000000Z\t50\t00000000 11 96 37 08 dd 98 ef 54 88 2a a2 ad e7\tVFGPPRGSXBH\t7^\uDBF8\uDD28\uDB37\uDC95Qǜbȶ\u05EC˟'ꋯɟ\uF6BE腠
                    1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t1373528915\ttrue\tW\t0.38509066982448115\tnull\t658\t2015-12-24T01:28:12.922Z\tJCKF\t-7745861463408011425\t1970-01-01T00:33:20.000000Z\t43\t\tKXEJCTIZKYFLU\tһτ鏻Ê띘Ѷ>͓\uDA8B\uDFC4︵Ƀ^
                    1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t}龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH\t1\t1120609071\ttrue\t\tnull\t0.13890666\t984\t2015-04-30T08:35:52.508Z\tOGMX\t-6929866925584807039\t1970-01-01T00:50:00.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\t\t$c~{=T@Xz
                    2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t2\t-1583707719\tfalse\tO\t0.03314618075579956\t0.838306\t711\t2015-10-17T09:06:19.735Z\tMQNT\t3396017735551392340\t1970-01-01T01:06:40.000000Z\t28\t00000000 4c 0e 8f f1 0c c5 60 b7 d1 5a 0c e9 db 51\tBZWNIJEEHRUG\t
                    2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t2\t-2016176825\ttrue\tT\tnull\t0.23567414\t813\t2015-12-27T00:19:42.415Z\tMQNT\t3464609208866088600\t1970-01-01T01:23:20.000000Z\t49\t\tFNUHNR\t\\0zpA
                    2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t2\t1947808961\ttrue\tE\t0.7783351753890267\t0.33046818\t725\t2015-12-22T01:44:08.182Z\t\t8809114770260886433\t1970-01-01T01:40:00.000000Z\t43\t00000000 92 a3 9b e3 cb c2 64 8a b0 35\tBOSEPGIUQZHEISQH\t"k[JYtuW/
                    2\t-1787109293\ttrue\tG\tnull\t0.80011207\t489\t2015-02-21T15:42:26.301Z\tCPSW\t-4692986177227268943\t1970-01-01T00:16:40.000000Z\t31\t00000000 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\tUVSDOTSEDY\tk\\<*i^!{\t2\t1271828924\tfalse\t\tnull\t0.43757588\t397\t2015-02-06T00:08:58.203Z\tUKLG\t6903369264246740332\t1970-01-01T01:56:40.000000Z\t50\t00000000 ad 79 87 fc 92 83 fc 88 f3 32\tRLPTY\t芊,\uD931\uDF48ҽ\uDA01\uDE60E죢魷
                    3\t-1172180184\tfalse\tS\t0.5891216483879789\t0.28200203\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:33:20.000000Z\t27\t\tIQBZXIOVIKJS\t\uDAB2\uDF79軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t3\t-481534978\tfalse\tI\t0.21224614178286005\tnull\t169\t2015-11-10T00:58:54.194Z\tMQNT\t-6128888161808465767\t1970-01-01T02:13:20.000000Z\t14\t\tKPYVGP\t>XzlGEYDcSIJLy
                    3\t-1172180184\tfalse\tS\t0.5891216483879789\t0.28200203\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:33:20.000000Z\t27\t\tIQBZXIOVIKJS\t\uDAB2\uDF79軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t3\t-1169915830\ttrue\tP\tnull\t0.058909357\t359\t2015-05-26T17:24:24.749Z\t\t-7350430133595690521\t1970-01-01T02:30:00.000000Z\t14\t00000000 35 3b 1c 9c 1d 5c c1 5d 2d 44 ea 00 81 c4 19 a1
                    00000010 ec\tSMIFDYPDK\t
                    3\t-1172180184\tfalse\tS\t0.5891216483879789\t0.28200203\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:33:20.000000Z\t27\t\tIQBZXIOVIKJS\t\uDAB2\uDF79軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t3\t-1505690678\tfalse\tR\t0.09854153834719315\t0.23285526\t82\t2015-06-03T01:01:00.230Z\tUKLG\t-7725099828175109832\t1970-01-01T02:46:40.000000Z\t27\t\tZUPVQFULMER\tM\uDB48\uDC78{ϸ\uD9F4\uDFB9\uDA0A\uDC7A\uDA76\uDC87>\uD8F0\uDF66Ҫb\uDBB1\uDEA3
                    3\t-1172180184\tfalse\tS\t0.5891216483879789\t0.28200203\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:33:20.000000Z\t27\t\tIQBZXIOVIKJS\t\uDAB2\uDF79軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t3\t600986867\tfalse\tM\t0.19823647700531244\tnull\t557\t2015-01-30T03:27:34.392Z\t\t5324839128380055812\t1970-01-01T03:03:20.000000Z\t25\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a
                    00000010 42 71 a3 7a\tDNZNLCNGZTOY\t1\uDA8F\uDC319믓˫ᡙ\uDBEC\uDE3B櫑߸!>\uD9F3\uDFD5a~=V
                    4\t862447505\ttrue\tV\t0.2711532808184136\t0.48524046\t556\t2015-12-06T14:13:54.132Z\tPEHN\t2387397055355257412\t1970-01-01T00:50:00.000000Z\t5\t00000000 34 e0 b0 e9 98 f7 67 62 28 60 b0 ec 0b 92\tOHNZHZ\t1CW#k1.xo\t4\t100444418\tfalse\tK\t0.28400807705010733\t0.5784462\t1015\t2015-05-21T09:22:31.780Z\tOGMX\t-2052253029650705565\t1970-01-01T03:20:00.000000Z\t18\t00000000 4b b7 e2 7f ab 6e 23 03 dd c7 d6\tDRHFBCZI\tB8^嘢\uD952\uDF63^寻&
                    4\t862447505\ttrue\tV\t0.2711532808184136\t0.48524046\t556\t2015-12-06T14:13:54.132Z\tPEHN\t2387397055355257412\t1970-01-01T00:50:00.000000Z\t5\t00000000 34 e0 b0 e9 98 f7 67 62 28 60 b0 ec 0b 92\tOHNZHZ\t1CW#k1.xo\t4\t473980\ttrue\tK\t0.7066431848881077\tnull\t486\t2015-04-18T21:58:29.097Z\t\t-8829329332761013903\t1970-01-01T03:36:40.000000Z\t27\t00000000 40 4e 8c 47 84 e9 c0 55 12 44 dc\tQCMZCCYVBDMQE\t:\uDACD\uDD7D%륤\uD8F4\uDC67YͥɈ\uDAB6\uDF33\uDB00\uDF8AϿ˄礏ɍ\uDB2C\uDD55\uD904\uDFA0
                    4\t862447505\ttrue\tV\t0.2711532808184136\t0.48524046\t556\t2015-12-06T14:13:54.132Z\tPEHN\t2387397055355257412\t1970-01-01T00:50:00.000000Z\t5\t00000000 34 e0 b0 e9 98 f7 67 62 28 60 b0 ec 0b 92\tOHNZHZ\t1CW#k1.xo\t4\t-45671426\tfalse\tG\t0.8825940193001498\tnull\t405\t2015-02-23T23:20:35.948Z\tOGMX\t1708771870007419078\t1970-01-01T03:53:20.000000Z\t40\t\tUIOXLQLUUZIZ\t
                    4\t862447505\ttrue\tV\t0.2711532808184136\t0.48524046\t556\t2015-12-06T14:13:54.132Z\tPEHN\t2387397055355257412\t1970-01-01T00:50:00.000000Z\t5\t00000000 34 e0 b0 e9 98 f7 67 62 28 60 b0 ec 0b 92\tOHNZHZ\t1CW#k1.xo\t4\t-1917313611\tfalse\tK\t0.1855717716409928\t0.69262904\t766\t2015-11-01T03:24:58.178Z\tMQNT\t-5387461693978657124\t1970-01-01T04:10:00.000000Z\t18\t\tGYDEQNNGKFDONP\t7?TPa,m9=
                    5\t-903066492\tfalse\tZ\t0.7260468106076399\t0.722936\t393\t2015-04-04T13:16:46.517Z\tPEHN\t-4058426794463997577\t1970-01-01T01:06:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ\t}$\uDA43\uDFF0-㔍x\t5\t-642526996\ttrue\tG\t0.38014703172702147\tnull\t251\t2015-05-22T02:07:31.345Z\tOGMX\t7509515980141386401\t1970-01-01T04:26:40.000000Z\t21\t00000000 c2 a2 b4 8e 99 a8 2b 8d 35 c5 85 9a\tTKIBWFC\t fF.R
                    5\t-903066492\tfalse\tZ\t0.7260468106076399\t0.722936\t393\t2015-04-04T13:16:46.517Z\tPEHN\t-4058426794463997577\t1970-01-01T01:06:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ\t}$\uDA43\uDFF0-㔍x\t5\t671650197\ttrue\tC\t0.2977278793266547\t0.4953196\t454\t2015-06-27T19:24:50.416Z\t\t-8775249844552344320\t1970-01-01T04:43:20.000000Z\t25\t00000000 77 91 b2 de 58 45 d0 1b 58 be 33 92\t\tC\uDB4E\uDC43\uDAAD\uDE0A\uE916G[ꫭ\uDA99\uDC83\uD8F9\uDF14߂ؠ葶\u2433\uEE49
                    5\t-903066492\tfalse\tZ\t0.7260468106076399\t0.722936\t393\t2015-04-04T13:16:46.517Z\tPEHN\t-4058426794463997577\t1970-01-01T01:06:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ\t}$\uDA43\uDFF0-㔍x\t5\t-671347440\tfalse\tC\t0.6455308455173533\t0.5938364\t64\t2015-04-01T22:42:30.344Z\tOGMX\t7356286536462170873\t1970-01-01T05:00:00.000000Z\t47\t00000000 92 08 f1 96 7f a0 cf 00 74 7c 32 16 38 00\tZDYHD\t❍\uDB17\uDC72쬉반+Eږ胵zݒ邍\uF7F86H
                    5\t-903066492\tfalse\tZ\t0.7260468106076399\t0.722936\t393\t2015-04-04T13:16:46.517Z\tPEHN\t-4058426794463997577\t1970-01-01T01:06:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ\t}$\uDA43\uDFF0-㔍x\t5\t-2033189695\tfalse\tK\t0.1672705743728916\t0.28764933\t271\t2015-03-17T09:46:55.817Z\tOGMX\t-7429841700499010243\t1970-01-01T05:16:40.000000Z\t14\t\tSWHLSWPF\tJ\uD9FB\uDE6C\uDA85\uDF29䚭ϸ\uD9A8\uDFFBi⟃2
                    """;

            execute(
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

            execute(
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
            assertQuery("select * from x join y on (kk)")
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoin)
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    private void testJoinInnerDifferentColumnNames0(boolean fullFatJoin) throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    c\ta\tb\td\tcolumn
                    1\t120\t39\t0\t-39
                    1\t120\t39\t50\t11
                    1\t120\t42\t0\t-42
                    1\t120\t42\t50\t8
                    1\t120\t71\t0\t-71
                    1\t120\t71\t50\t-21
                    1\t120\t6\t0\t-6
                    1\t120\t6\t50\t44
                    2\t568\t48\t968\t920
                    2\t568\t48\t55\t7
                    2\t568\t16\t968\t952
                    2\t568\t16\t55\t39
                    2\t568\t72\t968\t896
                    2\t568\t72\t55\t-17
                    2\t568\t14\t968\t954
                    2\t568\t14\t55\t41
                    3\t333\t3\t964\t961
                    3\t333\t3\t305\t302
                    3\t333\t81\t964\t883
                    3\t333\t81\t305\t224
                    3\t333\t12\t964\t952
                    3\t333\t12\t305\t293
                    3\t333\t16\t964\t948
                    3\t333\t16\t305\t289
                    4\t371\t97\t171\t74
                    4\t371\t97\t104\t7
                    4\t371\t5\t171\t166
                    4\t371\t5\t104\t99
                    4\t371\t74\t171\t97
                    4\t371\t74\t104\t30
                    4\t371\t67\t171\t104
                    4\t371\t67\t104\t37
                    5\t251\t47\t279\t232
                    5\t251\t47\t198\t151
                    5\t251\t44\t279\t235
                    5\t251\t44\t198\t154
                    5\t251\t97\t279\t182
                    5\t251\t97\t198\t101
                    5\t251\t7\t279\t272
                    5\t251\t7\t198\t191
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))");
            execute("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))");
            execute("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))");
            assertQuery("select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c)")
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoin)
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    private void testJoinInnerInnerFilter0(boolean fullFatJoin) throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    c\ta\tb\td\tcolumn
                    1\t120\t6\t0\t-6
                    1\t120\t6\t50\t44
                    2\t568\t14\t55\t41
                    2\t568\t14\t968\t954
                    2\t568\t16\t55\t39
                    2\t568\t16\t968\t952
                    3\t333\t3\t305\t302
                    3\t333\t3\t964\t961
                    3\t333\t12\t305\t293
                    3\t333\t12\t964\t952
                    3\t333\t16\t305\t289
                    3\t333\t16\t964\t948
                    4\t371\t5\t104\t99
                    4\t371\t5\t171\t166
                    5\t251\t7\t198\t191
                    5\t251\t7\t279\t272
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))");
            execute("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))");
            execute("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(16))");

            // filter is applied to intermediate join result
            assertQuery("select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where y.b < 20 order by z.c, b, d")
                    .noLeakCheck()
                    .returns(expected);

            execute("insert into x select cast(x+6 as int) c, abs(rnd_int() % 650) a from long_sequence(3)");
            execute("insert into y select cast((x+19)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(16)");
            execute("insert into z select cast((x+15)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(2)");

            assertQuery("select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where y.b < 20 order by z.c, b, d")
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoin)
                    .expectSize()
                    .returns(expected +
                            "7\t253\t14\t228\t214\n" +
                            "7\t253\t14\t723\t709\n" +
                            "8\t431\t0\t348\t348\n" +
                            "8\t431\t0\t790\t790\n" +
                            "9\t100\t8\t456\t448\n" +
                            "9\t100\t8\t667\t659\n" +
                            "9\t100\t19\t456\t437\n" +
                            "9\t100\t19\t667\t648\n");
        });
    }

    private void testJoinInnerLastFilter0(boolean fullFatJoin) throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    c\ta\tb\td\tcolumn
                    2\t568\t72\t968\t896
                    2\t568\t48\t968\t920
                    2\t568\t16\t968\t952
                    2\t568\t14\t968\t954
                    3\t333\t81\t305\t224
                    3\t333\t16\t305\t289
                    3\t333\t12\t305\t293
                    3\t333\t3\t305\t302
                    3\t333\t81\t964\t883
                    3\t333\t16\t964\t948
                    3\t333\t12\t964\t952
                    3\t333\t3\t964\t961
                    4\t371\t67\t171\t104
                    4\t371\t5\t171\t166
                    5\t251\t97\t198\t101
                    5\t251\t47\t198\t151
                    5\t251\t44\t198\t154
                    5\t251\t97\t279\t182
                    5\t251\t7\t198\t191
                    5\t251\t47\t279\t232
                    5\t251\t44\t279\t235
                    5\t251\t7\t279\t272
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))");
            execute("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))");
            execute("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))");

            // filter is applied to final join result
            assertQuery("select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where d-b > 100 order by z.c, d-b")
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoin)
                    .returns(expected);
        });
    }

    private void testJoinInnerNoSlaveRecords0(boolean fullFatJoin) throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    c\ta\tb
                    2\t568\t16
                    2\t568\t72
                    4\t371\t3
                    4\t371\t14
                    6\t439\t12
                    6\t439\t81
                    8\t521\t16
                    8\t521\t97
                    10\t598\t5
                    10\t598\t74
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(10))");
            execute("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))");

            assertQuery("select x.c, x.a, b from x join y on y.m = x.c order by x.c, b")
                    .noLeakCheck()
                    .returns(expected);

            execute("insert into x select cast(x+10 as int) c, abs(rnd_int() % 650) a from long_sequence(4)");
            execute("insert into y select x, cast(2*((x-1+10)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(6)");

            assertQuery("select x.c, x.a, b from x join y on y.m = x.c order by x.c, b")
                    .fullFatJoins(fullFatJoin)
                    .noLeakCheck()
                    .expectSize(fullFatJoin) // full-fat join materializes (known size); optimized path streams (-1)
                    .returns(expected +
                            "12\t347\t0\n" +
                            "12\t347\t7\n" +
                            "14\t197\t50\n" +
                            "14\t197\t68\n");
        });
    }

    private void testJoinInnerOnSymbol0(boolean fullFatJoin) throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    xc\tzc\tyc\ta\tb\td\tcolumn
                    \t\t\t598\t69\t2\t-67
                    \t\t\t521\t69\t2\t-67
                    \t\t\t598\t68\t2\t-66
                    \t\t\t521\t68\t2\t-66
                    \t\t\t598\t53\t2\t-51
                    \t\t\t521\t53\t2\t-51
                    \t\t\t598\t3\t2\t-1
                    \t\t\t521\t3\t2\t-1
                    \t\t\t598\t69\t8\t-61
                    \t\t\t521\t69\t8\t-61
                    \t\t\t598\t68\t8\t-60
                    \t\t\t521\t68\t8\t-60
                    \t\t\t598\t53\t8\t-45
                    \t\t\t521\t53\t8\t-45
                    \t\t\t598\t3\t8\t5
                    \t\t\t521\t3\t8\t5
                    \t\t\t598\t69\t540\t471
                    \t\t\t521\t69\t540\t471
                    \t\t\t598\t68\t540\t472
                    \t\t\t521\t68\t540\t472
                    \t\t\t598\t53\t540\t487
                    \t\t\t521\t53\t540\t487
                    \t\t\t598\t3\t540\t537
                    \t\t\t521\t3\t540\t537
                    \t\t\t598\t69\t908\t839
                    \t\t\t521\t69\t908\t839
                    \t\t\t598\t68\t908\t840
                    \t\t\t521\t68\t908\t840
                    \t\t\t598\t53\t908\t855
                    \t\t\t521\t53\t908\t855
                    \t\t\t598\t3\t908\t905
                    \t\t\t521\t3\t908\t905
                    A\tA\tA\t568\t74\t263\t189
                    A\tA\tA\t568\t71\t263\t192
                    A\tA\tA\t568\t54\t263\t209
                    A\tA\tA\t568\t12\t263\t251
                    A\tA\tA\t568\t74\t319\t245
                    A\tA\tA\t568\t71\t319\t248
                    A\tA\tA\t568\t54\t319\t265
                    A\tA\tA\t568\t12\t319\t307
                    A\tA\tA\t568\t74\t456\t382
                    A\tA\tA\t568\t71\t456\t385
                    A\tA\tA\t568\t54\t456\t402
                    A\tA\tA\t568\t12\t456\t444
                    B\tB\tB\t439\t97\t467\t370
                    B\tB\tB\t371\t97\t467\t370
                    B\tB\tB\t439\t97\t467\t370
                    B\tB\tB\t371\t97\t467\t370
                    B\tB\tB\t439\t79\t467\t388
                    B\tB\tB\t371\t79\t467\t388
                    B\tB\tB\t439\t72\t467\t395
                    B\tB\tB\t371\t72\t467\t395
                    B\tB\tB\t439\t97\t667\t570
                    B\tB\tB\t371\t97\t667\t570
                    B\tB\tB\t439\t97\t667\t570
                    B\tB\tB\t371\t97\t667\t570
                    B\tB\tB\t439\t79\t667\t588
                    B\tB\tB\t371\t79\t667\t588
                    B\tB\tB\t439\t72\t667\t595
                    B\tB\tB\t371\t72\t667\t595
                    B\tB\tB\t439\t97\t703\t606
                    B\tB\tB\t371\t97\t703\t606
                    B\tB\tB\t439\t97\t703\t606
                    B\tB\tB\t371\t97\t703\t606
                    B\tB\tB\t439\t79\t703\t624
                    B\tB\tB\t371\t79\t703\t624
                    B\tB\tB\t439\t72\t703\t631
                    B\tB\tB\t371\t72\t703\t631
                    B\tB\tB\t439\t97\t842\t745
                    B\tB\tB\t371\t97\t842\t745
                    B\tB\tB\t439\t97\t842\t745
                    B\tB\tB\t371\t97\t842\t745
                    B\tB\tB\t439\t79\t842\t763
                    B\tB\tB\t371\t79\t842\t763
                    B\tB\tB\t439\t72\t842\t770
                    B\tB\tB\t371\t72\t842\t770
                    B\tB\tB\t439\t97\t933\t836
                    B\tB\tB\t371\t97\t933\t836
                    B\tB\tB\t439\t97\t933\t836
                    B\tB\tB\t371\t97\t933\t836
                    B\tB\tB\t439\t79\t933\t854
                    B\tB\tB\t371\t79\t933\t854
                    B\tB\tB\t439\t72\t933\t861
                    B\tB\tB\t371\t72\t933\t861
                    """;

            execute("create table x as (select rnd_symbol('A','B',null,'D') c, abs(rnd_int() % 650) a from long_sequence(5))");
            execute("create table y as (select rnd_symbol('B','A',null,'D') m, abs(rnd_int() % 100) b from long_sequence(20))");
            execute("create table z as (select rnd_symbol('D','B',null,'A') c, abs(rnd_int() % 1000) d from long_sequence(16))");

            // filter is applied to intermediate join result
            assertQuery("select x.c xc, z.c zc, y.m yc, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) order by x.c, d, d-b")
                    .noLeakCheck()
                    .returns(expected);

            execute("insert into x select rnd_symbol('L','K','P') c, abs(rnd_int() % 650) a from long_sequence(3)");
            execute("insert into y select rnd_symbol('P','L','K') m, abs(rnd_int() % 100) b from long_sequence(6)");
            execute("insert into z select rnd_symbol('K','P','L') c, abs(rnd_int() % 1000) d from long_sequence(6)");

            assertQuery("select x.c xc, z.c zc, y.m yc, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) order by x.c, d, d-b")
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoin)
                    .expectSize()
                    .returns("""
                            xc\tzc\tyc\ta\tb\td\tcolumn
                            \t\t\t521\t69\t2\t-67
                            \t\t\t598\t69\t2\t-67
                            \t\t\t521\t68\t2\t-66
                            \t\t\t598\t68\t2\t-66
                            \t\t\t521\t53\t2\t-51
                            \t\t\t598\t53\t2\t-51
                            \t\t\t521\t3\t2\t-1
                            \t\t\t598\t3\t2\t-1
                            \t\t\t521\t69\t8\t-61
                            \t\t\t598\t69\t8\t-61
                            \t\t\t521\t68\t8\t-60
                            \t\t\t598\t68\t8\t-60
                            \t\t\t521\t53\t8\t-45
                            \t\t\t598\t53\t8\t-45
                            \t\t\t521\t3\t8\t5
                            \t\t\t598\t3\t8\t5
                            \t\t\t521\t69\t540\t471
                            \t\t\t598\t69\t540\t471
                            \t\t\t521\t68\t540\t472
                            \t\t\t598\t68\t540\t472
                            \t\t\t521\t53\t540\t487
                            \t\t\t598\t53\t540\t487
                            \t\t\t521\t3\t540\t537
                            \t\t\t598\t3\t540\t537
                            \t\t\t521\t69\t908\t839
                            \t\t\t598\t69\t908\t839
                            \t\t\t521\t68\t908\t840
                            \t\t\t598\t68\t908\t840
                            \t\t\t521\t53\t908\t855
                            \t\t\t598\t53\t908\t855
                            \t\t\t521\t3\t908\t905
                            \t\t\t598\t3\t908\t905
                            A\tA\tA\t568\t74\t263\t189
                            A\tA\tA\t568\t71\t263\t192
                            A\tA\tA\t568\t54\t263\t209
                            A\tA\tA\t568\t12\t263\t251
                            A\tA\tA\t568\t74\t319\t245
                            A\tA\tA\t568\t71\t319\t248
                            A\tA\tA\t568\t54\t319\t265
                            A\tA\tA\t568\t12\t319\t307
                            A\tA\tA\t568\t74\t456\t382
                            A\tA\tA\t568\t71\t456\t385
                            A\tA\tA\t568\t54\t456\t402
                            A\tA\tA\t568\t12\t456\t444
                            B\tB\tB\t371\t97\t467\t370
                            B\tB\tB\t371\t97\t467\t370
                            B\tB\tB\t439\t97\t467\t370
                            B\tB\tB\t439\t97\t467\t370
                            B\tB\tB\t371\t79\t467\t388
                            B\tB\tB\t439\t79\t467\t388
                            B\tB\tB\t371\t72\t467\t395
                            B\tB\tB\t439\t72\t467\t395
                            B\tB\tB\t371\t97\t667\t570
                            B\tB\tB\t371\t97\t667\t570
                            B\tB\tB\t439\t97\t667\t570
                            B\tB\tB\t439\t97\t667\t570
                            B\tB\tB\t371\t79\t667\t588
                            B\tB\tB\t439\t79\t667\t588
                            B\tB\tB\t371\t72\t667\t595
                            B\tB\tB\t439\t72\t667\t595
                            B\tB\tB\t371\t97\t703\t606
                            B\tB\tB\t371\t97\t703\t606
                            B\tB\tB\t439\t97\t703\t606
                            B\tB\tB\t439\t97\t703\t606
                            B\tB\tB\t371\t79\t703\t624
                            B\tB\tB\t439\t79\t703\t624
                            B\tB\tB\t371\t72\t703\t631
                            B\tB\tB\t439\t72\t703\t631
                            B\tB\tB\t371\t97\t842\t745
                            B\tB\tB\t371\t97\t842\t745
                            B\tB\tB\t439\t97\t842\t745
                            B\tB\tB\t439\t97\t842\t745
                            B\tB\tB\t371\t79\t842\t763
                            B\tB\tB\t439\t79\t842\t763
                            B\tB\tB\t371\t72\t842\t770
                            B\tB\tB\t439\t72\t842\t770
                            B\tB\tB\t371\t97\t933\t836
                            B\tB\tB\t371\t97\t933\t836
                            B\tB\tB\t439\t97\t933\t836
                            B\tB\tB\t439\t97\t933\t836
                            B\tB\tB\t371\t79\t933\t854
                            B\tB\tB\t439\t79\t933\t854
                            B\tB\tB\t371\t72\t933\t861
                            B\tB\tB\t439\t72\t933\t861
                            L\tL\tL\t148\t52\t121\t69
                            L\tL\tL\t148\t38\t121\t83
                            """);

        });
    }

    private void testJoinInnerPostJoinFilter0(boolean fullFatJoin) throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    c\ta\tb\td\tcolumn
                    1\t120\t6\t0\t126
                    1\t120\t6\t50\t126
                    1\t120\t39\t0\t159
                    1\t120\t39\t50\t159
                    1\t120\t42\t0\t162
                    1\t120\t42\t50\t162
                    1\t120\t71\t0\t191
                    1\t120\t71\t50\t191
                    5\t251\t7\t198\t258
                    5\t251\t7\t279\t258
                    5\t251\t44\t198\t295
                    5\t251\t44\t279\t295
                    5\t251\t47\t198\t298
                    5\t251\t47\t279\t298
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5))");
            execute("create table y as (select cast((x-1)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(20))");
            execute("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(16))");

            // filter is applied to intermediate join result
            assertQuery("select z.c, x.a, b, d, a+b from x join y on y.m = x.c join z on (c) where a+b < 300 order by z.c, b, d")
                    .noLeakCheck()
                    .returns(expected);

            execute("insert into x select cast(x+6 as int) c, abs(rnd_int() % 650) a from long_sequence(3)");
            execute("insert into y select cast((x+19)/4 + 1 as int) m, abs(rnd_int() % 100) b from long_sequence(16)");
            execute("insert into z select cast((x+15)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(2)");

            assertQuery("select z.c, x.a, b, d, a+b from x join y on y.m = x.c join z on (c) where a+b < 300 order by z.c, b, d")
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoin)
                    .expectSize()
                    .returns(expected +
                            "7\t253\t14\t228\t267\n" +
                            "7\t253\t14\t723\t267\n" +
                            "7\t253\t35\t228\t288\n" +
                            "7\t253\t35\t723\t288\n" +
                            "9\t100\t8\t456\t108\n" +
                            "9\t100\t8\t667\t108\n" +
                            "9\t100\t19\t456\t119\n" +
                            "9\t100\t19\t667\t119\n" +
                            "9\t100\t38\t456\t138\n" +
                            "9\t100\t38\t667\t138\n" +
                            "9\t100\t63\t456\t163\n" +
                            "9\t100\t63\t667\t163\n");
        });
    }

    private void testJoinOuterAllTypes0(boolean fullFatJoins) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (select" +
                            " x id, " +
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

            execute(
                    "create table y as (select" +
                            " x id, " +
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
                            " from long_sequence(11))"
            );

            final String expected = """
                    id\tkk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tid1\tkk1\ta1\tb1\tc1\td1\te1\tf1\tg1\ti1\tj1\tk1\tl1\tm1\tn1
                    10\t10\t-1915752164\tfalse\tI\t0.8786111112537701\t0.9966377\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:30:00.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t\t10\t10\t-682294338\ttrue\tG\t0.9153044839960652\t0.79431856\t646\t2015-11-20T14:44:35.439Z\t\t8432832362817764490\t1970-01-01T02:30:00.000000Z\t38\t\tBOSEPGIUQZHEISQH
                    10\t10\t-1915752164\tfalse\tI\t0.8786111112537701\t0.9966377\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:30:00.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t\t9\t10\t815018557\tfalse\t\t0.07383464174908916\t0.8791439\t187\t\tYRZL\t8725895078168602870\t1970-01-01T02:13:20.000000Z\t36\t\tVLOMPBETTTKRIV
                    9\t9\t976011946\ttrue\tU\t0.24001459007748394\t0.9292491\t379\t\tVTJW\t3820631780839257855\t1970-01-01T02:13:20.000000Z\t12\t00000000 8a b3 14 cd 47 0b 0c 39 12 f7 05 10 f4\tGMXUKLGMXSLUQDYO\tnull\tnull\tnull\tfalse\t\tnull\tnull\t0\t\t\tnull\t\t0\t\t
                    8\t8\t-1234141625\tfalse\tC\t0.06381657870188628\t0.76062524\t397\t2015-02-14T21:43:16.924Z\tHYRX\t-8888027247206813045\t1970-01-01T01:56:40.000000Z\t10\t00000000 b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4\tUIZULIGYVFZFK\t8\t8\t450540087\tfalse\t\tnull\t0.13535291\t932\t\t\t-6426355179359373684\t1970-01-01T01:56:40.000000Z\t30\t\tKVSBEGM
                    8\t8\t-1234141625\tfalse\tC\t0.06381657870188628\t0.76062524\t397\t2015-02-14T21:43:16.924Z\tHYRX\t-8888027247206813045\t1970-01-01T01:56:40.000000Z\t10\t00000000 b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4\tUIZULIGYVFZFK\t7\t8\t882350590\ttrue\tZ\tnull\t0.033146143\t575\t2015-08-28T02:22:07.682Z\tHHIU\t-6342128731155487317\t1970-01-01T01:40:00.000000Z\t26\t00000000 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a 0c\tVFDBZW
                    7\t7\t-2077041000\ttrue\tM\t0.7340656260730631\t0.50258905\t345\t2015-02-16T05:23:30.407Z\t\t-8534688874718947140\t1970-01-01T01:40:00.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE\tnull\tnull\tnull\tfalse\t\tnull\tnull\t0\t\t\tnull\t\t0\t\t
                    6\t6\t1431425139\tfalse\t\t0.30716667810043663\t0.4274704\t181\t2015-07-26T11:59:20.003Z\t\t-8546113611224784332\t1970-01-01T01:23:20.000000Z\t11\t00000000 d8 57 91 88 28 a5 18 93 bd 0b\tJOXPKRGIIHYH\t6\t6\t-1751905058\tfalse\tV\t0.8977957942059742\t0.18967962\t262\t2015-06-14T03:59:52.156Z\tHHIU\t8231256356538221412\t1970-01-01T01:23:20.000000Z\t13\t\tXFSUWPNXH
                    6\t6\t1431425139\tfalse\t\t0.30716667810043663\t0.4274704\t181\t2015-07-26T11:59:20.003Z\t\t-8546113611224784332\t1970-01-01T01:23:20.000000Z\t11\t00000000 d8 57 91 88 28 a5 18 93 bd 0b\tJOXPKRGIIHYH\t5\t6\t1159512064\ttrue\tH\t0.8124306844969832\t0.0032519698\t432\t2015-09-12T17:45:31.519Z\tHHIU\t7964539812331152681\t1970-01-01T01:06:40.000000Z\t8\t\tWLEVMLKC
                    5\t5\t-2088317486\tfalse\tU\t0.7446000371089992\tnull\t651\t2015-07-18T10:50:24.009Z\tVTJW\t3446015290144635451\t1970-01-01T01:06:40.000000Z\t8\t00000000 92 fe 69 38 e1 77 9a e7 0c 89 14 58\tUMLGLHMLLEOY\tnull\tnull\tnull\tfalse\t\tnull\tnull\t0\t\t\tnull\t\t0\t\t
                    4\t4\t-1172180184\tfalse\tS\t0.5891216483879789\t0.28200203\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\t4\t4\t263487884\ttrue\t\tnull\t0.948288\t59\t2015-01-20T06:18:18.583Z\t\t-5873213601796545477\t1970-01-01T00:50:00.000000Z\t26\t00000000 4a c9 cf fb 9d 63 ca 94 00 6b dd\tHHGGIWH
                    4\t4\t-1172180184\tfalse\tS\t0.5891216483879789\t0.28200203\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\t3\t4\t325316\tfalse\tG\t0.27068535446692277\t0.0031075478\t809\t2015-02-24T12:10:43.199Z\t\t-4990885278588247665\t1970-01-01T00:33:20.000000Z\t8\t00000000 98 80 85 20 53 3b 51 9d 5d 28 ac 02 2e fe\tQQEMXDKXEJCTIZ
                    3\t3\t161592763\ttrue\tZ\t0.18769708157331322\t0.16381371\t137\t2015-03-12T05:14:11.462Z\t\t7522482991756933150\t1970-01-01T00:33:20.000000Z\t43\t00000000 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3 b3 59
                    00000010 8e e5 61 2f\tQOLYXWC\tnull\tnull\tnull\tfalse\t\tnull\tnull\t0\t\t\tnull\t\t0\t\t
                    2\t2\t-1271909747\ttrue\tB\tnull\t0.1250304\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t2\t2\t1704158532\tfalse\tN\t0.43493246663794993\t0.9611983\t344\t2015-09-09T21:39:05.530Z\tHHIU\t-4645139889518544281\t1970-01-01T00:16:40.000000Z\t47\t\tGGIJYDV
                    2\t2\t-1271909747\ttrue\tB\tnull\t0.1250304\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t1\t2\t415709351\tfalse\tM\t0.5626370294064983\t0.76532555\t712\t\tGGLN\t6235849401126045090\t1970-01-01T00:00:00.000000Z\t36\t00000000 62 e1 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43\tIZJSVTNP
                    1\t1\t1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\tnull\tnull\tnull\tfalse\t\tnull\tnull\t0\t\t\tnull\t\t0\t\t
                    """;
            final String fullJoinExpected = expected +
                    "null\tnull\tnull\tfalse\t\tnull\tnull\t0\t\t\tnull\t\t0\t\t\t11\t12\t-2099411412\ttrue\t\tnull\tnull\t119\t2015-09-08T05:51:33.432Z\tYRZL\t8196152051414471878\t1970-01-01T02:46:40.000000Z\t17\t00000000 05 2b 73 51 cf c3 7e c0 1d 6c a9 65 81 ad 79 87\tYWXBBZVRLPT\n";

            // filter is applied to final join result
            assertQuery("select * from x left join y on (kk) order by x.id desc, y.id desc")
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoins)
                    .returns(expected);

            assertQuery("select x.*, y.* from y right join x on (kk) order by x.id desc, y.id desc")
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoins)
                    .returns(expected);

            assertQuery("select * from x full join y on (kk) order by x.id desc, y.id desc")
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoins)
                    .returns(fullJoinExpected);
        });
    }

    private void testJoinOuterNoSlaveRecords0(boolean fullFatJoins) throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    c\ta\tb
                    1\t120\tnull
                    2\t568\t16
                    2\t568\t72
                    3\t333\tnull
                    4\t371\t3
                    4\t371\t14
                    5\t251\tnull
                    6\t439\t12
                    6\t439\t81
                    7\t42\tnull
                    8\t521\t16
                    8\t521\t97
                    9\t356\tnull
                    10\t598\t5
                    10\t598\t74
                    """;

            execute("create table x as (select cast(x as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x ts from long_sequence(10)) timestamp(ts)");
            execute("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))");

            // master records should be filtered out because slave records missing
            assertQuery("select x.c, x.a, b from x left join y on y.m = x.c order by x.c, b")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select x.c, x.a, b from y right join x on y.m = x.c order by x.c, b")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select x.c, x.a, b from y full join x on y.m = x.c order by x.c, b")
                    .noLeakCheck()
                    .returns(expected);

            execute("insert into x select * from (select cast(x+10 as int) c, abs(rnd_int() % 650) a, to_timestamp('2018-03-01', 'yyyy-MM-dd') + x + 10 ts from long_sequence(4)) timestamp(ts)");
            execute("insert into y select x, cast(2*((x-1+10)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(6)");

            assertQuery("select x.c, x.a, b from x left join y on y.m = x.c order by x.c, b")
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoins)
                    .returns(expected +
                            "11\t467\tnull\n" +
                            "12\t347\t0\n" +
                            "12\t347\t7\n" +
                            "13\t244\tnull\n" +
                            "14\t197\t50\n" +
                            "14\t197\t68\n");

            assertQuery("select x.c, x.a, b from y right join x on y.m = x.c order by x.c, b")
                    .noLeakCheck()
                    .fullFatJoins(fullFatJoins)
                    .returns(expected +
                            "11\t467\tnull\n" +
                            "12\t347\t0\n" +
                            "12\t347\t7\n" +
                            "13\t244\tnull\n" +
                            "14\t197\t50\n" +
                            "14\t197\t68\n");
        });
    }

    private void testTypeMismatch0(boolean fullFatJoins) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x c, abs(rnd_int() % 650) a from long_sequence(5))");
            execute("create table y as (select cast((x-1)/4 + 1 as int) c, abs(rnd_int() % 100) b from long_sequence(20))");
            execute("create table z as (select cast((x-1)/2 + 1 as int) c, abs(rnd_int() % 1000) d from long_sequence(40))");
            assertExceptionNoLeakCheck(
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
