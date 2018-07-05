/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin;

import com.questdb.cairo.AbstractCairoTest;
import com.questdb.cairo.Engine;
import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.common.ColumnType;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.griffin.engine.functions.rnd.SharedRandom;
import com.questdb.std.BinarySequence;
import com.questdb.std.LongList;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class SqlCodeGeneratorTest extends AbstractCairoTest {

    private static final BindVariableService bindVariableService = new BindVariableService();
    private static final Engine engine = new Engine(configuration);
    private static final SqlCompiler compiler = new SqlCompiler(engine, configuration);
    private static final LongList rows = new LongList();

    @Before
    public void setUp2() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testFilterConstantFalse() throws Exception {
        final String expected = "a\tb\tk\n";
        assertQuery(expected,
                "select * from x o where 10 < 8",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)" +
                        ")" +
                        "), index(b) timestamp(k)",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " rnd_symbol(5,4,4,1)," +
                        " timestamp_sequence(to_timestamp('2019', 'yyyy'), 1000000000) timestamp" +
                        " from long_sequence(50)" +
                        ") timestamp(timestamp)",
                expected);
    }

    @Test
    public void testFilterInterval() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "84.452581772111\tPEHN\t1970-01-01T03:36:40.000000Z\n" +
                        "97.501988537251\t\t1970-01-01T03:53:20.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-01T04:10:00.000000Z\n",
                "select * from x o where k = '1970-01-01T03:36:40;45m'",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)" +
                        ")" +
                        "), index(b) timestamp(k)",
                "k");
    }

    @Test
    public void testFilterIntervalAndField() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "84.452581772111\tPEHN\t1970-01-01T03:36:40.000000Z\n" +
                        "97.501988537251\t\t1970-01-01T03:53:20.000000Z\n",
                "select * from x o where k = '1970-01-01T03:36:40;45m' and a > 50",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)" +
                        ")" +
                        "), index(b) timestamp(k)",
                "k");
    }

    @Test
    public void testFilterIntrinsicFalse() throws Exception {
        assertQuery("a\tb\tk\n",
                "select * from x o where o.b in ('HYRX','PEHN', null) and a < a",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,1), 'k', timestamp_sequence(to_timestamp(0), 1000000000))), index(b)",
                null);
    }

    @Test
    public void testFilterKeyAndFields() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "11.427984775756\t\t1970-01-01T00:00:00.000000Z\n" +
                        "32.881769076795\t\t1970-01-01T01:23:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-01T02:30:00.000000Z\n" +
                        "26.922103479745\t\t1970-01-01T03:03:20.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-01T04:10:00.000000Z\n" +
                        "45.634456960908\t\t1970-01-01T05:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-01T05:16:40.000000Z\n",
                "select * from x o where o.b in ('HYRX','PEHN', null) and a < 50",
                "create table x as (" +
                        "select" +
                        " *" +
                        " from" +
                        " random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,1), 'k', timestamp_sequence(to_timestamp(0), 1000000000))" +
                        ")," +
                        " index(b)",
                null,
                "insert into x (a,b)" +
                        " select" +
                        " rnd_double(0)*100," +
                        " rnd_symbol(5,4,4,1)" +
                        " from" +
                        " long_sequence(10)",
                "a\tb\tk\n" +
                        "11.427984775756\t\t1970-01-01T00:00:00.000000Z\n" +
                        "32.881769076795\t\t1970-01-01T01:23:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-01T02:30:00.000000Z\n" +
                        "26.922103479745\t\t1970-01-01T03:03:20.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-01T04:10:00.000000Z\n" +
                        "45.634456960908\t\t1970-01-01T05:00:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-01T05:16:40.000000Z\n" +
                        "44.804689668614\t\t\n");
    }

    @Test
    public void testFilterKeyAndInterval() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "84.452581772111\tPEHN\t1970-01-01T03:36:40.000000Z\n" +
                        "97.501988537251\t\t1970-01-01T03:53:20.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-01T04:10:00.000000Z\n",
                "select * from x o where o.b in ('HYRX','PEHN', null) and k = '1970-01-01T03:36:40;45m'",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)" +
                        ")" +
                        "), index(b) timestamp(k)",
                "k");
    }

    @Test
    public void testFilterMultipleKeyValues() throws Exception {
        final String expected1 = "a\tb\tk\n" +
                "11.427984775756\t\t1970-01-01T00:00:00.000000Z\n" +
                "70.943604871712\tPEHN\t1970-01-01T00:50:00.000000Z\n" +
                "87.996347253916\t\t1970-01-01T01:06:40.000000Z\n" +
                "32.881769076795\t\t1970-01-01T01:23:20.000000Z\n" +
                "97.711031460512\tHYRX\t1970-01-01T01:40:00.000000Z\n" +
                "81.468079445006\tPEHN\t1970-01-01T01:56:40.000000Z\n" +
                "57.934663268622\t\t1970-01-01T02:13:20.000000Z\n" +
                "12.026122412833\tHYRX\t1970-01-01T02:30:00.000000Z\n" +
                "26.922103479745\t\t1970-01-01T03:03:20.000000Z\n" +
                "52.984059417621\t\t1970-01-01T03:20:00.000000Z\n" +
                "84.452581772111\tPEHN\t1970-01-01T03:36:40.000000Z\n" +
                "97.501988537251\t\t1970-01-01T03:53:20.000000Z\n" +
                "49.005104498852\tPEHN\t1970-01-01T04:10:00.000000Z\n" +
                "80.011211397392\t\t1970-01-01T04:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-01T04:43:20.000000Z\n" +
                "45.634456960908\t\t1970-01-01T05:00:00.000000Z\n" +
                "40.455469747939\t\t1970-01-01T05:16:40.000000Z\n";

        assertQuery(expected1,
                "select * from x o where o.b in ('HYRX','PEHN', null)",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,1), 'k', timestamp_sequence(to_timestamp(0), 1000000000))), index(b)",
                null,
                "insert into x (a,b)" +
                        " select" +
                        " rnd_double(0)*100," +
                        " rnd_symbol(5,4,4,1)" +
                        " from" +
                        " long_sequence(10)",
                expected1 +
                        "54.491550215189\t\t\n" +
                        "76.923818943378\t\t\n" +
                        "58.912164838798\t\t\n" +
                        "44.804689668614\t\t\n" +
                        "89.409171265819\t\t\n");
    }

    @Test
    public void testFilterNullKeyValue() throws Exception {
        final String expected = "a\tb\n" +
                "11.427984775756\t\n" +
                "87.996347253916\t\n" +
                "32.881769076795\t\n" +
                "57.934663268622\t\n" +
                "26.922103479745\t\n" +
                "52.984059417621\t\n" +
                "97.501988537251\t\n" +
                "80.011211397392\t\n" +
                "92.050039469858\t\n" +
                "45.634456960908\t\n" +
                "40.455469747939\t\n";
        assertQuery(expected,
                "select * from x where b = null",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,1))), index(b)",
                null,
                "insert into x (a,b)" +
                        " select" +
                        " rnd_double(0)*100, " +
                        " rnd_symbol(5,4,4,1)" +
                        " from long_sequence(15)",
                expected +
                        "54.491550215189\t\n" +
                        "76.923818943378\t\n" +
                        "58.912164838798\t\n" +
                        "44.804689668614\t\n" +
                        "89.409171265819\t\n" +
                        "3.993124821273\t\n");
    }

    @Test
    public void testFilterSingleKeyValue() throws Exception {
        final String expected = "a\tb\n" +
                "11.427984775756\tHYRX\n" +
                "52.984059417621\tHYRX\n" +
                "40.455469747939\tHYRX\n" +
                "72.300157631336\tHYRX\n";
        assertQuery(expected,
                "select * from x where b = 'HYRX'",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,0))), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'HYRX'" +
                        " from long_sequence(2)",
                expected +
                        "75.881754034549\tHYRX\n" +
                        "57.789479151824\tHYRX\n");
    }

    @Test
    public void testFilterSingleKeyValueAndField() throws Exception {
        final String expected = "a\tb\n" +
                "52.984059417621\tHYRX\n" +
                "72.300157631336\tHYRX\n";
        assertQuery(expected,
                "select * from x where b = 'HYRX' and a > 41",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,0))), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'HYRX'" +
                        " from long_sequence(2)",
                expected +
                        "75.881754034549\tHYRX\n" +
                        "57.789479151824\tHYRX\n");
    }

    @Test
    public void testFilterSingleNonExistingSymbol() throws Exception {
        assertQuery("a\tb\n",
                "select * from x where b = 'ABC'",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,0))), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'ABC'" +
                        " from long_sequence(2)",
                "a\tb\n" +
                        "75.881754034549\tABC\n" +
                        "57.789479151824\tABC\n");
    }

    @Test
    public void testFilterSingleNonExistingSymbolAndFilter() throws Exception {
        assertQuery("a\tb\n",
                "select * from x where b = 'ABC' and a > 30",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,0))), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'ABC'" +
                        " from long_sequence(2)",
                "a\tb\n" +
                        "75.881754034549\tABC\n" +
                        "57.789479151824\tABC\n");
    }

    @Test
    public void testLatestByKey() throws Exception {
        final String expected = "a\tb\tk\n" +
                "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "48.820511018587\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected,
                "select * from x latest by b",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612\tVTJW\t2019-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByKeyNoIndex() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "48.820511018587\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612\tVTJW\t2019-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByKeyValue() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "23.905290108465\tRXGZ\t1970-01-03T07:33:20.000000Z\n",
                "select * from x latest by b where b = 'RXGZ'",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "56.594291398612\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByKeyValueInterval() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "84.452581772111\tPEHN\t1970-01-16T01:06:40.000000Z\n",
                "select * from x latest by b where b = 'PEHN' and k = '1970-01-06T18:53:20;11d'",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        "), index(b) timestamp(k) partition by DAY",
                "k");
    }

    @Test
    public void testLatestByMissingKeyValue() throws Exception {
        assertQuery("a\tb\tk\n",
                "select * from x latest by b where b = 'XYZ'",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'XYZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "56.594291398612\tXYZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLongCursor() throws Exception {
        assertQuery("x\n" +
                        "1\n" +
                        "2\n" +
                        "3\n" +
                        "4\n" +
                        "5\n" +
                        "6\n" +
                        "7\n" +
                        "8\n" +
                        "9\n" +
                        "10\n",
                "select * from long_sequence(10)",
                null,
                null);


        // test another record count

        assertQuery("x\n" +
                        "1\n" +
                        "2\n" +
                        "3\n" +
                        "4\n" +
                        "5\n" +
                        "6\n" +
                        "7\n" +
                        "8\n" +
                        "9\n" +
                        "10\n" +
                        "11\n" +
                        "12\n" +
                        "13\n" +
                        "14\n" +
                        "15\n" +
                        "16\n" +
                        "17\n" +
                        "18\n" +
                        "19\n" +
                        "20\n",
                "select * from long_sequence(20)",
                null,
                null);

        // test 0 record count

        assertQuery("x\n",
                "select * from long_sequence(0)",
                null,
                null);

        assertQuery("x\n",
                "select * from long_sequence(-2)",
                null,
                null);
    }

    @Test
    public void testSelectColumns() throws Exception {
        assertQuery("a\ta1\tb\tc\td\te\tf1\tf\tg\th\ti\tj\tj1\tk\tl\tm\n" +
                        "NaN\t1569490116\tfalse\t\tNaN\t0.7611\t-1593\t428\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\t1970-01-01T00:00:00.000000Z\t4\t00000000 af 19 c4 95 94 36 53 49 b4 59 7e\n" +
                        "10\t1253890363\tfalse\tXYS\t0.191123461757\t0.5793\t-1379\t881\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\t1970-01-01T00:16:40.000000Z\t50\t00000000 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78 b5\n" +
                        "27\t-1819240775\ttrue\tGOO\t0.041428124702\t0.9205\t-9039\t97\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\t1970-01-01T00:33:20.000000Z\t21\t\n" +
                        "18\t-1201923128\ttrue\tUVS\t0.758817540345\t0.5779\t-4379\t480\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\t1970-01-01T00:50:00.000000Z\t27\t00000000 28 c7 84 47 dc d2 85 7f a5 b8 7b 4a 9d 46\n" +
                        "NaN\t865832060\ttrue\t\t0.148305523358\t0.9442\t2508\t95\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\t1970-01-01T01:06:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                        "00000010 38 e1\n" +
                        "22\t1100812407\tfalse\tOVL\tNaN\t0.7633\t-17778\t698\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\t1970-01-01T01:23:20.000000Z\t23\t\n" +
                        "18\t1677463366\tfalse\tMNZ\t0.337470756550\t0.1179\t18904\t533\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\t1970-01-01T01:40:00.000000Z\t29\t00000000 5d d0 eb 67 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62\n" +
                        "00000010 28 60\n" +
                        "4\t39497392\tfalse\tUOH\t0.029227696943\t0.1718\t14242\t652\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\t1970-01-01T01:56:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                        "00000010 ea 4e ea 8b\n" +
                        "10\t1545963509\tfalse\tNWI\t0.113718418361\t0.0620\t-29980\t356\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\t1970-01-01T02:13:20.000000Z\t13\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\n" +
                        "4\t53462821\tfalse\tGOO\t0.055149337562\t0.1195\t-6087\t115\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\t1970-01-01T02:30:00.000000Z\t46\t\n" +
                        "30\t-2139296159\tfalse\t\t0.185864355816\t0.5638\t21020\t299\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\t1970-01-01T02:46:40.000000Z\t38\t00000000 b8 07 b1 32 57 ff 9a ef 88 cb 4b\n" +
                        "21\t-406528351\tfalse\tNLE\tNaN\tNaN\t21057\t968\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\t1970-01-01T03:03:20.000000Z\t43\t\n" +
                        "17\t415709351\tfalse\tGQZ\t0.491990017163\t0.6292\t18605\t581\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\t1970-01-01T03:20:00.000000Z\t19\t00000000 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e\n" +
                        "00000010 44 a8 0d fe\n" +
                        "19\t-1387693529\ttrue\tMCG\t0.848083900630\t0.4699\t24206\t119\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\t1970-01-01T03:36:40.000000Z\t12\t00000000 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\n" +
                        "00000010 20 53 3b 51\n" +
                        "21\t346891421\tfalse\t\t0.933609514583\t0.6380\t15084\t405\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\t1970-01-01T03:53:20.000000Z\t43\t\n" +
                        "27\t263487884\ttrue\tHZQ\t0.703978540803\t0.8461\t31562\t834\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\t1970-01-01T04:10:00.000000Z\t15\t00000000 71 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2\n" +
                        "9\t-1034870849\tfalse\tLSV\t0.650660460171\t0.7020\t-838\t110\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\t1970-01-01T04:26:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\n" +
                        "26\t1848218326\ttrue\tSUW\t0.803404910559\t0.0440\t-3502\t854\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\t1970-01-01T04:43:20.000000Z\t35\t00000000 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a\n" +
                        "5\t-1496904948\ttrue\tDBZ\t0.286271736488\tNaN\t5698\t764\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\t1970-01-01T05:00:00.000000Z\t19\t00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                        "20\t856634079\ttrue\tRJU\t0.108206023861\t0.4565\t13505\t669\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\t1970-01-01T05:16:40.000000Z\t3\t00000000 f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8\n" +
                        "00000010 ab 3f a1 f5\n",
                "select a,a1,b,c,d,e,f1,f,g,h,i,j,j1,k,l,m from x",
                "create table x as (" +
                        "select * from random_cursor(" +
                        " 20," + // record count
                        " 'a1', rnd_int()," +
                        " 'a', rnd_int(0, 30, 2)," +
                        " 'b', rnd_boolean()," +
                        " 'c', rnd_str(3,3,2)," +
                        " 'd', rnd_double(2)," +
                        " 'e', rnd_float(2)," +
                        " 'f', rnd_short(10,1024)," +
                        " 'f1', rnd_short()," +
                        " 'g', rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                        " 'h', rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)," +
                        " 'i', rnd_symbol(4,4,4,2)," +
                        " 'j', rnd_long(100,200,2)," +
                        " 'j1', rnd_long()," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)," +
                        " 'l', rnd_byte(2,50)," +
                        " 'm', rnd_bin(10, 20, 2)" +
                        "))  timestamp(k) partition by DAY",
                "k");
    }

    @Test
    public void testSelectColumnsSansTimestamp() throws Exception {
        assertQuery("a\ta1\tb\tc\td\te\tf1\tf\tg\th\ti\tj\tj1\n" +
                        "NaN\t1569490116\tfalse\t\tNaN\t0.7611\t-1593\t428\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\n" +
                        "10\t1253890363\tfalse\tXYS\t0.191123461757\t0.5793\t-1379\t881\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\n" +
                        "27\t-1819240775\ttrue\tGOO\t0.041428124702\t0.9205\t-9039\t97\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\n" +
                        "18\t-1201923128\ttrue\tUVS\t0.758817540345\t0.5779\t-4379\t480\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\n" +
                        "NaN\t865832060\ttrue\t\t0.148305523358\t0.9442\t2508\t95\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\n" +
                        "22\t1100812407\tfalse\tOVL\tNaN\t0.7633\t-17778\t698\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\n" +
                        "18\t1677463366\tfalse\tMNZ\t0.337470756550\t0.1179\t18904\t533\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\n" +
                        "4\t39497392\tfalse\tUOH\t0.029227696943\t0.1718\t14242\t652\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\n" +
                        "10\t1545963509\tfalse\tNWI\t0.113718418361\t0.0620\t-29980\t356\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\n" +
                        "4\t53462821\tfalse\tGOO\t0.055149337562\t0.1195\t-6087\t115\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\n" +
                        "30\t-2139296159\tfalse\t\t0.185864355816\t0.5638\t21020\t299\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\n" +
                        "21\t-406528351\tfalse\tNLE\tNaN\tNaN\t21057\t968\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\n" +
                        "17\t415709351\tfalse\tGQZ\t0.491990017163\t0.6292\t18605\t581\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\n" +
                        "19\t-1387693529\ttrue\tMCG\t0.848083900630\t0.4699\t24206\t119\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\n" +
                        "21\t346891421\tfalse\t\t0.933609514583\t0.6380\t15084\t405\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\n" +
                        "27\t263487884\ttrue\tHZQ\t0.703978540803\t0.8461\t31562\t834\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\n" +
                        "9\t-1034870849\tfalse\tLSV\t0.650660460171\t0.7020\t-838\t110\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\n" +
                        "26\t1848218326\ttrue\tSUW\t0.803404910559\t0.0440\t-3502\t854\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\n" +
                        "5\t-1496904948\ttrue\tDBZ\t0.286271736488\tNaN\t5698\t764\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\n" +
                        "20\t856634079\ttrue\tRJU\t0.108206023861\t0.4565\t13505\t669\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\n",
                "select a,a1,b,c,d,e,f1,f,g,h,i,j,j1 from x",
                "create table x as (" +
                        "select * from random_cursor(" +
                        " 20," + // record count
                        " 'a1', rnd_int()," +
                        " 'a', rnd_int(0, 30, 2)," +
                        " 'b', rnd_boolean()," +
                        " 'c', rnd_str(3,3,2)," +
                        " 'd', rnd_double(2)," +
                        " 'e', rnd_float(2)," +
                        " 'f', rnd_short(10,1024)," +
                        " 'f1', rnd_short()," +
                        " 'g', rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                        " 'h', rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)," +
                        " 'i', rnd_symbol(4,4,4,2)," +
                        " 'j', rnd_long(100,200,2)," +
                        " 'j1', rnd_long()," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)," +
                        " 'l', rnd_byte(2,50)," +
                        " 'm', rnd_bin(10, 20, 2)" +
                        "))  timestamp(k) partition by DAY",
                null);
    }

    @Test
    public void testVirtualColumns() throws Exception {
        assertQuery("a\ta1\tb\tc\tcolumn\tf1\tf\tg\th\ti\tj\tj1\tk\tl\tm\n" +
                        "NaN\t1569490116\tfalse\t\tNaN\t-1593\t428\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\t1970-01-01T00:00:00.000000Z\t4\t00000000 af 19 c4 95 94 36 53 49 b4 59 7e\n" +
                        "10\t1253890363\tfalse\tXYS\t0.770470058952\t-1379\t881\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\t1970-01-01T00:16:40.000000Z\t50\t00000000 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78 b5\n" +
                        "27\t-1819240775\ttrue\tGOO\t0.961928462780\t-9039\t97\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\t1970-01-01T00:33:20.000000Z\t21\t\n" +
                        "18\t-1201923128\ttrue\tUVS\t1.336712287603\t-4379\t480\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\t1970-01-01T00:50:00.000000Z\t27\t00000000 28 c7 84 47 dc d2 85 7f a5 b8 7b 4a 9d 46\n" +
                        "NaN\t865832060\ttrue\t\t1.092471408807\t2508\t95\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\t1970-01-01T01:06:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                        "00000010 38 e1\n" +
                        "22\t1100812407\tfalse\tOVL\tNaN\t-17778\t698\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\t1970-01-01T01:23:20.000000Z\t23\t\n" +
                        "18\t1677463366\tfalse\tMNZ\t0.455323861618\t18904\t533\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\t1970-01-01T01:40:00.000000Z\t29\t00000000 5d d0 eb 67 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62\n" +
                        "00000010 28 60\n" +
                        "4\t39497392\tfalse\tUOH\t0.201030575323\t14242\t652\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\t1970-01-01T01:56:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                        "00000010 ea 4e ea 8b\n" +
                        "10\t1545963509\tfalse\tNWI\t0.175745872737\t-29980\t356\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\t1970-01-01T02:13:20.000000Z\t13\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\n" +
                        "4\t53462821\tfalse\tGOO\t0.174661478313\t-6087\t115\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\t1970-01-01T02:30:00.000000Z\t46\t\n" +
                        "30\t-2139296159\tfalse\t\t0.749638583912\t21020\t299\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\t1970-01-01T02:46:40.000000Z\t38\t00000000 b8 07 b1 32 57 ff 9a ef 88 cb 4b\n" +
                        "21\t-406528351\tfalse\tNLE\tNaN\t21057\t968\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\t1970-01-01T03:03:20.000000Z\t43\t\n" +
                        "17\t415709351\tfalse\tGQZ\t1.121198641526\t18605\t581\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\t1970-01-01T03:20:00.000000Z\t19\t00000000 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e\n" +
                        "00000010 44 a8 0d fe\n" +
                        "19\t-1387693529\ttrue\tMCG\t1.317948686301\t24206\t119\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\t1970-01-01T03:36:40.000000Z\t12\t00000000 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\n" +
                        "00000010 20 53 3b 51\n" +
                        "21\t346891421\tfalse\t\t1.571608691562\t15084\t405\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\t1970-01-01T03:53:20.000000Z\t43\t\n" +
                        "27\t263487884\ttrue\tHZQ\t1.550099673177\t31562\t834\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\t1970-01-01T04:10:00.000000Z\t15\t00000000 71 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2\n" +
                        "9\t-1034870849\tfalse\tLSV\t1.352704947170\t-838\t110\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\t1970-01-01T04:26:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\n" +
                        "26\t1848218326\ttrue\tSUW\t0.847444875235\t-3502\t854\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\t1970-01-01T04:43:20.000000Z\t35\t00000000 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a\n" +
                        "5\t-1496904948\ttrue\tDBZ\tNaN\t5698\t764\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\t1970-01-01T05:00:00.000000Z\t19\t00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                        "20\t856634079\ttrue\tRJU\t0.564672758270\t13505\t669\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\t1970-01-01T05:16:40.000000Z\t3\t00000000 f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8\n" +
                        "00000010 ab 3f a1 f5\n",
                "select a,a1,b,c,d+e,f1,f,g,h,i,j,j1,k,l,m from x",
                "create table x as (" +
                        "select * from random_cursor(" +
                        " 20," + // record count
                        " 'a1', rnd_int()," +
                        " 'a', rnd_int(0, 30, 2)," +
                        " 'b', rnd_boolean()," +
                        " 'c', rnd_str(3,3,2)," +
                        " 'd', rnd_double(2)," +
                        " 'e', rnd_float(2)," +
                        " 'f', rnd_short(10,1024)," +
                        " 'f1', rnd_short()," +
                        " 'g', rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                        " 'h', rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)," +
                        " 'i', rnd_symbol(4,4,4,2)," +
                        " 'j', rnd_long(100,200,2)," +
                        " 'j1', rnd_long()," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)," +
                        " 'l', rnd_byte(2,50)," +
                        " 'm', rnd_bin(10, 20, 2)" +
                        "))  timestamp(k) partition by DAY",
                "k");
    }

    @Test
    public void testVirtualColumnsSansTimestamp() throws Exception {
        assertQuery("a\ta1\tb\tc\tcolumn\n" +
                        "NaN\t1569490116\tfalse\t\tNaN\n" +
                        "10\t1253890363\tfalse\tXYS\t0.770470058952\n" +
                        "27\t-1819240775\ttrue\tGOO\t0.961928462780\n" +
                        "18\t-1201923128\ttrue\tUVS\t1.336712287603\n" +
                        "NaN\t865832060\ttrue\t\t1.092471408807\n" +
                        "22\t1100812407\tfalse\tOVL\tNaN\n" +
                        "18\t1677463366\tfalse\tMNZ\t0.455323861618\n" +
                        "4\t39497392\tfalse\tUOH\t0.201030575323\n" +
                        "10\t1545963509\tfalse\tNWI\t0.175745872737\n" +
                        "4\t53462821\tfalse\tGOO\t0.174661478313\n" +
                        "30\t-2139296159\tfalse\t\t0.749638583912\n" +
                        "21\t-406528351\tfalse\tNLE\tNaN\n" +
                        "17\t415709351\tfalse\tGQZ\t1.121198641526\n" +
                        "19\t-1387693529\ttrue\tMCG\t1.317948686301\n" +
                        "21\t346891421\tfalse\t\t1.571608691562\n" +
                        "27\t263487884\ttrue\tHZQ\t1.550099673177\n" +
                        "9\t-1034870849\tfalse\tLSV\t1.352704947170\n" +
                        "26\t1848218326\ttrue\tSUW\t0.847444875235\n" +
                        "5\t-1496904948\ttrue\tDBZ\tNaN\n" +
                        "20\t856634079\ttrue\tRJU\t0.564672758270\n",
                "select a,a1,b,c,d+e from x",
                "create table x as (" +
                        "select * from random_cursor(" +
                        " 20," + // record count
                        " 'a1', rnd_int()," +
                        " 'a', rnd_int(0, 30, 2)," +
                        " 'b', rnd_boolean()," +
                        " 'c', rnd_str(3,3,2)," +
                        " 'd', rnd_double(2)," +
                        " 'e', rnd_float(2)," +
                        " 'f', rnd_short(10,1024)," +
                        " 'f1', rnd_short()," +
                        " 'g', rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                        " 'h', rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)," +
                        " 'i', rnd_symbol(4,4,4,2)," +
                        " 'j', rnd_long(100,200,2)," +
                        " 'j1', rnd_long()," +
                        " 'k', timestamp_sequence(to_timestamp(0), 1000000000)," +
                        " 'l', rnd_byte(2,50)," +
                        " 'm', rnd_bin(10, 20, 2)" +
                        "))  timestamp(k) partition by DAY",
                null);
    }

    private void assertCursor(CharSequence expected, RecordCursorFactory factory) throws IOException {
        try (RecordCursor cursor = factory.getCursor()) {
            sink.clear();
            rows.clear();
            printer.print(cursor, factory.getMetadata(), true);

            if (expected == null) {
                return;
            }

            TestUtils.assertEquals(expected, sink);
            cursor.toTop();

            sink.clear();
            while (cursor.hasNext()) {
                rows.add(cursor.next().getRowId());
            }

            final RecordMetadata metadata = factory.getMetadata();

            // test external record
            Record record = cursor.getRecord();

            printer.printHeader(metadata);
            for (int i = 0, n = rows.size(); i < n; i++) {
                cursor.recordAt(record, rows.getQuick(i));
                printer.print(record, metadata);
            }

            TestUtils.assertEquals(expected, sink);

            // test internal record
            sink.clear();
            printer.printHeader(metadata);
            for (int i = 0, n = rows.size(); i < n; i++) {
                printer.print(cursor.recordAt(rows.getQuick(i)), metadata);
            }

            TestUtils.assertEquals(expected, sink);

            // test _new_ record

            sink.clear();
            record = cursor.newRecord();
            printer.printHeader(metadata);
            for (int i = 0, n = rows.size(); i < n; i++) {
                cursor.recordAt(record, rows.getQuick(i));
                printer.print(record, metadata);
            }

            TestUtils.assertEquals(expected, sink);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void assertQuery(
            CharSequence expected,
            CharSequence query,
            @Nullable CharSequence ddl,
            @Nullable CharSequence verify,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2
    ) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                if (ddl != null) {
                    compiler.execute(ddl, bindVariableService);
                }
                if (verify != null) {
                    printSqlResult(null, verify, expectedTimestamp, ddl2, expected2);
                    System.out.println(sink);
                }
                printSqlResult(expected, query, expectedTimestamp, ddl2, expected2);
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    private void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp) throws Exception {
        assertQuery(expected, query, ddl, null, expectedTimestamp, null, null);
    }

    private void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2) throws Exception {
        assertQuery(expected, query, ddl, null, expectedTimestamp, ddl2, expected2);
    }

    private void assertTimestampColumnValues(RecordCursorFactory factory) {
        int index = factory.getMetadata().getTimestampIndex();
        long timestamp = Long.MIN_VALUE;
        try (RecordCursor cursor = factory.getCursor()) {
            while (cursor.hasNext()) {
                long ts = cursor.next().getTimestamp(index);
                Assert.assertTrue(timestamp <= ts);
                timestamp = ts;
            }
        }
    }

    private void assertVariableColumns(RecordCursorFactory factory) {
        try (RecordCursor cursor = factory.getCursor()) {
            RecordMetadata metadata = factory.getMetadata();
            final int columnCount = metadata.getColumnCount();
            while (cursor.hasNext()) {
                Record record = cursor.next();
                for (int i = 0; i < columnCount; i++) {
                    switch (metadata.getColumnType(i)) {
                        case ColumnType.STRING:
                            CharSequence a = record.getStr(i);
                            CharSequence b = record.getStrB(i);
                            if (a == null) {
                                Assert.assertNull(b);
                                Assert.assertEquals(TableUtils.NULL_LEN, record.getStrLen(i));
                            } else {
                                Assert.assertNotSame(a, b);
                                TestUtils.assertEquals(a, b);
                                Assert.assertEquals(a.length(), record.getStrLen(i));
                            }
                            break;
                        case ColumnType.BINARY:
                            BinarySequence s = record.getBin(i);
                            if (s == null) {
                                Assert.assertEquals(TableUtils.NULL_LEN, record.getBinLen(i));
                            } else {
                                Assert.assertEquals(s.length(), record.getBinLen(i));
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    }

    private void printSqlResult(
            CharSequence expected,
            CharSequence query,
            CharSequence expectedTimestamp,
            CharSequence ddl2,
            CharSequence expected2
    ) throws IOException, SqlException {
        try (final RecordCursorFactory factory = compiler.compile(query, bindVariableService)) {
            if (expectedTimestamp == null) {
                Assert.assertEquals(-1, factory.getMetadata().getTimestampIndex());
            } else {
                int index = factory.getMetadata().getColumnIndex(expectedTimestamp);
                Assert.assertNotEquals(-1, index);
                Assert.assertEquals(index, factory.getMetadata().getTimestampIndex());
                assertTimestampColumnValues(factory);
            }
            assertCursor(expected, factory);
            // make sure we get the same outcome when we get factory to create new cursor
            assertCursor(expected, factory);
            // make sure strings, binary fields and symbols are compliant with expected record behaviour
            assertVariableColumns(factory);

            if (ddl2 != null) {
                compiler.execute(ddl2, bindVariableService);
                assertCursor(expected2, factory);
                // and again
                assertCursor(expected2, factory);
            }
        }
    }
}