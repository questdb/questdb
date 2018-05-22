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
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.griffin.engine.functions.rnd.SharedRandom;
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
        assertQuery("a\tb\tk\n",
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
                        "), index(b) timestamp(k)");
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
                        "), index(b) timestamp(k)");
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
                        "), index(b) timestamp(k)");
    }

    @Test
    public void testFilterIntrinsicFalse() throws Exception {
        assertQuery("a\tb\tk\n",
                "select * from x o where o.b in ('HYRX','PEHN', null) and a < a",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,1), 'k', timestamp_sequence(to_timestamp(0), 1000000000))), index(b)");
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
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,1), 'k', timestamp_sequence(to_timestamp(0), 1000000000))), index(b)");
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
                        "), index(b) timestamp(k)");
    }

    @Test
    public void testFilterMultipleKeyValues() throws Exception {
        assertQuery("a\tb\tk\n" +
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
                        "40.455469747939\t\t1970-01-01T05:16:40.000000Z\n",
                "select * from x o where o.b in ('HYRX','PEHN', null)",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,1), 'k', timestamp_sequence(to_timestamp(0), 1000000000))), index(b)");
    }

    @Test
    public void testFilterNullKeyValue() throws Exception {
        assertQuery("a\tb\n" +
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
                        "40.455469747939\t\n",
                "select * from x where b = null",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,1))), index(b)");
    }

    @Test
    public void testFilterSingleKeyValue() throws Exception {
        assertQuery("a\tb\n" +
                        "11.427984775756\tHYRX\n" +
                        "52.984059417621\tHYRX\n" +
                        "40.455469747939\tHYRX\n" +
                        "72.300157631336\tHYRX\n",
                "select * from x where b = 'HYRX'",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,0))), index(b)");
    }

    @Test
    public void testFilterSingleKeyValueAndField() throws Exception {
        assertQuery("a\tb\n" +
                        "52.984059417621\tHYRX\n" +
                        "72.300157631336\tHYRX\n",
                "select * from x where b = 'HYRX' and a > 41",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,0))), index(b)");
    }

    @Test
    public void testFilterSingleNonExistingSymbol() throws Exception {
        assertQuery("a\tb\n",
                "select * from x where b = 'ABC'",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,0))), index(b)");
    }

    @Test
    public void testLatestByKeyValue() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "49.005104498852\tPEHN\t1970-01-18T08:40:00.000000Z\n",
                "select * from x latest by b where b = 'PEHN'",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 100000000000)" +
                        ")" +
                        "), index(b) timestamp(k) partition by DAY");
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
                        "), index(b) timestamp(k) partition by DAY");
    }

    private void assertQuery(CharSequence expected, CharSequence query, CharSequence ddl) throws Exception {
        assertQuery(expected, query, ddl, null);
    }

    @SuppressWarnings("SameParameterValue")
    private void assertQuery(CharSequence expected, CharSequence query, CharSequence ddl, @Nullable CharSequence verify) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.execute(ddl, bindVariableService);
            if (verify != null) {
                printSqlResult(null, verify);
                System.out.println(sink);
            }
            printSqlResult(expected, query);
            Assert.assertEquals(0, engine.getBusyReaderCount());
            Assert.assertEquals(0, engine.getBusyWriterCount());
            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

    private void printSqlResult(CharSequence expected, CharSequence query) throws IOException, SqlException {
        sink.clear();
        rows.clear();

        try (RecordCursor cursor = compiler.compile(query, bindVariableService).getCursor()) {
            printer.print(cursor, true);

            if (expected == null) {
                return;
            }

            TestUtils.assertEquals(expected, sink);
            cursor.toTop();

            sink.clear();
            while (cursor.hasNext()) {
                rows.add(cursor.next().getRowId());
            }

            final RecordMetadata metadata = cursor.getMetadata();

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
}