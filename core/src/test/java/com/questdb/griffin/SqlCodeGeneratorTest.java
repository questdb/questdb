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

import com.questdb.cairo.*;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.common.ColumnType;
import com.questdb.common.PartitionBy;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.griffin.engine.functions.rnd.SharedRandom;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class SqlCodeGeneratorTest extends AbstractCairoTest {

    private static final BindVariableService bindVariableService = new BindVariableService();
    private static final CairoEngine engine = new Engine(configuration);
    private static final SqlCompiler compiler = new SqlCompiler(engine, configuration);

    @Before
    public void setUp2() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testFilterMultipleKeyValues() throws SqlException, IOException {
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
    public void testFilterNullKeyValue() throws SqlException, IOException {
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
    public void testFilterSingleKeyValue() throws SqlException, IOException {
        assertQuery("a\tb\n" +
                        "11.427984775756\tHYRX\n" +
                        "52.984059417621\tHYRX\n" +
                        "40.455469747939\tHYRX\n" +
                        "72.300157631336\tHYRX\n",
                "select * from x where b = 'HYRX'",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,0))), index(b)");
    }

    @Test
    public void testFilterSingleKeyValueAndFilter() throws SqlException, IOException {
        CairoEngine engine = new Engine(configuration);
        SqlCompiler compiler = new SqlCompiler(engine, configuration);

        try (TableModel model = new TableModel(configuration, "tab", PartitionBy.NONE)) {
            model.col("sym", ColumnType.SYMBOL).indexed(true, 256);
            model.col("value", ColumnType.DOUBLE);
            CairoTestUtils.create(model);
        }

        final int N = 20;
        final String[] symbols = {"ABC", "CDE", "EFG"};
        final Rnd rnd = new Rnd();

        try (TableWriter writer = engine.getWriter("tab")) {
            for (int i = 0; i < N; i++) {
                TableWriter.Row row = writer.newRow(0);
                row.putSym(0, symbols[rnd.nextPositiveInt() % symbols.length]);
                row.putDouble(1, rnd.nextDouble2());
                row.append();
            }
            writer.commit();
        }

        try (TableReader reader = engine.getReader("tab")) {
            sink.clear();
            printer.print(reader.getCursor(), true);
        }
        System.out.println(sink);
        System.out.println("----------------------");


        RecordCursorFactory rcf = compiler.compile("select * from tab where sym = 'ABC' and value < 1.0", bindVariableService);
        RecordCursor cursor = rcf.getCursor();
        sink.clear();
        printer.print(cursor, true);
        System.out.println(sink);
    }

    @Test
    public void testFilterSingleNonExistingSymbol() throws SqlException, IOException {
        assertQuery("a\tb\n",
                "select * from x where b = 'ABC'",
                "create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,0))), index(b)");
    }

    private void assertQuery(CharSequence expected, CharSequence query, CharSequence ddl) throws SqlException, IOException {
        assertQuery(expected, query, ddl, null);
    }

    @SuppressWarnings("SameParameterValue")
    private void assertQuery(CharSequence expected, CharSequence query, CharSequence ddl, @Nullable CharSequence verify) throws SqlException, IOException {
        compiler.execute(ddl, bindVariableService);
        if (verify != null) {
            printSqlResult(verify);
            System.out.println(sink);
        }
        printSqlResult(query);
        TestUtils.assertEquals(expected, sink);
    }

    private void printSqlResult(CharSequence query) throws IOException, SqlException {
        sink.clear();
        try (RecordCursor cursor = compiler.compile(query, bindVariableService).getCursor()) {
            printer.print(cursor, true);
        }
    }
}