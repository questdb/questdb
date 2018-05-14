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
    public void testFilterSingleKeyValue() throws SqlException, IOException {
        compiler.execute("create table x as (select * from random_cursor(20, 'a', rnd_double(0)*100, 'b', rnd_symbol(5,4,4,0))), index(b)", bindVariableService);
        printSqlResult("select * from x where b = 'HYRX'");
        TestUtils.assertEquals("a\tb\n" +
                        "11.427984775756\tHYRX\n" +
                        "52.984059417621\tHYRX\n" +
                        "40.455469747939\tHYRX\n" +
                        "72.300157631336\tHYRX\n",
                sink);
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

    private void printSqlResult(CharSequence query) throws IOException, SqlException {
        sink.clear();
        try (RecordCursor cursor = compiler.compile(query, bindVariableService).getCursor()) {
            printer.print(cursor, true);
        }
    }
}