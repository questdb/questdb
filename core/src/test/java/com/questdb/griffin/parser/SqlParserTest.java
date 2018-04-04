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

package com.questdb.griffin.parser;

import com.questdb.cairo.*;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.common.ColumnType;
import com.questdb.common.PartitionBy;
import com.questdb.griffin.lexer.ParserException;
import com.questdb.std.Rnd;
import org.junit.Test;

import java.io.IOException;

public class SqlParserTest extends AbstractCairoTest {
    @Test
    public void testFilterSingleKeyValue() throws ParserException, IOException {
        CairoEngine engine = new Engine(configuration);
        SqlParser parser = new SqlParser(engine, configuration);

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
                row.putDouble(1, rnd.nextDouble());
                row.append();
            }
            writer.commit();
        }


        RecordCursorFactory rcf = parser.parseQuery("select * from tab where sym = 'ABC'");
        RecordCursor cursor = rcf.getCursor();
        printer.print(cursor, true, cursor.getMetadata());
        System.out.println(sink);
    }
}