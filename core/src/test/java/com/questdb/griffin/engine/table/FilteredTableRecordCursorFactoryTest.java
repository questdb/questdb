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

package com.questdb.griffin.engine.table;

import com.questdb.cairo.*;
import com.questdb.common.ColumnType;
import com.questdb.common.PartitionBy;
import com.questdb.common.Record;
import com.questdb.common.RecordCursor;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Test;

public class FilteredTableRecordCursorFactoryTest extends AbstractCairoTest {
    @Test
    public void testFactory() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 100;
            // separate two symbol columns with primitive. It will make problems apparent if index does not shift correctly
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.DAY).
                    col("a", ColumnType.STRING).
                    col("b", ColumnType.SYMBOL).indexed(true, N / 4).
                    col("i", ColumnType.INT).
                    col("c", ColumnType.SYMBOL).indexed(true, N / 4).
                    timestamp()
            ) {
                CairoTestUtils.create(model);
            }

            final Rnd rnd = new Rnd();
            final String symbols[] = new String[N];
            final int M = 1000;
            final long increment = 1000000 * 60L * 4;

            for (int i = 0; i < N; i++) {
                symbols[i] = rnd.nextChars(8).toString();
            }

            rnd.reset();

            // prepare the data
            long timestamp = 0;
            try (TableWriter writer = new TableWriter(configuration, "x")) {
                for (int i = 0; i < M; i++) {
                    TableWriter.Row row = writer.newRow(timestamp += increment);
                    row.putStr(0, rnd.nextChars(20));
                    row.putSym(1, symbols[rnd.nextPositiveInt() % N]);
                    row.putInt(2, rnd.nextInt());
                    row.putSym(3, symbols[rnd.nextPositiveInt() % N]);
                    row.append();
                }
                writer.commit();
            }

            try (Engine engine = new Engine(configuration)) {
                String value = symbols[N - 10];
                SymbolIndexRowCursorFactory symbolIndexRowCursorFactory = new SymbolIndexRowCursorFactory(engine, "x", "b", value);
                FullTableFrameCursorFactory dataFrameFactory = new FullTableFrameCursorFactory(engine, "x");
                FilteredTableRecordCursorFactory factory = new FilteredTableRecordCursorFactory(dataFrameFactory, symbolIndexRowCursorFactory);

                RecordCursor cursor = factory.getCursor();
                while (cursor.hasNext()) {
                    Record record = cursor.next();
                    TestUtils.assertEquals(value, record.getSym(1));
                }
                cursor.releaseCursor();
            }
        });
    }
}