/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cairo;

import com.questdb.cairo.security.AllowAllCairoSecurityContext;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.griffin.SqlExecutionContextImpl;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableReaderRecordCursorFactoryTest extends AbstractCairoTest {
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

            final String expectedMetadata = "{\"columnCount\":5,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"STRING\"},{\"index\":1,\"name\":\"b\",\"type\":\"SYMBOL\",\"indexed\":true,\"indexValueBlockCapacity\":32},{\"index\":2,\"name\":\"i\",\"type\":\"INT\"},{\"index\":3,\"name\":\"c\",\"type\":\"SYMBOL\",\"indexed\":true,\"indexValueBlockCapacity\":32},{\"index\":4,\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"}],\"timestampIndex\":4}";

            final Rnd rnd = new Rnd();
            final String[] symbols = new String[N];
            final int M = 1000;
            final long increment = 1000000 * 60L * 10;

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


            try (CairoEngine engine = new CairoEngine(configuration)) {

                final RecordMetadata metadata;
                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", -1)) {
                    metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
                }
                try (RecordCursorFactory factory = new TableReaderRecordCursorFactory(metadata, engine, "x", TableUtils.ANY_TABLE_VERSION)) {

                    long count = 0;
                    final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl().with(AllowAllCairoSecurityContext.INSTANCE, new BindVariableService());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        rnd.reset();
                        while (cursor.hasNext()) {
                            TestUtils.assertEquals(rnd.nextChars(20), record.getStr(0));
                            TestUtils.assertEquals(symbols[rnd.nextPositiveInt() % N], record.getSym(1));
                            Assert.assertEquals(rnd.nextInt(), record.getInt(2));
                            TestUtils.assertEquals(symbols[rnd.nextPositiveInt() % N], record.getSym(3));
                            count++;
                        }
                        sink.clear();
                        factory.getMetadata().toJson(sink);
                        TestUtils.assertEquals(expectedMetadata, sink);
                    }
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                    Assert.assertEquals(M, count);

                    Assert.assertTrue(factory.isRandomAccessCursor());
                }
            }
        });
    }
}