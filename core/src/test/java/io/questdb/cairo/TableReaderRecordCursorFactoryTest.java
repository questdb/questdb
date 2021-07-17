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

package io.questdb.cairo;

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableReaderRecordCursorFactoryTest extends AbstractCairoTest {
    @Test
    public void testFactory() throws Exception {
        assertMemoryLeak(() -> {
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
            final RecordMetadata metadata;
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_ID, -1)) {
                metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
            }

            IntList columnIndexes = new IntList();
            IntList columnSizes = new IntList();

            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                columnIndexes.add(i);
                columnSizes.add(Numbers.msb(ColumnType.sizeOf(metadata.getColumnType(i))));
            }
            try (RecordCursorFactory factory = new TableReaderRecordCursorFactory(
                    metadata,
                    engine,
                    "x",
                    TableUtils.ANY_TABLE_ID,
                    TableUtils.ANY_TABLE_VERSION,
                    columnIndexes,
                    columnSizes,
                    false
            )) {
                long count = 0;
                final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                        .with(
                                AllowAllCairoSecurityContext.INSTANCE,
                                new BindVariableServiceImpl(engine.getConfiguration()),
                                null,
                                -1,
                                null
                        );
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

                Assert.assertTrue(factory.recordCursorSupportsRandomAccess());
            }
        });
    }
}