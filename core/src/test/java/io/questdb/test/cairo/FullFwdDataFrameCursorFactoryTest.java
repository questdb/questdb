/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.test.cutlass.text.SqlExecutionContextStub;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_ASC;

public class FullFwdDataFrameCursorFactoryTest extends AbstractCairoTest {
    @Test
    public void testFactory() throws Exception {
        assertMemoryLeak(() -> {
            final int N = 100;
            TableToken tableToken;
            // separate two symbol columns with primitive. It will make problems apparent if index does not shift correctly
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.DAY).
                    col("a", ColumnType.STRING).
                    col("b", ColumnType.SYMBOL).indexed(true, N / 4).
                    col("i", ColumnType.INT).
                    col("c", ColumnType.SYMBOL).indexed(true, N / 4).
                    timestamp()
            ) {
                tableToken = CreateTableTestUtils.create(model);
            }

            final Rnd rnd = new Rnd();
            final String[] symbols = new String[N];
            final int M = 1000;
            final long increment = 1000000 * 60L * 10;

            for (int i = 0; i < N; i++) {
                symbols[i] = rnd.nextChars(8).toString();
            }

            GenericRecordMetadata metadata;
            // prepare the data
            long timestamp = 0;
            try (TableWriter writer = newTableWriter(configuration, "x", metrics)) {
                for (int i = 0; i < M; i++) {
                    TableWriter.Row row = writer.newRow(timestamp += increment);
                    row.putStr(0, rnd.nextChars(20));
                    row.putSym(1, symbols[rnd.nextPositiveInt() % N]);
                    row.putInt(2, rnd.nextInt());
                    row.putSym(3, symbols[rnd.nextPositiveInt() % N]);
                    row.append();
                }
                writer.commit();
                metadata = GenericRecordMetadata.copyOf(writer.getMetadata());
            }

            try (FullFwdDataFrameCursorFactory factory = new FullFwdDataFrameCursorFactory(tableToken, TableUtils.ANY_TABLE_ID, 0, metadata)) {
                long count = 0;
                try (DataFrameCursor cursor = factory.getCursor(new SqlExecutionContextStub(engine), ORDER_ASC)) {
                    DataFrame frame;
                    while ((frame = cursor.next()) != null) {
                        count += frame.getRowHi() - frame.getRowLo();
                    }
                }
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(M, count);

                try (TableWriter writer = TestUtils.getWriter(engine, tableToken)) {
                    writer.removeColumn("b");
                }

                try {
                    factory.getCursor(new SqlExecutionContextStub(engine), ORDER_ASC);
                    Assert.fail();
                } catch (TableReferenceOutOfDateException ignored) {
                }
            }
        });
    }
}
