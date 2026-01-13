/*******************************************************************************
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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.FullPartitionFrameCursorFactory;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.std.IntList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cutlass.text.SqlExecutionContextStub;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

@RunWith(Parameterized.class)
public class FullPartitionFrameCursorFactoryTest extends AbstractCairoTest {
    private final boolean convertToParquet;

    public FullPartitionFrameCursorFactoryTest(boolean convertToParquet) {
        this.convertToParquet = convertToParquet;
    }

    @Parameterized.Parameters(name = "parquet={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {true},
                {false},
        });
    }

    @Test
    public void testFactory() throws Exception {
        assertMemoryLeak(() -> {
            final int N = 100;
            TableToken tableToken;
            // separate two symbol columns with primitive. It will make problems apparent if index does not shift correctly
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY).
                    col("a", ColumnType.STRING).
                    col("b", ColumnType.SYMBOL).indexed(true, N / 4).
                    col("i", ColumnType.INT).
                    col("c", ColumnType.SYMBOL).indexed(true, N / 4).
                    timestamp();
            tableToken = AbstractCairoTest.create(model);

            final Rnd rnd = new Rnd();
            final String[] symbols = new String[N];
            final int M = 1000;
            final long increment = 1000000L * 60 * 10;

            for (int i = 0; i < N; i++) {
                symbols[i] = rnd.nextChars(8).toString();
            }

            GenericRecordMetadata metadata;
            // prepare the data
            long timestamp = 0;
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
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

            if (convertToParquet) {
                execute("alter table x convert partition to parquet where timestamp >= 0;");
            }

            try (FullPartitionFrameCursorFactory factory = new FullPartitionFrameCursorFactory(tableToken, 0, metadata, ORDER_ASC, null, 0, false)) {
                long count = 0;
                try (PartitionFrameCursor cursor = factory.getCursor(new SqlExecutionContextStub(engine), new IntList(), ORDER_ASC)) {
                    PartitionFrame frame;
                    while ((frame = cursor.next()) != null) {
                        count += frame.getRowHi() - frame.getRowLo();
                    }
                }
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(M, count);

                // TODO(puzpuzpuz): remove if condition when we handle column drop properly in parquet
                if (!convertToParquet) {
                    try (TableWriter writer = TestUtils.getWriter(engine, tableToken)) {
                        writer.removeColumn("b");
                    }

                    try {
                        factory.getCursor(new SqlExecutionContextStub(engine), new IntList(), ORDER_ASC);
                        Assert.fail();
                    } catch (TableReferenceOutOfDateException ignored) {
                    }
                }
            }
        });
    }
}
