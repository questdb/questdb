/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TableReaderMetadataTimestampTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public TableReaderMetadataTimestampTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testReAddColumn() throws Exception {
        TableModel model = CreateTableTestUtils.getAllTypesModel(configuration, PartitionBy.NONE);
        model.timestamp(timestampType.getTimestampType());
        AbstractCairoTest.create(model);
        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n" +
                "date:DATE\n" +
                "varchar:" + ColumnType.nameOf(ColumnType.VARCHAR) + "\n" +
                "timestamp:" + timestampType.getTypeName() + "\n" +
                "str:" + ColumnType.nameOf(ColumnType.STRING) + "\n";

        assertThatTimestampRemains((w) -> {
            w.removeColumn("str");
            w.addColumn("str", ColumnType.STRING);
        }, expected, 12, 11, 13);
    }

    @Test
    public void testRemoveColumnAfterTimestamp() throws Exception {
        TableModel model = new TableModel(configuration, "all", PartitionBy.NONE)
                .col("int", ColumnType.INT)
                .col("short", ColumnType.SHORT)
                .col("byte", ColumnType.BYTE)
                .col("double", ColumnType.DOUBLE)
                .col("float", ColumnType.FLOAT)
                .timestamp(timestampType.getTimestampType())
                .col("long", ColumnType.LONG)
                .col("str", ColumnType.STRING)
                .col("sym", ColumnType.SYMBOL)
                .col("bool", ColumnType.BOOLEAN)
                .col("bin", ColumnType.BINARY)
                .col("date", ColumnType.DATE)
                .col("varchar", ColumnType.VARCHAR);

        AbstractCairoTest.create(model);

        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "timestamp:" + timestampType.getTypeName() + "\n" +
                "long:LONG\n" +
                "str:" + ColumnType.nameOf(ColumnType.STRING) + "\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "date:DATE\n" +
                "varchar:" + ColumnType.nameOf(ColumnType.VARCHAR) + "\n";
        assertThatTimestampRemains((w) -> w.removeColumn("bin"), expected, 5, 5, 12);
    }

    @Test
    public void testRemoveColumnBeforeTimestamp() throws Exception {
        TableModel model = CreateTableTestUtils.getAllTypesModel(configuration, PartitionBy.NONE);
        model.timestamp(timestampType.getTimestampType());
        AbstractCairoTest.create(model);
        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n" +
                "date:DATE\n" +
                "varchar:" + ColumnType.nameOf(ColumnType.VARCHAR) + "\n" +
                "timestamp:" + timestampType.getTypeName() + "\n";
        assertThatTimestampRemains((w) -> w.removeColumn("str"), expected, 12, 11, 12);
    }

    @Test
    public void testRemoveFirstTimestamp() throws Exception {
        TableModel model = new TableModel(configuration, "all", PartitionBy.NONE)
                .timestamp(timestampType.getTimestampType())
                .col("int", ColumnType.INT)
                .col("short", ColumnType.SHORT)
                .col("byte", ColumnType.BYTE)
                .col("double", ColumnType.DOUBLE)
                .col("float", ColumnType.FLOAT)
                .col("long", ColumnType.LONG)
                .col("str", ColumnType.STRING)
                .col("sym", ColumnType.SYMBOL)
                .col("bool", ColumnType.BOOLEAN)
                .col("bin", ColumnType.BINARY)
                .col("date", ColumnType.DATE)
                .col("varchar", ColumnType.VARCHAR);
        AbstractCairoTest.create(model);
        assertThat(0);
    }

    @Test
    public void testRemoveMiddleTimestamp() throws Exception {
        TableModel model = new TableModel(configuration, "all", PartitionBy.NONE)
                .col("int", ColumnType.INT)
                .col("short", ColumnType.SHORT)
                .col("byte", ColumnType.BYTE)
                .col("double", ColumnType.DOUBLE)
                .col("float", ColumnType.FLOAT)
                .timestamp(timestampType.getTimestampType())
                .col("long", ColumnType.LONG)
                .col("str", ColumnType.STRING)
                .col("sym", ColumnType.SYMBOL)
                .col("bool", ColumnType.BOOLEAN)
                .col("bin", ColumnType.BINARY)
                .col("date", ColumnType.DATE)
                .col("varchar", ColumnType.VARCHAR);
        AbstractCairoTest.create(model);
        assertThat(5);
    }

    @Test
    public void testRemoveTailTimestamp() throws Exception {
        TableModel model = CreateTableTestUtils.getAllTypesModel(configuration, PartitionBy.NONE)
                .timestamp(timestampType.getTimestampType());
        AbstractCairoTest.create(model);
        assertThat(12);
    }

    private void assertThat(int expectedInitialTimestampIndex) throws Exception {
        int columnCount = 12;
        assertMemoryLeak(() -> {
            String tableName = "all";
            try (TableReaderMetadata metadata = new TableReaderMetadata(configuration, engine.verifyTableName(tableName))) {
                metadata.loadMetadata();
                Assert.assertEquals(13, metadata.getColumnCount());
                Assert.assertEquals(expectedInitialTimestampIndex, metadata.getTimestampIndex());
                long structureVersion;
                try (TableWriter writer = newOffPoolWriter(configuration, tableName)) {
                    writer.removeColumn("timestamp");
                    structureVersion = writer.getMetadataVersion();
                }

                metadata.prepareTransition(structureVersion);
                StringSink sink = new StringSink();
                metadata.applyTransition();
                Assert.assertEquals(columnCount, metadata.getColumnCount());
                for (int i = 0; i < columnCount; i++) {
                    sink.put(metadata.getColumnName(i)).put(':').put(ColumnType.nameOf(metadata.getColumnType(i))).put('\n');
                }

                final String expected = "int:INT\n" +
                        "short:SHORT\n" +
                        "byte:BYTE\n" +
                        "double:DOUBLE\n" +
                        "float:FLOAT\n" +
                        "long:LONG\n" +
                        "str:" + ColumnType.nameOf(ColumnType.STRING) + "\n" +
                        "sym:SYMBOL\n" +
                        "bool:BOOLEAN\n" +
                        "bin:BINARY\n" +
                        "date:DATE\n" +
                        "varchar:" + ColumnType.nameOf(ColumnType.VARCHAR) + "\n";

                TestUtils.assertEquals(expected, sink);
                Assert.assertEquals(-1, metadata.getTimestampIndex());
            }
        });
    }

    private void assertThatTimestampRemains(TableReaderMetadataTest.ColumnManipulator manipulator,
                                            String expected,
                                            int expectedInitialTimestampIndex,
                                            int expectedFinalTimestampIndex,
                                            int expectedColumnCount) throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "all";
            try (TableReaderMetadata metadata = new TableReaderMetadata(configuration, engine.verifyTableName(tableName))) {
                metadata.loadMetadata();
                Assert.assertEquals(13, metadata.getColumnCount());
                Assert.assertEquals(expectedInitialTimestampIndex, metadata.getTimestampIndex());
                long structVersion;
                try (TableWriter writer = newOffPoolWriter(configuration, tableName)) {
                    manipulator.restructure(writer);
                    structVersion = writer.getMetadataVersion();
                }

                metadata.prepareTransition(structVersion);
                StringSink sink = new StringSink();
                metadata.applyTransition();
                Assert.assertEquals(expectedColumnCount, metadata.getColumnCount());
                for (int i = 0; i < expectedColumnCount; i++) {
                    sink.put(metadata.getColumnName(i)).put(':').put(ColumnType.nameOf(metadata.getColumnType(i))).put('\n');
                }

                TestUtils.assertEquals(expected, sink);
                Assert.assertEquals(expectedFinalTimestampIndex, metadata.getTimestampIndex());
            }
        });
    }
}
