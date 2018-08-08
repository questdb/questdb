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

package com.questdb.cairo;

import com.questdb.std.FilesFacadeImpl;
import com.questdb.std.str.Path;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableReaderMetadataTimestampTest extends AbstractCairoTest {

    @Test
    public void testReAddColumn() throws Exception {
        try (TableModel model = CairoTestUtils.getAllTypesModel(configuration, PartitionBy.NONE)) {
            model.timestamp();
            CairoTestUtils.create(model);
        }
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
                "timestamp:TIMESTAMP\n" +
                "str:STRING\n";
        assertThatTimestampRemains((w) -> {
            w.removeColumn("str");
            w.addColumn("str", ColumnType.STRING);
        }, expected, 11, 10, 12);
    }

    @Test
    public void testRemoveColumnAfterTimestamp() throws Exception {
        try (TableModel model = new TableModel(configuration, "all", PartitionBy.NONE)
                .col("int", ColumnType.INT)
                .col("short", ColumnType.SHORT)
                .col("byte", ColumnType.BYTE)
                .col("double", ColumnType.DOUBLE)
                .col("float", ColumnType.FLOAT)
                .timestamp()
                .col("long", ColumnType.LONG)
                .col("str", ColumnType.STRING)
                .col("sym", ColumnType.SYMBOL)
                .col("bool", ColumnType.BOOLEAN)
                .col("bin", ColumnType.BINARY)
                .col("date", ColumnType.DATE)) {

            CairoTestUtils.create(model);
        }

        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "timestamp:TIMESTAMP\n" +
                "long:LONG\n" +
                "str:STRING\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "date:DATE\n";
        assertThatTimestampRemains((w) -> w.removeColumn("bin"), expected, 5, 5, 11);
    }

    @Test
    public void testRemoveColumnBeforeTimestamp() throws Exception {
        try (TableModel model = CairoTestUtils.getAllTypesModel(configuration, PartitionBy.NONE)) {
            model.timestamp();
            CairoTestUtils.create(model);
        }
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
                "timestamp:TIMESTAMP\n";
        assertThatTimestampRemains((w) -> w.removeColumn("str"), expected, 11, 10, 11);
    }

    @Test
    public void testRemoveFirstTimestamp() throws Exception {
        try (TableModel model = new TableModel(configuration, "all", PartitionBy.NONE)
                .timestamp()
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
                .col("date", ColumnType.DATE)) {

            CairoTestUtils.create(model);
        }
        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "str:STRING\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n" +
                "date:DATE\n";
        assertThat(expected, 0);
    }

    @Test
    public void testRemoveMiddleTimestamp() throws Exception {
        try (TableModel model = new TableModel(configuration, "all", PartitionBy.NONE)
                .col("int", ColumnType.INT)
                .col("short", ColumnType.SHORT)
                .col("byte", ColumnType.BYTE)
                .col("double", ColumnType.DOUBLE)
                .col("float", ColumnType.FLOAT)
                .timestamp()
                .col("long", ColumnType.LONG)
                .col("str", ColumnType.STRING)
                .col("sym", ColumnType.SYMBOL)
                .col("bool", ColumnType.BOOLEAN)
                .col("bin", ColumnType.BINARY)
                .col("date", ColumnType.DATE)) {

            CairoTestUtils.create(model);
        }

        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "str:STRING\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n" +
                "date:DATE\n";

        assertThat(expected, 5);
    }

    @Test
    public void testRemoveTailTimestamp() throws Exception {
        try (TableModel model = CairoTestUtils.getAllTypesModel(configuration, PartitionBy.NONE)
                .timestamp()) {
            CairoTestUtils.create(model);
        }
        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "str:STRING\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n" +
                "date:DATE\n";
        assertThat(expected, 11);
    }

    private void assertThat(String expected, int expectedInitialTimestampIndex) throws Exception {
        int columnCount = 11;
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(root).concat("all")) {
                try (TableReaderMetadata metadata = new TableReaderMetadata(FilesFacadeImpl.INSTANCE, path.concat(TableUtils.META_FILE_NAME).$())) {

                    Assert.assertEquals(12, metadata.getColumnCount());
                    Assert.assertEquals(expectedInitialTimestampIndex, metadata.getTimestampIndex());
                    try (TableWriter writer = new TableWriter(configuration, "all")) {
                        writer.removeColumn("timestamp");
                    }

                    long pTransitionIndex = metadata.createTransitionIndex();
                    StringSink sink = new StringSink();
                    try {
                        metadata.applyTransitionIndex(pTransitionIndex);
                        Assert.assertEquals(columnCount, metadata.getColumnCount());
                        for (int i = 0; i < columnCount; i++) {
                            sink.put(metadata.getColumnName(i)).put(':').put(ColumnType.nameOf(metadata.getColumnType(i))).put('\n');
                        }

                        TestUtils.assertEquals(expected, sink);
                        Assert.assertEquals(-1, metadata.getTimestampIndex());
                    } finally {
                        TableReaderMetadata.freeTransitionIndex(pTransitionIndex);
                    }
                }
            }
        });
    }

    private void assertThatTimestampRemains(TableReaderMetadataTest.ColumnManipulator manipulator,
                                            String expected,
                                            int expectedInitialTimestampIndex,
                                            int expectedFinalTimestampIndex,
                                            int expectedColumnCount) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(root).concat("all")) {
                try (TableReaderMetadata metadata = new TableReaderMetadata(FilesFacadeImpl.INSTANCE, path.concat(TableUtils.META_FILE_NAME).$())) {

                    Assert.assertEquals(12, metadata.getColumnCount());
                    Assert.assertEquals(expectedInitialTimestampIndex, metadata.getTimestampIndex());
                    try (TableWriter writer = new TableWriter(configuration, "all")) {
                        manipulator.restructure(writer);
                    }

                    long address = metadata.createTransitionIndex();
                    StringSink sink = new StringSink();
                    try {
                        metadata.applyTransitionIndex(address);
                        Assert.assertEquals(expectedColumnCount, metadata.getColumnCount());
                        for (int i = 0; i < expectedColumnCount; i++) {
                            sink.put(metadata.getColumnName(i)).put(':').put(ColumnType.nameOf(metadata.getColumnType(i))).put('\n');
                        }

                        TestUtils.assertEquals(expected, sink);
                        Assert.assertEquals(expectedFinalTimestampIndex, metadata.getTimestampIndex());
                    } finally {
                        TableReaderMetadata.freeTransitionIndex(address);
                    }
                }
            }
        });
    }
}
