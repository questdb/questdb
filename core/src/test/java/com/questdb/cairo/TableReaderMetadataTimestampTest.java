package com.questdb.cairo;

import com.questdb.common.ColumnType;
import com.questdb.common.PartitionBy;
import com.questdb.common.RecordColumnMetadata;
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
                    try (TableWriter writer = new TableWriter(FilesFacadeImpl.INSTANCE, root, "all")) {
                        writer.removeColumn("timestamp");
                    }

                    long address = metadata.createTransitionIndex();
                    StringSink sink = new StringSink();
                    try {
                        metadata.applyTransitionIndex(address);
                        Assert.assertEquals(columnCount, metadata.getColumnCount());
                        for (int i = 0; i < columnCount; i++) {
                            RecordColumnMetadata m = metadata.getColumnQuick(i);
                            sink.put(m.getName()).put(':').put(ColumnType.nameOf(m.getType())).put('\n');
                        }

                        TestUtils.assertEquals(expected, sink);
                        Assert.assertEquals(-1, metadata.getTimestampIndex());
                    } finally {
                        TableReaderMetadata.freeTransitionIndex(address);
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
                    try (TableWriter writer = new TableWriter(FilesFacadeImpl.INSTANCE, root, "all")) {
                        manipulator.restructure(writer);
                    }

                    long address = metadata.createTransitionIndex();
                    StringSink sink = new StringSink();
                    try {
                        metadata.applyTransitionIndex(address);
                        Assert.assertEquals(expectedColumnCount, metadata.getColumnCount());
                        for (int i = 0; i < expectedColumnCount; i++) {
                            RecordColumnMetadata m = metadata.getColumnQuick(i);
                            sink.put(m.getName()).put(':').put(ColumnType.nameOf(m.getType())).put('\n');
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
