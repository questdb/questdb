package com.questdb.cairo;

import com.questdb.misc.FilesFacadeImpl;
import com.questdb.std.str.Path;
import com.questdb.std.str.StringSink;
import com.questdb.store.ColumnType;
import com.questdb.store.PartitionBy;
import com.questdb.store.factory.configuration.JournalStructure;
import com.questdb.store.factory.configuration.RecordColumnMetadata;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableReaderMetadataTimestampTest extends AbstractCairoTest {

    @Test
    public void testReAddColumn() throws Exception {
        JournalStructure structure = CairoTestUtils.getAllStructure();
        CairoTestUtils.createTable(FilesFacadeImpl.INSTANCE, root, structure.$ts().partitionBy(PartitionBy.NONE));
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
                "timestamp:DATE\n" +
                "str:STRING\n";
        assertThatTimestampRemains((w) -> {
            w.removeColumn("str");
            w.addColumn("str", ColumnType.STRING);
        }, expected, 11, 10, 12);
    }

    @Test
    public void testRemoveColumnAfterTimestamp() throws Exception {
        JournalStructure structure = new JournalStructure("all").
                $int("int").
                $short("short").
                $byte("byte").
                $double("double").
                $float("float").
                $ts().
                $long("long").
                $str("str").
                $sym("sym").
                $bool("bool").
                $bin("bin").
                $date("date").partitionBy(PartitionBy.NONE);


        CairoTestUtils.createTable(FilesFacadeImpl.INSTANCE, root, structure);
        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "timestamp:DATE\n" +
                "long:LONG\n" +
                "str:STRING\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "date:DATE\n";
        assertThatTimestampRemains((w) -> w.removeColumn("bin"), expected, 5, 5, 11);
    }

    @Test
    public void testRemoveColumnBeforeTimestamp() throws Exception {
        JournalStructure structure = CairoTestUtils.getAllStructure();
        CairoTestUtils.createTable(FilesFacadeImpl.INSTANCE, root, structure.$ts().partitionBy(PartitionBy.NONE));
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
                "timestamp:DATE\n";
        assertThatTimestampRemains((w) -> w.removeColumn("str"), expected, 11, 10, 11);
    }

    @Test
    public void testRemoveFirstTimestamp() throws Exception {
        JournalStructure structure = new JournalStructure("all").
                $ts().
                $int("int").
                $short("short").
                $byte("byte").
                $double("double").
                $float("float").
                $long("long").
                $str("str").
                $sym("sym").
                $bool("bool").
                $bin("bin").
                $date("date").partitionBy(PartitionBy.NONE);

        CairoTestUtils.createTable(FilesFacadeImpl.INSTANCE, root, structure);
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
        JournalStructure structure = new JournalStructure("all").
                $int("int").
                $short("short").
                $byte("byte").
                $double("double").
                $float("float").
                $ts().
                $long("long").
                $str("str").
                $sym("sym").
                $bool("bool").
                $bin("bin").
                $date("date").partitionBy(PartitionBy.NONE);

        CairoTestUtils.createTable(FilesFacadeImpl.INSTANCE, root, structure);
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
        JournalStructure structure = CairoTestUtils.getAllStructure();
        CairoTestUtils.createTable(FilesFacadeImpl.INSTANCE, root, structure.$ts().partitionBy(PartitionBy.NONE));
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
