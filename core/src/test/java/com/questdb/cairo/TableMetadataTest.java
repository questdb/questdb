package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.std.ObjIntHashMap;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.StringSink;
import com.questdb.store.ColumnType;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TableMetadataTest extends AbstractCairoTest {

    @Before
    public void setUp2() throws Exception {
        CairoTestUtils.createAllTable(root, PartitionBy.DAY);
    }

    @Test
    public void testAddColumn() throws Exception {
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
                "date:DATE\n" +
                "xyz:STRING\n";
        assertThat(expected, (w) -> w.addColumn("xyz", ColumnType.STRING), 12);
    }

    @Test
    public void testColumnIndex() throws Exception {
        ObjIntHashMap<String> expected = new ObjIntHashMap<>();
        expected.put("int", 0);
        expected.put("byte", 2);
        expected.put("bin", 9);
        expected.put("short", 1);
        expected.put("float", 4);
        expected.put("long", 5);
        expected.put("xyz", -1);
        expected.put("str", 6);
        expected.put("double", 3);
        expected.put("sym", 7);
        expected.put("bool", 8);

        try (CompositePath path = new CompositePath().of(root).concat("all").concat(TableUtils.META_FILE_NAME).$();
             TableMetadata metadata = new TableMetadata(FilesFacadeImpl.INSTANCE, path)) {
            for (ObjIntHashMap.Entry<String> e : expected) {
                Assert.assertEquals(e.value, metadata.getColumnIndexQuiet(e.key));
            }
        }
    }

    @Test
    public void testDeleteTwoAddOneColumn() throws Exception {
        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n" +
                "date:DATE\n" +
                "xyz:STRING\n";
        assertThat(expected, (w) -> {
            w.removeColumn("double");
            w.removeColumn("str");
            w.addColumn("xyz", ColumnType.STRING);

        }, 10);
    }

    @Test
    public void testFreeNullAddressAsIndex() throws Exception {
        TableMetadata.freeTransitionIndex(0);
    }

    @Test
    public void testRemoveAllColumns() throws Exception {
        final String expected = "";
        assertThat(expected, (w) -> {
            w.removeColumn("int");
            w.removeColumn("short");
            w.removeColumn("byte");
            w.removeColumn("float");
            w.removeColumn("long");
            w.removeColumn("str");
            w.removeColumn("sym");
            w.removeColumn("bool");
            w.removeColumn("bin");
            w.removeColumn("date");
            w.removeColumn("double");
        }, 0);
    }

    @Test
    public void testRemoveAndAddSameColumn() throws Exception {
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
                "str:STRING\n";
        assertThat(expected, (w) -> {
            w.removeColumn("str");
            w.addColumn("str", ColumnType.STRING);
        }, 11);
    }

    @Test
    public void testRemoveColumnAndReAdd() throws Exception {
        final String expected = "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n" +
                "date:DATE\n" +
                "str:STRING\n" +
                "short:INT\n";

        assertThat(expected, (w) -> {
            w.removeColumn("short");
            w.removeColumn("str");
            w.removeColumn("int");
            w.addColumn("str", ColumnType.STRING);
            // change column type
            w.addColumn("short", ColumnType.INT);
        }, 10);
    }

    @Test
    public void testRemoveDenseColumns() throws Exception {
        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "long:LONG\n" +
                "str:STRING\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n" +
                "date:DATE\n";
        assertThat(expected, (w) -> {
            w.removeColumn("double");
            w.removeColumn("float");
        }, 9);
    }

    @Test
    public void testRemoveFirstAndLastColumns() throws Exception {
        final String expected = "short:SHORT\n" +
                "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "str:STRING\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n";
        assertThat(expected, (w) -> {
            w.removeColumn("date");
            w.removeColumn("int");
        }, 9);
    }

    @Test
    public void testRemoveFirstColumn() throws Exception {
        final String expected =
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
        assertThat(expected, (w) -> w.removeColumn("int"), 10);
    }

    @Test
    public void testRemoveLastColumn() throws Exception {
        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "str:STRING\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n";
        assertThat(expected, (w) -> w.removeColumn("date"), 10);
    }

    @Test
    public void testRemoveSparseColumns() throws Exception {
        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n" +
                "date:DATE\n";

        assertThat(expected, (w) -> {
            w.removeColumn("double");
            w.removeColumn("str");
        }, 9);
    }

    private void assertThat(String expected, ColumnManipulator manipulator, int columnCount) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CompositePath path = new CompositePath().of(root).concat("all")) {
                try (TableMetadata metadata = new TableMetadata(FilesFacadeImpl.INSTANCE, path.concat(TableUtils.META_FILE_NAME).$())) {

                    try (TableWriter writer = new TableWriter(FilesFacadeImpl.INSTANCE, root, "all")) {
                        manipulator.restructure(writer);
                    }

                    try {
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

                            if (expected.length() > 0) {
                                String[] lines = expected.split("\n");
                                Assert.assertEquals(columnCount, lines.length);

                                for (int i = 0; i < columnCount; i++) {
                                    int p = lines[i].indexOf(':');
                                    Assert.assertEquals(i, metadata.getColumnIndexQuiet(lines[i].substring(0, p)));
                                }
                            }
                        } finally {
                            TableMetadata.freeTransitionIndex(address);
                        }
                    } finally {
                        TableUtils.freeThreadLocals();
                    }
                }
            }
        });
    }

    @FunctionalInterface
    public interface ColumnManipulator {
        void restructure(TableWriter writer);
    }
}