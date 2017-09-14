package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.misc.Files;
import com.questdb.misc.FilesFacadeImpl;
import com.questdb.ql.parser.AbstractOptimiserTest;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.StringSink;
import com.questdb.store.ColumnType;
import com.questdb.test.tools.TestUtils;
import org.junit.*;

public class TableMetadataTest extends AbstractOptimiserTest {
    private static CharSequence root;

    @BeforeClass
    public static void setUp() throws Exception {
        root = FACTORY_CONTAINER.getConfiguration().getJournalBase().getAbsolutePath();
    }

    @Before
    public void setUp2() throws Exception {
        CairoTestUtils.createAllTable(root, PartitionBy.DAY);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        try (CompositePath path = new CompositePath().of(root).concat("all").$()) {
            Files.rmdir(path);
        }
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
    private interface ColumnManipulator {
        void restructure(TableWriter writer);
    }

}