package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.misc.FilesFacadeImpl;
import com.questdb.ql.parser.AbstractOptimiserTest;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.StringSink;
import com.questdb.store.ColumnType;
import com.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class TableMetadataTest extends AbstractOptimiserTest {
    private static CharSequence root;

    @BeforeClass
    public static void setUp() throws Exception {
        root = FACTORY_CONTAINER.getConfiguration().getJournalBase().getAbsolutePath();
    }

    @Test
    public void testMeta() throws Exception {
        CairoTestUtils.createAllTable(root, PartitionBy.DAY);

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

        TestUtils.assertMemoryLeak(() -> {
            try (CompositePath path = new CompositePath().of(root).concat("all")) {
                try (TableMetadata metadata = new TableMetadata(FilesFacadeImpl.INSTANCE, path.concat(TableUtils.META_FILE_NAME).$())) {

                    TableUtils.addColumn(FilesFacadeImpl.INSTANCE, root, "all", "xyz", ColumnType.STRING);
                    try {
                        long address = metadata.createTransitionIndex();
                        StringSink sink = new StringSink();
                        try {
                            metadata.applyTransitionIndex(address);
                            for (int i = 0; i < metadata.getColumnCount(); i++) {
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

}