package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.misc.FilesFacadeImpl;
import com.questdb.ql.parser.AbstractOptimiserTest;
import com.questdb.std.str.CompositePath;
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

        TestUtils.assertMemoryLeak(() -> {
            try (CompositePath path = new CompositePath().of(root).concat("all")) {
                try (TableMetadata metadata = new TableMetadata(FilesFacadeImpl.INSTANCE, path.concat(TableUtils.META_FILE_NAME).$())) {
                    long address = metadata.createTransitionIndex();
                    try {
                        metadata.applyTransitionIndex(address);
                    } finally {
                        TableMetadata.freeTransitionIndex(address);
                    }
                }
            }
        });
    }

}