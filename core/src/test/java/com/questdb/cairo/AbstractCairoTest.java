package com.questdb.cairo;

import com.questdb.misc.Files;
import com.questdb.parser.sql.AbstractOptimiserTest;
import com.questdb.std.str.CompositePath;
import org.junit.After;
import org.junit.BeforeClass;

public class AbstractCairoTest extends AbstractOptimiserTest {
    protected static CharSequence root;

    @BeforeClass
    public static void setUp() throws Exception {
        root = FACTORY_CONTAINER.getConfiguration().getJournalBase().getAbsolutePath();
    }

    @After
    public void tearDown0() throws Exception {
        try (CompositePath path = new CompositePath().of(root)) {
            Files.rmdir(path.$());
        }
    }
}
