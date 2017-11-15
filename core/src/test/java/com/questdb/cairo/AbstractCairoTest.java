package com.questdb.cairo;

import com.questdb.parser.sql.AbstractOptimiserTest;
import com.questdb.std.Files;
import com.questdb.std.str.Path;
import org.junit.After;
import org.junit.BeforeClass;

public class AbstractCairoTest extends AbstractOptimiserTest {
    protected static CharSequence root;
    protected static CairoConfiguration configuration;

    @BeforeClass
    public static void setUp() throws Exception {
        root = FACTORY_CONTAINER.getConfiguration().getJournalBase().getAbsolutePath();
        configuration = new DefaultCairoConfiguration(root);
    }

    @After
    public void tearDown0() throws Exception {
        try (Path path = new Path().of(root)) {
            Files.rmdir(path.$());
        }
    }
}
