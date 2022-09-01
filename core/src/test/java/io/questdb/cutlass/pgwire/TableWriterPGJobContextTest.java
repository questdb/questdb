package io.questdb.cutlass.pgwire;

import org.junit.*;

public class TableWriterPGJobContextTest extends BasePGJobContextTest {
    private static int prevDefaultTableWriteMode = 0;

    @BeforeClass
    public static void setUpStatic() {
        prevDefaultTableWriteMode = defaultTableWriteMode;
        defaultTableWriteMode = 0;  // Use TableWriter, without the WAL.
        BasePGJobContextTest.setUpStatic();
    }

    @AfterClass
    public static void tearDownStatic() {
        BasePGJobContextTest.tearDownStatic();
        defaultTableWriteMode = prevDefaultTableWriteMode;
    }
}
