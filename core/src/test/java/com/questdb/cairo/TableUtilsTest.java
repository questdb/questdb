package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.Files;
import com.questdb.misc.FilesFacade;
import com.questdb.misc.FilesFacadeImpl;
import com.questdb.std.str.CompositePath;
import com.questdb.store.ColumnType;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TableUtilsTest {
    private final static FilesFacade FF = FilesFacadeImpl.INSTANCE;
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testCreate() throws Exception {
        JournalStructure struct = new JournalStructure("abc")
                .$int("i")
                .$double("d")
                .$float("f")
                .$byte("b")
                .$long("l")
                .$str("str")
                .$bool("boo")
                .$sym("sym")
                .$short("sho")
                .$date("date")
                .$ts()
                .$();

        final CharSequence root = temp.getRoot().getAbsolutePath();
        final JournalMetadata metadata = struct.build();

        try (TableUtils tabU = new TableUtils(FF)) {
            tabU.create(root, metadata, 509);

            try (CompositePath path = new CompositePath()) {
                Assert.assertEquals(0, tabU.exists(root, metadata.getName()));

                path.of(root).concat(metadata.getName()).concat("_meta").$();

                try (ReadOnlyMemory mem = new ReadOnlyMemory(FF, path, Files.PAGE_SIZE, Files.length(path))) {
                    long p = 0;
                    Assert.assertEquals(metadata.getColumnCount(), mem.getInt(p));
                    p += 4;
                    Assert.assertEquals(PartitionBy.NONE, mem.getInt(p));
                    p += 4;
                    Assert.assertEquals(ColumnType.INT, mem.getInt(p));
                    p += 4;
                    Assert.assertEquals(ColumnType.DOUBLE, mem.getInt(p));
                    p += 4;
                    Assert.assertEquals(ColumnType.FLOAT, mem.getInt(p));
                    p += 4;
                    Assert.assertEquals(ColumnType.BYTE, mem.getInt(p));
                    p += 4;
                    Assert.assertEquals(ColumnType.LONG, mem.getInt(p));
                    p += 4;
                    Assert.assertEquals(ColumnType.STRING, mem.getInt(p));
                    p += 4;
                    Assert.assertEquals(ColumnType.BOOLEAN, mem.getInt(p));
                    p += 4;
                    Assert.assertEquals(ColumnType.SYMBOL, mem.getInt(p));
                    p += 4;
                    Assert.assertEquals(ColumnType.SHORT, mem.getInt(p));
                    p += 4;
                    Assert.assertEquals(ColumnType.DATE, mem.getInt(p));
                    p += 4;
                    Assert.assertEquals(ColumnType.DATE, mem.getInt(p));
                    p += 4;

                    p = assertCol(mem, p, "i");
                    p = assertCol(mem, p, "d");

                    p = assertCol(mem, p, "f");
                    p = assertCol(mem, p, "b");
                    p = assertCol(mem, p, "l");
                    p = assertCol(mem, p, "str");
                    p = assertCol(mem, p, "boo");
                    p = assertCol(mem, p, "sym");
                    p = assertCol(mem, p, "sho");
                    p = assertCol(mem, p, "date");
                    assertCol(mem, p, "timestamp");
                }
            }
        }
    }

    private static long assertCol(ReadOnlyMemory mem, long p, CharSequence expected) {
        CharSequence name = mem.getStr(p);
        TestUtils.assertEquals(expected, name);
        Assert.assertNotNull(name);
        p += 4 + name.length() * 2;
        return p;
    }
}