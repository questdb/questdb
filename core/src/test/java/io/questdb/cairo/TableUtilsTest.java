/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo;

import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static io.questdb.cairo.TableUtils.TABLE_RESERVED;

public class TableUtilsTest {
    private final static FilesFacade FF = FilesFacadeImpl.INSTANCE;
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

//    @Test
//    public void testCreate() {
//
//        final CharSequence root = temp.getRoot().getAbsolutePath();
//        final JournalMetadata metadata = getJournalStructure().build();
//
//        try (AppendMemory appendMemory = new AppendMemory()) {
//            try (Path path = new Path()) {
//                TableUtils.create(FF, path, appendMemory, root, metadata, 509);
//                Assert.assertEquals(TABLE_EXISTS, TableUtils.exists(FF, path, root, metadata.getName()));
//
//                path.of(root).concat(metadata.getName()).concat("_meta").$();
//
//                try (ReadOnlyMemory mem = new ReadOnlyMemory(FF, path, Files.PAGE_SIZE, FF.length(path))) {
//                    long p = 0;
//                    Assert.assertEquals(metadata.getColumnCount(), mem.getInt(p));
//                    p += 4;
//                    Assert.assertEquals(PartitionBy.NONE, mem.getInt(p));
//                    p += 4;
//                    Assert.assertEquals(metadata.getTimestampIndex(), mem.getInt(p));
//                    p += 4;
//                    Assert.assertEquals(ColumnType.INT, mem.getInt(p));
//                    p += 4;
//                    Assert.assertEquals(ColumnType.DOUBLE, mem.getInt(p));
//                    p += 4;
//                    Assert.assertEquals(ColumnType.FLOAT, mem.getInt(p));
//                    p += 4;
//                    Assert.assertEquals(ColumnType.BYTE, mem.getInt(p));
//                    p += 4;
//                    Assert.assertEquals(ColumnType.LONG, mem.getInt(p));
//                    p += 4;
//                    Assert.assertEquals(ColumnType.STRING, mem.getInt(p));
//                    p += 4;
//                    Assert.assertEquals(ColumnType.BOOLEAN, mem.getInt(p));
//                    p += 4;
//                    Assert.assertEquals(ColumnType.SYMBOL, mem.getInt(p));
//                    p += 4;
//                    Assert.assertEquals(ColumnType.SHORT, mem.getInt(p));
//                    p += 4;
//                    Assert.assertEquals(ColumnType.DATE, mem.getInt(p));
//                    p += 4;
//                    Assert.assertEquals(ColumnType.DATE, mem.getInt(p));
//                    p += 4;
//
//                    p = assertCol(mem, p, "i");
//                    p = assertCol(mem, p, "d");
//
//                    p = assertCol(mem, p, "f");
//                    p = assertCol(mem, p, "b");
//                    p = assertCol(mem, p, "l");
//                    p = assertCol(mem, p, "str");
//                    p = assertCol(mem, p, "boo");
//                    p = assertCol(mem, p, "sym");
//                    p = assertCol(mem, p, "sho");
//                    p = assertCol(mem, p, "date");
//                    assertCol(mem, p, "timestamp");
//                }
//            }
//        }
//    }
//
//    @Test
//    public void testCreateFailure() throws Exception {
//        TestUtils.assertMemoryLeak(() -> {
//            class X extends FilesFacadeImpl {
//                @Override
//                public int mkdirs(LPSZ path, int mode) {
//                    return -1;
//                }
//            }
//
//            X ff = new X();
//            JournalMetadata metadata = getJournalStructure().build();
//            try (AppendMemory appendMemory = new AppendMemory()) {
//                try (Path path = new Path()) {
//                    TableUtils.create(ff, path, appendMemory, temp.getRoot().getAbsolutePath(), metadata, 509);
//                    Assert.fail();
//                } catch (CairoException e) {
//                    Assert.assertNotNull(e.getMessage());
//                }
//            }
//        });
//    }

    @Test
    public void testForeignDirectory() {
        try (Path path = new Path()) {
            Assert.assertEquals(TABLE_RESERVED, TableUtils.exists(FF, path, temp.getRoot().getAbsolutePath(), ""));
        }
    }

    @Test
    public void testUnknownTodo() {
        TestUtils.assertEquals("unknown", TableUtils.getTodoText(7879797987L));
    }

    @Test
    public void testIsValidInfluxColumnName() {
        Assert.assertTrue(TableUtils.isValidInfluxColumnName("a"));

        Assert.assertFalse(TableUtils.isValidInfluxColumnName("_a"));

        Assert.assertFalse(TableUtils.isValidInfluxColumnName("-a"));

        Assert.assertFalse(TableUtils.isValidInfluxColumnName("a-"));

        Assert.assertTrue(TableUtils.isValidInfluxColumnName("a_"));

        Assert.assertTrue(TableUtils.isValidInfluxColumnName("a_b"));

        Assert.assertTrue(TableUtils.isValidInfluxColumnName("data_connectionSource_user-agent"));
    }
}