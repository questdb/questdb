/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;

import static io.questdb.cairo.TableUtils.TABLE_RESERVED;

public class TableUtilsTest {
    private final static FilesFacade FF = TestFilesFacadeImpl.INSTANCE;
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    @Rule
    public final TestName testName = new TestName();

    private Path path;

    @Before
    public void setUp() throws IOException {
        path = new Path();
    }

    @After
    public void tearDown() {
        Misc.free(path);
    }

    @Test
    public void testCreateTableInVolumeFailsCauseTableExistsAsADir() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        String tableName = testName.getMethodName();
        File dbRoot = temp.newFolder(tableName, "db");
        File volumeRoot = temp.newFolder(tableName, "volume");
        path.of(volumeRoot.getAbsolutePath()).concat(tableName).$();
        Assert.assertTrue(new File(dbRoot, tableName).mkdir());
        try {
            TableUtils.createTable(
                    FF,
                    dbRoot.getAbsolutePath(),
                    509,
                    null,
                    path,
                    true,
                    tableName,
                    null,
                    0,
                    0);
            Assert.fail();
        } catch (CairoException e) {
            path.of(dbRoot.getAbsolutePath()).concat(tableName).$();
            TestUtils.assertContains(e.getFlyweightMessage(), "table folder already exists in volume [path=" + path.toString() + ']');
        } finally {
            dbRoot.delete();
            volumeRoot.delete();
        }
    }

    @Test
    public void testCreateTableInVolumeFailsCauseTableExistsAsADirInVolume() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        String tableName = testName.getMethodName();
        File dbRoot = temp.newFolder(tableName, "db");
        File volumeRoot = temp.newFolder(tableName, "volume");
        path.of(volumeRoot.getAbsolutePath()).concat(tableName).$();
        Assert.assertTrue(new File(volumeRoot, tableName).mkdir());
        try {
            TableUtils.createTable(
                    FF,
                    dbRoot.getAbsolutePath(),
                    509,
                    null,
                    path,
                    true,
                    tableName,
                    null,
                    0,
                    0);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "table folder already exists in volume [path=" + path.toString() + ']');
        } finally {
            dbRoot.delete();
            volumeRoot.delete();
        }
    }

    @Test
    public void testCreateTableInVolumeFailsCauseTableExistsAsAFile() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        String tableName = testName.getMethodName();
        File dbRoot = temp.newFolder(tableName, "db");
        File volumeRoot = temp.newFolder(tableName, "volume");
        path.of(volumeRoot.getAbsolutePath()).concat(tableName).$();
        Assert.assertTrue(new File(dbRoot, tableName).createNewFile());
        try {
            TableUtils.createTable(
                    FF,
                    dbRoot.getAbsolutePath(),
                    509,
                    null,
                    path,
                    true,
                    tableName,
                    null,
                    0,
                    0);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "could not create soft link [src=" + path.toString() + ", tableName=" + tableName + ']');
            Assert.assertFalse(Files.exists(path));
        } finally {
            dbRoot.delete();
            volumeRoot.delete();
        }
    }

    @Test
    public void testForeignDirectory() {
        try (Path path = new Path()) {
            Assert.assertEquals(TABLE_RESERVED, TableUtils.exists(FF, path, temp.getRoot().getAbsolutePath(), ""));
        }
    }

    @Test
    public void testIsValidColumnName() {
        testIsValidColumnName('?', false);
        testIsValidColumnName('.', false);
        testIsValidColumnName(',', false);
        testIsValidColumnName('\'', false);
        testIsValidColumnName('\"', false);
        testIsValidColumnName('\\', false);
        testIsValidColumnName('/', false);
        testIsValidColumnName('\0', false);
        testIsValidColumnName(':', false);
        testIsValidColumnName(')', false);
        testIsValidColumnName('(', false);
        testIsValidColumnName('+', false);
        testIsValidColumnName('-', false);
        testIsValidColumnName('*', false);
        testIsValidColumnName('%', false);
        testIsValidColumnName('~', false);
        testIsValidColumnName('\n', false);
        Assert.assertFalse(TableUtils.isValidColumnName("..", 127));
        Assert.assertFalse(TableUtils.isValidColumnName(".", 127));
        Assert.assertFalse(TableUtils.isValidColumnName("t\u007Ftcsv", 127));

        testIsValidColumnName('!', true);
        testIsValidColumnName('a', true);
        testIsValidColumnName('b', true);
        testIsValidColumnName('^', true);
        testIsValidColumnName('[', true);
        testIsValidColumnName('$', true);
        Assert.assertFalse(TableUtils.isValidColumnName("", 2));
        Assert.assertFalse(TableUtils.isValidColumnName("abc", 2));
    }

    @Test
    public void testIsValidTableName() {
        Assert.assertFalse(TableUtils.isValidTableName("?abcd", 127));
        Assert.assertFalse(TableUtils.isValidTableName("", 127));
        Assert.assertFalse(TableUtils.isValidTableName(" ", 127));
        Assert.assertFalse(TableUtils.isValidTableName("./", 127));
        Assert.assertFalse(TableUtils.isValidTableName("/asdf", 127));
        Assert.assertFalse(TableUtils.isValidTableName("\\asdf", 127));
        Assert.assertFalse(TableUtils.isValidTableName("asdf\rasdf", 127));
        Assert.assertFalse(TableUtils.isValidTableName("t..t.csv", 127));
        Assert.assertFalse(TableUtils.isValidTableName("\"", 127));
        Assert.assertFalse(TableUtils.isValidTableName("t\u007Ft.csv", 127));
        Assert.assertFalse(TableUtils.isValidTableName(".", 127));
        Assert.assertFalse(TableUtils.isValidTableName("..", 127));
        Assert.assertFalse(TableUtils.isValidTableName("...", 127));
        Assert.assertFalse(TableUtils.isValidTableName("..\\", 127));
        Assert.assertFalse(TableUtils.isValidTableName("\\..", 127));
        Assert.assertFalse(TableUtils.isValidTableName("/..", 127));
        Assert.assertFalse(TableUtils.isValidTableName("../", 127));

        Assert.assertTrue(TableUtils.isValidTableName("table name", 127));
        Assert.assertTrue(TableUtils.isValidTableName("table name.csv", 127));
        Assert.assertTrue(TableUtils.isValidTableName("table-name", 127));
        Assert.assertTrue(TableUtils.isValidTableName("table_name", 127));
        Assert.assertTrue(TableUtils.isValidTableName("table$name", 127));
        Assert.assertFalse(TableUtils.isValidTableName("asdf\nasdf", 127));

        Assert.assertFalse(TableUtils.isValidTableName("abc", 2));
        Assert.assertTrue(TableUtils.isValidTableName("الْعَرَبِيَّة", 127));
    }

    private void testIsValidColumnName(char c, boolean expected) {
        Assert.assertEquals(expected, TableUtils.isValidColumnName(Character.toString(c), 127));
        Assert.assertEquals(expected, TableUtils.isValidColumnName(c + "abc", 127));
        Assert.assertEquals(expected, TableUtils.isValidColumnName("abc" + c, 127));
        Assert.assertEquals(expected, TableUtils.isValidColumnName("ab" + c + "c", 127));
    }
}
