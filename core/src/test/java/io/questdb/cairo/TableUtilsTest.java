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

import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static io.questdb.cairo.TableUtils.TABLE_RESERVED;

public class TableUtilsTest {
    private final static FilesFacade FF = FilesFacadeImpl.INSTANCE;
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

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
        Assert.assertTrue(TableUtils.isValidTableName("asdfasdf", 127));

        Assert.assertFalse(TableUtils.isValidTableName("abc", 2));
    }

    private void testIsValidColumnName(char c, boolean expected) {
        Assert.assertEquals(expected, TableUtils.isValidColumnName(Character.toString(c), 127));
        Assert.assertEquals(expected, TableUtils.isValidColumnName(c + "abc", 127));
        Assert.assertEquals(expected, TableUtils.isValidColumnName("abc" + c, 127));
        Assert.assertEquals(expected, TableUtils.isValidColumnName("ab" + c + "c", 127));
    }
}
