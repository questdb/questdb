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
        Assert.assertFalse(TableUtils.isValidColumnName(".."));
        Assert.assertFalse(TableUtils.isValidColumnName("."));
        Assert.assertFalse(TableUtils.isValidColumnName("t\u007Ftcsv"));

        testIsValidColumnName('!', true);
        testIsValidColumnName('a', true);
        testIsValidColumnName('b', true);
        testIsValidColumnName('^', true);
        testIsValidColumnName('[', true);
        testIsValidColumnName('$', true);
    }

    private void testIsValidColumnName(char c, boolean expected) {
        Assert.assertEquals(expected, TableUtils.isValidColumnName(Character.toString(c)));
        Assert.assertEquals(expected, TableUtils.isValidColumnName(c + "abc"));
        Assert.assertEquals(expected, TableUtils.isValidColumnName("abc" + c));
        Assert.assertEquals(expected, TableUtils.isValidColumnName("ab" + c + "c"));
    }

    @Test
    public void testIsValidTableName() {
        Assert.assertFalse(TableUtils.isValidTableName("?abcd"));
        Assert.assertFalse(TableUtils.isValidTableName(""));
        Assert.assertFalse(TableUtils.isValidTableName(" "));
        Assert.assertFalse(TableUtils.isValidTableName("./"));
        Assert.assertFalse(TableUtils.isValidTableName("/asdf"));
        Assert.assertFalse(TableUtils.isValidTableName("\\asdf"));
        Assert.assertFalse(TableUtils.isValidTableName("asdf\nasdf"));
        Assert.assertFalse(TableUtils.isValidTableName("asdf\rasdf"));
        Assert.assertFalse(TableUtils.isValidTableName("t..t.csv"));
        Assert.assertFalse(TableUtils.isValidTableName("\""));
        Assert.assertFalse(TableUtils.isValidTableName("t\u007Ft.csv"));
        Assert.assertFalse(TableUtils.isValidTableName("."));
        Assert.assertFalse(TableUtils.isValidTableName(".."));
        Assert.assertFalse(TableUtils.isValidTableName("..."));
        Assert.assertFalse(TableUtils.isValidTableName("..\\"));
        Assert.assertFalse(TableUtils.isValidTableName("\\.."));
        Assert.assertFalse(TableUtils.isValidTableName("/.."));
        Assert.assertFalse(TableUtils.isValidTableName("../"));

        Assert.assertTrue(TableUtils.isValidTableName("table name"));
        Assert.assertTrue(TableUtils.isValidTableName("table name.csv"));
        Assert.assertTrue(TableUtils.isValidTableName("table-name"));
        Assert.assertTrue(TableUtils.isValidTableName("table_name"));
        Assert.assertTrue(TableUtils.isValidTableName("table$name"));
    }
}