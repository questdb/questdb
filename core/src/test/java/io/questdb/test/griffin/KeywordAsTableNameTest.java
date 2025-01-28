/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.cairo.TableUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class KeywordAsTableNameTest extends AbstractCairoTest {
    @Test
    public void testAlterTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"table\" (a int)");
            assertExceptionNoLeakCheck("alter table table add column b float", 12, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"table\"");
            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "a\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n",
                    "table_columns('table')"
            );
            execute("alter table \"table\" add column b float");
            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "a\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                            "b\tFLOAT\tfalse\t256\tfalse\t0\tfalse\tfalse\n",
                    "table_columns('table')"
            );
        });
    }

    @Test
    public void testCreateTable() throws Exception {
        assertMemoryLeak(() -> {
            assertException("create table from (a int)", 13, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");
            execute("create table \"from\" (a int)");
            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "a\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n",
                    "table_columns('from')"
            );
        });
    }

    @Test
    public void testCreateTableColumn() throws Exception {
        assertMemoryLeak(() -> {
            assertException("create table a (from int)", 16, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");
            execute("create table a (\"from\" int)");
            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "from\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n",
                    "table_columns('a')"
            );
        });
    }

    @Test
    public void testDropTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"select\" (a int)");
            assertException("drop table select", 11, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"select\"");
            execute("drop table \"select\"");
            Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, engine.getTableStatus("select"));
        });
    }

    @Test
    public void testInsert() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"table\" (a int)");
            assertException("insert into table values(10)", 12, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"table\"");
            execute("insert into \"table\" values(10)");
            assertSql(
                    "a\n" +
                            "10\n",
                    "\"table\""
            );
        });
    }

    @Test
    public void testInsertColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"from\" (\"from\" int)");
            assertException("insert into \"from\" (from) values(50)", 20, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");
            execute("insert into \"from\" (\"from\") values(50)");
            assertSql(
                    "from\n" +
                            "50\n",
                    "select * from \"from\""
            );

            assertSql(
                    "from\n" +
                            "50\n",
                    "\"from\""
            );
            // alias cannot be unquoted keyword
            assertException("select a from \"from\" select", 21, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"select\"");
        });
    }

    @Test
    public void testRenameTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"from\" (a int)");
            assertException("rename table from to to", 13, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");
            assertException("rename table \"from\" to to", 23, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"to\"");
            execute("rename table \"from\" to \"to\"");
            assertSql(
                    "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\twalEnabled\tdirectoryName\tdedup\tttlValue\tttlUnit\n" +
                            "1\tto\t\tNONE\t1000\t300000000\tfalse\tto~\tfalse\t0\tHOUR\n", "tables()"
            );
        });
    }

    @Test
    public void testSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"from\" (a int)");
            execute("insert into \"from\" values(50)");
            assertException("select a from from", 14, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");
            assertSql(
                    "a\n" +
                            "50\n",
                    "select * from \"from\""
            );

            assertSql(
                    "a\n" +
                            "50\n",
                    "\"from\""
            );
            // alias cannot be unquoted keyword
            assertException("select a from \"from\" select", 21, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"select\"");
        });
    }

    @Test
    public void testSelectColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"from\" (\"from\" int)");
            execute("insert into \"from\" values(50)");
            assertException("select from from from", 7, "column expression expected");
            assertSql(
                    "from\n" +
                            "50\n",
                    "select * from \"from\""
            );

            // alias cannot be unquoted keyword
            assertException("select \"from\" select from \"from\"", 14, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"select\"");
            // alias via "as" cannot be unquoted
            assertException("select \"from\" as select from \"from\"", 17, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"select\"");
            // column name cannot be unquoted when referenced via .
            assertException("select a.from from \"from\" a", 9, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");


            // simple alias
            assertSql(
                    "select\n" +
                            "50\n",
                    "select \"from\" \"select\" from \"from\""
            );

            // alias via "as"
            assertSql(
                    "select\n" +
                            "50\n",
                    "select \"from\" as \"select\" from \"from\""
            );

            assertSql(
                    "from\n" +
                            "50\n",
                    "select a.\"from\" from \"from\" a"
            );
        });
    }

    @Test
    public void testSelectOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (\"from\" int)");
            execute("insert into t values(50)");
            assertException("select * from t order by from", 25, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");
            assertSql(
                    "from\n" +
                            "50\n",
                    "select * from t order by \"from\""

            );
        });
    }

    @Test
    public void testSelectWithAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"from\" (\"from\" int)");
            execute("insert into \"from\" values(50)");
            assertException("with select as (select * from \"from\") select * from \"select\"", 5, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"select\"");
            assertSql(
                    "from\n" +
                            "50\n",
                    "with \"select\" as (select * from \"from\") select * from \"select\""

            );
        });
    }

    @Test
    public void testUpdate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"table\" (a int)");
            execute("insert into \"table\" values(10)");
            assertException("update table set a = 20", 7, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"table\"");
            // alias cannot be keyword either
            assertException("update \"table\" table set a = 20", 15, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"table\"");
            update("update \"table\" set a = 20");
            assertSql(
                    "a\n" +
                            "20\n",
                    "\"table\""
            );

            assertException("update \"table\" \"from set a = 30", 15, "unclosed quotation mark");

            assertSql(
                    "a\n" +
                            "20\n",
                    "\"table\""
            );

            update("update \"table\" \"from\" set a = 30");
            assertSql(
                    "a\n" +
                            "30\n",
                    "\"table\""
            );
        });
    }

    @Test
    public void testVacuum() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"from\" (a int)");
            assertException("vacuum table from", 13, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");
            execute("vacuum table \"from\"");
        });
    }

    @Test
    public void testVacuumTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"table\" (a int)");
            assertException("vacuum table table", 13, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"table\"");
            execute("vacuum table \"table\"");
        });
    }

}
