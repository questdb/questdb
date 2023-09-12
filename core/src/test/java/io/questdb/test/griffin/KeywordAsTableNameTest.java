/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
            ddl("create table \"table\" (a int)");
            assertException("alter table table add column b float", 12, "table name is a keyword, use double quotes, such as \"table\"");
            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "a\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n",
                    "table_columns('table')"
            );
            ddl("alter table \"table\" add column b float");
            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "a\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                            "b\tFLOAT\tfalse\t256\tfalse\t0\tfalse\tfalse\n",
                    "table_columns('table')"
            );
        });
    }

    // vacuum
    // select

    @Test
    public void testCreateTable() throws Exception {
        assertMemoryLeak(() -> {
            assertException("create table from (a int)", 13, "table name is a keyword, use double quotes, such as \"from\"");
            ddl("create table \"from\" (a int)");
            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "a\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n",
                    "table_columns('from')"
            );
        });
    }

    @Test
    public void testVacuum() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table \"from\" (a int)");
            assertException("vacuum table from", 13, "table name is a keyword, use double quotes, such as \"from\"");
            ddl("vacuum table \"from\"");
        });
    }

    @Test
    public void testDropTable() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table \"select\" (a int)");
            assertException("drop table select", 11, "table name is a keyword, use double quotes, such as \"select\"");
            drop("drop table \"select\"");
            Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, engine.getTableStatus("select"));
        });
    }

    @Test
    public void testInsert() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table \"table\" (a int)");
            assertException("insert into table values(10)", 12, "table name is a keyword, use double quotes, such as \"table\"");
            insert("insert into \"table\" values(10)");
            assertSql(
                    "a\n" +
                            "10\n",
                    "table"
            );
        });
    }

    @Test
    public void testRenameTable() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table \"from\" (a int)");
            assertException("rename table from to to", 13, "table name is a keyword, use double quotes, such as \"from\"");
            assertException("rename table \"from\" to to", 23, "table name is a keyword, use double quotes, such as \"to\"");
            ddl("rename table \"from\" to \"to\"");
            assertSql(
                    "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\twalEnabled\tdirectoryName\tdedup\n" +
                            "1\tto\t\tNONE\t1000\t300000000\tfalse\tto~\tfalse\n", "tables()"
            );
        });
    }

    @Test
    public void testSelect() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table \"from\" (a int)");
            insert("insert into \"from\" values(50)");
            assertException("select a from from", 14, "table name is a keyword, use double quotes, such as \"from\"");
            assertException("from", 0, "table name is a keyword, use double quotes, such as \"from\"");
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
            assertException("select a from \"from\" select", 21, "table name is a keyword, use double quotes, such as \"select\"");
        });
    }

    @Test
    public void testUpdate() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table \"table\" (a int)");
            insert("insert into \"table\" values(10)");
            assertException("update table set a = 20", 7, "table name is a keyword, use double quotes, such as \"table\"");
            // alias cannot be keyword either
            assertException("update \"table\" table set a = 20", 15, "table name is a keyword, use double quotes, such as \"table\"");
            update("update \"table\" set a = 20");
            assertSql(
                    "a\n" +
                            "20\n",
                    "table"
            );

            assertException("update \"table\" \"from set a = 30", 15, "unclosed quotation mark");

            assertSql(
                    "a\n" +
                            "20\n",
                    "table"
            );

            update("update \"table\" \"from\" set a = 30");
            assertSql(
                    "a\n" +
                            "30\n",
                    "table"
            );
        });
    }

}
