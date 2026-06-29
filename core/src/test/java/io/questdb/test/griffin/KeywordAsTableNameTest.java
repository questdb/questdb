/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
            assertQuery("alter table table add column b float")
                    .noLeakCheck()
                    .fails(12, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"table\"");
            assertQuery("table_columns('table')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude\tindexReplicaOnly
                            a\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\tfalse
                            """);
            execute("alter table \"table\" add column b float");
            assertQuery("table_columns('table')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude\tindexReplicaOnly
                            a\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\tfalse
                            b\tFLOAT\tfalse\t256\tfalse\t0\t0\tfalse\tfalse\t\t\tfalse
                            """);
        });
    }

    @Test
    public void testCreateTable() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("create table from (a int)")
                    .fails(13, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");
            execute("create table \"from\" (a int)");
            assertQuery("table_columns('from')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude\tindexReplicaOnly
                            a\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\tfalse
                            """);
        });
    }

    @Test
    public void testCreateTableColumn() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("create table a (from int)")
                    .fails(16, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");
            execute("create table a (\"from\" int)");
            assertQuery("table_columns('a')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude\tindexReplicaOnly
                            from\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\tfalse
                            """);
        });
    }

    @Test
    public void testDropTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"select\" (a int)");
            assertQuery("drop table select")
                    .fails(11, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"select\"");
            execute("drop table \"select\"");
            Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, engine.getTableStatus("select"));
        });
    }

    @Test
    public void testInsert() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"table\" (a int)");
            assertQuery("insert into table values(10)")
                    .fails(12, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"table\"");
            execute("insert into \"table\" values(10)");
            assertQuery("\"table\"")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a
                            10
                            """);
        });
    }

    @Test
    public void testInsertColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"from\" (\"from\" int)");
            assertQuery("insert into \"from\" (from) values(50)")
                    .fails(20, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");
            execute("insert into \"from\" (\"from\") values(50)");
            assertQuery("select * from \"from\"")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            from
                            50
                            """);

            assertQuery("\"from\"")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            from
                            50
                            """);
            // alias cannot be unquoted keyword
            assertQuery("select a from \"from\" select")
                    .fails(21, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"select\"");
        });
    }

    @Test
    public void testRenameTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"from\" (a int)");
            assertQuery("rename table from to to")
                    .fails(13, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");
            assertQuery("rename table \"from\" to to")
                    .fails(23, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"to\"");
            execute("rename table \"from\" to \"to\"");
            assertQuery("select id, table_name, designatedTimestamp, partitionBy, maxUncommittedRows, o3MaxLag, walEnabled, directoryName, dedup, ttlValue, ttlUnit, matView from tables()")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\twalEnabled\tdirectoryName\tdedup\tttlValue\tttlUnit\tmatView
                            1\tto\t\tNONE\t1000\t300000000\tfalse\tto~\tfalse\t0\tHOUR\tfalse
                            """);
        });
    }

    @Test
    public void testSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"from\" (a int)");
            execute("insert into \"from\" values(50)");
            assertQuery("select a from from")
                    .fails(14, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");
            assertQuery("select * from \"from\"")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a
                            50
                            """);

            assertQuery("\"from\"")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a
                            50
                            """);
            // alias cannot be unquoted keyword
            assertQuery("select a from \"from\" select")
                    .fails(21, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"select\"");
        });
    }

    @Test
    public void testSelectColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"from\" (\"from\" int)");
            execute("insert into \"from\" values(50)");
            assertQuery("select from from from")
                    .fails(7, "column expression expected");
            assertQuery("select * from \"from\"")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            from
                            50
                            """);

            // alias cannot be unquoted keyword
            assertQuery("select \"from\" select from \"from\"")
                    .fails(14, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"select\"");
            // alias via "as" cannot be unquoted
            assertQuery("select \"from\" as select from \"from\"")
                    .fails(17, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"select\"");
            // column name cannot be unquoted when referenced via .
            assertQuery("select a.from from \"from\" a")
                    .fails(9, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");


            // simple alias
            assertQuery("select \"from\" \"select\" from \"from\"")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            select
                            50
                            """);

            // alias via "as"
            assertQuery("select \"from\" as \"select\" from \"from\"")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            select
                            50
                            """);

            assertQuery("select a.\"from\" from \"from\" a")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            from
                            50
                            """);
        });
    }

    @Test
    public void testSelectOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (\"from\" int)");
            execute("insert into t values(50)");
            assertQuery("select * from t order by from")
                    .fails(25, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");
            assertQuery("select * from t order by \"from\"")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            from
                            50
                            """);
        });
    }

    @Test
    public void testSelectWithAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"from\" (\"from\" int)");
            execute("insert into \"from\" values(50)");
            assertQuery("with select as (select * from \"from\") select * from \"select\"")
                    .fails(5, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"select\"");
            assertQuery("with \"select\" as (select * from \"from\") select * from \"select\"")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            from
                            50
                            """);
        });
    }

    @Test
    public void testUpdate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"table\" (a int)");
            execute("insert into \"table\" values(10)");
            assertQuery("update table set a = 20")
                    .fails(7, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"table\"");
            // alias cannot be keyword either
            assertQuery("update \"table\" table set a = 20")
                    .fails(15, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"table\"");
            update("update \"table\" set a = 20");
            assertQuery("\"table\"")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a
                            20
                            """);

            assertQuery("update \"table\" \"from set a = 30")
                    .fails(15, "unclosed quotation mark");

            assertQuery("\"table\"")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a
                            20
                            """);

            update("update \"table\" \"from\" set a = 30");
            assertQuery("\"table\"")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a
                            30
                            """);
        });
    }

    @Test
    public void testVacuum() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"from\" (a int)");
            assertQuery("vacuum table from")
                    .fails(13, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"from\"");
            execute("vacuum table \"from\"");
        });
    }

    @Test
    public void testVacuumTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"table\" (a int)");
            assertQuery("vacuum table table")
                    .fails(13, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"table\"");
            execute("vacuum table \"table\"");
        });
    }

}
