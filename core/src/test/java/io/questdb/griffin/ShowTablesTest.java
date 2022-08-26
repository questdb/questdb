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

package io.questdb.griffin;

import org.junit.Assert;
import org.junit.Test;

public class ShowTablesTest extends AbstractGriffinTest {

    @Test
    public void testShowTablesWithSingleTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            assertQuery("table\nbalances\n", "show tables", null, false, sqlExecutionContext, false);
        });
    }

    @Test
    public void testShowStandardConformingStrings() throws Exception {
        assertMemoryLeak(() -> assertQuery("standard_conforming_strings\n" +
                "on\n", "show standard_conforming_strings", null, null, false, false, true));
    }

    @Test
    public void testShowTablesWithDrop() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            assertQuery("table\nbalances\n", "show tables", null, false, sqlExecutionContext, false);
            compiler.compile("create table balances2(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            compiler.compile("drop table balances", sqlExecutionContext);
            assertQuery("table\nbalances2\n", "show tables", null, false, sqlExecutionContext, false);
        });
    }

    @Test
    public void testShowColumnsWithSimpleTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            assertQuery(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\n" +
                            "cust_id\tINT\tfalse\t0\tfalse\t0\tfalse\n" +
                            "ccy\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\n" +
                            "balance\tDOUBLE\tfalse\t0\tfalse\t0\tfalse\n",
                    "show columns from balances", null, false, sqlExecutionContext, false);
        });
    }

    @Test
    public void testShowColumnsWithMissingTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                assertQuery("columnName\tcolumnType\ncust_id\tINT\nccy\tSYMBOL\nbalance\tDOUBLE\n", "show columns from balances2", null, false, sqlExecutionContext, false);
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(ex.toString().contains("table does not exist"));
            }
        });
    }

    @Test
    public void testSqlSyntax1() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                assertQuery("columnName\tcolumnType\ncust_id\tINT\nccy\tSYMBOL\nbalance\tDOUBLE\n", "show", null, false, sqlExecutionContext, false);
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(ex.toString().contains("expected 'tables', 'columns' or 'time zone'"));
            }
        });
    }

    @Test
    public void testSqlSyntax2() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                assertQuery("columnName\tcolumnType\ncust_id\tINT\nccy\tSYMBOL\nbalance\tDOUBLE\n", "show columns balances", null, false, sqlExecutionContext, false);
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(ex.toString().contains("expected 'from'"));
            }
        });
    }

    @Test
    public void testShowTablesWithFunction() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            assertQuery("table\nbalances\n", "select * from all_tables()", null, false, sqlExecutionContext, false);
        });
    }

    @Test
    public void testShowColumnsWithFunction() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            assertQuery(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\n" +
                            "cust_id\tINT\tfalse\t0\tfalse\t0\tfalse\n" +
                            "ccy\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\n" +
                            "balance\tDOUBLE\tfalse\t0\tfalse\t0\tfalse\n",
                    "select * from table_columns('balances')", null, false, sqlExecutionContext, false);
        });
    }

    @Test
    public void testShowTimeZone() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "TimeZone\nUTC\n",
                "show time zone", null, false, sqlExecutionContext, false, true));
    }

    @Test
    public void testShowTimeZoneWrongSyntax() throws Exception {
        assertMemoryLeak(() -> assertFailure("show time", null, 5, "expected 'tables', 'columns' or 'time zone'"));
    }
}
