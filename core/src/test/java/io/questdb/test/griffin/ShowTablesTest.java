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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ShowTablesTest extends AbstractGriffinTest {

    @Test
    public void testShowColumnsWithFunction() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            assertQuery8(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "cust_id\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                            "ccy\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\tfalse\n" +
                            "balance\tDOUBLE\tfalse\t0\tfalse\t0\tfalse\tfalse\n",
                    "select * from table_columns('balances')"
            );
        });
    }

    @Test
    public void testShowColumnsWithMissingTable() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                assertQuery8("columnName\tcolumnType\ncust_id\tINT\nccy\tSYMBOL\nbalance\tDOUBLE\n", "show columns from balances2");
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(ex.toString().contains("table does not exist"));
            }
        });
    }

    @Test
    public void testShowColumnsWithSimpleTable() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            assertQuery8(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "cust_id\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                            "ccy\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\tfalse\n" +
                            "balance\tDOUBLE\tfalse\t0\tfalse\t0\tfalse\tfalse\n",
                    "show columns from balances");
        });
    }

    @Test
    public void testShowStandardConformingStrings() throws Exception {
        assertMemoryLeak(() -> assertQuery("standard_conforming_strings\n" +
                "on\n", "show standard_conforming_strings", null, null, false, true));
    }

    @Test
    public void testShowTablesWithDrop() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            assertQuery8("table\nbalances\n", "show tables");
            compile("create table balances2(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            compile("drop table balances", sqlExecutionContext);
            assertQuery8("table\nbalances2\n", "show tables");
        });
    }

    @Test
    public void testShowTablesWithFunction() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            assertQuery8("table\nbalances\n", "select * from all_tables()");
        });
    }

    @Test
    public void testShowTablesWithSingleTable() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            assertQuery8("table\nbalances\n", "show tables");
        });
    }

    @Test
    public void testShowTimeZone() throws Exception {
        assertMemoryLeak(() -> assertQuery12(
                "TimeZone\nUTC\n",
                "show time zone", null, false, sqlExecutionContext, true));
    }

    @Test
    public void testShowTimeZoneWrongSyntax() throws Exception {
        assertMemoryLeak(() -> assertFailure("show time", null, 5,
                "expected 'TABLES', 'COLUMNS FROM <tab>', 'PARTITIONS FROM <tab>', " +
                        "'TRANSACTION ISOLATION LEVEL', 'transaction_isolation', " +
                        "'max_identifier_length', 'standard_conforming_strings', " +
                        "'search_path', 'datestyle', or 'time zone'"));
    }

    @Test
    public void testSqlSyntax1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                assertQuery8("columnName\tcolumnType\ncust_id\tINT\nccy\tSYMBOL\nbalance\tDOUBLE\n", "show");
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(
                        ex.getFlyweightMessage(),
                        "expected 'TABLES', 'COLUMNS FROM <tab>', 'PARTITIONS FROM <tab>', " +
                                "'TRANSACTION ISOLATION LEVEL', 'transaction_isolation', " +
                                "'max_identifier_length', 'standard_conforming_strings', " +
                                "'search_path', 'datestyle', or 'time zone'"
                );
            }
        });
    }

    @Test
    public void testSqlSyntax2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                assertQuery8("columnName\tcolumnType\ncust_id\tINT\nccy\tSYMBOL\nbalance\tDOUBLE\n", "show columns balances");
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(ex.toString().contains("expected 'from'"));
            }
        });
    }

    @Test
    public void testSqlSyntax3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                assertQuery8("columnName\tcolumnType\ncust_id\tINT\nccy\tSYMBOL\nbalance\tDOUBLE\n", "show columns from balances where");
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "unexpected token [where]");
            }
        });
    }
}
