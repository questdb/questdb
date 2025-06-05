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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ShowTablesTest extends AbstractCairoTest {

    @Test
    public void testDropAndRecreateTable() throws Exception {
        // Tests that cached query plans of `tables() correctly handle table recreation
        //
        // Purpose: Verify that when a table is dropped and recreated, pre-existing
        // query plans using tables() function correctly show the new table ID.
        //
        // Key assumption: Table IDs must change when a table is dropped and recreated.
        //
        // Background: This is a regression test for an issue where the tables() function
        // returned stale table IDs from cached plans after DROP TABLE + CREATE TABLE operations.

        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp) timestamp(ts) partition by DAY");

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery compile = compiler.compile("tables()", sqlExecutionContext);

                // we use a single instance of RecordCursorFactory before and after table drop
                // this mimic behavior of a query cache.
                try (RecordCursorFactory recordCursorFactory = compile.getRecordCursorFactory()) {
                    try (RecordCursor cursor = recordCursorFactory.getCursor(sqlExecutionContext)) {
                        assertCursor(
                                "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\twalEnabled\tdirectoryName\tdedup\tttlValue\tttlUnit\tmatView\n" +
                                        "1\tx\tts\tDAY\t1000\t300000000\tfalse\tx~\tfalse\t0\tHOUR\tfalse\n",
                                false,
                                true,
                                true,
                                cursor,
                                recordCursorFactory.getMetadata(),
                                false
                        );
                    }

                    // recreate the same table again
                    execute("drop table x");
                    execute("create table x (ts timestamp) timestamp(ts) partition by DAY");
                    drainWalQueue();

                    try (RecordCursor cursor = recordCursorFactory.getCursor(sqlExecutionContext)) {
                        // note the ID is 2 now!
                        assertCursor(
                                "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\twalEnabled\tdirectoryName\tdedup\tttlValue\tttlUnit\tmatView\n" +
                                        "2\tx\tts\tDAY\t1000\t300000000\tfalse\tx~\tfalse\t0\tHOUR\tfalse\n",
                                false,
                                true,
                                true,
                                cursor,
                                recordCursorFactory.getMetadata(),
                                false
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testShowColumnsWithFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            assertQueryNoLeakCheck(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "cust_id\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                            "ccy\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\tfalse\n" +
                            "balance\tDOUBLE\tfalse\t0\tfalse\t0\tfalse\tfalse\n",
                    "select * from table_columns('balances')",
                    null,
                    null,
                    false
            );
        });
    }

    @Test
    public void testShowColumnsWithFunctionAndMissingTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            assertException(
                    "select * from table_columns('balances2')",
                    28,
                    "table does not exist"
            );
        });
    }

    @Test
    public void testShowColumnsWithMissingTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            assertException(
                    "show columns from balances2",
                    18,
                    "table does not exist"
            );
        });
    }

    @Test
    public void testShowColumnsWithSimpleTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            assertQueryNoLeakCheck(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "cust_id\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                            "ccy\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\tfalse\n" +
                            "balance\tDOUBLE\tfalse\t0\tfalse\t0\tfalse\tfalse\n",
                    "show columns from balances",
                    null,
                    null,
                    false
            );
        });
    }

    @Test
    public void testShowStandardConformingStrings() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "standard_conforming_strings\n" +
                        "on\n",
                "show standard_conforming_strings",
                null,
                null,
                false,
                true
        ));
    }

    @Test
    public void testShowTablesWithDrop() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            assertSql("table_name\nbalances\n", "show tables");
            execute("create table balances2(cust_id int, ccy symbol, balance double)");
            execute("drop table balances");
            assertSql("table_name\nbalances2\n", "show tables");
        });
    }

    @Test
    public void testShowTablesWithFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            assertSql("table_name\nbalances\n", "select * from all_tables()");
        });
    }

    @Test
    public void testShowTablesWithSingleTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            assertSql("table_name\nbalances\n", "show tables");
        });
    }

    @Test
    public void testShowTimeZone() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "TimeZone\nUTC\n",
                "show time zone",
                null,
                false,
                true
        ));
    }

    @Test
    public void testShowTimeZoneWrongSyntax() throws Exception {
        assertMemoryLeak(() -> assertException(
                "show time",
                9,
                "expected 'TABLES', 'COLUMNS FROM <tab>', 'PARTITIONS FROM <tab>', 'TRANSACTION ISOLATION LEVEL', 'transaction_isolation', 'max_identifier_length', 'standard_conforming_strings', 'parameters', 'server_version', 'server_version_num', 'search_path', 'datestyle', or 'time zone'"
        ));
    }

    @Test
    public void testSqlSyntax1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            assertException(
                    "show",
                    4,
                    "expected 'TABLES', 'COLUMNS FROM <tab>', 'PARTITIONS FROM <tab>', 'TRANSACTION ISOLATION LEVEL', 'transaction_isolation', 'max_identifier_length', 'standard_conforming_strings', 'parameters', 'server_version', 'server_version_num', 'search_path', 'datestyle', or 'time zone'"
            );
        });
    }

    @Test
    public void testSqlSyntax2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            assertException(
                    "show columns balances",
                    13,
                    "expected 'from'"
            );
        });
    }

    @Test
    public void testSqlSyntax3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            assertException(
                    "show columns from balances where",
                    27,
                    "unexpected token [where]"
            );
        });
    }

    @Test
    public void testTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances (ts timestamp, cust_id int, ccy symbol, balance double) timestamp(ts) partition by day wal");
            execute("create materialized view balances_1h as (select ts, max(balance) from balances sample by 1h) partition by week");
            assertSql(
                    "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\twalEnabled\tdirectoryName\tdedup\tttlValue\tttlUnit\tmatView\n" +
                            "1\tbalances\tts\tDAY\t1000\t300000000\ttrue\tbalances~1\tfalse\t0\tHOUR\tfalse\n" +
                            "2\tbalances_1h\tts\tWEEK\t1000\t-1\ttrue\tbalances_1h~2\tfalse\t0\tHOUR\ttrue\n",
                    "tables() order by table_name"
            );
        });
    }
}
