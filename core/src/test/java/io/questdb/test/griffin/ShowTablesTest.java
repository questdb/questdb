/*******************************************************************************
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
                                """
                                        id	table_name	designatedTimestamp	partitionBy	walEnabled	dedup	ttlValue	ttlUnit	matView	directoryName	maxUncommittedRows	o3MaxLag	table_suspended	table_type	table_row_count	table_min_timestamp	table_max_timestamp	table_last_write_timestamp	table_txn	table_memory_pressure_level	table_write_amp_count	table_write_amp_p50	table_write_amp_p90	table_write_amp_p99	table_write_amp_max	table_merge_rate_count	table_merge_rate_p50	table_merge_rate_p90	table_merge_rate_p99	table_merge_rate_max	wal_pending_row_count	wal_dedup_row_count_since_start	wal_txn	wal_max_timestamp	wal_tx_count	wal_tx_size_p50	wal_tx_size_p90	wal_tx_size_p99	wal_tx_size_max	replica_batch_count	replica_batch_size_p50	replica_batch_size_p90	replica_batch_size_p99	replica_batch_size_max	replica_more_pending
                                        1	x	ts	DAY	false	false	0	HOUR	false	x~	1000	300000000	false	T	null				null	null	0	0.0	0.0	0.0	0.0	0	0	0	0	0	0	0	null		0	0	0	0	0	0	0	0	0	0	false
                                        """,
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
                                """
                                        id	table_name	designatedTimestamp	partitionBy	walEnabled	dedup	ttlValue	ttlUnit	matView	directoryName	maxUncommittedRows	o3MaxLag	table_suspended	table_type	table_row_count	table_min_timestamp	table_max_timestamp	table_last_write_timestamp	table_txn	table_memory_pressure_level	table_write_amp_count	table_write_amp_p50	table_write_amp_p90	table_write_amp_p99	table_write_amp_max	table_merge_rate_count	table_merge_rate_p50	table_merge_rate_p90	table_merge_rate_p99	table_merge_rate_max	wal_pending_row_count	wal_dedup_row_count_since_start	wal_txn	wal_max_timestamp	wal_tx_count	wal_tx_size_p50	wal_tx_size_p90	wal_tx_size_p99	wal_tx_size_max	replica_batch_count	replica_batch_size_p50	replica_batch_size_p90	replica_batch_size_p99	replica_batch_size_max	replica_more_pending
                                        2	x	ts	DAY	false	false	0	HOUR	false	x~	1000	300000000	false	T	null				null	null	0	0.0	0.0	0.0	0.0	0	0	0	0	0	0	0	null		0	0	0	0	0	0	0	0	0	0	false
                                        """,
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
            execute("create table balances (cust_id int, ccy symbol, balance double)");
            execute("insert into balances values (0, null, 0)");
            execute("insert into balances values (1, 'a', 1)");
            execute("insert into balances values (2, 'b', 2)");
            execute("insert into balances values (3, 'c', 3)");
            assertQueryNoLeakCheck(
                    """
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey
                            cust_id\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse
                            ccy\tSYMBOL\tfalse\t256\ttrue\t128\t4\tfalse\tfalse
                            balance\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse
                            """,
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
            execute("create table balances (cust_id int, ccx symbol, ccy symbol, balance double)");
            execute("insert into balances values (1, 'foo', 'bar', 1)");
            execute("insert into balances values (2, 'foo', null, 2)");
            assertQueryNoLeakCheck(
                    """
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey
                            cust_id\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse
                            ccx\tSYMBOL\tfalse\t256\ttrue\t128\t1\tfalse\tfalse
                            ccy\tSYMBOL\tfalse\t256\ttrue\t128\t2\tfalse\tfalse
                            balance\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse
                            """,
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
                """
                        standard_conforming_strings
                        on
                        """,
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
            execute("create view balances_view as (select ts, max(balance) from balances sample by 1h)");
            drainWalAndViewQueues();
            assertSql(
                    """
                            id	table_name	designatedTimestamp	partitionBy	walEnabled	dedup	ttlValue	ttlUnit	matView	directoryName	maxUncommittedRows	o3MaxLag	table_suspended	table_type	table_row_count	table_min_timestamp	table_max_timestamp	table_last_write_timestamp	table_txn	table_memory_pressure_level	table_write_amp_count	table_write_amp_p50	table_write_amp_p90	table_write_amp_p99	table_write_amp_max	table_merge_rate_count	table_merge_rate_p50	table_merge_rate_p90	table_merge_rate_p99	table_merge_rate_max	wal_pending_row_count	wal_dedup_row_count_since_start	wal_txn	wal_max_timestamp	wal_tx_count	wal_tx_size_p50	wal_tx_size_p90	wal_tx_size_p99	wal_tx_size_max	replica_batch_count	replica_batch_size_p50	replica_batch_size_p90	replica_batch_size_p99	replica_batch_size_max	replica_more_pending
                            1	balances	ts	DAY	true	false	0	HOUR	false	balances~1	1000	300000000	false	T	null				null	0	0	0.0	0.0	0.0	0.0	0	0	0	0	0	0	0	null		0	0	0	0	0	0	0	0	0	0	false
                            2	balances_1h	ts	WEEK	true	false	0	HOUR	true	balances_1h~2	1000	-1	false	M	null				null	0	0	0.0	0.0	0.0	0.0	0	0	0	0	0	0	0	null		0	0	0	0	0	0	0	0	0	0	false
                            3	balances_view	ts	N/A	true	false	0	HOUR	false	balances_view~3	0	0	false	V	null				null	0	0	0.0	0.0	0.0	0.0	0	0	0	0	0	0	0	null		0	0	0	0	0	0	0	0	0	0	false
                            """,
                    "tables() order by table_name"
            );
        });
    }
}
