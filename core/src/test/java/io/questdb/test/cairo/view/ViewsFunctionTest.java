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

package io.questdb.test.cairo.view;

import org.junit.Test;

public class ViewsFunctionTest extends AbstractViewTest {

    @Test
    public void testShowColumnsView() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            final String query = "select ts, k, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
            createView("test", query, TABLE1);
            assertQueryNoLeakCheck(
                    """
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey
                            ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse
                            k\tSYMBOL\tfalse\t0\tfalse\t0\t0\tfalse\tfalse
                            doubleV\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse
                            avg\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse
                            """,
                    "show columns from test",
                    null,
                    false
            );
        });
    }

    @Test
    public void testShowCreateView() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
            execute("create view test as (" + query + ")");
            assertQueryNoLeakCheck(
                    """
                            ddl
                            CREATE VIEW 'test' AS (\s
                            select ts, v+v doubleV, avg(v) from table1 sample by 30s
                            );
                            """,
                    "show create view test",
                    null,
                    false
            );
        });
    }

    @Test
    public void testShowCreateViewFail() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
            execute("create view test as (" + query + ")");

            assertExceptionNoLeakCheck(
                    "show create test",
                    12,
                    "expected 'TABLE' or 'VIEW' or 'MATERIALIZED VIEW'"
            );
        });
    }

    @Test
    public void testShowCreateViewFail2() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            assertExceptionNoLeakCheck(
                    "show create view " + TABLE1,
                    17,
                    "view name expected, got table name"
            );
        });
    }

    @Test
    public void testShowCreateViewFail3() throws Exception {
        assertException(
                "show create view 'test';",
                17,
                "view does not exist [view=test]"
        );
    }

    @Test
    public void testViewsConsistentWithMatViewsAndTablesCommands() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(1750345200000000L);

            createTable(TABLE1);
            createTable(TABLE2);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            createView(VIEW1, query1, TABLE1);

            final String query2 = VIEW1 + " where v_max > 6";
            createView(VIEW2, query2, TABLE1, VIEW1);

            final String query3 = "select ts, k, max(v) as v_max from " + TABLE1 + " sample by 1m";
            createMatView(VIEW3, query3);

            final String query4 = "select ts, avg(v) as v_avg from " + TABLE2 + " sample by 15m";
            createMatView(VIEW4, query4);

            assertQueryAndPlan(
                    """
                            view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time
                            view1\tselect ts, k, max(v) as v_max from table1 where v > 4\tview1~3\t\tvalid\t2025-06-19T15:00:00.000000Z
                            view2\tview1 where v_max > 6\tview2~4\t\tvalid\t2025-06-19T15:00:00.000000Z
                            """,
                    "views() order by 1",
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            Sort
                              keys: [view_name]
                                views()
                            """
            );

            assertQueryAndPlan(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_period_hi\trefresh_base_table_txn\tbase_table_txn\trefresh_limit\trefresh_limit_unit\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit\tperiod_length\tperiod_length_unit\tperiod_delay\tperiod_delay_unit
                            view3\timmediate\ttable1\t2025-06-19T15:00:00.000000Z\t2025-06-19T15:00:00.000000Z\tselect ts, k, max(v) as v_max from table1 sample by 1m\tview3~5\t\tvalid\t\t9\t9\t0\t\t\t\t0\t\t0\t\t0\t
                            view4\timmediate\ttable2\t2025-06-19T15:00:00.000000Z\t2025-06-19T15:00:00.000000Z\tselect ts, avg(v) as v_avg from table2 sample by 15m\tview4~6\t\tvalid\t\t9\t9\t0\t\t\t\t0\t\t0\t\t0\t
                            """,
                    "materialized_views() order by 1",
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            Sort
                              keys: [view_name]
                                materialized_views()
                            """
            );

            assertQueryAndPlan(
                    """
                            id	table_name	designatedTimestamp	partitionBy	walEnabled	dedup	ttlValue	ttlUnit	table_suspended	table_type
                            1	table1	ts	DAY	true	false	0	HOUR	false	T
                            2	table2	ts	DAY	true	false	0	HOUR	false	T
                            3	view1		N/A	true	false	0	HOUR	false	V
                            4	view2		N/A	true	false	0	HOUR	false	V
                            5	view3	ts	DAY	true	false	0	HOUR	false	M
                            6	view4	ts	DAY	true	false	0	HOUR	false	M
                            """,
                    "select id,table_name,designatedTimestamp,partitionBy,walEnabled,dedup,ttlValue,ttlUnit,table_suspended,table_type from tables() order by 1",
                    null,
                    true,
                    true,
                    """
                            QUERY PLAN
                            Sort
                              keys: [id]
                                SelectedRecord
                                    tables()
                            """
            );

            assertQueryAndPlan(
                    """
                            name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure
                            table1\tfalse\t9\t0\t9\t\t\t0
                            table2\tfalse\t9\t0\t9\t\t\t0
                            view1\tfalse\t0\t0\t0\t\t\t0
                            view2\tfalse\t0\t0\t0\t\t\t0
                            view3\tfalse\t1\t0\t1\t\t\t0
                            view4\tfalse\t1\t0\t1\t\t\t0
                            """,
                    "wal_tables() order by 1",
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            Sort
                              keys: [name]
                                wal_tables()
                            """
            );

            assertQueryAndPlan(
                    """
                            table_name
                            table1
                            table2
                            view1
                            view2
                            view3
                            view4
                            """,
                    "all_tables() order by 1",
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            Sort
                              keys: [table_name]
                                all_tables()
                            """
            );
        });
    }

    @Test
    public void testViewsStatement() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(1750345200000000L);

            createTable(TABLE1);
            createTable(TABLE2);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n",
                    "views() order by 1",
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            Sort
                              keys: [view_name]
                                views()
                            """
            );

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            createView(VIEW1, query1, TABLE1);

            final String query2 = VIEW1 + " where v_max > 6";
            createView(VIEW2, query2, TABLE1, VIEW1);

            final String query3 = VIEW2 + " where v_max > 7";
            createView(VIEW3, query3, TABLE1, VIEW1, VIEW2);

            final String query4 = "select date_trunc('hour', ts), avg(v) as v_avg from " + TABLE2;
            createView(VIEW4, query4, TABLE2);

            assertQueryAndPlan(
                    """
                            view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time
                            view1\tselect ts, k, max(v) as v_max from table1 where v > 4\tview1~3\t\tvalid\t2025-06-19T15:00:00.000000Z
                            view2\tview1 where v_max > 6\tview2~4\t\tvalid\t2025-06-19T15:00:00.000000Z
                            view3\tview2 where v_max > 7\tview3~5\t\tvalid\t2025-06-19T15:00:00.000000Z
                            view4\tselect date_trunc('hour', ts), avg(v) as v_avg from table2\tview4~6\t\tvalid\t2025-06-19T15:00:00.000000Z
                            """,
                    "views() order by 1",
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            Sort
                              keys: [view_name]
                                views()
                            """
            );

            execute("DROP VIEW " + VIEW2);
            execute("DROP VIEW " + VIEW4);

            assertQueryAndPlan(
                    """
                            view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time
                            view1\tselect ts, k, max(v) as v_max from table1 where v > 4\tview1~3\t\tvalid\t2025-06-19T15:00:00.000000Z
                            view3\tview2 where v_max > 7\tview3~5\t\tvalid\t2025-06-19T15:00:00.000000Z
                            """,
                    "views() order by 1",
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            Sort
                              keys: [view_name]
                                views()
                            """
            );

            execute("DROP VIEW " + VIEW1);
            execute("DROP VIEW " + VIEW3);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n",
                    "views() order by 1",
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            Sort
                              keys: [view_name]
                                views()
                            """
            );
        });
    }
}
