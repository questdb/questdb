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

import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class CreateMatViewTest extends AbstractCairoTest {
    private static final String TABLE1 = "table1";
    private static final String TABLE2 = "table2";
    private static final String TABLE3 = "table3";

    @Test
    public void testCreateMatViewGroupByTimestampTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            ddl("create materialized view test as (select timestamp_floor('30s', ts) as ts, avg(v) from " + TABLE1 + " order by ts) partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
        });
    }

    @Test
    public void testCreateMatViewInvalidSampleByTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                ddl("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 3M) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Materialized view query with invalid sampling interval, 'M' and 'y' intervals are not supported");
            }

            try {
                ddl("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 1y) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Materialized view query with invalid sampling interval, 'M' and 'y' intervals are not supported");
            }
        });
    }

    @Test
    public void testCreateMatViewInvalidTimestampTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                ddl("create materialized view test as (select ts, k, avg(v) from " + TABLE1 + " sample by 30s) timestamp(k) partition by week");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "TIMESTAMP column expected [actual=SYMBOL]");
            }
        });
    }

    @Test
    public void testCreateMatViewKeyedSampleByTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            ddl("create materialized view test as (select ts, k, avg(v) from " + TABLE1 + " sample by 30s) partition by day");

            assertQuery("ts\tk\tavg\n", "test", "ts", true, true);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertFalse(metadata.isDedupKey(0));
                assertTrue(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
            }
        });
    }

    @Test
    public void testCreateMatViewMultipleTablesTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            createTable(TABLE3);

            try {
                ddl("create materialized view test as (select t1.ts, avg(t1.v) from " + TABLE1 + " as t1 " +
                        "join " + TABLE2 + " as t2 on v sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "More than one table used in query, base table has to be set using 'WITH BASE'");
            }

            try {
                ddl("create materialized view test as (select ts, avg(v) from " + TABLE3 + " sample by 30s " +
                        "union select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "More than one table used in query, base table has to be set using 'WITH BASE'");
            }

            try {
                ddl("create materialized view test as (select ts, avg(v) from " + TABLE3 + " sample by 30s " +
                        "union select t1.ts, avg(t1.v) from " + TABLE1 + " as t1 join " + TABLE2 + " as t2 on v sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "More than one table used in query, base table has to be set using 'WITH BASE'");
            }
        });
    }

    @Test
    public void testCreateMatViewNoPartitionByTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                ddl("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s)");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "'partition by' expected");
            }

            try {
                ddl("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by 3d");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "'HOUR', 'DAY', 'WEEK', 'MONTH' or 'YEAR' expected");
            }

            try {
                ddl("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by NONE");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Materialized view has to be partitioned");
            }
        });
    }

    @Test
    public void testCreateMatViewNoSampleByTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                ddl("create materialized view test as (select * from " + TABLE1 + " where v % 2 = 0) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Materialized view query requires a sampling interval");
            }
        });
    }

    @Test
    public void testCreateMatViewNonDeterministicSampleByTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                ddl("create materialized view test as (select ts, rnd_boolean(), avg(v) from " + TABLE1 + " sample by 30s) partition by month");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Non-deterministic column: rnd_boolean");
            }
        });
    }

    @Test
    public void testCreateMatViewNonOptimizedSampleByMultipleTimestampsTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                ddl("create materialized view test as (select ts, 1L::timestamp as ts2, avg(v) from (" +
                        "select ts, k, v+10 as v from " + TABLE1 + ") sample by 30s) partition by week");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Designated timestamp should be set explicitly");
            }

            ddl("create materialized view test as (select ts, 1L::timestamp as ts2, avg(v) from (" +
                    "select ts, k, v+10 as v from " + TABLE1 + ") sample by 30s) timestamp(ts) partition by week");

            assertQuery("ts\tts2\tavg\n", "test", "ts", true, true);
        });
    }

    @Test
    public void testCreateMatViewNonOptimizedSampleByTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            ddl("create materialized view test as (select ts, avg(v) from (select ts, k, v+10 as v from " + TABLE1 + ") sample by 30s) partition by week");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
        });
    }

    @Test
    public void testCreateMatViewNonWalBaseTableTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1, false);

            try {
                ddl("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "The base table has to be WAL enabled");
            }
        });
    }

    @Test
    public void testCreateMatViewRewrittenSampleByMultipleTimestampsTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            ddl("create materialized view test as (select ts, 1L::timestamp as ts2, avg(v) from " + TABLE1 + " sample by 30s) partition by day");

            assertQuery("ts\tts2\tavg\n", "test", "ts", true, true);
        });
    }

    @Test
    public void testCreateMatViewRewrittenSampleByTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            ddl("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
        });
    }

    @Test
    public void testCreateMatViewWithBaseTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            ddl("create materialized view test with base " + TABLE1
                    + " as (select t1.ts, avg(t1.v) from " + TABLE1 + " as t1 join " + TABLE2 + " as t2 on v sample by 60s)"
                    + " partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
        });
    }

    @Test
    public void testCreateMatViewWithExistingTableNameTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            try {
                ddl("create materialized view " + TABLE2 + " as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "A view or a table already exists with this name");
            }

            try {
                ddl("create materialized view if not exists " + TABLE2 + " as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "A table already exists with this name");
            }

            ddl("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");

            // without IF NOT EXISTS
            try {
                ddl("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "A view or a table already exists with this name");
            }

            // with IF NOT EXISTS
            ddl("create materialized view if not exists test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
        });
    }

    @Test
    public void testCreateMatViewWithIndexTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            ddl("create materialized view test as (select ts, k, avg(v) from " + TABLE1 + " sample by 30s), index (k) partition by day");

            assertQuery("ts\tk\tavg\n", "test", "ts", true, true);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertFalse(metadata.isDedupKey(0));
                assertTrue(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
            }
        });
    }

    @Test
    public void testCreateMatViewWithOperatorTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            ddl("create materialized view test as (select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s) partition by day");

            assertQuery("ts\tdoubleV\tavg\n", "test", "ts", true, true);
        });
    }

    private void createTable(String tableName) throws SqlException {
        createTable(tableName, true);
    }

    private void createTable(String tableName, boolean walEnabled) throws SqlException {
        ddl("create table " + tableName + " (ts timestamp, k symbol, v long) timestamp(ts) partition by day" + (walEnabled ? "" : " bypass") + " wal");
        for (int i = 0; i < 9; i++) {
            insert("insert into " + tableName + " values (" + (i * 10000000) + ", 'k" + i + "', " + i + ")");
        }
    }
}
