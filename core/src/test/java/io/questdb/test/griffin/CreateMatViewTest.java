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

import io.questdb.cairo.mv.MaterializedViewDefinition;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.std.Os;
import io.questdb.std.str.Sinkable;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import static org.junit.Assert.*;

public class CreateMatViewTest extends AbstractCairoTest {
    private static final String TABLE1 = "table1";
    private static final String TABLE2 = "table2";
    private static final String TABLE3 = "table3";

    @Before
    public void setUp() {
        super.setUp();
        engine.getMaterializedViewGraph().clear();
    }

    @Test
    public void testCreateMatViewGroupByTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select timestamp_floor('1m', ts) as ts, avg(v) from " + TABLE1 + " order by ts";
            ddl("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMaterializedViewDefinition("test", query, TABLE1, 1, 'm');
        });
    }

    @Test
    public void testCreateMatViewInvalidTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                final String query = "select ts, k, avg(v) from " + TABLE1 + " sample by 30s";
                ddl("create materialized view test as (" + query + ") timestamp(k) partition by week");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "TIMESTAMP column expected [actual=SYMBOL]");
            }
            assertNull(engine.getMaterializedViewGraph().getView("test"));
        });
    }

    @Test
    public void testCreateMatViewKeyedSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, k, avg(v) from " + TABLE1 + " sample by 30s";
            ddl("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tk\tavg\n", "test", "ts", true, true);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertFalse(metadata.isDedupKey(0));
                assertTrue(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
            }
            assertMaterializedViewDefinition("test", query, TABLE1, 30, 's');
        });
    }

    @Test
    public void testCreateMatViewModelToSink() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, k, avg(v) from " + TABLE1 + " sample by 30s";
            final String sql = "create materialized view test as (" + query + "), index (k capacity 1024) partition by day" +
                    (Os.isWindows() ? "" : " in volume vol1");

            sink.clear();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final ExecutionModel model = compiler.testCompileModel(sql, sqlExecutionContext);
                assertEquals(model.getModelType(), ExecutionModel.CREATE_MAT_VIEW);
                ((Sinkable) model).toSink(sink);
                TestUtils.assertEquals("create materialized view test with base table1 as (" + query +
                        "), index(k capacity 1024) timestamp(ts) partition by DAY" +
                        (Os.isWindows() ? "" : " in volume 'vol1'"), sink);
            }
        });
    }

    @Test
    public void testCreateMatViewMultipleTables() throws Exception {
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
            assertNull(engine.getMaterializedViewGraph().getView("test"));
        });
    }

    @Test
    public void testCreateMatViewNoPartitionBy() throws Exception {
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
            assertNull(engine.getMaterializedViewGraph().getView("test"));
        });
    }

    @Test
    public void testCreateMatViewNoSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                ddl("create materialized view test as (select * from " + TABLE1 + " where v % 2 = 0) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Materialized view query requires a sampling interval");
            }
            assertNull(engine.getMaterializedViewGraph().getView("test"));
        });
    }

    @Test
    public void testCreateMatViewNonDeterministicFunction() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                ddl("create materialized view test as (select ts, rnd_boolean(), avg(v) from " + TABLE1 + " sample by 30s) partition by month");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Non-deterministic column: rnd_boolean");
            }
            assertNull(engine.getMaterializedViewGraph().getView("test"));
        });
    }

    @Test
    public void testCreateMatViewNonOptimizedSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, avg(v) from (select ts, k, v+10 as v from " + TABLE1 + ") sample by 30s";
            ddl("create materialized view test as (" + query + ") partition by week");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMaterializedViewDefinition("test", query, TABLE1, 30, 's');
        });
    }

    @Test
    public void testCreateMatViewNonOptimizedSampleByMultipleTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, 1L::timestamp as ts2, avg(v) from (select ts, k, v+10 as v from " + TABLE1 + ") sample by 30s";
            try {
                ddl("create materialized view test as (" + query + ") partition by week");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Designated timestamp should be set explicitly");
            }
            assertNull(engine.getMaterializedViewGraph().getView("test"));

            ddl("create materialized view test as (" + query + ") timestamp(ts) partition by week");

            assertQuery("ts\tts2\tavg\n", "test", "ts", true, true);
            assertMaterializedViewDefinition("test", query, TABLE1, 30, 's');
        });
    }

    @Test
    public void testCreateMatViewNonWalBaseTable() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1, false);

            try {
                ddl("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "The base table has to be WAL enabled");
            }
            assertNull(engine.getMaterializedViewGraph().getView("test"));
        });
    }

    @Test
    public void testCreateMatViewRewrittenSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, avg(v) from " + TABLE1 + " sample by 30s";
            ddl("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMaterializedViewDefinition("test", query, TABLE1, 30, 's');
        });
    }

    @Test
    public void testCreateMatViewRewrittenSampleByMultipleTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE3);

            final String query = "select ts, 1L::timestamp as ts2, avg(v) from " + TABLE3 + " sample by 30s";
            ddl("create materialized view test_view as (" + query + ") partition by day");

            assertQuery("ts\tts2\tavg\n", "test_view", "ts", true, true);
            assertMaterializedViewDefinition("test_view", query, TABLE3, 30, 's');
        });
    }

    @Test
    public void testCreateMatViewSampleByFromTo() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String from = "2024-03-01";
            final String to = "2024-06-30";
            final String query = "select ts, avg(v) from " + TABLE1 + " where ts in '2024' sample by 1d from '" + from + "' to '" + to + "'";
            ddl("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMaterializedViewDefinition("test", query, TABLE1, 1, 'd', parseAsMicros(from), parseAsMicros(to), null, null);
        });
    }

    @Test
    public void testCreateMatViewSampleByTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String tz = "Europe/Berlin";
            final String query = "select ts, avg(v) from " + TABLE1 + " where ts in '2024' sample by 1d align to calendar time zone '" + tz + "'";
            ddl("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMaterializedViewDefinition("test", query, TABLE1, 1, 'd', -1L, -1L, tz, null);
        });
    }

    @Test
    public void testCreateMatViewSampleByTimeZoneWithOffset() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String tz = "Europe/Berlin";
            final String offset = "00:45";
            final String query = "select ts, avg(v) from " + TABLE1 + " where ts in '2024' sample by 1d align to calendar time zone '" + tz + "' with offset '" + offset + "'";
            ddl("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMaterializedViewDefinition("test", query, TABLE1, 1, 'd', -1L, -1L, tz, offset);
        });
    }

    @Test
    public void testCreateMatViewSampleByWithOffset() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String offset = "00:45";
            final String query = "select ts, avg(v) from " + TABLE1 + " where ts in '2024' sample by 1d align to calendar with offset '" + offset + "'";
            ddl("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMaterializedViewDefinition("test", query, TABLE1, 1, 'd', -1L, -1L, null, offset);
        });
    }

    @Test
    public void testCreateMatViewWithBase() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            final String query = "select t1.ts, avg(t1.v) from " + TABLE1 + " as t1 join " + TABLE2 + " as t2 on v sample by 60s";
            ddl("create materialized view test with base " + TABLE1 + " as (" + query + ") partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMaterializedViewDefinition("test", query, TABLE1, 60, 's');
        });
    }

    @Test
    public void testCreateMatViewWithExistingTableName() throws Exception {
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

            final String query = "select ts, avg(v) from " + TABLE2 + " sample by 4h";
            ddl("create materialized view test as (" + query + ") partition by day");

            // without IF NOT EXISTS
            try {
                ddl("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "A view or a table already exists with this name");
            }

            // with IF NOT EXISTS
            ddl("create materialized view if not exists test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
            assertMaterializedViewDefinition("test", query, TABLE2, 4, 'h');
        });
    }

    @Test
    public void testCreateMatViewWithIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, k, avg(v) from " + TABLE1 + " sample by 30s";
            ddl("create materialized view test as (" + query + "), index (k) partition by day");

            assertQuery("ts\tk\tavg\n", "test", "ts", true, true);
            assertMaterializedViewDefinition("test", query, TABLE1, 30, 's');

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertFalse(metadata.isDedupKey(0));
                assertTrue(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));

                assertFalse(metadata.isColumnIndexed(0));
                assertTrue(metadata.isColumnIndexed(1));
                assertFalse(metadata.isColumnIndexed(2));
            }
        });
    }

    @Test
    public void testCreateMatViewWithOperator() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
            ddl("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tdoubleV\tavg\n", "test", "ts", true, true);
            assertMaterializedViewDefinition("test", query, TABLE1, 30, 's');
        });
    }

    private static void assertMaterializedViewDefinition(String name, String query, String baseTableName, int intervalValue, char intervalQualifier) {
        assertMaterializedViewDefinition(name, query, baseTableName, intervalValue, intervalQualifier, -1L, -1L, null, null);
    }

    private static void assertMaterializedViewDefinition(
            String name, String query, String baseTableName, int intervalValue, char intervalQualifier,
            long fromMicros, long toMicros, String timeZone, String timeZoneOffset
    ) {
        final MaterializedViewDefinition matViewDefinition = engine.getMaterializedViewGraph().getView(name);
        assertTrue(matViewDefinition.getMatViewToken().isMatView());
        assertTrue(matViewDefinition.getMatViewToken().isWal());
        assertEquals(query, matViewDefinition.getQuery());
        assertEquals(baseTableName, matViewDefinition.getBaseTableName());
        assertEquals(intervalValue, matViewDefinition.getIntervalValue());
        assertEquals(intervalQualifier, matViewDefinition.getIntervalQualifier());
        assertEquals(fromMicros, matViewDefinition.getFromMicros());
        assertEquals(toMicros, matViewDefinition.getToMicros());
        assertEquals(timeZone, timeZone != null ? matViewDefinition.getTimeZone() : null);
        assertEquals(timeZoneOffset != null ? timeZoneOffset : "00:00", matViewDefinition.getTimeZoneOffset());
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

    private long parseAsMicros(String date) throws ParseException {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.parse(date).getTime() * 1000;
    }
}
