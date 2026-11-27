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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class HourGroupByTest extends AbstractCairoTest {
    private static final String EXPECTED = "h\ts\tn\nnull\t5.0\t1\n0\t7.0\t3\n1\t3.0\t1\n2\tnull\t1\n23\t6.0\t1\n";
    private static final String EXPECTED_DESIGNATED = "h\ts\tn\n0\t7.0\t3\n1\t3.0\t1\n2\tnull\t1\n23\t6.0\t1\n";

    @Test
    public void testComputedTimestampAndAggregateArgumentsKeepOrdinaryGrouping() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_hour", "TIMESTAMP");
            assertHourGroupBy(
                    "SELECT hour(dateadd('h',1,ts)) h,sum(d) s,count() n FROM lp_hour ORDER BY h",
                    "h\ts\tn\nnull\t5.0\t1\n0\t6.0\t1\n1\t7.0\t3\n2\t3.0\t1\n3\tnull\t1\n",
                    "Async Group By"
            );
            assertHourGroupBy(
                    "SELECT hour(ts) h,sum(d+1.0) s,count() n FROM lp_hour ORDER BY h",
                    "h\ts\tn\nnull\t6.0\t1\n0\t10.0\t3\n1\t4.0\t1\n2\tnull\t1\n23\t7.0\t1\n",
                    "Async Group By"
            );
        });
    }

    @Test
    public void testHourKeysMicroAndNanoKeepDayBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            for (int precision = 0; precision < 2; precision++) {
                final String type = precision == 0 ? "TIMESTAMP" : "TIMESTAMP_NS";
                final String table = "lp_hour_" + type;
                createDesignatedRows(table, type);
                assertHourGroupBy("SELECT hour(ts) h,sum(d) s,count() n FROM " + table + " ORDER BY h", EXPECTED_DESIGNATED, "GroupBy vectorized: true");
                assertHourGroupBy("SELECT hour(ts) h FROM " + table + " GROUP BY h ORDER BY h", "h\n0\n1\n2\n23\n", "GroupBy vectorized: true");
                assertHourGroupBy("SELECT DISTINCT hour(ts) h FROM " + table + " ORDER BY h", "h\n0\n1\n2\n23\n", "GroupBy vectorized: true");
            }
            execute("CREATE TABLE lp_empty (ts TIMESTAMP_NS,d DOUBLE)");
            assertHourGroupBy("SELECT hour(ts) h,sum(d) s,count() n FROM lp_empty ORDER BY h", "h\ts\tn\n", null);
        });
    }

    @Test
    public void testNullableHourKeysUseVectorizedGrouping() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_hour", "TIMESTAMP");
            assertHourGroupBy(
                    "SELECT hour(ts) h,sum(d) s,count() n FROM lp_hour ORDER BY h",
                    "h\ts\tn\n0\t7.0\t3\n1\t3.0\t1\n2\tnull\t1\n19\t5.0\t1\n23\t6.0\t1\n",
                    "GroupBy vectorized: true"
            );
            createRows("lp_hour_ns", "TIMESTAMP_NS");
            assertHourGroupBy(
                    "SELECT hour(ts) h,sum(d) s,count() n FROM lp_hour_ns ORDER BY h",
                    "h\ts\tn\n0\t12.0\t4\n1\t3.0\t1\n2\tnull\t1\n23\t6.0\t1\n",
                    "GroupBy vectorized: true"
            );
        });
    }

    @Test
    public void testParallelDisabledKeepsSerialGrouping() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_hour", "TIMESTAMP_NS");
            final boolean wasParallel = sqlExecutionContext.isParallelGroupByEnabled();
            sqlExecutionContext.setParallelGroupByEnabled(false);
            try {
                assertHourGroupBy("SELECT hour(ts) h,sum(d) s,count() n FROM lp_hour ORDER BY h", EXPECTED, "GroupBy vectorized: false");
            } finally {
                sqlExecutionContext.setParallelGroupByEnabled(wasParallel);
            }
        });
    }

    @Test
    public void testParquetConvertedColumnKeepsGuardedAsyncGrouping() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_hour AS (SELECT timestamp_sequence(0,3600000000) ts,x::int d FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("ALTER TABLE lp_hour CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
            drainWalQueue();
            final String sql = "SELECT hour(ts) h,sum(d) s,count() n FROM lp_hour ORDER BY h";
            assertHourGroupBy(sql, "h\ts\tn\n0\t1\t1\n1\t2\t1\n2\t3\t1\n", "GroupBy vectorized: true");
            execute("ALTER TABLE lp_hour ALTER COLUMN d TYPE DOUBLE");
            drainWalQueue();
            assertHourGroupBy(sql, "h\ts\tn\n0\t1.0\t1\n1\t2.0\t1\n2\t3.0\t1\n", "Async Group By");
        });
    }

    @Test
    public void testRetainedHourFactorySurvivesDifferentPrecisionCompilation() throws Exception {
        assertMemoryLeak(() -> {
            createDesignatedRows("lp_micro", "TIMESTAMP");
            createDesignatedRows("lp_nano", "TIMESTAMP_NS");
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT hour(ts) h,sum(d) s,count() n FROM lp_micro ORDER BY h", sqlExecutionContext).getRecordCursorFactory();
                    assertResult(retained, EXPECTED_DESIGNATED);
                    try (RecordCursorFactory other = compiler.compile("SELECT hour(ts) h,sum(d) s,count() n FROM lp_nano ORDER BY h", sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(other, EXPECTED_DESIGNATED);
                    }
                    compiler.clear();
                }
                execute("INSERT INTO lp_micro VALUES ('1970-01-02T03:00:00.000000Z',8.0)");
                assertResult(retained, "h\ts\tn\n0\t7.0\t3\n1\t3.0\t1\n2\tnull\t1\n3\t8.0\t1\n23\t6.0\t1\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    private void assertHourGroupBy(String sql, String expected, String planPart) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            if (planPart != null) {
                final TextPlanSink sink = new TextPlanSink();
                sink.of(factory, sqlExecutionContext);
                TestUtils.assertContains(sink.getSink(), planPart);
            }
            assertResult(factory, expected);
        }
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        Assert.assertEquals(ColumnType.INT, factory.getMetadata().getColumnType(0));
        Assert.assertEquals(-1, factory.getMetadata().getTimestampIndex());
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().sizeMayVary().returns(expected);
    }

    private void createDesignatedRows(String table, String timestampType) throws Exception {
        execute("CREATE TABLE " + table + " (ts " + timestampType + ",d DOUBLE) TIMESTAMP(ts)");
        execute("INSERT INTO " + table + " VALUES ('1970-01-01T00:00:00.000000Z',1.0),"
                + "('1970-01-01T00:59:59.999999Z',2.0),('1970-01-01T01:00:00.000000Z',3.0),"
                + "('1970-01-01T02:00:00.000000Z',null),('1970-01-01T23:59:59.999999Z',6.0),"
                + "('1970-01-02T00:00:00.000000Z',4.0)");
    }

    private void createRows(String table, String timestampType) throws Exception {
        execute("CREATE TABLE " + table + " (ts " + timestampType + ",d DOUBLE)");
        execute("INSERT INTO " + table + " VALUES (null,5.0),('1969-12-31T23:59:59.999999Z',6.0),"
                + "('1970-01-01T00:00:00.000000Z',1.0),('1970-01-01T00:59:59.999999Z',2.0),"
                + "('1970-01-01T01:00:00.000000Z',3.0),('1970-01-01T02:00:00.000000Z',null),"
                + "('1970-01-02T00:00:00.000000Z',4.0)");
    }
}
