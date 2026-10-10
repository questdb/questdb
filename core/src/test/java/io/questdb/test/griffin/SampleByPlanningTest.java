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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SampleByPlanningTest extends AbstractCairoTest {
    @Test
    public void testBaseWithoutProjectedTimestampSamplesByItsSourceTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_sample", "TIMESTAMP");
            assertRowsOnly(
                    "SELECT count() c,sum(v) total FROM (SELECT v FROM lp_sample) SAMPLE BY 1h",
                    """
                            c	total
                            2	3.0
                            2	3.0
                            2	11.0
                            """
            );
            assertRowsOnly(
                    "SELECT count() c FROM (SELECT v FROM lp_sample WHERE v > 1) SAMPLE BY 1h",
                    """
                            c
                            1
                            1
                            2
                            """
            );
            assertRowsOnly(
                    "SELECT sum(total) total FROM (SELECT sum(v) total FROM lp_sample SAMPLE BY 1h) SAMPLE BY 1d",
                    """
                            total
                            17.0
                            """
            );
        });
    }

    @Test
    public void testCalendarBucketsBothPrecisionsAndEmptyInput() throws Exception {
        assertMemoryLeak(() -> {
            {
                final String type = "TIMESTAMP";
                final String table = "lp_sample_" + type;
                createRows(table, type);
                assertRowsOnly(
                        "SELECT ts,count(),sum(v) total FROM " + table + " SAMPLE BY 1h",
                        """
                                ts	count	total
                                2024-03-31T00:00:00.000000Z	2	3.0
                                2024-03-31T01:00:00.000000Z	2	3.0
                                2024-03-31T02:00:00.000000Z	2	11.0
                                """
                );
                assertRowsOnly(
                        "SELECT ts,count() FROM " + table + " SAMPLE BY 15m FILL(NONE) ALIGN TO CALENDAR",
                        """
                                ts	count
                                2024-03-31T00:00:00.000000Z	1
                                2024-03-31T00:15:00.000000Z	1
                                2024-03-31T01:00:00.000000Z	1
                                2024-03-31T01:30:00.000000Z	1
                                2024-03-31T02:00:00.000000Z	1
                                2024-03-31T02:30:00.000000Z	1
                                """
                );
                assertRowsOnly(
                        "SELECT ts,count() FROM " + table + " WHERE id<0 SAMPLE BY 1h",
                        """
                                ts	count
                                """
                );
                assertRowsOnly(
                        "SELECT ts,count() FROM " + table + " SAMPLE BY 1d ALIGN TO CALENDAR",
                        """
                                ts	count
                                2024-03-31T00:00:00.000000Z	6
                                """
                );
            }
            {
                final String type = "TIMESTAMP_NS";
                final String table = "lp_sample_" + type;
                createRows(table, type);
                assertRowsOnly(
                        "SELECT ts,count(),sum(v) total FROM " + table + " SAMPLE BY 1h",
                        """
                                ts	count	total
                                2024-03-31T00:00:00.000000000Z	2	3.0
                                2024-03-31T01:00:00.000000000Z	2	3.0
                                2024-03-31T02:00:00.000000000Z	2	11.0
                                """
                );
                assertRowsOnly(
                        "SELECT ts,count() FROM " + table + " SAMPLE BY 15m FILL(NONE) ALIGN TO CALENDAR",
                        """
                                ts	count
                                2024-03-31T00:00:00.000000000Z	1
                                2024-03-31T00:15:00.000000000Z	1
                                2024-03-31T01:00:00.000000000Z	1
                                2024-03-31T01:30:00.000000000Z	1
                                2024-03-31T02:00:00.000000000Z	1
                                2024-03-31T02:30:00.000000000Z	1
                                """
                );
                assertRowsOnly(
                        "SELECT ts,count() FROM " + table + " WHERE id<0 SAMPLE BY 1h",
                        """
                                ts	count
                                """
                );
                assertRowsOnly(
                        "SELECT ts,count() FROM " + table + " SAMPLE BY 1d ALIGN TO CALENDAR",
                        """
                                ts	count
                                2024-03-31T00:00:00.000000000Z	6
                                """
                );
            }
        });
    }

    @Test
    public void testKeysAliasesAndTimestampComputationsUseBuckets() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_sample", "TIMESTAMP");
            assertRowsOnly(
                    "SELECT ts bucket,k,sum(v) total FROM lp_sample SAMPLE BY 1h ORDER BY bucket,k",
                    """
                            bucket	k	total
                            2024-03-31T00:00:00.000000Z	a	1.0
                            2024-03-31T00:00:00.000000Z	b	2.0
                            2024-03-31T01:00:00.000000Z	a	3.0
                            2024-03-31T01:00:00.000000Z	b	null
                            2024-03-31T02:00:00.000000Z	a	5.0
                            2024-03-31T02:00:00.000000Z	b	6.0
                            """
            );
            assertRowsOnly(
                    "SELECT ts bucket,id%2 parity,count() FROM lp_sample SAMPLE BY 1h ORDER BY bucket,parity",
                    """
                            bucket	parity	count
                            2024-03-31T00:00:00.000000Z	0	1
                            2024-03-31T00:00:00.000000Z	1	1
                            2024-03-31T01:00:00.000000Z	0	1
                            2024-03-31T01:00:00.000000Z	1	1
                            2024-03-31T02:00:00.000000Z	0	1
                            2024-03-31T02:00:00.000000Z	1	1
                            """
            );
            assertRowsOnly("SELECT ts a,ts b,count() FROM lp_sample SAMPLE BY 1h", """
                    a	b	count
                    2024-03-31T00:00:00.000000Z	2024-03-31T00:00:00.000000Z	2
                    2024-03-31T01:00:00.000000Z	2024-03-31T01:00:00.000000Z	2
                    2024-03-31T02:00:00.000000Z	2024-03-31T02:00:00.000000Z	2
                    """);
            assertRowsOnly("SELECT hour(ts) h,count() FROM lp_sample SAMPLE BY 1h", """
                    h	count
                    0	2
                    1	2
                    2	2
                    """);
            assertRowsOnly(
                    "SELECT dateadd('m',5,ts) shifted,count() FROM lp_sample SAMPLE BY 1h",
                    """
                            shifted	count
                            2024-03-31T00:05:00.000000Z	2
                            2024-03-31T01:05:00.000000Z	2
                            2024-03-31T02:05:00.000000Z	2
                            """
            );
            assertRowsOnly(
                    "SELECT ts,count(),first(ts) first_ts,last(ts) last_ts FROM lp_sample SAMPLE BY 1h",
                    """
                            ts	count	first_ts	last_ts
                            2024-03-31T00:00:00.000000Z	2	2024-03-31T00:00:00.000000Z	2024-03-31T00:15:00.000000Z
                            2024-03-31T01:00:00.000000Z	2	2024-03-31T01:00:00.000000Z	2024-03-31T01:30:00.000000Z
                            2024-03-31T02:00:00.000000Z	2	2024-03-31T02:00:00.000000Z	2024-03-31T02:30:00.000000Z
                            """
            );
            assertRowsOnly(
                    "SELECT q.ts clock,count() FROM lp_sample q SAMPLE BY 1h ORDER BY clock DESC",
                    """
                            clock	count
                            2024-03-31T02:00:00.000000Z	2
                            2024-03-31T01:00:00.000000Z	2
                            2024-03-31T00:00:00.000000Z	2
                            """
            );
            assertRowsOnly("SELECT count() ts FROM lp_sample SAMPLE BY 1h", """
                    ts
                    2
                    2
                    2
                    """);
        });
    }

    @Test
    public void testStaticFromToIntersectsWhereAndAnchorsBuckets() throws Exception {
        assertMemoryLeak(() -> {
            {
                final String type = "TIMESTAMP";
                final String table = "lp_sample_range_" + type;
                createRows(table, type);
                final String prefix = "SELECT ts,count(),sum(v) FROM " + table;
                assertRowsOnly(
                        prefix + " SAMPLE BY 1h FROM '2024-03-31T00:15:00' TO '2024-03-31T02:00:00' FILL(NONE)",
                        """
                                ts	count	sum
                                2024-03-31T00:15:00.000000Z	2	5.0
                                2024-03-31T01:15:00.000000Z	1	null
                                """
                );
                assertRowsOnly(
                        prefix + " WHERE ts>='2024-03-31T01:00:00' SAMPLE BY 1h FROM '2024-03-31T00:15:00' TO '2024-03-31T02:00:00'",
                        """
                                ts	count	sum
                                2024-03-31T00:15:00.000000Z	1	3.0
                                2024-03-31T01:15:00.000000Z	1	null
                                """
                );
                assertRowsOnly(prefix + " SAMPLE BY 1h TO '2024-03-31T01:00:00'", """
                        ts	count	sum
                        2024-03-31T00:00:00.000000Z	2	3.0
                        """);
                assertRowsOnly(prefix + " SAMPLE BY 1h FROM '2024-04-01' TO '2024-04-02'", """
                        ts	count	sum
                        """);
                assertRowsOnly(
                        prefix + " SAMPLE BY 1h FROM '2024-03-31T01:00:00.000001' TO '2024-03-31T02:00:00.000001'"
                                + " ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                        """
                                ts	count	sum
                                2024-03-31T00:00:00.000001Z	2	5.0
                                """
                );
            }
            {
                final String type = "TIMESTAMP_NS";
                final String table = "lp_sample_range_" + type;
                createRows(table, type);
                final String prefix = "SELECT ts,count(),sum(v) FROM " + table;
                assertRowsOnly(
                        prefix + " SAMPLE BY 1h FROM '2024-03-31T00:15:00' TO '2024-03-31T02:00:00' FILL(NONE)",
                        """
                                ts	count	sum
                                2024-03-31T00:15:00.000000000Z	2	5.0
                                2024-03-31T01:15:00.000000000Z	1	null
                                """
                );
                assertRowsOnly(
                        prefix + " WHERE ts>='2024-03-31T01:00:00' SAMPLE BY 1h FROM '2024-03-31T00:15:00' TO '2024-03-31T02:00:00'",
                        """
                                ts	count	sum
                                2024-03-31T00:15:00.000000000Z	1	3.0
                                2024-03-31T01:15:00.000000000Z	1	null
                                """
                );
                assertRowsOnly(prefix + " SAMPLE BY 1h TO '2024-03-31T01:00:00'", """
                        ts	count	sum
                        2024-03-31T00:00:00.000000000Z	2	3.0
                        """);
                assertRowsOnly(prefix + " SAMPLE BY 1h FROM '2024-04-01' TO '2024-04-02'", """
                        ts	count	sum
                        """);
                assertRowsOnly(
                        prefix + " SAMPLE BY 1h FROM '2024-03-31T01:00:00.000001' TO '2024-03-31T02:00:00.000001'"
                                + " ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                        """
                                ts	count	sum
                                2024-03-31T00:00:00.000001000Z	2	5.0
                                """
                );
            }
            execute("CREATE TABLE lp_sample_ns_boundary(v INT,ts TIMESTAMP_NS) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sample_ns_boundary VALUES(1,'2024-03-31T00:00:00.000000999Z'),"
                    + "(2,'2024-03-31T00:00:00.000001000Z'),(3,'2024-03-31T00:00:00.000001001Z')");
            assertRowsOnly(
                    "SELECT ts,count(),sum(v) FROM lp_sample_ns_boundary SAMPLE BY 1s "
                            + "FROM '2024-03-31T01:00:00.000001000' TO '2024-03-31T01:00:00.000001001' "
                            + "ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                    """
                            ts	count	sum
                            2024-03-31T00:00:00.000001000Z	1	2
                            """
            );
        });
    }

    @Test
    public void testTimezoneDstSubdayAndOffset() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_sample", "TIMESTAMP");
            execute("INSERT INTO lp_sample VALUES(7,'a',7.0,'2024-03-31T23:30:00'),"
                    + "(8,'b',8.0,'2024-04-01T00:30:00'),(9,'a',9.0,'2024-10-26T23:30:00'),"
                    + "(10,'b',10.0,'2024-10-27T01:30:00'),(11,'a',11.0,'2024-10-27T23:30:00')");
            assertRowsOnly(
                    "SELECT ts,count() FROM lp_sample SAMPLE BY 1d ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                    """
                            ts	count
                            2024-03-30T23:00:00.000000Z	6
                            2024-03-31T22:00:00.000000Z	2
                            2024-10-26T22:00:00.000000Z	2
                            2024-10-27T23:00:00.000000Z	1
                            """
            );
            assertRowsOnly(
                    "SELECT ts,count() FROM lp_sample SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE 'Asia/Kathmandu'",
                    """
                            ts	count
                            2024-03-30T23:15:00.000000Z	1
                            2024-03-31T00:15:00.000000Z	2
                            2024-03-31T01:15:00.000000Z	2
                            2024-03-31T02:15:00.000000Z	1
                            2024-03-31T23:15:00.000000Z	1
                            2024-04-01T00:15:00.000000Z	1
                            2024-10-26T23:15:00.000000Z	1
                            2024-10-27T01:15:00.000000Z	1
                            2024-10-27T23:15:00.000000Z	1
                            """
            );
            assertRowsOnly(
                    "SELECT ts,count() FROM lp_sample SAMPLE BY 1h ALIGN TO CALENDAR WITH OFFSET '00:15'",
                    """
                            ts	count
                            2024-03-30T23:15:00.000000Z	1
                            2024-03-31T00:15:00.000000Z	2
                            2024-03-31T01:15:00.000000Z	2
                            2024-03-31T02:15:00.000000Z	1
                            2024-03-31T23:15:00.000000Z	1
                            2024-04-01T00:15:00.000000Z	1
                            2024-10-26T23:15:00.000000Z	1
                            2024-10-27T01:15:00.000000Z	1
                            2024-10-27T23:15:00.000000Z	1
                            """
            );
            assertRowsOnly(
                    "SELECT ts,count() FROM lp_sample SAMPLE BY 1d FROM '2024-03-31' TO '2024-04-02'"
                            + " ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                    """
                            ts	count
                            2024-03-30T23:00:00.000000Z	6
                            2024-03-31T22:00:00.000000Z	2
                            """
            );
            assertRowsOnly(
                    "SELECT ts,count() FROM lp_sample SAMPLE BY 1h FROM '2024-03-31T06:00:00' TO '2024-03-31T08:00:00'"
                            + " ALIGN TO CALENDAR TIME ZONE 'Asia/Kathmandu' WITH OFFSET '00:15'",
                    """
                            ts	count
                            2024-03-31T00:30:00.000000Z	2
                            2024-03-31T01:30:00.000000Z	2
                            """
            );
        });
    }

    @Test
    public void testFinalOrderingAndLimitsApplyAfterSampling() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_sample", "TIMESTAMP");
            assertRowsOnly(
                    "SELECT ts bucket,sum(v) total FROM lp_sample SAMPLE BY 1h ORDER BY total DESC",
                    """
                            bucket	total
                            2024-03-31T02:00:00.000000Z	11.0
                            2024-03-31T01:00:00.000000Z	3.0
                            2024-03-31T00:00:00.000000Z	3.0
                            """
            );
            assertRowsOnly(
                    "SELECT ts bucket,sum(v) total FROM lp_sample SAMPLE BY 1h ORDER BY 1 DESC LIMIT 2",
                    """
                            bucket	total
                            2024-03-31T02:00:00.000000Z	11.0
                            2024-03-31T01:00:00.000000Z	3.0
                            """
            );
            assertRowsOnly(
                    "SELECT ts bucket,count() FROM lp_sample SAMPLE BY 1h ORDER BY bucket DESC LIMIT -2",
                    """
                            bucket	count
                            2024-03-31T01:00:00.000000Z	2
                            2024-03-31T00:00:00.000000Z	2
                            """
            );
            assertRowsOnly("SELECT ts,count() FROM lp_sample SAMPLE BY 1h LIMIT 0", """
                    ts	count
                    """);
            assertRowsOnly("SELECT ts,count() FROM lp_sample SAMPLE BY 1h LIMIT 1,3", """
                    ts	count
                    2024-03-31T01:00:00.000000Z	2
                    2024-03-31T02:00:00.000000Z	2
                    """);
            bindVariableService.setLong(0, 1);
            final String query = "SELECT ts,count() FROM lp_sample SAMPLE BY 1h LIMIT $1";
            try (RecordCursorFactory factory = select(query)) {
                assertRowsOnly(factory, """
                        ts	count
                        2024-03-31T00:00:00.000000Z	2
                        """);
                bindVariableService.setLong(0, 0);
                assertRowsOnly(factory, """
                        ts	count
                        """);
                bindVariableService.setLong(0, -2);
                assertRowsOnly(factory, """
                        ts	count
                        2024-03-31T01:00:00.000000Z	2
                        2024-03-31T02:00:00.000000Z	2
                        """);
            }
        });
    }

    @Test
    public void testValidationAndRemainingShapesRecover() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_sample", "TIMESTAMP");
            execute("CREATE TABLE lp_sample_no_ts(v DOUBLE,ts TIMESTAMP)");
            assertQuery("SELECT ts,count() FROM lp_sample SAMPLE BY 1h GROUP BY ts").noLeakCheck().fails(55, "SELECT query must not contain both GROUP BY and SAMPLE BY");
            assertQuery("SELECT *,count() FROM lp_sample SAMPLE BY 1h").noLeakCheck().fails(7, "wildcard column select is not allowed in sample-by queries");
            assertQuery("SELECT ts FROM lp_sample SAMPLE BY 1h").noLeakCheck().fails(35, "at least one aggregation function must be present in 'select' clause");
            assertQuery("SELECT ts,count() FROM lp_sample_no_ts SAMPLE BY 1h FROM '2024-01-01'").noLeakCheck().fails(49, "Sample by requires a designated TIMESTAMP");
            assertQuery("SELECT ts,count() FROM lp_sample SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE 'No/SuchZone'").noLeakCheck().fails(74, "invalid timezone: No/SuchZone");
            assertQuery("SELECT ts,count() FROM lp_sample SAMPLE BY 1h ALIGN TO CALENDAR WITH OFFSET 'invalid'").noLeakCheck().fails(76, "invalid offset: invalid");
            assertRowsOnly(
                    "SELECT ts,count() FROM lp_sample SAMPLE BY 1h ALIGN TO FIRST OBSERVATION",
                    """
                            ts	count
                            2024-03-31T00:00:00.000000Z	2
                            2024-03-31T01:00:00.000000Z	2
                            2024-03-31T02:00:00.000000Z	2
                            """
            );
            assertQuery("SELECT ts,count() FROM (SELECT * FROM lp_sample) SAMPLE BY 1h FROM '2024-01-01'").noLeakCheck().fails(59, "Sample by requires a designated TIMESTAMP");
            assertRowsOnly(
                    "SELECT ts,count() FROM (SELECT * FROM lp_sample) TIMESTAMP(ts) SAMPLE BY 1h FROM '2024-01-01'",
                    """
                            ts	count
                            2024-03-31T00:00:00.000000Z	2
                            2024-03-31T01:00:00.000000Z	2
                            2024-03-31T02:00:00.000000Z	2
                            """
            );
            assertRowsOnly("SELECT ts,count() FROM lp_sample SAMPLE BY 1h FILL(PREV)", """
                    ts	count
                    2024-03-31T00:00:00.000000Z	2
                    2024-03-31T01:00:00.000000Z	2
                    2024-03-31T02:00:00.000000Z	2
                    """);
        });
    }

    @Test
    public void testRetainedFactoryReopensAfterCompilerReuseAndGrowth() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_sample", "TIMESTAMP");
            RecordCursorFactory retained = null;
            try {
                final String originalPlan;
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT ts bucket,count() FROM lp_sample SAMPLE BY 1h", sqlExecutionContext).getRecordCursorFactory();
                    originalPlan = planText(retained);
                    compiler.clear();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT ts,sum(v) FROM lp_sample SAMPLE BY 15m", sqlExecutionContext).getRecordCursorFactory()) {
                        compiler.clear();
                    }
                }
                Assert.assertEquals(originalPlan, planText(retained));
                assertRowsOnly(retained, "bucket\tcount\n2024-03-31T00:00:00.000000Z\t2\n2024-03-31T01:00:00.000000Z\t2\n2024-03-31T02:00:00.000000Z\t2\n");
                execute("INSERT INTO lp_sample VALUES(7,'c',7.0,'2024-03-31T03:00:00')");
                assertRowsOnly(retained, "bucket\tcount\n2024-03-31T00:00:00.000000Z\t2\n2024-03-31T01:00:00.000000Z\t2\n2024-03-31T02:00:00.000000Z\t2\n2024-03-31T03:00:00.000000Z\t1\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    private void createRows(String table, String type) throws SqlException {
        execute("CREATE TABLE " + table + "(id INT,k SYMBOL,v DOUBLE,ts " + type + ") TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO " + table + " VALUES(1,'a',1.0,'2024-03-31T00:00:00'),"
                + "(2,'b',2.0,'2024-03-31T00:15:00'),(3,'a',3.0,'2024-03-31T01:00:00'),"
                + "(4,'b',null,'2024-03-31T01:30:00'),(5,'a',5.0,'2024-03-31T02:00:00'),"
                + "(6,'b',6.0,'2024-03-31T02:30:00')");
    }

}
