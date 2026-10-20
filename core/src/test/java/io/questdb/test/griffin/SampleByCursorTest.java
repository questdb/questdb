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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FillMode;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SampleByCursorTest extends AbstractCairoTest {
    @Test
    public void testSparsePrevFirstObservationBothPrecisions() throws Exception {
        assertMemoryLeak(() -> {
            for (int precision = 0; precision < 2; precision++) {
                final String type = precision == 0 ? "TIMESTAMP" : "TIMESTAMP_NS";
                final String table = "sparse_prev_" + precision;
                execute("CREATE TABLE " + table + " (v DOUBLE, ts " + type + ") TIMESTAMP(ts)");
                execute("INSERT INTO " + table + " VALUES"
                        + "(2.0,'1970-01-01T00:00:03'),(4.0,'1970-01-01T00:00:07'),"
                        + "(NULL,'1970-01-01T00:00:38'),(6.0,'1970-01-01T00:01:20'),"
                        + "(8.0,'1970-01-01T00:01:25'),(10.0,'1970-01-01T00:01:54')");
                final String sql = "SELECT ts,avg(v) a FROM " + table
                        + " SAMPLE BY 10s FILL(PREV) ALIGN TO FIRST OBSERVATION";
                final String expected = """
                        ts	a
                        1970-01-01T00:00:03.000000Z	3.0
                        1970-01-01T00:00:13.000000Z	3.0
                        1970-01-01T00:00:23.000000Z	3.0
                        1970-01-01T00:00:33.000000Z	null
                        1970-01-01T00:00:43.000000Z	null
                        1970-01-01T00:00:53.000000Z	null
                        1970-01-01T00:01:03.000000Z	null
                        1970-01-01T00:01:13.000000Z	6.0
                        1970-01-01T00:01:23.000000Z	8.0
                        1970-01-01T00:01:33.000000Z	8.0
                        1970-01-01T00:01:43.000000Z	8.0
                        1970-01-01T00:01:53.000000Z	10.0
                        """;
                assertQuery(sql).noLeakCheck().timestamp("ts").noRandomAccess()
                        .returns(precision == 0 ? expected : expected.replace(".000000Z", ".000000000Z"));
                assertQuery("SELECT sum(a) FROM (" + sql + ")").noLeakCheck().noRandomAccess().expectSize().returns("sum\n49.0\n");
                assertQuery(sql.replace(" SAMPLE BY ", " WHERE v < 0 SAMPLE BY "))
                        .noLeakCheck().timestamp("ts").noRandomAccess().returns("ts\ta\n");
            }
        });
    }

    @Test
    public void testPrevFirstObservationTimestampBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            for (int precision = 0; precision < 2; precision++) {
                final String type = precision == 0 ? "TIMESTAMP" : "TIMESTAMP_NS";
                final String unit = precision == 0 ? "U" : "n";
                final String sample = " SAMPLE BY 10" + unit + " FILL(PREV) ALIGN TO FIRST OBSERVATION";
                assertQuery("SELECT ts::long t, a FROM (SELECT ts,avg(v) a FROM "
                        + "((SELECT (9_223_372_036_854_775_807L - (3-x)*10)::" + type
                        + " ts,x::double v FROM long_sequence(3) ORDER BY ts) TIMESTAMP(ts))" + sample + ")")
                        .noLeakCheck().noRandomAccess().returns("""
                                t	a
                                9223372036854775787	1.0
                                9223372036854775797	2.0
                                """);
                assertQuery("SELECT ts::long t, a FROM (SELECT ts,avg(v) a FROM "
                        + "((SELECT 9_223_372_036_854_775_806L::" + type
                        + " ts,1.0 v FROM long_sequence(1)) TIMESTAMP(ts))" + sample + ")")
                        .noLeakCheck().noRandomAccess().returns("t\ta\n9223372036854775806\t1.0\n");
                assertQuery("SELECT ts::long t, a FROM (SELECT ts,avg(v) a FROM "
                        + "((SELECT 9_223_372_036_854_775_807L::" + type
                        + " ts,1.0 v FROM long_sequence(1)) TIMESTAMP(ts))" + sample + ")")
                        .noLeakCheck().noRandomAccess().returns("t\ta\n");
                assertQuery("SELECT ts,avg(v) a FROM ((SELECT "
                        + "(CASE WHEN x = 1 THEN -2L ELSE 9_223_372_036_854_775_806L END)::" + type
                        + " ts,1.0 v FROM long_sequence(2) ORDER BY ts) TIMESTAMP(ts))"
                        + " SAMPLE BY " + (precision == 0 ? "2000000000h" : "2000000h") + " FILL(PREV) ALIGN TO FIRST OBSERVATION")
                        .noLeakCheck().failsWith("data row timestamp 4046744073709551614 precedes next bucket 7199999999999999998");
                assertQuery("SELECT ts,avg(v) a FROM ((SELECT NULL::" + type
                        + " ts,1.0 v FROM long_sequence(1)) TIMESTAMP(ts))" + sample)
                        .noLeakCheck().failsWith("SAMPLE BY designated timestamp cannot be NULL");
            }
        });
    }

    @Test
    public void testFirstObservationFillModesBothPrecisions() throws Exception {
        assertMemoryLeak(() -> {
            {
                final String type = "TIMESTAMP";
                final String table = "lp_sample_cursor_" + type;
                createRows(table, type);
                {
                    final String fill = "";
                    assertRowsOnly(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000Z	4.0
                                    2024-01-01T02:15:00.000000Z	5.0
                                    2024-01-01T03:15:00.000000Z	7.0
                                    """
                    );
                    assertRowsOnly(
                            "SELECT ts,k,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION ORDER BY ts,k",
                            """
                                    ts	k	total
                                    2024-01-01T00:15:00.000000Z	a	1.0
                                    2024-01-01T00:15:00.000000Z	b	3.0
                                    2024-01-01T02:15:00.000000Z	a	5.0
                                    2024-01-01T03:15:00.000000Z	b	7.0
                                    """
                    );
                }
                {
                    final String fill = " FILL(NONE)";
                    assertRowsOnly(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000Z	4.0
                                    2024-01-01T02:15:00.000000Z	5.0
                                    2024-01-01T03:15:00.000000Z	7.0
                                    """
                    );
                    assertRowsOnly(
                            "SELECT ts,k,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION ORDER BY ts,k",
                            """
                                    ts	k	total
                                    2024-01-01T00:15:00.000000Z	a	1.0
                                    2024-01-01T00:15:00.000000Z	b	3.0
                                    2024-01-01T02:15:00.000000Z	a	5.0
                                    2024-01-01T03:15:00.000000Z	b	7.0
                                    """
                    );
                }
                {
                    final String fill = " FILL(PREV)";
                    assertRowsOnly(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000Z	4.0
                                    2024-01-01T01:15:00.000000Z	4.0
                                    2024-01-01T02:15:00.000000Z	5.0
                                    2024-01-01T03:15:00.000000Z	7.0
                                    """
                    );
                    assertRowsOnly(
                            "SELECT ts,k,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION ORDER BY ts,k",
                            """
                                    ts	k	total
                                    2024-01-01T00:15:00.000000Z	a	1.0
                                    2024-01-01T00:15:00.000000Z	b	3.0
                                    2024-01-01T01:15:00.000000Z	a	1.0
                                    2024-01-01T01:15:00.000000Z	b	3.0
                                    2024-01-01T02:15:00.000000Z	a	5.0
                                    2024-01-01T02:15:00.000000Z	b	3.0
                                    2024-01-01T03:15:00.000000Z	a	5.0
                                    2024-01-01T03:15:00.000000Z	b	7.0
                                    """
                    );
                }
                {
                    final String fill = " FILL(NULL)";
                    assertRowsOnly(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000Z	4.0
                                    2024-01-01T01:15:00.000000Z	null
                                    2024-01-01T02:15:00.000000Z	5.0
                                    2024-01-01T03:15:00.000000Z	7.0
                                    """
                    );
                    assertRowsOnly(
                            "SELECT ts,k,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION ORDER BY ts,k",
                            """
                                    ts	k	total
                                    2024-01-01T00:15:00.000000Z	a	1.0
                                    2024-01-01T00:15:00.000000Z	b	3.0
                                    2024-01-01T01:15:00.000000Z	a	null
                                    2024-01-01T01:15:00.000000Z	b	null
                                    2024-01-01T02:15:00.000000Z	a	5.0
                                    2024-01-01T02:15:00.000000Z	b	null
                                    2024-01-01T03:15:00.000000Z	a	null
                                    2024-01-01T03:15:00.000000Z	b	7.0
                                    """
                    );
                }
                {
                    final String fill = " FILL(42.0)";
                    assertRowsOnly(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000Z	4.0
                                    2024-01-01T01:15:00.000000Z	42.0
                                    2024-01-01T02:15:00.000000Z	5.0
                                    2024-01-01T03:15:00.000000Z	7.0
                                    """
                    );
                    assertRowsOnly(
                            "SELECT ts,k,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION ORDER BY ts,k",
                            """
                                    ts	k	total
                                    2024-01-01T00:15:00.000000Z	a	1.0
                                    2024-01-01T00:15:00.000000Z	b	3.0
                                    2024-01-01T01:15:00.000000Z	a	42.0
                                    2024-01-01T01:15:00.000000Z	b	42.0
                                    2024-01-01T02:15:00.000000Z	a	5.0
                                    2024-01-01T02:15:00.000000Z	b	42.0
                                    2024-01-01T03:15:00.000000Z	a	42.0
                                    2024-01-01T03:15:00.000000Z	b	7.0
                                    """
                    );
                }
                assertRowsOnly(
                        "SELECT ts,sum(v) total FROM " + table + " WHERE id<0 SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION",
                        """
                                ts	total
                                """
                );
                assertRowsOnly(
                        "SELECT hour(ts),count(),first(ts),last(ts) FROM " + table + " SAMPLE BY 2h ALIGN TO FIRST OBSERVATION",
                        """
                                hour	count	first	last
                                0	2	2024-01-01T00:15:00.000000Z	2024-01-01T00:35:00.000000Z
                                2	2	2024-01-01T02:15:00.000000Z	2024-01-01T03:35:00.000000Z
                                """
                );
            }
            {
                final String type = "TIMESTAMP_NS";
                final String table = "lp_sample_cursor_" + type;
                createRows(table, type);
                {
                    final String fill = "";
                    assertRowsOnly(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000000Z	4.0
                                    2024-01-01T02:15:00.000000000Z	5.0
                                    2024-01-01T03:15:00.000000000Z	7.0
                                    """
                    );
                    assertRowsOnly(
                            "SELECT ts,k,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION ORDER BY ts,k",
                            """
                                    ts	k	total
                                    2024-01-01T00:15:00.000000000Z	a	1.0
                                    2024-01-01T00:15:00.000000000Z	b	3.0
                                    2024-01-01T02:15:00.000000000Z	a	5.0
                                    2024-01-01T03:15:00.000000000Z	b	7.0
                                    """
                    );
                }
                {
                    final String fill = " FILL(NONE)";
                    assertRowsOnly(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000000Z	4.0
                                    2024-01-01T02:15:00.000000000Z	5.0
                                    2024-01-01T03:15:00.000000000Z	7.0
                                    """
                    );
                    assertRowsOnly(
                            "SELECT ts,k,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION ORDER BY ts,k",
                            """
                                    ts	k	total
                                    2024-01-01T00:15:00.000000000Z	a	1.0
                                    2024-01-01T00:15:00.000000000Z	b	3.0
                                    2024-01-01T02:15:00.000000000Z	a	5.0
                                    2024-01-01T03:15:00.000000000Z	b	7.0
                                    """
                    );
                }
                {
                    final String fill = " FILL(PREV)";
                    assertRowsOnly(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000000Z	4.0
                                    2024-01-01T01:15:00.000000000Z	4.0
                                    2024-01-01T02:15:00.000000000Z	5.0
                                    2024-01-01T03:15:00.000000000Z	7.0
                                    """
                    );
                    assertRowsOnly(
                            "SELECT ts,k,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION ORDER BY ts,k",
                            """
                                    ts	k	total
                                    2024-01-01T00:15:00.000000000Z	a	1.0
                                    2024-01-01T00:15:00.000000000Z	b	3.0
                                    2024-01-01T01:15:00.000000000Z	a	1.0
                                    2024-01-01T01:15:00.000000000Z	b	3.0
                                    2024-01-01T02:15:00.000000000Z	a	5.0
                                    2024-01-01T02:15:00.000000000Z	b	3.0
                                    2024-01-01T03:15:00.000000000Z	a	5.0
                                    2024-01-01T03:15:00.000000000Z	b	7.0
                                    """
                    );
                }
                {
                    final String fill = " FILL(NULL)";
                    assertRowsOnly(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000000Z	4.0
                                    2024-01-01T01:15:00.000000000Z	null
                                    2024-01-01T02:15:00.000000000Z	5.0
                                    2024-01-01T03:15:00.000000000Z	7.0
                                    """
                    );
                    assertRowsOnly(
                            "SELECT ts,k,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION ORDER BY ts,k",
                            """
                                    ts	k	total
                                    2024-01-01T00:15:00.000000000Z	a	1.0
                                    2024-01-01T00:15:00.000000000Z	b	3.0
                                    2024-01-01T01:15:00.000000000Z	a	null
                                    2024-01-01T01:15:00.000000000Z	b	null
                                    2024-01-01T02:15:00.000000000Z	a	5.0
                                    2024-01-01T02:15:00.000000000Z	b	null
                                    2024-01-01T03:15:00.000000000Z	a	null
                                    2024-01-01T03:15:00.000000000Z	b	7.0
                                    """
                    );
                }
                {
                    final String fill = " FILL(42.0)";
                    assertRowsOnly(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000000Z	4.0
                                    2024-01-01T01:15:00.000000000Z	42.0
                                    2024-01-01T02:15:00.000000000Z	5.0
                                    2024-01-01T03:15:00.000000000Z	7.0
                                    """
                    );
                    assertRowsOnly(
                            "SELECT ts,k,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION ORDER BY ts,k",
                            """
                                    ts	k	total
                                    2024-01-01T00:15:00.000000000Z	a	1.0
                                    2024-01-01T00:15:00.000000000Z	b	3.0
                                    2024-01-01T01:15:00.000000000Z	a	42.0
                                    2024-01-01T01:15:00.000000000Z	b	42.0
                                    2024-01-01T02:15:00.000000000Z	a	5.0
                                    2024-01-01T02:15:00.000000000Z	b	42.0
                                    2024-01-01T03:15:00.000000000Z	a	42.0
                                    2024-01-01T03:15:00.000000000Z	b	7.0
                                    """
                    );
                }
                assertRowsOnly(
                        "SELECT ts,sum(v) total FROM " + table + " WHERE id<0 SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION",
                        """
                                ts	total
                                """
                );
                assertRowsOnly(
                        "SELECT hour(ts),count(),first(ts),last(ts) FROM " + table + " SAMPLE BY 2h ALIGN TO FIRST OBSERVATION",
                        """
                                hour	count	first	last
                                0	2	2024-01-01T00:15:00.000000000Z	2024-01-01T00:35:00.000000000Z
                                2	2	2024-01-01T02:15:00.000000000Z	2024-01-01T03:35:00.000000000Z
                                """
                );
            }
        });
    }

    @Test
    public void testLinearAndMixedValuesKeepTheirFactories() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_sample_cursor", "TIMESTAMP");
            assertRowsOnly(
                    "SELECT ts,sum(v) FROM lp_sample_cursor SAMPLE BY 1h FILL(LINEAR) ALIGN TO CALENDAR",
                    """
                            ts	sum
                            2024-01-01T00:00:00.000000Z	4.0
                            2024-01-01T01:00:00.000000Z	4.5
                            2024-01-01T02:00:00.000000Z	5.0
                            2024-01-01T03:00:00.000000Z	7.0
                            """
            );
            assertRowsOnly(
                    "SELECT ts,k,sum(v) FROM lp_sample_cursor SAMPLE BY 1h FILL(LINEAR) ALIGN TO FIRST OBSERVATION ORDER BY ts,k",
                    """
                            ts	k	sum
                            2024-01-01T00:15:00.000000Z	a	1.0
                            2024-01-01T00:15:00.000000Z	b	3.0
                            2024-01-01T01:15:00.000000Z	a	3.0
                            2024-01-01T01:15:00.000000Z	b	4.333333333333333
                            2024-01-01T02:15:00.000000Z	a	5.0
                            2024-01-01T02:15:00.000000Z	b	5.666666666666667
                            2024-01-01T03:15:00.000000Z	a	7.0
                            2024-01-01T03:15:00.000000Z	b	7.0
                            """
            );
            assertRowsOnly(
                    "SELECT ts,sum(v) a,count() b FROM lp_sample_cursor SAMPLE BY 1h FILL(42.0,0) ALIGN TO FIRST OBSERVATION",
                    """
                            ts	a	b
                            2024-01-01T00:15:00.000000Z	4.0	2
                            2024-01-01T01:15:00.000000Z	42.0	0
                            2024-01-01T02:15:00.000000Z	5.0	1
                            2024-01-01T03:15:00.000000Z	7.0	1
                            """
            );
            assertRowsOnly(
                    "SELECT ts,sum(v) a,sum(v) b FROM lp_sample_cursor SAMPLE BY 1h FILL(NULL,42.0) ALIGN TO FIRST OBSERVATION",
                    """
                            ts	a	b
                            2024-01-01T00:15:00.000000Z	4.0	4.0
                            2024-01-01T01:15:00.000000Z	null	42.0
                            2024-01-01T02:15:00.000000Z	5.0	5.0
                            2024-01-01T03:15:00.000000Z	7.0	7.0
                            """
            );
            assertRowsOnly(
                    "SELECT ts,sum(v) a,max(v) b FROM lp_sample_cursor SAMPLE BY 1h FILL(LINEAR,42.0) ALIGN TO FIRST OBSERVATION",
                    """
                            ts	a	b
                            2024-01-01T00:15:00.000000Z	4.0	3.0
                            2024-01-01T01:15:00.000000Z	4.5	42.0
                            2024-01-01T02:15:00.000000Z	5.0	5.0
                            2024-01-01T03:15:00.000000Z	7.0	7.0
                            """
            );
            assertRowsOnly(
                    "SELECT ts,sum(id*2) total FROM lp_sample_cursor SAMPLE BY 1h FILL(42) ALIGN TO FIRST OBSERVATION",
                    """
                            ts	total
                            2024-01-01T00:15:00.000000Z	6
                            2024-01-01T01:15:00.000000Z	42
                            2024-01-01T02:15:00.000000Z	6
                            2024-01-01T03:15:00.000000Z	8
                            """
            );
        });
    }

    @Test
    public void testConstantExpressionStrideAndIndexedFirstLast() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_sample_cursor", "TIMESTAMP");
            assertRowsOnly(
                    "SELECT ts,sum(v) FROM lp_sample_cursor SAMPLE BY (1+0) h ALIGN TO CALENDAR",
                    """
                            ts	sum
                            2024-01-01T00:00:00.000000Z	4.0
                            2024-01-01T02:00:00.000000Z	5.0
                            2024-01-01T03:00:00.000000Z	7.0
                            """
            );
            assertRowsOnly(
                    "SELECT ts,count() FROM lp_sample_cursor SAMPLE BY (1+1)*30 m ALIGN TO FIRST OBSERVATION",
                    """
                            ts	count
                            2024-01-01T00:15:00.000000Z	2
                            2024-01-01T02:15:00.000000Z	1
                            2024-01-01T03:15:00.000000Z	1
                            """
            );
            assertRowsOnly(
                    "SELECT ts,first(v) first_v,last(v) last_v FROM lp_sample_cursor WHERE k='a' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION",
                    """
                            ts	first_v	last_v
                            2024-01-01T00:15:00.000000Z	1.0	1.0
                            2024-01-01T02:15:00.000000Z	5.0	5.0
                            """
            );
            assertRowsOnly(
                    "SELECT ts,first(v) first_v,last(v) last_v FROM lp_sample_cursor WHERE k='a' SAMPLE BY 1h FILL(NONE) ALIGN TO FIRST OBSERVATION",
                    """
                            ts	first_v	last_v
                            2024-01-01T00:15:00.000000Z	1.0	1.0
                            2024-01-01T02:15:00.000000Z	5.0	5.0
                            """
            );
            assertRowsOnly(
                    "SELECT ts,k,first(v),last(v) FROM lp_sample_cursor WHERE k='a' SAMPLE BY (1+0) h ALIGN TO CALENDAR",
                    """
                            ts	k	first	last
                            2024-01-01T00:00:00.000000Z	a	1.0	1.0
                            2024-01-01T02:00:00.000000Z	a	5.0	5.0
                            """
            );
        });
    }

    @Test
    public void testComputedKeysUseRawInputAndUserOrderFollowsSampling() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_sample_cursor", "TIMESTAMP");
            assertRowsOnly(
                    "SELECT ts,id%2 parity,sum(v) FROM lp_sample_cursor SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION ORDER BY ts,parity",
                    """
                            ts	parity	sum
                            2024-01-01T00:15:00.000000Z	0	3.0
                            2024-01-01T00:15:00.000000Z	1	1.0
                            2024-01-01T01:15:00.000000Z	0	3.0
                            2024-01-01T01:15:00.000000Z	1	1.0
                            2024-01-01T02:15:00.000000Z	0	3.0
                            2024-01-01T02:15:00.000000Z	1	5.0
                            2024-01-01T03:15:00.000000Z	0	7.0
                            2024-01-01T03:15:00.000000Z	1	5.0
                            """
            );
            assertRowsOnly(
                    "SELECT hour(ts) h,count() FROM lp_sample_cursor SAMPLE BY 2h ALIGN TO FIRST OBSERVATION ORDER BY h",
                    """
                            h	count
                            0	2
                            2	2
                            """
            );
            assertRowsOnly(
                    "SELECT count() ts FROM lp_sample_cursor SAMPLE BY 1h ALIGN TO FIRST OBSERVATION",
                    """
                            ts
                            2
                            1
                            1
                            """
            );
            assertRowsOnly(
                    "SELECT CAST(ts AS TIMESTAMP) bucket,count() FROM lp_sample_cursor SAMPLE BY 1h ALIGN TO FIRST OBSERVATION",
                    """
                            bucket	count
                            2024-01-01T00:15:00.000000Z	2
                            2024-01-01T02:15:00.000000Z	1
                            2024-01-01T03:15:00.000000Z	1
                            """
            );
            assertRowsOnly(
                    "SELECT ts,count() FROM lp_sample_cursor SAMPLE BY 1h ALIGN TO FIRST OBSERVATION ORDER BY ts DESC LIMIT 1",
                    """
                            ts	count
                            2024-01-01T03:15:00.000000Z	1
                            """
            );
            assertRowsOnly(
                    "SELECT ts,count() FROM lp_sample_cursor SAMPLE BY 1h ALIGN TO FIRST OBSERVATION LIMIT -2",
                    """
                            ts	count
                            2024-01-01T02:15:00.000000Z	1
                            2024-01-01T03:15:00.000000Z	1
                            """
            );
            assertRowsOnly(
                    "SELECT ts,count() FROM lp_sample_cursor SAMPLE BY 1h ALIGN TO FIRST OBSERVATION LIMIT 0",
                    """
                            ts	count
                            """
            );
        });
    }

    @Test
    public void testFillReadsLatestSourceRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_fill_rows (k SYMBOL, s STRING, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO lp_fill_rows VALUES
                    ('a', 'x', 1.0, '2024-01-01T00:10:00'),
                    ('a', 'y', 2.0, '2024-01-01T01:10:00'),
                    ('b', 'z', 3.0, '2024-01-01T01:20:00'),
                    ('a', 'w', 4.0, '2024-01-01T04:10:00')
                    """);
            assertRowsOnly(
                    "SELECT ts, k, first(s) s, sum(v) v FROM lp_fill_rows SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION",
                    """
                            ts	k	s	v
                            2024-01-01T00:10:00.000000Z	a	x	1.0
                            2024-01-01T00:10:00.000000Z	b		null
                            2024-01-01T01:10:00.000000Z	a	y	2.0
                            2024-01-01T01:10:00.000000Z	b	z	3.0
                            2024-01-01T02:10:00.000000Z	a	y	2.0
                            2024-01-01T02:10:00.000000Z	b	z	3.0
                            2024-01-01T03:10:00.000000Z	a	y	2.0
                            2024-01-01T03:10:00.000000Z	b	z	3.0
                            2024-01-01T04:10:00.000000Z	a	w	4.0
                            2024-01-01T04:10:00.000000Z	b	z	3.0
                            """
            );
            assertRowsOnly(
                    "SELECT ts, k, first(s) s, sum(v) v FROM lp_fill_rows SAMPLE BY 1h FILL(PREV, NULL) ALIGN TO FIRST OBSERVATION",
                    """
                            ts	k	s	v
                            2024-01-01T00:10:00.000000Z	a	x	1.0
                            2024-01-01T00:10:00.000000Z	b		null
                            2024-01-01T01:10:00.000000Z	a	y	2.0
                            2024-01-01T01:10:00.000000Z	b	z	3.0
                            2024-01-01T02:10:00.000000Z	a	y	null
                            2024-01-01T02:10:00.000000Z	b	z	null
                            2024-01-01T03:10:00.000000Z	a	y	null
                            2024-01-01T03:10:00.000000Z	b	z	null
                            2024-01-01T04:10:00.000000Z	a	w	4.0
                            2024-01-01T04:10:00.000000Z	b	z	null
                            """
            );
            assertRowsOnly(
                    "SELECT ts, first(s) s FROM lp_fill_rows WHERE k = 'b' SAMPLE BY (0+1)h FROM '2024-01-01' TO '2024-01-01T04:00' FILL(PREV)",
                    """
                            ts	s
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T01:00:00.000000Z	z
                            2024-01-01T02:00:00.000000Z	z
                            2024-01-01T03:00:00.000000Z	z
                            """
            );
        });
    }

    @Test
    public void testRuntimeBoundsAndDerivedBounds() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_sample_cursor", "TIMESTAMP");
            bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2024-01-01T00:15:00.000000Z"));
            bindVariableService.setTimestamp(1, MicrosFormatUtils.parseTimestamp("2024-01-01T04:00:00.000000Z"));
            final String sql = "SELECT ts,sum(v) FROM lp_sample_cursor SAMPLE BY 1h FROM $1 TO $2 FILL(NONE) ALIGN TO CALENDAR";
            try (RecordCursorFactory factory = select(sql)) {
                assertRowsOnly(factory, """
                        ts	sum
                        2024-01-01T00:15:00.000000Z	4.0
                        2024-01-01T02:15:00.000000Z	5.0
                        2024-01-01T03:15:00.000000Z	7.0
                        """);
                bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2024-01-01T01:15:00.000000Z"));
                assertRowsOnly(factory, """
                        ts	sum
                        2024-01-01T02:15:00.000000Z	5.0
                        2024-01-01T03:15:00.000000Z	7.0
                        """);
                bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2024-01-01T05:00:00.000000Z"));
                assertRowsOnly(factory, """
                        ts	sum
                        """);
            }
            assertRowsOnly(
                    "SELECT ts,sum(v) FROM (SELECT * FROM lp_sample_cursor) TIMESTAMP(ts) SAMPLE BY 1h FROM '2024-01-01T00:15:00' TO '2024-01-01T04:00:00'",
                    """
                            ts	sum
                            2024-01-01T00:15:00.000000Z	4.0
                            2024-01-01T02:15:00.000000Z	5.0
                            2024-01-01T03:15:00.000000Z	7.0
                            """
            );
            assertRowsOnly(
                    "SELECT ts,sum(v) FROM (SELECT * FROM lp_sample_cursor WHERE id>0) SAMPLE BY 1h FROM '2024-01-01T00:15:00' TO '2024-01-01T04:00:00'",
                    """
                            ts	sum
                            2024-01-01T00:15:00.000000Z	4.0
                            2024-01-01T02:15:00.000000Z	5.0
                            2024-01-01T03:15:00.000000Z	7.0
                            """
            );
            final String kathmandu = """
                    ts	sum
                    2024-01-01T00:15:00.000000Z	4.0
                    2024-01-01T02:15:00.000000Z	5.0
                    2024-01-01T03:15:00.000000Z	7.0
                    """;
            assertRowsOnly(
                    "SELECT ts,sum(v) FROM lp_sample_cursor SAMPLE BY (1+0) h FROM '2024-01-01T06:00:00' TO '2024-01-01T10:00:00'"
                            + " ALIGN TO CALENDAR TIME ZONE 'Asia/Kathmandu'",
                    kathmandu
            );
            assertRowsOnly(
                    "SELECT ts,sum(v) FROM lp_sample_cursor SAMPLE BY 1h FROM '2024-01-01T06:00:00' TO '2024-01-01T10:00:00'"
                            + " ALIGN TO CALENDAR TIME ZONE 'Asia/Kathmandu'",
                    kathmandu
            );
        });
    }

    @Test
    public void testErrorsKeepInputOrderFillAndParameterDiagnostics() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_sample_cursor", "TIMESTAMP");
            assertQuery("SELECT ts,sum(v) FROM (SELECT * FROM lp_sample_cursor) SAMPLE BY 1h FROM '2024-01-01T00:15:00' TO '2024-01-01T04:00:00'").noLeakCheck().fails(65, "Sample by requires a designated TIMESTAMP");
            assertQuery("SELECT ts,sum(v) FROM (SELECT * FROM lp_sample_cursor ORDER BY ts DESC) SAMPLE BY 1h ALIGN TO FIRST OBSERVATION").noLeakCheck().fails(0, "base query does not provide ASC order over designated TIMESTAMP column");
            assertQuery("SELECT ts,sum(v) FROM (SELECT * FROM lp_sample_cursor ORDER BY ts DESC) TIMESTAMP(ts) SAMPLE BY 1h ALIGN TO FIRST OBSERVATION").noLeakCheck().fails(0, "base query does not provide ASC order over designated TIMESTAMP column");
            assertQuery("SELECT ts,sum(v) FROM lp_sample_cursor SAMPLE BY (1.5) h ALIGN TO FIRST OBSERVATION").noLeakCheck().fails(55, "unexpected token [h]");
            assertQuery("SELECT ts,sum(v) FROM lp_sample_cursor SAMPLE BY 1h FILL('bad') ALIGN TO FIRST OBSERVATION").noLeakCheck().fails(57, "invalid fill value: 'bad'");
            assertQuery("SELECT ts,sum(v),count(),max(v) FROM lp_sample_cursor SAMPLE BY 1h FILL(0,0) ALIGN TO FIRST OBSERVATION").noLeakCheck().fails(72, "not enough fill values");
        });
    }

    @Test
    public void testRetainedFactoryAndPlanPoolReset() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_sample_cursor", "TIMESTAMP");
            RecordCursorFactory retained = null;
            try {
                final String plan;
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT ts,sum(v) total FROM lp_sample_cursor SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION", sqlExecutionContext).getRecordCursorFactory();
                    plan = planText(retained);
                    compiler.clear();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT ts,count() FROM lp_sample_cursor SAMPLE BY 1h", sqlExecutionContext).getRecordCursorFactory()) {
                        compiler.clear();
                    }
                }
                Assert.assertEquals(plan, planText(retained));
                assertRowsOnly(retained, "ts\ttotal\n2024-01-01T00:15:00.000000Z\t4.0\n2024-01-01T01:15:00.000000Z\t4.0\n2024-01-01T02:15:00.000000Z\t5.0\n2024-01-01T03:15:00.000000Z\t7.0\n");
                execute("INSERT INTO lp_sample_cursor VALUES(5,'a',9.0,'2024-01-01T04:15:00')");
                assertRowsOnly(retained, "ts\ttotal\n2024-01-01T00:15:00.000000Z\t4.0\n2024-01-01T01:15:00.000000Z\t4.0\n2024-01-01T02:15:00.000000Z\t5.0\n2024-01-01T03:15:00.000000Z\t7.0\n2024-01-01T04:15:00.000000Z\t9.0\n");
            } finally {
                Misc.free(retained);
            }
            final SampleByPlan pooled = new SampleByPlan();
            pooled.setTimestampColumnId(42);
            pooled.setTimestampRequired(false);
            pooled.setPeriod("1h", null, 3, 'h', 4);
            pooled.setFrom(new ConstantExpression().ofTimestamp(1, ColumnType.TIMESTAMP_MICRO, 5));
            pooled.getFillModes().add(FillMode.VALUE);
            pooled.clear();
            Assert.assertEquals(-1, pooled.getTimestampColumnId());
            Assert.assertTrue(pooled.isTimestampRequired());
            Assert.assertNull(pooled.getPeriodToken());
            Assert.assertNull(pooled.getPeriod());
            Assert.assertNull(pooled.getFrom());
            Assert.assertEquals(0, pooled.getFillModes().size());
        });
    }

    private void createRows(String table, String type) throws SqlException {
        execute("CREATE TABLE " + table + "(id INT,k SYMBOL INDEX,v DOUBLE,ts " + type + ") TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO " + table + " VALUES(1,'a',1.0,'2024-01-01T00:15:00'),"
                + "(2,'b',3.0,'2024-01-01T00:35:00'),(3,'a',5.0,'2024-01-01T02:15:00'),"
                + "(4,'b',7.0,'2024-01-01T03:35:00')");
    }
}
