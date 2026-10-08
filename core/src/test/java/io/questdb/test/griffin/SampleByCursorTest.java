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
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SampleByCursorTest extends AbstractCairoTest {
    @Test
    public void testFirstObservationFillModesBothPrecisions() throws Exception {
        assertMemoryLeak(() -> {
            {
                final String type = "TIMESTAMP";
                final String table = "lp_sample_cursor_" + type;
                createRows(table, type);
                {
                    final String fill = "";
                    assertQueryRows(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000Z	4.0
                                    2024-01-01T02:15:00.000000Z	5.0
                                    2024-01-01T03:15:00.000000Z	7.0
                                    """
                    );
                    assertQueryRows(
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
                    assertQueryRows(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000Z	4.0
                                    2024-01-01T02:15:00.000000Z	5.0
                                    2024-01-01T03:15:00.000000Z	7.0
                                    """
                    );
                    assertQueryRows(
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
                    assertQueryRows(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000Z	4.0
                                    2024-01-01T01:15:00.000000Z	4.0
                                    2024-01-01T02:15:00.000000Z	5.0
                                    2024-01-01T03:15:00.000000Z	7.0
                                    """
                    );
                    assertQueryRows(
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
                    assertQueryRows(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000Z	4.0
                                    2024-01-01T01:15:00.000000Z	null
                                    2024-01-01T02:15:00.000000Z	5.0
                                    2024-01-01T03:15:00.000000Z	7.0
                                    """
                    );
                    assertQueryRows(
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
                    assertQueryRows(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000Z	4.0
                                    2024-01-01T01:15:00.000000Z	42.0
                                    2024-01-01T02:15:00.000000Z	5.0
                                    2024-01-01T03:15:00.000000Z	7.0
                                    """
                    );
                    assertQueryRows(
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
                assertQueryRows(
                        "SELECT ts,sum(v) total FROM " + table + " WHERE id<0 SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION",
                        """
                                ts	total
                                """
                );
                assertQueryRows(
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
                    assertQueryRows(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000000Z	4.0
                                    2024-01-01T02:15:00.000000000Z	5.0
                                    2024-01-01T03:15:00.000000000Z	7.0
                                    """
                    );
                    assertQueryRows(
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
                    assertQueryRows(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000000Z	4.0
                                    2024-01-01T02:15:00.000000000Z	5.0
                                    2024-01-01T03:15:00.000000000Z	7.0
                                    """
                    );
                    assertQueryRows(
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
                    assertQueryRows(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000000Z	4.0
                                    2024-01-01T01:15:00.000000000Z	4.0
                                    2024-01-01T02:15:00.000000000Z	5.0
                                    2024-01-01T03:15:00.000000000Z	7.0
                                    """
                    );
                    assertQueryRows(
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
                    assertQueryRows(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000000Z	4.0
                                    2024-01-01T01:15:00.000000000Z	null
                                    2024-01-01T02:15:00.000000000Z	5.0
                                    2024-01-01T03:15:00.000000000Z	7.0
                                    """
                    );
                    assertQueryRows(
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
                    assertQueryRows(
                            "SELECT ts,sum(v) total FROM " + table + " SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION",
                            """
                                    ts	total
                                    2024-01-01T00:15:00.000000000Z	4.0
                                    2024-01-01T01:15:00.000000000Z	42.0
                                    2024-01-01T02:15:00.000000000Z	5.0
                                    2024-01-01T03:15:00.000000000Z	7.0
                                    """
                    );
                    assertQueryRows(
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
                assertQueryRows(
                        "SELECT ts,sum(v) total FROM " + table + " WHERE id<0 SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION",
                        """
                                ts	total
                                """
                );
                assertQueryRows(
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
            assertQueryRows(
                    "SELECT ts,sum(v) FROM lp_sample_cursor SAMPLE BY 1h FILL(LINEAR) ALIGN TO CALENDAR",
                    """
                            ts	sum
                            2024-01-01T00:00:00.000000Z	4.0
                            2024-01-01T01:00:00.000000Z	4.5
                            2024-01-01T02:00:00.000000Z	5.0
                            2024-01-01T03:00:00.000000Z	7.0
                            """
            );
            assertQueryRows(
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
            assertQueryRows(
                    "SELECT ts,sum(v) a,count() b FROM lp_sample_cursor SAMPLE BY 1h FILL(42.0,0) ALIGN TO FIRST OBSERVATION",
                    """
                            ts	a	b
                            2024-01-01T00:15:00.000000Z	4.0	2
                            2024-01-01T01:15:00.000000Z	42.0	0
                            2024-01-01T02:15:00.000000Z	5.0	1
                            2024-01-01T03:15:00.000000Z	7.0	1
                            """
            );
            assertQueryRows(
                    "SELECT ts,sum(v) a,sum(v) b FROM lp_sample_cursor SAMPLE BY 1h FILL(NULL,42.0) ALIGN TO FIRST OBSERVATION",
                    """
                            ts	a	b
                            2024-01-01T00:15:00.000000Z	4.0	4.0
                            2024-01-01T01:15:00.000000Z	null	42.0
                            2024-01-01T02:15:00.000000Z	5.0	5.0
                            2024-01-01T03:15:00.000000Z	7.0	7.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,sum(v) a,max(v) b FROM lp_sample_cursor SAMPLE BY 1h FILL(LINEAR,42.0) ALIGN TO FIRST OBSERVATION",
                    """
                            ts	a	b
                            2024-01-01T00:15:00.000000Z	4.0	3.0
                            2024-01-01T01:15:00.000000Z	4.5	42.0
                            2024-01-01T02:15:00.000000Z	5.0	5.0
                            2024-01-01T03:15:00.000000Z	7.0	7.0
                            """
            );
            assertQueryRows(
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
            assertQueryRows(
                    "SELECT ts,sum(v) FROM lp_sample_cursor SAMPLE BY (1+0) h ALIGN TO CALENDAR",
                    """
                            ts	sum
                            2024-01-01T00:00:00.000000Z	4.0
                            2024-01-01T02:00:00.000000Z	5.0
                            2024-01-01T03:00:00.000000Z	7.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,count() FROM lp_sample_cursor SAMPLE BY (1+1)*30 m ALIGN TO FIRST OBSERVATION",
                    """
                            ts	count
                            2024-01-01T00:15:00.000000Z	2
                            2024-01-01T02:15:00.000000Z	1
                            2024-01-01T03:15:00.000000Z	1
                            """
            );
            assertQueryRows(
                    "SELECT ts,first(v) first_v,last(v) last_v FROM lp_sample_cursor WHERE k='a' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION",
                    """
                            ts	first_v	last_v
                            2024-01-01T00:15:00.000000Z	1.0	1.0
                            2024-01-01T02:15:00.000000Z	5.0	5.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,first(v) first_v,last(v) last_v FROM lp_sample_cursor WHERE k='a' SAMPLE BY 1h FILL(NONE) ALIGN TO FIRST OBSERVATION",
                    """
                            ts	first_v	last_v
                            2024-01-01T00:15:00.000000Z	1.0	1.0
                            2024-01-01T02:15:00.000000Z	5.0	5.0
                            """
            );
            assertQueryRows(
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
            assertQueryRows(
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
            assertQueryRows(
                    "SELECT hour(ts) h,count() FROM lp_sample_cursor SAMPLE BY 2h ALIGN TO FIRST OBSERVATION ORDER BY h",
                    """
                            h	count
                            0	2
                            2	2
                            """
            );
            assertQueryRows(
                    "SELECT count() ts FROM lp_sample_cursor SAMPLE BY 1h ALIGN TO FIRST OBSERVATION",
                    """
                            ts
                            2
                            1
                            1
                            """
            );
            assertQueryRows(
                    "SELECT CAST(ts AS TIMESTAMP) bucket,count() FROM lp_sample_cursor SAMPLE BY 1h ALIGN TO FIRST OBSERVATION",
                    """
                            bucket	count
                            2024-01-01T00:15:00.000000Z	2
                            2024-01-01T02:15:00.000000Z	1
                            2024-01-01T03:15:00.000000Z	1
                            """
            );
            assertQueryRows(
                    "SELECT ts,count() FROM lp_sample_cursor SAMPLE BY 1h ALIGN TO FIRST OBSERVATION ORDER BY ts DESC LIMIT 1",
                    """
                            ts	count
                            2024-01-01T03:15:00.000000Z	1
                            """
            );
            assertQueryRows(
                    "SELECT ts,count() FROM lp_sample_cursor SAMPLE BY 1h ALIGN TO FIRST OBSERVATION LIMIT -2",
                    """
                            ts	count
                            2024-01-01T02:15:00.000000Z	1
                            2024-01-01T03:15:00.000000Z	1
                            """
            );
            assertQueryRows(
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
            assertQueryRows(
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
            assertQueryRows(
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
            assertQueryRows(
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
                assertResult(factory, """
                        ts	sum
                        2024-01-01T00:15:00.000000Z	4.0
                        2024-01-01T02:15:00.000000Z	5.0
                        2024-01-01T03:15:00.000000Z	7.0
                        """);
                bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2024-01-01T01:15:00.000000Z"));
                assertResult(factory, """
                        ts	sum
                        2024-01-01T02:15:00.000000Z	5.0
                        2024-01-01T03:15:00.000000Z	7.0
                        """);
                bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2024-01-01T05:00:00.000000Z"));
                assertResult(factory, """
                        ts	sum
                        """);
            }
            assertQueryRows(
                    "SELECT ts,sum(v) FROM (SELECT * FROM lp_sample_cursor) TIMESTAMP(ts) SAMPLE BY 1h FROM '2024-01-01T00:15:00' TO '2024-01-01T04:00:00'",
                    """
                            ts	sum
                            2024-01-01T00:15:00.000000Z	4.0
                            2024-01-01T02:15:00.000000Z	5.0
                            2024-01-01T03:15:00.000000Z	7.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,sum(v) FROM (SELECT * FROM lp_sample_cursor WHERE id>0) SAMPLE BY 1h FROM '2024-01-01T00:15:00' TO '2024-01-01T04:00:00'",
                    """
                            ts	sum
                            2024-01-01T00:15:00.000000Z	4.0
                            2024-01-01T02:15:00.000000Z	5.0
                            2024-01-01T03:15:00.000000Z	7.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,sum(v) FROM lp_sample_cursor SAMPLE BY (1+0) h FROM '2024-01-01T06:00:00' TO '2024-01-01T10:00:00'"
                            + " ALIGN TO CALENDAR TIME ZONE 'Asia/Kathmandu'",
                    """
                            ts	sum
                            2023-12-31T23:30:00.000000Z	1.0
                            2024-01-01T00:30:00.000000Z	3.0
                            2024-01-01T01:30:00.000000Z	5.0
                            2024-01-01T03:30:00.000000Z	7.0
                            """
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
                    plan = planOf(retained);
                    compiler.clear();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT ts,count() FROM lp_sample_cursor SAMPLE BY 1h", sqlExecutionContext).getRecordCursorFactory()) {
                        compiler.clear();
                    }
                }
                Assert.assertEquals(plan, planOf(retained));
                assertResult(retained, "ts\ttotal\n2024-01-01T00:15:00.000000Z\t4.0\n2024-01-01T01:15:00.000000Z\t4.0\n2024-01-01T02:15:00.000000Z\t5.0\n2024-01-01T03:15:00.000000Z\t7.0\n");
                execute("INSERT INTO lp_sample_cursor VALUES(5,'a',9.0,'2024-01-01T04:15:00')");
                assertResult(retained, "ts\ttotal\n2024-01-01T00:15:00.000000Z\t4.0\n2024-01-01T01:15:00.000000Z\t4.0\n2024-01-01T02:15:00.000000Z\t5.0\n2024-01-01T03:15:00.000000Z\t7.0\n2024-01-01T04:15:00.000000Z\t9.0\n");
            } finally {
                Misc.free(retained);
            }
            final SampleByPlan pooled = new SampleByPlan();
            pooled.setTimestampColumnId(42);
            pooled.setTimestampRequired(false);
            pooled.setFillMode(SampleByPlan.FILL_VALUE);
            pooled.setPeriod("1h", null, 3, 'h', 4);
            pooled.setFrom(new ConstantExpression().ofTimestamp(1, ColumnType.TIMESTAMP_MICRO, 5));
            pooled.getFillTokens().add("42");
            pooled.clear();
            Assert.assertEquals(-1, pooled.getTimestampColumnId());
            Assert.assertTrue(pooled.isTimestampRequired());
            Assert.assertEquals(SampleByPlan.FILL_NONE, pooled.getFillMode());
            Assert.assertNull(pooled.getPeriodToken());
            Assert.assertNull(pooled.getPeriod());
            Assert.assertNull(pooled.getFrom());
            Assert.assertEquals(0, pooled.getFillTokens().size());
        });
    }

    private void assertResult(RecordCursorFactory factory, String rows) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(rows);
    }

    private void createRows(String table, String type) throws SqlException {
        execute("CREATE TABLE " + table + "(id INT,k SYMBOL INDEX,v DOUBLE,ts " + type + ") TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO " + table + " VALUES(1,'a',1.0,'2024-01-01T00:15:00'),"
                + "(2,'b',3.0,'2024-01-01T00:35:00'),(3,'a',5.0,'2024-01-01T02:15:00'),"
                + "(4,'b',7.0,'2024-01-01T03:35:00')");
    }

    private String planOf(RecordCursorFactory factory) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, sqlExecutionContext);
        return sink.getSink().toString();
    }

    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
