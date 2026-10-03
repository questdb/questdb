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
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FillTest extends AbstractCairoTest {
    @Test
    public void testBroadcastFillBothTimestampPrecisions() throws Exception {
        assertMemoryLeak(() -> {
            {
                final String type = "TIMESTAMP";
                final String table = "lp_fill_" + type;
                createRows(table, type);
                {
                    final String fill = "PREV";
                    assertQueryRows(
                            "SELECT ts,sum(v),count(),max(v) FROM " + table + " SAMPLE BY 1h FILL(" + fill + ")",
                            """
                                    ts	sum	count	max
                                    2024-01-01T00:00:00.000000Z	4.0	2	3.0
                                    2024-01-01T01:00:00.000000Z	4.0	2	3.0
                                    2024-01-01T02:00:00.000000Z	5.0	1	5.0
                                    2024-01-01T03:00:00.000000Z	7.0	1	7.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,k,sum(v),count() FROM " + table + " SAMPLE BY 1h FILL(" + fill + ") ORDER BY ts,k",
                            """
                                    ts	k	sum	count
                                    2024-01-01T00:00:00.000000Z	a	1.0	1
                                    2024-01-01T00:00:00.000000Z	b	3.0	1
                                    2024-01-01T01:00:00.000000Z	a	1.0	1
                                    2024-01-01T01:00:00.000000Z	b	3.0	1
                                    2024-01-01T02:00:00.000000Z	a	5.0	1
                                    2024-01-01T02:00:00.000000Z	b	3.0	1
                                    2024-01-01T03:00:00.000000Z	a	5.0	1
                                    2024-01-01T03:00:00.000000Z	b	7.0	1
                                    """
                    );
                }
                {
                    final String fill = "NULL";
                    assertQueryRows(
                            "SELECT ts,sum(v),count(),max(v) FROM " + table + " SAMPLE BY 1h FILL(" + fill + ")",
                            """
                                    ts	sum	count	max
                                    2024-01-01T00:00:00.000000Z	4.0	2	3.0
                                    2024-01-01T01:00:00.000000Z	null	null	null
                                    2024-01-01T02:00:00.000000Z	5.0	1	5.0
                                    2024-01-01T03:00:00.000000Z	7.0	1	7.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,k,sum(v),count() FROM " + table + " SAMPLE BY 1h FILL(" + fill + ") ORDER BY ts,k",
                            """
                                    ts	k	sum	count
                                    2024-01-01T00:00:00.000000Z	a	1.0	1
                                    2024-01-01T00:00:00.000000Z	b	3.0	1
                                    2024-01-01T01:00:00.000000Z	a	null	null
                                    2024-01-01T01:00:00.000000Z	b	null	null
                                    2024-01-01T02:00:00.000000Z	a	5.0	1
                                    2024-01-01T02:00:00.000000Z	b	null	null
                                    2024-01-01T03:00:00.000000Z	a	null	null
                                    2024-01-01T03:00:00.000000Z	b	7.0	1
                                    """
                    );
                }
                {
                    final String fill = "42";
                    assertQueryRows(
                            "SELECT ts,sum(v),count(),max(v) FROM " + table + " SAMPLE BY 1h FILL(" + fill + ")",
                            """
                                    ts	sum	count	max
                                    2024-01-01T00:00:00.000000Z	4.0	2	3.0
                                    2024-01-01T01:00:00.000000Z	42.0	42	42.0
                                    2024-01-01T02:00:00.000000Z	5.0	1	5.0
                                    2024-01-01T03:00:00.000000Z	7.0	1	7.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,k,sum(v),count() FROM " + table + " SAMPLE BY 1h FILL(" + fill + ") ORDER BY ts,k",
                            """
                                    ts	k	sum	count
                                    2024-01-01T00:00:00.000000Z	a	1.0	1
                                    2024-01-01T00:00:00.000000Z	b	3.0	1
                                    2024-01-01T01:00:00.000000Z	a	42.0	42
                                    2024-01-01T01:00:00.000000Z	b	42.0	42
                                    2024-01-01T02:00:00.000000Z	a	5.0	1
                                    2024-01-01T02:00:00.000000Z	b	42.0	42
                                    2024-01-01T03:00:00.000000Z	a	42.0	42
                                    2024-01-01T03:00:00.000000Z	b	7.0	1
                                    """
                    );
                }
            }
            {
                final String type = "TIMESTAMP_NS";
                final String table = "lp_fill_" + type;
                createRows(table, type);
                {
                    final String fill = "PREV";
                    assertQueryRows(
                            "SELECT ts,sum(v),count(),max(v) FROM " + table + " SAMPLE BY 1h FILL(" + fill + ")",
                            """
                                    ts	sum	count	max
                                    2024-01-01T00:00:00.000000000Z	4.0	2	3.0
                                    2024-01-01T01:00:00.000000000Z	4.0	2	3.0
                                    2024-01-01T02:00:00.000000000Z	5.0	1	5.0
                                    2024-01-01T03:00:00.000000000Z	7.0	1	7.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,k,sum(v),count() FROM " + table + " SAMPLE BY 1h FILL(" + fill + ") ORDER BY ts,k",
                            """
                                    ts	k	sum	count
                                    2024-01-01T00:00:00.000000000Z	a	1.0	1
                                    2024-01-01T00:00:00.000000000Z	b	3.0	1
                                    2024-01-01T01:00:00.000000000Z	a	1.0	1
                                    2024-01-01T01:00:00.000000000Z	b	3.0	1
                                    2024-01-01T02:00:00.000000000Z	a	5.0	1
                                    2024-01-01T02:00:00.000000000Z	b	3.0	1
                                    2024-01-01T03:00:00.000000000Z	a	5.0	1
                                    2024-01-01T03:00:00.000000000Z	b	7.0	1
                                    """
                    );
                }
                {
                    final String fill = "NULL";
                    assertQueryRows(
                            "SELECT ts,sum(v),count(),max(v) FROM " + table + " SAMPLE BY 1h FILL(" + fill + ")",
                            """
                                    ts	sum	count	max
                                    2024-01-01T00:00:00.000000000Z	4.0	2	3.0
                                    2024-01-01T01:00:00.000000000Z	null	null	null
                                    2024-01-01T02:00:00.000000000Z	5.0	1	5.0
                                    2024-01-01T03:00:00.000000000Z	7.0	1	7.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,k,sum(v),count() FROM " + table + " SAMPLE BY 1h FILL(" + fill + ") ORDER BY ts,k",
                            """
                                    ts	k	sum	count
                                    2024-01-01T00:00:00.000000000Z	a	1.0	1
                                    2024-01-01T00:00:00.000000000Z	b	3.0	1
                                    2024-01-01T01:00:00.000000000Z	a	null	null
                                    2024-01-01T01:00:00.000000000Z	b	null	null
                                    2024-01-01T02:00:00.000000000Z	a	5.0	1
                                    2024-01-01T02:00:00.000000000Z	b	null	null
                                    2024-01-01T03:00:00.000000000Z	a	null	null
                                    2024-01-01T03:00:00.000000000Z	b	7.0	1
                                    """
                    );
                }
                {
                    final String fill = "42";
                    assertQueryRows(
                            "SELECT ts,sum(v),count(),max(v) FROM " + table + " SAMPLE BY 1h FILL(" + fill + ")",
                            """
                                    ts	sum	count	max
                                    2024-01-01T00:00:00.000000000Z	4.0	2	3.0
                                    2024-01-01T01:00:00.000000000Z	42.0	42	42.0
                                    2024-01-01T02:00:00.000000000Z	5.0	1	5.0
                                    2024-01-01T03:00:00.000000000Z	7.0	1	7.0
                                    """
                    );
                    assertQueryRows(
                            "SELECT ts,k,sum(v),count() FROM " + table + " SAMPLE BY 1h FILL(" + fill + ") ORDER BY ts,k",
                            """
                                    ts	k	sum	count
                                    2024-01-01T00:00:00.000000000Z	a	1.0	1
                                    2024-01-01T00:00:00.000000000Z	b	3.0	1
                                    2024-01-01T01:00:00.000000000Z	a	42.0	42
                                    2024-01-01T01:00:00.000000000Z	b	42.0	42
                                    2024-01-01T02:00:00.000000000Z	a	5.0	1
                                    2024-01-01T02:00:00.000000000Z	b	42.0	42
                                    2024-01-01T03:00:00.000000000Z	a	42.0	42
                                    2024-01-01T03:00:00.000000000Z	b	7.0	1
                                    """
                    );
                }
            }
        });
    }

    @Test
    public void testAliasesComputedKeysAndRepeatedAggregates() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_fill", "TIMESTAMP");
            assertQueryRows(
                    "SELECT ts a,ts b,sum(v) total FROM lp_fill SAMPLE BY 1h FILL(PREV)",
                    """
                            a	b	total
                            2024-01-01T00:00:00.000000Z	2024-01-01T00:00:00.000000Z	4.0
                            2024-01-01T01:00:00.000000Z	2024-01-01T01:00:00.000000Z	4.0
                            2024-01-01T02:00:00.000000Z	2024-01-01T02:00:00.000000Z	5.0
                            2024-01-01T03:00:00.000000Z	2024-01-01T03:00:00.000000Z	7.0
                            """
            );
            assertQueryRows("SELECT sum(v) ts FROM lp_fill SAMPLE BY 1h FILL(PREV)", """
                    ts
                    4.0
                    4.0
                    5.0
                    7.0
                    """);
            assertQueryRows(
                    "SELECT hour(ts) h,sum(v) total FROM lp_fill SAMPLE BY 1h FILL(PREV)",
                    """
                            h	total
                            0	4.0
                            1	4.0
                            2	5.0
                            3	7.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,id%2 parity,sum(v) FROM lp_fill SAMPLE BY 1h FILL(PREV) ORDER BY ts,parity",
                    """
                            ts	parity	sum
                            2024-01-01T00:00:00.000000Z	0	3.0
                            2024-01-01T00:00:00.000000Z	1	1.0
                            2024-01-01T01:00:00.000000Z	0	3.0
                            2024-01-01T01:00:00.000000Z	1	1.0
                            2024-01-01T02:00:00.000000Z	0	3.0
                            2024-01-01T02:00:00.000000Z	1	5.0
                            2024-01-01T03:00:00.000000Z	0	7.0
                            2024-01-01T03:00:00.000000Z	1	5.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,sum(v) a,sum(v) b FROM lp_fill SAMPLE BY 1h FILL(NULL,42.0)",
                    """
                            ts	a	b
                            2024-01-01T00:00:00.000000Z	4.0	4.0
                            2024-01-01T01:00:00.000000Z	null	42.0
                            2024-01-01T02:00:00.000000Z	5.0	5.0
                            2024-01-01T03:00:00.000000Z	7.0	7.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,sum(id*2) total FROM lp_fill SAMPLE BY 1h FILL(42)",
                    """
                            ts	total
                            2024-01-01T00:00:00.000000Z	6
                            2024-01-01T01:00:00.000000Z	42
                            2024-01-01T02:00:00.000000Z	6
                            2024-01-01T03:00:00.000000Z	8
                            """
            );
            assertQueryRows(
                    "SELECT ts,sum(v)+1 total FROM lp_fill SAMPLE BY 1h FILL(42)",
                    """
                            ts	total
                            2024-01-01T00:00:00.000000Z	5.0
                            2024-01-01T01:00:00.000000Z	43.0
                            2024-01-01T02:00:00.000000Z	6.0
                            2024-01-01T03:00:00.000000Z	8.0
                            """
            );
        });
    }

    @Test
    public void testCrossColumnPrevAndSelfPrev() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_fill", "TIMESTAMP");
            assertQueryRows(
                    "SELECT ts,sum(v) a,max(v) b FROM lp_fill SAMPLE BY 1h FILL(PREV(b),PREV)",
                    """
                            ts	a	b
                            2024-01-01T00:00:00.000000Z	4.0	3.0
                            2024-01-01T01:00:00.000000Z	3.0	3.0
                            2024-01-01T02:00:00.000000Z	5.0	5.0
                            2024-01-01T03:00:00.000000Z	7.0	7.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,sum(v) a,max(v) b FROM lp_fill SAMPLE BY 1h FILL(PREV,PREV(a))",
                    """
                            ts	a	b
                            2024-01-01T00:00:00.000000Z	4.0	3.0
                            2024-01-01T01:00:00.000000Z	4.0	4.0
                            2024-01-01T02:00:00.000000Z	5.0	5.0
                            2024-01-01T03:00:00.000000Z	7.0	7.0
                            """
            );
            assertQueryRows("SELECT ts,sum(v) a FROM lp_fill SAMPLE BY 1h FILL(PREV(a))", """
                    ts	a
                    2024-01-01T00:00:00.000000Z	4.0
                    2024-01-01T01:00:00.000000Z	4.0
                    2024-01-01T02:00:00.000000Z	5.0
                    2024-01-01T03:00:00.000000Z	7.0
                    """);
            assertQueryRows(
                    "SELECT ts,id%2 parity,max(id) a FROM lp_fill SAMPLE BY 1h FILL(PREV(parity)) ORDER BY ts,parity",
                    """
                            ts	parity	a
                            2024-01-01T00:00:00.000000Z	0	2
                            2024-01-01T00:00:00.000000Z	1	1
                            2024-01-01T01:00:00.000000Z	0	0
                            2024-01-01T01:00:00.000000Z	1	1
                            2024-01-01T02:00:00.000000Z	0	0
                            2024-01-01T02:00:00.000000Z	1	3
                            2024-01-01T03:00:00.000000Z	0	4
                            2024-01-01T03:00:00.000000Z	1	1
                            """
            );
            assertQueryRows(
                    "SELECT ts,sum(v) \"a.b\",max(v) b FROM lp_fill SAMPLE BY 1h FILL(PREV,PREV(\"a.b\"))",
                    """
                            ts	a.b	b
                            2024-01-01T00:00:00.000000Z	4.0	3.0
                            2024-01-01T01:00:00.000000Z	4.0	4.0
                            2024-01-01T02:00:00.000000Z	5.0	5.0
                            2024-01-01T03:00:00.000000Z	7.0	7.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,sum(v) \"a\"\"b\",max(v) b FROM lp_fill SAMPLE BY 1h FILL(PREV,PREV(\"a\"\"b\"))",
                    """
                            ts	a""b	b
                            2024-01-01T00:00:00.000000Z	4.0	3.0
                            2024-01-01T01:00:00.000000Z	4.0	4.0
                            2024-01-01T02:00:00.000000Z	5.0	5.0
                            2024-01-01T03:00:00.000000Z	7.0	7.0
                            """
            );
        });
    }

    @Test
    public void testLiteralAndExpressionFillTypes() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_fill", "TIMESTAMP");
            assertQueryRows("SELECT ts,sum(v) a FROM lp_fill SAMPLE BY 1h FILL(40+2)", """
                    ts	a
                    2024-01-01T00:00:00.000000Z	4.0
                    2024-01-01T01:00:00.000000Z	42.0
                    2024-01-01T02:00:00.000000Z	5.0
                    2024-01-01T03:00:00.000000Z	7.0
                    """);
            assertQueryRows(
                    "SELECT ts,sum(v) a,max(v) b FROM lp_fill SAMPLE BY 1h FILL(40+2,43::DOUBLE)",
                    """
                            ts	a	b
                            2024-01-01T00:00:00.000000Z	4.0	3.0
                            2024-01-01T01:00:00.000000Z	42.0	43.0
                            2024-01-01T02:00:00.000000Z	5.0	5.0
                            2024-01-01T03:00:00.000000Z	7.0	7.0
                            """
            );
            {
                final String type = "TIMESTAMP";
                final String table = "lp_fill_ts_" + type;
                createRows(table, type);
                assertQueryRows(
                        "SELECT ts,first(ts) a,last(ts) b FROM " + table
                                + " SAMPLE BY 1h FILL('2020-01-02T03:04:05.123456789Z')",
                        """
                                ts	a	b
                                2024-01-01T00:00:00.000000Z	2024-01-01T00:15:00.000000Z	2024-01-01T00:35:00.000000Z
                                2024-01-01T01:00:00.000000Z	2020-01-02T03:04:05.123456Z	2020-01-02T03:04:05.123456Z
                                2024-01-01T02:00:00.000000Z	2024-01-01T02:15:00.000000Z	2024-01-01T02:15:00.000000Z
                                2024-01-01T03:00:00.000000Z	2024-01-01T03:35:00.000000Z	2024-01-01T03:35:00.000000Z
                                """
                );
            }
            {
                final String type = "TIMESTAMP_NS";
                final String table = "lp_fill_ts_" + type;
                createRows(table, type);
                assertQueryRows(
                        "SELECT ts,first(ts) a,last(ts) b FROM " + table
                                + " SAMPLE BY 1h FILL('2020-01-02T03:04:05.123456789Z')",
                        """
                                ts	a	b
                                2024-01-01T00:00:00.000000000Z	2024-01-01T00:15:00.000000000Z	2024-01-01T00:35:00.000000000Z
                                2024-01-01T01:00:00.000000000Z	2020-01-02T03:04:05.123456789Z	2020-01-02T03:04:05.123456789Z
                                2024-01-01T02:00:00.000000000Z	2024-01-01T02:15:00.000000000Z	2024-01-01T02:15:00.000000000Z
                                2024-01-01T03:00:00.000000000Z	2024-01-01T03:35:00.000000000Z	2024-01-01T03:35:00.000000000Z
                                """
                );
            }
        });
    }

    @Test
    public void testInvalidFillValueFailsAtCompileTimeAndCompilerRecovers() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_fill", "TIMESTAMP");
            final String sql = "SELECT ts,sum(v) a FROM lp_fill SAMPLE BY 1h FILL('bad')";
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int reuse = 0; reuse < 2; reuse++) {
                    try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail(sql);
                    } catch (SqlException e) {
                        Assert.assertEquals(sql.indexOf("'bad'"), e.getPosition());
                        TestUtils.assertEquals("invalid fill value: 'bad'", e.getFlyweightMessage());
                    }
                    try (RecordCursorFactory factory = compiler.compile("SELECT count() FROM lp_fill", sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(factory, "count\n4\n");
                    }
                }
            }
        });
    }

    @Test
    public void testBoundsTimezoneOffsetAndEmptyInput() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_fill", "TIMESTAMP");
            assertQueryRows(
                    "SELECT ts,sum(v) FROM lp_fill SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T06:00:00' FILL(0)",
                    """
                            ts	sum
                            2024-01-01T00:00:00.000000Z	4.0
                            2024-01-01T01:00:00.000000Z	0.0
                            2024-01-01T02:00:00.000000Z	5.0
                            2024-01-01T03:00:00.000000Z	7.0
                            2024-01-01T04:00:00.000000Z	0.0
                            2024-01-01T05:00:00.000000Z	0.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,sum(v) FROM lp_fill WHERE id<0 SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T04:00:00' FILL(0)",
                    """
                            ts	sum
                            2024-01-01T00:00:00.000000Z	0.0
                            2024-01-01T01:00:00.000000Z	0.0
                            2024-01-01T02:00:00.000000Z	0.0
                            2024-01-01T03:00:00.000000Z	0.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,sum(v) FROM lp_fill SAMPLE BY 1h FROM '2024-01-01T06:00:00' TO '2024-01-01T10:00:00' FILL(PREV)"
                            + " ALIGN TO CALENDAR TIME ZONE 'Asia/Kathmandu' WITH OFFSET '00:10'",
                    """
                            ts	sum
                            2024-01-01T00:25:00.000000Z	4.0
                            2024-01-01T01:25:00.000000Z	5.0
                            2024-01-01T02:25:00.000000Z	5.0
                            2024-01-01T03:25:00.000000Z	7.0
                            """
            );
            {
                final String type = "TIMESTAMP";
                final String table = "lp_fill_dst_" + type;
                execute("CREATE TABLE " + table + "(v DOUBLE,ts " + type + ") TIMESTAMP(ts) PARTITION BY DAY");
                execute("INSERT INTO " + table + " VALUES(1.0,'2024-03-30T12:00:00'),(4.0,'2024-04-02T12:00:00')");
                assertQueryRows(
                        "SELECT ts,sum(v) FROM " + table + " SAMPLE BY 1d FILL(PREV) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                        """
                                ts	sum
                                2024-03-29T23:00:00.000000Z	1.0
                                2024-03-30T23:00:00.000000Z	1.0
                                2024-03-31T22:00:00.000000Z	1.0
                                2024-04-01T22:00:00.000000Z	4.0
                                """
                );
            }
            {
                final String type = "TIMESTAMP_NS";
                final String table = "lp_fill_dst_" + type;
                execute("CREATE TABLE " + table + "(v DOUBLE,ts " + type + ") TIMESTAMP(ts) PARTITION BY DAY");
                execute("INSERT INTO " + table + " VALUES(1.0,'2024-03-30T12:00:00'),(4.0,'2024-04-02T12:00:00')");
                assertQueryRows(
                        "SELECT ts,sum(v) FROM " + table + " SAMPLE BY 1d FILL(PREV) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                        """
                                ts	sum
                                2024-03-29T23:00:00.000000000Z	1.0
                                2024-03-30T23:00:00.000000000Z	1.0
                                2024-03-31T22:00:00.000000000Z	1.0
                                2024-04-01T22:00:00.000000000Z	4.0
                                """
                );
            }
        });
    }

    @Test
    public void testInvalidFillDiagnosticsAndCompilerRecovery() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_fill", "TIMESTAMP");
            bindVariableService.setDouble(0, 42.0);
            {
                final String sql = "SELECT ts,sum(v) FROM lp_fill SAMPLE BY 1h FILL(NONE,1)";
                assertQuery(sql).noLeakCheck().fails(48, "FILL(NONE) cannot be combined with other fill values");
            }
            {
                final String sql = "SELECT ts,sum(v),max(v) FROM lp_fill SAMPLE BY 1h FILL(40+2)";
                assertQuery(sql).noLeakCheck().fails(57, "not enough fill values");
            }
            {
                final String sql = "SELECT ts,sum(v),max(v),count() FROM lp_fill SAMPLE BY 1h FILL(1,2)";
                assertQuery(sql).noLeakCheck().fails(63, "not enough fill values");
            }
            {
                final String sql = "SELECT ts,sum(v) FROM lp_fill SAMPLE BY 1h FILL(PREV(1))";
                assertQuery(sql).noLeakCheck().fails(48, "PREV argument must be a single column name");
            }
            {
                final String sql = "SELECT ts,sum(v),max(v) b FROM lp_fill SAMPLE BY 1h FILL(PREV(b))";
                assertQuery(sql).noLeakCheck().fails(57, "FILL(PREV(b)) cannot be broadcast across aggregates; specify one fill value per aggregate");
            }
            {
                final String sql = "SELECT ts,sum(v) FROM lp_fill SAMPLE BY 1h FILL(PREV(missing))";
                assertQuery(sql).noLeakCheck().fails(53, "PREV(col): column not found in output: missing");
            }
            {
                final String sql = "SELECT ts,sum(v) FROM lp_fill SAMPLE BY 1h FILL(PREV(ts))";
                assertQuery(sql).noLeakCheck().fails(53, "PREV cannot reference the designated timestamp column");
            }
            {
                final String sql = "SELECT ts,sum(v) a,count() b FROM lp_fill SAMPLE BY 1h FILL(PREV(b),PREV)";
                assertQuery(sql).noLeakCheck().fails(65, "FILL(PREV(b)): source type LONG cannot fill target column of type DOUBLE");
            }
            {
                final String sql = "SELECT ts,sum(v) a,max(v) b FROM lp_fill SAMPLE BY 1h FILL(PREV(b),0)";
                assertQuery(sql).noLeakCheck().fails(59, "FILL(PREV) cannot reference a column that is itself filled with a constant");
            }
            {
                final String sql = "SELECT ts,sum(v) a,max(v) b FROM lp_fill SAMPLE BY 1h FILL(PREV(b),PREV(a))";
                assertQuery(sql).noLeakCheck().fails(59, "FILL(PREV) chains are not supported: source column is itself a cross-column PREV");
            }
            {
                final String sql = "SELECT ts,k,last(k) a FROM lp_fill SAMPLE BY 1h FILL(PREV(k))";
                assertQuery(sql).noLeakCheck().fails(58, "FILL(PREV(k)) is not supported on SYMBOL columns; use bare FILL(PREV) instead");
            }
            {
                final String sql = "SELECT ts,sum(v) FROM lp_fill SAMPLE BY 1h FILL($1)";
                assertQuery(sql).noLeakCheck().fails(48, "fill value must be a constant expression");
            }
            {
                final String sql = "SELECT ts,first(ts) FROM lp_fill SAMPLE BY 1h FILL(42)";
                assertQuery(sql).noLeakCheck().fails(51, "Invalid fill value: '42'. Timestamp fill value must be in quotes. Example: '2019-01-01T00:00:00.000Z'");
            }
        });
    }

    @Test
    public void testRuntimeToRetainedAcrossCompilerReuseAndReopens() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_fill", "TIMESTAMP");
            bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2024-01-01T04:00:00.000000Z"));
            final String sql = "SELECT ts,sum(v) total FROM lp_fill SAMPLE BY 1h TO $1 FILL(PREV)";
            RecordCursorFactory retained = null;
            try {
                final String plan;
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    plan = planOf(retained);
                    compiler.clear();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT ts,count() FROM lp_fill SAMPLE BY 1h FILL(NULL)", sqlExecutionContext).getRecordCursorFactory()) {
                        compiler.clear();
                    }
                }
                Assert.assertEquals(plan, planOf(retained));
                bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2024-01-01T04:00:00.000000Z"));
                assertResult(retained, """
                        ts	total
                        2024-01-01T00:00:00.000000Z	4.0
                        2024-01-01T01:00:00.000000Z	4.0
                        2024-01-01T02:00:00.000000Z	5.0
                        2024-01-01T03:00:00.000000Z	7.0
                        """);
                bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2024-01-01T05:00:00.000000Z"));
                assertResult(retained, """
                        ts	total
                        2024-01-01T00:00:00.000000Z	4.0
                        2024-01-01T01:00:00.000000Z	4.0
                        2024-01-01T02:00:00.000000Z	5.0
                        2024-01-01T03:00:00.000000Z	7.0
                        2024-01-01T04:00:00.000000Z	7.0
                        """);
                bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2024-01-01T06:00:00.000000Z"));
                assertResult(retained, """
                        ts	total
                        2024-01-01T00:00:00.000000Z	4.0
                        2024-01-01T01:00:00.000000Z	4.0
                        2024-01-01T02:00:00.000000Z	5.0
                        2024-01-01T03:00:00.000000Z	7.0
                        2024-01-01T04:00:00.000000Z	7.0
                        2024-01-01T05:00:00.000000Z	7.0
                        """);
            } finally {
                Misc.free(retained);
            }
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
