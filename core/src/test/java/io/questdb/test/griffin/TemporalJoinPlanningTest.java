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

import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TemporalJoinPlanningTest extends AbstractCairoTest {
    @Test
    public void testDerivedHiddenTimestampsPreserveVisibleScope() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertJoin(
                    "SELECT l.*,r.* FROM (SELECT id,k FROM lp_tc_m) l ASOF JOIN (SELECT id,k FROM lp_tc_s) r ON(k)",
                    false,
                    "AsOf Join Fast",
                    null,
                    """
                            id	k	id1	k1
                            1	1	11	1
                            2	1	11	1
                            3	2	12	2
                            4	3	null	null
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM (SELECT id,k FROM (SELECT id,k FROM lp_tc_m)) l LT JOIN (SELECT id,k FROM lp_tc_s) r ON(k)",
                    false,
                    "Lt Join Light",
                    null,
                    """
                            lid	rid
                            1	10
                            2	11
                            3	12
                            4	null
                            """
            );
            assertJoin(
                    "SELECT l.* FROM (SELECT k ts,id FROM lp_tc_m) l ASOF JOIN lp_tc_s r ON l.ts=r.k",
                    false,
                    "AsOf Join Fast",
                    null,
                    """
                            ts	id
                            1	1
                            1	2
                            2	3
                            3	4
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM (SELECT id,k FROM lp_tc_m LIMIT 3) l ASOF JOIN (SELECT id,k FROM lp_tc_s LIMIT 3) r ON(k)",
                    false,
                    "AsOf Join Light",
                    null,
                    """
                            lid	rid
                            1	11
                            2	11
                            3	12
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM (SELECT id,k FROM lp_tc_m WHERE id IN(1,2,4)) l ASOF JOIN (SELECT id,k FROM lp_tc_s WHERE id IN(10,11,13)) r ON(k)",
                    false,
                    "AsOf Join Fast",
                    null,
                    """
                            lid	rid
                            1	11
                            2	11
                            4	null
                            """
            );
            assertQuery("SELECT l.ts FROM (SELECT id,k FROM lp_tc_m) l ASOF JOIN lp_tc_s r ON(k)").noLeakCheck().fails(7, "Invalid column: l.ts");
            assertQuery("SELECT l.\"\" FROM (SELECT id,k FROM lp_tc_m) l ASOF JOIN lp_tc_s r ON(k)").noLeakCheck().fails(9, "'*' or column name expected");
            assertQuery("SELECT l.id FROM (SELECT id+1 id,k FROM lp_tc_m) l ASOF JOIN lp_tc_s r ON(k)").noLeakCheck().fails(51, "left side of time series join has no timestamp");
        });
    }

    @Test
    public void testEmptyInputsAndTemporalCardinality() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l ASOF JOIN (SELECT * FROM lp_tc_s WHERE false) r ON(k)",
                    false,
                    null,
                    null,
                    """
                            lid	rid
                            1	null
                            2	null
                            3	null
                            4	null
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_tc_m WHERE false) l LT JOIN lp_tc_s r ON(k)",
                    false,
                    null,
                    null,
                    "lid\trid\n"
            );
            assertJoin(
                    "SELECT count() FROM lp_tc_m l ASOF JOIN lp_tc_s r ON(k)",
                    false,
                    "Count",
                    null,
                    """
                            count
                            4
                            """
            );
            assertJoin(
                    "SELECT count() FROM lp_tc_m l LT JOIN lp_tc_s r",
                    true,
                    "Count",
                    null,
                    """
                            count
                            4
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l LT JOIN lp_tc_s r ON(k) ORDER BY lid DESC LIMIT 2",
                    false,
                    "Lt Join Light",
                    null,
                    """
                            lid	rid
                            4	null
                            3	12
                            """
            );
        });
    }

    @Test
    public void testFactoriesSurviveReuseFailureAndCompilerClose() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT l.id lid,r.id rid FROM lp_tc_m l ASOF JOIN "
                    + "(SELECT * FROM lp_tc_s WHERE id IN(10,11,13)) r ON(k) WHERE l.id IN(1,2,4)";
            RecordCursorFactory retained = null;
            RecordCursorFactory explain = null;
            final String expected = """
                    lid	rid
                    1	11
                    2	11
                    4	null
                    """;
            final String expectedPlan;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(factory, expected);
                }
                try {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    explain = compiler.compile("EXPLAIN " + sql, sqlExecutionContext).getRecordCursorFactory();
                    expectedPlan = print(explain);
                    try (RecordCursorFactory ignored = compiler.compile("SELECT l.id FROM lp_tc_m l ASOF JOIN "
                            + "(SELECT * FROM lp_tc_s WHERE id IN(10,11,13)) r ON(k) "
                            + "WHERE l.id IN(1,2) AND missing>0", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail("invalid column must fail after prepared IN predicates");
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column: missing");
                    }
                    try (RecordCursorFactory factory = compiler.compile("SELECT count() FROM lp_tc_m", sqlExecutionContext).getRecordCursorFactory()) {
                        TestUtils.assertEquals("count\n4\n", print(factory));
                    }
                    compiler.clear();
                } catch (Throwable th) {
                    Misc.free(retained, th);
                    Misc.free(explain, th);
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained; RecordCursorFactory plan = explain) {
                assertResult(factory, expected);
                assertResult(factory, expected);
                TestUtils.assertEquals(expectedPlan, print(plan));
            }
        });
    }

    @Test
    public void testKeyedAndUnkeyedNativeInputs() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l ASOF JOIN lp_tc_s r ON(k)",
                    false,
                    "AsOf Join Fast",
                    """
                            SelectedRecord
                                AsOf Join Fast
                                  condition: r.k=l.k
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_m
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid
                            1	11
                            2	11
                            3	12
                            4	null
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l LT JOIN lp_tc_s r ON(k)",
                    false,
                    "Lt Join Light",
                    """
                            SelectedRecord
                                Lt Join Light
                                  condition: r.k=l.k
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_m
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid
                            1	10
                            2	11
                            3	12
                            4	null
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l ASOF JOIN lp_tc_s r",
                    false,
                    "AsOf Join Fast",
                    """
                            SelectedRecord
                                AsOf Join Fast
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_m
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid
                            1	11
                            2	12
                            3	13
                            4	13
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l LT JOIN lp_tc_s r",
                    false,
                    "Lt Join Fast",
                    """
                            SelectedRecord
                                Lt Join Fast
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_m
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid
                            1	10
                            2	11
                            3	13
                            4	13
                            """
            );
            assertJoin(
                    "SELECT * FROM lp_tc_m l ASOF JOIN lp_tc_s r ON(k)",
                    true,
                    "AsOf Join",
                    null,
                    """
                            id	k	v	sym	ts	id1	k1	v1	sym1	ts1
                            1	1	5	A	2020-01-01T00:00:01.000000Z	11	1	20	A	2020-01-01T00:00:01.000000Z
                            2	1	6	A	2020-01-01T00:00:02.000000Z	11	1	20	A	2020-01-01T00:00:01.000000Z
                            3	2	7	B	2020-01-01T00:00:03.000000Z	12	2	30	B	2020-01-01T00:00:02.000000Z
                            4	3	8	C	2020-01-01T00:00:04.000000Z	null	null	null	\t
                            """
            );
            assertJoin(
                    "SELECT * FROM lp_tc_m l LT JOIN lp_tc_s r ON(k)",
                    true,
                    "Lt Join",
                    null,
                    """
                            id	k	v	sym	ts	id1	k1	v1	sym1	ts1
                            1	1	5	A	2020-01-01T00:00:01.000000Z	10	1	10	A	2020-01-01T00:00:00.500000Z
                            2	1	6	A	2020-01-01T00:00:02.000000Z	11	1	20	A	2020-01-01T00:00:01.000000Z
                            3	2	7	B	2020-01-01T00:00:03.000000Z	12	2	30	B	2020-01-01T00:00:02.000000Z
                            4	3	8	C	2020-01-01T00:00:04.000000Z	null	null	null	\t
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid,r.sym FROM lp_tc_m l ASOF JOIN lp_tc_s r ON(sym)",
                    false,
                    "AsOf Join Fast",
                    null,
                    """
                            lid	rid	sym
                            1	11	A
                            2	11	A
                            3	12	B
                            4	null\t
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid,r.sym FROM lp_tc_m l LT JOIN lp_tc_s r ON(sym)",
                    true,
                    "Lt Join",
                    null,
                    """
                            lid	rid	sym
                            1	10	A
                            2	11	A
                            3	12	B
                            4	null\t
                            """
            );
        });
    }

    @Test
    public void testSpliceDerivedTimestampsAndMixedPrecision() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertJoin(
                    "SELECT l.*,r.* FROM (SELECT id,k FROM lp_tc_m) l SPLICE JOIN (SELECT id,k FROM lp_tc_s) r ON(k)",
                    false,
                    "Splice Join",
                    null,
                    """
                            id	k	id1	k1
                            null	null	10	1
                            1	1	11	1
                            2	1	10	1
                            1	1	13	1
                            3	2	12	2
                            4	3	null	null
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM (SELECT id,k FROM lp_tc_m LIMIT 3) l SPLICE JOIN (SELECT id,k FROM lp_tc_s LIMIT 3) r ON(k)",
                    false,
                    "Splice Join",
                    null,
                    """
                            lid	rid
                            null	10
                            1	11
                            2	10
                            3	12
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM (SELECT id,k,ts mt FROM lp_tc_m) l TIMESTAMP(mt) SPLICE JOIN (SELECT id,k,ts rt FROM lp_tc_s) r ON(k)",
                    false,
                    "Splice Join",
                    null,
                    """
                            lid	rid
                            null	10
                            1	11
                            2	10
                            1	13
                            3	12
                            4	null
                            """
            );
            execute("CREATE TABLE lp_tc_ns(id INT,k INT,ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO lp_tc_ns VALUES(10,1,'2020-01-01T00:00:00.999999999Z'),(11,1,'2020-01-01T00:00:02.000000001Z')");
            assertJoin(
                    "SELECT l.id lid,r.id rid,l.ts,r.ts FROM lp_tc_m l SPLICE JOIN lp_tc_ns r",
                    false,
                    "Splice Join",
                    """
                            SelectedRecord
                                Splice Join
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_m
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_ns
                            """,
                    """
                            lid	rid	ts	ts1
                            null	10		2020-01-01T00:00:00.999999999Z
                            1	10	2020-01-01T00:00:01.000000Z	2020-01-01T00:00:00.999999999Z
                            2	10	2020-01-01T00:00:02.000000Z	2020-01-01T00:00:00.999999999Z
                            2	11	2020-01-01T00:00:02.000000Z	2020-01-01T00:00:02.000000001Z
                            3	11	2020-01-01T00:00:03.000000Z	2020-01-01T00:00:02.000000001Z
                            4	11	2020-01-01T00:00:04.000000Z	2020-01-01T00:00:02.000000001Z
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN lp_tc_ns r ON(k)",
                    false,
                    "Splice Join",
                    """
                            SelectedRecord
                                Splice Join
                                  condition: r.k=l.k
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_m
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_ns
                            """,
                    """
                            lid	rid
                            null	10
                            1	10
                            2	10
                            1	11
                            3	null
                            4	null
                            """
            );
            assertQuery("SELECT l.ts FROM (SELECT id,k FROM lp_tc_m) l SPLICE JOIN lp_tc_s r ON(k)").noLeakCheck().fails(7, "Invalid column: l.ts");
        });
    }

    @Test
    public void testSpliceEmptyInputsAndCardinality() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN (SELECT * FROM lp_tc_s WHERE false) r ON(k)",
                    false,
                    "Splice Join",
                    null,
                    """
                            lid	rid
                            1	null
                            2	null
                            3	null
                            4	null
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_tc_m WHERE false) l SPLICE JOIN lp_tc_s r ON(k)",
                    false,
                    "Splice Join",
                    null,
                    """
                            lid	rid
                            null	10
                            null	11
                            null	12
                            null	13
                            """
            );
            assertJoin(
                    "SELECT count() FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON(k)",
                    false,
                    "Count",
                    null,
                    """
                            count
                            6
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON(k) ORDER BY lid,rid LIMIT 2",
                    false,
                    "Splice Join",
                    null,
                    """
                            lid	rid
                            null	10
                            1	11
                            """
            );
        });
    }

    @Test
    public void testSpliceFactorySurvivesCompilerFailureAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON(k) "
                    + "WHERE l.id IN(1,2,4) OR r.id IN(10,11,13)";
            RecordCursorFactory retained = null;
            final String expected = """
                    lid	rid
                    null	10
                    1	11
                    2	10
                    1	13
                    4	null
                    """;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(factory, expected);
                }
                try {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT l.id FROM "
                                    + "(SELECT * FROM lp_tc_m WHERE id IN(1,2)) l SPLICE JOIN lp_tc_s r ON l.v<r.v",
                            sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail("SPLICE residual must fail after input generation");
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "unsupported SPLICE join expression");
                    }
                    try (RecordCursorFactory factory = compiler.compile("SELECT count() FROM lp_tc_m", sqlExecutionContext).getRecordCursorFactory()) {
                        TestUtils.assertEquals("count\n4\n", print(factory));
                    }
                    compiler.clear();
                } catch (Throwable th) {
                    Misc.free(retained, th);
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained) {
                assertResult(factory, expected);
            }
        });
    }

    @Test
    public void testSpliceKeysAndTimestampEquality() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON(k)",
                    false,
                    "Splice Join",
                    """
                            SelectedRecord
                                Splice Join
                                  condition: r.k=l.k
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_m
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid
                            null	10
                            1	11
                            2	10
                            1	13
                            3	12
                            4	null
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN lp_tc_s r",
                    false,
                    "Splice Join",
                    """
                            SelectedRecord
                                Splice Join
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_m
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid
                            null	10
                            1	11
                            2	12
                            2	13
                            3	13
                            4	13
                            """
            );
            assertJoin(
                    "SELECT * FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON(ts)",
                    false,
                    "Splice Join",
                    """
                            SelectedRecord
                                Splice Join
                                  condition: r.ts=l.ts
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_m
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_s
                            """,
                    """
                            id	k	v	sym	ts	id1	k1	v1	sym1	ts1
                            null	null	null			10	1	10	A	2020-01-01T00:00:00.500000Z
                            1	1	5	A	2020-01-01T00:00:01.000000Z	11	1	20	A	2020-01-01T00:00:01.000000Z
                            2	1	6	A	2020-01-01T00:00:02.000000Z	12	2	30	B	2020-01-01T00:00:02.000000Z
                            null	null	null			13	1	40	A	2020-01-01T00:00:02.500000Z
                            3	2	7	B	2020-01-01T00:00:03.000000Z	null	null	null	\t
                            4	3	8	C	2020-01-01T00:00:04.000000Z	null	null	null	\t
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON(k,ts)",
                    false,
                    "Splice Join",
                    """
                            SelectedRecord
                                Splice Join
                                  condition: r.ts=l.ts and r.k=l.k
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_m
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid
                            null	10
                            1	11
                            2	null
                            null	13
                            3	null
                            4	null
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON l.k=r.k AND l.ts=r.ts",
                    false,
                    "Splice Join",
                    """
                            SelectedRecord
                                Splice Join
                                  condition: r.k=l.k and r.ts=l.ts
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_m
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid
                            null	10
                            1	11
                            2	null
                            null	13
                            3	null
                            4	null
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON l.k=r.k AND l.ts=r.ts AND r.k=l.k",
                    false,
                    "Splice Join",
                    """
                            SelectedRecord
                                Splice Join
                                  condition: r.k=l.k and r.ts=l.ts
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_m
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid
                            null	10
                            1	11
                            2	null
                            null	13
                            3	null
                            4	null
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid,l.sym,r.sym FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON(sym)",
                    false,
                    "Splice Join",
                    """
                            SelectedRecord
                                Splice Join
                                  condition: r.sym=l.sym
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_m
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid	sym	sym1
                            null	10		A
                            1	11	A	A
                            2	10	A	A
                            1	13	A	A
                            3	12	B	B
                            4	null	C\t
                            """
            );
        });
    }

    @Test
    public void testSpliceResidualOnDiagnostics() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String prefix = "SELECT l.id FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON ";
            assertQuery(prefix + "l.missing < r.id").noLeakCheck().fails(52, "Invalid column: missing");
            assertQuery(prefix + "l.missing = r.id+1").noLeakCheck().fails(52, "Invalid column: l.missing");
            assertQuery(prefix + "l.missing ~ r.sym").noLeakCheck().fails(52, "Invalid column: l.missing");
            assertQuery(prefix + "abs(l.missing) < r.id").noLeakCheck().fails(56, "Invalid column: missing");
            assertQuery(prefix + "r.missing_r < l.missing_l").noLeakCheck().fails(66, "Invalid column: missing_l");
            assertQuery(prefix + "l.missing_l < r.id AND r.missing_r = l.id+1").noLeakCheck().fails(75, "Invalid column: r.missing_r");
            assertQuery(prefix + "id = r.id+1").noLeakCheck().fails(52, "Ambiguous column [name=id]");
            assertQuery(prefix + "missing < r.id").noLeakCheck().fails(60, "unsupported SPLICE join expression [expr='missing < r.id']");
            assertQuery(prefix + "id < r.id").noLeakCheck().fails(55, "unsupported SPLICE join expression [expr='id < r.id']");
            assertQuery(prefix + "unknown.id < r.id").noLeakCheck().fails(63, "unsupported SPLICE join expression [expr='unknown.id < r.id']");
            assertQuery(prefix + "l.id < r.id AND l.id > r.id").noLeakCheck().fails(73, "unsupported SPLICE join expression [expr='l.id < r.id and l.id > r.id']");
            assertQuery(prefix + "l.id < r.id AND (l.id > r.id AND l.id != r.id)").noLeakCheck().fails(90, "unsupported SPLICE join expression [expr='l.id < r.id and l.id > r.id and l.id != r.id']");
            assertQuery(prefix + "l.id < r.id AND l.k=r.k AND l.id > r.id").noLeakCheck().fails(85, "unsupported SPLICE join expression [expr='l.id < r.id and l.id > r.id']");
        });
    }

    @Test
    public void testSpliceValidationOrderAndRecovery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE lp_tc_plain(id INT,k INT,v INT)");
            assertQuery("SELECT l.id FROM lp_tc_plain l SPLICE JOIN lp_tc_s r ON l.v<r.v").noLeakCheck().fails(31, "left side of time series join has no timestamp");
            assertQuery("SELECT l.id FROM lp_tc_m l SPLICE JOIN lp_tc_plain r ON l.v<r.v").noLeakCheck().fails(27, "right side of time series join has no timestamp");
            assertQuery("SELECT l.id FROM (SELECT * FROM lp_tc_m ORDER BY ts DESC) l SPLICE JOIN lp_tc_s r ON(k)").noLeakCheck().fails(60, "left side of time series join doesn't have ASC timestamp order");
            assertQuery("SELECT l.id FROM lp_tc_m l SPLICE JOIN (SELECT * FROM lp_tc_s ORDER BY ts DESC) r ON(k)").noLeakCheck().fails(27, "right side of time series join doesn't have ASC timestamp order");
            assertQuery("SELECT l.id FROM (SELECT * FROM lp_tc_m ORDER BY ts DESC) l SPLICE JOIN lp_tc_s r ON l.v<r.v").noLeakCheck().fails(88, "unsupported SPLICE join expression [expr='l.v < r.v']");
            assertQuery("SELECT l.id FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON l.v<r.v").noLeakCheck().fails(55, "unsupported SPLICE join expression [expr='l.v < r.v']");
            assertQuery("SELECT l.id FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON l.k=r.sym").noLeakCheck().fails(56, "join column type mismatch");
            assertQuery("SELECT l.id FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON(k)").noLeakCheck().fullFatJoins().fails(27, "splice join doesn't support full fat mode");
            assertQuery("SELECT l.id FROM (SELECT m.id,m.k,m.ts FROM lp_tc_m m ASOF JOIN lp_tc_s s ON(k)) l SPLICE JOIN lp_tc_s r ON(k)").noLeakCheck().fails(83, "left side of splice join doesn't support random access");
            assertQuery("SELECT l.id FROM lp_tc_m l SPLICE JOIN (SELECT m.id,m.k,m.ts FROM lp_tc_m m ASOF JOIN lp_tc_s s ON(k)) r ON(k)").noLeakCheck().fails(27, "right side of splice join doesn't support random access");
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON(k)",
                    false,
                    "Splice Join",
                    """
                            SelectedRecord
                                Splice Join
                                  condition: r.k=l.k
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_m
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid
                            null	10
                            1	11
                            2	10
                            1	13
                            3	12
                            4	null
                            """
            );
        });
    }

    @Test
    public void testSpliceWhereStaysAfterBothInputs() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON(k) WHERE l.id=1",
                    false,
                    "Splice Join",
                    """
                            SelectedRecord
                                Filter filter: l.id=1
                                    Splice Join
                                      condition: r.k=l.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_tc_m
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid
                            1	11
                            1	13
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON(k) WHERE r.id=10",
                    false,
                    "Splice Join",
                    """
                            SelectedRecord
                                Filter filter: r.id=10
                                    Splice Join
                                      condition: r.k=l.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_tc_m
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid
                            null	10
                            2	10
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON(k) WHERE l.id=null OR r.id=null",
                    false,
                    "Splice Join",
                    """
                            SelectedRecord
                                Filter filter: (l.id=null or r.id=null)
                                    Splice Join
                                      condition: r.k=l.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_tc_m
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid
                            null	10
                            4	null
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN lp_tc_s r WHERE l.k=r.k",
                    false,
                    "Splice Join",
                    """
                            SelectedRecord
                                Filter filter: l.k=r.k
                                    Splice Join
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_tc_m
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid
                            1	11
                            2	13
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON(k) WHERE l.ts>='2020-01-01T00:00:02Z'",
                    false,
                    "Splice Join",
                    """
                            SelectedRecord
                                Filter filter: l.ts>=2020-01-01T00:00:02.000000Z
                                    Splice Join
                                      condition: r.k=l.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_tc_m
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_tc_s
                            """,
                    """
                            lid	rid
                            2	10
                            3	12
                            4	null
                            """
            );
            assertJoin(
                    "SELECT * FROM (SELECT l.id lid,r.id rid FROM lp_tc_m l SPLICE JOIN lp_tc_s r ON(k)) WHERE lid=1",
                    false,
                    "Splice Join",
                    null,
                    """
                            lid	rid
                            1	11
                            1	13
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_tc_m WHERE id=1) l SPLICE JOIN lp_tc_s r ON(k)",
                    false,
                    "Splice Join",
                    null,
                    """
                            lid	rid
                            null	10
                            1	11
                            null	12
                            1	13
                            """
            );
        });
    }

    @Test
    public void testTimestampAliasesAndExplicitDesignation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM (SELECT id,k,ts mt FROM lp_tc_m) l TIMESTAMP(mt) ASOF JOIN "
                            + "(SELECT id,k,ts rt FROM lp_tc_s) r ON(k)",
                    false,
                    "AsOf Join Fast",
                    null,
                    """
                            lid	rid
                            1	11
                            2	11
                            3	12
                            4	null
                            """
            );
            assertJoin(
                    "SELECT l.*,r.id rid FROM (SELECT id,k,ts first,ts second FROM lp_tc_m) l ASOF JOIN lp_tc_s r ON(k)",
                    false,
                    "AsOf Join Fast",
                    null,
                    """
                            id	k	first	second	rid
                            1	1	2020-01-01T00:00:01.000000Z	2020-01-01T00:00:01.000000Z	11
                            2	1	2020-01-01T00:00:02.000000Z	2020-01-01T00:00:02.000000Z	11
                            3	2	2020-01-01T00:00:03.000000Z	2020-01-01T00:00:03.000000Z	12
                            4	3	2020-01-01T00:00:04.000000Z	2020-01-01T00:00:04.000000Z	null
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l ASOF JOIN lp_tc_s r ON(ts)",
                    false,
                    "AsOf Join Fast",
                    null,
                    """
                            lid	rid
                            1	11
                            2	12
                            3	13
                            4	13
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l LT JOIN lp_tc_s r ON(ts)",
                    false,
                    "Lt Join Fast",
                    null,
                    """
                            lid	rid
                            1	10
                            2	11
                            3	13
                            4	13
                            """
            );
        });
    }

    @Test
    public void testToleranceAndMixedTimestampPrecision() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l ASOF JOIN lp_tc_s r ON(k) TOLERANCE 250000U",
                    false,
                    "AsOf Join Fast",
                    null,
                    """
                            lid	rid
                            1	11
                            2	null
                            3	null
                            4	null
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l LT JOIN lp_tc_s r ON(k) TOLERANCE 1s",
                    true,
                    "Lt Join",
                    null,
                    """
                            lid	rid
                            1	10
                            2	11
                            3	12
                            4	null
                            """
            );
            execute("CREATE TABLE lp_tc_ns(id INT,k INT,ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO lp_tc_ns VALUES(10,1,'2020-01-01T00:00:00.999999999Z'),(11,1,'2020-01-01T00:00:02.000000001Z')");
            assertJoin(
                    "SELECT l.id lid,r.id rid,r.ts FROM lp_tc_m l ASOF JOIN lp_tc_ns r ON(k) TOLERANCE 1n",
                    false,
                    "AsOf Join Fast",
                    null,
                    """
                            lid	rid	ts
                            1	10	2020-01-01T00:00:00.999999999Z
                            2	null\t
                            3	null\t
                            4	null\t
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid,r.ts FROM lp_tc_m l LT JOIN lp_tc_ns r ON(k) TOLERANCE 1n",
                    true,
                    "Lt Join",
                    null,
                    """
                            lid	rid	ts
                            1	10	2020-01-01T00:00:00.999999999Z
                            2	null\t
                            3	null\t
                            4	null\t
                            """
            );
        });
    }

    @Test
    public void testValidationOrderingAndCompilerRecovery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE lp_tc_plain(id INT,k INT)");
            assertQuery("SELECT l.id FROM lp_tc_plain l ASOF JOIN lp_tc_s r ON(k) TOLERANCE 1y").noLeakCheck().fails(31, "left side of time series join has no timestamp");
            assertQuery("SELECT l.id FROM lp_tc_m l LT JOIN lp_tc_plain r ON(k) TOLERANCE 1y").noLeakCheck().fails(27, "right side of time series join has no timestamp");
            assertQuery("SELECT l.id FROM lp_tc_m l ASOF JOIN lp_tc_s r ON l.v<r.v TOLERANCE 1y").noLeakCheck().fails(53, "unsupported ASOF join expression [expr='l.v < r.v']");
            assertQuery("SELECT l.id FROM (SELECT * FROM lp_tc_m ORDER BY ts DESC) l ASOF JOIN lp_tc_s r ON(k) TOLERANCE 1s").noLeakCheck().fails(60, "left side of time series join doesn't have ASC timestamp order");
            assertQuery("SELECT l.id FROM lp_tc_m l LT JOIN (SELECT * FROM lp_tc_s ORDER BY ts DESC) r ON(k) TOLERANCE 1s").noLeakCheck().fails(27, "right side of time series join doesn't have ASC timestamp order");
            assertQuery("SELECT l.id FROM (SELECT * FROM lp_tc_m ORDER BY ts DESC) l ASOF JOIN lp_tc_s r ON(k) TOLERANCE 1y").noLeakCheck().fails(96, "unsupported TOLERANCE unit [unit=y]");
            assertQuery("SELECT l.id FROM lp_tc_m l ASOF JOIN lp_tc_s r ON l.k=r.ts TOLERANCE 1y").noLeakCheck().fails(54, "join column type mismatch");
            assertQuery("SELECT l.id FROM lp_tc_m l ASOF JOIN lp_tc_s r ON(k,ts) TOLERANCE 1y").noLeakCheck().fails(27, "ASOF/LT JOIN cannot use designated timestamp as a join key");
            assertQuery("SELECT l.id FROM lp_tc_m l ASOF JOIN lp_tc_s r ON(k) TOLERANCE 1y").noLeakCheck().fails(63, "unsupported TOLERANCE unit [unit=y]");
            assertQuery("SELECT l.id FROM lp_tc_m l LT JOIN lp_tc_s r ON(k) TOLERANCE 0s").noLeakCheck().fails(61, "zero is not a valid tolerance value");
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l ASOF JOIN lp_tc_s r ON(k)",
                    false,
                    "AsOf Join Fast",
                    null,
                    """
                            lid	rid
                            1	11
                            2	11
                            3	12
                            4	null
                            """
            );
        });
    }

    @Test
    public void testWherePreservesSlaveMatchingAndMasterPushdown() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l ASOF JOIN lp_tc_s r ON(k) WHERE r.v=10",
                    false,
                    "AsOf Join Fast",
                    null,
                    "lid\trid\n"
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l ASOF JOIN lp_tc_s r ON(k) WHERE r.id=null",
                    false,
                    "AsOf Join Fast",
                    null,
                    """
                            lid	rid
                            4	null
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l ASOF JOIN lp_tc_s r ON(k) WHERE l.id IN(1,3) AND r.v>10",
                    false,
                    "AsOf Join Fast",
                    null,
                    """
                            lid	rid
                            1	11
                            3	12
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l LT JOIN lp_tc_s r ON(k) WHERE l.ts>='2020-01-01T00:00:02Z' AND (r.v>10 OR r.id=null)",
                    false,
                    "Lt Join Light",
                    null,
                    """
                            lid	rid
                            2	11
                            3	12
                            4	null
                            """
            );
            assertJoin(
                    "SELECT l.id lid,r.id rid FROM lp_tc_m l ASOF JOIN (SELECT * FROM lp_tc_s WHERE v=10) r ON(k)",
                    false,
                    "AsOf Join Fast",
                    null,
                    """
                            lid	rid
                            1	10
                            2	10
                            3	null
                            4	null
                            """
            );
        });
    }

    private void assertJoin(String sql, boolean isFullFat, String algorithm, String expectedPlan, String expected) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            compiler.setFullFatJoins(isFullFat);
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                if (algorithm != null) {
                    TestUtils.assertContains(plan(factory), algorithm);
                }
                if (expectedPlan != null) {
                    TestUtils.assertEquals(expectedPlan, plan(factory));
                }
                assertResult(factory, expected);
            }
        }
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE lp_tc_m(id INT,k INT,v INT,sym SYMBOL,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE lp_tc_s(id INT,k INT,v INT,sym SYMBOL,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO lp_tc_m VALUES(1,1,5,'A','2020-01-01T00:00:01Z'),(2,1,6,'A','2020-01-01T00:00:02Z'),"
                + "(3,2,7,'B','2020-01-01T00:00:03Z'),(4,3,8,'C','2020-01-01T00:00:04Z')");
        execute("INSERT INTO lp_tc_s VALUES(10,1,10,'A','2020-01-01T00:00:00.500000Z'),(11,1,20,'A','2020-01-01T00:00:01Z'),"
                + "(12,2,30,'B','2020-01-01T00:00:02Z'),(13,1,40,'A','2020-01-01T00:00:02.500000Z')");
    }

    private String plan(RecordCursorFactory factory) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, sqlExecutionContext);
        final StringSink text = new StringSink();
        for (int i = 1, n = sink.getLineCount(); i <= n; i++) {
            text.put(sink.getLine(i)).put('\n');
        }
        return text.toString();
    }

    private String print(RecordCursorFactory factory) throws Exception {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
        }
        return sink.toString();
    }
}
