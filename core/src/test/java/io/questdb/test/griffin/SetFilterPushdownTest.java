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

import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ComparisonOperator;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.PlanExpressionVisitor;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.fuzz.clauses.SetOperationClause;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.LongPredicate;

public class SetFilterPushdownTest extends AbstractCairoTest {
    // 2024-01-01T00:00:00Z in nanoseconds
    private static final long BASE_NANOS = 1_704_067_200_000_000_000L;

    @Test
    public void testAliasesThreeBranchesAndConjunctResidualKeepTheirScopes() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String source = "(SELECT id,ta time FROM lp_sp_a UNION ALL SELECT id,tb other FROM lp_sp_b"
                    + " UNION ALL SELECT id,ta changed FROM lp_sp_a)";
            assertRowsAndIntervalScans("SELECT id FROM " + source + " WHERE time>='2024-01-03T00:00:00.000000Z' AND id<5 ORDER BY id",
                    "id\n3\n3\n3\n3\n4\n4\n", 3);
            assertRowsAndIntervalScans("SELECT id FROM " + source + " WHERE time='2024-01-02T00:00:00.000000Z'"
                            + " OR time='2024-01-04T00:00:00.000000Z' ORDER BY id",
                    "id\n2\n2\n2\n2\n2\n5\n", 3);
            assertLogicalResidual("SELECT id FROM " + source + " WHERE id>3 ORDER BY id",
                    "id\n4\n4\n5\n", 1);
        });
    }

    @Test
    public void testAllSetOperationsKeepTupleAndDuplicateSemantics() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            // Balance the overlapping tuple; unmatched duplicates must survive EXCEPT ALL.
            execute("INSERT INTO lp_sp_b VALUES ('2024-01-02',2)");
            final ObjList<String> operations = new ObjList<>("UNION ALL", "UNION", "EXCEPT", "EXCEPT ALL", "INTERSECT", "INTERSECT ALL");
            final ObjList<String> expected = new ObjList<>(
                    "id\n2\n2\n2\n2\n3\n3\n4\n4\n5\n", "id\n2\n3\n4\n5\n",
                    "id\n3\n", "id\n3\n3\n", "id\n2\n", "id\n2\n2\n"
            );
            for (int i = 0, n = operations.size(); i < n; i++) {
                final String sql = "SELECT id FROM (SELECT ta time,id FROM lp_sp_a " + operations.getQuick(i)
                        + " SELECT tb other,id FROM lp_sp_b) WHERE time>='2024-01-02T00:00:00.000000Z' ORDER BY id";
                assertRowsAndIntervalScans(sql, expected.getQuick(i), 2);
            }
        });
    }

    @Test
    public void testJoinConjunctsBoundPerBranchLeaveNoFilterAboveTheJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE b (k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO a SELECT x::INT % 4, timestamp_sequence('2024-01-01', 1_800_000_000L) FROM long_sequence(150)");
            execute("INSERT INTO b SELECT x::INT % 4, timestamp_sequence('2024-01-01', 1_800_000_000L) FROM long_sequence(150)");
            final String set = "(SELECT ts v, k id FROM a UNION ALL SELECT ts::TIMESTAMP_NS v1, k id1 FROM b";
            final String filter = "u.v > '2024-01-02T05:00:00.000000Z'";
            final String plan = """
                    SelectedRecord
                        Hash Join Light
                          condition: x.k=u.id
                            Union All
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: a
                                          intervals: [("2024-01-02T05:00:00.000001Z","MAX")]
                                Filter filter: 2024-01-02T05:00:00.000000000Z<v1
                                    VirtualRecord
                                      functions: [ts::timestamp_ns,k]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            Hash
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: b
                    """;
            for (String shape : new String[]{" u JOIN b x ON u.id = x.k AND %s", " u JOIN b x ON u.id = x.k WHERE %s"}) {
                final String query = "SELECT v, id FROM " + set + ")" + String.format(shape, filter);
                assertQuery(query).noLeakCheck().assertsPlan(plan);
                assertSqlCursors("SELECT v, id FROM " + set + " LIMIT 1_000_000)" + String.format(shape, filter), query);
            }
        });
    }

    @Test
    public void testLimitAndLatestBranchesFilterAboveTheirBarriers() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_events (ts TIMESTAMP,id INT,k SYMBOL) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_events VALUES ('2024-01-01T00:00:00',1,'A'),('2024-01-01T01:00:00',2,'A'),('2024-01-01T02:00:00',3,'A')");
            execute("CREATE TABLE lp_sp_plain (ts TIMESTAMP,id INT,k SYMBOL) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_plain VALUES ('2024-01-01T01:00:00',30,'B')");
            final String limited = "SELECT id FROM (SELECT * FROM (SELECT ts,id FROM lp_sp_events LIMIT 1)"
                    + " UNION ALL SELECT ts,id FROM lp_sp_plain) WHERE hour(ts)>0 ORDER BY id";
            assertRowsAndIntervalScans(limited, "id\n30\n", 0);
            assertLogicalResidual(limited, "id\n30\n", 0);
            final String latest = " FROM (SELECT ts,id FROM lp_sp_events LATEST ON ts PARTITION BY k"
                    + " UNION ALL SELECT ts,id FROM lp_sp_plain) WHERE hour(ts)<2 ORDER BY id";
            assertRowsAndIntervalScans("SELECT ts,id" + latest, "ts\tid\n2024-01-01T01:00:00.000000Z\t30\n", 0);
            assertLogicalResidual("SELECT id" + latest, "id\n30\n", 0);
            final String setLimit = "SELECT id FROM (SELECT ts,id FROM lp_sp_events UNION ALL SELECT ts,id FROM lp_sp_plain LIMIT 1)"
                    + " WHERE hour(ts)>0 ORDER BY id";
            assertRowsAndIntervalScans(setLimit, "id\n", 0);
        });
    }

    @Test
    public void testMixedPrecisionBoundsReachIntervalScansAtBranchPrecision() throws Exception {
        assertMemoryLeak(() -> {
            createBoundaryRows();
            final String set = "SELECT * FROM (SELECT ts,id FROM lp_sp_micro UNION ALL SELECT ts,id FROM lp_sp_nano) WHERE ";
            // ts_us * 1000 > 1001 holds from 2us on; ts_ns > 1001 from 1002ns on
            assertQuery(set + "ts > " + nanos(1_001))
                    .noLeakCheck()
                    .assertsPlan("""
                            Union All
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: lp_sp_micro
                                      intervals: [("2024-01-01T00:00:00.000002Z","MAX")]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: lp_sp_nano
                                      intervals: [("2024-01-01T00:00:00.000001002Z","MAX")]
                            """);
            // ts_us * 1000 >= 1001 holds from 2us on, the lower bound rounds up
            assertQuery(set + "ts >= " + nanos(1_001))
                    .noLeakCheck()
                    .assertsPlan("""
                            Union All
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: lp_sp_micro
                                      intervals: [("2024-01-01T00:00:00.000002Z","MAX")]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: lp_sp_nano
                                      intervals: [("2024-01-01T00:00:00.000001001Z","MAX")]
                            """);
            // ts_us * 1000 < 999 and <= 999 hold up to 0us, the upper bound rounds down
            for (String operator : new String[]{"<", "<="}) {
                assertQuery(set + "ts " + operator + ' ' + nanos(999))
                        .noLeakCheck()
                        .assertsPlan("Union All\n"
                                + "    PageFrame\n"
                                + "        Row forward scan\n"
                                + "        Interval forward scan on: lp_sp_micro\n"
                                + "          intervals: [(\"MIN\",\"2024-01-01T00:00:00.000000Z\")]\n"
                                + "    PageFrame\n"
                                + "        Row forward scan\n"
                                + "        Interval forward scan on: lp_sp_nano\n"
                                + "          intervals: [(\"MIN\",\"2024-01-01T00:00:00.000000" + (operator.length() == 1 ? "998" : "999") + "Z\")]\n");
            }
            // BETWEEN keeps [ceil(999ns), floor(1001ns)] = [1us, 1us] in microseconds
            assertQuery(set + "ts BETWEEN " + nanos(999) + " AND " + nanos(1_001))
                    .noLeakCheck()
                    .assertsPlan("""
                            Union All
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: lp_sp_micro
                                      intervals: [("2024-01-01T00:00:00.000001Z","2024-01-01T00:00:00.000001Z")]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: lp_sp_nano
                                      intervals: [("2024-01-01T00:00:00.000000999Z","2024-01-01T00:00:00.000001001Z")]
                            """);
            // a microsecond column holds 1000ns, but not 999ns
            assertQuery(set + "ts IN (" + nanos(999) + ',' + nanos(1_000) + ')')
                    .noLeakCheck()
                    .assertsPlan("""
                            Union All
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: lp_sp_micro
                                      intervals: [("2024-01-01T00:00:00.000001Z","2024-01-01T00:00:00.000001Z")]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: lp_sp_nano
                                      intervals: [("2024-01-01T00:00:00.000000999Z","2024-01-01T00:00:00.000000999Z"),("2024-01-01T00:00:00.000001000Z","2024-01-01T00:00:00.000001000Z")]
                            """);
        });
    }

    @Test
    public void testMixedPrecisionBranchFiltersSurviveParameterRebind() throws Exception {
        assertMemoryLeak(() -> {
            createBoundaryRows();
            final String set = "(SELECT ts,id FROM lp_sp_micro UNION ALL SELECT ts,id FROM lp_sp_nano)";
            final String sql = "SELECT id FROM " + set + " WHERE ts > $1 ORDER BY id";
            for (int defined = 0; defined < 2; defined++) {
                bindVariableService.clear();
                if (defined == 1) {
                    bindVariableService.setTimestampNano(0, BASE_NANOS + 1_001);
                }
                // an undefined parameter takes the set's TIMESTAMP_NS type, and both branches scan the intervals it bounds
                try (
                        SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                        RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
                ) {
                    Assert.assertEquals(0, countSetResiduals(compiler.getPlanForTesting()));
                    Assert.assertEquals(2, countIntervalScans(factory, new TextPlanSink()));
                    bindVariableService.setTimestampNano(0, BASE_NANOS + 1_001);
                    assertRowsOnly(factory, "id\n3\n");
                    bindVariableService.setTimestampNano(0, BASE_NANOS + 999);
                    assertRowsOnly(factory, "id\n2\n3\n12\n13\n");
                    bindVariableService.setTimestampNano(0, BASE_NANOS - 1);
                    assertRowsOnly(factory, "id\n1\n2\n3\n11\n12\n13\n");
                }
                bindVariableService.setTimestampNano(0, BASE_NANOS + 1_000);
                assertMixedFilter(set, "ts > $1", "id\n3\n13\n", 0);
                assertRowsAndIntervalScans(sql, "id\n3\n13\n", 2);
            }
        });
    }

    @Test
    public void testMixedPrecisionComparisonsMatchTheSetAtMicrosecondBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            createBoundaryRows();
            final String set = "(SELECT ts,id FROM lp_sp_micro UNION ALL SELECT ts,id FROM lp_sp_nano)";
            // the instants the set returns, in nanoseconds after 2024-01-01, ordered by id
            final int[] ids = {1, 2, 3, 11, 12, 13};
            final long[] values = {0, 1_000, 2_000, 999, 1_000, 1_001};
            // one nanosecond before, at and after the microsecond boundary
            for (long c : new long[]{999, 1_000, 1_001}) {
                assertModelFilters(set, "ts", ids, values, c, 2_000, 0, 1_500);
            }
            // a cast to a coarser temporal type truncates the set's value to what the branch already holds
            assertMixedFilter(set, "ts::TIMESTAMP >= '2024-01-01T00:00:00.000001Z'", "id\n2\n3\n12\n13\n", 0);
            assertMixedFilter(set, "ts::DATE = '2024-01-01'", "id\n1\n2\n3\n11\n12\n13\n", 0);
            // a TIMESTAMP branch compares text parameters exactly
            bindVariableService.clear();
            bindVariableService.setVarchar(0, new Utf8String("2024-01-01T00:00:00.000000001Z"));
            assertMixedFilter(set, "ts >= $1", "id\n2\n3\n11\n12\n13\n", 0);
        });
    }

    @Test
    public void testMixedPrecisionDateBranchesMatchTheSetAtMillisecondBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_dates (d DATE,id INT)");
            execute("INSERT INTO lp_sp_dates VALUES ('2024-01-01T00:00:00.000Z',1),('2024-01-01T00:00:00.001Z',2),('2024-01-01T00:00:00.002Z',3),(NULL,4)");
            execute("CREATE TABLE lp_sp_micro (ts TIMESTAMP,id INT) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_micro VALUES ('2024-01-01T00:00:00.000999Z',11),('2024-01-01T00:00:00.001000Z',12),('2024-01-01T00:00:00.001001Z',13)");
            execute("CREATE TABLE lp_sp_nano (ts TIMESTAMP_NS,id INT) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_nano VALUES ('2024-01-01T00:00:00.000999999Z',21),('2024-01-01T00:00:00.001000000Z',22),('2024-01-01T00:00:00.001000001Z',23)");
            final String micros = "(SELECT d c,id FROM lp_sp_dates UNION ALL SELECT ts,id FROM lp_sp_micro)";
            final int[] ids = {1, 2, 3, 4, 11, 12, 13};
            // the instants the sets return, in nanoseconds after 2024-01-01, ordered by id
            final long[] microValues = {0, 1_000_000, 2_000_000, Numbers.LONG_NULL, 999_000, 1_000_000, 1_001_000};
            // a microsecond before, at and after the millisecond boundary
            for (long c : new long[]{999_000, 1_000_000, 1_001_000}) {
                assertModelFilters(micros, "c", ids, microValues, c, 2_000_000, 0, 1_500_000);
            }
            final int[] nanoIds = {1, 2, 3, 4, 21, 22, 23};
            final long[] nanoValues = {0, 1_000_000, 2_000_000, Numbers.LONG_NULL, 999_999, 1_000_000, 1_000_001};
            // a nanosecond before, at and after the millisecond boundary
            for (long c : new long[]{999_999, 1_000_000, 1_000_001}) {
                assertModelFilters("(SELECT d c,id FROM lp_sp_dates UNION ALL SELECT ts,id FROM lp_sp_nano)", "c", nanoIds, nanoValues, c,
                        2_000_000, 0, 1_500_000);
            }
            // computed text the set types as TIMESTAMP rounds into the DATE branch as a literal does
            assertMixedFilter(micros, "c >= '2024-01-01T00:00:00.000999Z'::VARCHAR", "id\n2\n3\n11\n12\n13\n", 0);
            // calls whose value does not depend on the precision bind again over the DATE column
            assertMixedFilter(micros, "hour(c) = 0", "id\n1\n2\n3\n11\n12\n13\n", 0);
            assertMixedFilter(micros, "timestamp_floor('T', c) = '2024-01-01T00:00:00.001Z'", "id\n2\n12\n13\n", 0);
            assertMixedFilter(micros, "dateadd('h', 1, c) > '2024-01-01T01:00:00.000999Z'", "id\n2\n3\n12\n13\n", 0);
            // a text literal IN spans its interval in every branch; computed text stays above the DATE branch
            assertMixedFilter(micros, "c IN '2024-01-01T00:00:00.001'", "id\n2\n12\n13\n", 0);
            assertMixedFilter(micros, "c IN concat('2024-01-01T00:00:00', '.001')", "id\n2\n12\n13\n", 1);
            // text that is no literal truncates to milliseconds in a DATE comparison, so it stays above the set
            bindVariableService.clear();
            bindVariableService.setStr(0, "2024-01-01T00:00:00.000999Z");
            assertMixedFilter(micros, "c >= $1", "id\n2\n3\n11\n12\n13\n", 1);
            // a DATE parameter is an instant at every precision
            bindVariableService.clear();
            bindVariableService.setDate(0, 1_704_067_200_001L);
            assertMixedFilter(micros, "c >= $1", "id\n2\n3\n12\n13\n", 0);
            // a call whose factory declares no precision use may read the raw value, so it stays above the set
            assertMixedFilter(micros, "coalesce(NULL, c) = '2024-01-01T00:00:00.001000Z'", "id\n2\n12\n", 1);
            assertMixedFilter(micros, "c = NULL", "id\n4\n", 0);
            assertMixedFilter(micros, "c != NULL", "id\n1\n2\n3\n11\n12\n13\n", 0);
            assertMixedFilter(micros, "c IN (NULL, '2024-01-01T00:00:00.001000Z')", "id\n2\n4\n12\n", 0);
        });
    }

    @Test
    public void testMixedPrecisionFiltersCrossColumnProjectionsAndJoins() throws Exception {
        assertMemoryLeak(() -> {
            createBoundaryRows();
            execute("CREATE TABLE lp_sp_keys (k INT)");
            execute("INSERT INTO lp_sp_keys VALUES (2),(3),(13)");
            final String set = "(SELECT ts,id FROM lp_sp_micro UNION ALL SELECT ts,id FROM lp_sp_nano)";
            final String projected = "SELECT t,id FROM (SELECT id,ts t FROM (SELECT ts,id FROM " + set + ")) WHERE t >= " + nanos(1_001) + " ORDER BY id";
            assertRowsAndIntervalScans(projected, "t\tid\n2024-01-01T00:00:00.000002000Z\t3\n2024-01-01T00:00:00.000001001Z\t13\n", 2);
            assertSqlCursors(projected.replace(set, SetOperationClause.fence(set)), projected);
            // an inner join pushes the conjunct into the set, which rounds it per branch
            for (String shape : new String[]{" u JOIN lp_sp_keys k ON u.id = k.k AND u.ts = ", " u JOIN lp_sp_keys k ON u.id = k.k WHERE u.ts = "}) {
                final String sql = "SELECT u.id FROM " + set + shape + nanos(1_001) + " ORDER BY u.id";
                assertRowsAndIntervalScans(sql, "id\n13\n", 1);
                assertSqlCursors(sql.replace(set, SetOperationClause.fence(set)), sql);
            }
            // an ON condition of one conjunct over the set
            final String crossed = "SELECT count() FROM " + set + " u JOIN lp_sp_keys k ON u.ts >= " + nanos(1_001);
            assertRowsAndIntervalScans(crossed, "count\n6\n", 2);
            assertSqlCursors(crossed.replace(set, SetOperationClause.fence(set)), crossed);
            // an outer join of inputs without a designated timestamp has no timestamp intrinsics to stop
            assertQuery("SELECT k.k a, j.k b FROM lp_sp_keys k LEFT JOIN lp_sp_keys j ON k.k = j.k AND j.k > 2 ORDER BY a")
                    .noLeakCheck()
                    .returns("a\tb\n2\tnull\n3\t3\n13\t13\n");
            // an ON conjunct over the preserved side of an outer join filters no input row
            final String outer = "SELECT u.id, k.k FROM " + set + " u LEFT JOIN lp_sp_keys k ON u.id = k.k AND u.ts > " + nanos(999) + " ORDER BY u.id";
            assertRowsAndIntervalScans(outer, "id\tk\n1\tnull\n2\t2\n3\t3\n11\tnull\n12\tnull\n13\t13\n", 0);
            assertSqlCursors(outer.replace(set, SetOperationClause.fence(set)), outer);
        });
    }

    @Test
    public void testMixedPrecisionLatestBranchKeepsItsFilterAboveTheLatestRows() throws Exception {
        assertMemoryLeak(() -> {
            createBoundaryRows();
            execute("CREATE TABLE lp_sp_events (ts TIMESTAMP,id INT,k SYMBOL) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_events VALUES ('2024-01-01T00:00:00.000000Z',31,'A'),('2024-01-01T00:00:00.000001Z',32,'A'),('2024-01-01T00:00:00.000002Z',33,'B')");
            final String set = "(SELECT ts,id FROM lp_sp_events LATEST ON ts PARTITION BY k UNION ALL SELECT ts,id FROM lp_sp_nano)";
            // the latest rows are 32 and 33; a filter beneath LATEST ON would return 31 for 'A'
            assertMixedFilter(set, "ts < " + nanos(999), "id\n", 0);
            assertMixedFilter(set, "ts <= " + nanos(1_000), "id\n11\n12\n32\n", 0);
            assertMixedFilter(set, "ts > " + nanos(1_000), "id\n13\n33\n", 0);
            assertQuery("SELECT * FROM " + set + " WHERE ts > " + nanos(1_000))
                    .noLeakCheck()
                    .assertsPlanContaining(
                            "Filter filter: 2024-01-01T00:00:00.000001Z<ts",
                            "Interval forward scan on: lp_sp_nano"
                    );
        });
    }

    @Test
    public void testMixedPrecisionLimitsBeforeEpochAndFarFuture() throws Exception {
        assertMemoryLeak(() -> {
            createBoundaryRows();
            execute("CREATE TABLE lp_sp_old (t TIMESTAMP,id INT)");
            // the extreme microseconds whose nanoseconds TIMESTAMP_NS still holds
            execute("""
                    INSERT INTO lp_sp_old VALUES
                        (-9_223_372_036_854_775,1),(-1_000_001,2),(-1,3),(0,4),(NULL,5),(9_223_372_036_854_775,6)
                    """);
            final String set = "(SELECT t c,id FROM lp_sp_old UNION ALL SELECT ts,id FROM lp_sp_nano)";
            final int[] ids = {1, 2, 3, 4, 5, 6, 11, 12, 13};
            final long[] values = {
                    -9_223_372_036_854_775_000L, -1_000_001_000L, -1_000L, 0, Numbers.LONG_NULL, 9_223_372_036_854_775_000L,
                    BASE_NANOS + 999, BASE_NANOS + 1_000, BASE_NANOS + 1_001
            };
            final long[] constants = {
                    Long.MIN_VALUE + 1, -9_223_372_036_854_775_001L, -9_223_372_036_854_775_000L, -9_223_372_036_854_774_999L,
                    -1_000_001_001L, -1_001, -1_000, -999, -1, 0, 1,
                    9_223_372_036_854_774_999L, 9_223_372_036_854_775_000L, 9_223_372_036_854_775_001L, Long.MAX_VALUE
            };
            final String[] operators = {"=", "!=", "<", "<=", ">", ">="};
            for (long c : constants) {
                final String constant = "(" + withUnderscores(c) + ")::TIMESTAMP_NS";
                for (String operator : operators) {
                    final ComparisonOperator comparison = ComparisonOperator.of(operator);
                    Assert.assertNotNull(comparison);
                    assertMixedFilter(set, "c " + operator + ' ' + constant,
                            expectedIds(ids, values, v -> comparison.test(v, c), comparison == ComparisonOperator.NOT_EQUAL), 0);
                }
                assertMixedFilter(set, "c BETWEEN " + constant + " AND (9_223_372_036_854_775_000)::TIMESTAMP_NS",
                        expectedIds(ids, values, v -> Math.min(c, 9_223_372_036_854_775_000L) <= v
                                && v <= Math.max(c, 9_223_372_036_854_775_000L), false), 0);
            }
            // the branch compares a microsecond beyond the nanosecond range in microseconds; only reading the
            // set's column converts it, and fails
            execute("CREATE TABLE lp_sp_far (t TIMESTAMP,id INT)");
            execute("INSERT INTO lp_sp_far VALUES ('3000-01-01',1),('2024-01-01',2)");
            final String far = "(SELECT t c,id FROM lp_sp_far UNION ALL SELECT ts,id FROM lp_sp_nano)";
            assertQuery("SELECT id FROM " + far + " WHERE c < '2200-01-01' ORDER BY id")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("id\n2\n11\n12\n13\n");
            assertQuery("SELECT id FROM " + far + " WHERE c > '2200-01-01'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("id\n1\n");
            try (RecordCursorFactory factory = select("SELECT c, id FROM " + far + " WHERE c > '2200-01-01'")) {
                assertRowsOnly(factory, "c\tid\n");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: 32503680000000000 [TIMESTAMP -> TIMESTAMP_NS]");
            }
        });
    }

    @Test
    public void testMixedPrecisionNestedSetsRoundAtEachLevel() throws Exception {
        assertMemoryLeak(() -> {
            createBoundaryRows();
            execute("CREATE TABLE lp_sp_micro2 (ts TIMESTAMP,id INT) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_micro2 VALUES ('2024-01-01T00:00:00.000001Z',21),('2024-01-01T00:00:00.000003Z',22)");
            execute("CREATE TABLE lp_sp_dates (d DATE,id INT)");
            execute("INSERT INTO lp_sp_dates VALUES ('2024-01-01T00:00:00.000Z',31),('2024-01-01T00:00:00.001Z',32)");
            final String micros = "(SELECT ts,id FROM (SELECT ts,id FROM lp_sp_micro UNION ALL SELECT ts,id FROM lp_sp_micro2)"
                    + " UNION ALL SELECT ts,id FROM lp_sp_nano)";
            assertMixedFilter(micros, "ts >= " + nanos(1_001), "id\n3\n13\n22\n", 0);
            assertRowsAndIntervalScans("SELECT id FROM " + micros + " WHERE ts >= " + nanos(1_001) + " ORDER BY id", "id\n3\n13\n22\n", 3);
            assertMixedFilter(micros, "ts = " + nanos(1_000), "id\n2\n12\n21\n", 0);
            // nanoseconds round to microseconds and then to milliseconds
            final String dates = "(SELECT ts,id FROM lp_sp_nano UNION ALL SELECT c,id FROM (SELECT d c,id FROM lp_sp_dates"
                    + " UNION ALL SELECT ts,id FROM lp_sp_micro))";
            assertMixedFilter(dates, "ts > " + nanos(999), "id\n2\n3\n12\n13\n32\n", 0);
            assertMixedFilter(dates, "ts <= '2024-01-01T00:00:00.000999999Z'", "id\n1\n2\n3\n11\n12\n13\n31\n", 0);
            // no microsecond holds the instant, so the inner set reads under a false filter
            assertMixedFilter(dates, "ts = '2024-01-01T00:00:00.001000001Z'", "id\n", 1);
            final String chain = "(SELECT ts,id FROM lp_sp_micro UNION ALL SELECT ts,id FROM lp_sp_nano UNION ALL SELECT ts,id FROM lp_sp_micro2)";
            assertMixedFilter(chain, "ts BETWEEN " + nanos(999) + " AND " + nanos(2_999), "id\n2\n3\n11\n12\n13\n21\n", 0);
            assertMixedFilter("(SELECT ts,id FROM lp_sp_nano INTERSECT SELECT ts,id FROM lp_sp_micro)", "ts >= " + nanos(999), "id\n", 0);
            assertMixedFilter("(SELECT ts,1 id FROM lp_sp_nano INTERSECT SELECT ts,1 FROM lp_sp_micro)", "ts >= " + nanos(999), "id\n1\n", 0);
            assertMixedFilter("(SELECT ts,id FROM lp_sp_micro EXCEPT SELECT ts,id - 10 FROM lp_sp_nano)", "ts <= " + nanos(1_001), "id\n1\n", 0);
        });
    }

    @Test
    public void testMixedPrecisionStatefulAndSubqueryConjunctsStayAbove() throws Exception {
        assertMemoryLeak(() -> {
            createBoundaryRows();
            final String set = "(SELECT ts,id FROM lp_sp_micro UNION ALL SELECT ts,id FROM lp_sp_nano)";
            // rnd_int(0, 1, 0) * 0 is always 0 but is not one value per execution
            final String random = "SELECT id FROM " + set + " WHERE ts > dateadd('s', rnd_int(0, 1, 0) * 0, " + nanos(999) + "::TIMESTAMP_NS) ORDER BY id";
            assertLogicalResidual(random, "id\n2\n3\n12\n13\n", 1);
            assertRowsAndIntervalScans(random, "id\n2\n3\n12\n13\n", 0);
            // the sub-query binds once; the branch of the set's own precision takes a copy, as it did before
            final String subquery = "SELECT id FROM " + set + " WHERE ts > (SELECT min(ts) FROM lp_sp_nano) ORDER BY id";
            assertQuery(subquery).noLeakCheck().sizeMayVary().returns("id\n2\n3\n12\n13\n");
            assertSqlCursors(subquery.replace(set, SetOperationClause.fence(set)), subquery);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(subquery, sqlExecutionContext).getRecordCursorFactory()) {
                    final ObjList<CursorExpression> cursors = new ObjList<>();
                    collectCursors(compiler.getPlanForTesting(), cursors);
                    Assert.assertEquals(2, cursors.size());
                    Assert.assertSame(cursors.getQuick(0).getSubquery(), cursors.getQuick(1).getSubquery());
                    Assert.assertEquals(1, countSetResiduals(compiler.getPlanForTesting()));
                    assertRowsOnly(factory, "id\n2\n3\n12\n13\n");
                }
            }
        });
    }

    @Test
    public void testMixedPrecisionTimestampBoundsBindPerBranch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_micro (ts TIMESTAMP,id INT) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_sp_nano (ts TIMESTAMP_NS,id INT) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_micro VALUES (1,5),('2024-01-01T00:00:00.000000Z',1),('2024-01-01T00:00:00.000001Z',2)");
            execute("INSERT INTO lp_sp_nano VALUES (1,6),('2024-01-01T00:00:00.000000001Z',3),('2024-01-01T00:00:00.000001000Z',4)");
            final String set = " FROM (SELECT id,ts FROM lp_sp_micro UNION ALL SELECT id,ts FROM lp_sp_nano)";
            final String nano = "'2024-01-01T00:00:00.000000001Z'";
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts=" + nano + " ORDER BY id", "id\n3\n", 1);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts>=" + nano + " ORDER BY id", "id\n2\n3\n4\n", 2);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts!=" + nano + " ORDER BY id", "id\n1\n2\n4\n5\n6\n", 1);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts IN (" + nano + ") ORDER BY id", "id\n3\n", 1);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts BETWEEN " + nano + " AND '2025' ORDER BY id", "id\n2\n3\n4\n", 2);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts=1 ORDER BY id", "id\n6\n", 1);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts>=1 AND ts<" + nano + " AND id>0 ORDER BY id", "id\n1\n5\n6\n", 2);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts=" + nano + " OR id=5 ORDER BY id", "id\n3\n5\n", 0);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts+0L!=" + nano + " ORDER BY id", "id\n1\n2\n4\n5\n6\n", 0);
            assertRowsAndIntervalScans("SELECT id FROM (SELECT id,ts AS t FROM lp_sp_micro UNION ALL SELECT id,ts FROM lp_sp_nano) x"
                    + " WHERE x.t=" + nano + " ORDER BY id", "id\n3\n", 1);
            assertRowsAndIntervalScans("SELECT id FROM (SELECT id,ts FROM (SELECT id,ts FROM lp_sp_micro UNION ALL SELECT id,ts FROM lp_sp_nano))"
                    + " WHERE ts=" + nano + " ORDER BY id", "id\n3\n", 1);
            assertRowsAndIntervalScans("SELECT id FROM (SELECT id,ts FROM lp_sp_micro UNION ALL (SELECT id,ts FROM lp_sp_nano LIMIT 2))"
                    + " WHERE ts=" + nano + " ORDER BY id", "id\n3\n", 0);
            assertRowsAndIntervalScans("SELECT id FROM (SELECT id,ts FROM lp_sp_micro EXCEPT SELECT id,ts FROM lp_sp_nano)"
                    + " WHERE ts<" + nano + " ORDER BY id", "id\n1\n5\n", 2);
            assertRowsAndIntervalScans("SELECT a.id FROM lp_sp_micro a JOIN (SELECT id,ts FROM lp_sp_micro UNION ALL SELECT id,ts FROM lp_sp_nano) b"
                    + " ON a.id=b.id WHERE b.ts=" + nano, "id\n", 1);
            assertRowsAndIntervalScans("SELECT a.id,b.id FROM lp_sp_micro a LEFT JOIN (SELECT id,ts FROM lp_sp_micro UNION ALL SELECT id,ts FROM lp_sp_nano) b"
                    + " ON a.id=b.id AND b.ts=" + nano + " ORDER BY a.id", "id\tid1\n1\tnull\n2\tnull\n5\tnull\n", 0);
            bindVariableService.clear();
            bindVariableService.setLong(0, 1);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts=$1 ORDER BY id", "id\n6\n", 1);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts>$1 ORDER BY id", "id\n1\n2\n3\n4\n5\n", 1);
            assertRowsAndIntervalScans("SELECT id FROM lp_sp_micro WHERE ts=$1", "id\n5\n", 1);
            assertRowsAndIntervalScans("SELECT id FROM lp_sp_nano WHERE ts!=$1 ORDER BY id", "id\n3\n4\n", 1);
            assertRowsAndIntervalScans("SELECT id FROM (SELECT id,ts FROM lp_sp_micro UNION ALL SELECT id,ts FROM lp_sp_micro) WHERE ts=$1",
                    "id\n5\n5\n", 2);
        });
    }

    @Test
    public void testNanosecondFloorStridesBindPerBranchOnlyWhenExact() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_values (t TIMESTAMP)");
            execute("INSERT INTO lp_sp_values VALUES (-1001),(-1000),(-999),(-1),(0),(1),(999),(1000),(1001),(NULL)");
            assertFloorPrecisionMismatches("n", "");
            assertFloorPrecisionMismatches("1000n", "");
            assertFloorPrecisionMismatches("2000n", "");
            assertFloorPrecisionMismatches("500n", "");
            assertFloorPrecisionMismatches("3n", "-1001\n-1000\n-1\n1\n1000\n1001\n");
            assertFloorPrecisionMismatches("1500n", "-1000\n-1\n1001\n");

            execute("CREATE TABLE lp_sp_micro (ts TIMESTAMP,id INT) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_sp_nano (ts TIMESTAMP_NS,id INT) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_micro VALUES (0,1),(1,2),(2,3),(999,4),(1000,5),(1001,6)");
            execute("INSERT INTO lp_sp_nano VALUES (0,11),(1,12),(999,13),(1000,14),(1001,15),(1500,16)");
            execute("CREATE TABLE lp_sp_one (x INT)");
            execute("INSERT INTO lp_sp_one VALUES (1)");
            final String set = "(SELECT ts c,id FROM lp_sp_micro UNION ALL SELECT ts,id FROM lp_sp_nano)";
            assertNanosecondFloorBinding(set, "n", "1970-01-01T00:00:00.000001Z", "2\n14\n", 2, 0);
            assertNanosecondFloorBinding(set, "1000n", "1970-01-01T00:00:00.000001Z", "2\n14\n15\n16\n", 2, 0);
            assertNanosecondFloorBinding(set, "2000n", "1970-01-01T00:00:00.000000Z", "1\n2\n11\n12\n13\n14\n15\n16\n", 2, 0);
            assertNanosecondFloorBinding(set, "500n", "1970-01-01T00:00:00.000001Z", "2\n14\n15\n", 1, 0);
            assertNanosecondFloorBinding(set, "3n", "1970-01-01T00:00:00.000000999Z", "2\n13\n14\n15\n", 1, 1);
            assertNanosecondFloorBinding(set, "1500n", "1970-01-01T00:00:00.000001500Z", "3\n16\n", 1, 1);
        });
    }

    @Test
    public void testNullBranchFiltersOverUnifiedType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_micro (ts TIMESTAMP,id INT) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_micro VALUES ('2024-01-01T00:00:00',1),('2024-01-01T01:00:00',2)");
            final String set = " FROM (SELECT ts,id FROM lp_sp_micro UNION ALL SELECT null,id+2 FROM lp_sp_micro)";
            assertQuery("SELECT id" + set + " WHERE typeOf(ts)='TIMESTAMP' ORDER BY id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\n1\n2\n3\n4\n");
            assertQuery("SELECT id" + set + " WHERE ts IS NULL ORDER BY id")
                    .noLeakCheck()
                    .returns("id\n3\n4\n");
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts>'2024-01-01T00:30' ORDER BY id", "id\n2\n", 1);
            assertLogicalResidual("SELECT id" + set + " WHERE ts>'2024-01-01T00:30' ORDER BY id", "id\n2\n", 0);
        });
    }

    @Test
    public void testPrecisionAndNullCastsDistributeIntoBranches() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_micro (ts TIMESTAMP,id INT) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_sp_nano (ts TIMESTAMP_NS,id INT) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_micro VALUES ('2024-01-01T00:00:00',1),('2024-01-01T01:00:00',2)");
            execute("INSERT INTO lp_sp_nano VALUES ('2024-01-01T00:00:00',3),('2024-01-01T02:00:00',4)");
            final String mixed = "SELECT id FROM (SELECT ts,id FROM lp_sp_micro UNION ALL SELECT ts,id FROM lp_sp_nano)"
                    + " WHERE hour(ts)>0 ORDER BY id";
            assertRowsAndIntervalScans(mixed, "id\n2\n4\n", 0);
            assertLogicalResidual(mixed, "id\n2\n4\n", 0);
            final String nulls = " FROM (SELECT ts,id FROM lp_sp_micro UNION ALL SELECT null ts,id FROM lp_sp_nano)"
                    + " WHERE hour(ts)>0 ORDER BY id";
            assertRowsAndIntervalScans("SELECT ts,id" + nulls, "ts\tid\n2024-01-01T01:00:00.000000Z\t2\n", 0);
            assertLogicalResidual("SELECT id" + nulls, "id\n2\n", 0);
            assertRowsAndIntervalScans("SELECT id FROM (SELECT ts,id FROM lp_sp_micro UNION ALL SELECT ts,id FROM lp_sp_nano)"
                    + " WHERE ts>'2024-01-01T00:00:00.000000000Z' ORDER BY id", "id\n2\n4\n", 2);
        });
    }

    @Test
    public void testPrecisionDependentFiltersBindOverUnifiedType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_micro (ts TIMESTAMP,id INT,d DATE) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_sp_nano (ts TIMESTAMP_NS,id INT) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO lp_sp_micro VALUES
                        (1,1,'1970-01-01T00:00:00.001Z'),
                        ('2024-01-01T00:00:00.000000Z',2,'2024-01-01T00:00:00.000Z'),
                        ('2024-01-01T00:00:00.000001Z',3,NULL)
                    """);
            execute("INSERT INTO lp_sp_nano VALUES (1,11),('2024-01-01T00:00:00.000000001Z',12),('2024-01-01T00:00:00.000001000Z',13)");
            execute("CREATE TABLE lp_sp_one (x INT)");
            execute("INSERT INTO lp_sp_one VALUES (1)");
            final String precisions = "(SELECT ts c,id FROM lp_sp_micro UNION ALL SELECT ts,id FROM lp_sp_nano)";
            assertPrecisionFilter(precisions, "typeOf({c})='TIMESTAMP'", "");
            assertPrecisionFilter(precisions, "{c}::STRING LIKE '%000Z'", "1\n2\n3\n13\n");
            assertPrecisionFilter(precisions, "{c}=1", "11\n");
            assertPrecisionFilter(precisions, "{c}::LONG=1000", "1\n");
            final String dates = "(SELECT d c,id FROM lp_sp_micro UNION ALL SELECT ts,id+10 FROM lp_sp_micro)";
            assertPrecisionFilter(dates, "typeOf({c})='TIMESTAMP'", "1\n2\n3\n11\n12\n13\n");
            assertPrecisionFilter(dates, "{c}::STRING LIKE '%000Z'", "1\n2\n12\n");
            assertPrecisionFilter(dates, "{c}=1", "11\n");
            assertPrecisionFilter(dates, "{c}::LONG=1000", "1\n");
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE c::LONG=1000 ORDER BY id", "id\n1\n", 1);
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE year(c)=2024 AND hour(c)=0 ORDER BY id", "id\n2\n3\n12\n13\n", 0);
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE c>1+1 ORDER BY id", "id\n1\n2\n3\n12\n13\n", 1);
            assertRowsAndIntervalScans("SELECT id FROM " + precisions + " WHERE c>dateadd('y',-100,now()) ORDER BY id",
                    "id\n1\n2\n3\n11\n12\n13\n", 2);
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE c<'2024-01-01'::TIMESTAMP_NS+1 ORDER BY id", "id\n1\n2\n11\n", 0);
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE timestamp_floor('h',c)='2024-01-01' ORDER BY id", "id\n2\n3\n12\n13\n", 0);
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE dateadd('h',1,c)>'2024-01-01T00:30' ORDER BY id", "id\n2\n3\n12\n13\n", 0);
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE extract(hour FROM c)=0 ORDER BY id", "id\n1\n2\n3\n11\n12\n13\n", 0);
            assertPrecisionFilter(precisions, "dateadd('n',1,{c})='2024-01-01T00:00:00.000000001Z'", "2\n");
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE dateadd('n',1,c)='2024-01-01T00:00:00.000000001Z' ORDER BY id", "id\n2\n", 1);
            assertPrecisionFilter(precisions, "timestamp_floor('3n',{c})='1970-01-01T00:00:00.000000999Z'::TIMESTAMP_NS", "1\n");
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE timestamp_floor('3n',c)='1970-01-01T00:00:00.000000999Z'::TIMESTAMP_NS ORDER BY id",
                    "id\n1\n", 1);
            assertPrecisionFilter(precisions, "timestamp_floor('3U',{c})='1970-01-01T00:00:00.000000Z'", "1\n11\n");
            assertPrecisionFilter(precisions, "timestamp_ceil('n',{c})='1970-01-01T00:00:00.000001Z'", "1\n");
            assertPrecisionFilter(precisions, "date_trunc('nanosecond',{c})='1970-01-01T00:00:00.000001Z'", "1\n");
            assertPrecisionFilter(precisions, "to_utc({c},'Europe/Berlin')='1969-12-31T23:00:00.000001Z'", "1\n");
            assertPrecisionFilter(precisions, "to_timezone({c},'Europe/Berlin')='1970-01-01T01:00:00.000001Z'", "1\n");
            bindVariableService.clear();
            bindVariableService.setStr(0, "2024-01-01T00:00:00.000000001Z");
            assertRowsAndIntervalScans("SELECT id FROM " + precisions + " WHERE c>=$1 ORDER BY id", "id\n3\n12\n13\n", 2);
            assertLogicalResidual("SELECT id FROM " + dates + " WHERE c>=$1 ORDER BY id", "id\n13\n", 1);
            assertPrecisionFilter(dates, "{c}>=$1", "13\n");
        });
    }

    @Test
    public void testRetainedBranchPredicatesSurviveCompilerReuseAndParameterRebind() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setTimestamp(0, 1_704_153_600_000_000L);
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT id FROM (SELECT ta ts,id FROM lp_sp_a UNION ALL SELECT tb ts,id FROM lp_sp_b)"
                            + " WHERE ts>=$1 AND hour(ts)=0 ORDER BY id", sqlExecutionContext).getRecordCursorFactory();
                    assertRowsOnly(retained, "id\n2\n2\n2\n3\n3\n4\n4\n5\n");
                    try (RecordCursorFactory ignored = compiler.compile("SELECT id FROM lp_sp_a WHERE id>1", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                    compiler.clear();
                }
                bindVariableService.setTimestamp(0, 1_704_240_000_000_000L);
                assertRowsOnly(retained, "id\n3\n3\n4\n4\n5\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testSharedDefinitionsKeepConsumerPredicatesIndependent() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsAndIntervalScans("WITH q AS (SELECT ta ts,id FROM lp_sp_a UNION ALL SELECT tb ts,id FROM lp_sp_b)"
                            + " SELECT * FROM (SELECT id FROM q WHERE ts='2024-01-02T00:00:00.000000Z'"
                            + " UNION ALL SELECT id FROM q WHERE ts='2024-01-04T00:00:00.000000Z') ORDER BY id",
                    "id\n2\n2\n2\n5\n", 4);
        });
    }

    @Test
    public void testStatefulFiltersStayAboveMixedPrecisionSets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_rollup (ts TIMESTAMP_NS,a INT,v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE lp_sp_plain (ts TIMESTAMP,a INT,v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO lp_sp_rollup VALUES ('2024-01-01',1,3)");
            execute("INSERT INTO lp_sp_plain VALUES ('2024-01-01',1,3)");
            final String[] producers = {
                    "SELECT ts,a,SUM(v) s FROM lp_sp_rollup GROUP BY ROLLUP(ts,a)",
                    "SELECT ts,a,SUM(v) s FROM lp_sp_rollup GROUP BY GROUPING SETS ((ts,a),(ts),())",
                    "SELECT ts,a,v FROM lp_sp_rollup"
            };
            final String[] stableCounts = {"4", "4", "2"};
            for (int i = 0; i < producers.length; i++) {
                final String set = " FROM (" + producers[i] + " UNION ALL SELECT ts,a,v FROM lp_sp_plain)";
                // every row of the set reads hour 0, and only the sequence's first value is 0
                assertQuery("SELECT count()" + set + " WHERE coalesce(hour(ts),0)=timestamp_sequence(0,1)::LONG")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n1\n");
                assertQuery("SELECT count()" + set + " WHERE coalesce(hour(ts),0)=0")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n" + stableCounts[i] + "\n");
            }
            // both rows of the set join the single plain row; a WHERE or an ON conjunct over the set alone reaches
            // the join binder, which must keep a sequence to one instance as well
            final String join = " FROM (SELECT ts,a,v FROM lp_sp_rollup UNION ALL SELECT ts,a,v FROM lp_sp_plain) s"
                    + " JOIN lp_sp_plain p ON s.a = p.a";
            for (String conjunct : new String[]{" WHERE ", " AND "}) {
                assertQuery("SELECT count()" + join + conjunct + "coalesce(hour(s.ts),0)=timestamp_sequence(0,1)::LONG")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n1\n");
                assertQuery("SELECT count()" + join + conjunct + "coalesce(hour(s.ts),0)=0")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n2\n");
            }
        });
    }

    @Test
    public void testTextAndTimestampFiltersBindOverUnifiedType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_ts (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO lp_sp_ts VALUES ('2024-01-01T03:00:00'),('2024-01-02T02:00:00')");
            execute("CREATE TABLE lp_sp_text (sym SYMBOL,s STRING,v VARCHAR,u UUID,ip IPv4)");
            execute("""
                    INSERT INTO lp_sp_text VALUES
                        ('1q','1q','1q','10000000-0000-0000-0000-000000000000','1.1.1.1'),
                        ('q','q','q','f0000000-0000-0000-0000-000000000000','9.9.9.9')
                    """);
            execute("CREATE TABLE lp_sp_one (x INT)");
            execute("INSERT INTO lp_sp_one VALUES (1)");
            final ObjList<String> columns = new ObjList<>("sym", "s", "v", "u", "ip");
            final ObjList<String> lows = new ObjList<>("1q", "1q", "1q", "10000000-0000-0000-0000-000000000000", "1.1.1.1");
            final ObjList<String> highs = new ObjList<>("q", "q", "q", "f0000000-0000-0000-0000-000000000000", "9.9.9.9");
            for (int i = 0, n = columns.size(); i < n; i++) {
                final String column = columns.getQuick(i);
                final String high = highs.getQuick(i);
                final String in = " IN ('2024-01-02T02','" + high + "')";
                for (int order = 0; order < 2; order++) {
                    final String set = order == 0
                            ? "(SELECT " + column + " c FROM lp_sp_text UNION ALL SELECT ts FROM lp_sp_ts)"
                            : "(SELECT ts c FROM lp_sp_ts UNION ALL SELECT " + column + " FROM lp_sp_text)";
                    assertQuery("SELECT c, c>'2024-01-02T02' gt, c='2024-01-02T02' eq, c" + in + " inl, c LIKE '2024-01-02%' lk FROM "
                            + set + " ORDER BY c")
                            .noLeakCheck()
                            .expectSize()
                            .returns("c\tgt\teq\tinl\tlk\n"
                                    + lows.getQuick(i) + "\tfalse\tfalse\tfalse\tfalse\n"
                                    + "2024-01-01T03:00:00.000000Z\tfalse\tfalse\tfalse\tfalse\n"
                                    + "2024-01-02T02:00:00.000000Z\ttrue\tfalse\tfalse\ttrue\n"
                                    + high + "\ttrue\tfalse\ttrue\tfalse\n");
                    assertUnifiedFilter(set, ">'2024-01-02T02'", "2024-01-02T02:00:00.000000Z\n" + high + "\n");
                    assertUnifiedFilter(set, "='2024-01-02T02'", "");
                    assertUnifiedFilter(set, in, high + "\n");
                    assertUnifiedFilter(set, " LIKE '2024-01-02%'", "2024-01-02T02:00:00.000000Z\n");
                }
            }
        });
    }

    private static void collectCursors(LogicalPlan plan, ObjList<CursorExpression> sink) {
        plan.visitReads(new PlanExpressionVisitor() {
            @Override
            public int visitColumnId(int columnId, int position) {
                return columnId;
            }

            @Override
            public BoundExpression visitExpression(BoundExpression expression) {
                expression.walk(node -> {
                    if (node instanceof CursorExpression cursor) {
                        sink.add(cursor);
                    }
                    return TreeWalk.CONTINUE;
                });
                return expression;
            }
        });
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            collectCursors(plan.inputAt(i), sink);
        }
    }

    private static int countSetResiduals(LogicalPlan plan) {
        int count = plan instanceof FilterPlan filter && filter.getChild() instanceof SetOperationPlan ? 1 : 0;
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            count += countSetResiduals(plan.inputAt(i));
        }
        return count;
    }

    // The ids, in their order, whose value the predicate keeps, or whose value is NULL when isNullKept.
    private static String expectedIds(int[] ids, long[] values, LongPredicate predicate, boolean isNullKept) {
        final StringBuilder expected = new StringBuilder("id\n");
        for (int i = 0; i < ids.length; i++) {
            if (values[i] == Numbers.LONG_NULL ? isNullKept : predicate.test(values[i])) {
                expected.append(ids[i]).append('\n');
            }
        }
        return expected.toString();
    }

    // The nanosecond literal of 2024-01-01 plus the offset, which is under a second.
    private static String nanos(long offset) {
        return String.format("'2024-01-01T00:00:00.%09dZ'", offset);
    }

    private static String withUnderscores(long value) {
        final String digits = value < 0 ? Long.toString(value).substring(1) : Long.toString(value);
        final StringBuilder text = new StringBuilder(value < 0 ? "-" : "");
        for (int i = 0; i < digits.length(); i++) {
            if (i > 0 && (digits.length() - i) % 3 == 0) {
                text.append('_');
            }
            text.append(digits.charAt(i));
        }
        return text.toString();
    }

    private void assertFloorPrecisionMismatches(String stride, String expected) throws Exception {
        assertQuery("SELECT t::LONG v FROM lp_sp_values WHERE timestamp_floor('" + stride + "',t)!=timestamp_floor('" + stride + "',t::TIMESTAMP_NS) ORDER BY v")
                .noLeakCheck()
                .returns("v\n" + expected);
    }

    private void assertLogicalResidual(String sql, String expected, int expectedResiduals) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertEquals(expectedResiduals, countSetResiduals(compiler.getPlanForTesting()));
                assertRowsOnly(factory, expected);
            }
        }
    }

    private void assertMixedFilter(String set, String predicate, String expected, int residuals) throws Exception {
        final String sql = "SELECT id FROM " + set + " WHERE " + predicate + " ORDER BY id";
        assertQuery(sql).noLeakCheck().sizeMayVary().returns(expected);
        assertSqlCursors("SELECT id FROM " + SetOperationClause.fence(set) + " WHERE " + predicate + " ORDER BY id", sql);
        assertLogicalResidual(sql, expected, residuals);
    }

    /**
     * Every operator against the constant {@code c}, nanoseconds after 2024-01-01, matched against the same
     * comparison of the instants the set returns, {@code values} in nanoseconds; {@code other} is a second IN element,
     * {@code lo} and {@code hi} the other BETWEEN bounds, which BETWEEN takes in either order. A NULL value fails every comparison and passes its negation.
     */
    private void assertModelFilters(String set, String column, int[] ids, long[] values, long c, long other, long lo, long hi) throws Exception {
        final String constant = nanos(c);
        assertMixedFilter(set, column + " = " + constant, expectedIds(ids, values, v -> v == c, false), 0);
        assertMixedFilter(set, column + " != " + constant, expectedIds(ids, values, v -> v != c, true), 0);
        assertMixedFilter(set, column + " < " + constant, expectedIds(ids, values, v -> v < c, false), 0);
        assertMixedFilter(set, column + " <= " + constant, expectedIds(ids, values, v -> v <= c, false), 0);
        assertMixedFilter(set, column + " > " + constant, expectedIds(ids, values, v -> v > c, false), 0);
        assertMixedFilter(set, column + " >= " + constant, expectedIds(ids, values, v -> v >= c, false), 0);
        assertMixedFilter(set, constant + " < " + column, expectedIds(ids, values, v -> v > c, false), 0);
        assertMixedFilter(set, column + " IN (" + constant + ", " + nanos(other) + ')',
                expectedIds(ids, values, v -> v == c || v == other, false), 0);
        assertMixedFilter(set, column + " NOT IN (" + constant + ", " + nanos(other) + ')',
                expectedIds(ids, values, v -> v != c && v != other, true), 0);
        assertMixedFilter(set, column + " BETWEEN " + constant + " AND " + nanos(hi),
                expectedIds(ids, values, v -> Math.min(c, hi) <= v && v <= Math.max(c, hi), false), 0);
        assertMixedFilter(set, column + " BETWEEN " + nanos(lo) + " AND " + constant,
                expectedIds(ids, values, v -> Math.min(lo, c) <= v && v <= Math.max(lo, c), false), 0);
        assertMixedFilter(set, column + " NOT BETWEEN " + constant + " AND " + nanos(hi),
                expectedIds(ids, values, v -> v < Math.min(c, hi) || Math.max(c, hi) < v, true), 0);
    }

    private void assertNanosecondFloorBinding(String set, String stride, String floor, String expected, int intervalScans, int residuals) throws Exception {
        final String predicate = "timestamp_floor('" + stride + "',{c})='" + floor + "'";
        assertPrecisionFilter(set, predicate, expected);
        final String sql = "SELECT id FROM " + set + " WHERE " + predicate.replace("{c}", "c") + " ORDER BY id";
        assertRowsAndIntervalScans(sql, "id\n" + expected, intervalScans);
        assertLogicalResidual(sql, "id\n" + expected, residuals);
    }

    private void assertPrecisionFilter(String set, String predicate, String expected) throws Exception {
        final String rows = "id\n" + expected;
        assertQuery("SELECT id FROM " + set + " WHERE " + predicate.replace("{c}", "c") + " ORDER BY id")
                .noLeakCheck()
                .sizeMayVary()
                .returns(rows);
        assertQuery("SELECT u.id FROM " + set + " u JOIN lp_sp_one k ON " + predicate.replace("{c}", "u.c") + " ORDER BY u.id")
                .noLeakCheck()
                .sizeMayVary()
                .returns(rows);
        assertQuery("SELECT id FROM (SELECT id," + predicate.replace("{c}", "c") + " k FROM " + set + ") WHERE k ORDER BY id")
                .noLeakCheck()
                .sizeMayVary()
                .returns(rows);
    }

    private void assertRowsAndIntervalScans(String sql, String expected, int intervalScans) throws Exception {
        final int jitMode = sqlExecutionContext.getJitMode();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (RecordCursorFactory factory = select(sql)) {
            assertRowsOnly(factory, expected);
            final TextPlanSink sink = new TextPlanSink();
            final int count = countIntervalScans(factory, sink);
            Assert.assertEquals(sql + "\n" + sink.getSink(), intervalScans, count);
        } finally {
            sqlExecutionContext.setJitMode(jitMode);
        }
    }

    private void assertUnifiedFilter(String set, String predicate, String expected) throws Exception {
        final String rows = "c\n" + expected;
        assertQuery("SELECT c FROM " + set + " WHERE c" + predicate + " ORDER BY c")
                .noLeakCheck()
                .returns(rows);
        assertQuery("SELECT c FROM (SELECT c::STRING c FROM " + set + ") WHERE c" + predicate + " ORDER BY c")
                .noLeakCheck()
                .returns(rows);
        assertQuery("SELECT u.c FROM " + set + " u JOIN lp_sp_one k ON u.c" + predicate + " ORDER BY u.c")
                .noLeakCheck()
                .returns(rows);
    }

    // The interval scans of the factory's plan, which it prints into the sink.
    private int countIntervalScans(RecordCursorFactory factory, TextPlanSink sink) {
        sink.of(factory, sqlExecutionContext);
        int count = 0;
        for (int line = 1; line <= sink.getLineCount(); line++) {
            if (sink.getLine(line).toString().contains("Interval forward scan")) {
                count++;
            }
        }
        return count;
    }

    // The microsecond and nanosecond rows on either side of the first microsecond boundary of 2024.
    private void createBoundaryRows() throws Exception {
        execute("CREATE TABLE lp_sp_micro (ts TIMESTAMP,id INT) TIMESTAMP(ts)");
        execute("CREATE TABLE lp_sp_nano (ts TIMESTAMP_NS,id INT) TIMESTAMP(ts)");
        execute("INSERT INTO lp_sp_micro VALUES ('2024-01-01T00:00:00.000000Z',1),('2024-01-01T00:00:00.000001Z',2),('2024-01-01T00:00:00.000002Z',3)");
        execute("INSERT INTO lp_sp_nano VALUES ('2024-01-01T00:00:00.000000999Z',11),('2024-01-01T00:00:00.000001000Z',12),('2024-01-01T00:00:00.000001001Z',13)");
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_sp_a (ta TIMESTAMP,id INT) TIMESTAMP(ta) PARTITION BY DAY");
        execute("CREATE TABLE lp_sp_b (tb TIMESTAMP,id INT) TIMESTAMP(tb) PARTITION BY DAY");
        execute("INSERT INTO lp_sp_a VALUES ('2024-01-01',1),('2024-01-02',2),('2024-01-02',2),('2024-01-03',3),('2024-01-03',3)");
        execute("INSERT INTO lp_sp_b VALUES ('2024-01-02',2),('2024-01-03',4),('2024-01-03',4),('2024-01-04',5)");
    }
}
