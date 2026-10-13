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
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class WindowDeduplicationTest extends AbstractCairoTest {
    private static final String CREATE_TABLE = "CREATE TABLE lp_window_prune (unused INT,id INT,grp SYMBOL,v DOUBLE)";
    private static final String INSERT_ROWS = "INSERT INTO lp_window_prune VALUES (30,3,'a',30),(10,1,'a',10),(20,2,'b',20),(40,4,'b',40)";

    @Test
    public void testCountOverWindowSkipsUnreadCalls() throws Exception {
        assertQuery("SELECT count(*) c FROM (SELECT id, sum(unused::DECIMAL(1,0)) OVER () s FROM lp_window_prune)")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .noRandomAccess()
                .expectSize()
                .withPlan("""
                        Count
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        c
                        4
                        """);
    }

    @Test
    public void testEqualCallDesignatingConsumerTimestampStaysSeparate() throws Exception {
        assertQuery("SELECT ts, first_value(ts) OVER (ORDER BY ts) a, first_value(ts) OVER (ORDER BY ts) b FROM lp_window_ts ORDER BY b")
                .ddl("CREATE TABLE lp_window_ts AS (SELECT timestamp_sequence('2024-01-01', 3_600_000_000) ts FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY")
                .timestamp("b")
                .noRandomAccess()
                .expectSize()
                .withPlan("""
                        Window
                          functions: [first_value(ts) over (),first_value(ts) over ()]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_window_ts
                        """)
                .returns("""
                        ts\ta\tb
                        2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:00.000000Z
                        2024-01-01T01:00:00.000000Z\t2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:00.000000Z
                        2024-01-01T02:00:00.000000Z\t2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:00.000000Z
                        """);
    }

    @Test
    public void testEqualCallsOfLowerWindowMergeIntoOneUpperCall() throws Exception {
        assertQuery("SELECT id, sum(v) OVER (PARTITION BY grp ORDER BY id) a, sum(v) OVER (PARTITION BY grp ORDER BY id) b, " +
                "sum(a) OVER (ORDER BY id) c, sum(b) OVER (ORDER BY id) d FROM lp_window_prune")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .expectSize()
                .withPlan("""
                        SelectedRecord
                            CachedWindowLight
                              orderedFunctions: [[id] => [sum(a) over (rows between unbounded preceding and current row)]]
                                CachedWindowLight
                                  orderedFunctions: [[id] => [sum(v) over (partition by [grp] rows between unbounded preceding and current row)]]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        id\ta\tb\tc\td
                        3\t40.0\t40.0\t70.0\t70.0
                        1\t10.0\t10.0\t10.0\t10.0
                        2\t20.0\t20.0\t30.0\t30.0
                        4\t60.0\t60.0\t130.0\t130.0
                        """);
    }

    @Test
    public void testEqualCallsOfNestedAndTopLevelWindowsComputeOnce() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT id, sum(v) OVER (ORDER BY id) a, sum(sum(v) OVER (ORDER BY id)) OVER (ORDER BY id) b FROM lp_window_prune")
                    .expectSize()
                    .withPlan("""
                            CachedWindowLight
                              orderedFunctions: [[id] => [sum(sum) over (rows between unbounded preceding and current row)]]
                                CachedWindowLight
                                  orderedFunctions: [[id] => [sum(v) over (rows between unbounded preceding and current row)]]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_window_prune
                            """)
                    .returns("""
                            id\ta\tb
                            3\t60.0\t100.0
                            1\t10.0\t10.0
                            2\t30.0\t40.0
                            4\t100.0\t200.0
                            """);
        });
    }

    @Test
    public void testEqualCallsOrderedByEqualAliasesComputeOnce() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT id + 1 a, id + 1 b, sum(v) OVER (ORDER BY a) s1, sum(v) OVER (ORDER BY b) s2 FROM lp_window_prune")
                    .expectSize()
                    .withPlan("""
                            SelectedRecord
                                CachedWindowLight
                                  orderedFunctions: [[a] => [sum(v) over (rows between unbounded preceding and current row)]]
                                    VirtualRecord
                                      functions: [id+1,id+1,v]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_window_prune
                            """)
                    .returns("""
                            a\tb\ts1\ts2
                            4\t4\t60.0\t60.0
                            2\t2\t10.0\t10.0
                            3\t3\t30.0\t30.0
                            5\t5\t100.0\t100.0
                            """);
        });
    }

    @Test
    public void testEqualTopLevelCallsMergeIntoOneNestedCall() throws Exception {
        assertQuery("SELECT id, sum(v) OVER (ORDER BY id) a, sum(sum(v) OVER (ORDER BY id)) OVER (ORDER BY id) b, sum(v) OVER (ORDER BY id) c FROM lp_window_prune")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .expectSize()
                .withPlan("""
                        CachedWindowLight
                          orderedFunctions: [[id] => [sum(sum) over (rows between unbounded preceding and current row)]]
                            CachedWindowLight
                              orderedFunctions: [[id] => [sum(v) over (rows between unbounded preceding and current row)]]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        id\ta\tb\tc
                        3\t60.0\t100.0\t60.0
                        1\t10.0\t10.0\t10.0
                        2\t30.0\t40.0\t30.0
                        4\t100.0\t200.0\t100.0
                        """);
    }

    @Test
    public void testEqualWindowCallsComputeOnce() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT id, sum(v) OVER (PARTITION BY grp) a, sum(v) OVER (PARTITION BY grp) b, sum(lp_window_prune.v) OVER (PARTITION BY grp) c FROM lp_window_prune")
                    .expectSize()
                    .withPlan("""
                            SelectedRecord
                                CachedWindowLight
                                  unorderedFunctions: [sum(v) over (partition by [grp])]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_window_prune
                            """)
                    .returns("""
                            id\ta\tb\tc
                            3\t40.0\t40.0\t40.0
                            1\t40.0\t40.0\t40.0
                            2\t60.0\t60.0\t60.0
                            4\t60.0\t60.0\t60.0
                            """);
        });
    }

    @Test
    public void testEqualWindowCallsMergeUnderAggregate() throws Exception {
        assertQuery("SELECT a = b eq, count() FROM (SELECT sum(v) OVER (PARTITION BY grp) a, sum(v) OVER (PARTITION BY grp) b FROM lp_window_prune)")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .expectSize()
                .withPlan("""
                        GroupBy vectorized: false
                          keys: [eq]
                          values: [count(*)]
                            SelectedRecord
                                CachedWindowLight
                                  unorderedFunctions: [sum(v) over (partition by [grp])]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        eq\tcount
                        true\t4
                        """);
    }

    @Test
    public void testEqualWindowCallsMergeUnderFilter() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT * FROM (SELECT id, row_number() OVER (ORDER BY v) a, row_number() OVER (ORDER BY v) b FROM lp_window_prune) WHERE b > 2")
                    .withPlan("""
                            SelectedRecord
                                Filter filter: 2<a
                                    CachedWindowLight
                                      orderedFunctions: [[v] => [row_number()]]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_window_prune
                            """)
                    .returns("""
                            id\ta\tb
                            3\t3\t3
                            4\t4\t4
                            """);
        });
    }

    @Test
    public void testEqualWindowCallsReadByOneExpressionComputeOnce() throws Exception {
        assertQuery("SELECT id, sum(v) OVER (PARTITION BY grp) + sum(v) OVER (PARTITION BY grp) * sum(v) OVER (PARTITION BY grp) s FROM lp_window_prune")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .expectSize()
                .withPlan("""
                        VirtualRecord
                          functions: [id,sum+sum*sum]
                            CachedWindowLight
                              unorderedFunctions: [sum(v) over (partition by [grp])]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        id\ts
                        3\t1640.0
                        1\t1640.0
                        2\t3660.0
                        4\t3660.0
                        """);
    }

    @Test
    public void testFilterCannotChangeWindowPartition() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryBothPaths("SELECT id,rn FROM (SELECT id,row_number() OVER (ORDER BY id) rn FROM lp_window_prune) WHERE id>=3 ORDER BY id",
                    "id\trn\n3\t3\n4\t4\n");
        });
    }

    @Test
    public void testHiddenPartitionAndOrderColumnsSurvivePruning() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryBothPaths("SELECT rn FROM (SELECT id,row_number() OVER (PARTITION BY grp ORDER BY v DESC) rn FROM lp_window_prune) ORDER BY rn",
                    "rn\n1\n1\n2\n2\n");
        });
    }

    @Test
    public void testInputOrderIsPreservedThroughWindowBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryBothPaths("SELECT id,prev FROM (SELECT id,lag(id) OVER () prev FROM (SELECT id FROM lp_window_prune ORDER BY id DESC)) WHERE id<4",
                    "id\tprev\n3\t4\n2\t3\n1\t2\n");
        });
    }

    @Test
    public void testInsertSelectUsesWindowSource() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            execute("CREATE TABLE lp_window_insert (id INT,rn LONG)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                execute(compiler, "INSERT INTO lp_window_insert SELECT id,row_number() OVER (ORDER BY id) FROM lp_window_prune");
                Assert.assertNotNull(compiler.getPlanForTesting());
                assertWindowLayouts(compiler.getPlanForTesting());
            }
            assertQuery("SELECT * FROM lp_window_insert ORDER BY id").expectSize().returns("id\trn\n1\t1\n2\t2\n3\t3\n4\t4\n");
        });
    }

    @Test
    public void testMergedWindowCallReadThroughUnreadAlias() throws Exception {
        assertQuery("SELECT id, b FROM (SELECT id, sum(v) OVER (PARTITION BY grp) a, sum(v) OVER (PARTITION BY grp) b FROM lp_window_prune)")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .expectSize()
                .withPlan("""
                        CachedWindowLight
                          unorderedFunctions: [sum(v) over (partition by [grp])]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        id\tb
                        3\t40.0
                        1\t40.0
                        2\t60.0
                        4\t60.0
                        """);
    }

    @Test
    public void testMergedWindowCallWithUnreadAlias() throws Exception {
        assertQuery("SELECT id, a FROM (SELECT id, sum(v) OVER (PARTITION BY grp) a, sum(v) OVER (PARTITION BY grp) b FROM lp_window_prune)")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .expectSize()
                .withPlan("""
                        CachedWindowLight
                          unorderedFunctions: [sum(v) over (partition by [grp])]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        id\ta
                        3\t40.0
                        1\t40.0
                        2\t60.0
                        4\t60.0
                        """);
    }

    @Test
    public void testNestedWindowReadByKeptAndUnreadCallsSurvives() throws Exception {
        assertQuery("""
                SELECT id, b FROM (
                    SELECT id, sum(row_number() OVER (ORDER BY v)) OVER (ORDER BY id) b,
                        sum((row_number() OVER (ORDER BY v) * 10)::DECIMAL(1,0)) OVER () c
                    FROM lp_window_prune
                )""")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .expectSize()
                .withPlan("""
                        CachedWindowLight
                          orderedFunctions: [[id] => [sum(row_number) over (rows between unbounded preceding and current row)]]
                            CachedWindowLight
                              orderedFunctions: [[v] => [row_number()]]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        id\tb
                        3\t6.0
                        1\t1.0
                        2\t3.0
                        4\t10.0
                        """);
    }

    @Test
    public void testNestedWindowReadOnlyByUnreadCallIsRemoved() throws Exception {
        assertQuery("SELECT id, x FROM (SELECT id, sum((row_number() OVER (ORDER BY v) * 10)::DECIMAL(1,0)) OVER () s, sum(v) OVER () x FROM lp_window_prune)")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .expectSize()
                .withPlan("""
                        CachedWindowLight
                          unorderedFunctions: [sum(v) over ()]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        id\tx
                        3\t100.0
                        1\t100.0
                        2\t100.0
                        4\t100.0
                        """);
    }

    @Test
    public void testNestedWindowsKeepSeparateInputLayouts() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryBothPaths("SELECT id,rn,lag(rn) OVER (ORDER BY id) prev FROM (SELECT id,row_number() OVER (PARTITION BY grp ORDER BY v) rn FROM lp_window_prune) ORDER BY id",
                    "id\trn\tprev\n1\t1\tnull\n2\t1\t1\n3\t2\t1\n4\t2\t2\n");
        });
    }

    @Test
    public void testNestedWindowsWithEveryCallUnreadAreRemoved() throws Exception {
        assertQuery("SELECT id FROM (SELECT id, sum((row_number() OVER (ORDER BY v) * 10)::DECIMAL(1,0)) OVER (ORDER BY id) b FROM lp_window_prune)")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .expectSize()
                .withPlan("""
                        PageFrame
                            Row forward scan
                            Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        id
                        3
                        1
                        2
                        4
                        """);
    }

    @Test
    public void testUnreadWindowCallBetweenReadCallsIsDropped() throws Exception {
        assertQuery("""
                SELECT id, a, c FROM (
                    SELECT id, row_number() OVER (PARTITION BY grp ORDER BY v DESC) a,
                        sum(unused::DECIMAL(1,0)) OVER (PARTITION BY unused ORDER BY id) b,
                        sum(v) OVER () c
                    FROM lp_window_prune
                )""")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .expectSize()
                .withPlan("""
                        CachedWindowLight
                          orderedFunctions: [[v desc] => [row_number() over (partition by [grp])]]
                          unorderedFunctions: [sum(v) over ()]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        id\ta\tc
                        3\t1\t100.0
                        1\t2\t100.0
                        2\t2\t100.0
                        4\t1\t100.0
                        """);
    }

    @Test
    public void testUnreadWindowCallIsNotEvaluated() throws Exception {
        assertQuery("SELECT id FROM (SELECT id, sum(id::DECIMAL(2,0)) OVER () s FROM wp) ORDER BY id")
                .ddl("CREATE TABLE wp (id INT)", "INSERT INTO wp VALUES (1), (2), (3), (100)")
                .expectSize()
                .withPlan("""
                        Encode sort light
                          keys: [id]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: wp
                        """)
                .returns("""
                        id
                        1
                        2
                        3
                        100
                        """);
    }

    @Test
    public void testUnreadWindowCallSharingInputWithReadCallIsDropped() throws Exception {
        assertQuery("SELECT id, a FROM (SELECT id, sum(v) OVER (ORDER BY id) a, sum(v::DECIMAL(1,0)) OVER (PARTITION BY grp) b FROM lp_window_prune)")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .expectSize()
                .withPlan("""
                        CachedWindowLight
                          orderedFunctions: [[id] => [sum(v) over (rows between unbounded preceding and current row)]]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        id\ta
                        3\t60.0
                        1\t10.0
                        2\t30.0
                        4\t100.0
                        """);
    }

    @Test
    public void testUnreadWindowCallUnderReadFilterIsDropped() throws Exception {
        assertQuery("SELECT id FROM (SELECT id, row_number() OVER (ORDER BY v) rn, sum(unused::DECIMAL(1,0)) OVER () b FROM lp_window_prune) WHERE rn > 2")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .withPlan("""
                        SelectedRecord
                            Filter filter: 2<rn
                                CachedWindowLight
                                  orderedFunctions: [[v] => [row_number()]]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        id
                        3
                        4
                        """);
    }

    @Test
    public void testVolatileUnreadWindowCallIsDropped() throws Exception {
        assertQuery("SELECT id FROM (SELECT id, sum(rnd_int(1, 1_000_000, 0)) OVER () r FROM lp_window_prune)")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .expectSize()
                .withPlan("""
                        PageFrame
                            Row forward scan
                            Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        id
                        3
                        1
                        2
                        4
                        """);
    }

    @Test
    public void testVolatileWindowCallsDoNotMerge() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT a = b eq, count() FROM (SELECT first_value(rnd_int(1, 1_000_000, 0)) OVER (ORDER BY id ROWS CURRENT ROW) a, " +
                    "first_value(rnd_int(1, 1_000_000, 0)) OVER (ORDER BY id ROWS CURRENT ROW) b FROM lp_window_prune)")
                    .expectSize()
                    .withPlan("""
                            GroupBy vectorized: false
                              keys: [eq]
                              values: [count(*)]
                                CachedWindowLight
                                  orderedFunctions: [[id] => [first_value(rnd_int(1,1000000,0)) over (),first_value(rnd_int(1,1000000,0)) over ()]]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_window_prune
                            """)
                    .returns("""
                            eq\tcount
                            false\t4
                            """);
        });
    }

    @Test
    public void testWindowUnderAggregateWithEveryCallUnreadIsRemoved() throws Exception {
        assertQuery("SELECT grp, c FROM (SELECT grp, max((row_number() OVER (ORDER BY v) * 10)::DECIMAL(1,0)) m, count() c FROM lp_window_prune) ORDER BY grp")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .expectSize()
                .withPlan("""
                        Encode sort light
                          keys: [grp]
                            GroupBy vectorized: true workers: 1
                              keys: [grp]
                              values: [count(*)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        grp\tc
                        a\t2
                        b\t2
                        """);
    }

    @Test
    public void testWindowUnderFilterWithEveryCallUnreadIsRemoved() throws Exception {
        assertQuery("SELECT id FROM (SELECT id, sum(unused::DECIMAL(1,0)) OVER () s FROM lp_window_prune) WHERE id > 2")
                .ddl(CREATE_TABLE, INSERT_ROWS)
                .withPlan("""
                        Async JIT Filter workers: 1
                          filter: 2<id
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_window_prune
                        """)
                .returns("""
                        id
                        3
                        4
                        """);
    }

    @Test
    public void testWideDistinctWindowCallsCompute() throws Exception {
        final StringSink expected = new StringSink();
        for (int i = 0; i < 1_024; i++) {
            expected.put(i > 0 ? "\tw" : "w").put(i);
        }
        expected.put('\n');
        for (int i = 0; i < 1_024; i++) {
            expected.put(i > 0 ? "\tnull" : "null");
        }
        expected.put('\n');
        assertQuery(wideDistinctWindowCalls(1_024))
                .noRandomAccess()
                .expectSize()
                .returns(expected);
    }

    @Test
    public void testWideWindowCallsMergeInLinearComparisons() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int windowCount = 1_024; windowCount <= 4_096; windowCount *= 4) {
                    compile(compiler, wideIdenticalWindowCalls(windowCount));
                    final long identicalComparisons = compiler.getWindowCallComparisonCount();
                    compile(compiler, wideDistinctWindowCalls(windowCount));
                    final long distinctComparisons = compiler.getWindowCallComparisonCount();
                    Assert.assertTrue("window call comparisons grow superlinearly [windows=" + windowCount + ", identical=" + identicalComparisons
                            + ", distinct=" + distinctComparisons + ']', identicalComparisons < 2L * windowCount && distinctComparisons < 2L * windowCount);
                }
            }
        });
    }

    @Test
    public void testWideIdenticalWindowCallsMergeInLinearPlanColumns() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                final int planColumns = assertWideIdenticalWindowCalls(compiler, 1_024);
                final int widerPlanColumns = assertWideIdenticalWindowCalls(compiler, 2_048);
                Assert.assertTrue("plan columns grow superlinearly with the window count [1024=" + planColumns + ", 2048=" + widerPlanColumns + ']',
                        widerPlanColumns <= 2 * planColumns);
            }
        });
    }

    private static void assertWindowLayouts(LogicalPlan plan) {
        if (plan instanceof WindowPlan) {
            final WindowPlan window = (WindowPlan) plan;
            final OutputSchema input = window.getChild().getOutput();
            final OutputSchema output = window.getOutput();
            Assert.assertEquals(input.getColumnCount() + window.getFunctions().size(), output.getColumnCount());
            for (int i = 0, n = input.getColumnCount(); i < n; i++) {
                Assert.assertEquals(input.getColumnId(i), output.getColumnId(i));
            }
            for (int i = 0, n = window.getFunctions().size(); i < n; i++) {
                Assert.assertEquals(window.getFunctionColumnIds().getQuick(i), output.getColumnId(input.getColumnCount() + i));
            }
            Assert.assertEquals(-1, input.getColumnIndexQuiet("unused"));
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            assertWindowLayouts(plan.inputAt(i));
        }
    }

    private static void compile(SqlCompilerImpl compiler, CharSequence sql) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.assertNotNull(factory);
        }
    }

    private static StringSink wideDistinctWindowCalls(int windowCount) {
        final StringSink sql = new StringSink();
        sql.put("SELECT ");
        for (int i = 0; i < windowCount; i++) {
            sql.put(i > 0 ? ", lag(x, " : "lag(x, ").put(i + 1).put(") OVER () w").put(i);
        }
        sql.put(" FROM long_sequence(1)");
        return sql;
    }

    private static StringSink wideIdenticalWindowCalls(int windowCount) {
        final StringSink sql = new StringSink();
        sql.put("SELECT ");
        for (int i = 0; i < windowCount; i++) {
            sql.put(i > 0 ? ", sum(x) OVER () w" : "sum(x) OVER () w").put(i);
        }
        sql.put(" FROM long_sequence(1)");
        return sql;
    }

    private void assertQueryBothPaths(String sql, String expected) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
            }
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                assertWindowLayouts(compiler.getPlanForTesting());
                assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
            }
        }
    }

    private int assertWideIdenticalWindowCalls(SqlCompilerImpl compiler, int windowCount) throws Exception {
        final StringSink expected = new StringSink();
        for (int i = 0; i < windowCount; i++) {
            expected.put(i > 0 ? "\tw" : "w").put(i);
        }
        expected.put('\n');
        for (int i = 0; i < windowCount; i++) {
            expected.put(i > 0 ? "\t1.0" : "1.0");
        }
        expected.put('\n');
        final StringSink sql = wideIdenticalWindowCalls(windowCount);
        assertQuery(sql)
                .noLeakCheck()
                .expectSize()
                .withPlan("""
                        SelectedRecord
                            CachedWindowLight
                              unorderedFunctions: [sum(x) over ()]
                                long_sequence count: 1
                        """)
                .returns(expected);
        compile(compiler, sql);
        return compiler.getPlanColumnPoolCapacity();
    }

    private void createRows() throws Exception {
        execute(CREATE_TABLE);
        execute(INSERT_ROWS);
    }
}
