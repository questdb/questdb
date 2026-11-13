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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

// An execution of a LATERAL join fails while the LATERAL reads the GROUP BY of the outer side through a shared
// cursor. The failed execution must release what the shared cursor opened, and the next execution of the same
// factory must read the table anew, as it does when the HTTP select cache runs the factory again.
@RunWith(Parameterized.class)
public class LateralJoinSharedOwnerFailureTest extends AbstractCairoTest {
    private static final String EXPECTED_KEYED_AFTER = """
            k\tn\tmv\tmv2
            1\t2\t100\t100
            2\t2\t200\t200
            3\t1\t300\t300
            4\t1\t300\tnull
            """;
    private static final String EXPECTED_KEYED_BEFORE = """
            k\tn\tmv\tmv2
            1\t1\t100\t100
            2\t2\t200\t200
            3\t1\t300\t300
            """;
    private static final String EXPECTED_NOT_KEYED_AFTER = """
            k\tn\tmv\tmv2
            4\t6\t300\tnull
            """;
    private static final String EXPECTED_NOT_KEYED_BEFORE = """
            k\tn\tmv\tmv2
            3\t4\t300\t300
            """;
    // the regex in the LATERAL compiles from the bind variable when its filter opens: after the LATERAL opened its
    // shared cursor over the GROUP BY of the outer side, and before the GROUP BY itself opened
    private static final String LATERAL = " CROSS JOIN LATERAL (SELECT max(u.v) mv FROM u WHERE u.k <= o.k AND (u.s ~ $1 OR o.k < 0)) l ORDER BY o.k";
    // The hash joins open their LATERAL side first: the shared cursor of one LATERAL opens the owner state of the
    // GROUP BY, the shared cursor of the other one finds it open, and the primary cursor claims it last.
    private static final String TWO_LATERALS = " CROSS JOIN LATERAL (SELECT max(u.v) mv FROM u WHERE u.k <= o.k) l"
            + " CROSS JOIN LATERAL (SELECT min(u.v) mv2 FROM u WHERE u.k >= o.k) l2 ORDER BY o.k";
    private final boolean enableParallelGroupBy;

    public LateralJoinSharedOwnerFailureTest(boolean enableParallelGroupBy) {
        this.enableParallelGroupBy = enableParallelGroupBy;
    }

    @Parameterized.Parameters(name = "parallel={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false},
                {true},
        });
    }

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(enableParallelGroupBy));
        super.setUp();
    }

    @Test
    public void testKeyedGroupBy() throws Exception {
        // vectorized GROUP BY in parallel, serial GROUP BY otherwise
        assertFailedExecutionReleasesSharedOwner(
                "SELECT k, count() n FROM t GROUP BY k",
                """
                        k\tn\tmv
                        1\t2\t100
                        2\t2\t200
                        3\t1\t300
                        4\t1\t300
                        """
        );
    }

    @Test
    public void testKeyedGroupByAllocationFailures() throws Exception {
        assertFaultsReleaseSharedOwner(
                "SELECT k, count() n FROM t GROUP BY k",
                enableParallelGroupBy ? "GroupBy vectorized: true" : "GroupBy vectorized: false",
                EXPECTED_KEYED_BEFORE,
                EXPECTED_KEYED_AFTER,
                LateralJoinSharedOwnerFailureTest::executeUnderRssCeiling
        );
    }

    @Test
    public void testKeyedGroupByNotVectorized() throws Exception {
        // Async Group By in parallel, serial GROUP BY otherwise
        assertFailedExecutionReleasesSharedOwner(
                "SELECT k, count_distinct(v) n FROM t GROUP BY k",
                """
                        k\tn\tmv
                        1\t2\t100
                        2\t2\t200
                        3\t1\t300
                        4\t1\t300
                        """
        );
    }

    @Test
    public void testKeyedGroupByNotVectorizedAllocationFailures() throws Exception {
        assertFaultsReleaseSharedOwner(
                "SELECT k, count_distinct(v) n FROM t GROUP BY k",
                enableParallelGroupBy ? "Async Group By" : "GroupBy vectorized: false",
                EXPECTED_KEYED_BEFORE,
                EXPECTED_KEYED_AFTER,
                LateralJoinSharedOwnerFailureTest::executeUnderRssCeiling
        );
    }

    @Test
    public void testKeyedGroupByNotVectorizedInitFailures() throws Exception {
        // in dev mode, test_fault() returns true, so the aggregate counts the distinct values of v
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertFaultsReleaseSharedOwner(
                "SELECT k, count_distinct(CASE WHEN test_fault() THEN v END) n FROM t GROUP BY k",
                enableParallelGroupBy ? "Async Group By" : "GroupBy vectorized: false",
                EXPECTED_KEYED_BEFORE,
                EXPECTED_KEYED_AFTER,
                LateralJoinSharedOwnerFailureTest::executeWithInitFault
        );
    }

    @Test
    public void testNotKeyedGroupBy() throws Exception {
        // not keyed Async Group By in parallel, not keyed serial GROUP BY otherwise
        assertFailedExecutionReleasesSharedOwner(
                "SELECT max(k) k, count_distinct(v) n FROM t",
                """
                        k\tn\tmv
                        4\t6\t300
                        """
        );
    }

    @Test
    public void testNotKeyedGroupByAllocationFailures() throws Exception {
        assertFaultsReleaseSharedOwner(
                "SELECT max(k) k, count_distinct(v) n FROM t",
                enableParallelGroupBy ? "Async Group By" : "GroupBy vectorized: false",
                EXPECTED_NOT_KEYED_BEFORE,
                EXPECTED_NOT_KEYED_AFTER,
                LateralJoinSharedOwnerFailureTest::executeUnderRssCeiling
        );
    }

    @Test
    public void testNotKeyedGroupByInitFailures() throws Exception {
        // in dev mode, test_fault() returns true, so the aggregate counts the distinct values of v
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertFaultsReleaseSharedOwner(
                "SELECT max(k) k, count_distinct(CASE WHEN test_fault() THEN v END) n FROM t",
                enableParallelGroupBy ? "Async Group By" : "GroupBy vectorized: false",
                EXPECTED_NOT_KEYED_BEFORE,
                EXPECTED_NOT_KEYED_AFTER,
                LateralJoinSharedOwnerFailureTest::executeWithInitFault
        );
    }

    private static void assertExecutionFails(RecordCursorFactory factory, String query) throws SqlException {
        bindVariableService.setStr(0, "(");
        try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
            Assert.fail("the pattern must not compile");
        } catch (SqlException e) {
            // the error points into the pattern: past $1 by the index of the error in the pattern plus one
            Assert.assertEquals(query.indexOf("$1") + 2, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "Unclosed group near index 1");
        }
        // the failed execution keeps no reader open, the one of t that the shared cursor opened included
        Assert.assertEquals(0, engine.getBusyReaderCount());
    }

    private static void createTables() throws SqlException {
        execute("CREATE TABLE t (k INT, v INT)");
        execute("INSERT INTO t VALUES (1, 10), (2, 20), (2, 21), (3, 30)");
        execute("CREATE TABLE u (k INT, v INT, s STRING)");
        execute("INSERT INTO u VALUES (1, 100, 'a'), (2, 200, 'b'), (3, 300, 'c')");
    }

    // An RSS ceiling above the native memory in use fails the first allocation of the execution that the slack does
    // not fit. The next point fits that allocation, so that the walk fails the allocations of the execution in turn.
    private static long executeUnderRssCeiling(RecordCursorFactory factory, long slack, StringSink rows) throws SqlException {
        RecordCursor cursor = null;
        final long rssBefore = Unsafe.getRssMemUsed();
        Unsafe.setRssMemLimit(rssBefore + slack);
        try {
            cursor = factory.getCursor(sqlExecutionContext);
            CursorPrinter.println(cursor, factory.getMetadata(), rows);
            return -1;
        } catch (CairoException e) {
            final String message = e.getFlyweightMessage().toString();
            Assert.assertTrue(message, e.isOutOfMemory());
            TestUtils.assertContains(message, "global RSS memory limit exceeded");
            return Math.max(slack + 1, messageValue(message, "usage=") + messageValue(message, "size=") - rssBefore);
        } finally {
            // disarm before the cursor closes, so that its close cannot trip the ceiling
            Unsafe.setRssMemLimit(0);
            Misc.free(cursor);
        }
    }

    // test_fault() fails the initialization that follows the given number of successful ones
    private static long executeWithInitFault(RecordCursorFactory factory, long successfulInits, StringSink rows) throws SqlException {
        RecordCursor cursor = null;
        TestFaultFunctionFactory.armToFailAfterInits((int) successfulInits);
        try {
            cursor = factory.getCursor(sqlExecutionContext);
            CursorPrinter.println(cursor, factory.getMetadata(), rows);
            Assert.assertEquals(0, TestFaultFunctionFactory.faultsTriggered());
            return -1;
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "test_fault: injected init failure");
            Assert.assertEquals(1, TestFaultFunctionFactory.faultsTriggered());
            return successfulInits + 1;
        } finally {
            TestFaultFunctionFactory.disarm();
            Misc.free(cursor);
        }
    }

    private static long messageValue(String message, String key) {
        final int lo = message.indexOf(key) + key.length();
        Assert.assertTrue(message, lo >= key.length());
        int hi = lo;
        while (hi < message.length() && Character.isDigit(message.charAt(hi))) {
            hi++;
        }
        return Long.parseLong(message, lo, hi, 10);
    }

    private void assertFailedExecutionReleasesSharedOwner(String outer, String expected) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String query = "SELECT o.k, o.n, l.mv FROM (" + outer + ") o" + LATERAL;
            bindVariableService.clear();
            bindVariableService.setStr(0, ".*");
            assertQuery(query).noLeakCheck().assertsPlanContaining("(Shared)");
            try (RecordCursorFactory factory = select(query)) {
                assertExecutionFails(factory, query);
                // the next execution must see the rows that the failed one could not
                execute("INSERT INTO t VALUES (4, 40), (1, 11)");
                bindVariableService.setStr(0, ".*");
                new QueryAssertion(engine, factory)
                        .withContext(sqlExecutionContext)
                        .returns(expected);
                // the factory closes right after a failed execution
                assertExecutionFails(factory, query);
            }
        });
    }

    // Walks the fault across the execution, from its first point to the first one that the execution survives. At
    // each point, one factory closes right after its failed execution, and another one runs again after it.
    private void assertFaultsReleaseSharedOwner(
            String outer,
            String groupBy,
            String expectedBefore,
            String expectedAfter,
            Fault fault
    ) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String query = "SELECT o.k, o.n, l.mv, l2.mv2 FROM (" + outer + ") o" + TWO_LATERALS;
            assertQuery(query).noLeakCheck().assertsPlanContaining("(Shared)", groupBy);
            // the readers are warm, so that both executions at a point allocate alike
            assertQuery(query).noLeakCheck().returns(expectedBefore);
            final StringSink rows = new StringSink();
            int failureCount = 0;
            long point = 0;
            while (true) {
                final long memUsedBefore = Unsafe.getMemUsed();
                final long nextPoint;
                try (RecordCursorFactory factory = select(query)) {
                    nextPoint = fault.execute(factory, point, rows);
                }
                // the execution and the factory leave no native memory behind
                Assert.assertEquals("memory after the execution at point " + point, memUsedBefore, Unsafe.getMemUsed());
                if (nextPoint < 0) {
                    break;
                }
                failureCount++;
                // the failed execution keeps no reader open, the ones of t that the shared cursors opened included
                Assert.assertEquals("busy readers after the failure at point " + point, 0, engine.getBusyReaderCount());
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertTrue("the second execution survived point " + point, fault.execute(factory, point, rows) >= 0);
                    Assert.assertEquals("busy readers after the failure at point " + point, 0, engine.getBusyReaderCount());
                    // the next execution must see the rows that the failed one could not
                    execute("INSERT INTO t VALUES (4, 40), (1, 11)");
                    new QueryAssertion(engine, factory)
                            .withContext(sqlExecutionContext)
                            .returns(expectedAfter);
                }
                // the next point starts from the same rows, and the query warms the reader of t again
                execute("TRUNCATE TABLE t");
                execute("INSERT INTO t VALUES (1, 10), (2, 20), (2, 21), (3, 30)");
                assertQuery(query).noLeakCheck().returns(expectedBefore);
                point = nextPoint;
            }
            Assert.assertTrue("the fault failed no execution", failureCount > 0);
            // the execution that survived the fault returns the rows
            TestUtils.assertEquals(expectedBefore, rows);
        });
    }

    @FunctionalInterface
    private interface Fault {
        // Runs an execution of the factory with the fault at the given point, and prints the rows of an execution
        // that survives it. Returns the next point, or -1 when the execution survived the point.
        long execute(RecordCursorFactory factory, long point, StringSink rows) throws SqlException;
    }
}
