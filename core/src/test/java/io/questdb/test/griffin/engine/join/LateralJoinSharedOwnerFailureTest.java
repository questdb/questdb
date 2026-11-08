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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
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

// An execution of a LATERAL join fails after the LATERAL opened its shared cursor over the GROUP BY of the outer
// side and before the GROUP BY itself opened: the regex in the LATERAL compiles from the bind variable when its
// filter opens. The failed execution must release what the shared cursor opened, and the next execution of the
// same factory must read the table anew, as it does when the HTTP select cache runs the factory again.
@RunWith(Parameterized.class)
public class LateralJoinSharedOwnerFailureTest extends AbstractCairoTest {
    private static final String LATERAL = " CROSS JOIN LATERAL (SELECT max(u.v) mv FROM u WHERE u.k <= o.k AND (u.s ~ $1 OR o.k < 0)) l ORDER BY o.k";
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

    private void assertFailedExecutionReleasesSharedOwner(String outer, String expected) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (2, 21), (3, 30)");
            execute("CREATE TABLE u (k INT, v INT, s STRING)");
            execute("INSERT INTO u VALUES (1, 100, 'a'), (2, 200, 'b'), (3, 300, 'c')");
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
}
