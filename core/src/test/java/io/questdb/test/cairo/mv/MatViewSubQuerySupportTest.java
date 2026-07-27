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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Accept-side regression corpus for sub-queries inside materialized views.
 * <p>
 * Every shape here compiled on master. The mat-view guard rejects only sub-queries that read a
 * source outside the database, so all of them must keep compiling. Each shape runs under the
 * cross-product of the parallelism flags that change which {@code RecordCursorFactory} the code
 * generator picks, because a guard that consults a per-factory property is otherwise trivially
 * config-dependent: the same DDL would be legal on a 2-vCPU node and illegal on a 4-vCPU one.
 *
 * @see MatViewExternalSourceRejectionTest for the matching reject-side corpus
 * @see MatViewGuardConfigInvarianceTest for the verdict-invariance property
 */
@RunWith(Parameterized.class)
public class MatViewSubQuerySupportTest extends AbstractCairoTest {
    private static final AtomicInteger VIEW_SEQ = new AtomicInteger();
    private final boolean parallelFilter;
    private final boolean parallelGroupBy;

    public MatViewSubQuerySupportTest(boolean parallelGroupBy, boolean parallelFilter) {
        this.parallelGroupBy = parallelGroupBy;
        this.parallelFilter = parallelFilter;
    }

    @Parameterized.Parameters(name = "parallelGroupBy={0},parallelFilter={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {true, true},
                {true, false},
                {false, true},
                {false, false},
        });
    }

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(parallelGroupBy));
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, String.valueOf(parallelFilter));
        super.setUp();
    }

    @Test
    public void testAggregateSubQueries() throws Exception {
        assertAccepted(
                "ts > (SELECT max(lim) FROM cfg)",
                "ts > (SELECT min(lim) FROM cfg)",
                "n > (SELECT count() FROM cfg)",
                "n > (SELECT sum(n) FROM cfg)",
                "n > (SELECT avg(n) FROM cfg)",
                "n > (SELECT count_distinct(k) FROM cfg)"
        );
    }

    @Test
    public void testDistinctAndSetOperationSubQueries() throws Exception {
        assertAccepted(
                "ts > (SELECT DISTINCT lim FROM cfg LIMIT 1)",
                "ts > (SELECT lim FROM cfg UNION SELECT lim FROM cfg LIMIT 1)",
                "ts > (SELECT lim FROM cfg UNION ALL SELECT lim FROM cfg LIMIT 1)",
                "ts > (SELECT lim FROM cfg EXCEPT SELECT lim FROM cfg LIMIT 1)",
                "ts > (SELECT lim FROM cfg INTERSECT SELECT lim FROM cfg LIMIT 1)"
        );
    }

    @Test
    public void testGroupBySubQueries() throws Exception {
        assertAccepted(
                "ts > (SELECT max(lim) FROM cfg GROUP BY k LIMIT 1)",
                "ts > (SELECT max(lim) FROM cfg GROUP BY k, n LIMIT 1)",
                "ts > (SELECT max(lim) FROM cfg GROUP BY concat(k, 'x') LIMIT 1)",
                "ts > (SELECT max(lim) FROM cfg WHERE n > 0 GROUP BY k LIMIT 1)"
        );
    }

    @Test
    public void testJoinSubQueries() throws Exception {
        assertAccepted(
                "ts > (SELECT max(c1.lim) FROM cfg c1 JOIN cfg c2 ON c1.k = c2.k)",
                "ts > (SELECT max(c1.lim) FROM cfg c1 LEFT JOIN cfg c2 ON c1.k = c2.k)",
                "ts > (SELECT max(c1.lim) FROM cfg c1 ASOF JOIN cfg c2)",
                "ts > (SELECT max(c1.lim) FROM cfg c1 LT JOIN cfg c2)",
                "ts > (SELECT max(c1.lim) FROM cfg c1 CROSS JOIN cfg c2)"
        );
    }

    @Test
    public void testNestedAndCteSubQueries() throws Exception {
        assertAccepted(
                "ts > (SELECT max(lim) FROM (SELECT lim FROM cfg))",
                "ts > (SELECT max(lim) FROM (SELECT lim FROM (SELECT lim FROM cfg)))",
                "ts > (SELECT max(lim) FROM (SELECT lim FROM cfg WHERE k = 'a'))",
                "ts > (SELECT max(lim) FROM (SELECT lim FROM cfg ORDER BY lim DESC LIMIT 10))"
        );
    }

    @Test
    public void testOrderLimitAndTimeSeriesSubQueries() throws Exception {
        assertAccepted(
                "ts > (SELECT lim FROM cfg ORDER BY ts DESC LIMIT 1)",
                "ts > (SELECT lim FROM cfg ORDER BY ts ASC LIMIT 1)",
                "ts > (SELECT lim FROM cfg LIMIT 1)",
                "ts > (SELECT lim FROM cfg LIMIT 1, 2)",
                "ts > (SELECT max(lim) FROM cfg SAMPLE BY 1h LIMIT 1)",
                "ts > (SELECT lim FROM cfg LATEST ON ts PARTITION BY k LIMIT 1)"
        );
    }

    @Test
    public void testPlainAndFilteredSubQueries() throws Exception {
        assertAccepted(
                "ts > (SELECT lim FROM cfg LIMIT 1)",
                "ts > (SELECT max(lim) FROM cfg WHERE k = 'a')",
                "ts > (SELECT max(lim) FROM cfg WHERE n > 0)",
                "ts > (SELECT max(lim) FROM cfg WHERE k = 'a' AND n > 0)",
                "ts > (SELECT max(lim) FROM cfg WHERE k IN ('a', 'b'))"
        );
    }

    @Test
    public void testSubQueriesInInPredicate() throws Exception {
        assertAccepted(
                "k IN (SELECT k FROM cfg)",
                "k NOT IN (SELECT k FROM cfg)",
                "k IN (SELECT k FROM cfg WHERE n > 0)",
                "k IN (SELECT DISTINCT k FROM cfg)"
        );
    }

    private void assertAccepted(String... predicates) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE, n LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE cfg (ts TIMESTAMP, k SYMBOL, lim TIMESTAMP, n LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cfg VALUES ('2024-01-01T00:00:00Z', 'a', '2024-01-01T00:00:00Z', 1)");
            drainWalQueue();
            for (String predicate : predicates) {
                final String view = "mv_" + VIEW_SEQ.incrementAndGet();
                final String sql = "CREATE MATERIALIZED VIEW " + view + " AS SELECT ts, sum(v) AS s FROM base WHERE "
                        + predicate + " SAMPLE BY 1h";
                try {
                    execute(sql);
                } catch (Throwable e) {
                    throw new AssertionError(
                            "materialized view must accept sub-query that compiled before the "
                                    + "external-source guard was introduced.\n  predicate: " + predicate
                                    + "\n  parallelGroupBy=" + parallelGroupBy + " parallelFilter=" + parallelFilter
                                    + "\n  error: " + e.getMessage(), e);
                }
            }
        });
    }
}
