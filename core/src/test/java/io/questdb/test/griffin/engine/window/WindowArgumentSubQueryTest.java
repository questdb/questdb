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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class WindowArgumentSubQueryTest extends AbstractCairoTest {
    private final boolean isCachedLightEnabled;

    public WindowArgumentSubQueryTest(boolean isCachedLightEnabled) {
        this.isCachedLightEnabled = isCachedLightEnabled;
    }

    @Parameterized.Parameters(name = "cachedLight={0}")
    public static Collection<Boolean> parameters() {
        return List.of(false, true);
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(isCachedLightEnabled));
    }

    @Test
    public void testCompilerReuseAfterInvalidSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final WindowContext parent = sqlExecutionContext.getWindowContext();
            final boolean isTimestampRequired = sqlExecutionContext.isTimestampRequired();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // Reject a genuinely invalid range inside the nested compilation, then verify
                // that cleanup restores the caller's context and leaves the compiler reusable.
                try {
                    compiler.compile("""
                            SELECT sum(CASE WHEN s IN (SELECT rnd_str(2, 1, 0) FROM lookup) THEN x ELSE 0 END)
                                   OVER (PARTITION BY s) AS total
                            FROM t
                            """, sqlExecutionContext);
                    Assert.fail("expected the sub-query to reject the invalid rnd_str range");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "invalid range");
                }
                Assert.assertSame(parent, sqlExecutionContext.getWindowContext());
                Assert.assertTrue(parent.isEmpty());
                Assert.assertEquals(isTimestampRequired, sqlExecutionContext.isTimestampRequired());
                assertQuery("""
                        SELECT sum(CASE WHEN s IN (SELECT s FROM lookup) THEN x ELSE 0 END)
                               OVER (PARTITION BY s) AS total
                        FROM t
                        """)
                        .noLeakCheck()
                        .withCompiler(compiler)
                        .expectSize()
                        .returns("""
                                total
                                4.0
                                0.0
                                4.0
                                0.0
                                """);
                Assert.assertSame(parent, sqlExecutionContext.getWindowContext());
                Assert.assertTrue(parent.isEmpty());
            }
        });
    }

    @Test
    public void testMultipleWindowsWithNestedWindowSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT ts, s,
                           sum(x) OVER (PARTITION BY s ORDER BY x DESC) AS total,
                           sum(CASE WHEN s IN (
                               SELECT s FROM (SELECT s, row_number() OVER (ORDER BY s) AS rn FROM lookup) WHERE rn = 1
                           ) THEN x ELSE 0 END) OVER (PARTITION BY s ORDER BY x DESC) AS matched,
                           avg(x) OVER (PARTITION BY s) AS mean
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\ts\ttotal\tmatched\tmean
                            2024-01-01T00:00:00.000000Z\ta\t4.0\t4.0\t2.0
                            2024-01-01T00:00:01.000000Z\tb\t2.0\t0.0\t2.0
                            2024-01-01T00:00:02.000000Z\ta\t3.0\t3.0\t2.0
                            2024-01-01T00:00:03.000000Z\t\t4.0\t0.0\t4.0
                            """);
        });
    }

    @Test
    public void testNestedWindowSubQueries() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT sum(CASE WHEN s IN (
                        SELECT s FROM (
                            SELECT s, sum(CASE WHEN s IN (
                                SELECT s FROM (SELECT s, row_number() OVER () AS rn FROM lookup) WHERE rn = 1
                            ) THEN 1 ELSE 0 END) OVER (PARTITION BY s) AS hits
                            FROM lookup
                        ) WHERE hits > 0
                    ) THEN x ELSE 0 END) OVER (PARTITION BY s) AS total
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            total
                            4.0
                            0.0
                            4.0
                            0.0
                            """);
        });
    }

    @Test
    public void testSumPartitionedAggregateSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT sum(CASE WHEN s IN (SELECT max(s) FROM lookup) THEN x ELSE 0 END)
                           OVER (PARTITION BY s) AS total
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            total
                            4.0
                            0.0
                            4.0
                            0.0
                            """);
        });
    }

    @Test
    public void testSumPartitionedEmptySubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE empty_lookup (s SYMBOL)");
            assertQuery("""
                    SELECT sum(CASE WHEN s IN (SELECT s FROM empty_lookup) THEN x ELSE 0 END)
                           OVER (PARTITION BY s) AS total
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            total
                            0.0
                            0.0
                            0.0
                            0.0
                            """);
        });
    }

    @Test
    public void testSumPartitionedFilteredSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT sum(CASE WHEN s IN (SELECT s FROM t WHERE x = 2) THEN x ELSE 0 END)
                           OVER (PARTITION BY s) AS total
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            total
                            0.0
                            2.0
                            0.0
                            0.0
                            """);
        });
    }

    @Test
    public void testSumPartitionedLeadSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT sum(CASE WHEN s IN (
                        SELECT next_s FROM (SELECT x, lead(s) OVER (ORDER BY x) AS next_s FROM t) WHERE x = 1
                    ) THEN x ELSE 0 END) OVER (PARTITION BY s) AS total
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            total
                            0.0
                            2.0
                            0.0
                            0.0
                            """);
        });
    }

    @Test
    public void testSumPartitionedLiteralList() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT sum(CASE WHEN s IN ('a') THEN x ELSE 0 END)
                           OVER (PARTITION BY s) AS total
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            total
                            4.0
                            0.0
                            4.0
                            0.0
                            """);
        });
    }

    @Test
    public void testSumPartitionedSubQueryNullValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("""
                    INSERT INTO t VALUES
                        ('a', null, '2024-01-01T00:00:04.000000Z'),
                        ('c', null, '2024-01-01T00:00:05.000000Z')
                    """);
            execute("INSERT INTO lookup VALUES ('c')");
            assertQuery("""
                    SELECT sum(CASE WHEN s IN (SELECT s FROM lookup) THEN x END)
                           OVER (PARTITION BY s) AS total
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            total
                            4.0
                            null
                            4.0
                            null
                            4.0
                            null
                            """);
        });
    }

    @Test
    public void testSumPartitionedSubQueryReopensAfterInsert() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT sum(CASE WHEN s IN (SELECT s FROM lookup) THEN x ELSE 0 END)
                           OVER (PARTITION BY s) AS total
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .mutateWith("INSERT INTO lookup VALUES ('b')")
                    .returns("""
                            total
                            4.0
                            0.0
                            4.0
                            0.0
                            """, """
                            total
                            4.0
                            2.0
                            4.0
                            0.0
                            """);
        });
    }

    @Test
    public void testSumPartitionedSubQuerySymbolFirst() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT s, sum(CASE WHEN s IN (SELECT s FROM lookup) THEN x ELSE 0 END)
                              OVER (PARTITION BY s) AS total
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\ttotal
                            a\t4.0
                            b\t0.0
                            a\t4.0
                            \t0.0
                            """);
        });
    }

    @Test
    public void testSumPartitionedSubQueryTimestampFirst() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT ts, sum(CASE WHEN s IN (SELECT s FROM lookup) THEN x ELSE 0 END)
                               OVER (PARTITION BY s) AS total
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\ttotal
                            2024-01-01T00:00:00.000000Z\t4.0
                            2024-01-01T00:00:01.000000Z\t0.0
                            2024-01-01T00:00:02.000000Z\t4.0
                            2024-01-01T00:00:03.000000Z\t0.0
                            """);
        });
    }

    @Test
    public void testSumPartitionedSubQueryWindowFirst() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT sum(CASE WHEN s IN (SELECT s FROM lookup) THEN x ELSE 0 END)
                           OVER (PARTITION BY s) AS total, ts
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            total\tts
                            4.0\t2024-01-01T00:00:00.000000Z
                            0.0\t2024-01-01T00:00:01.000000Z
                            4.0\t2024-01-01T00:00:02.000000Z
                            0.0\t2024-01-01T00:00:03.000000Z
                            """);
        });
    }

    @Test
    public void testSumPartitionedSubQueryWithOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT sum(CASE WHEN s IN (SELECT s FROM lookup) THEN x ELSE 0 END)
                           OVER (PARTITION BY s ORDER BY ts) AS total
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            total
                            1.0
                            0.0
                            4.0
                            0.0
                            """);
        });
    }

    @Test
    public void testSumPartitionedUnionSubQueryWithCompositeKey() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT sum(CASE WHEN s IN (SELECT s FROM lookup UNION SELECT s FROM t WHERE x = 2) THEN x ELSE 0 END)
                           OVER (PARTITION BY s::STRING, x % 2) AS total
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            total
                            4.0
                            2.0
                            4.0
                            0.0
                            """);
        });
    }

    @Test
    public void testSumUnpartitionedSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT sum(CASE WHEN s IN (SELECT s FROM lookup) THEN x ELSE 0 END)
                           OVER () AS total
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            total
                            4.0
                            4.0
                            4.0
                            4.0
                            """);
        });
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE t (s SYMBOL, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t VALUES
                    ('a', 1, '2024-01-01T00:00:00.000000Z'),
                    ('b', 2, '2024-01-01T00:00:01.000000Z'),
                    ('a', 3, '2024-01-01T00:00:02.000000Z'),
                    (null, 4, '2024-01-01T00:00:03.000000Z')
                """);
        execute("CREATE TABLE lookup (s SYMBOL)");
        execute("INSERT INTO lookup VALUES ('a')");
    }
}
