/*******************************************************************************
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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Regression coverage for the posting-index DISTINCT fast path in
 * {@code SqlCodeGenerator.generateSelectGroupBy()} when the base table's WHERE clause
 * carries a scalar-subquery designated-timestamp bound (a plain {@code ts >= (SELECT ...)}
 * or a nested {@code LATEST ON ...} subquery).
 *
 * <p>That branch re-enters {@code WhereClauseParser.extract()} on the base table's WHERE clause
 * to decide whether the predicate reduces to interval-only intrinsics. When the scalar-subquery
 * timestamp bound resolves to a pure runtime interval (no residual filter, no key predicate), the
 * generator dispatches to {@code PostingIndexDistinctRecordCursorFactory} over an
 * {@code IntervalPartitionFrameCursorFactory}. These tests pin that both the plan (posting-index
 * DISTINCT over an interval scan) and the rows match the equivalent constant-bound query.
 */
public class PostingIndexDistinctSubqueryBoundTest extends AbstractCairoTest {

    @Test
    public void testPostingIndexDistinctWithNestedLatestBySubqueryBound() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE d_events (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO d_events VALUES
                        ('2024-01-01T00:00:00.000000Z', 'A'),
                        ('2024-01-01T12:00:00.000000Z', 'B'),
                        ('2024-01-02T00:00:00.000000Z', 'A'),
                        ('2024-01-02T06:00:00.000000Z', 'C'),
                        ('2024-01-03T00:00:00.000000Z', 'B'),
                        ('2024-01-03T12:00:00.000000Z', 'D')
                    """);
            // The latest lo per selector 'x' is 2024-01-02T12:00, so the bound is that instant.
            execute("CREATE TABLE latest_bounds (lo TIMESTAMP, selector SYMBOL) TIMESTAMP(lo) PARTITION BY DAY");
            execute("""
                    INSERT INTO latest_bounds VALUES
                        ('2024-01-01T00:00:00.000000Z', 'x'),
                        ('2024-01-02T12:00:00.000000Z', 'x')
                    """);
            engine.releaseAllWriters();

            String subquery = """
                    SELECT DISTINCT sym FROM d_events
                    WHERE ts >= (
                        SELECT lo FROM latest_bounds
                        WHERE selector = 'x'
                        LATEST ON lo PARTITION BY selector
                    )
                    """;

            // Plan: the scalar LATEST BY bound reduces to an interval-only intrinsic, so the
            // generator takes the posting-index DISTINCT fast path over an interval scan.
            assertQuery(subquery)
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlan("""
                            PostingIndex op: distinct on: sym
                                Interval forward scan on: d_events
                                  intervals: [("2024-01-02T12:00:00.000000Z","MAX")]
                            """)
                    .returns("""
                            sym
                            B
                            D
                            """);

            // Rows must equal the equivalent constant-bound query, which itself takes the same
            // posting-index DISTINCT interval path, so the cursor order is apples-to-apples.
            assertSqlCursors(
                    "SELECT DISTINCT sym FROM d_events WHERE ts >= '2024-01-02T12:00:00.000000Z'",
                    subquery
            );
        });
    }

    @Test
    public void testPostingIndexDistinctWithScalarSubqueryBound() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE d_events2 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO d_events2 VALUES
                        ('2024-01-01T00:00:00.000000Z', 'A'),
                        ('2024-01-01T12:00:00.000000Z', 'B'),
                        ('2024-01-02T00:00:00.000000Z', 'A'),
                        ('2024-01-02T06:00:00.000000Z', 'C'),
                        ('2024-01-03T00:00:00.000000Z', 'B'),
                        ('2024-01-03T12:00:00.000000Z', 'D')
                    """);
            execute("CREATE TABLE bounds2 (lo TIMESTAMP, sel INT)");
            execute("INSERT INTO bounds2 VALUES ('2024-01-02T12:00:00.000000Z', 100)");
            engine.releaseAllWriters();

            String subquery = """
                    SELECT DISTINCT sym FROM d_events2
                    WHERE ts >= (SELECT lo FROM bounds2 WHERE sel = 100)
                    """;

            assertQuery(subquery)
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlan("""
                            PostingIndex op: distinct on: sym
                                Interval forward scan on: d_events2
                                  intervals: [("2024-01-02T12:00:00.000000Z","MAX")]
                            """)
                    .returns("""
                            sym
                            B
                            D
                            """);

            assertSqlCursors(
                    "SELECT DISTINCT sym FROM d_events2 WHERE ts >= '2024-01-02T12:00:00.000000Z'",
                    subquery
            );
        });
    }
}
