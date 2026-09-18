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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ExplicitTimestampJoinTest extends AbstractCairoTest {
    private static final String EXPECTED_SAMPLE = """
            ts\tcount
            2024-01-01T00:00:00.000000Z\t2
            2024-01-01T01:00:00.000000Z\t1
            """;
    private static final String INSERT_A = """
            INSERT INTO a VALUES
            ('2024-01-01T00:10:00.000000Z', 1),
            ('2024-01-01T00:20:00.000000Z', 2),
            ('2024-01-01T01:10:00.000000Z', 3)
            """;
    private static final String INSERT_B = """
            INSERT INTO b VALUES
            ('2024-01-01T00:10:00.000000Z', 1),
            ('2024-01-01T01:10:00.000000Z', 3)
            """;

    @Test
    public void testAsOfJoinWithQuotedDottedTimestamp() throws Exception {
        assertSample("""
                SELECT ts, count()
                FROM (
                    SELECT s."clock.ts" AS ts, x
                    FROM (SELECT ts AS "clock.ts", x FROM a) s TIMESTAMP("clock.ts")
                    ASOF JOIN b
                ) SAMPLE BY 1h
                """);
    }

    @Test
    public void testInvalidBranchTimestampCannotResolveToSlave() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = """
                    SELECT ts, count()
                    FROM ((SELECT ts, x FROM a) s TIMESTAMP(ts2) SPLICE JOIN b)
                    SAMPLE BY 1h
                    """;
            assertException(sql, sql.indexOf("ts2"), "Invalid column: ts2");
        });
    }

    @Test
    public void testNonTimestampBranchColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = """
                    SELECT ts, count()
                    FROM ((SELECT ts, x FROM a) s TIMESTAMP(x) SPLICE JOIN b)
                    SAMPLE BY 1h
                    """;
            assertException(sql, sql.indexOf("TIMESTAMP(x)") + "TIMESTAMP(".length(), "not a TIMESTAMP");
        });
    }

    @Test
    public void testSharedCteWithDifferentTimestampDesignations() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            assertQuery("""
                    WITH q AS (SELECT ts, dateadd('h', 10, ts) AS shifted, x FROM a)
                    SELECT ts, count() FROM (q s TIMESTAMP(ts) ASOF JOIN b) SAMPLE BY 1h
                    UNION ALL
                    SELECT shifted AS ts, count() FROM (q t TIMESTAMP(shifted) ASOF JOIN b) SAMPLE BY 1h
                    """)
                    .noRandomAccess()
                    .returns("""
                            ts\tcount
                            2024-01-01T00:00:00.000000Z\t2
                            2024-01-01T01:00:00.000000Z\t1
                            2024-01-01T10:00:00.000000Z\t2
                            2024-01-01T11:00:00.000000Z\t1
                            """);
        });
    }

    @Test
    public void testSpliceJoinAsOuterAsOfMaster() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            execute("CREATE TABLE c (ts3 TIMESTAMP, z INT) TIMESTAMP(ts3) PARTITION BY DAY");
            execute("INSERT INTO c VALUES ('2024-01-01T00:00:00.000000Z', 5)");
            assertQuery("""
                    SELECT ts, x, ts2, y, ts3, z
                    FROM ((SELECT ts, x FROM a) TIMESTAMP(ts) SPLICE JOIN b)
                    ASOF JOIN c
                    """)
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tx\tts2\ty\tts3\tz
                            2024-01-01T00:10:00.000000Z\t1\t2024-01-01T00:10:00.000000Z\t1\t2024-01-01T00:00:00.000000Z\t5
                            2024-01-01T00:20:00.000000Z\t2\t2024-01-01T00:10:00.000000Z\t1\t2024-01-01T00:00:00.000000Z\t5
                            2024-01-01T01:10:00.000000Z\t3\t2024-01-01T01:10:00.000000Z\t3\t2024-01-01T00:00:00.000000Z\t5
                            """);
        });
    }

    @Test
    public void testSpliceJoinSampleBy() throws Exception {
        assertSample("""
                SELECT ts, count()
                FROM ((SELECT ts, x FROM a) TIMESTAMP(ts) SPLICE JOIN b)
                SAMPLE BY 1h
                """);
    }

    @Test
    public void testSpliceJoinSampleByAliasedBranch() throws Exception {
        assertSample("""
                SELECT ts, count()
                FROM ((SELECT ts, x FROM a) s TIMESTAMP(ts) SPLICE JOIN b)
                SAMPLE BY 1h
                """);
    }

    @Test
    public void testSpliceJoinSampleByCaseInsensitiveTimestamp() throws Exception {
        assertSample("""
                SELECT ts, count()
                FROM ((SELECT ts, x FROM a) s TIMESTAMP(TS) SPLICE JOIN b)
                SAMPLE BY 1h
                """);
    }

    @Test
    public void testSpliceJoinSampleByCollidingTimestamp() throws Exception {
        assertSample("""
                SELECT ts, count()
                FROM (
                    (SELECT ts, x FROM a) s TIMESTAMP(ts)
                    SPLICE JOIN (SELECT ts2 AS ts, y FROM b) r
                ) SAMPLE BY 1h
                """);
    }

    @Test
    public void testSpliceJoinSampleByCte() throws Exception {
        assertSample("""
                WITH q AS (SELECT ts, x FROM a)
                SELECT ts, count() FROM (q TIMESTAMP(ts) SPLICE JOIN b) SAMPLE BY 1h
                """);
    }

    @Test
    public void testSpliceJoinSampleByDifferentOutputDesignation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            assertQuery("""
                    SELECT shifted AS ts, count()
                    FROM (
                        (SELECT ts, dateadd('h', 10, ts) AS shifted, x FROM a) s TIMESTAMP(ts)
                        SPLICE JOIN b
                    ) TIMESTAMP(shifted) SAMPLE BY 1h
                    """)
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tcount
                            2024-01-01T10:00:00.000000Z\t2
                            2024-01-01T11:00:00.000000Z\t1
                            """);
        });
    }

    @Test
    public void testSpliceJoinSampleByEmptyThenPopulated() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT ts, count()
                    FROM ((SELECT ts, x FROM a) TIMESTAMP(ts) SPLICE JOIN b)
                    SAMPLE BY 1h
                    """)
                    .timestamp("ts")
                    .noRandomAccess()
                    .mutateWith(INSERT_A, INSERT_B)
                    .returns("ts\tcount\n", EXPECTED_SAMPLE);
        });
    }

    @Test
    public void testSpliceJoinSampleByParenthesizedTable() throws Exception {
        assertSample("""
                SELECT ts, count() FROM ((a) TIMESTAMP(ts) SPLICE JOIN b) SAMPLE BY 1h
                """);
    }

    @Test
    public void testSpliceJoinSampleByQuotedDottedTableAlias() throws Exception {
        assertSample("""
                SELECT ts, count()
                FROM ((SELECT ts, x FROM a) "s.dot" TIMESTAMP(ts) SPLICE JOIN b)
                SAMPLE BY 1h
                """);
    }

    @Test
    public void testSpliceJoinSampleByQuotedDottedTimestamp() throws Exception {
        assertSample("""
                SELECT ts, count()
                FROM (
                    SELECT s."clock.ts" AS ts, x
                    FROM (SELECT ts AS "clock.ts", x FROM a) s TIMESTAMP("clock.ts")
                    SPLICE JOIN b
                ) SAMPLE BY 1h
                """);
    }

    @Test
    public void testSpliceJoinSampleByRenamedCollidingTimestamp() throws Exception {
        assertSample("""
                SELECT stamp AS ts, count()
                FROM (
                    (SELECT ts AS stamp, x FROM a) s TIMESTAMP(stamp)
                    SPLICE JOIN (SELECT ts2 AS stamp, y FROM b) r
                ) SAMPLE BY 1h
                """);
    }

    @Test
    public void testSpliceJoinSharedCte() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            assertQuery("""
                    WITH q AS (SELECT ts, x FROM a)
                    SELECT ts, count() FROM (q s TIMESTAMP(ts) SPLICE JOIN b) SAMPLE BY 1h
                    UNION ALL
                    SELECT ts, count() FROM (q t ASOF JOIN b) SAMPLE BY 1h
                    """)
                    .noRandomAccess()
                    .returns("""
                            ts\tcount
                            2024-01-01T00:00:00.000000Z\t2
                            2024-01-01T01:00:00.000000Z\t1
                            2024-01-01T00:00:00.000000Z\t2
                            2024-01-01T01:00:00.000000Z\t1
                            """);
        });
    }

    private void assertSample(String sql) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            assertQuery(sql).timestamp("ts").noRandomAccess().returns(EXPECTED_SAMPLE);
        });
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE a (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE b (ts2 TIMESTAMP, y INT) TIMESTAMP(ts2) PARTITION BY DAY");
    }

    private void insertRows() throws Exception {
        execute(INSERT_A);
        execute(INSERT_B);
    }
}
