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

/**
 * Nests the users of binder scratch that one owner shares, so a user that runs inside another's
 * window would corrupt the result.
 */
public class SqlBinderSharedScratchTest extends AbstractCairoTest {

    @Test
    public void testJoinSubqueriesInsideJoinFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (k SYMBOL, v INT)");
            execute("INSERT INTO src VALUES ('a', 10), ('b', 20), ('c', 30)");
            assertQuery("""
                    SELECT a.k, b.v
                    FROM src a
                    JOIN src b ON a.k = b.k
                    WHERE a.k IN (SELECT s1.k FROM src s1 JOIN src s2 ON s1.k = s2.k WHERE s1.v < 30)
                    AND b.k IN (SELECT s1.k FROM src s1 JOIN src s2 ON s1.k = s2.k WHERE s2.v > 10)
                    ORDER BY a.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\tv
                            b\t20
                            """);
        });
    }

    @Test
    public void testPivotsUnderJoinAndWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE data (grp SYMBOL, cat STRING, val INT)");
            execute("INSERT INTO data VALUES ('A', 'x', 1), ('A', 'y', 2), ('B', 'x', 3), ('B', 'y', 4)");
            assertQuery("""
                    SELECT t1.grp, t1.x, t2.y, sum(t1.x) OVER () total
                    FROM (SELECT * FROM data PIVOT (SUM(val) FOR cat IN (SELECT DISTINCT cat FROM data ORDER BY cat) GROUP BY grp)) t1
                    JOIN (SELECT * FROM data PIVOT (SUM(val) FOR cat IN ('x', 'y') GROUP BY grp)) t2 ON t1.grp = t2.grp
                    ORDER BY t1.grp
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            grp\tx\ty\ttotal
                            A\t1\t2\t4.0
                            B\t3\t4\t4.0
                            """);
        });
    }

    @Test
    public void testProjectionReferencesOverMixedPrecisionUnionFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE b (ts TIMESTAMP_NS, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO a VALUES ('2024-01-01T00:00:00.000000Z', 1), ('2024-01-01T02:00:00.000000Z', 2)");
            execute("INSERT INTO b VALUES ('2024-01-01T01:00:00.000000001Z', 3)");
            assertQuery("""
                    SELECT v + 1 AS w, w * 2 AS z
                    FROM (SELECT ts, v FROM a UNION ALL SELECT ts, v FROM b)
                    WHERE ts > '2024-01-01T00:30:00.000000Z'
                    ORDER BY w
                    """)
                    .noLeakCheck()
                    .returns("""
                            w\tz
                            3\t6
                            4\t8
                            """);
        });
    }

    @Test
    public void testLatestByIntervalScanUnderJoinAndSort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE b (k SYMBOL, w INT)");
            execute("""
                    INSERT INTO a VALUES
                    ('x', 1, '2024-01-01T00:00:00.000000Z'),
                    ('y', 2, '2024-01-01T01:00:00.000000Z'),
                    ('x', 3, '2024-01-01T02:00:00.000000Z'),
                    ('y', 4, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("INSERT INTO b VALUES ('x', 10), ('y', 20)");
            assertQuery("""
                    SELECT l.k, l.v, b.w
                    FROM (SELECT * FROM a WHERE ts IN '2024-01-01' LATEST ON ts PARTITION BY k) l
                    JOIN b ON l.k = b.k
                    ORDER BY l.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\tv\tw
                            x\t3\t10
                            y\t2\t20
                            """);
        });
    }

    @Test
    public void testSortedJoinFilteredBySortedJoinSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k SYMBOL, v INT)");
            execute("CREATE TABLE b (k SYMBOL, w INT)");
            execute("INSERT INTO a VALUES ('x', 1), ('y', 2), ('x', 3), ('y', 4)");
            execute("INSERT INTO b VALUES ('x', 10), ('y', 20)");
            assertQuery("""
                    SELECT a.k, v, w
                    FROM a JOIN b ON a.k = b.k
                    WHERE a.k IN (SELECT b.k FROM b JOIN a ON a.k = b.k ORDER BY a.v DESC LIMIT 1)
                    ORDER BY v DESC
                    LIMIT 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\tv\tw
                            y\t4\t20
                            y\t2\t20
                            """);
        });
    }
}
