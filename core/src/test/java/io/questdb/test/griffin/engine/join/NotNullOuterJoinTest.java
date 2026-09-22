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

package io.questdb.test.griffin.engine.join;

import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Pins the NOT-NULL-flag clearing on every outer-join shape. Each join that can
 * null-pad a side must clear the NOT NULL flags of that side's columns in the
 * joined metadata; if a clearing site regresses, IS NULL on a padded NOT NULL
 * column constant-folds to FALSE and the anti-join silently returns zero rows
 * (and group-by aggregates count the padding null as data). One test per shape:
 * hash LEFT/RIGHT/FULL, nested-loop LEFT/FULL (non-equi condition), ASOF light,
 * ASOF full-fat, LT light, SPLICE (each null-padded direction), HORIZON.
 */
public class NotNullOuterJoinTest extends AbstractCairoTest {

    @Test
    public void testAsofJoinFullFatNullPadding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (id INT NOT NULL, k INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES (1, 1, '2024-01-01T10:00:00'), (2, 2, '2024-01-01T10:02:00')");
            execute("CREATE TABLE s (k INT, v INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES (1, 10, '2024-01-01T09:59:00')");

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                assertQuery("SELECT m.id, s.v FROM m ASOF JOIN s ON m.k = s.k WHERE s.v IS NULL")
                        .withCompiler(compiler)
                        .withContext(sqlExecutionContext)
                        .noRandomAccess()
                        .noLeakCheck()
                        .returns("""
                                id\tv
                                2\tnull
                                """);
                assertQuery("SELECT m.id, s.v FROM m ASOF JOIN s ON m.k = s.k WHERE s.v IS NOT NULL")
                        .withCompiler(compiler)
                        .withContext(sqlExecutionContext)
                        .noRandomAccess()
                        .noLeakCheck()
                        .returns("""
                                id\tv
                                1\t10
                                """);
            }
        });
    }

    @Test
    public void testAsofJoinLightNullPadding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (id INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES (1, '2024-01-01T10:00:00'), (2, '2024-01-01T10:02:00')");
            execute("CREATE TABLE s (v INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES (10, '2024-01-01T10:01:00')");

            assertQuery("SELECT m.id, s.v FROM m ASOF JOIN s WHERE s.v IS NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\tnull
                            """);
            assertQuery("SELECT m.id, s.v FROM m ASOF JOIN s WHERE s.v IS NOT NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            2\t10
                            """);
        });
    }

    @Test
    public void testHashFullJoinNullPadding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (id INT NOT NULL, k INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES (1, 1, '2024-01-01T10:00:00'), (2, 2, '2024-01-01T10:02:00')");
            execute("CREATE TABLE s (k INT, v INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES (1, 10, '2024-01-01T10:01:00'), (3, 30, '2024-01-01T10:03:00')");

            assertQuery("SELECT m.id, s.v FROM m FULL JOIN s ON m.k = s.k WHERE s.v IS NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            2\tnull
                            """);
            assertQuery("SELECT m.id, s.v FROM m FULL JOIN s ON m.k = s.k WHERE m.id IS NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            null\t30
                            """);
        });
    }

    @Test
    public void testHashLeftJoinNullPadding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (id INT NOT NULL, k INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES (1, 1, '2024-01-01T10:00:00'), (2, 2, '2024-01-01T10:02:00')");
            execute("CREATE TABLE s (k INT, v INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES (1, 10, '2024-01-01T10:01:00'), (3, 30, '2024-01-01T10:03:00')");

            assertQuery("SELECT m.id, s.v FROM m LEFT JOIN s ON m.k = s.k WHERE s.v IS NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            2\tnull
                            """);
            assertQuery("SELECT m.id, s.v FROM m LEFT JOIN s ON m.k = s.k WHERE s.v IS NOT NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t10
                            """);
        });
    }

    @Test
    public void testHashRightJoinNullPadding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (id INT NOT NULL, k INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES (1, 1, '2024-01-01T10:00:00'), (2, 2, '2024-01-01T10:02:00')");
            execute("CREATE TABLE s (k INT, v INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES (1, 10, '2024-01-01T10:01:00'), (3, 30, '2024-01-01T10:03:00')");

            assertQuery("SELECT m.id, s.v FROM m RIGHT JOIN s ON m.k = s.k WHERE m.id IS NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            null\t30
                            """);
            assertQuery("SELECT m.id, s.v FROM m RIGHT JOIN s ON m.k = s.k WHERE m.id IS NOT NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t10
                            """);
        });
    }

    @Test
    public void testHorizonJoinNullPadding() throws Exception {
        assertMemoryLeak(() -> {
            // HORIZON runs as a keyed GROUP BY over the joined rows. An unmatched master
            // row feeds a null slave record into the aggregation; count(s.v) must skip
            // it. If the slave NOT NULL flags survived into the horizon metadata, the
            // NOT-NULL-aware count would treat the padding null's sentinel as data and
            // return 1 instead of 0.
            execute("CREATE TABLE m (id INT NOT NULL, k SYMBOL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES (1, 'a', '2024-01-01T10:00:00'), (2, 'b', '2024-01-01T10:00:01')");
            execute("CREATE TABLE s (k SYMBOL, v INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES ('a', 10, '2024-01-01T10:00:00')");

            assertQuery("SELECT m.id, count(s.v) c FROM m HORIZON JOIN s ON (m.k = s.k) RANGE FROM 0s TO 1s STEP 1s AS h ORDER BY m.id")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            id\tc
                            1\t2
                            2\t0
                            """);
            // id=1: the slave row falls into both step windows (offsets 0s and 1s), so
            // count(s.v) sees it twice. id=2: both steps feed a padding null and the
            // count stays 0 -- with the NOT NULL flag wrongly retained it would read
            // the null record's sentinel as data and return 2.
        });
    }

    @Test
    public void testLtJoinLightNullPadding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (id INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES (1, '2024-01-01T10:00:00'), (2, '2024-01-01T10:02:00')");
            execute("CREATE TABLE s (v INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES (10, '2024-01-01T10:01:00')");

            assertQuery("SELECT m.id, s.v FROM m LT JOIN s WHERE s.v IS NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\tnull
                            """);
            assertQuery("SELECT m.id, s.v FROM m LT JOIN s WHERE s.v IS NOT NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            2\t10
                            """);
        });
    }

    @Test
    public void testNestedLoopFullJoinNullPadding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (id INT NOT NULL, k INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES (1, 1, '2024-01-01T10:00:00'), (2, 2, '2024-01-01T10:02:00')");
            execute("CREATE TABLE s (k INT, v INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES (1, 10, '2024-01-01T10:01:00'), (3, 30, '2024-01-01T10:03:00')");

            // Non-equi condition routes to the nested-loop join.
            assertQuery("SELECT m.id, s.v FROM m FULL JOIN s ON m.id > s.k WHERE s.v IS NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\tnull
                            """);
            assertQuery("SELECT m.id, s.v FROM m FULL JOIN s ON m.id > s.k WHERE m.id IS NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            null\t30
                            """);
        });
    }

    @Test
    public void testNestedLoopLeftJoinNullPadding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (id INT NOT NULL, k INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES (1, 1, '2024-01-01T10:00:00'), (2, 2, '2024-01-01T10:02:00')");
            execute("CREATE TABLE s (k INT, v INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES (1, 10, '2024-01-01T10:01:00'), (3, 30, '2024-01-01T10:03:00')");

            // Non-equi condition routes to the nested-loop join.
            assertQuery("SELECT m.id, s.v FROM m LEFT JOIN s ON m.id > s.k WHERE s.v IS NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\tnull
                            """);
            assertQuery("SELECT m.id, s.v FROM m LEFT JOIN s ON m.id > s.k WHERE s.v IS NOT NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            2\t10
                            """);
        });
    }

    @Test
    public void testSpliceJoinMasterNullPadding() throws Exception {
        assertMemoryLeak(() -> {
            // Slave rows before the first master row splice against a null master record.
            execute("CREATE TABLE m (id INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES (2, '2024-01-01T10:02:00')");
            execute("CREATE TABLE s (v INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES (10, '2024-01-01T10:01:00'), (30, '2024-01-01T10:03:00')");

            assertQuery("SELECT m.id, s.v FROM m SPLICE JOIN s WHERE m.id IS NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            null\t10
                            """);
            assertQuery("SELECT m.id, s.v FROM m SPLICE JOIN s WHERE m.id IS NOT NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            2\t10
                            2\t30
                            """);
        });
    }

    @Test
    public void testSpliceJoinSlaveNullPadding() throws Exception {
        assertMemoryLeak(() -> {
            // Master rows before the first slave row splice against a null slave record.
            execute("CREATE TABLE m (id INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES (1, '2024-01-01T10:00:00'), (2, '2024-01-01T10:02:00')");
            execute("CREATE TABLE s (v INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES (10, '2024-01-01T10:01:00'), (30, '2024-01-01T10:03:00')");

            assertQuery("SELECT m.id, s.v FROM m SPLICE JOIN s WHERE s.v IS NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\tnull
                            """);
            assertQuery("SELECT m.id, s.v FROM m SPLICE JOIN s WHERE s.v IS NOT NULL")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t10
                            2\t10
                            2\t30
                            """);
        });
    }
}
