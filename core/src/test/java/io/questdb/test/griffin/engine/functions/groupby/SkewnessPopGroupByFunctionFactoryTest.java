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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SkewnessPopGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testSkewnessPopAllNull() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "select skewness_pop(x) from (select cast(null as double) x from long_sequence(100))")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("skewness_pop\nnull\n"));
    }

    @Test
    public void testSkewnessPopAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT 1 x FROM long_sequence(100))");
            assertQuery("SELECT skewness_pop(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("skewness_pop\nnull\n");
        });
    }

    @Test
    public void testSkewnessPopDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(4))");
            assertQuery("SELECT skewness_pop(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("skewness_pop\n0.0\n");
        });
    }

    @Test
    public void testSkewnessPopFirstNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 (x DOUBLE)");
            execute("INSERT INTO tbl1 VALUES (null)");
            execute("INSERT INTO tbl1 SELECT cast(x AS DOUBLE) FROM long_sequence(4)");
            assertQuery("SELECT skewness_pop(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("skewness_pop\n0.0\n");
        });
    }

    @Test
    public void testSkewnessPopFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT cast(x AS FLOAT) x FROM long_sequence(4))");
            assertQuery("SELECT skewness_pop(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("skewness_pop\n0.0\n");
        });
    }

    @Test
    public void testSkewnessPopIntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT cast(x AS INT) x FROM long_sequence(4))");
            assertQuery("SELECT skewness_pop(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("skewness_pop\n0.0\n");
        });
    }

    @Test
    public void testSkewnessPopLongValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT x FROM long_sequence(4))");
            assertQuery("SELECT skewness_pop(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("skewness_pop\n0.0\n");
        });
    }

    @Test
    public void testSkewnessPopMultiColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE aggr (k INT, v INT, v2 INT)");
            execute("""
                    INSERT INTO aggr VALUES
                        (1, 10, null),
                        (2, 10, 11),
                        (2, 10, 15),
                        (2, 10, 18),
                        (2, 20, 22),
                        (2, 20, 25),
                        (2, 25, null),
                        (2, 30, 35),
                        (2, 30, 40),
                        (2, 30, 50),
                        (2, 30, 51)""");
            assertQuery("""
                    SELECT round(skewness_pop(k), 6) k,
                           round(skewness_pop(v), 6) v,
                           round(skewness_pop(v2), 6) v2
                    FROM aggr""")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("k\tv\tv2\n-2.84605\t-0.140254\t0.30144\n");
        });
    }

    @Test
    public void testSkewnessPopNoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 (x DOUBLE)");
            assertQuery("SELECT skewness_pop(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("skewness_pop\nnull\n");
        });
    }

    @Test
    public void testSkewnessPopOneValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 (x DOUBLE)");
            execute("INSERT INTO tbl1 VALUES (1)");
            assertQuery("SELECT skewness_pop(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("skewness_pop\nnull\n");
        });
    }

    @Test
    public void testSkewnessPopSomeNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(4))");
            execute("INSERT INTO tbl1 VALUES (null)");
            assertQuery("SELECT skewness_pop(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("skewness_pop\n0.0\n");
        });
    }

    @Test
    public void testSkewnessPopThreeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(3))");
            assertQuery("SELECT skewness_pop(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("skewness_pop\n0.0\n");
        });
    }

    @Test
    public void testSkewnessPopTwoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(2))");
            assertQuery("SELECT skewness_pop(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("skewness_pop\n0.0\n");
        });
    }
}
