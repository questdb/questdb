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

public class KurtosisSampleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testKurtosisSampAllNull() throws Exception {
        assertQuery(
                "select kurtosis_samp(x) from (select cast(null as double) x from long_sequence(100))")
                .noRandomAccess()
                .expectSize()
                .returns("kurtosis_samp\nnull\n");
    }

    @Test
    public void testKurtosisSampAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT 1 x FROM long_sequence(100))");
            assertQuery("SELECT kurtosis_samp(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("kurtosis_samp\nnull\n");
        });
    }

    @Test
    public void testKurtosisSampDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(4))");
            assertQuery("SELECT round(kurtosis_samp(x), 6) kurtosis_samp FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("kurtosis_samp\n-1.2\n");
        });
    }

    @Test
    public void testKurtosisSampFirstNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 (x DOUBLE)");
            execute("INSERT INTO tbl1 VALUES (null)");
            execute("INSERT INTO tbl1 SELECT cast(x AS DOUBLE) FROM long_sequence(4)");
            assertQuery("SELECT round(kurtosis_samp(x), 6) kurtosis_samp FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("kurtosis_samp\n-1.2\n");
        });
    }

    @Test
    public void testKurtosisSampFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT cast(x AS FLOAT) x FROM long_sequence(4))");
            assertQuery("SELECT round(kurtosis_samp(x), 6) kurtosis_samp FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("kurtosis_samp\n-1.2\n");
        });
    }

    @Test
    public void testKurtosisSampHugeValues() throws Exception {
        // Arithmetic sequence: sample excess kurtosis is ~-1.2 at any scale, so huge values keep a known target.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT 100_000_000 * x x FROM long_sequence(1_000_000))");
            assertQuery("SELECT kurtosis_samp(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("kurtosis_samp\n-1.2000000000000608\n");
        });
    }

    @Test
    public void testKurtosisSampIntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT cast(x AS INT) x FROM long_sequence(4))");
            assertQuery("SELECT round(kurtosis_samp(x), 6) kurtosis_samp FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("kurtosis_samp\n-1.2\n");
        });
    }

    @Test
    public void testKurtosisSampLongValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT x FROM long_sequence(4))");
            assertQuery("SELECT round(kurtosis_samp(x), 6) kurtosis_samp FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("kurtosis_samp\n-1.2\n");
        });
    }

    @Test
    public void testKurtosisSampMultiColumn() throws Exception {
        // Verified with DuckDB
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
                    SELECT round(kurtosis_samp(k), 6) k,
                           round(kurtosis_samp(v), 6) v,
                           round(kurtosis_samp(v2), 6) v2
                    FROM aggr""")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("k\tv\tv2\n11.0\t-1.961428\t-1.44512\n");
        });
    }

    @Test
    public void testKurtosisSampNoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 (x DOUBLE)");
            assertQuery("SELECT kurtosis_samp(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("kurtosis_samp\nnull\n");
        });
    }

    @Test
    public void testKurtosisSampOneValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 (x DOUBLE)");
            execute("INSERT INTO tbl1 VALUES (1)");
            assertQuery("SELECT kurtosis_samp(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("kurtosis_samp\nnull\n");
        });
    }

    @Test
    public void testKurtosisSampSomeNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(4))");
            execute("INSERT INTO tbl1 VALUES (null)");
            assertQuery("SELECT round(kurtosis_samp(x), 6) kurtosis_samp FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("kurtosis_samp\n-1.2\n");
        });
    }

    @Test
    public void testKurtosisSampThreeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(3))");
            assertQuery("SELECT kurtosis_samp(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("kurtosis_samp\nnull\n");
        });
    }

    @Test
    public void testKurtosisSampTwoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl1 AS (SELECT cast(x AS DOUBLE) x FROM long_sequence(2))");
            assertQuery("SELECT kurtosis_samp(x) FROM tbl1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("kurtosis_samp\nnull\n");
        });
    }

}
