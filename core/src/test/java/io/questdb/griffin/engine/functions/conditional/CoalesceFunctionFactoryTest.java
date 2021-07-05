/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CoalesceFunctionFactoryTest extends AbstractGriffinTest {
    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testFailsWithSingleArg() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table alex as (" +
                    "select CASE WHEN x % 2 = 0 THEN CAST(NULL as long) ELSE x END as x," +
                    " CASE WHEN x % 3 = 0 THEN x * 2 ELSE CAST(NULL as long) END as a," +
                    " CASE WHEN x % 3 = 1 THEN x * 3 ELSE CAST(NULL as long) END as b" +
                    " from long_sequence(6)" +
                    ")", sqlExecutionContext);

            try {
                compiler.compile("select coalesce(b)\n" +
                        "from alex", sqlExecutionContext);
                Assert.fail("SqlException epected");
            } catch (SqlException ex) {
                Assert.assertTrue(ex.getMessage().contains("coalesce"));
            }
        });
    }

    @Test
    public void testLong2Args() throws Exception {
        assertQuery(
                "c1\tc2\ta\tb\tx\n" +
                        "3\t3\tNaN\t3\t1\n" +
                        "NaN\tNaN\tNaN\tNaN\tNaN\n" +
                        "6\t6\t6\tNaN\t3\n" +
                        "12\t12\tNaN\t12\tNaN\n" +
                        "5\tNaN\tNaN\tNaN\t5\n" +
                        "12\t12\t12\tNaN\tNaN\n",
                "select coalesce(b, a, x) c1, coalesce(b, a) c2, a, b, x\n" +
                        "from alex",
                "create table alex as (" +
                        "select CASE WHEN x % 2 = 0 THEN CAST(NULL as long) ELSE x END as x," +
                        " CASE WHEN x % 3 = 0 THEN x * 2 ELSE CAST(NULL as long) END as a," +
                        " CASE WHEN x % 3 = 1 THEN x * 3 ELSE CAST(NULL as long) END as b" +
                        " from long_sequence(6)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testLong2ArgsWithNulls() throws Exception {
        assertQuery(
                "c1\tc2\ta\tb\tx\n" +
                        "3\t3\tNaN\t3\t1\n" +
                        "NaN\tNaN\tNaN\tNaN\tNaN\n" +
                        "6\t6\t6\tNaN\t3\n" +
                        "12\t12\tNaN\t12\tNaN\n" +
                        "5\tNaN\tNaN\tNaN\t5\n" +
                        "12\t12\t12\tNaN\tNaN\n",
                "select coalesce(b, a, x) c1, coalesce(b, a) c2, a, b, x\n" +
                        "from alex",
                "create table alex as (" +
                        "select CASE WHEN x % 2 = 0 THEN NULL ELSE x END as x," +
                        " CASE WHEN x % 3 = 0 THEN x * 2 ELSE NULL END as a," +
                        " CASE WHEN x % 3 = 1 THEN x * 3 ELSE NULL END as b" +
                        " from long_sequence(6)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testIntArgs() throws Exception {
        assertQuery(
                "c1\tc2\ta\tb\tx\n" +
                        "10\t10\tNaN\t10\tNaN\n" +
                        "NaN\tNaN\tNaN\tNaN\tNaN\n" +
                        "6\t6\t6\tNaN\tNaN\n" +
                        "40\t40\tNaN\t40\t4\n" +
                        "5\tNaN\tNaN\tNaN\t5\n",
                "select coalesce(a, b, x) c1, coalesce(a, b) c2, a, b, x\n" +
                        "from alex",
                "create table alex as (" +
                        "select " +
                        " CASE WHEN x > 3 THEN CAST(x as INT)ELSE CAST(NULL as INT) END x," +
                        " CASE WHEN x % 3 = 0 THEN CAST(x AS INT) * 2 ELSE CAST(NULL as INT) END as a," +
                        " CASE WHEN x % 3 = 1 THEN CAST(x AS INT) * 10 ELSE CAST(NULL as INT) END as b\n" +
                        " from long_sequence(5)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testDouble3Args() throws Exception {
        assertQuery(
                "coalesce\n" +
                        "10.0\n" +
                        "NaN\n" +
                        "0.5\n" +
                        "10.0\n" +
                        "NaN\n" +
                        "0.5\n",
                "select coalesce(b, a, x) " +
                        "from alex",
                "create table alex as (" +
                        "select CASE WHEN x % 3 = 0 THEN x / 10.0 ELSE CAST(NULL as double) END as x," +
                        " CASE WHEN x % 3 = 0 THEN 0.5 ELSE CAST(NULL as double) END as a," +
                        " CASE WHEN x % 3 = 1 THEN 10.0 ELSE CAST(NULL as double) END as b" +
                        " from long_sequence(6)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testFloat3Args() throws Exception {
        assertQuery(
                "c1\tc2\ta\tb\tx\n" +
                        "10.0000\t10.0000\tNaN\t10.0000\tNaN\n" +
                        "NaN\tNaN\tNaN\tNaN\tNaN\n" +
                        "0.5000\t0.5000\t0.5000\tNaN\tNaN\n" +
                        "10.0000\t10.0000\tNaN\t10.0000\t4.0000\n" +
                        "5.0000\tNaN\tNaN\tNaN\t5.0000\n",
                "select coalesce(b, a, x) c1, coalesce(b, a) c2, a, b, x\n" +
                        "from alex",
                "create table alex as (" +
                        "select " +
                        " CASE WHEN x > 3 THEN CAST(x as float) ELSE CAST(NULL as float) END as x," +
                        " CASE WHEN x % 3 = 0 THEN 0.5f ELSE CAST(NULL as float) END as a," +
                        " CASE WHEN x % 3 = 1 THEN 10.0f ELSE CAST(NULL as float) END as b" +
                        " from long_sequence(5)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testDoubleAndLongMixed3Args() throws Exception {
        assertQuery(
                "c1\tc2\ta\tb\tx\n" +
                        "10.0\t10.0\tNaN\t10.0\t0.1\n" +
                        "0.2\tNaN\tNaN\tNaN\t0.2\n" +
                        "100.0\t100.0\t100\tNaN\t0.3\n" +
                        "10.0\t10.0\tNaN\t10.0\t0.4\n" +
                        "0.5\tNaN\tNaN\tNaN\t0.5\n",
                "select coalesce(b, a, x) c1, coalesce(b, a) c2, a, b, x\n" +
                        "from alex",
                "create table alex as (" +
                        "select x / 10.0 as x," +
                        " CASE WHEN x % 3 = 0 THEN 100L ELSE CAST(NULL as long) END as a," +
                        " CASE WHEN x % 3 = 1 THEN 10.0 ELSE CAST(NULL as double) END as b" +
                        " from long_sequence(5)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testStr3Args() throws Exception {
        assertQuery(
                "c1\tc2\tx\ta\tb\n" +
                        "X\tX\tX\t\t\n" +
                        "AA\tAA\t\tAA\t\n" +
                        "B\t\t\t\tB\n" +
                        "A\tA\t\tA\tB\n" +
                        "\t\t\t\t\n",
                "select coalesce(x, a, b) c1, coalesce(x, a) c2, x, a, b\n" +
                        "from alex",
                "create table alex as (" +
                        "SELECT rnd_str('X',NULL,NULL) as x\n" +
                        ", rnd_str('A','AA',NULL,NULL) as a\n" +
                        ", rnd_str('B',NULL) as b\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testSymbol3Args() throws Exception {
        assertQuery(
                "c1\tc2\tx\ta\tb\n" +
                        "X\tX\tX\tA\tBB\n" +
                        "BB\t\t\t\tBB\n" +
                        "AA\tAA\t\tAA\tB\n" +
                        "AA\tAA\t\tAA\tB\n" +
                        "AA\tAA\t\tAA\tBB\n",
                "select coalesce(x, a, b) c1, coalesce(x, a) c2, x, a, b " +
                        "from alex",
                "create table alex as (" +
                        "SELECT rnd_symbol('X',NULL,NULL) as x\n" +
                        ", rnd_symbol('A','AA', NULL) as a\n" +
                        ", rnd_symbol('B','BB') as b\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testSymbolCoalesceStr2() throws Exception {
        assertQuery(
                "coalesce\tx\ta\n" +
                        "X\tX\tA\n" +
                        "AA\t\tAA\n" +
                        "AA\t\tAA\n" +
                        "X\tX\tAA\n" +
                        "X\tX\tA\n",
                "select coalesce(x, a) as coalesce, x, a " +
                        "from alex",
                "create table alex as (" +
                        "SELECT rnd_str('X',NULL) as x\n" +
                        ", rnd_symbol('A', 'AA') as a\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testSymbolCoalesceStrSorted() throws Exception {
        assertQuery(
                "coalesce\tx\ta\n" +
                        "AA\t\tAA\n" +
                        "AA\t\tAA\n" +
                        "X\tX\tA\n" +
                        "X\tX\tAA\n" +
                        "X\tX\tA\n",
                "select coalesce(x, a) as coalesce, x, a\n" +
                        "from alex\n" +
                        "order by 1",
                "create table alex as (" +
                        "SELECT rnd_str('X',NULL) as x\n" +
                        ", rnd_symbol('A', 'AA') as a\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testSymbolNocacheCoalesceStrSorted() throws Exception {
        assertQuery(
                "coalesce\tx\ta\n",
                "select coalesce(a, x) as coalesce, x, a\n" +
                        "from t\n" +
                        "order by 1",
                "create table t (x string, a symbol nocache)",
                null,
                "insert into t select " +
                        " rnd_str('X', NULL) as x,\n" +
                        " rnd_symbol('A', 'AA') as a\n" +
                        "from long_sequence(5)",
                "coalesce\tx\ta\n" +
                        "A\tX\tA\n" +
                        "A\tX\tA\n" +
                        "AA\tX\tAA\n" +
                        "AA\t\tAA\n" +
                        "AA\t\tAA\n",
                true,
                false,
                true
        );
    }

    @Test
    public void testStrCoalesceSymbolNocacheSorted() throws Exception {
        assertQuery(
                "coalesce\tx\ta\n",
                "select coalesce(x, a) as coalesce, x, a\n" +
                        "from t\n" +
                        "order by 1",
                "create table t (x string, a symbol nocache)",
                null,
                "insert into t select " +
                        " rnd_str(NULL, 'X', 'Y') as x,\n" +
                        " rnd_symbol('A', 'B', NULL) as a\n" +
                        "from long_sequence(5)",
                "coalesce\tx\ta\n" +
                        "A\t\tA\n" +
                        "B\t\tB\n" +
                        "X\tX\t\n" +
                        "Y\tY\tB\n" +
                        "Y\tY\t\n",
                true,
                false,
                true
        );
    }

    @Test
    public void testSymbolNocacheCoalesceSorted() throws Exception {
        assertQuery(
                "coalesce\tx\ta\n",
                "select coalesce(a, x) as coalesce, x, a\n" +
                        "from t\n" +
                        "order by 1",
                "create table t (x symbol nocache, a symbol nocache)",
                null,
                "insert into t select " +
                        " rnd_symbol('X', 'Y', 'Z', NULL) as x,\n" +
                        " rnd_symbol('A', 'B', 'C', NULL) as a\n" +
                        "from long_sequence(10)",
                "coalesce\tx\ta\n" +
                        "A\tY\tA\n" +
                        "A\tX\tA\n" +
                        "A\tZ\tA\n" +
                        "B\tX\tB\n" +
                        "C\tY\tC\n" +
                        "C\tX\tC\n" +
                        "Y\tY\t\n" +
                        "Y\tY\t\n" +
                        "Z\tZ\t\n" +
                        "Z\tZ\t\n",
                true,
                false,
                true
        );
    }

    @Test
    public void testSymbolNocache3ArgsSorted() throws Exception {
        assertQuery(
                "coalesce\tx\ta\tb\n",
                "select coalesce(x, a, b) coalesce, x, a, b " +
                        "from t\n" +
                        "order by 1",
                "create table t (x symbol nocache, a symbol nocache, b symbol nocache)",
                null,
                "insert into t select " +
                        " rnd_symbol('X', 'Y', NULL) as x,\n" +
                        " rnd_symbol('A', 'AA', NULL) as a,\n" +
                        " rnd_symbol('B', 'BB', NULL) as b\n" +
                        "from long_sequence(5)",
                "coalesce\tx\ta\tb\n" +
                        "\t\t\t\n" +
                        "AA\t\tAA\tB\n" +
                        "X\tX\tA\tBB\n" +
                        "Y\tY\tAA\tBB\n" +
                        "Y\tY\tAA\t\n",
                true,
                false,
                true
        );
    }

    @Test
    public void testTimestampCoalesce() throws Exception {
        assertQuery(
                "c1\tc2\ta\tb\tx\n" +
                        "1970-01-01T13:53:20.000000Z\t1970-01-01T13:53:20.000000Z\t\t1970-01-01T13:53:20.000000Z\t\n" +
                        "1970-01-01T00:00:00.000002Z\t\t\t\t1970-01-01T00:00:00.000002Z\n" +
                        "\t\t\t\t\n" +
                        "1970-01-01T11:06:40.000000Z\t1970-01-01T11:06:40.000000Z\t1970-01-01T11:06:40.000000Z\t\t1970-01-01T00:00:00.000004Z\n" +
                        "1970-01-03T21:26:40.000000Z\t1970-01-03T21:26:40.000000Z\t\t1970-01-03T21:26:40.000000Z\t\n",
                "select coalesce(b, a, x) c1, coalesce(b, a) c2, a, b, x\n" +
                        "from alex",
                "create table alex as (" +
                        "select " +
                        "  CASE WHEN x % 2 = 0 THEN CAST(x as Timestamp) ELSE CAST(NULL as Timestamp) END as x" +
                        ", CASE WHEN x % 4 = 0 THEN CAST(x * 10000000000 as Timestamp) ELSE CAST(NULL as Timestamp) END as a" +
                        ", CASE WHEN x % 4 = 1 THEN CAST(x * 50000000000 as Timestamp) ELSE CAST(NULL as Timestamp) END as b" +
                        " from long_sequence(5)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testDateCoalesce() throws Exception {
        assertQuery(
                "c1\tc2\ta\tx\n" +
                        "1970-01-11T00:00:00.000Z\t1970-01-11T00:00:00.000Z\t\t\n" +
                        "\t\t\t\n" +
                        "1970-01-31T00:00:00.000Z\t1970-01-31T00:00:00.000Z\t1970-01-31T00:00:00.000Z\t\n" +
                        "1970-02-10T00:00:00.000Z\t1970-02-10T00:00:00.000Z\t\t1970-01-01T00:00:00.004Z\n" +
                        "1970-01-01T00:00:00.005Z\t\t\t1970-01-01T00:00:00.005Z\n",
                "select coalesce(a, b, x) as c1, coalesce(a, b) c2, a, x \n" +
                        "from alex",
                "create table alex as (" +
                        "WITH tx as (\n" +
                        "select CAST(dateadd('d', CAST(x as INT), CAST(0 AS DATE)) AS DATE) as x, \n" +
                        "CAST(dateadd('d', CAST(x as INT) * 10, CAST(0 AS DATE)) AS DATE) as xx, \n" +
                        "CAST(x AS DATE) x," +
                        "x as n from long_sequence(5))\n" +
                        "select " +
                        "CASE WHEN n > 3 THEN x ELSE CAST(NULL as DATE) END as x, \n" +
                        "CASE WHEN n % 3 = 0 THEN xx ELSE CAST(NULL as DATE) END as a, \n" +
                        "CASE WHEN n % 3 = 1 THEN xx ELSE CAST(NULL as DATE) END as b \n" +
                        "from tx " +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testCoalesceLong256() throws Exception {
        assertQuery(
                "c1\tc2\ta\tb\tx\n" +
                        "0x1408db63570045c42b9133f96d5ac9b49395464592076d7cc536ccc3235f0a72\t0x1408db63570045c42b9133f96d5ac9b49395464592076d7cc536ccc3235f0a72\t\t0x1408db63570045c42b9133f96d5ac9b49395464592076d7cc536ccc3235f0a72\t\n" +
                        "0xa4cdac45d508383f9c0a1370d099b7237e25b91255572a8f86fd0ebdb6707e47\t\t\t\t0xa4cdac45d508383f9c0a1370d099b7237e25b91255572a8f86fd0ebdb6707e47\n" +
                        "\t\t\t\t\n" +
                        "0x2000c672c7af13b68f38b4e22684beea970e01b3e4aca8b29e144cd789d939f0\t0x2000c672c7af13b68f38b4e22684beea970e01b3e4aca8b29e144cd789d939f0\t0x2000c672c7af13b68f38b4e22684beea970e01b3e4aca8b29e144cd789d939f0\t\t0x9ec31d67e4bc804a761b47dbe5d724a075234fffc7e1e6917d2037c10d3c9d2e\n" +
                        "0x2ab49a7d2ed9aec81233ce62b3c6cf03600e7d2ef68eeb777b8273a492471abc\t0x2ab49a7d2ed9aec81233ce62b3c6cf03600e7d2ef68eeb777b8273a492471abc\t\t0x2ab49a7d2ed9aec81233ce62b3c6cf03600e7d2ef68eeb777b8273a492471abc\t\n",
                "select coalesce(a, b, x) as c1, coalesce(a, b) c2, a, b, x \n" +
                        "from alex",
                "create table alex as (" +
                        "select CASE WHEN x % 2 = 0 THEN rnd_long256(1000) ELSE CAST(NULL as LONG256) END as x," +
                        " CASE WHEN x % 4 = 0 THEN rnd_long256(10) ELSE CAST(NULL as LONG256) END as a," +
                        " CASE WHEN x % 4 = 1 THEN rnd_long256(30) ELSE CAST(NULL as LONG256) END as b" +
                        " from long_sequence(5)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testSymbolCoalesceCharAndString() throws Exception {
        assertQuery(
                "coalesce\tx\ta\n" +
                        "P\t\tP\n" +
                        "W\t\tW\n" +
                        "X\tX\tY\n" +
                        "X\tX\tW\n" +
                        "X\tX\tT\n",
                "select coalesce(x, a) as coalesce, x, a\n" +
                        "from alex\n" +
                        "order by 1",
                "create table alex as (" +
                        "SELECT rnd_str('X',NULL) as x\n" +
                        ", rnd_char() as a\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testSymbolCoalesceShortAndByte() throws Exception {
        assertQuery(
                "coalesce1\tcoalesce2\tx\ta\n" +
                        "1\t2\t1\t2\n" +
                        "2\t4\t2\t4\n" +
                        "3\t6\t3\t6\n" +
                        "4\t8\t4\t8\n" +
                        "5\t10\t5\t10\n",
                "select coalesce(x, a) as coalesce1, coalesce(a, x) as coalesce2, x, a\n" +
                        "from alex\n" +
                        "order by 1",
                "create table alex as (" +
                        "SELECT CAST(x as BYTE) as x\n" +
                        ", CAST(x*2 as SHORT) as a\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testFailsWithUnsupportedType() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table alex as (" +
                    "select CAST(NULL as binary) x, CAST(NULL as binary) a" +
                    " from long_sequence(6)" +
                    ")", sqlExecutionContext);

            try {
                compiler.compile("select coalesce(x, a)\n" +
                        "from alex", sqlExecutionContext);
                Assert.fail("SqlException epected");
            } catch (SqlException ex) {
                Assert.assertTrue(ex.getMessage().contains("coalesce"));
            }
        });
    }
}