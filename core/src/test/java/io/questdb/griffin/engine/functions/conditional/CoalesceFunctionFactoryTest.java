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
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import org.junit.Before;
import org.junit.Test;

public class CoalesceFunctionFactoryTest extends AbstractGriffinTest {
    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testLong2Args() throws Exception {
        assertQuery(
                "coalesce\n" +
                        "2\n" +
                        "4\n" +
                        "3\n" +
                        "8\n" +
                        "10\n" +
                        "NaN\n",
                "select coalesce(a, x) " +
                        "from alex",
                "create table alex as (" +
                        "select CASE WHEN x % 2 = 0 THEN CAST(NULL as long) ELSE x END as x," +
                        " CASE WHEN x % 3 = 0 THEN CAST(NULL as long) ELSE x * 2 END as a" +
                        " from long_sequence(6)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testInt2Args() throws Exception {
        assertQuery(
                "coalesce\n" +
                        "2\n" +
                        "2\n" +
                        "6\n" +
                        "4\n" +
                        "10\n",
                "select coalesce(a, x) " +
                        "from alex",
                "create table alex as (" +
                        "select CAST(x as INT) x," +
                        " CASE WHEN x % 2 = 0 THEN CAST(NULL as INT) ELSE CAST(x AS INT) * 2 END as a" +
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
                "coalesce\n" +
                        "10.0000\n" +
                        "2.0000\n" +
                        "0.5000\n" +
                        "10.0000\n" +
                        "5.0000\n",
                "select coalesce(b, a, x) " +
                        "from alex",
                "create table alex as (" +
                        "select CAST(x as float) as x," +
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
                "coalesce\n" +
                        "10.0\n" +
                        "0.2\n" +
                        "100.0\n" +
                        "10.0\n" +
                        "0.5\n",
                "select coalesce(b, a, x) " +
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
    public void testStrl3Args() throws Exception {
        assertQuery(
                "coalesce\n" +
                        "A\n" +
                        "\n" +
                        "B\n" +
                        "B\n" +
                        "X\n",
                "select coalesce(b, a, x) " +
                        "from alex",
                "create table alex as (" +
                        "SELECT rnd_str('X',NULL) as x\n" +
                        ", rnd_str('A',NULL) as a\n" +
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
                "coalesce\tx\ta\tb\n" +
                        "X\tX\tA\tBB\n" +
                        "BB\t\t\tBB\n" +
                        "AA\t\tAA\tB\n" +
                        "AA\t\tAA\tB\n" +
                        "AA\t\tAA\tBB\n",
                "select coalesce(x, a, b), x, a, b " +
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
    public void testTimestampCoalesce() throws Exception {
        assertQuery(
                "coalesce\ta\tx\n" +
                        "1970-01-01T00:00:00.000001Z\t\t1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000002Z\t\t1970-01-01T00:00:00.000002Z\n" +
                        "1970-01-01T08:20:00.000000Z\t1970-01-01T08:20:00.000000Z\t1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000004Z\t\t1970-01-01T00:00:00.000004Z\n" +
                        "1970-01-01T00:00:00.000005Z\t\t1970-01-01T00:00:00.000005Z\n",
                "select coalesce(a, x) as coalesce, a, x \n" +
                        "from alex",
                "create table alex as (" +
                        "select CAST(x as Timestamp) as x," +
                        " CASE WHEN x % 3 = 0 THEN CAST(x * 10000000000 as Timestamp) ELSE CAST(NULL as Timestamp) END as a" +
                        " from long_sequence(5)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testTimestampAndDateCoalesce() throws Exception {
        assertQuery(
                "coalesce\ta\tx\n" +
                        "1970-01-02T00:00:00.000Z\t\t1970-01-02T00:00:00.000Z\n" +
                        "1970-01-21T00:00:00.000Z\t1970-01-21T00:00:00.000Z\t1970-01-03T00:00:00.000Z\n" +
                        "1970-01-04T00:00:00.000Z\t\t1970-01-04T00:00:00.000Z\n" +
                        "1970-02-10T00:00:00.000Z\t1970-02-10T00:00:00.000Z\t1970-01-05T00:00:00.000Z\n" +
                        "1970-01-06T00:00:00.000Z\t\t1970-01-06T00:00:00.000Z\n",
                "select coalesce(a, x) as coalesce, a, x \n" +
                        "from alex",
                "create table alex as (" +
                        "WITH tx as (\n" +
                        "select CAST(dateadd('d', CAST(x as INT), CAST(0 AS DATE)) AS DATE) as x, \n" +
                        "CAST(dateadd('d', CAST(x as INT) * 10, CAST(0 AS DATE)) AS DATE) as xx, \n" +
                        "x as n from long_sequence(5))\n" +
                        "select x, \n" +
                        "CASE WHEN n % 2 = 0 THEN xx ELSE CAST(NULL as DATE) END as a \n" +
                        "from tx "+
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
                "coalesce\ta\tx\n" +
                        "\t\t\n" +
                        "0x4ff974be989fa1d8313c564a4a70df3a6ad4d66b23b47bdf9b46ced4655c6674\t\t0x4ff974be989fa1d8313c564a4a70df3a6ad4d66b23b47bdf9b46ced4655c6674\n" +
                        "\t\t\n" +
                        "0x9d34356da0f68f1fbc7827bb6dc3ce15b85b002a9fea625b95bdcc28b3dfce1e\t0x9d34356da0f68f1fbc7827bb6dc3ce15b85b002a9fea625b95bdcc28b3dfce1e\t0xb98b79aef5acec1fdb8cfee61427b2cc7d13df361ce20ad91905dceefe6983dd\n" +
                        "\t\t\n",
                "select coalesce(a, x), a, x\n" +
                        "from alex",
                "create table alex as (" +
                        "select CASE WHEN x % 2 = 0 THEN rnd_long256(1000) ELSE CAST(NULL as LONG256) END as x," +
                        " CASE WHEN x % 4 = 0 THEN rnd_long256(10) ELSE CAST(NULL as LONG256) END as a" +
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
}