/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.PropertyKey;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.CustomisableRunnable;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class SumDecimalGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testSum() throws Exception {
        assertQuery(
                """
                        s8\ts16\ts32\ts64\ts128\ts256
                        10\t10.0\t10.0\t100.00\t1000.000\t10000.000000
                        """,
                "select sum(d8) s8, sum(d16) s16, sum(d32) s32, sum(d64) s64, sum(d128) s128, sum(d256) s256 from x",
                "create table x as (" +
                        "select" +
                        " cast(1 as decimal(2,0)) d8, " +
                        " cast(1 as decimal(4,1)) d16, " +
                        " cast(1 as decimal(7,1)) d32, " +
                        " cast(10 as decimal(15,2)) d64, " +
                        " cast(100 as decimal(32,3)) d128, " +
                        " cast(1000 as decimal(76,6)) d256, " +
                        " timestamp_sequence(0, 1000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by month",
                null,
                false,
                true
        );
    }

    @Test
    public void testSumDecimal128AccumulatesAfterPromotion() throws Exception {
        assertQuery(
                "sum\n200000000000000000000000000000000000000\n",
                "select sum(v) sum from x",
                "create table x as (" +
                        "select case x " +
                        " when 1 then cast('99999999999999999999999999999999999999' as decimal(38,0))" +
                        " when 2 then cast('99999999999999999999999999999999999999' as decimal(38,0))" +
                        " else cast(2 as decimal(38,0)) end v " +
                        "from long_sequence(3)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testSumDecimal128AllNull() throws Exception {
        assertQuery(
                "sum\n\n",
                "select sum(x) from (select cast(null as decimal(28,6)) x from long_sequence(1000))",
                null,
                false,
                true
        );
    }

    @Test
    public void testSumDecimal128AllOverflown() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 2);
            engine.execute(
                    "create table x as (" +
                            "select " +
                            "cast('99999999999999999999999999999999999999' as decimal(38,0)) v " +
                            "from long_sequence(10_000_000)" +
                            ")"
            );
            TestUtils.execute(
                    pool,
                    (CustomisableRunnable) (engine, compiler, sqlExecutionContext) -> TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "select sum(v) sum from x",
                            sink,
                            """
                                    sum
                                    999999999999999999999999999999999999990000000
                                    """
                    ),
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testSumDecimal128PromotionHandlesNegativeValues() throws Exception {
        assertQuery(
                "sum\n99999999999999999999999999999999999999\n",
                "select sum(v) sum from x",
                "create table x as (" +
                        "select case x " +
                        " when 1 then cast('99999999999999999999999999999999999999' as decimal(38,0))" +
                        " when 2 then cast('99999999999999999999999999999999999999' as decimal(38,0))" +
                        " else cast('-99999999999999999999999999999999999999' as decimal(38,0)) end v " +
                        "from long_sequence(3)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testSumDecimal128SomeOverflown() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            engine.execute(
                    "create table x as (" +
                            "select case " +
                            " when x%1_000_001 = 0 then cast('99999999999999999999999999999999999999' as decimal(38,0))" +
                            " when x%2 = 0 then cast('99' as decimal(38,0))" +
                            " else null end v " +
                            "from long_sequence(10_000_000)" +
                            ")"
            );
            TestUtils.execute(
                    pool,
                    (CustomisableRunnable) (engine, compiler, sqlExecutionContext) -> TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "select sum(v) sum_dec from (x)",
                            sink,
                            """
                                    sum_dec
                                    900000000000000000000000000000494999595
                                    """
                    ),
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testSumDecimal256AllNull() throws Exception {
        assertQuery(
                "sum\n\n",
                "select sum(x) from (select cast(null as decimal(36,31)) x from long_sequence(1000))",
                null,
                false,
                true
        );
    }

    @Test
    public void testSumDecimal64AllNull() throws Exception {
        assertQuery(
                "sum\n\n",
                "select sum(x) from (select cast(null as decimal(10,2)) x from long_sequence(1000))",
                null,
                false,
                true
        );
    }

    @Test
    public void testSumDecimal64AllOverflown() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 2);
            engine.execute(
                    "create table x as (" +
                            "select " +
                            "cast('99999999999999' as decimal(14,0)) v " +
                            "from long_sequence(10_000_000)" +
                            ")"
            );
            TestUtils.execute(
                    pool,
                    (CustomisableRunnable) (engine, compiler, sqlExecutionContext) -> TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "select sum(v) sum from x",
                            sink,
                            """
                                    sum
                                    999999999999990000000
                                    """
                    ),
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testSumDecimal64SomeOverflown() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            engine.execute(
                    "create table x as (" +
                            "select case " +
                            " when x%500_001 = 0 then cast('999999999999999999' as decimal(18,0))" +
                            " when x%2 = 0 then cast('99' as decimal(18,0))" +
                            " else null end v " +
                            "from long_sequence(10_000_000)" +
                            ")"
            );
            TestUtils.execute(
                    pool,
                    (CustomisableRunnable) (engine, compiler, sqlExecutionContext) -> TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "select sum(v) sum_dec, ksum(v::double) sum_d from (x)",
                            sink,
                            """
                                    sum_dec	sum_d
                                    19000000000494999090	1.9000000000494998E19
                                    """
                    ),
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testSumKeyedFixedSize() throws Exception {
        testSumMaps(false, null);
    }

    @Test
    public void testSumKeyedOrdered2() throws Exception {
        testSumMaps(true, "short");
    }

    @Test
    public void testSumKeyedOrdered4() throws Exception {
        testSumMaps(true, "int");
    }

    @Test
    public void testSumKeyedOrdered8() throws Exception {
        testSumMaps(true, "long");
    }

    @Test
    public void testSumKeyedString() throws Exception {
        testSumMaps(true, "string");
    }

    @Test
    public void testSumKeyedSymbol() throws Exception {
        testSumMaps(true, "symbol");
    }

    @Test
    public void testSumKeyedVarchar() throws Exception {
        testSumMaps(true, "varchar");
    }

    @Test
    public void testSumOverflow() throws Exception {
        assertException(
                "select sum(d) from x",
                "create table x as (" +
                        "select cast('9999999999999999999999999999999999999999999999999999999999999999999999999999' as decimal(76,0)) d " +
                        "from long_sequence(10)" +
                        ")",
                7,
                "sum aggregation failed: Overflow in addition: result exceeds 256-bit capacity"
        );
    }

    private static void testSumMaps(boolean capSize, String cast) throws Exception {
        // force use of Ordered2Map, short key and extended value size
        if (capSize) {
            // this is required to have UnorderedXMap to kick in
            node1.setProperty(PropertyKey.CAIRO_SQL_UNORDERED_MAP_MAX_ENTRY_SIZE, 512);
        }
        assertQuery(
                """
                        key\ts8\ts16\ts32\ts64\ts128\ts256
                        4\t80386\t845311.7\t821434105.2\t8348895194209079.96\t15013283333426745763707016020885.282\t52779948702077994023236013815523645989075105980406522786268903747063.13817
                        3\t80052\t796832.5\t827966494.9\t8345959911128896.35\t15154544307290026970451231019922.168\t51251454463562292211132154814850817944769567503938901926757504381147.61852
                        2\t78683\t829124.0\t836041388.6\t8253669346672007.00\t15417703753728556442893444549179.725\t52904245001254252425187490141056944976735612591090829420279896671714.34624
                        1\t83500\t827562.3\t850925503.1\t8116974206197377.95\t15192384355256387634603186438612.296\t51441914250075825514110580782058502630242389498747004683992921485723.14033
                        0\t82134\t829301.0\t813129696.1\t8048105687658807.13\t15530196947752501423217417500835.720\t52440068225367850832904319868657172192628674768852717640440379520888.16696
                        """,
                "select (id%5)" + (cast != null ? ("::" + cast) : "") + " key, " +
                        "sum(d8) s8, sum(d16) s16, sum(d32) s32, " +
                        "sum(d64) s64, sum(d128) s128, sum(d256) s256 " +
                        "from x " +
                        "order by key desc",
                "create table x as (" +
                        "select" +
                        " x id," +
                        " rnd_decimal(2,0,2) d8," +
                        " rnd_decimal(4,1,2) d16," +
                        " rnd_decimal(7,1,2) d32," +
                        " rnd_decimal(15,2,2) d64," +
                        " rnd_decimal(32,3,2) d128," +
                        " rnd_decimal(70,5,2) d256," +
                        " timestamp_sequence(0, 1000) ts" +
                        " from long_sequence(10000)" +
                        ") timestamp(ts) partition by month",
                null,
                true,
                true
        );
    }
}
