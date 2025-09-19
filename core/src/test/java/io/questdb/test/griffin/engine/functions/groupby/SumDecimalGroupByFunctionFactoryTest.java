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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SumDecimalGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testSum() throws Exception {
        assertQuery(
                "key\ts8\ts16\ts32\ts64\ts128\ts256\n" +
                        "4\t80386\t845311.7\t821434105.2\t8348895194209079.96\t15013283333426745763707016020885.282\t52779948702077994023236013815523645989075105980406522786268903747063.13817\n" +
                        "3\t80052\t796832.5\t827966494.9\t8345959911128896.35\t15154544307290026970451231019922.168\t51251454463562292211132154814850817944769567503938901926757504381147.61852\n" +
                        "2\t78683\t829124.0\t836041388.6\t8253669346672007.00\t15417703753728556442893444549179.725\t52904245001254252425187490141056944976735612591090829420279896671714.34624\n" +
                        "1\t83500\t827562.3\t850925503.1\t8116974206197377.95\t15192384355256387634603186438612.296\t51441914250075825514110580782058502630242389498747004683992921485723.14033\n" +
                        "0\t82134\t829301.0\t813129696.1\t8048105687658807.13\t15530196947752501423217417500835.720\t52440068225367850832904319868657172192628674768852717640440379520888.16696\n",
                "select id%5 key, sum(d8) s8, sum(d16) s16, sum(d32) s32, " +
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

    @Test
    public void testSumAllNull() throws Exception {
        assertQuery(
                "sum\n\n",
                "select sum(x) from (select cast(null as decimal(10,2)) x from long_sequence(1000))",
                null,
                false,
                true
        );
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
                "sum aggregation failed: Overflow in addition: result exceeds maximum precision"
        );
    }
}
