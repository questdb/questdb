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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.cairo.PartitionBy;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import org.junit.Test;

public class ConcatFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            CreateTableTestUtils.createAllTableWithNewTypes(engine, PartitionBy.NONE);

            insert(
                    "insert into all2 select * from (" +
                            "select" +
                            " rnd_int()," +
                            " rnd_short()," +
                            " rnd_byte()," +
                            " rnd_double()," +
                            " rnd_float()," +
                            " rnd_long()," +
                            " rnd_str(2,3,0)," +
                            " rnd_symbol('A','D')," +
                            " rnd_boolean()," +
                            " rnd_bin()," +
                            " rnd_date()," +
                            " rnd_long256()," +
                            " rnd_char()," +
                            " rnd_uuid4()," +
                            " rnd_ipv4()," +
                            " rnd_varchar(2,3,0)," +
                            " timestamp_sequence(0L, 10L) ts from long_sequence(10)) timestamp(ts)"
            );
            assertSql(
                    "concat\n" +
                            "-1148479920/24814/27/0.12966659791573354/0.285/-7611843578141082998/YR/A/false/[]/2827518/0x63eb3740c80f661e9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b/D/cec82869-edec-421b-8259-3f82b430328d/235.156.195.158/龘и/0\n" +
                            "-1204896732/16589/25/0.3549235578142891/0.912/-5585859058782510591/GZ/A/true/[]/2917086/0xa5cc689c20b5585b6034c8592e2666c46a10f5a35fa476fdc1250f6e6e2992d6/F/d2565264-70b1-4f19-bec1-b56d70fe6ce9/181.158.170.71/ܾ\uDA34\uDF73L/90\n" +
                            "-1272693194/-22934/50/0.8912587536603974/0.344/-7885528361265853230/FJG/D/false/[]/3198086/0xba37e200ad5b17cdada00dc8b85c1bc8a5f80be4b45bf437492990e1a29afcac/G/6cecb916-a1ad-492b-9979-18f622d62989/171.30.189.77/Ѓَ/10\n" +
                            "-1377625589/-25710/28/0.8952510116133903/0.694/-3491277789316049618/ZS/A/true/[]/9020303/0x87e79b63e0b9f43355d3686d5da27e14255a91b0e28abeb36c3493fcb2d0272d/W/05e73700-cbeb-45eb-bf8c-c4fc476cacc9/166.88.5.163/X夺/40\n" +
                            "-705157637/28974/114/0.8786111112537701/0.235/-3463832009795858033/PHF/D/true/[]/6313135/0x6265e3472b31b40847d39008fac595375ecd4cecbeeff59672e4c0c7d1eefb11/Z/97a77df3-0ee9-4def-9914-c3ec60c46098/71.53.138.84/[Ԣ/60\n" +
                            "-916132123/-5535/97/0.910141759290032/0.325/-8906871108655466881/XWC/A/false/[]/2731534/0x7ee54df55f49e9ac6ea837f54a4154397f3f9fef24a116ed61a4be9e1b8dcc3c/K/5bee3da4-8400-45a6-a827-63e262d6903b/239.28.217.220/㒾\uD99D\uDEA7K/20\n" +
                            "1180113884/11356/27/0.7704949839249925/0.042/5513479607887040119/VF/D/false/[]/3051249/0x34570a2bee44673552c395ffb8982d589be6b53be30f19ee69e00a3e8d5fdfe3/K/34a05899-0880-498b-bcb0-55c54725b952/104.19.242.11/[;윦/50\n" +
                            "1247654963/17560/84/0.37286547899075506/0.784/-5828188148408093893/QOE/A/true/[]/7463497/0x25b7e06ed01be163838fd7305a038e9d891ad048864ce2fb5073897a288aa6cf/E/908e7246-5eb9-4500-9ad9-4e48f6832194/46.193.192.107/Ѷ>/80\n" +
                            "1503763988/-18600/91/0.26369335635512836/0.763/2155318342410845737/HML/D/false/[]/2558357/0xd24b84c08ea7606a70061ac6a4115ca72121bcf90e43824476ffd1a81bf39767/I/a07934b2-a15d-48e0-9509-88dbaca49734/103.2.142.176/碨Ь/30\n" +
                            "724165345/21582/62/0.33976095270593043/0.747/-4036499202601723677/BHY/A/false/[]/2691241/0x5ec6d73428fb1c01b680be3ee552450eef8b1c47f7e7f9ecae395228bc24ce17/R/d50b579d-9a43-4d9f-aff9-96c5758852eb/53.18.140.19/믏G\uF6BE/70\n",
                    "select concat(int, '/', short, '/', byte, '/', double, '/', float, '/', long, '/', str, '/', sym, '/', bool, '/', bin, '/', date, '/', long256, '/', chr, '/', uuid, '/', ipv4, '/', varchar, '/', timestamp) from all2 order by 1"
            );
        });
    }

    @Test
    public void testNull() throws Exception {
        assertSql(
                "concat\n" +
                        "foo1.2\n",
                "select concat('foo', null, 1.2)"
        );
    }

    @Test
    public void testNullStr() throws Exception {
        assertQuery(
                "concat\n" +
                        "\n",
                "select concat(sym, str, v) from x",
                "create table x as (select rnd_symbol(null) sym, rnd_str(null) str, rnd_varchar(null) v from long_sequence(1))",
                null,
                true,
                true
        );
    }
}
