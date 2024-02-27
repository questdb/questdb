/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

            insert("insert into all2 select * from (" +
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
                    " rnd_ipv4()," +
                    " rnd_varchar(2,3,0)," +
                    " timestamp_sequence(0L, 10L) ts from long_sequence(10)) timestamp(ts)"
            );
            assertSql(
                    "concat\n" +
                            "-1148479920/24814/27/0.12966659791573354/0.285/-7611843578141082998/YR/A/false/[]/2827518/0x63eb3740c80f661e9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b/D/180.48.50.141/}龘/0\n" +
                            "-432358603/24975/50/0.8574212636138532/0.169/6127380367288643456/QQ/D/true/[]/6545477/0x62588b84eddb7b4a64a4822086748dc4b096d89b65baebefc4a411134408f49d/S/40.138.166.207/ʔ_\uDA8B\uDFC4/80\n" +
                            "-483853667/-11472/20/0.13450170570900255/0.670/-7723703968879725602/UL/D/false/[]/6933150/0x07efe23cedb3250630d46a3a4749c41d7a902c77fa1a889c51686790e59377ca/C/180.91.244.55/bV1/10\n" +
                            "-636975106/-30103/92/0.4892743433711657/0.098/8889492928577876455/OVL/D/true/[]/5276379/0x2accfc7ab9ae2e0b5825a545d3d3e2bdd095456a4d3d5993fdb12ef0d2c74218/O/133.153.21.245/Ǆ /30\n" +
                            "-895337819/-29733/113/0.818064803221824/0.555/5922689877598858022/MY/D/true/[]/3489481/0x92384aabd888ecb34a653286b010912b72f1d68675d867cf58b000a0492ff296/L/24.199.56.167/Oa\uDA76\uDDD4/60\n" +
                            "1234796102/-12400/29/0.5406709846540508/0.970/7122109662042058469/TGQ/D/true/[]/6589509/0x5b3ef21223ee884965009e89eacf0aadd25adf928386cdd2d992946a26184664/U/130.40.224.242/x\uDB59\uDF3B룒/20\n" +
                            "1307291490/-7455/110/0.5692090442741059/0.568/-5611837907908424613/XBH/D/false/[]/2577846/0xb680be3ee552450eef8b1c47f7e7f9ecae395228bc24ce175e2a6cc6972cc3a9/V/0.4.246.196/'ꋯɟ/70\n" +
                            "1323499098/-18321/108/0.4028291715584078/0.730/-7951611777871079805/WWL/A/true/[]/7007746/0x25ab6a3b3808d94d30ec2498d018efdd67bf677cfe82f2528ebaf26ca19a89b9/D/68.146.113.249/y\uDB7A\uDF54/90\n" +
                            "273567866/13082/37/0.7203170014947307/0.093/8503557900983561786/IZ/D/false/[]/5940665/0x402416cee4460c100b4735986b97a80520051a2ed05467f71d3abd90d55b0a12/N/8.125.226.109/w\uD908\uDECBŗ/50\n" +
                            "82099057/-26828/108/0.9859070322196475/0.297/-7336930007738575369/YS/A/true/[]/6879041/0xc7ad9255390bd17782b119de6cda359f8e21105648a5a07cd0db758e935d7aae/N/186.133.206.233/ꋵ\uD96E\uDCE7/40\n",
                    "select concat(int, '/', short, '/', byte, '/', double, '/', float, '/', long, '/', str, '/', sym, '/', bool, '/', bin, '/', date, '/', long256, '/', chr, '/', ipv4, '/', varchar, '/', timestamp) from all2 order by 1"
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
}
