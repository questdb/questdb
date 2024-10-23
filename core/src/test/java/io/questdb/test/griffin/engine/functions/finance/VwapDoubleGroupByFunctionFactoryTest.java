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

package io.questdb.test.griffin.engine.functions.finance;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class VwapDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    String DDL_BENCH = "create atomic table trades as (select \n" +
            "            rnd_timestamp('2022-03-08T00:00:00Z'::timestamp, \n" +
            "            '2022-03-15T23:59:59Z'::timestamp, 0) timestamp, \n" +
            "            abs(rnd_int()) % x as price, \n" +
            "            abs(rnd_int()) % x as amount, \n" +
            "            rnd_symbol('x', 'y', 'z') symbol from long_sequence(100000) order by timestamp) timestamp(timestamp) partition by day wal;";


    @Test
    public void testRawQueryNoRewrite() throws Exception {
        assertMemoryLeak(() -> {
            ddl(DDL_BENCH);
            drainWalQueue();
            assertSql("timestamp\tsymbol\tvwap\n" +
                            "2022-03-08T00:00:00.000000Z\tx\t64862.333333333336\n" +
                            "2022-03-08T00:00:00.000000Z\tz\t65223.3141384082\n" +
                            "2022-03-08T00:00:00.000000Z\ty\t65067.719961818046\n" +
                            "2022-03-08T08:00:00.000000Z\tz\t64927.220363321394\n" +
                            "2022-03-08T08:00:00.000000Z\tx\t64611.51358105875\n" +
                            "2022-03-08T08:00:00.000000Z\ty\t64726.42527997777\n" +
                            "2022-03-08T16:00:00.000000Z\ty\t64908.88282654894\n" +
                            "2022-03-08T16:00:00.000000Z\tx\t64756.34384531534\n" +
                            "2022-03-08T16:00:00.000000Z\tz\t64603.57593944086\n" +
                            "2022-03-09T00:00:00.000000Z\ty\t64738.0\n" +
                            "2022-03-09T00:00:00.000000Z\tz\t65465.48242455836\n" +
                            "2022-03-09T00:00:00.000000Z\tx\t64929.44826557444\n" +
                            "2022-03-09T08:00:00.000000Z\tz\t64897.91255092547\n" +
                            "2022-03-09T08:00:00.000000Z\ty\t64830.51669078724\n" +
                            "2022-03-09T08:00:00.000000Z\tx\t64891.49674541972\n" +
                            "2022-03-09T16:00:00.000000Z\tx\t64955.69275856079\n" +
                            "2022-03-09T16:00:00.000000Z\ty\t64913.947136900286\n" +
                            "2022-03-09T16:00:00.000000Z\tz\t64935.67805643783\n" +
                            "2022-03-10T00:00:00.000000Z\tz\t66093.66666666667\n" +
                            "2022-03-10T00:00:00.000000Z\ty\t65590.50088328491\n" +
                            "2022-03-10T00:00:00.000000Z\tx\t65118.53148264591\n" +
                            "2022-03-10T08:00:00.000000Z\ty\t65223.08186331457\n" +
                            "2022-03-10T08:00:00.000000Z\tx\t64978.56580331711\n" +
                            "2022-03-10T08:00:00.000000Z\tz\t64713.35884912949\n" +
                            "2022-03-10T16:00:00.000000Z\tz\t64628.96821582012\n" +
                            "2022-03-10T16:00:00.000000Z\tx\t64452.16891199036\n" +
                            "2022-03-10T16:00:00.000000Z\ty\t64410.04436199934\n" +
                            "2022-03-11T00:00:00.000000Z\ty\t63461.666666666664\n" +
                            "2022-03-11T00:00:00.000000Z\tx\t64720.990661150245\n" +
                            "2022-03-11T00:00:00.000000Z\tz\t64094.398776489084\n" +
                            "2022-03-11T08:00:00.000000Z\ty\t64044.90849605121\n" +
                            "2022-03-11T08:00:00.000000Z\tx\t64069.07894588057\n" +
                            "2022-03-11T08:00:00.000000Z\tz\t64339.19222551965\n" +
                            "2022-03-11T16:00:00.000000Z\tx\t64523.1564551989\n" +
                            "2022-03-11T16:00:00.000000Z\ty\t64649.428410493565\n" +
                            "2022-03-11T16:00:00.000000Z\tz\t64686.819658617314\n" +
                            "2022-03-12T00:00:00.000000Z\ty\t65391.0\n" +
                            "2022-03-12T00:00:00.000000Z\tz\t64976.76220580677\n" +
                            "2022-03-12T00:00:00.000000Z\tx\t64726.58715111293\n" +
                            "2022-03-12T08:00:00.000000Z\ty\t64983.83529605692\n" +
                            "2022-03-12T08:00:00.000000Z\tx\t64900.565183759914\n" +
                            "2022-03-12T08:00:00.000000Z\tz\t65093.425172009294\n" +
                            "2022-03-12T16:00:00.000000Z\tz\t65094.27084231691\n" +
                            "2022-03-12T16:00:00.000000Z\tx\t64960.79963362835\n" +
                            "2022-03-12T16:00:00.000000Z\ty\t65042.99801027536\n" +
                            "2022-03-13T00:00:00.000000Z\ty\t65017.666666666664\n" +
                            "2022-03-13T00:00:00.000000Z\tx\t64839.35369069215\n" +
                            "2022-03-13T00:00:00.000000Z\tz\t64731.67143857638\n" +
                            "2022-03-13T08:00:00.000000Z\tx\t64357.94383652315\n" +
                            "2022-03-13T08:00:00.000000Z\ty\t64711.66601538703\n" +
                            "2022-03-13T08:00:00.000000Z\tz\t64845.20665214627\n" +
                            "2022-03-13T16:00:00.000000Z\ty\t64921.43928562733\n" +
                            "2022-03-13T16:00:00.000000Z\tx\t64829.24002775769\n" +
                            "2022-03-13T16:00:00.000000Z\tz\t64755.46320102003\n" +
                            "2022-03-14T00:00:00.000000Z\ty\t63960.666666666664\n" +
                            "2022-03-14T00:00:00.000000Z\tx\t63668.87171920535\n" +
                            "2022-03-14T00:00:00.000000Z\tz\t63337.06725939944\n" +
                            "2022-03-14T08:00:00.000000Z\tx\t63506.228594016175\n" +
                            "2022-03-14T08:00:00.000000Z\ty\t63985.21843202245\n" +
                            "2022-03-14T08:00:00.000000Z\tz\t64244.27491861241\n" +
                            "2022-03-14T16:00:00.000000Z\ty\t64240.66068857563\n" +
                            "2022-03-14T16:00:00.000000Z\tz\t64519.510070717704\n" +
                            "2022-03-14T16:00:00.000000Z\tx\t64285.90821428881\n" +
                            "2022-03-15T00:00:00.000000Z\tz\t64774.0\n" +
                            "2022-03-15T00:00:00.000000Z\ty\t64646.52054279925\n" +
                            "2022-03-15T00:00:00.000000Z\tx\t63999.18700105883\n" +
                            "2022-03-15T08:00:00.000000Z\tx\t63847.107913339416\n" +
                            "2022-03-15T08:00:00.000000Z\tz\t63970.420721277966\n" +
                            "2022-03-15T08:00:00.000000Z\ty\t63859.63989615813\n" +
                            "2022-03-15T16:00:00.000000Z\ty\t63993.608790120845\n" +
                            "2022-03-15T16:00:00.000000Z\tz\t64132.988474548714\n" +
                            "2022-03-15T16:00:00.000000Z\tx\t64248.54643647679\n",
                    "select timestamp, symbol, vwap(timestamp, min_price, max_price, closing_price, volume)\n" +
                            "from (\n" +
                            "    select timestamp, symbol, min(price) min_price, max(price) max_price, last(price) closing_price, sum(amount) volume\n" +
                            "    from trades\n" +
                            "    sample by 8h\n" +
                            ")\n" +
                            "group by timestamp, symbol\n" +
                            "order by timestamp asc;\n");
        });
    }


    @Test
    public void testRewrittenQuery() throws Exception {
        assertMemoryLeak(() -> {
            ddl(DDL_BENCH);
            drainWalQueue();


            assertSql("", "select timestamp, symbol, vwap(price, amount) from trades sample by 8h");
        });
    }
}
