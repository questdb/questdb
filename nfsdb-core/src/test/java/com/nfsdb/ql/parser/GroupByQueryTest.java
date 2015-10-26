/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.parser;

import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Rnd;
import org.junit.BeforeClass;
import org.junit.Test;

public class GroupByQueryTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUp() throws Exception {
        int recordCount = 10000;
        int employeeCount = 10;
        try (JournalWriter orders = factory.writer(
                new JournalStructure("orders").
                        $int("orderId").
                        $int("customerId").
                        $int("productId").
                        $str("employeeId").
                        $ts("orderDate").
                        $int("quantity").
                        $double("price").
                        $float("rate").
                        recordCountHint(recordCount).
                        $()
        )) {


            Rnd rnd = new Rnd();

            String employees[] = new String[employeeCount];
            for (int i = 0; i < employees.length; i++) {
                employees[i] = rnd.nextString(9);
            }

            long timestamp = Dates.parseDateTime("2014-05-04T10:30:00.000Z");
            int tsIncrement = 10000;

            int orderId = 0;
            for (int i = 0; i < recordCount; i++) {
                JournalEntryWriter w = orders.entryWriter();
                w.putInt(0, ++orderId);
                w.putInt(1, rnd.nextPositiveInt() % 500);
                w.putInt(2, rnd.nextPositiveInt() % 200);
                w.putStr(3, employees[rnd.nextPositiveInt() % employeeCount]);
                w.putDate(4, timestamp += tsIncrement);
                w.putInt(5, rnd.nextPositiveInt());
                w.putDouble(6, rnd.nextDouble());
                w.putFloat(7, rnd.nextFloat());
                w.append();
            }
            orders.commit();
        }
    }

    @Test
    public void testAggregateExpression() throws Exception {
        assertThat("employeeId\tsum\tsum2\tx\n" +
                        "TGPGWFFYU\t97328\t-21968.018329648252\t75359.981670351760\n" +
                        "DEYYQEHBH\t95288\t-4394.647402081921\t90893.352597918080\n" +
                        "SRYRFBVTM\t96798\t1945.437433247252\t98743.437433247248\n" +
                        "GZSXUXIBB\t97026\t3710.011166965701\t100736.011166965712\n" +
                        "UEDRQQULO\t104395\t-5341.399618807004\t99053.600381192992\n" +
                        "FOWLPDXYS\t98350\t-25051.961159685804\t73298.038840314192\n" +
                        "FJGETJRSZ\t103481\t-5023.046150211212\t98457.953849788784\n" +
                        "BEOUOJSHR\t96459\t-7031.317012047984\t89427.682987952016\n" +
                        "YRXPEHNRX\t96407\t-5897.650745672292\t90509.349254327712\n" +
                        "VTJWCPSWH\t102802\t-15878.443493302174\t86923.556506697824\n",
                "select employeeId, sum(productId) sum, sum(price) sum2, sum(price)+sum(productId) x from orders", true);
    }

    @Test
    public void testLSumInt() throws Exception {
        assertThat("TGPGWFFYU\t1039152863257\t-229222375\n" +
                        "DEYYQEHBH\t1052562284318\t295296798\n" +
                        "SRYRFBVTM\t1063416340387\t-1735549021\n" +
                        "GZSXUXIBB\t1091735231209\t813538025\n" +
                        "UEDRQQULO\t1149120836083\t-1930399245\n" +
                        "FOWLPDXYS\t1085760619210\t-866106678\n" +
                        "FJGETJRSZ\t1112236778603\t-159751061\n" +
                        "BEOUOJSHR\t1046534829281\t-1437190943\n" +
                        "YRXPEHNRX\t1070359446329\t912589625\n" +
                        "VTJWCPSWH\t1121957229257\t970765001\n",
                "select employeeId, lsum(quantity) s, sum(quantity) s2 from orders");

    }

    @Test
    public void testSumDouble() throws Exception {
        assertThat("employeeId\tsum\n" +
                        "TGPGWFFYU\t-21968.018329648252\n" +
                        "DEYYQEHBH\t-4394.647402081921\n" +
                        "SRYRFBVTM\t1945.437433247252\n" +
                        "GZSXUXIBB\t3710.011166965701\n" +
                        "UEDRQQULO\t-5341.399618807004\n" +
                        "FOWLPDXYS\t-25051.961159685804\n" +
                        "FJGETJRSZ\t-5023.046150211212\n" +
                        "BEOUOJSHR\t-7031.317012047984\n" +
                        "YRXPEHNRX\t-5897.650745672292\n" +
                        "VTJWCPSWH\t-15878.443493302174\n",
                "select employeeId, sum(price) sum from orders", true);
    }

    @Test
    public void testSumFloat() throws Exception {
        assertThat("TGPGWFFYU\t489.412259876728\n" +
                        "DEYYQEHBH\t490.928304672241\n" +
                        "SRYRFBVTM\t490.777038216591\n" +
                        "GZSXUXIBB\t500.867759287357\n" +
                        "UEDRQQULO\t516.323502302170\n" +
                        "FOWLPDXYS\t497.242702662945\n" +
                        "FJGETJRSZ\t507.531779348850\n" +
                        "BEOUOJSHR\t490.108210325241\n" +
                        "YRXPEHNRX\t506.545447528362\n" +
                        "VTJWCPSWH\t508.323197126389\n",
                "select employeeId, sum(rate) s from orders");

    }

    @Test
    public void testSumInt() throws Exception {
        assertThat("employeeId\tsum\n" +
                        "TGPGWFFYU\t97328\n" +
                        "DEYYQEHBH\t95288\n" +
                        "SRYRFBVTM\t96798\n" +
                        "GZSXUXIBB\t97026\n" +
                        "UEDRQQULO\t104395\n" +
                        "FOWLPDXYS\t98350\n" +
                        "FJGETJRSZ\t103481\n" +
                        "BEOUOJSHR\t96459\n" +
                        "YRXPEHNRX\t96407\n" +
                        "VTJWCPSWH\t102802\n",
                "select employeeId, sum(productId) sum from orders", true);
    }
}
