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
                        $double("quantity").
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
                w.putDouble(5, rnd.nextDouble());
                w.putDouble(6, rnd.nextDouble());
                w.putFloat(7, rnd.nextFloat());
                w.append();
            }
            orders.commit();
        }
    }

    @Test
    public void testSumDouble() throws Exception {
        assertThat("employeeId\tsum\n" +
                        "TGPGWFFYU\t146.715367621278\n" +
                        "YRXPEHNRX\t7840.331858809368\n" +
                        "SRYRFBVTM\t13477.542281401748\n" +
                        "FOWLPDXYS\t-1370.880496708310\n" +
                        "VTJWCPSWH\t12075.233063181322\n" +
                        "GZSXUXIBB\t-9776.804545029104\n" +
                        "FJGETJRSZ\t-13132.014957685670\n" +
                        "BEOUOJSHR\t4947.305823288266\n" +
                        "DEYYQEHBH\t13619.666196977540\n" +
                        "UEDRQQULO\t-25999.561755591844\n",
                "select employeeId, sum(price) sum from orders", true);
    }

    @Test
    public void testSumInt() throws Exception {
        assertThat("employeeId\tsum\n" +
                        "TGPGWFFYU\t101526\n" +
                        "YRXPEHNRX\t92787\n" +
                        "SRYRFBVTM\t99794\n" +
                        "FOWLPDXYS\t101835\n" +
                        "VTJWCPSWH\t105804\n" +
                        "GZSXUXIBB\t96551\n" +
                        "FJGETJRSZ\t99962\n" +
                        "BEOUOJSHR\t96631\n" +
                        "DEYYQEHBH\t98242\n" +
                        "UEDRQQULO\t100043\n",
                "select employeeId, sum(productId) sum from orders", true);
    }
}
