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
import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Rnd;
import org.junit.BeforeClass;
import org.junit.Test;

public class NullCountingTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUp() throws Exception {
        int recordCount = 10000;
        int productCount = 200;
        int employeeCount = 10;
        try (JournalWriter orders = factory.writer(
                new JournalStructure("orders").
                        $int("orderId").
                        $int("customerId").
                        $str("productId").
                        $sym("employeeId").
                        $ts("orderDate").
                        $int("quantity").
                        $double("price").
                        $float("rate").
                        $long("x").
                        recordCountHint(recordCount).
                        $()
        )) {

            Rnd rnd = new Rnd();

            String employees[] = new String[employeeCount];
            for (int i = 0; i < employees.length; i++) {
                if (rnd.nextPositiveInt() % 10 == 0) {
                    employees[i] = null;
                } else {
                    employees[i] = rnd.nextString(9);
                }
            }

            String[] productId = new String[productCount];
            for (int i = 0; i < productId.length; i++) {
                if (rnd.nextPositiveInt() % 30 == 0) {
                    productId[i] = null;
                } else {
                    productId[i] = rnd.nextString(9);
                }
            }

            long timestamp = Dates.parseDateTime("2014-05-04T10:30:00.000Z");
            int tsIncrement = 10000;

            int orderId = 0;
            for (int i = 0; i < recordCount; i++) {
                JournalEntryWriter w = orders.entryWriter();
                w.putInt(0, ++orderId);
                w.putInt(1, rnd.nextPositiveInt() % 500);
                w.putStr(2, productId[rnd.nextPositiveInt() % productCount]);
                w.putSym(3, employees[rnd.nextPositiveInt() % employeeCount]);
                w.putDate(4, timestamp += tsIncrement);
                w.putInt(5, rnd.nextPositiveInt() % 10 == 0 ? Numbers.INT_NaN : rnd.nextPositiveInt());
                w.putDouble(6, rnd.nextPositiveInt() % 10 == 0 ? Double.NaN : rnd.nextDouble());
                w.putFloat(7, rnd.nextPositiveInt() % 10 == 0 ? Float.NaN : rnd.nextFloat());
                w.putLong(8, rnd.nextPositiveInt() % 10 == 0 ? Numbers.LONG_NaN : rnd.nextLong());
                w.append();
            }
            orders.commit();
        }
    }

    @Test
    public void testCountDouble() throws Exception {
        assertThat("customerId\tcol0\tc\n" +
                        "437\t24\t2\n",
                "select customerId, count(), count() - count(price) c from orders where customerId = 437", true);
    }

    @Test
    public void testCountFloat() throws Exception {
        assertThat("customerId\tcol0\tc\n" +
                        "437\t24\t5\n",
                "select customerId, count(), count() - count(rate) c from orders where customerId = 437", true);
    }

    @Test
    public void testCountInt() throws Exception {
        assertThat("customerId\tcol0\tc\n" +
                        "437\t24\t4\n",
                "select customerId, count(), count() - count(quantity) c from orders where customerId = 437", true);
    }

    @Test
    public void testCountLong() throws Exception {
        assertThat("customerId\tcol0\tc\n" +
                        "437\t24\t3\n",
                "select customerId, count(), count() - count(x) c from orders where customerId = 437", true);
    }

    @Test
    public void testCountStr() throws Exception {
        assertThat("customerId\tcol0\tc\n" +
                        "437\t24\t1\n",
                "select customerId, count(), count() - count(productId) c from orders where customerId = 437", true);
    }

    @Test
    public void testCountSym() throws Exception {
        assertThat("customerId\tcol0\tc\n" +
                        "437\t24\t3\n",
                "select customerId, count(), count() - count(employeeId) c from orders where customerId = 437", true);
    }

    @Test
    public void testStrConcat() throws Exception {
        assertThat("customerId\tcol0\n" +
                        "437\txnull-\n",
                "select customerId, 'x'+productId+'-' from orders where customerId = 437 and productId = null", true);
    }
}
