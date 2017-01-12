/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.parser;

import com.questdb.JournalEntryWriter;
import com.questdb.JournalWriter;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.Dates;
import com.questdb.misc.Numbers;
import com.questdb.misc.Rnd;
import org.junit.BeforeClass;
import org.junit.Test;

public class NullCountingTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUp() throws Exception {
        int recordCount = 10000;
        int productCount = 200;
        int employeeCount = 10;
        try (JournalWriter orders = FACTORY_CONTAINER.getFactory().writer(
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
                        "437\tx-\n",
                "select customerId, 'x'+productId+'-' from orders where customerId = 437 and productId = null", true);
    }

    @Test
    public void testStrNotEqSym() throws Exception {
        assertThat("col0\n" +
                        "6327\n",
                "select count() from orders " +
                        "where productId != null " +
                        "and employeeId != null " +
                        "and quantity != NaN " +
                        "and price != NaN " +
                        "and x != NaN " +
                        "and productId != 'FCLTJCKFM' " +
                        "and quantity != 12570116270 " +
                        "and x != 2454524639747643470", true);
    }
}
