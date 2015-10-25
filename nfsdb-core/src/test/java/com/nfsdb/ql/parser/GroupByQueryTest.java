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
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class GroupByQueryTest extends AbstractOptimiserTest {
    @Before
    public void setUp() throws Exception {
        int recordCount = 10000;
        int employeeCount = 10;
        JournalWriter orders = factory.writer(
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
        );


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

    @Test
    @Ignore
    public void testPlain() throws Exception {
        assertThat("", "select avg(price) from orders");
    }
}
