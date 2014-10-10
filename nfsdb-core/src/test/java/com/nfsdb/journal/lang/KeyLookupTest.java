/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.journal.lang;

import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.PartitionType;
import com.nfsdb.journal.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.journal.lang.cst.DataSource;
import com.nfsdb.journal.lang.cst.Q;
import com.nfsdb.journal.lang.cst.impl.QImpl;
import com.nfsdb.journal.lang.cst.impl.ref.MutableIntVariableSource;
import com.nfsdb.journal.lang.cst.impl.ref.StringRef;
import com.nfsdb.journal.test.tools.JournalTestFactory;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Files;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class KeyLookupTest {

    @ClassRule
    public static final JournalTestFactory factory = new JournalTestFactory(
            new JournalConfigurationBuilder() {{
                $(Order.class)
                        .partitionBy(PartitionType.DAY)
                        .$int("id").index().buckets(5)
                        .$str("strId").index().buckets(5)
                        .$ts()
                ;
            }}.build(Files.makeTempDir())
    );

    @Test
    public void testLookup() throws Exception {
        JournalWriter<Order> writer = factory.writer(Order.class);

        long timestamp = Dates.toMillis("2013-01-01T00:00:00.000Z");
        long ts = timestamp;

        // total 1500 rows to append
        // over 3 days
        // millis
        long period = 3 * 24 * 60 * 60 * 1000L;
        long inc = period / 1500;

        Order order = new Order();
        for (int i = 0; i < 1000; i++) {
            order.setId(i);
            order.setTimestamp(ts + i * inc);
            order.setStrId(Integer.toString(i));
            writer.append(order);
        }
        writer.commit();

        ts += 1000 * inc + 3000;

        // insert part of same IDs again
        // search should pick up latest timestamp
        for (int i = 500; i < 1000; i++) {
            order.setId(i);
            order.setTimestamp(ts + i * inc);
            order.setStrId(Integer.toString(i));
            writer.append(order);
        }

        writer.commit();

        Q q = new QImpl();

        StringRef intCol = new StringRef("id");
        MutableIntVariableSource intId = new MutableIntVariableSource();

        // statement to search for INT id value
        DataSource<Order> dsInt = q.ds(q.forEachPartition(q.sourceDesc(writer), q.headEquals(intCol, intId)), order);

        for (int i = 0; i < 1000; i++) {
            intId.setValue(i);
            Order o = dsInt.$new().head();
            Assert.assertEquals(i, o.id);
            Assert.assertEquals("Mismatch for INT " + i, timestamp + i * inc + (i >= 500 ? 1000 * inc + 3000 : 0), o.timestamp);
        }

        StringRef strCol = new StringRef("strId");
        StringRef strId = new StringRef();
        DataSource<Order> dsStr = q.ds(q.forEachPartition(q.sourceDesc(writer), q.headEquals(strCol, strId)), order);
        for (int i = 0; i < 1000; i++) {
            strId.value = Integer.toString(i);
            Order o = dsStr.$new().head();
            Assert.assertEquals(strId.value, o.strId);
            Assert.assertEquals("Mismatch for STRING " + i, timestamp + i * inc + (i >= 500 ? 1000 * inc + 3000 : 0), o.timestamp);
        }
    }

    public static class Order {
        private int id;

        private String strId;

        private long timestamp;

        public void setId(int id) {
            this.id = id;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public void setStrId(String strId) {
            this.strId = strId;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "id=" + id +
                    ", strId='" + strId + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
