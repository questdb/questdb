/*
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
 */

package com.nfsdb.ql;

import com.nfsdb.Journal;
import com.nfsdb.JournalWriter;
import com.nfsdb.PartitionType;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.test.tools.JournalTestFactory;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Files;
import org.junit.Before;
import org.junit.Rule;

public class SearchByKeysTest {

    @Rule
    public final JournalTestFactory factory = new JournalTestFactory(
            new JournalConfigurationBuilder() {{
                $(Order.class)
                        .partitionBy(PartitionType.DAY)
                        .$int("id").index().buckets(5)
                        .$str("strId").index().buckets(5)
                        .$ts()
                ;
            }}.build(Files.makeTempDir())
    );

    private long timestamp;

    @Before
    public void setUp() {
        timestamp = Dates.parseDateTime("2013-01-01T00:00:00.000Z");
        // total 1500 rows to append
        // over 3 days
        // millis
    }

/*
    @Test
    public void testSearchByIntKey() throws Exception {

        Journal<Order> journal = prepareTestData();
        IntParameter param = new IntParameter();
        IntEqualsOperator filter = new IntEqualsOperator();
        filter.setLhs(new RecordSourceColumn("id", journal.getMetadata()));
        filter.setRhs(param);

        /*/

    /**
     * QUERY
     * // from order head by id = 123
     * // **selects latest version of record with int id 123
     * DataSource<Order> dsInt = new DataSourceImpl<>(
     * new JournalSource(
     * new JournalDescPartitionSource(journal, false),
     * new KvIndexHeadRowSource("id",
     * new SingleIntHashKeySource("id", param),
     * 1,
     * 0,
     * filter
     * )
     * ),
     * new Order()
     * );
     * <p/>
     * // assert
     * for (int i = 0; i < 1000; i++) {
     * param.setValue(i);
     * Order o = dsInt.$new().head();
     * Assert.assertEquals(i, o.id);
     * Assert.assertEquals("Mismatch for INT " + i, timestamp + i * inc + (i >= 500 ? 1000 * inc + 3000 : 0), o.timestamp);
     * }
     * }
     */

    private Journal<Order> prepareTestData() throws JournalException {
        JournalWriter<Order> writer = factory.writer(Order.class);
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
        return writer;
    }

    public static class Order {
        private int id;

        private String strId;

        private long timestamp;

        public void setId(int id) {
            this.id = id;
        }

        public void setStrId(String strId) {
            this.strId = strId;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
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
