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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class LastIntGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNull() throws SqlException {
        ddl("create table tab (f int)");

        try (TableWriter w = getWriter("tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select last(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(Numbers.INT_NaN, record.getInt(0));
            }
        }
    }

    @Test
    public void testNonNull() throws SqlException {
        ddl("create table tab (f int)");

        try (TableWriter w = getWriter("tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.putInt(0, i);
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select last(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(11, record.getInt(0));
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery(
                "b\tlast\tk\n" +
                        "\t1627393380\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t-2002373666\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-1520872171\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t1978144263\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t-2034804966\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t-1272693194\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-1269042121\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-68027832\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t602954926\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-882371473\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t-1593630138\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t2124174232\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t862447505\t1970-01-03T06:00:00.000000Z\n" +
                        "\t-1204245663\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t2077827933\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t-246923735\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t-2080340570\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t-1003259995\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t597366062\t1970-01-03T09:00:00.000000Z\n" +
                        "\t-210935524\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t1278547815\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t-1377625589\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t-737477868\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-03T09:00:00.000000Z\n",
                "select b, last(a), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_int() a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_int() a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                "b\tlast\tk\n" +
                        "\t1627393380\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t-2002373666\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-1520872171\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t1978144263\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t-2034804966\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t-1272693194\t1970-01-03T00:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-1269042121\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-68027832\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t602954926\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-882371473\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t-1593630138\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t2124174232\t1970-01-03T03:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t862447505\t1970-01-03T06:00:00.000000Z\n" +
                        "\t-1204245663\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t2077827933\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t-246923735\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t-2080340570\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t-1003259995\t1970-01-03T06:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t597366062\t1970-01-03T09:00:00.000000Z\n" +
                        "\t-210935524\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t1278547815\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t-1377625589\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t-737477868\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-03T09:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t-321401708\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t-471695742\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t2147483647\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t332284618\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-03T12:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t-431867892\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t-205913616\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t2147483647\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t67203176\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-03T15:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t-542334076\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t59868509\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t2147483647\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t-197878266\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-03T18:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t-652800260\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t325650634\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t2147483647\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t-462959710\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-03T21:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t-763266444\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t591432760\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t2147483647\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t-728041152\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-04T00:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t-873732629\t1970-01-04T03:00:00.000000Z\n" +
                        "ZMZV\t-2043541236\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t857214887\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t2147483647\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t-993122595\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\t-1300367617\t1970-01-04T06:00:00.000000Z\n" +
                        "LOGI\t53462821\t1970-01-04T06:00:00.000000Z\n" +
                        "QEBN\t171760612\t1970-01-04T06:00:00.000000Z\n" +
                        "\t1193654610\t1970-01-04T06:00:00.000000Z\n" +
                        "FOUS\t-1923096605\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t1122997013\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t2147483647\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t-1258204039\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-04T06:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-04T06:00:00.000000Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSomeNull() throws SqlException {
        ddl("create table tab (f int)");

        try (TableWriter w = getWriter("tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                if (i % 4 == 0) {
                    r.putInt(0, i);
                }
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select last(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(Numbers.INT_NaN, record.getInt(0));
            }
        }
    }
}
