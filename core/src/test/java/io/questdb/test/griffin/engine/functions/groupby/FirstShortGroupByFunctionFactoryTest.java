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
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class FirstShortGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testFirstShort() throws SqlException {
        ddl("create table tab (f short)");

        try (TableWriter w = getWriter("tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.putShort(0, (short) i);
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select first(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(100, record.getShort(0));
            }
        }
    }

    @Test
    public void testFirstZero() throws SqlException {
        ddl("create table tab (f short)");

        try (TableWriter w = getWriter("tab")) {
            TableWriter.Row r = w.newRow();
            r.append();
            for (int i = 100; i > 10; i--) {
                r = w.newRow();
                r.putInt(0, i);
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select first(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getShort(0));
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery(
                "b\tfirst\tk\n" +
                        "\t-24357\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t-1593\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t21781\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t18457\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t-19127\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t26142\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t29978\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-18357\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t-2018\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-15331\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t-10913\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t3172\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-1605\t1970-01-03T06:00:00.000000Z\n" +
                        "\t2733\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t24092\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t-5716\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t-31322\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t-30103\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t-26777\t1970-01-03T09:00:00.000000Z\n" +
                        "\t24682\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-26828\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t6667\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t-24648\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t-6439\t1970-01-03T09:00:00.000000Z\n",
                "select b, first(a), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_short() a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_short() a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                "b\tfirst\tk\n" +
                        "\t-24357\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t-1593\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t21781\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t18457\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t-19127\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t26142\t1970-01-03T00:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t29978\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-18357\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t-2018\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-15331\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t-10913\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t3172\t1970-01-03T03:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-1605\t1970-01-03T06:00:00.000000Z\n" +
                        "\t2733\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t24092\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t-5716\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t-31322\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t-30103\t1970-01-03T06:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t-26777\t1970-01-03T09:00:00.000000Z\n" +
                        "\t24682\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-26828\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t6667\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t-24648\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t-6439\t1970-01-03T09:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\t17459\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t19050\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t-19193\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t-22334\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t13587\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t28566\t1970-01-03T12:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "\t10237\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t31433\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t-13738\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t-17840\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t-11585\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t-1965\t1970-01-03T15:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "\t3014\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t-21720\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t-8283\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t-13346\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t28779\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t-32496\t1970-01-03T18:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "\t-4208\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t-9337\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t-2828\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t-8852\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t3607\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t2509\t1970-01-03T21:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "\t-11430\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t3046\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t2627\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-4358\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t-21565\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t-28022\t1970-01-04T00:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "\t-18653\t1970-01-04T03:00:00.000000Z\n" +
                        "ZMZV\t31406\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t15429\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t8082\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t136\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t18799\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t6983\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\t28775\t1970-01-04T06:00:00.000000Z\n" +
                        "LOGI\t25721\t1970-01-04T06:00:00.000000Z\n" +
                        "QEBN\t-9244\t1970-01-04T06:00:00.000000Z\n" +
                        "\t11356\t1970-01-04T06:00:00.000000Z\n" +
                        "FOUS\t2503\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t27812\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t13537\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t4630\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t-6373\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t-23548\t1970-01-04T06:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-04T06:00:00.000000Z\n",
                true,
                true,
                false
        );
    }
}
