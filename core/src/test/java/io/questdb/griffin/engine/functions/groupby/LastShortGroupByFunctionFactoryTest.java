/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LastShortGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testAllNull() throws SqlException {

        compiler.compile("create table tab (f short)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select last(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getShort(0));
            }
        }
    }

    @Test
    public void testNonNull() throws SqlException {

        compiler.compile("create table tab (f short)", sqlExecutionContext);

        final Rnd rnd = new Rnd();
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.putShort(0, rnd.nextShort());
                r.append();
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select last(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(25296, record.getShort(0));
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery("b\tlast\tk\n" +
                        "\t3428\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t13278\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t21781\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t5639\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t22298\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t15926\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-3017\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-1464\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t23726\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t5231\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t8774\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t21400\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-6255\t1970-01-03T06:00:00.000000Z\n" +
                        "\t-21663\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t9053\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t15913\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t-31322\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t30629\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t5422\t1970-01-03T09:00:00.000000Z\n" +
                        "\t24860\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t5991\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t6667\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t-1261\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t9332\t1970-01-03T09:00:00.000000Z\n",
                "select b, last(a), k from x sample by 3h fill(linear)",
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
                "b\tlast\tk\n" +
                        "\t3428\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t13278\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t21781\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t5639\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t22298\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t15926\t1970-01-03T00:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-3017\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-1464\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t23726\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t5231\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t8774\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t21400\t1970-01-03T03:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-6255\t1970-01-03T06:00:00.000000Z\n" +
                        "\t-21663\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t9053\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t15913\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t-31322\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t30629\t1970-01-03T06:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t5422\t1970-01-03T09:00:00.000000Z\n" +
                        "\t24860\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t5991\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t6667\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t-1261\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t9332\t1970-01-03T09:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\t19603\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t-2579\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t32385\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t-22232\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t17099\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t9611\t1970-01-03T12:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "\t14347\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t-11825\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t495\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t15081\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t28776\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t9890\t1970-01-03T15:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "\t9091\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t-21071\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t-31395\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t-13142\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t-25083\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t10169\t1970-01-03T18:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "\t3835\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t-30317\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t2251\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t24171\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t-13406\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t10448\t1970-01-03T21:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "\t-1420\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t25973\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t-29639\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-4052\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t-1729\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t10727\t1970-01-04T00:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "\t-6677\t1970-01-04T03:00:00.000000Z\n" +
                        "ZMZV\t2316\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t16727\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t4007\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-32275\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t9948\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t11006\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\t-2305\t1970-01-04T06:00:00.000000Z\n" +
                        "LOGI\t-14555\t1970-01-04T06:00:00.000000Z\n" +
                        "QEBN\t-9244\t1970-01-04T06:00:00.000000Z\n" +
                        "\t-18094\t1970-01-04T06:00:00.000000Z\n" +
                        "FOUS\t-8221\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t7481\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t-27883\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t5038\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t21625\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t11285\t1970-01-04T06:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-04T06:00:00.000000Z\n",
                true,
                true,
                true
        );
    }
}