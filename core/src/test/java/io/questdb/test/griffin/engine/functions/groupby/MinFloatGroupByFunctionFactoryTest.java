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
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class MinFloatGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNull() throws SqlException {
        ddl("create table tab (f float)");

        try (TableWriter w = getWriter("tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select min(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertTrue(Float.isNaN(record.getFloat(0)));
            }
        }
    }

    @Test
    public void testFirstNull() throws SqlException {
        ddl("create table tab (f float)");

        final Rnd rnd = new Rnd();
        try (TableWriter w = getWriter("tab")) {
            TableWriter.Row r = w.newRow();
            r.append();
            for (int i = 100; i > 10; i--) {
                r = w.newRow();
                r.putFloat(0, rnd.nextFloat());
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select min(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0.0011075139045715332, record.getFloat(0), 0.0001);
            }
        }
    }

    @Test
    public void testNonNull() throws SqlException {
        ddl("create table tab (f float)");

        final Rnd rnd = new Rnd();
        try (TableWriter w = getWriter("tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.putFloat(0, rnd.nextFloat());
                r.append();
            }
            w.commit();
        }
        try (RecordCursorFactory factory = select("select min(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0.0011075139045715332, record.getFloat(0), 0.0001);
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery(
                "b\tmin\tk\n" +
                        "HYRX\t0.1143\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.1250\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.0230\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.2692\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.3361\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.3864\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.3495\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.0268\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.1053\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.6807\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.8231\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.5403\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.0658\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.0502\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t0.7365\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.4592\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.2464\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.6941\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.4440\t1970-01-03T09:00:00.000000Z\n" +
                        "\t0.0962\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t0.1195\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t0.2535\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.0400\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t-0.2179\t1970-01-03T09:00:00.000000Z\n",
                "select b, min(a), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_float(0) a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_float(0) a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                "b\tmin\tk\n" +
                        "HYRX\t0.1143\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.1250\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.0230\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.2692\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.3361\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.3864\t1970-01-03T00:00:00.000000Z\n" +
                        "UQDY\t1.9459\t1970-01-03T00:00:00.000000Z\n" +
                        "UKLG\t3.9211\t1970-01-03T00:00:00.000000Z\n" +
                        "IMYF\t1.5957\t1970-01-03T00:00:00.000000Z\n" +
                        "OPHN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "MXSL\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.3495\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.0268\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.1053\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.6807\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.8231\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.5403\t1970-01-03T03:00:00.000000Z\n" +
                        "UQDY\t1.8404\t1970-01-03T03:00:00.000000Z\n" +
                        "UKLG\t3.5400\t1970-01-03T03:00:00.000000Z\n" +
                        "IMYF\t1.4828\t1970-01-03T03:00:00.000000Z\n" +
                        "OPHN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "MXSL\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.0658\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.0502\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t0.7365\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.4592\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.2464\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.6941\t1970-01-03T06:00:00.000000Z\n" +
                        "UQDY\t1.7350\t1970-01-03T06:00:00.000000Z\n" +
                        "UKLG\t3.1589\t1970-01-03T06:00:00.000000Z\n" +
                        "IMYF\t1.3699\t1970-01-03T06:00:00.000000Z\n" +
                        "OPHN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "MXSL\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.4440\t1970-01-03T09:00:00.000000Z\n" +
                        "\t0.0962\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t0.1195\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t0.2535\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.0400\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t-0.2179\t1970-01-03T09:00:00.000000Z\n" +
                        "UQDY\t1.6295\t1970-01-03T09:00:00.000000Z\n" +
                        "UKLG\t2.7779\t1970-01-03T09:00:00.000000Z\n" +
                        "IMYF\t1.2570\t1970-01-03T09:00:00.000000Z\n" +
                        "OPHN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "MXSL\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t-0.1664\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t0.0479\t1970-01-03T12:00:00.000000Z\n" +
                        "\t0.1284\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t-0.5016\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t-0.4975\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t0.1940\t1970-01-03T12:00:00.000000Z\n" +
                        "UQDY\t1.5240\t1970-01-03T12:00:00.000000Z\n" +
                        "UKLG\t2.3968\t1970-01-03T12:00:00.000000Z\n" +
                        "IMYF\t1.1441\t1970-01-03T12:00:00.000000Z\n" +
                        "OPHN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "MXSL\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t-0.3728\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t-0.1578\t1970-01-03T15:00:00.000000Z\n" +
                        "\t0.1607\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t-0.7853\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t-1.1145\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t-0.0561\t1970-01-03T15:00:00.000000Z\n" +
                        "UQDY\t1.4185\t1970-01-03T15:00:00.000000Z\n" +
                        "UKLG\t2.0158\t1970-01-03T15:00:00.000000Z\n" +
                        "IMYF\t1.0312\t1970-01-03T15:00:00.000000Z\n" +
                        "OPHN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "MXSL\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t-0.5792\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t-0.3635\t1970-01-03T18:00:00.000000Z\n" +
                        "\t0.1929\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t-1.0690\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t-1.7315\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t-0.3062\t1970-01-03T18:00:00.000000Z\n" +
                        "UQDY\t1.3131\t1970-01-03T18:00:00.000000Z\n" +
                        "UKLG\t1.6347\t1970-01-03T18:00:00.000000Z\n" +
                        "IMYF\t0.9184\t1970-01-03T18:00:00.000000Z\n" +
                        "OPHN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "MXSL\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t-0.7856\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t-0.5692\t1970-01-03T21:00:00.000000Z\n" +
                        "\t0.2252\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t-1.3526\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t-2.3485\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t-0.5562\t1970-01-03T21:00:00.000000Z\n" +
                        "UQDY\t1.2076\t1970-01-03T21:00:00.000000Z\n" +
                        "UKLG\t1.2537\t1970-01-03T21:00:00.000000Z\n" +
                        "IMYF\t0.8055\t1970-01-03T21:00:00.000000Z\n" +
                        "OPHN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "MXSL\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t-0.9920\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-0.7748\t1970-01-04T00:00:00.000000Z\n" +
                        "\t0.2574\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t-1.6363\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t-2.9655\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t-0.8063\t1970-01-04T00:00:00.000000Z\n" +
                        "UQDY\t1.1021\t1970-01-04T00:00:00.000000Z\n" +
                        "UKLG\t0.8726\t1970-01-04T00:00:00.000000Z\n" +
                        "IMYF\t0.6926\t1970-01-04T00:00:00.000000Z\n" +
                        "OPHN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "MXSL\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "UQDY\t0.9966\t1970-01-04T03:00:00.000000Z\n" +
                        "\t0.2896\t1970-01-04T03:00:00.000000Z\n" +
                        "UKLG\t0.4915\t1970-01-04T03:00:00.000000Z\n" +
                        "IMYF\t0.5797\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t-1.1984\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-0.9805\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t-1.9200\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t-3.5825\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t-1.0564\t1970-01-04T03:00:00.000000Z\n" +
                        "OPHN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "MXSL\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "\t0.0140\t1970-01-04T06:00:00.000000Z\n" +
                        "IMYF\t0.4668\t1970-01-04T06:00:00.000000Z\n" +
                        "OPHN\t0.0181\t1970-01-04T06:00:00.000000Z\n" +
                        "UKLG\t0.1105\t1970-01-04T06:00:00.000000Z\n" +
                        "MXSL\t0.7408\t1970-01-04T06:00:00.000000Z\n" +
                        "UQDY\t0.8912\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t-1.4048\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t-1.1862\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t-2.2037\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t-4.1995\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t-1.3064\t1970-01-04T06:00:00.000000Z\n",
                true,
                true,
                false
        );
    }
}
