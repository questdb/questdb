/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class FirstFloatGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNull() throws SqlException {
        execute("create table tab (f float)");

        try (TableWriter w = getWriter("tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select first(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertTrue(Numbers.isNull(record.getFloat(0)));
            }
        }
    }

    @Test
    public void testFirstNull() throws SqlException {
        execute("create table tab (f float)");

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

        try (RecordCursorFactory factory = select("select first(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertTrue(Numbers.isNull(record.getFloat(0)));
            }
        }
    }

    @Test
    public void testNonNull() throws SqlException {
        execute("create table tab (f float)");

        final Rnd rnd = new Rnd();
        try (TableWriter w = getWriter("tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.putFloat(0, rnd.nextFloat());
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select first(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0.6607777894187332, record.getFloat(0), 0.0001);
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery(
                "b\tfirst\tk\n" +
                        "HYRX\t0.1143\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.4218\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.7261\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.3854\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.3361\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.9687\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.8685\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.4372\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.6359\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.6807\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.8231\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.8314\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.2263\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.2712\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t0.7365\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.9419\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.2464\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.6941\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.4440\t1970-01-03T09:00:00.000000Z\n" +
                        "\t0.0962\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t0.7292\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t0.2535\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.0400\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t-0.4159\t1970-01-03T09:00:00.000000Z\n",
                "select b, first(a), k from x sample by 3h fill(linear)",
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
                "b\tfirst\tk\n" +
                        "HYRX\t0.1143\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.4218\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.7261\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.3854\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.3361\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.9687\t1970-01-03T00:00:00.000000Z\n" +
                        "UQDY\t1.9459\t1970-01-03T00:00:00.000000Z\n" +
                        "UKLG\t4.2643\t1970-01-03T00:00:00.000000Z\n" +
                        "IMYF\t0.7330\t1970-01-03T00:00:00.000000Z\n" +
                        "OPHN\tnull\t1970-01-03T00:00:00.000000Z\n" +
                        "MXSL\tnull\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.8685\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.4372\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.6359\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.6807\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.8231\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.8314\t1970-01-03T03:00:00.000000Z\n" +
                        "UQDY\t1.8404\t1970-01-03T03:00:00.000000Z\n" +
                        "UKLG\t3.8671\t1970-01-03T03:00:00.000000Z\n" +
                        "IMYF\t0.7160\t1970-01-03T03:00:00.000000Z\n" +
                        "OPHN\tnull\t1970-01-03T03:00:00.000000Z\n" +
                        "MXSL\tnull\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.2263\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.2712\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t0.7365\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.9419\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.2464\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.6941\t1970-01-03T06:00:00.000000Z\n" +
                        "UQDY\t1.7350\t1970-01-03T06:00:00.000000Z\n" +
                        "UKLG\t3.4698\t1970-01-03T06:00:00.000000Z\n" +
                        "IMYF\t0.6989\t1970-01-03T06:00:00.000000Z\n" +
                        "OPHN\tnull\t1970-01-03T06:00:00.000000Z\n" +
                        "MXSL\tnull\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.4440\t1970-01-03T09:00:00.000000Z\n" +
                        "\t0.0962\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t0.7292\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t0.2535\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.0400\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t-0.4159\t1970-01-03T09:00:00.000000Z\n" +
                        "UQDY\t1.6295\t1970-01-03T09:00:00.000000Z\n" +
                        "UKLG\t3.0725\t1970-01-03T09:00:00.000000Z\n" +
                        "IMYF\t0.6819\t1970-01-03T09:00:00.000000Z\n" +
                        "OPHN\tnull\t1970-01-03T09:00:00.000000Z\n" +
                        "MXSL\tnull\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t-0.1664\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t-0.4348\t1970-01-03T12:00:00.000000Z\n" +
                        "\t0.1302\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t-1.0581\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t0.7220\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t0.1940\t1970-01-03T12:00:00.000000Z\n" +
                        "UQDY\t1.5240\t1970-01-03T12:00:00.000000Z\n" +
                        "UKLG\t2.6752\t1970-01-03T12:00:00.000000Z\n" +
                        "IMYF\t0.6648\t1970-01-03T12:00:00.000000Z\n" +
                        "OPHN\tnull\t1970-01-03T12:00:00.000000Z\n" +
                        "MXSL\tnull\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t-0.3728\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t-1.1231\t1970-01-03T15:00:00.000000Z\n" +
                        "\t0.1642\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t-1.7003\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t0.7147\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t-0.0561\t1970-01-03T15:00:00.000000Z\n" +
                        "UQDY\t1.4185\t1970-01-03T15:00:00.000000Z\n" +
                        "UKLG\t2.2779\t1970-01-03T15:00:00.000000Z\n" +
                        "IMYF\t0.6478\t1970-01-03T15:00:00.000000Z\n" +
                        "OPHN\tnull\t1970-01-03T15:00:00.000000Z\n" +
                        "MXSL\tnull\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t-0.5792\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t-1.8115\t1970-01-03T18:00:00.000000Z\n" +
                        "\t0.1982\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t-2.3425\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t0.7075\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t-0.3062\t1970-01-03T18:00:00.000000Z\n" +
                        "UQDY\t1.3131\t1970-01-03T18:00:00.000000Z\n" +
                        "UKLG\t1.8806\t1970-01-03T18:00:00.000000Z\n" +
                        "IMYF\t0.6308\t1970-01-03T18:00:00.000000Z\n" +
                        "OPHN\tnull\t1970-01-03T18:00:00.000000Z\n" +
                        "MXSL\tnull\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t-0.7856\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t-2.4998\t1970-01-03T21:00:00.000000Z\n" +
                        "\t0.2323\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t-2.9847\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t0.7002\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t-0.5562\t1970-01-03T21:00:00.000000Z\n" +
                        "UQDY\t1.2076\t1970-01-03T21:00:00.000000Z\n" +
                        "UKLG\t1.4834\t1970-01-03T21:00:00.000000Z\n" +
                        "IMYF\t0.6137\t1970-01-03T21:00:00.000000Z\n" +
                        "OPHN\tnull\t1970-01-03T21:00:00.000000Z\n" +
                        "MXSL\tnull\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t-0.9920\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-3.1882\t1970-01-04T00:00:00.000000Z\n" +
                        "\t0.2663\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t-3.6269\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t0.6929\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t-0.8063\t1970-01-04T00:00:00.000000Z\n" +
                        "UQDY\t1.1021\t1970-01-04T00:00:00.000000Z\n" +
                        "UKLG\t1.0861\t1970-01-04T00:00:00.000000Z\n" +
                        "IMYF\t0.5967\t1970-01-04T00:00:00.000000Z\n" +
                        "OPHN\tnull\t1970-01-04T00:00:00.000000Z\n" +
                        "MXSL\tnull\t1970-01-04T00:00:00.000000Z\n" +
                        "UQDY\t0.9966\t1970-01-04T03:00:00.000000Z\n" +
                        "\t0.3003\t1970-01-04T03:00:00.000000Z\n" +
                        "UKLG\t0.6888\t1970-01-04T03:00:00.000000Z\n" +
                        "IMYF\t0.5797\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t-1.1984\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-3.8765\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t-4.2691\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t0.6857\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t-1.0564\t1970-01-04T03:00:00.000000Z\n" +
                        "OPHN\tnull\t1970-01-04T03:00:00.000000Z\n" +
                        "MXSL\tnull\t1970-01-04T03:00:00.000000Z\n" +
                        "\t0.8380\t1970-01-04T06:00:00.000000Z\n" +
                        "IMYF\t0.5626\t1970-01-04T06:00:00.000000Z\n" +
                        "OPHN\t0.4920\t1970-01-04T06:00:00.000000Z\n" +
                        "UKLG\t0.2915\t1970-01-04T06:00:00.000000Z\n" +
                        "MXSL\t0.7408\t1970-01-04T06:00:00.000000Z\n" +
                        "UQDY\t0.8912\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t-1.4048\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t-4.5648\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t-4.9113\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t0.6784\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t-1.3064\t1970-01-04T06:00:00.000000Z\n",
                true,
                true,
                false
        );
    }
}
