/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
                """
                        b\tfirst\tk
                        HYRX\t0.11427981\t1970-01-03T00:00:00.000000Z
                        PEHN\t0.42177683\t1970-01-03T00:00:00.000000Z
                        \t0.7261136\t1970-01-03T00:00:00.000000Z
                        VTJW\t0.38539946\t1970-01-03T00:00:00.000000Z
                        CPSW\t0.33608252\t1970-01-03T00:00:00.000000Z
                        RXGZ\t0.9687423\t1970-01-03T00:00:00.000000Z
                        VTJW\t0.8685154\t1970-01-03T03:00:00.000000Z
                        \t0.43717694\t1970-01-03T03:00:00.000000Z
                        CPSW\t0.63591444\t1970-01-03T03:00:00.000000Z
                        HYRX\t0.6806873\t1970-01-03T03:00:00.000000Z
                        PEHN\t0.82312495\t1970-01-03T03:00:00.000000Z
                        RXGZ\t0.83141637\t1970-01-03T03:00:00.000000Z
                        VTJW\t0.2263152\t1970-01-03T06:00:00.000000Z
                        \t0.27115327\t1970-01-03T06:00:00.000000Z
                        CPSW\t0.73651147\t1970-01-03T06:00:00.000000Z
                        PEHN\t0.94187194\t1970-01-03T06:00:00.000000Z
                        HYRX\t0.24642265\t1970-01-03T06:00:00.000000Z
                        RXGZ\t0.6940904\t1970-01-03T06:00:00.000000Z
                        RXGZ\t0.44402504\t1970-01-03T09:00:00.000000Z
                        \t0.09618586\t1970-01-03T09:00:00.000000Z
                        CPSW\t0.7292482\t1970-01-03T09:00:00.000000Z
                        PEHN\t0.25353473\t1970-01-03T09:00:00.000000Z
                        HYRX\t0.04001695\t1970-01-03T09:00:00.000000Z
                        VTJW\t-0.41588497\t1970-01-03T09:00:00.000000Z
                        """,
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
                """
                        b\tfirst\tk
                        HYRX\t0.11427981\t1970-01-03T00:00:00.000000Z
                        PEHN\t0.42177683\t1970-01-03T00:00:00.000000Z
                        \t0.7261136\t1970-01-03T00:00:00.000000Z
                        VTJW\t0.38539946\t1970-01-03T00:00:00.000000Z
                        CPSW\t0.33608252\t1970-01-03T00:00:00.000000Z
                        RXGZ\t0.9687423\t1970-01-03T00:00:00.000000Z
                        UQDY\t1.945923\t1970-01-03T00:00:00.000000Z
                        UKLG\t4.2643375\t1970-01-03T00:00:00.000000Z
                        IMYF\t0.73298883\t1970-01-03T00:00:00.000000Z
                        OPHN\tnull\t1970-01-03T00:00:00.000000Z
                        MXSL\tnull\t1970-01-03T00:00:00.000000Z
                        VTJW\t0.8685154\t1970-01-03T03:00:00.000000Z
                        \t0.43717694\t1970-01-03T03:00:00.000000Z
                        CPSW\t0.63591444\t1970-01-03T03:00:00.000000Z
                        HYRX\t0.6806873\t1970-01-03T03:00:00.000000Z
                        PEHN\t0.82312495\t1970-01-03T03:00:00.000000Z
                        RXGZ\t0.83141637\t1970-01-03T03:00:00.000000Z
                        UQDY\t1.8404468\t1970-01-03T03:00:00.000000Z
                        UKLG\t3.8670547\t1970-01-03T03:00:00.000000Z
                        IMYF\t0.71595365\t1970-01-03T03:00:00.000000Z
                        OPHN\tnull\t1970-01-03T03:00:00.000000Z
                        MXSL\tnull\t1970-01-03T03:00:00.000000Z
                        VTJW\t0.2263152\t1970-01-03T06:00:00.000000Z
                        \t0.27115327\t1970-01-03T06:00:00.000000Z
                        CPSW\t0.73651147\t1970-01-03T06:00:00.000000Z
                        PEHN\t0.94187194\t1970-01-03T06:00:00.000000Z
                        HYRX\t0.24642265\t1970-01-03T06:00:00.000000Z
                        RXGZ\t0.6940904\t1970-01-03T06:00:00.000000Z
                        UQDY\t1.7349707\t1970-01-03T06:00:00.000000Z
                        UKLG\t3.4697719\t1970-01-03T06:00:00.000000Z
                        IMYF\t0.69891846\t1970-01-03T06:00:00.000000Z
                        OPHN\tnull\t1970-01-03T06:00:00.000000Z
                        MXSL\tnull\t1970-01-03T06:00:00.000000Z
                        RXGZ\t0.44402504\t1970-01-03T09:00:00.000000Z
                        \t0.09618586\t1970-01-03T09:00:00.000000Z
                        CPSW\t0.7292482\t1970-01-03T09:00:00.000000Z
                        PEHN\t0.25353473\t1970-01-03T09:00:00.000000Z
                        HYRX\t0.04001695\t1970-01-03T09:00:00.000000Z
                        VTJW\t-0.41588497\t1970-01-03T09:00:00.000000Z
                        UQDY\t1.6294945\t1970-01-03T09:00:00.000000Z
                        UKLG\t3.0724893\t1970-01-03T09:00:00.000000Z
                        IMYF\t0.6818833\t1970-01-03T09:00:00.000000Z
                        OPHN\tnull\t1970-01-03T09:00:00.000000Z
                        MXSL\tnull\t1970-01-03T09:00:00.000000Z
                        HYRX\t-0.16638875\t1970-01-03T12:00:00.000000Z
                        PEHN\t-0.43480247\t1970-01-03T12:00:00.000000Z
                        \t0.13020201\t1970-01-03T12:00:00.000000Z
                        VTJW\t-1.0580852\t1970-01-03T12:00:00.000000Z
                        CPSW\t0.721985\t1970-01-03T12:00:00.000000Z
                        RXGZ\t0.19395965\t1970-01-03T12:00:00.000000Z
                        UQDY\t1.5240184\t1970-01-03T12:00:00.000000Z
                        UKLG\t2.6752064\t1970-01-03T12:00:00.000000Z
                        IMYF\t0.6648481\t1970-01-03T12:00:00.000000Z
                        OPHN\tnull\t1970-01-03T12:00:00.000000Z
                        MXSL\tnull\t1970-01-03T12:00:00.000000Z
                        HYRX\t-0.37279445\t1970-01-03T15:00:00.000000Z
                        PEHN\t-1.1231396\t1970-01-03T15:00:00.000000Z
                        \t0.16421817\t1970-01-03T15:00:00.000000Z
                        VTJW\t-1.7002853\t1970-01-03T15:00:00.000000Z
                        CPSW\t0.71472174\t1970-01-03T15:00:00.000000Z
                        RXGZ\t-0.056105733\t1970-01-03T15:00:00.000000Z
                        UQDY\t1.4185423\t1970-01-03T15:00:00.000000Z
                        UKLG\t2.2779236\t1970-01-03T15:00:00.000000Z
                        IMYF\t0.6478129\t1970-01-03T15:00:00.000000Z
                        OPHN\tnull\t1970-01-03T15:00:00.000000Z
                        MXSL\tnull\t1970-01-03T15:00:00.000000Z
                        HYRX\t-0.57920015\t1970-01-03T18:00:00.000000Z
                        PEHN\t-1.811477\t1970-01-03T18:00:00.000000Z
                        \t0.19823432\t1970-01-03T18:00:00.000000Z
                        VTJW\t-2.3424854\t1970-01-03T18:00:00.000000Z
                        CPSW\t0.7074585\t1970-01-03T18:00:00.000000Z
                        RXGZ\t-0.30617112\t1970-01-03T18:00:00.000000Z
                        UQDY\t1.3130661\t1970-01-03T18:00:00.000000Z
                        UKLG\t1.8806409\t1970-01-03T18:00:00.000000Z
                        IMYF\t0.6307777\t1970-01-03T18:00:00.000000Z
                        OPHN\tnull\t1970-01-03T18:00:00.000000Z
                        MXSL\tnull\t1970-01-03T18:00:00.000000Z
                        HYRX\t-0.78560585\t1970-01-03T21:00:00.000000Z
                        PEHN\t-2.499814\t1970-01-03T21:00:00.000000Z
                        \t0.23225047\t1970-01-03T21:00:00.000000Z
                        VTJW\t-2.9846857\t1970-01-03T21:00:00.000000Z
                        CPSW\t0.70019525\t1970-01-03T21:00:00.000000Z
                        RXGZ\t-0.5562365\t1970-01-03T21:00:00.000000Z
                        UQDY\t1.20759\t1970-01-03T21:00:00.000000Z
                        UKLG\t1.4833581\t1970-01-03T21:00:00.000000Z
                        IMYF\t0.61374253\t1970-01-03T21:00:00.000000Z
                        OPHN\tnull\t1970-01-03T21:00:00.000000Z
                        MXSL\tnull\t1970-01-03T21:00:00.000000Z
                        HYRX\t-0.99201155\t1970-01-04T00:00:00.000000Z
                        PEHN\t-3.1881514\t1970-01-04T00:00:00.000000Z
                        \t0.2662666\t1970-01-04T00:00:00.000000Z
                        VTJW\t-3.626886\t1970-01-04T00:00:00.000000Z
                        CPSW\t0.692932\t1970-01-04T00:00:00.000000Z
                        RXGZ\t-0.8063019\t1970-01-04T00:00:00.000000Z
                        UQDY\t1.1021138\t1970-01-04T00:00:00.000000Z
                        UKLG\t1.0860753\t1970-01-04T00:00:00.000000Z
                        IMYF\t0.59670734\t1970-01-04T00:00:00.000000Z
                        OPHN\tnull\t1970-01-04T00:00:00.000000Z
                        MXSL\tnull\t1970-01-04T00:00:00.000000Z
                        UQDY\t0.9966377\t1970-01-04T03:00:00.000000Z
                        \t0.30028278\t1970-01-04T03:00:00.000000Z
                        UKLG\t0.6887925\t1970-01-04T03:00:00.000000Z
                        IMYF\t0.57967216\t1970-01-04T03:00:00.000000Z
                        HYRX\t-1.1984172\t1970-01-04T03:00:00.000000Z
                        PEHN\t-3.8764884\t1970-01-04T03:00:00.000000Z
                        VTJW\t-4.269086\t1970-01-04T03:00:00.000000Z
                        CPSW\t0.68566877\t1970-01-04T03:00:00.000000Z
                        RXGZ\t-1.0563673\t1970-01-04T03:00:00.000000Z
                        OPHN\tnull\t1970-01-04T03:00:00.000000Z
                        MXSL\tnull\t1970-01-04T03:00:00.000000Z
                        \t0.83798915\t1970-01-04T06:00:00.000000Z
                        IMYF\t0.562637\t1970-01-04T06:00:00.000000Z
                        OPHN\t0.49198997\t1970-01-04T06:00:00.000000Z
                        UKLG\t0.29150975\t1970-01-04T06:00:00.000000Z
                        MXSL\t0.7407843\t1970-01-04T06:00:00.000000Z
                        UQDY\t0.89116156\t1970-01-04T06:00:00.000000Z
                        HYRX\t-1.404823\t1970-01-04T06:00:00.000000Z
                        PEHN\t-4.5648255\t1970-01-04T06:00:00.000000Z
                        VTJW\t-4.9112864\t1970-01-04T06:00:00.000000Z
                        CPSW\t0.6784055\t1970-01-04T06:00:00.000000Z
                        RXGZ\t-1.3064327\t1970-01-04T06:00:00.000000Z
                        """,
                true,
                true,
                false
        );
    }
}
