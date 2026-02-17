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

public class MinFloatGroupByFunctionFactoryTest extends AbstractCairoTest {

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

        try (RecordCursorFactory factory = select("select min(f) from tab")) {
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
                """
                        b\tmin\tk
                        HYRX\t0.11427981\t1970-01-03T00:00:00.000000Z
                        PEHN\t0.1250304\t1970-01-03T00:00:00.000000Z
                        \t0.02296561\t1970-01-03T00:00:00.000000Z
                        VTJW\t0.269221\t1970-01-03T00:00:00.000000Z
                        CPSW\t0.33608252\t1970-01-03T00:00:00.000000Z
                        RXGZ\t0.38642335\t1970-01-03T00:00:00.000000Z
                        VTJW\t0.34947264\t1970-01-03T03:00:00.000000Z
                        \t0.026836812\t1970-01-03T03:00:00.000000Z
                        CPSW\t0.10527277\t1970-01-03T03:00:00.000000Z
                        HYRX\t0.6806873\t1970-01-03T03:00:00.000000Z
                        PEHN\t0.82312495\t1970-01-03T03:00:00.000000Z
                        RXGZ\t0.54025686\t1970-01-03T03:00:00.000000Z
                        VTJW\t0.065787554\t1970-01-03T06:00:00.000000Z
                        \t0.05024612\t1970-01-03T06:00:00.000000Z
                        CPSW\t0.73651147\t1970-01-03T06:00:00.000000Z
                        PEHN\t0.45920676\t1970-01-03T06:00:00.000000Z
                        HYRX\t0.24642265\t1970-01-03T06:00:00.000000Z
                        RXGZ\t0.6940904\t1970-01-03T06:00:00.000000Z
                        RXGZ\t0.44402504\t1970-01-03T09:00:00.000000Z
                        \t0.09618586\t1970-01-03T09:00:00.000000Z
                        CPSW\t0.11951214\t1970-01-03T09:00:00.000000Z
                        PEHN\t0.25353473\t1970-01-03T09:00:00.000000Z
                        HYRX\t0.04001695\t1970-01-03T09:00:00.000000Z
                        VTJW\t-0.21789753\t1970-01-03T09:00:00.000000Z
                        """,
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
                """
                        b\tmin\tk
                        HYRX\t0.11427981\t1970-01-03T00:00:00.000000Z
                        PEHN\t0.1250304\t1970-01-03T00:00:00.000000Z
                        \t0.02296561\t1970-01-03T00:00:00.000000Z
                        VTJW\t0.269221\t1970-01-03T00:00:00.000000Z
                        CPSW\t0.33608252\t1970-01-03T00:00:00.000000Z
                        RXGZ\t0.38642335\t1970-01-03T00:00:00.000000Z
                        UQDY\t1.945923\t1970-01-03T00:00:00.000000Z
                        UKLG\t3.9210684\t1970-01-03T00:00:00.000000Z
                        IMYF\t1.5957208\t1970-01-03T00:00:00.000000Z
                        OPHN\tnull\t1970-01-03T00:00:00.000000Z
                        MXSL\tnull\t1970-01-03T00:00:00.000000Z
                        VTJW\t0.34947264\t1970-01-03T03:00:00.000000Z
                        \t0.026836812\t1970-01-03T03:00:00.000000Z
                        CPSW\t0.10527277\t1970-01-03T03:00:00.000000Z
                        HYRX\t0.6806873\t1970-01-03T03:00:00.000000Z
                        PEHN\t0.82312495\t1970-01-03T03:00:00.000000Z
                        RXGZ\t0.54025686\t1970-01-03T03:00:00.000000Z
                        UQDY\t1.8404468\t1970-01-03T03:00:00.000000Z
                        UKLG\t3.5400088\t1970-01-03T03:00:00.000000Z
                        IMYF\t1.4828265\t1970-01-03T03:00:00.000000Z
                        OPHN\tnull\t1970-01-03T03:00:00.000000Z
                        MXSL\tnull\t1970-01-03T03:00:00.000000Z
                        VTJW\t0.065787554\t1970-01-03T06:00:00.000000Z
                        \t0.05024612\t1970-01-03T06:00:00.000000Z
                        CPSW\t0.73651147\t1970-01-03T06:00:00.000000Z
                        PEHN\t0.45920676\t1970-01-03T06:00:00.000000Z
                        HYRX\t0.24642265\t1970-01-03T06:00:00.000000Z
                        RXGZ\t0.6940904\t1970-01-03T06:00:00.000000Z
                        UQDY\t1.7349707\t1970-01-03T06:00:00.000000Z
                        UKLG\t3.1589494\t1970-01-03T06:00:00.000000Z
                        IMYF\t1.3699322\t1970-01-03T06:00:00.000000Z
                        OPHN\tnull\t1970-01-03T06:00:00.000000Z
                        MXSL\tnull\t1970-01-03T06:00:00.000000Z
                        RXGZ\t0.44402504\t1970-01-03T09:00:00.000000Z
                        \t0.09618586\t1970-01-03T09:00:00.000000Z
                        CPSW\t0.11951214\t1970-01-03T09:00:00.000000Z
                        PEHN\t0.25353473\t1970-01-03T09:00:00.000000Z
                        HYRX\t0.04001695\t1970-01-03T09:00:00.000000Z
                        VTJW\t-0.21789753\t1970-01-03T09:00:00.000000Z
                        UQDY\t1.6294945\t1970-01-03T09:00:00.000000Z
                        UKLG\t2.7778897\t1970-01-03T09:00:00.000000Z
                        IMYF\t1.2570379\t1970-01-03T09:00:00.000000Z
                        OPHN\tnull\t1970-01-03T09:00:00.000000Z
                        MXSL\tnull\t1970-01-03T09:00:00.000000Z
                        HYRX\t-0.16638875\t1970-01-03T12:00:00.000000Z
                        PEHN\t0.04786271\t1970-01-03T12:00:00.000000Z
                        \t0.12842958\t1970-01-03T12:00:00.000000Z
                        VTJW\t-0.5015826\t1970-01-03T12:00:00.000000Z
                        CPSW\t-0.4974872\t1970-01-03T12:00:00.000000Z
                        RXGZ\t0.19395965\t1970-01-03T12:00:00.000000Z
                        UQDY\t1.5240184\t1970-01-03T12:00:00.000000Z
                        UKLG\t2.3968303\t1970-01-03T12:00:00.000000Z
                        IMYF\t1.1441436\t1970-01-03T12:00:00.000000Z
                        OPHN\tnull\t1970-01-03T12:00:00.000000Z
                        MXSL\tnull\t1970-01-03T12:00:00.000000Z
                        HYRX\t-0.37279445\t1970-01-03T15:00:00.000000Z
                        PEHN\t-0.15780932\t1970-01-03T15:00:00.000000Z
                        \t0.1606733\t1970-01-03T15:00:00.000000Z
                        VTJW\t-0.7852677\t1970-01-03T15:00:00.000000Z
                        CPSW\t-1.1144865\t1970-01-03T15:00:00.000000Z
                        RXGZ\t-0.056105733\t1970-01-03T15:00:00.000000Z
                        UQDY\t1.4185423\t1970-01-03T15:00:00.000000Z
                        UKLG\t2.0157707\t1970-01-03T15:00:00.000000Z
                        IMYF\t1.0312493\t1970-01-03T15:00:00.000000Z
                        OPHN\tnull\t1970-01-03T15:00:00.000000Z
                        MXSL\tnull\t1970-01-03T15:00:00.000000Z
                        HYRX\t-0.57920015\t1970-01-03T18:00:00.000000Z
                        PEHN\t-0.36348134\t1970-01-03T18:00:00.000000Z
                        \t0.19291702\t1970-01-03T18:00:00.000000Z
                        VTJW\t-1.0689528\t1970-01-03T18:00:00.000000Z
                        CPSW\t-1.7314858\t1970-01-03T18:00:00.000000Z
                        RXGZ\t-0.30617112\t1970-01-03T18:00:00.000000Z
                        UQDY\t1.3130661\t1970-01-03T18:00:00.000000Z
                        UKLG\t1.6347113\t1970-01-03T18:00:00.000000Z
                        IMYF\t0.91835505\t1970-01-03T18:00:00.000000Z
                        OPHN\tnull\t1970-01-03T18:00:00.000000Z
                        MXSL\tnull\t1970-01-03T18:00:00.000000Z
                        HYRX\t-0.78560585\t1970-01-03T21:00:00.000000Z
                        PEHN\t-0.56915337\t1970-01-03T21:00:00.000000Z
                        \t0.22516073\t1970-01-03T21:00:00.000000Z
                        VTJW\t-1.3526379\t1970-01-03T21:00:00.000000Z
                        CPSW\t-2.3484852\t1970-01-03T21:00:00.000000Z
                        RXGZ\t-0.5562365\t1970-01-03T21:00:00.000000Z
                        UQDY\t1.20759\t1970-01-03T21:00:00.000000Z
                        UKLG\t1.2536516\t1970-01-03T21:00:00.000000Z
                        IMYF\t0.80546075\t1970-01-03T21:00:00.000000Z
                        OPHN\tnull\t1970-01-03T21:00:00.000000Z
                        MXSL\tnull\t1970-01-03T21:00:00.000000Z
                        HYRX\t-0.99201155\t1970-01-04T00:00:00.000000Z
                        PEHN\t-0.7748254\t1970-01-04T00:00:00.000000Z
                        \t0.25740445\t1970-01-04T00:00:00.000000Z
                        VTJW\t-1.636323\t1970-01-04T00:00:00.000000Z
                        CPSW\t-2.9654846\t1970-01-04T00:00:00.000000Z
                        RXGZ\t-0.8063019\t1970-01-04T00:00:00.000000Z
                        UQDY\t1.1021138\t1970-01-04T00:00:00.000000Z
                        UKLG\t0.87259215\t1970-01-04T00:00:00.000000Z
                        IMYF\t0.69256645\t1970-01-04T00:00:00.000000Z
                        OPHN\tnull\t1970-01-04T00:00:00.000000Z
                        MXSL\tnull\t1970-01-04T00:00:00.000000Z
                        UQDY\t0.9966377\t1970-01-04T03:00:00.000000Z
                        \t0.28964818\t1970-01-04T03:00:00.000000Z
                        UKLG\t0.49153262\t1970-01-04T03:00:00.000000Z
                        IMYF\t0.57967216\t1970-01-04T03:00:00.000000Z
                        HYRX\t-1.1984172\t1970-01-04T03:00:00.000000Z
                        PEHN\t-0.9804974\t1970-01-04T03:00:00.000000Z
                        VTJW\t-1.9200081\t1970-01-04T03:00:00.000000Z
                        CPSW\t-3.5824838\t1970-01-04T03:00:00.000000Z
                        RXGZ\t-1.0563673\t1970-01-04T03:00:00.000000Z
                        OPHN\tnull\t1970-01-04T03:00:00.000000Z
                        MXSL\tnull\t1970-01-04T03:00:00.000000Z
                        \t0.013960779\t1970-01-04T06:00:00.000000Z
                        IMYF\t0.46677786\t1970-01-04T06:00:00.000000Z
                        OPHN\t0.018146396\t1970-01-04T06:00:00.000000Z
                        UKLG\t0.1104731\t1970-01-04T06:00:00.000000Z
                        MXSL\t0.7407843\t1970-01-04T06:00:00.000000Z
                        UQDY\t0.89116156\t1970-01-04T06:00:00.000000Z
                        HYRX\t-1.404823\t1970-01-04T06:00:00.000000Z
                        PEHN\t-1.1861694\t1970-01-04T06:00:00.000000Z
                        VTJW\t-2.2036932\t1970-01-04T06:00:00.000000Z
                        CPSW\t-4.199483\t1970-01-04T06:00:00.000000Z
                        RXGZ\t-1.3064327\t1970-01-04T06:00:00.000000Z
                        """,
                true,
                true,
                false
        );
    }
}
