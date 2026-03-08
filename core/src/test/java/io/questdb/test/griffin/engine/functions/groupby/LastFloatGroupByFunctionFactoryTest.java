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

public class LastFloatGroupByFunctionFactoryTest extends AbstractCairoTest {

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

        try (RecordCursorFactory factory = select("select last(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertTrue(Numbers.isNull(record.getFloat(0)));
            }
        }
    }

    @Test
    public void testLastFloat() throws Exception {
        assertQuery(
                """
                        x
                        10.0
                        """,
                "select last(x) x from tab",
                "create table tab as (select cast(x as float) x from long_sequence(10))",
                null,
                false,
                true
        );
    }

    @Test
    public void testLastFloatNull() throws Exception {
        assertQuery(
                """
                        y
                        null
                        """,
                "select last(y) y from tab",
                "create table tab as (select cast(x as float) x, cast(null as float) y from long_sequence(100))",
                null,
                false,
                true
        );
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
        try (RecordCursorFactory factory = select("select last(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0.4900510311126709, record.getFloat(0), 0.0001);
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery(
                """
                        b\tlast\tk
                        HYRX\t0.96441835\t1970-01-03T00:00:00.000000Z
                        PEHN\t0.1250304\t1970-01-03T00:00:00.000000Z
                        \t0.97030604\t1970-01-03T00:00:00.000000Z
                        VTJW\t0.269221\t1970-01-03T00:00:00.000000Z
                        CPSW\t0.41495174\t1970-01-03T00:00:00.000000Z
                        RXGZ\t0.86641586\t1970-01-03T00:00:00.000000Z
                        VTJW\t0.9918093\t1970-01-03T03:00:00.000000Z
                        \t0.8221637\t1970-01-03T03:00:00.000000Z
                        CPSW\t0.10527277\t1970-01-03T03:00:00.000000Z
                        HYRX\t0.86771816\t1970-01-03T03:00:00.000000Z
                        PEHN\t0.82312495\t1970-01-03T03:00:00.000000Z
                        RXGZ\t0.78644454\t1970-01-03T03:00:00.000000Z
                        VTJW\t0.8720995\t1970-01-03T06:00:00.000000Z
                        \t0.7215959\t1970-01-03T06:00:00.000000Z
                        CPSW\t0.73651147\t1970-01-03T06:00:00.000000Z
                        PEHN\t0.8144207\t1970-01-03T06:00:00.000000Z
                        HYRX\t0.56910527\t1970-01-03T06:00:00.000000Z
                        RXGZ\t0.7064733\t1970-01-03T06:00:00.000000Z
                        RXGZ\t0.48352557\t1970-01-03T09:00:00.000000Z
                        \t0.78732294\t1970-01-03T09:00:00.000000Z
                        CPSW\t0.11951214\t1970-01-03T09:00:00.000000Z
                        PEHN\t0.25353473\t1970-01-03T09:00:00.000000Z
                        HYRX\t0.04001695\t1970-01-03T09:00:00.000000Z
                        VTJW\t0.7523897\t1970-01-03T09:00:00.000000Z
                        """,
                "select b, last(a), k from x sample by 3h fill(linear)",
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
                        b\tlast\tk
                        HYRX\t0.96441835\t1970-01-03T00:00:00.000000Z
                        PEHN\t0.1250304\t1970-01-03T00:00:00.000000Z
                        \t0.97030604\t1970-01-03T00:00:00.000000Z
                        VTJW\t0.269221\t1970-01-03T00:00:00.000000Z
                        CPSW\t0.41495174\t1970-01-03T00:00:00.000000Z
                        RXGZ\t0.86641586\t1970-01-03T00:00:00.000000Z
                        UQDY\t1.945923\t1970-01-03T00:00:00.000000Z
                        UKLG\t3.9210684\t1970-01-03T00:00:00.000000Z
                        IMYF\t1.5957208\t1970-01-03T00:00:00.000000Z
                        OPHN\tnull\t1970-01-03T00:00:00.000000Z
                        MXSL\tnull\t1970-01-03T00:00:00.000000Z
                        VTJW\t0.9918093\t1970-01-03T03:00:00.000000Z
                        \t0.8221637\t1970-01-03T03:00:00.000000Z
                        CPSW\t0.10527277\t1970-01-03T03:00:00.000000Z
                        HYRX\t0.86771816\t1970-01-03T03:00:00.000000Z
                        PEHN\t0.82312495\t1970-01-03T03:00:00.000000Z
                        RXGZ\t0.78644454\t1970-01-03T03:00:00.000000Z
                        UQDY\t1.8404468\t1970-01-03T03:00:00.000000Z
                        UKLG\t3.5400088\t1970-01-03T03:00:00.000000Z
                        IMYF\t1.4828265\t1970-01-03T03:00:00.000000Z
                        OPHN\tnull\t1970-01-03T03:00:00.000000Z
                        MXSL\tnull\t1970-01-03T03:00:00.000000Z
                        VTJW\t0.8720995\t1970-01-03T06:00:00.000000Z
                        \t0.7215959\t1970-01-03T06:00:00.000000Z
                        CPSW\t0.73651147\t1970-01-03T06:00:00.000000Z
                        PEHN\t0.8144207\t1970-01-03T06:00:00.000000Z
                        HYRX\t0.56910527\t1970-01-03T06:00:00.000000Z
                        RXGZ\t0.7064733\t1970-01-03T06:00:00.000000Z
                        UQDY\t1.7349707\t1970-01-03T06:00:00.000000Z
                        UKLG\t3.1589494\t1970-01-03T06:00:00.000000Z
                        IMYF\t1.3699322\t1970-01-03T06:00:00.000000Z
                        OPHN\tnull\t1970-01-03T06:00:00.000000Z
                        MXSL\tnull\t1970-01-03T06:00:00.000000Z
                        RXGZ\t0.48352557\t1970-01-03T09:00:00.000000Z
                        \t0.78732294\t1970-01-03T09:00:00.000000Z
                        CPSW\t0.11951214\t1970-01-03T09:00:00.000000Z
                        PEHN\t0.25353473\t1970-01-03T09:00:00.000000Z
                        HYRX\t0.04001695\t1970-01-03T09:00:00.000000Z
                        VTJW\t0.7523897\t1970-01-03T09:00:00.000000Z
                        UQDY\t1.6294945\t1970-01-03T09:00:00.000000Z
                        UKLG\t2.7778897\t1970-01-03T09:00:00.000000Z
                        IMYF\t1.2570379\t1970-01-03T09:00:00.000000Z
                        OPHN\tnull\t1970-01-03T09:00:00.000000Z
                        MXSL\tnull\t1970-01-03T09:00:00.000000Z
                        HYRX\t-0.48907137\t1970-01-03T12:00:00.000000Z
                        PEHN\t-0.30735123\t1970-01-03T12:00:00.000000Z
                        \t0.78161067\t1970-01-03T12:00:00.000000Z
                        VTJW\t0.63267994\t1970-01-03T12:00:00.000000Z
                        CPSW\t-0.4974872\t1970-01-03T12:00:00.000000Z
                        RXGZ\t0.26057786\t1970-01-03T12:00:00.000000Z
                        UQDY\t1.5240184\t1970-01-03T12:00:00.000000Z
                        UKLG\t2.3968303\t1970-01-03T12:00:00.000000Z
                        IMYF\t1.1441436\t1970-01-03T12:00:00.000000Z
                        OPHN\tnull\t1970-01-03T12:00:00.000000Z
                        MXSL\tnull\t1970-01-03T12:00:00.000000Z
                        HYRX\t-1.0181596\t1970-01-03T15:00:00.000000Z
                        PEHN\t-0.8682372\t1970-01-03T15:00:00.000000Z
                        \t0.77589846\t1970-01-03T15:00:00.000000Z
                        VTJW\t0.51297015\t1970-01-03T15:00:00.000000Z
                        CPSW\t-1.1144865\t1970-01-03T15:00:00.000000Z
                        RXGZ\t0.03763014\t1970-01-03T15:00:00.000000Z
                        UQDY\t1.4185423\t1970-01-03T15:00:00.000000Z
                        UKLG\t2.0157707\t1970-01-03T15:00:00.000000Z
                        IMYF\t1.0312493\t1970-01-03T15:00:00.000000Z
                        OPHN\tnull\t1970-01-03T15:00:00.000000Z
                        MXSL\tnull\t1970-01-03T15:00:00.000000Z
                        HYRX\t-1.547248\t1970-01-03T18:00:00.000000Z
                        PEHN\t-1.4291232\t1970-01-03T18:00:00.000000Z
                        \t0.7701862\t1970-01-03T18:00:00.000000Z
                        VTJW\t0.39326036\t1970-01-03T18:00:00.000000Z
                        CPSW\t-1.7314858\t1970-01-03T18:00:00.000000Z
                        RXGZ\t-0.18531758\t1970-01-03T18:00:00.000000Z
                        UQDY\t1.3130661\t1970-01-03T18:00:00.000000Z
                        UKLG\t1.6347113\t1970-01-03T18:00:00.000000Z
                        IMYF\t0.91835505\t1970-01-03T18:00:00.000000Z
                        OPHN\tnull\t1970-01-03T18:00:00.000000Z
                        MXSL\tnull\t1970-01-03T18:00:00.000000Z
                        HYRX\t-2.0763364\t1970-01-03T21:00:00.000000Z
                        PEHN\t-1.9900091\t1970-01-03T21:00:00.000000Z
                        \t0.7644739\t1970-01-03T21:00:00.000000Z
                        VTJW\t0.27355057\t1970-01-03T21:00:00.000000Z
                        CPSW\t-2.3484852\t1970-01-03T21:00:00.000000Z
                        RXGZ\t-0.4082653\t1970-01-03T21:00:00.000000Z
                        UQDY\t1.20759\t1970-01-03T21:00:00.000000Z
                        UKLG\t1.2536516\t1970-01-03T21:00:00.000000Z
                        IMYF\t0.80546075\t1970-01-03T21:00:00.000000Z
                        OPHN\tnull\t1970-01-03T21:00:00.000000Z
                        MXSL\tnull\t1970-01-03T21:00:00.000000Z
                        HYRX\t-2.6054246\t1970-01-04T00:00:00.000000Z
                        PEHN\t-2.5508952\t1970-01-04T00:00:00.000000Z
                        \t0.7587617\t1970-01-04T00:00:00.000000Z
                        VTJW\t0.15384078\t1970-01-04T00:00:00.000000Z
                        CPSW\t-2.9654846\t1970-01-04T00:00:00.000000Z
                        RXGZ\t-0.631213\t1970-01-04T00:00:00.000000Z
                        UQDY\t1.1021138\t1970-01-04T00:00:00.000000Z
                        UKLG\t0.87259215\t1970-01-04T00:00:00.000000Z
                        IMYF\t0.69256645\t1970-01-04T00:00:00.000000Z
                        OPHN\tnull\t1970-01-04T00:00:00.000000Z
                        MXSL\tnull\t1970-01-04T00:00:00.000000Z
                        UQDY\t0.9966377\t1970-01-04T03:00:00.000000Z
                        \t0.75304943\t1970-01-04T03:00:00.000000Z
                        UKLG\t0.49153262\t1970-01-04T03:00:00.000000Z
                        IMYF\t0.57967216\t1970-01-04T03:00:00.000000Z
                        HYRX\t-3.134513\t1970-01-04T03:00:00.000000Z
                        PEHN\t-3.1117811\t1970-01-04T03:00:00.000000Z
                        VTJW\t0.03413099\t1970-01-04T03:00:00.000000Z
                        CPSW\t-3.5824838\t1970-01-04T03:00:00.000000Z
                        RXGZ\t-0.8541607\t1970-01-04T03:00:00.000000Z
                        OPHN\tnull\t1970-01-04T03:00:00.000000Z
                        MXSL\tnull\t1970-01-04T03:00:00.000000Z
                        \t0.013960779\t1970-01-04T06:00:00.000000Z
                        IMYF\t0.46677786\t1970-01-04T06:00:00.000000Z
                        OPHN\t0.720279\t1970-01-04T06:00:00.000000Z
                        UKLG\t0.1104731\t1970-01-04T06:00:00.000000Z
                        MXSL\t0.7942522\t1970-01-04T06:00:00.000000Z
                        UQDY\t0.89116156\t1970-01-04T06:00:00.000000Z
                        HYRX\t-3.6636014\t1970-01-04T06:00:00.000000Z
                        PEHN\t-3.672667\t1970-01-04T06:00:00.000000Z
                        VTJW\t-0.0855788\t1970-01-04T06:00:00.000000Z
                        CPSW\t-4.199483\t1970-01-04T06:00:00.000000Z
                        RXGZ\t-1.0771084\t1970-01-04T06:00:00.000000Z
                        """,
                true,
                true,
                false
        );
    }
}
