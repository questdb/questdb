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

public class MaxDoubleGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testFirstNull() throws SqlException {

        compiler.compile("create table tab (f double)");

        final Rnd rnd = new Rnd();
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            TableWriter.Row r = w.newRow();
            r.append();
            for (int i = 100; i > 10; i--) {
                r = w.newRow();
                r.putDouble(0, rnd.nextDouble());
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select max(f) from tab").getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor()) {
                Record record = cursor.getRecord();
                Assert.assertEquals(-1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0.9856290845874263, record.getDouble(0), 0.0001);
            }
        }
    }

    @Test
    public void testAllNull() throws SqlException {

        compiler.compile("create table tab (f double)");

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select max(f) from tab").getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor()) {
                Record record = cursor.getRecord();
                Assert.assertEquals(-1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertTrue(Double.isNaN(record.getDouble(0)));
            }
        }
    }

    @Test
    public void testNonNull() throws SqlException {

        compiler.compile("create table tab (f double)");

        final Rnd rnd = new Rnd();
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, rnd.nextDouble());
                r.append();
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select max(f) from tab").getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor()) {
                Record record = cursor.getRecord();
                Assert.assertEquals(-1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0.9856290845874263, record.getDouble(0), 0.0001);
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery("b\tmax\tk\n" +
                        "\t0.975019885373\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.868515430542\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.565942913986\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.844525817721\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t0.977110314605\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.675250954711\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.894091712658\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.944165897553\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.945721264691\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.945589300480\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.843845956391\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.736511521557\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.942367162414\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t0.819655474584\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.578074627654\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.690197677807\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.434613581293\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t1.216191574671\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.837989199122\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t0.727211975593\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t0.773222984852\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t0.132715641029\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.210559954828\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t1.486661884651\t1970-01-03T09:00:00.000000Z\n",
                "select b, max(a), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0) a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0) a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                "b\tmax\tk\n" +
                        "\t0.975019885373\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.868515430542\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.565942913986\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.844525817721\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t0.977110314605\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.675250954711\t1970-01-03T00:00:00.000000Z\n" +
                        "CGFN\t2.717109453333\t1970-01-03T00:00:00.000000Z\n" +
                        "NPIW\t4.115364146194\t1970-01-03T00:00:00.000000Z\n" +
                        "PEVM\t-3.725890404358\t1970-01-03T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.894091712658\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.944165897553\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.945721264691\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.945589300480\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.843845956391\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.736511521557\t1970-01-03T03:00:00.000000Z\n" +
                        "CGFN\t2.505608562669\t1970-01-03T03:00:00.000000Z\n" +
                        "NPIW\t3.750106582629\t1970-01-03T03:00:00.000000Z\n" +
                        "PEVM\t-3.268689246947\t1970-01-03T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.942367162414\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t0.819655474584\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.578074627654\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.690197677807\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.434613581293\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t1.216191574671\t1970-01-03T06:00:00.000000Z\n" +
                        "CGFN\t2.294107672004\t1970-01-03T06:00:00.000000Z\n" +
                        "NPIW\t3.384849019063\t1970-01-03T06:00:00.000000Z\n" +
                        "PEVM\t-2.811488089536\t1970-01-03T06:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.837989199122\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t0.727211975593\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t0.773222984852\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t0.132715641029\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.210559954828\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t1.486661884651\t1970-01-03T09:00:00.000000Z\n" +
                        "CGFN\t2.082606781340\t1970-01-03T09:00:00.000000Z\n" +
                        "NPIW\t3.019591455498\t1970-01-03T09:00:00.000000Z\n" +
                        "PEVM\t-2.354286932125\t1970-01-03T09:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t0.846851259786\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t0.726790495120\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t0.764226273379\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t-0.169182299235\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t-0.156954717997\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t1.757132194631\t1970-01-03T12:00:00.000000Z\n" +
                        "CGFN\t1.871105890675\t1970-01-03T12:00:00.000000Z\n" +
                        "NPIW\t2.654333891932\t1970-01-03T12:00:00.000000Z\n" +
                        "PEVM\t-1.897085774714\t1970-01-03T12:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t0.855713320449\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t0.680358005387\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t0.801240571164\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t-0.471080239499\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t-0.524469390823\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t2.027602504611\t1970-01-03T15:00:00.000000Z\n" +
                        "CGFN\t1.659605000011\t1970-01-03T15:00:00.000000Z\n" +
                        "NPIW\t2.289076328367\t1970-01-03T15:00:00.000000Z\n" +
                        "PEVM\t-1.439884617304\t1970-01-03T15:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t0.864575381112\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t0.633925515655\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t0.838254868950\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t-0.772978179763\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t-0.891984063649\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t2.298072814591\t1970-01-03T18:00:00.000000Z\n" +
                        "CGFN\t1.448104109346\t1970-01-03T18:00:00.000000Z\n" +
                        "NPIW\t1.923818764802\t1970-01-03T18:00:00.000000Z\n" +
                        "PEVM\t-0.982683459893\t1970-01-03T18:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t0.873437441775\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t0.587493025923\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t0.875269166736\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t-1.074876120027\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t-1.259498736475\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t2.568543124571\t1970-01-03T21:00:00.000000Z\n" +
                        "CGFN\t1.236603218682\t1970-01-03T21:00:00.000000Z\n" +
                        "NPIW\t1.558561201236\t1970-01-03T21:00:00.000000Z\n" +
                        "PEVM\t-0.525482302482\t1970-01-03T21:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t0.882299502439\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t0.541060536191\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t0.912283464522\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-1.376774060291\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t-1.627013409301\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t2.839013434550\t1970-01-04T00:00:00.000000Z\n" +
                        "CGFN\t1.025102328017\t1970-01-04T00:00:00.000000Z\n" +
                        "NPIW\t1.193303637671\t1970-01-04T00:00:00.000000Z\n" +
                        "PEVM\t-0.068281145071\t1970-01-04T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t0.891161563102\t1970-01-04T03:00:00.000000Z\n" +
                        "CGFN\t0.813601437353\t1970-01-04T03:00:00.000000Z\n" +
                        "NPIW\t0.828046074105\t1970-01-04T03:00:00.000000Z\n" +
                        "PEVM\t0.388920012340\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t0.494628046458\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t0.949297762308\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-1.678672000555\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t-1.994528082127\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t3.109483744530\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\t0.851484980066\t1970-01-04T06:00:00.000000Z\n" +
                        "CGFN\t0.602100546689\t1970-01-04T06:00:00.000000Z\n" +
                        "\t0.943513809864\t1970-01-04T06:00:00.000000Z\n" +
                        "PEVM\t0.846121169751\t1970-01-04T06:00:00.000000Z\n" +
                        "ZNFK\t0.780618344203\t1970-01-04T06:00:00.000000Z\n" +
                        "NPIW\t0.462788510540\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t0.448195556726\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t0.986312060094\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t-1.980569940819\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t-2.362042754953\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t3.379954054510\t1970-01-04T06:00:00.000000Z\n",
                true);
    }
}