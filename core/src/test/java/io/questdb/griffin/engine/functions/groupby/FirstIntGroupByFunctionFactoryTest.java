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
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FirstIntGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testAllNull() throws SqlException {

        compiler.compile("create table tab (f int)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select first(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(Numbers.INT_NaN, record.getInt(0));
            }
        }
    }

    @Test
    public void testFirstNull() throws SqlException {

        compiler.compile("create table tab (f int)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            TableWriter.Row r = w.newRow();
            r.append();
            for (int i = 100; i > 10; i--) {
                r = w.newRow();
                r.putInt(0, i);
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select first(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        compiler.compile("create table tab (f int)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.putInt(0, i);
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select first(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(100, record.getInt(0));
            }
        }
    }

    @Test
    public void testSomeNull() throws SqlException {

        compiler.compile("create table tab (f int)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                if (i % 4 == 0) {
                    r.putInt(0, i);
                }
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select first(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(100, record.getInt(0));
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery("b\tfirst\tk\n" +
                        "\t1530831067\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t1125579207\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-1520872171\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t-1125169127\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t-2119387831\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t-938514914\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t1920890138\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-235358133\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t-661194722\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t1196016669\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t532665695\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t215354468\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t519895483\t1970-01-03T06:00:00.000000Z\n" +
                        "\t-355530067\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t1743740444\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t1598679468\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t-2080340570\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t-1383560599\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t468948839\t1970-01-03T09:00:00.000000Z\n" +
                        "\t-1901633430\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-1398499532\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t-1377625589\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-03T09:00:00.000000Z\n",
                "select b, first(a), k from x sample by 3h fill(linear)",
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
                "b\tfirst\tk\n" +
                        "\t1530831067\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t1125579207\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-1520872171\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t-1125169127\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t-2119387831\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t-938514914\t1970-01-03T00:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t1920890138\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-235358133\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t-661194722\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t1196016669\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t532665695\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t215354468\t1970-01-03T03:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t519895483\t1970-01-03T06:00:00.000000Z\n" +
                        "\t-355530067\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t1743740444\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t1598679468\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t-2080340570\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t-1383560599\t1970-01-03T06:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t468948839\t1970-01-03T09:00:00.000000Z\n" +
                        "\t-1901633430\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-1398499532\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t-1377625589\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-03T09:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t-1717233271\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t-716658493\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t418002195\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-03T12:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t-1532833112\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t-34817456\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t367055551\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-03T15:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t-1348432953\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t647023581\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t316108906\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-03T18:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t-1164032794\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t1328864620\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t265162262\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-03T21:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t-979632635\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t2010705657\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t214215619\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-04T00:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t-795232477\t1970-01-04T03:00:00.000000Z\n" +
                        "ZMZV\t-1822590290\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t2147483647\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t163268974\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\t-365989785\t1970-01-04T06:00:00.000000Z\n" +
                        "LOGI\t-358259591\t1970-01-04T06:00:00.000000Z\n" +
                        "QEBN\t171760612\t1970-01-04T06:00:00.000000Z\n" +
                        "\t719793244\t1970-01-04T06:00:00.000000Z\n" +
                        "FOUS\t380766663\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t2147483647\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t112322330\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t2147483647\t1970-01-04T06:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-04T06:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

}