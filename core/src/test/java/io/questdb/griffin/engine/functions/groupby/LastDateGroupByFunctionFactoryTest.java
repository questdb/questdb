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

public class LastDateGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testAllNull() throws SqlException {

        compiler.compile("create table tab (f date)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
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
                Assert.assertEquals(Numbers.LONG_NaN, record.getLong(0));
            }
        }
    }

    @Test
    public void testNonNull() throws SqlException {

        compiler.compile("create table tab (f date)", sqlExecutionContext);

        final Rnd rnd = new Rnd();
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.putLong(0, rnd.nextLong());
                r.append();
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select last(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(-6919361415374675248L, record.getLong(0));
            }
        }
    }

    @Test
    public void testSomeNull() throws SqlException {

        compiler.compile("create table tab (f date)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                if (i % 4 == 0) {
                    r.putLong(0, i);
                }
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select last(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(Numbers.LONG_NaN, record.getDate(0));
            }
        }
    }


    @Test
    public void testSampleFill() throws Exception {
        assertQuery("b\tlast\tk\n" +
                        "\t1970-01-01T13:12:42.067Z\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t1970-01-01T15:26:16.392Z\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T04:59:57.289Z\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T21:53:41.502Z\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T09:32:04.371Z\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t1970-01-02T03:22:30.766Z\t1970-01-03T00:00:00.000000Z\n" +
                        "\t1970-01-02T00:05:13.621Z\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t1970-01-02T02:34:44.603Z\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T23:02:28.059Z\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T18:28:29.011Z\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T05:08:03.426Z\t1970-01-03T03:00:00.000000Z\n" +
                        "\t\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t1970-01-01T06:56:29.955Z\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T06:20:11.045Z\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t1970-01-02T00:12:49.678Z\t1970-01-03T06:00:00.000000Z\n" +
                        "\t1970-01-01T10:07:15.931Z\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T17:31:31.822Z\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-03T09:00:00.000000Z\n",
                "select b, last(a), k from x sample by 3h fill(prev)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_date(0, 100000000L, 2) a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_date() a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                "b\tlast\tk\n" +
                        "\t1970-01-01T13:12:42.067Z\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t1970-01-01T15:26:16.392Z\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T04:59:57.289Z\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T21:53:41.502Z\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T09:32:04.371Z\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t1970-01-02T03:22:30.766Z\t1970-01-03T00:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T00:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T00:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T00:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T00:00:00.000000Z\n" +
                        "\t1970-01-02T00:05:13.621Z\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t1970-01-02T02:34:44.603Z\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T23:02:28.059Z\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T18:28:29.011Z\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T05:08:03.426Z\t1970-01-03T03:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T03:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T03:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T03:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T03:00:00.000000Z\n" +
                        "\t\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t1970-01-01T06:56:29.955Z\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T06:20:11.045Z\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t1970-01-02T00:12:49.678Z\t1970-01-03T06:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T06:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T06:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T06:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T06:00:00.000000Z\n" +
                        "\t1970-01-01T10:07:15.931Z\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T17:31:31.822Z\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-03T09:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T09:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T09:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T09:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T09:00:00.000000Z\n" +
                        "\t1970-01-01T10:07:15.931Z\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T17:31:31.822Z\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-03T12:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T12:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T12:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T12:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T12:00:00.000000Z\n" +
                        "\t1970-01-01T10:07:15.931Z\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T17:31:31.822Z\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-03T15:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T15:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T15:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T15:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T15:00:00.000000Z\n" +
                        "\t1970-01-01T10:07:15.931Z\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T17:31:31.822Z\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-03T18:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T18:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T18:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T18:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T18:00:00.000000Z\n" +
                        "\t1970-01-01T10:07:15.931Z\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T17:31:31.822Z\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-03T21:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T21:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T21:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T21:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T21:00:00.000000Z\n" +
                        "\t1970-01-01T10:07:15.931Z\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T17:31:31.822Z\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-04T00:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-04T00:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-04T00:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-04T00:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-04T00:00:00.000000Z\n" +
                        "\t1970-01-01T01:55:59.541Z\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T17:31:31.822Z\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-04T03:00:00.000000Z\n" +
                        "LGMX\t1970-01-01T01:23:01.038Z\t1970-01-04T03:00:00.000000Z\n" +
                        "MXUK\t1970-01-01T00:19:23.253Z\t1970-01-04T03:00:00.000000Z\n" +
                        "SLUQ\t1970-01-01T00:19:15.412Z\t1970-01-04T03:00:00.000000Z\n" +
                        "KFMQ\t1970-01-01T00:21:32.033Z\t1970-01-04T03:00:00.000000Z\n" +
                        "\t1970-01-01T01:13:18.496Z\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T17:31:31.822Z\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-04T06:00:00.000000Z\n" +
                        "LGMX\t1970-01-01T02:19:34.432Z\t1970-01-04T06:00:00.000000Z\n" +
                        "MXUK\t1970-01-01T00:19:23.253Z\t1970-01-04T06:00:00.000000Z\n" +
                        "SLUQ\t1970-01-01T00:00:41.026Z\t1970-01-04T06:00:00.000000Z\n" +
                        "KFMQ\t1970-01-01T02:24:28.146Z\t1970-01-04T06:00:00.000000Z\n",
                false
        );
    }
}