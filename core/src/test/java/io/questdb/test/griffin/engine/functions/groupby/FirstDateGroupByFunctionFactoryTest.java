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
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class FirstDateGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNull() throws SqlException {
        ddl("create table tab (f date)");

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
                Assert.assertEquals(Numbers.LONG_NaN, record.getLong(0));
            }
        }
    }

    @Test
    public void testFirstNull() throws SqlException {
        ddl("create table tab (f date)");

        final Rnd rnd = new Rnd();
        try (TableWriter w = getWriter("tab")) {
            TableWriter.Row r = w.newRow();
            r.append();
            for (int i = 100; i > 10; i--) {
                r = w.newRow();
                r.putLong(0, rnd.nextLong());
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select first(f) from tab")) {
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
        ddl("create table tab (f date)");

        final Rnd rnd = new Rnd();
        try (TableWriter w = getWriter("tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.putLong(0, rnd.nextLong());
                r.append();
            }
            w.commit();
        }
        try (RecordCursorFactory factory = select("select first(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(4689592037643856L, record.getLong(0));
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery(
                "b\tfirst\tk\n" +
                        "\t\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t1970-01-01T23:18:12.817Z\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T14:03:03.614Z\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T16:45:23.117Z\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t1970-01-02T01:39:42.121Z\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t1970-01-02T03:03:27.641Z\t1970-01-03T00:00:00.000000Z\n" +
                        "\t1970-01-01T10:38:00.046Z\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t1970-01-02T00:55:08.236Z\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T20:48:47.882Z\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T06:50:05.223Z\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t\t1970-01-03T03:00:00.000000Z\n" +
                        "\t1970-01-01T16:14:13.153Z\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t1970-01-02T02:58:09.421Z\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T02:16:17.492Z\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T18:31:12.742Z\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T12:47:01.468Z\t1970-01-03T06:00:00.000000Z\n" +
                        "\t1970-01-01T18:34:38.297Z\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t1970-01-01T16:31:18.999Z\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T00:09:58.820Z\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-03T09:00:00.000000Z\n",
                "select b, first(a), k from x sample by 3h fill(prev)",
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
                "b\tfirst\tk\n" +
                        "\t\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t1970-01-01T23:18:12.817Z\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T14:03:03.614Z\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T16:45:23.117Z\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t1970-01-02T01:39:42.121Z\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t1970-01-02T03:03:27.641Z\t1970-01-03T00:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T00:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T00:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T00:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T00:00:00.000000Z\n" +
                        "\t1970-01-01T10:38:00.046Z\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t1970-01-02T00:55:08.236Z\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T20:48:47.882Z\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T06:50:05.223Z\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t\t1970-01-03T03:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T03:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T03:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T03:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T03:00:00.000000Z\n" +
                        "\t1970-01-01T16:14:13.153Z\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t1970-01-02T02:58:09.421Z\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T02:16:17.492Z\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T18:31:12.742Z\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T12:47:01.468Z\t1970-01-03T06:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T06:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T06:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T06:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T06:00:00.000000Z\n" +
                        "\t1970-01-01T18:34:38.297Z\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t1970-01-01T16:31:18.999Z\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T00:09:58.820Z\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-03T09:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T09:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T09:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T09:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T09:00:00.000000Z\n" +
                        "\t1970-01-01T18:34:38.297Z\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t1970-01-01T16:31:18.999Z\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T00:09:58.820Z\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-03T12:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T12:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T12:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T12:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T12:00:00.000000Z\n" +
                        "\t1970-01-01T18:34:38.297Z\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t1970-01-01T16:31:18.999Z\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T00:09:58.820Z\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-03T15:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T15:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T15:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T15:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T15:00:00.000000Z\n" +
                        "\t1970-01-01T18:34:38.297Z\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t1970-01-01T16:31:18.999Z\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T00:09:58.820Z\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-03T18:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T18:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T18:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T18:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T18:00:00.000000Z\n" +
                        "\t1970-01-01T18:34:38.297Z\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t1970-01-01T16:31:18.999Z\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T00:09:58.820Z\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-03T21:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-03T21:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-03T21:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-03T21:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-03T21:00:00.000000Z\n" +
                        "\t1970-01-01T18:34:38.297Z\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t1970-01-01T16:31:18.999Z\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T00:09:58.820Z\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-04T00:00:00.000000Z\n" +
                        "LGMX\t\t1970-01-04T00:00:00.000000Z\n" +
                        "MXUK\t\t1970-01-04T00:00:00.000000Z\n" +
                        "SLUQ\t\t1970-01-04T00:00:00.000000Z\n" +
                        "KFMQ\t\t1970-01-04T00:00:00.000000Z\n" +
                        "\t1970-01-01T00:06:06.464Z\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t1970-01-01T16:31:18.999Z\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T00:09:58.820Z\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-04T03:00:00.000000Z\n" +
                        "LGMX\t1970-01-01T00:16:46.241Z\t1970-01-04T03:00:00.000000Z\n" +
                        "MXUK\t1970-01-01T00:39:19.580Z\t1970-01-04T03:00:00.000000Z\n" +
                        "SLUQ\t1970-01-01T00:19:15.412Z\t1970-01-04T03:00:00.000000Z\n" +
                        "KFMQ\t1970-01-01T00:44:04.687Z\t1970-01-04T03:00:00.000000Z\n" +
                        "\t1970-01-01T00:05:27.247Z\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t1970-01-01T16:31:18.999Z\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t1970-01-01T12:59:52.324Z\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t1970-01-01T14:39:15.119Z\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t1970-01-01T00:09:58.820Z\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t1970-01-01T17:07:59.067Z\t1970-01-04T06:00:00.000000Z\n" +
                        "LGMX\t1970-01-01T00:58:09.481Z\t1970-01-04T06:00:00.000000Z\n" +
                        "MXUK\t1970-01-01T00:39:19.580Z\t1970-01-04T06:00:00.000000Z\n" +
                        "SLUQ\t1970-01-01T00:00:41.026Z\t1970-01-04T06:00:00.000000Z\n" +
                        "KFMQ\t1970-01-01T01:01:22.249Z\t1970-01-04T06:00:00.000000Z\n",
                false
        );
    }

    @Test
    public void testSomeNull() throws SqlException {
        ddl("create table tab (f date)");

        try (TableWriter w = getWriter("tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                if (i % 4 == 0) {
                    r.putLong(0, i);
                }
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select first(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(100, record.getLong(0));
            }
        }
    }
}
