/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

public class MinShortGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testNonNull() throws SqlException {

        compiler.compile("create table tab (f short)");

        final Rnd rnd = new Rnd();
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.putShort(0, rnd.nextShort());
                r.append();
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select min(f) from tab").getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor()) {
                Record record = cursor.getRecord();
                Assert.assertEquals(-1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(-32679, record.getShort(0));
            }
        }
    }

    @Test
    public void testAllNull() throws SqlException {

        compiler.compile("create table tab (f short)");

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select min(f) from tab").getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor()) {
                Record record = cursor.getRecord();
                Assert.assertEquals(-1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getShort(0));
            }
        }
    }

    @Test
    public void testFirstNull() throws SqlException {

        compiler.compile("create table tab (f short)");

        final Rnd rnd = new Rnd();
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            TableWriter.Row r = w.newRow();
            r.append();
            for (int i = 100; i > 10; i--) {
                r = w.newRow();
                r.putShort(0, rnd.nextShort());
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select min(f) from tab").getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor()) {
                Record record = cursor.getRecord();
                Assert.assertEquals(-1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(-32679, record.getShort(0));
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery("b\tmin\tk\n" +
                        "\t-31228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t-1593\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t21781\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t-1271\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t-19127\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t15926\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-13523\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-24397\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t-2018\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-15331\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t-10913\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t3172\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-8221\t1970-01-03T06:00:00.000000Z\n" +
                        "\t-26776\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t-12397\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t-25068\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t-31322\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t-30103\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t-30595\t1970-01-03T09:00:00.000000Z\n" +
                        "\t24682\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-26828\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t6667\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t18853\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t-13881\t1970-01-03T09:00:00.000000Z\n",
                "select b, min(a), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_short() a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_short() a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                "b\tmin\tk\n" +
                        "\t-31228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t-1593\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t21781\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t-1271\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t-19127\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t15926\t1970-01-03T00:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-13523\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-24397\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t-2018\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-15331\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t-10913\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t3172\t1970-01-03T03:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-8221\t1970-01-03T06:00:00.000000Z\n" +
                        "\t-26776\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t-12397\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t-25068\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t-31322\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t-30103\t1970-01-03T06:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t-30595\t1970-01-03T09:00:00.000000Z\n" +
                        "\t24682\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-26828\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t6667\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t18853\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t-13881\t1970-01-03T09:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\t15446\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t-27134\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t2273\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t-22334\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t12567\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t-15365\t1970-01-03T12:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "\t6211\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t4601\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t-14307\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t-17840\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t-9807\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t-16849\t1970-01-03T15:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "\t-3024\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t-29200\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t-30887\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t-13346\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t-32181\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t-18333\t1970-01-03T18:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "\t-12260\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t2535\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t18069\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t-8852\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t10981\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t-19817\t1970-01-03T21:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "\t-21495\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t-31266\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t1489\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-4358\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t-11393\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t-21301\t1970-01-04T00:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "\t-30731\t1970-01-04T03:00:00.000000Z\n" +
                        "ZMZV\t-11913\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t469\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t-15091\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t136\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t31769\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t-22785\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\t-15702\t1970-01-04T06:00:00.000000Z\n" +
                        "LOGI\t-14555\t1970-01-04T06:00:00.000000Z\n" +
                        "QEBN\t-9244\t1970-01-04T06:00:00.000000Z\n" +
                        "\t-30798\t1970-01-04T06:00:00.000000Z\n" +
                        "FOUS\t-22523\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t32204\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t-31671\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t4630\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t9395\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t-24269\t1970-01-04T06:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-04T06:00:00.000000Z\n",
                true);
    }
}