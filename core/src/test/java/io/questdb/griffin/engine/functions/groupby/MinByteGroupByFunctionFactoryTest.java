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

public class MinByteGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testNonNull() throws SqlException {

        compiler.compile("create table tab (f byte)");

        final Rnd rnd = new Rnd();
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.putByte(0, rnd.nextByte());
                r.append();
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select min(f) from tab").getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor()) {
                Record record = cursor.getRecord();
                Assert.assertEquals(-1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(-127, record.getByte(0));
            }
        }
    }

    @Test
    public void testAllNull() throws SqlException {

        compiler.compile("create table tab (f byte)");

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
                Assert.assertEquals(0, record.getByte(0));
            }
        }
    }

    @Test
    public void testFirstNull() throws SqlException {

        compiler.compile("create table tab (f byte)");

        final Rnd rnd = new Rnd();
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            TableWriter.Row r = w.newRow();
            r.append();
            for (int i = 100; i > 10; i--) {
                r = w.newRow();
                r.putByte(0, rnd.nextByte());
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select min(f) from tab").getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor()) {
                Record record = cursor.getRecord();
                Assert.assertEquals(-1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(-127, record.getByte(0));
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery("b\tmin\tk\n" +
                        "\t7\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t4\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t6\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t6\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t10\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t12\t1970-01-03T03:00:00.000000Z\n" +
                        "\t4\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t24\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t24\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t5\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t17\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t16\t1970-01-03T06:00:00.000000Z\n" +
                        "\t9\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t8\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t7\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t6\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t15\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t8\t1970-01-03T09:00:00.000000Z\n" +
                        "\t8\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t8\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t29\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t18\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t11\t1970-01-03T09:00:00.000000Z\n",
                "select b, min(a), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_byte(4,30) a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_byte(4,30) a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                "b\tmin\tk\n" +
                        "\t7\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t4\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t6\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t6\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t10\t1970-01-03T00:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t12\t1970-01-03T03:00:00.000000Z\n" +
                        "\t4\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t24\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t24\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t5\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t17\t1970-01-03T03:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t16\t1970-01-03T06:00:00.000000Z\n" +
                        "\t9\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t8\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t7\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t6\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t15\t1970-01-03T06:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t8\t1970-01-03T09:00:00.000000Z\n" +
                        "\t8\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t8\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t29\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t18\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t11\t1970-01-03T09:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\t8\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t51\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t21\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t10\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t14\t1970-01-03T12:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "\t8\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t73\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t24\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t12\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t-8\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t17\t1970-01-03T15:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "\t8\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t95\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t27\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t14\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t-16\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20\t1970-01-03T18:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "\t8\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t117\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t30\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t16\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t-24\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t23\t1970-01-03T21:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "\t8\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t-117\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t33\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t18\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t-32\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t26\t1970-01-04T00:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "\t9\t1970-01-04T03:00:00.000000Z\n" +
                        "ZMZV\t9\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t-95\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t36\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t20\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t-40\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t29\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\t14\t1970-01-04T06:00:00.000000Z\n" +
                        "LOGI\t6\t1970-01-04T06:00:00.000000Z\n" +
                        "QEBN\t14\t1970-01-04T06:00:00.000000Z\n" +
                        "\t8\t1970-01-04T06:00:00.000000Z\n" +
                        "FOUS\t9\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t-73\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t39\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t22\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t-48\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t32\t1970-01-04T06:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-04T06:00:00.000000Z\n",
                true);
    }
}