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

public class MinDoubleGroupByFunctionFactoryTest extends AbstractGriffinTest {

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

        try (RecordCursorFactory factory = compiler.compile("select min(f) from tab").getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor()) {
                Record record = cursor.getRecord();
                Assert.assertEquals(-1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0.0011075139045715332, record.getDouble(0), 0.0001);
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

        try (RecordCursorFactory factory = compiler.compile("select min(f) from tab").getRecordCursorFactory()) {
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
        try (RecordCursorFactory factory = compiler.compile("select min(f) from tab").getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor()) {
                Record record = cursor.getRecord();
                Assert.assertEquals(-1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0.0011075139045715332, record.getDouble(0), 0.0001);
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery("b\tmin\tk\n" +
                        "\t0.097505744144\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.421776884197\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.239052901085\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.121056302736\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t0.120261224128\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.120241608757\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.039931248213\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.218586583503\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.029080850169\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.051584599293\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.659082927506\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.736511521557\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.078280206815\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t0.483525620204\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.234937936017\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.007985454959\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.235077540295\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t-0.062079908420\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.474068460469\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t0.031921080750\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t0.491990017163\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-0.266356440968\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.418291272742\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t-0.153240667009\t1970-01-03T09:00:00.000000Z\n",
                "select b, min(a), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0) a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0) a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                "b\tmin\tk\n" +
                        "\t0.097505744144\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.421776884197\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.239052901085\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.121056302736\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t0.120261224128\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.120241608757\t1970-01-03T00:00:00.000000Z\n" +
                        "CGFN\t3.029975503129\t1970-01-03T00:00:00.000000Z\n" +
                        "NPIW\t4.115364146194\t1970-01-03T00:00:00.000000Z\n" +
                        "PEVM\t-3.154296897767\t1970-01-03T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.039931248213\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.218586583503\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.029080850169\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.051584599293\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.659082927506\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.736511521557\t1970-01-03T03:00:00.000000Z\n" +
                        "CGFN\t2.783711718043\t1970-01-03T03:00:00.000000Z\n" +
                        "NPIW\t3.750106582629\t1970-01-03T03:00:00.000000Z\n" +
                        "PEVM\t-2.760606129977\t1970-01-03T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.078280206815\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t0.483525620204\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.234937936017\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.007985454959\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.235077540295\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t-0.062079908420\t1970-01-03T06:00:00.000000Z\n" +
                        "CGFN\t2.537447932957\t1970-01-03T06:00:00.000000Z\n" +
                        "NPIW\t3.384849019063\t1970-01-03T06:00:00.000000Z\n" +
                        "PEVM\t-2.366915362188\t1970-01-03T06:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.474068460469\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t0.031921080750\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t0.491990017163\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-0.266356440968\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.418291272742\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t-0.153240667009\t1970-01-03T09:00:00.000000Z\n" +
                        "CGFN\t2.291184147871\t1970-01-03T09:00:00.000000Z\n" +
                        "NPIW\t3.019591455498\t1970-01-03T09:00:00.000000Z\n" +
                        "PEVM\t-1.973224594398\t1970-01-03T09:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t0.413469242415\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t0.500454414123\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t0.055856706541\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t-0.767790422230\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t0.601644609467\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t-0.244401425598\t1970-01-03T12:00:00.000000Z\n" +
                        "CGFN\t2.044920362784\t1970-01-03T12:00:00.000000Z\n" +
                        "NPIW\t2.654333891932\t1970-01-03T12:00:00.000000Z\n" +
                        "PEVM\t-1.579533826608\t1970-01-03T12:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t0.352870024362\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t0.508918811082\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t0.079792332332\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t-1.269224403493\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t0.784997946192\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t-0.335562184186\t1970-01-03T15:00:00.000000Z\n" +
                        "CGFN\t1.798656577698\t1970-01-03T15:00:00.000000Z\n" +
                        "NPIW\t2.289076328367\t1970-01-03T15:00:00.000000Z\n" +
                        "PEVM\t-1.185843058819\t1970-01-03T15:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t0.292270806308\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t0.517383208042\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t0.103727958123\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t-1.770658384755\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t0.968351282916\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t-0.426722942775\t1970-01-03T18:00:00.000000Z\n" +
                        "CGFN\t1.552392792612\t1970-01-03T18:00:00.000000Z\n" +
                        "NPIW\t1.923818764802\t1970-01-03T18:00:00.000000Z\n" +
                        "PEVM\t-0.792152291029\t1970-01-03T18:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t0.231671588255\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t0.525847605001\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t0.127663583915\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t-2.272092366017\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t1.151704619641\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t-0.517883701364\t1970-01-03T21:00:00.000000Z\n" +
                        "CGFN\t1.306129007526\t1970-01-03T21:00:00.000000Z\n" +
                        "NPIW\t1.558561201236\t1970-01-03T21:00:00.000000Z\n" +
                        "PEVM\t-0.398461523240\t1970-01-03T21:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t0.171072370201\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t0.534312001961\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t0.151599209706\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-2.773526347280\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t1.335057956366\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t-0.609044459952\t1970-01-04T00:00:00.000000Z\n" +
                        "CGFN\t1.059865222439\t1970-01-04T00:00:00.000000Z\n" +
                        "NPIW\t1.193303637671\t1970-01-04T00:00:00.000000Z\n" +
                        "PEVM\t-0.004770755450\t1970-01-04T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t0.110473152148\t1970-01-04T03:00:00.000000Z\n" +
                        "CGFN\t0.813601437353\t1970-01-04T03:00:00.000000Z\n" +
                        "NPIW\t0.828046074105\t1970-01-04T03:00:00.000000Z\n" +
                        "PEVM\t0.388920012340\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t0.542776398920\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t0.175534835497\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-3.274960328542\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t1.518411293091\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t-0.700205218541\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\t0.523489245443\t1970-01-04T06:00:00.000000Z\n" +
                        "CGFN\t0.567337652267\t1970-01-04T06:00:00.000000Z\n" +
                        "\t0.034652347087\t1970-01-04T06:00:00.000000Z\n" +
                        "PEVM\t0.782610780129\t1970-01-04T06:00:00.000000Z\n" +
                        "ZNFK\t0.138906713030\t1970-01-04T06:00:00.000000Z\n" +
                        "NPIW\t0.462788510540\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t0.551240795880\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t0.199470461288\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t-3.776394309805\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t1.701764629815\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t-0.791365977130\t1970-01-04T06:00:00.000000Z\n",
                true);
    }
}