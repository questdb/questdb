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
    public void testAllNull() throws SqlException {

        compiler.compile("create table tab (f double)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select max(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertTrue(Double.isNaN(record.getDouble(0)));
            }
        }
    }

    @Test
    public void testFirstNull() throws SqlException {

        compiler.compile("create table tab (f double)", sqlExecutionContext);

        final Rnd rnd = new Rnd();
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            TableWriter.Row r = w.newRow();
            r.append();
            for (int i = 100; i > 10; i--) {
                r = w.newRow();
                r.putDouble(0, rnd.nextDouble());
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select max(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0.9856290845874263, record.getDouble(0), 0.0001);
            }
        }
    }

    @Test
    public void testNonNull() throws SqlException {

        compiler.compile("create table tab (f double)", sqlExecutionContext);

        final Rnd rnd = new Rnd();
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, rnd.nextDouble());
                r.append();
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select max(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0.9856290845874263, record.getDouble(0), 0.0001);
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery("b\tmax\tk\n" +
                        "\t0.975019885372507\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.8685154305419587\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.5659429139861241\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.8445258177211064\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t0.9771103146051203\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.6752509547112409\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.8940917126581895\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.9441658975532605\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.9457212646911386\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.9455893004802433\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.8438459563914771\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.7365115215570027\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.9423671624137644\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t0.8196554745841765\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.5780746276543334\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.6901976778065181\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.4346135812930124\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t1.2161915746710363\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.8379891991223047\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t0.7272119755925095\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t0.7732229848518976\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t0.13271564102902209\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.21055995482842357\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t1.486661884650934\t1970-01-03T09:00:00.000000Z\n",
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
                        "\t0.975019885372507\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.8685154305419587\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.5659429139861241\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.8445258177211064\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t0.9771103146051203\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.6752509547112409\t1970-01-03T00:00:00.000000Z\n" +
                        "CGFN\t2.7171094533334066\t1970-01-03T00:00:00.000000Z\n" +
                        "NPIW\t4.115364146194077\t1970-01-03T00:00:00.000000Z\n" +
                        "PEVM\t-3.7258904043577563\t1970-01-03T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.8940917126581895\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.9441658975532605\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.9457212646911386\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.9455893004802433\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.8438459563914771\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.7365115215570027\t1970-01-03T03:00:00.000000Z\n" +
                        "CGFN\t2.505608562668917\t1970-01-03T03:00:00.000000Z\n" +
                        "NPIW\t3.750106582628655\t1970-01-03T03:00:00.000000Z\n" +
                        "PEVM\t-3.2686892469469284\t1970-01-03T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.9423671624137644\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t0.8196554745841765\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.5780746276543334\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.6901976778065181\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.4346135812930124\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t1.2161915746710363\t1970-01-03T06:00:00.000000Z\n" +
                        "CGFN\t2.294107672004426\t1970-01-03T06:00:00.000000Z\n" +
                        "NPIW\t3.3848490190632345\t1970-01-03T06:00:00.000000Z\n" +
                        "PEVM\t-2.8114880895361005\t1970-01-03T06:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.8379891991223047\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t0.7272119755925095\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t0.7732229848518976\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t0.13271564102902209\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.21055995482842357\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t1.486661884650934\t1970-01-03T09:00:00.000000Z\n" +
                        "CGFN\t2.0826067813399365\t1970-01-03T09:00:00.000000Z\n" +
                        "NPIW\t3.0195914554978125\t1970-01-03T09:00:00.000000Z\n" +
                        "PEVM\t-2.354286932125272\t1970-01-03T09:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t0.8468512597855531\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t0.7267904951196187\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t0.7642262733785008\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t-0.1691822992349681\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t-0.15695471799748634\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t1.757132194630832\t1970-01-03T12:00:00.000000Z\n" +
                        "CGFN\t1.8711058906754459\t1970-01-03T12:00:00.000000Z\n" +
                        "NPIW\t2.6543338919323913\t1970-01-03T12:00:00.000000Z\n" +
                        "PEVM\t-1.8970857747144447\t1970-01-03T12:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t0.8557133204488016\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t0.6803580053873398\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t0.8012405711644923\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t-0.4710802394989586\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t-0.5244693908233964\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t2.02760250461073\t1970-01-03T15:00:00.000000Z\n" +
                        "CGFN\t1.6596050000109557\t1970-01-03T15:00:00.000000Z\n" +
                        "NPIW\t2.28907632836697\t1970-01-03T15:00:00.000000Z\n" +
                        "PEVM\t-1.4398846173036166\t1970-01-03T15:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t0.8645753811120501\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t0.6339255156550605\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t0.8382548689504835\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t-0.7729781797629487\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t-0.8919840636493057\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t2.2980728145906273\t1970-01-03T18:00:00.000000Z\n" +
                        "CGFN\t1.4481041093464653\t1970-01-03T18:00:00.000000Z\n" +
                        "NPIW\t1.9238187648015488\t1970-01-03T18:00:00.000000Z\n" +
                        "PEVM\t-0.9826834598927885\t1970-01-03T18:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t0.8734374417752984\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t0.5874930259227816\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t0.8752691667364753\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t-1.0748761200269392\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t-1.2594987364752162\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t2.568543124570526\t1970-01-03T21:00:00.000000Z\n" +
                        "CGFN\t1.2366032186819753\t1970-01-03T21:00:00.000000Z\n" +
                        "NPIW\t1.5585612012361274\t1970-01-03T21:00:00.000000Z\n" +
                        "PEVM\t-0.5254823024819606\t1970-01-03T21:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t0.8822995024385468\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t0.5410605361905028\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t0.9122834645224663\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-1.3767740602909293\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t-1.6270134093011264\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t2.839013434550423\t1970-01-04T00:00:00.000000Z\n" +
                        "CGFN\t1.025102328017485\t1970-01-04T00:00:00.000000Z\n" +
                        "NPIW\t1.1933036376707062\t1970-01-04T00:00:00.000000Z\n" +
                        "PEVM\t-0.06828114507113253\t1970-01-04T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t0.8911615631017953\t1970-01-04T03:00:00.000000Z\n" +
                        "CGFN\t0.8136014373529948\t1970-01-04T03:00:00.000000Z\n" +
                        "NPIW\t0.8280460741052847\t1970-01-04T03:00:00.000000Z\n" +
                        "PEVM\t0.3889200123396954\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t0.4946280464582231\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t0.9492977623084576\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-1.6786720005549198\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t-1.9945280821270364\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t3.1094837445303205\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\t0.8514849800664227\t1970-01-04T06:00:00.000000Z\n" +
                        "CGFN\t0.6021005466885047\t1970-01-04T06:00:00.000000Z\n" +
                        "\t0.9435138098640453\t1970-01-04T06:00:00.000000Z\n" +
                        "PEVM\t0.8461211697505234\t1970-01-04T06:00:00.000000Z\n" +
                        "ZNFK\t0.7806183442034064\t1970-01-04T06:00:00.000000Z\n" +
                        "NPIW\t0.4627885105398635\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t0.44819555672594497\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t0.986312060094449\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t-1.9805699408189095\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t-2.362042754952945\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t3.379954054510219\t1970-01-04T06:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testSampleInterpolateRandomAccessConsistency() throws Exception {
        assertQuery(
                "b\tmax\tk\n" +
                        "PEHN\t0.8445258177211064\t1970-01-03T00:18:00.000000Z\n" +
                        "VTJW\t0.9125204540487346\t1970-01-03T00:18:00.000000Z\n" +
                        "PEHN\t0.7365115215570027\t1970-01-03T03:18:00.000000Z\n" +
                        "VTJW\t0.8660879643164553\t1970-01-03T03:18:00.000000Z\n" +
                        "PEHN\t0.4346135812930124\t1970-01-03T06:18:00.000000Z\n" +
                        "VTJW\t0.8196554745841765\t1970-01-03T06:18:00.000000Z\n" +
                        "PEHN\t0.13271564102902209\t1970-01-03T09:18:00.000000Z\n" +
                        "VTJW\t0.7732229848518976\t1970-01-03T09:18:00.000000Z\n",
                "select b, max(a), k from " +
                        " (x where b = 'PEHN' union all x where b = 'VTJW' ) timestamp(k)" +
                        "sample by 3h fill(linear) order by 3, 2, 1",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0) a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                null,
                true,
                true,
                true
        );
    }
}