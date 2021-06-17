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

public class LastDoubleGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testLastDouble() throws Exception {
        assertQuery(
                "x\n" +
                        "10.0\n",
                "select last(x) x from tab",
                "create table tab as (select cast(x as double) x from long_sequence(10))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testLastDoubleNull() throws Exception {
        assertQuery(
                "y\n" +
                        "NaN\n",
                "select last(y) y from tab",
                "create table tab as (select cast(x as double) x, cast(null as double) y from long_sequence(100))",
                null,
                false,
                true,
                true
        );
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

        try (RecordCursorFactory factory = compiler.compile("select last(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertTrue(Double.isNaN(record.getDouble(0)));
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
        try (RecordCursorFactory factory = compiler.compile("select last(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0.3901731258748704, record.getDouble(0), 0.0001);
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery("b\tlast\tk\n" +
                        "\t0.44804689668613573\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.8685154305419587\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.5659429139861241\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.12105630273556178\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t0.49428905119584543\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.6752509547112409\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.05758228485190853\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.6697969295620055\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.6940904779678791\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.05158459929273784\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.6590829275055244\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.7365115215570027\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.49153268154777974\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t0.8196554745841765\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.23493793601747937\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.007985454958725269\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.4346135812930124\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t0.7129300012245174\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.7202789791127316\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t0.03192108074989719\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t0.7732229848518976\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t0.13271564102902209\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.4182912727422209\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t0.7317695244811556\t1970-01-03T09:00:00.000000Z\n",
                "select b, last(a), k from x sample by 3h fill(linear)",
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
                "b\tlast\tk\n" +
                        "\t0.44804689668613573\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.8685154305419587\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.5659429139861241\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.12105630273556178\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t0.49428905119584543\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.6752509547112409\t1970-01-03T00:00:00.000000Z\n" +
                        "CGFN\t2.7171094533334066\t1970-01-03T00:00:00.000000Z\n" +
                        "NPIW\t4.115364146194077\t1970-01-03T00:00:00.000000Z\n" +
                        "PEVM\t-3.15429689776691\t1970-01-03T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.05758228485190853\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.6697969295620055\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.6940904779678791\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.05158459929273784\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.6590829275055244\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.7365115215570027\t1970-01-03T03:00:00.000000Z\n" +
                        "CGFN\t2.505608562668917\t1970-01-03T03:00:00.000000Z\n" +
                        "NPIW\t3.750106582628655\t1970-01-03T03:00:00.000000Z\n" +
                        "PEVM\t-2.7606061299772864\t1970-01-03T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.49153268154777974\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t0.8196554745841765\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.23493793601747937\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.007985454958725269\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.4346135812930124\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t0.7129300012245174\t1970-01-03T06:00:00.000000Z\n" +
                        "CGFN\t2.294107672004426\t1970-01-03T06:00:00.000000Z\n" +
                        "NPIW\t3.3848490190632345\t1970-01-03T06:00:00.000000Z\n" +
                        "PEVM\t-2.3669153621876635\t1970-01-03T06:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.7202789791127316\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t0.03192108074989719\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t0.7732229848518976\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t0.13271564102902209\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.4182912727422209\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t0.7317695244811556\t1970-01-03T09:00:00.000000Z\n" +
                        "CGFN\t2.0826067813399365\t1970-01-03T09:00:00.000000Z\n" +
                        "NPIW\t3.0195914554978125\t1970-01-03T09:00:00.000000Z\n" +
                        "PEVM\t-1.9732245943980409\t1970-01-03T09:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t0.6914896391199901\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t0.7267904951196187\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t0.055856706541069105\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t-0.1691822992349681\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t0.6016446094669624\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t0.7506090477377941\t1970-01-03T12:00:00.000000Z\n" +
                        "CGFN\t1.8711058906754459\t1970-01-03T12:00:00.000000Z\n" +
                        "NPIW\t2.6543338919323913\t1970-01-03T12:00:00.000000Z\n" +
                        "PEVM\t-1.5795338266084187\t1970-01-03T12:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t0.6627002991272485\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t0.6803580053873398\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t0.07979233233224103\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t-0.4710802394989586\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t0.7849979461917039\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t0.7694485709944322\t1970-01-03T15:00:00.000000Z\n" +
                        "CGFN\t1.6596050000109557\t1970-01-03T15:00:00.000000Z\n" +
                        "NPIW\t2.28907632836697\t1970-01-03T15:00:00.000000Z\n" +
                        "PEVM\t-1.1858430588187956\t1970-01-03T15:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t0.6339109591345069\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t0.6339255156550605\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t0.10372795812341296\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t-0.7729781797629487\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t0.9683512829164456\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t0.7882880942510704\t1970-01-03T18:00:00.000000Z\n" +
                        "CGFN\t1.4481041093464653\t1970-01-03T18:00:00.000000Z\n" +
                        "NPIW\t1.9238187648015488\t1970-01-03T18:00:00.000000Z\n" +
                        "PEVM\t-0.7921522910291726\t1970-01-03T18:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t0.6051216191417653\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t0.5874930259227816\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t0.12766358391458488\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t-1.0748761200269392\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t1.1517046196411869\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t0.8071276175077092\t1970-01-03T21:00:00.000000Z\n" +
                        "CGFN\t1.2366032186819753\t1970-01-03T21:00:00.000000Z\n" +
                        "NPIW\t1.5585612012361274\t1970-01-03T21:00:00.000000Z\n" +
                        "PEVM\t-0.3984615232395501\t1970-01-03T21:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t0.5763322791490237\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t0.5410605361905028\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t0.15159920970575683\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-1.3767740602909293\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t1.3350579563659284\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t0.8259671407643474\t1970-01-04T00:00:00.000000Z\n" +
                        "CGFN\t1.025102328017485\t1970-01-04T00:00:00.000000Z\n" +
                        "NPIW\t1.1933036376707062\t1970-01-04T00:00:00.000000Z\n" +
                        "PEVM\t-0.004770755449927295\t1970-01-04T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t0.5475429391562822\t1970-01-04T03:00:00.000000Z\n" +
                        "CGFN\t0.8136014373529948\t1970-01-04T03:00:00.000000Z\n" +
                        "NPIW\t0.8280460741052847\t1970-01-04T03:00:00.000000Z\n" +
                        "PEVM\t0.3889200123396954\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t0.4946280464582231\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t0.1755348354969287\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-1.6786720005549198\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t1.51841129309067\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t0.8448066640209855\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\t0.8387598218385978\t1970-01-04T06:00:00.000000Z\n" +
                        "CGFN\t0.6021005466885047\t1970-01-04T06:00:00.000000Z\n" +
                        "\t0.5364603752015349\t1970-01-04T06:00:00.000000Z\n" +
                        "PEVM\t0.7826107801293182\t1970-01-04T06:00:00.000000Z\n" +
                        "ZNFK\t0.7806183442034064\t1970-01-04T06:00:00.000000Z\n" +
                        "NPIW\t0.4627885105398635\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t0.44819555672594497\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t0.19947046128810061\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t-1.9805699408189095\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t1.7017646298154117\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t0.8636461872776237\t1970-01-04T06:00:00.000000Z\n",
                true,
                true,
                true
        );
    }
}
