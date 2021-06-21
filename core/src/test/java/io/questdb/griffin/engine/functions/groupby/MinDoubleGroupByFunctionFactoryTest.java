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

public class MinDoubleGroupByFunctionFactoryTest extends AbstractGriffinTest {

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

        try (RecordCursorFactory factory = compiler.compile("select min(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        try (RecordCursorFactory factory = compiler.compile("select min(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0.0011075139045715332, record.getDouble(0), 0.0001);
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
        try (RecordCursorFactory factory = compiler.compile("select min(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0.0011075139045715332, record.getDouble(0), 0.0001);
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery("b\tmin\tk\n" +
                        "\t0.09750574414434399\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.4217768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.2390529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.12105630273556178\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t0.12026122412833129\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.1202416087573498\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.03993124821273464\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.2185865835029681\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.029080850168636263\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.05158459929273784\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.6590829275055244\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.7365115215570027\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.07828020681514525\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t0.4835256202036067\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.23493793601747937\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.007985454958725269\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.23507754029460548\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t-0.062079908420077275\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.4740684604688953\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t0.03192108074989719\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t0.49199001716312474\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-0.2663564409677917\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.4182912727422209\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t-0.15324066700879083\t1970-01-03T09:00:00.000000Z\n",
                "select b, min(a), k from x sample by 3h fill(linear)",
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
                "b\tmin\tk\n" +
                        "\t0.09750574414434399\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.4217768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.2390529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.12105630273556178\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t0.12026122412833129\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.1202416087573498\t1970-01-03T00:00:00.000000Z\n" +
                        "CGFN\t3.0299755031293305\t1970-01-03T00:00:00.000000Z\n" +
                        "NPIW\t4.115364146194077\t1970-01-03T00:00:00.000000Z\n" +
                        "PEVM\t-3.15429689776691\t1970-01-03T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.03993124821273464\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.2185865835029681\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.029080850168636263\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.05158459929273784\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.6590829275055244\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.7365115215570027\t1970-01-03T03:00:00.000000Z\n" +
                        "CGFN\t2.783711718043071\t1970-01-03T03:00:00.000000Z\n" +
                        "NPIW\t3.750106582628655\t1970-01-03T03:00:00.000000Z\n" +
                        "PEVM\t-2.7606061299772864\t1970-01-03T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.07828020681514525\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t0.4835256202036067\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.23493793601747937\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.007985454958725269\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.23507754029460548\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t-0.062079908420077275\t1970-01-03T06:00:00.000000Z\n" +
                        "CGFN\t2.5374479329568107\t1970-01-03T06:00:00.000000Z\n" +
                        "NPIW\t3.3848490190632345\t1970-01-03T06:00:00.000000Z\n" +
                        "PEVM\t-2.3669153621876635\t1970-01-03T06:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.4740684604688953\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t0.03192108074989719\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t0.49199001716312474\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-0.2663564409677917\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.4182912727422209\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t-0.15324066700879083\t1970-01-03T09:00:00.000000Z\n" +
                        "CGFN\t2.291184147870552\t1970-01-03T09:00:00.000000Z\n" +
                        "NPIW\t3.0195914554978125\t1970-01-03T09:00:00.000000Z\n" +
                        "PEVM\t-1.9732245943980409\t1970-01-03T09:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t0.4134692424154023\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t0.5004544141226428\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t0.055856706541069105\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t-0.7677904222301889\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t0.6016446094669624\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t-0.24440142559750433\t1970-01-03T12:00:00.000000Z\n" +
                        "CGFN\t2.0449203627842922\t1970-01-03T12:00:00.000000Z\n" +
                        "NPIW\t2.6543338919323913\t1970-01-03T12:00:00.000000Z\n" +
                        "PEVM\t-1.5795338266084187\t1970-01-03T12:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t0.3528700243619092\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t0.5089188110821609\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t0.07979233233224103\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t-1.2692244034925861\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t0.7849979461917039\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t-0.33556218418621786\t1970-01-03T15:00:00.000000Z\n" +
                        "CGFN\t1.7986565776980326\t1970-01-03T15:00:00.000000Z\n" +
                        "NPIW\t2.28907632836697\t1970-01-03T15:00:00.000000Z\n" +
                        "PEVM\t-1.1858430588187956\t1970-01-03T15:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t0.2922708063084161\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t0.5173832080416787\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t0.10372795812341296\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t-1.7706583847549835\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t0.9683512829164456\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t-0.4267229427749314\t1970-01-03T18:00:00.000000Z\n" +
                        "CGFN\t1.552392792611773\t1970-01-03T18:00:00.000000Z\n" +
                        "NPIW\t1.9238187648015488\t1970-01-03T18:00:00.000000Z\n" +
                        "PEVM\t-0.7921522910291726\t1970-01-03T18:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t0.23167158825492307\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t0.525847605001197\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t0.12766358391458488\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t-2.2720923660173806\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t1.1517046196411869\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t-0.5178837013636449\t1970-01-03T21:00:00.000000Z\n" +
                        "CGFN\t1.3061290075255139\t1970-01-03T21:00:00.000000Z\n" +
                        "NPIW\t1.5585612012361274\t1970-01-03T21:00:00.000000Z\n" +
                        "PEVM\t-0.3984615232395501\t1970-01-03T21:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t0.17107237020143\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t0.5343120019607148\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t0.15159920970575683\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-2.7735263472797778\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t1.3350579563659284\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t-0.6090444599523585\t1970-01-04T00:00:00.000000Z\n" +
                        "CGFN\t1.0598652224392542\t1970-01-04T00:00:00.000000Z\n" +
                        "NPIW\t1.1933036376707062\t1970-01-04T00:00:00.000000Z\n" +
                        "PEVM\t-0.004770755449927295\t1970-01-04T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t0.11047315214793696\t1970-01-04T03:00:00.000000Z\n" +
                        "CGFN\t0.8136014373529948\t1970-01-04T03:00:00.000000Z\n" +
                        "NPIW\t0.8280460741052847\t1970-01-04T03:00:00.000000Z\n" +
                        "PEVM\t0.3889200123396954\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t0.5427763989202327\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t0.1755348354969287\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-3.2749603285421753\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t1.51841129309067\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t-0.700205218541072\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\t0.5234892454427748\t1970-01-04T06:00:00.000000Z\n" +
                        "CGFN\t0.5673376522667354\t1970-01-04T06:00:00.000000Z\n" +
                        "\t0.034652347087289925\t1970-01-04T06:00:00.000000Z\n" +
                        "PEVM\t0.7826107801293182\t1970-01-04T06:00:00.000000Z\n" +
                        "ZNFK\t0.1389067130304884\t1970-01-04T06:00:00.000000Z\n" +
                        "NPIW\t0.4627885105398635\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t0.5512407958797512\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t0.19947046128810061\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t-3.7763943098045716\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t1.7017646298154117\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t-0.7913659771297854\t1970-01-04T06:00:00.000000Z\n",
                true,
                true,
                true);
    }
}