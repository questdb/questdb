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
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class FirstDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNull() throws SqlException {
        ddl("create table tab (f double)");

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
                Assert.assertTrue(Double.isNaN(record.getDouble(0)));
            }
        }
    }

    @Test
    public void testFirstNull() throws SqlException {
        ddl("create table tab (f double)");

        final Rnd rnd = new Rnd();
        try (TableWriter w = getWriter("tab")) {
            TableWriter.Row r = w.newRow();
            r.append();
            for (int i = 100; i > 10; i--) {
                r = w.newRow();
                r.putDouble(0, rnd.nextDouble());
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select first(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(Double.NaN, record.getDouble(0), 0.0001);
            }
        }
    }

    @Test
    public void testNonNull() throws SqlException {
        ddl("create table tab (f double)");

        final Rnd rnd = new Rnd();
        try (TableWriter w = getWriter("tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, rnd.nextDouble());
                r.append();
            }
            w.commit();
        }
        try (RecordCursorFactory factory = select("select first(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0.6607777894187332, record.getDouble(0), 0.0001);
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery(
                "b\tfirst\tk\n" +
                        "\t0.11427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.4217768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.2390529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.7094360487171202\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t0.9771103146051203\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.1202416087573498\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.8940917126581895\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.9441658975532605\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.625966045857722\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.9455893004802433\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.8438459563914771\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.7365115215570027\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.4592067757817594\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t0.7606252634124595\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.5780746276543334\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.6901976778065181\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.23507754029460548\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t1.131690482958094\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.8379891991223047\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t0.3004874521886858\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t0.49199001716312474\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-0.2663564409677917\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.21055995482842357\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t1.6374149200584662\t1970-01-03T09:00:00.000000Z\n",
                "select b, first(a), k from x sample by 3h fill(linear)",
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
                "b\tfirst\tk\n" +
                        "\t0.11427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t0.4217768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t0.2390529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t0.7094360487171202\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t0.9771103146051203\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.1202416087573498\t1970-01-03T00:00:00.000000Z\n" +
                        "CGFN\t3.0299755031293305\t1970-01-03T00:00:00.000000Z\n" +
                        "NPIW\t4.115364146194077\t1970-01-03T00:00:00.000000Z\n" +
                        "PEVM\t-3.7258904043577563\t1970-01-03T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t0.8940917126581895\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t0.9441658975532605\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t0.625966045857722\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t0.9455893004802433\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t0.8438459563914771\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t0.7365115215570027\t1970-01-03T03:00:00.000000Z\n" +
                        "CGFN\t2.783711718043071\t1970-01-03T03:00:00.000000Z\n" +
                        "NPIW\t3.750106582628655\t1970-01-03T03:00:00.000000Z\n" +
                        "PEVM\t-3.2686892469469284\t1970-01-03T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t0.4592067757817594\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t0.7606252634124595\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t0.5780746276543334\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t0.6901976778065181\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t0.23507754029460548\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t1.131690482958094\t1970-01-03T06:00:00.000000Z\n" +
                        "CGFN\t2.5374479329568107\t1970-01-03T06:00:00.000000Z\n" +
                        "NPIW\t3.3848490190632345\t1970-01-03T06:00:00.000000Z\n" +
                        "PEVM\t-2.8114880895361005\t1970-01-03T06:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t0.8379891991223047\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t0.3004874521886858\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t0.49199001716312474\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-0.2663564409677917\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t0.21055995482842357\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t1.6374149200584662\t1970-01-03T09:00:00.000000Z\n" +
                        "CGFN\t2.291184147870552\t1970-01-03T09:00:00.000000Z\n" +
                        "NPIW\t3.0195914554978125\t1970-01-03T09:00:00.000000Z\n" +
                        "PEVM\t-2.354286932125272\t1970-01-03T09:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t0.8468512597855531\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t0.22335477091378989\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t-0.08922277342914652\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t-0.7677904222301889\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t-0.15695471799748634\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t2.1431393571588386\t1970-01-03T12:00:00.000000Z\n" +
                        "CGFN\t2.0449203627842922\t1970-01-03T12:00:00.000000Z\n" +
                        "NPIW\t2.6543338919323913\t1970-01-03T12:00:00.000000Z\n" +
                        "PEVM\t-1.8970857747144447\t1970-01-03T12:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t0.8557133204488016\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t-0.045280475335544836\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t-0.4789329990469788\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t-1.2692244034925861\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t-0.5244693908233964\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t2.6488637942592104\t1970-01-03T15:00:00.000000Z\n" +
                        "CGFN\t1.7986565776980326\t1970-01-03T15:00:00.000000Z\n" +
                        "NPIW\t2.28907632836697\t1970-01-03T15:00:00.000000Z\n" +
                        "PEVM\t-1.4398846173036166\t1970-01-03T15:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t0.8645753811120501\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t-0.31391572158487957\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t-0.8686432246648113\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t-1.7706583847549835\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t-0.8919840636493057\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t3.154588231359583\t1970-01-03T18:00:00.000000Z\n" +
                        "CGFN\t1.552392792611773\t1970-01-03T18:00:00.000000Z\n" +
                        "NPIW\t1.9238187648015488\t1970-01-03T18:00:00.000000Z\n" +
                        "PEVM\t-0.9826834598927885\t1970-01-03T18:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t0.8734374417752984\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t-0.5825509678342145\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t-1.2583534502826434\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t-2.2720923660173806\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t-1.2594987364752162\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t3.660312668459955\t1970-01-03T21:00:00.000000Z\n" +
                        "CGFN\t1.3061290075255139\t1970-01-03T21:00:00.000000Z\n" +
                        "NPIW\t1.5585612012361274\t1970-01-03T21:00:00.000000Z\n" +
                        "PEVM\t-0.5254823024819606\t1970-01-03T21:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t0.8822995024385468\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t-0.851186214083549\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t-1.6480636759004759\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-2.7735263472797778\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t-1.6270134093011264\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t4.166037105560327\t1970-01-04T00:00:00.000000Z\n" +
                        "CGFN\t1.0598652224392542\t1970-01-04T00:00:00.000000Z\n" +
                        "NPIW\t1.1933036376707062\t1970-01-04T00:00:00.000000Z\n" +
                        "PEVM\t-0.06828114507113253\t1970-01-04T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t0.8911615631017953\t1970-01-04T03:00:00.000000Z\n" +
                        "CGFN\t0.8136014373529948\t1970-01-04T03:00:00.000000Z\n" +
                        "NPIW\t0.8280460741052847\t1970-01-04T03:00:00.000000Z\n" +
                        "PEVM\t0.3889200123396954\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t-1.119821460332884\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t-2.037773901518308\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-3.2749603285421753\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t-1.9945280821270364\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t4.671761542660699\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\t0.8514849800664227\t1970-01-04T06:00:00.000000Z\n" +
                        "CGFN\t0.5673376522667354\t1970-01-04T06:00:00.000000Z\n" +
                        "\t0.9435138098640453\t1970-01-04T06:00:00.000000Z\n" +
                        "PEVM\t0.8461211697505234\t1970-01-04T06:00:00.000000Z\n" +
                        "ZNFK\t0.5900836401674938\t1970-01-04T06:00:00.000000Z\n" +
                        "NPIW\t0.4627885105398635\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t-1.388456706582219\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t-2.4274841271361405\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t-3.7763943098045716\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t-2.362042754952945\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t5.177485979761071\t1970-01-04T06:00:00.000000Z\n",
                true,
                true,
                false
        );
    }
}
