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

public class LastLongGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testAllNull() throws SqlException {

        compiler.compile("create table tab (f long)", sqlExecutionContext);

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

        compiler.compile("create table tab (f long)", sqlExecutionContext);

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

        compiler.compile("create table tab (f long)", sqlExecutionContext);

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
                Assert.assertEquals(Numbers.LONG_NaN, record.getLong(0));
            }
        }
    }


    @Test
    public void testSampleFill() throws Exception {
        assertQuery("b\tlast\tk\n" +
                        "\t2968650253814730084\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t-7723703968879725602\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t7039584373105579285\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t-6253307669002054137\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t7392877322819819290\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t1205595184115760694\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-6487422186320825289\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-7103100524321179064\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t-6626590012581323602\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-6161552193869048721\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t1761725072747471430\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t-5439556746612026472\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-9147563299122452591\t1970-01-03T06:00:00.000000Z\n" +
                        "\t6601850686822460257\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t4360855047041000285\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t2155318342410845737\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t6624299878707135910\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t-2000273984235276379\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t-3491277789316049618\t1970-01-03T09:00:00.000000Z\n" +
                        "\t7037372650941669660\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t5552835357100545895\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t-8371487291073160693\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t2486874217850272768\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t6959985021334530048\t1970-01-03T09:00:00.000000Z\n",
                "select b, last(a), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_long() a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_long() a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                "b\tlast\tk\n" +
                        "\t2968650253814730084\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t-7723703968879725602\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t7039584373105579285\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t-6253307669002054137\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t7392877322819819290\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t1205595184115760694\t1970-01-03T00:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-6487422186320825289\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-7103100524321179064\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t-6626590012581323602\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-6161552193869048721\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t1761725072747471430\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t-5439556746612026472\t1970-01-03T03:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-9147563299122452591\t1970-01-03T06:00:00.000000Z\n" +
                        "\t6601850686822460257\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t4360855047041000285\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t2155318342410845737\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t6624299878707135910\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t-2000273984235276379\t1970-01-03T06:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t-3491277789316049618\t1970-01-03T09:00:00.000000Z\n" +
                        "\t7037372650941669660\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t5552835357100545895\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t-8371487291073160693\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t2486874217850272768\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t6959985021334530048\t1970-01-03T09:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t5935372070179819520\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t6974022419935821824\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t4481370835493956096\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t2165007720490353664\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t9223372036854775807\t1970-01-03T12:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t4833371489417967616\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t9223372036854775807\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t3409906313887367680\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t7821293230296757248\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t9223372036854775807\t1970-01-03T15:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t3731370908656116224\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t9223372036854775807\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t2338441792280776704\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t9223372036854775807\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t9223372036854775807\t1970-01-03T18:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t2629370327894265344\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t9223372036854775807\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t1266977270674186496\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t9223372036854775807\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t9223372036854775807\t1970-01-03T21:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t1527369747132414464\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t9223372036854775807\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t195512749067602464\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t9223372036854775807\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t9223372036854775807\t1970-01-04T00:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t425369166370563563\t1970-01-04T03:00:00.000000Z\n" +
                        "ZMZV\t750145151786158348\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t9223372036854775807\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-875951772538991360\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t9223372036854775807\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t9223372036854775807\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\t3820631780839257855\t1970-01-04T06:00:00.000000Z\n" +
                        "LOGI\t9200214878918264613\t1970-01-04T06:00:00.000000Z\n" +
                        "QEBN\t-8841102831894340636\t1970-01-04T06:00:00.000000Z\n" +
                        "\t8984775562394712402\t1970-01-04T06:00:00.000000Z\n" +
                        "FOUS\t7629109032541741027\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t9223372036854775807\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t-1947416294145578496\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t9223372036854775807\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t9223372036854775807\t1970-01-04T06:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-04T06:00:00.000000Z\n",
                true,
                true,
                true
        );
    }
}