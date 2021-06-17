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

public class MinLongGroupByFunctionFactoryTest extends AbstractGriffinTest {

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

        try (RecordCursorFactory factory = compiler.compile("select min(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        compiler.compile("create table tab (f long)", sqlExecutionContext);

        final Rnd rnd = new Rnd();
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            TableWriter.Row r = w.newRow();
            r.append();
            for (int i = 100; i > 10; i--) {
                r = w.newRow();
                r.putLong(0, rnd.nextLong());
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select min(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(-8968886490993754893L, record.getLong(0));
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
        try (RecordCursorFactory factory = compiler.compile("select min(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(-8968886490993754893L, record.getLong(0));
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

        try (RecordCursorFactory factory = compiler.compile("select min(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(12, record.getLong(0));
            }
        }
    }


    @Test
    public void testSampleFill() throws Exception {
        assertQuery("b\tmin\tk\n" +
                        "\t-7885528361265853230\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t-7723703968879725602\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t7039584373105579285\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t-6253307669002054137\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t6270672455202306717\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t1205595184115760694\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-7689224645273531603\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-9128506055317587235\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t-6626590012581323602\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-6161552193869048721\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t-7995393784734742820\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t-5439556746612026472\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-9147563299122452591\t1970-01-03T06:00:00.000000Z\n" +
                        "\t-8757007522346766135\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t-5817309269683380708\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t-5852887087189258121\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t6624299878707135910\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t-6912707344119330199\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t-6703401424236463520\t1970-01-03T09:00:00.000000Z\n" +
                        "\t-3293392739929464726\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t5552835357100545895\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t-8371487291073160693\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t-6136190042965128192\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t-3639224754632017920\t1970-01-03T09:00:00.000000Z\n",
                "select b, min(a), k from x sample by 3h fill(linear)",
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
                "b\tmin\tk\n" +
                        "\t-7885528361265853230\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t-7723703968879725602\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t7039584373105579285\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t-6253307669002054137\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t6270672455202306717\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t1205595184115760694\t1970-01-03T00:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-7689224645273531603\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-9128506055317587235\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t-6626590012581323602\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-6161552193869048721\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t-7995393784734742820\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t-5439556746612026472\t1970-01-03T03:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-9147563299122452591\t1970-01-03T06:00:00.000000Z\n" +
                        "\t-8757007522346766135\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t-5817309269683380708\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t-5852887087189258121\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t6624299878707135910\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t-6912707344119330199\t1970-01-03T06:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t-6703401424236463520\t1970-01-03T09:00:00.000000Z\n" +
                        "\t-3293392739929464726\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t5552835357100545895\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t-8371487291073160693\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t-6136190042965128192\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t-3639224754632017920\t1970-01-03T09:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t-3944981163069335552\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t-5359672741810924544\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t4481370835493956096\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t-4259239549350473728\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t-1461140239580656384\t1970-01-03T12:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t-4596569586209205760\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t-4583155440656723968\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t3409906313887367680\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t-1815077674464482816\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t716944275470705152\t1970-01-03T15:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t-5248158009349075968\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t-3806638139502523904\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t2338441792280776704\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t629084200421505024\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t2895028790522070016\t1970-01-03T18:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t-5899746432488946688\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t-3030120838348320256\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t1266977270674186496\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t3073246075307492864\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t5073113305573431296\t1970-01-03T21:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t-6551334855628817408\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t-2253603537194116608\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t195512749067602464\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t5517407950193483776\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t7251197820624792576\t1970-01-04T00:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t-7202923278768687325\t1970-01-04T03:00:00.000000Z\n" +
                        "ZMZV\t-4058426794463997577\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t-1477086236039919616\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-875951772538991360\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t7961569825079475200\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t9223372036854775807\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\t-4284648096271470489\t1970-01-04T06:00:00.000000Z\n" +
                        "LOGI\t8984932460293088377\t1970-01-04T06:00:00.000000Z\n" +
                        "QEBN\t-8841102831894340636\t1970-01-04T06:00:00.000000Z\n" +
                        "\t-8960406850507339854\t1970-01-04T06:00:00.000000Z\n" +
                        "FOUS\t812677186520066053\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t-700568934885709440\t1970-01-04T06:00:00.000000Z\n" +
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