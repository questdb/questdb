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

public class FirstLongGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNull() throws SqlException {
        ddl("create table tab (f long)");

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
        ddl("create table tab (f long)");

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
        ddl("create table tab (f long)");

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
                        "\t7953532976996720859\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t-3985256597569472057\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t7039584373105579285\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t375856366519011353\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t8000176386900538697\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t7199909180655756830\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-7316123607359392486\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-5233802075754153909\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t7522482991756933150\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t3518554007419864093\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t4014104627539596639\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t-4908948886680892316\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-5024542231726589509\t1970-01-03T06:00:00.000000Z\n" +
                        "\t7277991313017866925\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t-5817309269683380708\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t9143800334706665900\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t6624299878707135910\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t-6912707344119330199\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t8574802735490373479\t1970-01-03T09:00:00.000000Z\n" +
                        "\t-3293392739929464726\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t6127579245089953588\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t-8371487291073160693\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t-6509291080879266816\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T09:00:00.000000Z\n",
                "select b, first(a), k from x sample by 3h fill(linear)",
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
                "b\tfirst\tk\n" +
                        "\t7953532976996720859\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t-3985256597569472057\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t7039584373105579285\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t375856366519011353\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t8000176386900538697\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t7199909180655756830\t1970-01-03T00:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t-7316123607359392486\t1970-01-03T03:00:00.000000Z\n" +
                        "\t-5233802075754153909\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t7522482991756933150\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t3518554007419864093\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t4014104627539596639\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t-4908948886680892316\t1970-01-03T03:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t-5024542231726589509\t1970-01-03T06:00:00.000000Z\n" +
                        "\t7277991313017866925\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t-5817309269683380708\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t9143800334706665900\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t6624299878707135910\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t-6912707344119330199\t1970-01-03T06:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t8574802735490373479\t1970-01-03T09:00:00.000000Z\n" +
                        "\t-3293392739929464726\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t6127579245089953588\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t-8371487291073160693\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t-6509291080879266816\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t-3944981163069335552\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t-6105874817639201792\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t5630858611472772096\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t9223372036854775807\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t-4596569586209205760\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t-5702458554399140864\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t5134137977855591424\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t9223372036854775807\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t-5248158009349075968\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t-5299042291159078912\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t4637417344238409728\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t9223372036854775807\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t-5899746432488946688\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t-4895626027919009792\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t4140696710621224448\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t9223372036854775807\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t-6551334855628817408\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t-4492209764678950912\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t3643976077004049408\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t9223372036854775807\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t-7202923278768687325\t1970-01-04T03:00:00.000000Z\n" +
                        "ZMZV\t-3396992238702724434\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t-4088793501438891520\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t3147255443386861568\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t9223372036854775807\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "LOGI\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "QEBN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "FOUS\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\t-4284648096271470489\t1970-01-04T06:00:00.000000Z\n" +
                        "LOGI\t8984932460293088377\t1970-01-04T06:00:00.000000Z\n" +
                        "QEBN\t-8841102831894340636\t1970-01-04T06:00:00.000000Z\n" +
                        "\t663602980874300508\t1970-01-04T06:00:00.000000Z\n" +
                        "FOUS\t9044897286885345735\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t-3685377238198819840\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t2650534809769686528\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t9223372036854775807\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "ZMZV\tNaN\t1970-01-04T06:00:00.000000Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSomeNull() throws SqlException {
        ddl("create table tab (f long)");

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
