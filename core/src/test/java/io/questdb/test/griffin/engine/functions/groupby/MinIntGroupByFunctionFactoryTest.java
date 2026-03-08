/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class MinIntGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNull() throws SqlException {
        execute("create table tab (f int)");

        try (TableWriter w = getWriter("tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select min(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(Numbers.INT_NULL, record.getInt(0));
            }
        }
    }

    @Test
    public void testFirstNull() throws SqlException {
        execute("create table tab (f int)");

        try (TableWriter w = getWriter("tab")) {
            TableWriter.Row r = w.newRow();
            r.append();
            for (int i = 100; i > 10; i--) {
                r = w.newRow();
                r.putInt(0, i);
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select min(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(11, record.getInt(0));
            }
        }
    }

    @Test
    public void testMaxIntOrNull() throws Exception {
        assertQuery("""
                a\tmin
                1\tnull
                """, "select a, min(f) from tab", "create table tab as (select cast(1 as int) a, cast(null as int) f from long_sequence(33))", null, "insert into tab select 1, 2147483647 from long_sequence(1)", """
                a\tmin
                1\t2147483647
                """, true, true, false);
    }

    @Test
    public void testNonNull() throws SqlException {
        execute("create table tab (f int)");

        try (TableWriter w = getWriter("tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.putInt(0, i);
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select min(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(11, record.getInt(0));
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery("""
                b\tmin\tk
                \t-1792928964\t1970-01-03T00:00:00.000000Z
                VTJW\t-2002373666\t1970-01-03T00:00:00.000000Z
                RXGZ\t-1520872171\t1970-01-03T00:00:00.000000Z
                PEHN\t-1252906348\t1970-01-03T00:00:00.000000Z
                CPSW\t-2119387831\t1970-01-03T00:00:00.000000Z
                HYRX\t-1272693194\t1970-01-03T00:00:00.000000Z
                RXGZ\t-1538602195\t1970-01-03T03:00:00.000000Z
                \t-2075675260\t1970-01-03T03:00:00.000000Z
                PEHN\t-661194722\t1970-01-03T03:00:00.000000Z
                CPSW\t-882371473\t1970-01-03T03:00:00.000000Z
                HYRX\t-1593630138\t1970-01-03T03:00:00.000000Z
                VTJW\t215354468\t1970-01-03T03:00:00.000000Z
                CPSW\t519895483\t1970-01-03T06:00:00.000000Z
                \t-1470806499\t1970-01-03T06:00:00.000000Z
                HYRX\t-948252781\t1970-01-03T06:00:00.000000Z
                VTJW\t-246923735\t1970-01-03T06:00:00.000000Z
                PEHN\t-2080340570\t1970-01-03T06:00:00.000000Z
                RXGZ\t-1383560599\t1970-01-03T06:00:00.000000Z
                CPSW\t-1182156192\t1970-01-03T09:00:00.000000Z
                \t-1901633430\t1970-01-03T09:00:00.000000Z
                PEHN\t-2062507031\t1970-01-03T09:00:00.000000Z
                VTJW\t-1377625589\t1970-01-03T09:00:00.000000Z
                RXGZ\t-1228519003\t1970-01-03T09:00:00.000000Z
                HYRX\t-302875424\t1970-01-03T09:00:00.000000Z
                """, "select b, min(a), k from x sample by 3h fill(linear)", "create table x as " +
                "(" +
                "select" +
                " rnd_int() a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(172800000000, 360000000) k" +
                " from" +
                " long_sequence(100)" +
                ") timestamp(k) partition by NONE", "k", "insert into x select * from (" +
                "select" +
                " rnd_int() a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(277200000000, 360000000) k" +
                " from" +
                " long_sequence(35)" +
                ") timestamp(k)", """
                b\tmin\tk
                \t-1792928964\t1970-01-03T00:00:00.000000Z
                VTJW\t-2002373666\t1970-01-03T00:00:00.000000Z
                RXGZ\t-1520872171\t1970-01-03T00:00:00.000000Z
                PEHN\t-1252906348\t1970-01-03T00:00:00.000000Z
                CPSW\t-2119387831\t1970-01-03T00:00:00.000000Z
                HYRX\t-1272693194\t1970-01-03T00:00:00.000000Z
                ZMZV\tnull\t1970-01-03T00:00:00.000000Z
                QLDG\tnull\t1970-01-03T00:00:00.000000Z
                LOGI\tnull\t1970-01-03T00:00:00.000000Z
                QEBN\tnull\t1970-01-03T00:00:00.000000Z
                FOUS\tnull\t1970-01-03T00:00:00.000000Z
                RXGZ\t-1538602195\t1970-01-03T03:00:00.000000Z
                \t-2075675260\t1970-01-03T03:00:00.000000Z
                PEHN\t-661194722\t1970-01-03T03:00:00.000000Z
                CPSW\t-882371473\t1970-01-03T03:00:00.000000Z
                HYRX\t-1593630138\t1970-01-03T03:00:00.000000Z
                VTJW\t215354468\t1970-01-03T03:00:00.000000Z
                ZMZV\tnull\t1970-01-03T03:00:00.000000Z
                QLDG\tnull\t1970-01-03T03:00:00.000000Z
                LOGI\tnull\t1970-01-03T03:00:00.000000Z
                QEBN\tnull\t1970-01-03T03:00:00.000000Z
                FOUS\tnull\t1970-01-03T03:00:00.000000Z
                CPSW\t519895483\t1970-01-03T06:00:00.000000Z
                \t-1470806499\t1970-01-03T06:00:00.000000Z
                HYRX\t-948252781\t1970-01-03T06:00:00.000000Z
                VTJW\t-246923735\t1970-01-03T06:00:00.000000Z
                PEHN\t-2080340570\t1970-01-03T06:00:00.000000Z
                RXGZ\t-1383560599\t1970-01-03T06:00:00.000000Z
                ZMZV\tnull\t1970-01-03T06:00:00.000000Z
                QLDG\tnull\t1970-01-03T06:00:00.000000Z
                LOGI\tnull\t1970-01-03T06:00:00.000000Z
                QEBN\tnull\t1970-01-03T06:00:00.000000Z
                FOUS\tnull\t1970-01-03T06:00:00.000000Z
                CPSW\t-1182156192\t1970-01-03T09:00:00.000000Z
                \t-1901633430\t1970-01-03T09:00:00.000000Z
                PEHN\t-2062507031\t1970-01-03T09:00:00.000000Z
                VTJW\t-1377625589\t1970-01-03T09:00:00.000000Z
                RXGZ\t-1228519003\t1970-01-03T09:00:00.000000Z
                HYRX\t-302875424\t1970-01-03T09:00:00.000000Z
                ZMZV\tnull\t1970-01-03T09:00:00.000000Z
                QLDG\tnull\t1970-01-03T09:00:00.000000Z
                LOGI\tnull\t1970-01-03T09:00:00.000000Z
                QEBN\tnull\t1970-01-03T09:00:00.000000Z
                FOUS\tnull\t1970-01-03T09:00:00.000000Z
                \t-1800524746\t1970-01-03T12:00:00.000000Z
                VTJW\tnull\t1970-01-03T12:00:00.000000Z
                RXGZ\t-1073477407\t1970-01-03T12:00:00.000000Z
                PEHN\t-2044673491\t1970-01-03T12:00:00.000000Z
                CPSW\tnull\t1970-01-03T12:00:00.000000Z
                HYRX\t342501933\t1970-01-03T12:00:00.000000Z
                ZMZV\tnull\t1970-01-03T12:00:00.000000Z
                QLDG\tnull\t1970-01-03T12:00:00.000000Z
                LOGI\tnull\t1970-01-03T12:00:00.000000Z
                QEBN\tnull\t1970-01-03T12:00:00.000000Z
                FOUS\tnull\t1970-01-03T12:00:00.000000Z
                \t-1699416063\t1970-01-03T15:00:00.000000Z
                VTJW\tnull\t1970-01-03T15:00:00.000000Z
                RXGZ\t-918435810\t1970-01-03T15:00:00.000000Z
                PEHN\t-2026839952\t1970-01-03T15:00:00.000000Z
                CPSW\tnull\t1970-01-03T15:00:00.000000Z
                HYRX\t987879289\t1970-01-03T15:00:00.000000Z
                ZMZV\tnull\t1970-01-03T15:00:00.000000Z
                QLDG\tnull\t1970-01-03T15:00:00.000000Z
                LOGI\tnull\t1970-01-03T15:00:00.000000Z
                QEBN\tnull\t1970-01-03T15:00:00.000000Z
                FOUS\tnull\t1970-01-03T15:00:00.000000Z
                \t-1598307380\t1970-01-03T18:00:00.000000Z
                VTJW\tnull\t1970-01-03T18:00:00.000000Z
                RXGZ\t-763394215\t1970-01-03T18:00:00.000000Z
                PEHN\t-2009006413\t1970-01-03T18:00:00.000000Z
                CPSW\tnull\t1970-01-03T18:00:00.000000Z
                HYRX\t1633256647\t1970-01-03T18:00:00.000000Z
                ZMZV\tnull\t1970-01-03T18:00:00.000000Z
                QLDG\tnull\t1970-01-03T18:00:00.000000Z
                LOGI\tnull\t1970-01-03T18:00:00.000000Z
                QEBN\tnull\t1970-01-03T18:00:00.000000Z
                FOUS\tnull\t1970-01-03T18:00:00.000000Z
                \t-1497198697\t1970-01-03T21:00:00.000000Z
                VTJW\tnull\t1970-01-03T21:00:00.000000Z
                RXGZ\t-608352619\t1970-01-03T21:00:00.000000Z
                PEHN\t-1991172874\t1970-01-03T21:00:00.000000Z
                CPSW\tnull\t1970-01-03T21:00:00.000000Z
                HYRX\t2147483647\t1970-01-03T21:00:00.000000Z
                ZMZV\tnull\t1970-01-03T21:00:00.000000Z
                QLDG\tnull\t1970-01-03T21:00:00.000000Z
                LOGI\tnull\t1970-01-03T21:00:00.000000Z
                QEBN\tnull\t1970-01-03T21:00:00.000000Z
                FOUS\tnull\t1970-01-03T21:00:00.000000Z
                \t-1396090014\t1970-01-04T00:00:00.000000Z
                VTJW\tnull\t1970-01-04T00:00:00.000000Z
                RXGZ\t-453311023\t1970-01-04T00:00:00.000000Z
                PEHN\t-1973339336\t1970-01-04T00:00:00.000000Z
                CPSW\tnull\t1970-01-04T00:00:00.000000Z
                HYRX\t2147483647\t1970-01-04T00:00:00.000000Z
                ZMZV\tnull\t1970-01-04T00:00:00.000000Z
                QLDG\tnull\t1970-01-04T00:00:00.000000Z
                LOGI\tnull\t1970-01-04T00:00:00.000000Z
                QEBN\tnull\t1970-01-04T00:00:00.000000Z
                FOUS\tnull\t1970-01-04T00:00:00.000000Z
                \t-1294981331\t1970-01-04T03:00:00.000000Z
                ZMZV\t-2043541236\t1970-01-04T03:00:00.000000Z
                VTJW\tnull\t1970-01-04T03:00:00.000000Z
                RXGZ\t-298269426\t1970-01-04T03:00:00.000000Z
                PEHN\t-1955505797\t1970-01-04T03:00:00.000000Z
                CPSW\tnull\t1970-01-04T03:00:00.000000Z
                HYRX\t2147483647\t1970-01-04T03:00:00.000000Z
                QLDG\tnull\t1970-01-04T03:00:00.000000Z
                LOGI\tnull\t1970-01-04T03:00:00.000000Z
                QEBN\tnull\t1970-01-04T03:00:00.000000Z
                FOUS\tnull\t1970-01-04T03:00:00.000000Z
                QLDG\t-1300367617\t1970-01-04T06:00:00.000000Z
                LOGI\t-358259591\t1970-01-04T06:00:00.000000Z
                QEBN\t171760612\t1970-01-04T06:00:00.000000Z
                \t-2102739504\t1970-01-04T06:00:00.000000Z
                FOUS\t-1923096605\t1970-01-04T06:00:00.000000Z
                VTJW\tnull\t1970-01-04T06:00:00.000000Z
                RXGZ\t-143227831\t1970-01-04T06:00:00.000000Z
                PEHN\t-1937672257\t1970-01-04T06:00:00.000000Z
                CPSW\tnull\t1970-01-04T06:00:00.000000Z
                HYRX\t2147483647\t1970-01-04T06:00:00.000000Z
                ZMZV\tnull\t1970-01-04T06:00:00.000000Z
                """, true, true, false);
    }

    @Test
    public void testSomeNull() throws SqlException {
        execute("create table tab (f int)");

        try (TableWriter w = getWriter("tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                if (i % 4 == 0) {
                    r.putInt(0, i);
                }
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select min(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(12, record.getInt(0));
            }
        }
    }

}
