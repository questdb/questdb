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

public class LastByteGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testAllNull() throws SqlException {

        compiler.compile("create table tab (f byte)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
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
                Assert.assertEquals(0, record.getByte(0));
            }
        }
    }

    @Test
    public void testNonNull() throws SqlException {

        compiler.compile("create table tab (f byte)", sqlExecutionContext);

        final Rnd rnd = new Rnd();
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            for (int i = 100; i > 10; i--) {
                TableWriter.Row r = w.newRow();
                r.putByte(0, rnd.nextByte());
                r.append();
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select last(f) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(-48, record.getByte(0));
            }
        }
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery("b\tlast\tk\n" +
                        "\t30\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t25\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t27\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t27\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t27\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t24\t1970-01-03T03:00:00.000000Z\n" +
                        "\t10\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t24\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t24\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t30\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t20\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t22\t1970-01-03T06:00:00.000000Z\n" +
                        "\t13\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t12\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t14\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t6\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t15\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t26\t1970-01-03T09:00:00.000000Z\n" +
                        "\t24\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t28\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t29\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t6\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t-6\t1970-01-03T09:00:00.000000Z\n",
                "select b, last(a), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_byte(4,30) a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_byte(4,30) a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                "b\tlast\tk\n" +
                        "\t30\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t25\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t27\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t27\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t27\t1970-01-03T00:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t24\t1970-01-03T03:00:00.000000Z\n" +
                        "\t10\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t24\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t24\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t30\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t20\t1970-01-03T03:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t22\t1970-01-03T06:00:00.000000Z\n" +
                        "\t13\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t12\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t14\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t6\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t15\t1970-01-03T06:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t26\t1970-01-03T09:00:00.000000Z\n" +
                        "\t24\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t28\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t29\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t6\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t-6\t1970-01-03T09:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\t22\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t44\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t-3\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t50\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t30\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t-24\t1970-01-03T12:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "\t20\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t59\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t-12\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t72\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t34\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t-42\t1970-01-03T15:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "\t18\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t74\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t-21\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t94\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t38\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t-60\t1970-01-03T18:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "\t16\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t89\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t-30\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t116\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t42\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t-78\t1970-01-03T21:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "\t14\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t104\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t-39\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-118\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t46\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t-96\t1970-01-04T00:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "\t12\t1970-01-04T03:00:00.000000Z\n" +
                        "ZMZV\t25\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t119\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t-48\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-96\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t50\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t-114\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "LOGI\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "QEBN\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "FOUS\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\t14\t1970-01-04T06:00:00.000000Z\n" +
                        "LOGI\t6\t1970-01-04T06:00:00.000000Z\n" +
                        "QEBN\t14\t1970-01-04T06:00:00.000000Z\n" +
                        "\t8\t1970-01-04T06:00:00.000000Z\n" +
                        "FOUS\t17\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t-122\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t-57\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t-74\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t54\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t124\t1970-01-04T06:00:00.000000Z\n" +
                        "ZMZV\t0\t1970-01-04T06:00:00.000000Z\n",
                true,
                true,
                true
        );
    }
}