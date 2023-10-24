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
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

public class LastNotNullGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNull() throws Exception {

        assertQuery("a0\ta1\ta2\ta3\ta4\ta5\ta6\ta7\ta8\ta9\n" +
                        "\t\tNaN\tNaN\tNaN\tNaN\t\t\t\t\n",
                "select last_not_null(a0)a0,last_not_null(a1)a1,last_not_null(a2)a2,last_not_null(a3)a3,last_not_null(a4)a4," +
                        "last_not_null(a5)a5,last_not_null(a6)a6,last_not_null(a7)a7,last_not_null(a8)a8,last_not_null(a9)a9 from tab",
                "create table tab as (select cast(null as char)a0,cast(null as date)a1,cast(null as double)a2,cast(null as float)a3,cast(null as int)a4,\n" +
                        "cast(null as long)a5,cast(null as symbol)a6,cast(null as timestamp)a7,cast(null as uuid)a8,cast(null as string)a9\n" +
                        "from long_sequence(3))",
                null,
                false,
                true
        );
    }

    @Test
    public void testLastNotNull() throws Exception {

        ddl("create table tab (a0 char,a1 date,a2 double,a3 float,a4 int,a5 long,a6 symbol,a7 timestamp,a8 uuid,a9 string)");

        long now = Instant.now().truncatedTo(ChronoUnit.DAYS).getEpochSecond() * 1_000_000;
        UUID firstUuid = UUID.randomUUID();
        try (TableWriter w = getWriter("tab")) {
            TableWriter.Row r = w.newRow();
            r.putChar(0, 'b');
            r.putDate(1, now + 1000);
            r.putDouble(2, 22.2);
            r.putFloat(3, 33.3f);
            r.putInt(4, 44);
            r.putLong(5, 55L);
            r.putSym(6, "b_symbol");
            r.putTimestamp(7, now + 1700);
            r.putUuid(8, UUID.randomUUID().toString());
            r.putStr(9, "b_string");
            r.append();

            r = w.newRow();
            r.putChar(0, 'a');
            r.putDate(1, now);
            r.putDouble(2, 2.2);
            r.putFloat(3, 3.3f);
            r.putInt(4, 4);
            r.putLong(5, 5L);
            r.putSym(6, "a_symbol");
            r.putTimestamp(7, now + 700);
            r.putUuid(8, firstUuid.toString());
            r.putStr(9, "a_string");
            r.append();

            w.commit();

            for (int i = 10; i > 0; i--) {
                TableWriter.Row rr = w.newRow();
                rr.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select(
                "select last_not_null(a0)a0,last_not_null(a1)a1,last_not_null(a2)a2,last_not_null(a3)a3,last_not_null(a4)a4," +
                        "last_not_null(a5)a5,last_not_null(a6)a6,last_not_null(a7)a7,last_not_null(a8)a8,last_not_null(a9)a9 from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());

                Assert.assertEquals('a', record.getChar(0));
                Assert.assertEquals(now, record.getDate(1));
                Assert.assertEquals(2.2, record.getDouble(2), 0.001);
                Assert.assertEquals(3.3, record.getFloat(3), 0.001);
                Assert.assertEquals(4, record.getInt(4));
                Assert.assertEquals(5, record.getLong(5));
                Assert.assertEquals("a_symbol", record.getSym(6));
                Assert.assertEquals(now + 700, record.getTimestamp(7));
                Assert.assertEquals(firstUuid, new UUID(record.getLong128Hi(8), record.getLong128Lo(8)));
                Assert.assertEquals("a_string", record.getStr(9).toString());
            }
        }
    }
}
