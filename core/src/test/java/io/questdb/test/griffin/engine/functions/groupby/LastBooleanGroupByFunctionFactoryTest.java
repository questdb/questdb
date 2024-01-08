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
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class LastBooleanGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNull() throws SqlException {
        ddl("create table tab (f boolean)");

        try (TableWriter w = getWriter("tab")) {
            for (int i = 10; i > 0; i--) {
                TableWriter.Row r = w.newRow();
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = select("select last(f) from tab")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertFalse(record.getBool(0));
            }
        }
    }

    @Test
    public void testLastBoolean() throws Exception {
        assertQuery(
                "a\n" +
                        "false\n",
                "select last(a)a from tab",
                "create table tab as (select true a union select true a union select false a)",
                null,
                false,
                true
        );
    }

    @Test
    public void testLastBoolean2() throws Exception {
        assertQuery(
                "a\n" +
                        "true\n",
                "select last(a)a from tab",
                "create table tab as (select false a union select false a union select true a)",
                null,
                false,
                true
        );
    }
}
