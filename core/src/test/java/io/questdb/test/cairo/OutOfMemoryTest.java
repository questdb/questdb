/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.fail;

public class OutOfMemoryTest extends AbstractCairoTest {

    @After
    public void tearDown() throws Exception {
        Unsafe.setRssMemLimit(0);
        super.tearDown();
    }

    @Test
    public void testCreateAtomicTable() throws Exception {
        long limitMB = 12;
        Unsafe.setRssMemLimit(limitMB * 1_000_000);
        assertMemoryLeak(() -> {
            try {
                ddl("create atomic table x as (select" +
                        " rnd_timestamp(to_timestamp('2024-03-01', 'yyyy-mm-dd'), to_timestamp('2024-04-01', 'yyyy-mm-dd'), 0) ts" +
                        " from long_sequence(10000000)) timestamp(ts) partition by day;");
                fail("Managed to create table with RSS limit " + limitMB + " MB");
            } catch (SqlException e) {
                drop("drop table x;");
            }
        });
    }

    @Test
    public void testGroupBy() throws Exception {
        long limitMB = 10;
        Unsafe.setRssMemLimit(limitMB * 1_000_000);
        assertMemoryLeak(() -> {
            try {
                ddl("create table x as (select" +
                        " rnd_varchar(10, 1000, 0) val," +
                        " rnd_long() num," +
                        " from long_sequence(500000));");
                try (RecordCursorFactory fac = select("select val, max(num) from x group by val")) {
                    RecordCursor cursor = fac.getCursor(sqlExecutionContext);
                    while (cursor.hasNext()) {
                        cursor.getRecord();
                    }
                }
                fail("Query completed with RSS limit " + limitMB + " MB");
            } catch (CairoException e) {
                // success
            }
        });
    }

    @Test
    public void testOrderBy() throws Exception {
        long limitMB = 10;
        Unsafe.setRssMemLimit(limitMB * 1_000_000);
        assertMemoryLeak(() -> {
            try {
                ddl("create table x as (select rnd_varchar(10, 1000, 0) val, from long_sequence(200000));");
                select("x order by val").close();
                fail("Query completed with RSS limit " + limitMB + " MB");
            } catch (CairoException e) {
                // success
            }
        });
    }
}
