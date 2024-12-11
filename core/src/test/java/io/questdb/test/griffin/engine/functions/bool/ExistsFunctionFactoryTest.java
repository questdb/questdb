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

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ExistsFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testFactoryReuse() throws Exception {
        assertMemoryLeak(() -> {
            engine.execute("create table x as (select * from long_sequence(1))");
            try (SqlCompiler sqlCompiler = engine.getSqlCompiler()) {
                CompiledQuery cq = sqlCompiler.compile("select exists(select * from x)", sqlExecutionContext);
                try (RecordCursorFactory recordCursorFactory = cq.getRecordCursorFactory()) {
                    for (int i = 0; i < 10; i++) {
                        try (RecordCursor cursor = recordCursorFactory.getCursor(sqlExecutionContext)) {
                            for (int j = 0; j < 10; j++) {
                                Assert.assertTrue(cursor.hasNext());
                                Record record = cursor.getRecord();
                                boolean bool = record.getBool(0);
                                Assert.assertTrue(bool);
                                cursor.toTop();
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testHappyPath() throws Exception {
        assertMemoryLeak(() -> {
            assertSql("exists\ntrue\n", "select exists(select * from long_sequence(1))");
            assertSql("exists\nfalse\n", "select exists(select * from long_sequence(0))");
        });
    }

    @Test
    public void testInnerDoesNotExist() throws Exception {
        assertException("select exists(select * from x)", 28, "table does not exist [table=x]");
    }

    @Test
    public void testInnerDropped() throws Exception {
        assertMemoryLeak(() -> {
            engine.execute("create table x as (select * from long_sequence(1))");
            try (SqlCompiler sqlCompiler = engine.getSqlCompiler()) {
                CompiledQuery cq = sqlCompiler.compile("select exists(select * from x)", sqlExecutionContext);
                try (RecordCursorFactory recordCursorFactory = cq.getRecordCursorFactory()) {

                    // table dropped only after obtaining cursor -> the cursor should still work
                    try (RecordCursor cursor = recordCursorFactory.getCursor(sqlExecutionContext)) {
                        engine.execute("drop table x");

                        Assert.assertTrue(cursor.hasNext());
                        Record record = cursor.getRecord();
                        boolean bool = record.getBool(0);
                        Assert.assertTrue(bool);
                    }

                    // now the table is dropped this should fail during cursor acquisition
                    try (RecordCursor cursor = recordCursorFactory.getCursor(sqlExecutionContext)) {
                        Assert.fail("expected exception");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getMessage(), "table does not exist [table=x]");
                    }
                }
            }
        });
    }
}
