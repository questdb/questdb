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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.table.WriterPoolFunctionFactory;
import io.questdb.griffin.engine.table.WriterPoolRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class WriterPoolTableFunctionTest extends AbstractCairoTest {

    @Test
    public void testCursorDoesHaveUpfrontSize() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    RecordCursorFactory factory = select("select * from writer_pool()");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertEquals(-1, cursor.size());
            }
        });
    }

    @Test
    public void testCursorNotRuntimeConstant() throws Exception {
        assertMemoryLeak(() -> {
            try (Function cursorFunction = new WriterPoolFunctionFactory().newInstance(0, new ObjList<>(), new IntList(), configuration, sqlExecutionContext)) {
                Assert.assertFalse(cursorFunction.isRuntimeConstant());
            }
        });
    }

    @Test
    public void testEmptyPool() throws Exception {
        assertQuery(
                "table_name\towner_thread_id\tlast_access_timestamp\townership_reason\n",
                "select * from writer_pool()",
                null
        );
    }

    @Test
    public void testFactoryDoesNotSupportRandomAccess() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select("select * from writer_pool()")) {
                Assert.assertFalse(factory.recordCursorSupportsRandomAccess());
            }
        });
    }

    @Test
    public void testMetadata() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select("select * from writer_pool()")) {
                RecordMetadata metadata = factory.getMetadata();
                Assert.assertEquals(4, metadata.getColumnCount());
                Assert.assertEquals("table_name", metadata.getColumnName(0));
                Assert.assertEquals("owner_thread_id", metadata.getColumnName(1));
                Assert.assertEquals("last_access_timestamp", metadata.getColumnName(2));
                Assert.assertEquals("ownership_reason", metadata.getColumnName(3));
                Assert.assertEquals(ColumnType.STRING, metadata.getColumnType(0));
                Assert.assertEquals(ColumnType.LONG, metadata.getColumnType(1));
                Assert.assertEquals(ColumnType.TIMESTAMP, metadata.getColumnType(2));
                Assert.assertEquals(ColumnType.STRING, metadata.getColumnType(3));
            }
        });
    }

    @Test
    public void testRandomAccessUnsupported() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    RecordCursorFactory writerPoolFactory = new WriterPoolRecordCursorFactory(sqlExecutionContext.getCairoEngine());
                    RecordCursor writerPoolCursor = writerPoolFactory.getCursor(sqlExecutionContext)
            ) {
                Record record = writerPoolCursor.getRecord();
                writerPoolCursor.recordAt(record, 0);
                Assert.fail("Random access is not expected to be implemented");
            } catch (UnsupportedOperationException ignored) {
            }
        });
    }

    @Test
    public void testRecordBNotImplemented() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    RecordCursorFactory writerPoolFactory = new WriterPoolRecordCursorFactory(sqlExecutionContext.getCairoEngine());
                    RecordCursor writerPoolCursor = writerPoolFactory.getCursor(sqlExecutionContext)
            ) {
                writerPoolCursor.getRecordB();
                Assert.fail("RecordB is not expected to be implemented");
            } catch (UnsupportedOperationException ignored) {
            }
        });
    }

    @Test
    public void testWriterList() throws Exception {
        currentMicros = MicrosFormatUtils.parseTimestamp("2024-10-24T17:22:09.842574Z");
        assertMemoryLeak(() -> {
            execute("create table a as (select 1 as u)");

            assertQueryNoLeakCheck(
                    "table_name\towner_thread_id\tlast_access_timestamp\townership_reason\n" +
                            "a\tnull\t2024-10-24T17:22:09.842574Z\t\n",
                    "writer_pool",
                    null,
                    false
            );

            execute("create table b as (select 1 as u)");
            execute("create table c as (select 1 as u)");
            execute("create table d as (select 1 as u)");

            assertQueryNoLeakCheck(
                    "table_name\townership_reason\n" +
                            "a\t\n" +
                            "b\t\n" +
                            "c\t\n" +
                            "d\t\n",
                    "select table_name,ownership_reason from writer_pool order by 1",
                    null,
                    true
            );

            // assert ordering
            assertQueryNoLeakCheck(
                    "table_name\townership_reason\n" +
                            "d\t\n" +
                            "c\t\n" +
                            "b\t\n" +
                            "a\t\n",
                    "select table_name,ownership_reason from writer_pool order by 1 desc",
                    null,
                    true
            );

            engine.releaseAllWriters();

            assertQueryNoLeakCheck(
                    "table_name\townership_reason\n",
                    "select table_name,ownership_reason from writer_pool order by 1",
                    null,
                    true
            );

            assertQueryNoLeakCheck(
                    "table_name\townership_reason\n",
                    "select table_name,ownership_reason from writer_pool order by 1 desc",
                    null,
                    true
            );

            try (TableWriter ignored = engine.getWriter(engine.getTableTokenIfExists("a"), "test reason")) {
                assertQueryNoLeakCheck(
                        "table_name\townership_reason\n" +
                                "a\ttest reason\n",
                        "select table_name,ownership_reason from writer_pool order by 1 desc",
                        null,
                        true
                );
            }

            assertQueryNoLeakCheck(
                    "table_name\townership_reason\n" +
                            "a\t\n",
                    "select table_name,ownership_reason from writer_pool order by 1 desc",
                    null,
                    true
            );
        });
    }
}
