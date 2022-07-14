/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Test;

import static org.junit.Assert.*;

public class MemoryMetricsRecordCursorFactoryTest extends AbstractGriffinTest {

    @Test
    public void testCursorHasOneRow() throws Exception {
        try (MemoryMetricsRecordCursorFactory factory = new MemoryMetricsRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {

            assertTrue(cursor.hasNext());
            assertFalse(cursor.hasNext());
        }
    }

    @Test
    public void testMetadata() throws Exception {
        try (MemoryMetricsRecordCursorFactory factory = new MemoryMetricsRecordCursorFactory()) {
            assertMetadata(factory.getMetadata());
        }
    }

    @Test
    public void testValues() throws Exception {
        try (MemoryMetricsRecordCursorFactory factory = new MemoryMetricsRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            assertCursor(cursor);
        }
    }

    @Test
    public void testSql() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = compiler.compile("select * from memory_metrics()", sqlExecutionContext).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                RecordMetadata metadata = factory.getMetadata();
                assertMetadata(metadata);
                assertCursor(cursor);
            }
        });
    }

    private static void assertCursor(RecordCursor cursor) {
        cursor.hasNext();
        Record record = cursor.getRecord();
        assertEquals(Unsafe.getMemUsed(), record.getLong(0));
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            assertEquals(Unsafe.getMemUsedByTag(i), record.getLong(i + 1));
        }
    }

    private static void assertMetadata(RecordMetadata metadata) {
        // chances are these tests fails during an innocent change. for example when you add a new memory tag.
        // this is intentional, it's meant to prevent accidentally removing or renaming a tag
        // why? by exposing memory utilization tags via SQL the tag names have effectively become an API and we should
        // treat them as such. in fact this was already the case when we exposed them via Prometheus.
        // if you just intentionally add/change a tag then just change the test and carry on
        int columnCount = metadata.getColumnCount();
        assertEquals(34, columnCount);
        for (int i = 0; i < columnCount; i++) {
            assertEquals(ColumnType.LONG, metadata.getColumnType(i));
        }
        assertEquals("TOTAL_USED", metadata.getColumnName(0));
        assertEquals("MMAP_DEFAULT", metadata.getColumnName(1));
        assertEquals("NATIVE_DEFAULT", metadata.getColumnName(2));
        assertEquals("MMAP_O3", metadata.getColumnName(3));
        assertEquals("NATIVE_O3", metadata.getColumnName(4));
        assertEquals("NATIVE_RECORD_CHAIN", metadata.getColumnName(5));
        assertEquals("MMAP_TABLE_WRITER", metadata.getColumnName(6));
        assertEquals("NATIVE_TREE_CHAIN", metadata.getColumnName(7));
        assertEquals("MMAP_TABLE_READER", metadata.getColumnName(8));
        assertEquals("NATIVE_COMPACT_MAP", metadata.getColumnName(9));
        assertEquals("NATIVE_FAST_MAP", metadata.getColumnName(10));
        assertEquals("NATIVE_FAST_MAP_LONG_LIST", metadata.getColumnName(11));
        assertEquals("NATIVE_HTTP_CONN", metadata.getColumnName(12));
        assertEquals("NATIVE_PGW_CONN", metadata.getColumnName(13));
        assertEquals("MMAP_INDEX_READER", metadata.getColumnName(14));
        assertEquals("MMAP_INDEX_WRITER", metadata.getColumnName(15));
        assertEquals("MMAP_INDEX_SLIDER", metadata.getColumnName(16));
        assertEquals("MMAP_BLOCK_WRITER", metadata.getColumnName(17));
        assertEquals("NATIVE_REPL", metadata.getColumnName(18));
        assertEquals("NATIVE_SAMPLE_BY_LONG_LIST", metadata.getColumnName(19));
        assertEquals("NATIVE_LATEST_BY_LONG_LIST", metadata.getColumnName(20));
        assertEquals("NATIVE_JIT_LONG_LIST", metadata.getColumnName(21));
        assertEquals("NATIVE_LONG_LIST", metadata.getColumnName(22));
        assertEquals("NATIVE_JIT", metadata.getColumnName(23));
        assertEquals("NATIVE_OFFLOAD", metadata.getColumnName(24));
        assertEquals("NATIVE_PATH", metadata.getColumnName(25));
        assertEquals("NATIVE_TABLE_READER", metadata.getColumnName(26));
        assertEquals("NATIVE_TABLE_WRITER", metadata.getColumnName(27));
        assertEquals("MMAP_UPDATE", metadata.getColumnName(28));
        assertEquals("NATIVE_CB1", metadata.getColumnName(29));
        assertEquals("NATIVE_CB2", metadata.getColumnName(30));
        assertEquals("NATIVE_CB3", metadata.getColumnName(31));
        assertEquals("NATIVE_CB4", metadata.getColumnName(32));
        assertEquals("NATIVE_CB5", metadata.getColumnName(33));
    }
}
