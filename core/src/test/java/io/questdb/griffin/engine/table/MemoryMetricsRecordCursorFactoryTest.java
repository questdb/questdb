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
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Test;

import static org.junit.Assert.*;

public class MemoryMetricsRecordCursorFactoryTest extends AbstractGriffinTest {

    @Test
    public void testMetadata() {
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
        printSqlResult(MemoryMetricsRecordCursorFactoryTest::expectedTableContent, "select * from memory_metrics()", null, null, null, false, true, true, false, null);
    }

    private static String expectedTableContent() {
        StringBuilder sb = new StringBuilder("memory_tag").append('\t').append("bytes").append('\n');
        sb.append("TOTAL_USED").append('\t').append(Unsafe.getMemUsed()).append('\n');
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            sb.append(MemoryTag.nameOf(i)).append('\t').append(Unsafe.getMemUsedByTag(i)).append('\n');
        }
        return sb.toString();
    }

    private static void assertCursor(RecordCursor cursor) {
        cursor.hasNext();
        Record record = cursor.getRecord();

        assertEquals("TOTAL_USED", record.getStr(0));
        assertEquals(Unsafe.getMemUsed(), record.getLong(1));
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            assertTrue(cursor.hasNext());
            assertEquals(MemoryTag.nameOf(i), record.getStr(0));
            assertEquals(Unsafe.getMemUsedByTag(i), record.getLong(1));
        }
        assertFalse(cursor.hasNext());
    }

    private static void assertMetadata(RecordMetadata metadata) {
        int columnCount = metadata.getColumnCount();
        assertEquals(2, columnCount);
        assertEquals(ColumnType.STRING, metadata.getColumnType(0));
        assertEquals("memory_tag", metadata.getColumnName(0));

        assertEquals(ColumnType.LONG, metadata.getColumnType(1));
        assertEquals("bytes", metadata.getColumnName(1));
    }
}
