/*+*****************************************************************************
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

package io.questdb.test.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitRecordCursorFactory;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class LimitRecordCursorFactoryTest extends AbstractCairoTest {

    @Test
    public void testNegativeLimitBeyondBufferLimitTraversesUnknownSizeCursorTwice() throws Exception {
        assertMemoryLeak(() -> {
            final CountingRandomAccessCursor baseCursor = new CountingRandomAccessCursor(10);
            try (
                    LimitRecordCursorFactory factory = new LimitRecordCursorFactory(
                            new CountingRandomAccessCursorFactory(baseCursor),
                            new LongConstant(-2),
                            null,
                            0,
                            1
                    );
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                final Record record = cursor.getRecord();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(8, record.getLong(0));
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(9, record.getLong(0));
                Assert.assertFalse(cursor.hasNext());
                Assert.assertEquals(21, baseCursor.getHasNextCount());
            }
        });
    }

    @Test
    public void testNegativeLimitTraversesUnknownSizeCursorOnce() throws Exception {
        assertMemoryLeak(() -> {
            final CountingRandomAccessCursor baseCursor = new CountingRandomAccessCursor(10);
            try (
                    LimitRecordCursorFactory factory = new LimitRecordCursorFactory(
                            new CountingRandomAccessCursorFactory(baseCursor),
                            new LongConstant(-2),
                            null,
                            0,
                            10
                    );
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                final Record record = cursor.getRecord();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(8, record.getLong(0));
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(9, record.getLong(0));
                Assert.assertFalse(cursor.hasNext());
                Assert.assertEquals(11, baseCursor.getHasNextCount());

                cursor.toTop();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(8, record.getLong(0));
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(9, record.getLong(0));
                Assert.assertFalse(cursor.hasNext());
                Assert.assertEquals(11, baseCursor.getHasNextCount());
            }
        });
    }

    private static class CountingRandomAccessCursor implements RecordCursor {
        private final TestRecord recordA = new TestRecord();
        private final TestRecord recordB = new TestRecord();
        private final int rowCount;
        private int hasNextCount;
        private int rowIndex;

        private CountingRandomAccessCursor(int rowCount) {
            this.rowCount = rowCount;
        }

        @Override
        public void close() {
        }

        public int getHasNextCount() {
            return hasNextCount;
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public Record getRecordB() {
            return recordB;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            hasNextCount++;
            if (rowIndex < rowCount) {
                recordA.of(rowIndex++);
                return true;
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((TestRecord) record).of(atRowId);
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            rowIndex = 0;
        }
    }

    private static class CountingRandomAccessCursorFactory extends AbstractRecordCursorFactory {
        private final CountingRandomAccessCursor cursor;

        private CountingRandomAccessCursorFactory(CountingRandomAccessCursor cursor) {
            super(null);
            this.cursor = cursor;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }

        @Override
        protected void _close() {
            cursor.close();
        }
    }

    private static class TestRecord implements Record {
        private long rowId;

        @Override
        public long getLong(int col) {
            return rowId;
        }

        @Override
        public long getRowId() {
            return rowId;
        }

        private void of(long rowId) {
            this.rowId = rowId;
        }
    }
}
