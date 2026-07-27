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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.LongTreeChain;
import io.questdb.std.LongList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LongTreeChainTest extends AbstractCairoTest {

    @Test
    public void testKeyHeapClampsToMaxHeapSize() throws Exception {
        // A 200-byte key budget is not a power of two, while every doubling step is. The tree
        // goes 64 -> 128 and then wants 256; rejecting there stranded a quarter of the budget
        // at 5 blocks. Clamping to 200 fits 8 of the 24-byte blocks instead.
        assertMemoryLeak(() -> {
            try (
                    LongTreeChain chain = new LongTreeChain(
                            64,             // key page >= BLOCK_SIZE
                            200,            // key heap budget, deliberately not a power of two
                            128 * 1024,
                            Long.MAX_VALUE, // value heap uncapped
                            PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                            PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath()
                    )
            ) {
                Assert.assertEquals(8, fillUntilOverflow(chain, "memory exceeded in RedBlackTree"));
            }
        });
    }

    @Test
    public void testValueHeapClampsToMaxHeapSize() throws Exception {
        // Same clamp on the value heap: a 96-byte budget is not a power of two, the chain goes
        // 16 -> 32 -> 64 and then wants 128. Clamping to 96 fits 8 of the 12-byte chain values
        // instead of the 5 that fitted before.
        assertMemoryLeak(() -> {
            try (
                    LongTreeChain chain = new LongTreeChain(
                            64,
                            Long.MAX_VALUE, // key heap uncapped
                            16,             // value page >= CHAIN_VALUE_SIZE
                            96,             // value heap budget, deliberately not a power of two
                            PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                            PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath()
                    )
            ) {
                Assert.assertEquals(8, fillUntilOverflow(chain, "memory exceeded in LongTreeChain"));
            }
        });
    }

    /**
     * Inserts distinct ascending values until one of the heaps runs out, and returns how many
     * of them the chain accepted. Fails when no overflow happens at all.
     */
    private int fillUntilOverflow(LongTreeChain chain, String expectedMessage) {
        final long[] values = new long[256];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }
        final TestRecordCursor cursor = new TestRecordCursor(values);
        final Record left = cursor.getRecord();
        final Record placeholder = cursor.getRecordB();
        final RecordComparator comparator = new TestRecordComparator();

        int inserted = 0;
        while (cursor.hasNext()) {
            comparator.setLeft(left);
            try {
                chain.put(left, cursor, placeholder, comparator);
            } catch (LimitOverflowException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
                return inserted;
            }
            inserted++;
        }
        Assert.fail("expected LimitOverflowException");
        return -1;
    }

    private static class TestRecord implements Record {
        long position;
        long value;

        @Override
        public long getLong(int col) {
            return value;
        }

        @Override
        public long getRowId() {
            return position;
        }
    }

    private static class TestRecordComparator implements RecordComparator {
        Record left;

        @Override
        public int compare(Record record) {
            return Long.compare(left.getLong(0), record.getLong(0));
        }

        @Override
        public void setLeft(Record record) {
            left = record;
        }
    }

    private static class TestRecordCursor implements RecordCursor {
        final Record left = new TestRecord();
        final Record right = new TestRecord();
        final LongList values = new LongList();
        int position = -1;

        TestRecordCursor(long... newValues) {
            for (int i = 0; i < newValues.length; i++) {
                values.add(newValues[i]);
            }
        }

        @Override
        public void close() {
            // nothing to do here
        }

        @Override
        public Record getRecord() {
            return left;
        }

        @Override
        public Record getRecordB() {
            return right;
        }

        @Override
        public boolean hasNext() {
            if (position < values.size() - 1) {
                position++;
                recordAt(left, position);
                return true;
            }
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((TestRecord) record).value = values.get((int) atRowId);
            ((TestRecord) record).position = atRowId;
        }

        @Override
        public long size() {
            return values.size();
        }

        @Override
        public void toTop() {
            position = 0;
        }
    }
}
