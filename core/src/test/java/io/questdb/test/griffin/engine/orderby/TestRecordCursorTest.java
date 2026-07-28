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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.cairo.sql.Record;
import io.questdb.std.LongList;
import org.junit.Assert;
import org.junit.Test;

/**
 * The tree-chain tests drive this cursor in a single pass, so its {@code toTop()} contract had no
 * cover of its own. It is a real contract: {@code hasNext()} pre-increments, so the pre-iteration
 * position has to be -1, and the extraction that moved this class out of the tree-chain tests
 * corrected it from 0 without anything exercising the difference.
 */
public class TestRecordCursorTest {

    @Test
    public void testToTopReplaysEveryRowIncludingTheFirst() {
        final long[] values = {10, 20, 30};
        try (TestRecordCursor cursor = new TestRecordCursor(values)) {
            final Record record = cursor.getRecord();

            assertPassYields(cursor, record, values);
            cursor.toTop();
            // Resetting to 0 rather than -1 drops the first row, so the second pass has to be
            // asserted whole rather than just by length.
            assertPassYields(cursor, record, values);
        }
    }

    @Test
    public void testToTopWithoutIteratingStartsAtTheFirstRow() {
        final long[] values = {7, 8};
        try (TestRecordCursor cursor = new TestRecordCursor(values)) {
            cursor.toTop();
            assertPassYields(cursor, cursor.getRecord(), values);
        }
    }

    private static void assertPassYields(TestRecordCursor cursor, Record record, long[] expected) {
        final LongList seen = new LongList();
        while (cursor.hasNext()) {
            seen.add(record.getLong(0));
        }
        Assert.assertEquals("row count", expected.length, seen.size());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("row " + i, expected[i], seen.getQuick(i));
        }
    }
}
