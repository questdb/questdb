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
package io.questdb.test;

import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.std.MemoryTag;
import org.junit.Test;

import static io.questdb.cairo.AbstractIntervalDataFrameCursor.binarySearch;
import static io.questdb.cairo.AbstractIntervalDataFrameCursor.SCAN_UP;
import static io.questdb.cairo.AbstractIntervalDataFrameCursor.SCAN_DOWN;
import static org.junit.Assert.assertEquals;

public class AbstractIntervalDataFrameCursorTest extends AbstractCairoTest {
    // see implementation of Vect.binarySearch64Bit 
    static final int THRESHOLD = 65;

    @Test
    public void testBinarySearchOnArrayWithSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            int size = 1024 * 1024;
            int rows = (size / Long.BYTES);
            try (MemoryCARWImpl mem = new MemoryCARWImpl(size, 1, MemoryTag.NATIVE_DEFAULT)) {
                for (int i = 0; i < rows; i++) {
                    mem.putLong(0);
                }

                assertEquals(rows - 1, binarySearch(mem, 0, 0, rows - 1, SCAN_DOWN));
                assertEquals(131071, binarySearch(mem, 0, 0, rows - 1, SCAN_DOWN));

                assertEquals(-1, binarySearch(mem, -1, 0, rows - 1, SCAN_DOWN));
                assertEquals(-1, binarySearch(mem, -1, 0, rows - 1, SCAN_UP));
                assertEquals(-131073, binarySearch(mem, 1, 0, rows - 1, SCAN_DOWN));
                assertEquals(-131073, binarySearch(mem, 1, 0, rows - 1, SCAN_UP));
            }
        });
    }

    @Test
    public void testBinarySearchOnArrayWithUniqueElements() throws Exception {
        assertMemoryLeak(() -> {
            int size = 1024 * 1024;
            int rows = (size / Long.BYTES);
            try (MemoryCARWImpl mem = new MemoryCARWImpl(size, 1, MemoryTag.NATIVE_DEFAULT)) {
                for (int i = 0; i < rows; i++) {
                    mem.putLong(i);
                }

                for (long i = 0; i < rows; i++) {
                    assertEquals(i, mem.getLong(i * Long.BYTES));
                    assertEquals(i, binarySearch(mem, i, 0, rows - 1, SCAN_DOWN));
                    assertEquals(i, binarySearch(mem, i, 0, rows - 1, SCAN_UP));
                }

                assertEquals(-1, binarySearch(mem, -1, 0, rows - 1, SCAN_DOWN));
                assertEquals(-1, binarySearch(mem, -1, 0, rows - 1, SCAN_UP));
                assertEquals(-131073, binarySearch(mem, rows, 0, rows - 1, SCAN_DOWN));
                assertEquals(-131073, binarySearch(mem, rows, 0, rows - 1, SCAN_UP));
            }
        });
    }

    @Test
    public void testtestBinarySearchOnArrayWith4Duplicates() throws Exception {
        testBinarySearchOnArrayWithDuplicates(4);
    }

    @Test
    public void testtestBinarySearchOnArrayWith65Duplicates() throws Exception {
        testBinarySearchOnArrayWithDuplicates(THRESHOLD + 5);
    }

    private void testBinarySearchOnArrayWithDuplicates(int dupCount) throws Exception {
        assertMemoryLeak(() -> {
            int size = 1024 * 1024;
            int rows = (size / Long.BYTES);
            int values = rows / dupCount;
            try (MemoryCARWImpl mem = new MemoryCARWImpl(size, 1, MemoryTag.NATIVE_DEFAULT)) {
                for (int i = 0; i < rows; i++) {
                    mem.putLong(i / dupCount);
                }

                for (long i = 0; i < values; i++) {
                    long value = i / dupCount;
                    assertEquals(value, mem.getLong(i * Long.BYTES));
                    assertEquals(i - (i % dupCount) + dupCount - 1, binarySearch(mem, value, 0, rows - 1, SCAN_DOWN));
                    assertEquals(i - (i % dupCount), binarySearch(mem, value, 0, rows - 1, SCAN_UP));
                }

                assertEquals(-1, binarySearch(mem, -1, 0, rows - 1, SCAN_DOWN));
                assertEquals(-1, binarySearch(mem, -1, 0, rows - 1, SCAN_UP));
                assertEquals(-131073, binarySearch(mem, rows, 0, rows - 1, SCAN_DOWN));
                assertEquals(-131073, binarySearch(mem, rows, 0, rows - 1, SCAN_UP));
            }
        });
    }

}
