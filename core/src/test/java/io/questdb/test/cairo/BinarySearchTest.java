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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.std.Vect.BIN_SEARCH_SCAN_DOWN;
import static io.questdb.std.Vect.BIN_SEARCH_SCAN_UP;
import static org.junit.Assert.assertEquals;

public class BinarySearchTest extends AbstractCairoTest {
    // see implementation of Vect.binarySearch64Bit
    private static final int THRESHOLD = 65;

    @Test
    public void testBinarySearchOnArrayWith4Duplicates() throws Exception {
        testBinarySearchOnArrayWithDuplicates(4);
    }

    @Test
    public void testBinarySearchOnArrayWith65Duplicates() throws Exception {
        testBinarySearchOnArrayWithDuplicates(THRESHOLD + 5);
    }

    @Test
    public void testBinarySearchOnArrayWithSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            int size = 1024 * 1024;
            int rows = (size / Long.BYTES);
            try (MemoryCARWImpl mem = new MemoryCARWImpl(size, 1, MemoryTag.NATIVE_DEFAULT)) {
                for (int i = 0; i < rows; i++) {
                    mem.putLong(0);
                }

                assertEquals(rows - 1, binarySearch(mem, 0, 0, rows - 1, BIN_SEARCH_SCAN_DOWN));
                assertEquals(131071, binarySearch(mem, 0, 0, rows - 1, BIN_SEARCH_SCAN_DOWN));

                assertEquals(-1, binarySearch(mem, -1, 0, rows - 1, BIN_SEARCH_SCAN_DOWN));
                assertEquals(-1, binarySearch(mem, -1, 0, rows - 1, BIN_SEARCH_SCAN_UP));
                assertEquals(-131073, binarySearch(mem, 1, 0, rows - 1, BIN_SEARCH_SCAN_DOWN));
                assertEquals(-131073, binarySearch(mem, 1, 0, rows - 1, BIN_SEARCH_SCAN_UP));
            }
        });
    }

    @Test
    public void testBinarySearchEqualScanDown() throws Exception {
        assertMemoryLeak(() -> {
            int rows = 63;
            int size = rows * Long.BYTES;
            try (MemoryCARWImpl mem = new MemoryCARWImpl(size, 1, MemoryTag.NATIVE_DEFAULT)) {
                for (int i = 0; i < rows; i++) {
                    mem.putLong(123);
                }

                assertEquals(rows - 1, binarySearch(mem, 123, 0, rows - 1, BIN_SEARCH_SCAN_DOWN));
            }
        });
    }

    @Test
    public void testBinarySearchEqualScanUpWithOffset() throws Exception {
        assertMemoryLeak(() -> {
            int rows = 120;
            int size = rows * Long.BYTES;
            try (MemoryCARWImpl mem = new MemoryCARWImpl(size, 1, MemoryTag.NATIVE_DEFAULT)) {
                for (int i = 0; i < rows; i++) {
                    mem.putLong(0);
                }

                long memAddress = mem.getPageAddress(0);
                long shift = memAddress / Long.BYTES;

                long index = Vect.binarySearch64Bit(memAddress - shift * Long.BYTES, 0, shift, shift + rows - 1, BIN_SEARCH_SCAN_UP);
                Assert.assertEquals(shift, index);
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
                    assertEquals(i, binarySearch(mem, i, 0, rows - 1, BIN_SEARCH_SCAN_DOWN));
                    assertEquals(i, binarySearch(mem, i, 0, rows - 1, BIN_SEARCH_SCAN_UP));
                }

                assertEquals(-1, binarySearch(mem, -1, 0, rows - 1, BIN_SEARCH_SCAN_DOWN));
                assertEquals(-1, binarySearch(mem, -1, 0, rows - 1, BIN_SEARCH_SCAN_UP));
                assertEquals(-131073, binarySearch(mem, rows, 0, rows - 1, BIN_SEARCH_SCAN_DOWN));
                assertEquals(-131073, binarySearch(mem, rows, 0, rows - 1, BIN_SEARCH_SCAN_UP));
            }
        });
    }

    @Test
    public void testFindForward1() throws Exception {
        testColumnFindForward(1, 24, 113, BIN_SEARCH_SCAN_DOWN);
    }

    @Test
    public void testFindForward1Even() throws Exception {
        testColumnFindForward(1, 20, 21, BIN_SEARCH_SCAN_DOWN);
    }

    @Test
    public void testFindForward1Odd() throws Exception {
        testColumnFindForward(1, 21, 21, BIN_SEARCH_SCAN_DOWN);
    }

    @Test
    public void testFindForward2() throws Exception {
        testColumnFindForward(2, 140, 141, BIN_SEARCH_SCAN_DOWN);
    }

    @Test
    public void testFindForward3() throws Exception {
        testColumnFindForward(3, 20, 100, BIN_SEARCH_SCAN_DOWN);
    }

    @Test
    public void testFindForwardBeforeRange() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                path.of(root).concat("binsearch.d").$();
                try (
                        MemoryCMARW appendMem = Vm.getSmallCMARWInstance(
                                TestFilesFacadeImpl.INSTANCE,
                                path.$(),
                                MemoryTag.MMAP_DEFAULT,
                                CairoConfiguration.O_NONE
                        )
                ) {
                    for (int i = 0; i < 100; i++) {
                        for (int j = 0; j < 3; j++) {
                            appendMem.putLong(i);
                        }
                    }

                    long max = 100 * 3 - 1;
                    try (MemoryMR mem = Vm.getCMRInstance(TestFilesFacadeImpl.INSTANCE, path.$(), 400 * Long.BYTES, MemoryTag.MMAP_DEFAULT)) {
                        long index = binarySearch(mem, -20, 0, max, BIN_SEARCH_SCAN_DOWN);
                        Assert.assertEquals(-1, index);
                    }
                }
            }
        });
    }

    @Test
    public void testFindForwardTwoValues() {
        try (Path path = new Path()) {
            path.of(root).concat("binsearch.d").$();
            try (
                    MemoryMA appendMem = Vm.getSmallCMARWInstance(
                            TestFilesFacadeImpl.INSTANCE,
                            path.$(),
                            MemoryTag.MMAP_DEFAULT,
                            CairoConfiguration.O_NONE
                    )
            ) {
                appendMem.putLong(1);
                appendMem.putLong(3);

                try (MemoryMR mem = Vm.getCMRInstance(TestFilesFacadeImpl.INSTANCE, path.$(), 400 * Long.BYTES, MemoryTag.MMAP_DEFAULT)) {
                    Assert.assertEquals(0, binarySearch(mem, 1, 0, 1, BIN_SEARCH_SCAN_DOWN));
                    Assert.assertEquals(1, binarySearch(mem, 3, 0, 1, BIN_SEARCH_SCAN_DOWN));
                    Assert.assertEquals(-2, binarySearch(mem, 2, 0, 1, BIN_SEARCH_SCAN_DOWN));
                }
            }
        }
    }

    @Test
    public void testFindReverse1() throws Exception {
        testColumnFindForward(1, 24, 113, BIN_SEARCH_SCAN_UP);
    }

    @Test
    public void testFindReverse1Even() throws Exception {
        testColumnFindForward(1, 20, 21, BIN_SEARCH_SCAN_UP);
    }

    @Test
    public void testFindReverse1Odd() throws Exception {
        testColumnFindForward(1, 21, 21, BIN_SEARCH_SCAN_UP);
    }

    @Test
    public void testFindReverse2() throws Exception {
        testColumnFindForward(2, 140, 141, BIN_SEARCH_SCAN_UP);
    }

    @Test
    public void testFindReverse3() throws Exception {
        testColumnFindForward(3, 20, 100, BIN_SEARCH_SCAN_UP);
    }

    @Test
    public void testFindReverseTwoValues() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                path.of(root).concat("binsearch.d").$();
                try (
                        MemoryMA appendMem = Vm.getSmallCMARWInstance(
                                TestFilesFacadeImpl.INSTANCE,
                                path.$(),
                                MemoryTag.MMAP_DEFAULT,
                                CairoConfiguration.O_NONE
                        )
                ) {
                    appendMem.putLong(1);
                    appendMem.putLong(3);

                    try (MemoryMR mem = Vm.getCMRInstance(TestFilesFacadeImpl.INSTANCE, path.$(), 400 * Long.BYTES, MemoryTag.MMAP_DEFAULT)) {
                        Assert.assertEquals(0, binarySearch(mem, 1, 0, 1, BIN_SEARCH_SCAN_UP));
                        Assert.assertEquals(1, binarySearch(mem, 3, 0, 1, BIN_SEARCH_SCAN_UP));
                        Assert.assertEquals(-2, binarySearch(mem, 2, 0, 1, BIN_SEARCH_SCAN_UP));
                    }
                }
            }
        });
    }

    @Test
    public void testMem256FindForward1() {
        testMem256Find(1, 24, 113, BIN_SEARCH_SCAN_DOWN);
    }

    @Test
    public void testMem256FindForward1Even() {
        testMem256Find(1, 20, 21, BIN_SEARCH_SCAN_DOWN);
    }

    @Test
    public void testMem256FindForward1Odd() {
        testMem256Find(1, 21, 21, BIN_SEARCH_SCAN_DOWN);
    }

    @Test
    public void testMem256FindForward2() {
        testMem256Find(2, 140, 141, BIN_SEARCH_SCAN_DOWN);
    }

    @Test
    public void testMem256FindForward3() {
        testMem256Find(3, 20, 100, BIN_SEARCH_SCAN_DOWN);
    }

    @Test
    public void testMem256FindReverse1() {
        testMem256Find(1, 24, 113, BIN_SEARCH_SCAN_UP);
    }

    @Test
    public void testMem256FindReverse1Even() {
        testMem256Find(1, 20, 21, BIN_SEARCH_SCAN_UP);
    }

    @Test
    public void testMem256FindReverse1Odd() {
        testMem256Find(1, 21, 21, BIN_SEARCH_SCAN_UP);
    }

    @Test
    public void testMem256FindReverse2() {
        testMem256Find(2, 140, 141, BIN_SEARCH_SCAN_UP);
    }

    @Test
    public void testMem256FindReverse3() {
        testMem256Find(3, 20, 100, BIN_SEARCH_SCAN_UP);
    }

    private static long binarySearch(MemoryR column, long value, long low, long high, int scanDir) {
        return Vect.binarySearch64Bit(column.getPageAddress(0), value, low, high, scanDir);
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
                    assertEquals(i - (i % dupCount) + dupCount - 1, binarySearch(mem, value, 0, rows - 1, BIN_SEARCH_SCAN_DOWN));
                    assertEquals(i - (i % dupCount), binarySearch(mem, value, 0, rows - 1, BIN_SEARCH_SCAN_UP));
                }

                assertEquals(-1, binarySearch(mem, -1, 0, rows - 1, BIN_SEARCH_SCAN_DOWN));
                assertEquals(-1, binarySearch(mem, -1, 0, rows - 1, BIN_SEARCH_SCAN_UP));
                assertEquals(-131073, binarySearch(mem, rows, 0, rows - 1, BIN_SEARCH_SCAN_DOWN));
                assertEquals(-131073, binarySearch(mem, rows, 0, rows - 1, BIN_SEARCH_SCAN_UP));
            }
        });
    }

    private void testColumnFindForward(
            long repeatCount,
            long searchValue,
            int distinctValueCount,
            int scanDirection
    ) throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                path.of(root).concat("binsearch.d").$();
                try (
                        MemoryA appendMem = Vm.getSmallCMARWInstance(
                                TestFilesFacadeImpl.INSTANCE,
                                path.$(),
                                MemoryTag.MMAP_DEFAULT,
                                CairoConfiguration.O_NONE
                        )
                ) {
                    for (int i = 0; i < distinctValueCount; i++) {
                        for (int j = 0; j < repeatCount; j++) {
                            appendMem.putLong(i);
                        }
                    }

                    long max = distinctValueCount * repeatCount - 1;
                    try (MemoryMR mem = Vm.getCMRInstance(TestFilesFacadeImpl.INSTANCE, path.$(), 400 * Long.BYTES, MemoryTag.MMAP_DEFAULT)) {
                        long index = binarySearch(mem, searchValue, 0, max, scanDirection);
                        if (searchValue > distinctValueCount - 1) {
                            Assert.assertEquals(-max - 2, index);
                        } else {
                            Assert.assertEquals(searchValue, mem.getLong(index * Long.BYTES));
                            if (scanDirection == BIN_SEARCH_SCAN_DOWN) {
                                Assert.assertTrue(index == max || searchValue < mem.getLong((index + 1) * Long.BYTES));
                            } else {
                                Assert.assertTrue(index == 0 || searchValue > mem.getLong((index - 1) * Long.BYTES));
                            }
                        }
                    }
                }
            }
        });
    }

    private void testMem256Find(long repeatCount, long searchValue, int distinctValueCount, int scanDirection) {
        long size = distinctValueCount * repeatCount * 16L;
        long mem = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < distinctValueCount; i++) {
                for (int j = 0; j < repeatCount; j++) {
                    Unsafe.getUnsafe().putLong(mem + (i * repeatCount + j) * 16, i);
                }
            }

            long max = distinctValueCount * repeatCount - 1;
            long index = Vect.boundedBinarySearchIndexT(mem, searchValue, 0, max, scanDirection);
            if (searchValue > distinctValueCount - 1) {
                Assert.assertEquals(max, index);
            } else {
                Assert.assertEquals(searchValue, Unsafe.getUnsafe().getLong(mem + index * 16));
                if (scanDirection == BIN_SEARCH_SCAN_DOWN) {
                    Assert.assertTrue(index == max || searchValue < Unsafe.getUnsafe().getLong(mem + (index + 1) * 16));
                } else {
                    Assert.assertTrue(index == 0 || searchValue > Unsafe.getUnsafe().getLong(mem + (index - 1) * 16));
                }
            }
        } finally {
            Unsafe.free(mem, size, MemoryTag.NATIVE_DEFAULT);
        }
    }
}