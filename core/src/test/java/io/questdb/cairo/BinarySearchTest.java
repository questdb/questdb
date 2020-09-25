/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

public class BinarySearchTest extends AbstractCairoTest {

    @Test
    public void testFindForward1() {
        testColumnFindForward(1, 24, 113, BinarySearch.SCAN_DOWN);
    }

    @Test
    public void testFindForward1Even() {
        testColumnFindForward(1, 20, 21, BinarySearch.SCAN_DOWN);
    }

    @Test
    public void testFindForward1Odd() {
        testColumnFindForward(1, 21, 21, BinarySearch.SCAN_DOWN);
    }

    @Test
    public void testFindForward2() {
        testColumnFindForward(2, 140, 141, BinarySearch.SCAN_DOWN);
    }

    @Test
    public void testFindForward3() {
        testColumnFindForward(3, 20, 100, BinarySearch.SCAN_DOWN);
    }

    @Test
    public void testFindForwardBeforeRange() {
        try (Path path = new Path()) {
            path.of(root).concat("binsearch.d").$();
            try (AppendMemory appendMem = new AppendMemory(FilesFacadeImpl.INSTANCE, path, 4096)) {
                for (int i = 0; i < 100; i++) {
                    for (int j = 0; j < 3; j++) {
                        appendMem.putLong(i);
                    }
                }

                long max = 100 * 3 - 1;
                try (OnePageMemory mem = new OnePageMemory(FilesFacadeImpl.INSTANCE, path, 400 * Long.BYTES)) {
                    long index = BinarySearch.find(mem, -20, 0, max, BinarySearch.SCAN_DOWN);
                    Assert.assertEquals(-1, index);
                }
            }
        }
    }

    @Test
    public void testFindForwardTwoValues() {
        try (Path path = new Path()) {
            path.of(root).concat("binsearch.d").$();
            try (AppendMemory appendMem = new AppendMemory(FilesFacadeImpl.INSTANCE, path, 4096)) {
                appendMem.putLong(1);
                appendMem.putLong(3);

                try (OnePageMemory mem = new OnePageMemory(FilesFacadeImpl.INSTANCE, path, 400 * Long.BYTES)) {
                    Assert.assertEquals(0, BinarySearch.find(mem, 2, 0, 1, BinarySearch.SCAN_DOWN));
                }
            }
        }
    }

    @Test
    public void testFindReverse1() {
        testColumnFindForward(1, 24, 113, BinarySearch.SCAN_UP);
    }

    @Test
    public void testFindReverse1Even() {
        testColumnFindForward(1, 20, 21, BinarySearch.SCAN_UP);
    }

    @Test
    public void testFindReverse1Odd() {
        testColumnFindForward(1, 21, 21, BinarySearch.SCAN_UP);
    }

    @Test
    public void testFindReverse2() {
        testColumnFindForward(2, 140, 141, BinarySearch.SCAN_UP);
    }

    @Test
    public void testFindReverse3() {
        testColumnFindForward(3, 20, 100, BinarySearch.SCAN_UP);
    }

    @Test
    public void testFindReverseTwoValues() {
        try (Path path = new Path()) {
            path.of(root).concat("binsearch.d").$();
            try (AppendMemory appendMem = new AppendMemory(FilesFacadeImpl.INSTANCE, path, 4096)) {
                appendMem.putLong(1);
                appendMem.putLong(3);

                try (OnePageMemory mem = new OnePageMemory(FilesFacadeImpl.INSTANCE, path, 400 * Long.BYTES)) {
                    Assert.assertEquals(0, BinarySearch.find(mem, 2, 0, 1, BinarySearch.SCAN_UP));
                }
            }
        }
    }

    @Test
    public void testMem256FindForward1() {
        testMem256Find(1, 24, 113, BinarySearch.SCAN_DOWN);
    }

    @Test
    public void testMem256FindForward1Even() {
        testMem256Find(1, 20, 21, BinarySearch.SCAN_DOWN);
    }

    @Test
    public void testMem256FindForward1Odd() {
        testMem256Find(1, 21, 21, BinarySearch.SCAN_DOWN);
    }

    @Test
    public void testMem256FindForward2() {
        testMem256Find(2, 140, 141, BinarySearch.SCAN_DOWN);
    }

    @Test
    public void testMem256FindForward3() {
        testMem256Find(3, 20, 100, BinarySearch.SCAN_DOWN);
    }

    @Test
    public void testMem256FindReverse1() {
        testMem256Find(1, 24, 113, BinarySearch.SCAN_UP);
    }

    @Test
    public void testMem256FindReverse1Even() {
        testMem256Find(1, 20, 21, BinarySearch.SCAN_UP);
    }

    @Test
    public void testMem256FindReverse1Odd() {
        testMem256Find(1, 21, 21, BinarySearch.SCAN_UP);
    }

    @Test
    public void testMem256FindReverse2() {
        testMem256Find(2, 140, 141, BinarySearch.SCAN_UP);
    }

    @Test
    public void testMem256FindReverse3() {
        testMem256Find(3, 20, 100, BinarySearch.SCAN_UP);
    }

    private void testColumnFindForward(int repeatCount, long searchValue, int distinctValueCount, int scanDirection) {
        try (Path path = new Path()) {
            path.of(root).concat("binsearch.d").$();
            try (AppendMemory appendMem = new AppendMemory(FilesFacadeImpl.INSTANCE, path, 4096)) {
                for (int i = 0; i < distinctValueCount; i++) {
                    for (int j = 0; j < repeatCount; j++) {
                        appendMem.putLong(i);
                    }
                }

                long max = distinctValueCount * repeatCount - 1;
                try (OnePageMemory mem = new OnePageMemory(FilesFacadeImpl.INSTANCE, path, 400 * Long.BYTES)) {
                    long index = BinarySearch.find(mem, searchValue, 0, max, scanDirection);
                    if (searchValue > distinctValueCount - 1) {
                        Assert.assertEquals(max, index);
                    } else {
                        Assert.assertEquals(searchValue, mem.getLong(index * Long.BYTES));
                        if (scanDirection == BinarySearch.SCAN_DOWN) {
                            Assert.assertTrue(index == max || searchValue < mem.getLong((index + 1) * Long.BYTES));
                        } else {
                            Assert.assertTrue(index == 0 || searchValue > mem.getLong((index - 1) * Long.BYTES));
                        }
                    }
                }
            }
        }
    }

    private void testMem256Find(int repeatCount, long searchValue, int distinctValueCount, int scanDirection) {
        long size = distinctValueCount * repeatCount * 16;
        long mem = Unsafe.malloc(size);
        try {
            for (int i = 0; i < distinctValueCount; i++) {
                for (int j = 0; j < repeatCount; j++) {
                    Unsafe.getUnsafe().putLong(mem + (i * repeatCount + j) * 16, i);
                }
            }

            long max = distinctValueCount * repeatCount - 1;
            long index = TableWriter.searchIndex(mem, searchValue, 0, max, scanDirection);
            if (searchValue > distinctValueCount - 1) {
                Assert.assertEquals(max, index);
            } else {
                Assert.assertEquals(searchValue, Unsafe.getUnsafe().getLong(mem + index * 16));
                if (scanDirection == BinarySearch.SCAN_DOWN) {
                    Assert.assertTrue(index == max || searchValue < Unsafe.getUnsafe().getLong(mem + (index + 1) * 16));
                } else {
                    Assert.assertTrue(index == 0 || searchValue > Unsafe.getUnsafe().getLong(mem + (index - 1) * 16));
                }
            }
        } finally {
            Unsafe.free(mem, size);
        }
    }
}