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

package io.questdb.test.cairo.map;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.map.FastMap;
import io.questdb.cairo.map.SpillableHeapMemory;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TestRecord;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.map.SpillableHeapMemory;
import io.questdb.test.griffin.engine.TestBinarySequence;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.Assert.*;

public class SpillableHeapMemoryTest extends AbstractCairoTest {
    private static FilesFacade ff = new FilesFacadeImpl();
    private static Path spillPath = Path.getThreadLocal(root).concat("fastmap.tmp").$();

    @Test
    public void testJumpTo() {
        int n = 1000;
        long diskSpillThreshold = Long.BYTES * 512;
        try (SpillableHeapMemory mem = new SpillableHeapMemory(11,
                diskSpillThreshold,
                CairoConfiguration.DEFAULT_FAST_MAP_EXTEND_SEGMENT_SIZE,
                FastMap.MAX_HEAP_SIZE,
                spillPath)) {
            assertNotSpilled(mem);
            mem.putByte((byte) 1);
            long curSize = 1;
            for (int i = n; i > 0; i--) {
                mem.putLong(i);
                curSize += 8;
                if (curSize > diskSpillThreshold) {
                    assertSpilled(mem);
                } else {
                    assertNotSpilled(mem);
                }
            }

            assertEquals(1, mem.getByte(0));

            mem.jumpTo(1);
            for (int i = n; i > 0; i--) {
                mem.putLong(n - i);
                assertWhetherSpilled(mem, diskSpillThreshold);
            }

            long o = 1;
            for (int i = n; i > 0; i--) {
                assertEquals(n - i, mem.getLong(o));
                o += 8;
            }

            assertSpilled(mem);
        }
    }

    @Test
    public void testJumpTo2() {
        int n = 1000;
        long diskSpillThreshold = Long.BYTES * 512;
        try (SpillableHeapMemory mem = new SpillableHeapMemory(11,
                diskSpillThreshold,
                CairoConfiguration.DEFAULT_FAST_MAP_EXTEND_SEGMENT_SIZE,
                FastMap.MAX_HEAP_SIZE,
                spillPath)) {
            assertNotSpilled(mem);
            mem.jumpTo(8);
            long curSize = 8;
            for (int i = n; i > 0; i--) {
                mem.putLong(i);
                curSize += 8;
                if (curSize > diskSpillThreshold) {
                    assertSpilled(mem);
                } else {
                    assertNotSpilled(mem);
                }
            }
            long o = 8;
            for (int i = n; i > 0; i--) {
                assertEquals(i, mem.getLong(o));
                assertWhetherSpilled(mem, diskSpillThreshold);
                o += 8;
            }

            assertSpilled(mem);
        }
    }

    @Test
    public void testJumpTo3() {
        int n = 999;
        long diskSpillThreshold = 128;
        try (SpillableHeapMemory mem = new SpillableHeapMemory(11,
                diskSpillThreshold,
                CairoConfiguration.DEFAULT_FAST_MAP_EXTEND_SEGMENT_SIZE,
                FastMap.MAX_HEAP_SIZE,
                spillPath)) {
            assertNotSpilled(mem);
            mem.jumpTo(256);
            assertSpilled(mem);
            for (int i = n; i > 0; i--) {
                mem.putLong(i);
                assertSpilled(mem);
            }
            long o = 256;
            for (int i = n; i > 0; i--) {
                assertEquals(i, mem.getLong(o));
                o += 8;
            }

            mem.jumpTo(0);
            mem.jumpTo(5);
            mem.jumpTo(0);
            for (int i = n; i > 0; i--) {
                mem.putLong(i);
                assertSpilled(mem);
            }

            o = 0;
            for (int i = n; i > 0; i--) {
                assertEquals(i, mem.getLong(o));
                o += 8;
            }

            assertSpilled(mem);
        }
    }


    @Test
    public void testSkip() {
        int pageSize = 256;
        long extendSegmentSize = 4 * 1024;
        long spillThreshold = extendSegmentSize;
        long maxSizes = extendSegmentSize * 32;
        try (SpillableHeapMemory mem = new SpillableHeapMemory(pageSize,
                spillThreshold,
                extendSegmentSize,
                maxSizes,
                spillPath)) {
            mem.putByte((byte) 1);
            long curSize = 1;
            int n = 999;
            for (int i = n; i > 0; i--) {
                mem.putLong(i);
                mem.skip(3);
                curSize += 8 + 3;
                if (curSize > spillThreshold) {
                    assertSpilled(mem);
                } else {
                    assertNotSpilled(mem);
                }
            }
            assertSpilled(mem);

            long o = 1;
            assertEquals(1, mem.getByte(0));

            for (int i = n; i > 0; i--) {
                assertEquals(i, mem.getLong(o));
                o += 11;
            }
            assertEquals(curSize, mem.getAppendOffset());
        }
    }

    @Test
    public void testStableResize() {
        int pageSize = 1024;
        long extendSegmentSize = CairoConfiguration.DEFAULT_FAST_MAP_EXTEND_SEGMENT_SIZE;
        long maxSizes = extendSegmentSize * 4;
        try (SpillableHeapMemory mem = new SpillableHeapMemory(pageSize,
                CairoConfiguration.DEFAULT_FAST_MAP_EXTEND_SEGMENT_SIZE,
                extendSegmentSize,
                maxSizes,
                spillPath)) {
            Assert.assertEquals(0, mem.size());

            mem.resize(pageSize);
            Assert.assertEquals(pageSize, mem.size());
            assertNotSpilled(mem);

            mem.resize(pageSize * 2);
            Assert.assertEquals(pageSize * 2, mem.size());
            assertNotSpilled(mem);

            mem.resize(extendSegmentSize);
            Assert.assertEquals(extendSegmentSize, mem.size());
            assertNotSpilled(mem);

            mem.resize(extendSegmentSize + 1);
            Assert.assertEquals(extendSegmentSize * 2, mem.size());
            assertSpilled(mem);

            // for MemoryCMARWImpl, it will extend by adding one more segment.
            mem.resize(extendSegmentSize * 3);
            Assert.assertEquals(extendSegmentSize * 3, mem.size());
            assertSpilled(mem);

            mem.resize(maxSizes);
            Assert.assertEquals(maxSizes, mem.size());
            assertSpilled(mem);

        }
    }

    @Test
    public void testTruncate() {
        {
            int pageSize = 256;
            long extendSegmentSize = 16 * 1024;
            long spillThreshold = extendSegmentSize;
            long maxSize = extendSegmentSize * 2;
            long writeUntil, expectedFinalSize;
            Consumer<SpillableHeapMemory> expect;

            // test truncate when not spilled
            writeUntil = pageSize * 2;
            expectedFinalSize = pageSize * 2;
            expect = SpillableHeapMemoryTest::assertNotSpilled;
            testTruncateUtils(pageSize, extendSegmentSize, spillThreshold, maxSize, writeUntil, expectedFinalSize, expect);

            // also test truncate when not spilled
            writeUntil = spillThreshold;
            expectedFinalSize = spillThreshold;
            expect = SpillableHeapMemoryTest::assertNotSpilled;
            testTruncateUtils(pageSize, extendSegmentSize, spillThreshold, maxSize, writeUntil, expectedFinalSize, expect);

            // test truncate when spilled
            writeUntil = spillThreshold + 1;
            expectedFinalSize = spillThreshold * 2;
            expect = SpillableHeapMemoryTest::assertSpilled;
            testTruncateUtils(pageSize, extendSegmentSize, spillThreshold, maxSize, writeUntil, expectedFinalSize, expect);

            // also test truncate when spilled
            writeUntil = maxSize;
            expectedFinalSize = maxSize;
            expect = SpillableHeapMemoryTest::assertSpilled;
            testTruncateUtils(pageSize, extendSegmentSize, spillThreshold, maxSize, writeUntil, expectedFinalSize, expect);
        }


    }

    @Test
    public void testWrite() {
        {
            // case possibly spill.
            // spillThreshold == extendSegmentSize
            int pageSize = 256;
            long extendSegmentSize = 16 * 1024;
            long spillThreshold = extendSegmentSize;
            long maxSize = extendSegmentSize * 2;
            long writeUntil, expectedFinalSize;
            Consumer<SpillableHeapMemory> expect;

            writeUntil = pageSize;
            expectedFinalSize = pageSize;
            expect = SpillableHeapMemoryTest::assertNotSpilled;
            testWriteUtils(pageSize, extendSegmentSize, spillThreshold, maxSize, writeUntil, expectedFinalSize, expect);

            writeUntil = pageSize - 1;
            expectedFinalSize = pageSize;
            expect = SpillableHeapMemoryTest::assertNotSpilled;
            testWriteUtils(pageSize, extendSegmentSize, spillThreshold, maxSize, writeUntil, expectedFinalSize, expect);

            writeUntil = pageSize + 1;
            expectedFinalSize = pageSize * 2;
            expect = SpillableHeapMemoryTest::assertNotSpilled;
            testWriteUtils(pageSize, extendSegmentSize, spillThreshold, maxSize, writeUntil, expectedFinalSize, expect);

            writeUntil = spillThreshold;
            expectedFinalSize = spillThreshold;
            expect = SpillableHeapMemoryTest::assertNotSpilled;
            testWriteUtils(pageSize, extendSegmentSize, spillThreshold, maxSize, writeUntil, expectedFinalSize, expect);

            writeUntil = spillThreshold + 1;
            expectedFinalSize = spillThreshold * 2;
            expect = SpillableHeapMemoryTest::assertSpilled;
            testWriteUtils(pageSize, extendSegmentSize, spillThreshold, maxSize, writeUntil, expectedFinalSize, expect);

            writeUntil = spillThreshold - 1;
            expectedFinalSize = spillThreshold;
            expect = SpillableHeapMemoryTest::assertNotSpilled;
            testWriteUtils(pageSize, extendSegmentSize, spillThreshold, maxSize, writeUntil, expectedFinalSize, expect);

            writeUntil = maxSize;
            expectedFinalSize = maxSize;
            expect = SpillableHeapMemoryTest::assertSpilled;
            testWriteUtils(pageSize, extendSegmentSize, spillThreshold, maxSize, writeUntil, expectedFinalSize, expect);

            writeUntil = maxSize - 1;
            expectedFinalSize = maxSize;
            expect = SpillableHeapMemoryTest::assertSpilled;
            testWriteUtils(pageSize, extendSegmentSize, spillThreshold, maxSize, writeUntil, expectedFinalSize, expect);

            Assert.assertThrows(LimitOverflowException.class, () ->
                    testWriteUtils(pageSize, extendSegmentSize, spillThreshold, maxSize, maxSize + 1, 0, SpillableHeapMemoryTest::assertSpilled));

        }

        {
            // case never spilled
            int pageSize = 256;
            long maxSize = 16 * pageSize;
            long writeUntil, expectedFinalSize;
            Consumer<SpillableHeapMemory> expect;

            writeUntil = pageSize;
            expectedFinalSize = pageSize;
            expect = SpillableHeapMemoryTest::assertNotSpilled;
            testWriteUnspillableUtils(pageSize, maxSize, writeUntil, expectedFinalSize, expect);

            writeUntil = pageSize - 1;
            expectedFinalSize = pageSize;
            expect = SpillableHeapMemoryTest::assertNotSpilled;
            testWriteUnspillableUtils(pageSize, maxSize, writeUntil, expectedFinalSize, expect);

            writeUntil = pageSize + 1;
            expectedFinalSize = pageSize * 2;
            expect = SpillableHeapMemoryTest::assertNotSpilled;
            testWriteUnspillableUtils(pageSize, maxSize, writeUntil, expectedFinalSize, expect);

            writeUntil = maxSize;
            expectedFinalSize = maxSize;
            expect = SpillableHeapMemoryTest::assertNotSpilled;
            testWriteUnspillableUtils(pageSize, maxSize, writeUntil, expectedFinalSize, expect);

            writeUntil = maxSize - 1;
            expectedFinalSize = maxSize;
            expect = SpillableHeapMemoryTest::assertNotSpilled;
            testWriteUnspillableUtils(pageSize, maxSize, writeUntil, expectedFinalSize, expect);

            Assert.assertThrows(LimitOverflowException.class, () ->
                    testWriteUnspillableUtils(pageSize, maxSize, maxSize + 1, 0, SpillableHeapMemoryTest::assertSpilled));

        }
    }

    private static void assertNotSpilled(SpillableHeapMemory mem) {
        Assert.assertFalse(mem.isSpilled());
        Assert.assertFalse(ff.exists(spillPath));
    }

    private static void assertSpilled(SpillableHeapMemory mem) {
        Assert.assertTrue(mem.isSpilled());
        Assert.assertTrue(ff.exists(spillPath));
    }

    private static void assertWhetherSpilled(SpillableHeapMemory mem, long diskSpillThreshold) {
        diskSpillThreshold = Numbers.ceilPow2(diskSpillThreshold);
        if (mem.size() > diskSpillThreshold) {
            assertSpilled(mem);
        } else {
            assertNotSpilled(mem);
        }
    }

    private void testTruncateUtils(int pageSize,
                                   long extendSegmentSize,
                                   long spillThreshold,
                                   long maxSize,
                                   long writeSize,
                                   long expectedFinalSize,
                                   Consumer<SpillableHeapMemory> expect) {
        try (SpillableHeapMemory mem = new SpillableHeapMemory(pageSize,
                spillThreshold,
                extendSegmentSize,
                maxSize,
                spillPath
        )) {
            for (int i = 0; i < writeSize; i++) {
                mem.putByte(i, (byte) i);
            }
            Assert.assertEquals(expectedFinalSize, mem.size());
            expect.accept(mem);

            // truncate
            mem.truncate();
            Assert.assertEquals(pageSize, mem.size());
            Assert.assertEquals(0, mem.getAppendOffset());

            // should return to non-spilled mem after truncate
            assertNotSpilled(mem);
        }
    }

    private void testWriteUnspillableUtils(int pageSize, long maxSize, long writeSize, long expectedFinalSize, Consumer<SpillableHeapMemory> expect) {
        try (SpillableHeapMemory mem = new SpillableHeapMemory(pageSize, maxSize, maxSize, maxSize, null)) {
            for (long i = 0; i < writeSize; i++) {
                mem.putByte(i, (byte) i);
            }
            Assert.assertEquals(expectedFinalSize, mem.size());
            expect.accept(mem);
        }
    }

    private void testWriteUtils(int pageSize,
                                long extendSegmentSize,
                                long spillThreshold,
                                long maxSize,
                                long writeSize,
                                long expectedFinalSize,
                                Consumer<SpillableHeapMemory> expect) {
        try (SpillableHeapMemory mem = new SpillableHeapMemory(pageSize,
                spillThreshold,
                extendSegmentSize,
                maxSize,
                spillPath
        )) {
            for (long i = 0; i < writeSize; i++) {
                mem.putByte(i, (byte) i);
            }
            Assert.assertEquals(expectedFinalSize, mem.size());
            expect.accept(mem);
        }
    }

}
