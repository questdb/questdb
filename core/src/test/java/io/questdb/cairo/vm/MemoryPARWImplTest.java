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

package io.questdb.cairo.vm;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TestRecord;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class MemoryPARWImplTest {

    @BeforeClass
    public static void setUpClass() {
        LogFactory.getLog(MemoryPARWImplTest.class);
    }

    @Test
    public void testBinSequence() {
        MemoryCARWImplTest.testBinSequence0(700, 2048);
    }

    @Test
    public void testBinSequence2() {
        MemoryCARWImplTest.testBinSequence0(1024, 600);
    }

    @Test
    public void testBinSequenceOnEdge() {
        final Rnd rnd = new Rnd();
        try (MemoryPARWImpl mem = new MemoryPARWImpl(32, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            TestRecord.ArrayBinarySequence seq = new TestRecord.ArrayBinarySequence();
            int N = 33;
            int O = 10;

            seq.of(rnd.nextBytes(N));
            mem.putBin(seq);

            BinarySequence actual = mem.getBin(0);
            assertNotNull(actual);

            TestUtils.assertEquals(seq, actual, N);

            long buffer = Unsafe.malloc(1024, MemoryTag.NATIVE_DEFAULT);
            try {
                // supply length of our buffer
                // blob content would be shorter
                Vect.memset(buffer, 1024, 5);
                actual.copyTo(buffer, 0, 1024);

                for (int i = 0; i < N; i++) {
                    assertEquals(seq.byteAt(i), Unsafe.getUnsafe().getByte(buffer + i));
                }

                // rest of the buffer must not be overwritten
                for (int i = N; i < 1024; i++) {
                    assertEquals(5, Unsafe.getUnsafe().getByte(buffer + i));
                }

                // copy from middle
                Vect.memset(buffer, 1024, 5);
                actual.copyTo(buffer, O, 1024);

                for (int i = 0; i < N - O; i++) {
                    assertEquals(seq.byteAt(i + O), Unsafe.getUnsafe().getByte(buffer + i));
                }

                // rest of the buffer must not be overwritten
                for (int i = N - O; i < 1024; i++) {
                    assertEquals(5, Unsafe.getUnsafe().getByte(buffer + i));
                }
            } finally {
                Unsafe.free(buffer, 1024, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testBool() {
        Rnd rnd = new Rnd();
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            int n = 120;

            for (int i = 0; i < n; i++) {
                mem.putBool(rnd.nextBoolean());
            }

            long o = 0;
            rnd.reset();
            for (int i = 0; i < n; i++) {
                assertEquals(rnd.nextBoolean(), mem.getBool(o++));
            }
        }
    }

    @Test
    public void testBoolRnd() {
        Rnd rnd = new Rnd();
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            int n = 120;
            long o = 0;

            for (int i = 0; i < n; i++) {
                mem.putBool(o++, rnd.nextBoolean());
            }

            o = 0;
            rnd.reset();
            for (int i = 0; i < n; i++) {
                assertEquals(rnd.nextBoolean(), mem.getBool(o++));
            }
        }
    }

    @Test
    public void testBulkCopy() {
        int N = 1000;
        try (MemoryPARWImpl mem = new MemoryPARWImpl(128, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            for (int i = 0; i < N; i++) {
                mem.putShort((short) i);
            }

            long target = N * 2;
            long offset = 0;
            short i = 0;
            while (target > 0) {
                long len = mem.pageRemaining(offset);
                target -= len;
                long address = mem.addressOf(offset);
                offset += len;
                while (len > 0 & i < N) {
                    assertEquals(i++, Unsafe.getUnsafe().getShort(address));
                    address += 2;
                    len -= 2;
                }
            }
        }
    }

    @Test
    public void testByte() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            int n = 120;

            for (int i = 0; i < n; i++) {
                mem.putByte((byte) i);
            }

            long o = 0;
            for (int i = 0; i < n; i++) {
                assertEquals(i, mem.getByte(o++));
            }
        }
    }

    @Test
    public void testByteRandom() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(128, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            long offset1 = 512;
            mem.putByte(offset1, (byte) 3);
            mem.putByte(offset1 + 1, (byte) 4);
            mem.jumpTo(offset1 + 2);
            mem.putByte((byte) 5);
            assertEquals(3, mem.getByte(offset1));
            assertEquals(4, mem.getByte(offset1 + 1));
            assertEquals(5, mem.getByte(offset1 + 2));
        }
    }

    @Test
    public void testByteRnd() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            int n = 120;

            long o = 0;
            for (int i = 0; i < n; i++, o++) {
                mem.putByte(o, (byte) i);
            }

            o = 0;
            for (int i = 0; i < n; i++) {
                assertEquals(i, mem.getByte(o++));
            }
        }
    }

    @Test
    public void testChar() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(7, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            char n = 999;
            long o = 0;
            for (char i = n; i > 0; i--) {
                mem.putChar(i);
                o += 2;
                Assert.assertEquals(o, mem.getAppendOffset());
            }

            o = 0;
            for (char i = n; i > 0; i--) {
                assertEquals(i, mem.getChar(o));
                o += 2;
            }
        }
    }

    @Test
    public void testCharWithOffset() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(7, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            char n = 999;
            long o = 0;
            for (char i = n; i > 0; i--) {
                mem.putChar(o, i);
                o += 2;
            }

            o = 0;
            for (char i = n; i > 0; i--) {
                assertEquals(i, mem.getChar(o));
                o += 2;
            }
        }
    }

    @Test
    public void testDouble() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            Rnd rnd = new Rnd();
            int n = 999;

            mem.putByte((byte) 1);

            for (int i = 0; i < n; i++) {
                mem.putDouble(rnd.nextDouble());
            }

            assertEquals(7993, mem.getAppendOffset());

            rnd.reset();
            long o = 1;
            assertEquals(1, mem.getByte(0));
            for (int i = 0; i < n; i++) {
                assertEquals(rnd.nextDouble(), mem.getDouble(o), 0.00001);
                o += 8;
            }
        }
    }

    @Test
    public void testDoubleCompatibility() {
        long pageSize = 64;
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putInt(10);
            mem.putDouble(8980980284.22234);
            mem.putDoubleBytes(8979283749.72983477);
            assertEquals(8980980284.22234, mem.getDoubleBytes(0, 4, pageSize), 0.00001);
            assertEquals(8979283749.72983477, mem.getDouble(12), 0.00001);
        }
    }

    @Test
    public void testDoubleRnd() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            Rnd rnd = new Rnd();
            int n = 999;

            long o = 1;
            mem.putByte((byte) 1);

            for (int i = 0; i < n; i++) {
                mem.putDouble(o, rnd.nextDouble());
                o += 8;
            }

            rnd.reset();
            o = 1;
            assertEquals(1, mem.getByte(0));
            for (int i = 0; i < n; i++) {
                assertEquals(rnd.nextDouble(), mem.getDouble(o), 0.00001);
                o += 8;
            }
        }
    }

    @Test
    public void testDoubleRndCompatibility() {
        long pageSize = 64;
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            // prime
            mem.putInt(10, 900);
            mem.putDouble(22, 8980980284.22234);
            mem.putDoubleBytes(84, 8979283749.72983477);
            assertEquals(8980980284.22234, mem.getDoubleBytes(0, 22, pageSize), 0.00001);
            assertEquals(8979283749.72983477, mem.getDouble(84), 0.00001);
        }
    }

    @Test
    public void testEvenPageSize() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(32, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            MemoryCARWImplTest.assertStrings(mem);
        }
    }

    @Test
    public void testExtendDoesNotMoveAppendPosition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long pageSize = 128;
            try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                Assert.assertEquals(0, mem.getAppendOffset());
                Assert.assertEquals(0, mem.getPageCount());

                mem.extend(pageSize * 5);
                Assert.assertEquals(0, mem.getAppendOffset());
                Assert.assertEquals(5, mem.getPageCount());
            }
        });
    }

    @Test
    public void testFloat() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            Rnd rnd = new Rnd();
            int n = 999;

            mem.putByte((byte) 1);

            for (int i = 0; i < n; i++) {
                mem.putFloat(rnd.nextFloat());
            }

            rnd.reset();
            long o = 1;
            assertEquals(1, mem.getByte(0));
            for (int i = 0; i < n; i++) {
                assertEquals(rnd.nextFloat(), mem.getFloat(o), 0.00001f);
                o += 4;
            }
        }
    }

    @Test
    public void testFloatCompatibility() {
        long pageSize = 64;
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putFloat(1024f);
            mem.putFloatBytes(2048f);
            assertEquals(1024f, mem.getFloatBytes(0, 0), 0.00001f);
            assertEquals(2048f, mem.getFloat(4), 0.0001f);
        }
    }

    @Test
    public void testFloatRnd() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            Rnd rnd = new Rnd();
            int n = 999;

            long o = 1;
            mem.putByte((byte) 1);

            for (int i = 0; i < n; i++) {
                mem.putFloat(o, rnd.nextFloat());
                o += 4;
            }

            rnd.reset();
            o = 1;
            assertEquals(1, mem.getByte(0));
            for (int i = 0; i < n; i++) {
                assertEquals(rnd.nextFloat(), mem.getFloat(o), 0.00001f);
                o += 4;
            }
        }
    }

    @Test
    public void testFloatRndCompatibility() {
        long pageSize = 64;
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            // prime
            mem.putByte(10, (byte) 5);
            mem.putFloat(61, 1024f);
            mem.putFloatBytes(99, 2048f);
            assertEquals(1024f, mem.getFloatBytes(0, 61), 0.00001f);
            assertEquals(2048f, mem.getFloat(99), 0.0001f);
        }
    }

    @Test
    public void testInt() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(7, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putByte((byte) 1);
            int n = 999;
            for (int i = n; i > 0; i--) {
                mem.putInt(i);
            }

            long o = 1;
            assertEquals(1, mem.getByte(0));

            for (int i = n; i > 0; i--) {
                assertEquals(i, mem.getInt(o));
                o += 4;
            }
        }
    }

    @Test
    public void testIntCompatibility() {
        long pageSize = 64;
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putInt(1024);
            mem.putIntBytes(2048);
            assertEquals(1024, mem.getIntBytes(0, 0));
            assertEquals(2048, mem.getInt(4));
        }
    }

    @Test
    public void testIntRnd() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(7, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            long o = 1;
            mem.putByte(0, (byte) 1);
            int n = 999;
            for (int i = n; i > 0; i--) {
                mem.putInt(o, i);
                o += 4;
            }

            o = 1;
            assertEquals(1, mem.getByte(0));

            for (int i = n; i > 0; i--) {
                assertEquals(i, mem.getInt(o));
                o += 4;
            }
        }
    }

    @Test
    public void testIntRndCompatibility() {
        long pageSize = 64;
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            // prime page
            mem.putByte(10, (byte) 22);
            mem.putInt(15, 1024);
            mem.putIntBytes(55, 2048);
            assertEquals(1024, mem.getIntBytes(0, 15));
            assertEquals(2048, mem.getInt(55));
        }
    }

    @Test
    public void testJumpTo() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putByte((byte) 1);
            int n = 999;
            for (int i = n; i > 0; i--) {
                mem.putLong(i);
            }

            assertEquals(1, mem.getByte(0));

            mem.jumpTo(1);
            for (int i = n; i > 0; i--) {
                mem.putLong(n - i);
            }

            long o = 1;
            for (int i = n; i > 0; i--) {
                assertEquals(n - i, mem.getLong(o));
                o += 8;
            }
        }
    }

    @Test
    public void testJumpTo2() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.jumpTo(8);
            int n = 999;
            for (int i = n; i > 0; i--) {
                mem.putLong(i);
            }
            long o = 8;
            for (int i = n; i > 0; i--) {
                assertEquals(i, mem.getLong(o));
                o += 8;
            }

        }
    }

    @Test
    public void testJumpTo3() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.jumpTo(256);
            int n = 999;
            for (int i = n; i > 0; i--) {
                mem.putLong(i);
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
            }

            o = 0;
            for (int i = n; i > 0; i--) {
                assertEquals(i, mem.getLong(o));
                o += 8;
            }

        }
    }

    @Test
    public void testLong256() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(256, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putLong256("0xEA674fdDe714fd979de3EdF0F56AA9716B898ec8");
            mem.putLong256("0xEA674fdDe714fd979de3EdF0F56AA9716B898ec8");
        }
    }

    @Test
    public void testLong256Direct() {
        long pageSize = 64;
        Rnd rnd = new Rnd();
        Long256Impl sink = new Long256Impl();
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            for (int i = 0; i < 1000; i++) {
                mem.putLong256(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
            }

            rnd.reset();
            long offset = 0;
            for (int i = 0; i < 1000; i++) {
                mem.getLong256(offset, sink);
                Assert.assertEquals(rnd.nextLong(), sink.getLong0());
                Assert.assertEquals(rnd.nextLong(), sink.getLong1());
                Assert.assertEquals(rnd.nextLong(), sink.getLong2());
                Assert.assertEquals(rnd.nextLong(), sink.getLong3());

                Long256 long256A = mem.getLong256A(offset);
                Assert.assertEquals(sink, long256A);
                Long256 long256B = mem.getLong256B(offset);
                Assert.assertEquals(sink, long256B);

                offset += Long256.BYTES;
            }
        }
    }

    @Test
    public void testLong256DirectExternallySequenced() {
        long pageSize = 64;
        Rnd rnd = new Rnd();
        Long256Impl sink = new Long256Impl();
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            long offset = 0;
            for (int i = 0; i < 1000; i++) {
                mem.putLong256(offset, rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
                offset += Long256.BYTES;
            }

            rnd.reset();
            offset = 0;

            for (int i = 0; i < 1000; i++) {
                mem.getLong256(offset, sink);
                offset += Long256.BYTES;
                Assert.assertEquals(rnd.nextLong(), sink.getLong0());
                Assert.assertEquals(rnd.nextLong(), sink.getLong1());
                Assert.assertEquals(rnd.nextLong(), sink.getLong2());
                Assert.assertEquals(rnd.nextLong(), sink.getLong3());
            }
        }
    }

    @Test
    public void testLong256FullStr() {
        String expected = "0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060";
        long pageSize = 128;
        Long256Impl long256 = new Long256Impl();
        Long256Impl long256a = new Long256Impl();
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {

            mem.putLong256(expected);
            mem.putLong256(expected);

            mem.getLong256(0, long256);
            String actual = "0x" + Long.toHexString(long256.getLong3())
                    + Long.toHexString(long256.getLong2())
                    + Long.toHexString(long256.getLong1())
                    + Long.toHexString(long256.getLong0()
            );

            Assert.assertEquals(expected, actual);
            mem.getLong256(Long256.BYTES, long256a);

            String actual2 = "0x" + Long.toHexString(long256a.getLong3())
                    + Long.toHexString(long256a.getLong2())
                    + Long.toHexString(long256a.getLong1())
                    + Long.toHexString(long256a.getLong0()
            );
            Assert.assertEquals(expected, actual2);
        }
    }

    @Test
    public void testLong256Null() {
        long pageSize = 64;
        final int N = 1000;
        Long256Impl long256 = new Long256Impl();
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            for (int i = 0; i < N; i++) {
                mem.putLong256((CharSequence) null);
            }

            StringSink sink = new StringSink();
            long offset = 0;
            for (int i = 0; i < N; i++) {
                mem.getLong256(offset, long256);
                Assert.assertEquals(Numbers.LONG_NaN, long256.getLong0());
                Assert.assertEquals(Numbers.LONG_NaN, long256.getLong1());
                Assert.assertEquals(Numbers.LONG_NaN, long256.getLong2());
                Assert.assertEquals(Numbers.LONG_NaN, long256.getLong3());
                mem.getLong256(offset, sink);
                Assert.assertEquals(0, sink.length());
                offset += Long256.BYTES;
            }
        }
    }

    @Test
    public void testLong256Obj() {
        long pageSize = 64;
        Rnd rnd = new Rnd();
        Long256Impl long256 = new Long256Impl();
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            for (int i = 0; i < 1000; i++) {
                long256.fromRnd(rnd);
                mem.putLong256(long256);
            }

            rnd.reset();
            long offset = 0;
            for (int i = 0; i < 1000; i++) {
                mem.getLong256(offset, long256);
                offset += Long256.BYTES;
                Assert.assertEquals(rnd.nextLong(), long256.getLong0());
                Assert.assertEquals(rnd.nextLong(), long256.getLong1());
                Assert.assertEquals(rnd.nextLong(), long256.getLong2());
                Assert.assertEquals(rnd.nextLong(), long256.getLong3());
            }
        }
    }

    @Test
    public void testLong256ObjExternallySequenced() {
        long pageSize = 64;
        Rnd rnd = new Rnd();
        long offset = 0;
        Long256Impl long256 = new Long256Impl();
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            for (int i = 0; i < 1000; i++) {
                long256.fromRnd(rnd);
                mem.putLong256(offset, long256);
                offset += Long256.BYTES;
            }

            rnd.reset();
            offset = 0;
            for (int i = 0; i < 1000; i++) {
                mem.getLong256(offset, long256);
                offset += Long256.BYTES;
                Assert.assertEquals(rnd.nextLong(), long256.getLong0());
                Assert.assertEquals(rnd.nextLong(), long256.getLong1());
                Assert.assertEquals(rnd.nextLong(), long256.getLong2());
                Assert.assertEquals(rnd.nextLong(), long256.getLong3());
            }
        }
    }

    @Test
    public void testLong256PartialStr() {
        final String expected = "0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed";
        long pageSize = 128;
        Long256Impl long256 = new Long256Impl();
        Long256Impl long256a = new Long256Impl();
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putLong256(expected);
            mem.putLong256(expected);
            mem.getLong256(0, long256);

            String actual = "0x";
            if (long256.getLong3() != 0) {
                actual += Long.toHexString(long256.getLong3());
            }
            if (long256.getLong2() != 0) {
                actual += Long.toHexString(long256.getLong2());
            }
            if (long256.getLong1() != 0) {
                actual += Long.toHexString(long256.getLong1());
            }
            if (long256.getLong0() != 0) {
                actual += Long.toHexString(long256.getLong0());
            }

            Assert.assertEquals(expected, actual);
            mem.getLong256(Long256.BYTES, long256a);

            String actual2 = "0x";
            if (long256a.getLong3() != 0) {
                actual2 += Long.toHexString(long256a.getLong3());
            }
            if (long256a.getLong2() != 0) {
                actual2 += Long.toHexString(long256a.getLong2());
            }
            if (long256a.getLong1() != 0) {
                actual2 += Long.toHexString(long256a.getLong1());
            }
            if (long256a.getLong0() != 0) {
                actual2 += Long.toHexString(long256a.getLong0());
            }
            Assert.assertEquals(expected, actual2);

            long o = mem.getAppendOffset();
            mem.putLong256(expected, 2, expected.length());
            Assert.assertEquals(long256, mem.getLong256A(o));
            String padded = "JUNK" + expected + "MOREJUNK";
            mem.putLong256(padded, 6, 4 + expected.length());
            Assert.assertEquals(long256, mem.getLong256A(o));

            try {
                mem.putLong256(padded);
                Assert.fail();
            } catch (CairoException ex) {
                Assert.assertTrue(ex.getMessage().contains("invalid long256"));
                Assert.assertTrue(ex.getMessage().contains(padded));
            }
        }
    }

    @Test
    public void testLongCompatibility() {
        long pageSize = 64;
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putLong(8980980284302834L);
            mem.putLongBytes(897928374972983477L);
            assertEquals(8980980284302834L, mem.getLongBytes(0, 0, pageSize));
            assertEquals(897928374972983477L, mem.getLong(8));
        }
    }

    @Test
    public void testLongEven() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            int n = 999;
            for (int i = n; i > 0; i--) {
                mem.putLong(i);
            }

            long o = 0;
            for (int i = n; i > 0; i--) {
                assertEquals(i, mem.getLong(o));
                o += 8;
            }
        }
    }

    @Test
    public void testLongOdd() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putByte((byte) 1);
            int n = 999;
            for (int i = n; i > 0; i--) {
                mem.putLong(i);
            }

            long o = 1;
            assertEquals(1, mem.getByte(0));

            for (int i = n; i > 0; i--) {
                assertEquals(i, mem.getLong(o));
                o += 8;
            }
        }
    }

    @Test
    public void testLongRndCompatibility() {
        long pageSize = 64;
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putLong(33, 8980980284302834L);
            mem.putLongBytes(12, 897928374972983477L);
            assertEquals(8980980284302834L, mem.getLongBytes(0, 33, pageSize));
            assertEquals(897928374972983477L, mem.getLong(12));
        }
    }

    @Test
    public void testLongRndEven() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            int n = 999;
            long o = 0;
            for (int i = n; i > 0; i--) {
                mem.putLong(o, i);
                o += 8;
            }

            o = 0;
            for (int i = n; i > 0; i--) {
                assertEquals(i, mem.getLong(o));
                o += 8;
            }
        }
    }

    @Test
    public void testLongRndOdd() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putByte(0, (byte) 1);
            int n = 999;
            long o = 1;
            for (int i = n; i > 0; i--) {
                mem.putLong(o, i);
                o += 8;
            }

            o = 1;
            assertEquals(1, mem.getByte(0));

            for (int i = n; i > 0; i--) {
                assertEquals(i, mem.getLong(o));
                o += 8;
            }
        }
    }

    @Test
    public void testMaxPages() {
        int pageSize = 256;
        int maxPages = 3;
        int sz = 256 * 3;
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, maxPages, MemoryTag.NATIVE_DEFAULT)) {
            Assert.assertEquals(pageSize, mem.getExtendSegmentSize());
            int n = 0;
            try {
                while (n <= sz) {
                    mem.putByte((byte) n);
                    n++;
                }
                Assert.fail();
            } catch (CairoException ex) {
                Assert.assertTrue(ex.getMessage().contains("breached"));
            }
            Assert.assertEquals(sz, n);

            for (n = 0; n < sz; n++) {
                byte b = mem.getByte(n);
                Assert.assertEquals((byte) n, b);
            }
        }
    }

    @Test
    public void testNullBin() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            MemoryCARWImplTest.testNullBin0(mem);
        }
    }

    @Test
    public void testOkSize() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            MemoryCARWImplTest.assertStrings(mem);
        }
    }

    @Test
    public void testShort() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(7, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putByte((byte) 1);
            short n = 999;
            for (short i = n; i > 0; i--) {
                mem.putShort(i);
            }

            long o = 1;
            assertEquals(1, mem.getByte(0));

            for (short i = n; i > 0; i--) {
                assertEquals(i, mem.getShort(o));
                o += 2;
            }
        }
    }

    @Test
    public void testShortCompatibility() {
        long pageSize = 64;
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putShort((short) 1024);
            mem.putShortBytes((short) 2048);
            assertEquals(1024, mem.getShortBytes(0, 0, pageSize));
            assertEquals(2048, mem.getShort(2));
        }
    }

    @Test
    public void testShortRnd() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(7, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            long o = 1;
            mem.putByte(0, (byte) 1);
            short n = 999;
            for (short i = n; i > 0; i--) {
                mem.putShort(o, i);
                o += 2;
            }

            assertEquals(1, mem.getByte(0));

            o = 1;
            for (short i = n; i > 0; i--) {
                assertEquals(i, mem.getShort(o));
                o += 2;
            }
        }
    }

    @Test
    public void testShortRndCompatibility() {
        long pageSize = 64;
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            // prime the page
            mem.putShort(5, (short) 3);
            mem.putShort(11, (short) 1024);
            mem.putShortBytes(33, (short) 2048);
            assertEquals(1024, mem.getShortBytes(0, 11, pageSize));
            assertEquals(2048, mem.getShort(33));
        }
    }

    @Test
    public void testSkip() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putByte((byte) 1);
            int n = 999;
            for (int i = n; i > 0; i--) {
                mem.putLong(i);
                mem.skip(3);
            }

            long o = 1;
            assertEquals(1, mem.getByte(0));

            for (int i = n; i > 0; i--) {
                assertEquals(i, mem.getLong(o));
                o += 11;
            }
            assertEquals(10990, mem.getAppendOffset());
        }
    }

    @Test
    public void testSmallEven() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(2, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            MemoryCARWImplTest.assertStrings(mem);
        }
    }

    @Test
    public void testStrRndEven() {
        testStrRnd(0, 4);
    }

    @Test
    public void testStrRndLargePage() {
        testStrRnd(1, 16);
    }

    @Test
    public void testStrRndOdd() {
        testStrRnd(1, 4);
    }

    @Test
    public void testStringStorageDimensions() {
        assertEquals(10, Vm.getStorageLength("xyz"));
        assertEquals(4, Vm.getStorageLength(""));
        assertEquals(4, Vm.getStorageLength(null));
    }

    private void testStrRnd(long offset, long pageSize) {
        Rnd rnd = new Rnd();
        int N = 1000;
        final int M = 4;
        try (MemoryPARWImpl mem = new MemoryPARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            long o = offset;
            for (int i = 0; i < N; i++) {
                int flag = rnd.nextInt();
                if ((flag % 4) == 0) {
                    mem.putStr(o, null);
                    o += 4;
                } else if ((flag % 2) == 0) {
                    mem.putStr(o, "");
                    o += 4;
                } else {
                    mem.putStr(o, rnd.nextChars(M));
                    o += M * 2 + 4;
                }
            }

            rnd.reset();
            o = offset;
            for (int i = 0; i < N; i++) {
                int flag = rnd.nextInt();
                if ((flag % 4) == 0) {
                    assertNull(mem.getStr(o));
                    o += 4;
                } else if ((flag % 2) == 0) {
                    TestUtils.assertEquals("", mem.getStr(o));
                    o += 4;
                } else {
                    TestUtils.assertEquals(rnd.nextChars(M), mem.getStr(o));
                    o += M * 2 + 4;
                }
            }
        }
    }

}