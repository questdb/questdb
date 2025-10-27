/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.vm;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.TestRecord;
import io.questdb.test.griffin.engine.TestBinarySequence;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class MemoryCARWImplTest {
    Decimal128 decimal128 = new Decimal128();
    Decimal256 decimal256 = new Decimal256();

    @AfterClass
    public static void afterClass() {
        ColumnType.resetStringToDefault();
    }

    @BeforeClass
    public static void beforeClass() {
        ColumnType.makeUtf16DefaultString();
    }

    @Test
    public void testBinSequence() {
        testBinSequence0(700, 2048);
    }

    @Test
    public void testBinSequence2() {
        testBinSequence0(1024, 600);
    }

    @Test
    public void testBinSequenceOnEdge() {
        final Rnd rnd = new Rnd();
        try (MemoryARW mem = new MemoryCARWImpl(32, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(128, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            for (int i = 0; i < N; i++) {
                mem.putShort((short) i);
            }

            long target = N * 2;
            long offset = 0;
            short i = 0;
            while (target > 0) {
                long len = mem.size()
                        - offset;
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
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(128, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(7, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(7, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
    public void testDeadCodeForUtf8() {
        try (MemoryARW mem = new MemoryCARWImpl(256, 1, MemoryTag.NATIVE_DEFAULT)) {
            try {
                mem.putStrUtf8(null);
                Assert.fail();
            } catch (UnsupportedOperationException ignored) {
            }
        }
    }

    @Test
    public void testDecimal128() {
        try (MemoryARW mem = new MemoryCARWImpl(32, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            int n = 120;

            long o = 0;
            for (int i = 0; i < n; i++) {
                mem.putDecimal128(i, -i);
                o += Long.BYTES << 1;
                Assert.assertEquals(o, mem.getAppendOffset());
            }

            o = 0;
            for (int i = 0; i < n; i++) {
                mem.getDecimal128(o, decimal128);
                assertEquals(i, decimal128.getHigh());
                assertEquals(-i, decimal128.getLow());
                o += Long.BYTES << 1;
            }
        }
    }

    @Test
    public void testDecimal128WithOffset() {
        try (MemoryARW mem = new MemoryCARWImpl(32, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            int n = 120;

            long o = 0;
            for (int i = n; i > 0; i--) {
                mem.putDecimal128(o, i, -i);
                o += Long.BYTES << 1;
            }

            o = 0;
            for (int i = n; i > 0; i--) {
                mem.getDecimal128(o, decimal128);
                assertEquals(i, decimal128.getHigh());
                assertEquals(-i, decimal128.getLow());
                o += Long.BYTES << 1;
            }
        }
    }

    @Test
    public void testDecimal256() {
        try (MemoryARW mem = new MemoryCARWImpl(32, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            int n = 120;

            long o = 0;
            for (int i = 0; i < n; i++) {
                mem.putDecimal256(i, -i, i + 1, -i - 1);
                o += Long.BYTES << 2;
                Assert.assertEquals(o, mem.getAppendOffset());
            }

            o = 0;
            for (int i = 0; i < n; i++) {
                mem.getDecimal256(o, decimal256);
                assertEquals(i, decimal256.getHh());
                assertEquals(-i, decimal256.getHl());
                assertEquals(i + 1, decimal256.getLh());
                assertEquals(-i - 1, decimal256.getLl());
                o += Long.BYTES << 2;
            }
        }
    }

    @Test
    public void testDecimal256WithOffset() {
        try (MemoryARW mem = new MemoryCARWImpl(32, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            int n = 120;

            long o = 0;
            for (int i = n; i > 0; i--) {
                mem.putDecimal256(o, i, -i, i + 1, -i - 1);
                o += Long.BYTES << 2;
            }

            o = 0;
            for (int i = n; i > 0; i--) {
                mem.getDecimal256(o, decimal256);
                assertEquals(i, decimal256.getHh());
                assertEquals(-i, decimal256.getHl());
                assertEquals(i + 1, decimal256.getLh());
                assertEquals(-i - 1, decimal256.getLl());
                o += Long.BYTES << 2;
            }
        }
    }

    @Test
    public void testDouble() {
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putInt(10);
            mem.putDouble(8980980284.22234);
            assertEquals(8980980284.22234, mem.getDouble(4), 0.00001);
        }
    }

    @Test
    public void testDoubleRnd() {
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            // prime
            mem.putInt(10, 900);
            mem.putDouble(22, 8980980284.22234);
            mem.putDouble(84, 8979283749.72983477);
            assertEquals(8980980284.22234, mem.getDouble(22), 0.00001);
            assertEquals(8979283749.72983477, mem.getDouble(84), 0.00001);
        }
    }

    @Test
    public void testEvenPageSize() {
        try (MemoryARW mem = new MemoryCARWImpl(32, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            assertStrings(mem);
        }
    }

    @Test
    public void testFloat() {
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putFloat(1024f);
            mem.putFloat(2048f);
            assertEquals(1024f, mem.getFloat(0), 0.00001f);
            assertEquals(2048f, mem.getFloat(4), 0.0001f);
        }
    }

    @Test
    public void testFloatRnd() {
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            // prime
            mem.putByte(10, (byte) 5);
            mem.putFloat(61, 1024f);
            mem.putFloat(99, 2048f);
            assertEquals(1024f, mem.getFloat(61), 0.00001f);
            assertEquals(2048f, mem.getFloat(99), 0.0001f);
        }
    }

    @Test
    public void testInt() {
        try (MemoryARW mem = new MemoryCARWImpl(7, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putInt(1024);
            mem.putInt(2048);
            assertEquals(1024, mem.getInt(0));
            assertEquals(2048, mem.getInt(4));
        }
    }

    @Test
    public void testIntRnd() {
        try (MemoryARW mem = new MemoryCARWImpl(7, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            // prime page
            mem.putByte(10, (byte) 22);
            mem.putInt(15, 1024);
            mem.putInt(55, 2048);
            assertEquals(1024, mem.getInt(15));
            assertEquals(2048, mem.getInt(55));
        }
    }

    @Test
    public void testJumpTo() {
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(256, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putLong256("0xEA674fdDe714fd979de3EdF0F56AA9716B898ec8");
            mem.putLong256("0xEA674fdDe714fd979de3EdF0F56AA9716B898ec8");
        }
    }

    @Test
    public void testLong256Direct() {
        long pageSize = 64;
        Rnd rnd = new Rnd();
        Long256Impl sink = new Long256Impl();
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {

            mem.putLong256(expected);
            mem.putLong256(expected);

            mem.getLong256(0, long256);
            String actual = "0x" + Long.toHexString(long256.getLong3()) + Long.toHexString(long256.getLong2()) + Long.toHexString(long256.getLong1()) + Long.toHexString(long256.getLong0());

            Assert.assertEquals(expected, actual);
            mem.getLong256(Long256.BYTES, long256a);

            String actual2 = "0x" + Long.toHexString(long256a.getLong3()) + Long.toHexString(long256a.getLong2()) + Long.toHexString(long256a.getLong1()) +
                    Long.toHexString(long256a.getLong0());
            Assert.assertEquals(expected, actual2);
        }
    }

    @Test
    public void testLong256Null() {
        long pageSize = 64;
        final int N = 1000;
        Long256Impl long256 = new Long256Impl();
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            for (int i = 0; i < N; i++) {
                mem.putLong256((CharSequence) null);
            }

            StringSink sink = new StringSink();
            long offset = 0;
            for (int i = 0; i < N; i++) {
                mem.getLong256(offset, long256);
                Assert.assertEquals(Numbers.LONG_NULL, long256.getLong0());
                Assert.assertEquals(Numbers.LONG_NULL, long256.getLong1());
                Assert.assertEquals(Numbers.LONG_NULL, long256.getLong2());
                Assert.assertEquals(Numbers.LONG_NULL, long256.getLong3());
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
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
            } catch (ImplicitCastException ex) {
                TestUtils.assertContains(
                        ex.getFlyweightMessage(),
                        "inconvertible value: `JUNK0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfedMOREJUNK` [STRING -> LONG256]"
                );
            }
        }
    }

    @Test
    public void testLongCompatibility() {
        long pageSize = 64;
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putLong(8980980284302834L);
            mem.putLong(897928374972983477L);
            assertEquals(8980980284302834L, mem.getLong(0));
            assertEquals(897928374972983477L, mem.getLong(8));
        }
    }

    @Test
    public void testLongEven() {
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putLong(33, 8980980284302834L);
            mem.putLong(12, 897928374972983477L);
            assertEquals(8980980284302834L, mem.getLong(33));
            assertEquals(897928374972983477L, mem.getLong(12));
        }
    }

    @Test
    public void testLongRndEven() {
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, maxPages, MemoryTag.NATIVE_DEFAULT)) {
            Assert.assertEquals(0, mem.size());
            Assert.assertEquals(256, mem.getExtendSegmentSize());
            Assert.assertEquals(256, mem.getPageSize());
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
        try (MemoryARW mem = new MemoryCARWImpl(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            testNullBin0(mem);
        }
    }

    @Test
    public void testOkSize() {
        try (MemoryARW mem = new MemoryCARWImpl(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            assertStrings(mem);
        }
    }

    @Test
    public void testShort() {
        try (MemoryARW mem = new MemoryCARWImpl(7, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putShort((short) 1024);
            mem.putShort((short) 2048);
            assertEquals(1024, mem.getShort(0));
            assertEquals(2048, mem.getShort(2));
        }
    }

    @Test
    public void testShortRnd() {
        try (MemoryARW mem = new MemoryCARWImpl(7, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            // prime the page
            mem.putShort(5, (short) 3);
            mem.putShort(11, (short) 1024);
            mem.putShort(33, (short) 2048);
            assertEquals(1024, mem.getShort(11));
            assertEquals(2048, mem.getShort(33));
        }
    }

    @Test
    public void testSkip() {
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
        try (MemoryARW mem = new MemoryCARWImpl(2, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            assertStrings(mem);
        }
    }

    @Test
    public void testStableResize() {
        int pageSize = 16 * 1024 * 1024;
        try (MemoryCARWImpl mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            Assert.assertEquals(0, mem.size());

            mem.resize(pageSize);
            Assert.assertEquals(pageSize, mem.size());
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
        Assert.assertEquals(10, Vm.getStorageLength("xyz"));
        assertEquals(4, Vm.getStorageLength(""));
        assertEquals(4, Vm.getStorageLength(null));
    }

    @Test
    public void testTruncate() {
        final int pageSize = 256;
        final int maxPages = 3;
        final int sz = 256 * 3;
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, maxPages, MemoryTag.NATIVE_DEFAULT)) {
            for (int i = 0; i < sz; i++) {
                mem.putByte(i, (byte) i);
            }
            Assert.assertEquals(sz, mem.size());

            mem.truncate();

            Assert.assertEquals(pageSize, mem.size());
            Assert.assertEquals(0, mem.getAppendOffset());
        }
    }

    @Test
    public void testTruncateDoesNotAllocate() {
        try (MemoryARW mem = new MemoryCARWImpl(11, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.truncate();
            Assert.assertEquals(0, mem.getPageAddress(0));
        }
    }

    private void testStrRnd(long offset, long pageSize) {
        Rnd rnd = new Rnd();
        int N = 1000;
        final int M = 4;
        try (MemoryARW mem = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
                    assertNull(mem.getStrA(o));
                    o += 4;
                } else if ((flag % 2) == 0) {
                    TestUtils.assertEquals("", mem.getStrA(o));
                    o += 4;
                } else {
                    TestUtils.assertEquals(rnd.nextChars(M), mem.getStrA(o));
                    o += M * 2 + 4;
                }
            }
        }
    }

    static void assertStrings(MemoryARW mem) {
        Assert.assertEquals(10, Vm.getStorageLength("123"));
        Assert.assertEquals(6, Vm.getStorageLength("x"));

        long o1 = mem.putStr("123");
        long o2 = mem.putStr("0987654321abcd");
        Assert.assertEquals(o1, Vm.getStorageLength("123"));
        long o3 = mem.putStr(null);
        Assert.assertEquals(o2 - o1, Vm.getStorageLength("0987654321abcd"));
        long o4 = mem.putStr("xyz123");
        Assert.assertEquals(o3 - o2, Vm.getStorageLength(null));
        long o5 = mem.putNullStr();
        long o6 = mem.putStr("123ohh4", 3, 3);
        long o7 = mem.putStr(null, 0, 2);
        long o8 = mem.putStr((char) 0);
        long o9 = mem.putStr('x');
        Assert.assertEquals(o9, mem.getAppendOffset());

        TestUtils.assertEquals("123", mem.getStrA(0));
        assertEquals(3, mem.getStrLen(0));
        TestUtils.assertEquals("123", mem.getStrB(0));

        String expected = "0987654321abcd";
        TestUtils.assertEquals("0987654321abcd", mem.getStrA(o1));
        TestUtils.assertEquals("0987654321abcd", mem.getStrB(o1));

        for (int i = 0; i < expected.length(); i++) {
            long offset = o1 + 4 + i * 2;
            assertEquals(expected.charAt(i), mem.getChar(offset));
        }

        assertNull(mem.getStrA(o2));
        assertNull(mem.getStrB(o2));
        TestUtils.assertEquals("xyz123", mem.getStrA(o3));
        TestUtils.assertEquals("xyz123", mem.getStrB(o3));
        assertNull(mem.getStrA(o4));
        assertNull(mem.getStrB(o4));
        assertEquals(-1, mem.getStrLen(o4));

        TestUtils.assertEquals("ohh", mem.getStrA(o5));
        assertNull(mem.getStrA(o6));

        CharSequence s1 = mem.getStrA(0);
        CharSequence s2 = mem.getStrB(o1);
        assertFalse(Chars.equals(s1, s2));

        assertNull(mem.getStrA(o7));
        TestUtils.assertEquals("x", mem.getStrA(o8));
    }

    static void testBinSequence0(long mem1Size, long mem2Size) {
        Rnd rnd = new Rnd();
        int n = 999;

        final TestBinarySequence binarySequence = new TestBinarySequence();
        final byte[] buffer = new byte[600];
        final long bufAddr = Unsafe.malloc(buffer.length, MemoryTag.NATIVE_DEFAULT);
        binarySequence.of(buffer);

        try (MemoryARW mem = new MemoryCARWImpl(mem1Size, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            Assert.assertEquals(0, mem.size());
            Assert.assertEquals(Numbers.ceilPow2(mem1Size), mem.getPageSize());
            long offset1 = 8;
            for (int i = 0; i < n; i++) {
                long o;
                if (rnd.nextPositiveInt() % 16 == 0) {
                    o = mem.putBin(null);
                    Assert.assertEquals(offset1, o);
                    offset1 += 8;
                    continue;
                }

                int sz = buffer.length;
                for (int j = 0; j < sz; j++) {
                    buffer[j] = rnd.nextByte();
                    Unsafe.getUnsafe().putByte(bufAddr + j, buffer[j]);
                }

                o = mem.putBin(binarySequence);
                Assert.assertEquals(offset1 + sz, o);
                offset1 += 8 + sz;
                o = mem.putBin(bufAddr, sz);
                Assert.assertEquals(offset1 + sz, o);
                offset1 += 8 + sz;
            }

            try (MemoryARW mem2 = new MemoryCARWImpl(mem2Size, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                Assert.assertEquals(0, mem2.size());
                Assert.assertEquals(Numbers.ceilPow2(mem2Size), mem2.getPageSize());
                offset1 = 0;
                for (int i = 0; i < n; i++) {
                    BinarySequence sequence = mem.getBin(offset1);
                    if (sequence == null) {
                        offset1 += 8;
                    } else {
                        offset1 += 2 * (sequence.length() + 8);
                    }

                    mem2.putBin(sequence);
                }

                offset1 = 0;
                long offset2 = 0;

                // compare
                for (int i = 0; i < n; i++) {
                    BinarySequence sequence1 = mem.getBin(offset1);
                    BinarySequence sequence2 = mem2.getBin(offset2);

                    if (sequence1 == null) {
                        assertNull(sequence2);
                        Assert.assertEquals(TableUtils.NULL_LEN, mem2.getBinLen(offset2));
                        offset1 += 8;
                        offset2 += 8;
                    } else {
                        assertNotNull(sequence2);
                        assertEquals(mem.getBinLen(offset1), mem2.getBinLen(offset2));
                        assertEquals(sequence1.length(), sequence2.length());
                        for (long l = 0, len = sequence1.length(); l < len; l++) {
                            assertEquals(sequence1.byteAt(l), sequence2.byteAt(l));
                        }

                        offset1 += sequence1.length() + 8;
                        sequence1 = mem.getBin(offset1);
                        assertNotNull(sequence1);
                        assertEquals(sequence1.length(), sequence2.length());
                        for (long l = 0, len = sequence1.length(); l < len; l++) {
                            assertEquals(sequence1.byteAt(l), sequence2.byteAt(l));
                        }

                        offset1 += sequence1.length() + 8;
                        offset2 += sequence1.length() + 8;
                    }
                }
            }
        }
        Unsafe.free(bufAddr, buffer.length, MemoryTag.NATIVE_DEFAULT);
    }

    static void testNullBin0(MemoryARW mem) {
        final TestBinarySequence binarySequence = new TestBinarySequence();
        final byte[] buf = new byte[0];
        binarySequence.of(buf);
        mem.putBin(null);
        mem.putBin(0, 0);
        long o1 = mem.putBin(binarySequence);
        mem.putNullBin();

        assertNull(mem.getBin(0));
        assertNull(mem.getBin(8));
        BinarySequence bsview = mem.getBin(16);
        assertNotNull(bsview);
        assertEquals(0, bsview.length());
        assertNull(mem.getBin(o1));
    }
}
