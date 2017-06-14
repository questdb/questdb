/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.misc.ByteBuffers;
import com.questdb.misc.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class VirtualMemoryTest {

    @Test
    public void testBinBuf() throws Exception {
        assertBuf(ByteBuffer.allocate(1024 * 1024), 1024);
    }

    @Test
    public void testBinBufDirect() throws Exception {
        ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024);
        try {
            assertBuf(buf, 1024);
        } finally {
            ByteBuffers.release(buf);
        }
    }

    @Test
    public void testByte() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(11)) {
            int n = 120;


            for (int i = 0; i < n; i++) {
                mem.putByte((byte) i);
            }

            long o = 0;
            for (int i = 0; i < n; i++) {
                Assert.assertEquals(i, mem.getByte(o++));
            }
        }
    }

    @Test
    public void testDouble() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(11)) {
            Rnd rnd = new Rnd();
            int n = 999;

            mem.putByte((byte) 1);

            for (int i = 0; i < n; i++) {
                mem.putDouble(rnd.nextDouble());
            }

            Assert.assertEquals(7993, mem.getAppendOffset());

            rnd = new Rnd();
            long o = 1;
            Assert.assertEquals(1, mem.getByte(0));
            for (int i = 0; i < n; i++) {
                Assert.assertEquals(rnd.nextDouble(), mem.getDouble(o), 0.00001);
                o += 8;
            }
        }
    }

    @Test
    public void testDoubleCompatibility() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(64)) {
            mem.putDouble(8980980284.22234);
            mem.putDoubleBytes(8979283749.72983477);
            Assert.assertEquals(8980980284.22234, mem.getDoubleBytes(0, 0), 0.00001);
            Assert.assertEquals(8979283749.72983477, mem.getDouble(8), 0.00001);
        }
    }

    @Test
    public void testEvenPageSize() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(32)) {
            assertStrings(mem, false);
        }
    }

    @Test
    public void testFloat() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(11)) {
            Rnd rnd = new Rnd();
            int n = 999;

            mem.putByte((byte) 1);

            for (int i = 0; i < n; i++) {
                mem.putFloat(rnd.nextFloat());
            }

            rnd = new Rnd();
            long o = 1;
            Assert.assertEquals(1, mem.getByte(0));
            for (int i = 0; i < n; i++) {
                Assert.assertEquals(rnd.nextFloat(), mem.getFloat(o), 0.00001f);
                o += 4;
            }
        }
    }

    @Test
    public void testFloatCompatibility() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(64)) {
            mem.putFloat(1024f);
            mem.putFloatBytes(2048f);
            Assert.assertEquals(1024f, mem.getFloatBytes(0, 0), 0.00001f);
            Assert.assertEquals(2048f, mem.getFloat(4), 0.0001f);
        }
    }

    @Test
    public void testInt() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(7)) {
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
    public void testIntCompatibility() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(64)) {
            mem.putInt(1024);
            mem.putIntBytes(2048);
            Assert.assertEquals(1024, mem.getIntBytes(0, 0));
            Assert.assertEquals(2048, mem.getInt(4));
        }
    }

    @Test
    public void testJumpTo() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(11)) {
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
    public void testLargePageBinBuf() throws Exception {
        assertBuf(ByteBuffer.allocate(1024 * 1024), 4 * 1024 * 1024);
    }

    @Test
    public void testLargePageBinBufDirect() throws Exception {
        ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024);
        try {
            assertBuf(buf, 4 * 1024 * 1024);
        } finally {
            ByteBuffers.release(buf);
        }
    }

    @Test
    public void testLongCompatibility() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(64)) {
            mem.putLong(8980980284302834L);
            mem.putLongBytes(897928374972983477L);
            Assert.assertEquals(8980980284302834L, mem.getLongBytes(0, 0));
            Assert.assertEquals(897928374972983477L, mem.getLong(8));
        }
    }

    @Test
    public void testLongEven() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(11)) {
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
    public void testLongOdd() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(11)) {
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
    public void testNullBin() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(1024)) {
            ByteBuffer buf = ByteBuffer.allocate(1024);
            mem.putBin(null);
            buf.flip();
            mem.putBin(buf);
            long o1 = mem.putNullBin();

            Assert.assertNull(mem.getBin(0));
            VirtualMemory.ByteSequenceView bsview = mem.getBin(8);
            Assert.assertNotNull(bsview);
            Assert.assertEquals(0, bsview.length());
            Assert.assertNull(mem.getBin(o1));
        }
    }

    @Test
    public void testOffPageSize() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(12)) {
            assertStrings(mem, true);
        }
    }

    @Test
    public void testOkSize() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(1024)) {
            assertStrings(mem, false);
        }
    }

    @Test
    public void testShort() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(7)) {
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
    public void testShortCompatibility() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(64)) {
            mem.putShort((short) 1024);
            mem.putShortBytes((short) 2048);
            Assert.assertEquals(1024, mem.getShortBytes(0, 0));
            Assert.assertEquals(2048, mem.getShort(2));
        }
    }

    @Test
    public void testSkip() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(11)) {
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
            Assert.assertEquals(10990, mem.getAppendOffset());
        }
    }

    @Test
    public void testSmallEven() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(2)) {
            assertStrings(mem, false);
        }
    }

    @Test
    public void testSmallOdd() throws Exception {
        try (VirtualMemory mem = new VirtualMemory(2)) {
            assertStrings(mem, true);
        }
    }

    private void assertBuf(ByteBuffer buf, int pageSize) {
        Rnd rnd = new Rnd();
        int n = 99;

        try (VirtualMemory mem = new VirtualMemory(pageSize)) {
            for (int i = 0; i < n; i++) {
                int sz = rnd.nextPositiveInt() % buf.capacity();
                for (int j = 0; j < sz; j++) {
                    buf.put(rnd.nextByte());
                }
                buf.flip();

                mem.putBin(buf);
                Assert.assertEquals(0, buf.remaining());
                buf.clear();
            }

            rnd = new Rnd();

            long o = 0;
            for (int i = 0; i < n; i++) {
                long sz = rnd.nextPositiveInt() % buf.capacity();
                VirtualMemory.ByteSequenceView bsview = mem.getBin(o);
                Assert.assertNotNull(bsview);
                Assert.assertEquals(sz, bsview.length());

                o += sz + 8;

                for (long j = 0; j < sz; j++) {
                    Assert.assertEquals(rnd.nextByte(), bsview.byteAt(j));
                }
            }
        }
    }

    private void assertStrings(VirtualMemory mem, boolean b) {
        if (b) {
            mem.putByte((byte) 1);
        }

        long o1 = mem.putStr("123");
        long o2 = mem.putStr("0987654321abcd");
        long o3 = mem.putStr(null);
        long o4 = mem.putStr("xyz123");
        long o5 = mem.putNullStr();

        if (b) {
            assertEquals(1, mem.getByte(0));
        }

        TestUtils.assertEquals("123", mem.getStr(o1));
        TestUtils.assertEquals("0987654321abcd", mem.getStr(o2));
        assertNull(mem.getStr(o3));
        TestUtils.assertEquals("xyz123", mem.getStr(o4));
        assertNull(mem.getStr(o5));
    }
}