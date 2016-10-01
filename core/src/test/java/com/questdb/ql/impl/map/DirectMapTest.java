/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.impl.map;

import com.questdb.io.sink.StringSink;
import com.questdb.misc.Chars;
import com.questdb.misc.Numbers;
import com.questdb.misc.Rnd;
import com.questdb.misc.Unsafe;
import com.questdb.std.IntList;
import com.questdb.store.ColumnType;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DirectMapTest {

    private static final IntList COLUMN_TYPES = new IntList();

    @BeforeClass
    public static void setUp() throws Exception {
        COLUMN_TYPES.add(ColumnType.INT);
        COLUMN_TYPES.add(ColumnType.LONG);
        COLUMN_TYPES.add(ColumnType.SHORT);
        COLUMN_TYPES.add(ColumnType.BYTE);
        COLUMN_TYPES.add(ColumnType.DOUBLE);
        COLUMN_TYPES.add(ColumnType.FLOAT);
    }

    @Test
    public void testAllKeysAndCursor() throws Exception {

        // Objective of this test is to create DirectMap with all
        // possible types in both key and value. Simultaneously create
        // regular hash map, where key is hex encoded bytes of direct map
        // and value is object, which holds same primitive values we
        // put into direct map.


        Rnd rnd = new Rnd();
        final int addressSize = 2 * 1024 * 1024;
        long address = Unsafe.malloc(addressSize);
        final int tmpSize = 140;
        long tmp = Unsafe.malloc(tmpSize);

        DirectMap map = new DirectMap(1024, 64, COLUMN_TYPES);
        StringSink sink = new StringSink();

        HashMap<String, MapValue> hashMap = new HashMap<>();

        for (int i = 0; i < 1000; i++) {
            sink.clear();
            DirectMap.KeyWriter w = map.keyWriter();

            long p = address;

            CharSequence cs = rnd.nextChars(rnd.nextInt() % 64);
            long l = rnd.nextLong();
            boolean b = rnd.nextBoolean();
            int ii = rnd.nextInt();
            short s = (short) rnd.nextInt();
            byte by = (byte) rnd.nextInt();
            double d = rnd.nextDouble();
            float f = rnd.nextFloat();
            CharSequence s2 = rnd.nextBoolean() ? null : cs;

            w.put(tmp, Chars.put(tmp, cs));
            w.putLong(l);
            w.putBool(b);
            w.putInt(ii);
            w.putShort(s);
            w.putByte(by);
            w.putDouble(d);
            w.putFloat(f);
            w.putLong(l);
            w.putStr(s2);
            w.putByte(by);

            // write same string to base64 buffer
            p = put(p, cs);
            p = put(p, l);
            p = put(p, b);
            p = put(p, ii);
            p = put(p, s);
            p = put(p, by);
            p = put(p, d);
            p = put(p, f);
            p = put(p, l);
            p = put(p, s2);
            p = put(p, by);

            MapValue v = new MapValue();
            DirectMapValues values = map.getOrCreateValues(w);
            values.putInt(0, v.i = rnd.nextPositiveInt());
            values.putLong(1, v.l = rnd.nextPositiveLong());
            values.putShort(2, v.s = (short) rnd.nextInt());
            values.putByte(3, v.bt = (byte) rnd.nextInt());
            values.putDouble(4, v.d = rnd.nextDouble());
            values.putFloat(5, v.f = rnd.nextFloat());

            hashMap.put(toStr(sink, address, p), v);
        }

        Assert.assertEquals(hashMap.size(), map.size());

        HashMap<Long, MapValue> rowidMap = new HashMap<>();

        for (DirectMapEntry e : map) {
            long p = address;

            // check that A and B return same sequence
            CharSequence csA = e.getFlyweightStr(6);
            CharSequence csB = e.getFlyweightStrB(6);
            TestUtils.assertEquals(csA, csB);

            p = put(p, csA);
            p = put(p, e.getLong(7));
            p = put(p, e.getBool(8));
            p = put(p, e.getInt(9));
            p = put(p, e.getShort(10));
            p = put(p, e.get(11));
            p = put(p, e.getDouble(12));
            p = put(p, e.getFloat(13));
            p = put(p, e.getDate(14));

            String s = e.getStr(15);
            p = put(p, s);

            sink.clear();
            e.getStr(15, sink);

            if (s == null) {
                Assert.assertEquals(0, sink.length());
                Assert.assertEquals(-1, e.getStrLen(15));
            } else {
                TestUtils.assertEquals(s, sink);
                Assert.assertEquals(s.length(), e.getStrLen(15));
            }

            p = put(p, e.get(16));

            sink.clear();

            MapValue v = hashMap.get(toStr(sink, address, p));

            Assert.assertEquals(v.i, e.getInt(0));
            Assert.assertEquals(v.l, e.getLong(1));
            Assert.assertEquals(v.s, e.getShort(2));
            Assert.assertEquals(v.bt, e.get(3));
            Assert.assertEquals(v.d, e.getDouble(4), 0.000000001);
            Assert.assertEquals(v.f, e.getFloat(5), 0.0000000001f);
            rowidMap.put(e.getRowId(), v);
        }

        // retrieve map by rowid

        for (Map.Entry<Long, MapValue> me : rowidMap.entrySet()) {
            DirectMapEntry e = map.entryAt(me.getKey());
            MapValue v = me.getValue();
            Assert.assertEquals(v.i, e.getInt(0));
            Assert.assertEquals(v.l, e.getLong(1));
            Assert.assertEquals(v.s, e.getShort(2));
            Assert.assertEquals(v.bt, e.get(3));
            Assert.assertEquals(v.d, e.getDouble(4), 0.000000001);
            Assert.assertEquals(v.f, e.getFloat(5), 0.0000000001f);
        }

        map.clear();
        Assert.assertEquals(0, map.size());
        int count = 0;
        for (DirectMapEntry ignored : map) {
            count++;
        }
        Assert.assertEquals(0, count);

        map.close();

        Unsafe.free(address, addressSize);
        Unsafe.free(tmp, tmpSize);
    }

    @Test
    public void testValuesReadWrite() throws Exception {
        DirectMap map = new DirectMap(1024, 64, COLUMN_TYPES);
        HashMap<String, MapValue> hashMap = new HashMap<>();
        Rnd rnd = new Rnd();
        int n = 1000;

        for (int i = 0; i < n; i++) {
            DirectMap.KeyWriter w = map.keyWriter();
            String s = rnd.nextString(rnd.nextPositiveInt() % 32);
            w.putStr(s);
            MapValue v = new MapValue();
            DirectMapValues values = map.getOrCreateValues(w);
            values.putInt(0, v.i = rnd.nextPositiveInt());
            values.putLong(1, v.l = rnd.nextPositiveLong());
            values.putShort(2, v.s = (short) rnd.nextInt());
            values.putByte(3, v.bt = (byte) rnd.nextInt());
            values.putDouble(4, v.d = rnd.nextDouble());
            values.putFloat(5, v.f = rnd.nextFloat());
            hashMap.put(s, v);
        }

        for (Map.Entry<String, MapValue> me : hashMap.entrySet()) {
            DirectMap.KeyWriter kw = map.keyWriter();
            kw.putStr(me.getKey());
            DirectMapValues values = map.getValues(kw);
            Assert.assertNotNull(values);

            MapValue v = me.getValue();
            Assert.assertEquals(v.i, values.getInt(0));
            Assert.assertEquals(v.l, values.getLong(1));
            Assert.assertEquals(v.s, values.getShort(2));
            Assert.assertEquals(v.bt, values.get(3));
            Assert.assertEquals(v.d, values.getDouble(4), 0.000000001);
            Assert.assertEquals(v.f, values.getFloat(5), 0.0000000001f);

        }
        map.close();
    }

    private static long put(long address, CharSequence cs) {
        if (cs == null) {
            return address;
        }
        return address + Chars.put(address, cs);
    }

    private static long put(long address, long value) {
        Unsafe.getUnsafe().putLong(address, value);
        return address + 8;
    }

    private static String toStr(StringSink s, long lo, long hi) {
        for (long p = lo; p < hi; p++) {
            Numbers.appendHex(s, (int) Unsafe.getUnsafe().getByte(p));
        }
        return s.toString();
    }

    private static long put(long address, boolean value) {
        Unsafe.getUnsafe().putByte(address, (byte) (value ? 1 : 0));
        return address + 1;
    }

    private static long put(long address, int value) {
        Unsafe.getUnsafe().putInt(address, value);
        return address + 4;
    }

    private static long put(long address, byte value) {
        Unsafe.getUnsafe().putByte(address, value);
        return address + 1;
    }

    private static long put(long address, double value) {
        Unsafe.getUnsafe().putDouble(address, value);
        return address + 1;
    }

    private static long put(long address, float value) {
        Unsafe.getUnsafe().putFloat(address, value);
        return address + 1;
    }

    private static class MapValue {
        byte bt;
        double d;
        float f;
        int i;
        long l;
        short s;
    }
}