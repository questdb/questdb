package com.questdb.cairo;

import com.questdb.misc.Rnd;
import com.questdb.ql.Record;
import com.questdb.std.BinarySequence;

public class TestRecord implements Record {
    final Rnd rnd = new Rnd();
    final ArrayBinarySequence abs = new ArrayBinarySequence().of(new byte[1024]);

    @Override
    public byte get(int col) {
        return rnd.nextByte();
    }

    @Override
    public BinarySequence getBin2(int col) {
        if (rnd.nextPositiveInt() % 32 == 0) {
            return null;
        }
        for (int i = 0, n = abs.array.length; i < n; i++) {
            abs.array[i] = rnd.nextByte();
        }
        return abs;
    }

    @Override
    public boolean getBool(int col) {
        return rnd.nextBoolean();
    }

    @Override
    public long getDate(int col) {
        return rnd.nextPositiveLong();
    }

    @Override
    public double getDouble(int col) {
        return rnd.nextDouble();
    }

    @Override
    public float getFloat(int col) {
        return rnd.nextFloat();
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        return rnd.nextInt() % 16 == 0 ? null : rnd.nextChars(15);
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        return rnd.nextInt() % 16 == 0 ? null : rnd.nextChars(15);
    }

    @Override
    public int getInt(int col) {
        return rnd.nextInt();
    }

    @Override
    public long getLong(int col) {
        return rnd.nextLong();
    }

    @Override
    public long getRowId() {
        return -1;
    }

    @Override
    public short getShort(int col) {
        return rnd.nextShort();
    }

    @Override
    public int getStrLen(int col) {
        return 15;
    }

    @Override
    public CharSequence getSym(int col) {
        return rnd.nextChars(10);
    }

    public static class ArrayBinarySequence implements BinarySequence {
        private byte[] array;

        @Override
        public byte byteAt(long index) {
            return array[(int) index];
        }

        @Override
        public long length() {
            return array.length;
        }

        ArrayBinarySequence of(byte[] array) {
            this.array = array;
            return this;
        }
    }

}
