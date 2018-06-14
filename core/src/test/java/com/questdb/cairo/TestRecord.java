/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.cairo.sql.Record;
import com.questdb.std.BinarySequence;
import com.questdb.std.Rnd;

public class TestRecord implements Record {
    final Rnd rnd = new Rnd();
    final ArrayBinarySequence abs = new ArrayBinarySequence().of(new byte[1024]);

    @Override
    public BinarySequence getBin(int col) {
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
    public byte getByte(int col) {
        return rnd.nextByte();
    }

    @Override
    public long getDate(int col) {
        return rnd.nextPositiveLong();
    }

    @Override
    public double getDouble(int col) {
        return rnd.nextDouble2();
    }

    @Override
    public float getFloat(int col) {
        return rnd.nextFloat2();
    }

    @Override
    public CharSequence getStr(int col) {
        return rnd.nextInt() % 16 == 0 ? null : rnd.nextChars(15);
    }

    @Override
    public CharSequence getStrB(int col) {
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

        public ArrayBinarySequence of(byte[] array) {
            this.array = array;
            return this;
        }
    }
}
