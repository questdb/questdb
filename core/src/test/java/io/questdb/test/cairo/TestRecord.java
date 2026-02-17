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

import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Rnd;

public class TestRecord implements Record {
    final ArrayBinarySequence abs = new ArrayBinarySequence().of(new byte[1024]);
    final Rnd rnd = new Rnd();

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
        return rnd.nextDouble();
    }

    @Override
    public float getFloat(int col) {
        return rnd.nextFloat();
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
    public CharSequence getStrA(int col) {
        return rnd.nextInt() % 16 == 0 ? null : rnd.nextChars(15);
    }

    @Override
    public CharSequence getStrB(int col) {
        return rnd.nextInt() % 16 == 0 ? null : rnd.nextChars(15);
    }

    @Override
    public int getStrLen(int col) {
        return 15;
    }

    @Override
    public CharSequence getSymA(int col) {
        return rnd.nextChars(10);
    }

    @Override
    public CharSequence getSymB(int col) {
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
