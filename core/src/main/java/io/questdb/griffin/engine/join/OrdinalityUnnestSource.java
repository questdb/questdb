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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Numbers;
import io.questdb.std.str.Utf8Sequence;

/**
 * UnnestSource that produces a 1-based ordinality column.
 * Returns elementIndex + 1 for numeric getters, null/default for others.
 * Does not contribute to the array length (init returns 0).
 */
public class OrdinalityUnnestSource implements UnnestSource {

    @Override
    public ArrayView getArray(int sourceCol, int elementIndex, int columnType) {
        return null;
    }

    @Override
    public boolean getBool(int sourceCol, int elementIndex) {
        return false;
    }

    @Override
    public byte getByte(int sourceCol, int elementIndex) {
        return 0;
    }

    @Override
    public char getChar(int sourceCol, int elementIndex) {
        return 0;
    }

    @Override
    public int getColumnCount() {
        return 1;
    }

    @Override
    public int getColumnType(int sourceCol) {
        return ColumnType.LONG;
    }

    @Override
    public long getDate(int sourceCol, int elementIndex) {
        return Numbers.LONG_NULL;
    }

    @Override
    public double getDouble(int sourceCol, int elementIndex) {
        return elementIndex + 1;
    }

    @Override
    public float getFloat(int sourceCol, int elementIndex) {
        return Float.NaN;
    }

    @Override
    public int getInt(int sourceCol, int elementIndex) {
        return elementIndex + 1;
    }

    @Override
    public long getLong(int sourceCol, int elementIndex) {
        return elementIndex + 1;
    }

    @Override
    public short getShort(int sourceCol, int elementIndex) {
        return (short) (elementIndex + 1);
    }

    @Override
    public CharSequence getStrA(int sourceCol, int elementIndex) {
        return null;
    }

    @Override
    public CharSequence getStrB(int sourceCol, int elementIndex) {
        return null;
    }

    @Override
    public int getStrLen(int sourceCol, int elementIndex) {
        return -1;
    }

    @Override
    public long getTimestamp(int sourceCol, int elementIndex) {
        return Numbers.LONG_NULL;
    }

    @Override
    public Utf8Sequence getVarcharA(int sourceCol, int elementIndex) {
        return null;
    }

    @Override
    public Utf8Sequence getVarcharB(int sourceCol, int elementIndex) {
        return null;
    }

    @Override
    public int getVarcharSize(int sourceCol, int elementIndex) {
        return -1;
    }

    @Override
    public int init(Record baseRecord) {
        return 0;
    }
}
