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
import io.questdb.cairo.arr.DerivedArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Numbers;
import io.questdb.std.str.Utf8Sequence;

/**
 * UnnestSource implementation for typed arrays. Wraps a single array
 * function and exposes its elements as a single output column.
 */
public class ArrayUnnestSource implements UnnestSource {
    private final DerivedArrayView derivedView = new DerivedArrayView();
    private final Function function;
    private final int outputType;
    private ArrayView view;

    /**
     * @param function the array-producing function
     */
    public ArrayUnnestSource(Function function) {
        this.function = function;
        int columnType = function.getType();
        int dims = ColumnType.decodeArrayDimensionality(columnType);
        short elemType = ColumnType.decodeArrayElementType(columnType);
        if (dims > 1) {
            this.outputType = ColumnType.encodeArrayType(
                    elemType, dims - 1
            );
        } else {
            this.outputType = elemType;
        }
    }

    @Override
    public ArrayView getArray(
            int sourceCol,
            int elementIndex,
            int columnType
    ) {
        if (view == null) {
            return null;
        }
        int len = view.getDimLen(0);
        if (elementIndex >= len) {
            return null;
        }
        derivedView.of(view);
        derivedView.subArray(0, elementIndex);
        return derivedView;
    }

    // Only DOUBLE[] is currently enabled in ColumnType.arrayTypeSet.
    // These getters throw so that enabling a new element type without
    // implementing the corresponding getter surfaces immediately
    // rather than silently returning wrong values.

    @Override
    public boolean getBool(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public char getChar(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getColumnCount() {
        return 1;
    }

    @Override
    public int getColumnType(int sourceCol) {
        return outputType;
    }

    @Override
    public long getDate(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int sourceCol, int elementIndex) {
        if (view == null) {
            return Double.NaN;
        }
        int len = view.getDimLen(0);
        if (elementIndex >= len) {
            return Double.NaN;
        }
        return view.getDouble(elementIndex);
    }

    @Override
    public float getFloat(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    // getLong is implemented because ArrayView.getLong() is ready.
    // It will be reachable once LONG[] is enabled in ColumnType.arrayTypeSet.
    @Override
    public long getLong(int sourceCol, int elementIndex) {
        if (view == null) {
            return Numbers.LONG_NULL;
        }
        int len = view.getDimLen(0);
        if (elementIndex >= len) {
            return Numbers.LONG_NULL;
        }
        return view.getLong(elementIndex);
    }

    @Override
    public short getShort(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrA(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrB(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStrLen(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTimestamp(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Utf8Sequence getVarcharA(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Utf8Sequence getVarcharB(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getVarcharSize(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int init(Record baseRecord) {
        ArrayView v = function.getArray(baseRecord);
        if (v.isNull()) {
            this.view = null;
            return 0;
        }
        this.view = v;
        return v.isEmpty() ? 0 : v.getDimLen(0);
    }
}
