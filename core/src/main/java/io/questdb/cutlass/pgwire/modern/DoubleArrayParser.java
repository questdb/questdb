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

package io.questdb.cutlass.pgwire.modern;
// todo: move to another package, it's not PGWire-specific


import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.DoubleList;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;

public final class DoubleArrayParser extends ArrayView implements FlatArrayView {
    private static final int STATE_IDLE = 0;
    private static final int STATE_IN_NUMBER = 2;
    private static final int STATE_IN_QUOTE = 1;
    private final DoubleList values = new DoubleList();

    public DoubleArrayParser() {
        this.flatView = this;
    }

    @Override
    public void appendToMemFlat(MemoryA mem) {
        for (int i = 0, n = values.size(); i < n; i++) {
            mem.putDouble(values.getQuick(i));
        }
    }

    @Override
    public short elemType() {
        return ColumnType.DOUBLE;
    }

    @Override
    public double getDouble(int elemIndex) {
        return values.getQuick(elemIndex);
    }

    @Override
    public long getLong(int elemIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int length() {
        return values.size();
    }

    public void of(CharSequence input) {
        values.clear();
        shape.clear();
        strides.clear();
        type = ColumnType.ARRAY; // todo: what's the right type when array is null?
        if (input != null) {
            parse(input);
            type = ColumnType.encodeArrayType(ColumnType.DOUBLE, shape.size());
        }
        strides.clear();
        resetToDefaultStrides();
    }
    
    private void parse(CharSequence input) {
        IntList currentDimSizes = strides;
        assert currentDimSizes.size() == 0;

        int numberStart = -1;
        int state = STATE_IDLE;

        for (int position = 0; position < input.length(); position++) {
            char c = input.charAt(position);

            if (c == '"') {
                if (state == STATE_IN_QUOTE) {
                    parseAndAddNumber(input, numberStart, position);
                    state = STATE_IDLE;
                } else {
                    numberStart = position + 1;
                    state = STATE_IN_QUOTE;
                }
                continue;
            }

            if (state == STATE_IN_QUOTE) {
                continue;
            }

            switch (c) {
                case '{':
                    currentDimSizes.add(1);
                    break;
                case '}':
                    if (state == STATE_IN_NUMBER) {
                        parseAndAddNumber(input, numberStart, position);
                        state = STATE_IDLE;
                    }

                    int depth = currentDimSizes.size() - 1;
                    int currentCount = currentDimSizes.getQuick(depth);
                    updateShapeInfo(depth, currentCount, position);
                    currentDimSizes.removeIndex(depth);
                    break;
                case ',':
                    if (state == STATE_IN_NUMBER) {
                        parseAndAddNumber(input, numberStart, position);
                        state = STATE_IDLE;
                    }

                    int lastIndex = currentDimSizes.size() - 1;
                    currentDimSizes.increment(lastIndex);
                    break;

                // Skip whitespace. {\r{1,2.0}, {3.1,\n0.4}} is a legal input
                case ' ':
                case '\t':
                case '\n':
                case '\r':
                    break;

                default:
                    if (state != STATE_IN_NUMBER) {
                        numberStart = position;
                        state = STATE_IN_NUMBER;
                    }
            }
        }
    }

    private void parseAndAddNumber(CharSequence input, int numberStart, int i) {
        int len = i - numberStart;
        if (len == 4
                && (input.charAt(numberStart) | 32) == 'n'
                && (input.charAt(numberStart + 1) | 32) == 'u'
                && (input.charAt(numberStart + 2) | 32) == 'l'
                && (input.charAt(numberStart + 3) | 32) == 'l') {
            values.add(Double.NaN);
        } else {
            try {
                values.add(Numbers.parseDouble(input, numberStart, len));
            } catch (NumericException e) {
                throw new IllegalArgumentException("Invalid number format at position " + numberStart, e);
            }
        }
        flatViewLength++;
    }

    private void updateShapeInfo(int depth, int currentCount, int position) {
        if (shape.size() <= depth) {
            shape.extendAndSet(depth, currentCount);
        } else {
            int alreadyObservedCount = shape.getQuick(depth);
            if (alreadyObservedCount == 0) {
                // first time we see this dimension
                shape.setQuick(depth, currentCount);
            } else if (currentCount != alreadyObservedCount) {
                throw new IllegalArgumentException("inconsistent array [depth=" + depth + ", currentCount=" + currentCount + ", alreadyObservedCount=" + alreadyObservedCount + ", position=" + position + "]");
            }
        }
    }
}
