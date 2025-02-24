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
    private final DoubleList values = new DoubleList();

    public DoubleArrayParser() {
        this.flatView = this;
    }

    @Override
    public void appendToMem(MemoryA mem) {
        for (int i = 0, n = values.size(); i < n; i++) {
            mem.putDouble(values.getQuick(i));
        }
    }

    @Override
    public double getDouble(int elemIndex) {
        return values.getQuick(elemIndex);
    }

    @Override
    public long getLong(int elemIndex) {
        throw new UnsupportedOperationException();
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
        calculateStrides();
    }

    private void addElement(CharSequence input, int numberStart, int i) {
        try {
            values.add(Numbers.parseDouble(input, numberStart, i - numberStart));
            flatViewLength++;
        } catch (NumericException e) {
            throw new IllegalArgumentException("Invalid number format at position " + numberStart, e);
        }
    }

    private void calculateStrides() {
        assert strides.size() == 0;

        int stride = 1;
        for (int i = shape.size() - 1; i >= 0; i--) {
            strides.add(stride);
            stride *= shape.getQuick(i);
        }
    }

    private void parse(CharSequence input) {
        IntList currentDimSizes = strides;
        assert currentDimSizes.size() == 0;

        boolean inQuote = false;
        int numberStart = -1;
        boolean inNumber = false;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);

            if (c == '"') {
                inQuote = !inQuote;
                if (inQuote) {
                    numberStart = i + 1;
                } else {
                    addElement(input, numberStart, i);
                    inNumber = false;
                }
            } else if (!inQuote) {
                switch (c) {
                    case '{':
                        currentDimSizes.add(1);
                        break;
                    case '}':
                        if (inNumber) {
                            addElement(input, numberStart, i);
                            inNumber = false;
                        }
                        int depth = currentDimSizes.size() - 1;
                        int currentCount = currentDimSizes.getQuick(depth);
                        if (shape.size() <= depth) {
                            shape.extendAndSet(depth, currentCount);
                        } else {
                            int alreadyObservedCount = shape.getQuick(depth);
                            if (alreadyObservedCount == 0) {
                                // first time we see this dimension
                                shape.setQuick(depth, currentCount);
                            } else if (currentCount != alreadyObservedCount) {
                                throw new IllegalArgumentException("inconsistent array [depth=" + depth + ", currentCount=" + currentCount + ", alreadyObservedCount=" + alreadyObservedCount + ", position=" + i + "]");
                            }
                        }
                        currentDimSizes.removeIndex(depth);
                        break;
                    case ',':
                        if (inNumber) {
                            addElement(input, numberStart, i);
                            inNumber = false;
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
                        if (!inNumber) {
                            numberStart = i;
                            inNumber = true;
                        }
                }
            }
        }
    }
}
