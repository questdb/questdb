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

package io.questdb.cairo.arr;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.DoubleList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;

public final class DoubleArrayParser extends MutableArray implements FlatArrayView {
    private static final int STATE_IDLE = 0;
    private static final int STATE_IN_NUMBER = 2;
    private static final int STATE_IN_QUOTE = 1;
    private final DoubleList values = new DoubleList();

    public DoubleArrayParser() {
        this.flatView = this;
    }

    @Override
    public void appendToMemFlat(MemoryA mem, int offset, int length) {
        for (int i = 0, n = Math.min(values.size(), length); i < n; i++) {
            mem.putDouble(values.getQuick(i + offset));
        }
    }

    @Override
    public double getDoubleAtAbsIndex(int elemIndex) {
        return values.getQuick(elemIndex);
    }

    @Override
    public long getLongAtAbsIndex(int elemIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int length() {
        return values.size();
    }

    public void of(CharSequence input) {
        of(input, -1);
    }

    public void of(CharSequence input, int expectedDimCount) {
        values.clear();
        shape.clear();
        strides.clear();
        flatViewLength = 0;
        if (input == null) {
            type = ColumnType.NULL;
            return;
        }
        parse(input);
        if (expectedDimCount != -1) {
            if (shape.size() < expectedDimCount) {
                if (isEmpty()) {
                    // empty arrays can show fewer dimensions when serialized into string,
                    // we can cast them to expected dimensions
                    shape.extendAndSet(expectedDimCount - 1, 0);
                } else {
                    // non-empty arrays must have the same number of dimensions as expected
                    throw new IllegalArgumentException("expected " + expectedDimCount + " dimensions, got " + shape.size());
                }
            } else if (shape.size() > expectedDimCount) {
                // Arrays with more dimensions than expected are not allowed, even if they are empty.
                // Think of `{{},{}}' - this is an empty array, but we cannot insert it into a 1-dimensional array
                // since it most likely means an error in the input
                throw new IllegalArgumentException("expected " + expectedDimCount + " dimensions, got " + shape.size());
            }
        }
        type = ColumnType.encodeArrayType(ColumnType.DOUBLE, shape.size());
        resetToDefaultStrides();
    }

    private void parse(CharSequence input) {
        assert strides.size() == 0;

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
                case '[':
                    // fallthrough
                case '{': {
                    strides.add(0);
                    break;
                }
                case ']':
                    // fallthrough
                case '}': {
                    if (state == STATE_IN_NUMBER) {
                        parseAndAddNumber(input, numberStart, position);
                        state = STATE_IDLE;
                    }
                    int depth = strides.size() - 1;
                    if (depth > 0) {
                        strides.increment(depth - 1);
                    }

                    int currentCount = strides.getQuick(depth);
                    updateShapeInfo(depth, currentCount, position);
                    strides.removeIndex(depth);
                    break;
                }
                case ',': {
                    if (state == STATE_IN_NUMBER) {
                        parseAndAddNumber(input, numberStart, position);
                        state = STATE_IDLE;
                    }
                    break;
                }
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
            throw new IllegalArgumentException("NULL is not supported in arrays");
        } else {
            try {
                values.add(Numbers.parseDouble(input, numberStart, len));
            } catch (NumericException e) {
                throw new IllegalArgumentException("Invalid number format at position " + numberStart, e);
            }
        }
        strides.increment(strides.size() - 1);
        flatViewLength++;
    }

    private void updateShapeInfo(int depth, int currentCount, int position) {
        if (shape.size() <= depth) {
            for (int i = shape.size(); i < depth; i++) {
                shape.add(0);
            }
            shape.add(currentCount);
        } else {
            int alreadyObservedCount = shape.getQuick(depth);
            if (alreadyObservedCount == 0) {
                // first time we see this dimension
                shape.setQuick(depth, currentCount);
            } else if (currentCount != alreadyObservedCount) {
                throw new IllegalArgumentException("element counts in sub-arrays don't match [depth=" + depth +
                        ", currentCount=" + currentCount +
                        ", alreadyObservedCount=" + alreadyObservedCount +
                        ", position=" + position + "]");
            }
        }
    }
}
