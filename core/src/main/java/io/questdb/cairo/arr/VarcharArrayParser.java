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
import io.questdb.std.BoolList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSinkList;

public final class VarcharArrayParser extends ArrayView implements FlatArrayView {
    private static final int STATE_IDLE = 0;
    private static final int STATE_IN_QUOTE = 1;
    private static final int STATE_IN_VALUE = 2;
    private final BoolList nullFlags = new BoolList();
    private final Utf8StringSinkList values = new Utf8StringSinkList(32);

    public VarcharArrayParser() {
        this.flatView = this;
    }

    @Override
    public void appendToMemFlat(MemoryA mem, int offset, int length) {
        for (int i = 0, n = Math.min(nullFlags.size(), length); i < n; i++) {
            int idx = i + offset;
            if (nullFlags.get(idx)) {
                mem.putVarchar(null);
            } else {
                mem.putVarchar(values.getQuick(idx));
            }
        }
    }

    @Override
    public double getDoubleAtAbsIndex(int elemIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLongAtAbsIndex(int elemIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Utf8Sequence getVarcharAt(int index) {
        if (nullFlags.get(index)) {
            return null;
        }
        return values.getQuick(index);
    }

    @Override
    public int length() {
        return nullFlags.size();
    }

    public void of(long lo, long hi) {
        of(lo, hi, -1);
    }

    public void of(long lo, long hi, int expectedDimCount) {
        values.clear();
        nullFlags.clear();
        shape.clear();
        strides.clear();
        flatViewLength = 0;
        if (lo >= hi) {
            type = ColumnType.NULL;
            return;
        }
        parse(lo, hi);
        if (shape.size() > 1) {
            throw new IllegalArgumentException("varchar arrays must be 1-dimensional, got " + shape.size() + " dimensions");
        }
        if (expectedDimCount != -1) {
            if (shape.size() < expectedDimCount) {
                if (isEmpty()) {
                    shape.extendAndSet(expectedDimCount - 1, 0);
                } else {
                    throw new IllegalArgumentException("expected " + expectedDimCount + " dimensions, got " + shape.size());
                }
            } else if (shape.size() > expectedDimCount) {
                throw new IllegalArgumentException("expected " + expectedDimCount + " dimensions, got " + shape.size());
            }
        }
        type = ColumnType.encodeArrayType(ColumnType.VARCHAR, shape.size(), false);
        defaultStrides();
    }

    public void of(CharSequence input) {
        of(input, -1);
    }

    public void of(CharSequence input, int expectedDimCount) {
        values.clear();
        nullFlags.clear();
        shape.clear();
        strides.clear();
        flatViewLength = 0;
        if (input == null) {
            type = ColumnType.NULL;
            return;
        }
        parse(input);
        if (shape.size() > 1) {
            throw new IllegalArgumentException("varchar arrays must be 1-dimensional, got " + shape.size() + " dimensions");
        }
        if (expectedDimCount != -1) {
            if (shape.size() < expectedDimCount) {
                if (isEmpty()) {
                    shape.extendAndSet(expectedDimCount - 1, 0);
                } else {
                    throw new IllegalArgumentException("expected " + expectedDimCount + " dimensions, got " + shape.size());
                }
            } else if (shape.size() > expectedDimCount) {
                throw new IllegalArgumentException("expected " + expectedDimCount + " dimensions, got " + shape.size());
            }
        }
        type = ColumnType.encodeArrayType(ColumnType.VARCHAR, shape.size(), false);
        defaultStrides();
    }

    private void addValueFromMemory(long valueStart, long valueEnd) {
        long len = valueEnd - valueStart;
        if (len == 4
                && (Unsafe.getUnsafe().getByte(valueStart) | 32) == 'n'
                && (Unsafe.getUnsafe().getByte(valueStart + 1) | 32) == 'u'
                && (Unsafe.getUnsafe().getByte(valueStart + 2) | 32) == 'l'
                && (Unsafe.getUnsafe().getByte(valueStart + 3) | 32) == 'l') {
            nullFlags.add(true);
            values.setElem();
        } else {
            nullFlags.add(false);
            values.put(valueStart, valueEnd);
            values.setElem();
        }
        flatViewLength++;
    }

    private void defaultStrides() {
        final int nDims = shape.size();
        strides.clear();
        for (int i = 0; i < nDims; i++) {
            strides.add(0);
        }

        int stride = 1;
        for (int i = nDims - 1; i >= 0; i--) {
            strides.set(i, stride);
            stride *= shape.get(i);
        }
    }

    private void parse(long lo, long hi) {
        assert strides.size() == 0;

        if (lo >= hi) {
            return;
        }

        byte firstChar = Unsafe.getUnsafe().getByte(lo);
        if (firstChar != '{' && firstChar != '[') {
            // Single element
            shape.add(1);
            addValueFromMemory(lo, hi);
            return;
        }

        long valueStart = -1;
        int state = STATE_IDLE;

        for (long pos = lo; pos < hi; pos++) {
            byte c = Unsafe.getUnsafe().getByte(pos);

            if (c == '"') {
                if (state == STATE_IN_QUOTE) {
                    addValueFromMemory(valueStart, pos);
                    strides.increment(strides.size() - 1);
                    state = STATE_IDLE;
                } else {
                    valueStart = pos + 1;
                    state = STATE_IN_QUOTE;
                }
                continue;
            }

            if (state == STATE_IN_QUOTE) {
                continue;
            }

            switch (c) {
                case '[':
                case '{': {
                    strides.add(0);
                    break;
                }
                case ']':
                case '}': {
                    if (state == STATE_IN_VALUE) {
                        addValueFromMemory(valueStart, pos);
                        strides.increment(strides.size() - 1);
                        state = STATE_IDLE;
                    }
                    int depth = strides.size() - 1;
                    if (depth > 0) {
                        strides.increment(depth - 1);
                    }

                    int currentCount = strides.getQuick(depth);
                    updateShapeInfo(depth, currentCount, (int) (pos - lo));
                    strides.removeIndex(depth);
                    break;
                }
                case ',': {
                    if (state == STATE_IN_VALUE) {
                        addValueFromMemory(valueStart, pos);
                        strides.increment(strides.size() - 1);
                        state = STATE_IDLE;
                    }
                    break;
                }
                case ' ':
                case '\t':
                case '\n':
                case '\r':
                    break;

                default:
                    if (state != STATE_IN_VALUE) {
                        valueStart = pos;
                        state = STATE_IN_VALUE;
                    }
            }
        }
    }

    private void parse(CharSequence input) {
        assert strides.size() == 0;

        if (input.length() == 0) {
            return;
        }

        // Check if input starts with array delimiter
        char firstChar = input.charAt(0);
        if (firstChar != '{' && firstChar != '[') {
            // Single element
            parseSingleElement(input);
            return;
        }

        int valueStart = -1;
        int state = STATE_IDLE;

        for (int position = 0; position < input.length(); position++) {
            char c = input.charAt(position);

            if (c == '"') {
                if (state == STATE_IN_QUOTE) {
                    parseAndAddValue(input, valueStart, position);
                    state = STATE_IDLE;
                } else {
                    valueStart = position + 1;
                    state = STATE_IN_QUOTE;
                }
                continue;
            }

            if (state == STATE_IN_QUOTE) {
                continue;
            }

            switch (c) {
                case '[':
                case '{': {
                    strides.add(0);
                    break;
                }
                case ']':
                case '}': {
                    if (state == STATE_IN_VALUE) {
                        parseAndAddValue(input, valueStart, position);
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
                    if (state == STATE_IN_VALUE) {
                        parseAndAddValue(input, valueStart, position);
                        state = STATE_IDLE;
                    }
                    break;
                }
                case ' ':
                case '\t':
                case '\n':
                case '\r':
                    break;

                default:
                    if (state != STATE_IN_VALUE) {
                        valueStart = position;
                        state = STATE_IN_VALUE;
                    }
            }
        }
    }

    private void parseAndAddValue(CharSequence input, int valueStart, int i) {
        int len = i - valueStart;
        if (len == 4
                && (input.charAt(valueStart) | 32) == 'n'
                && (input.charAt(valueStart + 1) | 32) == 'u'
                && (input.charAt(valueStart + 2) | 32) == 'l'
                && (input.charAt(valueStart + 3) | 32) == 'l') {
            nullFlags.add(true);
            values.setElem(); // add empty element to keep indices in sync
        } else {
            nullFlags.add(false);
            values.put(input, valueStart, i);
        }
        strides.increment(strides.size() - 1);
        flatViewLength++;
    }

    private void parseAndAddValueDirect(CharSequence input) {
        int len = input.length();
        if (len == 4
                && (input.charAt(0) | 32) == 'n'
                && (input.charAt(0 + 1) | 32) == 'u'
                && (input.charAt(0 + 2) | 32) == 'l'
                && (input.charAt(0 + 3) | 32) == 'l') {
            nullFlags.add(true);
            values.setElem();
        } else {
            nullFlags.add(false);
            values.put(input, 0, len);
        }
        flatViewLength++;
    }

    private void parseSingleElement(CharSequence input) {
        shape.add(1);
        parseAndAddValueDirect(input);
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
