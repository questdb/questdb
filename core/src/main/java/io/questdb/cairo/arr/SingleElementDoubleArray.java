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
import io.questdb.std.Numbers;

public final class SingleElementDoubleArray extends ArrayView {
    private double value;

    public SingleElementDoubleArray(int dimCount) {
        this.flatView = new SingleElementFlatArrayView();
        this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, dimCount);
        for (int i = 0; i < dimCount; i++) {
            this.shape.add(1);
            this.strides.add(1); // stride is irrelevant for single element arrays, adding just for the consistency sake
        }
        flatViewLength = 1;
    }

    public void of(double value) {
        this.value = value;
    }

    private class SingleElementFlatArrayView implements FlatArrayView {

        @Override
        public void appendToMemFlat(MemoryA mem, int offset, int length) {
            if (length == 1) {
                mem.putDouble(value);
            }
        }

        @Override
        public double avgDouble(int offset, int length) {
            return length == 0 ? Double.NaN : value;
        }

        @Override
        public int countDouble(int offset, int length) {
            return length == 0 || Numbers.isNull(value) ? 0 : 1;
        }

        @Override
        public double getDoubleAtAbsIndex(int elemIndex) {
            return value;
        }

        @Override
        public long getLongAtAbsIndex(int elemIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int length() {
            return 1;
        }

        @Override
        public double sumDouble(int offset, int length) {
            return length == 0 ? 0d : value;
        }
    }
}
