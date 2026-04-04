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

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DerivedArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Numbers;

/**
 * UnnestSource implementation for typed arrays. Wraps a single array
 * function and exposes its elements as a single output column.
 */
public class ArrayUnnestSource implements UnnestSource {
    private final DerivedArrayView derivedView = new DerivedArrayView();
    private final Function function;
    private int cachedBaseOffset;
    private int cachedLen;
    private int cachedStride0;
    private boolean isDerivedViewReady;
    private ArrayView view;

    /**
     * @param function the array-producing function
     */
    public ArrayUnnestSource(Function function) {
        this.function = function;
    }

    @Override
    public ArrayView getArray(
            int sourceCol,
            int elementIndex,
            int columnType
    ) {
        if (elementIndex >= cachedLen) {
            return null;
        }
        if (!isDerivedViewReady) {
            // First call: full setup — of() copies shape/strides,
            // subArray() slices and removes dim 0.
            derivedView.of(view);
            cachedStride0 = view.getStride(0);
            cachedBaseOffset = view.getFlatViewOffset();
            derivedView.subArray(0, elementIndex);
            isDerivedViewReady = true;
        } else {
            // Subsequent calls: just reposition the offset.
            derivedView.setFlatViewOffset(
                    cachedBaseOffset + elementIndex * cachedStride0
            );
        }
        return derivedView;
    }

    @Override
    public int getColumnCount() {
        return 1;
    }

    @Override
    public double getDouble(int sourceCol, int elementIndex) {
        if (elementIndex >= cachedLen) {
            return Double.NaN;
        }
        return view.getDouble(elementIndex);
    }

    // getLong is implemented because ArrayView.getLong() is ready.
    // It will be reachable once LONG[] is enabled in ColumnType.arrayTypeSet.
    @Override
    public long getLong(int sourceCol, int elementIndex) {
        if (elementIndex >= cachedLen) {
            return Numbers.LONG_NULL;
        }
        return view.getLong(elementIndex);
    }

    @Override
    public int init(Record baseRecord) {
        ArrayView v = function.getArray(baseRecord);
        if (v.isNull()) {
            this.view = null;
            this.cachedLen = 0;
            return 0;
        }
        this.view = v;
        this.cachedLen = v.isEmpty() ? 0 : v.getDimLen(0);
        this.isDerivedViewReady = false;
        return cachedLen;
    }
}
