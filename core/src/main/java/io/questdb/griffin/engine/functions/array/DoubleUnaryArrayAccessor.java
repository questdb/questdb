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

package io.questdb.griffin.engine.functions.array;

import io.questdb.cairo.arr.ArrayView;

public interface DoubleUnaryArrayAccessor {

    void applyToElement(ArrayView view, int index);

    void applyToEntireVanillaArray(ArrayView view);

    void applyToNullArray();

    default void calculate(ArrayView view) {
        if (view.isNull()) {
            applyToNullArray();
        } else if (view.isVanilla()) {
            applyToEntireVanillaArray(view);
        } else {
            calculateRecursive(view, 0, 0);
        }
    }

    private void calculateRecursive(ArrayView view, int dim, int flatIndex) {
        final int count = view.getDimLen(dim);
        final int stride = view.getStride(dim);
        final boolean atDeepestDim = dim == view.getDimCount() - 1;
        if (atDeepestDim) {
            for (int i = 0; i < count; i++) {
                //TODO: this is a megamorphic call site in the middle of a hot loop. We should consider benchmarking
                // it and devirtualize if needed.
                applyToElement(view, flatIndex);
                flatIndex += stride;
            }
        } else {
            for (int i = 0; i < count; i++) {
                calculateRecursive(view, dim + 1, flatIndex);
                flatIndex += stride;
            }
        }
    }
}
