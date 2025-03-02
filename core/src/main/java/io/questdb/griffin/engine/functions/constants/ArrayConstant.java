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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Record;

public final class ArrayConstant extends ArrayFunction implements ConstantFunction {
    private final DirectArray array = new DirectArray();

    public ArrayConstant(ArrayView arrayIn) {
        this.type = arrayIn.getType();
        if (ColumnType.isNull(type)) {
            array.ofNull();
            return;
        }
        array.setType(type);
        int nDims = arrayIn.getDimCount();
        for (int dim = 0; dim < nDims; dim++) {
            array.setDimLen(dim, arrayIn.getDimLen(dim));
        }
        array.applyShape(-1);
        FlatArrayView flatViewIn = arrayIn.flatView();
        int elemCount = arrayIn.getFlatViewLength();
        for (int i = 0; i < elemCount; i++) {
            array.putDouble(i, flatViewIn.getDouble(i));
        }
    }

    @Override
    public void close() {
        array.close();
    }

    @Override
    public ArrayView getArray(Record rec) {
        return array;
    }
}
