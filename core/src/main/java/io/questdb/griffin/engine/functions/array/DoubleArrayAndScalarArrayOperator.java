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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.std.Misc;

abstract class DoubleArrayAndScalarArrayOperator extends ArrayFunction implements DoubleUnaryArrayAccessor, BinaryFunction {
    protected final Function arrayArg;
    protected final Function scalarArg;
    private final DirectArray array;
    private final String name;
    protected MemoryA memory;
    protected double scalarValue;

    public DoubleArrayAndScalarArrayOperator(String name, Function arrayArg, Function scalarArg, CairoConfiguration configuration) {
        this.name = name;
        this.arrayArg = arrayArg;
        this.scalarArg = scalarArg;
        this.type = arrayArg.getType();
        this.array = new DirectArray(configuration);
    }

    @Override
    public void applyToNullArray() {
    }

    @Override
    public void close() {
        BinaryFunction.super.close();
        Misc.free(array);
    }

    @Override
    public ArrayView getArray(Record rec) {
        ArrayView arr = arrayArg.getArray(rec);
        if (arr.isNull()) {
            array.ofNull();
            return array;
        }

        scalarValue = scalarArg.getDouble(rec);
        array.setType(getType());
        array.copyShapeFrom(arr);
        array.applyShape();
        memory = array.startMemoryA();
        calculate(arr);
        return array;
    }

    @Override
    public Function getLeft() {
        return arrayArg;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Function getRight() {
        return scalarArg;
    }

    @Override
    public boolean isOperator() {
        return true;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }
}
