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
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.arr.NoopArrayState;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;

public class DoubleArray2dConstant extends ArrayFunction implements ConstantFunction {
    private DirectArray array;

    public DoubleArray2dConstant(double[][] vals) {
        this.array = new DirectArray(Integer.MAX_VALUE);
        this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);
        array.setType(type);
        array.setDimLen(0, vals.length);
        array.setDimLen(1, vals[0].length);
        array.applyShape(-1);
        for (int n = vals.length, i = 0; i < n; i++) {
            for (int m = vals[0].length, j = 0; j < m; j++) {
                array.putDouble(i * j, vals[i][j]);
            }
        }
    }

    @Override
    public void close() {
        this.array = Misc.free(this.array);
    }

    @Override
    public ArrayView getArray(Record rec) {
        return array;
    }

    @Override
    public void toPlan(PlanSink sink) {
        StringSink strSink = new StringSink();
        ArrayTypeDriver.arrayToJson(array, strSink, NoopArrayState.INSTANCE);
        sink.val("ARRAY" + strSink);
    }
}
