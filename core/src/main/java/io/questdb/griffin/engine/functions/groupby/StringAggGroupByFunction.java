/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.str.DirectCharSink;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

public class StringAggGroupByFunction extends FirstStringGroupByFunction implements GroupByFunction, UnaryFunction {
    private static final int INITIAL_SINK_CAPACITY = 8 * 1024;
    protected int valueIndex;
    private final char delimiter;

    @Override
    public void close() {
        for(CharSequence s : stringValues) {
            ((DirectCharSink)s).close();
        }
        stringValues.clear();
    }

    public StringAggGroupByFunction(@NotNull Function arg, char delimiter) {
        super(arg);
        this.delimiter = delimiter;
        this.stringValues = new ArrayList<>(32);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        int index = stringValues.size();
        mapValue.putInt(this.valueIndex, index);
        DirectCharSink cs = new DirectCharSink(INITIAL_SINK_CAPACITY);
        CharSequence str = this.arg.getStr(record);
        if(str != null) {
            cs.put(str);
        }

        stringValues.add(cs);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        CharSequence str = this.arg.getStr(record);
        if(str != null) {
            int index = mapValue.getInt(this.valueIndex);
            DirectCharSink cs = (DirectCharSink) stringValues.get(index);
            if(cs.length() > 0) {
                cs.put(delimiter);
            }
            cs.put(str);
        }
    }
}
