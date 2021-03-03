/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.ObjList;

public class StringAggGroupByFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "string_agg(Sa)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new StringAggGroupByFunction(position, args.getQuick(0), args.getQuick(1).getChar(null));
    }

    private static class StringAggGroupByFunction extends StrFunction implements GroupByFunction {
        private final Function arg;
        private final char delimiter;
        private int valueIndex;
        private boolean empty;

        public StringAggGroupByFunction(int position, Function arg, char delimiter) {
            super(position);
            this.arg = arg;
            this.delimiter = delimiter;
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record) {
            CharSequence str = arg.getStr(record);
            if (str != null) {
                mapValue.putStr(valueIndex, str);
                empty = false;
            } else {
                mapValue.putNullStr(valueIndex);
                empty = true;
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record) {
            CharSequence str = arg.getStr(record);
            if (str != null) {
                if (!empty) {
                    mapValue.appendChar(valueIndex, delimiter);
                } else {
                    empty = false;
                }
                mapValue.appendStr(valueIndex, arg.getStr(record));
            }
        }

        @Override
        public void pushValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.STRING);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putNullStr(valueIndex);
            empty = true;
        }

        @Override
        public CharSequence getStr(Record rec) {
            return rec.getStr(valueIndex);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return rec.getStrB(valueIndex);
        }

        @Override
        public boolean isConstant() {
            return false;
        }
    }
}
