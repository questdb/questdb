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
import io.questdb.std.str.DirectCharSink;

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
        private static final int INITIAL_SINK_CAPACITY = 8 * 1024;
        private final Function arg;
        private final char delimiter;
        private final DirectCharSink sink = new DirectCharSink(INITIAL_SINK_CAPACITY);
        private boolean nullValue = true;

        public StringAggGroupByFunction(int position, Function arg, char delimiter) {
            super(position);
            this.arg = arg;
            this.delimiter = delimiter;
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record) {
            setNull();
            CharSequence str = arg.getStr(record);
            if (str != null) {
                append(str);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record) {
            CharSequence str = arg.getStr(record);
            if (str != null) {
                if (!nullValue) {
                    appendDelimiter();
                }
                append(str);
            }
        }

        @Override
        public void pushValueTypes(ArrayColumnTypes columnTypes) {
            columnTypes.add(ColumnType.STRING);
        }

        @Override
        public void setNull(MapValue mapValue) {
            setNull();
        }

        @Override
        public void close() {
            sink.close();
        }

        @Override
        public CharSequence getStr(Record rec) {
            if (nullValue) {
                return null;
            }
            return sink;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStr(rec);
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        private void setNull() {
            sink.clear();
            nullValue = true;
        }

        private void appendDelimiter() {
            sink.put(delimiter);
        }

        private void append(CharSequence str) {
            sink.put(str);
            nullValue = false;
        }
    }
}
