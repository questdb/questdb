/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class LastStrGroupByFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "last(S)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new Func(args.getQuick(0));
    }

    /**
     * Wraps {@link LastDirectStrGroupByFunction} and {@link LastStrGroupByFunction} depending
     * on whether direct strings are supported by the record cursor factory or not.
     */
    public class Func extends StrFunction implements GroupByFunction, UnaryFunction {
        private final Function arg;
        private GroupByFunction delegate;

        public Func(Function arg) {
            this.arg = arg;
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            delegate.computeFirst(mapValue, record, rowId);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            delegate.computeNext(mapValue, record, rowId);
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public String getName() {
            return "last";
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return delegate.getStrA(rec);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return delegate.getStrB(rec);
        }

        @Override
        public int getValueIndex() {
            return delegate.getValueIndex();
        }

        @Override
        public void initValueIndex(int valueIndex, boolean directStrSupported) {
            initDelegate(directStrSupported);
            delegate.initValueIndex(valueIndex, directStrSupported);
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes, boolean directStrSupported) {
            initDelegate(directStrSupported);
            delegate.initValueTypes(columnTypes, directStrSupported);
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        @Override
        public boolean isScalar() {
            return false;
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            delegate.merge(destValue, srcValue);
        }

        @Override
        public void setAllocator(GroupByAllocator allocator) {
            delegate.setAllocator(allocator);
        }

        @Override
        public void setNull(MapValue mapValue) {
            delegate.setNull(mapValue);
        }

        @Override
        public boolean supportsParallelism() {
            return UnaryFunction.super.supportsParallelism();
        }

        @Override
        public void toTop() {
            UnaryFunction.super.toTop();
        }

        private void initDelegate(boolean directStrSupported) {
            assert delegate == null;
            if (directStrSupported && arg.supportsDirectStr()) {
                delegate = new LastDirectStrGroupByFunction(arg);
            } else {
                delegate = new LastStrGroupByFunction(arg);
            }
        }
    }
}
