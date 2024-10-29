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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.IntervalFunction;
import io.questdb.griffin.engine.functions.constants.IntervalConstant;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;


public class IntervalFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "interval(NN)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function loFunc = args.getQuick(0);
        final Function hiFunc = args.getQuick(1);
        if (loFunc.isConstant() && hiFunc.isConstant()) {
            long lo = loFunc.getTimestamp(null);
            long hi = hiFunc.getTimestamp(null);
            if (lo == Numbers.LONG_NULL || hi == Numbers.LONG_NULL) {
                return IntervalConstant.NULL;
            }
            if (lo > hi) {
                throw SqlException.position(position).put("invalid interval boundaries");
            }
            return IntervalConstant.newInstance(lo, hi);
        }
        if ((loFunc.isConstant() || loFunc.isRuntimeConstant())
                || (hiFunc.isConstant() || hiFunc.isRuntimeConstant())) {
            return new RuntimeConstFunc(position, loFunc, hiFunc);
        }
        return new Func(loFunc, hiFunc);
    }

    private static class Func extends IntervalFunction implements BinaryFunction {
        private final Function hiFunc;
        private final Interval interval = new Interval();
        private final Function loFunc;

        public Func(Function loFunc, Function hiFunc) {
            this.loFunc = loFunc;
            this.hiFunc = hiFunc;
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            long l = loFunc.getTimestamp(rec);
            long r = hiFunc.getTimestamp(rec);
            if (l == Numbers.LONG_NULL || r == Numbers.LONG_NULL) {
                return Interval.NULL;
            }
            if (l > r) {
                throw CairoException.nonCritical().put("invalid interval boundaries");
            }
            return interval.of(l, r);
        }

        @Override
        public Function getLeft() {
            return loFunc;
        }

        @Override
        public String getName() {
            return "interval";
        }

        @Override
        public Function getRight() {
            return hiFunc;
        }
    }

    private static class RuntimeConstFunc extends IntervalFunction implements BinaryFunction {
        private final Function hiFunc;
        private final Interval interval = new Interval();
        private final Function loFunc;
        private final int position;

        public RuntimeConstFunc(int position, Function loFunc, Function hiFunc) {
            this.position = position;
            this.loFunc = loFunc;
            this.hiFunc = hiFunc;
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            return interval;
        }

        @Override
        public Function getLeft() {
            return loFunc;
        }

        @Override
        public String getName() {
            return "interval";
        }

        @Override
        public Function getRight() {
            return hiFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);
            long lo = loFunc.getTimestamp(null);
            long hi = hiFunc.getTimestamp(null);
            if (lo == Numbers.LONG_NULL || hi == Numbers.LONG_NULL) {
                interval.of(Interval.NULL.getLo(), Interval.NULL.getHi());
            }
            if (lo > hi) {
                throw SqlException.position(position).put("invalid interval boundaries");
            }
            interval.of(lo, hi);
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }
    }
}
