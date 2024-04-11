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

package io.questdb.griffin.engine.functions.lt;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class LtIPv4StrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "<(XS)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function ipv4Func = args.getQuick(0);
        final int strFuncPosition = argPositions.getQuick(1);
        final Function strFunc = args.getQuick(1);
        if (strFunc.isConstant()) {
            int ipv4 = strFunc.getIPv4(null);
            return new ConstStrFunc(ipv4Func, ipv4);
        } else if (strFunc.isRuntimeConstant()) {
            return new RuntimeConstStrFunc(ipv4Func, strFunc);
        }
        throw SqlException.$(strFuncPosition, "STRING constant expected");
    }

    private static class ConstStrFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final int constIPv4;

        public ConstStrFunc(Function arg, int constIPv4) {
            this.arg = arg;
            this.constIPv4 = constIPv4;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            if (constIPv4 != Numbers.IPv4_NULL) {
                int ipv4 = arg.getIPv4(rec);
                if (ipv4 != Numbers.IPv4_NULL) {
                    return negated == (Numbers.ipv4ToLong(ipv4) >= Numbers.ipv4ToLong(constIPv4));
                }
            }
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
            if (negated) {
                sink.val(">=");
            } else {
                sink.val('<');
            }
            sink.valIPv4(constIPv4);
        }
    }

    private static class RuntimeConstStrFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final Function ipv4Func;
        private final Function strFunc;
        private int constIPv4;

        public RuntimeConstStrFunc(Function ipv4Func, Function strFunc) {
            this.ipv4Func = ipv4Func;
            this.strFunc = strFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            if (constIPv4 != Numbers.IPv4_NULL) {
                int ipv4 = ipv4Func.getIPv4(rec);
                if (ipv4 != Numbers.IPv4_NULL) {
                    return negated == (Numbers.ipv4ToLong(ipv4) >= Numbers.ipv4ToLong(constIPv4));
                }
            }
            return false;
        }

        @Override
        public Function getLeft() {
            return ipv4Func;
        }

        @Override
        public Function getRight() {
            return strFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);
            constIPv4 = strFunc.getIPv4(null);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(ipv4Func);
            if (negated) {
                sink.val(">=");
            } else {
                sink.val('<');
            }
            sink.val(strFunc);
        }
    }
}
