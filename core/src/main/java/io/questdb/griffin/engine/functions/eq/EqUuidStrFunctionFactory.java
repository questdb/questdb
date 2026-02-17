/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.griffin.engine.functions.eq;

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
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Uuid;

public final class EqUuidStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(ZS)";
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
    ) {
        Function uuidFunc = args.getQuick(0);
        Function strFunc = args.getQuick(1);
        if (strFunc.isConstant()) {
            CharSequence uuidStr = strFunc.getStrA(null);
            long lo;
            long hi;
            if (uuidStr == null) {
                lo = Numbers.LONG_NULL;
                hi = Numbers.LONG_NULL;
            } else {
                try {
                    Uuid.checkDashesAndLength(uuidStr);
                    lo = Uuid.parseLo(uuidStr);
                    hi = Uuid.parseHi(uuidStr);
                } catch (NumericException e) {
                    // ok, so the constant string is not a UUID format -> it cannot be equal to any UUID
                    return BooleanConstant.FALSE;
                }
            }
            return new ConstStrFunc(lo, hi, uuidFunc);
        } else if (strFunc.isRuntimeConstant()) {
            return new RuntimeConstStrFunc(strFunc, uuidFunc);
        } else if (uuidFunc.isConstant()) {
            long lo = uuidFunc.getLong128Lo(null);
            long hi = uuidFunc.getLong128Hi(null);
            if (Uuid.isNull(lo, hi)) {
                return new EqStrFunctionFactory.NullCheckFunc(strFunc);
            }
            return new ConstUuidFunc(lo, hi, strFunc);
        }
        return new Func(strFunc, uuidFunc);
    }

    /**
     * The string function is constant and the UUID function is not constant.
     */
    private static class ConstStrFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final long constStrHi;
        private final long constStrLo;
        private final Function uuidFunc;

        private ConstStrFunc(long constStrLo, long constStrHi, Function uuidFunc) {
            this.constStrLo = constStrLo;
            this.constStrHi = constStrHi;
            this.uuidFunc = uuidFunc;
        }

        @Override
        public Function getArg() {
            return uuidFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (constStrHi == uuidFunc.getLong128Hi(rec) && constStrLo == uuidFunc.getLong128Lo(rec));
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(uuidFunc);
            if (negated) {
                sink.val('!');
            }
            sink.val("='").valUuid(constStrLo, constStrHi).val('\'');
        }
    }

    /**
     * The UUID function is constant and the string function is not constant.
     */
    private static class ConstUuidFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final long constUuidHi;
        private final long constUuidLo;
        private final Function fun;

        private ConstUuidFunc(long constUuidLo, long constUuidHi, Function fun) {
            this.constUuidLo = constUuidLo;
            this.constUuidHi = constUuidHi;
            this.fun = fun;
        }

        @Override
        public Function getArg() {
            return fun;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence uuidStr = fun.getStrA(rec);
            if (uuidStr == null) {
                return negated != (constUuidHi == Numbers.LONG_NULL && constUuidLo == Numbers.LONG_NULL);
            }
            try {
                Uuid.checkDashesAndLength(uuidStr);
                return negated != (constUuidHi == Uuid.parseHi(uuidStr) && constUuidLo == Uuid.parseLo(uuidStr));
            } catch (NumericException e) {
                return negated;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(fun);
            if (negated) {
                sink.val('!');
            }
            sink.val("='").valUuid(constUuidLo, constUuidHi).val('\'');
        }
    }

    /**
     * The string function is not constant and the UUID function is not constant either.
     */
    private static class Func extends AbstractEqBinaryFunction implements BinaryFunction {

        private Func(Function strFunc, Function uuidFunc) {
            super(strFunc, uuidFunc);
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence str = left.getStrA(rec);
            long lo = right.getLong128Lo(rec);
            long hi = right.getLong128Hi(rec);
            if (str == null) {
                return negated != Uuid.isNull(lo, hi);
            }
            try {
                Uuid.checkDashesAndLength(str);
                return negated != (hi == Uuid.parseHi(str) && lo == Uuid.parseLo(str));
            } catch (NumericException e) {
                return negated;
            }
        }
    }

    /**
     * The string function is runtime constant and the UUID function is not constant.
     */
    private static class RuntimeConstStrFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final Function strFunc;
        private final Function uuidFunc;
        private long constStrHi;
        private long constStrLo;
        private boolean validUuidStr;

        private RuntimeConstStrFunc(Function strFunc, Function uuidFunc) {
            this.strFunc = strFunc;
            this.uuidFunc = uuidFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            if (!validUuidStr) {
                // ok, so the constant string is not a UUID format -> it cannot be equal to any UUID
                return negated;
            }
            return negated != (constStrHi == uuidFunc.getLong128Hi(rec) && constStrLo == uuidFunc.getLong128Lo(rec));
        }

        @Override
        public Function getLeft() {
            return uuidFunc;
        }

        @Override
        public Function getRight() {
            return strFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            validUuidStr = true;
            CharSequence uuidStr = strFunc.getStrA(null);
            if (uuidStr == null) {
                constStrLo = Numbers.LONG_NULL;
                constStrHi = Numbers.LONG_NULL;
            } else {
                try {
                    Uuid.checkDashesAndLength(uuidStr);
                    constStrLo = Uuid.parseLo(uuidStr);
                    constStrHi = Uuid.parseHi(uuidStr);
                } catch (NumericException e) {
                    validUuidStr = false;
                }
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(uuidFunc);
            if (negated) {
                sink.val('!');
            }
            sink.val("='").val(strFunc).val('\'');
        }
    }
}
