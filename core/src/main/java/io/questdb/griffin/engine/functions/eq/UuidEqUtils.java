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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Uuid;
import org.jetbrains.annotations.Nullable;

/**
 * Specialized functions for comparing UUIDs with Strings.
 * <p>
 * This is a performance optimization for cases where UUIDs are compared with Strings and vice versa. In such cases
 * we can either convert a UUID to a String or a String to a UUID and then compare. This could be done by automatically
 * by implicit casting, but we want to have a fine control over the conversion process. Why? For example when comparing
 * a UUID column with a constant string we want to convert the constant String into UUID bytes and then compare just the
 * bytes. Implicit casting could naively convert a UUID column to a String column and then use a string comparison
 * function which would be much slower.
 */
final class UuidEqUtils {

    private UuidEqUtils() {
    }

    @Nullable
    static BooleanFunction eqStrUuid(Function strFun, Function uuidFun) {
        if (strFun.isConstant()) {
            CharSequence uuidStr = strFun.getStr(null);
            long lo;
            long hi;
            if (uuidStr == null) {
                lo = Numbers.LONG_NaN;
                hi = Numbers.LONG_NaN;
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
            if (uuidFun.isConstant()) {
                return BooleanConstant.of(hi == uuidFun.getLong128Hi(null) && lo == uuidFun.getLong128Hi(null));
            } else {
                return new ConstStrFun(lo, hi, uuidFun);
            }
        } else {
            if (uuidFun.isConstant()) {
                long lo = uuidFun.getLong128Lo(null);
                long hi = uuidFun.getLong128Hi(null);
                if (Uuid.isNull(lo, hi)) {
                    return new EqStrFunctionFactory.NullCheckFunc(strFun);
                } else {
                    return new ConstUuidFun(lo, hi, strFun);
                }
            } else {
                return new Fun(strFun, uuidFun);
            }
        }
    }

    /**
     * The string function is constant and the UUID function is not constant.
     */
    private static class ConstStrFun extends NegatableBooleanFunction implements UnaryFunction {
        private final long constStrHi;
        private final long constStrLo;
        private final Function fun;

        private ConstStrFun(long constStrLo, long constStrHi, Function fun) {
            this.constStrLo = constStrLo;
            this.constStrHi = constStrHi;
            this.fun = fun;
        }

        @Override
        public Function getArg() {
            return fun;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (constStrHi == fun.getLong128Hi(rec) && constStrLo == fun.getLong128Lo(rec));
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(fun);
            if (negated) {
                sink.val('!');
            }
            sink.val("='").valUuid(constStrLo, constStrHi).val('\'');
        }
    }

    /**
     * The UUID function is constant and the string function is not constant.
     */
    private static class ConstUuidFun extends NegatableBooleanFunction implements UnaryFunction {
        private final long constUuidHi;
        private final long constUuidLo;
        private final Function fun;

        private ConstUuidFun(long constUuidLo, long constUuidHi, Function fun) {
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
            CharSequence uuidStr = fun.getStr(rec);
            if (uuidStr == null) {
                return negated != (constUuidHi == Numbers.LONG_NaN && constUuidLo == Numbers.LONG_NaN);
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
    private static class Fun extends AbstractEqBinaryFunction implements BinaryFunction {
        private Fun(Function strFunction, Function uuidFunction) {
            super(strFunction, uuidFunction);
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence str = left.getStr(rec);
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
}
