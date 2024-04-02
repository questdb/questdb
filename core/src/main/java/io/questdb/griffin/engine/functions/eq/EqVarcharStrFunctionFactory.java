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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

public class EqVarcharStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "=(ØS)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function varchar = args.getQuick(0);
        final Function str = args.getQuick(1);

        return newInstance(str, varchar);
    }

    private Function createStrConstantFunc(Function constFunc, Function varFunc) {
        Utf8Sequence constValue = constFunc.getVarcharA(null);
        if (constValue == null) {
            return new EqStrFunctionFactory.NullCheckFunc(varFunc);
        }
        return new EqStrFunctionFactory.ConstCheckFunc(varFunc, Utf8s.toString(constValue));
    }

    private Function createVarcharConstantFunc(Function a, Function b) {
        CharSequence constValue = a.getStrA(null);
        if (constValue == null) {
            return new EqVarcharFunctionFactory.NullCheckFunc(b);
        }
        Utf8String utf8ConstValue = new Utf8String(constValue);
        return new EqVarcharFunctionFactory.ConstCheckFunc(b, utf8ConstValue);
    }

    @NotNull
    protected Function newInstance(Function str, Function varchar) {
        // there are optimisation opportunities
        if (str.isConstant() && !varchar.isConstant()) {
            // str is constant, varchar is not
            return createVarcharConstantFunc(str, varchar);
        }

        if (!str.isConstant() && varchar.isConstant()) {
            // str is not constant, varchar is constant
            return createStrConstantFunc(varchar, str);
        }

        return new Func(str, varchar);
    }

    private static class Func extends AbstractEqBinaryFunction {
        public Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            // important to compare A and B varchars in case
            // these are columns of the same record
            // records have re-usable character sequences
            final CharSequence a = left.getStrA(rec);
            final Utf8Sequence b = right.getVarcharB(rec);

            if (a == null) {
                return negated != (b == null);
            }

            return negated != Utf8s.equalsUtf16Nc(a, b);
        }

        @Override
        public String getName() {
            if (negated) {
                return "!=";
            } else {
                return "=";
            }
        }
    }
}
