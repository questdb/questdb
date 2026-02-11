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

package io.questdb.griffin.engine.functions.bool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

/**
 * Handles "str IN $1" where $1 is a VARCHAR[] bind variable.
 * <p>
 * VARCHAR[] is currently only supported as a bind variable type in PGWire protocol,
 * passed via the BIND message. There is no SQL literal syntax for VARCHAR arrays.
 */
public class InStrVarcharArrayFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "in(SØ[])";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function arrayFunc = args.getQuick(1);
        if (!arrayFunc.isConstant() && !arrayFunc.isRuntimeConstant()) {
            throw SqlException.$(argPositions.getQuick(1), "constant or bind variable expected");
        }
        return new Func(args.getQuick(0), arrayFunc);
    }

    private static class Func extends BooleanFunction implements BinaryFunction {
        private final Function arrayFunc;
        private final Function strFunc;
        private CharSequenceHashSet set;
        private boolean stateInherited = false;

        public Func(Function strFunc, Function arrayFunc) {
            this.strFunc = strFunc;
            this.arrayFunc = arrayFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            final CharSequence val = strFunc.getStrA(rec);
            return set.contains(val);
        }

        @Override
        public Function getLeft() {
            return strFunc;
        }

        @Override
        public Function getRight() {
            return arrayFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);
            if (stateInherited) {
                return;
            }

            if (set == null) {
                set = new CharSequenceHashSet();
            }
            set.clear();
            ArrayView arrayView = arrayFunc.getArray(null);
            for (int i = 0, n = arrayView.getCardinality(); i < n; i++) {
                Utf8Sequence element = arrayView.getVarchar(i);
                if (element == null) {
                    set.addNull();
                } else {
                    set.add(Utf8s.toString(element));
                }
            }
        }

        @Override
        public void offerStateTo(Function that) {
            BinaryFunction.super.offerStateTo(that);
            if (that instanceof InStrVarcharArrayFunctionFactory.Func other) {
                other.set = this.set;
                other.stateInherited = true;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(strFunc).val(" in ").val(arrayFunc);
        }
    }
}
