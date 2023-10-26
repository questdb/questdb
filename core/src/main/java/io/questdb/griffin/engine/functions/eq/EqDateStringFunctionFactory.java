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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public final class EqDateStringFunctionFactory implements FunctionFactory {
    
    @Override
    public String getSignature() {
        return "=(Ms)";
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
        CharSequence str = args.getQuick(1).getStr(null);
        long millis = SqlUtil.implicitCastStrAsDate(str);
        return new EqDateStringFunction(args.getQuick(0), millis);
    }

    private static class EqDateStringFunction extends NegatableBooleanFunction {
        private final Function left;
        private final long millis;

        public EqDateStringFunction(Function left, long millis) {
            this.left = left;
            this.millis = millis;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (left.getDate(rec) == millis);
        }

        @Override
        public void close() {
            left.close();
        }

        @Override
        public boolean isConstant() {
            return left.isConstant();
        }

        @Override
        public boolean isReadThreadSafe() {
            return left.isReadThreadSafe();
        }

        @Override
        public void initCursor() {
            left.initCursor();
        }

        @Override
        public boolean isRuntimeConstant() {
            return left.isRuntimeConstant() || left.isConstant();
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            left.init(symbolTableSource, executionContext);
        }

        @Override
        public void toTop() {
            left.toTop();
        }
    }
}
