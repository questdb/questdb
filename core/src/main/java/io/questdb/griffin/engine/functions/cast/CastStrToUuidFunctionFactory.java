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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.UuidFunction;
import io.questdb.griffin.engine.functions.constants.UuidConstant;
import io.questdb.std.*;

public final class CastStrToUuidFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Sz)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (args.getQuick(0).isConstant()) {
            final CharSequence value = args.getQuick(0).getStr(null);
            if (value == null || value.length() == 0) {
                return UuidConstant.NULL;
            }
            final Uuid uuid = new Uuid();
            try {
                uuid.of(value);
            } catch (NumericException e) {
                throw SqlException.$(argPositions.getQuick(0), "invalid UUID constant");
            }
            return new UuidConstant(uuid);
        }
        return new Func(args.getQuick(0));
    }

    public static class Func extends UuidFunction implements UnaryFunction {
        private final Function arg;

        public Func(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getLong128Hi(Record rec) {
            final CharSequence value = arg.getStr(rec);
            if (value == null) {
                return Numbers.LONG_NaN;
            }
            try {
                Uuid.checkDashesAndLength(value);
                return Uuid.parseHi(value);
            } catch (NumericException e) {
                return Numbers.LONG_NaN;
            }
        }

        @Override
        public long getLong128Lo(Record rec) {
            final CharSequence value = arg.getStr(rec);
            if (value == null) {
                return Numbers.LONG_NaN;
            }
            try {
                Uuid.checkDashesAndLength(value);
                return Uuid.parseLo(value);
            } catch (NumericException e) {
                return Numbers.LONG_NaN;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getArg()).val("::uuid");
        }
    }
}
