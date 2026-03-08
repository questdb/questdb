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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.std.*;

import static io.questdb.cutlass.pgwire.PGOids.PG_CLASS_OID;
import static io.questdb.cutlass.pgwire.PGOids.PG_NAMESPACE_OID;

public class CastStrToRegClassFunctionFactory implements FunctionFactory {
    private static final CharSequenceObjHashMap<Function> funcMap = new CharSequenceObjHashMap<>();
    private static final CharSequenceIntHashMap valueMap = new CharSequenceIntHashMap(4, 0.6, Numbers.INT_NULL);

    @Override
    public String getSignature() {
        return "cast(Sp)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function arg = args.getQuick(0);
        if (arg.isConstant()) {
            Function result = funcMap.get(arg.getStrA(null));
            if (result != null) {
                return result;
            }
            throw SqlException.$(argPositions.getQuick(0), "unsupported class [name=").put(arg.getStrA(null)).put(']');
        }
        return new CastStrToRegClassFunction(arg);
    }

    private static class CastStrToRegClassFunction extends IntFunction implements UnaryFunction {
        private final Function arg;

        public CastStrToRegClassFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getInt(Record rec) {
            CharSequence val = arg.getStrA(rec);
            if (val != null) {
                return valueMap.get(val);
            }
            return Numbers.INT_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val("::regclass");
        }
    }

    static {
        funcMap.put("pg_namespace", new IntConstant(PG_NAMESPACE_OID));
        funcMap.put("pg_class", new IntConstant(PG_CLASS_OID));

        valueMap.put("pg_namespace", PG_NAMESPACE_OID);
        valueMap.put("pg_class", PG_CLASS_OID);
    }
}
