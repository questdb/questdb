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

package io.questdb.griffin.engine.functions.array;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

// This is a hack. pg_index.indkey in PG is an int vector. We have it as INT
// because we don't support array column type yet. Some metadata PG SQLs
// dereference this column as an array. We do not care, because composite indexes are
// not yet supported either
public class IntArrayDereferenceHackFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "[](II)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new StrArrayDereferenceFunction(args.getQuick(0), args.getQuick(1));
    }

    private static class StrArrayDereferenceFunction extends IntFunction implements BinaryFunction {
        private final Function arrayFunction;
        private final Function indexFunction;

        public StrArrayDereferenceFunction(Function arrayFunction, Function indexFunction) {
            this.arrayFunction = arrayFunction;
            this.indexFunction = indexFunction;
        }

        @Override
        public int getInt(Record rec) {
            return 0;
        }

        @Override
        public Function getLeft() {
            return arrayFunction;
        }

        @Override
        public Function getRight() {
            return indexFunction;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arrayFunction).val('[').val(indexFunction).val(']');
        }
    }
}
