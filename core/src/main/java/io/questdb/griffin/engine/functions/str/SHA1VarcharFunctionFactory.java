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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Digest;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

public class SHA1VarcharFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "sha1(Ø)";
    }

    @Override
    public Function newInstance(int position,
                                ObjList<Function> args,
                                IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext) {

        Function func = args.get(0);
        return new SHA1VarcharFunctionFactory.SHA1Func(func);
    }

    private static class SHA1Func extends VarcharFunction implements UnaryFunction {
        private final Function data;
        private final Digest hashFn = new Digest(Digest.DigestAlgorithm.SHA1);
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();

        public SHA1Func(final Function data) {
            this.data = data;
        }

        @Override
        public Function getArg() {
            return data;
        }

        @Override
        public Utf8Sequence getVarcharA(final Record rec) {
            final Utf8Sequence sequence = getArg().getVarcharA(rec);
            if (sequence == null) {
                return null;
            }
            sinkA.clear();
            this.hashFn.hash(sequence, sinkA);
            return sinkA;
        }

        @Override
        public Utf8Sequence getVarcharB(final Record rec) {
            final Utf8Sequence sequence = getArg().getVarcharB(rec);
            if (sequence == null) {
                return null;
            }
            sinkB.clear();
            this.hashFn.hash(sequence, sinkB);
            return sinkB;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("sha1(").val(data).val(')');
        }
    }
}
