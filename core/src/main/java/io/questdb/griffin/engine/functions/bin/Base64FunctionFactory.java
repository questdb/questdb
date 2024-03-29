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

package io.questdb.griffin.engine.functions.bin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;

public class Base64FunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "base64(Ui)";
    }

    @Override
    public Function newInstance(int position,
                                ObjList<Function> args,
                                IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext) throws SqlException {

        final int maxLength = args.get(1).getInt(null);

        if (maxLength < 1) {
            throw SqlException.$(argPositions.getQuick(1), "maxLength has to be greater than 0");
        }

        Function func = args.get(0);
        final int length = Math.min(maxLength, configuration.getBinaryEncodingMaxLength());
        return new Base64Func(func, length);
    }

    private static class Base64Func extends StrFunction implements UnaryFunction {
        private final Function data;
        private final int maxLength;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public Base64Func(final Function data, final int maxLength) {
            this.data = data;
            this.maxLength = maxLength;
        }

        @Override
        public Function getArg() {
            return data;
        }

        @Override
        public void getStr(Record rec, Utf16Sink utf16Sink) {
            final BinarySequence sequence = getArg().getBin(rec);
            Chars.base64Encode(sequence, this.maxLength, utf16Sink);
        }

        @Override
        public CharSequence getStrA(final Record rec) {
            final BinarySequence sequence = getArg().getBin(rec);
            sinkA.clear();
            Chars.base64Encode(sequence, this.maxLength, sinkA);
            return sinkA;
        }

        @Override
        public CharSequence getStrB(final Record rec) {
            final BinarySequence sequence = getArg().getBin(rec);
            sinkB.clear();
            Chars.base64Encode(sequence, this.maxLength, sinkB);
            return sinkB;
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("base64(").val(data).val(',').val(maxLength).val(')');
        }
    }
}
