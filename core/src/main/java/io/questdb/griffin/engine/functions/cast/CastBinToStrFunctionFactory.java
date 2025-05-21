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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;


public class CastBinToStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Us)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        // expression parser will only allow casting 'null' to binary, nothing else.
        return new CastBinToStrFunctionFactory.Func(args.getQuick(0));
    }

    public static class Func extends AbstractCastToStrFunction implements UnaryFunction {
        StringSink sinkA = new StringSink();
        StringSink sinkB = new StringSink();

        public Func(Function arg) {
            super(arg);
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getBinLen(Record rec) {
            CharSequence cs = arg.getStrA(rec);
            if (cs == null) {
                return -1;
            } else {
                return getBin(rec).length();
            }
        }

        public CharSequence getStr(Record rec, StringSink sink) {
            sink.clear();
            BinarySequence bs = arg.getBin(rec);
            if (bs == null) {
                return null;
            } else {
                sink.putAscii("'\\x").putHex(bs).putAscii('\'');
            }
            return sink;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return getStr(rec, sinkA);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStr(rec, sinkB);
        }

        @Override
        public int getStrLen(Record rec) {
            CharSequence cs = getStrA(rec);
            if (cs == null) {
                return -1;
            } else {
                return cs.length();
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val("::string");
        }
    }
}
