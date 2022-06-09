/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class CastLong256ToStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Hs)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return null;
    }

    public static class CastLong256ToStrFunction extends StrFunction {
        private final Function arg;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public CastLong256ToStrFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public CharSequence getStr(Record rec) {
            return toSink(rec, sinkA);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return toSink(rec, sinkB);
        }

        private StringSink toSink(Record rec, StringSink sinkA) {
            sinkA.clear();
            arg.getLong256(rec, sinkA);
            return sinkA;
        }
    }
}
