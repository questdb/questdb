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
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.UuidFunction;
import io.questdb.griffin.engine.functions.constants.UuidConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import java.util.UUID;

public final class CastStrToUuidFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Sz)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return new Func(args.getQuick(0));
    }

    private static class Func extends UuidFunction implements UnaryFunction {
        private final Function arg;

        public Func(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getUuidLeastSig(Record rec) {
            final CharSequence value = arg.getStr(rec);
            if (value == null) {
                return UuidConstant.NULL_LEAST_SIG;
            }
            // TODO: This is a horrible hack to make the UUID tests to pass.
            // We must reimplement this with a proper zero-gc UUID codec
            UUID uuid = UUID.fromString(value.toString());
            return uuid.getLeastSignificantBits();
        }

        @Override
        public long getUuidMostSig(Record rec) {
            final CharSequence value = arg.getStr(rec);
            if (value == null) {
                return UuidConstant.NULL_MOST_SIG;
            }
            // TODO: This is a horrible hack to make the UUID tests to pass.
            // We must reimplement this with a proper zero-gc UUID codec.
            UUID uuid = UUID.fromString(value.toString());
            return uuid.getMostSignificantBits();
        }
    }
}
