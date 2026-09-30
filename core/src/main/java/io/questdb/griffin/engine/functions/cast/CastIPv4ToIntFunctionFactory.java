/*+*****************************************************************************
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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class CastIPv4ToIntFunctionFactory implements FunctionFactory {

    public static int value(int operand) {
        return operand;
    }

    @Override
    public String getSignature() {
        return "cast(Xi)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new CastIPv4ToIntFunction(args.getQuick(0));
    }

    private static class CastIPv4ToIntFunction extends AbstractCastToIntFunction {

        public CastIPv4ToIntFunction(Function arg) {
            super(arg);
        }

        @Override
        public int getInt(Record rec) {
            // Read once: the mirror of the int -> IPv4 direction. A non-deterministic argument
            // returns a different value on every call, so testing one draw and returning another
            // turned a NULL into the address 0 and an address into NULL.
            final int val = arg.getIPv4(rec);
            return val == Numbers.IPv4_NULL ? Numbers.INT_NULL : value(val);
        }
    }
}
