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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.IPv4Constant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;

public class CastVarcharToIPv4FunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Øx)";
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
            final Utf8Sequence value = arg.getVarcharA(null);
            if (value == null || value.size() == 0) {
                return IPv4Constant.NULL;
            }
            try {
                final int ip = Numbers.parseIPv4(value);
                return IPv4Constant.newInstance(ip);
            } catch (NumericException e) {
                throw SqlException.$(argPositions.getQuick(0), "invalid IPv4 constant");
            }
        }
        return new Func(arg);
    }

    private static class Func extends AbstractCastToIPv4Function {

        public Func(Function arg) {
            super(arg);
        }

        @Override
        public int getIPv4(Record rec) {
            final Utf8Sequence value = arg.getVarcharA(rec);
            return Numbers.parseIPv4Quiet(value != null ? value.asAsciiCharSequence() : null);
        }
    }
}
