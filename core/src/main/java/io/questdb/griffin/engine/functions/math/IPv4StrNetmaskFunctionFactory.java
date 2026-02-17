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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.IPv4Function;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.IPv4Constant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class IPv4StrNetmaskFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "netmask(S)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function strFunc = args.getQuick(0);
        if (strFunc.isConstant()) {
            final CharSequence str = strFunc.getStrA(null);
            if (str == null) {
                return IPv4Constant.NULL;
            }
            final int val = Numbers.getIPv4Netmask(str);
            return val == Numbers.BAD_NETMASK ? IPv4Constant.NULL : IPv4Constant.newInstance(val);
        }
        return new Func(strFunc);
    }

    private static class Func extends IPv4Function implements UnaryFunction {
        private final Function strFunc;

        public Func(Function strFunc) {
            this.strFunc = strFunc;
        }

        @Override
        public Function getArg() {
            return strFunc;
        }

        @Override
        public int getIPv4(Record rec) {
            final CharSequence str = strFunc.getStrA(rec);
            if (str == null) {
                return Numbers.IPv4_NULL;
            }
            final int val = Numbers.getIPv4Netmask(str);
            return val == Numbers.BAD_NETMASK ? Numbers.IPv4_NULL : val;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("netmask(").val(strFunc).val(')');
        }
    }
}
