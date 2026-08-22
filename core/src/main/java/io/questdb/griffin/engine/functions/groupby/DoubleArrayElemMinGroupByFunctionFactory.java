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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class DoubleArrayElemMinGroupByFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "array_elem_min(D[])";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new DoubleArrayElemMinGroupByFunction(args.getQuick(0), configuration);
    }

    private static final class DoubleArrayElemMinGroupByFunction extends AbstractDoubleArrayElemAggGroupByFunction {

        public DoubleArrayElemMinGroupByFunction(@NotNull Function arg, @NotNull CairoConfiguration configuration) {
            super(arg, configuration);
        }

        @Override
        public String getName() {
            return "array_elem_min";
        }

        @Override
        protected void accumulateOne(long dataPtr, int accFi, double inputVal) {
            long addr = dataPtr + (long) accFi * Double.BYTES;
            double accVal = Unsafe.getDouble(addr);
            Unsafe.putDouble(addr, Numbers.isFinite(accVal) ? Math.min(accVal, inputVal) : inputVal);
        }

        @Override
        protected void mergeOne(long destDataPtr, int destFi, double srcVal, int srcFi) {
            long addr = destDataPtr + (long) destFi * Double.BYTES;
            double destVal = Unsafe.getDouble(addr);
            Unsafe.putDouble(addr, Numbers.isFinite(destVal) ? Math.min(destVal, srcVal) : srcVal);
        }
    }
}
