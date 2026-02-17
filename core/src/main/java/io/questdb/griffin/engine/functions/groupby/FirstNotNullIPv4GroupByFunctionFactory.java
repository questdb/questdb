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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class FirstNotNullIPv4GroupByFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "first_not_null(X)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new Func(args.getQuick(0));
    }

    private static class Func extends FirstIPv4GroupByFunction {
        public Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeBatch(MapValue mapValue, long ptr, int count) {
            if (count > 0) {
                final long hi = ptr + count * 4L;
                for (; ptr < hi; ptr += 4L) {
                    int value = Unsafe.getUnsafe().getInt(ptr);
                    if (value != Numbers.IPv4_NULL) {
                        mapValue.putInt(valueIndex + 1, value);
                        break;
                    }
                }
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            if (mapValue.getIPv4(valueIndex + 1) == Numbers.IPv4_NULL) {
                computeFirst(mapValue, record, rowId);
            }
        }

        @Override
        public String getName() {
            return "first_not_null";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            int srcVal = srcValue.getIPv4(valueIndex + 1);
            if (srcVal == Numbers.IPv4_NULL) {
                return;
            }
            long srcRowId = srcValue.getLong(valueIndex);
            long destRowId = destValue.getLong(valueIndex);
            // srcRowId is non-null at this point since we know that the value is non-null
            if (srcRowId < destRowId || destRowId == Numbers.LONG_NULL || destValue.getIPv4(valueIndex + 1) == Numbers.IPv4_NULL) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putInt(valueIndex + 1, srcVal);
            }
        }
    }
}
