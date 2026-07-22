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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.LongWidthIntFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class NullIfIntFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "nullif(II)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new NullIfIntFunction(args.getQuick(0), args.getQuick(1));
    }

    private static class NullIfIntFunction extends LongWidthIntFunction implements BinaryFunction {
        private final Function intFunc1;
        private final Function intFunc2;
        private final boolean isFunc1IntWidthStable;
        private final boolean isFunc1RowStable;

        public NullIfIntFunction(Function intFunc1, Function intFunc2) {
            this.intFunc1 = intFunc1;
            this.intFunc2 = intFunc2;
            this.isFunc1IntWidthStable = intFunc1.isIntWidthStable();
            this.isFunc1RowStable = intFunc1.isRowStable();
        }

        @Override
        public int getInt(Record rec) {
            // Read once: a second read of a non-deterministic argument is a fresh draw, and
            // returning that draw hands back the very value the comparison just excluded.
            final int value = intFunc1.getInt(rec);
            return value == intFunc2.getInt(rec) ? Numbers.INT_NULL : value;
        }

        @Override
        public Function getLeft() {
            return intFunc1;
        }

        @Override
        public long getLong(Record rec) {
            // A row-unstable overflowing argument cannot be read at both widths at all, so the
            // first arm compares at long width. The two arms after it compare at INT width, exactly
            // as getInt() does, so both getters null out the same rows there. An overflowing INT
            // argument needs both of its widths for that - the narrow one to compare, the wide one
            // to return - so it is read twice only when it is row stable.
            if (!isFunc1IntWidthStable && !isFunc1RowStable) {
                // A second read of the first argument would be a fresh draw, so the whole comparison
                // moves to long width and each argument is read once there. Both sides must move
                // together: comparing a 64-bit value against the wrapped 32-bit read of the other
                // argument misses an equal pair, and nullif would return the value it excludes.
                // Reading a width-stable argument at long width costs nothing - IntFunction.getLong()
                // is Numbers.intToLong(getInt()) - so this narrows to the same comparison for it.
                final long wide = intFunc1.getLong(rec);
                return wide == intFunc2.getLong(rec) ? Numbers.LONG_NULL : wide;
            }
            final int value = intFunc1.getInt(rec);
            if (value == intFunc2.getInt(rec)) {
                return Numbers.LONG_NULL;
            }
            // The read above carries a width-stable argument's whole value, but INT_NULL is a
            // sentinel rather than a number: sign-extending it would turn a NULL into a real
            // -2147483648 at every 64-bit read, the store path included.
            return isFunc1IntWidthStable ? Numbers.intToLong(value) : intFunc1.getLong(rec);
        }

        @Override
        public Function getRight() {
            return intFunc2;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("nullif(").val(intFunc1).val(',').val(intFunc2).val(')');
        }
    }
}
