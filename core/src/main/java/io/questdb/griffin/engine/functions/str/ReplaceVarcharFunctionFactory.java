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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ReplaceVarcharFunctionFactory implements FunctionFactory {

    private static final String SIGNATURE = "replace(ØØØ)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position, ObjList<Function> args, IntList argPositions,
            CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext
    ) {
        final Function replaceWith = args.getQuick(2);
        if (replaceWith.isConstant() && replaceWith.getVarcharSize(null) < 0) {
            return VarcharConstant.NULL;
        }
        final Function lookFor = args.getQuick(1);
        if (lookFor.isConstant()) {
            if (lookFor.getVarcharSize(null) < 0) {
                return VarcharConstant.NULL;
            } else if (lookFor.getVarcharSize(null) == 0) {
                return args.getQuick(0);
            }
        }

        final int maxSize = configuration.getStrFunctionMaxBufferLength();
        final Function value = args.getQuick(0);
        if (value.isConstant()) {
            final Utf8Sequence valueValue = value.getVarcharA(null);
            if (valueValue == null) {
                return value;
            }

            if (lookFor.isConstant() && replaceWith.isConstant()) {
                try {
                    return new VarcharConstant(
                            replace(
                                    valueValue,
                                    lookFor.getVarcharA(null),
                                    replaceWith.getVarcharA(null),
                                    new Utf8StringSink(4),
                                    maxSize
                            ));
                } catch (CairoException e) {
                    // We can get an exception if the output string exceeds the size limit.
                    // However, factory.newInstance() shouldn't throw it, so ignore the exception
                    // here and let the call proceed to return a non-constant function.
                    // The same exception will then pop up when the function is called.
                }
            }

        }

        return new Func(value, lookFor, replaceWith, maxSize);
    }

    static void checkSizeLimit(int size, int maxSize) {
        if (size > maxSize) {
            throw CairoException.nonCritical()
                    .put("breached memory limit set for ").put(SIGNATURE)
                    .put(" [maxSize=").put(maxSize)
                    .put(", requiredSize=").put(size).put(']');
        }
    }

    // if result is null, return null; otherwise return sink
    static <T extends Utf8Sink> @Nullable T replace(
            @NotNull Utf8Sequence value, Utf8Sequence lookFor, Utf8Sequence replaceWith, T sink, int maxSize
    ) throws CairoException {
        int valueSize = value.size();
        if (valueSize < 1) {
            return sink;
        }
        if (lookFor == null || replaceWith == null) {
            return null;
        }
        checkSizeLimit(valueSize, maxSize);
        final int lookForSize = lookFor.size();
        if (lookForSize < 1) {
            sink.putAny(value);
            return sink;
        }
        final int replaceWithSize = replaceWith.size();

        int i = 0;
        int curLen = 0;
        OUTER:
        while (i <= valueSize - lookForSize) {
            final byte bi = value.byteAt(i);
            int k = 0;
            byte bk = bi;
            while (true) {
                if (bk != lookFor.byteAt(k)) {
                    i++;
                    curLen++;
                    checkSizeLimit(curLen, maxSize);
                    sink.putAny(bi);
                    continue OUTER;
                }
                k++;
                if (k == lookForSize) {
                    break;
                }
                bk = value.byteAt(i + k);
            }
            i += lookForSize;
            curLen += replaceWithSize;
            checkSizeLimit(curLen, maxSize);
            sink.putAny(replaceWith);
        }
        curLen++;
        checkSizeLimit(curLen, maxSize);
        sink.putAny(value, i, valueSize);
        return sink;
    }

    private static class Func extends VarcharFunction implements TernaryFunction {
        private final Function lookFor;
        private final int maxSize;
        private final Function replaceWith;
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();
        private final Function value;

        public Func(Function value, Function lookFor, Function replaceWith, int maxSize) {
            this.value = value;
            this.lookFor = lookFor;
            this.replaceWith = replaceWith;
            this.maxSize = maxSize;
        }

        @Override
        public Function getCenter() {
            return lookFor;
        }

        @Override
        public Function getLeft() {
            return value;
        }

        @Override
        public Function getRight() {
            return replaceWith;
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            final Utf8Sequence value = this.value.getVarcharA(rec);
            if (value != null) {
                sinkA.clear();
                return replace(value, lookFor.getVarcharA(rec), replaceWith.getVarcharA(rec), sinkA, maxSize);
            }
            return null;
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            final Utf8Sequence value = this.value.getVarcharB(rec);
            if (value != null) {
                sinkB.clear();
                return replace(value, lookFor.getVarcharB(rec), replaceWith.getVarcharB(rec), sinkB, maxSize);
            }
            return null;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("replace(").val(value).val(',').val(lookFor).val(',').val(replaceWith).val(')');
        }
    }
}
