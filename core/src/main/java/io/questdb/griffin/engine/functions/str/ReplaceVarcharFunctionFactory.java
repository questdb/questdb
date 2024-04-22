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

        final Function value = args.getQuick(0);
        if (value.isConstant()) {
            int len = value.getVarcharSize(null);
            if (len < 1) {
                return value;
            }
        }

        final int maxLength = configuration.getStrFunctionMaxBufferLength();
        return new Func(value, lookFor, replaceWith, maxLength);
    }

    private static class Func extends VarcharFunction implements TernaryFunction {

        private final Function lookFor;
        private final int maxSize;
        private final Function replaceWith;
        private final Utf8StringSink sink = new Utf8StringSink();
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
        public void getVarchar(Record rec, Utf8Sink sink) {
            final Utf8Sequence value = this.value.getVarcharA(rec);
            if (value != null) {
                replace(value, lookFor.getVarcharA(rec), replaceWith.getVarcharA(rec), sink);
            }
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            final Utf8Sequence value = this.value.getVarcharA(rec);
            if (value != null) {
                sink.clear();
                return replace(value, lookFor.getVarcharA(rec), replaceWith.getVarcharA(rec), sink);
            }
            return null;
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            final Utf8Sequence value = this.value.getVarcharB(rec);
            if (value != null) {
                sinkB.clear();
                return replace(value, lookFor.getVarcharB(rec), replaceWith.getVarcharB(rec), sinkB);
            }
            return null;
        }

        @Override
        public boolean isConstant() {
            return value.isConstant() && lookFor.isConstant() && replaceWith.isConstant();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("replace(").val(value).val(',').val(lookFor).val(',').val(replaceWith).val(')');
        }

        private void checkSizeLimit(int size) {
            if (size > maxSize) {
                throw CairoException.nonCritical()
                        .put("breached memory limit set for ").put(SIGNATURE)
                        .put(" [maxSize=").put(maxSize)
                        .put(", requiredSize=").put(size).put(']');
            }
        }

        // if result is null, return null; otherwise return sink
        private <T extends Utf8Sink> T replace(
                @NotNull Utf8Sequence value, Utf8Sequence lookFor, Utf8Sequence replaceWith, T sink
        ) throws CairoException {
            int valueSize = value.size();
            if (valueSize < 1) {
                return sink;
            }
            if (lookFor == null || replaceWith == null) {
                return null;
            }
            checkSizeLimit(valueSize);
            final int lookForSize = lookFor.size();
            if (lookForSize < 1) {
                sink.put(value);
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
                        checkSizeLimit(curLen);
                        sink.put(bi);
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
                checkSizeLimit(curLen);
                sink.put(replaceWith);
            }
            curLen++;
            checkSizeLimit(curLen);
            sink.put(value, i, valueSize);
            return sink;
        }
    }
}
