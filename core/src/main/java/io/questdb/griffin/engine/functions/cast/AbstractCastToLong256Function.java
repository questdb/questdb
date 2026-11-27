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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract base class for functions that cast values to long256.
 */
public abstract class AbstractCastToLong256Function extends Long256Function implements CastFunction {
    protected final Function arg;

    public AbstractCastToLong256Function(Function arg) {
        this.arg = arg;
    }

    /**
     * Replaces a BYTE, SHORT, INT or LONG argument at {@code index} with its implicit cast to LONG256 and returns
     * the factory of that cast; leaves any other argument as is and returns null.
     */
    public static @Nullable Class<? extends FunctionFactory> widenInteger(ObjList<Function> args, int index) {
        final Function arg = args.getQuick(index);
        final Class<? extends FunctionFactory> factory;
        switch (ColumnType.tagOf(arg.getType())) {
            case ColumnType.BYTE -> {
                args.setQuick(index, new CastByteToLong256FunctionFactory.Func(arg));
                factory = CastByteToLong256FunctionFactory.class;
            }
            case ColumnType.SHORT -> {
                args.setQuick(index, new CastShortToLong256FunctionFactory.Func(arg));
                factory = CastShortToLong256FunctionFactory.class;
            }
            case ColumnType.INT -> {
                args.setQuick(index, new CastIntToLong256FunctionFactory.Func(arg));
                factory = CastIntToLong256FunctionFactory.class;
            }
            case ColumnType.LONG -> {
                args.setQuick(index, new CastLongToLong256FunctionFactory.Func(arg));
                factory = CastLongToLong256FunctionFactory.class;
            }
            default -> factory = null;
        }
        return factory;
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public boolean isThreadSafe() {
        // All concrete subclasses hold a mutable Long256Impl.
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(getArg()).val("::long256");
    }
}
