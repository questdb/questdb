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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public interface MultiArgFunction extends Function {

    @Override
    default void close() {
        Misc.freeObjList(getArgs());
    }

    ObjList<Function> getArgs();

    @Override
    default void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        Function.init(getArgs(), symbolTableSource, executionContext, null);
    }

    @Override
    default boolean isConstant() {
        ObjList<Function> args = getArgs();
        int n = args.size();
        if (n == 2) {
            return args.getQuick(0).isConstant() && args.getQuick(1).isConstant();
        } else if (n == 3) {
            return args.getQuick(0).isConstant() && args.getQuick(1).isConstant() && args.getQuick(2).isConstant();
        } else if (n == 4) {
            return args.getQuick(0).isConstant() && args.getQuick(1).isConstant() && args.getQuick(2).isConstant() && args.getQuick(3).isConstant();
        } else {
            for (int i = 0; i < n; i++) {
                if (!args.getQuick(i).isConstant()) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    default boolean isNonDeterministic() {
        final ObjList<Function> args = getArgs();
        int n = args.size();
        if (n == 2) {
            return args.getQuick(0).isNonDeterministic() || args.getQuick(1).isNonDeterministic();
        } else if (n == 3) {
            return args.getQuick(0).isNonDeterministic() || args.getQuick(1).isNonDeterministic() || args.getQuick(2).isNonDeterministic();
        } else if (n == 4) {
            return args.getQuick(0).isNonDeterministic() || args.getQuick(1).isNonDeterministic() || args.getQuick(2).isNonDeterministic() || args.getQuick(3).isNonDeterministic();
        } else {
            for (int i = 0; i < n; i++) {
                final Function function = args.getQuick(i);
                if (function.isNonDeterministic()) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    default boolean isRandom() {
        final ObjList<Function> args = getArgs();
        int n = args.size();
        if (n == 2) {
            return args.getQuick(0).isRandom() || args.getQuick(1).isRandom();
        } else if (n == 3) {
            return args.getQuick(0).isRandom() || args.getQuick(1).isRandom() || args.getQuick(2).isRandom();
        } else if (n == 4) {
            return args.getQuick(0).isRandom() || args.getQuick(1).isRandom() || args.getQuick(2).isRandom() || args.getQuick(3).isRandom();
        } else {
            for (int i = 0; i < n; i++) {
                final Function function = args.getQuick(i);
                if (function.isRandom()) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    default boolean isRuntimeConstant() {
        final ObjList<Function> args = getArgs();
        int n = args.size();
        if (n == 2) {
            return (args.getQuick(0).isRuntimeConstant() || args.getQuick(0).isConstant()) &&
                   (args.getQuick(1).isRuntimeConstant() || args.getQuick(1).isConstant());
        } else if (n == 3) {
            return (args.getQuick(0).isRuntimeConstant() || args.getQuick(0).isConstant()) &&
                   (args.getQuick(1).isRuntimeConstant() || args.getQuick(1).isConstant()) &&
                   (args.getQuick(2).isRuntimeConstant() || args.getQuick(2).isConstant());
        } else if (n == 4) {
            return (args.getQuick(0).isRuntimeConstant() || args.getQuick(0).isConstant()) &&
                   (args.getQuick(1).isRuntimeConstant() || args.getQuick(1).isConstant()) &&
                   (args.getQuick(2).isRuntimeConstant() || args.getQuick(2).isConstant()) &&
                   (args.getQuick(3).isRuntimeConstant() || args.getQuick(3).isConstant());
        } else {
            for (int i = 0; i < n; i++) {
                final Function function = args.getQuick(i);
                if (!function.isRuntimeConstant() && !function.isConstant()) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    default boolean isThreadSafe() {
        final ObjList<Function> args = getArgs();
        int n = args.size();
        if (n == 2) {
            return args.getQuick(0).isThreadSafe() && args.getQuick(1).isThreadSafe();
        } else if (n == 3) {
            return args.getQuick(0).isThreadSafe() && args.getQuick(1).isThreadSafe() && args.getQuick(2).isThreadSafe();
        } else if (n == 4) {
            return args.getQuick(0).isThreadSafe() && args.getQuick(1).isThreadSafe() && args.getQuick(2).isThreadSafe() && args.getQuick(3).isThreadSafe();
        } else {
            for (int i = 0; i < n; i++) {
                final Function function = args.getQuick(i);
                if (!function.isThreadSafe()) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    default void offerStateTo(Function that) {
        if (that instanceof MultiArgFunction) {
            ObjList<Function> thatArgs = ((MultiArgFunction) that).getArgs();
            ObjList<Function> thisArgs = getArgs();
            if (thatArgs.size() == thisArgs.size()) {
                for (int i = 0; i < thisArgs.size(); i++) {
                    thisArgs.getQuick(i).offerStateTo(thatArgs.getQuick(i));
                }
            }
        }
    }

    @Override
    default boolean shouldMemoize() {
        final ObjList<Function> args = getArgs();
        int n = args.size();
        if (n == 2) {
            return args.getQuick(0).shouldMemoize() || args.getQuick(1).shouldMemoize();
        } else if (n == 3) {
            return args.getQuick(0).shouldMemoize() || args.getQuick(1).shouldMemoize() || args.getQuick(2).shouldMemoize();
        } else if (n == 4) {
            return args.getQuick(0).shouldMemoize() || args.getQuick(1).shouldMemoize() || args.getQuick(2).shouldMemoize() || args.getQuick(3).shouldMemoize();
        } else {
            for (int i = 0; i < n; i++) {
                final Function function = args.getQuick(i);
                if (function.shouldMemoize()) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    default boolean supportsParallelism() {
        final ObjList<Function> args = getArgs();
        int n = args.size();
        if (n == 2) {
            return args.getQuick(0).supportsParallelism() && args.getQuick(1).supportsParallelism();
        } else if (n == 3) {
            return args.getQuick(0).supportsParallelism() && args.getQuick(1).supportsParallelism() && args.getQuick(2).supportsParallelism();
        } else if (n == 4) {
            return args.getQuick(0).supportsParallelism() && args.getQuick(1).supportsParallelism() && args.getQuick(2).supportsParallelism() && args.getQuick(3).supportsParallelism();
        } else {
            for (int i = 0; i < n; i++) {
                final Function function = args.getQuick(i);
                if (!function.supportsParallelism()) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    default void toPlan(PlanSink sink) {
        sink.val(getName()).val('(').val(getArgs()).val(')');
    }

    @Override
    default void toTop() {
        GroupByUtils.toTop(getArgs());
    }
}
