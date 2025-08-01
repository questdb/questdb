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
        final ObjList<Function> args = getArgs();
        final int n = args.size();
        switch (n) {
            case 0:
                return true;
            case 1:
                return args.getQuick(0).isConstant();
            case 2:
                return args.getQuick(0).isConstant()
                        && args.getQuick(1).isConstant();
            case 3:
                return args.getQuick(0).isConstant()
                        && (args.getQuick(1).isConstant()
                        && args.getQuick(2).isConstant());
            case 4:
                return args.getQuick(0).isConstant()
                        && (args.getQuick(1).isConstant()
                        && (args.getQuick(2).isConstant()
                        && args.getQuick(3).isConstant()));
        }
        for (int i = 0; i < n; i++) {
            if (!args.getQuick(i).isConstant()) {
                return false;
            }
        }
        return true;
    }

    @Override
    default boolean isNonDeterministic() {
        final ObjList<Function> args = getArgs();
        final int n = args.size();
        switch (n) {
            case 0:
                return false;
            case 1:
                return args.getQuick(0).isNonDeterministic();
            case 2:
                return args.getQuick(0).isNonDeterministic()
                        || args.getQuick(1).isNonDeterministic();
            case 3:
                return args.getQuick(0).isNonDeterministic()
                        || args.getQuick(1).isNonDeterministic()
                        || args.getQuick(2).isNonDeterministic();
            case 4:
                return args.getQuick(0).isNonDeterministic()
                        || args.getQuick(1).isNonDeterministic()
                        || args.getQuick(2).isNonDeterministic()
                        || args.getQuick(3).isNonDeterministic();
        }
        for (int i = 0; i < n; i++) {
            if (args.getQuick(i).isNonDeterministic()) {
                return true;
            }
        }
        return false;
    }

    //todo delete after code review
    default boolean isRandom1() {
        final ObjList<Function> args = getArgs();
        final int n = args.size();
        switch (n) {
            case 0:
                return false;
            case 1:
                return args.getQuick(0).isRandom();
            case 2:
                return args.getQuick(0).isRandom()
                        || args.getQuick(1).isRandom();
            case 3:
                return args.getQuick(0).isRandom()
                        || args.getQuick(1).isRandom()
                        || args.getQuick(2).isRandom();
            case 4:
                return args.getQuick(0).isRandom()
                        || args.getQuick(1).isRandom()
                        || args.getQuick(2).isRandom()
                        || args.getQuick(3).isRandom();
        }
        for (int i = 0; i < n; i++) {
            if (args.getQuick(i).isRandom()) {
                return true;
            }
        }
        return false;
    }

    @Override
    default boolean isRandom() {
        final ObjList<Function> args = getArgs();
        final int n = args.size();
        int i = 0;
        switch (n) {
            case 0:
                return false;
            case 1:
                return args.getQuick(i).isRandom();
            case 2:
                return args.getQuick(i++).isRandom()
                        || args.getQuick(i).isRandom();
            case 3:
                return args.getQuick(i++).isRandom()
                        || args.getQuick(i++).isRandom()
                        || args.getQuick(i).isRandom();
            case 4:
                return args.getQuick(i++).isRandom()
                        || args.getQuick(i++).isRandom()
                        || args.getQuick(i++).isRandom()
                        || args.getQuick(i).isRandom();
        }
        for (; i < n; i++) {
            if (args.getQuick(i).isRandom()) {
                return true;
            }
        }
        return false;
    }

    @Override
    default boolean isRuntimeConstant() {
        final ObjList<Function> args = getArgs();
        final int n = args.size();
        switch (n) {
            case 0:
                return true;
            case 1:
                final Function function = args.getQuick(0);
                return function.isRuntimeConstant() || function.isConstant();
        }
        for (int i = 0; i < n; i++) {
            final Function function = args.getQuick(i);
            if (!function.isRuntimeConstant() && !function.isConstant()) {
                return false;
            }
        }
        return true;
    }

    @Override
    default boolean isThreadSafe() {
        final ObjList<Function> args = getArgs();
        final int n = args.size();
        switch (n) {
            case 0:
                return true;
            case 1:
                return args.getQuick(0).isThreadSafe();
            case 2:
                return args.getQuick(0).isThreadSafe()
                        && args.getQuick(1).isThreadSafe();
            case 3:
                return args.getQuick(0).isThreadSafe()
                        && (args.getQuick(1).isThreadSafe()
                        && args.getQuick(2).isThreadSafe());
            default:
                if (!args.getQuick(0).isThreadSafe()) {
                    return false;
                }
                if (!args.getQuick(1).isThreadSafe()) {
                    return false;
                }
                if (!args.getQuick(2).isThreadSafe()) {
                    return false;
                }
                if (!args.getQuick(3).isThreadSafe()) {
                    return false;
                }
        }
        for (int i = 4; i < n; i++) {
            final Function function = args.getQuick(i);
            if (!function.isThreadSafe()) {
                return false;
            }
        }
        return true;
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
        for (int i = 0, n = args.size(); i < n; i++) {
            final Function function = args.getQuick(i);
            if (function.shouldMemoize()) {
                return true;
            }
        }
        return false;
    }

    @Override
    default boolean supportsParallelism() {
        final ObjList<Function> args = getArgs();
        final int n = args.size();
        switch (n) {
            case 0:
                return true;
            case 1:
                return args.getQuick(0).supportsParallelism();
            case 2:
                return args.getQuick(0).supportsParallelism()
                        && args.getQuick(1).supportsParallelism();
            case 3:
                return args.getQuick(0).supportsParallelism()
                        && (args.getQuick(1).supportsParallelism()
                        && args.getQuick(2).supportsParallelism());
            default:
                if (!args.getQuick(0).supportsParallelism()) {
                    return false;
                }
                if (!args.getQuick(1).supportsParallelism()) {
                    return false;
                }
                if (!args.getQuick(2).supportsParallelism()) {
                    return false;
                }
                if (!args.getQuick(3).supportsParallelism()) {
                    return false;
                }
        }
        for (int i = 4; i < n; i++) {
            final Function function = args.getQuick(i);
            if (!function.supportsParallelism()) {
                return false;
            }
        }
        return true;
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
