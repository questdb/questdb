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

    ObjList<Function> args();

    @Override
    default void close() {
        Misc.freeObjList(args());
    }

    @Override
    default void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        Function.init(args(), symbolTableSource, executionContext, null);
    }

    @Override
    default boolean isConstant() {
        ObjList<Function> args = args();
        for (int i = 0, n = args.size(); i < n; i++) {
            if (!args.getQuick(i).isConstant()) {
                return false;
            }
        }
        return true;
    }

    @Override
    default boolean isNonDeterministic() {
        final ObjList<Function> args = args();
        for (int i = 0, n = args.size(); i < n; i++) {
            final Function function = args.getQuick(i);
            if (function.isNonDeterministic()) {
                return true;
            }
        }
        return false;
    }

    @Override
    default boolean isRandom() {
        final ObjList<Function> args = args();
        for (int i = 0, n = args.size(); i < n; i++) {
            final Function function = args.getQuick(i);
            if (function.isRandom()) {
                return true;
            }
        }
        return false;
    }

    @Override
    default boolean isRuntimeConstant() {
        final ObjList<Function> args = args();
        for (int i = 0, n = args.size(); i < n; i++) {
            final Function function = args.getQuick(i);
            if (!function.isRuntimeConstant() && !function.isConstant()) {
                return false;
            }
        }
        return true;
    }

    @Override
    default boolean isThreadSafe() {
        final ObjList<Function> args = args();
        for (int i = 0, n = args.size(); i < n; i++) {
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
            ObjList<Function> thatArgs = ((MultiArgFunction) that).args();
            ObjList<Function> thisArgs = args();
            if (thatArgs.size() == thisArgs.size()) {
                for (int i = 0; i < thisArgs.size(); i++) {
                    thisArgs.getQuick(i).offerStateTo(thatArgs.getQuick(i));
                }
            }
        }
    }

    @Override
    default boolean shouldMemoize() {
        final ObjList<Function> args = args();
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
        final ObjList<Function> args = args();
        for (int i = 0, n = args.size(); i < n; i++) {
            final Function function = args.getQuick(i);
            if (!function.supportsParallelism()) {
                return false;
            }
        }
        return true;
    }

    @Override
    default void toPlan(PlanSink sink) {
        sink.val(getName()).val('(').val(args()).val(')');
    }

    @Override
    default void toTop() {
        GroupByUtils.toTop(args());
    }
}
