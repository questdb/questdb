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

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

/**
 * A CASE, searched or switch. Its {@link #args()} are its children in an order each factory
 * picks, conditions and keys among them; its values, by role, are {@link #branches()}.
 */
public interface CaseFunction extends MultiArgFunction, CaseBranches {

    /**
     * The THEN values and the ELSE this CASE returns, see {@link CaseBranches}.
     */
    CaseBranches.Values branches();

    @Override
    default int getComplexity() {
        return Function.addComplexity(5, MultiArgFunction.super.getComplexity());
    }

    @Override
    default @NotNull Function getElseValue() {
        return branches().elseValue();
    }

    @Override
    default @NotNull ObjList<Function> getThenValues() {
        return branches().thenValues();
    }

    @Override
    default void toPlan(PlanSink sink) {
        sink.val("case(").val(args()).val(')');
    }
}
