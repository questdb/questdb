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
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

/**
 * The values a CASE returns, by their role rather than their position: a CASE's
 * {@code args()} hold its conditions, its switch key and helpers too, in an order each factory
 * picks. A searched CASE ({@code CASE WHEN c THEN v ... ELSE e END}) holds
 * {@code [c1, v1, ..., e]}, a switch ({@code CASE x WHEN k THEN v ... END}, which the parser also
 * makes of a searched CASE that compares one column to constants) holds
 * {@code [v1, ..., e, x]}. A reader of values, such as a proof that a CASE is never negative,
 * must take them from here.
 * <p>
 * Every row's value is one of {@link #getThenValues()} or {@link #getElseValue()}, each read
 * with the CASE's own type, as the CASE reads it.
 */
public interface CaseBranches {

    /**
     * The value a row takes when no WHEN matches: the ELSE, or a NULL constant of the CASE's type
     * when there is none. A CASE whose WHENs cover every row (a BOOLEAN switch over both values)
     * still names it.
     */
    @NotNull
    Function getElseValue();

    /**
     * The THEN values, one per WHEN, in the order of the WHENs; a switch's THEN for a NULL key
     * comes last.
     */
    @NotNull
    ObjList<Function> getThenValues();

    /**
     * The THEN values and the ELSE of a CASE, as {@link CaseCommon#getCaseFunction} hands them to
     * the function it builds.
     */
    record Values(@NotNull ObjList<Function> thenValues, @NotNull Function elseValue) {
    }
}
