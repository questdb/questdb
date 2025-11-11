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

package io.questdb.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public interface FunctionFactory {
    /**
     * Function signature in a form of "name(type...)". Name is a literal that does not
     * start with number and contains no control characters, which can be confused with
     * SQL language punctuation. Control characters include but not limited to:
     * ',', '(', ')', '*', '/', '%', '+', '-', '='.
     * <p>
     * Argument types are represented by single character from this table:
     * <ul>
     * <li>A = char</li>
     * <li>B = byte</li>
     * <li>C = cursor</li>
     * <li>D = double</li>
     * <li>E = short</li>
     * <li>F = float</li>
     * <li>G = GeoHash</li>
     * <li>H = long256</li>
     * <li>I = int</li>
     * <li>J = long128</li>
     * <li>K = symbol</li>
     * <li>L = long</li>
     * <li>M = date</li>
     * <li>N = timestamp</li>
     * <li>o = NULL - this type is used in cast()</li>
     * <li>p = REGCLASS - this type is used in cast()</li>
     * <li>q = REGPROCEDURE - this type is used in cast()</li>
     * <li>R = record</li>
     * <li>S = string</li>
     * <li>T = boolean</li>
     * <li>U = binary</li>
     * <li>V = variable argument list</li>
     * <li>W = string array</li>
     * <li>X = ipv4</li>
     * <li>Z = uuid</li>
     * <li>Ø(ø) = varchar</li>
     * <li>Δ(δ) = interval</li>
     * <li>Ξ(ξ) = decimal</li>
     * </ul>
     * <p>
     * Lower-case letters will require arguments to be constant expressions. Upper-case letters allow both constant and
     * non-constant expressions.
     *
     * @return signature, for example "substr(SII)"
     * @see Function#isConstant()
     */
    String getSignature();

    default boolean isBoolean() {
        return false;
    }

    default boolean isCursor() {
        return false;
    }

    default boolean isGroupBy() {
        return false;
    }

    /**
     * @return true if the {@link Function} produced by the factory is guaranteed to be constant for
     * a query such that its result does not depend on any {@link Record} in the result set, i.e. now().
     */
    default boolean isRuntimeConstant() {
        return false;
    }

    default boolean isWindow() {
        return false;
    }

    Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException;

    /**
     * If function has variable number of arguments, this method should return preferred type
     * for a variadic argument at given index.
     * <p>
     * SQL Compiler will use this as a hint to determine type of variadic arguments when they have the
     * UNDEFINED type at compile time.
     *
     * @param sqlPos sql position of the argument being resolved
     * @param argPos index of the argument being resolved
     * @param args   list of arguments, function type can be undefined
     * @return preferred type for variadic arguments
     * @throws SqlException if a function cannot resolve preferred type
     */
    default int resolvePreferredVariadicType(int sqlPos, int argPos, ObjList<Function> args) throws SqlException {
        return ColumnType.STRING;
    }

    /**
     * This method should return true when the function signature specifies two parameters
     * of different types, but we want to accept them in the opposite order as well.
     * <p>
     * Example: {@code array + scalar}, where we also want to support {@code scalar + array}.
     * <p>
     * When this returns true, a function signature with the opposite parameter order will
     * be automatically generated.
     */
    default boolean shouldSwapArgs() {
        return false;
    }

    default boolean supportImplicitCastCharToStr() {
        return true;
    }
}
