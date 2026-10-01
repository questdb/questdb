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

package io.questdb.griffin.engine.functions.lt;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;

/**
 * Implements {@code long > (sub-query)} where the right-hand operand is a cursor (scalar sub-query)
 * providing exactly one column and (conceptually) one row.
 * <p>
 * When the cursor scalar is an integer type ({@code byte}/{@code short}/{@code int}/{@code long}) the
 * comparison is performed as a {@code long} comparison, so no precision is lost for {@code long} values
 * beyond 2^53 (unlike a comparison that widens both operands to {@code double}). When the cursor scalar
 * is a {@code float}/{@code double} the comparison is performed as a {@code double} comparison.
 * <p>
 * The sub-query is executed once per query execution - not per row - in {@link Function#init} and its
 * value is cached as a scalar. An empty cursor or a {@code null} value follows QuestDB's
 * sentinel-null convention: the strict comparison matches no rows, while its negated inclusive
 * form matches rows whose left operand is also null (null equals null).
 */
public class GtLongCursorFunctionFactory extends AbstractLongCursorFunctionFactory {

    @Override
    public String getSignature() {
        return ">(LC)";
    }

    @Override
    protected Function newDoubleFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos) {
        return new GtDoubleCursorFunction(factory, leftFunc, rightFunc, cursorTag, rightPos);
    }

    @Override
    protected Function newLongFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos) {
        return new GtLongCursorFunction(factory, leftFunc, rightFunc, cursorTag, rightPos);
    }
}
