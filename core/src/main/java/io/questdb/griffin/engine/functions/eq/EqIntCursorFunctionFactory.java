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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.lt.AbstractIntCursorFunctionFactory;

/**
 * Implements {@code int = (sub-query)} (also covering {@code byte} and {@code short} left operands)
 * where the right-hand operand is a cursor (scalar sub-query) providing exactly one column and
 * (conceptually) one row.
 * <p>
 * The comparison widens to the cursor scalar type so it is never narrowed: a {@code long} scalar
 * compares as {@code long}, a {@code float}/{@code double} scalar compares as {@code double}, and a
 * narrow integer scalar compares as {@code int}. The sub-query executes once per query execution and
 * a null (or empty) cursor follows QuestDB's {@code null = null} convention.
 * <p>
 * The {@code !=} and {@code <>} forms, and the argument-swapped {@code (sub-query) = int} form, are
 * derived automatically from this factory.
 */
public class EqIntCursorFunctionFactory extends AbstractIntCursorFunctionFactory {

    @Override
    public String getSignature() {
        return "=(IC)";
    }

    @Override
    protected Function newDoubleFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos) {
        return new EqDoubleCursorFunction(factory, leftFunc, rightFunc, cursorTag, rightPos);
    }

    @Override
    protected Function newIntFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos) {
        return new EqIntCursorFunction(factory, leftFunc, rightFunc, cursorTag, rightPos);
    }

    @Override
    protected Function newLongFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos) {
        return new EqLongCursorFunction(factory, leftFunc, rightFunc, cursorTag, rightPos);
    }
}
