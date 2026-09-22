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
import io.questdb.griffin.engine.functions.lt.AbstractLongCursorFunctionFactory;

/**
 * Implements {@code long = (sub-query)} where the right-hand operand is a cursor (scalar sub-query)
 * providing exactly one column and (conceptually) one row.
 * <p>
 * An integer cursor scalar compares as {@code long} (preserving values beyond the exact integer
 * range of {@code double}), while a {@code float}/{@code double} scalar compares as {@code double}.
 * The sub-query executes once per query execution and a null (or empty) cursor follows QuestDB's
 * {@code null = null} convention.
 * <p>
 * The {@code !=} and {@code <>} forms, and the argument-swapped {@code (sub-query) = long} form, are
 * derived automatically from this factory.
 */
public class EqLongCursorFunctionFactory extends AbstractLongCursorFunctionFactory {

    @Override
    public String getSignature() {
        return "=(LC)";
    }

    @Override
    protected Function newDoubleFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos) {
        return new EqDoubleCursorFunction(factory, leftFunc, rightFunc, cursorTag, rightPos);
    }

    @Override
    protected Function newLongFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos) {
        return new EqLongCursorFunction(factory, leftFunc, rightFunc, cursorTag, rightPos);
    }
}
