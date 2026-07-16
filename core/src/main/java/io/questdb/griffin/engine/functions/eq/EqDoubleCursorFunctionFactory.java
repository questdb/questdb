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
import io.questdb.griffin.engine.functions.lt.AbstractDoubleCursorFunctionFactory;

/**
 * Implements {@code double = (sub-query)} where the right-hand operand is a cursor (scalar
 * sub-query) providing exactly one column and (conceptually) one row.
 * <p>
 * The sub-query is executed once per query execution - not per row - and the resulting value is
 * cached as a scalar {@code double}. If the cursor selects no rows, or the value is {@code null},
 * the cached value is {@link Double#NaN} and the predicate follows QuestDB's {@code null = null}
 * convention: a null scalar equals a null left operand and nothing else.
 * <p>
 * The {@code !=} and {@code <>} forms, and the argument-swapped {@code (sub-query) = double} form,
 * are derived automatically from this factory.
 */
public class EqDoubleCursorFunctionFactory extends AbstractDoubleCursorFunctionFactory {

    @Override
    public String getSignature() {
        return "=(DC)";
    }

    @Override
    protected Function newDoubleFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos) {
        return new EqDoubleCursorFunction(factory, leftFunc, rightFunc, cursorTag, rightPos);
    }
}
