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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Chars;

/**
 * Implements {@code string < (sub-query)} for a SYMBOL, STRING or VARCHAR left operand and a scalar
 * sub-query that selects one text column, with the ordering and null semantics of the
 * {@code string < string} operator.
 */
public class LtStrCursorFunctionFactory extends AbstractStrCursorFunctionFactory {

    public LtStrCursorFunctionFactory() {
        super(new LtDoubleCursorFunctionFactory());
    }

    @Override
    public String getSignature() {
        return "<(SC)";
    }

    @Override
    protected Function newFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos) {
        return new Func(factory, leftFunc, rightFunc, cursorTag, rightPos);
    }

    private static class Func extends StrCursorFunction {

        Func(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos) {
            super(factory, leftFunc, rightFunc, cursorTag, rightPos);
        }

        @Override
        public boolean getBool(Record rec) {
            return Chars.lessThan(leftFunc.getStrA(rec), value, negated);
        }

        @Override
        protected String negatedOperator() {
            return " >= ";
        }

        @Override
        protected String operator() {
            return " < ";
        }
    }
}
