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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.ScalarSubQueryUtils;
import io.questdb.griffin.engine.functions.lt.AbstractScalarCursorFunction;
import io.questdb.std.Numbers;

/**
 * Per-row {@code left = value} comparison of a {@code double}-read left operand against the cached
 * {@code double} cursor scalar. All cold lifecycle behavior (sub-query execution, cardinality
 * enforcement, worker state donation, plan rendering) lives in {@link AbstractScalarCursorFunction}.
 * <p>
 * Equality uses {@link Numbers#equals(double, double)}, so it matches the tolerance, NaN, infinity
 * and signed-zero semantics of the column-to-column {@code double = double} operator. A null cursor
 * scalar is cached as {@link Double#NaN}, and {@code Numbers.equals(NaN, NaN)} is {@code true}, so a
 * null left operand equals a null scalar (QuestDB's {@code null = null} convention).
 */
class EqDoubleCursorFunction extends AbstractScalarCursorFunction {
    private final int cursorTag;
    private double value;

    EqDoubleCursorFunction(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos) {
        super(factory, leftFunc, rightFunc, rightPos);
        this.cursorTag = cursorTag;
    }

    @Override
    public boolean getBool(Record rec) {
        return negated != Numbers.equals(leftFunc.getDouble(rec), value);
    }

    @Override
    protected void donateValueTo(AbstractScalarCursorFunction that) {
        ((EqDoubleCursorFunction) that).value = value;
    }

    @Override
    protected String negatedOperator() {
        return " != ";
    }

    @Override
    protected String operator() {
        return " = ";
    }

    @Override
    protected void readValue(Record record) {
        value = ScalarSubQueryUtils.readDoubleValue(record, cursorTag);
    }

    @Override
    protected void setNullValue() {
        value = Double.NaN;
    }
}
