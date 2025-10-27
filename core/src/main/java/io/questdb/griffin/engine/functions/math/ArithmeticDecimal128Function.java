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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.decimal.Decimal128Function;
import io.questdb.griffin.engine.functions.decimal.Decimal128LoaderFunctionFactory;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimals;
import io.questdb.std.NumericException;

abstract class ArithmeticDecimal128Function extends Decimal128Function implements BinaryFunction {
    protected final Decimal128 decimal = new Decimal128();
    protected final Decimal128 decimalRight = new Decimal128();
    protected final Function left;
    protected final int precision;
    protected final Function right;
    protected final int scale;
    private final int leftScale;
    private final int position;
    private final int rightScale;

    public ArithmeticDecimal128Function(Function left, Function right, int targetType, int position) {
        super(targetType);
        this.left = Decimal128LoaderFunctionFactory.getInstance(left);
        this.leftScale = ColumnType.getDecimalScale(left.getType());
        this.right = Decimal128LoaderFunctionFactory.getInstance(right);
        this.position = position;
        this.precision = ColumnType.getDecimalPrecision(targetType);
        this.scale = ColumnType.getDecimalScale(targetType);
        this.rightScale = ColumnType.getDecimalScale(right.getType());
    }

    @Override
    public void getDecimal128(Record rec, Decimal128 sink) {
        if (!calc(rec)) {
            sink.ofRawNull();
            return;
        }
        sink.copyRaw(decimal);
    }

    @Override
    public short getDecimal16(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL16_NULL;
        }
        return (short) decimal.getLow();
    }

    @Override
    public int getDecimal32(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL32_NULL;
        }
        return (int) decimal.getLow();
    }

    @Override
    public long getDecimal64(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL64_NULL;
        }
        return decimal.getLow();
    }

    @Override
    public byte getDecimal8(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL8_NULL;
        }
        return (byte) decimal.getLow();
    }

    @Override
    public Function getLeft() {
        return left;
    }

    @Override
    public Function getRight() {
        return right;
    }

    @Override
    public boolean isOperator() {
        return true;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    /**
     * @return whether the result is not null.
     */
    private boolean calc(Record rec) {
        left.getDecimal128(rec, decimal);
        if (decimal.isNull()) {
            return false;
        }
        decimal.setScale(leftScale);

        right.getDecimal128(rec, decimalRight);
        if (decimalRight.isNull()) {
            return false;
        }
        decimalRight.setScale(rightScale);

        try {
            exec(decimalRight);
        } catch (NumericException e) {
            throw CairoException.nonCritical().position(position)
                    .put('\'').put(getName()).put("' operation failed: ").put(e.getFlyweightMessage());
        }
        return true;
    }

    /**
     * The implementation must execute the operation against the decimal here.
     * The right-hand values are given as inputs to this method.
     */
    protected abstract void exec(Decimal128 right);
}
