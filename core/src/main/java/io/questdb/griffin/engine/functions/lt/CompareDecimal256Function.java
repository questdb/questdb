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

package io.questdb.griffin.engine.functions.lt;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.decimal.Decimal256LoaderFunctionFactory;
import io.questdb.std.Decimal256;

public abstract class CompareDecimal256Function extends NegatableBooleanFunction implements BinaryFunction {
    protected final Decimal256 decimalLeft = new Decimal256();
    protected final Decimal256 decimalRight = new Decimal256();
    protected final Function left;
    protected final int leftScale;
    protected final Function right;
    protected final int rightScale;

    public CompareDecimal256Function(Function left, Function right) {
        this.left = Decimal256LoaderFunctionFactory.getInstance(left);
        this.leftScale = ColumnType.getDecimalScale(this.left.getType());
        this.right = Decimal256LoaderFunctionFactory.getInstance(right);
        this.rightScale = ColumnType.getDecimalScale(this.right.getType());
    }

    @Override
    public boolean getBool(Record rec) {
        left.getDecimal256(rec, decimalLeft);
        decimalLeft.setScale(leftScale);
        right.getDecimal256(rec, decimalRight);
        decimalRight.setScale(rightScale);
        return negated != exec();
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
    public boolean isThreadSafe() {
        return false;
    }

    /**
     * Should execute comparison against decimal holding left argument value and the right-arg inputs.
     */
    protected abstract boolean exec();
}
