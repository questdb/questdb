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
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimals;

public abstract class CompareDecimal128Function extends NegatableBooleanFunction implements BinaryFunction {
    protected final Decimal128 decimal = new Decimal128();
    protected final Function left;
    protected final Function right;
    private final int rightScale;
    private final int rightStorageSizePow2;

    public CompareDecimal128Function(Function left, Function right) {
        this.left = left;
        this.right = right;
        this.rightScale = ColumnType.getDecimalScale(right.getType());
        this.rightStorageSizePow2 = Decimals.getStorageSizePow2(ColumnType.getDecimalPrecision(right.getType()));
    }

    @Override
    public boolean getBool(Record rec) {
        DecimalUtil.load(decimal, left, rec);
        final long rightHigh, rightLow;
        switch (rightStorageSizePow2) {
            case 0:
                rightLow = right.getDecimal8(rec);
                rightHigh = rightLow < 0 ? -1L : 0L;
                break;
            case 1:
                rightLow = right.getDecimal16(rec);
                rightHigh = rightLow < 0 ? -1L : 0L;
                break;
            case 2:
                rightLow = right.getDecimal32(rec);
                rightHigh = rightLow < 0 ? -1L : 0L;
                break;
            case 3:
                rightLow = right.getDecimal64(rec);
                rightHigh = rightLow < 0 ? -1L : 0L;
                break;
            default:
                rightHigh = right.getDecimal128Hi(rec);
                rightLow = right.getDecimal128Lo(rec);
                break;
        }
        return negated != exec(rightHigh, rightLow, rightScale);
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
    protected abstract boolean exec(long rightHigh, long rightLow, int rightScale);
}
