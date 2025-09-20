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
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;

public abstract class CompareDecimal256Function extends NegatableBooleanFunction implements BinaryFunction {
    protected final Decimal256 decimal = new Decimal256();
    protected final Function left;
    protected final Function right;
    private final int rightScale;
    private final int rightStorageSizePow2;

    public CompareDecimal256Function(Function left, Function right) {
        this.left = left;
        this.right = right;
        this.rightScale = ColumnType.getDecimalScale(right.getType());
        this.rightStorageSizePow2 = Decimals.getStorageSizePow2(ColumnType.getDecimalPrecision(right.getType()));
    }

    @Override
    public boolean getBool(Record rec) {
        DecimalUtil.load(decimal, left, rec);
        final long rightHH, rightHL, rightLH, rightLL;
        switch (rightStorageSizePow2) {
            case 0:
                rightLL = right.getDecimal8(rec);
                rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                break;
            case 1:
                rightLL = right.getDecimal16(rec);
                rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                break;
            case 2:
                rightLL = right.getDecimal32(rec);
                rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                break;
            case 3:
                rightLL = right.getDecimal64(rec);
                rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                break;
            case 4:
                rightLH = right.getDecimal128Hi(rec);
                rightLL = right.getDecimal128Lo(rec);
                rightHL = rightHH = rightLH < 0 ? -1 : 0;
                break;
            default:
                rightHH = right.getDecimal256HH(rec);
                rightHL = right.getDecimal256HL(rec);
                rightLH = right.getDecimal256LH(rec);
                rightLL = right.getDecimal256LL(rec);
                break;
        }
        return negated != exec(rightHH, rightHL, rightLH, rightLL, rightScale);
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
    protected abstract boolean exec(long rightHH, long rightHL, long rightLH, long rightLL, int rightScale);
}
