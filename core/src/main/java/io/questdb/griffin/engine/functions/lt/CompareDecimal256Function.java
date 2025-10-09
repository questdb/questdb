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
        final int rightType = right.getType();
        this.rightScale = ColumnType.getDecimalScale(rightType);
        // We may receive a NullConstant, not only a valid Decimal
        final int precision = ColumnType.isDecimal(rightType) ? ColumnType.getDecimalPrecision(rightType) : 1;
        this.rightStorageSizePow2 = Decimals.getStorageSizePow2(precision);
    }

    @Override
    public boolean getBool(Record rec) {
        DecimalUtil.load(decimal, left, rec);
        final long rightHH, rightHL, rightLH, rightLL;
        switch (rightStorageSizePow2) {
            case 0: {
                byte value = right.getDecimal8(rec);
                if (value == Decimals.DECIMAL8_NULL) {
                    rightHH = Decimals.DECIMAL256_HH_NULL;
                    rightHL = Decimals.DECIMAL256_HL_NULL;
                    rightLH = Decimals.DECIMAL256_LH_NULL;
                    rightLL = Decimals.DECIMAL256_LL_NULL;
                } else {
                    rightLL = value;
                    rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                }
                break;
            }
            case 1: {
                short value = right.getDecimal16(rec);
                if (value == Decimals.DECIMAL16_NULL) {
                    rightHH = Decimals.DECIMAL256_HH_NULL;
                    rightHL = Decimals.DECIMAL256_HL_NULL;
                    rightLH = Decimals.DECIMAL256_LH_NULL;
                    rightLL = Decimals.DECIMAL256_LL_NULL;
                } else {
                    rightLL = value;
                    rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                }
                break;
            }
            case 2: {
                int value = right.getDecimal32(rec);
                if (value == Decimals.DECIMAL32_NULL) {
                    rightHH = Decimals.DECIMAL256_HH_NULL;
                    rightHL = Decimals.DECIMAL256_HL_NULL;
                    rightLH = Decimals.DECIMAL256_LH_NULL;
                    rightLL = Decimals.DECIMAL256_LL_NULL;
                } else {
                    rightLL = value;
                    rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                }
                break;
            }
            case 3: {
                long value = right.getDecimal64(rec);
                if (value == Decimals.DECIMAL64_NULL) {
                    rightHH = Decimals.DECIMAL256_HH_NULL;
                    rightHL = Decimals.DECIMAL256_HL_NULL;
                    rightLH = Decimals.DECIMAL256_LH_NULL;
                    rightLL = Decimals.DECIMAL256_LL_NULL;
                } else {
                    rightLL = value;
                    rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                }
                break;
            }
            case 4: {
                long hi = right.getDecimal128Hi(rec);
                long lo = right.getDecimal128Lo(rec);
                if (Decimal128.isNull(hi, lo)) {
                    rightHH = Decimals.DECIMAL256_HH_NULL;
                    rightHL = Decimals.DECIMAL256_HL_NULL;
                    rightLH = Decimals.DECIMAL256_LH_NULL;
                    rightLL = Decimals.DECIMAL256_LL_NULL;
                } else {
                    rightLL = lo;
                    rightLH = hi;
                    rightHL = rightHH = rightLH < 0 ? -1 : 0;
                }
                break;
            }
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
