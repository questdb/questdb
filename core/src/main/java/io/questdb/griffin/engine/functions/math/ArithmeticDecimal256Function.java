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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.NumericException;

abstract class ArithmeticDecimal256Function extends DecimalFunction implements BinaryFunction {
    protected final Decimal256 decimal = new Decimal256();
    protected final Function left;
    protected final int precision;
    protected final Function right;
    protected final int scale;
    private final int rightScale;
    private final int rightStorageSizePow2;
    private boolean isNull;

    public ArithmeticDecimal256Function(Function left, Function right, int targetType) {
        super(targetType);
        this.left = left;
        this.right = right;
        this.precision = ColumnType.getDecimalPrecision(targetType);
        this.scale = ColumnType.getDecimalScale(targetType);
        this.rightScale = ColumnType.getDecimalScale(right.getType());
        this.rightStorageSizePow2 = Decimals.getStorageSizePow2(ColumnType.getDecimalPrecision(right.getType()));
    }

    @Override
    public long getDecimal128Hi(Record rec) {
        if (!calc(rec)) {
            isNull = true;
            return Decimals.DECIMAL128_HI_NULL;
        }
        isNull = false;
        return decimal.getLh();
    }

    @Override
    public long getDecimal128Lo(Record rec) {
        if (isNull) {
            return Decimals.DECIMAL128_LO_NULL;
        }
        return decimal.getLl();
    }

    @Override
    public short getDecimal16(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL16_NULL;
        }
        return (short) decimal.getLl();
    }

    @Override
    public long getDecimal256HH(Record rec) {
        if (!calc(rec)) {
            isNull = true;
            return Decimals.DECIMAL256_HH_NULL;
        }
        isNull = false;
        return decimal.getHh();
    }

    @Override
    public long getDecimal256HL(Record rec) {
        if (isNull) {
            return Decimals.DECIMAL256_HL_NULL;
        }
        return decimal.getHl();
    }

    @Override
    public long getDecimal256LH(Record rec) {
        if (isNull) {
            return Decimals.DECIMAL256_LH_NULL;
        }
        return decimal.getLh();
    }

    @Override
    public long getDecimal256LL(Record rec) {
        if (isNull) {
            return Decimals.DECIMAL256_LL_NULL;
        }
        return decimal.getLl();
    }

    @Override
    public int getDecimal32(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL32_NULL;
        }
        return (int) decimal.getLl();
    }

    @Override
    public long getDecimal64(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL64_NULL;
        }
        return decimal.getLl();
    }

    @Override
    public byte getDecimal8(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL8_NULL;
        }
        return (byte) decimal.getLl();
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

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(left).val(getName()).val(right);
    }

    /**
     * @return whether the result is not null.
     */
    private boolean calc(Record rec) {
        DecimalUtil.load(decimal, left, rec);
        if (decimal.isNull()) {
            return false;
        }

        final long rightHH, rightHL, rightLH, rightLL;
        switch (rightStorageSizePow2) {
            case 0:
                rightLL = right.getDecimal8(rec);
                rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                if (rightLL == Decimals.DECIMAL8_NULL) {
                    return false;
                }
                break;
            case 1:
                rightLL = right.getDecimal16(rec);
                rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                if (rightLL == Decimals.DECIMAL16_NULL) {
                    return false;
                }
                break;
            case 2:
                rightLL = right.getDecimal32(rec);
                rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                if (rightLL == Decimals.DECIMAL32_NULL) {
                    return false;
                }
                break;
            case 3:
                rightLL = right.getDecimal64(rec);
                rightLH = rightHL = rightHH = rightLL < 0 ? -1 : 0;
                if (Decimal64.isNull(rightLL)) {
                    return false;
                }
                break;
            case 4:
                rightLH = right.getDecimal128Hi(rec);
                rightLL = right.getDecimal128Lo(rec);
                rightHL = rightHH = rightLH < 0 ? -1 : 0;
                if (Decimal128.isNull(rightLH, rightLL)) {
                    return false;
                }
                break;
            default:
                rightHH = right.getDecimal256HH(rec);
                rightHL = right.getDecimal256HL(rec);
                rightLH = right.getDecimal256LH(rec);
                rightLL = right.getDecimal256LL(rec);
                if (Decimal256.isNull(rightHH, rightHL, rightLH, rightLL)) {
                    return false;
                }
                break;
        }

        try {
            exec(rightHH, rightHL, rightLH, rightLL, rightScale);
        } catch (NumericException ignore) {
            return false;
        }
        return true;
    }

    /**
     * The implementation must execute the operation against the decimal here.
     * The right-hand values are given as inputs to this method.
     */
    protected abstract void exec(long rightHH, long rightHL, long rightLH, long rightLL, int rightScale);
}
