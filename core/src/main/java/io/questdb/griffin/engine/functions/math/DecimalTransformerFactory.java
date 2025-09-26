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
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;

public final class DecimalTransformerFactory {
    private DecimalTransformerFactory() {
    }

    public static DecimalFunction newInstance(Function value, int targetType, DecimalTransformer transformer) {
        final int sourceType = value.getType();
        final int sourceTag = ColumnType.tagOf(sourceType);
        final int targetTag = ColumnType.tagOf(targetType);

        switch (sourceTag) {
            case ColumnType.DECIMAL8:
                switch (targetTag) {
                    case ColumnType.DECIMAL8:
                        return new Decimal8To8Func(targetType, value, transformer);
                    case ColumnType.DECIMAL16:
                        return new Decimal8To16Func(targetType, value, transformer);
                    case ColumnType.DECIMAL32:
                        return new Decimal8To32Func(targetType, value, transformer);
                    case ColumnType.DECIMAL64:
                        return new Decimal8To64Func(targetType, value, transformer);
                    case ColumnType.DECIMAL128:
                        return new Decimal8To128Func(targetType, value, transformer);
                    case ColumnType.DECIMAL256:
                        return new Decimal8To256Func(targetType, value, transformer);
                }
                break;

            case ColumnType.DECIMAL16:
                switch (targetTag) {
                    case ColumnType.DECIMAL8:
                        return new Decimal16To8Func(targetType, value, transformer);
                    case ColumnType.DECIMAL16:
                        return new Decimal16To16Func(targetType, value, transformer);
                    case ColumnType.DECIMAL32:
                        return new Decimal16To32Func(targetType, value, transformer);
                    case ColumnType.DECIMAL64:
                        return new Decimal16To64Func(targetType, value, transformer);
                    case ColumnType.DECIMAL128:
                        return new Decimal16To128Func(targetType, value, transformer);
                    case ColumnType.DECIMAL256:
                        return new Decimal16To256Func(targetType, value, transformer);
                }
                break;

            case ColumnType.DECIMAL32:
                switch (targetTag) {
                    case ColumnType.DECIMAL8:
                        return new Decimal32To8Func(targetType, value, transformer);
                    case ColumnType.DECIMAL16:
                        return new Decimal32To16Func(targetType, value, transformer);
                    case ColumnType.DECIMAL32:
                        return new Decimal32To32Func(targetType, value, transformer);
                    case ColumnType.DECIMAL64:
                        return new Decimal32To64Func(targetType, value, transformer);
                    case ColumnType.DECIMAL128:
                        return new Decimal32To128Func(targetType, value, transformer);
                    case ColumnType.DECIMAL256:
                        return new Decimal32To256Func(targetType, value, transformer);
                }
                break;

            case ColumnType.DECIMAL64:
                switch (targetTag) {
                    case ColumnType.DECIMAL8:
                        return new Decimal64To8Func(targetType, value, transformer);
                    case ColumnType.DECIMAL16:
                        return new Decimal64To16Func(targetType, value, transformer);
                    case ColumnType.DECIMAL32:
                        return new Decimal64To32Func(targetType, value, transformer);
                    case ColumnType.DECIMAL64:
                        return new Decimal64To64Func(targetType, value, transformer);
                    case ColumnType.DECIMAL128:
                        return new Decimal64To128Func(targetType, value, transformer);
                    case ColumnType.DECIMAL256:
                        return new Decimal64To256Func(targetType, value, transformer);
                }
                break;

            case ColumnType.DECIMAL128:
                switch (targetTag) {
                    case ColumnType.DECIMAL8:
                        return new Decimal128To8Func(targetType, value, transformer);
                    case ColumnType.DECIMAL16:
                        return new Decimal128To16Func(targetType, value, transformer);
                    case ColumnType.DECIMAL32:
                        return new Decimal128To32Func(targetType, value, transformer);
                    case ColumnType.DECIMAL64:
                        return new Decimal128To64Func(targetType, value, transformer);
                    case ColumnType.DECIMAL128:
                        return new Decimal128To128Func(targetType, value, transformer);
                    case ColumnType.DECIMAL256:
                        return new Decimal128To256Func(targetType, value, transformer);
                }
                break;

            case ColumnType.DECIMAL256:
                switch (targetTag) {
                    case ColumnType.DECIMAL8:
                        return new Decimal256To8Func(targetType, value, transformer);
                    case ColumnType.DECIMAL16:
                        return new Decimal256To16Func(targetType, value, transformer);
                    case ColumnType.DECIMAL32:
                        return new Decimal256To32Func(targetType, value, transformer);
                    case ColumnType.DECIMAL64:
                        return new Decimal256To64Func(targetType, value, transformer);
                    case ColumnType.DECIMAL128:
                        return new Decimal256To128Func(targetType, value, transformer);
                    case ColumnType.DECIMAL256:
                        return new Decimal256To256Func(targetType, value, transformer);
                }
                break;
        }

        throw new UnsupportedOperationException("Unsupported decimal transformation from " +
                ColumnType.nameOf(sourceType) + " to " + ColumnType.nameOf(targetType));
    }

    private static class Decimal128To128Func extends DecimalTransformerFunction {
        private final Decimal128 decimal128 = new Decimal128();

        private Decimal128To128Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal128Hi(Record record) {
            long hi = value.getDecimal128Hi(record);
            long lo = value.getDecimal128Lo(record);
            if (Decimal128.isNull(hi, lo)) {
                decimal128.ofNull();
            } else {
                decimal128.of(hi, lo, fromScale);
                transformer.transform(decimal128);
            }
            return decimal128.getHigh();
        }

        @Override
        public long getDecimal128Lo(Record record) {
            return decimal128.getLow();
        }
    }

    private static class Decimal128To16Func extends DecimalTransformerFunction {
        private final Decimal128 decimal128 = new Decimal128();

        private Decimal128To16Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public short getDecimal16(Record record) {
            long hi = value.getDecimal128Hi(record);
            long lo = value.getDecimal128Lo(record);
            if (Decimal128.isNull(hi, lo)) {
                return Decimals.DECIMAL16_NULL;
            }
            decimal128.of(hi, lo, fromScale);
            transformer.transform(decimal128);
            return (short) decimal128.getLow();
        }
    }

    private static class Decimal128To256Func extends DecimalTransformerFunction {
        private final Decimal256 decimal256 = new Decimal256();

        private Decimal128To256Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal256HH(Record record) {
            long hi = value.getDecimal128Hi(record);
            long lo = value.getDecimal128Lo(record);
            if (Decimal128.isNull(hi, lo)) {
                decimal256.ofNull();
            } else {
                long s = hi < 0 ? -1 : 0;
                decimal256.of(s, s, hi, lo, fromScale);
                transformer.transform(decimal256);
            }
            return decimal256.getHh();
        }

        @Override
        public long getDecimal256HL(Record record) {
            return decimal256.getHl();
        }

        @Override
        public long getDecimal256LH(Record record) {
            return decimal256.getLh();
        }

        @Override
        public long getDecimal256LL(Record record) {
            return decimal256.getLl();
        }
    }

    private static class Decimal128To32Func extends DecimalTransformerFunction {
        private final Decimal128 decimal128 = new Decimal128();

        private Decimal128To32Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public int getDecimal32(Record record) {
            long hi = value.getDecimal128Hi(record);
            long lo = value.getDecimal128Lo(record);
            if (Decimal128.isNull(hi, lo)) {
                return Decimals.DECIMAL32_NULL;
            }
            decimal128.of(hi, lo, fromScale);
            transformer.transform(decimal128);
            return (int) decimal128.getLow();
        }
    }

    private static class Decimal128To64Func extends DecimalTransformerFunction {
        private final Decimal128 decimal128 = new Decimal128();

        private Decimal128To64Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal64(Record record) {
            long hi = value.getDecimal128Hi(record);
            long lo = value.getDecimal128Lo(record);
            if (Decimal128.isNull(hi, lo)) {
                return Decimals.DECIMAL64_NULL;
            }
            decimal128.of(hi, lo, fromScale);
            transformer.transform(decimal128);
            return decimal128.getLow();
        }
    }

    private static class Decimal128To8Func extends DecimalTransformerFunction {
        private final Decimal128 decimal128 = new Decimal128();

        private Decimal128To8Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public byte getDecimal8(Record record) {
            long hi = value.getDecimal128Hi(record);
            long lo = value.getDecimal128Lo(record);
            if (Decimal128.isNull(hi, lo)) {
                return Decimals.DECIMAL8_NULL;
            }
            decimal128.of(hi, lo, fromScale);
            transformer.transform(decimal128);
            return (byte) decimal128.getLow();
        }
    }

    private static class Decimal16To128Func extends DecimalTransformerFunction {
        private final Decimal128 decimal128 = new Decimal128();

        private Decimal16To128Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal128Hi(Record record) {
            short v = value.getDecimal16(record);
            if (v == Decimals.DECIMAL16_NULL) {
                decimal128.ofNull();
            } else {
                decimal128.of(0, v, fromScale);
                transformer.transform(decimal128);
            }
            return decimal128.getHigh();
        }

        @Override
        public long getDecimal128Lo(Record record) {
            return decimal128.getLow();
        }
    }

    private static class Decimal16To16Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal16To16Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public short getDecimal16(Record record) {
            short v = value.getDecimal16(record);
            if (v == Decimals.DECIMAL16_NULL) {
                return Decimals.DECIMAL16_NULL;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return (short) decimal64.getValue();
        }
    }

    private static class Decimal16To256Func extends DecimalTransformerFunction {
        private final Decimal256 decimal256 = new Decimal256();

        private Decimal16To256Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal256HH(Record record) {
            short v = value.getDecimal16(record);
            if (v == Decimals.DECIMAL16_NULL) {
                decimal256.ofNull();
            } else {
                decimal256.ofLong(v, fromScale);
                transformer.transform(decimal256);
            }
            return decimal256.getHh();
        }

        @Override
        public long getDecimal256HL(Record record) {
            return decimal256.getHl();
        }

        @Override
        public long getDecimal256LH(Record record) {
            return decimal256.getLh();
        }

        @Override
        public long getDecimal256LL(Record record) {
            return decimal256.getLl();
        }
    }

    private static class Decimal16To32Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal16To32Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public int getDecimal32(Record record) {
            short v = value.getDecimal16(record);
            if (v == Decimals.DECIMAL16_NULL) {
                return Decimals.DECIMAL32_NULL;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return (int) decimal64.getValue();
        }
    }

    private static class Decimal16To64Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal16To64Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal64(Record record) {
            short v = value.getDecimal16(record);
            if (v == Decimals.DECIMAL16_NULL) {
                return Decimals.DECIMAL64_NULL;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return decimal64.getValue();
        }
    }

    private static class Decimal16To8Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal16To8Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public byte getDecimal8(Record record) {
            short v = value.getDecimal16(record);
            if (v == Decimals.DECIMAL16_NULL) {
                return Decimals.DECIMAL8_NULL;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return (byte) decimal64.getValue();
        }
    }

    private static class Decimal256To128Func extends DecimalTransformerFunction {
        private final Decimal256 decimal256 = new Decimal256();

        private Decimal256To128Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal128Hi(Record record) {
            long hh = value.getDecimal256HH(record);
            long hl = value.getDecimal256HL(record);
            long lh = value.getDecimal256LH(record);
            long ll = value.getDecimal256LL(record);
            if (Decimal256.isNull(hh, hl, lh, ll)) {
                decimal256.of(0, 0, Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, fromScale);
            } else {
                decimal256.of(hh, hl, lh, ll, fromScale);
                transformer.transform(decimal256);
            }
            return decimal256.getLh();
        }

        @Override
        public long getDecimal128Lo(Record record) {
            return decimal256.getLl();
        }
    }

    private static class Decimal256To16Func extends DecimalTransformerFunction {
        private final Decimal256 decimal256 = new Decimal256();

        private Decimal256To16Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public short getDecimal16(Record record) {
            long hh = value.getDecimal256HH(record);
            long hl = value.getDecimal256HL(record);
            long lh = value.getDecimal256LH(record);
            long ll = value.getDecimal256LL(record);
            if (Decimal256.isNull(hh, hl, lh, ll)) {
                return Decimals.DECIMAL16_NULL;
            }
            decimal256.of(hh, hl, lh, ll, fromScale);
            transformer.transform(decimal256);
            return (short) decimal256.getLl();
        }
    }

    private static class Decimal256To256Func extends DecimalTransformerFunction {
        private final Decimal256 decimal256 = new Decimal256();

        private Decimal256To256Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal256HH(Record record) {
            long hh = value.getDecimal256HH(record);
            long hl = value.getDecimal256HL(record);
            long lh = value.getDecimal256LH(record);
            long ll = value.getDecimal256LL(record);
            if (Decimal256.isNull(hh, hl, lh, ll)) {
                decimal256.ofNull();
            } else {
                decimal256.of(hh, hl, lh, ll, fromScale);
                transformer.transform(decimal256);
            }
            return decimal256.getHh();
        }

        @Override
        public long getDecimal256HL(Record record) {
            return decimal256.getHl();
        }

        @Override
        public long getDecimal256LH(Record record) {
            return decimal256.getLh();
        }

        @Override
        public long getDecimal256LL(Record record) {
            return decimal256.getLl();
        }
    }

    private static class Decimal256To32Func extends DecimalTransformerFunction {
        private final Decimal256 decimal256 = new Decimal256();

        private Decimal256To32Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public int getDecimal32(Record record) {
            long hh = value.getDecimal256HH(record);
            long hl = value.getDecimal256HL(record);
            long lh = value.getDecimal256LH(record);
            long ll = value.getDecimal256LL(record);
            if (Decimal256.isNull(hh, hl, lh, ll)) {
                return Decimals.DECIMAL32_NULL;
            }
            decimal256.of(hh, hl, lh, ll, fromScale);
            transformer.transform(decimal256);
            return (int) decimal256.getLl();
        }
    }

    private static class Decimal256To64Func extends DecimalTransformerFunction {
        private final Decimal256 decimal256 = new Decimal256();

        private Decimal256To64Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal64(Record record) {
            long hh = value.getDecimal256HH(record);
            long hl = value.getDecimal256HL(record);
            long lh = value.getDecimal256LH(record);
            long ll = value.getDecimal256LL(record);
            if (Decimal256.isNull(hh, hl, lh, ll)) {
                return Decimals.DECIMAL64_NULL;
            }
            decimal256.of(hh, hl, lh, ll, fromScale);
            transformer.transform(decimal256);
            return decimal256.getLl();
        }
    }

    private static class Decimal256To8Func extends DecimalTransformerFunction {
        private final Decimal256 decimal256 = new Decimal256();

        private Decimal256To8Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public byte getDecimal8(Record record) {
            long hh = value.getDecimal256HH(record);
            long hl = value.getDecimal256HL(record);
            long lh = value.getDecimal256LH(record);
            long ll = value.getDecimal256LL(record);
            if (Decimal256.isNull(hh, hl, lh, ll)) {
                return Decimals.DECIMAL8_NULL;
            }
            decimal256.of(hh, hl, lh, ll, fromScale);
            transformer.transform(decimal256);
            return (byte) decimal256.getLl();
        }
    }

    private static class Decimal32To128Func extends DecimalTransformerFunction {
        private final Decimal128 decimal128 = new Decimal128();

        private Decimal32To128Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal128Hi(Record record) {
            int v = value.getDecimal32(record);
            if (v == Decimals.DECIMAL32_NULL) {
                decimal128.ofNull();
            } else {
                decimal128.of(0, v, fromScale);
                transformer.transform(decimal128);
            }
            return decimal128.getHigh();
        }

        @Override
        public long getDecimal128Lo(Record record) {
            return decimal128.getLow();
        }
    }

    private static class Decimal32To16Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal32To16Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public short getDecimal16(Record record) {
            int v = value.getDecimal32(record);
            if (v == Decimals.DECIMAL32_NULL) {
                return Decimals.DECIMAL16_NULL;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return (short) decimal64.getValue();
        }
    }

    private static class Decimal32To256Func extends DecimalTransformerFunction {
        private final Decimal256 decimal256 = new Decimal256();

        private Decimal32To256Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal256HH(Record record) {
            int v = value.getDecimal32(record);
            if (v == Decimals.DECIMAL32_NULL) {
                decimal256.ofNull();
            } else {
                decimal256.ofLong(v, fromScale);
                transformer.transform(decimal256);
            }
            return decimal256.getHh();
        }

        @Override
        public long getDecimal256HL(Record record) {
            return decimal256.getHl();
        }

        @Override
        public long getDecimal256LH(Record record) {
            return decimal256.getLh();
        }

        @Override
        public long getDecimal256LL(Record record) {
            return decimal256.getLl();
        }
    }

    private static class Decimal32To32Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal32To32Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public int getDecimal32(Record record) {
            int v = value.getDecimal32(record);
            if (v == Decimals.DECIMAL32_NULL) {
                return Decimals.DECIMAL32_NULL;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return (int) decimal64.getValue();
        }
    }

    private static class Decimal32To64Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal32To64Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal64(Record record) {
            int v = value.getDecimal32(record);
            if (v == Decimals.DECIMAL32_NULL) {
                return Decimals.DECIMAL64_NULL;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return decimal64.getValue();
        }
    }

    private static class Decimal32To8Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal32To8Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public byte getDecimal8(Record record) {
            int v = value.getDecimal32(record);
            if (v == Decimals.DECIMAL32_NULL) {
                return Decimals.DECIMAL8_NULL;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return (byte) decimal64.getValue();
        }
    }

    private static class Decimal64To128Func extends DecimalTransformerFunction {
        private final Decimal128 decimal128 = new Decimal128();

        private Decimal64To128Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal128Hi(Record record) {
            long v = value.getDecimal64(record);
            if (v == Decimals.DECIMAL64_NULL) {
                decimal128.ofNull();
            } else {
                decimal128.of(0, v, fromScale);
                transformer.transform(decimal128);
            }
            return decimal128.getHigh();
        }

        @Override
        public long getDecimal128Lo(Record record) {
            return decimal128.getLow();
        }
    }

    private static class Decimal64To16Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal64To16Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public short getDecimal16(Record record) {
            long v = value.getDecimal64(record);
            if (v == Decimals.DECIMAL64_NULL) {
                return Decimals.DECIMAL16_NULL;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return (short) decimal64.getValue();
        }
    }

    private static class Decimal64To256Func extends DecimalTransformerFunction {
        private final Decimal256 decimal256 = new Decimal256();

        private Decimal64To256Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal256HH(Record record) {
            long v = value.getDecimal64(record);
            if (v == Decimals.DECIMAL64_NULL) {
                decimal256.ofNull();
            } else {
                decimal256.ofLong(v, fromScale);
                transformer.transform(decimal256);
            }
            return decimal256.getHh();
        }

        @Override
        public long getDecimal256HL(Record record) {
            return decimal256.getHl();
        }

        @Override
        public long getDecimal256LH(Record record) {
            return decimal256.getLh();
        }

        @Override
        public long getDecimal256LL(Record record) {
            return decimal256.getLl();
        }
    }

    private static class Decimal64To32Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal64To32Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public int getDecimal32(Record record) {
            long v = value.getDecimal64(record);
            if (v == Decimals.DECIMAL64_NULL) {
                return Decimals.DECIMAL32_NULL;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return (int) decimal64.getValue();
        }
    }

    private static class Decimal64To64Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal64To64Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal64(Record record) {
            long v = value.getDecimal64(record);
            if (v == Decimals.DECIMAL64_NULL) {
                return Decimals.DECIMAL64_NULL;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return decimal64.getValue();
        }
    }

    private static class Decimal64To8Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal64To8Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public byte getDecimal8(Record record) {
            long v = value.getDecimal64(record);
            if (v == Decimals.DECIMAL64_NULL) {
                return Decimals.DECIMAL8_NULL;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return (byte) decimal64.getValue();
        }
    }

    private static class Decimal8To128Func extends DecimalTransformerFunction {
        private final Decimal128 decimal128 = new Decimal128();

        private Decimal8To128Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal128Hi(Record record) {
            byte v = value.getDecimal8(record);
            if (v == Decimals.DECIMAL8_NULL) {
                decimal128.ofNull();
            } else {
                decimal128.of(0, v, fromScale);
                transformer.transform(decimal128);
            }
            return decimal128.getHigh();
        }

        @Override
        public long getDecimal128Lo(Record record) {
            return decimal128.getLow();
        }
    }

    private static class Decimal8To16Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal8To16Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public short getDecimal16(Record record) {
            byte v = value.getDecimal8(record);
            if (v == Decimals.DECIMAL8_NULL) {
                return Decimals.DECIMAL16_NULL;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return (short) decimal64.getValue();
        }
    }

    private static class Decimal8To256Func extends DecimalTransformerFunction {
        private final Decimal256 decimal256 = new Decimal256();

        private Decimal8To256Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal256HH(Record record) {
            byte v = value.getDecimal8(record);
            if (v == Decimals.DECIMAL8_NULL) {
                decimal256.ofNull();
            } else {
                decimal256.ofLong(v, fromScale);
                transformer.transform(decimal256);
            }
            return decimal256.getHh();
        }

        @Override
        public long getDecimal256HL(Record record) {
            return decimal256.getHl();
        }

        @Override
        public long getDecimal256LH(Record record) {
            return decimal256.getLh();
        }

        @Override
        public long getDecimal256LL(Record record) {
            return decimal256.getLl();
        }
    }

    private static class Decimal8To32Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal8To32Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public int getDecimal32(Record record) {
            byte v = value.getDecimal8(record);
            if (v == Decimals.DECIMAL8_NULL) {
                return Decimals.DECIMAL32_NULL;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return (int) decimal64.getValue();
        }
    }

    private static class Decimal8To64Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal8To64Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public long getDecimal64(Record record) {
            byte v = value.getDecimal8(record);
            if (v == Decimals.DECIMAL8_NULL) {
                return Decimals.DECIMAL64_NULL;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return decimal64.getValue();
        }
    }

    private static class Decimal8To8Func extends DecimalTransformerFunction {
        private final Decimal64 decimal64 = new Decimal64();

        private Decimal8To8Func(int targetType, Function value, DecimalTransformer transformer) {
            super(value, transformer, targetType);
        }

        @Override
        public byte getDecimal8(Record record) {
            byte v = value.getDecimal8(record);
            if (v == Decimals.DECIMAL8_NULL) {
                return v;
            }
            decimal64.of(v, fromScale);
            transformer.transform(decimal64);
            return (byte) decimal64.getValue();
        }
    }

    private static class DecimalTransformerFunction extends DecimalFunction implements UnaryFunction {
        protected final int fromScale;
        protected final DecimalTransformer transformer;
        protected final Function value;

        private DecimalTransformerFunction(Function value, DecimalTransformer transformer, int targetType) {
            super(targetType);
            this.value = value;
            this.transformer = transformer;
            this.fromScale = ColumnType.getDecimalScale(value.getType());
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public String getName() {
            return transformer.getName();
        }
    }
}
