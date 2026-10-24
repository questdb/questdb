/*+*****************************************************************************
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


package io.questdb.cairo;

import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.engine.functions.columns.DecimalColumn;
import io.questdb.std.Decimals;
import io.questdb.std.Vect;

/**
 * Type driver for the stored decimal family: DECIMAL8 to DECIMAL256 are one type stored at
 * six widths, so they share one class with one instance per tag. Precision and scale are part
 * of the encoded column type and are passed as an argument where a method needs them. The
 * DECIMAL pseudo tag, which only resolves function overloads, has no driver.
 * <p>
 * The DECIMAL pseudo tag names the widths in function signatures and in CAST, with its
 * precision and scale, so a width has no signature character and no type constant of its own
 * and is no CAST target.
 */
public final class DecimalTypeDriver extends FixedSizeTypeDriver {
    public static final DecimalTypeDriver DECIMAL128 = new DecimalTypeDriver(
            new TypeFacts(
                    ColumnTypeTag.DECIMAL128,
                    PhysicalDescriptor.Movement.W16,
                    PhysicalDescriptor.Arithmetic.WIDE,
                    PhysicalDescriptor.Accessor.DECIMAL128,
                    NullPolicy.SENTINEL,
                    WireKind.DECIMAL128,
                    RelationKind.DECIMAL,
                    128,
                    new short[]{ColumnType.DECIMAL128, ColumnType.DECIMAL256, ColumnType.DECIMAL},
                    PgTypeOids.PG_NUMERIC,
                    FunctionFactoryDescriptor.NO_SIGNATURE_CHAR,
                    0,
                    Decimals.DECIMAL128_HI_NULL,
                    CastTarget.NEVER,
                    ColumnType.UNKNOWN_NAME
            ),
            (dataMem, auxMem) -> () -> dataMem.putDecimal128(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL),
            (addr, count) -> Vect.setMemoryLong128(addr, Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, count)
    );
    public static final DecimalTypeDriver DECIMAL16 = new DecimalTypeDriver(
            new TypeFacts(
                    ColumnTypeTag.DECIMAL16,
                    PhysicalDescriptor.Movement.W2,
                    PhysicalDescriptor.Arithmetic.I16,
                    PhysicalDescriptor.Accessor.DECIMAL16,
                    NullPolicy.SENTINEL,
                    WireKind.DECIMAL16,
                    RelationKind.DECIMAL,
                    16,
                    new short[]{ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64, ColumnType.DECIMAL128, ColumnType.DECIMAL256, ColumnType.DECIMAL},
                    PgTypeOids.PG_NUMERIC,
                    FunctionFactoryDescriptor.NO_SIGNATURE_CHAR,
                    0,
                    Decimals.DECIMAL16_NULL,
                    CastTarget.NEVER,
                    ColumnType.UNKNOWN_NAME
            ),
            (dataMem, auxMem) -> () -> dataMem.putShort(Decimals.DECIMAL16_NULL),
            (addr, count) -> Vect.setMemoryShort(addr, Decimals.DECIMAL16_NULL, count)
    );
    public static final DecimalTypeDriver DECIMAL256 = new DecimalTypeDriver(
            new TypeFacts(
                    ColumnTypeTag.DECIMAL256,
                    PhysicalDescriptor.Movement.W32,
                    PhysicalDescriptor.Arithmetic.WIDE,
                    PhysicalDescriptor.Accessor.DECIMAL256,
                    NullPolicy.SENTINEL,
                    WireKind.DECIMAL256,
                    RelationKind.DECIMAL,
                    256,
                    new short[]{ColumnType.DECIMAL256, ColumnType.DECIMAL},
                    PgTypeOids.PG_NUMERIC,
                    FunctionFactoryDescriptor.NO_SIGNATURE_CHAR,
                    0,
                    Decimals.DECIMAL256_HH_NULL,
                    CastTarget.NEVER,
                    ColumnType.UNKNOWN_NAME
            ),
            (dataMem, auxMem) -> () -> dataMem.putDecimal256(Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL, Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL),
            (addr, count) -> Vect.setMemoryLong256(addr, Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                    Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL, count)
    );
    public static final DecimalTypeDriver DECIMAL32 = new DecimalTypeDriver(
            new TypeFacts(
                    ColumnTypeTag.DECIMAL32,
                    PhysicalDescriptor.Movement.W4,
                    PhysicalDescriptor.Arithmetic.I32,
                    PhysicalDescriptor.Accessor.DECIMAL32,
                    NullPolicy.SENTINEL,
                    WireKind.DECIMAL32,
                    RelationKind.DECIMAL,
                    32,
                    new short[]{ColumnType.DECIMAL32, ColumnType.DECIMAL64, ColumnType.DECIMAL128, ColumnType.DECIMAL256, ColumnType.DECIMAL},
                    PgTypeOids.PG_NUMERIC,
                    FunctionFactoryDescriptor.NO_SIGNATURE_CHAR,
                    0,
                    Decimals.DECIMAL32_NULL,
                    CastTarget.NEVER,
                    ColumnType.UNKNOWN_NAME
            ),
            (dataMem, auxMem) -> () -> dataMem.putInt(Decimals.DECIMAL32_NULL),
            (addr, count) -> Vect.setMemoryInt(addr, Decimals.DECIMAL32_NULL, count)
    );
    public static final DecimalTypeDriver DECIMAL64 = new DecimalTypeDriver(
            new TypeFacts(
                    ColumnTypeTag.DECIMAL64,
                    PhysicalDescriptor.Movement.W8,
                    PhysicalDescriptor.Arithmetic.I64,
                    PhysicalDescriptor.Accessor.DECIMAL64,
                    NullPolicy.SENTINEL,
                    WireKind.DECIMAL64,
                    RelationKind.DECIMAL,
                    64,
                    new short[]{ColumnType.DECIMAL64, ColumnType.DECIMAL128, ColumnType.DECIMAL256, ColumnType.DECIMAL},
                    PgTypeOids.PG_NUMERIC,
                    FunctionFactoryDescriptor.NO_SIGNATURE_CHAR,
                    0,
                    Decimals.DECIMAL64_NULL,
                    CastTarget.NEVER,
                    ColumnType.UNKNOWN_NAME
            ),
            (dataMem, auxMem) -> () -> dataMem.putLong(Decimals.DECIMAL64_NULL),
            (addr, count) -> Vect.setMemoryLong(addr, Decimals.DECIMAL64_NULL, count)
    );
    public static final DecimalTypeDriver DECIMAL8 = new DecimalTypeDriver(
            new TypeFacts(
                    ColumnTypeTag.DECIMAL8,
                    PhysicalDescriptor.Movement.W1,
                    PhysicalDescriptor.Arithmetic.I8,
                    PhysicalDescriptor.Accessor.DECIMAL8,
                    NullPolicy.SENTINEL,
                    WireKind.DECIMAL8,
                    RelationKind.DECIMAL,
                    8,
                    new short[]{ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64, ColumnType.DECIMAL128, ColumnType.DECIMAL256, ColumnType.DECIMAL},
                    PgTypeOids.PG_NUMERIC,
                    FunctionFactoryDescriptor.NO_SIGNATURE_CHAR,
                    0,
                    Decimals.DECIMAL8_NULL,
                    CastTarget.NEVER,
                    ColumnType.UNKNOWN_NAME
            ),
            (dataMem, auxMem) -> () -> dataMem.putByte(Decimals.DECIMAL8_NULL),
            (addr, count) -> Vect.memset(addr, count, Decimals.DECIMAL8_NULL)
    );
    // DECIMAL(<precision>,<scale>), built on first use: most of the 77 x 77 names are never printed
    private static final String[][] NAMES = new String[Decimals.MAX_PRECISION + 1][Decimals.MAX_SCALE + 1];

    private DecimalTypeDriver(TypeFacts facts, NullAppenderFactory nullAppenderFactory, NullFiller nullFiller) {
        super(
                facts,
                (service, index, columnType, position) -> {
                    service.setDecimal(index, columnType);
                    return columnType;
                },
                // typed by the encoded precision and scale
                columnType -> DecimalUtil.createNullDecimalConstant(
                        ColumnType.getDecimalPrecision(columnType),
                        ColumnType.getDecimalScale(columnType)
                ),
                columnType -> null,
                (columnIndex, columnType) -> DecimalColumn.newInstance(columnIndex, columnType),
                nullAppenderFactory,
                nullFiller
        );
    }

    /**
     * Named by the encoded precision and scale; a bare tag, which carries neither, has no name.
     */
    @Override
    public String getName(int columnType) {
        final int precision = ColumnType.getDecimalPrecision(columnType);
        final int scale = ColumnType.getDecimalScale(columnType);
        if (precision < 1 || precision > Decimals.MAX_PRECISION || scale > Decimals.MAX_SCALE
                || columnType != ColumnType.getDecimalType(precision, scale)) {
            return ColumnType.UNKNOWN_NAME;
        }
        String name = NAMES[precision][scale];
        if (name == null) {
            name = "DECIMAL(" + precision + ',' + scale + ')';
            NAMES[precision][scale] = name;
        }
        return name;
    }

    /**
     * The NULL of DECIMAL128 and DECIMAL256 differs from long to long, so the NULL word is only
     * the first long.
     */
    @Override
    public long getNullLong(int longIndex) {
        return switch (getPow2Width()) {
            case 0 -> Decimals.DECIMAL8_NULL;
            case 1 -> Decimals.DECIMAL16_NULL;
            case 2 -> Decimals.DECIMAL32_NULL;
            case 3 -> Decimals.DECIMAL64_NULL;
            case 4 -> longIndex == 0 ? Decimals.DECIMAL128_HI_NULL : Decimals.DECIMAL128_LO_NULL;
            case 5 -> switch (longIndex) {
                case 0 -> Decimals.DECIMAL256_HH_NULL;
                case 1 -> Decimals.DECIMAL256_HL_NULL;
                case 2 -> Decimals.DECIMAL256_LH_NULL;
                default -> Decimals.DECIMAL256_LL_NULL;
            };
            default -> throw new IllegalStateException("no decimal width " + getPow2Width());
        };
    }
}
