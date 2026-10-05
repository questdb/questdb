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

import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.constants.TimestampTypeConstant;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for TIMESTAMP, both TIMESTAMP_MICRO and TIMESTAMP_NANO. Precision-specific logic
 * lives in {@link TimestampDriver}; methods that need it fetch it with {@link
 * ColumnType#getTimestampDriver(int)}.
 */
public final class TimestampTypeDriver extends FixedSizeTypeDriver {
    public static final TimestampTypeDriver INSTANCE = new TimestampTypeDriver();

    private TimestampTypeDriver() {
        super(
                new TypeFacts(
                        ColumnTypeTag.TIMESTAMP,
                        PhysicalDescriptor.Movement.W8,
                        PhysicalDescriptor.Arithmetic.I64,
                        PhysicalDescriptor.Accessor.TIMESTAMP,
                        NullPolicy.SENTINEL,
                        WireKind.TIMESTAMP,
                        RelationKind.TEMPORAL,
                        64,
                        new short[]{ColumnType.TIMESTAMP, ColumnType.LONG, ColumnType.DATE, ColumnType.DOUBLE},
                        PgTypeOids.PG_TIMESTAMP,
                        'n',
                        0,
                        Numbers.LONG_NULL,
                        CastTarget.ALWAYS,
                        "TIMESTAMP"
                ),
                (service, index, columnType, position) -> {
                    service.setTimestampWithType(index, columnType, Numbers.LONG_NULL);
                    return columnType;
                },
                columnType -> ColumnType.getTimestampDriver(columnType).getTimestampConstantNull(),
                columnType -> switch (columnType) {
                    case ColumnType.TIMESTAMP_MICRO -> TimestampTypeConstant.TIMESTAMP_MS_CONSTANT;
                    case ColumnType.TIMESTAMP_NANO -> TimestampTypeConstant.TIMESTAMP_NS_CONSTANT;
                    default -> null;
                },
                (columnIndex, columnType) -> TimestampColumn.newInstance(columnIndex, columnType),
                (dataMem, auxMem) -> () -> dataMem.putLong(Numbers.LONG_NULL),
                (addr, count) -> Vect.setMemoryLong(addr, Numbers.LONG_NULL, count)
        );
    }

    /**
     * Named by precision; a TIMESTAMP with the designated flag set has no name.
     */
    @Override
    public String getName(int columnType) {
        return switch (columnType) {
            case ColumnType.TIMESTAMP_MICRO -> "TIMESTAMP";
            case ColumnType.TIMESTAMP_NANO -> "TIMESTAMP_NS";
            default -> ColumnType.UNKNOWN_NAME;
        };
    }
}
