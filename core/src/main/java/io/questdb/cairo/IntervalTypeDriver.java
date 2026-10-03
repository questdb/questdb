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

import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.functions.columns.IntervalColumn;
import io.questdb.griffin.engine.functions.constants.IntervalConstant;
import io.questdb.griffin.engine.functions.constants.IntervalTypeConstant;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for INTERVAL.
 * <p>
 * INTERVAL is an in-memory value type that is never persisted; the width is that of
 * its two-long value, and it is NULL when both longs are LONG_NULL.
 */
public final class IntervalTypeDriver extends FixedSizeTypeDriver {
    public static final IntervalTypeDriver INSTANCE = new IntervalTypeDriver();

    private IntervalTypeDriver() {
        super(
                new TypeFacts(
                        ColumnTypeTag.INTERVAL,
                        PhysicalDescriptor.Movement.W16,
                        PhysicalDescriptor.Arithmetic.NONE,
                        PhysicalDescriptor.Accessor.INTERVAL,
                        NullPolicy.SENTINEL,
                        WireKind.INTERVAL,
                        RelationKind.INTERVAL,
                        0,
                        new short[]{ColumnType.INTERVAL, ColumnType.STRING},
                        // the interval travels as its text
                        PgTypeOids.PG_VARCHAR,
                        'δ',
                        0,
                        Numbers.LONG_NULL,
                        // the parser takes INTERVAL as a CAST target from NULL only
                        CastTarget.FROM_NULL_ONLY,
                        "INTERVAL"
                ),
                (service, index, columnType, position) -> {
                    // no bind variable holds an INTERVAL
                    throw BindVariableServiceImpl.newBindRefusal(position, columnType, index);
                },
                // an interval type carries its timestamp precision; the bare tag is the raw interval
                columnType -> {
                    if (columnType != ColumnType.INTERVAL) {
                        return IntervalUtils.getTimestampDriverByIntervalType(columnType).getIntervalConstantNull();
                    }
                    return IntervalConstant.RAW_NULL;
                },
                columnType -> switch (columnType) {
                    case ColumnType.INTERVAL_RAW -> IntervalTypeConstant.RAW_INSTANCE;
                    case ColumnType.INTERVAL_TIMESTAMP_MICRO -> IntervalTypeConstant.TIMESTAMP_MICRO_INSTANCE;
                    case ColumnType.INTERVAL_TIMESTAMP_NANO -> IntervalTypeConstant.TIMESTAMP_NANO_INSTANCE;
                    default -> null;
                },
                (columnIndex, columnType) -> IntervalColumn.newInstance(columnIndex, columnType),
                (dataMem, auxMem) -> () -> dataMem.putLong128(Numbers.LONG_NULL, Numbers.LONG_NULL),
                (addr, count) -> Vect.setMemoryLong(addr, Numbers.LONG_NULL, count * 2)
        );
    }

    /**
     * The raw interval and both timestamp precisions share one name.
     */
    @Override
    public String getName(int columnType) {
        return switch (columnType) {
            case ColumnType.INTERVAL_RAW, ColumnType.INTERVAL_TIMESTAMP_MICRO, ColumnType.INTERVAL_TIMESTAMP_NANO ->
                    "INTERVAL";
            default -> ColumnType.UNKNOWN_NAME;
        };
    }
}
