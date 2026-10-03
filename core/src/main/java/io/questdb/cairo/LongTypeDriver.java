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

import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.LongTypeConstant;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for LONG.
 */
public final class LongTypeDriver extends FixedSizeTypeDriver {
    public static final LongTypeDriver INSTANCE = new LongTypeDriver();

    private LongTypeDriver() {
        super(
                new TypeFacts(
                        ColumnTypeTag.LONG,
                        PhysicalDescriptor.Movement.W8,
                        PhysicalDescriptor.Arithmetic.I64,
                        PhysicalDescriptor.Accessor.LONG,
                        NullPolicy.SENTINEL,
                        WireKind.LONG,
                        RelationKind.INT,
                        64,
                        new short[]{ColumnType.LONG, ColumnType.DOUBLE, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.DECIMAL},
                        PgTypeOids.PG_INT8,
                        'l',
                        0,
                        Numbers.LONG_NULL,
                        CastTarget.ALWAYS,
                        "LONG"
                ),
                (service, index, columnType, position) -> {
                    service.setLong(index);
                    return columnType;
                },
                columnType -> LongConstant.NULL,
                columnType -> columnType == ColumnType.LONG ? LongTypeConstant.INSTANCE : null,
                (columnIndex, columnType) -> LongColumn.newInstance(columnIndex),
                (dataMem, auxMem) -> () -> dataMem.putLong(Numbers.LONG_NULL),
                (addr, count) -> Vect.setMemoryLong(addr, Numbers.LONG_NULL, count)
        );
    }
}
