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

import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.DoubleTypeConstant;
import io.questdb.std.Vect;

/**
 * Type driver for DOUBLE.
 */
public final class DoubleTypeDriver extends FixedSizeTypeDriver {
    public static final DoubleTypeDriver INSTANCE = new DoubleTypeDriver();

    private DoubleTypeDriver() {
        super(
                new TypeFacts(
                        ColumnTypeTag.DOUBLE,
                        PhysicalDescriptor.Movement.W8,
                        PhysicalDescriptor.Arithmetic.F64,
                        PhysicalDescriptor.Accessor.DOUBLE,
                        NullPolicy.SENTINEL,
                        WireKind.DOUBLE,
                        RelationKind.FLOAT,
                        64,
                        new short[]{ColumnType.DOUBLE},
                        PgTypeOids.PG_FLOAT8,
                        'd',
                        PgTypeOids.PG_ARR_FLOAT8,
                        Double.doubleToLongBits(Double.NaN),
                        CastTarget.ALWAYS,
                        "DOUBLE"
                ),
                (service, index, columnType, position) -> {
                    service.setDouble(index);
                    return columnType;
                },
                columnType -> DoubleConstant.NULL,
                columnType -> columnType == ColumnType.DOUBLE ? DoubleTypeConstant.INSTANCE : null,
                (columnIndex, columnType) -> DoubleColumn.newInstance(columnIndex),
                (dataMem, auxMem) -> () -> dataMem.putDouble(Double.NaN),
                (addr, count) -> Vect.setMemoryDouble(addr, Double.NaN, count)
        );
    }
}
