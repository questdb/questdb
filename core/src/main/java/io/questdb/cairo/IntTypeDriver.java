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

import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.IntTypeConstant;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for INT.
 */
public final class IntTypeDriver extends FixedSizeTypeDriver {
    public static final IntTypeDriver INSTANCE = new IntTypeDriver();

    private IntTypeDriver() {
        super(
                new TypeFacts(
                        ColumnTypeTag.INT,
                        PhysicalDescriptor.Movement.W4,
                        PhysicalDescriptor.Arithmetic.I32,
                        PhysicalDescriptor.Accessor.INT,
                        NullPolicy.SENTINEL,
                        WireKind.INT,
                        RelationKind.INT,
                        32,
                        new short[]{ColumnType.INT, ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.DECIMAL},
                        PgTypeOids.PG_INT4,
                        'i',
                        0,
                        Numbers.encodeLowHighInts(Numbers.INT_NULL, Numbers.INT_NULL),
                        CastTarget.ALWAYS,
                        "INT"
                ),
                (service, index, columnType, position) -> {
                    service.setInt(index);
                    return columnType;
                },
                columnType -> IntConstant.NULL,
                columnType -> columnType == ColumnType.INT ? IntTypeConstant.INSTANCE : null,
                (columnIndex, columnType) -> IntColumn.newInstance(columnIndex),
                (dataMem, auxMem) -> () -> dataMem.putInt(Numbers.INT_NULL),
                (addr, count) -> Vect.setMemoryInt(addr, Numbers.INT_NULL, count)
        );
    }
}
