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

import io.questdb.griffin.engine.functions.columns.ShortColumn;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.griffin.engine.functions.constants.ShortTypeConstant;
import io.questdb.std.Vect;

public final class ShortTypeDriver extends FixedSizeTypeDriver {
    public static final ShortTypeDriver INSTANCE = new ShortTypeDriver();

    private ShortTypeDriver() {
        super(
                new TypeFacts(
                        ColumnTypeTag.SHORT,
                        PhysicalDescriptor.Movement.W2,
                        PhysicalDescriptor.Arithmetic.I16,
                        PhysicalDescriptor.Accessor.SHORT,
                        NullPolicy.NONE,
                        WireKind.SHORT,
                        RelationKind.INT,
                        16,
                        new short[]{ColumnType.SHORT, ColumnType.INT, ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.CHAR, ColumnType.DECIMAL},
                        PgTypeOids.PG_INT2,
                        'e',
                        0,
                        0L,
                        CastTarget.ALWAYS,
                        "SHORT"
                ),
                (service, index, columnType, position) -> {
                    service.setShort(index);
                    return columnType;
                },
                columnType -> ShortConstant.ZERO,
                columnType -> columnType == ColumnType.SHORT ? ShortTypeConstant.INSTANCE : null,
                (columnIndex, columnType) -> ShortColumn.newInstance(columnIndex),
                (dataMem, auxMem) -> () -> dataMem.putShort((short) 0),
                (addr, count) -> Vect.setMemoryShort(addr, (short) 0, count)
        );
    }
}
