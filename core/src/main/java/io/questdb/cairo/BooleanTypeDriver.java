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

import io.questdb.griffin.engine.functions.columns.BooleanColumn;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.BooleanTypeConstant;
import io.questdb.std.Vect;

public final class BooleanTypeDriver extends FixedSizeTypeDriver {
    public static final BooleanTypeDriver INSTANCE = new BooleanTypeDriver();

    private BooleanTypeDriver() {
        super(
                new TypeFacts(
                        ColumnTypeTag.BOOLEAN,
                        PhysicalDescriptor.Movement.W1,
                        PhysicalDescriptor.Arithmetic.U8,
                        PhysicalDescriptor.Accessor.BOOLEAN,
                        NullPolicy.NONE,
                        WireKind.BOOLEAN,
                        RelationKind.BOOL,
                        1,
                        new short[]{ColumnType.BOOLEAN},
                        PgTypeOids.PG_BOOL,
                        't',
                        0,
                        0L,
                        CastTarget.ALWAYS,
                        "BOOLEAN"
                ),
                (service, index, columnType, position) -> {
                    service.setBoolean(index);
                    return columnType;
                },
                columnType -> BooleanConstant.FALSE,
                columnType -> columnType == ColumnType.BOOLEAN ? BooleanTypeConstant.INSTANCE : null,
                (columnIndex, columnType) -> BooleanColumn.newInstance(columnIndex),
                (dataMem, auxMem) -> () -> dataMem.putByte((byte) 0),
                (addr, count) -> Vect.memset(addr, count, 0)
        );
    }
}
