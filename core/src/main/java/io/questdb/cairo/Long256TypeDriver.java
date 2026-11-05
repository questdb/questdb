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

import io.questdb.griffin.engine.functions.columns.Long256Column;
import io.questdb.griffin.engine.functions.constants.Long256NullConstant;
import io.questdb.griffin.engine.functions.constants.Long256TypeConstant;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for LONG256.
 * <p>
 * A LONG256 is NULL when all four longs are LONG_NULL.
 */
public final class Long256TypeDriver extends FixedSizeTypeDriver {
    public static final Long256TypeDriver INSTANCE = new Long256TypeDriver();

    private Long256TypeDriver() {
        super(
                new TypeFacts(
                        ColumnTypeTag.LONG256,
                        PhysicalDescriptor.Movement.W32,
                        PhysicalDescriptor.Arithmetic.WIDE,
                        PhysicalDescriptor.Accessor.LONG256,
                        NullPolicy.SENTINEL,
                        WireKind.LONG256,
                        RelationKind.LONG256,
                        256,
                        new short[]{ColumnType.LONG256, ColumnType.LONG},
                        // PostgreSQL has no 256-bit integer; the value travels as its hex text
                        PgTypeOids.PG_VARCHAR,
                        'h',
                        0,
                        Numbers.LONG_NULL,
                        CastTarget.ALWAYS,
                        "LONG256"
                ),
                (service, index, columnType, position) -> {
                    service.setLong256(index);
                    return columnType;
                },
                columnType -> Long256NullConstant.INSTANCE,
                columnType -> columnType == ColumnType.LONG256 ? Long256TypeConstant.INSTANCE : null,
                (columnIndex, columnType) -> Long256Column.newInstance(columnIndex),
                (dataMem, auxMem) -> () -> dataMem.putLong256(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL),
                (addr, count) -> Vect.setMemoryLong(addr, Numbers.LONG_NULL, count * 4)
        );
    }
}
