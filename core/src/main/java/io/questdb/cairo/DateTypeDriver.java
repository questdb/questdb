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

import io.questdb.griffin.engine.functions.columns.DateColumn;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.DateTypeConstant;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for DATE. Temporal arithmetic on DATE values lives in {@link MillisTimestampDriver}.
 */
public final class DateTypeDriver extends FixedSizeTypeDriver {
    public static final DateTypeDriver INSTANCE = new DateTypeDriver();

    private DateTypeDriver() {
        super(
                new TypeFacts(
                        ColumnTypeTag.DATE,
                        PhysicalDescriptor.Movement.W8,
                        PhysicalDescriptor.Arithmetic.I64,
                        PhysicalDescriptor.Accessor.DATE,
                        NullPolicy.SENTINEL,
                        WireKind.DATE,
                        RelationKind.TEMPORAL,
                        64,
                        new short[]{ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.LONG, ColumnType.DOUBLE},
                        // PostgreSQL DATE has day precision, so DATE travels as TIMESTAMP (millisecond precision kept)
                        PgTypeOids.PG_TIMESTAMP,
                        'm',
                        0,
                        Numbers.LONG_NULL,
                        CastTarget.ALWAYS,
                        "DATE"
                ),
                (service, index, columnType, position) -> {
                    service.setDate(index);
                    return columnType;
                },
                columnType -> DateConstant.NULL,
                columnType -> columnType == ColumnType.DATE ? DateTypeConstant.INSTANCE : null,
                (columnIndex, columnType) -> DateColumn.newInstance(columnIndex),
                (dataMem, auxMem) -> () -> dataMem.putLong(Numbers.LONG_NULL),
                (addr, count) -> Vect.setMemoryLong(addr, Numbers.LONG_NULL, count)
        );
    }
}
