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

import io.questdb.griffin.engine.functions.columns.IPv4Column;
import io.questdb.griffin.engine.functions.constants.IPv4Constant;
import io.questdb.griffin.engine.functions.constants.IPv4TypeConstant;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for IPv4.
 */
public final class IPv4TypeDriver extends FixedSizeTypeDriver {
    public static final IPv4TypeDriver INSTANCE = new IPv4TypeDriver();

    private IPv4TypeDriver() {
        super(
                new TypeFacts(
                        ColumnTypeTag.IPv4,
                        PhysicalDescriptor.Movement.W4,
                        PhysicalDescriptor.Arithmetic.U32,
                        PhysicalDescriptor.Accessor.IPv4,
                        NullPolicy.SENTINEL,
                        WireKind.IPV4,
                        RelationKind.IPV4,
                        32,
                        new short[]{ColumnType.IPv4, ColumnType.STRING, ColumnType.VARCHAR},
                        // the address travels as its dotted text
                        PgTypeOids.PG_VARCHAR,
                        'x',
                        0,
                        Numbers.IPv4_NULL,
                        CastTarget.ALWAYS,
                        "IPv4"
                ),
                (service, index, columnType, position) -> {
                    service.setIPv4(index);
                    return columnType;
                },
                columnType -> IPv4Constant.NULL,
                columnType -> columnType == ColumnType.IPv4 ? IPv4TypeConstant.INSTANCE : null,
                (columnIndex, columnType) -> IPv4Column.newInstance(columnIndex),
                (dataMem, auxMem) -> () -> dataMem.putInt(Numbers.IPv4_NULL),
                (addr, count) -> Vect.setMemoryInt(addr, Numbers.IPv4_NULL, count)
        );
    }
}
