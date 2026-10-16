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
import io.questdb.griffin.engine.functions.columns.Long128Column;
import io.questdb.griffin.engine.functions.constants.Long128Constant;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for LONG128.
 * <p>
 * A LONG128 is NULL when both longs are LONG_NULL.
 */
public final class Long128TypeDriver extends FixedSizeTypeDriver {
    public static final Long128TypeDriver INSTANCE = new Long128TypeDriver();

    private Long128TypeDriver() {
        super(
                new TypeFacts(
                        ColumnTypeTag.LONG128,
                        PhysicalDescriptor.Movement.W16,
                        PhysicalDescriptor.Arithmetic.WIDE,
                        PhysicalDescriptor.Accessor.LONG128,
                        NullPolicy.SENTINEL,
                        WireKind.LONG128,
                        RelationKind.LONG128,
                        128,
                        new short[]{ColumnType.LONG128},
                        // PostgreSQL wire cannot send LONG128
                        0,
                        'j',
                        0,
                        Numbers.LONG_NULL,
                        CastTarget.NEVER,
                        "LONG128"
                ),
                (service, index, columnType, position) -> {
                    // no bind variable holds a LONG128
                    throw BindVariableServiceImpl.newBindRefusal(position, columnType, index);
                },
                columnType -> Long128Constant.NULL,
                // LONG128 is never a CAST target (CastTarget.NEVER), so it has no type constant
                columnType -> null,
                (columnIndex, columnType) -> Long128Column.newInstance(columnIndex),
                (dataMem, auxMem) -> () -> dataMem.putLong128(Numbers.LONG_NULL, Numbers.LONG_NULL),
                (addr, count) -> Vect.setMemoryLong(addr, Numbers.LONG_NULL, count * 2)
        );
    }
}
