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

import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TypeConstant;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.functions.columns.Long128Column;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
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
    // the one declared implicit-cast list (F34, PA-7): the overload row, best match first
    private static final short[] IMPLICIT_CASTS = {ColumnType.LONG128};

    private Long128TypeDriver() {
        super(
                ColumnTypeTag.LONG128,
                PhysicalDescriptor.Movement.W16,
                PhysicalDescriptor.Arithmetic.WIDE,
                PhysicalDescriptor.Accessor.LONG128
        );
    }

    @Override
    public int defineBindVariable(BindVariableService service, int index, int columnType, int position) throws SqlException {
        // no bind variable holds a LONG128
        throw BindVariableServiceImpl.newBindRefusal(position, columnType, index);
    }

    @Override
    public short[] getImplicitCasts() {
        return IMPLICIT_CASTS;
    }

    @Override
    public String getName(int columnType) {
        return nameOfBareTag(columnType, ColumnType.LONG128, "LONG128");
    }

    @Override
    public ConstantFunction getNullConstant(int columnType) {
        return Long128Constant.NULL;
    }

    @Override
    public long getNullLong(int longIndex) {
        return Numbers.LONG_NULL;
    }

    @Override
    public NullPolicy getNullPolicy() {
        return NullPolicy.SENTINEL;
    }

    // LONG128 has no SQL type name to CAST to
    @Override
    public int getRelationBits() {
        return 128;
    }

    @Override
    public RelationKind getRelationKind() {
        return RelationKind.LONG128;
    }

    @Override
    public TypeConstant getTypeConstant(int columnType) {
        return null;
    }

    @Override
    public boolean isCastTarget(boolean isFromNull) {
        return false;
    }

    @Override
    public Function newColumnFunction(int columnIndex, int columnType) {
        return Long128Column.newInstance(columnIndex);
    }

    @Override
    public Runnable newNullAppender(MemoryA dataMem, MemoryA auxMem) {
        return () -> dataMem.putLong128(Numbers.LONG_NULL, Numbers.LONG_NULL);
    }

    @Override
    public void setNull(long addr, long count) {
        Vect.setMemoryLong(addr, Numbers.LONG_NULL, count * 2);
    }
}
