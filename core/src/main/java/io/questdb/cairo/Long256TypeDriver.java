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
import io.questdb.griffin.engine.functions.columns.Long256Column;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
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
    // the one declared implicit-cast list (F34, PA-7): the overload row, best match first
    private static final short[] IMPLICIT_CASTS = {ColumnType.LONG256, ColumnType.LONG};

    private Long256TypeDriver() {
        super(
                ColumnTypeTag.LONG256,
                PhysicalDescriptor.Movement.W32,
                PhysicalDescriptor.Arithmetic.WIDE,
                PhysicalDescriptor.Accessor.LONG256
        );
    }

    @Override
    public int defineBindVariable(BindVariableService service, int index, int columnType, int position) throws SqlException {
        service.setLong256(index);
        return columnType;
    }

    @Override
    public short[] getImplicitCasts() {
        return IMPLICIT_CASTS;
    }

    @Override
    public String getName(int columnType) {
        return nameOfBareTag(columnType, ColumnType.LONG256, "LONG256");
    }

    @Override
    public ConstantFunction getNullConstant(int columnType) {
        return Long256NullConstant.INSTANCE;
    }

    @Override
    public long getNullLong(int longIndex) {
        return Numbers.LONG_NULL;
    }

    @Override
    public NullPolicy getNullPolicy() {
        return NullPolicy.SENTINEL;
    }

    @Override
    public int getPgArrayOid() {
        return 0;
    }

    // PostgreSQL has no 256-bit integer; the value travels as its hex text
    @Override
    public int getPgOid() {
        return PgTypeOids.PG_VARCHAR;
    }

    @Override
    public int getRelationBits() {
        return 256;
    }

    @Override
    public RelationKind getRelationKind() {
        return RelationKind.LONG256;
    }

    @Override
    public char getSignatureChar() {
        return 'h';
    }

    @Override
    public TypeConstant getTypeConstant(int columnType) {
        return columnType == ColumnType.LONG256 ? Long256TypeConstant.INSTANCE : null;
    }

    @Override
    public WireKind getWireKind() {
        return WireKind.LONG256;
    }

    @Override
    public boolean isCastTarget(boolean isFromNull) {
        return true;
    }

    @Override
    public Function newColumnFunction(int columnIndex, int columnType) {
        return Long256Column.newInstance(columnIndex);
    }

    @Override
    public Runnable newNullAppender(MemoryA dataMem, MemoryA auxMem) {
        return () -> dataMem.putLong256(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
    }

    @Override
    public void setNull(long addr, long count) {
        Vect.setMemoryLong(addr, Numbers.LONG_NULL, count * 4);
    }
}
