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
import io.questdb.griffin.engine.functions.columns.ByteColumn;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.ByteTypeConstant;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.std.Vect;

/**
 * Type driver for BYTE.
 */
public final class ByteTypeDriver extends FixedSizeTypeDriver {
    public static final ByteTypeDriver INSTANCE = new ByteTypeDriver();
    // the one declared implicit-cast list (F34, PA-7): the overload row, best match first
    private static final short[] IMPLICIT_CASTS = {ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.DECIMAL};

    private ByteTypeDriver() {
        super(
                ColumnTypeTag.BYTE,
                PhysicalDescriptor.Movement.W1,
                PhysicalDescriptor.Arithmetic.I8,
                PhysicalDescriptor.Accessor.BYTE
        );
    }

    @Override
    public int defineBindVariable(BindVariableService service, int index, int columnType, int position) throws SqlException {
        service.setByte(index);
        return columnType;
    }

    @Override
    public short[] getImplicitCasts() {
        return IMPLICIT_CASTS;
    }

    @Override
    public String getName(int columnType) {
        return nameOfBareTag(columnType, ColumnType.BYTE, "BYTE");
    }

    @Override
    public ConstantFunction getNullConstant(int columnType) {
        return ByteConstant.ZERO;
    }

    @Override
    public long getNullLong(int longIndex) {
        return 0L;
    }

    @Override
    public NullPolicy getNullPolicy() {
        return NullPolicy.NONE;
    }

    @Override
    public int getPgArrayOid() {
        return 0;
    }

    @Override
    public int getPgOid() {
        return PgTypeOids.PG_INT2;
    }

    @Override
    public int getRelationBits() {
        return 8;
    }

    @Override
    public RelationKind getRelationKind() {
        return RelationKind.INT;
    }

    @Override
    public char getSignatureChar() {
        return 'b';
    }

    @Override
    public TypeConstant getTypeConstant(int columnType) {
        return columnType == ColumnType.BYTE ? ByteTypeConstant.INSTANCE : null;
    }

    @Override
    public WireKind getWireKind() {
        return WireKind.BYTE;
    }

    @Override
    public boolean isCastTarget(boolean isFromNull) {
        return true;
    }

    @Override
    public Function newColumnFunction(int columnIndex, int columnType) {
        return ByteColumn.newInstance(columnIndex);
    }

    @Override
    public Runnable newNullAppender(MemoryA dataMem, MemoryA auxMem) {
        return () -> dataMem.putByte((byte) 0);
    }

    @Override
    public void setNull(long addr, long count) {
        Vect.memset(addr, count, 0);
    }
}
