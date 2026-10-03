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
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.LongTypeConstant;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for LONG.
 */
public final class LongTypeDriver extends FixedSizeTypeDriver {
    public static final LongTypeDriver INSTANCE = new LongTypeDriver();
    // the one declared implicit-cast list (F34, PA-7): the overload row, best match first
    private static final short[] IMPLICIT_CASTS = {ColumnType.LONG, ColumnType.DOUBLE, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.DECIMAL};

    private LongTypeDriver() {
        super(
                ColumnTypeTag.LONG,
                PhysicalDescriptor.Movement.W8,
                PhysicalDescriptor.Arithmetic.I64,
                PhysicalDescriptor.Accessor.LONG
        );
    }

    @Override
    public int defineBindVariable(BindVariableService service, int index, int columnType, int position) throws SqlException {
        service.setLong(index);
        return columnType;
    }

    @Override
    public short[] getImplicitCasts() {
        return IMPLICIT_CASTS;
    }

    @Override
    public String getName(int columnType) {
        return nameOfBareTag(columnType, ColumnType.LONG, "LONG");
    }

    @Override
    public ConstantFunction getNullConstant(int columnType) {
        return LongConstant.NULL;
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

    @Override
    public int getPgOid() {
        return PgTypeOids.PG_INT8;
    }

    @Override
    public int getRelationBits() {
        return 64;
    }

    @Override
    public RelationKind getRelationKind() {
        return RelationKind.INT;
    }

    @Override
    public char getSignatureChar() {
        return 'l';
    }

    @Override
    public TypeConstant getTypeConstant(int columnType) {
        return columnType == ColumnType.LONG ? LongTypeConstant.INSTANCE : null;
    }

    @Override
    public WireKind getWireKind() {
        return WireKind.LONG;
    }

    @Override
    public boolean isCastTarget(boolean isFromNull) {
        return true;
    }

    @Override
    public Function newColumnFunction(int columnIndex, int columnType) {
        return LongColumn.newInstance(columnIndex);
    }

    @Override
    public Runnable newNullAppender(MemoryA dataMem, MemoryA auxMem) {
        return () -> dataMem.putLong(Numbers.LONG_NULL);
    }

    @Override
    public void setNull(long addr, long count) {
        Vect.setMemoryLong(addr, Numbers.LONG_NULL, count);
    }
}
