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
import io.questdb.griffin.engine.functions.columns.DateColumn;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.DateTypeConstant;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for DATE.
 * <p>
 * Temporal arithmetic on DATE values lives in {@link MillisTimestampDriver}, a
 * separate facet fetched where a method needs it.
 */
public final class DateTypeDriver extends FixedSizeTypeDriver {
    public static final DateTypeDriver INSTANCE = new DateTypeDriver();
    // the one declared implicit-cast list (F34, PA-7): the overload row, best match first
    private static final short[] IMPLICIT_CASTS = {ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.LONG, ColumnType.DOUBLE};

    private DateTypeDriver() {
        super(
                ColumnTypeTag.DATE,
                PhysicalDescriptor.Movement.W8,
                PhysicalDescriptor.Arithmetic.I64,
                PhysicalDescriptor.Accessor.DATE
        );
    }

    @Override
    public int defineBindVariable(BindVariableService service, int index, int columnType, int position) throws SqlException {
        service.setDate(index);
        return columnType;
    }

    @Override
    public short[] getImplicitCasts() {
        return IMPLICIT_CASTS;
    }

    @Override
    public String getName(int columnType) {
        return nameOfBareTag(columnType, ColumnType.DATE, "DATE");
    }

    @Override
    public ConstantFunction getNullConstant(int columnType) {
        return DateConstant.NULL;
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
    public int getRelationBits() {
        return 64;
    }

    @Override
    public RelationKind getRelationKind() {
        return RelationKind.TEMPORAL;
    }

    @Override
    public TypeConstant getTypeConstant(int columnType) {
        return columnType == ColumnType.DATE ? DateTypeConstant.INSTANCE : null;
    }

    @Override
    public boolean isCastTarget(boolean isFromNull) {
        return true;
    }

    @Override
    public Function newColumnFunction(int columnIndex, int columnType) {
        return DateColumn.newInstance(columnIndex);
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
