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
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.TimestampTypeConstant;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for TIMESTAMP.
 * <p>
 * Serves both TIMESTAMP_MICRO and TIMESTAMP_NANO; the precision-specific
 * {@link TimestampDriver} is a separate facet, fetched with
 * {@link ColumnType#getTimestampDriver(int)} where a method needs it.
 */
public final class TimestampTypeDriver extends FixedSizeTypeDriver {
    public static final TimestampTypeDriver INSTANCE = new TimestampTypeDriver();
    // the one declared implicit-cast list (F34, PA-7): the overload row, best match first
    private static final short[] IMPLICIT_CASTS = {ColumnType.TIMESTAMP, ColumnType.LONG, ColumnType.DATE, ColumnType.DOUBLE};

    private TimestampTypeDriver() {
        super(
                ColumnTypeTag.TIMESTAMP,
                PhysicalDescriptor.Movement.W8,
                PhysicalDescriptor.Arithmetic.I64,
                PhysicalDescriptor.Accessor.TIMESTAMP
        );
    }

    @Override
    public int defineBindVariable(BindVariableService service, int index, int columnType, int position) throws SqlException {
        service.setTimestampWithType(index, columnType, Numbers.LONG_NULL);
        return columnType;
    }

    @Override
    public short[] getImplicitCasts() {
        return IMPLICIT_CASTS;
    }

    /**
     * Named by precision: the designated flag has no name of its own.
     */
    @Override
    public String getName(int columnType) {
        return switch (columnType) {
            case ColumnType.TIMESTAMP_MICRO -> "TIMESTAMP";
            case ColumnType.TIMESTAMP_NANO -> "TIMESTAMP_NS";
            default -> ColumnType.UNKNOWN_NAME;
        };
    }

    @Override
    public ConstantFunction getNullConstant(int columnType) {
        return ColumnType.getTimestampDriver(columnType).getTimestampConstantNull();
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
        return PgTypeOids.PG_TIMESTAMP;
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
    public char getSignatureChar() {
        return 'n';
    }

    @Override
    public TypeConstant getTypeConstant(int columnType) {
        return switch (columnType) {
            case ColumnType.TIMESTAMP_MICRO -> TimestampTypeConstant.TIMESTAMP_MS_CONSTANT;
            case ColumnType.TIMESTAMP_NANO -> TimestampTypeConstant.TIMESTAMP_NS_CONSTANT;
            default -> null;
        };
    }

    @Override
    public WireKind getWireKind() {
        return WireKind.TIMESTAMP;
    }

    @Override
    public boolean isCastTarget(boolean isFromNull) {
        return true;
    }

    @Override
    public Function newColumnFunction(int columnIndex, int columnType) {
        return TimestampColumn.newInstance(columnIndex, columnType);
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
