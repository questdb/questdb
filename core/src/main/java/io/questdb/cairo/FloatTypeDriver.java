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
import io.questdb.griffin.engine.functions.columns.FloatColumn;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.FloatConstant;
import io.questdb.griffin.engine.functions.constants.FloatTypeConstant;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for FLOAT.
 */
public final class FloatTypeDriver extends FixedSizeTypeDriver {
    public static final FloatTypeDriver INSTANCE = new FloatTypeDriver();

    private FloatTypeDriver() {
        super(
                ColumnTypeTag.FLOAT,
                PhysicalDescriptor.Movement.W4,
                PhysicalDescriptor.Arithmetic.F32,
                PhysicalDescriptor.Accessor.FLOAT
        );
    }

    @Override
    public int defineBindVariable(BindVariableService service, int index, int columnType, int position) throws SqlException {
        service.setFloat(index);
        return columnType;
    }

    @Override
    public String getName(int columnType) {
        return nameOfBareTag(columnType, ColumnType.FLOAT, "FLOAT");
    }

    @Override
    public ConstantFunction getNullConstant(int columnType) {
        return FloatConstant.NULL;
    }

    @Override
    public long getNullLong(int longIndex) {
        return Numbers.encodeLowHighInts(Float.floatToIntBits(Float.NaN), Float.floatToIntBits(Float.NaN));
    }

    @Override
    public NullPolicy getNullPolicy() {
        return NullPolicy.SENTINEL;
    }

    @Override
    public TypeConstant getTypeConstant(int columnType) {
        return columnType == ColumnType.FLOAT ? FloatTypeConstant.INSTANCE : null;
    }

    @Override
    public boolean isCastTarget(boolean isFromNull) {
        return true;
    }

    @Override
    public Function newColumnFunction(int columnIndex, int columnType) {
        return FloatColumn.newInstance(columnIndex);
    }

    @Override
    public Runnable newNullAppender(MemoryA dataMem, MemoryA auxMem) {
        return () -> dataMem.putFloat(Float.NaN);
    }

    @Override
    public void setNull(long addr, long count) {
        Vect.setMemoryFloat(addr, Float.NaN, count);
    }
}
