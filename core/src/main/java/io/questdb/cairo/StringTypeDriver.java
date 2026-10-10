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
import io.questdb.griffin.engine.functions.columns.StrColumn;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.StrTypeConstant;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public final class StringTypeDriver extends NPlusOneAuxTypeDriver {
    public static final StringTypeDriver INSTANCE = new StringTypeDriver();
    // implicit-cast targets, best match first; see TypeDriver.getImplicitCasts()
    private static final short[] IMPLICIT_CASTS = {ColumnType.STRING, ColumnType.VARCHAR, ColumnType.CHAR, ColumnType.DOUBLE, ColumnType.LONG, ColumnType.INT, ColumnType.FLOAT, ColumnType.SHORT, ColumnType.BYTE, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.SYMBOL, ColumnType.IPv4};

    public static void appendValue(MemoryA auxMem, MemoryA dataMem, CharSequence value) {
        auxMem.putLong(dataMem.putStr(value));
    }

    @Override
    public void appendNull(MemoryA auxMem, MemoryA dataMem) {
        auxMem.putLong(dataMem.putNullStr());
    }

    @Override
    public int defineBindVariable(BindVariableService service, int index, int columnType, int position) throws SqlException {
        service.setStr(index);
        return columnType;
    }

    @Override
    public PhysicalDescriptor.Accessor getAccessor() {
        return PhysicalDescriptor.Accessor.STRING;
    }

    @Override
    public PhysicalDescriptor.Arithmetic getArithmetic() {
        return PhysicalDescriptor.Arithmetic.NONE;
    }

    @Override
    public short[] getImplicitCasts() {
        return IMPLICIT_CASTS;
    }

    @Override
    public PhysicalDescriptor.Movement getMovement() {
        return PhysicalDescriptor.Movement.VAR;
    }

    @Override
    public String getName(int columnType) {
        return columnType == ColumnType.STRING ? "STRING" : ColumnType.UNKNOWN_NAME;
    }

    @Override
    public ConstantFunction getNullConstant(int columnType) {
        return StrConstant.NULL;
    }

    /**
     * The 4-byte length prefix of a NULL string, NULL_LEN, in both halves of the long. The aux
     * entry holds the data offset, as for any string.
     */
    @Override
    public long getNullLong(int longIndex) {
        return Numbers.encodeLowHighInts(TableUtils.NULL_LEN, TableUtils.NULL_LEN);
    }

    /**
     * STRING keeps NULL in the length prefix.
     */
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
        return PgTypeOids.PG_VARCHAR;
    }

    @Override
    public int getRelationBits() {
        return 0;
    }

    @Override
    public RelationKind getRelationKind() {
        return RelationKind.TEXT;
    }

    @Override
    public ColumnTypeTag getTag() {
        return ColumnTypeTag.STRING;
    }

    @Override
    public char getSignatureChar() {
        return 's';
    }

    @Override
    public TypeConstant getTypeConstant(int columnType) {
        return columnType == ColumnType.STRING ? StrTypeConstant.INSTANCE : null;
    }

    @Override
    public WireKind getWireKind() {
        return WireKind.STRING;
    }

    @Override
    public boolean isCastTarget(boolean isFromNull) {
        return true;
    }

    /**
     * Always a new instance: {@link StrColumn} is not thread-safe, so it is never pooled.
     */
    @Override
    public Function newColumnFunction(int columnIndex, int columnType) {
        return new StrColumn(columnIndex);
    }

    @Override
    public long getDataVectorMinEntrySize() {
        return Integer.BYTES;
    }

    @Override
    public boolean isSparseDataVector(long auxMemAddr, long dataMemAddr, long rowCount) {
        for (int row = 0; row < rowCount; row++) {
            long offset = Unsafe.getLong(auxMemAddr + (long) row * Long.BYTES);
            long iLen = Unsafe.getLong(auxMemAddr + (long) (row + 1) * Long.BYTES) - offset;
            long dLen = Unsafe.getInt(dataMemAddr + offset);
            int lenLen = 4;
            long dataLen = dLen * 2;
            long dStorageLen = dLen > 0 ? dataLen + lenLen : lenLen;
            if (iLen != dStorageLen) {
                // Swiss cheese hole in var col file
                return true;
            }
        }
        return false;

    }

    @Override
    public void o3ColumnMerge(
            long timestampMergeIndexAddr,
            long timestampMergeIndexCount,
            long srcAuxAddr1,
            long srcDataAddr1,
            long srcAuxAddr2,
            long srcDataAddr2,
            long dstAuxAddr,
            long dstDataAddr,
            long dstDataOffset
    ) {
        Vect.oooMergeCopyStrColumn(
                timestampMergeIndexAddr,
                timestampMergeIndexCount,
                srcAuxAddr1,
                srcDataAddr1,
                srcAuxAddr2,
                srcDataAddr2,
                dstAuxAddr,
                dstDataAddr,
                dstDataOffset
        );
    }

    @Override
    public void setDataVectorEntriesToNull(long dataMemAddr, long rowCount) {
        Vect.memset(dataMemAddr, rowCount * Integer.BYTES, -1);
    }

    @Override
    public void setFullAuxVectorNull(long auxMemAddr, long rowCount) {
        Vect.setStringColumnNullRefs(auxMemAddr, 0, rowCount + 1);
    }

    @Override
    public void setPartAuxVectorNull(long auxMemAddr, long initialOffset, long columnTop) {
        Vect.setStringColumnNullRefs(auxMemAddr, initialOffset, columnTop);
    }

}
