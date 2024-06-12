/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.sql;

import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class ColumnChunkRecord implements Record, Closeable {

    private final ObjList<SymbolTable> symbolTableCache = new ObjList<>();
    private ObjList<MemoryCARW> columnChunks;
    private long rowIndex;
    // Makes it possible to determine real row id, not one relative to page.
    private SymbolTableSource symbolTableSource;

    public ColumnChunkRecord() {
    }

    @Override
    public void close() {
        Misc.freeObjListIfCloseable(symbolTableCache);
    }

    @Override
    public BinarySequence getBin(int columnIndex) {
        final MemoryR auxMem = getAuxMem(columnIndex);
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getBin(auxMem.getLong(rowIndex));
    }

    @Override
    public long getBinLen(int columnIndex) {
        final MemoryR auxMem = getAuxMem(columnIndex);
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getBinLen(auxMem.getLong(rowIndex));
    }

    @Override
    public boolean getBool(int columnIndex) {
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getBool(rowIndex);
    }

    @Override
    public byte getByte(int columnIndex) {
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getByte(rowIndex);
    }

    @Override
    public char getChar(int columnIndex) {
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getChar(rowIndex);
    }

    @Override
    public double getDouble(int columnIndex) {
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getDouble(rowIndex);
    }

    @Override
    public float getFloat(int columnIndex) {
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getFloat(rowIndex);
    }

    @Override
    public byte getGeoByte(int columnIndex) {
        return getByte(columnIndex);
    }

    @Override
    public int getGeoInt(int columnIndex) {
        return getInt(columnIndex);
    }

    @Override
    public long getGeoLong(int columnIndex) {
        return getLong(columnIndex);
    }

    @Override
    public short getGeoShort(int columnIndex) {
        return getShort(columnIndex);
    }

    @Override
    public int getIPv4(int columnIndex) {
        return getInt(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) {
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getInt(rowIndex);
    }

    @Override
    public long getLong(int columnIndex) {
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getLong(rowIndex);
    }

    @Override
    public long getLong128Hi(int columnIndex) {
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getLong(rowIndex + 1);
    }

    @Override
    public long getLong128Lo(int columnIndex) {
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getLong(rowIndex);
    }

    @Override
    public void getLong256(int columnIndex, CharSink<?> sink) {
        final MemoryR dataMem = getDataMem(columnIndex);
        dataMem.getLong256(rowIndex, sink);
    }

    @Override
    public Long256 getLong256A(int columnIndex) {
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getLong256A(rowIndex);
    }

    @Override
    public Long256 getLong256B(int columnIndex) {
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getLong256B(rowIndex);
    }

    @Override
    public short getShort(int columnIndex) {
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getShort(rowIndex);
    }

    @Override
    public CharSequence getStrA(int columnIndex) {
        final MemoryR auxMem = getAuxMem(columnIndex);
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getStrA(auxMem.getLong(rowIndex));
    }

    @Override
    public CharSequence getStrB(int columnIndex) {
        final MemoryR auxMem = getAuxMem(columnIndex);
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getStrB(auxMem.getLong(rowIndex));
    }

    @Override
    public int getStrLen(int columnIndex) {
        final MemoryR auxMem = getAuxMem(columnIndex);
        final MemoryR dataMem = getDataMem(columnIndex);
        return dataMem.getStrLen(auxMem.getLong(rowIndex));
    }

    @Override
    public CharSequence getSymA(int columnIndex) {
        final int key = getInt(columnIndex);
        return getSymbolTable(columnIndex).valueOf(key);
    }

    @Override
    public CharSequence getSymB(int columnIndex) {
        final int key = getInt(columnIndex);
        return getSymbolTable(columnIndex).valueBOf(key);
    }

    @Override
    public Utf8Sequence getVarcharA(int columnIndex) {
        return getVarchar(columnIndex, 1);
    }

    @Override
    public Utf8Sequence getVarcharB(int columnIndex) {
        return getVarchar(columnIndex, 2);
    }

    @Override
    public int getVarcharSize(int columnIndex) {
        final MemoryR auxMem = getAuxMem(columnIndex);
        return VarcharTypeDriver.getValueSize(auxMem, rowIndex);
    }

    public void of(SymbolTableSource symbolTableSource) {
        this.symbolTableSource = symbolTableSource;
        rowIndex = 0;
        Misc.freeObjListIfCloseable(symbolTableCache);
        symbolTableCache.clear();
    }

    public void setColumnChunks(ObjList<MemoryCARW> columnChunks) {
        this.columnChunks = columnChunks;
    }

    public void setRowIndex(long rowIndex) {
        this.rowIndex = rowIndex;
    }

    private MemoryR getAuxMem(int columnIndex) {
        return columnChunks.getQuick(2 * columnIndex + 1);
    }

    private MemoryR getDataMem(int columnIndex) {
        return columnChunks.getQuick(2 * columnIndex);
    }

    private SymbolTable getSymbolTable(int columnIndex) {
        SymbolTable symbolTable = symbolTableCache.getQuiet(columnIndex);
        if (symbolTable == null) {
            symbolTable = symbolTableSource.newSymbolTable(columnIndex);
            symbolTableCache.extendAndSet(columnIndex, symbolTable);
        }
        return symbolTable;
    }

    @Nullable
    private Utf8Sequence getVarchar(int columnIndex, int ab) {
        final MemoryR auxMem = getAuxMem(columnIndex);
        final MemoryR dataMem = getDataMem(columnIndex);
        return VarcharTypeDriver.getSplitValue(auxMem, dataMem, rowIndex, ab);
    }
}
