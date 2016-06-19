/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.impl.join.asof;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Chars;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.StorageFacade;
import com.questdb.std.IntList;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;

public class VarRecordHolder extends AbstractVarMemRecord implements RecordHolder {
    private final ObjList<ColumnType> types;
    private final IntList offsets;
    private final IntList strCols;
    private final int varOffset;
    private long address = 0;
    private int size = 0;
    private StorageFacade storageFacade;
    private boolean held = false;

    public VarRecordHolder(RecordMetadata metadata) {
        int cc = metadata.getColumnCount();
        this.types = new ObjList<>(cc);
        this.offsets = new IntList(cc);
        this.strCols = new IntList(cc);

        int offset = 0;
        for (int i = 0; i < cc; i++) {

            ColumnType type = metadata.getColumnQuick(i).getType();
            types.add(type);
            offsets.add(offset);

            switch (type) {
                case INT:
                case FLOAT:
                case SYMBOL:
                    offset += 4;
                    break;
                case LONG:
                case DOUBLE:
                case DATE:
                    offset += 8;
                    break;
                case BOOLEAN:
                case BYTE:
                    offset++;
                    break;
                case SHORT:
                    offset += 2;
                    break;
                default:
                    strCols.add(i);
                    offset += 4;
                    break;
            }
        }
        this.varOffset = offset;
    }

    @Override
    public void clear() {
        held = false;
    }

    @Override
    public Record peek() {
        return held ? this : null;
    }

    @Override
    public void setCursor(RecordCursor cursor) {
        this.storageFacade = cursor.getStorageFacade();
    }

    public void write(Record record) {
        this.held = true;
        int sz = varOffset;

        for (int i = 0, n = strCols.size(); i < n; i++) {
            sz += record.getStrLen(strCols.getQuick(i)) * 2 + 4;
        }

        if (sz > size) {
            alloc(sz);
        }

        int varOffset = this.varOffset;

        for (int i = 0, n = types.size(); i < n; i++) {
            long address = this.address + offsets.getQuick(i);
            switch (types.getQuick(i)) {
                case INT:
                case SYMBOL:
                    // write out int as symbol value
                    // need symbol facade to resolve back to string
                    Unsafe.getUnsafe().putInt(address, record.getInt(i));
                    break;
                case LONG:
                    Unsafe.getUnsafe().putLong(address, record.getLong(i));
                    break;
                case FLOAT:
                    Unsafe.getUnsafe().putFloat(address, record.getFloat(i));
                    break;
                case DOUBLE:
                    Unsafe.getUnsafe().putDouble(address, record.getDouble(i));
                    break;
                case BOOLEAN:
                case BYTE:
                    Unsafe.getUnsafe().putByte(address, record.get(i));
                    break;
                case SHORT:
                    Unsafe.getUnsafe().putShort(address, record.getShort(i));
                    break;
                case DATE:
                    Unsafe.getUnsafe().putLong(address, record.getDate(i));
                    break;
                case STRING:
                    Unsafe.getUnsafe().putInt(address, varOffset);
                    varOffset += Chars.put(this.address + varOffset, record.getFlyweightStr(i));
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public void close() {
        if (address != 0) {
            Unsafe.getUnsafe().freeMemory(address);
            address = 0;
            size = 0;
        }
    }

    @Override
    protected long address() {
        return address;
    }

    protected long address(int col) {
        return address + offsets.getQuick(col);
    }

    @Override
    protected SymbolTable getSymbolTable(int col) {
        return storageFacade.getSymbolTable(col);
    }

    private void alloc(int size) {
        if (address != 0) {
            Unsafe.getUnsafe().freeMemory(address);
        }

        address = Unsafe.getUnsafe().allocateMemory(size);
        this.size = size;
    }
}
