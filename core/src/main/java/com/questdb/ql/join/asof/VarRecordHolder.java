/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.join.asof;

import com.questdb.std.Chars;
import com.questdb.std.IntList;
import com.questdb.std.Unsafe;
import com.questdb.store.*;

public class VarRecordHolder extends AbstractVarMemRecord implements RecordHolder {
    private final IntList types;
    private final IntList offsets;
    private final IntList strCols;
    private final int varOffset;
    private long address = 0;
    private int size = 0;
    private StorageFacade storageFacade;
    private boolean held = false;

    public VarRecordHolder(RecordMetadata metadata) {
        super(metadata);

        int cc = metadata.getColumnCount();
        this.types = new IntList(cc);
        this.offsets = new IntList(cc);
        this.strCols = new IntList(cc);

        int offset = 0;
        for (int i = 0; i < cc; i++) {

            int type = metadata.getColumnQuick(i).getType();
            types.add(type);
            offsets.add(offset);

            switch (type) {
                case ColumnType.INT:
                case ColumnType.FLOAT:
                case ColumnType.SYMBOL:
                    offset += 4;
                    break;
                case ColumnType.LONG:
                case ColumnType.DOUBLE:
                case ColumnType.DATE:
                    offset += 8;
                    break;
                case ColumnType.BOOLEAN:
                case ColumnType.BYTE:
                    offset++;
                    break;
                case ColumnType.SHORT:
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
                case ColumnType.INT:
                case ColumnType.SYMBOL:
                    // write out int as symbol value
                    // need symbol facade to resolve back to string
                    Unsafe.getUnsafe().putInt(address, record.getInt(i));
                    break;
                case ColumnType.LONG:
                    Unsafe.getUnsafe().putLong(address, record.getLong(i));
                    break;
                case ColumnType.FLOAT:
                    Unsafe.getUnsafe().putFloat(address, record.getFloat(i));
                    break;
                case ColumnType.DOUBLE:
                    Unsafe.getUnsafe().putDouble(address, record.getDouble(i));
                    break;
                case ColumnType.BOOLEAN:
                case ColumnType.BYTE:
                    Unsafe.getUnsafe().putByte(address, record.getByte(i));
                    break;
                case ColumnType.SHORT:
                    Unsafe.getUnsafe().putShort(address, record.getShort(i));
                    break;
                case ColumnType.DATE:
                    Unsafe.getUnsafe().putLong(address, record.getDate(i));
                    break;
                case ColumnType.STRING:
                    Unsafe.getUnsafe().putInt(address, varOffset);
                    CharSequence cs = record.getFlyweightStr(i);
                    if (cs == null) {
                        Unsafe.getUnsafe().putInt(this.address + varOffset, VariableColumn.NULL_LEN);
                        varOffset += 4;
                    } else {
                        varOffset += Chars.strcpyw(cs, this.address + varOffset);
                    }
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public void close() {
        if (address != 0) {
            Unsafe.free(address, this.size);
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
            Unsafe.free(address, this.size);
        }

        address = Unsafe.malloc(size);
        this.size = size;
    }
}
