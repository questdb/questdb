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

import com.questdb.std.IntList;
import com.questdb.std.Unsafe;
import com.questdb.store.*;

public class FixRecordHolder extends AbstractMemRecord implements RecordHolder {
    private final IntList types;
    private final IntList offsets;
    private final int size;
    private long address;
    private StorageFacade storageFacade;
    private boolean held = false;

    public FixRecordHolder(RecordMetadata metadata) {
        int cc = metadata.getColumnCount();
        this.types = new IntList(cc);
        this.offsets = new IntList(cc);

        int size = 0;
        for (int i = 0; i < cc; i++) {
            int type = metadata.getColumnQuick(i).getType();
            types.add(type);
            offsets.add(size);

            switch (type) {
                case ColumnType.INT:
                case ColumnType.FLOAT:
                case ColumnType.SYMBOL:
                    size += 4;
                    break;
                case ColumnType.LONG:
                case ColumnType.DOUBLE:
                case ColumnType.DATE:
                    size += 8;
                    break;
                case ColumnType.BOOLEAN:
                case ColumnType.BYTE:
                    size++;
                    break;
                case ColumnType.SHORT:
                    size += 2;
                    break;
                default:
                    break;
            }
        }
        address = Unsafe.malloc(this.size = size);
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
        for (int i = 0, n = types.size(); i < n; i++) {
            RecordUtils.copyFixed(types.getQuick(i), record, i, this.address + offsets.getQuick(i));
        }
    }

    @Override
    public void close() {
        if (address != 0) {
            Unsafe.free(address, this.size);
            address = 0;
        }
    }

    protected long address(int col) {
        return address + offsets.getQuick(col);
    }

    @Override
    protected SymbolTable getSymbolTable(int col) {
        return storageFacade.getSymbolTable(col);
    }

}
