/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.collections;

import com.nfsdb.collections.IntList;
import com.nfsdb.collections.ObjList;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.storage.SymbolTable;
import com.nfsdb.utils.Unsafe;

public class FixRecordHolder extends AbstractMemRecord implements RecordHolder {
    private final ObjList<ColumnType> types;
    private final IntList offsets;
    private final long address;
    private StorageFacade storageFacade;

    public FixRecordHolder(RecordMetadata metadata) {
        super(metadata);

        int cc = metadata.getColumnCount();
        this.types = new ObjList<>(cc);
        this.offsets = new IntList(cc);

        int offset = 0;
        for (int i = 0; i < cc; i++) {
            ColumnType type = metadata.getColumn(i).getType();
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
            }
        }
        address = Unsafe.getUnsafe().allocateMemory(offset);
    }

    @Override
    public void close() {
        Unsafe.getUnsafe().freeMemory(address);
    }

    @Override
    public Record get() {
        return this;
    }

    @Override
    public void setCursor(RecordCursor<? extends Record> cursor) {
        this.storageFacade = cursor.getStorageFacade();
    }

    public void write(Record record) {
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
            }
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
