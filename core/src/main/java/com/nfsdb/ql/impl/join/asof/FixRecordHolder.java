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

package com.nfsdb.ql.impl.join.asof;

import com.nfsdb.collections.IntList;
import com.nfsdb.collections.ObjList;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.storage.SymbolTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class FixRecordHolder extends AbstractMemRecord implements RecordHolder {
    private final ObjList<ColumnType> types;
    private final IntList offsets;
    private long address;
    private StorageFacade storageFacade;
    private boolean held = false;

    @SuppressFBWarnings("SF_SWITCH_NO_DEFAULT")
    public FixRecordHolder(RecordMetadata metadata) {
        super(metadata);

        int cc = metadata.getColumnCount();
        this.types = new ObjList<>(cc);
        this.offsets = new IntList(cc);

        int size = 0;
        for (int i = 0; i < cc; i++) {
            ColumnType type = metadata.getColumnQuick(i).getType();
            types.add(type);
            offsets.add(size);

            switch (type) {
                case INT:
                case FLOAT:
                case SYMBOL:
                    size += 4;
                    break;
                case LONG:
                case DOUBLE:
                case DATE:
                    size += 8;
                    break;
                case BOOLEAN:
                case BYTE:
                    size++;
                    break;
                case SHORT:
                    size += 2;
                    break;
            }
        }
        address = Unsafe.getUnsafe().allocateMemory(size);
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
    public void setCursor(RecordCursor<? extends Record> cursor) {
        this.storageFacade = cursor.getStorageFacade();
    }

    @SuppressFBWarnings("SF_SWITCH_NO_DEFAULT")
    public void write(Record record) {
        this.held = true;
        for (int i = 0, n = types.size(); i < n; i++) {
            RecordUtils.copyFixed(types.getQuick(i), record, i, this.address + offsets.getQuick(i));
        }
    }

    @Override
    public void close() {
        if (address != 0) {
            Unsafe.getUnsafe().freeMemory(address);
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
