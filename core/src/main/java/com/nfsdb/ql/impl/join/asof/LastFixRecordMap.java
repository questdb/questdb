/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.impl.join.LongMetadata;
import com.nfsdb.ql.impl.map.MapValues;
import com.nfsdb.ql.impl.map.MultiMap;
import com.nfsdb.std.*;
import com.nfsdb.store.ColumnType;
import com.nfsdb.store.SymbolTable;

public class LastFixRecordMap implements LastRecordMap {
    private static final ObjList<RecordColumnMetadata> valueMetadata = new ObjList<>();
    private final MultiMap map;
    private final LongList pages = new LongList();
    private final int pageSize;
    private final IntHashSet slaveKeyIndexes;
    private final IntHashSet masterKeyIndexes;
    private final IntList slaveValueIndexes;
    private final ObjList<ColumnType> slaveKeyTypes;
    private final ObjList<ColumnType> masterKeyTypes;
    private final ObjList<ColumnType> slaveValueTypes;
    private final IntList fixedOffsets;
    private final int recordLen;
    private final RecordMetadata metadata;
    private final MapRecord record;
    private final int bits;
    private final int mask;
    private StorageFacade storageFacade;
    private long appendOffset;

    public LastFixRecordMap(
            RecordMetadata masterMetadata,
            RecordMetadata slaveMetadata,
            @Transient CharSequenceHashSet masterKeyColumns,
            @Transient CharSequenceHashSet slaveKeyColumns,
            int pageSize) {
        this.pageSize = Numbers.ceilPow2(pageSize);
        this.bits = Numbers.msb(this.pageSize);
        this.mask = this.pageSize - 1;

        final int ksz = masterKeyColumns.size();
        this.masterKeyTypes = new ObjList<>(ksz);
        this.slaveKeyTypes = new ObjList<>(ksz);
        this.masterKeyIndexes = new IntHashSet(ksz);
        this.slaveKeyIndexes = new IntHashSet(ksz);

        // collect key field indexes for slave
        ObjHashSet<String> keyCols = new ObjHashSet<>(ksz);

        for (int i = 0; i < ksz; i++) {
            int idx;
            idx = masterMetadata.getColumnIndex(masterKeyColumns.get(i));
            masterKeyTypes.add(masterMetadata.getColumnQuick(idx).getType());
            masterKeyIndexes.add(idx);

            idx = slaveMetadata.getColumnIndex(slaveKeyColumns.get(i));
            slaveKeyIndexes.add(idx);
            slaveKeyTypes.add(slaveMetadata.getColumnQuick(idx).getType());
            keyCols.add(slaveMetadata.getColumnName(idx));
        }

        int sz = ksz - keyCols.size();
        this.fixedOffsets = new IntList(sz);
        this.slaveValueIndexes = new IntList(sz);
        this.slaveValueTypes = new ObjList<>(sz);

        int varOffset = 0;
        // collect indexes of non-key fields in slave record
        for (int i = 0, n = slaveMetadata.getColumnCount(); i < n; i++) {
            fixedOffsets.add(varOffset);
            slaveValueIndexes.add(i);
            ColumnType type = slaveMetadata.getColumnQuick(i).getType();
            slaveValueTypes.add(type);
            varOffset += type.size();
        }

        if (varOffset > pageSize) {
            throw new JournalRuntimeException("Record size is too large");
        }

        this.recordLen = varOffset;
        this.map = new MultiMap(slaveMetadata, keyCols, valueMetadata, null);
        this.metadata = slaveMetadata;
        this.record = new MapRecord(this.metadata);
    }

    @Override
    public void close() {
        for (int i = 0; i < pages.size(); i++) {
            Unsafe.getUnsafe().freeMemory(pages.getQuick(i));
        }
        pages.clear();
        map.close();
    }

    public Record get(Record master) {
        long offset;
        MapValues values = getByMaster(master);
        if (values == null || (((offset = values.getLong(0)) & SET_BIT) == SET_BIT)) {
            return null;
        }
        values.putLong(0, offset | SET_BIT);
        return record.of(pages.getQuick(pageIndex(offset)) + pageOffset(offset));
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    public void put(Record record) {
        final MapValues values = getBySlave(record);
        // new record, append right away
        if (values.isNew()) {
            appendRec(record, values);
        } else {
            // old record, attempt to overwrite
            writeRec(record, values.getLong(0) & CLR_BIT, values);
        }
    }

    @Override
    public void reset() {
        appendOffset = 0;
        map.clear();
    }

    @Override
    public void setSlaveCursor(RecordCursor cursor) {
        this.storageFacade = cursor.getStorageFacade();
    }

    private void appendRec(Record record, MapValues values) {
        int pgInx = pageIndex(appendOffset);
        int pgOfs = pageOffset(appendOffset);

        if (pgOfs + recordLen > pageSize) {
            pgInx++;
            pgOfs = 0;
            values.putLong(0, appendOffset = (pgInx * pageSize));
        } else {
            values.putLong(0, appendOffset);
        }
        appendOffset += recordLen;
        // allocate if necessary
        if (pgInx == pages.size()) {
            pages.add(Unsafe.getUnsafe().allocateMemory(pageSize));
        }
        writeRec0(pages.getQuick(pgInx) + pgOfs, record);
    }

    private MapValues getByMaster(Record record) {
        return map.getValues(RecordUtils.createKey(map, record, masterKeyIndexes, masterKeyTypes));
    }

    private MapValues getBySlave(Record record) {
        return map.getOrCreateValues(RecordUtils.createKey(map, record, slaveKeyIndexes, slaveKeyTypes));
    }

    private int pageIndex(long offset) {
        return (int) (offset >> bits);
    }

    private int pageOffset(long offset) {
        return (int) (offset & mask);
    }

    private void writeRec(Record record, long offset, MapValues values) {
        values.putLong(0, offset);
        writeRec0(pages.getQuick(pageIndex(offset)) + pageOffset(offset), record);
    }

    private void writeRec0(long addr, Record record) {
        for (int i = 0, n = slaveValueIndexes.size(); i < n; i++) {
            RecordUtils.copyFixed(slaveValueTypes.getQuick(i), record, slaveValueIndexes.getQuick(i), addr + fixedOffsets.getQuick(i));
        }
    }

    private class MapRecord extends AbstractMemRecord {
        private long address;

        MapRecord(RecordMetadata metadata) {
            super(metadata);
        }

        @Override
        protected long address(int col) {
            return address + fixedOffsets.getQuick(col);
        }

        @Override
        protected SymbolTable getSymbolTable(int col) {
            return storageFacade.getSymbolTable(col);
        }

        private MapRecord of(long address) {
            this.address = address;
            return this;
        }
    }

    static {
        valueMetadata.add(LongMetadata.INSTANCE);
    }
}
