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

import com.nfsdb.collections.*;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.impl.AbstractRecord;
import com.nfsdb.ql.impl.SelectedColumnsMetadata;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;

import java.io.OutputStream;

public class LastFixRecordMap implements LastRecordMap {
    private static final ObjList<RecordColumnMetadata> valueMetadata = new ObjList<>();
    private final static CharSequenceObjHashMap<String> EMPTY_MAP = new CharSequenceObjHashMap<>();
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
    private final IntIntHashMap symTableRemap = new IntIntHashMap();
    private final SelectedColumnsMetadata metadata;
    private final MapRecord record;
    private final int bits;
    private final int mask;
    private long appendOffset;
    private StorageFacade storageFacade;

    public LastFixRecordMap(RecordMetadata masterMetadata, RecordMetadata slaveMetadata, CharSequenceHashSet keyColumns, int pageSize) {
        this.pageSize = Numbers.ceilPow2(pageSize);
        this.bits = Numbers.msb(this.pageSize);
        this.mask = this.pageSize - 1;

        final int ksz = keyColumns.size();
        this.masterKeyTypes = new ObjList<>(ksz);
        this.slaveKeyTypes = new ObjList<>(ksz);
        this.masterKeyIndexes = new IntHashSet(ksz);
        this.slaveKeyIndexes = new IntHashSet(ksz);

        // collect key field indexes for slave
        ObjList<RecordColumnMetadata> keyCols = new ObjList<>(ksz);

        for (int i = 0; i < ksz; i++) {
            int idx;
            idx = masterMetadata.getColumnIndex(keyColumns.get(i));
            masterKeyTypes.add(masterMetadata.getColumn(idx).getType());
            masterKeyIndexes.add(idx);

            idx = slaveMetadata.getColumnIndex(keyColumns.get(i));
            slaveKeyIndexes.add(idx);
            slaveKeyTypes.add(slaveMetadata.getColumn(idx).getType());
            keyCols.add(slaveMetadata.getColumn(idx));
        }

        this.fixedOffsets = new IntList(ksz - keyCols.size());
        this.slaveValueIndexes = new IntList(ksz - keyCols.size());
        this.slaveValueTypes = new ObjList<>(ksz - keyCols.size());

        ObjList<CharSequence> slaveColumnNames = new ObjList<>();
        int varOffset = 0;
        // collect indexes of non-key fields in slave record
        for (int i = 0, n = slaveMetadata.getColumnCount(); i < n; i++) {

            if (slaveKeyIndexes.contains(i)) {
                continue;
            }

            slaveColumnNames.add(slaveMetadata.getColumn(i).getName());
            fixedOffsets.add(varOffset);
            slaveValueIndexes.add(i);
            ColumnType type = slaveMetadata.getColumn(i).getType();
            slaveValueTypes.add(type);

            switch (type) {
                case INT:
                case FLOAT:
                case SYMBOL:
                    varOffset += 4;
                    break;
                case LONG:
                case DOUBLE:
                case DATE:
                    varOffset += 8;
                    break;
                case BOOLEAN:
                case BYTE:
                    varOffset++;
                    break;
                case SHORT:
                    varOffset += 2;
                    break;
            }
        }

        if (varOffset > pageSize) {
            throw new JournalRuntimeException("Record size is too large");
        }

        this.recordLen = varOffset;
        this.map = new MultiMap(valueMetadata, keyCols, null);
        this.metadata = new SelectedColumnsMetadata(slaveMetadata, slaveColumnNames, EMPTY_MAP);
        this.record = new MapRecord(this.metadata);
    }

    @Override
    public void close() {
        for (int i = 0; i < pages.size(); i++) {
            Unsafe.getUnsafe().freeMemory(pages.getQuick(i));
        }
        pages.clear();
    }

    public Record get(Record master) {
        MapValues values = getByMaster(master);
        if (values == null) {
            return null;
        }

        long offset = values.getLong(0);
        return record.of(pages.getQuick(pageIndex(offset)) + pageOffset(offset));
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public void put(Record record) {
        final MapValues values = getBySlave(record);
        // new record, append right away
        if (values.isNew()) {
            appendRec(record, values);
        } else {
            // old record, attempt to overwrite
            writeRec(record, values.getLong(0));
        }
    }

    public void setSlaveCursor(RecordCursor<? extends Record> cursor) {
        // hold on to storage facade an remap foreign indexes as
        // queries to symbols will be made using our indexes
        this.storageFacade = cursor.getStorageFacade();
        for (int i = 0, n = slaveValueTypes.size(); i < n; i++) {
            if (slaveValueTypes.getQuick(i) == ColumnType.SYMBOL) {
                symTableRemap.put(i, slaveValueIndexes.getQuick(i));
            }
        }
    }

    private static MultiMap.KeyWriter get(MultiMap map, Record record, IntHashSet indices, ObjList<ColumnType> types) {
        MultiMap.KeyWriter kw = map.keyWriter();
        for (int i = 0, n = indices.size(); i < n; i++) {
            int idx = indices.get(i);
            switch (types.getQuick(i)) {
                case INT:
                    kw.putInt(record.getInt(idx));
                    break;
                case LONG:
                    kw.putLong(record.getLong(idx));
                    break;
                case FLOAT:
                    kw.putFloat(record.getFloat(idx));
                    break;
                case DOUBLE:
                    kw.putDouble(record.getDouble(idx));
                    break;
                case BOOLEAN:
                    kw.putBoolean(record.getBool(idx));
                    break;
                case BYTE:
                    kw.putByte(record.get(idx));
                    break;
                case SHORT:
                    kw.putShort(record.getShort(idx));
                    break;
                case DATE:
                    kw.putLong(record.getDate(idx));
                    break;
                case STRING:
                    kw.putStr(record.getFlyweightStr(idx));
                    break;
                case SYMBOL:
                    // this is key field
                    // we have to write out string rather than int
                    // because master int values for same strings can be different
                    kw.putStr(record.getSym(idx));
                    break;
            }
        }
        return kw;
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
        return map.getValues(get(map, record, masterKeyIndexes, masterKeyTypes));
    }

    private MapValues getBySlave(Record record) {
        return map.getOrCreateValues(get(map, record, slaveKeyIndexes, slaveKeyTypes));
    }

    private int pageIndex(long offset) {
        return (int) (offset >> bits);
    }

    private int pageOffset(long offset) {
        return (int) (offset & mask);
    }

    private void writeRec(Record record, long offset) {
        writeRec0(pages.getQuick(pageIndex(offset)) + pageOffset(offset), record);
    }

    private void writeRec0(long addr, Record record) {
        for (int i = 0, n = slaveValueIndexes.size(); i < n; i++) {
            int idx = slaveValueIndexes.getQuick(i);
            long address = addr + fixedOffsets.getQuick(i);
            switch (slaveValueTypes.getQuick(i)) {
                case INT:
                case SYMBOL:
                    // write out int as symbol value
                    // need symbol facade to resolve back to string
                    Unsafe.getUnsafe().putInt(address, record.getInt(idx));
                    break;
                case LONG:
                    Unsafe.getUnsafe().putLong(address, record.getLong(idx));
                    break;
                case FLOAT:
                    Unsafe.getUnsafe().putFloat(address, record.getFloat(idx));
                    break;
                case DOUBLE:
                    Unsafe.getUnsafe().putDouble(address, record.getDouble(idx));
                    break;
                case BOOLEAN:
                case BYTE:
                    Unsafe.getUnsafe().putByte(address, record.get(idx));
                    break;
                case SHORT:
                    Unsafe.getUnsafe().putShort(address, record.getShort(idx));
                    break;
                case DATE:
                    Unsafe.getUnsafe().putLong(address, record.getDate(idx));
                    break;
            }
        }
    }

    public class MapRecord extends AbstractRecord {
        private long address;

        public MapRecord(RecordMetadata metadata) {
            super(metadata);
        }

        @Override
        public byte get(int col) {
            return Unsafe.getUnsafe().getByte(address + fixedOffsets.getQuick(col));
        }

        @Override
        public void getBin(int col, OutputStream s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DirectInputStream getBin(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getBinLen(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getBool(int col) {
            return Unsafe.getUnsafe().getByte(address + fixedOffsets.getQuick(col)) == 1;
        }

        @Override
        public long getDate(int col) {
            return Unsafe.getUnsafe().getLong(address + fixedOffsets.getQuick(col));
        }

        @Override
        public double getDouble(int col) {
            return Unsafe.getUnsafe().getDouble(address + fixedOffsets.getQuick(col));
        }

        @Override
        public float getFloat(int col) {
            return Unsafe.getUnsafe().getFloat(address + fixedOffsets.getQuick(col));
        }

        @Override
        public CharSequence getFlyweightStr(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getInt(int col) {
            return Unsafe.getUnsafe().getInt(address + fixedOffsets.getQuick(col));
        }

        @Override
        public long getLong(int col) {
            return Unsafe.getUnsafe().getLong(address + fixedOffsets.getQuick(col));
        }

        @Override
        public long getRowId() {
            return -1;
        }

        @Override
        public short getShort(int col) {
            return Unsafe.getUnsafe().getShort(address + fixedOffsets.getQuick(col));
        }

        @Override
        public CharSequence getStr(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void getStr(int col, CharSink sink) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getStrLen(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getSym(int col) {
            return storageFacade.getSymbolTable(symTableRemap.get(col)).value(Unsafe.getUnsafe().getInt(address + fixedOffsets.getQuick(col)));
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
