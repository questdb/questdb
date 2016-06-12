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

import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Chars;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.impl.map.MapUtils;
import com.questdb.ql.impl.map.MapValues;
import com.questdb.ql.impl.map.MultiMap;
import com.questdb.std.*;
import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;

public class LastVarRecordMap implements LastRecordMap {
    private final MultiMap map;
    private final LongList pages = new LongList();
    private final int pageSize;
    private final int maxRecordSize;
    private final IntHashSet slaveKeyIndexes;
    private final IntHashSet masterKeyIndexes;
    private final IntList slaveValueIndexes;
    private final IntList varColumns = new IntList();
    private final FreeList freeList = new FreeList();
    private final ObjList<ColumnType> slaveKeyTypes;
    private final ObjList<ColumnType> masterKeyTypes;
    private final ObjList<ColumnType> slaveValueTypes;
    private final IntList fixedOffsets;
    private final int varOffset;
    private final RecordMetadata metadata;
    private final MapRecord record;
    private final int bits;
    private final int mask;
    private StorageFacade storageFacade;
    private long appendOffset;

    // todo: make sure blobs are not supported and not provided
    public LastVarRecordMap(
            RecordMetadata masterMetadata,
            RecordMetadata slaveMetadata,
            CharSequenceHashSet masterKeyColumns,
            CharSequenceHashSet slaveKeyColumns,
            int dataPageSize,
            int offsetPageSize) {
        this.pageSize = Numbers.ceilPow2(dataPageSize);
        this.maxRecordSize = dataPageSize - 4;
        this.bits = Numbers.msb(this.pageSize);
        this.mask = this.pageSize - 1;

        final int ksz = masterKeyColumns.size();

        assert slaveKeyColumns.size() == ksz;

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
            int size = type.size();
            if (size == 0) {
                // variable size col
                varColumns.add(i);
                varOffset += 4;
            } else {
                varOffset += size;
            }
        }

        if (varOffset > maxRecordSize) {
            throw new JournalRuntimeException("Record size is too large");
        }

        this.varOffset = varOffset;
        this.map = new MultiMap(offsetPageSize, slaveMetadata, keyCols, MapUtils.ROWID_MAP_VALUES, null);
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

    @Override
    public Record get(Record master) {
        long offset;
        MapValues values = getByMaster(master);
        if (values == null || (((offset = values.getLong(0)) & SET_BIT) == SET_BIT)) {
            return null;
        }
        values.putLong(0, offset | SET_BIT);
        return record.of(pages.getQuick(pageIndex(offset)) + pageOffset(offset));
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public void put(Record record) {
        final MapValues values = getBySlave(record);
        // calculate record size
        int size = varOffset;
        for (int i = 0, n = varColumns.size(); i < n; i++) {
            size += record.getStrLen(varColumns.getQuick(i)) * 2 + 4;
        }

        // record is larger than page size
        // won't handle that as we don't write one record across multiple pages
        if (size > maxRecordSize) {
            throw new JournalRuntimeException("Record size is too large");
        }

        // new record, append right away
        if (values.isNew()) {
            appendRec(record, size, values);
        } else {
            // old record, attempt to overwrite
            long offset = values.getLong(0) & CLR_BIT;
            int pgInx = pageIndex(offset);
            int pgOfs = pageOffset(offset);

            int oldSize = Unsafe.getUnsafe().getInt(pages.getQuick(pgInx) + pgOfs);

            if (size > oldSize) {
                // new record is larger than previous, must write to new location
                // in the mean time free old location
                freeList.add(offset, oldSize);

                if (freeList.getTotalSize() < maxRecordSize) {
                    // if free list is too small, keep appending
                    appendRec(record, size, values);
                } else {
                    // free list is large enough, we need to start reusing
                    long _offset = freeList.findAndRemove(size);
                    if (_offset == -1) {
                        // could not find suitable free block, append
                        appendRec(record, size, values);
                    } else {
                        writeRec(record, _offset, values);
                    }
                }
            } else {
                // new record is smaller or equal in size to previous one, overwrite safely
                writeRec(record, offset, values);
            }
        }
    }

    @Override
    public void reset() {
        appendOffset = 0;
        map.clear();
        freeList.clear();
    }

    @Override
    public void setSlaveCursor(RecordCursor cursor) {
        this.storageFacade = cursor.getStorageFacade();
    }

    private void appendRec(Record record, final int sz, MapValues values) {
        int pgInx = pageIndex(appendOffset);
        int pgOfs = pageOffset(appendOffset);

        // input is net size of payload
        // add 4 byte prefix + 10%
        int size = sz + 4 + sz / 10;

        if (pgOfs + size > pageSize) {
            pgInx++;
            pgOfs = 0;
            values.putLong(0, appendOffset = (pgInx * pageSize));
        } else {
            values.putLong(0, appendOffset);
        }

        appendOffset += size;

        // allocateOffset if necessary
        if (pgInx == pages.size()) {
            pages.add(Unsafe.getUnsafe().allocateMemory(pageSize));
        }

        long addr = pages.getQuick(pgInx) + pgOfs;
        // write out record size + 10%
        // and actual size
        Unsafe.getUnsafe().putInt(addr, size - 4);
        writeRec0(addr + 4, record);
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
        writeRec0(pages.getQuick(pageIndex(offset)) + pageOffset(offset) + 4, record);
    }

    private void writeRec0(long addr, Record record) {
        int varOffset = this.varOffset;
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
                case STRING:
                    Unsafe.getUnsafe().putInt(address, varOffset);
                    varOffset += Chars.put(addr + varOffset, record.getFlyweightStr(idx));
                    break;
                default:
                    break;
            }
        }


    }

    private class MapRecord extends AbstractVarMemRecord {
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

        @Override
        protected long address() {
            return address;
        }

        private MapRecord of(long address) {
            this.address = address + 4;
            return this;
        }
    }
}
