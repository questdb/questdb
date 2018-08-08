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

import com.questdb.ql.map.*;
import com.questdb.std.*;
import com.questdb.store.*;

public class LastVarRecordMap implements LastRecordMap {
    private final DirectMap map;
    private final LongList pages = new LongList();
    private final int pageSize;
    private final int maxRecordSize;
    private final IntList slaveValueIndexes;
    private final IntList varColumns = new IntList();
    private final FreeList freeList = new FreeList();
    private final IntList slaveValueTypes;
    private final IntList fixedOffsets;
    private final int varOffset;
    private final RecordMetadata metadata;
    private final MapRecord record;
    private final int bits;
    private final int mask;
    private final RecordKeyCopier masterCopier;
    private final RecordKeyCopier slaveCopier;
    private StorageFacade storageFacade;
    private long appendOffset;

    // todo: make sure blobs are not supported and not provided
    public LastVarRecordMap(
            @Transient ColumnTypeResolver masterResolver,
            RecordMetadata slaveMetadata,
            RecordKeyCopier masterCopier,
            RecordKeyCopier slaveCopier,
            int dataPageSize,
            int offsetPageSize) {
        this.pageSize = Numbers.ceilPow2(dataPageSize);
        this.masterCopier = masterCopier;
        this.slaveCopier = slaveCopier;
        this.maxRecordSize = dataPageSize - 4;
        this.bits = Numbers.msb(this.pageSize);
        this.mask = this.pageSize - 1;
        this.fixedOffsets = new IntList();
        this.slaveValueIndexes = new IntList();
        this.slaveValueTypes = new IntList();

        int varOffset = 0;
        // collect indexes of non-key fields in slave record
        for (int i = 0, n = slaveMetadata.getColumnCount(); i < n; i++) {
            fixedOffsets.add(varOffset);
            slaveValueIndexes.add(i);
            int type = slaveMetadata.getColumnQuick(i).getType();
            slaveValueTypes.add(type);
            int size = ColumnType.sizeOf(type);
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
        this.map = new DirectMap(offsetPageSize, masterResolver, LongResolver.INSTANCE);
        this.metadata = slaveMetadata;
        this.record = new MapRecord(slaveMetadata);
    }

    @Override
    public void close() {
        for (int i = 0; i < pages.size(); i++) {
            Unsafe.free(pages.getQuick(i), pageSize);
        }
        pages.clear();
        map.close();
    }

    @Override
    public Record get(Record master) {
        long offset;
        DirectMapValues values = getByMaster(master);
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
    public Record getRecord() {
        return record;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public void put(Record record) {
        final DirectMapValues values = getBySlave(record);
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

    private void appendRec(Record record, final int sz, DirectMapValues values) {
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
            pages.add(Unsafe.malloc(pageSize));
        }

        long addr = pages.getQuick(pgInx) + pgOfs;
        // write out record size + 10%
        // and actual size
        Unsafe.getUnsafe().putInt(addr, size - 4);
        writeRec0(addr + 4, record);
    }

    private DirectMapValues getByMaster(Record record) {
        map.locate(masterCopier, record);
        return map.getValues();
    }

    private DirectMapValues getBySlave(Record record) {
        map.locate(slaveCopier, record);
        return map.getOrCreateValues();
    }

    private int pageIndex(long offset) {
        return (int) (offset >> bits);
    }

    private int pageOffset(long offset) {
        return (int) (offset & mask);
    }

    private void writeRec(Record record, long offset, DirectMapValues values) {
        values.putLong(0, offset);
        writeRec0(pages.getQuick(pageIndex(offset)) + pageOffset(offset) + 4, record);
    }

    private void writeRec0(long addr, Record record) {
        int varOffset = this.varOffset;
        for (int i = 0, n = slaveValueIndexes.size(); i < n; i++) {
            int idx = slaveValueIndexes.getQuick(i);
            long address = addr + fixedOffsets.getQuick(i);
            switch (slaveValueTypes.getQuick(i)) {
                case ColumnType.INT:
                case ColumnType.SYMBOL:
                    // write out int as symbol value
                    // need symbol facade to resolve back to string
                    Unsafe.getUnsafe().putInt(address, record.getInt(idx));
                    break;
                case ColumnType.LONG:
                    Unsafe.getUnsafe().putLong(address, record.getLong(idx));
                    break;
                case ColumnType.FLOAT:
                    Unsafe.getUnsafe().putFloat(address, record.getFloat(idx));
                    break;
                case ColumnType.DOUBLE:
                    Unsafe.getUnsafe().putDouble(address, record.getDouble(idx));
                    break;
                case ColumnType.BOOLEAN:
                case ColumnType.BYTE:
                    Unsafe.getUnsafe().putByte(address, record.getByte(idx));
                    break;
                case ColumnType.SHORT:
                    Unsafe.getUnsafe().putShort(address, record.getShort(idx));
                    break;
                case ColumnType.DATE:
                    Unsafe.getUnsafe().putLong(address, record.getDate(idx));
                    break;
                case ColumnType.STRING:
                    Unsafe.getUnsafe().putInt(address, varOffset);
                    CharSequence cs = record.getFlyweightStr(idx);
                    if (cs == null) {
                        Unsafe.getUnsafe().putInt(addr + varOffset, VariableColumn.NULL_LEN);
                        varOffset += 4;
                    } else {
                        varOffset += Chars.strcpyw(record.getFlyweightStr(idx), addr + varOffset);
                    }
                    break;
                default:
                    break;
            }
        }


    }

    private class MapRecord extends AbstractVarMemRecord {
        private long address;

        public MapRecord(RecordMetadata metadata) {
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
