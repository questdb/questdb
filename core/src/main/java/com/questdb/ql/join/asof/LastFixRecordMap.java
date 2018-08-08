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

public class LastFixRecordMap implements LastRecordMap {
    private final DirectMap map;
    private final LongList pages = new LongList();
    private final int pageSize;
    private final IntList slaveValueIndexes;
    private final IntList slaveValueTypes;
    private final IntList fixedOffsets;
    private final int recordLen;
    private final RecordMetadata metadata;
    private final MapRecord record = new MapRecord();
    private final int bits;
    private final int mask;
    private final RecordKeyCopier masterCopier;
    private final RecordKeyCopier slaveCopier;
    private StorageFacade storageFacade;
    private long appendOffset;

    public LastFixRecordMap(
            @Transient ColumnTypeResolver masterResolver,
            RecordMetadata slaveMetadata,
            RecordKeyCopier masterCopier,
            RecordKeyCopier slaveCopier,
            int dataPageSize,
            int offsetPageSize) {
        this.pageSize = Numbers.ceilPow2(dataPageSize);
        this.bits = Numbers.msb(this.pageSize);
        this.mask = this.pageSize - 1;
        this.masterCopier = masterCopier;
        this.slaveCopier = slaveCopier;

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
            varOffset += ColumnType.sizeOf(type);
        }

        if (varOffset > dataPageSize) {
            throw new JournalRuntimeException("Record size is too large");
        }

        this.recordLen = varOffset;
        this.map = new DirectMap(offsetPageSize, masterResolver, LongResolver.INSTANCE);
        this.metadata = slaveMetadata;
    }

    @Override
    public void close() {
        for (int i = 0; i < pages.size(); i++) {
            Unsafe.free(pages.getQuick(i), pageSize);
        }
        pages.clear();
        map.close();
    }

    public Record get(Record master) {
        long offset;
        DirectMapValues values = getByMaster(master);
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
    public Record getRecord() {
        return record;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    public void put(Record record) {
        final DirectMapValues values = getBySlave(record);
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

    private void appendRec(Record record, DirectMapValues values) {
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
        // allocateOffset if necessary
        if (pgInx == pages.size()) {
            pages.add(Unsafe.malloc(pageSize));
        }
        writeRec0(pages.getQuick(pgInx) + pgOfs, record);
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
        writeRec0(pages.getQuick(pageIndex(offset)) + pageOffset(offset), record);
    }

    private void writeRec0(long addr, Record record) {
        for (int i = 0, n = slaveValueIndexes.size(); i < n; i++) {
            RecordUtils.copyFixed(slaveValueTypes.getQuick(i), record, slaveValueIndexes.getQuick(i), addr + fixedOffsets.getQuick(i));
        }
    }

    private class MapRecord extends AbstractMemRecord {
        private long address;

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
}
