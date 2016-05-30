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

package com.questdb.ql.impl;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.StorageFacade;
import com.questdb.std.AbstractImmutableIterator;
import com.questdb.std.Mutable;
import com.questdb.store.MemoryPages;

import java.io.Closeable;

public class RecordList extends AbstractImmutableIterator<Record> implements Closeable, RecordCursor, Mutable {
    private final MemoryPages mem;
    private final RecordListRecord record;
    private final RecordMetadata metadata;
    private long readAddress = -1;

    public RecordList(RecordMetadata recordMetadata, int pageSize) {
        this.metadata = recordMetadata;
        this.mem = new MemoryPages(pageSize);
        record = new RecordListRecord(recordMetadata, mem);
    }

    public long append(Record record, long prevAddress) {
        long thisAddress = mem.addressOf(mem.allocate(8 + this.record.getFixedBlockLength()));
        if (prevAddress != -1) {
            Unsafe.getUnsafe().putLong(prevAddress, thisAddress);
        }
        Unsafe.getUnsafe().putLong(thisAddress, -1L);
        this.record.append(record, thisAddress + 8);
        return thisAddress;
    }

    public void clear() {
        mem.clear();
        readAddress = -1;
    }

    @Override
    public void close() {
        mem.close();
    }

    @Override
    public Record getByRowId(long rowId) {
        record.of(rowId + 8);
        return record;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return null;
    }

    public void setStorageFacade(StorageFacade storageFacade) {
        record.setStorageFacade(storageFacade);
    }

    @Override
    public boolean hasNext() {
        return readAddress > -1;
    }

    @Override
    public Record next() {
        record.of(readAddress + 8);
        readAddress = Unsafe.getUnsafe().getLong(readAddress);
        return record;
    }

    public void of(long offset) {
        this.readAddress = offset;
    }

}
