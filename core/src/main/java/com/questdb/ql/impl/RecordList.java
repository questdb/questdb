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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
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
    private long readOffset = -1;

    public RecordList(RecordMetadata recordMetadata, int pageSize) {
        this.metadata = recordMetadata;
        this.mem = new MemoryPages(pageSize);
        record = new RecordListRecord(recordMetadata, mem);
    }

    public long append(Record record, long prevOffset) {
        long offset = mem.allocate(8 + this.record.getFixedBlockLength());
        if (prevOffset != -1) {
            Unsafe.getUnsafe().putLong(mem.addressOf(prevOffset), offset);
        }
        Unsafe.getUnsafe().putLong(mem.addressOf(offset), -1L);
        this.record.append(record, offset + 8);
        return offset;
    }

    public void clear() {
        mem.clear();
        readOffset = -1;
    }

    @Override
    public void close() {
        mem.close();
    }

    @Override
    public Record getByRowId(long rowId) {
        return null;
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
        return readOffset > -1;
    }

    @Override
    public Record next() {
        record.of(readOffset + 8);
        readOffset = Unsafe.getUnsafe().getLong(mem.addressOf(readOffset));
        return record;
    }

    public void of(long offset) {
        this.readOffset = offset;
    }

    public Record recordAt(long offset) {
        record.of(offset + 8);
        return record;
    }
}
