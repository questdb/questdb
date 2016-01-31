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

package com.nfsdb.ql.impl.join.hash;

import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.std.AbstractImmutableIterator;
import com.nfsdb.std.Mutable;
import com.nfsdb.store.SequentialMemory;

import java.io.Closeable;
import java.io.IOException;

public class RecordDequeue extends AbstractImmutableIterator<Record> implements Closeable, RecordCursor<Record>, Mutable {
    private final SequentialMemory mem;
    private final MemoryRecordAccessor accessor;
    private final RecordMetadata metadata;
    private long readOffset = -1;

    public RecordDequeue(RecordMetadata recordMetadata, int pageSize) {
        this.metadata = recordMetadata;
        this.mem = new SequentialMemory(pageSize);
        accessor = new MemoryRecordAccessor(recordMetadata, mem);
    }

    public long append(Record record, long prevOffset) {
        long offset = mem.allocate(8 + accessor.getFixedBlockLength());
        if (prevOffset != -1) {
            Unsafe.getUnsafe().putLong(mem.addressOf(prevOffset), offset);
        }
        Unsafe.getUnsafe().putLong(mem.addressOf(offset), -1L);
        accessor.append(record, offset + 8);
        return offset;
    }

    public void clear() {
        mem.clear();
        readOffset = -1;
    }

    @Override
    public void close() throws IOException {
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
        accessor.setStorageFacade(storageFacade);
    }

    @Override
    public boolean hasNext() {
        return readOffset > -1;
    }

    @Override
    public Record next() {
        accessor.init(readOffset + 8);
        readOffset = Unsafe.getUnsafe().getLong(mem.addressOf(readOffset));
        return accessor;
    }

    public void init(long offset) {
        this.readOffset = offset;
    }
}
