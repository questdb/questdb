/*
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
 */

package com.nfsdb.ql.collections;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.collections.Mutable;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.ql.SymFacade;
import com.nfsdb.utils.Unsafe;

import java.io.Closeable;
import java.io.IOException;

public class DirectRecordLinkedList extends AbstractImmutableIterator<Record> implements Closeable, RecordCursor<Record>, Mutable {
    private final DirectPagedBuffer buffer;
    private final DirectRecord bufferRecord;
    private final RecordMetadata metadata;
    private long readOffset = -1;

    public DirectRecordLinkedList(RecordMetadata recordMetadata, long recordCount, long avgRecSize) {
        this.metadata = recordMetadata;
        this.buffer = new DirectPagedBuffer((recordCount * avgRecSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) (recordCount * avgRecSize)) / 2);
        bufferRecord = new DirectRecord(recordMetadata, buffer);
    }

    public long append(Record record, long prevOffset) {
        long offset = buffer.getWriteOffsetQuick(8 + bufferRecord.getFixedBlockLength());
        if (prevOffset != -1) {
            Unsafe.getUnsafe().putLong(buffer.toAddress(prevOffset), offset);
        }
        Unsafe.getUnsafe().putLong(buffer.toAddress(offset), -1L);
        bufferRecord.write(record, offset + 8);
        return offset;
    }

    public void clear() {
        buffer.clear();
    }

    @Override
    public void close() throws IOException {
        buffer.close();
    }

    @Override
    public Record getByRowId(long rowId) {
        return null;
    }

    @Override
    public SymFacade getSymFacade() {
        return null;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean hasNext() {
        return readOffset > -1;
    }

    @Override
    public Record next() {
        bufferRecord.init(readOffset + 8);
        readOffset = Unsafe.getUnsafe().getLong(buffer.toAddress(readOffset));
        return bufferRecord;
    }

    public void init(long offset) {
        this.readOffset = offset;
    }
}
