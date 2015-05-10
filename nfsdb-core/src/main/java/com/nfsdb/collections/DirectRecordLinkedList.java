/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.collections;

import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.utils.Unsafe;

import java.io.Closeable;
import java.io.IOException;

public class DirectRecordLinkedList extends AbstractImmutableIterator<Record> implements RecordSource<Record>, Closeable, RecordCursor<Record> {
    private final RecordMetadata recordMetadata;
    private final DirectPagedBuffer buffer;
    private final DirectRecord bufferRecord;
    private long readOffset = -1;

    public DirectRecordLinkedList(RecordMetadata recordMetadata, long recordCount, long avgRecSize) {
        this.recordMetadata = recordMetadata;
        this.buffer = new DirectPagedBuffer((recordCount * avgRecSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) (recordCount * avgRecSize)) / 2);
        bufferRecord = new DirectRecord(recordMetadata, buffer);
    }

    public long append(Record record, long prevRecordOffset) {
        long recordAddressBegin = buffer.getWriteOffsetQuick(8 + bufferRecord.getFixedBlockLength());
        Unsafe.getUnsafe().putLong(buffer.toAddress(recordAddressBegin), prevRecordOffset);
        bufferRecord.write(record, recordAddressBegin + 8);
        return recordAddressBegin;
    }

    @Override
    public void close() throws IOException {
        buffer.close();
    }

    @Override
    public RecordMetadata getMetadata() {
        return recordMetadata;
    }

    @Override
    public RecordCursor<Record> prepareCursor() {
        return this;
    }

    @Override
    public void unprepare() {
    }

    @Override
    public boolean hasNext() {
        return readOffset >= 0;
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
