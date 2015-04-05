package com.nfsdb.collections;

import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordMetadata;
import com.nfsdb.lang.cst.RecordSource;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class DirectRecordLinkedList implements Closeable, RecordSource<Record> {
    private final RecordMetadata recordMetadata;
    private final DirectLinkedBuffer buffer;
    private final DirectRecord bufferRecord;
    private long appendOffset;
    private long readOffset = -1;

    public DirectRecordLinkedList(RecordMetadata recordMetadata, long recordCount, long avgRecSize) {
        this.recordMetadata = recordMetadata;
        this.buffer = new DirectLinkedBuffer(
                (long)(Math.ceil(recordCount * avgRecSize / 2.0 / AbstractDirectList.CACHE_LINE_SIZE)) * AbstractDirectList.CACHE_LINE_SIZE);
        bufferRecord = new DirectRecord(recordMetadata, buffer);
        appendOffset = 0;
    }

    public long append(Record record, long prevRecordOffset) {
        long recordAddressBegin = appendOffset;
        buffer.writeLong(prevRecordOffset, appendOffset);
        appendOffset += 8;
        bufferRecord.init(appendOffset);
        appendOffset += bufferRecord.write(record);
        return recordAddressBegin;
    }

    public void init(long offset) {
        this.readOffset = offset;
    }

    @Override
    public void close() throws IOException {
        free();
    }

    @Override
    protected void finalize() throws Throwable {
        free();
        super.finalize();
    }

    private void free() throws IOException {
        buffer.close();
        bufferRecord.close();
    }

    @Override
    public RecordMetadata getMetadata() {
        return recordMetadata;
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Record> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return readOffset >= 0;
    }

    @Override
    public Record next() {
        bufferRecord.init(readOffset + 8);
        readOffset = buffer.readLong(readOffset);
        return bufferRecord;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
