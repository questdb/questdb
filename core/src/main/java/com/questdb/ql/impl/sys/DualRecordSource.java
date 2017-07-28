package com.questdb.ql.impl.sys;

import com.questdb.factory.ReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.CancellationHandler;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.DirectInputStream;
import com.questdb.std.str.CharSink;

import java.io.OutputStream;

public class DualRecordSource extends AbstractCombinedRecordSource {
    private final RecordMetadata metadata = new DualRecordMetadata();
    private final RecordImpl record;
    private int index = 0;

    public DualRecordSource() {
        this.record = new RecordImpl();
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(ReaderFactory factory, CancellationHandler cancellationHandler) {
        this.index = 0;
        return this;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record newRecord() {
        return new RecordImpl();
    }

    @Override
    public StorageFacade getStorageFacade() {
        return null;
    }

    @Override
    public void releaseCursor() {
    }

    @Override
    public void toTop() {
        this.index = 0;
    }

    @Override
    public boolean hasNext() {
        return index < 1;
    }

    @Override
    public Record next() {
        index++;
        return record;
    }

    @Override
    public Record recordAt(long rowId) {
        return record;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
    }

    @Override
    public boolean supportsRowIdAccess() {
        return true;
    }

    @Override
    public void toSink(CharSink sink) {

    }

    private static class RecordImpl implements Record {

        @Override
        public byte get(int col) {
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
        }

        @Override
        public long getDate(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getDouble(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getFloat(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getFlyweightStr(int col) {
            return "Y";
        }

        @Override
        public CharSequence getFlyweightStrB(int col) {
            return getFlyweightStr(col);
        }

        @Override
        public int getInt(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getRowId() {
            return 0;
        }

        @Override
        public short getShort(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getStrLen(int col) {
            return 1;
        }

        @Override
        public String getSym(int col) {
            throw new UnsupportedOperationException();
        }
    }
}
