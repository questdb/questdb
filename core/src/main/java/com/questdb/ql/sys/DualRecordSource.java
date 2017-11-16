package com.questdb.ql.sys;

import com.questdb.common.Record;
import com.questdb.common.RecordCursor;
import com.questdb.common.RecordMetadata;
import com.questdb.common.StorageFacade;
import com.questdb.ql.CancellationHandler;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.str.CharSink;
import com.questdb.store.factory.ReaderFactory;

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
        public CharSequence getFlyweightStr(int col) {
            return "Y";
        }

        @Override
        public CharSequence getFlyweightStrB(int col) {
            return getFlyweightStr(col);
        }

        @Override
        public long getRowId() {
            return 0;
        }

        @Override
        public int getStrLen(int col) {
            return 1;
        }
    }
}
