package com.questdb.ql.sys;

import com.questdb.ql.CancellationHandler;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.ObjList;
import com.questdb.std.str.CharSink;
import com.questdb.std.time.DateLocaleFactory;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.factory.configuration.RecordMetadata;

public class $LocalesRecordSource extends AbstractCombinedRecordSource {
    private final ObjList<CharSequence> locales;
    private final int count;
    private final RecordMetadata metadata = new $LocalesRecordMetadata();
    private final RecordImpl record;
    private int index = 0;

    public $LocalesRecordSource(DateLocaleFactory dateLocaleFactory) {
        locales = dateLocaleFactory.getAll().keys();
        count = locales.size();
        this.record = new RecordImpl(locales);
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
        return new RecordImpl(locales);
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
        return index < count;
    }

    @Override
    public Record next() {
        return record.of(index++);
    }

    @Override
    public Record recordAt(long rowId) {
        return record.of((int) rowId);
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((RecordImpl) record).of((int) atRowId);
    }

    @Override
    public boolean supportsRowIdAccess() {
        return true;
    }

    @Override
    public void toSink(CharSink sink) {

    }

    private static class RecordImpl implements Record {

        private final ObjList<CharSequence> locales;
        int index;

        public RecordImpl(ObjList<CharSequence> locales) {
            this.locales = locales;
        }

        @Override
        public CharSequence getFlyweightStr(int col) {
            return locales.getQuick(index);
        }

        @Override
        public CharSequence getFlyweightStrB(int col) {
            return locales.getQuick(index);
        }

        @Override
        public long getRowId() {
            return index;
        }

        @Override
        public int getStrLen(int col) {
            return locales.getQuick(index).length();
        }

        private Record of(int index) {
            this.index = index;
            return this;
        }
    }
}
