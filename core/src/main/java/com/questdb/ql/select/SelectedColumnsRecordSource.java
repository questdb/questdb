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

package com.questdb.ql.select;

import com.questdb.ql.CancellationHandler;
import com.questdb.ql.RecordSource;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.Misc;
import com.questdb.std.ObjList;
import com.questdb.std.Transient;
import com.questdb.std.str.CharSink;
import com.questdb.store.Record;
import com.questdb.store.RecordCursor;
import com.questdb.store.RecordMetadata;
import com.questdb.store.StorageFacade;
import com.questdb.store.factory.ReaderFactory;

public class SelectedColumnsRecordSource extends AbstractCombinedRecordSource {
    private final RecordSource delegate;
    private final RecordMetadata metadata;
    private final SelectedColumnsRecord record;
    private final SelectedColumnsStorageFacade storageFacade;
    private RecordCursor cursor;

    public SelectedColumnsRecordSource(RecordSource delegate, @Transient ObjList<CharSequence> names, @Transient CharSequenceHashSet aliases) {
        this.delegate = delegate;
        RecordMetadata dm = delegate.getMetadata();
        this.metadata = new SelectedColumnsMetadata(dm, names, aliases);
        this.record = new SelectedColumnsRecord(dm, names);
        this.storageFacade = new SelectedColumnsStorageFacade(dm, names);
    }

    public SelectedColumnsRecordSource(RecordSource delegate, @Transient ObjList<CharSequence> names) {
        this.delegate = delegate;
        RecordMetadata dm = delegate.getMetadata();
        this.metadata = new SelectedColumnsMetadata(dm, names);
        this.record = new SelectedColumnsRecord(dm, names);
        this.storageFacade = new SelectedColumnsStorageFacade(dm, names);
    }

    @Override
    public void close() {
        Misc.free(delegate);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(ReaderFactory factory, CancellationHandler cancellationHandler) {
        this.cursor = delegate.prepareCursor(factory, cancellationHandler);
        this.storageFacade.of(cursor.getStorageFacade());
        return this;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record newRecord() {
        return record.copy().of(delegate.newRecord());
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public void releaseCursor() {
        this.cursor.releaseCursor();
    }

    @Override
    public void toTop() {
        this.cursor.toTop();
    }

    @Override
    public boolean hasNext() {
        return cursor.hasNext();
    }

    @Override
    public Record next() {
        return record.of(cursor.next());
    }

    @Override
    public Record recordAt(long rowId) {
        return record.of(cursor.recordAt(rowId));
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        cursor.recordAt(((SelectedColumnsRecord) record).getBase(), atRowId);
    }

    @Override
    public boolean supportsRowIdAccess() {
        return delegate.supportsRowIdAccess();
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("SelectedColumnsRecordSource").put(',');
        sink.putQuoted("src").put(':').put(delegate);
        sink.put('}');
    }
}
