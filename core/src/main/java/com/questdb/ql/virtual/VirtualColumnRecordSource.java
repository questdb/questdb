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

package com.questdb.ql.virtual;

import com.questdb.ql.CancellationHandler;
import com.questdb.ql.RecordSource;
import com.questdb.ql.join.SplitRecordStorageFacade;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.Misc;
import com.questdb.std.ObjList;
import com.questdb.std.str.CharSink;
import com.questdb.store.*;
import com.questdb.store.factory.ReaderFactory;

public class VirtualColumnRecordSource extends AbstractCombinedRecordSource {
    private final RecordSource delegate;
    private final RecordMetadata metadata;
    private final VirtualRecord record;
    private final SplitRecordStorageFacade storageFacade;
    private final VirtualColumnStorageFacade virtualColumnStorageFacade;
    private final ObjList<VirtualColumn> virtualColumns;
    private RecordCursor cursor;

    public VirtualColumnRecordSource(RecordSource delegate, ObjList<VirtualColumn> virtualColumns) {
        this.delegate = delegate;
        this.virtualColumns = virtualColumns;
        RecordMetadata dm = delegate.getMetadata();
        this.metadata = new VirtualRecordMetadata(dm, virtualColumns);
        this.record = new VirtualRecord(dm.getColumnCount(), virtualColumns, delegate.getRecord());
        this.virtualColumnStorageFacade = new VirtualColumnStorageFacade();
        this.storageFacade = new SplitRecordStorageFacade(dm.getColumnCount());
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
        storageFacade.prepare(cursor.getStorageFacade(), this.virtualColumnStorageFacade);
        record.prepare(storageFacade);
        return this;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record newRecord() {
        return record.copy(delegate.newRecord());
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
        cursor.next();
        return record;
    }

    @Override
    public Record recordAt(long rowId) {
        cursor.recordAt(rowId);
        return record;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        cursor.recordAt(((VirtualRecord) record).getBase(), atRowId);
    }

    @Override
    public boolean supportsRowIdAccess() {
        return delegate.supportsRowIdAccess();
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("VirtualColumnRecordSource").put(',');
        sink.putQuoted("src").put(':').put(delegate);
        sink.put('}');
    }

    private class VirtualColumnStorageFacade implements StorageFacade {

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return virtualColumns.getQuick(columnIndex).getSymbolTable();
        }
    }
}
