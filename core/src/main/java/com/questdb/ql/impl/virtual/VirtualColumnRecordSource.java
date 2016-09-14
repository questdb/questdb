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
 ******************************************************************************/

package com.questdb.ql.impl.virtual;

import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Misc;
import com.questdb.ql.*;
import com.questdb.ql.impl.join.SplitRecordStorageFacade;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.CharSink;
import com.questdb.std.ObjList;
import com.questdb.store.MMappedSymbolTable;

public class VirtualColumnRecordSource extends AbstractCombinedRecordSource {
    private final RecordSource recordSource;
    private final RecordMetadata metadata;
    private final VirtualRecord current;
    private final SplitRecordStorageFacade storageFacade;
    private final VirtualColumnStorageFacade virtualColumnStorageFacade;
    private RecordCursor recordCursor;

    public VirtualColumnRecordSource(RecordSource recordSource, ObjList<VirtualColumn> virtualColumns) {
        this.recordSource = recordSource;
        RecordMetadata dm = recordSource.getMetadata();
        this.metadata = new VirtualRecordMetadata(dm, virtualColumns);
        this.current = new VirtualRecord(dm.getColumnCount(), virtualColumns);
        this.virtualColumnStorageFacade = new VirtualColumnStorageFacade();
        this.storageFacade = new SplitRecordStorageFacade(dm.getColumnCount());
    }

    @Override
    public void close() {
        Misc.free(recordSource);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) {
        this.recordCursor = recordSource.prepareCursor(factory, cancellationHandler);
        this.virtualColumnStorageFacade.prepare(factory);
        storageFacade.prepare(factory, recordCursor.getStorageFacade(), this.virtualColumnStorageFacade);
        current.prepare(storageFacade);
        return this;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public boolean hasNext() {
        return recordCursor.hasNext();
    }

    @Override
    public Record next() {
        current.setBase(recordCursor.next());
        return current;
    }

    @Override
    public Record newRecord() {
        VirtualRecord copy = current.copy();
        copy.setBase(recordCursor.newRecord());
        return copy;
    }

    @Override
    public Record recordAt(long rowId) {
        current.setBase(recordCursor.recordAt(rowId));
        return current;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        recordCursor.recordAt(((VirtualRecord) record).getBase(), atRowId);
    }

    @Override
    public boolean supportsRowIdAccess() {
        return recordSource.supportsRowIdAccess();
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("VirtualColumnRecordSource").put(',');
        sink.putQuoted("src").put(':').put(recordSource);
        sink.put('}');
    }

    private static class VirtualColumnStorageFacade implements StorageFacade {
        private JournalReaderFactory factory;

        @Override
        public JournalReaderFactory getFactory() {
            return factory;
        }

        @Override
        public MMappedSymbolTable getSymbolTable(int index) {
            return null;
        }

        public void prepare(JournalReaderFactory factory) {
            this.factory = factory;
        }
    }
}
