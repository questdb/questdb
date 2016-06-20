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

package com.questdb.ql.impl.select;

import com.questdb.ex.JournalException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Misc;
import com.questdb.ql.*;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.CharSink;
import com.questdb.std.ObjList;
import com.questdb.std.Transient;

public class SelectedColumnsRecordSource extends AbstractCombinedRecordSource {
    private final RecordSource recordSource;
    private final RecordMetadata metadata;
    private final SelectedColumnsRecord record;
    private final SelectedColumnsStorageFacade storageFacade;
    private RecordCursor recordCursor;

    public SelectedColumnsRecordSource(RecordSource recordSource, @Transient ObjList<CharSequence> names, @Transient CharSequenceHashSet aliases) {
        this.recordSource = recordSource;
        RecordMetadata dm = recordSource.getMetadata();
        this.metadata = new SelectedColumnsMetadata(dm, names, aliases);
        this.record = new SelectedColumnsRecord(dm, names);
        this.storageFacade = new SelectedColumnsStorageFacade(dm, names);
    }

    public SelectedColumnsRecordSource(RecordSource recordSource, @Transient ObjList<CharSequence> names) {
        this.recordSource = recordSource;
        RecordMetadata dm = recordSource.getMetadata();
        this.metadata = new SelectedColumnsMetadata(dm, names);
        this.record = new SelectedColumnsRecord(dm, names);
        this.storageFacade = new SelectedColumnsStorageFacade(dm, names);
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
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) throws JournalException {
        this.recordCursor = recordSource.prepareCursor(factory, cancellationHandler);
        this.storageFacade.of(recordCursor.getStorageFacade());
        return this;
    }

    @Override
    public void reset() {
        recordSource.reset();
    }

    @Override
    public boolean supportsRowIdAccess() {
        return recordSource.supportsRowIdAccess();
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public Record newRecord() {
        return record.copy().of(recordCursor.newRecord());
    }

    @Override
    public Record recordAt(long rowId) {
        return record.of(recordCursor.recordAt(rowId));
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        recordCursor.recordAt(((SelectedColumnsRecord) record).getBase(), atRowId);
    }

    @Override
    public boolean hasNext() {
        return recordCursor.hasNext();
    }

    @Override
    public Record next() {
        return record.of(recordCursor.next());
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("SelectedColumnsRecordSource").put(',');
        sink.putQuoted("src").put(':').put(recordSource);
        sink.put('}');
    }
}
