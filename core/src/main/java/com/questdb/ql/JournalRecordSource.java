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

package com.questdb.ql;

import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.Rows;
import com.questdb.std.str.CharSink;
import com.questdb.store.*;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.factory.configuration.JournalMetadata;

public class JournalRecordSource extends AbstractCombinedRecordSource {
    private final PartitionSource partitionSource;
    private final RowSource rowSource;
    private final JournalRecord record;
    private final JournalMetadata metadata;
    private PartitionCursor partitionCursor;
    private RowCursor cursor;

    public JournalRecordSource(PartitionSource partitionSource, RowSource rowSource) {
        this.metadata = partitionSource.getMetadata();
        this.record = new JournalRecord();
        this.partitionSource = partitionSource;
        rowSource.configure(partitionSource.getMetadata());
        this.rowSource = rowSource;
    }

    @Override
    public void close() {
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(ReaderFactory factory, CancellationHandler cancellationHandler) {
        rowSource.toTop();
        cursor = null;
        this.partitionCursor = partitionSource.prepareCursor(factory);
        this.rowSource.prepare(factory, partitionCursor.getStorageFacade(), cancellationHandler);
        return this;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record newRecord() {
        return new JournalRecord();
    }

    @Override
    public StorageFacade getStorageFacade() {
        return partitionCursor.getStorageFacade();
    }

    @Override
    public void releaseCursor() {
        this.partitionCursor.releaseCursor();
    }

    @Override
    public void toTop() {
        cursor = null;
        partitionCursor.toTop();
    }

    @Override
    public boolean hasNext() {
        return (cursor != null && cursor.hasNext()) || nextSlice();
    }

    @Override
    public JournalRecord next() {
        record.rowid = cursor.next();
        return record;
    }

    @Override
    public JournalRecord recordAt(long rowId) {
        record.rowid = Rows.toLocalRowID(rowId);
        setPartition(record, rowId);
        return record;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((JournalRecord) record).rowid = Rows.toLocalRowID(atRowId);
        setPartition((JournalRecord) record, atRowId);
    }

    @Override
    public boolean supportsRowIdAccess() {
        return true;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("JournalRecordSource").put(',');
        sink.putQuoted("psrc").put(':').put(partitionSource).put(',');
        sink.putQuoted("rsrc").put(':').put(rowSource);
        sink.put('}');
    }

    @SuppressWarnings("unchecked")
    private boolean nextSlice() {
        while (partitionCursor.hasNext()) {
            PartitionSlice slice = partitionCursor.next();
            cursor = rowSource.prepareCursor(slice);

            if (cursor == null) {
                return false;
            }

            if (cursor.hasNext()) {
                record.partition = slice.partition;
                record.partitionIndex = slice.partition.getPartitionIndex();
                return true;
            }
        }

        return false;
    }

    private void setPartition(JournalRecord record, long rowId) {
        int partIndex = Rows.toPartitionIndex(rowId);
        if (partIndex != record.partitionIndex) {
            record.partitionIndex = partIndex;
            record.partition = partitionCursor.getPartition(partIndex);
        }
    }
}
