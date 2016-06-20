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

package com.questdb.ql.impl;

import com.questdb.ex.JournalException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Rows;
import com.questdb.ql.*;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.CharSink;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class JournalRecordSource extends AbstractCombinedRecordSource {
    private final PartitionSource partitionSource;
    private final RowSource rowSource;
    private final JournalRecord rec;
    private final JournalMetadata metadata;
    private PartitionCursor partitionCursor;
    private RowCursor cursor;

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    public JournalRecordSource(PartitionSource partitionSource, RowSource rowSource) {
        this.metadata = partitionSource.getMetadata();
        this.rec = new JournalRecord();
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
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) throws JournalException {
        this.partitionCursor = partitionSource.prepareCursor(factory);
        this.rowSource.prepare(partitionCursor.getStorageFacade(), cancellationHandler);
        return this;
    }

    @Override
    public void reset() {
        // cursor type reset
        if (partitionCursor != null) {
            partitionCursor.reset();
        }
        rowSource.reset();
        cursor = null;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return partitionCursor.getStorageFacade();
    }

    @Override
    public boolean hasNext() {
        return (cursor != null && cursor.hasNext()) || nextSlice();
    }

    @Override
    public JournalRecord next() {
        rec.rowid = cursor.next();
        return rec;
    }

    @Override
    public Record newRecord() {
        return new JournalRecord();
    }

    @Override
    public JournalRecord recordAt(long rowId) {
        rec.rowid = Rows.toLocalRowID(rowId);
        setPartition(rec, rowId);
        return rec;
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
                rec.partition = slice.partition;
                rec.partitionIndex = slice.partition.getPartitionIndex();
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
