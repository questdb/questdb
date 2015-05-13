/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.impl;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.ql.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class JournalSource extends AbstractJournalSource implements JournalRecordSource, RandomAccessRecordCursor<JournalRecord> {
    private final PartitionSource partitionSource;
    private final RowSource rowSource;
    private final JournalRecord rec = new JournalRecord(this);
    private PartitionCursor partitionCursor;
    private RowCursor cursor;

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    public JournalSource(PartitionSource partitionSource, RowSource rowSource) {
        super(partitionSource.getMetadata());
        this.partitionSource = partitionSource;
        rowSource.configure(partitionSource.getMetadata());
        this.rowSource = rowSource;
    }

    @Override
    public JournalRecord getByRowId(long rowId) {
        rec.rowid = rowId;
        return rec;
    }

    @Override
    public RecordMetadata getMetadata() {
        return this;
    }

    @Override
    public void reset() {
        if (partitionCursor != null) {
            partitionCursor.reset();
        }
        rowSource.reset();
        cursor = null;
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
    public RandomAccessRecordCursor<JournalRecord> prepareCursor(JournalReaderFactory factory) throws JournalException {
        this.partitionCursor = partitionSource.prepareCursor(factory);
        return this;
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
                return true;
            }
        }

        return false;
    }
}
