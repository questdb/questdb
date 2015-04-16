/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

import com.nfsdb.ql.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class JournalSource extends AbstractJournalSource implements JournalRecordSource {
    private final PartitionSource partitionSource;
    private final RowSource rowSource;
    private final JournalRecord rec = new JournalRecord(this);
    private RowCursor cursor;

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    public JournalSource(PartitionSource partitionSource, RowSource rowSource) {
        super(partitionSource.getJournal().getMetadata());
        this.partitionSource = partitionSource;
        rowSource.configure(partitionSource.getJournal().getMetadata());
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
    public boolean hasNext() {
        return (cursor != null && cursor.hasNext()) || nextSlice();
    }

    @Override
    public JournalRecord next() {
        rec.rowid = cursor.next();
        return rec;
    }

    @Override
    public void reset() {
        partitionSource.reset();
        rowSource.reset();
        cursor = null;
    }

    @SuppressWarnings("unchecked")
    private boolean nextSlice() {
        while (partitionSource.hasNext()) {
            PartitionSlice slice = partitionSource.next();
            cursor = rowSource.cursor(slice);

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
