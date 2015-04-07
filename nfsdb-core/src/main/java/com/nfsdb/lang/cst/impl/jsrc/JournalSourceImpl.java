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

package com.nfsdb.lang.cst.impl.jsrc;

import com.nfsdb.Journal;
import com.nfsdb.lang.cst.PartitionSlice;
import com.nfsdb.lang.cst.PartitionSource;
import com.nfsdb.lang.cst.RowCursor;
import com.nfsdb.lang.cst.RowSource;
import com.nfsdb.lang.cst.impl.qry.AbstractJournalSource;
import com.nfsdb.lang.cst.impl.qry.JournalRecord;

public class JournalSourceImpl extends AbstractJournalSource {
    private final PartitionSource partitionSource;
    private final RowSource rowSource;
    private final JournalRecord item = new JournalRecord(this);
    private RowCursor cursor;

    public JournalSourceImpl(PartitionSource partitionSource, RowSource rowSource) {
        super(partitionSource.getJournal().getMetadata());
        this.partitionSource = partitionSource;
        this.rowSource = rowSource;
    }

    @Override
    public Journal getJournal() {
        return partitionSource.getJournal();
    }

    @Override
    public boolean hasNext() {
        return (cursor != null && cursor.hasNext()) || nextSlice();
    }

    @Override
    public JournalRecord next() {
        item.rowid = cursor.next();
        return item;
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
                item.partition = slice.partition;
                return true;
            }
        }

        return false;

/*
        do {
            if (partitionSource.hasNext()) {
                PartitionSlice slice = partitionSource.next();
                cursor = rowSource.cursor(slice);

                if (cursor == null) {
                    return false;
                }

                item.partition = slice.partition;
            } else {
                return false;
            }
        } while (!cursor.hasNext());

        return true;
*/
    }
}
