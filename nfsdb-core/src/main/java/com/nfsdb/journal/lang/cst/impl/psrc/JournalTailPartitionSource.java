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

package com.nfsdb.journal.lang.cst.impl.psrc;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.lang.cst.PartitionSlice;
import com.nfsdb.journal.lang.cst.PartitionSource;
import com.nfsdb.journal.utils.Rows;

public class JournalTailPartitionSource extends AbstractImmutableIterator<PartitionSlice> implements PartitionSource {
    private final Journal journal;
    private final boolean open;
    private final long rowid;
    private final PartitionSlice slice = new PartitionSlice();
    private int partitionIndex;
    private long lo;
    private int partitionCount;

    public JournalTailPartitionSource(Journal journal, boolean open, long rowid) {
        this.journal = journal;
        this.open = open;
        this.rowid = rowid;
        reset();
    }

    @Override
    public void reset() {
        partitionCount = journal.getPartitionCount();
        partitionIndex = Rows.toPartitionIndex(rowid);
        lo = Rows.toLocalRowID(rowid);
    }

    @Override
    public boolean hasNext() {
        return partitionIndex < partitionCount;
    }

    @Override
    public PartitionSlice next() {
        try {
            slice.partition = journal.getPartition(partitionIndex++, open);
            slice.lo = lo;
            slice.calcHi = true;
            lo = 0;
            return slice;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public Journal getJournal() {
        return journal;
    }
}
