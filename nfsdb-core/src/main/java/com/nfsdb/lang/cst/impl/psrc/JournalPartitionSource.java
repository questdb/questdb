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

package com.nfsdb.lang.cst.impl.psrc;

import com.nfsdb.Journal;
import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.lang.cst.PartitionSlice;
import com.nfsdb.lang.cst.PartitionSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class JournalPartitionSource extends AbstractImmutableIterator<PartitionSlice> implements PartitionSource {

    private final Journal journal;
    private final boolean open;
    private final PartitionSlice slice = new PartitionSlice();
    private int partitionCount;
    private int partitionIndex;

    public JournalPartitionSource(Journal journal, boolean open) {
        this.journal = journal;
        this.open = open;
        reset();
    }

    @Override
    public Journal getJournal() {
        return journal;
    }

    @Override
    public boolean hasNext() {
        return partitionIndex < partitionCount;
    }

    @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
    @Override
    public PartitionSlice next() {
        try {
            slice.partition = journal.getPartition(partitionIndex++, open);
            slice.lo = 0;
            slice.calcHi = true;
            return slice;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public final void reset() {
        partitionCount = journal.getPartitionCount();
        partitionIndex = 0;
    }
}
