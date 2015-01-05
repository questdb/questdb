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

package com.nfsdb.lang.cst.impl.psrc;

import com.nfsdb.Journal;
import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.lang.cst.PartitionSlice;
import com.nfsdb.lang.cst.PartitionSource;

public class JournalDescPartitionSource extends AbstractImmutableIterator<PartitionSlice> implements PartitionSource {

    private final Journal journal;
    private final boolean open;
    private final PartitionSlice slice = new PartitionSlice();
    private int partitionIndex;

    public JournalDescPartitionSource(Journal journal, boolean open) {
        this.journal = journal;
        this.open = open;
        reset();
    }

    @Override
    public boolean hasNext() {
        return partitionIndex >= 0;
    }

    @Override
    public PartitionSlice next() {
        try {
            slice.partition = journal.getPartition(partitionIndex--, open);
            slice.lo = 0;
            slice.calcHi = true;
            return slice;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public void reset() {
        partitionIndex = journal.getPartitionCount() - 1;
    }

    @Override
    public Journal getJournal() {
        return journal;
    }
}
