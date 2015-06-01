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

import com.nfsdb.Journal;
import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.PartitionCursor;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.PartitionSource;
import com.nfsdb.ql.SymFacade;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"CD_CIRCULAR_DEPENDENCY"})
public class JournalPartitionSource extends AbstractImmutableIterator<PartitionSlice> implements PartitionSource, PartitionCursor {

    private final boolean open;
    private final PartitionSlice slice = new PartitionSlice();
    private final JournalMetadata metadata;
    private final MasterSymFacade symFacade = new MasterSymFacade();
    private final boolean dynamicJournal;
    private Journal journal;
    private int partitionCount;
    private int partitionIndex;

    public JournalPartitionSource(Journal journal, boolean open) {
        this.journal = journal;
        this.metadata = journal.getMetadata();
        this.open = open;
        this.dynamicJournal = false;
        this.symFacade.setJournal(journal);
    }

    public JournalPartitionSource(JournalMetadata metadata, boolean open) {
        this.metadata = metadata;
        this.open = open;
        this.dynamicJournal = true;
    }

    @Override
    public JournalMetadata getMetadata() {
        return metadata;
    }

    @Override
    public PartitionCursor prepareCursor(JournalReaderFactory factory) throws JournalException {
        if (dynamicJournal) {
            this.journal = factory.reader(metadata).select();
            symFacade.setJournal(journal);
        }
        partitionCount = journal.getPartitionCount();
        partitionIndex = 0;
        return this;
    }

    @Override
    public SymFacade getSymFacade() {
        return symFacade;
    }

    @Override
    public final void reset() {
        partitionCount = journal.getPartitionCount();
        partitionIndex = 0;
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
    public String toString() {
        return "JournalPartitionSource{" +
                "open=" + open +
                ", metadata=" + metadata +
                '}';
    }
}
