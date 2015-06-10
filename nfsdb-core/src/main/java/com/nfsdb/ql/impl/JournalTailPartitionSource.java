/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/
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
import com.nfsdb.utils.Rows;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class JournalTailPartitionSource extends AbstractImmutableIterator<PartitionSlice> implements PartitionSource, PartitionCursor {
    private final boolean open;
    private final long rowid;
    private final PartitionSlice slice = new PartitionSlice();
    private final JournalMetadata metadata;
    private final boolean dynamicJournal;
    private final MasterSymFacade symFacade = new MasterSymFacade();
    private Journal journal;
    private int partitionIndex;
    private long lo;
    private int partitionCount;

    public JournalTailPartitionSource(Journal journal, boolean open, long rowid) {
        this.journal = journal;
        this.metadata = journal.getMetadata();
        this.open = open;
        this.rowid = rowid;
        this.dynamicJournal = false;
        this.symFacade.setJournal(journal);
    }

    public JournalTailPartitionSource(JournalMetadata metadata, boolean open, long rowid) {
        this.open = open;
        this.rowid = rowid;
        this.metadata = metadata;
        this.dynamicJournal = true;
    }

    @Override
    public JournalMetadata getMetadata() {
        return metadata;
    }

    @Override
    public SymFacade getSymFacade() {
        return symFacade;
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
            slice.lo = lo;
            slice.calcHi = true;
            lo = 0;
            return slice;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public PartitionCursor prepareCursor(JournalReaderFactory readerFactory) throws JournalException {
        if (dynamicJournal) {
            this.journal = readerFactory.reader(metadata);
            this.symFacade.setJournal(journal);
        }
        reset();
        return this;
    }

    @Override
    public final void reset() {
        partitionCount = journal.getPartitionCount();
        partitionIndex = Rows.toPartitionIndex(rowid);
        lo = Rows.toLocalRowID(rowid);
    }
}
