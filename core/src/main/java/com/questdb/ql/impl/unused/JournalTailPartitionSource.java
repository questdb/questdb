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

package com.questdb.ql.impl.unused;

import com.questdb.Journal;
import com.questdb.Partition;
import com.questdb.ex.JournalException;
import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.misc.Rows;
import com.questdb.ql.PartitionCursor;
import com.questdb.ql.PartitionSlice;
import com.questdb.ql.PartitionSource;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.impl.MasterStorageFacade;
import com.questdb.std.AbstractImmutableIterator;
import com.questdb.std.CharSink;
import com.questdb.std.FileNameExtractorCharSequence;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class JournalTailPartitionSource extends AbstractImmutableIterator<PartitionSlice> implements PartitionSource, PartitionCursor {
    private final boolean open;
    private final long rowid;
    private final PartitionSlice slice = new PartitionSlice();
    private final JournalMetadata metadata;
    private final boolean dynamicJournal;
    private final MasterStorageFacade symFacade = new MasterStorageFacade();
    private Journal journal;
    private int partitionIndex;
    private long lo;
    private int partitionCount;

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
    public PartitionCursor prepareCursor(JournalReaderFactory readerFactory) throws JournalException {
        if (dynamicJournal) {
            this.journal = readerFactory.reader(metadata);
            this.symFacade.setJournal(journal);
        }
        reset();
        return this;
    }

    @Override
    public Partition getPartition(int index) {
        try {
            return journal.getPartition(index, open);
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public StorageFacade getStorageFacade() {
        return symFacade;
    }

    @Override
    public final void reset() {
        partitionCount = journal.getPartitionCount();
        partitionIndex = Rows.toPartitionIndex(rowid);
        lo = Rows.toLocalRowID(rowid);
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
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("JournalTailPartitionSource").put(',');
        sink.putQuoted("journal").put(':').putQuoted(FileNameExtractorCharSequence.get(metadata.getLocation())).put(',');
        sink.putQuoted("rowid").put(':').put(rowid);
        sink.put('}');
    }
}
