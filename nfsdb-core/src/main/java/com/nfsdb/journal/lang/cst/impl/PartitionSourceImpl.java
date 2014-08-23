package com.nfsdb.journal.lang.cst.impl;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.Partition;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.lang.cst.PartitionSource;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class PartitionSourceImpl implements PartitionSource {

    private Journal journal;
    private int partitionCount;
    private int partitionIndex;
    private boolean open;

    public PartitionSourceImpl(Journal journal, boolean open) {
        this.journal = journal;
        this.open = open;
        this.partitionCount = journal.getPartitionCount();
    }


    @Override
    public boolean hasNext() {
        return partitionIndex < partitionCount;
    }

    @Override
    public Partition next() {
        try {
            return journal.getPartition(partitionIndex++, open);
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public void remove() {
        throw new NotImplementedException();
    }
}
