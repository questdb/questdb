package com.nfsdb.journal.lang.cst.impl;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.lang.cst.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

public class JournalSourceImpl implements JournalSource {
    private final PartitionSource partitionSource;
    private final RowSource rowSource;
    private RowCursor cursor;
    private final DataItemImpl item = new DataItemImpl();

    public JournalSourceImpl(PartitionSource partitionSource, RowSource rowSource) {
        this.partitionSource = partitionSource;
        this.rowSource = rowSource;
    }

    @Override
    public Iterator<DataItem> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        while (cursor == null || !cursor.hasNext()) {
            if (partitionSource.hasNext()) {
                Partition partition = partitionSource.next();
                cursor = rowSource.cursor(partition);
                item.partition = partition;
            } else {
                return false;
            }
        }
        return true;
    }

    @Override
    public DataItem next() {
        item.rowid = cursor.next();
        return item;
    }

    @Override
    public void remove() {
        throw new NotImplementedException();
    }
}
