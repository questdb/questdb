package com.nfsdb.journal.lang.cst.impl;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.lang.cst.DataItem;

public class DataItemImpl implements DataItem {
    public Partition partition;
    public long rowid;

    @Override
    public Partition getPartition() {
        return partition;
    }

    @Override
    public long localRowID() {
        return rowid;
    }
}
