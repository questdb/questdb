package com.nfsdb.journal.iterators;

public final class JournalIteratorRange {
    final int partitionID;
    final long upperRowIDBound;
    final long lowerRowIDBound;

    public JournalIteratorRange(int partitionID, long lowerRowIDBound, long upperRowIDBound) {
        this.partitionID = partitionID;
        this.lowerRowIDBound = lowerRowIDBound;
        this.upperRowIDBound = upperRowIDBound;
    }
}