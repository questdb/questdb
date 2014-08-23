package com.nfsdb.journal.lang.cst.impl;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.lang.cst.KeyCursor;
import com.nfsdb.journal.lang.cst.KeySource;
import com.nfsdb.journal.utils.Checksum;

public class HashKeySource implements KeySource, KeyCursor {
    private final String column;
    private final String[] values;
    private int bucketCount = -1;
    private int valueIndex;

    public HashKeySource(String column, String[] values) {
        this.column = column;
        this.values = values;
    }

    @Override
    public KeyCursor cursor(Partition partition) {
        if (bucketCount == -1) {
            bucketCount = partition.getJournal().getMetadata().getColumnMetadata(column).distinctCountHint;
        }
        this.valueIndex = 0;
        return this;
    }

    @Override
    public boolean hasNext() {
        return valueIndex < values.length;
    }

    @Override
    public int next() {
        return Checksum.hash(values[valueIndex++], bucketCount);
    }
}
