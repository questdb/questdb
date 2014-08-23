package com.nfsdb.journal.lang.cst.impl;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.lang.cst.PartitionSource;
import org.joda.time.Interval;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class PartitionSourceIntervalImpl implements PartitionSource {
    private final PartitionSource delegate;
    private final Interval interval;
    private Partition partition;

    public PartitionSourceIntervalImpl(PartitionSource delegate, Interval interval) {
        this.delegate = delegate;
        this.interval = interval;
    }

    @Override
    public boolean hasNext() {
        if (partition == null) {
            while (delegate.hasNext()) {
                Partition partition = delegate.next();
                if (partition.getInterval().overlaps(interval)) {
                    this.partition = partition;
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    @Override
    public Partition next() {
        Partition result = partition;
        partition = null;
        return result;
    }

    @Override
    public void remove() {
        throw new NotImplementedException();
    }
}
