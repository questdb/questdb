package com.nfsdb.journal.lang.cst.impl.rsrc;

import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.lang.cst.PartitionSlice;
import com.nfsdb.journal.lang.cst.RowCursor;
import com.nfsdb.journal.lang.cst.RowSource;

public class AllRowSource implements RowSource, RowCursor {
    private long lo;
    private long hi;

    @Override
    public RowCursor cursor(PartitionSlice slice) {
        try {
            this.lo = slice.lo;
            this.hi = slice.calcHi ? slice.partition.open().size() - 1 : slice.hi;
            return this;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public void reset() {

    }

    @Override
    public boolean hasNext() {
        return lo <= hi;
    }

    @Override
    public long next() {
        return lo++;
    }
}
