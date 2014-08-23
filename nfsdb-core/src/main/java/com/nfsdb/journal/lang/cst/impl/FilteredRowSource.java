package com.nfsdb.journal.lang.cst.impl;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.lang.cst.*;

public class FilteredRowSource implements RowSource, RowCursor {

    private final RowSource delegate;
    private final RowFilter filter;
    private RowCursor underlying;
    private RowAcceptor acceptor;
    private long rowid;
    private boolean skip;

    public FilteredRowSource(RowSource delegate, RowFilter filter) {
        this.delegate = delegate;
        this.filter = filter;
    }

    @Override
    public RowCursor cursor(Partition partition) {
        this.underlying = delegate.cursor(partition);
        this.acceptor = filter.acceptor(partition, null);
        this.rowid = -1;
        this.skip = false;
        return this;
    }

    @Override
    public boolean hasNext() {
        if (this.rowid == -1) {

            if (skip) {
                return false;
            }

            long rowid;

            A:
            while(underlying.hasNext()) {
                rowid = underlying.next();

                Choice choice = acceptor.accept(rowid, -1);
                switch (choice) {
                    case SKIP:
                        break;
                    case PICK:
                        this.rowid = rowid;
                        break A;
                    case PICK_AND_SKIP_PARTITION:
                        this.rowid = rowid;
                        this.skip = true;
                        break A;

                }
            }
        }
        return this.rowid > -1;
    }

    @Override
    public long next() {
        long rowid = this.rowid;
        this.rowid = -1;
        return rowid;
    }
}
