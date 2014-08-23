package com.nfsdb.journal.lang.cst.impl;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.lang.cst.RowCursor;
import com.nfsdb.journal.lang.cst.RowSource;

public class UnionRowSource implements RowSource, RowCursor {
    private final RowSource[] sources;
    private RowCursor[] cursors;
    private int cursorIndex;

    public UnionRowSource(RowSource[] sources) {
        this.sources = sources;
        this.cursors = new RowCursor[sources.length];
    }

    @Override
    public RowCursor cursor(Partition partition) {
        for (int i = 0; i < sources.length; i++) {
            RowSource source = sources[i];
            cursors[i] = source.cursor(partition);
        }
        cursorIndex = 0;
        return this;
    }

    @Override
    public boolean hasNext() {

        while (cursorIndex < cursors.length) {
            if (cursors[cursorIndex].hasNext()) {
                return true;
            }
            cursorIndex++;
        }

        return false;
    }

    @Override
    public long next() {
        return cursors[cursorIndex].next();
    }
}
