package com.nfsdb.journal.lang.cst.impl.ksrc;

import com.nfsdb.journal.lang.cst.KeyCursor;
import com.nfsdb.journal.lang.cst.KeySource;
import com.nfsdb.journal.lang.cst.PartitionSlice;
import com.nfsdb.journal.lang.cst.impl.ref.IntRef;

public class SingleKeySource implements KeySource, KeyCursor {

    private final IntRef keyRef;
    private boolean hasNext = true;

    public SingleKeySource(IntRef keyRef) {
        this.keyRef = keyRef;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public int next() {
        hasNext = false;
        return keyRef.value;
    }

    @Override
    public KeyCursor cursor(PartitionSlice partition) {
        return this;
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public void reset() {
        hasNext = true;
    }
}
