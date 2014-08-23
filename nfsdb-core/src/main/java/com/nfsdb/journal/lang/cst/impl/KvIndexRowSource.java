package com.nfsdb.journal.lang.cst.impl;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.index.Cursor;
import com.nfsdb.journal.index.KVIndex;
import com.nfsdb.journal.lang.cst.KeyCursor;
import com.nfsdb.journal.lang.cst.KeySource;
import com.nfsdb.journal.lang.cst.RowCursor;
import com.nfsdb.journal.lang.cst.RowSource;

public class KvIndexRowSource implements RowSource, RowCursor {

    private final String symbol;
    private final KeySource keySource;
    private KVIndex index;
    private Cursor indexCursor;
    private KeyCursor keyCursor;

    public KvIndexRowSource(String symbol, KeySource keySource) {
        this.symbol = symbol;
        this.keySource = keySource;
    }

    @Override
    public RowCursor cursor(Partition partition) {
        try {
            this.index = partition.getIndexForColumn(symbol);
            this.keyCursor = this.keySource.cursor(partition);
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
        return this;
    }

    @Override
    public boolean hasNext() {

        if (indexCursor == null && !keyCursor.hasNext()) {
            return false;
        }

        if (indexCursor == null) {
            this.indexCursor = index.cachedCursor(keyCursor.next());
        }

        return indexCursor.hasNext();
    }

    @Override
    public long next() {
        return indexCursor.next();
    }
}
