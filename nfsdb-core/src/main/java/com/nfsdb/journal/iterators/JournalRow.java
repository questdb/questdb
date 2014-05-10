package com.nfsdb.journal.iterators;

public class JournalRow<T> {
    private final T object;
    private long rowID;

    public JournalRow(T object) {
        this.object = object;
    }

    public T getObject() {
        return object;
    }

    public long getRowID() {
        return rowID;
    }

    void setRowID(long rowID) {
        this.rowID = rowID;
    }
}
