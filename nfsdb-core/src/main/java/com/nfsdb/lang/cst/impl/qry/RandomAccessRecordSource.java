package com.nfsdb.lang.cst.impl.qry;

public interface RandomAccessRecordSource<T extends Record> extends RecordSource<T> {
    T getByRowId(long rowId);
}
