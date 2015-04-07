package com.nfsdb.lang.cst.impl.qry;

import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordSource;

public interface RandomAccessRecordSource<T extends Record> extends RecordSource<T> {
    T getByRowId(long rowId);
}
