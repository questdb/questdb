package com.nfsdb.lang.cst.impl.join;

import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordSource;
import com.nfsdb.storage.ColumnType;

public interface JoinHashTable {
    RecordSource<? extends Record> getRows(Record r, int columnIndex, ColumnType type);
}
