package com.nfsdb.lang.cst.impl.join;

import com.nfsdb.lang.cst.impl.qry.Record;
import com.nfsdb.lang.cst.impl.qry.RecordSource;

public interface JoinHashTable {
    RecordSource<? extends Record> getRows(Object key);
}
