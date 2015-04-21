package com.nfsdb.ql.impl;

import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordMetadata;

public interface HashJoinStoreStrategyContext {
    Record getHashTableRecord(Record r);
    RecordMetadata getHashRecordMetadata();
    Record getNextSlave(Record hashTableRecord);
}
