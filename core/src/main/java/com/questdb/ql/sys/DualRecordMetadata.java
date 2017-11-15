package com.questdb.ql.sys;

import com.questdb.common.ColumnType;
import com.questdb.common.RecordColumnMetadata;
import com.questdb.ql.CollectionRecordMetadata;
import com.questdb.ql.RecordColumnMetadataImpl;

public class DualRecordMetadata extends CollectionRecordMetadata {
    private static final RecordColumnMetadata X = new RecordColumnMetadataImpl("X", ColumnType.STRING);

    public DualRecordMetadata() {
        this.add(X);
    }
}
