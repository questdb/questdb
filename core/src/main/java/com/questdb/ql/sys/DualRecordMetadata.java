package com.questdb.ql.sys;

import com.questdb.ql.CollectionRecordMetadata;
import com.questdb.ql.RecordColumnMetadataImpl;
import com.questdb.store.ColumnType;
import com.questdb.store.factory.configuration.RecordColumnMetadata;

public class DualRecordMetadata extends CollectionRecordMetadata {
    private static final RecordColumnMetadata X = new RecordColumnMetadataImpl("X", ColumnType.STRING);

    public DualRecordMetadata() {
        this.add(X);
    }
}
