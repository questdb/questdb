package com.questdb.ql.impl.sys;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.ql.impl.CollectionRecordMetadata;
import com.questdb.ql.impl.RecordColumnMetadataImpl;
import com.questdb.store.ColumnType;

public class DualRecordMetadata extends CollectionRecordMetadata {
    private static final RecordColumnMetadata X = new RecordColumnMetadataImpl("X", ColumnType.STRING);

    public DualRecordMetadata() {
        this.add(X);
    }
}
