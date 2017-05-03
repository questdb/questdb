package com.questdb.ql.impl.sys;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.ql.impl.CollectionRecordMetadata;
import com.questdb.ql.impl.RecordColumnMetadataImpl;
import com.questdb.store.ColumnType;

public class $LocalesRecordMetadata extends CollectionRecordMetadata {
    private static final RecordColumnMetadata TAG = new RecordColumnMetadataImpl("tag", ColumnType.STRING);

    public $LocalesRecordMetadata() {
        this.add(TAG);
    }
}
