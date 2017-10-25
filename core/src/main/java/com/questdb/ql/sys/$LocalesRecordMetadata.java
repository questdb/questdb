package com.questdb.ql.sys;

import com.questdb.ql.CollectionRecordMetadata;
import com.questdb.ql.RecordColumnMetadataImpl;
import com.questdb.store.ColumnType;
import com.questdb.store.factory.configuration.RecordColumnMetadata;

public class $LocalesRecordMetadata extends CollectionRecordMetadata {
    private static final RecordColumnMetadata TAG = new RecordColumnMetadataImpl("tag", ColumnType.STRING);

    public $LocalesRecordMetadata() {
        this.add(TAG);
    }
}
