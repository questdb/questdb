package com.questdb.ql.sys;

import com.questdb.common.ColumnType;
import com.questdb.common.RecordColumnMetadata;
import com.questdb.ql.CollectionRecordMetadata;
import com.questdb.ql.RecordColumnMetadataImpl;

public class $LocalesRecordMetadata extends CollectionRecordMetadata {
    private static final RecordColumnMetadata TAG = new RecordColumnMetadataImpl("tag", ColumnType.STRING);

    public $LocalesRecordMetadata() {
        this.add(TAG);
    }
}
