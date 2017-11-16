package com.questdb.ql.sys;

import com.questdb.BootstrapEnv;
import com.questdb.common.RecordMetadata;
import com.questdb.ql.RecordSource;
import com.questdb.store.factory.ReaderFactory;

public class DualFactory implements SystemViewFactory {

    public static final DualFactory INSTANCE = new DualFactory();

    @Override
    public RecordSource create(ReaderFactory factory, BootstrapEnv env) {
        return new DualRecordSource();
    }

    @Override
    public RecordMetadata getMetadata() {
        return new DualRecordMetadata();
    }
}
