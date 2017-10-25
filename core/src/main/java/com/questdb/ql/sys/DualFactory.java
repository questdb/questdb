package com.questdb.ql.sys;

import com.questdb.BootstrapEnv;
import com.questdb.ql.RecordSource;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.factory.configuration.RecordMetadata;

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
