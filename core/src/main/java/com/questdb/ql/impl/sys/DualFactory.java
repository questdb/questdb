package com.questdb.ql.impl.sys;

import com.questdb.BootstrapEnv;
import com.questdb.factory.ReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.RecordSource;

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
