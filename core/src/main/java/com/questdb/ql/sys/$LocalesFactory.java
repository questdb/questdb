package com.questdb.ql.sys;

import com.questdb.BootstrapEnv;
import com.questdb.common.RecordMetadata;
import com.questdb.ql.RecordSource;
import com.questdb.store.factory.ReaderFactory;

public class $LocalesFactory implements SystemViewFactory {

    public static final $LocalesFactory INSTANCE = new $LocalesFactory();

    @Override
    public RecordSource create(ReaderFactory factory, BootstrapEnv env) {
        return new $LocalesRecordSource(env.dateLocaleFactory);
    }

    @Override
    public RecordMetadata getMetadata() {
        return new $LocalesRecordMetadata();
    }
}
