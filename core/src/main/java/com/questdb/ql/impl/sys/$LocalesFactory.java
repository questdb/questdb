package com.questdb.ql.impl.sys;

import com.questdb.BootstrapEnv;
import com.questdb.factory.ReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.RecordSource;

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
