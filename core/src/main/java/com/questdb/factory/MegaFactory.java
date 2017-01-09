package com.questdb.factory;

import com.questdb.Journal;
import com.questdb.JournalKey;
import com.questdb.JournalWriter;
import com.questdb.ex.JournalException;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.factory.configuration.MetadataBuilder;

public class MegaFactory implements ReaderFactory, WriterFactory {
    private final CachingWriterFactory writerFactory;
    private final CachingReaderFactory2 readerFactory;

    public MegaFactory(JournalConfiguration configuration, long writerInactiveTTL, int readerCacheSegments) {
        this.writerFactory = new CachingWriterFactory(configuration, writerInactiveTTL);
        this.readerFactory = new CachingReaderFactory2(configuration, readerCacheSegments);
    }

    @Override
    public void close() {
        writerFactory.close();
        readerFactory.close();
    }

    @Override
    public JournalConfiguration getConfiguration() {
        return readerFactory.getConfiguration();
    }

    @Override
    public <T> Journal<T> reader(JournalKey<T> key) throws JournalException {
        return readerFactory.reader(key);
    }

    @Override
    public <T> JournalWriter<T> writer(Class<T> clazz) throws JournalException {
        return writerFactory.writer(clazz);
    }

    @Override
    public <T> Journal<T> reader(Class<T> clazz) throws JournalException {
        return readerFactory.reader(clazz);
    }

    @Override
    public <T> JournalWriter<T> writer(Class<T> clazz, String name) throws JournalException {
        return writerFactory.writer(clazz, name);
    }

    @Override
    public <T> Journal<T> reader(Class<T> clazz, String name) throws JournalException {
        return readerFactory.reader(clazz, name);
    }

    @Override
    public <T> JournalWriter<T> writer(Class<T> clazz, String name, int recordHint) throws JournalException {
        return writerFactory.writer(clazz, name, recordHint);
    }

    @Override
    public Journal reader(String name) throws JournalException {
        return readerFactory.reader(name);
    }

    @Override
    public JournalWriter writer(String name) throws JournalException {
        return writerFactory.writer(name);
    }

    @Override
    public <T> Journal<T> reader(Class<T> clazz, String name, int recordHint) throws JournalException {
        return readerFactory.reader(clazz, name, recordHint);
    }

    @Override
    public <T> JournalWriter<T> writer(JournalKey<T> key) throws JournalException {
        return writerFactory.writer(key);
    }

    @Override
    public <T> Journal<T> reader(JournalMetadata<T> metadata) throws JournalException {
        return readerFactory.reader(metadata);
    }

    @Override
    public <T> JournalWriter<T> writer(MetadataBuilder<T> metadataBuilder) throws JournalException {
        return writerFactory.writer(metadataBuilder);
    }

    @Override
    public <T> JournalWriter<T> writer(JournalMetadata<T> metadata) throws JournalException {
        return writerFactory.writer(metadata);
    }

    public int rename(CharSequence from, CharSequence to) {
        return 0;
    }
}
