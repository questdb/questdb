package com.questdb.factory;

import com.questdb.Journal;
import com.questdb.JournalKey;
import com.questdb.JournalWriter;
import com.questdb.ex.*;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.factory.configuration.MetadataBuilder;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Files;
import com.questdb.misc.Os;
import com.questdb.std.str.CompositePath;
import com.questdb.store.Lock;
import com.questdb.store.LockManager;

import java.io.File;

public class Factory implements ReaderFactory, WriterFactory {
    private static final Log LOG = LogFactory.getLog(Factory.class);

    private final CachingWriterFactory writerFactory;
    private final CachingReaderFactory readerFactory;

    public Factory(JournalConfiguration configuration, long writerInactiveTTL, int readerCacheSegments) {
        this.writerFactory = new CachingWriterFactory(configuration, writerInactiveTTL);
        this.readerFactory = new CachingReaderFactory(configuration, readerCacheSegments);
    }

    public Factory(String databaseHome, long writerInactiveTTL, int readerCacheSegments) {
        this.writerFactory = new CachingWriterFactory(databaseHome, writerInactiveTTL);
        this.readerFactory = new CachingReaderFactory(databaseHome, readerCacheSegments);
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
    public <T> Journal<T> reader(Class<T> clazz) throws JournalException {
        return readerFactory.reader(clazz);
    }

    @Override
    public <T> Journal<T> reader(Class<T> clazz, String name) throws JournalException {
        return readerFactory.reader(clazz, name);
    }

    @Override
    public Journal reader(String name) throws JournalException {
        return readerFactory.reader(name);
    }

    @Override
    public <T> Journal<T> reader(Class<T> clazz, String name, int recordHint) throws JournalException {
        return readerFactory.reader(clazz, name, recordHint);
    }

    @Override
    public <T> Journal<T> reader(JournalMetadata<T> metadata) throws JournalException {
        return readerFactory.reader(metadata);
    }

    public void delete(String name) throws JournalException {
        lock(name);
        try {
            delete0(name);
        } finally {
            unlock(name);
        }
    }

    public void expire() {
        writerFactory.run();
    }

    public int getBusyReaderCount() {
        return readerFactory.getBusyCount();
    }

    public int getBusyWriterCount() {
        return writerFactory.getBusyCount();
    }

    public void lock(String name) throws JournalException {
        writerFactory.lock(name);
        try {
            readerFactory.lock(name);
        } catch (JournalException e) {
            writerFactory.unlock(name);
            throw e;
        }
    }

    public void rename(String from, String to) throws JournalException {
        lock(from);
        try {
            rename0(from, to);
        } finally {
            unlock(from);
        }
    }

    public void unlock(String name) {
        readerFactory.unlock(name);
        writerFactory.unlock(name);
    }

    @Override
    public <T> JournalWriter<T> writer(Class<T> clazz) throws JournalException {
        return writerFactory.writer(clazz);
    }

    @Override
    public <T> JournalWriter<T> writer(Class<T> clazz, String name) throws JournalException {
        return writerFactory.writer(clazz, name);
    }

    @Override
    public <T> JournalWriter<T> writer(Class<T> clazz, String name, int recordHint) throws JournalException {
        return writerFactory.writer(clazz, name, recordHint);
    }

    @Override
    public JournalWriter writer(String name) throws JournalException {
        return writerFactory.writer(name);
    }

    @Override
    public <T> JournalWriter<T> writer(JournalKey<T> key) throws JournalException {
        return writerFactory.writer(key);
    }

    @Override
    public <T> JournalWriter<T> writer(MetadataBuilder<T> metadataBuilder) throws JournalException {
        return writerFactory.writer(metadataBuilder);
    }

    @Override
    public <T> JournalWriter<T> writer(JournalMetadata<T> metadata) throws JournalException {
        return writerFactory.writer(metadata);
    }

    private void delete0(String name) throws JournalException {
        File l = new File(getConfiguration().getJournalBase(), name);
        Lock lock = LockManager.lockExclusive(l.getAbsolutePath());
        try {
            if (lock == null || !lock.isValid()) {
                LOG.error().$("Cannot obtain lock on ").$(l).$();
                throw JournalWriterAlreadyOpenException.INSTANCE;
            }
            Files.deleteOrException(l);
        } finally {
            LockManager.release(lock);
        }

    }

    private void rename0(CharSequence from, CharSequence to) throws JournalException {
        try (CompositePath oldName = new CompositePath()) {
            try (CompositePath newName = new CompositePath()) {
                String path = getConfiguration().getJournalBase().getAbsolutePath();

                oldName.of(path).concat(from).$();
                newName.of(path).concat(to).$();

                if (!Files.exists(oldName)) {
                    LOG.error().$("Journal does not exist: ").$(oldName).$();
                    throw JournalDoesNotExistException.INSTANCE;
                }

                if (Os.type == Os.WINDOWS) {
                    oldName.of("\\\\?\\").concat(path).concat(from).$();
                    newName.of("\\\\?\\").concat(path).concat(to).$();
                }


                Lock lock = LockManager.lockExclusive(oldName.toString());
                try {
                    if (lock == null || !lock.isValid()) {
                        LOG.error().$("Cannot obtain lock on ").$(oldName).$();
                        throw JournalWriterAlreadyOpenException.INSTANCE;
                    }

                    if (Files.exists(newName)) {
                        throw JournalExistsException.INSTANCE;
                    }


                    Lock writeLock = LockManager.lockExclusive(newName.toString());
                    try {

                        // this should not happen because we checked for existence before
                        if (writeLock == null || !writeLock.isValid()) {
                            LOG.error().$("Cannot obtain lock on ").$(newName).$();
                            throw FactoryInternalException.INSTANCE;
                        }

                        if (!Files.rename(oldName, newName)) {
                            LOG.error().$("Cannot rename ").$(oldName).$(" to ").$(newName).$(": ").$(Os.errno()).$();
                            throw SystemException.INSTANCE;
                        }
                    } finally {
                        LockManager.release(writeLock);
                    }
                } finally {
                    LockManager.release(lock);
                }
            }
        }
    }
}
