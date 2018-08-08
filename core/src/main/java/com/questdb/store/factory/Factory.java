/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.store.factory;

import com.questdb.ex.*;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.mp.Job;
import com.questdb.mp.SynchronizedJob;
import com.questdb.std.Files;
import com.questdb.std.ObjHashSet;
import com.questdb.std.Os;
import com.questdb.std.ex.JournalException;
import com.questdb.std.str.Path;
import com.questdb.store.*;
import com.questdb.store.factory.configuration.JournalConfiguration;
import com.questdb.store.factory.configuration.JournalMetadata;
import com.questdb.store.factory.configuration.MetadataBuilder;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

public class Factory implements ReaderFactory, WriterFactory {
    private static final Log LOG = LogFactory.getLog(Factory.class);

    private final CachingWriterFactory writerFactory;
    private final CachingReaderFactory readerFactory;
    private final JournalConfiguration configuration;
    private final ConcurrentHashMap<String, JournalMetadata> metadataCache = new ConcurrentHashMap<>();
    private final WriterMaintenanceJob writerMaintenanceJob;
    private final ReaderMaintenanceJob readerMaintenanceJob;

    // used by tests only
    public Factory(JournalConfiguration configuration) {
        this(configuration, 0, 2, 0);
    }

    public Factory(JournalConfiguration configuration, long idleTimeoutMs, int maxSegments, long idleCheckIntervalMs) {
        this.writerFactory = new CachingWriterFactory(configuration, idleTimeoutMs);
        this.readerFactory = new CachingReaderFactory(configuration, idleTimeoutMs, maxSegments);
        this.configuration = configuration;
        this.writerMaintenanceJob = new WriterMaintenanceJob(idleCheckIntervalMs);
        this.readerMaintenanceJob = new ReaderMaintenanceJob(idleCheckIntervalMs);
    }

    public Factory(String databaseHome, long inactiveTtlMs, int readerCacheSegments, long idleCheckIntervalMs) {
        this.writerFactory = new CachingWriterFactory(databaseHome, inactiveTtlMs);
        this.readerFactory = new CachingReaderFactory(databaseHome, inactiveTtlMs, readerCacheSegments);
        this.configuration = this.readerFactory.getConfiguration();
        this.writerMaintenanceJob = new WriterMaintenanceJob(idleCheckIntervalMs);
        this.readerMaintenanceJob = new ReaderMaintenanceJob(idleCheckIntervalMs);
    }

    @Override
    public void close() {
        writerFactory.close();
        readerFactory.close();
    }

    @Override
    public JournalConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public final <T> Journal<T> reader(JournalKey<T> key) throws JournalException {
        return reader(getConfiguration().createMetadata(key));
    }

    @Override
    public final <T> Journal<T> reader(Class<T> clazz) throws JournalException {
        return reader(new JournalKey<>(clazz));
    }

    @Override
    public final <T> Journal<T> reader(Class<T> clazz, String name) throws JournalException {
        return reader(new JournalKey<>(clazz, name));
    }

    @Override
    @SuppressWarnings("unchecked")
    public final Journal reader(String name) throws JournalException {
        return reader(getMetadata(name));
    }

    @Override
    public final <T> Journal<T> reader(Class<T> clazz, String name, int recordHint) throws JournalException {
        return reader(new JournalKey<>(clazz, name, PartitionBy.DEFAULT, recordHint));
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
        writerFactory.releaseInactive();
        readerFactory.releaseInactive();
    }

    public void exportJobs(ObjHashSet<Job> jobs) {
        jobs.add(writerMaintenanceJob);
        jobs.add(readerMaintenanceJob);
    }

    public int getBusyReaderCount() {
        return readerFactory.getBusyCount();
    }

    public int getBusyWriterCount() {
        return writerFactory.getBusyCount();
    }

    public FactoryEventListener getEventListener() {
        return this.writerFactory.getEventListener();
    }

    public void setEventListener(FactoryEventListener eventListener) {
        this.writerFactory.setEventListener(eventListener);
        this.readerFactory.setEventListener(eventListener);
    }

    public JournalMetadata getMetadata(String name) throws JournalException {
        JournalMetadata metadata = metadataCache.get(name);
        if (metadata != null) {
            return metadata;
        }

        metadata = configuration.readMetadata(name);
        JournalMetadata other = metadataCache.putIfAbsent(name, metadata);
        if (other != null) {
            return other;
        }

        return metadata;
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
        return writer(new JournalKey<>(clazz));
    }

    @Override
    public <T> JournalWriter<T> writer(Class<T> clazz, String name) throws JournalException {
        return writer(new JournalKey<>(clazz, name));
    }

    @Override
    public <T> JournalWriter<T> writer(Class<T> clazz, String name, int recordHint) throws JournalException {
        return writer(new JournalKey<>(clazz, name, PartitionBy.DEFAULT, recordHint));
    }

    @Override
    @SuppressWarnings("unchecked")
    public JournalWriter writer(String name) throws JournalException {
        return writer(getMetadata(name));
    }

    @Override
    public <T> JournalWriter<T> writer(JournalKey<T> key) throws JournalException {
        return writer(getConfiguration().createMetadata(key));
    }

    @Override
    public <T> JournalWriter<T> writer(MetadataBuilder<T> metadataBuilder) throws JournalException {
        return writer(metadataBuilder.build());
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
            metadataCache.remove(name);
            com.questdb.store.Files.deleteOrException(l);
        } finally {
            LockManager.release(lock);
        }

    }

    private void rename0(CharSequence from, CharSequence to) throws JournalException {
        try (Path oldName = new Path()) {
            try (Path newName = new Path()) {
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

                String oname = oldName.toString();

                Lock lock = LockManager.lockExclusive(oname);
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

                        metadataCache.remove(oname);

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

    private static abstract class PeriodicSynchronizedJob extends SynchronizedJob {
        private final long checkInterval;
        private long last = 0;

        public PeriodicSynchronizedJob(long checkInterval) {
            this.checkInterval = checkInterval;
        }

        abstract boolean doRun();

        @Override
        protected boolean runSerially() {
            long t = System.currentTimeMillis();
            if (last + checkInterval < t) {
                last = t;
                return doRun();
            }
            return false;
        }
    }

    private class WriterMaintenanceJob extends PeriodicSynchronizedJob {

        public WriterMaintenanceJob(long checkInterval) {
            super(checkInterval);
        }

        @Override
        protected boolean doRun() {
            return writerFactory.releaseInactive();
        }
    }

    private class ReaderMaintenanceJob extends PeriodicSynchronizedJob {

        public ReaderMaintenanceJob(long checkInterval) {
            super(checkInterval);
        }

        @Override
        protected boolean doRun() {
            return readerFactory.releaseInactive();
        }
    }
}
