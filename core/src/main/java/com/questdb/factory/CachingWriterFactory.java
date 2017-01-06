/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.factory;

import com.questdb.Journal;
import com.questdb.JournalWriter;
import com.questdb.ex.JournalException;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Unsafe;
import com.questdb.mp.Job;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class maintains cache of open writers to avoid OS overhead of
 * opening and closing files. While doing so it abides by the the same
 * rule as non-cached writers: journal is only allowed single writer.
 * <p>
 * This implementation is thread-safe. Writer allocated by one thread
 * cannot be used by any other threads until it is released. This factory
 * will be returning NULL when writer is already in use and cached
 * instance of writer otherwise. Writers are released back to pool via
 * standard writer.close() call.
 * <p>
 * Writers that have been idle for some time can be expunged from pool
 * by calling Job.run() method asynchronously. Pool implementation is
 * guaranteeing thread-safety of this method at all times.
 * <p>
 * This factory can be closed via close() call. This method is also
 * thread-safe and is guarantying that all open writers will be eventually
 * closed.
 */
public class CachingWriterFactory extends WriterFactoryImpl implements JournalCloseInterceptor, Job {

    private static final Log LOG = LogFactory.getLog(CachingWriterFactory.class);

    private final static long ENTRY_OWNER;
    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final long inactiveTtl;
    private volatile boolean closed = false;

    public CachingWriterFactory(String databaseHome, long inactiveTtl) {
        super(databaseHome);
        this.inactiveTtl = inactiveTtl;
    }

    public CachingWriterFactory(JournalConfiguration configuration, long inactiveTtl) {
        super(configuration);
        this.inactiveTtl = inactiveTtl;
    }

    @Override
    public boolean canClose(Journal journal) {
        String name = journal.getName();
        Entry e = entries.get(name);
        if (e != null) {
            long threadId = Thread.currentThread().getId();
            if (Unsafe.getUnsafe().compareAndSwapLong(e, ENTRY_OWNER, threadId, -1L)) {

                if (closed) {
                    // pool is closed and one of two things could have happened:
                    // 1. writer was picked up by thread that called close() method
                    // 2. writer was missed by close() thread because it had this thread as the owner
                    // to ensure desired outcome, which is closing of writer, this thread
                    // will attempt to lock writer again and if successful - close it by returning 'true'
                    // if lock isn't successful it will mean that responsibility to close writer is
                    // with new owner
                    if (Unsafe.getUnsafe().compareAndSwapLong(e, ENTRY_OWNER, -1L, threadId)) {
                        LOG.info().$("Closing writer '").$(name).$('\'').$();
                        e.writer = null;
                        return true;
                    }
                }

                LOG.info().$("Writer '").$(name).$(" is back in pool").$();
                e.lastReleaseTime = System.currentTimeMillis();
            } else {
                LOG.error().$("Writer '").$(name).$("' is owned by thread ").$(e.owner).$();
            }
        } else {
            LOG.error().$("Writer '").$(name).$("' is not managed by this pool").$();
            return true;
        }

        return false;
    }

    @Override
    public void close() {
        closed = true;
        releaseAll(Long.MAX_VALUE);
    }

    public int countFreeWriters() {
        int count = 0;
        for (Map.Entry<String, Entry> me : entries.entrySet()) {
            if (me.getValue().owner == -1L) {
                count++;
            }
        }

        return count;
    }

    @Override
    public boolean run() {
        return releaseAll(System.currentTimeMillis() - inactiveTtl);
    }

    @Override
    public void setupThread() {
    }

    public int size() {
        return entries.size();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> JournalWriter<T> writer(JournalMetadata<T> metadata) throws JournalException {

        if (closed) {
            LOG.info().$("Pool is closed").$();
            return null;
        }

        final String path = metadata.getKey().getName();

        Entry e = entries.get(path);
        if (e == null) {
            // We are racing to create new writer!
            e = new Entry();
            if (entries.putIfAbsent(path, e) == null) {
                // race won

                try {
                    e.writer = super.writer(metadata);

                    if (!closed) {
                        e.writer.setCloseInterceptor(this);
                    }

                    LOG.info().$("Writer '").$(path).$("' is allocated by thread ").$(e.owner).$();
                    return e.writer;
                } catch (JournalException ex) {
                    LOG.error().$("Failed to allocate writer '").$(path).$("' in thread ").$(e.owner).$(": ").$(ex).$();
                    e.allocationFailure = true;
                    return null;
                }
            } else {
                LOG.info().$("Thread ").$(e.owner).$(" lost race to allocate writer '").$(path).$('\'').$();
                // discard object
                e = null;
            }
        }

        // race lost or was a non-starter
        if (e == null) {
            e = entries.get(path);
        }

        long threadId = Thread.currentThread().getId();

        // try to change owner
        if (e != null && Unsafe.getUnsafe().compareAndSwapLong(e, ENTRY_OWNER, -1L, threadId)) {
            LOG.debug().$("Thread ").$(e.owner).$(" allocated writer ").$(path).$();
            if (closed) {
                // pool closed but we somehow managed to lock writer
                // make sure that interceptor cleared to allow calling thread close writer normally
                e.writer.setCloseInterceptor(null);
            }
            return e.writer;
        } else {
            if (e == null) {
                LOG.error().$("Writer '").$(path).$("' is not managed by this pool. Internal error?").$();
            } else {
                if (e.owner == threadId) {
                    if (e.allocationFailure) {
                        // this writer failed to allocate by this very thread
                        // ensure consistent response
                        return null;
                    }

                    if (closed) {
                        LOG.info().$("Writer '").$(path).$("' is detached").$();
                        e.writer.setCloseInterceptor(null);
                    }
                    return e.writer;
                }
                LOG.error().$("Writer '").$(path).$("' is already owned by thread ").$(e.owner).$();
            }
        }

        return null;
    }

    private boolean releaseAll(long deadline) {
        long threadId = Thread.currentThread().getId();
        boolean removed = false;

        LOG.info().$("done?").$();
        Iterator<Map.Entry<String, Entry>> iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Entry> me = iterator.next();
            Entry e = me.getValue();
            // lastReleaseTime is volatile, which makes
            // order of conditions important
            if ((deadline > e.lastReleaseTime && e.owner == -1)) {
                // looks like this one can be expired
                // try to lock it
                if (Unsafe.getUnsafe().compareAndSwapLong(e, ENTRY_OWNER, -1L, threadId)) {
                    // lock successful
                    LOG.info().$("Closing writer '").$(me.getKey()).$('\'').$();
                    e.writer.setCloseInterceptor(null);
                    try {
                        e.writer.close();
                    } catch (Throwable e1) {
                        LOG.error().$("Cannot close writer '").$(e.writer.getName()).$("': ").$(e1.getMessage()).$();
                    }
                    iterator.remove();
                    removed = true;
                    Unsafe.getUnsafe().putOrderedLong(e, ENTRY_OWNER, -1L);
                }
            } else if (e.allocationFailure) {
                LOG.info().$("Removing entry for failed to allocate writer '").$(me.getKey()).$('\'').$();
                iterator.remove();
                removed = true;
            }
        }

        return removed;
    }

    private static class Entry {
        // owner thread id or -1 if writer is available for hire
        private final long owner = Thread.currentThread().getId();
        private JournalWriter writer;
        // time writer was last released
        private volatile long lastReleaseTime = System.currentTimeMillis();
        private boolean allocationFailure = false;
    }

    static {
        try {
            Field f = Entry.class.getDeclaredField("owner");
            ENTRY_OWNER = Unsafe.getUnsafe().objectFieldOffset(f);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("Cannot initialize class", e);
        }
    }
}
