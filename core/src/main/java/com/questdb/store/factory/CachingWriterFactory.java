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
import com.questdb.std.Unsafe;
import com.questdb.std.ex.JournalException;
import com.questdb.store.Journal;
import com.questdb.store.JournalRuntimeException;
import com.questdb.store.JournalWriter;
import com.questdb.store.PoolConstants;
import com.questdb.store.factory.configuration.JournalConfiguration;
import com.questdb.store.factory.configuration.JournalMetadata;

import java.io.File;
import java.util.Iterator;
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
public class CachingWriterFactory extends AbstractFactory implements JournalCloseInterceptor {

    private static final Log LOG = LogFactory.getLog(CachingWriterFactory.class);

    private final static long ENTRY_OWNER = Unsafe.getFieldOffset(Entry.class, "owner");
    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    public CachingWriterFactory(String databaseHome, long inactiveTtl) {
        super(databaseHome, inactiveTtl);
        notifyListener(Thread.currentThread().getId(), null, FactoryEventListener.EV_POOL_OPEN);
    }

    public CachingWriterFactory(JournalConfiguration configuration, long inactiveTtl) {
        super(configuration, inactiveTtl);
        notifyListener(Thread.currentThread().getId(), null, FactoryEventListener.EV_POOL_OPEN);
    }

    @Override
    public boolean canClose(Journal journal) {
        String name = journal.getName();
        Entry e = entries.get(name);
        long thread = Thread.currentThread().getId();
        if (e != null) {
            if (e.owner != -1) {

                if (e.writer.isCommitOnClose()) {
                    try {
                        e.writer.commit();
                    } catch (JournalException ex) {
                        notifyListener(thread, name, FactoryEventListener.EV_COMMIT_EX);
                        throw new JournalRuntimeException(ex);
                    }
                }

                LOG.info().$("Writer '").$(name).$(" is back in pool").$();
                e.lastReleaseTime = System.currentTimeMillis();

                if (closed || e.writer.isInError()) {
                    LOG.info().$("Closing writer '").$(name).$('\'').$();
                    e.writer.setCloseInterceptor(null);
                    e.writer = null;
                    entries.remove(name);
                    notifyListener(thread, name, FactoryEventListener.EV_OUT_OF_POOL_CLOSE);
                    return true;
                }

                e.owner = -1L;
                notifyListener(thread, name, FactoryEventListener.EV_RETURN);
            } else {
                LOG.error().$("Writer '").$(name).$("' is not allocated ").$(e.owner).$();
                notifyListener(thread, name, FactoryEventListener.EV_UNEXPECTED_CLOSE);
            }
        } else {
            LOG.error().$("Writer '").$(name).$("' is not managed by this pool").$();
            journal.setCloseInterceptor(null);
            notifyListener(thread, name, FactoryEventListener.EV_NOT_IN_POOL);
            return true;
        }

        return false;
    }

    @Override
    public void close() {
        closed = true;
        releaseAll(Long.MAX_VALUE);
        notifyListener(Thread.currentThread().getId(), null, FactoryEventListener.EV_POOL_CLOSED);
    }

    public void lock(String name) throws JournalException {
        if (closed) {
            LOG.info().$("Pool is closed").$();
            throw FactoryClosedException.INSTANCE;
        }

        long thread = Thread.currentThread().getId();

        Entry e = entries.get(name);
        if (e == null) {
            // We are racing to create new writer!
            e = new Entry();
            if (entries.putIfAbsent(name, e) == null) {
                notifyListener(thread, name, FactoryEventListener.EV_LOCK_SUCCESS);
                e.locked = true;
                return;
            } else {
                e = entries.get(name);
            }
        }

        // try to change owner

        if (e != null) {
            if ((Unsafe.cas(e, ENTRY_OWNER, -1L, thread) || Unsafe.cas(e, ENTRY_OWNER, thread, thread))) {
                LOG.info().$("Thread ").$(e.owner).$(" locked writer ").$(name).$();
                closeWriter(thread, e, FactoryEventListener.EV_LOCK_CLOSE, FactoryEventListener.EV_LOCK_CLOSE_EX, PoolConstants.CR_NAME_LOCK);
                e.locked = true;
                notifyListener(thread, name, FactoryEventListener.EV_LOCK_SUCCESS);
                return;
            } else {
                LOG.error().$("Writer '").$(name).$("' is owned by thread ").$(e.owner).$();
            }
        }
        notifyListener(thread, name, FactoryEventListener.EV_LOCK_BUSY);
        throw WriterBusyException.INSTANCE;
    }

    public int getBusyCount() {

        int count = 0;
        for (Entry e : entries.values()) {
            if (e.owner != -1) {
                count++;
            }
        }

        return count;
    }

    private JournalWriter checkAndReturn(long thread, Entry e, String name, JournalMetadata<?> metadata) throws JournalException {
        JournalMetadata wm = e.writer.getMetadata();
        if (metadata.isCompatible(wm, false)) {
            if (metadata.getModelClass() != null && wm.getModelClass() == null) {
                closeWriter(thread, e, FactoryEventListener.EV_CLOSE, FactoryEventListener.EV_CLOSE_EX, PoolConstants.CR_REOPEN);
                return createWriter(thread, name, e, metadata);
            } else {
                LOG.info().$("Writer '").$(name).$("' is assigned to thread ").$(thread).$();
                notifyListener(thread, name, FactoryEventListener.EV_GET);
                return e.writer;
            }
        }

        JournalMetadataException ex = new JournalMetadataException(wm, metadata);
        notifyListener(thread, name, FactoryEventListener.EV_INCOMPATIBLE);

        if (closed) {
            closeWriter(thread, e, FactoryEventListener.EV_CLOSE, FactoryEventListener.EV_CLOSE_EX, PoolConstants.CR_POOL_CLOSE);
        }

        e.owner = -1L;
        throw ex;
    }

    public int size() {
        return entries.size();
    }

    public void unlock(String name) {
        long thread = Thread.currentThread().getId();

        Entry e = entries.get(name);
        if (e == null) {
            notifyListener(thread, name, FactoryEventListener.EV_NOT_LOCKED);
            return;
        }

        // When entry is locked, writer must be null,
        // however if writer is not null, calling thread must be trying to unlock
        // writer that hasn't been locked. This qualifies for "illegal state"
        if (e.owner == thread) {

            if (e.writer != null) {
                notifyListener(thread, name, FactoryEventListener.EV_NOT_LOCKED);
                throw new IllegalStateException("Writer " + name + " is not locked");
            }

            // unlock must remove entry because pool does not deal with null writer
            entries.remove(name);
        }
        notifyListener(thread, name, FactoryEventListener.EV_UNLOCKED);
    }

    private void closeWriter(long thread, Entry e, short ev, short evex, int reason) {
        JournalWriter w = e.writer;
        if (w != null) {
            String name = e.writer.getName();
            w.setCloseInterceptor(null);
            try {
                w.close();
                e.writer = null;
                LOG.info().$("Closed writer '").$(name).$('\'').$(PoolConstants.closeReasonText(reason)).$();
                notifyListener(thread, name, ev);
            } catch (Throwable e1) {
                notifyListener(thread, name, evex);
                LOG.error().$("Cannot close writer '").$(w.getName()).$("': ").$(e1.getMessage()).$();
            }
        }
    }

    @Override
    protected boolean releaseAll(long deadline) {
        long thread = Thread.currentThread().getId();
        boolean removed = false;
        final int reason = deadline == Long.MAX_VALUE ? PoolConstants.CR_POOL_CLOSE : PoolConstants.CR_IDLE;

        Iterator<Entry> iterator = entries.values().iterator();
        while (iterator.hasNext()) {
            Entry e = iterator.next();
            // lastReleaseTime is volatile, which makes
            // order of conditions important
            if ((deadline > e.lastReleaseTime && e.owner == -1)) {
                // looks like this one can be released
                // try to lock it
                if (Unsafe.cas(e, ENTRY_OWNER, -1L, thread)) {
                    // lock successful
                    closeWriter(thread, e, FactoryEventListener.EV_EXPIRE, FactoryEventListener.EV_EXPIRE_EX, reason);
                    iterator.remove();
                    removed = true;
                    Unsafe.getUnsafe().putOrderedLong(e, ENTRY_OWNER, -1L);
                }
            } else if (e.ex != null) {
                LOG.info().$("Removing entry for failed to allocate writer").$();
                iterator.remove();
                removed = true;
            }
        }

        return removed;
    }

    int countFreeWriters() {
        int count = 0;
        for (Entry e : entries.values()) {
            if (e.owner == -1L) {
                count++;
            } else {
                LOG.info().$("Writer '").$(e.writer.getName()).$("' is still owned by ").$(e.owner).$();
            }
        }

        return count;
    }

    @SuppressWarnings("unchecked")
    private <T> JournalWriter<T> createWriter(long thread, String name, Entry e, JournalMetadata<T> metadata) throws JournalException {
        try {
            JournalMetadata<T> mo = getConfiguration().readMetadata(metadata.getName());
            if (mo != null && !mo.isCompatible(metadata, false)) {
                throw new JournalMetadataException(mo, metadata);
            }

            if (closed) {
                throw FactoryClosedException.INSTANCE;
            }

            JournalWriter<T> w = new JournalWriter<>(metadata, new File(getConfiguration().getJournalBase(), name));
            w.setCloseInterceptor(this);
            LOG.info().$("Writer '").$(name).$("' is allocated by thread ").$(e.owner).$();
            e.writer = w;
            notifyListener(thread, name, FactoryEventListener.EV_CREATE);
            return w;
        } catch (JournalException ex) {
            LOG.error().$("Failed to allocate writer '").$(name).$("' in thread ").$(e.owner).$(": ").$(ex).$();
            e.ex = ex;
            notifyListener(thread, name, FactoryEventListener.EV_CREATE_EX);
            throw ex;
        }
    }

    private void notifyListener(long thread, String name, short event) {
        FactoryEventListener listener = getEventListener();
        if (listener != null) {
            listener.onEvent(FactoryEventListener.SRC_WRITER, thread, name, event, (short) 0, (short) 0);
        }
    }

    @SuppressWarnings("unchecked")
    <T> JournalWriter<T> writer(JournalMetadata<T> metadata) throws JournalException {

        if (metadata.isPartialMapped()) {
            throw JournalPartiallyMappedException.INSTANCE;
        }

        if (closed) {
            LOG.info().$("Pool is closed").$();
            throw FactoryClosedException.INSTANCE;
        }

        final String name = metadata.getKey().getName();
        long thread = Thread.currentThread().getId();

        Entry e = entries.get(name);
        if (e == null) {
            // We are racing to create new writer!
            e = new Entry();
            if (entries.putIfAbsent(name, e) == null) {
                // race won
                return createWriter(thread, name, e, metadata);
            } else {
                LOG.info().$("Thread ").$(e.owner).$(" lost race to allocate writer '").$(name).$('\'').$();
                e = entries.get(name);
            }
        }


        // try to change owner
        if (e != null && Unsafe.cas(e, ENTRY_OWNER, -1L, thread)) {
            // in a race condition it is possible that e.writer will be null
            // in this case behaviour should be identical to entry missing entirely
            if (e.writer == null) {
                return createWriter(thread, name, e, metadata);
            }

            if (closed) {
                // pool closed but we somehow managed to lock writer
                // make sure that interceptor cleared to allow calling thread close writer normally
                e.writer.setCloseInterceptor(null);
            }
            return checkAndReturn(thread, e, name, metadata);
        } else {
            if (e == null) {
                LOG.error().$("Writer '").$(name).$("' is not managed by this pool. Internal error?").$();
                throw FactoryInternalException.INSTANCE;
            } else {
                long owner = e.owner;
                if (owner == thread) {

                    if (e.locked) {
                        throw JournalLockedException.INSTANCE;
                    }

                    if (e.ex != null) {
                        notifyListener(thread, name, FactoryEventListener.EV_EX_RESEND);
                        // this writer failed to allocate by this very thread
                        // ensure consistent response
                        throw e.ex;
                    }

                    if (closed) {
                        LOG.info().$("Writer '").$(name).$("' is detached").$();
                        e.writer.setCloseInterceptor(null);
                    }
                    return checkAndReturn(thread, e, name, metadata);
                }
                LOG.error().$("Writer '").$(name).$("' is already owned by thread ").$(owner).$();
                throw WriterBusyException.INSTANCE;
            }
        }
    }

    private static class Entry {
        // owner thread id or -1 if writer is available for hire
        private long owner = Thread.currentThread().getId();
        private JournalWriter writer;
        // time writer was last released
        private volatile long lastReleaseTime = System.currentTimeMillis();
        private JournalException ex = null;
        private volatile boolean locked = false;
    }
}
