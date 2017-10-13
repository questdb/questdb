/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.cairo.pool;

import com.questdb.cairo.CairoError;
import com.questdb.cairo.CairoException;
import com.questdb.cairo.FilesFacade;
import com.questdb.cairo.TableWriter;
import com.questdb.cairo.pool.ex.EntryLockedException;
import com.questdb.cairo.pool.ex.EntryUnavailableException;
import com.questdb.cairo.pool.ex.PoolClosedException;
import com.questdb.factory.FactoryConstants;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Unsafe;
import com.questdb.std.Sinkable;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class maintains cache of open writers to avoid OS overhead of
 * opening and closing files. While doing so it abides by the the same
 * rule as non-pooled writers: there can only be one TableWriter for any given name.
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
public class WriterPool extends AbstractPool {

    private static final Log LOG = LogFactory.getLog(WriterPool.class);

    private final static long ENTRY_OWNER;
    private final ConcurrentHashMap<CharSequence, Entry> entries = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    public WriterPool(FilesFacade ff, CharSequence root, long inactiveTtl) {
        super(ff, root, inactiveTtl);
        notifyListener(Thread.currentThread().getId(), null, PoolListener.EV_POOL_OPEN);
    }

    public void close() {
        if (closed) {
            return;
        }
        LOG.info().$("closing pool").$();

        closed = true;
        releaseAll(Long.MAX_VALUE);
        notifyListener(Thread.currentThread().getId(), null, PoolListener.EV_POOL_CLOSED);
        LOG.info().$("pool closed").$();
    }

    public int getBusyCount() {
        int count = 0;
        for (Entry e : entries.values()) {
            if (e.owner != UNALLOCATED) {
                count++;
            }
        }
        return count;
    }

    public void lock(CharSequence name) {

        checkClosed();

        long thread = Thread.currentThread().getId();

        Entry e = entries.get(name);
        if (e == null) {
            // We are racing to create new writer!
            e = new Entry();
            Entry other = entries.putIfAbsent(name, e);
            if (other == null) {
                notifyListener(thread, name, PoolListener.EV_LOCK_SUCCESS);
                e.locked = true;
                return;
            } else {
                e = other;
            }
        }

        // try to change owner
        if ((Unsafe.getUnsafe().compareAndSwapLong(e, ENTRY_OWNER, UNALLOCATED, thread)
                || Unsafe.getUnsafe().compareAndSwapLong(e, ENTRY_OWNER, thread, thread))) {
            LOG.info().$("locked '").$(name).$("' [thread=").$(thread).$(']').$();
            closeWriter(thread, e, PoolListener.EV_LOCK_CLOSE, FactoryConstants.CR_NAME_LOCK);
            e.locked = true;
            notifyListener(thread, name, PoolListener.EV_LOCK_SUCCESS);
            return;
        }

        LOG.error().$("cannot lock '").$(name).$("' [owner=").$(e.owner).$(", thread=").$(thread).$();
        notifyListener(thread, name, PoolListener.EV_LOCK_BUSY);
        throw EntryUnavailableException.INSTANCE;
    }

    public int size() {
        return entries.size();
    }

    public void unlock(CharSequence name) {
        long thread = Thread.currentThread().getId();

        Entry e = entries.get(name);
        if (e == null) {
            notifyListener(thread, name, PoolListener.EV_NOT_LOCKED);
            return;
        }

        // When entry is locked, writer must be null,
        // however if writer is not null, calling thread must be trying to unlock
        // writer that hasn't been locked. This qualifies for "illegal state"
        if (e.owner == thread) {

            if (e.writer != null) {
                notifyListener(thread, name, PoolListener.EV_NOT_LOCKED);
                throw new IllegalStateException("Writer " + name + " is not locked");
            }

            // unlock must remove entry because pool does not deal with null writer
            entries.remove(name);
        }
        notifyListener(thread, name, PoolListener.EV_UNLOCKED);
    }

    public TableWriter writer(CharSequence name) {

        checkClosed();

        long thread = Thread.currentThread().getId();

        Entry e = entries.get(name);
        if (e == null) {
            // We are racing to create new writer!
            e = new Entry();
            Entry other = entries.putIfAbsent(name, e);
            if (other == null) {
                // race won
                return createWriter(name, e);
            } else {
                LOG.info().$("Thread ").$(e.owner).$(" lost race to allocate '").$(name).$('\'').$();
                e = other;
            }
        }

        long owner = e.owner;
        // try to change owner
        if (Unsafe.getUnsafe().compareAndSwapLong(e, ENTRY_OWNER, UNALLOCATED, thread)) {
            // in a extreme race condition it is possible that e.writer will be null
            // in this case behaviour should be identical to entry missing entirely
            if (e.writer == null) {
                return createWriter(name, e);
            }

            if (closed) {
                // pool closed but we somehow managed to lock writer
                // make sure that interceptor cleared to allow calling thread close writer normally
                e.writer.entry = null;
            }
            return logAndReturn(e, PoolListener.EV_GET);
        } else {
            if (e.owner == thread) {

                if (e.locked) {
                    throw EntryLockedException.INSTANCE;
                }

                if (e.ex != null) {
                    notifyListener(thread, name, PoolListener.EV_EX_RESEND);
                    // this writer failed to allocate by this very thread
                    // ensure consistent response
                    throw e.ex;
                }

                if (closed) {
                    LOG.info().$("Writer '").$(name).$("' is detached").$();
                    e.writer.entry = null;
                }
                return logAndReturn(e, PoolListener.EV_GET);
            }
            LOG.error().$('\'').$(name).$("' is busy [owner=").$(owner).$(']').$();
            throw EntryUnavailableException.INSTANCE;
        }
    }

    private void checkClosed() {
        if (closed) {
            LOG.info().$("is down").$();
            throw PoolClosedException.INSTANCE;
        }
    }

    private void closeWriter(long thread, Entry e, short ev, int reason) {
        PooledTableWriter w = e.writer;
        if (w != null) {
            CharSequence name = e.writer.getName();
            w.entry = null;
            w.close();
            e.writer = null;
            LOG.info().$("closed '").$(name).$("' [reason=").$(FactoryConstants.closeReasonText(reason)).$(", by=").$(thread).$(']').$();
            notifyListener(thread, name, ev);
        }
    }

    int countFreeWriters() {
        int count = 0;
        for (Entry e : entries.values()) {
            if (e.owner == UNALLOCATED) {
                count++;
            } else {
                LOG.info().$("'").$(e.writer.getName()).$("' is still busy [owner=").$(e.owner).$(']').$();
            }
        }

        return count;
    }

    private PooledTableWriter createWriter(CharSequence name, Entry e) {
        try {
            checkClosed();
            e.writer = new PooledTableWriter(this, e, name);
            return logAndReturn(e, PoolListener.EV_CREATE);
        } catch (CairoException ex) {
            LOG.error().$("failed to allocate writer '").$(name).$("' in thread ").$(e.owner).$(": ").$((Sinkable) ex).$();
            e.ex = ex;
            notifyListener(e.owner, name, PoolListener.EV_CREATE_EX);
            throw ex;
        }
    }

    private PooledTableWriter logAndReturn(Entry e, short event) {
        LOG.info().$('\'').$(e.writer.getName()).$("' is assigned [thread=").$(e.owner).$(']').$();
        notifyListener(e.owner, e.writer.getName(), event);
        return e.writer;
    }

    private void notifyListener(long thread, CharSequence name, short event) {
        PoolListener listener = getPoolListener();
        if (listener != null) {
            listener.onEvent(PoolListener.SRC_WRITER, thread, name, event, (short) 0, (short) 0);
        }
    }

    @Override
    protected boolean releaseAll(long deadline) {
        long thread = Thread.currentThread().getId();
        boolean removed = false;
        final int reason;

        if (deadline == Long.MAX_VALUE) {
            reason = FactoryConstants.CR_POOL_CLOSE;
        } else {
            reason = FactoryConstants.CR_IDLE;
        }

        Iterator<Entry> iterator = entries.values().iterator();
        while (iterator.hasNext()) {
            Entry e = iterator.next();
            // lastReleaseTime is volatile, which makes
            // order of conditions important
            if ((deadline > e.lastReleaseTime && e.owner == UNALLOCATED)) {
                // looks like this one can be released
                // try to lock it
                if (Unsafe.getUnsafe().compareAndSwapLong(e, ENTRY_OWNER, UNALLOCATED, thread)) {
                    // lock successful
                    closeWriter(thread, e, PoolListener.EV_EXPIRE, reason);
                    iterator.remove();
                    removed = true;
                    Unsafe.getUnsafe().putOrderedLong(e, ENTRY_OWNER, UNALLOCATED);
                }
            } else if (e.ex != null) {
                LOG.info().$("Removing entry for failed to allocate writer").$();
                iterator.remove();
                removed = true;
            }
        }
        return removed;
    }

    private boolean returnToPool(Entry e) {
        CharSequence name = e.writer.getName();
        long thread = Thread.currentThread().getId();
        if (e.owner != UNALLOCATED) {
            LOG.info().$('\'').$(name).$(" is back [thread=").$(thread).$(']').$();
            e.lastReleaseTime = System.currentTimeMillis();

            if (closed) {
                LOG.info().$("allowing '").$(name).$("' to close [thread=").$(e.owner).$(']').$();
                e.writer.entry = null;
                e.writer = null;
                entries.remove(name);
                notifyListener(thread, name, PoolListener.EV_OUT_OF_POOL_CLOSE);
                return false;
            }

            e.owner = UNALLOCATED;
            notifyListener(thread, name, PoolListener.EV_RETURN);
        } else {
            LOG.error().$('\'').$(name).$("' has no owner").$();
            notifyListener(thread, name, PoolListener.EV_UNEXPECTED_CLOSE);
        }
        return true;
    }

    private static class Entry {
        // owner thread id or -1 if writer is available for hire
        private long owner = Thread.currentThread().getId();
        private PooledTableWriter writer;
        // time writer was last released
        private volatile long lastReleaseTime = System.currentTimeMillis();
        private CairoException ex = null;
        private volatile boolean locked = false;
    }

    private static class PooledTableWriter extends TableWriter {
        private final WriterPool pool;
        private Entry entry;

        public PooledTableWriter(WriterPool pool, Entry e, CharSequence name) {
            super(pool.ff, pool.root, name);
            this.pool = pool;
            this.entry = e;
        }

        @Override
        public void close() {
            if (entry != null && pool != null && pool.returnToPool(entry)) {
                return;
            }
            super.close();
        }
    }

    static {
        try {
            Field f = Entry.class.getDeclaredField("owner");
            ENTRY_OWNER = Unsafe.getUnsafe().objectFieldOffset(f);
        } catch (NoSuchFieldException e) {
            throw new CairoError("Cannot initialize class", e);
        }
    }
}
