/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.pool;

import io.questdb.MessageBus;
import io.questdb.cairo.*;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

/**
 * This class maintains cache of open writers to avoid OS overhead of
 * opening and closing files. While doing so it abides by the the same
 * rule as non-pooled writers: there can only be one TableWriter instance
 * for any given name.
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
    static final String UNLOCKING = "unlocking";
    private final static long ENTRY_OWNER = Unsafe.getFieldOffset(Entry.class, "owner");
    private final ConcurrentHashMap<Entry> entries = new ConcurrentHashMap<>();
    private final CairoConfiguration configuration;
    private final Path path = new Path();
    private final MicrosecondClock clock;
    private final CharSequence root;
    @NotNull
    private final MessageBus messageBus;

    /**
     * Pool constructor. WriterPool root directory is passed via configuration.
     *
     * @param configuration configuration parameters.
     * @param messageBus    message bus instance to allow index tasks to be communicated to available threads.
     */
    public WriterPool(CairoConfiguration configuration, @NotNull MessageBus messageBus) {
        super(configuration, configuration.getInactiveWriterTTL());
        this.configuration = configuration;
        this.messageBus = messageBus;
        this.clock = configuration.getMicrosecondClock();
        this.root = configuration.getRoot();
        notifyListener(Thread.currentThread().getId(), null, PoolListener.EV_POOL_OPEN);
    }

    /**
     * <p>
     * Creates or retrieves existing TableWriter from pool. Because of TableWriter compliance with <b>single
     * writer model</b> pool ensures there is single TableWriter instance for given table name. Table name is unique in
     * context of <b>root</b> and pool instance covers single root.
     * </p>
     * When TableWriter from this pool is used by another thread @{@link EntryUnavailableException} is thrown and
     * when table is locked outside of pool, which includes same or different process, @{@link CairoException} instead.
     * In case of former application can retry getting writer from pool again at any time. When latter occurs application has
     * to call {@link #releaseAll(long)} before retrying for TableWriter.
     *
     * @param tableName name of the table
     * @return cached TableWriter instance.
     */
    public TableWriter get(CharSequence tableName, CharSequence lockReason) {
        assert null != lockReason;
        checkClosed();

        long thread = Thread.currentThread().getId();

        Entry e = entries.get(tableName);
        if (e == null) {
            // We are racing to create new writer!
            e = new Entry(clock.getTicks());
            Entry other = entries.putIfAbsent(tableName, e);
            if (other == null) {
                // race won
                return createWriter(tableName, e, thread, lockReason);
            } else {
                e = other;
            }
        }

        long owner = e.owner;
        // try to change owner
        if (Unsafe.cas(e, ENTRY_OWNER, UNALLOCATED, thread)) {
            // in an extreme race condition it is possible that e.writer will be null
            // in this case behaviour should be identical to entry missing entirely
            if (e.writer == null) {
                return createWriter(tableName, e, thread, lockReason);
            }
            return checkClosedAndGetWriter(tableName, e, lockReason);
        } else {
            if (e.owner == thread) {
                if (e.lockFd != -1L) {
                    throw EntryLockedException.instance(e.ownershipReason);
                }

                if (e.ex != null) {
                    notifyListener(thread, tableName, PoolListener.EV_EX_RESEND);
                    // this writer failed to allocate by this very thread
                    // ensure consistent response
                    entries.remove(tableName);
                    throw e.ex;
                }
            }
            LOG.error().$("busy [table=`").utf8(tableName).$("`, owner=").$(owner).$(']').$();
            throw EntryUnavailableException.instance(e.ownershipReason);
        }
    }

    public boolean exists(CharSequence tableName) {
        checkClosed();
        return entries.contains(tableName);
    }

    /**
     * Locks writer. Locking operation is always non-blocking. Lock is usually successful
     * when writer is in pool or owned by calling thread, in which case
     * writer instance is closed. Lock will also succeed when writer does not exist.
     * This will prevent from writer being created before it is unlocked.
     * <p>
     * Lock fails immediately with {@link EntryUnavailableException} when writer is used by another thread and with
     * {@link PoolClosedException} when pool is closed.
     * </p>
     * <p>
     * Lock is beneficial before table directory is renamed or deleted.
     * </p>
     *
     * @param tableName table name
     * @return true if lock was successful, false otherwise
     */
    public CharSequence lock(CharSequence tableName, CharSequence lockReason) {
        checkClosed();

        long thread = Thread.currentThread().getId();

        Entry e = entries.get(tableName);
        if (e == null) {
            // We are racing to create new writer!
            e = new Entry(clock.getTicks());
            Entry other = entries.putIfAbsent(tableName, e);
            if (other == null) {
                if (lockAndNotify(thread, e, tableName, lockReason)) {
                    return null;
                } else {
                    entries.remove(tableName);
                    assert null != e.ownershipReason;
                    return e.ownershipReason;
                }
            } else {
                e = other;
            }
        }

        // try to change owner
        if ((Unsafe.cas(e, ENTRY_OWNER, UNALLOCATED, thread) /*|| Unsafe.cas(e, ENTRY_OWNER, thread, thread)*/)) {
            closeWriter(thread, e, PoolListener.EV_LOCK_CLOSE, PoolConstants.CR_NAME_LOCK);
            if (lockAndNotify(thread, e, tableName, lockReason)) {
                return null;
            }
            assert null != e.ownershipReason;
            return e.ownershipReason;
        }

        LOG.error().$("could not lock, busy [table=`").utf8(tableName).$("`, owner=").$(e.owner).$(", thread=").$(thread).$(']').$();
        notifyListener(thread, tableName, PoolListener.EV_LOCK_BUSY);
        assert null != e.ownershipReason;
        return e.ownershipReason;
    }

    /**
     * Counts busy writers in pool.
     *
     * @return number of busy writer instances.
     */
    public int getBusyCount() {
        int count = 0;
        for (Entry e : entries.values()) {
            if (e.owner != UNALLOCATED) {
                count++;
            }
        }
        return count;
    }

    private TableWriter checkClosedAndGetWriter(CharSequence tableName, Entry e, CharSequence lockReason) {
        if (null == lockReason) {
            throw new NullPointerException();
        }
        if (isClosed()) {
            // pool closed but we somehow managed to lock writer
            // make sure that interceptor cleared to allow calling thread close writer normally
            LOG.info().$('\'').utf8(tableName).$("' born free").$();
            return e.goodby();
        }
        e.ownershipReason = lockReason;
        return logAndReturn(e, PoolListener.EV_GET);
    }

    public int size() {
        return entries.size();
    }

    public void unlock(CharSequence name) {
        unlock(name, null, false);
    }

    public void unlock(CharSequence name, @Nullable TableWriter writer, boolean newTable) {
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
                throw CairoException.instance(0).put("Writer ").put(name).put(" is not locked");
            }

            if (newTable) {
                // Note that the TableUtils.createTable method will create files, but on some OS's these files will not immediately become
                // visible on all threads,
                // only in this thread will they definitely be visible. To prevent spurious file system errors (or even allowing the same
                // table to be created twice),
                // we cache the writer in the writerPool whose access via the engine is thread safe
                assert writer == null && e.lockFd != -1;
                LOG.info().$("created [table=`").utf8(name).$("`, thread=").$(thread).$(']').$();
                writer = new TableWriter(configuration, name, messageBus, false, e, root);
            }

            if (writer == null) {
                // unlock must remove entry because pool does not deal with null writer

                if (e.lockFd != -1) {
                    ff.close(e.lockFd);
                    TableUtils.lockName(path.of(root).concat(name));
                    if (!ff.remove(path)) {
                        LOG.error().$("could not remove [file=").$(path).$(']').$();
                    }
                }
                entries.remove(name);
            } else {
                e.writer = writer;
                writer.setLifecycleManager(e);
                writer.transferLock(e.lockFd);
                e.lockFd = -1;
                e.ownershipReason = UNLOCKING;
                Unsafe.getUnsafe().putOrderedLong(e, ENTRY_OWNER, UNALLOCATED);
            }
            notifyListener(thread, name, PoolListener.EV_UNLOCKED);
            LOG.debug().$("unlocked [table=`").utf8(name).$("`]").$();
        } else {
            notifyListener(thread, name, PoolListener.EV_NOT_LOCK_OWNER);
            throw CairoException.instance(0).put("Not lock owner of ").put(name);
        }
    }

    private void checkClosed() {
        if (isClosed()) {
            LOG.info().$("is closed").$();
            throw PoolClosedException.INSTANCE;
        }
    }

    /**
     * Closes writer pool. When pool is closed only writers that are in pool are proactively released. Writers that
     * are outside of pool will close when their close() method is invoked.
     * <p>
     * After pool is closed it will notify listener with #EV_POOL_CLOSED event.
     * </p>
     */
    @Override
    protected void closePool() {
        super.closePool();
        Misc.free(path);
        LOG.info().$("closed").$();
    }

    @Override
    protected boolean releaseAll(long deadline) {
        long thread = Thread.currentThread().getId();
        boolean removed = false;
        final int reason;

        if (deadline == Long.MAX_VALUE) {
            reason = PoolConstants.CR_POOL_CLOSE;
        } else {
            reason = PoolConstants.CR_IDLE;
        }

        Iterator<Entry> iterator = entries.values().iterator();
        while (iterator.hasNext()) {
            Entry e = iterator.next();
            // lastReleaseTime is volatile, which makes
            // order of conditions important
            if ((deadline > e.lastReleaseTime && e.owner == UNALLOCATED)) {
                // looks like this one can be released
                // try to lock it
                if (Unsafe.cas(e, ENTRY_OWNER, UNALLOCATED, thread)) {
                    // lock successful
                    closeWriter(thread, e, PoolListener.EV_EXPIRE, reason);
                    iterator.remove();
                    removed = true;
                }
            } else if (e.lockFd != -1L) {
                if (ff.close(e.lockFd)) {
                    e.lockFd = -1L;
                    iterator.remove();
                    removed = true;
                }
            } else if (e.ex != null) {
                LOG.info().$("purging entry for failed to allocate writer").$();
                iterator.remove();
                removed = true;
            }
        }
        return removed;
    }

    private void closeWriter(long thread, Entry e, short ev, int reason) {
        TableWriter w = e.writer;
        if (w != null) {
            CharSequence name = e.writer.getTableName();
            w.setLifecycleManager(DefaultLifecycleManager.INSTANCE);
            w.close();
            e.writer = null;
            LOG.info().$("closed [table=`").utf8(name).$("`, reason=").$(PoolConstants.closeReasonText(reason)).$(", by=").$(thread).$(']').$();
            notifyListener(thread, name, ev);
        }
    }

    int countFreeWriters() {
        int count = 0;
        for (Entry e : entries.values()) {
            if (e.owner == UNALLOCATED) {
                count++;
            } else {
                LOG.info().$("'").utf8(e.writer.getTableName()).$("' is still busy [owner=").$(e.owner).$(']').$();
            }
        }

        return count;
    }

    private TableWriter createWriter(CharSequence name, Entry e, long thread, CharSequence lockReason) {
        try {
            checkClosed();
            LOG.info().$("open [table=`").utf8(name).$("`, thread=").$(thread).$(']').$();
            e.writer = new TableWriter(configuration, name, messageBus, true, e, root);
            e.ownershipReason = lockReason;
            return logAndReturn(e, PoolListener.EV_CREATE);
        } catch (CairoException ex) {
            LOG.error()
                    .$("could not open [table=`").utf8(name)
                    .$("`, thread=").$(e.owner)
                    .$(", ex=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .$(']').$();
            e.ex = ex;
            e.ownershipReason = UNLOCKING;
            e.owner = UNALLOCATED;
            notifyListener(e.owner, name, PoolListener.EV_CREATE_EX);
            throw ex;
        }
    }

    private boolean lockAndNotify(long thread, Entry e, CharSequence tableName, CharSequence lockReason) {
        if (null == lockReason) {
            throw new NullPointerException();
        }
        TableUtils.lockName(path.of(root).concat(tableName));
        e.lockFd = TableUtils.lock(ff, path);
        if (e.lockFd == -1L) {
            LOG.error().$("could not lock [table=`").utf8(tableName).$("`, thread=").$(thread).$(']').$();
            e.ownershipReason = UNLOCKING;
            e.owner = UNALLOCATED;
            return false;
        }
        LOG.debug().$("locked [table=`").utf8(tableName).$("`, thread=").$(thread).$(']').$();
        notifyListener(thread, tableName, PoolListener.EV_LOCK_SUCCESS);
        e.ownershipReason = lockReason;
        return true;
    }

    private TableWriter logAndReturn(Entry e, short event) {
        LOG.info().$(">> [table=`").utf8(e.writer.getTableName()).$("`, thread=").$(e.owner).$(']').$();
        notifyListener(e.owner, e.writer.getTableName(), event);
        return e.writer;
    }

    private boolean returnToPool(Entry e) {
        final long thread = Thread.currentThread().getId();
        final CharSequence name = e.writer.getTableName();
        try {
            e.writer.rollback();
        } catch (Throwable ex) {
            // We are here because of a systemic issues of some kind
            // one of the known issues is "disk is full" so we could not rollback properly.
            // In this case we just close TableWriter
            entries.remove(name);
            closeWriter(thread, e, PoolListener.EV_LOCK_CLOSE, PoolConstants.CR_DISTRESSED);
            return true;
        }
        if (e.owner != UNALLOCATED) {
            LOG.info().$("<< [table=`").utf8(name).$("`, thread=").$(thread).$(']').$();
            if (isClosed()) {
                LOG.info().$("allowing '").utf8(name).$("' to close [thread=").$(e.owner).$(']').$();
                notifyListener(thread, name, PoolListener.EV_OUT_OF_POOL_CLOSE);
                return false;
            }

            e.ownershipReason = UNLOCKING;
            e.owner = UNALLOCATED;
            e.lastReleaseTime = configuration.getMicrosecondClock().getTicks();
            notifyListener(thread, name, PoolListener.EV_RETURN);
        } else {
            LOG.error().$("orphaned [table=`").utf8(name).$("`]").$();
            notifyListener(thread, name, PoolListener.EV_UNEXPECTED_CLOSE);
        }
        return true;
    }

    private class Entry implements LifecycleManager {
        // owner thread id or -1 if writer is available for hire
        private volatile long owner = Thread.currentThread().getId();
        private volatile CharSequence ownershipReason = UNLOCKING;
        private TableWriter writer;
        // time writer was last released
        private volatile long lastReleaseTime;
        private CairoException ex = null;
        private volatile long lockFd = -1L;

        public Entry(long lastReleaseTime) {
            this.lastReleaseTime = lastReleaseTime;
        }

        @Override
        public boolean close() {
            return !WriterPool.this.returnToPool(this);
        }

        public TableWriter goodby() {
            TableWriter w = writer;
            if (writer != null) {
                writer.setLifecycleManager(DefaultLifecycleManager.INSTANCE);
                writer = null;
            }
            return w;
        }
    }
}
