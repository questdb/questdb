/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultLifecycleManager;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.LifecycleManager;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.Iterator;
import java.util.Map;

/**
 * This class maintains cache of open writers to avoid OS overhead of
 * opening and closing files. While doing so it abides by the same
 * rule as non-pooled writers: there can only be one TableWriter instance
 * for any given table name.
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
    public static final String OWNERSHIP_REASON_MISSING = "missing or owned by other process";
    public static final String OWNERSHIP_REASON_NONE = null;
    public static final String OWNERSHIP_REASON_RELEASED = "released";
    public static final String OWNERSHIP_REASON_UNKNOWN = "unknown";
    static final String OWNERSHIP_REASON_WRITER_ERROR = "writer error";
    private static final long ENTRY_COMMAND_PUBLISHER_COUNT = Unsafe.getFieldOffset(Entry.class, "commandPublisherCount");
    private static final long ENTRY_OWNER = Unsafe.getFieldOffset(Entry.class, "owner");
    private static final long ENTRY_WRITER = Unsafe.getFieldOffset(Entry.class, "writer");
    private static final Log LOG = LogFactory.getLog(WriterPool.class);
    private static final long QUEUE_PROCESSING_OWNER = -2L;
    private final MicrosecondClock clock;
    private final CairoConfiguration configuration;
    @NotNull
    private final CairoEngine engine;
    private final ConcurrentHashMap<Entry> entries = new ConcurrentHashMap<>();
    private final RecentWriteTracker recentWriteTracker;
    private final CharSequence root;

    /**
     * Pool constructor. WriterPool root directory is passed via configuration.
     *
     * @param configuration      configuration parameters.
     * @param engine             engine instance.
     * @param recentWriteTracker tracker for recent table writes.
     */
    public WriterPool(CairoConfiguration configuration, @NotNull CairoEngine engine, @NotNull RecentWriteTracker recentWriteTracker) {
        super(configuration, configuration.getInactiveWriterTTL());
        this.configuration = configuration;
        this.clock = configuration.getMicrosecondClock();
        this.root = configuration.getDbRoot();
        this.engine = engine;
        this.recentWriteTracker = recentWriteTracker;
        notifyListener(Thread.currentThread().threadId(), null, PoolListener.EV_POOL_OPEN);
    }

    @TestOnly
    public int countFreeWriters() {
        int count = 0;
        for (Entry e : entries.values()) {
            final long owner = e.owner;
            if (owner == UNALLOCATED) {
                count++;
            } else {
                final TableWriter w = e.writer;
                LOG.info().$("table is still busy [table=").$(w != null ? w.getTableToken() : null)
                        .$(", owner=").$(owner)
                        .I$();
            }
        }
        return count;
    }

    public Map<CharSequence, Entry> entries() {
        return entries;
    }

    /**
     * <p>
     * Creates or retrieves existing TableWriter from pool. Because of TableWriter compliance with <b>single
     * writer model</b> pool ensures there is single TableWriter instance for given table name. Table name is unique in
     * context of <b>root</b>, and pool instance covers single root.
     * </p>
     * Exceptions thrown:
     * <ul>
     * <li>{@link EntryUnavailableException}: When TableWriter from this pool is used by another thread.
     * In this case, application can retry getting writer from pool again at any time.</li>
     * <li>{@link CairoException}: When table is locked outside of pool, which includes same or different process.
     * In this case, application has to call {@link #releaseAll(long)} before retrying for TableWriter.</li>
     * </ul>
     *
     * @param tableToken name of the table
     * @param lockReason description of where or why lock is held
     * @return cached TableWriter instance.
     */
    public TableWriter get(TableToken tableToken, @NotNull String lockReason) {
        // writer cannot be null because our async command is null
        TableWriter w = getWriterEntry(tableToken, lockReason, null);
        assert w != null;
        w.goActive();
        return w;
    }

    /**
     * Counts busy writers in pool.
     *
     * @return number of busy writer instances.
     */
    @TestOnly
    public int getBusyCount() {
        int count = 0;
        for (Entry e : entries.values()) {
            if (e.owner != UNALLOCATED) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns writer from the pool or sends writer command
     *
     * @param tableToken         name of the table
     * @param lockReason         reason for the action
     * @param asyncWriterCommand command to write to TableWriterTask
     * @return null if command is published or TableWriter instance if writer is available
     */
    public TableWriter getWriterOrPublishCommand(
            TableToken tableToken,
            @NotNull String lockReason,
            @NotNull AsyncWriterCommand asyncWriterCommand
    ) {
        while (true) {
            try {
                return getWriterEntry(tableToken, lockReason, asyncWriterCommand);
            } catch (EntryUnavailableException ex) {
                // means retry in this context
            }
        }
    }

    /**
     * Locks writer. Lock is usually successful when writer is in pool or
     * owned by calling thread, in which case writer instance is closed.
     * Lock will also succeed when writer does not exist. This will prevent
     * from writer being created before it is unlocked.
     * <p>
     * Lock fails immediately with {@link EntryUnavailableException} when writer is used by another thread and with
     * {@link PoolClosedException} when pool is closed.
     * </p>
     * <p>
     * Lock is beneficial before table directory is renamed or deleted.
     * </p>
     *
     * @param tableToken table name
     * @param lockReason description of where or why lock is held
     * @return lock reason as String
     */
    public String lock(TableToken tableToken, String lockReason) {
        checkClosed();

        long thread = Thread.currentThread().threadId();

        while (true) {
            Entry e = entries.get(tableToken.getDirName());
            if (e == null) {
                // We are racing to create new writer!
                final String dirName = tableToken.getDirName();
                e = new Entry(clock.getTicks(), dirName);
                Entry other = entries.putIfAbsent(dirName, e);
                if (other == null) {
                    if (lockAndNotify(thread, e, tableToken, lockReason)) {
                        return OWNERSHIP_REASON_NONE;
                    } else {
                        entries.remove(dirName);
                        return reinterpretOwnershipReason(e.ownershipReason);
                    }
                } else {
                    e = other;
                }
            }

            // try to change owner
            if (Unsafe.cas(e, ENTRY_OWNER, UNALLOCATED, thread)) {
                closeWriter(thread, e, PoolListener.EV_LOCK_CLOSE, PoolConstants.CR_NAME_LOCK);
                if (lockAndNotify(thread, e, tableToken, lockReason)) {
                    return OWNERSHIP_REASON_NONE;
                }
                return reinterpretOwnershipReason(e.ownershipReason);
            }

            long owner = e.owner;
            if (owner < 0) {
                // writer is about to be released from the pool by release method.
                // try again, it should become available soon.
                Os.pause();
                continue;
            }

            LOG.error().$("could not lock, busy [table=").$(tableToken)
                    .$(", owner=").$(owner)
                    .$(", thread=").$(thread).I$();
            notifyListener(thread, tableToken, PoolListener.EV_LOCK_BUSY);
            return reinterpretOwnershipReason(e.ownershipReason);
        }
    }

    public int size() {
        return entries.size();
    }

    public void unlock(TableToken tableToken, @Nullable TableWriter writer, boolean newTable) {
        long thread = Thread.currentThread().threadId();

        Entry e = entries.get(tableToken.getDirName());
        if (e == null) {
            notifyListener(thread, tableToken, PoolListener.EV_NOT_LOCKED);
            return;
        }

        if (e.owner == thread) {
            if (e.writer != null) {
                // When entry is locked, writer must be null, however if writer is not null,
                // calling thread must be trying to unlock writer that hasn't been locked.
                // This qualifies for "illegal state".
                notifyListener(thread, tableToken, PoolListener.EV_NOT_LOCKED);
                throw CairoException.critical(0).put("Writer ").put(tableToken.getDirName()).put(" is not locked");
            }

            assert !newTable || writer == null && e.lockFd != -1;

            if (writer == null) {
                // unlock must remove entry because pool does not deal with null writer
                if (e.lockFd != -1) {
                    Path path = Path.getThreadLocal(root).concat(tableToken.getDirName());
                    if (!ff.closeRemove(e.lockFd, TableUtils.lockName(path))) {
                        LOG.error().$("could not remove [file=").$(path).$(']').$();
                    }
                }
                entries.remove(tableToken.getDirName());
            } else {
                e.writer = writer;
                writer.setLifecycleManager(e);
                writer.transferLock(e.lockFd);
                e.lockFd = -1;
                e.ownershipReason = OWNERSHIP_REASON_NONE;
                Unsafe.storeFence();
                Unsafe.putOrderedLong(e, ENTRY_OWNER, UNALLOCATED);
            }
            notifyListener(thread, tableToken, PoolListener.EV_UNLOCKED);
            LOG.debug().$("unlocked [table=").$(tableToken)
                    .$(", thread=").$(thread).I$();
        } else {
            notifyListener(thread, tableToken, PoolListener.EV_NOT_LOCK_OWNER);
            throw CairoException.critical(0).put("Not lock owner of ").put(tableToken.getDirNameUtf8());
        }
    }

    public void unlock(TableToken tableToken) {
        unlock(tableToken, null, false);
    }

    private void addCommandToWriterQueue(TableToken tableToken, Entry e, AsyncWriterCommand asyncWriterCommand, long thread) {
        TableWriter writer;
        while ((writer = e.writer) == null && e.owner != UNALLOCATED) {
            // If the entry has been removed from the pool (e.g. distressed close),
            // our reference is orphaned and the spin condition will never be satisfied.
            // Bail out and let the caller retry.
            if (entries.get(tableToken.getDirName()) != e) {
                throw EntryUnavailableException.instance("please retry");
            }
            Os.pause();
        }
        if (writer == null) {
            // Retry from very beginning
            throw EntryUnavailableException.instance("please retry");
        }
        // Announce ourselves as an in-flight command publisher before serializing into
        // the writer's command queue, then re-validate the writer reference we captured.
        // closeWriter() nulls e.writer and drains this count to zero before it frees the
        // queue. This is a Dekker handshake: our seq-cst increment of the count is followed
        // by a volatile read of e.writer, while closeWriter() volatile-stores null into
        // e.writer and then reads the count. Sequential consistency forbids the symmetric
        // r1==r2==0 outcome (on AArch64 the StoreLoad comes from HotSpot's planted dmb ish,
        // not from the atomic's LL/SC loop being a fence), so closeWriter() either observes
        // our count and waits for us, or we observe the nulled writer and bail - the two can
        // never both slip through. That is what kept serialize() from writing into a
        // freed/zeroed TableWriterTask buffer and crashing the JVM with a SIGSEGV. Footgun:
        // keep the count and the e.writer accesses full-volatile; downgrading to
        // setRelease/getAcquire would re-admit r1==r2==0 and bring the crash back.
        Unsafe.getAndAddLong(e, ENTRY_COMMAND_PUBLISHER_COUNT, 1);
        try {
            if (Unsafe.getObjectVolatile(e, ENTRY_WRITER) != writer) {
                // A concurrent close pulled the writer out from under us. Do not serialize
                // into its soon-to-be-freed queue; let the caller retry.
                throw EntryUnavailableException.instance("please retry");
            }
            // Publish it. publishAsyncWriterCommand() marks the command as executing
            // asynchronously (startAsync) once it has validated the WAL invariant.
            writer.publishAsyncWriterCommand(asyncWriterCommand);
        } finally {
            Unsafe.getAndAddLong(e, ENTRY_COMMAND_PUBLISHER_COUNT, -1);
        }

        // Make sure writer does not go to the pool with command in the queue
        // Wait until writer is either in the pool or out
        while (e.owner == QUEUE_PROCESSING_OWNER) {
            Os.pause();
        }

        // If the writer is suddenly in the pool, lock it and call tick to process command queue
        if (Unsafe.cas(e, ENTRY_OWNER, UNALLOCATED, thread)) {
            // Writer became available straight after setting items in the queue.
            // Don't leave it unprocessed
            try {
                // Re-read the writer under ownership: a close racing our publish may have
                // freed the one we captured. Only tick a live writer.
                TableWriter w = e.writer;
                if (w != null) {
                    w.tick(true);
                }
            } finally {
                Unsafe.cas(e, ENTRY_OWNER, thread, UNALLOCATED);
            }
        }
    }

    private void assertLockReasonIsNone(String lockReason) {
        //noinspection StringEquality
        if (lockReason == OWNERSHIP_REASON_NONE) {
            throw new NullPointerException();
        }
    }

    private void checkClosed() {
        if (isClosed()) {
            LOG.info().$("is closed").$();
            throw PoolClosedException.INSTANCE;
        }
    }

    private TableWriter checkClosedAndGetWriter(TableToken tableToken, Entry e, String lockReason) {
        assertLockReasonIsNone(lockReason);
        if (isClosed()) {
            // pool closed, but we somehow managed to lock writer
            // make sure that interceptor cleared to allow calling thread close writer normally
            LOG.info().$("born free [table=").$(tableToken).I$();
            return e.goodbye();
        }
        e.ownershipReason = lockReason;
        e.writer.updateTableToken(tableToken);
        return logAndReturn(e, PoolListener.EV_GET);
    }

    private void closeWriter(long thread, Entry e, short ev, int reason) {
        TableWriter w = e.writer;
        if (w != null) {
            TableToken tableToken = w.getTableToken();
            // Wait out any in-flight async-command publisher before w.close() frees the
            // writer command queue (see drainCommandPublishers).
            drainCommandPublishers(e);
            w.setLifecycleManager(DefaultLifecycleManager.INSTANCE);
            w.close();
            e.ownershipReason = OWNERSHIP_REASON_RELEASED;
            LOG.info().$("closed [table=").$(tableToken)
                    .$(", reason=").$(PoolConstants.closeReasonText(reason))
                    .$(", by=").$(thread)
                    .I$();
            notifyListener(thread, tableToken, ev);
        }
    }

    private TableWriter createWriter(TableToken tableToken, Entry e, long thread, String lockReason) {
        try {
            checkClosed();
            LOG.info().$("open [table=").$(tableToken)
                    .$(", thread=").$(thread).I$();
            final TableWriter w = new TableWriter(
                    configuration,
                    tableToken,
                    engine.getMessageBus(),
                    null,
                    true,
                    e,
                    root,
                    engine.getDdlListener(tableToken),
                    engine
            );
            e.ownershipReason = lockReason;
            Unsafe.putObjectVolatile(e, ENTRY_WRITER, w);
            return logAndReturn(e, PoolListener.EV_CREATE);
        } catch (CairoException ex) {
            final LogRecord record = ex.isCritical() ? LOG.critical() : LOG.error();
            record.$("could not open [table=").$(tableToken)
                    .$(", thread=").$(e.owner)
                    .$(", msg=").$safe(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            e.ex = ex;
            e.ownershipReason = OWNERSHIP_REASON_WRITER_ERROR;
            e.owner = UNALLOCATED;
            notifyListener(e.owner, tableToken, PoolListener.EV_CREATE_EX);
            throw ex;
        } catch (CairoError ex) {
            LOG.critical().$("could not open [table=").$(tableToken)
                    .$(", thread=").$(e.owner)
                    .$(", ex=").$safe(ex.getFlyweightMessage())
                    .I$();
            e.ownershipReason = OWNERSHIP_REASON_WRITER_ERROR;
            e.owner = UNALLOCATED;
            notifyListener(e.owner, tableToken, PoolListener.EV_CREATE_EX);
            throw ex;
        }
    }

    // Makes the entry's writer unreachable to async-command publishers and waits for any
    // that already captured it to finish serializing into its command queue. A caller must
    // invoke this before freeing the writer (w.close() -> Misc.free(commandQueue)).
    // A publisher announces itself via commandPublisherCount in addCommandToWriterQueue;
    // without this drain it could serialize into a freed/zeroed TableWriterTask buffer and
    // crash the JVM with a SIGSEGV. The volatile store of null below and the volatile read of
    // commandPublisherCount form the closer's half of the Dekker handshake described in
    // addCommandToWriterQueue; sequential consistency between them and the publisher's seq-cst
    // increment is what stops either side from missing the other. Keep both full-volatile.
    private void drainCommandPublishers(Entry e) {
        Unsafe.putObjectVolatile(e, ENTRY_WRITER, null);
        while (e.commandPublisherCount > 0) {
            Os.pause();
        }
    }

    // Evicts an orphaned entry whose writer was already freed out of band (TableWriter.destroy()),
    // where the table dir name is no longer reachable through the nulled e.writer. The entry caches
    // the map key it was inserted under, so this removes it in O(1), and only when the key still
    // maps to this exact entry. Rare path - only a dropped WAL table reaches it.
    private void evictEntry(Entry e) {
        entries.remove(e.dirName, e);
    }

    private TableWriter getWriterEntry(
            TableToken tableToken,
            @NotNull String lockReason,
            @Nullable AsyncWriterCommand asyncWriterCommand
    ) {
        checkClosed();

        long thread = Thread.currentThread().threadId();

        while (true) {
            Entry e = entries.get(tableToken.getDirName());
            if (e == null) {
                // We are racing to create new writer!
                final String dirName = tableToken.getDirName();
                e = new Entry(clock.getTicks(), dirName);
                Entry other = entries.putIfAbsent(dirName, e);
                if (other == null) {
                    // race won
                    return createWriter(tableToken, e, thread, lockReason);
                } else {
                    e = other;
                }
            }

            // try to change owner
            if (Unsafe.cas(e, ENTRY_OWNER, UNALLOCATED, thread)) {
                // we managed to grab the writer

                // in an extreme race condition it is possible that e.writer will be null
                // in this case behaviour should be identical to entry missing entirely
                if (e.writer == null) {
                    return createWriter(tableToken, e, thread, lockReason);
                }
                return checkClosedAndGetWriter(tableToken, e, lockReason);
            } else {
                long owner = e.owner;
                if (owner < 0) {
                    // writer is about to be released from the pool by release method.
                    // try again, it should become available soon.
                    Os.pause();
                    continue;
                }
                if (owner == thread) {
                    if (e.lockFd != -1L) {
                        throw EntryLockedException.instance(reinterpretOwnershipReason(e.ownershipReason));
                    }

                    if (e.ex != null) {
                        notifyListener(thread, tableToken, PoolListener.EV_EX_RESEND);
                        // this writer failed to allocate by this very thread
                        // ensure consistent response
                        entries.remove(tableToken.getDirName());
                        throw e.ex;
                    }
                }
                if (asyncWriterCommand != null) {
                    addCommandToWriterQueue(tableToken, e, asyncWriterCommand, thread);
                    return null;
                }

                String reason = reinterpretOwnershipReason(e.ownershipReason);

                if (!tableToken.isWal()) {
                    // Don't log busy for WAL table it's BAU.
                    LOG.info().$("busy [table=").$(tableToken)
                            .$(", owner=").$(owner)
                            .$(", thread=").$(thread)
                            .$(", reason=").$(reason)
                            .I$();
                }
                throw EntryUnavailableException.instance(reason);
            }
        }
    }

    private boolean lockAndNotify(long thread, Entry e, TableToken tableToken, String lockReason) {
        assertLockReasonIsNone(lockReason);
        Path path = Path.getThreadLocal(root).concat(tableToken.getDirName());
        e.lockFd = TableUtils.lock(ff, TableUtils.lockName(path));
        if (e.lockFd == -1) {
            LOG.error().$("could not lock [table=").$(tableToken)
                    .$(", thread=").$(thread).I$();
            e.ownershipReason = OWNERSHIP_REASON_MISSING;
            e.owner = UNALLOCATED;
            return false;
        }
        LOG.debug().$("locked [table=").$(tableToken).$(", thread=").$(thread).I$();
        notifyListener(thread, tableToken, PoolListener.EV_LOCK_SUCCESS);
        e.ownershipReason = lockReason;
        return true;
    }

    private TableWriter logAndReturn(Entry e, short event) {
        LOG.debug().$(">> [table=").$(e.writer.getTableToken())
                .$(", thread=").$(e.owner).I$();
        notifyListener(e.owner, e.writer.getTableToken(), event);
        return e.writer;
    }

    private String reinterpretOwnershipReason(String providedReason) {
        // we cannot always guarantee that ownership reason is set
        // allocating writer and setting "reason" are non-atomic
        // therefore we could be in a situation where we can be confident writer is locked
        // but reason has not yet caught up. In this case we do not really know the reason
        // but not to confuse the caller, we have to provide a non-null value
        //noinspection StringEquality
        return providedReason == OWNERSHIP_REASON_NONE ? OWNERSHIP_REASON_UNKNOWN : providedReason;
    }

    private boolean returnToPool(Entry e) {
        final long thread = Thread.currentThread().threadId();
        if (e.writer == null) {
            // The doClose() pre-free hook already drained and nulled the writer: this is the
            // out-of-band TableWriter.destroy() path (the WAL drop-table purge). There is
            // nothing to roll back or hand back to the pool; just evict the orphaned entry.
            evictEntry(e);
            notifyListener(thread, null, PoolListener.EV_RETURN);
            return true;
        }
        final TableToken tableToken = e.writer.getTableToken();

        boolean isDistressed;
        try {
            e.writer.rollback();
            // Rollback can change writer state to distressed, do not observe it before rollback
            isDistressed = e.writer.isDistressed();
            if (!isDistressed) {
                if (e.owner != UNALLOCATED) {
                    e.owner = QUEUE_PROCESSING_OWNER;
                }
                // We can apply structure changes with ALTER TABLE and do UPDATE(s) before the writer returned to the pool
                e.writer.tick(true);
                e.writer.goPassive();
            }
        } catch (Throwable ex) {
            // We are here because of a systemic issues of some kind
            // one of the known issues is "disk is full" so we could not roll back properly.
            // In this case we just close TableWriter
            isDistressed = true;
        }

        if (isDistressed) {
            closeWriter(thread, e, PoolListener.EV_LOCK_CLOSE, PoolConstants.CR_DISTRESSED);
            entries.remove(tableToken.getDirName());
            notifyListener(thread, tableToken, PoolListener.EV_RETURN);
            return true;
        }

        if (e.owner != UNALLOCATED) {
            LOG.debug().$("<< [table=").$(tableToken)
                    .$(", thread=").$(thread).$(']').$();

            e.ownershipReason = OWNERSHIP_REASON_NONE;
            e.lastReleaseTime = configuration.getMicrosecondClock().getTicks();

            // Track the write for UI/observability purposes.
            // Use try-catch to ensure writer is always returned even if tracking fails.
            try {
                recentWriteTracker.recordWrite(tableToken, e.lastReleaseTime, e.writer.getRowCount(), e.writer.getAppliedSeqTxn());
            } catch (Throwable th) {
                LOG.error().$("failed to track write [table=").$(tableToken)
                        .$(", error=").$(th).I$();
            }

            Unsafe.storeFence();
            Unsafe.putOrderedLong(e, ENTRY_OWNER, UNALLOCATED);

            if (isClosed()) {
                // when pool is closed it could be busy releasing writer
                // to avoid race condition try to grab the writer before declaring it a
                // free agent
                if (Unsafe.cas(e, ENTRY_OWNER, UNALLOCATED, thread)) {
                    // Returning false makes TableWriter.close() run doClose(), which frees
                    // the command queue. Drain in-flight publishers first, just like
                    // closeWriter() does on the other free paths.
                    drainCommandPublishers(e);
                    notifyListener(thread, tableToken, PoolListener.EV_OUT_OF_POOL_CLOSE);
                    return false;
                }
            }

            notifyListener(thread, tableToken, PoolListener.EV_RETURN);
        } else {
            LOG.critical().$("orphaned [table=").$(tableToken).I$();
            notifyListener(thread, tableToken, PoolListener.EV_UNEXPECTED_CLOSE);
        }
        return true;
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
        LOG.debug().$("closed").$();
    }

    @Override
    protected boolean releaseAll(long deadline) {
        long thread = Thread.currentThread().threadId();
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
                // looks like this writer is unallocated and can be released
                // Lock with negative 3-based owner thread id to indicate it's that next
                // allocating thread can wait until the entry is released.
                // Avoid negative thread id clashing with UNALLOCATED and QUEUE_PROCESSING values
                if (Unsafe.cas(e, ENTRY_OWNER, UNALLOCATED, -thread - 3)) {
                    // lock successful
                    closeWriter(thread, e, PoolListener.EV_EXPIRE, reason);
                    iterator.remove();
                    removed = true;
                }
            } else if (e.lockFd != -1 && isClosed()) {
                // Close a held .lock fd ONLY while the pool itself is shutting down. close()
                // sets the closed flag before closePool() reaches this releaseAll(Long.MAX_VALUE),
                // so genuine shutdown still releases the lock. A live-engine caller that passes
                // Long.MAX_VALUE to force an idle reap (e.g. a role-switch drain) must NOT land
                // here: closing the lock fd and removing the entry of an in-flight lock() holder
                // would let the next get() build a second TableWriter on the same table,
                // breaking the single-writer-per-table invariant. Gating on isClosed() instead
                // of the deadline value keeps that hazard out of every live-engine path while
                // leaving the shutdown release intact.
                if (ff.close(e.lockFd)) {
                    e.lockFd = -1;
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

    public class Entry implements LifecycleManager {
        // Number of threads currently serializing an async writer command into this
        // entry's writer command queue. The pool drains this to zero (see
        // drainCommandPublishers) before it frees the queue so a publisher can never
        // write into freed memory.
        private volatile long commandPublisherCount;
        // The entries map key (tableToken.getDirName()) this entry was inserted under. The
        // dir name is stable across renames, so it stays valid for the entry's whole life and
        // lets evictEntry() remove the entry in O(1) even once e.writer has been nulled.
        private final String dirName;
        private CairoException ex = null;
        // time writer was last released
        private volatile long lastReleaseTime;
        private volatile long lockFd = -1;
        // owner thread id or -1 if writer is available for hire
        private volatile long owner = Thread.currentThread().threadId();
        private volatile String ownershipReason = OWNERSHIP_REASON_NONE;
        private TableWriter writer;

        public Entry(long lastReleaseTime, String dirName) {
            this.lastReleaseTime = lastReleaseTime;
            this.dirName = dirName;
        }

        @Override
        public boolean close() {
            return !WriterPool.this.returnToPool(this);
        }

        public long getLastReleaseTime() {
            return lastReleaseTime;
        }

        public long getOwnerThread() {
            return owner;
        }

        public String getOwnershipReason() {
            return ownershipReason;
        }

        public TableToken getTableToken() {
            final TableWriter w = writer;
            return w != null ? w.getTableToken() : null;
        }

        public TableWriter goodbye() {
            TableWriter w = writer;
            if (w != null) {
                w.setLifecycleManager(DefaultLifecycleManager.INSTANCE);
                // The caller takes the writer out of the pool and will close it itself,
                // freeing the command queue. Detach it from in-flight publishers first.
                drainCommandPublishers(this);
            }
            return w;
        }

        // doClose() pre-free hook. Reached only when a pooled writer is freed out of band:
        // TableWriter.destroy() (the WAL drop-table purge) calls doClose() directly, bypassing
        // the pool close paths that already drain. Those paths swap to DefaultLifecycleManager
        // before close(), so this override is a no-op for them; for the destroy() path it runs
        // the same drain, nulling the writer before doClose() frees the command queue. The
        // orphaned entry is evicted later, when the writer's own close() reaches returnToPool -
        // after doClose() has released the table lock, so no window exists where the entry is
        // gone but the lock is still held.
        @Override
        public void onBeforeClose() {
            drainCommandPublishers(this);
        }
    }
}
