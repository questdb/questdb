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

package io.questdb.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Runtime representation of a live view. Holds the InMemoryTable and metadata.
 * <p>
 * Concurrency model:
 * <ul>
 *     <li>A fair {@link ReentrantReadWriteLock} serialises reader cursors (read lock)
 *     against the refresh job's table mutation (write lock).</li>
 *     <li>{@link #refreshLatch} is a CAS latch held for the entire refresh operation
 *     (SQL compile + cursor + table copy), so a concurrent DROP cannot free the table
 *     while the refresh is compiling — a window the write lock alone does not cover.</li>
 *     <li>{@link #dropped} is the DROP signal. {@link #tryCloseIfDropped} races refresh,
 *     reader close, and drop to actually free the table; whichever party finds no lock
 *     contention wins, mirroring the {@code MatViewState.tryLock/tryCloseIfDropped} pattern.</li>
 * </ul>
 */
public class LiveViewInstance implements QuietCloseable {
    private final LiveViewDefinition definition;
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final AtomicBoolean refreshLatch = new AtomicBoolean(false);
    private final InMemoryTable table = new InMemoryTable();
    private volatile boolean dropped;
    private volatile String invalidationReason;
    private boolean isClosed;
    private volatile long lastProcessedSeqTxn = -1;

    public LiveViewInstance(LiveViewDefinition definition) {
        this.definition = definition;
        this.table.init(definition.getMetadata());
    }

    @Override
    public void close() {
        // Shutdown path only — called from LiveViewRegistry.close() after all workers
        // have stopped, so no concurrent refresh or reader is expected. Runtime drops
        // go through markAsDropped() + tryCloseIfDropped() instead.
        dropped = true;
        lock.writeLock().lock();
        try {
            if (!isClosed) {
                isClosed = true;
                Misc.free(table);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public LiveViewDefinition getDefinition() {
        return definition;
    }

    public long getLastProcessedSeqTxn() {
        return lastProcessedSeqTxn;
    }

    public String getInvalidationReason() {
        return invalidationReason;
    }

    public InMemoryTable getTable() {
        return table;
    }

    public void invalidate(String reason) {
        invalidationReason = reason;
    }

    public boolean isDropped() {
        return dropped;
    }

    public boolean isInvalid() {
        return invalidationReason != null;
    }

    /**
     * Signals that the view is being dropped. New readers and refreshes must bail out.
     * The actual free is handled by {@link #tryCloseIfDropped()}, which races drop,
     * refresh, and reader close paths — whichever runs last performs the free.
     */
    public void markAsDropped() {
        dropped = true;
    }

    /**
     * Populates the InMemoryTable from a cursor produced by compiling
     * the live view's SELECT query. Called by LiveViewRefreshJob while
     * the refresh latch is held.
     */
    public void refresh(RecordCursor cursor) {
        lock.writeLock().lock();
        try {
            if (dropped) {
                return;
            }
            table.clear();
            Record record = cursor.getRecord();
            int columnCount = table.getColumnCount();
            while (cursor.hasNext()) {
                for (int i = 0; i < columnCount; i++) {
                    int type = table.getColumnType(i);
                    switch (ColumnType.tagOf(type)) {
                        case ColumnType.INT:
                            table.putInt(i, record.getInt(i));
                            break;
                        case ColumnType.LONG:
                            table.putLong(i, record.getLong(i));
                            break;
                        case ColumnType.TIMESTAMP:
                            table.putLong(i, record.getTimestamp(i));
                            break;
                        case ColumnType.DATE:
                            table.putLong(i, record.getDate(i));
                            break;
                        case ColumnType.DOUBLE:
                            table.putDouble(i, record.getDouble(i));
                            break;
                        case ColumnType.FLOAT:
                            table.putFloat(i, record.getFloat(i));
                            break;
                        case ColumnType.SHORT:
                            table.putShort(i, record.getShort(i));
                            break;
                        case ColumnType.BYTE:
                            table.putByte(i, record.getByte(i));
                            break;
                        case ColumnType.BOOLEAN:
                            table.putBool(i, record.getBool(i));
                            break;
                        case ColumnType.CHAR:
                            table.putChar(i, record.getChar(i));
                            break;
                        case ColumnType.SYMBOL:
                            table.putSymbol(i, record.getSymA(i));
                            break;
                        case ColumnType.STRING:
                            table.putStr(i, record.getStrA(i));
                            break;
                        case ColumnType.VARCHAR:
                            table.putVarchar(i, record.getVarcharA(i));
                            break;
                        case ColumnType.BINARY:
                            table.putBin(i, record.getBin(i));
                            break;
                        default:
                            if (ColumnType.isArray(type)) {
                                table.putArray(i, record.getArray(i, type), type);
                            } else {
                                table.putLong(i, record.getLong(i));
                            }
                            break;
                    }
                }
                table.incrementRowCount();
            }
            table.applyRetention(definition.getRetentionMicros());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void setLastProcessedSeqTxn(long seqTxn) {
        this.lastProcessedSeqTxn = seqTxn;
    }

    /**
     * Non-blocking close: runs only when the view is dropped, the refresh latch is
     * free, and no reader currently holds the read lock. Callers (DROP path, refresh
     * finally hook, reader cursor close, failed {@link #tryLockForRead} drain) race
     * to be the one that frees the table; the loser is a no-op.
     * <p>
     * This is safe because every code path that can observe the instance — running
     * refresh, active reader, or a reader that bailed due to {@code dropped} —
     * invokes this method on its way out, so some thread eventually wins the CAS
     * and the write lock and performs the free.
     */
    public void tryCloseIfDropped() {
        if (!dropped) {
            return;
        }
        if (!refreshLatch.compareAndSet(false, true)) {
            // A refresh is in flight; its finally hook will retry.
            return;
        }
        try {
            if (!lock.writeLock().tryLock()) {
                // A reader is still holding the read lock; that cursor's close() will retry.
                return;
            }
            try {
                if (!isClosed) {
                    isClosed = true;
                    Misc.free(table);
                }
            } finally {
                lock.writeLock().unlock();
            }
        } finally {
            refreshLatch.set(false);
        }
    }

    /**
     * Acquires the read lock for a cursor. Returns false if the view has been
     * dropped, in which case the read lock is released and an opportunistic
     * {@link #tryCloseIfDropped} runs so that a reader racing drop also helps
     * drain the close. Called from {@code LiveViewRecordCursor.open()}.
     */
    public boolean tryLockForRead() {
        lock.readLock().lock();
        if (dropped) {
            lock.readLock().unlock();
            tryCloseIfDropped();
            return false;
        }
        return true;
    }

    /**
     * Acquires the refresh latch for the entire refresh operation
     * (compile + cursor + table copy). Returns false if another refresh,
     * or a {@link #tryCloseIfDropped} call, currently holds it.
     */
    public boolean tryLockForRefresh() {
        return refreshLatch.compareAndSet(false, true);
    }

    public void unlockAfterRead() {
        lock.readLock().unlock();
    }

    public void unlockAfterRefresh() {
        if (!refreshLatch.compareAndSet(true, false)) {
            throw new IllegalStateException("refresh latch is not held");
        }
    }
}
