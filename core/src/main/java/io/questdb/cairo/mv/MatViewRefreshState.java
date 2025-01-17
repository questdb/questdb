/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.mv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.str.StringSink;

import java.util.concurrent.atomic.AtomicBoolean;

public class MatViewRefreshState implements QuietCloseable {
    // protects errorCode and errorSink
    private final SimpleReadWriteLock errorLock = new SimpleReadWriteLock();
    // used to avoid concurrent refresh runs
    private final AtomicBoolean latch = new AtomicBoolean(false);
    private final MatViewDefinition viewDefinition;
    private RecordCursorFactory cursorFactory;
    private volatile boolean dropped;
    private int errorCode;
    private StringSink errorSink;
    private volatile long lastRefreshTimestamp = Numbers.LONG_NULL;
    private long recordRowCopierMetadataVersion;
    private RecordToRowCopier recordToRowCopier;

    public MatViewRefreshState(MatViewDefinition viewDefinition) {
        this.viewDefinition = viewDefinition;
    }

    public RecordCursorFactory acquireRecordFactory() {
        assert latch.get();
        RecordCursorFactory factory = cursorFactory;
        cursorFactory = null;
        return factory;
    }

    @Override
    public void close() {
        cursorFactory = Misc.free(cursorFactory);
    }

    public void compilationFail(SqlException e, long refreshTimestamp) {
        assert latch.get();
        this.lastRefreshTimestamp = refreshTimestamp;
        errorLock.writeLock().lock();
        try {
            errorCode = e.getPosition();
            getErrorSink().put(e.getFlyweightMessage());
        } finally {
            errorLock.writeLock().unlock();
        }
    }

    public void copyError(ErrorHolder holder) {
        errorLock.readLock().lock();
        try {
            holder.errorSink.clear();
            holder.errorSink.put(errorSink);
            holder.errorCode = errorCode;
        } finally {
            errorLock.readLock().unlock();
        }
    }

    public long getLastRefreshTimestamp() {
        return lastRefreshTimestamp;
    }

    public long getRecordRowCopierMetadataVersion() {
        return recordRowCopierMetadataVersion;
    }

    public RecordToRowCopier getRecordToRowCopier() {
        return recordToRowCopier;
    }

    public MatViewDefinition getViewDefinition() {
        return viewDefinition;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void markAsDropped() {
        dropped = true;
    }

    public void refreshFail(Throwable th, long refreshTimestamp) {
        assert latch.get();
        this.lastRefreshTimestamp = refreshTimestamp;
        errorLock.writeLock().lock();
        try {
            if (th instanceof CairoException) {
                errorCode = ((CairoException) th).getErrno();
                getErrorSink().put(((CairoException) th).getFlyweightMessage());
            } else {
                errorCode = -1;
                StringSink sink = getErrorSink();
                sink.put(th.getClass().getSimpleName());
                if (th.getMessage() != null) {
                    sink.put(": ");
                    sink.put(th.getMessage());
                }
            }
        } finally {
            errorLock.writeLock().unlock();
        }
    }

    public void refreshFail(CharSequence errorMessage, long refreshTimestamp) {
        assert latch.get();
        this.lastRefreshTimestamp = refreshTimestamp;
        errorLock.writeLock().lock();
        try {
            errorCode = Integer.MIN_VALUE;
            getErrorSink().put(errorMessage);
        } finally {
            errorLock.writeLock().unlock();
        }
    }

    public void refreshSuccess(
            RecordCursorFactory factory,
            RecordToRowCopier copier,
            long recordRowCopierMetadataVersion,
            long refreshTimestamp
    ) {
        assert latch.get();
        this.cursorFactory = factory;
        this.recordToRowCopier = copier;
        this.recordRowCopierMetadataVersion = recordRowCopierMetadataVersion;
        this.lastRefreshTimestamp = refreshTimestamp;
    }

    public boolean tryLock() {
        return latch.compareAndSet(false, true);
    }

    public void unlock() {
        if (latch.get() && dropped) {
            // Dropped while it was in use.
            close();
        }

        if (!latch.compareAndSet(true, false)) {
            throw new IllegalStateException("cannot unlock, not locked");
        }
    }

    private StringSink getErrorSink() {
        if (errorSink == null) {
            errorSink = new StringSink();
            return errorSink;
        }
        errorSink.clear();
        return errorSink;
    }

    public static class ErrorHolder {
        private final StringSink errorSink = new StringSink();
        private int errorCode;

        public int getErrorCode() {
            return errorCode;
        }

        public CharSequence getErrorMsg() {
            return errorSink;
        }
    }
}
