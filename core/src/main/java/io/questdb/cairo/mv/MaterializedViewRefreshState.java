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
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.StringSink;

import java.util.concurrent.atomic.AtomicBoolean;

public class MaterializedViewRefreshState implements QuietCloseable {
    private final AtomicBoolean locked = new AtomicBoolean(false);
    private final AtomicBoolean newNotification = new AtomicBoolean();
    private RecordCursorFactory cursorFactory;
    private StringSink error;
    private int errorCode;
    private long lastRefreshRowCount;
    private long recordRowCopierMetadataVersion;
    private RecordToRowCopier recordToRowCopier;

    @Override
    public void close() {
        Misc.free(cursorFactory);
    }

    public void compilationFail(SqlException e) {
        getSink().put(e.getFlyweightMessage());
        errorCode = e.getPosition();
    }

    public RecordCursorFactory acquireRecordFactory() {
        RecordCursorFactory factory = cursorFactory;
        cursorFactory = null;
        return factory;
    }

    public long getRecordRowCopierMetadataVersion() {
        return recordRowCopierMetadataVersion;
    }

    public RecordToRowCopier getRecordToRowCopier() {
        return recordToRowCopier;
    }

    public boolean notifyTxnApplied(long seqTxn) {
        return newNotification.compareAndSet(false, true);
    }

    public void refreshFail(Throwable th) {
        if (th instanceof CairoException) {
            getSink().put(((CairoException) th).getFlyweightMessage());
            errorCode = ((CairoException) th).getErrno();
        } else {
            errorCode = -1;
            StringSink sink = getSink();
            sink.put(th.getClass().getSimpleName());
            if (th.getMessage() != null) {
                sink.put(": ");
                sink.put(th.getMessage());
            }
        }
    }

    public void refreshSuccess(RecordCursorFactory factory, RecordToRowCopier copier, long recordRowCopierMetadataVersion, long rowCount) {
        this.cursorFactory = factory;
        this.recordToRowCopier = copier;
        this.recordRowCopierMetadataVersion = recordRowCopierMetadataVersion;
        this.lastRefreshRowCount = rowCount;
    }

    public boolean tryLock() {
        return locked.compareAndSet(false, true);
    }

    public void unlock() {
        if (!locked.compareAndSet(true, false)) {
            throw new IllegalStateException("cannot unlock, not locked");
        }
    }

    private StringSink getSink() {
        if (error == null) {
            error = new StringSink();
            return error;
        }
        error.clear();
        return error;
    }
}
