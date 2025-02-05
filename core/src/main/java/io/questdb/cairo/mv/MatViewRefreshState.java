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

import io.questdb.cairo.meta.MetaFileWriter;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.TelemetrySystemEvent.*;

public class MatViewRefreshState implements QuietCloseable {
    // used to avoid concurrent refresh runs
    private final AtomicBoolean latch = new AtomicBoolean(false);
    private final MatViewTelemetryFacade telemetryFacade;
    private final MatViewDefinition viewDefinition;
    private RecordCursorFactory cursorFactory;
    private volatile boolean dropped;
    private volatile boolean invalid;
    private volatile long lastRefreshTimestamp = Numbers.LONG_NULL;
    private long recordRowCopierMetadataVersion;
    private RecordToRowCopier recordToRowCopier;

    public MatViewRefreshState(MatViewDefinition viewDefinition, MatViewTelemetryFacade telemetryFacade) {
        this.viewDefinition = viewDefinition;
        this.telemetryFacade = telemetryFacade;
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

    public void init() {
        telemetryFacade.store(MAT_VIEW_CREATE, viewDefinition.getMatViewToken(), -1L, null, 0L);
    }

    public boolean isDropped() {
        return dropped;
    }

    public boolean isInvalid() {
        return invalid;
    }

    public boolean isLocked() {
        return latch.get();
    }

    public void markAsDropped() {
        dropped = true;
        telemetryFacade.store(MAT_VIEW_DROP, viewDefinition.getMatViewToken(), -1L, null, 0L);
    }

    public void markAsInvalid(final MetaFileWriter metaFileWriter, final Path dbRoot) {
        if (!invalid) {
            dbRoot
                    .concat(getViewDefinition().getMatViewToken())
                    .concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME);
            metaFileWriter.of(dbRoot.$());
            MatViewDefinition.dumpTo(metaFileWriter, getViewDefinition());
            //TODO(eugene): update invalid field, should it be part of the MatViewDefinition?
        }
        invalid = true;
        telemetryFacade.store(MAT_VIEW_INVALIDATE, viewDefinition.getMatViewToken(), -1L, null, 0L);
    }

    public void markAsValid(final MetaFileWriter metaFileWriter, final Path dbRoot) {
        if (invalid) {
            //TODO: update file
        }
        invalid = false;
    }

    public void refreshFail(final MetaFileWriter metaFileWriter, final Path dbRoot, long refreshTimestamp, CharSequence errorMessage) {
        assert latch.get();
        markAsInvalid(metaFileWriter, dbRoot);
        lastRefreshTimestamp = refreshTimestamp;
        telemetryFacade.store(MAT_VIEW_REFRESH_FAIL, viewDefinition.getMatViewToken(), -1L, errorMessage, 0L);
    }

    public void refreshSuccess(
            RecordCursorFactory factory,
            RecordToRowCopier copier,
            long recordRowCopierMetadataVersion,
            long refreshTimestamp,
            long refreshTriggeredTimestamp,
            long baseTableTxn
    ) {
        assert latch.get();
        this.cursorFactory = factory;
        this.recordToRowCopier = copier;
        this.recordRowCopierMetadataVersion = recordRowCopierMetadataVersion;
        this.lastRefreshTimestamp = refreshTimestamp;
        telemetryFacade.store(MAT_VIEW_REFRESH_SUCCESS, viewDefinition.getMatViewToken(), baseTableTxn, null, refreshTimestamp - refreshTriggeredTimestamp);
    }

    public void tryCloseIfDropped() {
        if (dropped && tryLock()) {
            try {
                close();
            } finally {
                unlock();
            }
        }
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
}
