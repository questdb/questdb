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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.TelemetrySystemEvent.*;

/**
 * Mat view refresh state serves the purpose of synchronizing and coordinating
 * {@link MatViewRefreshJob}s.
 */
public class MatViewState implements ReadableMatViewState, QuietCloseable {
    public static final String MAT_VIEW_STATE_FILE_NAME = "_mv.s";
    public static final int MAT_VIEW_STATE_FORMAT_MSG_TYPE = 0;
    public static final int MAT_VIEW_STATE_FORMAT_EXTRA_TS_MSG_TYPE = 1;

    // used to avoid concurrent refresh runs
    private final AtomicBoolean latch = new AtomicBoolean(false);
    private final MatViewTelemetryFacade telemetryFacade;
    private final MatViewDefinition viewDefinition;
    private RecordCursorFactory cursorFactory;
    private volatile boolean dropped;
    private volatile boolean invalid;
    private volatile String invalidationReason;
    private volatile long lastRefreshBaseTxn = -1;
    private volatile long lastRefreshTimestamp = Numbers.LONG_NULL;
    private volatile boolean pendingInvalidation;
    private long recordRowCopierMetadataVersion;
    private RecordToRowCopier recordToRowCopier;

    public MatViewState(
            @NotNull MatViewDefinition viewDefinition,
            MatViewTelemetryFacade telemetryFacade
    ) {
        this.viewDefinition = viewDefinition;
        this.telemetryFacade = telemetryFacade;
    }

    // refreshState can be null, in this case "default" record will be written
    public static void append(@Nullable ReadableMatViewState refreshState, @NotNull BlockFileWriter writer) {
        final AppendableBlock block = writer.append();
        append(refreshState, block);
        block.commit(MAT_VIEW_STATE_FORMAT_MSG_TYPE);
        final AppendableBlock blockTs = writer.append();
        appendTs(refreshState, blockTs);
        blockTs.commit(MAT_VIEW_STATE_FORMAT_EXTRA_TS_MSG_TYPE);
        writer.commit();
    }

    public static void append(@Nullable ReadableMatViewState refreshState, @NotNull AppendableBlock block) {
        if (refreshState == null) {
            block.putBool(false);
            block.putLong(-1L);
            block.putStr(null);
            return;
        }
        block.putBool(refreshState.isInvalid());
        block.putLong(refreshState.getLastRefreshBaseTxn());
        block.putStr(refreshState.getInvalidationReason());
    }

    public static void appendTs(@Nullable ReadableMatViewState refreshState, @NotNull AppendableBlock block) {
        if (refreshState == null) {
            block.putLong(Numbers.LONG_NULL);
            return;
        }
        block.putLong(refreshState.getLastRefreshTimestamp());
    }

    public static void readFrom(@NotNull BlockFileReader reader, @NotNull MatViewState refreshState) {
        boolean matViewStateBlockFound = false;
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() == MatViewState.MAT_VIEW_STATE_FORMAT_MSG_TYPE) {
                matViewStateBlockFound = true;
                refreshState.invalid = block.getBool(0);
                refreshState.lastRefreshBaseTxn = block.getLong(Byte.BYTES);
                refreshState.invalidationReason = Chars.toString(block.getStr(Long.BYTES + Byte.BYTES));
                refreshState.lastRefreshTimestamp = Numbers.LONG_NULL;
                // keep going, because V2 block might follow
                continue;
            }
            if (block.type() == MatViewState.MAT_VIEW_STATE_FORMAT_EXTRA_TS_MSG_TYPE) {
                refreshState.lastRefreshTimestamp = block.getLong(0);
                return;
            }
        }
        if (!matViewStateBlockFound) {
            final TableToken matViewToken = refreshState.getViewDefinition().getMatViewToken();
            throw CairoException.critical(0).put("cannot read materialized view state, block not found [view=")
                    .put(matViewToken.getTableName())
                    .put(']');
        }
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

    @Nullable
    @Override
    public String getInvalidationReason() {
        return invalidationReason;
    }

    @Override
    public long getLastRefreshBaseTxn() {
        return lastRefreshBaseTxn;
    }

    @Override
    public long getLastRefreshTimestamp() {
        return lastRefreshTimestamp;
    }

    public long getRecordRowCopierMetadataVersion() {
        return recordRowCopierMetadataVersion;
    }

    public RecordToRowCopier getRecordToRowCopier() {
        return recordToRowCopier;
    }

    public @NotNull MatViewDefinition getViewDefinition() {
        return viewDefinition;
    }

    public void init() {
        telemetryFacade.store(MAT_VIEW_CREATE, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, null, 0);
    }

    public boolean isDropped() {
        return dropped;
    }

    @Override
    public boolean isInvalid() {
        return invalid;
    }

    public boolean isLocked() {
        return latch.get();
    }

    public boolean isPendingInvalidation() {
        return pendingInvalidation;
    }

    public void markAsDropped() {
        dropped = true;
        telemetryFacade.store(MAT_VIEW_DROP, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, null, 0);
    }

    public void markAsInvalid(@Nullable String invalidationReason) {
        final boolean wasValid = !invalid;
        final boolean invalidationReasonChanged = Chars.compare(this.invalidationReason, invalidationReason) != 0;
        if (invalidationReasonChanged) {
            this.invalidationReason = invalidationReason;
        }
        this.invalid = true;
        if (wasValid || invalidationReasonChanged) {
            telemetryFacade.store(MAT_VIEW_INVALIDATE, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, invalidationReason, 0);
        }
    }

    public void markAsPendingInvalidation() {
        pendingInvalidation = true;
    }

    public void markAsValid() {
        this.invalid = false;
        this.pendingInvalidation = false;
        this.invalidationReason = null;
    }

    public void refreshFail(long refreshTimestamp, String errorMessage) {
        assert latch.get();
        this.lastRefreshTimestamp = refreshTimestamp;
        markAsInvalid(errorMessage);
        telemetryFacade.store(MAT_VIEW_REFRESH_FAIL, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, errorMessage, 0);
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
        telemetryFacade.store(
                MAT_VIEW_REFRESH_SUCCESS,
                viewDefinition.getMatViewToken(),
                baseTableTxn,
                null,
                refreshTimestamp - refreshTriggeredTimestamp
        );
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

    public void setLastRefreshBaseTableTxn(long txn) {
        lastRefreshBaseTxn = txn;
    }
}
