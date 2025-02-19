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
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.TelemetrySystemEvent.*;

public class MatViewRefreshState implements QuietCloseable {
    public static final String MAT_VIEW_STATE_FILE_NAME = "_mv.s";
    public static final int MAT_VIEW_STATE_FORMAT_MSG_TYPE = 0;

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

    public MatViewRefreshState(
            MatViewDefinition viewDefinition,
            boolean invalid,
            MatViewTelemetryFacade telemetryFacade
    ) {
        this.viewDefinition = viewDefinition;
        this.telemetryFacade = telemetryFacade;
        this.invalid = invalid;
    }

    // refreshState can be null, in this case "default" record will be written
    public static void append(@Nullable MatViewRefreshState refreshState, @NotNull BlockFileWriter writer) {
        final AppendableBlock block = writer.append();
        append(refreshState, block);
        block.commit(MAT_VIEW_STATE_FORMAT_MSG_TYPE);
        writer.commit();
    }

    public static void readFrom(@NotNull BlockFileReader reader, @NotNull MatViewRefreshState refreshState) {
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        // Iterate through the block until we find the one we recognize.
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() != MAT_VIEW_STATE_FORMAT_MSG_TYPE) {
                // Unknown block, skip.
                continue;
            }
            refreshState.invalid = block.getBool(0);
            refreshState.lastRefreshBaseTxn = block.getLong(Byte.BYTES);
            refreshState.invalidationReason = Chars.toString(block.getStr(Long.BYTES + Byte.BYTES));
            return;
        }
        final TableToken matViewToken = refreshState.getViewDefinition() != null ? refreshState.getViewDefinition().getMatViewToken() : null;
        throw CairoException.critical(0).put("cannot read materialized view state, block not found [view=")
                .put(matViewToken != null ? matViewToken.getTableName() : "N/A")
                .put(']');
    }

    public static void append(@Nullable MatViewRefreshState refreshState, @NotNull AppendableBlock block) {
        if (refreshState == null) {
            block.putBool(false);
            block.putLong(-1);
            block.putStr(null);
            return;
        }
        block.putBool(refreshState.isInvalid());
        block.putLong(refreshState.lastRefreshBaseTxn);
        block.putStr(refreshState.getInvalidationReason());
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
    public String getInvalidationReason() {
        return invalidationReason;
    }

    public long getLastRefreshBaseTxn() {
        return lastRefreshBaseTxn;
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
        telemetryFacade.store(MAT_VIEW_CREATE, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, null, 0);
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

    public boolean isPendingInvalidation() {
        return pendingInvalidation;
    }

    public void markAsDropped() {
        dropped = true;
        telemetryFacade.store(MAT_VIEW_DROP, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, null, 0);
    }

    public void markAsInvalid(@NotNull BlockFileWriter blockFileWriter, @NotNull Path dbRoot, @Nullable CharSequence invalidationReason) {
        final boolean wasValid = !invalid;
        final boolean invalidationReasonChanged = Chars.compare(this.invalidationReason, invalidationReason) != 0;
        if (invalidationReasonChanged) {
            this.invalidationReason = Chars.toString(invalidationReason);
        }
        this.invalid = true;
        if (wasValid || invalidationReasonChanged) {
            updateInvalidationStatus(blockFileWriter, dbRoot);
            telemetryFacade.store(MAT_VIEW_INVALIDATE, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, invalidationReason, 0);
        }
    }

    public void markAsPendingInvalidation() {
        pendingInvalidation = true;
    }

    public void markAsValid(@NotNull BlockFileWriter blockFileWriter, @NotNull Path dbRoot) {
        boolean wasInvalid = invalid;
        this.invalid = false;
        this.pendingInvalidation = false;
        this.invalidationReason = null;
        if (wasInvalid) {
            updateInvalidationStatus(blockFileWriter, dbRoot);
        }
    }

    public void refreshFail(@NotNull BlockFileWriter blockFileWriter, @NotNull Path dbRoot, long refreshTimestamp, CharSequence errorMessage) {
        assert latch.get();
        markAsInvalid(blockFileWriter, dbRoot, errorMessage);
        this.lastRefreshTimestamp = refreshTimestamp;
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

    public void writeLastRefreshBaseTableTxn(@NotNull BlockFileWriter blockFileWriter, @NotNull Path dbRoot, long txn) {
        if (lastRefreshBaseTxn != txn) {
            lastRefreshBaseTxn = txn;
            dbRoot
                    .concat(getViewDefinition().getMatViewToken())
                    .concat(MatViewRefreshState.MAT_VIEW_STATE_FILE_NAME);
            try (blockFileWriter) {
                blockFileWriter.of(dbRoot.$());
                MatViewRefreshState.append(this, blockFileWriter);
            }
        }
    }

    private void updateInvalidationStatus(@NotNull BlockFileWriter blockFileWriter, @NotNull Path dbRoot) {
        dbRoot
                .concat(getViewDefinition().getMatViewToken())
                .concat(MatViewRefreshState.MAT_VIEW_STATE_FILE_NAME);
        try (blockFileWriter) {
            blockFileWriter.of(dbRoot.$());
            MatViewRefreshState.append(this, blockFileWriter);
        }
    }
}
