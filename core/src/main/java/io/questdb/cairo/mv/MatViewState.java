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

import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.TelemetrySystemEvent.*;

/**
 * Mat view refresh state serves the purpose of synchronizing and coordinating
 * {@link MatViewRefreshJob}s.
 * <p>
 * Unlike {@link MatViewStateReader}, it doesn't include invalidation reason
 * string as that field is not needed for refresh jobs.
 */
public class MatViewState implements QuietCloseable {
    public static final String MAT_VIEW_STATE_FILE_NAME = "_mv.s";
    public static final int MAT_VIEW_STATE_FORMAT_EXTRA_PERIOD_MSG_TYPE = 2;
    public static final int MAT_VIEW_STATE_FORMAT_EXTRA_TS_MSG_TYPE = 1;
    public static final int MAT_VIEW_STATE_FORMAT_MSG_TYPE = 0;
    // used to avoid concurrent refresh runs
    private final AtomicBoolean latch = new AtomicBoolean(false);
    // Incremented each time an incremental/full refresh finishes.
    // Used by MatViewTimerJob to avoid queueing redundant refresh tasks.
    private final AtomicLong refreshSeq = new AtomicLong();
    private final MatViewTelemetryFacade telemetryFacade;
    private RecordCursorFactory cursorFactory;
    private volatile boolean dropped;
    private volatile boolean invalid;
    // Stands for last successful period (range) refresh high boundary.
    // Increases monotonically as long as mat view stays valid.
    private volatile long lastPeriodHi = Numbers.LONG_NULL;
    // Stands for last successful incremental refresh base table reader txn.
    // Increases monotonically as long as mat view stays valid.
    private volatile long lastRefreshBaseTxn = -1;
    private volatile long lastRefreshFinishTimestamp = Numbers.LONG_NULL;
    private volatile long lastRefreshStartTimestamp = Numbers.LONG_NULL;
    private volatile boolean pendingInvalidation;
    private long recordRowCopierMetadataVersion;
    private RecordToRowCopier recordToRowCopier;
    private volatile MatViewDefinition viewDefinition;

    public MatViewState(
            @NotNull MatViewDefinition viewDefinition,
            MatViewTelemetryFacade telemetryFacade
    ) {
        this.viewDefinition = viewDefinition;
        this.telemetryFacade = telemetryFacade;
    }

    public static void append(
            long lastRefreshTimestamp,
            long lastRefreshBaseTxn,
            boolean invalid,
            @Nullable CharSequence invalidationReason,
            long lastPeriodHi,
            @NotNull BlockFileWriter writer
    ) {
        AppendableBlock block = writer.append();
        appendState(lastRefreshBaseTxn, invalid, invalidationReason, block);
        block.commit(MAT_VIEW_STATE_FORMAT_MSG_TYPE);
        block = writer.append();
        appendTs(lastRefreshTimestamp, block);
        block.commit(MAT_VIEW_STATE_FORMAT_EXTRA_TS_MSG_TYPE);
        block = writer.append();
        appendPeriodHi(lastPeriodHi, block);
        block.commit(MAT_VIEW_STATE_FORMAT_EXTRA_PERIOD_MSG_TYPE);
        writer.commit();
    }

    // refreshState can be null, in this case "default" record will be written
    public static void append(@Nullable MatViewStateReader refreshState, @NotNull BlockFileWriter writer) {
        if (refreshState != null) {
            append(
                    refreshState.getLastRefreshTimestamp(),
                    refreshState.getLastRefreshBaseTxn(),
                    refreshState.isInvalid(),
                    refreshState.getInvalidationReason(),
                    refreshState.getLastPeriodHi(),
                    writer
            );
        } else {
            append(
                    Numbers.LONG_NULL,
                    -1,
                    false,
                    null,
                    Numbers.LONG_NULL,
                    writer
            );
        }
    }

    // kept public for tests
    public static void appendPeriodHi(long periodHi, @NotNull AppendableBlock block) {
        block.putLong(periodHi);
    }

    // kept public for tests
    public static void appendState(
            long lastRefreshBaseTxn,
            boolean invalid,
            @Nullable CharSequence invalidationReason,
            @NotNull AppendableBlock block
    ) {
        block.putBool(invalid);
        block.putLong(lastRefreshBaseTxn);
        block.putStr(invalidationReason);
    }

    // kept public for tests
    public static void appendTs(long lastRefreshTimestamp, @NotNull AppendableBlock block) {
        block.putLong(lastRefreshTimestamp);
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

    /**
     * Returns high boundary for all complete time periods up to which a period
     * mat view is refreshed.
     * <p>
     * A period view is a mat view that has REFRESH ... PERIOD (LENGTH ...) clause.
     * <p>
     * Each time when a period, e.g. a day, is complete, a range refresh is triggered
     * on the period mat view (unless it has manual refresh type). This range refresh
     * runs for the [lastPeriodHi, completePeriodHi) interval. If the refresh is successful,
     * completePeriodHi is stored as the new lastPeriodHi value.
     */
    public long getLastPeriodHi() {
        return lastPeriodHi;
    }

    /**
     * Returns base table txn read by the last incremental or full refresh.
     * Subsequent incremental refreshes should only refresh base table intervals
     * (think, slices of table partitions) that correspond to later txns.
     */
    public long getLastRefreshBaseTxn() {
        return lastRefreshBaseTxn;
    }

    public long getLastRefreshFinishTimestamp() {
        return lastRefreshFinishTimestamp;
    }

    public long getLastRefreshStartTimestamp() {
        return lastRefreshStartTimestamp;
    }

    public long getRecordRowCopierMetadataVersion() {
        return recordRowCopierMetadataVersion;
    }

    public RecordToRowCopier getRecordToRowCopier() {
        return recordToRowCopier;
    }

    public long getRefreshSeq() {
        return refreshSeq.get();
    }

    public @NotNull MatViewDefinition getViewDefinition() {
        return viewDefinition;
    }

    public void incrementRefreshSeq() {
        refreshSeq.incrementAndGet();
    }

    public void init() {
        telemetryFacade.store(MAT_VIEW_CREATE, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, null, 0);
    }

    public void initFromReader(MatViewStateReader reader) {
        this.invalid = reader.isInvalid();
        this.lastRefreshBaseTxn = reader.getLastRefreshBaseTxn();
        this.lastRefreshFinishTimestamp = reader.getLastRefreshTimestamp();
        this.lastPeriodHi = reader.getLastPeriodHi();
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

    public void markAsInvalid(CharSequence invalidationReason) {
        if (!invalid) {
            telemetryFacade.store(MAT_VIEW_INVALIDATE, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, invalidationReason, 0);
        }
        this.invalid = true;
    }

    public void markAsPendingInvalidation() {
        pendingInvalidation = true;
    }

    public void markAsValid() {
        this.invalid = false;
        this.pendingInvalidation = false;
    }

    public void rangeRefreshSuccess(
            RecordCursorFactory factory,
            RecordToRowCopier copier,
            long recordRowCopierMetadataVersion,
            long refreshFinishedTimestamp,
            long refreshTriggeredTimestamp,
            long periodHi
    ) {
        assert latch.get();
        this.cursorFactory = factory;
        this.recordToRowCopier = copier;
        this.recordRowCopierMetadataVersion = recordRowCopierMetadataVersion;
        this.lastRefreshFinishTimestamp = refreshFinishedTimestamp;
        this.lastPeriodHi = periodHi;
        telemetryFacade.store(
                MAT_VIEW_REFRESH_SUCCESS,
                viewDefinition.getMatViewToken(),
                -1,
                null,
                refreshFinishedTimestamp - refreshTriggeredTimestamp
        );
    }

    public void refreshFail(long refreshTimestamp, CharSequence errorMessage) {
        assert latch.get();
        this.lastRefreshFinishTimestamp = refreshTimestamp;
        markAsInvalid(errorMessage);
        telemetryFacade.store(MAT_VIEW_REFRESH_FAIL, viewDefinition.getMatViewToken(), Numbers.LONG_NULL, errorMessage, 0);
    }

    public void refreshSuccess(
            RecordCursorFactory factory,
            RecordToRowCopier copier,
            long recordRowCopierMetadataVersion,
            long refreshFinishedTimestamp,
            long refreshTriggeredTimestamp,
            long baseTableTxn,
            long periodHi
    ) {
        assert latch.get();
        this.cursorFactory = factory;
        this.recordToRowCopier = copier;
        this.recordRowCopierMetadataVersion = recordRowCopierMetadataVersion;
        this.lastRefreshFinishTimestamp = refreshFinishedTimestamp;
        this.lastRefreshBaseTxn = baseTableTxn;
        this.lastPeriodHi = periodHi;
        telemetryFacade.store(
                MAT_VIEW_REFRESH_SUCCESS,
                viewDefinition.getMatViewToken(),
                baseTableTxn,
                null,
                refreshFinishedTimestamp - refreshTriggeredTimestamp
        );
    }

    public void refreshSuccessNoRows(
            long refreshFinishedTimestamp,
            long refreshTriggeredTimestamp,
            long baseTableTxn,
            long periodHi
    ) {
        assert latch.get();
        this.lastRefreshFinishTimestamp = refreshFinishedTimestamp;
        this.lastRefreshBaseTxn = baseTableTxn;
        this.lastPeriodHi = periodHi;
        telemetryFacade.store(
                MAT_VIEW_REFRESH_SUCCESS,
                viewDefinition.getMatViewToken(),
                baseTableTxn,
                null,
                refreshFinishedTimestamp - refreshTriggeredTimestamp
        );
    }

    public void setLastPeriodHi(long lastPeriodHi) {
        this.lastPeriodHi = lastPeriodHi;
    }

    public void setLastRefreshBaseTableTxn(long txn) {
        lastRefreshBaseTxn = txn;
    }

    public void setLastRefreshStartTimestamp(long ts) {
        lastRefreshStartTimestamp = ts;
    }

    public void setLastRefreshTimestamp(long ts) {
        this.lastRefreshFinishTimestamp = ts;
    }

    public void setViewDefinition(MatViewDefinition viewDefinition) {
        this.viewDefinition = viewDefinition;
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
        if (!latch.compareAndSet(true, false)) {
            throw new IllegalStateException("cannot unlock, not locked");
        }
    }
}
