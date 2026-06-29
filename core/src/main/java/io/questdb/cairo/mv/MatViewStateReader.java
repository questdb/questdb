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

package io.questdb.cairo.mv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Contains all materialized view refresh state fields, including
 * invalidation reason string.
 */
public class MatViewStateReader implements Mutable {
    private final StringSink invalidationReason = new StringSink();
    private final LongList refreshIntervals = new LongList();
    private long backfillFrontier = Long.MIN_VALUE;
    private long frozenBoundaryFloor = Numbers.LONG_NULL;
    private boolean invalid;
    private long lastPeriodHi = Numbers.LONG_NULL;
    private long lastRefreshBaseTxn = -1;
    private long lastRefreshTimestampUs = Numbers.LONG_NULL;
    private long refreshIntervalsBaseTxn = -1;

    @Override
    public void clear() {
        invalid = false;
        invalidationReason.clear();
        lastRefreshBaseTxn = -1;
        lastRefreshTimestampUs = Numbers.LONG_NULL;
        lastPeriodHi = Numbers.LONG_NULL;
        refreshIntervalsBaseTxn = -1;
        refreshIntervals.clear();
        backfillFrontier = Long.MIN_VALUE;
        frozenBoundaryFloor = Numbers.LONG_NULL;
    }

    public long getBackfillFrontier() {
        return backfillFrontier;
    }

    public long getFrozenBoundaryFloor() {
        return frozenBoundaryFloor;
    }

    @Nullable
    public CharSequence getInvalidationReason() {
        return invalidationReason.length() > 0 ? invalidationReason : null;
    }

    public long getLastPeriodHi() {
        return lastPeriodHi;
    }

    public long getLastRefreshBaseTxn() {
        return lastRefreshBaseTxn;
    }

    public long getLastRefreshTimestampUs() {
        return lastRefreshTimestampUs;
    }

    public LongList getRefreshIntervals() {
        return refreshIntervals;
    }

    public long getRefreshIntervalsBaseTxn() {
        return refreshIntervalsBaseTxn;
    }

    public boolean isInvalid() {
        return invalid;
    }

    public MatViewStateReader of(@NotNull WalEventCursor.MatViewDataInfo info) {
        invalid = false;
        invalidationReason.clear();
        lastRefreshBaseTxn = info.getLastRefreshBaseTableTxn();
        lastRefreshTimestampUs = info.getLastRefreshTimestampUs();
        lastPeriodHi = info.getLastPeriodHi();
        // Mat view data commit means that cached intervals were applied and should be evicted.
        refreshIntervalsBaseTxn = -1;
        refreshIntervals.clear();
        // WAL events don't carry the frozen-zone runtime state; reset to defaults so the
        // caller (WalUtils.readMatViewState) can overlay it from the _mv.s state file.
        backfillFrontier = Long.MIN_VALUE;
        frozenBoundaryFloor = Numbers.LONG_NULL;
        return this;
    }

    public MatViewStateReader of(@NotNull WalEventCursor.MatViewInvalidationInfo info) {
        invalid = info.isInvalid();
        invalidationReason.clear();
        invalidationReason.put(info.getInvalidationReason());
        lastRefreshBaseTxn = info.getLastRefreshBaseTableTxn();
        lastRefreshTimestampUs = info.getLastRefreshTimestampUs();
        lastPeriodHi = info.getLastPeriodHi();
        refreshIntervalsBaseTxn = info.getRefreshIntervalsBaseTxn();
        refreshIntervals.clear();
        refreshIntervals.addAll(info.getRefreshIntervals());
        // WAL events don't carry the frozen-zone runtime state; reset to defaults so the
        // caller (WalUtils.readMatViewState) can overlay it from the _mv.s state file.
        backfillFrontier = Long.MIN_VALUE;
        frozenBoundaryFloor = Numbers.LONG_NULL;
        return this;
    }

    public MatViewStateReader of(
            @NotNull BlockFileReader reader,
            @NotNull TableToken matViewToken
    ) {
        // Reset first: a reused reader must not carry later-block fields (lastPeriodHi,
        // refreshIntervalsBaseTxn, refreshIntervals) from a previously-read view into a state file that
        // omits those blocks (a pre-intervals-block legacy file). The of(MatViewDataInfo) and
        // of(MatViewInvalidationInfo) siblings already fully repopulate; this block-file path must too.
        clear();
        boolean matViewStateBlockFound = false;
        // Default the frozen-zone fields: state files written before the frozen-zone feature
        // (and freshly-created views) have no block of this type.
        backfillFrontier = Long.MIN_VALUE;
        frozenBoundaryFloor = Numbers.LONG_NULL;
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() == MatViewState.MAT_VIEW_STATE_FORMAT_MSG_TYPE) {
                matViewStateBlockFound = true;
                invalid = block.getBool(0);
                lastRefreshBaseTxn = block.getLong(Byte.BYTES);
                invalidationReason.clear();
                invalidationReason.put(block.getStr(Long.BYTES + Byte.BYTES));
                lastRefreshTimestampUs = Numbers.LONG_NULL;
                // keep going, because V2/V3 block might follow
                continue;
            }
            if (block.type() == MatViewState.MAT_VIEW_STATE_FORMAT_EXTRA_TS_MSG_TYPE) {
                lastRefreshTimestampUs = block.getLong(0);
                // keep going, because V3 block might follow
                continue;
            }
            if (block.type() == MatViewState.MAT_VIEW_STATE_FORMAT_EXTRA_PERIOD_MSG_TYPE) {
                lastPeriodHi = block.getLong(0);
                // keep going, because V4 block might follow
                continue;
            }
            if (block.type() == MatViewState.MAT_VIEW_STATE_FORMAT_EXTRA_INTERVALS_MSG_TYPE) {
                long offset = 0;
                refreshIntervalsBaseTxn = block.getLong(offset);
                offset += Long.BYTES;
                final int intervalsLen = block.getInt(offset);
                offset += Integer.BYTES;
                refreshIntervals.clear();
                for (int i = 0; i < intervalsLen; i++) {
                    refreshIntervals.add(block.getLong(offset));
                    offset += Long.BYTES;
                }
                // keep going, because the frozen-zone block might follow
                continue;
            }
            if (block.type() == MatViewState.MAT_VIEW_STATE_FORMAT_EXTRA_FROZEN_MSG_TYPE) {
                backfillFrontier = block.getLong(0);
                frozenBoundaryFloor = block.getLong(Long.BYTES);
            }
        }
        if (!matViewStateBlockFound) {
            throw CairoException.critical(0).put("cannot read materialized view state, block not found [view=")
                    .put(matViewToken.getTableName())
                    .put(']');
        }
        return this;
    }

    /**
     * Overlays only the frozen-zone runtime fields (backfill frontier / boundary floor) from the
     * {@code _mv.s} state file, leaving the refresh-state fields untouched. Used by
     * {@link io.questdb.cairo.wal.WalUtils#readMatViewState} when the refresh state was reconstructed
     * from a WAL event (which does not carry the frozen-zone fields) so the frontier still survives a
     * restart. Resets the two fields to their defaults when the file has no frozen-zone block.
     */
    public void ofFrozenZoneOverlay(@NotNull BlockFileReader reader) {
        backfillFrontier = Long.MIN_VALUE;
        frozenBoundaryFloor = Numbers.LONG_NULL;
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() == MatViewState.MAT_VIEW_STATE_FORMAT_EXTRA_FROZEN_MSG_TYPE) {
                backfillFrontier = block.getLong(0);
                frozenBoundaryFloor = block.getLong(Long.BYTES);
                return;
            }
        }
    }
}
