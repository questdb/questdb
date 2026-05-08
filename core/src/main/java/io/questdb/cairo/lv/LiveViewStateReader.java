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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.vm.Vm;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Reader-side view of {@code _lv.s} CORE_STATE block.
 * <p>
 * Mirrors {@link io.questdb.cairo.mv.MatViewStateReader}: a mutable snapshot
 * populated either by reading the file or by direct setters during refresh.
 * <p>
 * Phase 1 fields:
 * <ul>
 *     <li>{@code invalid} / {@code invalidationReason} / {@code invalidationTimestampUs} —
 *     unified invalidation path</li>
 *     <li>{@code subscribeFromSeqTxn} — set at CREATE; the refresh worker starts at this
 *     base seqTxn</li>
 *     <li>{@code lastProcessedSeqTxn} — highest base seqTxn the refresh worker has
 *     consumed (may be ahead of {@code lvConsumedSeqTxn} when output rows are buffered
 *     in-memory but not yet flushed)</li>
 *     <li>{@code appliedWatermark} — T_w on the live view's WAL; reconstructable from
 *     {@code _txn} but persisted for catalogue / restart speed</li>
 *     <li>{@code lvConsumedSeqTxn} — WAL purge floor this view publishes</li>
 * </ul>
 */
public class LiveViewStateReader implements Mutable {
    private final StringSink invalidationReason = new StringSink();
    // Read lock-free by LiveViewsFunctionFactory (catalogue cursor) and by sibling refresh
    // worker code paths; written by the refresh worker. Volatile so lock-free readers see
    // a published value rather than a torn long.
    private volatile long appliedWatermark = -1L;
    private boolean invalid;
    private long invalidationTimestampUs = Numbers.LONG_NULL;
    // Same lock-free-read pattern as appliedWatermark. Refresh worker advances this after
    // committing the live view's WAL block; LiveViewsFunctionFactory exposes it.
    private volatile long lastProcessedSeqTxn = -1L;
    // Read lock-free by WalPurgeJob; writes are guarded by synchronized (LiveViewInstance)
    // in advanceLiveViewConsumedSeqTxn. Volatile so the lock-free read sees a published value.
    private volatile long lvConsumedSeqTxn = -1L;
    private long subscribeFromSeqTxn = -1L;

    @Override
    public void clear() {
        invalid = false;
        invalidationReason.clear();
        invalidationTimestampUs = Numbers.LONG_NULL;
        subscribeFromSeqTxn = -1L;
        lastProcessedSeqTxn = -1L;
        appliedWatermark = -1L;
        lvConsumedSeqTxn = -1L;
    }

    public long getAppliedWatermark() {
        return appliedWatermark;
    }

    @Nullable
    public CharSequence getInvalidationReason() {
        return invalidationReason.length() > 0 ? invalidationReason : null;
    }

    public long getInvalidationTimestampUs() {
        return invalidationTimestampUs;
    }

    public long getLastProcessedSeqTxn() {
        return lastProcessedSeqTxn;
    }

    public long getLvConsumedSeqTxn() {
        return lvConsumedSeqTxn;
    }

    public long getSubscribeFromSeqTxn() {
        return subscribeFromSeqTxn;
    }

    public boolean isInvalid() {
        return invalid;
    }

    /**
     * Populates this reader from {@code _lv.s} block file data. Throws
     * {@link CairoException} if the required CORE_STATE block is absent.
     */
    public LiveViewStateReader of(
            @NotNull BlockFileReader reader,
            @NotNull TableToken liveViewToken
    ) {
        boolean coreBlockFound = false;
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() == LiveViewState.LIVE_VIEW_STATE_CORE_MSG_TYPE) {
                coreBlockFound = true;
                long offset = 0;
                invalid = block.getBool(offset);
                offset += Byte.BYTES;
                invalidationReason.clear();
                CharSequence reasonCs = block.getStr(offset);
                if (reasonCs != null) {
                    invalidationReason.put(reasonCs);
                }
                offset += Vm.getStorageLength(reasonCs);
                invalidationTimestampUs = block.getLong(offset);
                offset += Long.BYTES;
                subscribeFromSeqTxn = block.getLong(offset);
                offset += Long.BYTES;
                lastProcessedSeqTxn = block.getLong(offset);
                offset += Long.BYTES;
                appliedWatermark = block.getLong(offset);
                offset += Long.BYTES;
                lvConsumedSeqTxn = block.getLong(offset);
                return this;
            }
        }
        if (!coreBlockFound) {
            throw CairoException.critical(0)
                    .put("cannot read live view state, block not found [view=")
                    .put(liveViewToken.getTableName()).put(']');
        }
        return this;
    }

    public LiveViewStateReader setAppliedWatermark(long appliedWatermark) {
        this.appliedWatermark = appliedWatermark;
        return this;
    }

    public LiveViewStateReader setInvalid(boolean invalid) {
        this.invalid = invalid;
        return this;
    }

    public LiveViewStateReader setInvalidationReason(@Nullable CharSequence reason) {
        invalidationReason.clear();
        if (reason != null) {
            invalidationReason.put(reason);
        }
        return this;
    }

    public LiveViewStateReader setInvalidationTimestampUs(long invalidationTimestampUs) {
        this.invalidationTimestampUs = invalidationTimestampUs;
        return this;
    }

    public LiveViewStateReader setLastProcessedSeqTxn(long lastProcessedSeqTxn) {
        this.lastProcessedSeqTxn = lastProcessedSeqTxn;
        return this;
    }

    public LiveViewStateReader setLvConsumedSeqTxn(long lvConsumedSeqTxn) {
        this.lvConsumedSeqTxn = lvConsumedSeqTxn;
        return this;
    }

    public LiveViewStateReader setSubscribeFromSeqTxn(long subscribeFromSeqTxn) {
        this.subscribeFromSeqTxn = subscribeFromSeqTxn;
        return this;
    }
}
