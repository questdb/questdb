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
import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Reader-side view of {@code _lv.s} CORE_STATE block.
 * <p>
 * Mirrors {@link io.questdb.cairo.mv.MatViewStateReader}: a mutable snapshot
 * populated either by reading the file or by direct setters during refresh.
 * <p>
 * Core fields:
 * <ul>
 *     <li>{@code invalid} / {@code invalidationReason} / {@code invalidationTimestampUs} —
 *     unified invalidation path</li>
 *     <li>{@code subscribeFromSeqTxn} — set at CREATE; the refresh worker starts at this
 *     base seqTxn</li>
 *     <li>{@code lastProcessedSeqTxn} — highest base seqTxn the refresh worker has
 *     consumed (may be ahead of {@code lvConsumedSeqTxn} when output rows are buffered
 *     in-memory but not yet flushed)</li>
 *     <li>{@code appliedWatermark} — T_w expressed as a <em>base</em> table seqTxn (the
 *     highest base seqTxn whose output has been applied to the LV's on-disk tier), not a
 *     live-view seqTxn; reconstructable from {@code _txn} but persisted for catalogue /
 *     restart speed. The head-checkpoint {@code <lvSeqTxn>.cp} key sits in the <em>same</em>
 *     base-seqTxn space despite its name: {@code maybeWriteHeadCheckpoint} stamps it from
 *     the same {@code advanceTo}, which is why {@code LiveViewRecovery} compares the parsed
 *     {@code .cp} key directly against {@code appliedWatermark}. The genuinely LV-space
 *     seqTxn is a different field entirely - the tier read fence
 *     ({@code LiveViewInMemoryBuffer.lvSeqTxn}, stamped from the LV table reader's
 *     {@code getSeqTxn()})</li>
 *     <li>{@code lvConsumedSeqTxn} — WAL purge floor this view publishes</li>
 * </ul>
 */
public class LiveViewStateReader implements Mutable {
    // Read lock-free by LiveViewsFunctionFactory (catalogue cursor) and by sibling refresh
    // worker code paths; written by the refresh worker. Volatile so lock-free readers see
    // a published value rather than a torn long.
    private volatile long appliedWatermark = -1L;
    // Defaults to BACKFILL_STATE_ACTIVE / Numbers.LONG_NULL; a BACKFILL view sets
    // BACKFILL_STATE_BACKFILLING and the target seqTxn while its sweep runs. Both
    // fields are preallocated in CORE_STATE so BACKFILL needed no _lv.s schema bump.
    // Volatile: the catalogue cursor derives the lifecycle state (getLifecycleState ->
    // BACKFILLING) and reads backfill_target_seqtxn lock-free while the refresh worker
    // advances them under synchronized(instance).
    private volatile byte backfillState = LiveViewState.BACKFILL_STATE_ACTIVE;
    private volatile long backfillTargetSeqTxn = Numbers.LONG_NULL;
    // invalid + invalidationReason + invalidationTimestampUs form the invalidation triple.
    // The invalidation writer mutates them under synchronized(instance); the catalogue
    // cursor reads invalid (via getLifecycleState) and invalidationReason lock-free.
    // invalidationReason is an immutable String published through the volatile so a
    // lock-free reader can never observe a torn or half-cleared value (a mutable
    // StringSink read concurrently with clear()+put() could throw AIOOBE, failing the
    // whole live_views() query). Null means "no reason".
    private volatile boolean invalid;
    private volatile String invalidationReason;
    private volatile long invalidationTimestampUs = Numbers.LONG_NULL;
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
        invalidationReason = null;
        invalidationTimestampUs = Numbers.LONG_NULL;
        subscribeFromSeqTxn = -1L;
        lastProcessedSeqTxn = -1L;
        appliedWatermark = -1L;
        lvConsumedSeqTxn = -1L;
        backfillState = LiveViewState.BACKFILL_STATE_ACTIVE;
        backfillTargetSeqTxn = Numbers.LONG_NULL;
    }

    public long getAppliedWatermark() {
        return appliedWatermark;
    }

    public byte getBackfillState() {
        return backfillState;
    }

    public long getBackfillTargetSeqTxn() {
        return backfillTargetSeqTxn;
    }

    @Nullable
    public CharSequence getInvalidationReason() {
        // Immutable snapshot published via the volatile: a lock-free catalogue read
        // gets a stable String (or null), never a mid-clear()+put() torn value.
        return invalidationReason;
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
                int onDiskVersion = block.getInt(offset);
                if (onDiskVersion > LiveViewState.LIVE_VIEW_STATE_FORMAT_VERSION) {
                    throw CairoException.critical(CairoException.LV_FILE_VERSION_UNSUPPORTED)
                            .put("live view state format version not supported [view=")
                            .put(liveViewToken.getTableName())
                            .put(", onDiskVersion=").put(onDiskVersion)
                            .put(", supportedVersion=").put(LiveViewState.LIVE_VIEW_STATE_FORMAT_VERSION)
                            .put(']');
                }
                offset += Integer.BYTES;
                invalid = block.getBool(offset);
                offset += Byte.BYTES;
                CharSequence reasonCs = block.getStr(offset);
                invalidationReason = (reasonCs == null || reasonCs.length() == 0) ? null : Chars.toString(reasonCs);
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
                offset += Long.BYTES;
                backfillState = block.getByte(offset);
                offset += Byte.BYTES;
                backfillTargetSeqTxn = block.getLong(offset);
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

    public LiveViewStateReader setBackfillState(byte backfillState) {
        this.backfillState = backfillState;
        return this;
    }

    public LiveViewStateReader setBackfillTargetSeqTxn(long backfillTargetSeqTxn) {
        this.backfillTargetSeqTxn = backfillTargetSeqTxn;
        return this;
    }

    public LiveViewStateReader setInvalid(boolean invalid) {
        this.invalid = invalid;
        return this;
    }

    public LiveViewStateReader setInvalidationReason(@Nullable CharSequence reason) {
        // Materialise an immutable copy: the caller may hand us a reusable sink or a
        // block-file flyweight, and lock-free readers must see a stable value.
        invalidationReason = (reason == null || reason.length() == 0) ? null : Chars.toString(reason);
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
