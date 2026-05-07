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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runtime representation of a Phase 1 live view.
 * <p>
 * Replaces the prototype's merge-buffer / cold-path state with the
 * disk-only RFC 123 Phase 1 surface:
 * <ul>
 *     <li>Lifecycle is derived from registry visibility + {@link #stateReader}.invalid;
 *         see {@link LiveViewLifecycleState}.</li>
 *     <li>The slow-path-only N=2 in-memory tier ({@link DoubleBufferedTable}) caches
 *         recent flushed rows for low-latency reads. The disk-side data is the live view's
 *         own WAL-backed table read through {@code TableReader}.</li>
 *     <li>{@link LiveViewStateReader} mirrors the durable contents of {@code _lv.s} —
 *         {@code invalid}, {@code subscribeFromSeqTxn}, {@code lastProcessedSeqTxn},
 *         {@code appliedWatermark}, {@code lvConsumedSeqTxn}. The instance exposes the
 *         reader; refresh / lifecycle code rewrites the file via
 *         {@link io.questdb.cairo.lv.LiveViewState#append}.</li>
 *     <li>{@code dependencyColumnIndexes} captures base-table writer indexes the
 *         compiled SELECT depends on. {@code ApplyWal2TableJob}'s schema-change hook
 *         consults this set to decide whether a base-table column change forces
 *         {@code markInvalid}. Populated at CREATE.</li>
 * </ul>
 * <p>
 * V1 omits BACKFILLING / per-window-state Maps / TableWriter ownership — those land
 * in later phases. The TableWriter for live-view-internal apply is acquired from the
 * engine's WAL writer pool per FLUSH cycle in V1 (Phase 1 keeps things stateless on
 * the instance side).
 */
public class LiveViewInstance implements QuietCloseable {
    private final DoubleBufferedTable bufferedTable = new DoubleBufferedTable();
    private final LiveViewDefinition definition;
    private final IntList dependencyColumnIndexes = new IntList();
    private final AtomicBoolean refreshLatch = new AtomicBoolean(false);
    private final LiveViewStateReader stateReader = new LiveViewStateReader();
    // Cached compiled factory. Window functions carry per-row state, so refresh must
    // reuse the same factory across calls. Accessed only while the refresh latch is held.
    private RecordCursorFactory compiledFactory;
    private volatile boolean dropped;
    private volatile boolean isClosed;
    // Last refresh-worker tick wall-clock (micros). Used by catalogue / lag metrics.
    private volatile long lastRefreshTimeUs = Numbers.LONG_NULL;
    // Live-view's own table token. Populated at construction.
    private final TableToken liveViewToken;
    // Cached RecordToRowCopier (compiled bytecode bridging the SELECT cursor's record
    // shape to the LV's WalWriter row). Invalidated when the WalWriter's metadata version
    // moves past recordRowCopierMetadataVersion. Accessed only while the refresh latch is held.
    private RecordToRowCopier recordToRowCopier;
    private long recordRowCopierMetadataVersion = -1;

    public LiveViewInstance(LiveViewDefinition definition, TableToken liveViewToken) {
        this.definition = definition;
        this.liveViewToken = liveViewToken;
        this.bufferedTable.init(definition.getMetadata());
    }

    public void abortWriteBuffer(InMemoryTable writeBuffer) {
        bufferedTable.abortWrite(writeBuffer);
    }

    /**
     * Returns the currently-published in-memory buffer pinned for the caller, or
     * {@code null} when the view has been dropped. Callers must release with
     * {@link #releaseAfterRead}.
     */
    public InMemoryTable acquireForRead() {
        if (dropped) {
            tryCloseIfDropped();
            return null;
        }
        return bufferedTable.acquire();
    }

    @Override
    public void close() {
        // Shutdown path only — called from CairoEngine.close after all workers stopped.
        dropped = true;
        if (!isClosed) {
            isClosed = true;
            Misc.free(bufferedTable);
            compiledFactory = Misc.free(compiledFactory);
        }
    }

    public RecordCursorFactory getCompiledFactory() {
        return compiledFactory;
    }

    public LiveViewDefinition getDefinition() {
        return definition;
    }

    public IntList getDependencyColumnIndexes() {
        return dependencyColumnIndexes;
    }

    public CharSequence getInvalidationReason() {
        return stateReader.getInvalidationReason();
    }

    public long getLastProcessedSeqTxn() {
        return stateReader.getLastProcessedSeqTxn();
    }

    public long getLastRefreshTimeUs() {
        return lastRefreshTimeUs;
    }

    public LiveViewLifecycleState getLifecycleState() {
        return LiveViewLifecycleState.derive(
                !dropped && !isClosed,
                dropped,
                refreshLatch.get(),
                stateReader.isInvalid()
        );
    }

    public TableToken getLiveViewToken() {
        return liveViewToken;
    }

    public long getRecordRowCopierMetadataVersion() {
        return recordRowCopierMetadataVersion;
    }

    public RecordToRowCopier getRecordToRowCopier() {
        return recordToRowCopier;
    }

    public LiveViewStateReader getStateReader() {
        return stateReader;
    }

    /**
     * Records the in-memory state-mirroring fields from a freshly-loaded {@code _lv.s}
     * snapshot. Called at startup after the file is read.
     */
    public void initFromState(@NotNull LiveViewStateReader source) {
        stateReader.setInvalid(source.isInvalid());
        stateReader.setInvalidationReason(source.getInvalidationReason());
        stateReader.setInvalidationTimestampUs(source.getInvalidationTimestampUs());
        stateReader.setSubscribeFromSeqTxn(source.getSubscribeFromSeqTxn());
        stateReader.setLastProcessedSeqTxn(source.getLastProcessedSeqTxn());
        stateReader.setAppliedWatermark(source.getAppliedWatermark());
        stateReader.setLvConsumedSeqTxn(source.getLvConsumedSeqTxn());
    }

    public boolean isDropped() {
        return dropped;
    }

    public boolean isInvalid() {
        return stateReader.isInvalid();
    }

    /**
     * Writes invalidation fields into the in-memory state mirror. The caller is responsible
     * for rewriting {@code _lv.s} via {@link io.questdb.cairo.lv.LiveViewState#append} —
     * this method only updates the in-memory side.
     */
    public void markInvalid(@Nullable CharSequence reason, long invalidationTimestampUs) {
        stateReader.setInvalid(true);
        stateReader.setInvalidationReason(reason);
        stateReader.setInvalidationTimestampUs(invalidationTimestampUs);
    }

    public void markAsDropped() {
        dropped = true;
    }

    /**
     * Returns the currently-published buffer without pinning it. Callers must hold the
     * refresh latch (only the refresh worker uses this).
     */
    public InMemoryTable peekPublishedBuffer() {
        return bufferedTable.peekPublished();
    }

    public void publishWriteBuffer() {
        bufferedTable.publishSwap();
    }

    public void releaseAfterRead(InMemoryTable buffer) {
        bufferedTable.release(buffer);
        tryCloseIfDropped();
    }

    public void setAppliedWatermark(long appliedWatermark) {
        stateReader.setAppliedWatermark(appliedWatermark);
    }

    public void setCompiledFactory(RecordCursorFactory factory) {
        if (compiledFactory != factory) {
            Misc.free(compiledFactory);
            compiledFactory = factory;
        }
    }

    public void setLastProcessedSeqTxn(long seqTxn) {
        stateReader.setLastProcessedSeqTxn(seqTxn);
    }

    public void setLastRefreshTimeUs(long lastRefreshTimeUs) {
        this.lastRefreshTimeUs = lastRefreshTimeUs;
    }

    public void setLvConsumedSeqTxn(long lvConsumedSeqTxn) {
        stateReader.setLvConsumedSeqTxn(lvConsumedSeqTxn);
    }

    public void setRecordToRowCopier(RecordToRowCopier copier, long metadataVersion) {
        this.recordToRowCopier = copier;
        this.recordRowCopierMetadataVersion = metadataVersion;
    }

    public void setSubscribeFromSeqTxn(long subscribeFromSeqTxn) {
        stateReader.setSubscribeFromSeqTxn(subscribeFromSeqTxn);
    }

    public void tryCloseIfDropped() {
        if (!dropped) {
            return;
        }
        if (!refreshLatch.compareAndSet(false, true)) {
            // Refresh in flight; its finally hook retries.
            return;
        }
        try {
            if (!bufferedTable.tryAcquireExclusive()) {
                return;
            }
            if (!isClosed) {
                isClosed = true;
                Misc.free(bufferedTable);
                compiledFactory = Misc.free(compiledFactory);
            }
        } finally {
            refreshLatch.set(false);
        }
    }

    public InMemoryTable tryAcquireWriteBuffer() {
        return bufferedTable.tryAcquireWrite();
    }

    public boolean tryLockForRefresh() {
        return refreshLatch.compareAndSet(false, true);
    }

    public void unlockAfterRefresh() {
        if (!refreshLatch.compareAndSet(true, false)) {
            throw new IllegalStateException("refresh latch is not held");
        }
    }
}
