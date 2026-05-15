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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface WindowFunction extends Function {
    int ONE_PASS = 1;
    int TWO_PASS = 2;
    int ZERO_PASS = 0;

    /**
     * Rebuilds the function's per-partition state Map to drop tombstoned entries
     * (partitions whose accumulator was reset by an anchor cross and which then
     * never received another row). Companion to {@link io.questdb.cairo.lv.LiveViewWindow#compact()}
     * which clears the anchor-map's tombstoned entries first; this method
     * matches the function's map to the new anchor-map shape so the two stay
     * consistent.
     * <p>
     * Default is a no-op: window functions that don't track per-partition
     * tombstones (most factories until each 2b commit migrates them) keep
     * their state Map unchanged. Migrated functions rebuild via a scratch
     * {@link Map} and a tombstone-skipping cursor walk, mirroring
     * {@link io.questdb.griffin.engine.functions.window.PartitionStateEvictor#rebuildKeeping}.
     * <p>
     * Auto-trigger from {@code processRow} stays off until every window
     * function family ships its own tombstone bookkeeping (see Phase 2b
     * function migration train); calling it before then on a partially-migrated
     * LV would corrupt the unmigrated functions' state because their maps
     * would still reference partitions the anchor map dropped.
     */
    default void compactPartitionMap() {
    }

    default void computeNext(Record record) {
    }

    /**
     * Drops partition-by accumulator state whose last-seen row timestamp falls
     * below {@code cutoffTs}. Default no-op; partitioned window functions that
     * maintain per-key state override this to shed keys that retention has made
     * unreachable (their last row has fallen outside the retained window and no
     * warm-path replay can reach it).
     * <p>
     * Called from the live view refresh path after {@code applyRetention} has
     * advanced the retention cutoff. Non-partitioned window functions keep the
     * no-op default.
     */
    default void evictStalePartitionState(long cutoffTs) {
    }

    @Override
    default ArrayView getArray(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default BinarySequence getBin(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getBinLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default boolean getBool(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default byte getByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default char getChar(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getDate(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void getDecimal128(Record rec, Decimal128 sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    default short getDecimal16(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void getDecimal256(Record rec, Decimal256 sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getDecimal32(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getDecimal64(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default byte getDecimal8(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default double getDouble(Record rec) {
        // unused
        throw new UnsupportedOperationException();
    }

    @Override
    default float getFloat(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default byte getGeoByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getGeoInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getGeoLong(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default short getGeoShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getIPv4(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default @NotNull Interval getInterval(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getLong(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getLong128Hi(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getLong128Lo(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void getLong256(Record rec, CharSink<?> sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Long256 getLong256A(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Long256 getLong256B(Record rec) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the maximum number of microseconds the function needs to look back from
     * the current row. Used by live view cold-path classification: a late row arriving
     * with {@code ts < oldest_visible_ts - max(lookback)} across all functions cannot
     * affect any visible output and may be skipped.
     * <p>
     * Returns {@code -1} (the default) when the lookback is not expressible as a
     * timestamp delta, i.e.:
     * <ul>
     *     <li>UNBOUNDED PRECEDING frame (accumulator depends on all prior rows),</li>
     *     <li>ROWS N PRECEDING frame (lookback is row-count, not time-based),</li>
     *     <li>ranking/numbering functions whose implicit frame spans the whole
     *         partition up to the current row.</li>
     * </ul>
     * RANGE-bounded frames override this to return {@code abs(rowsLo)} micros.
     * The default is conservative — unknown = must not skip.
     */
    default long getMaxLookbackMicros() {
        return -1;
    }

    /**
     * Exposes the per-instance partition-state {@link Map} used by the live-view
     * tombstone-compaction routine to rebuild the function's state container.
     * Returns {@code null} by default; window functions that maintain per-partition
     * state in a Map keyed by the named window's PARTITION BY columns override this
     * once they sign up for full compaction (the Phase 2b function migration
     * train).
     * <p>
     * Phase 2a only consumes the anchor-map compaction trigger; per-function map
     * rebuild lands as each group's 2b commit overrides this method. While the
     * default returns {@code null}, the function's Map continues to grow and is
     * reclaimed only when the live view is dropped.
     */
    @Nullable
    default Map getPartitionMap() {
        return null;
    }

    /**
     * @return pass1 scan direction.
     * Some {@link #ONE_PASS} and {@link #TWO_PASS} window functions may be more efficient when using a backward scan.
     */
    default Pass1ScanDirection getPass1ScanDirection() {
        return Pass1ScanDirection.FORWARD;
    }

    /**
     * Returns a pass-count-oriented optimization hint for window execution.
     * <p>
     * This value is also used by the planner as a streaming fast-path hint when the input cursor
     * already satisfies the window order. In that case, {@link #ZERO_PASS} functions are evaluated
     * row-by-row through {@link #computeNext(Record)}.
     * <p>
     * {@link #ZERO_PASS} is the strongest optimization hint, not a promise that cached execution
     * will skip this function. If the query is routed through the cached executor, every window
     * function, including {@link #ZERO_PASS}, must still implement
     * {@link #pass1(Record, long, WindowSPI)}. For a {@link #ZERO_PASS} function, {@code pass1()}
     * normally performs the cached equivalent of {@code computeNext(record)} and materializes the
     * current result into the output slot identified by {@link #setColumnIndex(int)}.
     *
     * @return cached execution pass count: {@link #ZERO_PASS}, {@link #ONE_PASS}, or {@link #TWO_PASS}
     */
    default int getPassCount() {
        return ONE_PASS;
    }

    @Override
    default RecordCursorFactory getRecordCursorFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    default short getShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getStrA(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getStrB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getStrLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getSymbol(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getSymbolB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getTimestamp(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Utf8Sequence getVarcharA(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Utf8Sequence getVarcharB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getVarcharSize(Record rec) {
        throw new UnsupportedOperationException();
    }

    default void initRecordComparator(
            SqlCodeGenerator sqlGenerator,
            RecordMetadata metadata,
            ArrayColumnTypes chainTypes,
            IntList orderIndices,
            ObjList<ExpressionNode> orderBy,
            IntList orderByDirections
    ) throws SqlException {
    }

    default boolean isIgnoreNulls() {
        return false;
    }

    /**
     * Performs the primary cached traversal for this function.
     * <p>
     * The cached executor calls this method for every window function, including functions whose
     * {@link #getPassCount()} returns {@link #ZERO_PASS}. Implementations must therefore not rely
     * on {@link #ZERO_PASS} to avoid cached execution. One-pass and zero-pass functions should
     * materialize their final result for {@code recordOffset}; two-pass functions may instead
     * build state or store scratch values for {@link #pass2(Record, long, WindowSPI)}.
     */
    void pass1(Record record, long recordOffset, WindowSPI spi);

    /**
     * Performs the optional secondary cached traversal. The cached executor calls this only when
     * {@link #getPassCount()} is greater than {@link #ONE_PASS}.
     */
    default void pass2(Record record, long recordOffset, WindowSPI spi) {
    }

    /**
     * Prepares state before the optional secondary cached traversal.
     */
    default void preparePass2() {
    }

    /**
     * Releases native memory and resets internal state to default/initial.
     * It differs from close() in that it doesn't release memory held by metadata, e.g. partition by key functions.
     * This means function may still be used after calling reopen().
     **/
    void reset();

    /**
     * Resets the per-partition accumulator for the partition the supplied record
     * belongs to. Called by the live-view ANCHOR runtime when the anchor expression's
     * value changes within a partition — the partition's state must be cleared to
     * the identity value before the new bucket's first row is processed.
     * <p>
     * The default no-op is correct for window functions whose state is intrinsically
     * per-row (ranking) or that do not maintain partitioned state. Window functions
     * that key per-partition state on PARTITION BY override this to reset the matching
     * Map entry's value bytes to identity (e.g. {@code sum -> 0}, {@code count -> 0},
     * {@code min/max -> NaN}, etc.).
     * <p>
     * The function is expected to use its own {@code partitionByRecord} +
     * {@code partitionBySink} to derive the Map key from {@code record}; for the
     * common case of multiple functions on the same named WINDOW, all of them use
     * the same partition shape, so the per-record cost of re-keying is just a
     * memcpy.
     * <p>
     * The live-view ANCHOR runtime drives this from {@link io.questdb.cairo.lv.LiveViewInstance};
     * non-live-view queries never invoke it.
     */
    default void resetPartition(Record record) {
    }

    /**
     * Rehydrates per-partition accumulator state previously written by {@link #snapshot(MemoryA)}.
     * The {@code formatVersion} is the per-function {@code snapshotFormatVersion()} the snapshot
     * was written under; implementations dispatch to a version-matching decoder, zero-fill any
     * new fields, and discard removed ones. A version lower than
     * {@link #snapshotMinSupportedVersion()} must not reach this method — the live view caller
     * unlinks the head checkpoint and falls into the head-miss path instead of attempting
     * restore.
     * <p>
     * The default throws — only window functions that {@link #supportsSnapshot()} override.
     * Functions on the default-throw path force their containing live view onto the head-miss
     * replay path (visible in {@code live_views()} as
     * {@code head_checkpoint_lv_seqtxn = LONG_NULL}).
     */
    default void restore(MemoryR source, int formatVersion) {
        throw new UnsupportedOperationException(
                "restore not implemented for " + getClass().getName()
        );
    }

    /*
      Set index of record chain column used to store window function result.
     */
    void setColumnIndex(int columnIndex);

    /**
     * Serialises the function's per-partition accumulator state into {@code sink} for later
     * {@link #restore(MemoryR, int)}. The framework writes the resulting bytes into the live
     * view's head {@code _checkpoints/<lvSeqTxn>.cp} file as a FUNCTION_SNAPSHOT block.
     * <p>
     * The default throws — only window functions that {@link #supportsSnapshot()} override.
     * The live-view refresh path checks {@link #supportsSnapshot()} at first refresh and
     * computes a per-LV {@code snapshotCapability} flag from the AND of every function's
     * answer; LVs whose flag is {@code false} emit no checkpoints and route restart and O3
     * through the head-miss replay path.
     */
    default void snapshot(MemoryA sink) {
        throw new UnsupportedOperationException(
                "snapshot not implemented for " + getClass().getName()
        );
    }

    /**
     * @return the snapshot layout version this build writes. The framework records this in the
     * FUNCTION_SNAPSHOT block header so future builds can dispatch through
     * {@link #restore(MemoryR, int)} to the correct decoder. Bump on any state-layout change.
     */
    default int snapshotFormatVersion() {
        return 0;
    }

    /**
     * @return the lowest snapshot {@code formatVersion} this build can {@link #restore(MemoryR, int)}.
     * A head checkpoint whose recorded version is strictly less than this value cannot be replayed;
     * the live view layer surfaces it as {@code "checkpoint format version unsupported"} via the
     * unified invalidation path.
     */
    default int snapshotMinSupportedVersion() {
        return 0;
    }

    /**
     * Reports whether {@link #snapshot(MemoryA)} / {@link #restore(MemoryR, int)} are implemented.
     * The live view refresh worker ANDs this across every window function in the compiled SELECT
     * at first refresh; the LV's per-instance {@code snapshotCapability} flag is the result.
     * Default {@code false} keeps unmigrated functions out of the checkpoint pipeline without
     * a try/catch — the cheaper probe.
     */
    default boolean supportsSnapshot() {
        return false;
    }

    enum Pass1ScanDirection {
        FORWARD, BACKWARD
    }
}
