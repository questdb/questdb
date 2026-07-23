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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointRingStateSink;
import io.questdb.cairo.lv.LiveViewCheckpointRingStateSource;
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.MemoryTracker;
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
     * Returns the compiler-produced localized-repair dependency descriptor, or
     * {@code null} outside a live-view compile / for a function that does not
     * support checkpoint state.
     */
    @Nullable
    default LiveViewCheckpointDependency checkpointDependency() {
        return null;
    }

    /**
     * Returns the stable identity persisted in the timeline function directory.
     */
    @Nullable
    default LiveViewCheckpointFunctionIdentity checkpointFunctionIdentity() {
        return null;
    }

    default void computeNext(Record record) {
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
     * Exposes the per-instance partition-state {@link Map} used by the live-view
     * tombstone-compaction routine to rebuild the function's state container.
     * Returns {@code null} by default; window functions that maintain per-partition
     * state in a Map keyed by the named window's PARTITION BY columns override this
     * once they sign up for full compaction by overriding this method.
     * <p>
     * A function that does not override this leaves its per-function Map out of the
     * rebuild; only the anchor-map compaction trigger runs for it. While the
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

    /**
     * @return the partition-key {@link ColumnTypes} the live-view checkpoint framework
     * writes into the state payload's key-shape header and validates on restore.
     * Returns {@code null} for scalar (no-map) functions, which the framework treats
     * as a single keyless partition. Partitioned functions override to return their
     * own partition-key types.
     */
    @Nullable
    default ColumnTypes getCheckpointKeyColumnTypes() {
        return null;
    }

    /**
     * @return the index of the first partition-key column inside the partition-state
     * {@link Map} record's column layout ({@code [value0..valueN, key0..keyM]}), i.e.
     * the value-slot count. The framework passes this to
     * {@link io.questdb.cairo.lv.LiveViewSnapshotKeyCodec#writeKey} when emitting a
     * partition's key. Default {@code 0}; partitioned functions override.
     */
    default int getCheckpointKeyStartIndex() {
        return 0;
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

    /**
     * @return the number of tombstoned (logically-evicted) partitions currently in
     * the partition-state {@link Map}. The live-view checkpoint framework uses it to
     * pick the cheap {@code map.size()} live-count when no entry is tombstoned, and
     * the two-pass count otherwise. Default {@code 0}; functions that track a
     * per-partition tombstone bit override.
     */
    default long getTombstoneCount() {
        return 0;
    }

    /**
     * @return the value-slot index of the per-partition tombstone byte, or {@code -1}
     * when the function tracks no tombstone bit. The snapshot framework reads this to
     * skip tombstoned partitions when emitting. Default {@code -1}; partitioned
     * functions in live-view mode override.
     */
    default int getTombstoneValueIndex() {
        return -1;
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

    /**
     * Reports whether this function's checkpoint state is <b>frame-local</b>: every value
     * it produces from a given row onward is determined by the rows within the look-behind
     * its descriptor declares - the
     * {@link LiveViewCheckpointDependency#getStateExtentLo() state extent} - so a replay
     * warmed up over that extent reproduces them. The extent is the declared frame's own
     * look-behind for every function that answers true today, which is where the name comes
     * from; a function whose state needs less than its frame says so by carrying a narrower
     * extent in the descriptor, not by answering differently here.
     * <p>
     * The live-view localized out-of-order repair rebuilds state from the dependency floor
     * {@code L} - the extent's lower edge at the output floor - and reads nothing below it.
     * A function whose value depends on rows outside the extent it declares would be
     * replayed against a warm-up that never fed those rows and would emit wrong output:
     * {@code lag(x, 5) OVER (... ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)} reaches five
     * rows back through a frame that promises three. Declaring {@code false} costs the
     * view only the localized path - the repair falls back to the rebuild from the
     * {@code START FROM} boundary, which needs no such guarantee.
     * <p>
     * Frame-local does not require the replayed state bytes to equal a whole-history
     * recompute's. A ring buffer replayed from {@code L} starts at a different rotation,
     * and a counter that saturates at the frame size stops short of the true total; both
     * produce the same values from the output floor onward, which is what the contract
     * asks for.
     * <p>
     * Default {@code false} fails closed: a function is enabled here only once its state
     * is proven to converge, one function and type at a time.
     */
    default boolean hasFrameLocalCheckpointState() {
        return false;
    }

    /**
     * Rebinds every inner expression that depends on the base cursor's per-cursor state -
     * the partition-by expressions, the function's {@code arg}, and any extra argument a
     * subclass carries (lag/lead's {@code defaultValue}, a bivariate function's second
     * arg) - to the new cursor, without resetting the accumulated per-partition state.
     * <p>
     * The live-view incremental refresh path skips the regular {@link #init} call on
     * window functions so their cross-cycle accumulator state survives, and calls this
     * instead. Every cycle hands the function a fresh WAL-segment-scoped
     * SymbolTableSource, and the WAL writer re-assigns symbol keys per commit, so any
     * binding cached against the previous cursor - a SYMBOL column's symbol table, the
     * int key a symbol comparison resolved its constant to - names the wrong value from
     * the second cycle on. Miss one and the window silently aggregates the wrong rows.
     * <p>
     * Despite the name, this is not partition-by-only: implementations must rebind every
     * such expression they own, and overrides must call super.
     */
    default void initPartitionBy(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
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

    /**
     * Reports whether this function holds no checkpoint state at all: the value it emits at a
     * row is that row's alone, so a freeze has nothing to write and a restore nothing to put
     * back. {@code last_value} over a frame ending at the current row is the family that
     * answers true - whatever its frame nominally spans, the whole of its {@code computeNext}
     * is {@code value = readArgValue(record)}, which makes the call equivalent to projecting
     * its own argument.
     * <p>
     * This is a disposition rather than an empty implementation of the freeze/restore pair. A
     * freeze that writes nothing is indistinguishable, at the call site, from one that forgot
     * to write something, and the whole checkpoint contract rests on that call site being
     * honest. Declaring the absence instead keeps {@link #supportsCheckpointState()} meaning
     * what it says, so every site that walks the checkpoint image keeps skipping this function
     * rather than carrying a zero-length entry for it. The two answers are therefore exclusive,
     * and {@code CairoEngine.validateLiveViewWindowFunction} rejects a function claiming both.
     * <p>
     * A live view still needs a dependency descriptor from such a function, because a repair
     * has to know the influence is bounded rather than merely that the state is empty. The
     * compiler gives it a state extent of zero under
     * {@link io.questdb.cairo.lv.LiveViewCheckpointContracts.DependencyKind#STATELESS_CURRENT_ROW},
     * and the bounds that follow are the cheapest the system can express: the replay floor is
     * the output floor itself, so no warm-up runs at all, and convergence is one tick above the
     * highest changed timestamp.
     */
    default boolean isCheckpointStateless() {
        return false;
    }

    default boolean isIgnoreNulls() {
        return false;
    }

    /**
     * Clears the per-function tombstone bit for the partition the supplied record
     * belongs to, if currently set. Called once per row by
     * {@link io.questdb.cairo.lv.LiveViewWindow#processRow(Record)} (post-projection,
     * post-filter) before the row reaches the underlying cursor stack's
     * {@link #computeNext(Record)} dispatch.
     * <p>
     * Decouples the "partition saw a row" signal from {@link #computeNext(Record)},
     * which previously cancelled the tombstone bit set by {@link #resetPartition(Record)}
     * on the same anchor-cross row. With markPartitionAlive driving the clear,
     * the anchor-map's tombstoneCount actually grows across anchor crosses and the
     * compaction trigger can engage in steady state.
     * <p>
     * Default no-op; override on every window function that tracks a per-partition
     * tombstone bit. Implementations must be branchless on the common (no-tombstone)
     * case -- check the function-local tombstoneCount first and bail before the
     * Map lookup.
     */
    default void markPartitionAlive(Record record) {
    }

    /**
     * Resets this function's per-partition state to empty before the live-view
     * snapshot framework rehydrates partitions via
     * {@link #restoreCheckpointState(LiveViewStatePageReader, long, MapValue)}. Partitioned
     * functions clear their {@link Map} and zero the tombstone counter here;
     * native-memory-backed ring/deque functions also truncate their backing
     * arena and clear their free list. Default no-op (scalar functions hold a
     * single field that {@code restoreCheckpointState} overwrites directly).
     */
    default void onCheckpointRestoreBegin() {
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
     * Rehydrates ONE partition's accumulator state previously written by
     * {@link #freezeCheckpointState(LiveViewStatePageWriter, MapValue)}. The live-view snapshot
     * framework owns iteration: it has already read the partition key and called
     * {@code createValue()}, passing the fresh {@code value} here; for scalar
     * (no-map) functions {@code value} is {@code null}. The function reads its own
     * state bytes from {@code source} starting at {@code offset} and returns the
     * advanced offset just past them. Native-memory-backed functions allocate the
     * partition's ring/deque from their arena here.
     * <p>
     * The default throws — only window functions that {@link #supportsCheckpointState()}
     * override.
     *
     * @return the offset just past this partition's consumed state bytes
     */
    default long restoreCheckpointState(LiveViewStatePageReader source, long offset, MapValue value) {
        throw new UnsupportedOperationException(
                "restoreCheckpointState not implemented for " + getClass().getName()
        );
    }

    /**
     * Rebuilds the per-partition state {@link Map} to keep only partitions whose
     * key is present in {@code survivingKeys}, dropping the rest. The live-view
     * ANCHOR runtime ({@link io.questdb.cairo.lv.LiveViewWindow#compact()}) calls
     * this after a frontier-gated sweep drops anchor-map partitions whose bucket
     * has fallen behind the retained window, so each function's map stays in
     * lockstep with the anchor map.
     * <p>
     * {@code survivingKeys} is the rebuilt anchor map; it shares this function's
     * partition-by key layout and {@link Map} implementation — the caller verifies
     * the impl match before dispatching, so the membership probe
     * ({@link io.questdb.griffin.engine.functions.window.PartitionStateEvictor#rebuildKeepingMembers})
     * never casts across impls. Default no-op for functions without per-partition state.
     */
    default void retainPartitions(Map survivingKeys) {
    }

    /*
      Set index of record chain column used to store window function result.
     */
    void setColumnIndex(int columnIndex);

    /**
     * Binds the per-query native memory tracker on this function's tracker-aware
     * state: the per-partition map, and the ring buffers holding the frame values
     * (both RANGE and ROWS frames, and the Max/Min monotonic deque). The owning
     * window cursor calls this before reopen() at cursor start, so the state
     * allocates against the bound tracker and frees against it at cursor close. A
     * null tracker degrades to global-only accounting. Default no-op for functions
     * with no tracker-aware state.
     * <p>
     * An implementation must therefore leave its ring buffer UNALLOCATED at
     * construction and let reopen() perform the first allocation, matching the
     * lazy-map pattern in BasePartitionedWindowFunction's constructor. A buffer
     * filled in the constructor allocates at newInstance() time, before any cursor
     * binds a tracker, so the malloc lands on the global counter while reset() later
     * frees it against the bound per-query tracker - driving that counter negative.
     * Direct callers (e.g. unit tests) must reopen() before use.
     */
    default void setMemoryTracker(@Nullable MemoryTracker tracker) {
    }

    /**
     * Called exactly once by the SQL compiler for a checkpoint-capable function
     * in a live-view SELECT. Implementations must retain these immutable values;
     * the default fails closed so an eligible function cannot silently fall back
     * to positional/object identity.
     */
    default void setCheckpointCompilerMetadata(
            LiveViewCheckpointFunctionIdentity identity,
            LiveViewCheckpointDependency dependency
    ) {
        throw new UnsupportedOperationException(
                "checkpoint compiler metadata not supported by " + getClass().getName()
        );
    }

    /**
     * The value kind this function's checkpoint ring stores per row, one of
     * {@link io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateReader#VALUE_KIND_DOUBLE}
     * or {@link io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateReader#VALUE_KIND_LONG}.
     * A DOUBLE ring stores exact IEEE-754 bits (raw or XOR-compressed); a LONG/DATE/TIMESTAMP
     * ring stores the raw 64-bit payload, which an integer value keeps out of a double so a
     * NaN bit pattern is never canonicalized. Read only for a
     * {@link #supportsCheckpointRingState() ring-shaped} function.
     */
    default int checkpointRingValueKind() {
        return io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE;
    }

    /**
     * The negated ROWS look-behind a warm-up must replay to reconstruct this function's state,
     * when that extent is a fixed row count of the function's own rather than the declared
     * frame's low bound. {@code lag(x, K)} reads the row {@code K} back and ignores its frame
     * entirely, so its state extent is {@code -K} however the frame is written - in particular
     * it can reach further back than the frame does, which the frame's own low bound never
     * describes. Returns {@link Long#MIN_VALUE} to defer to the frame, which is what every
     * function but {@code lag} does.
     * <p>
     * The compiler reads this only for ROWS framing. A RANGE frame's repair works in timestamp
     * width, which a fixed row count cannot express, so a function carrying this override
     * declines a RANGE frame rather than localize against an extent in the wrong units.
     */
    default long checkpointRowsStateExtentOverride() {
        return Long.MIN_VALUE;
    }

    /**
     * @return the checkpoint state layout version this build writes. The compiler folds it into
     * the function's state codec identity, and the checkpoint timeline records it in the function
     * root. Bump on any state-layout change: the bump changes the identity, so a root written under
     * the old layout no longer resolves to this function and the timeline rejects it wholesale
     * instead of decoding foreign bytes.
     */
    default int checkpointStateFormatVersion() {
        return 0;
    }

    /**
     * Serialises ONE partition's accumulator state into {@code sink} for later
     * {@link #restoreCheckpointState(LiveViewStatePageReader, long, MapValue)}. The live-view
     * snapshot framework owns iteration and the key-shape header:
     * it has already written this partition's key, so the function writes only its
     * own value bytes from {@code value} (the partition's {@link MapValue}; for
     * scalar no-map functions {@code value} is {@code null} and the function reads
     * its single field directly).
     * <p>
     * The default throws — only window functions that {@link #supportsCheckpointState()}
     * override.
     */
    default void freezeCheckpointState(LiveViewStatePageWriter sink, MapValue value) {
        throw new UnsupportedOperationException(
                "freezeCheckpointState not implemented for " + getClass().getName()
        );
    }

    /**
     * Streams ONE partition's frame ring into {@code sink} so the checkpoint seal can
     * share the chunk pages the previous boundary already wrote instead of encoding the
     * whole frame again. The function writes its exact aggregate continuation state and
     * then every live ring row in designated-timestamp order; the seal decides which of
     * those rows are new. For scalar no-map functions {@code value} is {@code null}.
     * <p>
     * The default throws — only window functions that {@link #supportsCheckpointRingState()}
     * override.
     */
    default void freezeCheckpointRingState(LiveViewCheckpointRingStateSink sink, MapValue value) {
        throw new UnsupportedOperationException(
                "freezeCheckpointRingState not implemented for " + getClass().getName()
        );
    }

    /**
     * Rehydrates ONE partition's frame ring previously written by
     * {@link #freezeCheckpointRingState(LiveViewCheckpointRingStateSink, MapValue)}. The
     * live-view checkpoint framework owns iteration and has already read the partition key
     * and called {@code createValue()}, passing the fresh {@code value} here; for scalar
     * no-map functions {@code value} is {@code null}. The function sizes its ring from
     * {@link LiveViewCheckpointRingStateSource#getRowCount()} and fills it from
     * {@link LiveViewCheckpointRingStateSource#forEachRow}.
     * <p>
     * The default throws — only window functions that {@link #supportsCheckpointRingState()}
     * override.
     */
    default void restoreCheckpointRingState(LiveViewCheckpointRingStateSource source, MapValue value) {
        throw new UnsupportedOperationException(
                "restoreCheckpointRingState not implemented for " + getClass().getName()
        );
    }

    /**
     * Reports whether this function persists its per-partition state as a ring of
     * {@code (timestamp, value)} rows plus an exact aggregate tail — the shape
     * {@link #freezeCheckpointRingState(LiveViewCheckpointRingStateSink, MapValue)} and
     * {@link #restoreCheckpointRingState(LiveViewCheckpointRingStateSource, MapValue)}
     * carry. The checkpoint seal routes such a function through the persistent chunk
     * layer, so adjacent roots reference the same pages for the rows they share; every
     * other function writes one complete state image per root through
     * {@link #freezeCheckpointState(LiveViewStatePageWriter, MapValue)}.
     * <p>
     * Only a bounded RANGE frame answers true today. The chunk layer keys sharing on the
     * designated timestamp: the seal splits a partition's streamed ring at the previous
     * boundary's maximum timestamp, treats every row at or below it as a page the previous
     * root already wrote, and encodes only the rows above it. A RANGE frame expires rows by
     * timestamp, so its survivors are exactly a timestamp suffix of the previous ring and the
     * split reproduces them. A ROWS frame keeps a fixed count of rows regardless of timestamp,
     * and QuestDB admits many rows at one designated timestamp, so a boundary can drop and add
     * rows that all sit at the split timestamp; the timestamp split cannot tell those survivors
     * from the new rows and would carry stale pages forward. Sharing a ROWS ring therefore needs
     * a separate positional chunk model, and since a ROWS frame's live state is already bounded
     * by its declared row count, the ROWS value and aggregate families keep the whole-state image
     * instead - they leave this false and override only the whole-state pair above.
     * <p>
     * Implies {@link #supportsCheckpointState()}: the ring shape is an alternative
     * encoding of checkpoint state, not an alternative to having any.
     */
    default boolean supportsCheckpointRingState() {
        return false;
    }

    /**
     * Reports whether {@link #freezeCheckpointState(LiveViewStatePageWriter, MapValue)} /
     * {@link #restoreCheckpointState(LiveViewStatePageReader, long, MapValue)} are implemented.
     * The live view refresh worker ANDs this across every window function in the compiled SELECT
     * at first refresh; the LV's per-instance {@code snapshotCapability} flag is the result.
     * Default {@code false} keeps unmigrated functions out of the checkpoint pipeline without
     * a try/catch — the cheaper probe.
     */
    default boolean supportsCheckpointState() {
        return false;
    }

    enum Pass1ScanDirection {
        FORWARD, BACKWARD
    }
}
