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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.HighBoundTag;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Discovers ROWS repair bounds in dependency order H -> Q -> L against one pinned
 * snapshot. Insert-only changes are required: a deleted key can disappear from the
 * post-change interval while still affecting later output.
 * <p>
 * H closes after each affected key has enough following rows for convergence, at the
 * next distinct timestamp so ties remain complete. A contains qualifying keys in the
 * change interval; a superset safely widens H. Q contains every qualifying output key
 * in [R, H), since replacement is timestamp-global. Only after Q is known can the
 * backward scan find Nmax predecessors per output key and derive L.
 * <p>
 * An indexed SYMBOL key permits per-key predecessor seeks. Otherwise scan backward
 * in timestamp order. A key with insufficient history sets L to START FROM S in either
 * case. Empty A, insufficient following rows or an unsupported backward scan yields
 * H = EOF and L = S.
 * <p>
 * Discovery uses reader-local SYMBOL ids; checkpoint publication needs encoded string
 * keys from the parallel map. collectOutputKeys is valid only when the complete Q is
 * available, since publication preserves all keys outside it.
 * <p>
 * Row and key budgets apply during all discovery scans, counting filtered-out base
 * rows too. Crossing a budget keeps conservative bounds and records the reason.
 * One worker reuses the scratch; discover() overwrites every result.
 */
public final class LiveViewCheckpointRowsBounds implements QuietCloseable {
    // Per-key value slots. FOLLOWING doubles as A membership: NOT_AFFECTED until the
    // key is seen inside the change interval, a count of its rows above changeMaxTs
    // from then on.
    private static final int IDX_FOLLOWING = 0;
    private static final int IDX_PRECEDING = 1;
    private static final long NOT_AFFECTED = -1;
    // One slot, because the checkpoint-form key map carries membership and nothing else.
    // Its width is also what says where a map record's key columns start.
    private static final ArrayColumnTypes OUTPUT_KEY_VALUE_TYPES = new ArrayColumnTypes()
            .add(ColumnType.LONG);
    private static final ArrayColumnTypes VALUE_TYPES = new ArrayColumnTypes()
            .add(ColumnType.LONG)
            .add(ColumnType.LONG);
    private final CairoConfiguration configuration;
    private final FilteringRecordCursor filteringCursor = new FilteringRecordCursor();
    // Q in first-seen order, as table-local symbol keys. Populated only while the indexed
    // seek is available, which is the only path that iterates the key domain.
    private final IntList outputKeys = new IntList();
    private long affectedKeyCount;
    private long backwardScanRows;
    private ColumnTypes checkpointKeyColumnTypes;
    private long dependencyLowTs;
    private long forwardScanRows;
    private HighBoundTag highBoundTag = HighBoundTag.EOF;
    private long highTsExclusive;
    private int indexedKeyColumnIndex = -1;
    private long indexedKeyLookups;
    private boolean isOutputKeyDomainComplete;
    private Map keyMap;
    private long outputKeyBudget;
    private long outputKeyCount;
    // Q again, in the encoding a checkpoint partition map keys an entry by. Separate
    // from keyMap because that one is per row and this one is per distinct key: the
    // scans keep their table-local SYMBOL integers and only a key joining Q pays for
    // the resolved string the publication has to compare against.
    private Map outputKeyMap;
    private ScanBudgetStatus scanBudgetStatus = ScanBudgetStatus.WITHIN;
    private long scanRowBudget;
    private long scanRows;

    public LiveViewCheckpointRowsBounds(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void close() {
        keyMap = Misc.free(keyMap);
        outputKeyMap = Misc.free(outputKeyMap);
    }

    /**
     * Copies {@code Q} into {@code out}, in the encoding a checkpoint partition map keys
     * an entry by. Meaningful only while {@link #isOutputKeyDomainComplete()} holds: a
     * discovery a budget cut short collected a fragment of the interval, and a fragment
     * of {@code Q} would let the publication drop a key it cannot describe.
     */
    public void collectOutputKeys(@NotNull LiveViewCheckpointOutputKeyDomain out) {
        out.clear();
        if (!isOutputKeyDomainComplete || outputKeyMap == null) {
            throw CairoException.critical(0)
                    .put("live view checkpoint repair output key domain is not available");
        }
        final int keyStartIndex = OUTPUT_KEY_VALUE_TYPES.getColumnCount();
        final MapRecordCursor cursor = outputKeyMap.getCursor();
        final MapRecord record = outputKeyMap.getRecord();
        while (cursor.hasNext()) {
            // Encoded straight into the domain's own storage. A key the codec fails
            // part-way through is dropped rather than left half-written in the domain.
            final MemoryA sink = out.beginKey();
            try {
                LiveViewSnapshotKeyCodec.writeKey(sink, record, checkpointKeyColumnTypes, keyStartIndex);
            } catch (Throwable th) {
                out.abortKey();
                throw th;
            }
            out.commitKey();
        }
    }

    /**
     * Runs the {@code H -> Q -> L} discovery against one pinned base snapshot.
     * <p>
     * The caller owns that snapshot: {@code executionContext} must already be bound to
     * the reader the repair plans and replays against, and {@code filter} must be the
     * view's own {@code WHERE}, so "qualifying" means here exactly what it means to the
     * replay. Both scans go through the bounded page-frame cursors, so the history
     * below {@code L} and the tail above {@code H} cost no partition open.
     * <p>
     * The scan budgets are read from the configuration per discovery, so a repair turn
     * spends at most one budget no matter how many searches it takes to answer.
     *
     * @param plan             the view's finite ROWS dependency union
     * @param pageFrameFactory the base factory the repair reads through
     * @param executionContext bound to the pinned base reader
     * @param filter           the view's WHERE filter, or null
     * @param viewLowerBoundTs {@code S}, the view's {@code START FROM} boundary
     * @param outputLowTs      {@code R}, the floor the replay emits and replaces from
     * @param changeLowTs      the lowest timestamp the incorporated change set can have
     *                         touched - the repair's retire floor, clamped to {@code S}
     * @param changeMaxTs      the highest timestamp it can have touched, or
     *                         {@link Numbers#LONG_NULL} when the caller cannot bound it,
     *                         which leaves both results at their no-bound values
     */
    public void discover(
            @NotNull LiveViewCheckpointRowsPlan plan,
            @NotNull PageFrameRecordCursorFactory pageFrameFactory,
            @NotNull SqlExecutionContext executionContext,
            @Nullable Function filter,
            long viewLowerBoundTs,
            long outputLowTs,
            long changeLowTs,
            long changeMaxTs
    ) throws SqlException {
        clear();
        scanRowBudget = budgetOf(configuration.getLiveViewCheckpointRepairScanMaxRows());
        outputKeyBudget = budgetOf(configuration.getLiveViewCheckpointRepairScanMaxKeys());
        // No bound is discoverable below S: the view holds no row down there, so a floor
        // below it would only widen the scan without changing what the replay computes.
        dependencyLowTs = viewLowerBoundTs;
        // An unbounded change ceiling leaves nothing to measure convergence from, and an
        // index-backed factory cannot be walked backwards in timestamp order, so the
        // predecessor count would run over the wrong rows.
        if (changeMaxTs == Numbers.LONG_NULL || !pageFrameFactory.isBackwardTimestampRangeSupported()) {
            return;
        }
        // A single indexed SYMBOL key column is what lets the dependency floor be sought
        // per key rather than walked over every key's rows. Decided once, before Q is
        // collected, because collecting the key domain is only worth its memory on that
        // path. An expression-keyed plan names no column at all and so takes the walk.
        final int keyColumnIndex = plan.getPartitionByColumnCount() == 1
                ? plan.getPartitionByColumnIndex(0)
                : -1;
        if (pageFrameFactory.isIndexedBackwardTimestampRangeSupported(keyColumnIndex)) {
            indexedKeyColumnIndex = keyColumnIndex;
        }
        openKeyMap(plan);
        if (discoverHighBoundAndKeys(plan, pageFrameFactory, executionContext, filter, outputLowTs, changeLowTs, changeMaxTs)) {
            isOutputKeyDomainComplete = true;
            discoverDependencyLowTs(plan, pageFrameFactory, executionContext, filter, viewLowerBoundTs, outputLowTs);
        }
    }

    /**
     * @return the number of keys the change set can have touched: keys with a
     * qualifying row in {@code [changeLowTs, changeMaxTs]}. A superset of the truly
     * affected keys, and the set every following-row count is kept for.
     */
    public long getAffectedKeyCount() {
        return affectedKeyCount;
    }

    /**
     * @return qualifying rows the backward predecessor scan pulled.
     */
    public long getBackwardScanRows() {
        return backwardScanRows;
    }

    /**
     * @return {@code L}: the inclusive timestamp from which replaying reconstructs the
     * state every key in {@code Q} holds at {@code R}. Equal to {@code S} when no bound
     * could be discovered, and to {@code R} when no key needs warm-up at all.
     */
    public long getDependencyLowTs() {
        return dependencyLowTs;
    }

    /**
     * @return qualifying rows the forward convergence scan pulled.
     */
    public long getForwardScanRows() {
        return forwardScanRows;
    }

    /**
     * @return whether {@code H} is a concrete exclusive timestamp
     * ({@link HighBoundTag#FINITE}) or pinned to end-of-frame ({@link HighBoundTag#EOF}).
     */
    public HighBoundTag getHighBoundTag() {
        return highBoundTag;
    }

    /**
     * @return {@code H}: the exclusive timestamp after which no output can have changed.
     * Meaningful only under {@link HighBoundTag#FINITE}.
     */
    public long getHighTsExclusive() {
        return highTsExclusive;
    }

    /**
     * @return per-key index seeks the dependency-floor discovery performed: the size of
     * {@code Q} when the indexed seek ran to completion, fewer when a key short of
     * history ended it early, and zero when the unrestricted backward walk answered
     * instead.
     */
    public long getIndexedKeyLookups() {
        return indexedKeyLookups;
    }

    /**
     * @return the size of {@code Q}: keys with a qualifying row in {@code [R, H)}, and
     * therefore the keys the timestamp-global replacement re-emits.
     */
    public long getOutputKeyCount() {
        return outputKeyCount;
    }

    /**
     * @return which budget, if any, stopped this discovery. {@link ScanBudgetStatus#WITHIN}
     * means both bounds are what the data proved; anything else means a search was cut
     * short and the bound it was proving fell back to its conservative value.
     */
    public ScanBudgetStatus getScanBudgetStatus() {
        return scanBudgetStatus;
    }

    /**
     * @return base rows this discovery pulled across every cursor it opened - the forward
     * convergence scan, the backward walk, and each per-key indexed seek. This is the count
     * the row budget binds on, so it counts reads rather than results: rows the view's
     * {@code WHERE} discarded are in it, because reading them cost what reading a
     * qualifying row costs, and so is the row the forward pass stops on to learn {@code H}.
     * It is therefore at or above
     * {@link #getForwardScanRows()} + {@link #getBackwardScanRows()}.
     */
    public long getScanRows() {
        return scanRows;
    }

    /**
     * @return whether the forward pass collected the whole of {@code Q} - every key with
     * a qualifying row in {@code [R, H)} - rather than the fragment a budget stopped it
     * on. Only then may {@link #collectOutputKeys} be read, because a publication that
     * keeps the old root's entry for every key outside {@code Q} needs the complete set
     * to know which keys those are.
     * <p>
     * The backward searches do not bear on it. Either proves {@code L} whole or leaves it
     * at {@code S}, and {@code S} is where a replay reconstructs every key anyway, so a
     * budget that stopped one of them costs the repair its floor rather than its key
     * domain.
     */
    public boolean isOutputKeyDomainComplete() {
        return isOutputKeyDomainComplete;
    }

    /**
     * @return whether a budget stopped this discovery, leaving at least one bound at its
     * conservative fallback rather than at what the data proves.
     */
    public boolean isScanBudgetExceeded() {
        return scanBudgetStatus != ScanBudgetStatus.WITHIN;
    }

    /**
     * Normalizes a configured budget, mapping the disabling {@code <= 0} to no bound.
     */
    private static long budgetOf(long configured) {
        return configured > 0 ? configured : Long.MAX_VALUE;
    }

    /**
     * Base rows the open scan has pulled, given the {@code qualifyingRows} it has
     * returned. The two coincide without a filter; with one, only the base count
     * describes the work done, and it is the work the budget has to bind on - a filter
     * that admits nothing reads every row it rejects while the qualifying count sits
     * at zero.
     */
    private long baseRowsConsumed(Function filter, long qualifyingRows) {
        return filter != null ? filteringCursor.getBaseRowsConsumed() : qualifyingRows;
    }

    private void clear() {
        affectedKeyCount = 0;
        backwardScanRows = 0;
        dependencyLowTs = Numbers.LONG_NULL;
        // Drops the previous discovery's base-row count, which every scan below folds into
        // scanRows even when opening its cursor throws before of() could reset it.
        filteringCursor.close();
        forwardScanRows = 0;
        highBoundTag = HighBoundTag.EOF;
        highTsExclusive = Numbers.LONG_NULL;
        indexedKeyColumnIndex = -1;
        indexedKeyLookups = 0;
        isOutputKeyDomainComplete = false;
        outputKeys.clear();
        outputKeyCount = 0;
        scanBudgetStatus = ScanBudgetStatus.WITHIN;
        scanRows = 0;
    }

    /**
     * Resolves the record's key, joining it to {@code Q} on first sight.
     */
    private MapValue createKey(LiveViewCheckpointRowsPlan plan, Record record) {
        final MapKey key = keyMap.withKey();
        key.put(record, plan.getKeySink());
        final MapValue value = key.createValue();
        if (value.isNew()) {
            value.putLong(IDX_FOLLOWING, NOT_AFFECTED);
            value.putLong(IDX_PRECEDING, 0);
            outputKeyCount++;
            if (indexedKeyColumnIndex > -1) {
                outputKeys.add(record.getInt(indexedKeyColumnIndex));
            }
            // Once per distinct key rather than once per row: the checkpoint form is
            // what the publication compares against, and nothing reads it until the
            // repair decides to splice.
            final MapKey checkpointKey = outputKeyMap.withKey();
            checkpointKey.put(record, plan.getCheckpointKeySink());
            checkpointKey.createValue().putLong(0, 0);
        }
        return value;
    }

    /**
     * Discovers {@code L}, the timestamp from which warm-up reconstructs the state every
     * key in {@code Q} holds at {@code R}, through whichever of the two searches this
     * view's key shape allows.
     * <p>
     * Either search leaves {@code L} at {@code S} when a key has less history than its
     * frame needs. That is the answer rather than a give-up: reaching {@code S} means the
     * view's whole history has been seen, so whatever the key is still short of does not
     * exist and starting it from {@code S} is exactly what a full recompute does.
     */
    private void discoverDependencyLowTs(
            LiveViewCheckpointRowsPlan plan,
            PageFrameRecordCursorFactory pageFrameFactory,
            SqlExecutionContext executionContext,
            Function filter,
            long viewLowerBoundTs,
            long outputLowTs
    ) throws SqlException {
        if (outputKeyCount == 0) {
            // The replacement interval holds no qualifying row, so it re-emits nothing
            // and there is no key whose state has to be warmed up to reach it.
            dependencyLowTs = outputLowTs;
            return;
        }
        if (outputLowTs <= viewLowerBoundTs) {
            // The replacement already starts at the view's boundary, so there is no
            // history below it to walk and no floor above S to discover.
            return;
        }
        if (indexedKeyColumnIndex > -1) {
            seekDependencyLowTs(plan, pageFrameFactory, executionContext, filter, viewLowerBoundTs, outputLowTs);
        } else {
            scanDependencyLowTs(plan, pageFrameFactory, executionContext, filter, viewLowerBoundTs, outputLowTs);
        }
    }

    /**
     * Forward pass: derives {@code H} and fills the key map with {@code Q}, marking the
     * subset {@code A} on the way through the change interval.
     *
     * @return true when {@code Q} is complete and the caller may go on to discover
     * {@code L}. False means no forward bound exists and the partial key set must not be
     * read as {@code Q}: either the change set is invisible in this snapshot, or a budget
     * cut the scan short before it had collected the whole key domain.
     */
    private boolean discoverHighBoundAndKeys(
            LiveViewCheckpointRowsPlan plan,
            PageFrameRecordCursorFactory pageFrameFactory,
            SqlExecutionContext executionContext,
            Function filter,
            long outputLowTs,
            long changeLowTs,
            long changeMaxTs
    ) throws SqlException {
        final long precedingRows = plan.getMaxPrecedingRows();
        final int timestampIndex = plan.getTimestampIndex();
        // Keys in A still short of Nmax rows above the change interval. It can only
        // reach zero after a key has joined A, which anyAffected below guards.
        long pendingKeys = 0;
        boolean anyAffected = false;
        // Timestamp of the row that satisfied the last key in A. H is the next distinct
        // timestamp above it, so the scan runs one row past this to read that value.
        long lastRequiredTs = Numbers.LONG_NULL;
        // Rows this cursor has returned, the terminating one included. The forward metric
        // deliberately excludes that row - it belongs to neither the interval nor Q - but
        // the budget counts every row read.
        long pulled = 0;
        try (RecordCursor pageCursor = pageFrameFactory.getCursorInTimestampRange(
                executionContext,
                outputLowTs,
                Long.MAX_VALUE
        )) {
            final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
            final RecordCursor source = openSource(plan, pageCursor, filter, executionContext);
            final Record record = source.getRecord();
            while (source.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                if (isOverScanRowBudget(filter, ++pulled)) {
                    // Q is a fragment of the interval at this point, so neither bound may
                    // be read off it: the caller keeps H at end-of-frame and L at S.
                    scanBudgetStatus = ScanBudgetStatus.ROWS_EXCEEDED;
                    return false;
                }
                final long ts = record.getTimestamp(timestampIndex);
                if (lastRequiredTs != Numbers.LONG_NULL && ts > lastRequiredTs) {
                    // First row of the next distinct timestamp group: every affected key
                    // converged below it, so this is the exclusive bound. The row itself
                    // sits outside [R, H) and must not join Q.
                    highBoundTag = HighBoundTag.FINITE;
                    highTsExclusive = ts;
                    return true;
                }
                if (ts > changeMaxTs && !anyAffected) {
                    // Past the change interval with nothing marked: the change set left
                    // no qualifying row this snapshot can see. Nothing bounds its forward
                    // influence, and Q would be a fragment of the interval.
                    return false;
                }
                forwardScanRows++;
                final MapValue value = createKey(plan, record);
                if (outputKeyCount > outputKeyBudget) {
                    // Too many keys to plan a replacement for, and the domain is still
                    // growing. Refuse it whole rather than re-emit a truncated Q.
                    scanBudgetStatus = ScanBudgetStatus.KEYS_EXCEEDED;
                    return false;
                }
                if (ts >= changeLowTs && ts <= changeMaxTs) {
                    if (value.getLong(IDX_FOLLOWING) == NOT_AFFECTED) {
                        value.putLong(IDX_FOLLOWING, 0);
                        affectedKeyCount++;
                        anyAffected = true;
                        pendingKeys++;
                    }
                } else if (ts > changeMaxTs) {
                    final long following = value.getLong(IDX_FOLLOWING);
                    if (following >= 0 && following < precedingRows) {
                        value.putLong(IDX_FOLLOWING, following + 1);
                        if (following + 1 == precedingRows) {
                            pendingKeys--;
                        }
                    }
                    if (pendingKeys == 0 && lastRequiredTs == Numbers.LONG_NULL) {
                        lastRequiredTs = ts;
                    }
                }
            }
        } finally {
            scanRows += baseRowsConsumed(filter, pulled);
        }
        // The scan reached the end of the base table. H stays at end-of-frame: either an
        // affected key never got its Nmax following rows, or the last one got them in the
        // final timestamp group and no distinct timestamp above it exists to name. Q is
        // complete either way, so L is still worth discovering.
        return true;
    }

    /**
     * Looks the record's key up without creating it. A miss means the key has no
     * qualifying row in {@code [R, H)}, so it sits outside {@code Q}.
     */
    private MapValue findKey(LiveViewCheckpointRowsPlan plan, Record record) {
        final MapKey key = keyMap.withKey();
        key.put(record, plan.getKeySink());
        return key.findValue();
    }

    /**
     * Whether the open scan has taken this discovery past its row budget, counting the
     * {@code qualifyingRows} it has returned so far against the rows every earlier scan
     * already spent. The budget is per discovery rather than per scan, so a repair turn
     * costs one budget however many searches answering it takes.
     */
    private boolean isOverScanRowBudget(Function filter, long qualifyingRows) {
        return scanRows + baseRowsConsumed(filter, qualifyingRows) > scanRowBudget;
    }

    /**
     * Builds the two maps this plan's key shape needs: the per-row counter map the scans
     * read through, and the per-key checkpoint-form map {@code Q} is collected out of.
     * One refresh worker serves many views and a map's key layout is fixed at
     * construction, so both are rebuilt per discovery rather than reused across key
     * shapes they were not built for.
     */
    private void openKeyMap(LiveViewCheckpointRowsPlan plan) {
        keyMap = Misc.free(keyMap);
        outputKeyMap = Misc.free(outputKeyMap);
        checkpointKeyColumnTypes = plan.getCheckpointKeyColumnTypes();
        keyMap = MapFactory.createUnorderedMap(configuration, plan.getKeyColumnTypes(), VALUE_TYPES);
        outputKeyMap = MapFactory.createUnorderedMap(configuration, checkpointKeyColumnTypes, OUTPUT_KEY_VALUE_TYPES);
    }

    /**
     * Binds one freshly opened page-frame cursor to the stack the scan reads through: the
     * plan's key projector first, then the view's own {@code WHERE}. Both bindings are per
     * cursor - a key function and a filter resolve symbols through the cursor's own symbol
     * tables - so every search goes through here rather than binding once per discovery.
     */
    private RecordCursor openSource(
            LiveViewCheckpointRowsPlan plan,
            RecordCursor pageCursor,
            Function filter,
            SqlExecutionContext executionContext
    ) throws SqlException {
        plan.initKeyFunctions(pageCursor, executionContext);
        if (filter == null) {
            return pageCursor;
        }
        filteringCursor.of(pageCursor, filter, executionContext);
        return filteringCursor;
    }

    /**
     * Walks every row down from {@code R} until each key in {@code Q} has {@code Nmax}
     * qualifying predecessors, and takes the timestamp of the row that satisfied the last
     * one as {@code L}. Because {@code L} is an inclusive bound, the complete tie at that
     * timestamp comes with it; over-feeding a bounded ROWS frame costs nothing, since the
     * extra rows fall out of the frame before it reaches {@code R}.
     * <p>
     * The row budget stops the walk where the history runs out would have: at {@code S}.
     * That floor is safe whichever way the walk ended - it is the lowest floor there is,
     * and warming up from it only feeds rows that leave the frame again before {@code R} -
     * so the budget costs the repair its localization below, not its correctness. What
     * separates the two endings is the recorded status: exhaustion proves the keys have no
     * more history, the budget proves nothing at all.
     */
    private void scanDependencyLowTs(
            LiveViewCheckpointRowsPlan plan,
            PageFrameRecordCursorFactory pageFrameFactory,
            SqlExecutionContext executionContext,
            Function filter,
            long viewLowerBoundTs,
            long outputLowTs
    ) throws SqlException {
        final long precedingRows = plan.getMaxPrecedingRows();
        long pendingKeys = outputKeyCount;
        try (RecordCursor pageCursor = pageFrameFactory.getCursorInTimestampRangeBackward(
                executionContext,
                viewLowerBoundTs,
                outputLowTs - 1
        )) {
            final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
            final RecordCursor source = openSource(plan, pageCursor, filter, executionContext);
            final Record record = source.getRecord();
            while (source.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                backwardScanRows++;
                if (isOverScanRowBudget(filter, backwardScanRows)) {
                    // L stays at S, the floor a walk that ran out of history also lands on.
                    scanBudgetStatus = ScanBudgetStatus.ROWS_EXCEEDED;
                    return;
                }
                final MapValue value = findKey(plan, record);
                if (value == null) {
                    // Outside Q: this key's rows warm nothing the replacement re-emits.
                    continue;
                }
                final long preceding = value.getLong(IDX_PRECEDING);
                if (preceding < precedingRows) {
                    value.putLong(IDX_PRECEDING, preceding + 1);
                    if (preceding + 1 == precedingRows && --pendingKeys == 0) {
                        dependencyLowTs = record.getTimestamp(plan.getTimestampIndex());
                        return;
                    }
                }
            }
        } finally {
            scanRows += baseRowsConsumed(filter, backwardScanRows);
        }
    }

    /**
     * Seeks each key in {@code Q} down from {@code R} through its own index cursor,
     * taking its {@code Nmax}-th qualifying predecessor, and reports the lowest such
     * timestamp as {@code L}. Every key must be able to reach {@code R} from below, so
     * the floor is the deepest of the per-key answers - the same row the unrestricted
     * walk stops on, arrived at without reading any other key's rows.
     * <p>
     * A key with fewer than {@code Nmax} predecessors ends the search rather than
     * lowering the floor further: {@code S} is already the lowest floor there is, so the
     * remaining keys cannot change the answer and their seeks are skipped.
     * <p>
     * The row budget ends it the same way, and for the same reason it cannot publish the
     * minimum it has collected so far: a key not yet sought may need a deeper floor than
     * every key already answered, so a partial minimum is not a floor at all. What the
     * budget bounds here is the {@code Nmax} rows one seek reads under a filter that
     * rejects most of them; the number of seeks is bounded already, by the key budget the
     * forward pass enforced on {@code Q}.
     */
    private void seekDependencyLowTs(
            LiveViewCheckpointRowsPlan plan,
            PageFrameRecordCursorFactory pageFrameFactory,
            SqlExecutionContext executionContext,
            Function filter,
            long viewLowerBoundTs,
            long outputLowTs
    ) throws SqlException {
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        final long precedingRows = plan.getMaxPrecedingRows();
        final int timestampIndex = plan.getTimestampIndex();
        long floor = Long.MAX_VALUE;
        for (int i = 0, n = outputKeys.size(); i < n; i++) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            indexedKeyLookups++;
            long preceding = 0;
            long keyLowTs = Numbers.LONG_NULL;
            try (RecordCursor pageCursor = pageFrameFactory.getCursorInTimestampRangeBackwardIndexed(
                    executionContext,
                    viewLowerBoundTs,
                    outputLowTs - 1,
                    indexedKeyColumnIndex,
                    outputKeys.getQuick(i)
            )) {
                final RecordCursor source = openSource(plan, pageCursor, filter, executionContext);
                final Record record = source.getRecord();
                while (preceding < precedingRows && source.hasNext()) {
                    backwardScanRows++;
                    preceding++;
                    if (isOverScanRowBudget(filter, preceding)) {
                        scanBudgetStatus = ScanBudgetStatus.ROWS_EXCEEDED;
                        return;
                    }
                    keyLowTs = record.getTimestamp(timestampIndex);
                }
            } finally {
                scanRows += baseRowsConsumed(filter, preceding);
            }
            if (preceding < precedingRows) {
                return;
            }
            floor = Math.min(floor, keyLowTs);
        }
        dependencyLowTs = floor;
    }

    /**
     * Which budget, if any, stopped one discovery. Every value leaves a usable result -
     * the bound the stopped search was proving falls back to what an unlocalized repair
     * would use - so this reports the cost of the answer rather than its validity.
     */
    public enum ScanBudgetStatus {
        /**
         * No budget bound. Both bounds are what the data proved, whether or not the data
         * proved a finite one.
         */
        WITHIN,
        /**
         * The base-row budget stopped a scan. A forward stop leaves {@code Q} incomplete,
         * so neither bound survives it: {@code H} stays at end-of-frame and {@code L} at
         * {@code S}. A backward stop keeps whatever {@code H} the forward pass proved and
         * drops {@code L} to {@code S}.
         */
        ROWS_EXCEEDED,
        /**
         * The output-key budget stopped the forward scan: the replacement interval holds
         * more partition keys than one repair may plan to re-emit. {@code Q} is a fragment
         * of the interval, so {@code H} stays at end-of-frame and {@code L} at {@code S}.
         */
        KEYS_EXCEEDED
    }
}
