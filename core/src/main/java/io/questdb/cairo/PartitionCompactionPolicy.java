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

package io.questdb.cairo;

import io.questdb.std.LongIntHashMap;
import io.questdb.std.LongList;
import io.questdb.std.LongLongMaxHeap;
import io.questdb.std.Mutable;
import io.questdb.std.datetime.microtime.Micros;

/**
 * Decides WHICH partition a commit should compact, and why.
 */
public class PartitionCompactionPolicy implements Mutable {
    public static final int REASON_AGE = 3;
    public static final int REASON_NONE = 0;
    public static final int REASON_PIECE_COUNT = 2;
    public static final int REASON_TABLE_PRESSURE = 4;
    public static final int REASON_WASTE_RATIO = 1;
    private static final int AGE_BITS = 30;
    private static final long AGE_MASK = (1L << AGE_BITS) - 1;
    private static final int BACKOFF_LONGS = 3;
    // Enough history to rank every partition written in the last ten years to the second. The remaining
    // 24 years in the 30-bit field are future headroom before the priority epoch needs rebuilding.
    private static final long EPOCH_HISTORY_SECONDS = 10L * 365 * 24 * 60 * 60;
    private static final int HEAP_ENTRY_LONGS = 2;
    private static final int HEAP_REBUILD_MIN_SIZE = 64;
    // Bounded so a table with a great many partitions cannot grow this list without end.
    private static final int MAX_TRACKED = 256;
    private static final int PRIORITY_TIER_AGE = 1;
    private static final int PRIORITY_TIER_PIECES = 2;
    private static final int PRIORITY_TIER_WASTE = 3;
    private static final int SEVERITY_BITS = 20;
    private static final long SEVERITY_MASK = (1L << SEVERITY_BITS) - 1;
    private static final int SEVERITY_SHIFT = AGE_BITS;
    private static final int STATE_DEAD_ROWS_OFFSET = 1;
    private static final int STATE_LONGS = 4;
    private static final int STATE_PRIORITY_OFFSET = 2;
    private static final int STATE_REF_OFFSET = 3;
    private static final int TIER_SHIFT = AGE_BITS + SEVERITY_BITS;
    private static final long WASTE_PERCENT_MAX = 1_000;
    // (partitionTimestamp, nextAttemptMicros, currentBackoffMicros)
    private final LongList backoff = new LongList();
    private final CairoConfiguration configuration;
    // Valid heap entries are adjacent (priority, partitionTimestamp) longs. Geometry updates append a new
    // entry and make any entry with a different priority stale; equal-priority duplicates are interchangeable
    // because selection resolves current geometry by timestamp. The selector drops stale heads, and a bounded
    // rebuild removes stale entries that never reach the head.
    private final LongList deferredHeapEntries = new LongList();
    private final LongLongMaxHeap heap = new LongLongMaxHeap();
    // Dense (partitionTimestamp, deadRows, priority, geometryRef) records. The map stores record offsets.
    private final LongList partitionStates = new LongList();
    private final LongIntHashMap stateIndexByTimestamp = new LongIntHashMap(16, 0.5, Long.MIN_VALUE);
    private long epochEndSeconds;
    private long epochStartSeconds;
    private boolean isInitialized;
    private boolean isSelectedPartitionHot;
    private long lastAvgRecordSize = -1;
    private long lastDeadMinSize = -1;
    private double lastDeadRowsRatio = Double.NaN;
    private long lastPieceAvgRowsLimit = -1;
    private int lastPieceThreshold = -1;
    private int selectedPartitionIndex = -1;
    private int selectedReason = REASON_NONE;
    private boolean tablePressureOn;
    private long totalDeadRows;

    public PartitionCompactionPolicy(CairoConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void clear() {
        backoff.clear();
        deferredHeapEntries.clear();
        heap.clear();
        partitionStates.clear();
        stateIndexByTimestamp.clear();
        epochEndSeconds = 0;
        epochStartSeconds = 0;
        isInitialized = false;
        isSelectedPartitionHot = false;
        lastAvgRecordSize = -1;
        lastDeadMinSize = -1;
        lastDeadRowsRatio = Double.NaN;
        lastPieceAvgRowsLimit = -1;
        lastPieceThreshold = -1;
        selectedPartitionIndex = -1;
        selectedReason = REASON_NONE;
        tablePressureOn = false;
        totalDeadRows = 0;
    }

    /**
     * The piece-count rule's actual cap for a folder holding {@code liveRows} live rows: never below the flat floor
     * {@link CairoConfiguration#getPartitionCompactionPieceThreshold()}, but scaled up for a large folder so it is not
     * flagged at the same absolute piece count a small one would be.
     */
    public static int effectiveMaxPieces(CairoConfiguration configuration, long liveRows) {
        final long scaled = liveRows / configuration.getPartitionCompactionAvgRowsPieceLim();
        return (int) Math.max(configuration.getPartitionCompactionPieceThreshold(), scaled);
    }

    /**
     * True if a piece count of {@code pieceCount}, or a dead-versus-live row split of {@code deadRows} against {@code
     * liveRows}, already crosses the same waste-ratio or piece-count thresholds {@link #selectPartition} enforces after
     * the fact.
     */
    public static boolean exceedsThresholds(
            CairoConfiguration configuration, long liveRows, long deadRows, int pieceCount, long avgRecordSize
    ) {
        if (pieceCount > effectiveMaxPieces(configuration, liveRows)) {
            return true;
        }
        final long deadMinRows = avgRecordSize > 0
                ? configuration.getPartitionCompactionDeadMinSize() / avgRecordSize
                : configuration.getPartitionCompactionDeadMinSize();
        return deadRows > configuration.getPartitionCompactionDeadRowsRatio() * liveRows && deadRows > deadMinRows;
    }

    public int getSelectedPartitionIndex() {
        return selectedPartitionIndex;
    }

    /**
     * Why the last {@link #selectPartition} picked what it picked.
     */
    public int getSelectedReason() {
        return selectedReason;
    }

    /**
     * True when the last {@link #selectPartition} picked, for {@link #REASON_TABLE_PRESSURE} alone, a
     * partition one of the last {@link CairoConfiguration#getPartitionCompactionHotCommits()} commits
     * wrote. The caller withholds REWRITE from such a partition; every cheaper step stays available.
     */
    public boolean isSelectedPartitionHot() {
        return isSelectedPartitionHot;
    }

    public void onCompacted(long partitionTimestamp) {
        clearBackoff(partitionTimestamp);
    }

    public void onDeclined(long partitionTimestamp, long nowMicros) {
        final long max = configuration.getPartitionCompactionDeclineBackoffMax();
        for (int i = 0, n = backoff.size(); i < n; i += BACKOFF_LONGS) {
            if (backoff.getQuick(i) == partitionTimestamp) {
                final long next = Math.min(max, Math.max(Micros.MINUTE_MICROS, backoff.getQuick(i + 2) * 2));
                backoff.setQuick(i + 1, nowMicros + next);
                backoff.setQuick(i + 2, next);
                return;
            }
        }
        if (backoff.size() >= MAX_TRACKED * BACKOFF_LONGS) {
            backoff.removeIndexBlock(0, BACKOFF_LONGS);
        }
        backoff.add(partitionTimestamp, nowMicros + Micros.MINUTE_MICROS);
        backoff.add(Micros.MINUTE_MICROS);
    }

    /**
     * Removes a partition from the incrementally maintained state. Heap entries remain until lazy validation
     * reaches them or the stale-entry bound rebuilds the heap.
     */
    public void onPartitionRemoved(long partitionTimestamp) {
        if (isInitialized) {
            removeState(partitionTimestamp);
        }
    }

    /**
     * Refreshes one partition after the writer publishes a new geometry reference. The callback is ignored before
     * the initial scan; {@link #selectPartition} seeds the complete state on its first call.
     */
    public void onPartitionUpdated(
            TxWriter txWriter,
            PartitionGeometry geometry,
            long partitionTimestamp,
            long avgRecordSize
    ) {
        if (!isInitialized) {
            return;
        }
        final int partitionIndex = txWriter.getPartitionIndex(partitionTimestamp);
        if (partitionIndex < 0 || !txWriter.isPartitionComposite(partitionIndex)) {
            removeState(partitionTimestamp);
            return;
        }
        putState(txWriter, geometry, partitionIndex, avgRecordSize);
        rebuildHeapIfNeeded();
    }

    /**
     * Picks the highest-priority partition. Waste-ratio candidates outrank piece-count candidates; piece-count
     * candidates outrank age/table-pressure candidates. Within those tiers the heap orders by waste percentage,
     * piece count and age respectively.
     */
    public int selectPartition(
            TxWriter txWriter,
            PartitionGeometry geometry,
            long avgRecordSize,
            long nowMicros,
            int fromIndex
    ) {
        selectedReason = REASON_NONE;
        selectedPartitionIndex = -1;
        isSelectedPartitionHot = false;
        if (txWriter.getLagRowCount() > 0) {
            return -1;
        }
        final int n = txWriter.getPartitionCount();
        if (n <= 0) {
            return -1;
        }
        ensureInitialized(txWriter, geometry, avgRecordSize, nowMicros, fromIndex);
        updateTablePressure(txWriter, avgRecordSize);

        deferredHeapEntries.clear();
        try {
            while (!heap.isEmpty()) {
                final long priority = heap.peekKey();
                final long partitionTimestamp = heap.peekValue();
                final int stateIndex = stateIndexByTimestamp.get(partitionTimestamp);
                if (stateIndex < 0) {
                    heap.pop();
                    continue;
                }
                assert stateIndex % STATE_LONGS == 0;
                assert partitionStates.getQuick(stateIndex) == partitionTimestamp;
                if (partitionStates.getQuick(stateIndex + STATE_PRIORITY_OFFSET) != priority) {
                    heap.pop();
                    continue;
                }

                final int partitionIndex = txWriter.getPartitionIndex(partitionTimestamp);
                assert partitionIndex >= 0
                        : "missing heap partition [partitionTimestamp=" + partitionTimestamp + ']';
                if (partitionIndex < 0) {
                    heap.pop();
                    removeState(partitionTimestamp);
                    continue;
                }
                assert txWriter.isPartitionComposite(partitionIndex)
                        : "plain heap partition [partitionTimestamp=" + partitionTimestamp + ']';
                if (!txWriter.isPartitionComposite(partitionIndex)) {
                    heap.pop();
                    removeState(partitionTimestamp);
                    continue;
                }
                final long currentGeometryRef = txWriter.getGeometryRef(partitionIndex);
                final long stateGeometryRef = partitionStates.getQuick(stateIndex + STATE_REF_OFFSET);
                assert currentGeometryRef == stateGeometryRef
                        : "stale heap partition ref [partitionTimestamp=" + partitionTimestamp
                        + ", stateGeometryRef=" + stateGeometryRef
                        + ", currentGeometryRef=" + currentGeometryRef + ']';
                if (currentGeometryRef != stateGeometryRef) {
                    heap.pop();
                    putState(txWriter, geometry, partitionIndex, avgRecordSize);
                    continue;
                }
                if (isSuppressed(partitionTimestamp, nowMicros)) {
                    deferHeapHead();
                    continue;
                }

                final int tier = priorityTier(priority);
                final int reason;
                if (tier == PRIORITY_TIER_WASTE) {
                    reason = REASON_WASTE_RATIO;
                } else if (tier == PRIORITY_TIER_PIECES) {
                    reason = REASON_PIECE_COUNT;
                } else {
                    final long lastWrite = geometry.getLastWriteMicros(partitionIndex);
                    if (tablePressureOn) {
                        reason = REASON_TABLE_PRESSURE;
                    } else if (lastWrite > 0
                            && nowMicros - lastWrite > configuration.getPartitionCompactionIdleTimeout()
                            && (partitionStates.getQuick(stateIndex + STATE_DEAD_ROWS_OFFSET) > 0
                            || geometry.getPieceCount(partitionIndex) > 1)) {
                        reason = REASON_AGE;
                    } else if (lastWrite <= 0) {
                        // Unknown provenance sorts as oldest for table pressure but cannot satisfy the age rule.
                        // Skip it temporarily because an older known record below it may satisfy that rule.
                        deferHeapHead();
                        continue;
                    } else {
                        // This is the oldest known tier-1 partition. If it is not old enough, none below it is.
                        return -1;
                    }
                }

                selectedReason = reason;
                selectedPartitionIndex = partitionIndex;
                if (reason == REASON_TABLE_PRESSURE) {
                    final int hotCommits = configuration.getPartitionCompactionHotCommits();
                    final long writerTxn = geometry.getWriterTxn(partitionIndex);
                    isSelectedPartitionHot = hotCommits > 0
                            && writerTxn >= 0
                            && writerTxn > txWriter.getTxn() - hotCommits;
                }
                return partitionIndex;
            }
            return -1;
        } finally {
            restoreDeferredHeapEntries();
        }
    }

    /**
     * The index of the next eligible partition at or after {@code fromIndex} that holds more than one piece, or -1.
     */
    public int selectFoldablePartition(TxWriter txWriter, PartitionGeometry geometry, long nowMicros, int fromIndex) {
        if (txWriter.getLagRowCount() > 0) {
            return -1;
        }
        int selected = -1;
        for (int stateIndex = 0, n = partitionStates.size(); stateIndex < n; stateIndex += STATE_LONGS) {
            final long partitionTimestamp = partitionStates.getQuick(stateIndex);
            assert isStateCurrent(txWriter, stateIndex, partitionTimestamp)
                    : "stale foldable partition state [partitionTimestamp=" + partitionTimestamp + ']';
            if (isSuppressed(partitionTimestamp, nowMicros)) {
                continue;
            }
            final int partitionIndex = txWriter.getPartitionIndex(partitionTimestamp);
            if (partitionIndex >= fromIndex
                    && (selected < 0 || partitionIndex < selected)
                    && txWriter.isPartitionComposite(partitionIndex)
                    && geometry.getPieceCount(partitionIndex) > 1) {
                selected = partitionIndex;
            }
        }
        selectedPartitionIndex = selected;
        return selected;
    }

    /**
     * True when {@code partitionIndex} is a composite partition already reduced to a single piece sitting at row 0 -
     * MOVE-TAIL's own end state, JOIN folding everything into one, or any commit that merely happened to leave it that
     * way - with real dead space above it, and none of the ordinary reasons a partition is off-limits (the last/active.
     */
    public static boolean isMakePlainShape(TxReader txWriter, PartitionGeometry geometry, int partitionIndex) {
        if (partitionIndex >= txWriter.getPartitionCount() - 1 || txWriter.getLagRowCount() > 0) {
            return false;
        }
        if (!txWriter.isPartitionComposite(partitionIndex)) {
            return false;
        }
        if (geometry.getPieceCount(partitionIndex) != 1 || geometry.getPieceRowOffset(partitionIndex, 0) != 0) {
            return false;
        }
        // A non-composite piece's tsLo is always the directory's own floor; a composite one can differ if
        // an earlier piece that used to sit before it was ever dropped. Only a match keeps becoming plain
        // from quietly relabelling the partition's routing floor.
        if (geometry.getPieceTimestampLo(partitionIndex, 0) != txWriter.getPartitionTimestampByIndex(partitionIndex)) {
            return false;
        }
        // E equal to the live rows is MAKE-PLAIN's own halfway state: it lowered E, then TRIM-FILES failed.
        return geometry.getE(partitionIndex) >= txWriter.getPartitionSize(partitionIndex);
    }

    /**
     * The index of the next MAKE-PLAIN candidate at or after {@code fromIndex} - see {@link #isMakePlainShape} - or -1.
     */
    public int selectMakePlainCandidate(TxWriter txWriter, PartitionGeometry geometry, long nowMicros, int fromIndex) {
        int selected = -1;
        for (int stateIndex = 0, n = partitionStates.size(); stateIndex < n; stateIndex += STATE_LONGS) {
            final long partitionTimestamp = partitionStates.getQuick(stateIndex);
            assert isStateCurrent(txWriter, stateIndex, partitionTimestamp)
                    : "stale make-plain partition state [partitionTimestamp=" + partitionTimestamp + ']';
            if (isSuppressed(partitionTimestamp, nowMicros)) {
                continue;
            }
            final int partitionIndex = txWriter.getPartitionIndex(partitionTimestamp);
            if (partitionIndex >= fromIndex
                    && (selected < 0 || partitionIndex < selected)
                    && isMakePlainShape(txWriter, geometry, partitionIndex)) {
                selected = partitionIndex;
            }
        }
        return selected;
    }

    private static long clamp(long value, long min, long max) {
        return Math.max(min, Math.min(value, max));
    }

    private static int priorityTier(long priority) {
        return (int) (priority >>> TIER_SHIFT);
    }

    private long ageRank(long lastWriteMicros) {
        if (lastWriteMicros <= 0) {
            return AGE_MASK;
        }
        final long lastWriteSeconds = lastWriteMicros / Micros.SECOND_MICROS;
        return clamp(epochEndSeconds - lastWriteSeconds, 0, AGE_MASK);
    }

    private long calculatePriority(
            long liveRows,
            long deadRows,
            int pieceCount,
            long lastWriteMicros,
            long avgRecordSize
    ) {
        final long deadMinRows = avgRecordSize > 0
                ? configuration.getPartitionCompactionDeadMinSize() / avgRecordSize
                : configuration.getPartitionCompactionDeadMinSize();
        final int tier;
        final long severity;
        if (deadRows > configuration.getPartitionCompactionDeadRowsRatio() * liveRows && deadRows > deadMinRows) {
            tier = PRIORITY_TIER_WASTE;
            severity = wastePercent(deadRows, liveRows);
        } else if (pieceCount > effectiveMaxPieces(configuration, liveRows)) {
            tier = PRIORITY_TIER_PIECES;
            severity = Math.min(1_000_000L, pieceCount);
        } else {
            tier = PRIORITY_TIER_AGE;
            severity = 0;
        }
        assert severity <= SEVERITY_MASK;
        return ((long) tier << TIER_SHIFT) | (severity << SEVERITY_SHIFT) | ageRank(lastWriteMicros);
    }

    private static long wastePercent(long deadRows, long liveRows) {
        if (liveRows <= 0) {
            return WASTE_PERCENT_MAX;
        }
        if (deadRows <= 0) {
            return 0;
        }

        final long whole = deadRows / liveRows;
        if (whole >= WASTE_PERCENT_MAX / 100) {
            return WASTE_PERCENT_MAX;
        }

        // Calculate floor((deadRows % liveRows) * 100 / liveRows) without overflowing long. For a candidate
        // percentage p, ceil(p * liveRows / 100) is the smallest remainder that reaches p percent. Splitting
        // liveRows into quotient and remainder before multiplication keeps every intermediate in range.
        final long remainder = deadRows % liveRows;
        if (remainder <= Long.MAX_VALUE / 100) {
            return whole * 100 + remainder * 100 / liveRows;
        }
        final long liveHundreds = liveRows / 100;
        final long liveRemainder = liveRows % 100;
        int lo = 0;
        int hi = 99;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final long threshold = mid * liveHundreds + (mid * liveRemainder + 99) / 100;
            if (remainder >= threshold) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return whole * 100 + hi;
    }

    private void clearBackoff(long partitionTimestamp) {
        if (backoff.size() == 0) {
            return;
        }
        for (int i = 0, n = backoff.size(); i < n; i += BACKOFF_LONGS) {
            if (backoff.getQuick(i) == partitionTimestamp) {
                backoff.removeIndexBlock(i, BACKOFF_LONGS);
                return;
            }
        }
    }

    private void deferHeapHead() {
        deferredHeapEntries.add(heap.peekKey(), heap.peekValue());
        heap.pop();
    }

    private void ensureInitialized(
            TxWriter txWriter,
            PartitionGeometry geometry,
            long avgRecordSize,
            long nowMicros,
            int fromIndex
    ) {
        final long nowSeconds = nowMicros / Micros.SECOND_MICROS;
        if (!isInitialized
                || nowSeconds < epochStartSeconds
                || nowSeconds >= epochEndSeconds
                || hasPriorityConfigurationChanged(avgRecordSize)) {
            rebuild(txWriter, geometry, avgRecordSize, nowMicros, fromIndex);
        }
    }

    private boolean hasPriorityConfigurationChanged(long avgRecordSize) {
        return lastAvgRecordSize != avgRecordSize
                || lastDeadMinSize != configuration.getPartitionCompactionDeadMinSize()
                || Double.compare(lastDeadRowsRatio, configuration.getPartitionCompactionDeadRowsRatio()) != 0
                || lastPieceAvgRowsLimit != configuration.getPartitionCompactionAvgRowsPieceLim()
                || lastPieceThreshold != configuration.getPartitionCompactionPieceThreshold();
    }

    private boolean isStateCurrent(TxWriter txWriter, int stateIndex, long partitionTimestamp) {
        final int partitionIndex = txWriter.getPartitionIndex(partitionTimestamp);
        return partitionIndex >= 0
                && txWriter.isPartitionComposite(partitionIndex)
                && txWriter.getGeometryRef(partitionIndex) == partitionStates.getQuick(stateIndex + STATE_REF_OFFSET);
    }

    private boolean isSuppressed(long partitionTimestamp, long nowMicros) {
        // The overwhelmingly common case is no partition on backoff at all - nothing has been declined - so
        // the check costs nothing per composite partition per pass until a decline populates the list.
        if (backoff.size() == 0) {
            return false;
        }
        for (int i = 0, n = backoff.size(); i < n; i += BACKOFF_LONGS) {
            if (backoff.getQuick(i) == partitionTimestamp) {
                return nowMicros < backoff.getQuick(i + 1);
            }
        }
        return false;
    }

    private void putState(TxWriter txWriter, PartitionGeometry geometry, int partitionIndex, long avgRecordSize) {
        final long partitionTimestamp = txWriter.getPartitionTimestampByIndex(partitionIndex);
        final long liveRows = txWriter.getPartitionSize(partitionIndex);
        final long deadRows = geometry.getE(partitionIndex) - liveRows;
        final int pieceCount = geometry.getPieceCount(partitionIndex);
        final long priority = calculatePriority(
                liveRows,
                deadRows,
                pieceCount,
                geometry.getLastWriteMicros(partitionIndex),
                avgRecordSize
        );
        final long geometryRef = txWriter.getGeometryRef(partitionIndex);

        final int stateIndex = stateIndexByTimestamp.get(partitionTimestamp);
        if (stateIndex < 0) {
            final int newStateIndex = partitionStates.size();
            partitionStates.add(partitionTimestamp, deadRows, priority, geometryRef);
            stateIndexByTimestamp.put(partitionTimestamp, newStateIndex);
        } else {
            assert stateIndex % STATE_LONGS == 0;
            assert partitionStates.getQuick(stateIndex) == partitionTimestamp;
            totalDeadRows -= partitionStates.getQuick(stateIndex + STATE_DEAD_ROWS_OFFSET);
            partitionStates.setQuick(stateIndex + STATE_DEAD_ROWS_OFFSET, deadRows);
            partitionStates.setQuick(stateIndex + STATE_PRIORITY_OFFSET, priority);
            partitionStates.setQuick(stateIndex + STATE_REF_OFFSET, geometryRef);
        }
        assert partitionStates.size() == stateIndexByTimestamp.size() * STATE_LONGS;
        totalDeadRows += deadRows;
        heap.push(priority, partitionTimestamp);
    }

    private void rebuild(
            TxWriter txWriter,
            PartitionGeometry geometry,
            long avgRecordSize,
            long nowMicros,
            int fromIndex
    ) {
        deferredHeapEntries.clear();
        heap.clear();
        partitionStates.clear();
        stateIndexByTimestamp.clear();
        totalDeadRows = 0;

        final long nowSeconds = nowMicros / Micros.SECOND_MICROS;
        epochStartSeconds = nowSeconds - EPOCH_HISTORY_SECONDS;
        epochEndSeconds = epochStartSeconds + AGE_MASK;
        lastAvgRecordSize = avgRecordSize;
        lastDeadMinSize = configuration.getPartitionCompactionDeadMinSize();
        lastDeadRowsRatio = configuration.getPartitionCompactionDeadRowsRatio();
        lastPieceAvgRowsLimit = configuration.getPartitionCompactionAvgRowsPieceLim();
        lastPieceThreshold = configuration.getPartitionCompactionPieceThreshold();
        isInitialized = true;

        for (int i = Math.max(0, fromIndex), n = txWriter.getPartitionCount(); i < n; i++) {
            if (txWriter.isPartitionComposite(i)) {
                putState(txWriter, geometry, i, avgRecordSize);
            }
        }
    }

    private void rebuildHeapIfNeeded() {
        final int maxHeapSize = Math.max(HEAP_REBUILD_MIN_SIZE, stateIndexByTimestamp.size() * 4);
        if (heap.size() <= maxHeapSize) {
            return;
        }
        heap.clear();
        for (int stateIndex = 0, n = partitionStates.size(); stateIndex < n; stateIndex += STATE_LONGS) {
            heap.push(
                    partitionStates.getQuick(stateIndex + STATE_PRIORITY_OFFSET),
                    partitionStates.getQuick(stateIndex)
            );
        }
    }

    private void removeState(long partitionTimestamp) {
        final int stateIndex = stateIndexByTimestamp.get(partitionTimestamp);
        if (stateIndex < 0) {
            return;
        }
        assert stateIndex % STATE_LONGS == 0;
        assert partitionStates.getQuick(stateIndex) == partitionTimestamp;
        totalDeadRows -= partitionStates.getQuick(stateIndex + STATE_DEAD_ROWS_OFFSET);
        stateIndexByTimestamp.remove(partitionTimestamp);

        final int lastStateIndex = partitionStates.size() - STATE_LONGS;
        if (stateIndex < lastStateIndex) {
            final long movedTimestamp = partitionStates.getQuick(lastStateIndex);
            for (int i = 0; i < STATE_LONGS; i++) {
                partitionStates.setQuick(stateIndex + i, partitionStates.getQuick(lastStateIndex + i));
            }
            stateIndexByTimestamp.put(movedTimestamp, stateIndex);
        }
        partitionStates.setPos(lastStateIndex);
        assert partitionStates.size() == stateIndexByTimestamp.size() * STATE_LONGS;
        assert totalDeadRows >= 0;
    }

    private void restoreDeferredHeapEntries() {
        for (int i = 0, n = deferredHeapEntries.size(); i < n; i += HEAP_ENTRY_LONGS) {
            heap.push(deferredHeapEntries.getQuick(i), deferredHeapEntries.getQuick(i + 1));
        }
        deferredHeapEntries.clear();
    }

    private void updateTablePressure(TxWriter txWriter, long avgRecordSize) {
        // Express dead rows as a percentage of the table's live, user-visible rows, not of the
        // physical live-plus-dead extent. For example, 100 live rows and 50 dead rows means 50% dead,
        // even though the column files physically hold 150 rows.
        // Two thresholds, not one: with a single one the rule would switch on and off around that point
        // and queue a partition on every commit forever.
        final long tableRowCount = txWriter.getRowCount();
        final long deadBytes = totalDeadRows * Math.max(1, avgRecordSize);
        if (tablePressureOn) {
            tablePressureOn = !(totalDeadRows * 100 < tableRowCount * configuration.getPartitionCompactionTableDeadStopPercent()
                    && deadBytes <= configuration.getPartitionCompactionTableDeadTrigger() / 2);
        } else {
            // tableRowCount == 0 must never turn this on: 0 >= 0 would otherwise satisfy the percentage
            // check trivially, latching table pressure on from the first commit of an empty table.
            tablePressureOn = (tableRowCount > 0 && deadBytes >= configuration.getPartitionCompactionTableDeadThreshold()
                    && totalDeadRows * 100 >= tableRowCount * configuration.getPartitionCompactionTableDeadThresholdPercent())
                    || deadBytes > configuration.getPartitionCompactionTableDeadTrigger();
        }
    }
}
