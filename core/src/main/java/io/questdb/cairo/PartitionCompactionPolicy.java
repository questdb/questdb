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

import io.questdb.std.LongList;
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
    private static final int BACKOFF_LONGS = 3;
    // Bounded so a table with a great many partitions cannot grow this list without end.
    private static final int MAX_TRACKED = 256;
    // (partitionTimestamp, nextAttemptMicros, currentBackoffMicros)
    private final LongList backoff = new LongList();
    private final CairoConfiguration configuration;
    private int selectedPartitionIndex = -1;
    private int selectedReason = REASON_NONE;
    private boolean tablePressureOn;

    public PartitionCompactionPolicy(CairoConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void clear() {
        backoff.clear();
        tablePressureOn = false;
        selectedReason = REASON_NONE;
        selectedPartitionIndex = -1;
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
     * Records that the partition was compacted, clearing any decline backoff a prior attempt left behind.
     */
    public void onCompacted(long partitionTimestamp) {
        clearBackoff(partitionTimestamp);
    }

    /**
     * Records that the partition could not be compacted this time, and doubles how long to wait before trying again.
     */
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
     * The partition index to compact next, or -1.
     */
    public int selectPartition(TxWriter txWriter, PartitionGeometry geometry, long avgRecordSize, long nowMicros) {
        selectedReason = REASON_NONE;
        selectedPartitionIndex = -1;
        if (txWriter.getLagRowCount() > 0) {
            return -1;
        }
        final int n = txWriter.getPartitionCount();
        if (n <= 0) {
            return -1;
        }
        final long deadMinRows = avgRecordSize > 0
                ? configuration.getPartitionCompactionDeadMinSize() / avgRecordSize
                : configuration.getPartitionCompactionDeadMinSize();
        final long idleTimeout = configuration.getPartitionCompactionIdleTimeout();
        final double ratio = configuration.getPartitionCompactionDeadRowsRatio();

        int chosen = -1;
        int chosenReason = REASON_NONE;
        int coldest = -1;
        long coldestMicros = Long.MAX_VALUE;
        long coldestTs = Long.MAX_VALUE;
        long deadRowsTable = 0;
        long liveRowsTable = 0;

        for (int i = 0; i < n; i++) {
            final long live = txWriter.getPartitionSize(i);
            final long e = geometry.getE(i);
            final int pieces = geometry.getPieceCount(i);
            // The gate is "any composite partition" - pieces>1 or dead space above the live rows - not "more than one
            // piece starting above row 0" as PARTITION_COMPACTION.md Sec.4 first states.
            if (pieces < 2 && e <= live) {
                continue;
            }
            final long partitionTs = txWriter.getPartitionTimestampByIndex(i);
            if (isSuppressed(partitionTs, nowMicros)) {
                continue;
            }
            final long dead = e - live;
            deadRowsTable += dead;
            liveRowsTable += live;

            final long lastWrite = geometry.getLastWriteMicros(i);
            if (lastWrite < coldestMicros || (lastWrite == coldestMicros && partitionTs < coldestTs)) {
                coldest = i;
                coldestMicros = lastWrite;
                coldestTs = partitionTs;
            }
            if (chosen > -1) {
                continue; // still counting totals for the table-wide rule
            }
            if (dead > ratio * live && dead > deadMinRows) {
                chosen = i;
                chosenReason = REASON_WASTE_RATIO;
            } else if (pieces > effectiveMaxPieces(configuration, live)) {
                chosen = i;
                chosenReason = REASON_PIECE_COUNT;
            } else if (lastWrite > 0 && nowMicros - lastWrite > idleTimeout && (dead > 0 || pieces > 1)) {
                chosen = i;
                chosenReason = REASON_AGE;
            }
        }

        // Two thresholds, not one: with a single one the rule would switch on and off around that point
        // and queue a partition on every commit forever.
        final long total = deadRowsTable + liveRowsTable;
        final long deadBytes = deadRowsTable * Math.max(1, avgRecordSize);
        if (tablePressureOn) {
            tablePressureOn = !(deadRowsTable * 100 < total * configuration.getPartitionCompactionTableDeadStopPercent()
                    && deadBytes <= configuration.getPartitionCompactionTableDeadTrigger() / 2);
        } else {
            // total == 0 (no composite partition seen yet) must never turn this on: 0 >= 0 would otherwise satisfy the
            // percentage check trivially, latching table pressure on from the very first commit of any table, well.
            tablePressureOn = (total > 0 && deadBytes >= configuration.getPartitionCompactionTableDeadThreshold()
                    && deadRowsTable * 100 >= total * configuration.getPartitionCompactionTableDeadThresholdPercent())
                    || deadBytes > configuration.getPartitionCompactionTableDeadTrigger();
        }

        if (chosen == -1 && tablePressureOn) {
            chosen = coldest;
            chosenReason = REASON_TABLE_PRESSURE;
        }
        if (chosen > -1) {
            selectedReason = chosenReason;
            selectedPartitionIndex = chosen;
        }
        return chosen;
    }

    /**
     * The index of the next eligible partition at or after {@code fromIndex} that holds more than one piece, or -1.
     */
    public int selectFoldablePartition(TxWriter txWriter, PartitionGeometry geometry, long nowMicros, int fromIndex) {
        if (txWriter.getLagRowCount() > 0) {
            return -1;
        }
        // Includes the last partition: JOIN only rewrites PartitionGeometry's piece array, never a byte
        // of the column files or the directory's nameTxn, so the writer's own active mapping stays valid.
        final int n = txWriter.getPartitionCount();
        for (int i = Math.max(0, fromIndex); i < n; i++) {
            if (geometry.getPieceCount(i) > 1 && !isSuppressed(txWriter.getPartitionTimestampByIndex(i), nowMicros)) {
                selectedPartitionIndex = i;
                return i;
            }
        }
        return -1;
    }

    /**
     * True when {@code partitionIndex} is a composite partition already reduced to a single piece sitting at row 0 -
     * MOVE-TAIL's own end state, JOIN folding everything into one, or any commit that merely happened to leave it that
     * way - with real dead space above it, and none of the ordinary reasons a partition is off-limits (the last/active.
     */
    public static boolean isMakePlainShape(TxWriter txWriter, PartitionGeometry geometry, int partitionIndex) {
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
        return geometry.getE(partitionIndex) > txWriter.getPartitionSize(partitionIndex);
    }

    /**
     * The index of the next MAKE-PLAIN candidate at or after {@code fromIndex} - see {@link #isMakePlainShape} - or -1.
     */
    public int selectMakePlainCandidate(TxWriter txWriter, PartitionGeometry geometry, long nowMicros, int fromIndex) {
        final int n = txWriter.getPartitionCount();
        for (int i = Math.max(0, fromIndex); i < n; i++) {
            if (isMakePlainShape(txWriter, geometry, i) && !isSuppressed(txWriter.getPartitionTimestampByIndex(i), nowMicros)) {
                return i;
            }
        }
        return -1;
    }

    private void clearBackoff(long partitionTimestamp) {
        for (int i = 0, n = backoff.size(); i < n; i += BACKOFF_LONGS) {
            if (backoff.getQuick(i) == partitionTimestamp) {
                backoff.removeIndexBlock(i, BACKOFF_LONGS);
                return;
            }
        }
    }

    private boolean isSuppressed(long partitionTimestamp, long nowMicros) {
        for (int i = 0, n = backoff.size(); i < n; i += BACKOFF_LONGS) {
            if (backoff.getQuick(i) == partitionTimestamp) {
                return nowMicros < backoff.getQuick(i + 1);
            }
        }
        return false;
    }
}
