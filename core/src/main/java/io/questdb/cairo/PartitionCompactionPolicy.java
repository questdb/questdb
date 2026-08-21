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
 * Decides WHICH partition a commit should compact, and why. The work itself belongs to
 * {@link TableWriter}; this class only reads {@link TxWriter}/{@link PartitionGeometry} - both already
 * resident, the latter lazily so - and applies the four rules of PARTITION_COMPACTION.md Sec.4.
 * <p>
 * Ported from the enterprise `feat-partition-top-split` branch's design onto this branch's simpler
 * model, where a composite partition is exactly one {@code attachedPartitions} entry: one directory, one
 * {@code (partitionTimestamp, nameTxn)}, one {@code _geometry} chain. There is no separate "folder" unit
 * distinct from "partition" - the reference repo needed one because a hardlink split let several
 * directories share one logical partition; this branch has no hardlink splits, so every accessor below
 * takes a plain partition index. See PARTITION_COMPACTION_state.md for the full list of corrections this
 * port required.
 * <p>
 * It is owned by one writer, allocates nothing per commit, and keeps no durable state: handing the
 * writer to another thread costs at most one wasted attempt, and making the cooling-off timers durable
 * would mean a per-partition {@code _txn} field for an advisory number plus recovery code that could
 * itself be wrong.
 * <p>
 * Every rule is worked out purely from COMMITTED state, so checking them in full on every commit can
 * never produce a different answer from checking them less often.
 */
public class PartitionCompactionPolicy implements Mutable {
    public static final int REASON_AGE = 3;
    public static final int REASON_NONE = 0;
    public static final int REASON_PIECE_COUNT = 2;
    public static final int REASON_TABLE_PRESSURE = 4;
    public static final int REASON_WASTE_RATIO = 1;
    private static final int BACKOFF_LONGS = 3;
    // (partitionTimestamp, untilMicros)
    private static final int COOLDOWN_LONGS = 2;
    // Bounded so a table with a great many partitions cannot grow these lists without end. Dropping the
    // oldest entry only ever costs one extra attempt.
    private static final int MAX_TRACKED = 256;
    // (partitionTimestamp, nextAttemptMicros, currentBackoffMicros)
    private final LongList backoff = new LongList();
    private final CairoConfiguration configuration;
    private final LongList cooldown = new LongList();
    private int selectedPartitionIndex = -1;
    private int selectedReason = REASON_NONE;
    private boolean tablePressureOn;

    public PartitionCompactionPolicy(CairoConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void clear() {
        backoff.clear();
        cooldown.clear();
        tablePressureOn = false;
        selectedReason = REASON_NONE;
        selectedPartitionIndex = -1;
    }

    /**
     * True if a piece count of {@code pieceCount}, or a dead-versus-live row split of {@code deadRows}
     * against {@code liveRows}, already crosses the same waste-ratio or piece-count thresholds
     * {@link #selectPartition} enforces after the fact. Static and stateless, with none of
     * {@link #selectPartition}'s cooldown or backoff bookkeeping - those exist to stop {@code housekeep}
     * from retrying a partition it just declined or just compacted, which does not apply to a decision
     * made before the write that would create the waste has even landed.
     */
    public static boolean exceedsThresholds(
            CairoConfiguration configuration, long liveRows, long deadRows, int pieceCount, long avgRecordSize
    ) {
        if (pieceCount > configuration.getPartitionCompactionMaxPieces()) {
            return true;
        }
        final long deadMinRows = avgRecordSize > 0
                ? configuration.getPartitionCompactionDeadMinSize() / avgRecordSize
                : configuration.getPartitionCompactionDeadMinSize();
        return deadRows > (long) configuration.getPartitionCompactionDeadRowsRatio() * liveRows && deadRows > deadMinRows;
    }

    public int getSelectedPartitionIndex() {
        return selectedPartitionIndex;
    }

    /**
     * Why the last {@link #selectPartition} picked what it picked. The piece-count rule is the one case
     * allowed to run over the per-commit row budget: the alternative is a geometry file that grows
     * without end.
     */
    public int getSelectedReason() {
        return selectedReason;
    }

    /**
     * Records that the partition was compacted, so the rules leave it alone for a while.
     */
    public void onCompacted(long partitionTimestamp, long nowMicros) {
        clearBackoff(partitionTimestamp);
        putCooldown(partitionTimestamp, nowMicros + configuration.getPartitionCompactionCooldown());
    }

    /**
     * Records that the partition could not be compacted this time, and doubles how long to wait before
     * trying again. Being declined is normal - a partition whose pieces are interleaved with an
     * outsider's has to wait for that to change.
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
     * The partition index to compact next, or -1. Reads every partition, including the last: one pass,
     * first three rules per partition, totals for the fourth, and the piece array touched only for the
     * partition that gets picked. A table with no composite partition costs one resident read per
     * partition and no {@code _geometry} I/O at all. The caller (TableWriter.runCompaction) is
     * responsible for closing and reopening the active partition around a REWRITE/MOVE-TAIL of it.
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
        final int maxPieces = configuration.getPartitionCompactionMaxPieces();
        final long idleTimeout = configuration.getPartitionCompactionIdleTimeout();
        final int ratio = configuration.getPartitionCompactionDeadRowsRatio();

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
            // The gate is "any composite partition" - pieces>1 or dead space above the live rows - not
            // "more than one piece starting above row 0" as PARTITION_COMPACTION.md Sec.4 first states.
            // That exclusion is right for MOVE-TAIL, which compacts in place and would copy rows to
            // where they already are; REWRITE (this port's only copy step) re-roots the pieces into a
            // fresh directory and deletes the old one, so it reclaims exactly this shape, and JOIN
            // manufactures it - excluding it would let JOIN destroy what REWRITE can reclaim.
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
            if (dead > (long) ratio * live && dead > deadMinRows) {
                chosen = i;
                chosenReason = REASON_WASTE_RATIO;
            } else if (pieces > maxPieces) {
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
                    && deadBytes <= configuration.getPartitionCompactionTableDeadMaxSize() / 2);
        } else {
            tablePressureOn = deadRowsTable * 100 >= total * configuration.getPartitionCompactionTableDeadPercent()
                    || deadBytes > configuration.getPartitionCompactionTableDeadMaxSize();
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
     * The index of the next eligible partition at or after {@code fromIndex} that holds more than one
     * piece, or -1. A fold moves no bytes, so it is worth doing on any partition that has something to
     * fold whether or not it crossed one of the four thresholds - waiting for a threshold would leave a
     * partition cut into many pieces fragmented for as long as it stays cold.
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

    private void clearBackoff(long partitionTimestamp) {
        for (int i = 0, n = backoff.size(); i < n; i += BACKOFF_LONGS) {
            if (backoff.getQuick(i) == partitionTimestamp) {
                backoff.removeIndexBlock(i, BACKOFF_LONGS);
                return;
            }
        }
    }

    private boolean isSuppressed(long partitionTimestamp, long nowMicros) {
        for (int i = 0, n = cooldown.size(); i < n; i += COOLDOWN_LONGS) {
            if (cooldown.getQuick(i) == partitionTimestamp) {
                if (nowMicros < cooldown.getQuick(i + 1)) {
                    return true;
                }
                cooldown.removeIndexBlock(i, COOLDOWN_LONGS);
                break;
            }
        }
        for (int i = 0, n = backoff.size(); i < n; i += BACKOFF_LONGS) {
            if (backoff.getQuick(i) == partitionTimestamp) {
                return nowMicros < backoff.getQuick(i + 1);
            }
        }
        return false;
    }

    private void putCooldown(long partitionTimestamp, long untilMicros) {
        for (int i = 0, n = cooldown.size(); i < n; i += COOLDOWN_LONGS) {
            if (cooldown.getQuick(i) == partitionTimestamp) {
                cooldown.setQuick(i + 1, untilMicros);
                return;
            }
        }
        if (cooldown.size() >= MAX_TRACKED * COOLDOWN_LONGS) {
            cooldown.removeIndexBlock(0, COOLDOWN_LONGS);
        }
        cooldown.add(partitionTimestamp, untilMicros);
    }
}
