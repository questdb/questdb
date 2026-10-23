/*******************************************************************************
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
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

/**
 * Plans what a commit does to ONE partition, as a list of actions over its pieces - the analogue of {@link
 * O3ParquetMergeStrategy} over a parquet file's row groups: pure computation over the piece bounds and the sorted O3
 * timestamps, no I/O, no writer state.
 */
public class O3CompositeMergeStrategy {
    /**
     * Stride of the piece bounds list: {@code tsLo}, {@code tsHi}, {@code rowOffset}, {@code rowCount},
     * {@code writerTxn}, {@code lastWriteMicros}.
     */
    public static final int LONGS_PER_BOUND = 6;
    /**
     * Stride of the cut list {@link #computeCuts} fills: {@code pieceIndex}, {@code cutTimestamp}, and the rows the cut
     * must leave below and above it once resolved against the real timestamp column.
     */
    public static final int LONGS_PER_CUT = 4;
    private static final int BOUND_LAST_WRITE_MICROS = 5;
    private static final int BOUND_ROW_COUNT = 3;
    private static final int BOUND_ROW_OFFSET = 2;
    private static final int BOUND_TS_HI = 1;
    private static final int BOUND_TS_LO = 0;
    private static final int BOUND_WRITER_TXN = 4;

    /**
     * @param rowOffset the FILE row this piece's first row sits at
     */
    public static void addPieceBounds(
            LongList bounds, long tsLo, long tsHi, long rowOffset, long rowCount, long writerTxn, long lastWriteMicros
    ) {
        bounds.add(tsLo, tsHi);
        bounds.add(rowOffset, rowCount);
        bounds.add(writerTxn, lastWriteMicros);
    }

    /**
     * Cuts the piece at {@code piece} in two, in place: the lower half keeps the first {@code below} rows and the upper
     * half takes the rest.
     *
     * @param below     rows of the piece below the cut
     * @param lowerTsHi timestamp of the lower half's LAST row
     * @param upperTsLo timestamp of the upper half's FIRST row
     * @return true when the cut was applied
     */
    public static boolean applyCut(LongList bounds, int piece, long below, long lowerTsHi, long upperTsLo) {
        final long tsHi = getTsHi(bounds, piece);
        final long rows = getRowCount(bounds, piece);
        if (tsHi == Numbers.LONG_NULL || below <= 0 || below >= rows) {
            return false;
        }
        final int at = piece * LONGS_PER_BOUND;
        // The lower half keeps the row offset it had; the upper half starts that many rows further in.
        final long rowOffset = getRowOffset(bounds, piece);
        bounds.setQuick(at + BOUND_TS_HI, lowerTsHi);
        bounds.setQuick(at + BOUND_ROW_COUNT, below);
        bounds.insert(at + LONGS_PER_BOUND, LONGS_PER_BOUND);
        bounds.setQuick(at + LONGS_PER_BOUND + BOUND_TS_LO, upperTsLo);
        bounds.setQuick(at + LONGS_PER_BOUND + BOUND_TS_HI, tsHi);
        bounds.setQuick(at + LONGS_PER_BOUND + BOUND_ROW_OFFSET, rowOffset + below);
        bounds.setQuick(at + LONGS_PER_BOUND + BOUND_ROW_COUNT, rows - below);
        // A cut moves no bytes, so both halves inherit the parent's pair.
        bounds.setQuick(at + LONGS_PER_BOUND + BOUND_WRITER_TXN, bounds.getQuick(at + BOUND_WRITER_TXN));
        bounds.setQuick(at + LONGS_PER_BOUND + BOUND_LAST_WRITE_MICROS, bounds.getQuick(at + BOUND_LAST_WRITE_MICROS));
        return true;
    }

    /**
     * Assigns every O3 row in {@code [srcOooLo, srcOooHi]} to a piece or to a gap, then emits the action list in
     * timestamp order.
     *
     * @param bounds               piece bounds, {@link #LONGS_PER_BOUND} longs each, ascending by tsLo
     * @param sortedTimestampsAddr native address of the sorted O3 timestamp index, 16 bytes per entry
     * @param srcOooLo             first O3 row, inclusive
     * @param srcOooHi             last O3 row, inclusive
     * @param smallPieceThreshold  a piece with fewer rows than this absorbs adjacent gap data instead of letting it
     *                             found a new piece
     * @param physicalRows         the partition's physical extent BEFORE this commit writes anything, used only to test whether
     *                             the last piece owns the shared files' tail; pass a value no piece can reach (e.g.
     *                             {@code -1}, as replace-commit mode does) to force no piece to own the tail
     * @param commitMayDedup       whether this commit's rows can collide with an existing row.
     * @param plan                 output, reused across calls: {@code plan.actions} is reset and repopulated so its {@code size()} IS
     *                             the action count, and {@code plan.appendActionIndex} is set to the {@link ActionType#APPEND} action's
     *                             position, or {@code -1} when this commit produces no APPEND action
     * @return {@code plan}, for a fluent call at the use site
     */
    public static Plan computeActions(
            LongList bounds,
            long sortedTimestampsAddr,
            long srcOooLo,
            long srcOooHi,
            long smallPieceThreshold,
            long physicalRows,
            boolean commitMayDedup,
            Plan plan
    ) {
        final int pieceCount = bounds.size() / LONGS_PER_BOUND;
        assert pieceCount > 0;
        plan.actions.setPos(0);
        plan.appendActionIndex = -1;
        plan.incomingMinTimestamp = srcOooHi >= srcOooLo
                ? TableWriter.getTimestampIndexValue(sortedTimestampsAddr, srcOooLo) : Long.MAX_VALUE;
        int actionCount = 0;
        long o3 = srcOooLo;

        for (int p = 0; p < pieceCount; p++) {
            final long tsLo = getTsLo(bounds, p);
            final long tsHi = getTsHi(bounds, p);

            // Gap BELOW this piece: rows under its tsLo that no earlier piece claimed.
            final long gapHi = findLastBelow(sortedTimestampsAddr, o3, srcOooHi, tsLo);
            if (gapHi >= o3) {
                final boolean absorb = getRowCount(bounds, p) < smallPieceThreshold;
                if (absorb) {
                    // fold into this piece's merge below
                } else {
                    actionAt(plan.actions, actionCount++).setNewPiece(o3, gapHi);
                    o3 = gapHi + 1;
                }
            }

            // Rows inside this piece's data range.
            final boolean isLastPiece = p == pieceCount - 1;
            final boolean ownsTail = getRowOffset(bounds, p) + getRowCount(bounds, p) == physicalRows;
            final boolean spareTie = !commitMayDedup && tsHi != Numbers.LONG_NULL
                    && ((isLastPiece && ownsTail) || tsLo < tsHi);
            final long claimHi = tsHi == Numbers.LONG_NULL
                    ? (p + 1 < pieceCount ? findLastBelow(sortedTimestampsAddr, o3, srcOooHi, getTsLo(bounds, p + 1)) : srcOooHi)
                    : lastAtOrBelow(sortedTimestampsAddr, o3, srcOooHi, spareTie ? tsHi - 1 : tsHi);
            if (claimHi >= o3) {
                actionAt(plan.actions, actionCount++).setMerge(p, o3, claimHi);
                o3 = claimHi + 1;
            } else if (isLastPiece && ownsTail && o3 <= srcOooHi) {
                // This piece claims none of the batch itself, and every remaining O3 row sits at or above
                // its tsHi with nothing needing deduplication against it - so, owning the tail, it is
                // extended in place instead of founding a new piece next to it.
                plan.appendActionIndex = actionCount;
                actionAt(plan.actions, actionCount++).setAppend(p, o3, srcOooHi);
                o3 = srcOooHi + 1;
            } else {
                actionAt(plan.actions, actionCount++).setKeep(p);
            }
        }

        // Everything above the last piece's data becomes a new piece at the shared tail.
        if (o3 <= srcOooHi) {
            actionAt(plan.actions, actionCount++).setNewPiece(o3, srcOooHi);
        }
        return plan;
    }

    /**
     * PRE-SPLIT. Chooses where to cut existing pieces so the batch lands on as little data as possible.
     * <p>
     * The batch inside a piece is first broken into CLUSTERS: two incoming rows belong to different clusters when the
     * existing rows between them outweigh the piece a cut costs. Each cluster is then carved out by a cut at its first
     * row and one just above its last. A piece's bounds describe the rows it holds, so the cluster belongs to neither
     * side and lands in the gap between them as a piece of its own - the existing rows around it are never read. A
     * batch that spans the whole piece, which has no outer edge to spare, is cut around all the same.
     *
     * @param minPieceRows the existing rows a cut has to spare to be worth the piece it makes
     * @param maxCuts      hard cap on the cuts one commit makes, a safety valve above the size rule
     * @param cutsOut      output, cleared first: {@link #LONGS_PER_CUT} longs per cut, ascending
     * @return the number of cuts proposed
     */
    public static int computeCuts(
            LongList bounds,
            long sortedTimestampsAddr,
            long srcOooLo,
            long srcOooHi,
            long minPieceRows,
            int maxCuts,
            LongList cutsOut
    ) {
        cutsOut.clear();
        // A cut costs a piece on each side of the cluster, so the gap between two clusters has to pay for both.
        final long minGapRows = 2 * minPieceRows;
        final int pieceCount = bounds.size() / LONGS_PER_BOUND;
        int cuts = 0;
        for (int p = 0; p < pieceCount && cuts < maxCuts; p++) {
            final long tsLo = getTsLo(bounds, p);
            final long tsHi = getTsHi(bounds, p);
            final long rows = getRowCount(bounds, p);
            if (tsHi == Numbers.LONG_NULL || tsHi <= tsLo || rows < 2 * minPieceRows) {
                continue;
            }
            final long firstInside = firstAtOrAbove(sortedTimestampsAddr, srcOooLo, srcOooHi, tsLo);
            if (firstInside > srcOooHi) {
                continue;
            }
            final long batchLo = TableWriter.getTimestampIndexValue(sortedTimestampsAddr, firstInside);
            if (batchLo > tsHi) {
                continue; // the batch does not reach this piece
            }
            final long lastInside = lastAtOrBelow(sortedTimestampsAddr, firstInside, srcOooHi, tsHi);

            long clusterLo = batchLo;
            long clusterHi = batchLo;
            // Rows of the piece below the last cut taken, so a cluster is measured from the cut before it rather
            // than from the piece's start.
            long lastCutBelow = 0;
            for (long o3 = firstInside + 1; o3 <= lastInside + 1 && cuts < maxCuts; o3++) {
                final boolean isLastCluster = o3 > lastInside;
                final long ts = isLastCluster ? tsHi : TableWriter.getTimestampIndexValue(sortedTimestampsAddr, o3);
                final long above = rowsBelow(tsLo, tsHi, rows, clusterHi + 1);
                if (!isLastCluster && rowsBelow(tsLo, tsHi, rows, ts) - above < minGapRows) {
                    clusterHi = ts;
                    continue;
                }
                // Spare the rows below the cluster.
                final long below = rowsBelow(tsLo, tsHi, rows, clusterLo);
                if (clusterLo > tsLo && below - lastCutBelow >= minPieceRows) {
                    addCut(cutsOut, p, clusterLo, minPieceRows, 0);
                    cuts++;
                    lastCutBelow = below;
                }
                // Spare the rows above it. Only the LAST cluster needs its own test here: every other one has the
                // gap that ended it, which already cleared twice this bar.
                if (clusterHi < tsHi && (!isLastCluster || rows - above >= minPieceRows) && cuts < maxCuts) {
                    addCut(cutsOut, p, clusterHi + 1, 0, minPieceRows);
                    cuts++;
                    lastCutBelow = above;
                }
                clusterLo = ts;
                clusterHi = ts;
            }
        }
        return cuts;
    }

    /**
     * The piece whose DATA range contains {@code ts}, or {@code -1}.
     */
    public static int findPieceContaining(LongList bounds, long ts) {
        for (int p = 0, n = bounds.size() / LONGS_PER_BOUND; p < n; p++) {
            final long tsHi = getTsHi(bounds, p);
            if (tsHi != Numbers.LONG_NULL && getTsLo(bounds, p) <= ts && ts <= tsHi) {
                return p;
            }
        }
        return -1;
    }

    /**
     * First O3 index in {@code [lo, hi]} whose timestamp is {@code >= value}, or {@code hi + 1}.
     */
    public static long firstAtOrAbove(long sortedTimestampsAddr, long lo, long hi, long value) {
        return lastAtOrBelow(sortedTimestampsAddr, lo, hi, value - 1) + 1;
    }

    /**
     * Forecasts the nonempty, file-adjacent-coalesced shape of the normal plan.
     */
    public static void forecast(LongList bounds, Plan plan, long extent) {
        long liveRows = 0;
        long previousEnd = -1;
        int pieces = 0;
        if (plan.appendActionIndex >= 0) {
            extent += plan.actions.getQuick(plan.appendActionIndex).getO3RowCount();
        }
        for (int i = 0; i < plan.actions.size(); i++) {
            final Action action = plan.actions.getQuick(i);
            final ActionType type = action.isProjectedNoop ? ActionType.KEEP : action.type;
            long rows;
            long offset;
            switch (type) {
                case KEEP -> {
                    rows = getRowCount(bounds, action.pieceIndex);
                    offset = getRowOffset(bounds, action.pieceIndex);
                }
                case APPEND -> {
                    rows = getRowCount(bounds, action.pieceIndex) + action.getO3RowCount();
                    offset = getRowOffset(bounds, action.pieceIndex);
                }
                case MERGE, NEW_PIECE -> {
                    if (action.projectedRows >= 0) {
                        rows = action.projectedRows;
                    } else {
                        rows = action.getO3RowCount();
                        if (type == ActionType.MERGE) {
                            rows += getRowCount(bounds, action.pieceIndex);
                        }
                    }
                    offset = extent;
                    extent += rows;
                }
                default -> {
                    continue;
                }
            }
            if (rows > 0) {
                if (offset != previousEnd) {
                    pieces++;
                }
                previousEnd = offset + rows;
                liveRows += rows;
            }
        }
        plan.projectedLiveRows = liveRows;
        plan.projectedDeadRows = extent - liveRows;
        plan.projectedPieceCount = pieces;
    }

    public static long getLastWriteMicros(LongList bounds, int piece) {
        return bounds.getQuick(piece * LONGS_PER_BOUND + BOUND_LAST_WRITE_MICROS);
    }

    public static long getRowCount(LongList bounds, int piece) {
        return bounds.getQuick(piece * LONGS_PER_BOUND + BOUND_ROW_COUNT);
    }

    public static long getRowOffset(LongList bounds, int piece) {
        return bounds.getQuick(piece * LONGS_PER_BOUND + BOUND_ROW_OFFSET);
    }

    public static long getTsHi(LongList bounds, int piece) {
        return bounds.getQuick(piece * LONGS_PER_BOUND + BOUND_TS_HI);
    }

    public static long getTsLo(LongList bounds, int piece) {
        return bounds.getQuick(piece * LONGS_PER_BOUND + BOUND_TS_LO);
    }

    public static long getWriterTxn(LongList bounds, int piece) {
        return bounds.getQuick(piece * LONGS_PER_BOUND + BOUND_WRITER_TXN);
    }

    /**
     * Whether the prefix left behind is more than {@code prefixMultiple} times the tail plus the incoming rows,
     * i.e. {@code prefixRows > prefixMultiple * (tailRows + incomingRows)}, without multiplying either count.
     */
    public static boolean isMoveTailEconomical(long prefixRows, long tailRows, long incomingRows, int prefixMultiple) {
        return prefixRows > 0 && tailRows > 0
                && tailRows <= (prefixRows - 1) / prefixMultiple
                && incomingRows <= (prefixRows - 1) / prefixMultiple - tailRows;
    }

    /**
     * MOVE-TAIL fires when dead rows exceed {@code deadRowThreshold} and either strictly exceed
     * {@code deadRowsPercent}% of the live rows or the folder holds more than {@code pieceThreshold} pieces.
     */
    public static boolean isMoveTailTriggered(
            long liveRows,
            long deadRows,
            int pieceCount,
            long deadRowThreshold,
            int deadRowsPercent,
            int pieceThreshold
    ) {
        return liveRows > 0 && deadRows > deadRowThreshold
                && (deadRows > percentOf(liveRows, deadRowsPercent) || pieceCount > pieceThreshold);
    }

    /**
     * Last O3 index in {@code [lo, hi]} whose timestamp is {@code <= value}, or {@code lo - 1}.
     */
    public static long lastAtOrBelow(long sortedTimestampsAddr, long lo, long hi, long value) {
        long result = lo - 1;
        long l = lo;
        long h = hi;
        while (l <= h) {
            final long mid = (l + h) >>> 1;
            if (TableWriter.getTimestampIndexValue(sortedTimestampsAddr, mid) <= value) {
                result = mid;
                l = mid + 1;
            } else {
                h = mid - 1;
            }
        }
        return result;
    }

    /**
     * The highest legal boundary before the incoming range, or zero if moving is not economical.
     */
    public static int moveTailCut(
            LongList bounds,
            Plan plan,
            long futureFloor,
            long deadRowThreshold,
            int deadRowsPercent,
            int pieceThreshold,
            int prefixMultiple
    ) {
        if (!isMoveTailTriggered(plan.projectedLiveRows, plan.projectedDeadRows, plan.projectedPieceCount,
                deadRowThreshold, deadRowsPercent, pieceThreshold)) {
            return 0;
        }
        final int pieceCount = bounds.size() / LONGS_PER_BOUND;
        long existingRows = 0;
        for (int p = 0; p < pieceCount; p++) {
            existingRows += getRowCount(bounds, p);
        }
        long incomingRows = 0;
        for (int a = 0; a < plan.actions.size(); a++) {
            incomingRows += plan.actions.getQuick(a).getO3RowCount();
        }
        final long floor = Math.min(plan.incomingMinTimestamp, futureFloor);
        long prefixRows = 0;
        int cut = 0;
        for (int a = 0; a < plan.actions.size(); a++) {
            final Action action = plan.actions.getQuick(a);
            final int p = action.pieceIndex;
            if (action.type != ActionType.KEEP || p != a || p + 1 >= pieceCount
                    || getTsHi(bounds, p) == Numbers.LONG_NULL || getTsHi(bounds, p) >= floor) {
                break;
            }
            prefixRows += getRowCount(bounds, p);
            if (getTsHi(bounds, p) < getTsLo(bounds, p + 1)
                    && isMoveTailEconomical(prefixRows, existingRows - prefixRows, incomingRows, prefixMultiple)) {
                cut = p + 1;
            }
        }
        return cut;
    }

    /**
     * {@code floor(rows * percent / 100)} for non-negative inputs, saturating at {@link Long#MAX_VALUE}
     * instead of overflowing.
     */
    public static long percentOf(long rows, int percent) {
        if (percent == 0) {
            return 0;
        }
        // floor(rows * percent / 100) == (rows / 100) * percent + floor((rows % 100) * percent / 100)
        final long hundreds = rows / 100;
        if (hundreds > Long.MAX_VALUE / percent) {
            return Long.MAX_VALUE;
        }
        final long whole = hundreds * percent;
        final long fraction = (rows % 100) * percent / 100;
        return whole > Long.MAX_VALUE - fraction ? Long.MAX_VALUE : whole + fraction;
    }

    private static void addCut(LongList cutsOut, int piece, long cutTs, long minRowsBelow, long minRowsAbove) {
        cutsOut.add((long) piece, cutTs, minRowsBelow, minRowsAbove);
    }

    /**
     * Last O3 index in {@code [lo, hi]} whose timestamp is strictly {@code < value}, or {@code lo - 1}.
     */
    private static long findLastBelow(long sortedTimestampsAddr, long lo, long hi, long value) {
        return lastAtOrBelow(sortedTimestampsAddr, lo, hi, value - 1);
    }

    /**
     * Rows of a piece below {@code ts}, apportioned linearly across its timestamp range.
     */
    private static long rowsBelow(long tsLo, long tsHi, long rows, long ts) {
        if (ts <= tsLo) {
            return 0;
        }
        if (ts > tsHi) {
            return rows;
        }
        return (long) ((double) rows * (ts - tsLo) / (tsHi - tsLo + 1));
    }

    private static Action actionAt(ObjList<Action> actions, int index) {
        if (index >= actions.size()) {
            // extendPos grows pos without touching the buffer, so an Action a longer PRIOR call left at
            // this slot survives a shorter call's plan.actions.setPos(0) and is reused here rather than
            // reallocated - only a slot no call has ever reached is genuinely null.
            actions.extendPos(index + 1);
            if (actions.getQuick(index) == null) {
                actions.setQuick(index, new Action());
            }
        }
        final Action action = actions.getQuick(index);
        // The plan that last used this slot freed its merge index - see Plan#freeMergeIndexes.
        assert action.mergeIndexAddr == 0;
        action.isProjectedNoop = false;
        action.projectedRows = -1;
        return action;
    }

    public enum ActionType {
        KEEP,
        MERGE,
        NEW_PIECE,
        APPEND,
        /**
         * The piece falls entirely inside a replace-range commit's declared range and carries no O3 rows of its own: it
         * is excluded from the new geometry rather than kept.
         */
        DROP
    }

    public static class Action {
        public boolean isProjectedNoop;
        /**
         * The dedup merge index the forecast built for this MERGE, or 0. Execution merges with it rather than
         * building it again; owned by the action until {@link #freeMergeIndex()}.
         */
        public long mergeIndexAddr;
        public long mergeIndexSize;
        public long o3Hi = -1;
        public long o3Lo = -1;
        public int pieceIndex = -1;
        public long projectedRows = -1;
        public ActionType type;

        public long getO3RowCount() {
            return o3Hi >= 0 ? o3Hi - o3Lo + 1 : 0;
        }

        public void freeMergeIndex() {
            if (mergeIndexAddr != 0) {
                mergeIndexAddr = Unsafe.free(mergeIndexAddr, mergeIndexSize, MemoryTag.NATIVE_O3);
                mergeIndexSize = 0;
            }
        }

        public void setAppend(int pieceIndex, long o3Lo, long o3Hi) {
            this.type = ActionType.APPEND;
            this.pieceIndex = pieceIndex;
            this.o3Lo = o3Lo;
            this.o3Hi = o3Hi;
        }

        public void setDrop(int pieceIndex) {
            this.type = ActionType.DROP;
            this.pieceIndex = pieceIndex;
            this.o3Lo = -1;
            this.o3Hi = -1;
        }

        public void setKeep(int pieceIndex) {
            this.type = ActionType.KEEP;
            this.pieceIndex = pieceIndex;
            this.o3Lo = -1;
            this.o3Hi = -1;
        }

        public void setMerge(int pieceIndex, long o3Lo, long o3Hi) {
            this.type = ActionType.MERGE;
            this.pieceIndex = pieceIndex;
            this.o3Lo = o3Lo;
            this.o3Hi = o3Hi;
        }

        public void setNewPiece(long o3Lo, long o3Hi) {
            this.type = ActionType.NEW_PIECE;
            this.pieceIndex = -1;
            this.o3Lo = o3Lo;
            this.o3Hi = o3Hi;
        }

        @Override
        public String toString() {
            return switch (type) {
                case APPEND -> "APPEND(p=" + pieceIndex + ", o3=[" + o3Lo + "," + o3Hi + "])";
                case KEEP -> "KEEP(p=" + pieceIndex + ")";
                case MERGE -> "MERGE(p=" + pieceIndex + ", o3=[" + o3Lo + "," + o3Hi + "])";
                case NEW_PIECE -> "NEW_PIECE(o3=[" + o3Lo + "," + o3Hi + "])";
                case DROP -> "DROP(p=" + pieceIndex + ")";
            };
        }
    }

    /**
     * The output of {@link #computeActions}: an action list plus the position of its {@link ActionType#APPEND} action,
     * if any.
     */
    public static class Plan {
        public final ObjList<Action> actions = new ObjList<>();
        /**
         * Position in {@link #actions} of the {@link ActionType#APPEND} action, or -1 when none was emitted.
         */
        public int appendActionIndex = -1;
        public long incomingMinTimestamp;
        public long projectedDeadRows;
        public long projectedLiveRows;
        public int projectedPieceCount;

        /**
         * Frees every merge index the forecast left on the actions. The plan's owner calls it once execution no
         * longer needs them, on every path, before the plan is computed again.
         */
        public void freeMergeIndexes() {
            for (int i = 0, n = actions.size(); i < n; i++) {
                actions.getQuick(i).freeMergeIndex();
            }
        }
    }
}
