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
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * Plans what a commit does to ONE partition, as a list of actions over its pieces - the analogue of
 * {@link O3ParquetMergeStrategy} over a parquet file's row groups: pure computation over the piece bounds
 * and the sorted O3 timestamps, no I/O, no writer state.
 * <p>
 * A native partition arrives as ONE piece covering all of its rows, where a parquet file arrives already
 * divided, so the structure has to be manufactured first. That is the PRE-SPLIT, and it is free.
 * <p>
 * Actions are emitted in timestamp order, each occupying one position in the new piece array:
 * <ul>
 *     <li>{@link ActionType#KEEP} - copies NOTHING: the piece's bytes stay and only its
 *     {@code (rowOffset, rowCount)} is carried into the new geometry record;</li>
 *     <li>{@link ActionType#MERGE} - the batch slice overlaps the piece, so the two go out as one image
 *     at the shared files' tail and the piece's offset and count move;</li>
 *     <li>{@link ActionType#NEW_PIECE} - the batch slice falls in a gap and is written at the tail as a
 *     new piece under its own {@code tsLo};</li>
 *     <li>{@link ActionType#APPEND} - the batch extends the last piece in place, when that piece already
 *     owns the tail of the files and claims no O3 row of its own. At most one per plan.</li>
 * </ul>
 */
public class O3CompositeMergeStrategy {
    /**
     * Stride of the piece bounds list: {@code tsLo}, {@code tsHi}, {@code rowOffset}, {@code rowCount}.
     */
    public static final int LONGS_PER_BOUND = 4;
    private static final int BOUND_ROW_COUNT = 3;
    private static final int BOUND_ROW_OFFSET = 2;
    private static final int BOUND_TS_HI = 1;
    private static final int BOUND_TS_LO = 0;

    /**
     * @param rowOffset the FILE row this piece's first row sits at
     */
    public static void addPieceBounds(LongList bounds, long tsLo, long tsHi, long rowOffset, long rowCount) {
        bounds.add(tsLo, tsHi);
        bounds.add(rowOffset, rowCount);
    }

    /**
     * Cuts the piece at {@code piece} in two, in place: the lower half keeps the first {@code below} rows
     * and the upper half takes the rest. Declines, leaving the list untouched, when either half would be
     * empty or the piece's data is unbounded.
     * <p>
     * All three numbers are RESOLVED AGAINST THE DATA by the caller, which searches the piece's own
     * timestamp column; this class reads no files, and {@link #rowsBelow} only estimates.
     * <p>
     * A piece's bounds describe THE ROWS IT HOLDS, not the range it routes, so a cut across a data gap
     * leaves the gap owned by neither half - which lets a later batch landing there become its own piece.
     * Cutting moves no bytes: both halves address the same files at the same offsets.
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
        return true;
    }

    /**
     * Assigns every O3 row in {@code [srcOooLo, srcOooHi]} to a piece or to a gap, then emits the action
     * list in timestamp order.
     * <p>
     * A row belongs to a piece when it falls inside that piece's DATA range {@code [tsLo, tsHi]}. A piece
     * whose {@code tsHi} was never recorded ({@link Numbers#LONG_NULL}) claims its whole routing range.
     * Rows in a gap become a new piece, except beside a small piece, where folding them in is cheaper.
     *
     * @param bounds               piece bounds, {@link #LONGS_PER_BOUND} longs each, ascending by tsLo
     * @param sortedTimestampsAddr native address of the sorted O3 timestamp index, 16 bytes per entry
     * @param srcOooLo             first O3 row, inclusive
     * @param srcOooHi             last O3 row, inclusive
     * @param smallPieceThreshold  a piece with fewer rows than this absorbs adjacent gap data instead of
     *                             letting it found a new piece
     * @param physicalRows         the partition's physical extent BEFORE this commit writes anything, used
     *                             only to test whether the last piece owns the shared files' tail; pass a
     *                             value no piece can reach (e.g. {@link Long#MAX_VALUE}) to disable
     *                             {@link ActionType#APPEND} entirely
     * @param commitMayDedup       whether this commit's rows can collide with an existing row. When
     *                             {@code false}, a batch row that only TIES a piece's {@code tsHi} needs no
     *                             key comparison and is spared from that piece's claim, rather than forcing
     *                             a {@link ActionType#MERGE} that reorders nothing - except on a
     *                             single-point piece ({@code tsLo == tsHi}), where sparing would found a
     *                             second piece at the same instant. The last piece is exempt from that
     *                             exception while it still owns the shared files' tail, which is what
     *                             {@link ActionType#APPEND} needs to grow it in place
     * @param plan                 output, reused across calls: {@code plan.actions} is reset and
     *                             repopulated so its {@code size()} IS the action count, and
     *                             {@code plan.appendActionIndex} is set to the {@link ActionType#APPEND}
     *                             action's position, or -1 when none was emitted
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

            // Rows inside this piece's data range. Under commitMayDedup=false a tie at tsHi is spared from
            // the claim, unless the piece is a single point in time - sparing there would found a second
            // piece at the same instant. The last piece is exempt from that exception only while it still
            // OWNS the tail, which is what lets APPEND absorb the tie by growing it in place; the exemption
            // therefore tests the same tail ownership APPEND does.
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

        // Everything above the last piece's data becomes a new piece at the shared tail. A plain append
        // leaves the two tiling the files, and the caller then drops the geometry - see O3PartitionJob.
        if (o3 <= srcOooHi) {
            actionAt(plan.actions, actionCount++).setNewPiece(o3, srcOooHi);
        }
        return plan;
    }

    /**
     * PRE-SPLIT. Chooses where to cut existing pieces so the batch lands on as little data as possible,
     * and returns the cuts as {@code (pieceIndex, cutTimestamp)} pairs in {@code cutsOut}.
     * <p>
     * The step with no parquet counterpart: a native directory can be ONE piece covering the whole logical
     * partition, so merging a batch into it rewrites the whole day. Cutting is free, so the structure is
     * manufactured lazily, aimed at exactly where this batch lands.
     * <p>
     * For each piece the batch overlaps, up to two cuts are proposed - at the batch's first timestamp
     * inside the piece and one past its last - and only when at least {@code minPieceRows} rows would be
     * left on the side being spared.
     * <p>
     * Row counts are apportioned by TIMESTAMP position, an estimate that only decides whether a cut is
     * worth making; the caller resolves each cut to an actual row and drops one that falls at an edge.
     *
     * @param cutsOut output, cleared first: pairs of (pieceIndex, cutTimestamp)
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
            final long batchHi = TableWriter.getTimestampIndexValue(sortedTimestampsAddr, lastInside);

            // Spare the rows below the batch.
            if (batchLo > tsLo && rowsBelow(tsLo, tsHi, rows, batchLo) >= minPieceRows && cuts < maxCuts) {
                cutsOut.add(p);
                cutsOut.add(batchLo);
                cuts++;
            }
            // Spare the rows above the batch. The cut goes one tick past the batch's last timestamp, so
            // the batch stays with the piece below it rather than straddling the boundary.
            if (batchHi < tsHi && rows - rowsBelow(tsLo, tsHi, rows, batchHi + 1) >= minPieceRows && cuts < maxCuts) {
                cutsOut.add(p);
                cutsOut.add(batchHi + 1);
                cuts++;
            }
        }
        return cuts;
    }

    /**
     * The piece whose DATA range contains {@code ts}, or {@code -1}. A piece whose {@code tsHi} was never
     * recorded cannot answer, and is skipped rather than guessed at.
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

    public static long getRowCount(LongList bounds, int piece) {
        return bounds.getQuick(piece * LONGS_PER_BOUND + BOUND_ROW_COUNT);
    }

    /**
     * The FILE row this piece's first row sits at.
     */
    public static long getRowOffset(LongList bounds, int piece) {
        return bounds.getQuick(piece * LONGS_PER_BOUND + BOUND_ROW_OFFSET);
    }

    public static long getTsHi(LongList bounds, int piece) {
        return bounds.getQuick(piece * LONGS_PER_BOUND + BOUND_TS_HI);
    }

    public static long getTsLo(LongList bounds, int piece) {
        return bounds.getQuick(piece * LONGS_PER_BOUND + BOUND_TS_LO);
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
     * Last O3 index in {@code [lo, hi]} whose timestamp is strictly {@code < value}, or {@code lo - 1}.
     */
    private static long findLastBelow(long sortedTimestampsAddr, long lo, long hi, long value) {
        return lastAtOrBelow(sortedTimestampsAddr, lo, hi, value - 1);
    }

    /**
     * Rows of a piece below {@code ts}, apportioned linearly across its timestamp range. An estimate: it
     * only decides whether a cut is worth proposing, never where the cut actually lands.
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
        return actions.getQuick(index);
    }

    public enum ActionType {
        /**
         * The piece is untouched: its bytes stay, only its extent is carried forward. Copies NOTHING.
         */
        KEEP,
        /**
         * The batch slice overlaps the piece; the two go out as one image at the shared files' tail.
         */
        MERGE,
        /**
         * The batch slice falls in a gap and becomes a new piece at the shared files' tail.
         */
        NEW_PIECE,
        /**
         * The batch slice falls above the last piece, which already owns the shared files' tail and
         * claims none of it. The batch is written right after it, exactly as {@link #NEW_PIECE} would, but
         * the piece's own {@code tsHi} and {@code rowCount} grow to cover it instead of a new piece being
         * founded - {@link O3PartitionJob}'s executor must write this action BEFORE any other, or a
         * {@link #NEW_PIECE} emitted earlier in timestamp order takes the tail first.
         */
        APPEND,
        /**
         * The piece falls entirely inside a replace-range commit's declared range and carries no O3 rows
         * of its own: it is excluded from the new geometry rather than kept. Its bytes stay on disk as
         * dead space - a drop moves nothing, exactly as a KEEP writes nothing - but the piece is gone from
         * the partition's live view. {@link #computeActions} never emits this; a caller in replace mode
         * downgrades a would-be KEEP to DROP once it knows the piece's bounds sit inside the range.
         */
        DROP
    }

    public static class Action {
        public long o3Hi = -1;
        public long o3Lo = -1;
        public int pieceIndex = -1;
        public ActionType type;

        public long getO3RowCount() {
            return o3Hi >= 0 ? o3Hi - o3Lo + 1 : 0;
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
     * The output of {@link #computeActions}: an action list plus the position of its
     * {@link ActionType#APPEND} action, if any. Kept as one reused holder, pooled per {@code O3PartitionJob}
     * worker thread, so planning a commit allocates nothing.
     */
    public static class Plan {
        public final ObjList<Action> actions = new ObjList<>();
        /**
         * Position in {@link #actions} of the {@link ActionType#APPEND} action, or -1 when none was
         * emitted. The executor must write this action before any other in the plan.
         */
        public int appendActionIndex = -1;
    }
}
