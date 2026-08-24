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
 * Plans what a commit does to ONE partition, as a list of actions over its pieces. The direct
 * analogue of {@link O3ParquetMergeStrategy} over a parquet file's row groups, and deliberately the same
 * shape: pure computation over the piece bounds and the sorted O3 timestamps, no I/O, no writer state.
 * <p>
 * A piece is to a partition what a row group is to a parquet file, with one difference that decides the
 * whole design: a parquet file arrives already divided, because the encoder wrote it in row groups. A
 * native partition arrives as ONE piece covering all of its rows, so before the
 * parquet-style "merge only the affected group" can pay off, the structure has to be manufactured. That
 * is the PRE-SPLIT, and it is free - cutting a piece in two is two entries over the SAME files, no bytes
 * move.
 * <p>
 * Actions are emitted in timestamp order and each occupies one position in the partition's new piece
 * array:
 * <ul>
 *     <li>{@link ActionType#KEEP} - the piece is untouched. Unlike parquet's {@code COPY_ROW_GROUP_SLICE}
 *     this copies NOTHING: the piece's bytes stay where they are and only its
 *     {@code (rowOffset, rowCount)} is carried into the new geometry record. Copying here would reinstate
 *     the write amplification the zero-copy design removes.</li>
 *     <li>{@link ActionType#MERGE} - the batch slice overlaps the piece's data, so the two go out as one
 *     image at the shared files' tail and the piece's row offset and row count move.</li>
 *     <li>{@link ActionType#NEW_PIECE} - the batch slice falls in a gap: below the first piece, between
 *     two, or above the last. It is written at the shared files' tail as a new piece under its own
 *     {@code tsLo}. One action type covers all three, which is why the design deletes both
 *     a batch below the first piece and a batch above the last are the same action.</li>
 *     <li>{@link ActionType#APPEND} - the batch slice falls above the last piece, or (outside dedup) only
 *     ties its {@code tsHi}, and that piece already owns the tail of the files and claims no O3 row of its
 *     own. Rather than found a new piece next to it, the batch is written right after it and the piece's
 *     own {@code tsHi} and {@code rowCount} grow
 *     to cover it - the two pieces were always going to be one contiguous run, so nothing distinguished
 *     them to begin with. At most one per plan, since only the last piece can own the tail.</li>
 * </ul>
 */
public class O3CompositeMergeStrategy {
    /**
     * Stride of the piece bounds list: {@code tsLo}, {@code tsHi}, {@code rowCount}.
     */
    public static final int LONGS_PER_BOUND = 4;
    private static final int BOUND_ROW_COUNT = 3;
    private static final int BOUND_ROW_OFFSET = 2;
    private static final int BOUND_TS_HI = 1;
    private static final int BOUND_TS_LO = 0;

    /**
     * @param rowOffset the FILE row this piece's first row sits at. Carried through the plan because the
     *                  executor reads a piece straight out of the partition's files, so it needs to know
     *                  where the piece is, not merely how big it is.
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
     * timestamp column; none can be derived here, because a timestamp range says nothing about where
     * inside it the rows sit and this class reads no files. {@link #rowsBelow} apportions rows evenly and
     * is only good enough to decide whether a cut is worth proposing.
     * <p>
     * A piece's bounds describe THE ROWS IT HOLDS, not the range it routes: the halves of a cut across a
     * data gap end up bounded by their own last and first rows, leaving the gap between them owned by
     * neither. That is what lets a later batch landing in the gap become a piece of its own instead of
     * merging into a neighbour that holds nothing near it.
     * <p>
     * Cutting a piece moves no bytes: both halves address the same column files at the same offsets, so
     * this is the whole of what a pre-split does to the geometry.
     *
     * @param below      rows of the piece below the cut
     * @param lowerTsHi  timestamp of the lower half's LAST row
     * @param upperTsLo  timestamp of the upper half's FIRST row
     * @return true when the cut was applied
     */
    public static boolean applyCut(LongList bounds, int piece, long below, long lowerTsHi, long upperTsLo) {
        final long tsHi = getTsHi(bounds, piece);
        final long rows = getRowCount(bounds, piece);
        if (tsHi == Numbers.LONG_NULL || below <= 0 || below >= rows) {
            return false;
        }
        final int at = piece * LONGS_PER_BOUND;
        // Both halves address the SAME files at the same places - that is what makes a cut free. The lower
        // half keeps the row offset it had; the upper half starts that many rows further in.
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
     * whose {@code tsHi} was never recorded ({@link Numbers#LONG_NULL}) claims its whole routing range,
     * because nothing else bounds it - the caller may narrow that by reading the piece's last timestamp
     * first, which is what makes a gap above such a piece usable.
     * <p>
     * Rows that fall in a gap become a new piece, EXCEPT when the piece on either side of the gap is
     * small: folding them into a small neighbour is cheaper than founding a piece, and it is the same
     * trade {@code smallRowGroupThreshold} makes for parquet.
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
     * @param commitMayDedup       whether this commit's rows can collide with an existing row and need a key
     *                             comparison to resolve it. When {@code false}, a batch row that only TIES a
     *                             piece's {@code tsHi} (rounds to the same tick, say the same second, as its
     *                             last row) needs no such comparison, so it is spared from that piece's claim
     *                             instead of forcing a {@link ActionType#MERGE} that would rewrite the piece
     *                             to reorder nothing - EXCEPT when the piece is a single point in time
     *                             ({@code tsLo == tsHi}): sparing it there would found a second piece at the
     *                             exact same instant, so it stays claimed and merges, growing that one piece
     *                             in place instead. A spared tie on the LAST piece is left for
     *                             {@link ActionType#APPEND}, which extends it in place with no piece founded
     *                             at all - the {@code tsLo == tsHi} exception does not apply there, since
     *                             growing the one piece a table ever has at its tail can never produce a
     *                             second one to collide with. A spared tie on an EARLIER piece has nowhere to
     *                             append to; it is left unclaimed for the next piece's own gap and claim
     *                             checks to resolve, exactly like ordinary gap data - which is what naturally
     *                             merges a repeat tie into the single-point piece a first one already founded,
     *                             rather than founding another next to it
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

            // Rows inside this piece's data range. A tie at this piece's own tsHi is spared from the claim
            // under commitMayDedup=false, UNLESS the piece is a single point in time (tsLo == tsHi): sparing
            // it there would found a second piece at that same exact instant, which nothing distinguishes
            // from the first - so a single-point piece keeps claiming its own ties and merges them in place
            // instead. The last piece is exempt from that exception: growing the one piece a table ever has
            // at its tail can never produce a second one to collide with, so its ties are always spared and
            // left for the APPEND check below, single point or not.
            final boolean isLastPiece = p == pieceCount - 1;
            final boolean spareTie = !commitMayDedup && tsHi != Numbers.LONG_NULL && (isLastPiece || tsLo < tsHi);
            final long claimHi = tsHi == Numbers.LONG_NULL
                    ? (p + 1 < pieceCount ? findLastBelow(sortedTimestampsAddr, o3, srcOooHi, getTsLo(bounds, p + 1)) : srcOooHi)
                    : lastAtOrBelow(sortedTimestampsAddr, o3, srcOooHi, spareTie ? tsHi - 1 : tsHi);
            if (claimHi >= o3) {
                actionAt(plan.actions, actionCount++).setMerge(p, o3, claimHi);
                o3 = claimHi + 1;
            } else if (isLastPiece
                    && o3 <= srcOooHi
                    && getRowOffset(bounds, p) + getRowCount(bounds, p) == physicalRows) {
                // This piece claims none of the batch itself: under commitMayDedup, tsHi's use of
                // lastAtOrBelow already guarantees every remaining O3 row sits strictly above it, which is
                // what keeps this safe under DEDUP - duplicates need equal timestamps, and there are none.
                // Under !commitMayDedup, spareTie let a tie at tsHi through unclaimed too, safe because
                // nothing needs deduplicating against it - either way it owns the shared files' tail, so
                // everything left in the batch extends it in place instead of founding a new piece next to
                // it.
                plan.appendActionIndex = actionCount;
                actionAt(plan.actions, actionCount++).setAppend(p, o3, srcOooHi);
                o3 = srcOooHi + 1;
            } else {
                actionAt(plan.actions, actionCount++).setKeep(p);
            }
        }

        // Everything above the last piece's data becomes a new piece at the shared tail. When that batch
        // is a plain append the two pieces end up tiling the files with no hole between them, and the
        // caller drops the geometry rather than record a boundary that says nothing - see isComposite in
        // O3PartitionJob. Nothing has to be decided here.
        if (o3 <= srcOooHi) {
            actionAt(plan.actions, actionCount++).setNewPiece(o3, srcOooHi);
        }
        return plan;
    }

    /**
     * PRE-SPLIT. Chooses where to cut existing pieces so the batch lands on as little data as possible,
     * and returns the cuts as {@code (pieceIndex, cutTimestamp)} pairs in {@code cutsOut}.
     * <p>
     * This is the step with no parquet counterpart. A parquet file arrives already divided into row
     * groups, so {@code computeMergeActions} always has somewhere small to merge into. A native directory
     * can be ONE piece covering the whole logical partition, and merging the batch into it rewrites the
     * whole day. Cutting is free - two records over the SAME files, no bytes move - so the structure can
     * be manufactured lazily, aimed at exactly where this batch lands.
     * <p>
     * For each piece the batch overlaps, up to two cuts are proposed: at the batch's first timestamp
     * inside the piece, so everything below it stays untouched, and one past the batch's last timestamp
     * inside the piece, so everything above stays untouched. A cut is proposed only when it would leave
     * at least {@code minPieceRows} rows on the side being spared - splitting off a sliver costs a record
     * and saves nothing.
     * <p>
     * Row counts are apportioned by TIMESTAMP position, which assumes rows are distributed evenly across
     * the piece's range. That is an estimate and it only decides whether a cut is worth making; the
     * caller resolves each cut timestamp to an actual row by binary-searching the piece's timestamp
     * column, and a cut that turns out to fall at row 0 or at the piece's end is simply dropped.
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
