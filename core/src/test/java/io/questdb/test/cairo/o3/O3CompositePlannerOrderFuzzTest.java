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

package io.questdb.test.cairo.o3;

import io.questdb.cairo.O3CompositeMergeStrategy;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Drives the pure planner - clusterer-style cuts, batch cuts, actions - over several commits of tie-heavy data
 * and checks that the pieces the executor would then record ascend strictly by tsLo, which is what
 * {@code PartitionGeometry.addPiece} asserts. The model keeps every piece's real rows, so a cut resolves exactly
 * as {@code O3PartitionJob.applyCutResolved} does, including its rule that a cut may not found a piece on the
 * next piece's tsLo: without that rule the shape {@code [10..20] [20..20]} plus a cut at 20 fails within a
 * few dozen seeds.
 */
public class O3CompositePlannerOrderFuzzTest {

    @Test
    public void testPiecesAscendAcrossCommits() {
        for (int iter = 0; iter < 20_000; iter++) {
            final long s0 = 1000L + iter, s1 = 7L * iter + 3;
            runOne(new Rnd(s0, s1), s0, s1);
        }
    }

    private static void runOne(Rnd rnd, long s0, long s1) {
        // pieces: list of sorted long[] (rows), plus bounds (tsLo, tsHi, rowOffset, rowCount)
        final ObjList<long[]> rows = new ObjList<>();
        final LongList bounds = new LongList();
        // Start from one plain run of rows at file 0.
        final int rowsPerTick = 1 + rnd.nextInt(5);
        final int ticks = 5 + rnd.nextInt(30);
        long[] first = new long[ticks * rowsPerTick];
        for (int t = 0, k = 0; t < ticks; t++) {
            for (int r = 0; r < rowsPerTick; r++) {
                first[k++] = (t + 1) * 10L;
            }
        }
        rows.add(first);
        long e = first.length;
        O3CompositeMergeStrategy.addPieceBounds(bounds, first[0], Numbers.LONG_NULL, 0, first.length, -1, 0);
        final long minPieceRows = 2 + rnd.nextInt(4);
        final int commits = 1 + rnd.nextInt(6);
        final LongList cuts = new LongList();
        final O3CompositeMergeStrategy.Plan plan = new O3CompositeMergeStrategy.Plan();
        final StringBuilder trace = new StringBuilder();
        for (int c = 0; c < commits; c++) {
            // batch: sorted timestamps with ties, some inside, some above
            final int n = 1 + rnd.nextInt(12);
            long[] batch = new long[n];
            final long maxTs = (ticks + 5) * 10L;
            for (int i = 0; i < n; i++) {
                batch[i] = (1 + rnd.nextInt((int) (maxTs / 10))) * 10L;
            }
            Arrays.sort(batch);
            final long addr = Unsafe.malloc(n * 16L, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < n; i++) {
                    Unsafe.getUnsafe().putLong(addr + i * 16L, batch[i]);
                    Unsafe.getUnsafe().putLong(addr + i * 16L + 8, i);
                }
                // resolve NULL tsHi like the planner does
                for (int p = 0; p < rows.size(); p++) {
                    if (O3CompositeMergeStrategy.getTsHi(bounds, p) == Numbers.LONG_NULL) {
                        long[] pr = rows.getQuick(p);
                        bounds.setQuick(p * O3CompositeMergeStrategy.LONGS_PER_BOUND + 1, pr[pr.length - 1]);
                    }
                }
                trace.setLength(0);
                trace.append("commit ").append(c).append(" before=").append(fmt(bounds, rows)).append(" batch=").append(Arrays.toString(batch)).append(" e=").append(e).append(" minPieceRows=").append(minPieceRows);
                // optional "cluster" cuts at random timestamps
                final int clusterCuts = rnd.nextInt(3);
                for (int k = 0; k < clusterCuts; k++) {
                    final long cutTs = (1 + rnd.nextInt((int) (maxTs / 10))) * 10L;
                    final int piece = O3CompositeMergeStrategy.findPieceContaining(bounds, cutTs);
                    if (piece > -1) {
                        resolveAndCut(bounds, rows, piece, cutTs, 0, 0);
                    }
                }
                O3CompositeMergeStrategy.computeCuts(bounds, addr, 0, n - 1, minPieceRows, 10_000, cuts);
                for (int k = cuts.size() - O3CompositeMergeStrategy.LONGS_PER_CUT; k >= 0; k -= O3CompositeMergeStrategy.LONGS_PER_CUT) {
                    resolveAndCut(bounds, rows, (int) cuts.getQuick(k), cuts.getQuick(k + 1), cuts.getQuick(k + 2), cuts.getQuick(k + 3));
                }
                trace.append(" afterCuts=").append(fmt(bounds, rows));
                O3CompositeMergeStrategy.computeActions(bounds, addr, 0, n - 1, minPieceRows, e, false, plan);
                // execute: build new pieces in action order
                final ObjList<long[]> outRows = new ObjList<>();
                final LongList out = new LongList();
                long tail = e;
                trace.append(" actions=");
                for (int i = 0; i < plan.actions.size(); i++) {
                    final O3CompositeMergeStrategy.Action a = plan.actions.getQuick(i);
                    trace.append(a.type).append('(').append(a.pieceIndex).append(',').append(a.o3Lo).append("..").append(a.o3Hi).append(") ");
                    switch (a.type) {
                        case KEEP -> {
                            addPiece(out, outRows, bounds, p(a), rows.getQuick(a.pieceIndex), O3CompositeMergeStrategy.getRowOffset(bounds, a.pieceIndex));
                        }
                        case NEW_PIECE -> {
                            long[] r = Arrays.copyOfRange(batch, (int) a.o3Lo, (int) a.o3Hi + 1);
                            out.add(r[0], r[r.length - 1], tail, r.length);
                            outRows.add(r);
                            tail += r.length;
                        }
                        case MERGE -> {
                            long[] pr = rows.getQuick(a.pieceIndex);
                            long[] br = Arrays.copyOfRange(batch, (int) a.o3Lo, (int) a.o3Hi + 1);
                            long[] m = new long[pr.length + br.length];
                            System.arraycopy(pr, 0, m, 0, pr.length);
                            System.arraycopy(br, 0, m, pr.length, br.length);
                            Arrays.sort(m);
                            out.add(Math.min(O3CompositeMergeStrategy.getTsLo(bounds, a.pieceIndex), br[0]),
                                    Math.max(O3CompositeMergeStrategy.getTsHi(bounds, a.pieceIndex), br[br.length - 1]), tail, m.length);
                            outRows.add(m);
                            tail += m.length;
                        }
                        case APPEND -> {
                            long[] pr = rows.getQuick(a.pieceIndex);
                            long[] br = Arrays.copyOfRange(batch, (int) a.o3Lo, (int) a.o3Hi + 1);
                            long[] m = new long[pr.length + br.length];
                            System.arraycopy(pr, 0, m, 0, pr.length);
                            System.arraycopy(br, 0, m, pr.length, br.length);
                            final long off = O3CompositeMergeStrategy.getRowOffset(bounds, a.pieceIndex);
                            Assert.assertEquals(trace.toString(), e, off + pr.length);
                            out.add(O3CompositeMergeStrategy.getTsLo(bounds, a.pieceIndex), br[br.length - 1], off, m.length);
                            outRows.add(m);
                            tail += br.length;
                        }
                        case DROP -> {
                        }
                    }
                }
                // APPEND writes first at e, so NEW_PIECE/MERGE offsets shift by its rows; mirror that.
                if (plan.appendActionIndex > -1) {
                    final O3CompositeMergeStrategy.Action ap = plan.actions.getQuick(plan.appendActionIndex);
                    final long shift = ap.o3Hi - ap.o3Lo + 1;
                    for (int i = 0; i < out.size(); i += 4) {
                        if (out.getQuick(i + 2) >= e) {
                            out.setQuick(i + 2, out.getQuick(i + 2) + shift);
                        }
                    }
                }
                e = tail;
                // check strictly ascending tsLo (fold does not change tsLo of a run's first piece)
                for (int i = 4; i < out.size(); i += 4) {
                    if (out.getQuick(i) <= out.getQuick(i - 4)) {
                        Assert.fail("pieces must ascend by tsLo at out piece " + (i / 4) + " [seeds=" + s0 + "L," + s1 + "L] " + trace
                                + " out=" + fmtOut(out));
                    }
                }
                // fold adjacent & rebuild model
                rows.clear();
                bounds.clear();
                for (int i = 0; i < out.size(); i += 4) {
                    final long tsLo = out.getQuick(i), tsHi = out.getQuick(i + 1), off = out.getQuick(i + 2), cnt = out.getQuick(i + 3);
                    final int last = bounds.size() - O3CompositeMergeStrategy.LONGS_PER_BOUND;
                    if (last >= 0 && off == O3CompositeMergeStrategy.getRowOffset(bounds, last / O3CompositeMergeStrategy.LONGS_PER_BOUND) + O3CompositeMergeStrategy.getRowCount(bounds, last / O3CompositeMergeStrategy.LONGS_PER_BOUND)) {
                        // fold
                        long[] prev = rows.getQuick(rows.size() - 1);
                        long[] cur = outRows.getQuick(i / 4);
                        long[] m = new long[prev.length + cur.length];
                        System.arraycopy(prev, 0, m, 0, prev.length);
                        System.arraycopy(cur, 0, m, prev.length, cur.length);
                        rows.setQuick(rows.size() - 1, m);
                        bounds.setQuick(last + 1, tsHi);
                        bounds.setQuick(last + 3, O3CompositeMergeStrategy.getRowCount(bounds, last / O3CompositeMergeStrategy.LONGS_PER_BOUND) + cnt);
                        continue;
                    }
                    O3CompositeMergeStrategy.addPieceBounds(bounds, tsLo, tsHi, off, cnt, -1, 0);
                    rows.add(outRows.getQuick(i / 4));
                }
            } finally {
                Unsafe.free(addr, n * 16L, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private static int p(O3CompositeMergeStrategy.Action a) {
        return a.pieceIndex;
    }

    private static void addPiece(LongList out, ObjList<long[]> outRows, LongList bounds, int p, long[] r, long off) {
        out.add(O3CompositeMergeStrategy.getTsLo(bounds, p), O3CompositeMergeStrategy.getTsHi(bounds, p), off, r.length);
        outRows.add(r);
    }

    private static String fmt(LongList bounds, ObjList<long[]> rows) {
        final StringBuilder sb = new StringBuilder("[");
        for (int p = 0; p < rows.size(); p++) {
            sb.append(p).append(":[").append(O3CompositeMergeStrategy.getTsLo(bounds, p)).append("..").append(O3CompositeMergeStrategy.getTsHi(bounds, p)).append("]@")
                    .append(O3CompositeMergeStrategy.getRowOffset(bounds, p)).append('+').append(O3CompositeMergeStrategy.getRowCount(bounds, p)).append(' ');
        }
        return sb.append(']').toString();
    }

    private static String fmtOut(LongList out) {
        final StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < out.size(); i += 4) {
            sb.append('[').append(out.getQuick(i)).append("..").append(out.getQuick(i + 1)).append("]@").append(out.getQuick(i + 2)).append('+').append(out.getQuick(i + 3)).append(' ');
        }
        return sb.append(']').toString();
    }

    /**
     * Mirrors O3PartitionJob.applyCutResolved over the piece's own rows.
     */
    private static void resolveAndCut(LongList bounds, ObjList<long[]> rows, int piece, long cutTs, long minBelow, long minAbove) {
        final long[] r = rows.getQuick(piece);
        if (r.length < 2) {
            return;
        }
        int row = 0;
        while (row < r.length && r[row] < cutTs) {
            row++;
        }
        if (row <= 0 || row >= r.length) {
            return;
        }
        if (row < minBelow || r.length - row < minAbove) {
            return;
        }
        // applyCutResolved: the upper half may not start on the next piece's tsLo.
        if (piece + 1 < rows.size() && O3CompositeMergeStrategy.getTsLo(bounds, piece + 1) == r[row]) {
            return;
        }
        if (O3CompositeMergeStrategy.applyCut(bounds, piece, row, r[row - 1], r[row])) {
            rows.setQuick(piece, Arrays.copyOfRange(r, 0, row));
            rows.add(null);
            for (int i = rows.size() - 1; i > piece + 1; i--) {
                rows.setQuick(i, rows.getQuick(i - 1));
            }
            rows.setQuick(piece + 1, Arrays.copyOfRange(r, row, r.length));
        }
    }
}
