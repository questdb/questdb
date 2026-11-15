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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.qwp.server.egress.QwpBackwardReadAhead;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.test.cutlass.qwp.QwpBackwardReadAheadTest.RecordingFacade;
import io.questdb.test.cutlass.qwp.QwpBackwardReadAheadTest.SyntheticFrame;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Fuzzes {@link QwpBackwardReadAhead} with streams of frames of random column types and value
 * sizes, which it walks down the way the egress appends them, in batches of random sizes. Over
 * data out of memory, the stream must ask for every byte of every column once, before the walk
 * reads its row, and no window may take more than the byte cap unless it holds one slice of rows.
 * Over data in memory but for random holes, the stream must ask for every hole before the walk
 * reads it. Neither may ask for anything outside the published extents.
 * <p>
 * The first var-size column of a stream cycles through every type, value shape and kind of data,
 * so every run covers each combination; the rest is random. A run prints its seeds, which
 * {@code -Dfuzz.s0=} and {@code -Dfuzz.s1=} replay, and {@code -Dquestdb.fuzz.read.ahead.streams=}
 * sets how many streams it runs.
 */
public class QwpBackwardReadAheadFuzzTest {
    private static final int[] BATCH_ROWS = {7, 100, 1_000, (int) QwpBackwardReadAhead.INITIAL_WINDOW_ROWS};
    private static final int[] FIXED_TYPES = {ColumnType.LONG, ColumnType.INT, ColumnType.SHORT, ColumnType.DOUBLE, ColumnType.UUID};
    private static final long[] FRAME_ROWS = {1, 100, 1_000, 1_023, 3_000, 16_383, 16_384, 20_000, 70_000, 250_000};
    private static final Log LOG = LogFactory.getLog(QwpBackwardReadAheadFuzzTest.class);
    // value shapes of a var-size column: 17,000 bytes, one slice takes more than the byte cap
    private static final int SHAPE_17_KB = 4;
    private static final int SHAPE_2_KIB = 3;
    private static final int SHAPE_70_KB = 5;
    // short values with bursts of 40 to 80 KB
    private static final int SHAPE_BURSTS = 6;
    private static final int SHAPE_COLUMN_TOP = 0;
    private static final int SHAPE_COUNT = 10;
    // aux entries that point anywhere, inside the data extent or past it
    private static final int SHAPE_HOSTILE = 9;
    // VARCHARs inline in the aux vector or NULL, values of no bytes in the other types
    private static final int SHAPE_NO_DATA = 1;
    // 1 to 8,000 bytes
    private static final int SHAPE_RANDOM = 7;
    private static final int SHAPE_SHORT = 2;
    // about 1 KB, every tenth of no bytes
    private static final int SHAPE_SPARSE = 8;
    // the egress walks a frame down in slices of at most this many rows
    private static final int SLICE_ROWS = 1_024;
    private static final int STREAMS = Integer.getInteger("questdb.fuzz.read.ahead.streams", 160);
    private static final int[] VAR_TYPES = {ColumnType.STRING, ColumnType.BINARY, ColumnType.VARCHAR, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1)};
    // streams it takes the first var-size column to cycle through every type, shape and kind of data
    private static final int CYCLE = VAR_TYPES.length * SHAPE_COUNT * 2;

    @Test
    public void testFuzz() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> fuzz(rnd, STREAMS, new Coverage()));
    }

    @Test
    public void testFuzzCoversEveryShape() throws Exception {
        // Whatever the seed, a run of the default budget covers every type, value shape and kind
        // of data, every batch size and frame size, and windows the stream fits to the byte cap
        // as well as windows of one slice that take more.
        final Coverage coverage = new Coverage();
        TestUtils.assertMemoryLeak(() -> fuzz(TestUtils.generateRandom(LOG, 42, 4_242), Math.max(STREAMS, CYCLE), coverage));
        Assert.assertEquals(VAR_TYPES.length * SHAPE_COUNT * 2, coverage.combinations.size());
        Assert.assertEquals(BATCH_ROWS.length, coverage.batchRows.size());
        Assert.assertEquals(FRAME_ROWS.length, coverage.frameRows.size());
        Assert.assertTrue("windows near the cap: " + coverage.windowsNearCap, coverage.windowsNearCap > 0);
        Assert.assertTrue("windows of one slice over the cap: " + coverage.windowsOfOneSliceOverCap, coverage.windowsOfOneSliceOverCap > 0);
        Assert.assertTrue("holes: " + coverage.holes, coverage.holes > 0);
    }

    // Adds up to four holes to data in memory, each in a random byte range of a random column of
    // the frame; returns them as [lo, hi) pairs.
    private static LongList addHoles(Rnd rnd, SyntheticFrame frame, RecordingFacade ff) {
        final LongList holes = new LongList();
        final LongList ranges = frame.rangesOfRows(0, frame.rows);
        for (int h = 0, n = rnd.nextInt(5); h < n && ranges.size() > 0; h++) {
            final int range = 2 * rnd.nextInt(ranges.size() / 2);
            final long lo = ranges.getQuick(range);
            final long size = ranges.getQuick(range + 1) - lo;
            final long holeLo = lo + rnd.nextLong(size);
            final long holeHi = Math.min(lo + size, holeLo + 1 + rnd.nextLong(1 << 20));
            ff.miss(holeLo, holeHi);
            holes.add(holeLo, holeHi);
        }
        return holes;
    }

    private static void applyShape(Rnd rnd, SyntheticFrame frame, int ci, int type, int shape) {
        // the frame's data starts where the rows of the frames before it end
        final long dataLo = rnd.nextBoolean() ? 0 : rnd.nextInt(1_000_000);
        switch (shape) {
            case SHAPE_COLUMN_TOP -> {
                // the frame publishes nothing for the column
            }
            case SHAPE_NO_DATA -> frame.values(ci, dataLo, ColumnType.isVarchar(type) ? r -> r % 10 : r -> 0);
            case SHAPE_SHORT -> frame.values(ci, dataLo, r -> 50);
            case SHAPE_2_KIB -> frame.values(ci, dataLo, r -> 2_048);
            case SHAPE_17_KB -> frame.values(ci, dataLo, r -> 17_000);
            case SHAPE_70_KB -> frame.values(ci, dataLo, r -> 70_000);
            case SHAPE_BURSTS -> {
                final long period = 1_000 + rnd.nextInt(5_000);
                frame.values(ci, dataLo, r -> (r / period) % 4 == 1 ? 40_000 + (r * 31) % 40_000 : 20 + r % 100);
            }
            case SHAPE_RANDOM -> {
                final long seed = rnd.nextLong();
                frame.values(ci, dataLo, r -> 1 + Math.floorMod(seed ^ (r * 0x9E3779B97F4A7C15L), 8_000L));
            }
            case SHAPE_SPARSE -> frame.values(ci, dataLo, r -> r % 10 == 3 ? 0 : 900 + (r * 13) % 300);
            case SHAPE_HOSTILE -> {
                final long dataSize = Math.max(1, frame.rows * rnd.nextInt(5_000));
                final long seed = rnd.nextLong();
                frame.offsets(ci, dataSize, r -> Math.floorMod(seed ^ (r * 0x9E3779B97F4A7C15L), 2 * dataSize));
            }
            default -> throw new AssertionError("shape " + shape);
        }
    }

    // every byte of the holes that the rows [rowLo, rowHi) take has been asked for
    private static void assertHolesCovered(String context, SyntheticFrame frame, RecordingFacade ff, LongList holes, long rowLo, long rowHi) {
        final LongList ranges = frame.rangesOfRows(rowLo, rowHi);
        for (int i = 0, n = ranges.size(); i < n; i += 2) {
            for (int h = 0, m = holes.size(); h < m; h += 2) {
                final long lo = Math.max(ranges.getQuick(i), holes.getQuick(h));
                final long hi = Math.min(ranges.getQuick(i + 1), holes.getQuick(h + 1));
                if (hi > lo) {
                    Assert.assertEquals(context + ": hole [" + lo + ", " + hi + ")", hi - lo, ff.coveredWithin(lo, hi));
                }
            }
        }
    }

    private static void fuzz(Rnd rnd, int streams, Coverage coverage) {
        for (int stream = 0; stream < streams; stream++) {
            fuzzStream(rnd, stream, coverage);
        }
    }

    private static void fuzzStream(Rnd rnd, int stream, Coverage coverage) {
        final int fixedCount = rnd.nextInt(3);
        final int varCount = 1 + rnd.nextInt(3);
        final int[] types = new int[fixedCount + varCount];
        for (int i = 0; i < fixedCount; i++) {
            types[i] = FIXED_TYPES[rnd.nextInt(FIXED_TYPES.length)];
        }
        // the first var-size column cycles through every type, shape and kind of data
        final int cycle = stream % CYCLE;
        types[fixedCount] = VAR_TYPES[cycle % VAR_TYPES.length];
        final int firstShape = (cycle / VAR_TYPES.length) % SHAPE_COUNT;
        final boolean isCold = cycle / (VAR_TYPES.length * SHAPE_COUNT) == 0;
        for (int i = fixedCount + 1; i < types.length; i++) {
            types[i] = VAR_TYPES[rnd.nextInt(VAR_TYPES.length)];
        }
        final int batchRows = BATCH_ROWS[rnd.nextInt(BATCH_ROWS.length)];
        coverage.batchRows.add(batchRows);
        coverage.combinations.add(cycle);

        final RecordingFacade ff = new RecordingFacade(true);
        if (!isCold) {
            ff.isResident = true;
            // without the thread's major faults, the stream never trusts a frame it did not check
            ff.majorFaults = -1;
        }
        final QwpBackwardReadAhead readAhead = new QwpBackwardReadAhead(ff);
        long batchLeft = batchRows;
        for (int f = 0, frames = 1 + rnd.nextInt(3); f < frames; f++) {
            final int frameRowsIndex = rnd.nextInt(FRAME_ROWS.length);
            final long rows = FRAME_ROWS[frameRowsIndex];
            coverage.frameRows.add(frameRowsIndex);
            try (SyntheticFrame frame = new SyntheticFrame(types, rows)) {
                boolean isHostile = false;
                for (int i = fixedCount; i < types.length; i++) {
                    final int shape = i == fixedCount ? firstShape : rnd.nextInt(SHAPE_COUNT);
                    applyShape(rnd, frame, i, types[i], shape);
                    isHostile |= shape == SHAPE_HOSTILE;
                }
                if (f == 0) {
                    readAhead.of(frame.metadata());
                }
                ff.clear();
                ff.clearMisses();
                final LongList holes = isCold ? null : addHoles(rnd, frame, ff);
                if (holes != null) {
                    coverage.holes += holes.size() / 2;
                }
                final String context = "stream " + stream + ", frame " + f + " of " + rows + " rows, batches of " + batchRows
                        + (isCold ? ", cold" : ", holes " + holes);
                readAhead.ofFrame(frame, rows);
                // the window checks read where windows start from the requests of a fixed-size column
                final boolean isWindowChecked = isCold && fixedCount > 0 && !isHostile;
                final long values = fixedCount > 0 ? frame.getPageAddress(0) : 0;
                final long valueSize = fixedCount > 0 ? ColumnType.sizeOf(types[0]) : 0;
                long advisedLo = rows;
                long previousWindowRows = 0;
                for (long emitted = 0; emitted < rows; ) {
                    if (batchLeft == 0) {
                        batchLeft = batchRows;
                    }
                    final long slice = Math.min(Math.min(batchLeft, rows - emitted), SLICE_ROWS);
                    final long lo = rows - emitted - slice;
                    readAhead.advise(lo);
                    if (isWindowChecked) {
                        final long windowLo = (ff.lowestWithin(values, values + valueSize * rows) - values) / valueSize;
                        if (windowLo < advisedLo) {
                            final long windowRows = advisedLo - windowLo;
                            final long windowBytes = frame.bytesOfRows(windowLo, advisedLo);
                            final boolean isOneSlice = windowRows <= Math.max(SLICE_ROWS, advisedLo - lo);
                            Assert.assertTrue(
                                    context + ": window [" + windowLo + ", " + advisedLo + ") takes " + windowBytes + " bytes",
                                    windowBytes <= QwpBackwardReadAhead.MAX_WINDOW_BYTES || isOneSlice
                            );
                            if (windowBytes > QwpBackwardReadAhead.MAX_WINDOW_BYTES) {
                                coverage.windowsOfOneSliceOverCap++;
                            } else if (windowBytes >= QwpBackwardReadAhead.MAX_WINDOW_BYTES * 3 / 4) {
                                coverage.windowsNearCap++;
                            }
                            if (previousWindowRows > 0 && windowLo > 0) {
                                Assert.assertTrue(context + ": window of " + windowRows + " rows after " + previousWindowRows, windowRows <= 2 * previousWindowRows);
                            }
                            previousWindowRows = windowRows;
                            advisedLo = windowLo;
                        }
                    }
                    if (isCold) {
                        frame.assertCovered(context + ": slice [" + lo + ", " + (lo + slice) + ")", ff, lo, lo + slice);
                    } else {
                        assertHolesCovered(context + ": slice [" + lo + ", " + (lo + slice) + ")", frame, ff, holes, lo, lo + slice);
                    }
                    emitted += slice;
                    batchLeft -= slice;
                }
                if (isCold) {
                    frame.assertTiled(context, ff);
                }
                ff.assertWithinExtents(context, frame);
            }
        }
    }

    private static final class Coverage {
        final IntHashSet batchRows = new IntHashSet();
        // the type, shape and kind of data of the first var-size column, as an index into the cycle
        final IntHashSet combinations = new IntHashSet();
        final IntHashSet frameRows = new IntHashSet();
        long holes;
        long windowsNearCap;
        long windowsOfOneSliceOverCap;
    }
}
