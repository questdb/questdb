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

package org.questdb;

import io.questdb.cairo.TableWriterSegmentCopyInfo;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Builds the sort index of a WAL transaction block of 1M rows: radix sort of all the rows versus
 * the sort plan, TableWriterSegmentCopyInfo.buildSortPlan(), falling back to the radix sort when the plan
 * is not worth it, the same way TableWriter does. Only the index is built, the column shuffle is not measured.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class WalBlockSortBenchmark {
    private static final int SEGMENT_COUNT = 4;
    private static final long TOTAL_ROWS = 1_000_000;
    private final DirectLongList segmentAddresses = new DirectLongList(SEGMENT_COUNT, MemoryTag.NATIVE_DEFAULT);
    private final long[] segmentSizes = new long[SEGMENT_COUNT];
    @Param({"ORDERED_SEGMENTS", "LATE_TXN", "LATE_WIDE_TXN", "OVERLAPPING_WRITERS", "TINY_TXNS", "UNORDERED"})
    public String scenario;
    // 1M rows 1ms apart span ~2^30 microseconds, 1s apart span ~2^40 (e.g. nanos)
    @Param({"1000", "1000000"})
    public long tsStep;
    private long bufSize;
    private TableWriterSegmentCopyInfo copyInfo;
    private long cpy;
    private long out;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(WalBlockSortBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public long plan() {
        if (copyInfo.buildSortPlan()) {
            long format = Vect.sortManySegmentsIndexByPlan(
                    out,
                    cpy,
                    segmentAddresses.getAddress(),
                    copyInfo.getSegmentsAddress(),
                    copyInfo.getSegmentCount(),
                    copyInfo.getTxnInfoAddress(),
                    copyInfo.getTxnCount(),
                    copyInfo.getMaxTxRowCount(),
                    copyInfo.getSortPlanItemsAddress(),
                    copyInfo.getSortPlanItemCount(),
                    copyInfo.getSortPlanTxnsAddress(),
                    copyInfo.getSortPlanTxnCount(),
                    TOTAL_ROWS
            );
            if (Vect.isIndexSuccess(format)) {
                return format;
            }
            throw new IllegalStateException("plan sort failed: " + format);
        }
        return radix();
    }

    @Benchmark
    public long radix() {
        return Vect.radixSortManySegmentsIndexAsc(
                out,
                cpy,
                segmentAddresses.getAddress(),
                copyInfo.getSegmentCount(),
                copyInfo.getTxnInfoAddress(),
                copyInfo.getTxnCount(),
                copyInfo.getMaxTxRowCount(),
                0,
                0,
                copyInfo.getMinTimestamp(),
                copyInfo.getMaxTimestamp(),
                TOTAL_ROWS,
                Vect.SHUFFLE_INDEX_FORMAT
        );
    }

    @Setup(Level.Trial)
    public void setup() {
        bufSize = TOTAL_ROWS * 3 * Long.BYTES + Long.BYTES;
        out = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        cpy = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        copyInfo = new TableWriterSegmentCopyInfo();

        // Transaction layout: [segment, row count, first timestamp, step, sorted]
        final Rnd rnd = new Rnd();
        final long base = 1_700_000_000_000_000L;
        final int txnCount;
        final long[][] txns;
        switch (scenario) {
            case "ORDERED_SEGMENTS", "LATE_TXN", "LATE_WIDE_TXN" -> {
                // 100 sorted commits of 10k rows, 4 writers taking turns, commits do not overlap
                final boolean late = !scenario.equals("ORDERED_SEGMENTS");
                txnCount = 100;
                txns = new long[txnCount][];
                final long rows = TOTAL_ROWS / txnCount;
                for (int t = 0; t < txnCount; t++) {
                    txns[t] = new long[]{t % SEGMENT_COUNT, rows, base + t * rows * tsStep, tsStep, 1};
                }
                if (late) {
                    // the last commit is late, it overlaps one commit or all of them
                    final long lateRows = 1000;
                    txns[txnCount - 2][1] += rows - lateRows;
                    final long lateStep = scenario.equals("LATE_TXN") ? tsStep : (TOTAL_ROWS / lateRows) * tsStep;
                    txns[txnCount - 1] = new long[]{SEGMENT_COUNT - 1, lateRows, base + 50 * rows * tsStep + 1, lateStep, 1};
                }
            }
            case "OVERLAPPING_WRITERS" -> {
                // 4 writers commit 10k rows each at the same time
                txnCount = 100;
                txns = new long[txnCount][];
                final long rows = TOTAL_ROWS / txnCount;
                for (int t = 0; t < txnCount; t++) {
                    final long round = t / SEGMENT_COUNT;
                    txns[t] = new long[]{t % SEGMENT_COUNT, rows, base + round * rows * tsStep + t % SEGMENT_COUNT, tsStep, 1};
                }
            }
            case "TINY_TXNS" -> {
                // 100k commits of 10 rows from 4 interleaving writers
                txnCount = 100_000;
                txns = new long[txnCount][];
                final long rows = TOTAL_ROWS / txnCount;
                for (int t = 0; t < txnCount; t++) {
                    final long round = t / SEGMENT_COUNT;
                    txns[t] = new long[]{t % SEGMENT_COUNT, rows, base + round * rows * SEGMENT_COUNT * tsStep + t % SEGMENT_COUNT, SEGMENT_COUNT * tsStep, 1};
                }
            }
            default -> {
                // 100 commits of 10k rows in random order
                txnCount = 100;
                txns = new long[txnCount][];
                final long rows = TOTAL_ROWS / txnCount;
                for (int t = 0; t < txnCount; t++) {
                    txns[t] = new long[]{t % SEGMENT_COUNT, rows, base, tsStep, 0};
                }
            }
        }

        // lay out the segments, transactions of a segment are in seqTxn order
        final long[] segmentRows = new long[SEGMENT_COUNT];
        for (long[] txn : txns) {
            segmentRows[(int) txn[0]] += txn[1];
        }
        for (int s = 0; s < SEGMENT_COUNT; s++) {
            segmentSizes[s] = Math.max(1, segmentRows[s]) * 2 * Long.BYTES;
            segmentAddresses.add(Unsafe.malloc(segmentSizes[s], MemoryTag.NATIVE_DEFAULT));
        }
        for (int s = 0; s < SEGMENT_COUNT; s++) {
            final long addr = segmentAddresses.get(s);
            long row = 0;
            for (int t = 0; t < txnCount; t++) {
                final long[] txn = txns[t];
                if (txn[0] != s) {
                    continue;
                }
                long min = Long.MAX_VALUE;
                long max = Long.MIN_VALUE;
                final long lo = row;
                for (long r = 0; r < txn[1]; r++, row++) {
                    final long ts = txn[4] == 1 ? txn[2] + r * txn[3] : txn[2] + rnd.nextLong(TOTAL_ROWS * tsStep);
                    Unsafe.putLong(addr + row * 16, ts);
                    Unsafe.putLong(addr + row * 16 + 8, row);
                    min = Math.min(min, ts);
                    max = Math.max(max, ts);
                }
                copyInfo.addTxn(lo, t, txn[1], s, min, max, txn[4] == 1);
            }
            copyInfo.addSegment(0, s, 0, row, false);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        Unsafe.free(out, bufSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(cpy, bufSize, MemoryTag.NATIVE_DEFAULT);
        for (int s = 0; s < SEGMENT_COUNT; s++) {
            Unsafe.free(segmentAddresses.get(s), segmentSizes[s], MemoryTag.NATIVE_DEFAULT);
        }
        segmentAddresses.close();
        copyInfo.close();
    }
}
