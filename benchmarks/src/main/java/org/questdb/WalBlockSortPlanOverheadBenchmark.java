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
 * Measures the planning overhead of small WAL blocks with many transactions, where
 * TableWriterSegmentCopyInfo.buildSortPlan() sorts one entry per run, and the run count is around 600, where
 * Vect.sortLongIndexAscInPlace() switches from pdqsort to radix sort. 4 writers take turns committing.
 * <ul>
 *     <li>plan: the sort plan, falling back to the radix sort when the plan is not worth it, as TableWriter does</li>
 *     <li>radix: the radix sort of all the rows</li>
 *     <li>sortRunMinima: Vect.sortLongIndexAscInPlace() on the run minima alone</li>
 * </ul>
 * 32-row commits are rejected before the runs are sorted. 64-row commits are copyable, with EQUAL minima they
 * all overlap and the plan is rejected after sorting the runs.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class WalBlockSortPlanOverheadBenchmark {
    private static final int SEGMENT_COUNT = 4;
    private final DirectLongList minima = new DirectLongList(2, MemoryTag.NATIVE_DEFAULT);
    private final DirectLongList minimaTemplate = new DirectLongList(2, MemoryTag.NATIVE_DEFAULT);
    private final DirectLongList segmentAddresses = new DirectLongList(SEGMENT_COUNT, MemoryTag.NATIVE_DEFAULT);
    private final long[] segmentSizes = new long[SEGMENT_COUNT];
    // EQUAL: every row has the same timestamp; the others order non-overlapping commits by time
    @Param({"EQUAL", "ASCENDING", "DESCENDING", "ORGAN_PIPE"})
    public String minimaOrder;
    @Param({"32", "64"})
    public int rowsPerTxn;
    @Param({"598", "599", "600", "601"})
    public int txnCount;
    private long bufSize;
    private TableWriterSegmentCopyInfo copyInfo;
    private long cpy;
    private long out;
    private long totalRows;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(WalBlockSortPlanOverheadBenchmark.class.getSimpleName())
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
                    totalRows
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
                totalRows,
                Vect.SHUFFLE_INDEX_FORMAT
        );
    }

    @Setup(Level.Trial)
    public void setup() {
        totalRows = (long) txnCount * rowsPerTxn;
        bufSize = totalRows * 3 * Long.BYTES + Long.BYTES;
        out = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        cpy = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        copyInfo = new TableWriterSegmentCopyInfo();

        final long base = 1_700_000_000_000_000L;
        // the planner sorts (min timestamp, run index) pairs, runs follow the segment-grouped transaction order
        long runIndex = 0;
        for (int s = 0; s < SEGMENT_COUNT; s++) {
            final int segmentTxns = (txnCount - s + SEGMENT_COUNT - 1) / SEGMENT_COUNT;
            segmentSizes[s] = Math.max(1L, (long) segmentTxns * rowsPerTxn) * 2 * Long.BYTES;
            final long addr = Unsafe.malloc(segmentSizes[s], MemoryTag.NATIVE_DEFAULT);
            segmentAddresses.add(addr);
            long row = 0;
            for (int t = s; t < txnCount; t += SEGMENT_COUNT) {
                final long slot = switch (minimaOrder) {
                    case "EQUAL" -> 0;
                    case "ASCENDING" -> t;
                    case "DESCENDING" -> txnCount - 1 - t;
                    default -> t % 2 == 0 ? t / 2 : txnCount - 1 - t / 2;
                };
                final long step = minimaOrder.equals("EQUAL") ? 0 : 1;
                final long minTs = base + slot * rowsPerTxn;
                final long lo = row;
                for (int r = 0; r < rowsPerTxn; r++, row++) {
                    Unsafe.putLong(addr + row * 16, minTs + r * step);
                    Unsafe.putLong(addr + row * 16 + 8, row);
                }
                copyInfo.addTxn(lo, t, rowsPerTxn, s, minTs, minTs + (rowsPerTxn - 1) * step, true);
                minimaTemplate.add(minTs ^ Long.MIN_VALUE);
                minimaTemplate.add(runIndex++);
            }
            copyInfo.addSegment(0, s, 0, row, false);
        }
        minima.setCapacity(minimaTemplate.size());
        minima.setPos(minimaTemplate.size());
    }

    @Benchmark
    public long sortRunMinima() {
        Vect.memcpy(minima.getAddress(), minimaTemplate.getAddress(), minimaTemplate.size() * Long.BYTES);
        Vect.sortLongIndexAscInPlace(minima.getAddress(), minima.size() / 2);
        return minima.get(1);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        Unsafe.free(out, bufSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(cpy, bufSize, MemoryTag.NATIVE_DEFAULT);
        for (int s = 0; s < SEGMENT_COUNT; s++) {
            Unsafe.free(segmentAddresses.get(s), segmentSizes[s], MemoryTag.NATIVE_DEFAULT);
        }
        segmentAddresses.close();
        minima.close();
        minimaTemplate.close();
        copyInfo.close();
    }
}
