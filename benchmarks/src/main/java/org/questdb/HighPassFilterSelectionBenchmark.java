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
import org.openjdk.jmh.annotations.Warmup;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Measures only the selection-vector production and consumption cost for a
 * high-pass page-frame filter. It establishes the ceiling for replacing the
 * current accepted-row list with rejected rows, a rejection bitmap, or runs;
 * it does not include predicate evaluation or covered-column decoding.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class HighPassFilterSelectionBenchmark {

    @Param({"uniform", "clustered"})
    public String distribution;

    @Param({"90", "95", "99", "99.9", "100"})
    public double passPercent;

    @Param({"100000"})
    public int rowCount;

    private boolean[] accepted;
    private long[] bitmap;
    private long[] excludeRows;
    private long[] includeRows;
    private long[] runs;

    @Benchmark
    public long acceptedRuns() {
        int runCount = 0;
        int row = 0;
        while (row < rowCount) {
            while (row < rowCount && !accepted[row]) {
                row++;
            }
            final int lo = row;
            while (row < rowCount && accepted[row]) {
                row++;
            }
            if (lo < row) {
                runs[2 * runCount] = lo;
                runs[2 * runCount + 1] = row;
                runCount++;
            }
        }

        long checksum = 0;
        for (int i = 0; i < runCount; i++) {
            for (long r = runs[2 * i], hi = runs[2 * i + 1]; r < hi; r++) {
                checksum = checksum * 31 + r;
            }
        }
        return checksum;
    }

    @Benchmark
    public long excludeList() {
        int rejectedCount = 0;
        for (int row = 0; row < rowCount; row++) {
            if (!accepted[row]) {
                excludeRows[rejectedCount++] = row;
            }
        }

        long checksum = 0;
        int rejectedIndex = 0;
        long nextRejected = rejectedCount > 0 ? excludeRows[0] : Long.MAX_VALUE;
        for (int row = 0; row < rowCount; row++) {
            if (row == nextRejected) {
                rejectedIndex++;
                nextRejected = rejectedIndex < rejectedCount ? excludeRows[rejectedIndex] : Long.MAX_VALUE;
            } else {
                checksum = checksum * 31 + row;
            }
        }
        return checksum;
    }

    @Benchmark
    public long includeList() {
        int acceptedCount = 0;
        for (int row = 0; row < rowCount; row++) {
            if (accepted[row]) {
                includeRows[acceptedCount++] = row;
            }
        }

        long checksum = 0;
        for (int i = 0; i < acceptedCount; i++) {
            checksum = checksum * 31 + includeRows[i];
        }
        return checksum;
    }

    @Benchmark
    public long rejectionBitmap() {
        Arrays.fill(bitmap, 0);
        for (int row = 0; row < rowCount; row++) {
            if (!accepted[row]) {
                bitmap[row >>> 6] |= 1L << (row & 63);
            }
        }

        long checksum = 0;
        for (int wordIndex = 0, n = bitmap.length; wordIndex < n; wordIndex++) {
            long acceptedBits = ~bitmap[wordIndex];
            if (wordIndex == n - 1 && (rowCount & 63) != 0) {
                acceptedBits &= (1L << (rowCount & 63)) - 1;
            }
            while (acceptedBits != 0) {
                final int bit = Long.numberOfTrailingZeros(acceptedBits);
                checksum = checksum * 31 + ((long) wordIndex << 6) + bit;
                acceptedBits &= acceptedBits - 1;
            }
        }
        return checksum;
    }

    @Setup(Level.Trial)
    public void setUp() {
        accepted = new boolean[rowCount];
        Arrays.fill(accepted, true);
        bitmap = new long[(rowCount + 63) >>> 6];
        excludeRows = new long[rowCount];
        includeRows = new long[rowCount];
        runs = new long[2 * rowCount];

        final int rejectedCount = (int) Math.round(rowCount * (100.0 - passPercent) / 100.0);
        if ("clustered".equals(distribution)) {
            final int lo = (rowCount - rejectedCount) / 2;
            Arrays.fill(accepted, lo, lo + rejectedCount, false);
        } else {
            for (int i = 0; i < rejectedCount; i++) {
                accepted[(int) ((long) i * rowCount / rejectedCount)] = false;
            }
        }

        final long expectedChecksum = includeList();
        if (excludeList() != expectedChecksum
                || rejectionBitmap() != expectedChecksum
                || acceptedRuns() != expectedChecksum) {
            throw new IllegalStateException("selection representations returned different rows");
        }
    }
}
