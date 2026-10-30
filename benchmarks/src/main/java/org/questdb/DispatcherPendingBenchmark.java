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

import io.questdb.std.ObjList;
import io.questdb.std.ObjLongMatrix;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * The dispatcher's work on its {@code pending} table for the events of one poll round,
 * single-threaded as in {@code IODispatcherLinux}. Each event looks up the connection's row by
 * its operation id and removes the row, and the worker's re-arm appends a new row, so the table
 * keeps {@code size} rows. One invocation is one round of {@code batch} events.
 * <ul>
 *     <li>{@link #perEvent}: each event removes its row with {@code deleteRow()}, which moves
 *     every later row down by one.</li>
 *     <li>{@link #batched}: each event marks its row with {@code markDeleted()}, and
 *     {@code deleteMarked()} removes all of them in one pass at the end of the round.</li>
 * </ul>
 * {@code front} fires the rows armed longest ago, as ping-pong traffic with similar round trips
 * does. {@code random} fires distinct, uniformly chosen rows in random order.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-XX:+UseParallelGC", "-Xms1g", "-Xmx1g"})
public class DispatcherPendingBenchmark {
    private static final int CONTEXT_COUNT = 1024;
    private static final int OPM_COLUMN_COUNT = 5;
    private static final int OPM_CREATE_TIMESTAMP = 0;
    private static final int OPM_FD = 1;
    private static final int OPM_HEARTBEAT_TIMESTAMP = 3;
    private static final int OPM_ID = 4;
    private static final int OPM_OPERATION = 2;
    private static final int PRECOMPUTED_ROUNDS = 4096;
    private final ObjList<Object> contexts = new ObjList<>(CONTEXT_COUNT);
    @Param({"1", "16", "64"})
    public int batch;
    @Param({"front", "random"})
    public String pattern;
    @Param({"256", "1024", "4096", "16384"})
    public int size;
    private long nextId;
    private ObjLongMatrix<Object> pending;
    private long[] polledIds;
    private int round;
    private int[][] rounds;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(DispatcherPendingBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public int batched() {
        final int n = pollRound();
        int sum = 0;
        for (int i = 0; i < n; i++) {
            final int row = pending.binarySearch(polledIds[i], OPM_ID);
            sum += row;
            pending.markDeleted(row);
        }
        pending.deleteMarked();
        for (int i = 0; i < n; i++) {
            appendRow();
        }
        return sum;
    }

    @Benchmark
    public int perEvent() {
        final int n = pollRound();
        int sum = 0;
        for (int i = 0; i < n; i++) {
            final int row = pending.binarySearch(polledIds[i], OPM_ID);
            sum += row;
            pending.deleteRow(row);
        }
        for (int i = 0; i < n; i++) {
            appendRow();
        }
        return sum;
    }

    @Setup
    public void setup() {
        for (int i = 0; i < CONTEXT_COUNT; i++) {
            contexts.add(new Object());
        }
        pending = new ObjLongMatrix<>(OPM_COLUMN_COUNT);
        for (nextId = 0; nextId < size; ) {
            appendRow();
        }
        polledIds = new long[batch];
        rounds = new int[PRECOMPUTED_ROUNDS][];
        final Rnd rnd = new Rnd();
        final boolean[] isTaken = new boolean[size];
        for (int i = 0; i < PRECOMPUTED_ROUNDS; i++) {
            final int[] rows = new int[batch];
            if ("front".equals(pattern)) {
                for (int j = 0; j < batch; j++) {
                    rows[j] = j;
                }
            } else {
                for (int j = 0; j < batch; j++) {
                    int row;
                    do {
                        row = rnd.nextInt(size);
                    } while (isTaken[row]);
                    isTaken[row] = true;
                    rows[j] = row;
                }
                for (int j = 0; j < batch; j++) {
                    isTaken[rows[j]] = false;
                }
            }
            rounds[i] = rows;
        }
    }

    private void appendRow() {
        final long opId = nextId++;
        final int r = pending.addRow();
        pending.set(r, OPM_CREATE_TIMESTAMP, opId);
        pending.set(r, OPM_HEARTBEAT_TIMESTAMP, opId);
        pending.set(r, OPM_FD, opId);
        pending.set(r, OPM_ID, opId);
        pending.set(r, OPM_OPERATION, 1);
        pending.set(r, contexts.getQuick((int) (opId & (CONTEXT_COUNT - 1))));
    }

    // the operation ids a poll round returns, the way epoll returns them
    private int pollRound() {
        final int[] rows = rounds[round++ & (PRECOMPUTED_ROUNDS - 1)];
        for (int i = 0; i < rows.length; i++) {
            polledIds[i] = pending.get(rows[i], OPM_ID);
        }
        return rows.length;
    }
}
