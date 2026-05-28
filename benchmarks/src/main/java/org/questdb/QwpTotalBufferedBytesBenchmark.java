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

import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.CharSequenceObjHashMap;
import io.questdb.client.std.Misc;
import io.questdb.client.std.ObjList;
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
 * Microbench for the per-row bytes-tracking step inside
 * {@code QwpWebSocketSender.sendRow()}.
 * <p>
 * Today {@code sendRow()} sets {@code pendingBytes = totalBufferedBytes()} on
 * every call. {@code totalBufferedBytes()} walks {@code tableBuffers.keys()},
 * does a hashmap {@code get()} for each table, and sums per-column
 * {@code getBufferedBytes()} values. We want a number on what that walk
 * actually costs, since the two consumers of {@code pendingBytes} are guarded:
 * <ul>
 *   <li>the byte-based auto-flush check, gated by
 *       {@code effectiveAutoFlushBytes > 0};</li>
 *   <li>the server-cap pre-flight, only checked when
 *       {@code pendingRowCount == 1 && serverMaxBatchSize > 0}.</li>
 * </ul>
 * When neither gate is active the walk is dead work.
 * <p>
 * Three variants:
 * <ul>
 *   <li>{@link #walkAllTables} -- current behaviour.</li>
 *   <li>{@link #currentTableOnly} -- alternative: track the active table only
 *       (requires upstream delta bookkeeping to be correct).</li>
 *   <li>{@link #shortCircuit} -- proposed fix's cheap path: skip the walk when
 *       no gate is active.</li>
 * </ul>
 * Params sweep {@code numTables} and {@code numColumns} so the O(N{*}K)
 * scaling is visible.
 * <p>
 * Run from the benchmarks module:
 * <pre>
 *   mvn -pl benchmarks -P local-client -DskipTests package
 *   java -jar benchmarks/target/benchmarks.jar QwpTotalBufferedBytesBenchmark
 * </pre>
 * The {@code local-client} profile rebuilds the client from source so the
 * bench measures the {@code QwpTableBuffer} version sitting in this tree
 * rather than the Maven-cached release.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class QwpTotalBufferedBytesBenchmark {

    @Param({"1", "4", "16"})
    private int numTables;

    @Param({"1", "8"})
    private int numColumns;

    @Param({"100"})
    private int rowsPrePopulated;

    private QwpTableBuffer currentTableBuffer;
    private CharSequenceObjHashMap<QwpTableBuffer> tableBuffers;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QwpTotalBufferedBytesBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    /**
     * Alternative B: only the currently-active table's bytes are summed.
     * The sender would need to maintain {@code pendingBytes} as a running
     * delta updated by column setters and {@code nextRow()} for this to be
     * correct across multi-table interleaving; the bench just measures the
     * single-table {@code getBufferedBytes()} cost in isolation.
     */
    @Benchmark
    public long currentTableOnly() {
        return currentTableBuffer.getBufferedBytes();
    }

    @Setup(Level.Trial)
    public void setUp() {
        tableBuffers = new CharSequenceObjHashMap<>();
        for (int t = 0; t < numTables; t++) {
            String name = "tbl_" + t;
            QwpTableBuffer tb = new QwpTableBuffer(name);
            for (int r = 0; r < rowsPrePopulated; r++) {
                for (int c = 0; c < numColumns; c++) {
                    QwpTableBuffer.ColumnBuffer col =
                            tb.getOrCreateColumn("c" + c, QwpConstants.TYPE_LONG, true);
                    col.addLong((long) r * 31L + c);
                }
                tb.nextRow();
            }
            tableBuffers.put(name, tb);
        }
        currentTableBuffer = tableBuffers.get("tbl_0");
    }

    /**
     * Proposed cheap path: when neither byte-based auto-flush nor server cap
     * are active, {@code pendingBytes} is never read, so skip the walk.
     * Returns a constant; JMH still has to call the method.
     */
    @Benchmark
    public long shortCircuit() {
        return 0L;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (tableBuffers != null) {
            ObjList<CharSequence> keys = tableBuffers.keys();
            for (int i = 0, n = keys.size(); i < n; i++) {
                CharSequence k = keys.getQuick(i);
                if (k != null) {
                    Misc.free(tableBuffers.get(k));
                }
            }
            tableBuffers.clear();
            tableBuffers = null;
        }
        currentTableBuffer = null;
    }

    /**
     * Baseline: replicates {@code QwpWebSocketSender.totalBufferedBytes()}
     * exactly so the bench measures the same map walk + per-table column
     * loop the sender pays on every row.
     */
    @Benchmark
    public long walkAllTables() {
        long total = 0;
        ObjList<CharSequence> keys = tableBuffers.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence name = keys.getQuick(i);
            if (name == null) {
                continue;
            }
            QwpTableBuffer tb = tableBuffers.get(name);
            if (tb != null) {
                total += tb.getBufferedBytes();
            }
        }
        return total;
    }
}
