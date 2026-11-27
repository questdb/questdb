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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * Times one WAL apply, rebuilding the pre-replay column-top layout outside every timed invocation.
 * The mixed-top baseline commits an incorrect duplicate; compare controls for timing neutrality.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {
        "-ea", "--sun-misc-unsafe-memory-access=allow", "--enable-native-access=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
})
public class ParquetDedupReplayBenchmark {
    @Param({"4", "100000"})
    public int groupSize;
    @Param({"BOOLEAN", "BYTE", "SHORT", "CHAR"})
    public String keyType;
    @Param({"mixedTop", "noTop", "allNullRange", "allNullChunk"})
    public String shape;
    private ApplyWal2TableJob applyJob;
    private CheckWalTransactionsJob checkJob;
    private SqlExecutionContextImpl context;
    private CairoEngine engine;
    private Path root;

    @Benchmark
    public void applyReplay() {
        drain();
    }

    @Setup(Level.Invocation)
    public void prepareReplay() throws Exception {
        root = Files.createTempDirectory("parquet-dedup-replay-");
        engine = new CairoEngine(new DefaultCairoConfiguration(root.toString()) {
            @Override
            public int getPartitionEncoderParquetRowGroupSize() {
                return groupSize;
            }
        });
        engine.load();
        context = new SqlExecutionContextImpl(engine, 1).with(engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext());
        applyJob = new ApplyWal2TableJob(engine, 0);
        checkJob = new CheckWalTransactionsJob(engine);
        boolean hasTop = !shape.equals("noTop");
        boolean isSingleChunk = shape.equals("allNullChunk");
        int groups = isSingleChunk ? 1 : 3;
        String key = keyType.equals("BOOLEAN") ? "true" : (keyType.equals("CHAR") ? "69" : "5") + "::" + keyType;
        execute("CREATE TABLE t (ts TIMESTAMP, x INT" + (hasTop ? "" : ",b " + keyType) + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO t (ts,x) SELECT CASE WHEN x=1 THEN '2024-01-01T00:00'::TIMESTAMP ELSE '2024-01-01T00:03'::TIMESTAMP END, (x-1)::INT FROM long_sequence(" + groups * groupSize + ")");
        if (hasTop) {
            execute("ALTER TABLE t ADD COLUMN b " + keyType);
        }
        execute("ALTER TABLE t DEDUP ENABLE UPSERT KEYS(ts,b)");
        execute("INSERT INTO t VALUES ('2024-01-02',900_000," + key + ")");
        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        boolean hasFirstDelivery = shape.equals("mixedTop") || shape.equals("noTop");
        if (hasFirstDelivery) {
            execute("INSERT INTO t VALUES ('2024-01-01T00:01',500_000," + key + ")");
        }
        try (TableReader reader = engine.getReader("t")) {
            reader.openPartition(0);
            ParquetMetaFileReader meta = reader.getAndInitParquetPartitionDecoder(0).metadata();
            if (meta.getRowGroupCount() != groups
                    || meta.getRowGroupSize(0) != groupSize + (hasFirstDelivery ? 1 : 0)
                    || meta.getChunkNullCount(0, 2) != (hasTop && !hasFirstDelivery ? groupSize : 0)) {
                throw new AssertionError("benchmark lost its pre-replay layout");
            }
        }
        engine.execute("INSERT INTO t VALUES ('2024-01-01T00:01',500_000," + key + "), ('2024-01-01T00:03',600_000," + key + ")", context);
    }

    @Setup(Level.Trial)
    public void quietLogs() {
        LogFactory.init();
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws Exception {
        applyJob = Misc.free(applyJob);
        checkJob = Misc.free(checkJob);
        context = Misc.free(context);
        engine = Misc.free(engine);
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    private void drain() {
        applyJob.drain(0);
        checkJob.run();
        applyJob.drain(0);
    }

    private void execute(String sql) throws Exception {
        engine.execute(sql, context);
        drain();
    }
}
