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

package org.questdb;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.QdbrWalLocker;
import io.questdb.cairo.wal.WalLocker;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark to measure allocation behavior of WalLocker operations.
 * <p>
 * The goal is to verify that steady-state lock/unlock operations do not allocate
 * because WalEntry objects are reused from an internal pool.
 * <p>
 * Run with: java -jar benchmarks/target/benchmarks.jar WalLockerBenchmark
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class WalLockerBenchmark {
    private static final int NUM_TABLES = 10;
    private static final int NUM_WALS = 5;
    private FilesFacade filesFacade;
    private WalLocker locker;
    private Path path;
    private int pathLen;
    private int segmentId;
    private int tableIndex;
    private TableToken[] tables;
    private java.nio.file.Path tempDir;
    private int walIndex;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(WalLockerBenchmark.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .build();

        new Runner(opt).run();
    }

    @Setup
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("wal-lock-benchmark");
        path = new Path();
        path.of(tempDir.toString());
        pathLen = path.size();
        filesFacade = FilesFacadeImpl.INSTANCE;
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        tableIndex = (tableIndex + 1) % NUM_TABLES;
        walIndex = (walIndex + 1) % NUM_WALS;
        segmentId = (segmentId + 1) % 100;
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        locker = new QdbrWalLocker();
        tables = new TableToken[NUM_TABLES];
        for (int i = 0; i < NUM_TABLES; i++) {
            String name = "table_" + i;
            tables[i] = new TableToken(name, name, null, i, false, false, false);
        }
    }

    @TearDown
    public void tearDown() throws IOException {
        try (java.util.stream.Stream<java.nio.file.Path> paths = Files.walk(tempDir)) {
            paths.sorted(java.util.Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEachOrdered(java.io.File::delete);
        }
        Files.deleteIfExists(tempDir);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        locker.close();
        path.close();
    }

    @Benchmark
    public void testWriterFileLockUnlock(Blackhole bh) {
        TableToken table = tables[tableIndex];
        path.trimTo(pathLen).concat(table.getDirNameUtf8());
        final LPSZ lpsz = TableUtils.lockName(path);
        long fd = TableUtils.lock(filesFacade, lpsz);
        bh.consume(fd);
        filesFacade.close(fd);
    }

    @Benchmark
    public void testWriterLockUnlock() {
        TableToken table = tables[tableIndex];
        locker.lockWriter(table, walIndex, segmentId);
        locker.unlockWriter(table, walIndex);
    }
}
