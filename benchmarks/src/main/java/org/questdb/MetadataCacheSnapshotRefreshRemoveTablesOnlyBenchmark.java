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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.CharSequenceObjSortedHashMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MetadataCacheSnapshotRefreshRemoveTablesOnlyBenchmark {
    public static final int CACHES_NUMBER_FOR_ITERATION = 10;
    private CairoEngine engine;
    private final CairoConfiguration configuration = new DefaultCairoConfiguration(".");

    @Param({"100", "1000", "10000"})
    public String size;

    private CharSequenceObjSortedHashMap<CairoTable>[] caches = new CharSequenceObjSortedHashMap[CACHES_NUMBER_FOR_ITERATION];

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MetadataCacheSnapshotRefreshRemoveTablesOnlyBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .warmupBatchSize(1)
                .measurementBatchSize(1)
                .operationsPerInvocation(CACHES_NUMBER_FOR_ITERATION)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void testCacheRefresh(Blackhole blackhole) {
        try (MetadataCacheReader metadataCacheReader = engine.getMetadataCache().readLock()) {
            for (int i = 0; i < CACHES_NUMBER_FOR_ITERATION; i++) {
                metadataCacheReader.snapshot(caches[i], -1);
                blackhole.consume(caches[i]);
            }
        }
    }

    @Setup(Level.Iteration)
    public void setup() throws SqlException, InterruptedException {
        engine = new CairoEngine(configuration);
        int max = Integer.parseInt(size);

        for (int i = max - 1; i > -1; i--) {
            execute("CREATE TABLE table" + i + " ( ts TIMESTAMP, x INT, y DOUBLE, z SYMBOL );");
        }

        try (MetadataCacheReader metadataCacheReader = engine.getMetadataCache().readLock()) {
            for (int i = 0; i < CACHES_NUMBER_FOR_ITERATION; i++) {
                CharSequenceObjSortedHashMap<CairoTable> cache = new CharSequenceObjSortedHashMap<>();
                metadataCacheReader.snapshot(cache, -1);
                caches[i] = cache;
            }
        }

        for (int i = 0; i < max; i++) {
            execute("drop table if exists table" + i);
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() throws SqlException, InterruptedException {
        int max = Integer.parseInt(size);
        for (int i = 0; i < max; i++) {
            execute("drop table if exists table" + i);
        }
        engine.close();
    }

    private void execute(String ddl) throws SqlException {
        SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                .with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );
        engine.execute(ddl, sqlExecutionContext);
    }
}
