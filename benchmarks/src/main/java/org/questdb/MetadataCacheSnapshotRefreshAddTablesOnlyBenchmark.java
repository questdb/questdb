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
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
public class MetadataCacheSnapshotRefreshAddTablesOnlyBenchmark {
    public static final int CACHES_NUMBER_FOR_ITERATION = 10;
    private final CairoConfiguration configuration = new DefaultCairoConfiguration(".");
    private CharSequenceObjSortedHashMap<CairoTable>[] caches = new CharSequenceObjSortedHashMap[CACHES_NUMBER_FOR_ITERATION];

    @Param({"100", "1000", "10000"})
    public String size;
    private CairoEngine engine;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MetadataCacheSnapshotRefreshAddTablesOnlyBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .warmupBatchSize(1)
                .warmupTime(TimeValue.seconds(30))
                .measurementTime(TimeValue.seconds(30))
                .measurementBatchSize(1)
                .measurementIterations(1)
                .operationsPerInvocation(CACHES_NUMBER_FOR_ITERATION)
                .forks(1)
                .jvmArgs("-Xms2G", "-Xmx2G")
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() throws SqlException {
        engine = new CairoEngine(configuration);
        int max = Integer.parseInt(size);
        for (int i = 0; i < max; i++) {
            execute("CREATE TABLE table" + i + " ( ts TIMESTAMP, x INT, y DOUBLE, z SYMBOL );");
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws SqlException {
        int max = Integer.parseInt(size);
        for (int i = 0; i < max; i++) {
            execute("drop table table" + i);
        }
        engine.close();
    }

    @Benchmark
    public void testCacheRefresh(Blackhole blackhole) {
        for (int i = 0; i < CACHES_NUMBER_FOR_ITERATION; i++) {
            try (MetadataCacheReader metadataCacheReader = engine.getMetadataCache().readLock()) {
                CharSequenceObjSortedHashMap<CairoTable> cache = new CharSequenceObjSortedHashMap<>();
                metadataCacheReader.snapshot(cache, -1);
                caches[i] = cache;
            }
        }
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
