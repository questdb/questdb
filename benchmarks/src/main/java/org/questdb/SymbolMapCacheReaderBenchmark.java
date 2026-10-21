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
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Chars;
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
import org.openjdk.jmh.infra.ThreadParams;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * Run the same benchmark bytecode with baseline or candidate core classes first
 * on the classpath. Each worker owns a real engine-backed TableReader: a standalone
 * SymbolMapReaderImpl does not acquire the candidate's shared cache.
 * The stopped scenario is for candidate CACHE versus NOCACHE, not baseline CACHE.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {"--enable-native-access=ALL-UNNAMED", "--sun-misc-unsafe-memory-access=allow", "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"})
public class SymbolMapCacheReaderBenchmark {
    @Benchmark
    public int chain(LocalState local) {
        return local.nextKey = SymbolMapCacheBenchmarkData.nextKey(local.symbols.valueOf(local.keyBase + local.nextKey));
    }

    @Benchmark
    public int keys(LocalState local) {
        int key = local.keys[local.position++ & local.mask];
        return SymbolMapCacheBenchmarkData.nextKey(local.symbols.valueOf(local.keyBase + key));
    }

    @State(Scope.Thread)
    public static class LocalState {
        private int keyBase;
        private int[] keys;
        private int mask;
        private int nextKey;
        private int position;
        private SymbolMapReaderImpl symbols;
        private TableReader table;

        @Setup(Level.Trial)
        public void setup(ReaderState state, ThreadParams threads) throws Exception {
            table = state.engine.getReader("symbols");
            symbols = (SymbolMapReaderImpl) table.getSymbolMapReader(0);
            keyBase = state.keyBase;
            keys = state.data.keys;
            mask = keys.length - 1;
            position = threads.getThreadIndex() * 17 & mask;
            nextKey = keys[position];
            int cacheSize = SymbolMapCacheBenchmarkData.cacheSize(symbols);
            for (int i = 0; i < state.symbolCount; i++) {
                if (!Chars.equals(state.data.values.getQuick(i), symbols.valueOf(keyBase + i))) {
                    throw new AssertionError("symbol key or value changed: " + i);
                }
            }
            if (state.cacheMode.equals("CACHE")) {
                if (state.scenario.equals("hit") && SymbolMapCacheBenchmarkData.cacheSize(symbols) != state.symbolCount) {
                    throw new AssertionError("resident lookup is not fully cached");
                }
                if (state.scenario.equals("stopped") && (cacheSize == 0 || cacheSize >= keyBase || SymbolMapCacheBenchmarkData.cacheSize(symbols) != cacheSize)) {
                    throw new AssertionError("stopped lookup admitted measured keys");
                }
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            table = Misc.free(table);
            io.questdb.std.str.Path.clearThreadLocals();
        }
    }

    @State(Scope.Benchmark)
    public static class ReaderState {
        @Param({"CACHE"})
        public String cacheMode;
        @Param({"hit"})
        public String scenario;
        @Param({"64", "65536"})
        public int symbolCount;
        private SqlExecutionContextImpl context;
        private SymbolMapCacheBenchmarkData data;
        private CairoEngine engine;
        private int keyBase;
        private TableReader pinned;
        private Path root;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            if ((!cacheMode.equals("CACHE") && !cacheMode.equals("NOCACHE")) || (!scenario.equals("hit") && !scenario.equals("stopped"))) {
                throw new IllegalArgumentException("expected CACHE/NOCACHE and hit/stopped");
            }
            data = new SymbolMapCacheBenchmarkData(symbolCount);
            keyBase = scenario.equals("stopped") ? 20000 : 0;
            root = Files.createTempDirectory("symbol-cache-reader-");
            try {
                DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root.toString());
                engine = new CairoEngine(configuration);
                SymbolMapCacheBenchmarkData.setBudget(engine, keyBase == 0 ? 64 * 1024 * 1024 : 256 * 1024);
                context = new SqlExecutionContextImpl(engine, 1);
                context.with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
                engine.execute("create table symbols (s symbol " + cacheMode + ")", context);
                try (TableWriter writer = engine.getWriter(engine.verifyTableName("symbols"), "benchmark setup")) {
                    for (int i = 0; i < keyBase + symbolCount; i++) {
                        TableWriter.Row row = writer.newRow();
                        row.putSym(0, i < keyBase ? "cold" + i : data.values.getQuick(i - keyBase));
                        row.append();
                    }
                    writer.commit();
                }
                pinned = engine.getReader("symbols");
                // Prefill shared hits before workers race on the non-blocking insertion lock.
                int warmKeys = keyBase == 0 ? symbolCount : keyBase;
                for (int i = 0; i < warmKeys; i++) {
                    pinned.getSymbolMapReader(0).valueOf(i);
                }
            } catch (Exception | Error e) {
                tearDown();
                throw e;
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            pinned = Misc.free(pinned);
            context = Misc.free(context);
            engine = Misc.free(engine);
            io.questdb.std.str.Path.clearThreadLocals();
            if (root != null) {
                try (var paths = Files.walk(root)) {
                    for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                        Files.delete(path);
                    }
                }
                root = null;
            }
        }
    }
}
