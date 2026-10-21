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
import io.questdb.std.str.DirectString;
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
 * The stopped scenarios are for candidate CACHE versus NOCACHE, not baseline CACHE.
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
            try {
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
                if (state.hole != null) {
                    state.hole.check(symbols, state);
                }
            } catch (Exception | Error e) {
                try {
                    tearDown();
                } catch (Exception | Error cleanup) {
                    e.addSuppressed(cleanup);
                }
                throw e;
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            try (TableReader closeTable = table) {
                table = null;
            } finally {
                io.questdb.std.str.Path.clearThreadLocals();
            }
        }
    }

    // Reflection keeps the identical fixture loadable on the baseline, which has no native map.
    // Only stopped-hole uses these checks; no reflection or assertions enter keys()/chain().
    private static class HoleState {
        private static final String SENTINEL = "hole-anchor";
        private final long bytes;
        private final int count;
        private final int low;
        private final long lowBytes;
        private final Object map;
        private final SymbolMapReaderImpl pinned;

        private HoleState(SymbolMapReaderImpl pinned) throws Exception {
            this.pinned = pinned;
            map = field(pinned, "cache");
            count = pinned.getCacheSize();
            low = (int) call(pinned, "getLowCacheSize");
            bytes = map == null ? 0 : (long) call(map, "allocatedBytes");
            lowBytes = (long) call(pinned, "getLowCacheAllocatedBytes");
        }

        private static Object call(Object target, String name) throws Exception {
            return target.getClass().getMethod(name).invoke(target);
        }

        private static Object field(Object target, String name) throws Exception {
            for (Class<?> owner = target.getClass(); owner != null; owner = owner.getSuperclass()) {
                try {
                    var field = owner.getDeclaredField(name);
                    field.setAccessible(true);
                    return field.get(target);
                } catch (NoSuchFieldException ignored) {
                    // The map can declare padded fields in a superclass.
                }
            }
            throw new NoSuchFieldException(name);
        }

        private void check(SymbolMapReaderImpl reader, ReaderState state) throws Exception {
            int sentinel = state.keyBase + state.symbolCount;
            if (reader.getSymbolCount() != sentinel + 1 || reader.keyOf(SENTINEL) != sentinel
                    || !Chars.equals(SENTINEL, reader.valueOf(sentinel))) {
                throw new AssertionError("hole sentinel dictionary key/value changed");
            }
            if (field(reader, "cache") != map || field(pinned, "cache") != map) {
                throw new AssertionError("hole reader does not share the pinned map");
            }
            if (state.cacheMode.equals("CACHE")) {
                if (map == null || low != 4096 || count <= 1 || low + count >= state.keyBase
                        || bytes <= 0 || bytes > 256 * 1024 || lowBytes <= 0
                        || (boolean) call(map, "isFilling")
                        || (int) call(map, "getStopReason") != map.getClass().getField("STOP_BUDGET").getInt(null)
                        || (int) field(map, "minKey") != low || (int) field(map, "maxKey") != sentinel) {
                    throw new AssertionError("hole map did not stop at its budget with the intended bounds");
                }
                if (!field(reader, "lowOffsets").equals(field(pinned, "lowOffsets"))
                        || !field(reader, "lowCharsBase").equals(field(pinned, "lowCharsBase"))) {
                    throw new AssertionError("hole reader does not share the low slice");
                }
                var get = map.getClass().getMethod("get", int.class, DirectString.class);
                DirectString view = new DirectString();
                if (get.invoke(map, sentinel, view) == null || !Chars.equals(SENTINEL, view)) {
                    throw new AssertionError("hole map lost its sentinel hit");
                }
                // One native entry is the sentinel; all others must be the ascending cold prefix.
                for (int key = low; key < low + count - 1; key++) {
                    if (get.invoke(map, key, view) == null || !Chars.equals("cold" + key, view)) {
                        throw new AssertionError("hole map lost native prefix hit: " + key);
                    }
                }
                if (get.invoke(map, low + count - 1, view) != null) {
                    throw new AssertionError("hole native prefix has an unexpected extra hit");
                }
                int min = (int) field(map, "minKey");
                int max = (int) field(map, "maxKey");
                for (int key = state.keyBase; key < sentinel; key++) {
                    if (key <= min || key >= max || get.invoke(map, key, view) != null) {
                        throw new AssertionError("measured key is not an absent in-range hole: " + key);
                    }
                }
                for (int key = 0; key < low; key++) {
                    if (!Chars.equals("cold" + key, reader.valueOf(key))) {
                        throw new AssertionError("hole reader lost low-slice hit: " + key);
                    }
                }
            } else if (map != null || count != 0 || low != 0 || bytes != 0 || lowBytes != 0) {
                throw new AssertionError("NOCACHE hole reader acquired a cache");
            }
            for (int i = 0; i < state.symbolCount; i++) {
                int key = state.data.keys[i];
                CharSequence actual = reader.valueOf(state.keyBase + key);
                // The shuffled key cycle supplies the oracle independently of decoding the stored value.
                if (!Chars.equals(state.data.values.getQuick(key), actual)
                        || SymbolMapCacheBenchmarkData.nextKey(actual) != state.data.keys[(i + 1) & (state.symbolCount - 1)]) {
                    throw new AssertionError("hole value/next key changed: " + key);
                }
            }
            if (reader.getCacheSize() != count || (int) call(reader, "getLowCacheSize") != low
                    || (long) call(reader, "getLowCacheAllocatedBytes") != lowBytes
                    || (map != null && ((int) call(map, "size") != count || (long) call(map, "allocatedBytes") != bytes))) {
                throw new AssertionError("hole lookup changed retained count or allocated bytes");
            }
        }

        private void report(String phase, ReaderState state) throws Exception {
            System.out.printf(java.util.Locale.ROOT,
                    "SYMBOL_READER hole phase=%s mode=%s keys=%d base=%d sentinel=%d cached=%d low=%d bytes=%d lowBytes=%d min=%d max=%d shared=%b filling=%b stopReason=%d verified=%d%n",
                    phase, state.cacheMode, state.symbolCount, state.keyBase, state.keyBase + state.symbolCount,
                    count, low, bytes, lowBytes, map == null ? -1 : (int) field(map, "minKey"),
                    map == null ? -1 : (int) field(map, "maxKey"), map != null,
                    map != null && (boolean) call(map, "isFilling"), map == null ? 0 : (int) call(map, "getStopReason"), state.symbolCount);
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
        private HoleState hole;
        private boolean isSetupComplete;
        private int keyBase;
        private TableReader pinned;
        private Path root;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            boolean isHole = scenario.equals("stopped-hole");
            if ((!cacheMode.equals("CACHE") && !cacheMode.equals("NOCACHE"))
                    || (!scenario.equals("hit") && !scenario.equals("stopped") && !isHole)
                    || (isHole && symbolCount != 65_536)) {
                throw new IllegalArgumentException("expected CACHE/NOCACHE and hit/stopped/stopped-hole (65536 keys)");
            }
            data = new SymbolMapCacheBenchmarkData(symbolCount);
            keyBase = scenario.equals("hit") ? 0 : 20_000;
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
                    if (isHole) {
                        TableWriter.Row row = writer.newRow();
                        row.putSym(0, HoleState.SENTINEL);
                        row.append();
                    }
                    writer.commit();
                }
                pinned = engine.getReader("symbols");
                if (isHole) {
                    pinned.getSymbolMapReader(0).valueOf(0);
                    pinned.getSymbolMapReader(0).valueOf(4095);
                    pinned.getSymbolMapReader(0).valueOf(keyBase + symbolCount);
                }
                // Prefill shared hits before workers race on the non-blocking insertion lock.
                int warmKeys = keyBase == 0 ? symbolCount : keyBase;
                for (int i = 0; i < warmKeys; i++) {
                    pinned.getSymbolMapReader(0).valueOf(i);
                    if (isHole && cacheMode.equals("CACHE")
                            && !(boolean) HoleState.call(HoleState.field(pinned.getSymbolMapReader(0), "cache"), "isFilling")) {
                        break;
                    }
                }
                if (isHole) {
                    hole = new HoleState((SymbolMapReaderImpl) pinned.getSymbolMapReader(0));
                    hole.check(hole.pinned, this);
                    hole.report("prepared", this);
                }
                isSetupComplete = true;
            } catch (Exception | Error e) {
                try {
                    tearDown();
                } catch (Exception | Error cleanup) {
                    e.addSuppressed(cleanup);
                }
                throw e;
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            Throwable failure = null;
            // Reverse-order resource close also runs when a setup/check fails; retain that first failure.
            try (CairoEngine closeEngine = engine;
                 SqlExecutionContextImpl closeContext = context;
                 TableReader closePinned = pinned) {
                engine = null;
                context = null;
                pinned = null;
                if (isSetupComplete && hole != null) {
                    hole.check(hole.pinned, this);
                    hole.report("finished", this);
                }
            } catch (Exception | Error e) {
                failure = e;
            } finally {
                isSetupComplete = false;
                hole = null;
                io.questdb.std.str.Path.clearThreadLocals();
            }
            try {
                if (root != null) {
                    try (var paths = Files.walk(root)) {
                        for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                            Files.delete(path);
                        }
                    }
                    root = null;
                }
            } catch (Exception | Error e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
            if (failure instanceof Exception e) {
                throw e;
            }
            if (failure instanceof Error e) {
                throw e;
            }
        }
    }
}
