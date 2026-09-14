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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.foreign.SymbolLookup;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end SQL benchmark for the {@code ~} regex operator over a 1,000,000-row VARCHAR column,
 * comparing the {@code old} JDK {@code java.util.regex} engine against the {@code new} native
 * (Rust {@code regex} crate via JDK 25 FFM) backend.
 * <p>
 * Both arms run the same query, {@code select count(*) from vt where name ~ '<pattern>'}, over the
 * same on-disk table (created once, deterministically, in {@link #main}). The only difference is
 * the per-engine flag {@code cairo.sql.varchar.regex.native.enabled}, read by
 * {@code MatchVarcharFunctionFactory} at query-compile time.
 * <p>
 * The count query forces the predicate to be evaluated on every row, so the measurement is
 * dominated by regex matching (plus, for the JDK arm, the UTF-8&rarr;UTF-16 transcode that the
 * {@code getStrA} path performs per row — which the native UTF-8 path avoids).
 * <p>
 * NOTE: for the {@code NATIVE} arm to actually use the native engine, {@code libquestdbr} must be
 * built with the {@code qdb_regex_*} symbols; otherwise the factory silently falls back to JDK and
 * the two arms will measure the same thing. {@link #main} prints which case applies.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = {"--enable-native-access=ALL-UNNAMED", "--enable-native-access=io.questdb.benchmarks"})
public class MatchVarcharRegexQueryBenchmark {

    private static final int NUM_ROWS = 1_000_000;
    private static final String TABLE = "vt";
    private static final String TMP = System.getProperty("java.io.tmpdir");

    @Param({"JDK", "NATIVE"})
    public Backend backend;
    @Param({"abc", "[a-f][0-9]", "[a-z]+[0-9]+[a-z]+"})
    public String pattern;

    private SqlCompilerImpl compiler;
    private RecordCursorFactory countFactory;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;

    public static void main(String[] args) throws RunnerException {
        System.out.println("native regex backend available: " + nativeRegexAvailable());

        // Build the 1M-row table once. It lives on disk under java.io.tmpdir, so the forked
        // benchmark JVM sees it. Seeded RNG keeps the data identical across runs.
        final CairoConfiguration cfg = config(false);
        try (CairoEngine engine = new CairoEngine(cfg)) {
            final SqlExecutionContextImpl ctx = newCtx(engine, cfg);
            ctx.setRandom(new Rnd(0x4d4154434856L, 0x5243484152L));
            engine.execute("drop table if exists " + TABLE, ctx);
            engine.execute(
                    "create table " + TABLE + " as (select rnd_varchar(8, 32, 20) name from long_sequence(" + NUM_ROWS + "))",
                    ctx
            );
        } catch (SqlException e) {
            e.printStackTrace(System.out);
        }

        Options opt = new OptionsBuilder()
                .include(MatchVarcharRegexQueryBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(10)
                .build();
        new Runner(opt).run();

        LogFactory.haltInstance();
    }

    @Benchmark
    public long countMatches() throws SqlException {
        long count = 0;
        try (RecordCursor cursor = countFactory.getCursor(ctx)) {
            if (cursor.hasNext()) {
                count = cursor.getRecord().getLong(0);
            }
        }
        return count;
    }

    @Setup(Level.Iteration)
    public void setup() throws Exception {
        final CairoConfiguration cfg = config(backend == Backend.NATIVE);
        engine = new CairoEngine(cfg);
        ctx = newCtx(engine, cfg);
        compiler = new SqlCompilerImpl(engine);
        final String q = "select count(*) from " + TABLE + " where name ~ '" + pattern + "'";
        countFactory = compiler.compile(q, ctx).getRecordCursorFactory();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        countFactory.close();
        compiler.close();
        engine.close();
    }

    private static CairoConfiguration config(boolean nativeRegex) {
        return new DefaultCairoConfiguration(TMP) {
            @Override
            public boolean isVarcharRegexNativeEnabled() {
                return nativeRegex;
            }
        };
    }

    private static boolean nativeRegexAvailable() {
        try {
            Os.init();
            return SymbolLookup.loaderLookup().find("qdb_regex_compile").isPresent();
        } catch (Throwable t) {
            return false;
        }
    }

    private static SqlExecutionContextImpl newCtx(CairoEngine engine, CairoConfiguration cfg) {
        final SqlExecutionContextImpl c = new SqlExecutionContextImpl(engine, 1);
        c.with(cfg.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        return c;
    }

    public enum Backend {
        JDK, NATIVE
    }
}
