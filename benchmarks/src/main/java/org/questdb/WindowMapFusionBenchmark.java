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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowMapGroups;
import io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowAccumulatorDescriptor;
import io.questdb.griffin.engine.window.WindowAccumulatorPlan;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowMapState;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;

/**
 * The ordinary-window counterpart of {@link LiveViewSteadyStateBenchmark}: what one streaming
 * {@code PARTITION BY} window query costs per row with the window Map group bound and with every
 * function on a private map, at both configured
 * {@code cairo.sql.unordered.map.max.entry.size} settings that ship.
 * <p>
 * Every arm reports the numbers the acceptance plan asks for - ns/row, the query's peak and
 * retained <b>tracked</b> native bytes, how many maps were open and of which implementation, the
 * configured entry-size limit, and the per-row lookup, accumulator-update and argument-evaluation
 * counts - so a win can be read as a structural fact rather than inferred from a clock.
 *
 * <h2>The two arms</h2>
 * {@code cairo.sql.window.map.fusion.enabled} is the control. With it off the query compiles the
 * same group and binds none of it, so every function keeps and probes the map it has always had;
 * with it on the group owns one map and makes the row's one lookup. The two arms run the same SQL
 * over the same table in the same JVM, and their output checksums must be equal - a mismatch fails
 * the run rather than being reported, because a faster wrong answer is not a measurement.
 *
 * <h2>Where the counters come from</h2>
 * The fused arm's lookup and update counts are {@link WindowMapState}'s own
 * {@code @TestOnly} counters. The unfused arm has none to read: a private partition map is probed
 * inside each function's {@code computeNext} and nothing counts it. Its numbers are therefore
 * structural - one lookup, one accumulator update and one argument evaluation per row per function
 * that owns an open map - which is exact for the families this benchmark drives, each of which
 * probes once and reads its argument once per row. The same derivation covers a residual function
 * sitting beside a bound group. Adding production counters to the private path for a measurement is
 * what step 3.2 declined to do for its own rule, and the same reasoning holds here.
 *
 * <h2>Cases 4 and 12: fusing across a Map-implementation change</h2>
 * Co-location widens the value and can push a group onto an {@code OrderedMap} that every member
 * would have kept an {@code Unordered4Map} for. Two shapes measure that trade, one at each shipped
 * entry-size limit, and the switch alone reaches both arms of it:
 * <ul>
 *     <li>case 4, {@code count(x) + count(y)} over an INT key at {@code --entry-size=16}: two
 *     {@code Unordered4Map}s at {@code 4 + 8 = 12} each unfused, one {@code OrderedMap} at
 *     {@code 4 + 16 = 20} fused;</li>
 *     <li>case 12, {@code sum(x) + sum(y)} at {@code --entry-size=32}: two {@code Unordered4Map}s
 *     at {@code 4 + 16 = 20} each unfused, one {@code OrderedMap} at {@code 4 + 32 = 36} fused.
 *     Same trade at the limit a server defaults to.</li>
 * </ul>
 * A Map-implementation decline rule used to refuse exactly these two, so reaching them fused
 * needed a second limit ({@code --entry-size=11}, where each member is over the limit too and the
 * rule did not fire). The rule is gone - fusing made case 4 2.0x faster and case 12 2.8x faster
 * over 1e6 INT keys - and the limit-11 arm remains useful for one thing only: run
 * {@code --shape=count-count --entry-size=11} and both arms are {@code OrderedMap}s, which
 * separates the saving fusion makes from the cost of the implementation change.
 * <p>
 * A VARCHAR key is the third implementation the trade can cross, {@code UnorderedVarcharMap} at
 * {@code 16 + valueSize}: {@code --shape=sum-count --key-type=varchar --entry-size=32} is one
 * {@code OrderedMap} fused against two {@code UnorderedVarcharMap}s unfused.
 *
 * <h2>The bounded-frame shapes</h2>
 * {@code --shape=rows-frame-sum-avg}, {@code rows-frame-sum-avg-count} and their
 * {@code range-frame-*} counterparts are the ring-backed shapes, and they are the only ones here
 * whose state is not all in a map value: a bounded accumulator keeps the frame's own values in an
 * arena its contributing function owns. So the unfused arm allocates one ring per call and the
 * fused arm one per component, and the fused value is four slots per bounded ROWS
 * {@code (sum, count)} and six per bounded RANGE one - wide enough that every configured entry-size
 * limit puts the group on an {@code OrderedMap} while a narrow key's unfused members may sit on an
 * {@code Unordered4Map}. That trade is cases 4 and 12's, measured there; what these add is the ring,
 * which the retained-bytes column counts because the arena is tracked like the maps are.
 * <p>
 * The RANGE pair prices what a resizable ring costs on top of that: its cells are
 * {@code (timestamp, value)} pairs rather than bare values, and its span is expressed in the
 * table's own timestamp step so that it holds the same rows the ROWS pair's does. It has no ordered
 * cached arm - a bounded RANGE frame is compiled only where the window's order was dismissed
 * against the base cursor - so {@code --cached-bucket=ordered} still forces its cached run with the
 * residual whole-partition call.
 *
 * <h2>Case 11: the cached cursors</h2>
 * {@code --cursor=cached} and {@code --cursor=cached-light} run the same shapes through
 * {@code CachedWindowRecordCursorFactory} and {@code CachedWindowLightRecordCursorFactory}, where
 * the rows reach a function through a materialized record chain rather than off the base cursor.
 * The arm asserts which factory the query landed on rather than assuming it.
 * <p>
 * A cumulative shape does not reach a cached cursor on its own - it is exactly what the streaming
 * fast path is for - so the run has to force it, and {@code --cached-bucket} says how:
 * <ul>
 *     <li>{@code natural} (the default) adds {@code avg(x) over (partition by k)} to the SELECT
 *     list. A whole-partition avg is a two-pass function, which the fast path declines on, and it
 *     is then a residual sitting beside the group with a map and two probes a row of its own -
 *     charged to both arms equally. The group's own window is one the base cursor's order already
 *     satisfies, so it is traversed with the scan that fills the chain;</li>
 *     <li>{@code ordered} writes the window {@code order by ts desc} instead. The SELECT list is
 *     then the streaming one exactly and the forcing is the sort, which every arm pays for and
 *     which is large enough at these row counts to compress the ratio between them.</li>
 * </ul>
 * The four {@code partition-*} shapes need neither: they are whole-partition two-pass functions
 * and so are cached by construction. Three of them fuse - a whole-partition {@code sum},
 * {@code avg} and {@code count} are three readings of one {@code (sum, nonNullCount)} pair, so
 * {@code partition-sum-avg-count} is three maps and six probes a row unfused against one map and
 * two fused - and {@code partition-avg} does not, because one fusible function is not a group. It
 * is the map work a fused {@code partition-sum-avg-count} is left with, which is what makes it that
 * shape's floor.
 *
 * <h2>{@code --null-pct}: the refused-row population</h2>
 * A whole-partition group's pass 1 does map work for a row before its contributors decide the row
 * was never their business, and a group whose components can all be predicated skips that work for
 * a row every one of them refuses - see {@code WindowMapState.isPass1SkipEnabled}. What decides
 * whether that skip pays is how often the argument is absent, which is what this option varies:
 * {@code --null-pct=0,50,90,99,100} makes {@code x} NULL on that percentage of rows and {@code y}
 * NULL on the same percentage offset by 37, so a two-argument group has to refuse both before it
 * skips anything. The {@code skips/row} column is where the answer is legible; a shape with no
 * skippable group reports zero there in both arms.
 *
 * <h2>Build and run</h2>
 * <pre>
 * mvn -pl benchmarks -am package -o -DskipTests -Dmaven.test.skip=true
 *
 * # the whole acceptance matrix at both shipped entry-size settings
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED -Xmx8g \
 *     -cp benchmarks/target/benchmarks.jar \
 *     org.questdb.WindowMapFusionBenchmark
 *
 * # one shape, one key type, the transition measurement of case 4
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED -Xmx8g \
 *     -cp benchmarks/target/benchmarks.jar \
 *     org.questdb.WindowMapFusionBenchmark \
 *     --shape=count-count --key-type=int --entry-size=11,16
 *
 * # case 11: both cached factories, including the two-pass shapes
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED -Xmx8g \
 *     -cp benchmarks/target/benchmarks.jar \
 *     org.questdb.WindowMapFusionBenchmark \
 *     --cursor=cached,cached-light --key-type=int --entry-size=32
 * </pre>
 */
public class WindowMapFusionBenchmark {

    /**
     * How many preceding rows the two {@code rows-frame-*} shapes span. Small deliberately: the
     * ring the ring-backed families keep is one cell per row of the frame per key, so a wide frame
     * would price the arena rather than the group.
     */
    private static final int BOUNDED_ROWS_PRECEDING = 8;
    private static final long START_TS = 1_704_067_200_000_000L;
    private static final long TS_STEP_MICROS = 1_000L;

    public static void main(String[] args) throws Exception {
        long rows = 2_000_000L;
        String keysArg = "1000,1000000";
        String keyTypesArg = "int,symbol,string,varchar";
        String entrySizesArg = "16,32";
        String shapesArg = "all";
        String fusionArg = "both";
        String cursorsArg = "streaming";
        String cachedBucketArg = "natural";
        String nullPctArg = "0";
        int warmups = 1;
        int runs = 3;
        for (String arg : args) {
            if (arg.startsWith("--rows=")) {
                rows = Long.parseLong(arg.substring(7));
            } else if (arg.startsWith("--keys=")) {
                keysArg = arg.substring(7);
            } else if (arg.startsWith("--key-type=")) {
                keyTypesArg = arg.substring(11);
            } else if (arg.startsWith("--entry-size=")) {
                entrySizesArg = arg.substring(13);
            } else if (arg.startsWith("--shape=")) {
                shapesArg = arg.substring(8);
            } else if (arg.startsWith("--fusion=")) {
                fusionArg = arg.substring(9);
            } else if (arg.startsWith("--cursor=")) {
                cursorsArg = arg.substring(9);
            } else if (arg.startsWith("--cached-bucket=")) {
                cachedBucketArg = arg.substring(16);
            } else if (arg.startsWith("--null-pct=")) {
                nullPctArg = arg.substring(11);
            } else if (arg.startsWith("--warmups=")) {
                warmups = Integer.parseInt(arg.substring(10));
            } else if (arg.startsWith("--runs=")) {
                runs = Integer.parseInt(arg.substring(7));
            } else {
                throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }
        if (runs < 1) {
            throw new IllegalArgumentException("--runs must be at least 1: " + runs);
        }

        final List<Long> cardinalities = parseLongs(keysArg, "--keys");
        final List<Integer> entrySizes = parseInts(entrySizesArg, "--entry-size");
        final List<Integer> nullPercentages = parseInts(nullPctArg, "--null-pct");
        for (int nullPct : nullPercentages) {
            if (nullPct < 0 || nullPct > 100) {
                throw new IllegalArgumentException("--null-pct must be between 0 and 100: " + nullPct);
            }
        }
        final List<KeyType> keyTypes = new ArrayList<>();
        for (String name : keyTypesArg.split(",")) {
            keyTypes.add(KeyType.of(name.trim()));
        }
        final List<Shape> shapes = new ArrayList<>();
        if ("all".equals(shapesArg)) {
            Collections.addAll(shapes, Shape.values());
        } else {
            for (String name : shapesArg.split(",")) {
                shapes.add(Shape.of(name.trim()));
            }
        }
        final boolean[] fusionSettings = switch (fusionArg) {
            case "both" -> new boolean[]{true, false};
            case "on" -> new boolean[]{true};
            case "off" -> new boolean[]{false};
            default -> throw new IllegalArgumentException("--fusion must be one of both, on, off: " + fusionArg);
        };
        final List<Cursor> cursors = new ArrayList<>();
        if ("all".equals(cursorsArg)) {
            Collections.addAll(cursors, Cursor.values());
        } else {
            for (String name : cursorsArg.split(",")) {
                cursors.add(Cursor.of(name.trim()));
            }
        }
        final boolean orderedBucket = switch (cachedBucketArg) {
            case "natural" -> false;
            case "ordered" -> true;
            default -> throw new IllegalArgumentException(
                    "--cached-bucket must be one of natural, ordered: " + cachedBucketArg
            );
        };

        Os.init();
        final Path dbRoot = Files.createTempDirectory("window-map-fusion-");
        CairoEngine engine = null;
        try {
            final BenchmarkConfiguration configuration = new BenchmarkConfiguration(dbRoot.toString());
            engine = new CairoEngine(configuration);
            engine.load();
            final SqlExecutionContextImpl sqlCtx = new SqlExecutionContextImpl(engine, 1).with(
                    AllowAllSecurityContext.INSTANCE, null, null, -1, null
            );

            System.out.printf(
                    Locale.ROOT,
                    "# rows=%d keys=%s keyTypes=%s entrySizes=%s nullPct=%s shapes=%s fusion=%s"
                            + " cursors=%s cachedBucket=%s warmups=%d runs=%d%n",
                    rows, keysArg, keyTypesArg, entrySizesArg, nullPctArg, shapesArg, fusionArg,
                    cursorsArg, cachedBucketArg, warmups, runs
            );

            for (KeyType keyType : keyTypes) {
                for (long keys : cardinalities) {
                    for (int nullPct : nullPercentages) {
                        engine.execute(createTableSql(keyType, keys, rows, nullPct), sqlCtx);
                    }
                }
            }

            final List<String> table = new ArrayList<>();
            table.add("shape\tcursor\tkey\tkeys\tnullPct\tmaxEntry\tfusion\tplans\tgroups\tmaps\tmapImpl"
                    + "\tcomps\tslots\tlookups/row\tskips/row\tupdates/row\targs/row\tns/row\trows/s"
                    + "\tpeakKiB\tretainedKiB\tchecksum");
            System.out.println(table.get(0));

            try (PeakSampler sampler = new PeakSampler()) {
                for (Shape shape : shapes) {
                    for (Cursor cursor : cursors) {
                        if (shape.wholePartition && cursor == Cursor.STREAMING) {
                            // A two-pass function is what the streaming fast path declines on, so
                            // there is no such arm to measure rather than a slow one.
                            System.out.println("# skipped " + shape.name + "/" + cursor.name
                                    + ": a whole-partition shape never reaches the streaming cursor");
                            continue;
                        }
                        for (KeyType keyType : keyTypes) {
                            for (long keys : cardinalities) {
                                for (int nullPct : nullPercentages) {
                                    for (int entrySize : entrySizes) {
                                        final Arm[] best = new Arm[fusionSettings.length];
                                        // Forward then backward over the settings, keeping each
                                        // one's fastest drain. One arm always runs into a JIT
                                        // state the other left behind, and a fixed order would
                                        // charge that to whichever arm goes first every time;
                                        // alternating gives each of them the warm position once.
                                        // Pointless with a single setting, so the second pass only
                                        // runs when there are two to alternate.
                                        final int passes = fusionSettings.length > 1 ? 2 : 1;
                                        for (int pass = 0; pass < passes; pass++) {
                                            for (int i = 0; i < fusionSettings.length; i++) {
                                                final int index = pass == 0
                                                        ? i
                                                        : fusionSettings.length - 1 - i;
                                                configuration.setSqlUnorderedMapMaxEntrySize(entrySize);
                                                configuration.setSqlWindowMapFusionEnabled(fusionSettings[index]);
                                                configuration.setSqlWindowCachedLightEnabled(
                                                        cursor == Cursor.CACHED_LIGHT
                                                );
                                                final Arm arm = runArm(
                                                        engine, sqlCtx, sampler, shape, cursor,
                                                        orderedBucket, keyType, keys, nullPct, rows,
                                                        warmups, runs
                                                );
                                                if (best[index] == null || arm.nanos < best[index].nanos) {
                                                    best[index] = arm;
                                                }
                                            }
                                        }
                                        for (int i = 0; i < fusionSettings.length; i++) {
                                            if (best[i].checksum != best[0].checksum) {
                                                throw new IllegalStateException(
                                                        "fused and unfused answers differ for "
                                                                + shape.name + "/" + cursor.name + "/"
                                                                + keyType.name + "/keys=" + keys
                                                                + "/nullPct=" + nullPct
                                                                + "/maxEntry=" + entrySize
                                                );
                                            }
                                            final String row = row(
                                                    shape, cursor, keyType, keys, nullPct, entrySize,
                                                    fusionSettings[i], best[i]
                                            );
                                            table.add(row);
                                            System.out.println(row);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Again, in one block: the engine logs to the same stdout, so the rows above have a
            // query log through them and only this copy is a table.
            System.out.println("# results");
            for (String row : table) {
                System.out.println(row);
            }
        } finally {
            engine = Misc.free(engine);
            deleteRecursively(dbRoot);
        }
    }

    /**
     * One table per (key type, cardinality), so a run scans exactly the key column its arms
     * partition by rather than carrying the other two.
     * <p>
     * Every column follows from the row number rather than from an {@code rnd_} call, so the
     * content follows from the rendered SQL and the two arms of a comparison read byte-identical
     * data. The row number is aliased out of {@code long_sequence} first: an expression over
     * {@code x} projected as {@code x} would be a column name resolving to two things.
     */
    private static String createTableSql(KeyType keyType, long keys, long rows, int nullPct) {
        return "create table " + tableName(keyType, keys, nullPct) + " as (select"
                + " (" + START_TS + " + rn * " + TS_STEP_MICROS + ")::timestamp as ts,"
                + " " + keyType.keyExpression("((rn - 1) % " + keys + ")") + " as k,"
                + " " + nullableDouble("(rn % 997)::double", nullPct, 0) + " as x,"
                + " " + nullableDouble("(rn % 991)::double", nullPct, 37) + " as y"
                + " from (select x as rn from long_sequence(" + rows + ")))"
                + " timestamp(ts) partition by day";
    }

    /**
     * Wraps a DOUBLE expression so it is absent on {@code nullPct} percent of the rows, with the
     * absent rows displaced by {@code offset}.
     * <p>
     * The displacement is what makes a two-argument group's gate visible: such a group skips a row
     * only where every one of its components refuses it, so {@code x} and {@code y} sharing a NULL
     * pattern would make that gate indistinguishable from a one-argument group's. Zero is returned
     * unwrapped rather than as a {@code case} that never fires, so the default run generates the
     * data it always did.
     */
    private static String nullableDouble(String expression, int nullPct, int offset) {
        if (nullPct <= 0) {
            return expression;
        }
        if (nullPct >= 100) {
            return "null::double";
        }
        return "case when (rn + " + offset + ") % 100 < " + nullPct
                + " then null::double else " + expression + " end";
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
                Files.delete(d);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static List<Integer> parseInts(String csv, String option) {
        final List<Integer> values = new ArrayList<>();
        for (String part : csv.split(",")) {
            values.add(Integer.parseInt(part.trim()));
        }
        if (values.isEmpty()) {
            throw new IllegalArgumentException(option + " needs at least one value");
        }
        return values;
    }

    private static List<Long> parseLongs(String csv, String option) {
        final List<Long> values = new ArrayList<>();
        for (String part : csv.split(",")) {
            values.add(Long.parseLong(part.trim()));
        }
        if (values.isEmpty()) {
            throw new IllegalArgumentException(option + " needs at least one value");
        }
        return values;
    }

    private static String row(
            Shape shape,
            Cursor cursor,
            KeyType keyType,
            long keys,
            int nullPct,
            int entrySize,
            boolean fusion,
            Arm arm
    ) {
        return String.format(
                Locale.ROOT,
                "%s\t%s\t%s\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t%s\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f"
                        + "\t%.1f\t%.0f\t%d\t%d\t%d",
                shape.name,
                cursor.name,
                keyType.name,
                keys,
                nullPct,
                entrySize,
                fusion ? "on" : "off",
                arm.plans,
                arm.groups,
                arm.openMaps,
                arm.mapImplementation,
                arm.components,
                arm.slots,
                arm.lookups / (double) arm.rows,
                arm.skips / (double) arm.rows,
                arm.updates / (double) arm.rows,
                arm.argumentEvaluations / (double) arm.rows,
                arm.nanos / (double) arm.rows,
                arm.rows / (arm.nanos / 1e9),
                arm.peakBytes / 1024,
                arm.retainedBytes / 1024,
                arm.checksum
        );
    }

    /**
     * Compiles one arm and returns its fastest measured drain. The fastest rather than the mean
     * because the alternative reports the machine's other work: every drain does identical work
     * over identical data, so the spread between them is noise and its floor is the signal.
     */
    private static Arm runArm(
            CairoEngine engine,
            SqlExecutionContextImpl sqlCtx,
            PeakSampler sampler,
            Shape shape,
            Cursor cursor,
            boolean orderedBucket,
            KeyType keyType,
            long keys,
            int nullPct,
            long rows,
            int warmups,
            int runs
    ) throws Exception {
        final String sql = shape.sql(tableName(keyType, keys, nullPct), cursor, orderedBucket, keys);
        RecordCursorFactory factory = null;
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            factory = compiler.compile(sql, sqlCtx).getRecordCursorFactory();
        } catch (Throwable th) {
            Misc.free(factory);
            throw th;
        }
        try {
            final WindowProbe windowFactory = probe(factory, cursor, sql);
            for (int i = 0; i < warmups; i++) {
                drainOnce(sqlCtx, factory, windowFactory, sampler);
            }
            Arm best = null;
            for (int i = 0; i < runs; i++) {
                final Arm arm = drainOnce(sqlCtx, factory, windowFactory, sampler);
                if (arm.rows != rows) {
                    throw new IllegalStateException("row mismatch: expected " + rows + ", got " + arm.rows);
                }
                if (best == null || arm.nanos < best.nanos) {
                    best = arm;
                }
            }
            return best;
        } finally {
            Misc.free(factory);
        }
    }

    private static String tableName(KeyType keyType, long keys, int nullPct) {
        return "w_" + keyType.name + "_" + keys + "_n" + nullPct;
    }

    /**
     * The window factory this query drives, read through one interface whichever of the three it
     * is, or a failure when it is not the one the arm asked for. Which cursor a query lands on is
     * a compiler decision that a SELECT list or a window clause can move without warning, and an
     * arm that measured a different one would report it under this arm's name.
     */
    private static WindowProbe probe(RecordCursorFactory factory, Cursor cursor, String sql) {
        RecordCursorFactory root = factory;
        while (root != null) {
            if (root instanceof WindowRecordCursorFactory f) {
                requireCursor(cursor, Cursor.STREAMING, sql);
                return new WindowProbe(f.getWindowFunctions(), f.getWindowAccumulatorPlans(), f.getWindowMapStates());
            }
            if (root instanceof CachedWindowRecordCursorFactory f) {
                requireCursor(cursor, Cursor.CACHED, sql);
                return cachedProbe(f.getAllWindowFunctions(), f.getWindowMapGroups());
            }
            if (root instanceof CachedWindowLightRecordCursorFactory f) {
                requireCursor(cursor, Cursor.CACHED_LIGHT, sql);
                return cachedProbe(f.getAllWindowFunctions(), f.getWindowMapGroups());
            }
            root = root.getBaseFactory();
        }
        throw new IllegalStateException("expected a window factory for: " + sql);
    }

    private static WindowProbe cachedProbe(
            ObjList<WindowFunction> functions,
            CachedWindowMapGroups groups
    ) {
        return new WindowProbe(
                functions,
                groups == null ? null : groups.getPlans(),
                groups == null ? null : groups.getStates()
        );
    }

    private static void requireCursor(Cursor expected, Cursor actual, String sql) {
        if (expected != actual) {
            throw new IllegalStateException(
                    "expected the " + expected.name + " cursor but the query compiled to " + actual.name + ": " + sql
            );
        }
    }

    /**
     * Reads every output column of one row into a running, order-sensitive digest. It is what
     * makes the fused and unfused arms comparable as answers rather than only as timings, and it
     * also keeps the drain from being a loop whose results nothing reads.
     */
    private static long checksum(long digest, Record record, RecordMetadata metadata, int columnCount) {
        for (int i = 0; i < columnCount; i++) {
            final int type = ColumnType.tagOf(metadata.getColumnType(i));
            final long bits;
            switch (type) {
                case ColumnType.DOUBLE:
                    // doubleToLongBits rather than raw: every NULL DOUBLE is a NaN, and only the
                    // canonicalizing conversion promises two NaNs compare equal here.
                    bits = Double.doubleToLongBits(record.getDouble(i));
                    break;
                case ColumnType.LONG:
                    bits = record.getLong(i);
                    break;
                case ColumnType.INT:
                    bits = record.getInt(i);
                    break;
                default:
                    throw new IllegalStateException(
                            "unsupported output column type: " + ColumnType.nameOf(metadata.getColumnType(i))
                    );
            }
            digest ^= bits;
            digest *= 0x9E3779B97F4A7C15L;
            digest ^= digest >>> 29;
        }
        return digest;
    }

    /**
     * Captures the structural facts of the drain that just finished, while the cursor is still
     * open: a close resets the group counters and frees the maps this reads.
     * <p>
     * A map is counted where it actually holds backing, so a bound function's dormant private map
     * is not one. The bound groups' lookup and update counts are measured; a function on its own
     * map contributes the structural one-per-row, since the private path carries no counter.
     */
    private static void captureStructure(Arm arm, WindowProbe factory, long rows) {
        final LinkedHashMap<String, Integer> implementations = new LinkedHashMap<>();
        final ObjList<WindowAccumulatorPlan> plans = factory.plans;
        arm.plans = plans == null ? 0 : plans.size();
        final ObjList<WindowMapState> states = factory.states;
        if (states != null) {
            for (int i = 0, n = states.size(); i < n; i++) {
                final WindowMapState state = states.getQuick(i);
                arm.groups++;
                arm.lookups += state.getLookupCount();
                arm.skips += state.getSkippedRowCount();
                arm.updates += state.getContributorUpdateCount();
                final WindowAccumulatorPlan plan = state.getPlan();
                arm.components += plan.getComponentCount();
                arm.slots += plan.getSlotCount();
                for (int c = 0, m = plan.getComponentCount(); c < m; c++) {
                    if (WindowAccumulatorDescriptor.familyTakesArgument(plan.getComponent(c).getFamily())) {
                        arm.argumentEvaluations += rows;
                    }
                }
                if (state.isMapOpen()) {
                    arm.openMaps++;
                    implementations.merge(state.getMapImplementation(), 1, Integer::sum);
                }
            }
        }
        final ObjList<WindowFunction> functions = factory.functions;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            final Map map = function.getPartitionMap();
            if (map == null || !map.isOpen()) {
                continue;
            }
            arm.openMaps++;
            implementations.merge(map.getClass().getSimpleName(), 1, Integer::sum);
            // Twice a row for a whole-partition two-pass function: pass 1 probes to accumulate and
            // pass 2 probes again to read the finished state back. Once for everything else, which
            // computes and writes its output in the one pass.
            arm.lookups += function.getPassCount() > WindowFunction.ONE_PASS ? 2 * rows : rows;
            arm.updates += rows;
            // Its own standalone image, which is what makes the components and slots column
            // comparable across the two arms: three private (sum, nonNullCount)-shaped states are
            // three components and five slots whether or not a group would fold them into one.
            arm.components++;
            arm.slots += WindowAccumulatorDescriptor.familySlotCount(function.windowAccumulatorFamily());
            // Exact for the families here, each of which reads its argument once per row inside
            // the same computeNext that probes. It is a declaration rather than a measurement, so
            // a function that reads an argument without declaring an accumulator family reports
            // none: the whole-partition shapes are exactly that, and every one of them evaluates
            // its argument once a row in pass 1.
            if (function.windowAccumulatorArgument() != null) {
                arm.argumentEvaluations += rows;
            }
        }
        final StringBuilder sink = new StringBuilder();
        for (java.util.Map.Entry<String, Integer> entry : implementations.entrySet()) {
            if (sink.length() > 0) {
                sink.append('+');
            }
            sink.append(entry.getKey());
            if (entry.getValue() > 1) {
                sink.append('x').append(entry.getValue());
            }
        }
        arm.mapImplementation = sink.length() == 0 ? "none" : sink.toString();
    }

    /**
     * One measured drain, reported against the {@link MemoryTracker} the query itself runs
     * under.
     * <p>
     * That tracker is not this method's to acquire: {@code QueryProgress.getCursor} registers the
     * query, and {@code QueryRegistry.register} binds a fresh QUERY tracker on the execution
     * context, deliberately <b>not</b> inheriting one already bound there. So the honest reading
     * is the one the cursor is charging - taken off the context after the open and read before
     * the close, which is where the registry hands it back to the pool.
     */
    private static Arm drainOnce(
            SqlExecutionContextImpl sqlCtx,
            RecordCursorFactory factory,
            WindowProbe windowFactory,
            PeakSampler sampler
    ) throws Exception {
        final Arm arm = new Arm();
        final long start = System.nanoTime();
        try (RecordCursor cursor = factory.getCursor(sqlCtx)) {
            final MemoryTracker tracker = sqlCtx.getMemoryTracker();
            if (tracker == null) {
                throw new IllegalStateException(
                        "the query bound no MemoryTracker, so its native bytes cannot be reported"
                );
            }
            sampler.watch(tracker);
            try {
                final Record record = cursor.getRecord();
                final RecordMetadata metadata = factory.getMetadata();
                final int columnCount = metadata.getColumnCount();
                long rows = 0;
                long digest = 0;
                while (cursor.hasNext()) {
                    rows++;
                    digest = checksum(digest, record, metadata, columnCount);
                }
                arm.nanos = System.nanoTime() - start;
                arm.rows = rows;
                arm.checksum = digest;
                arm.retainedBytes = tracker.getUsed();
                arm.peakBytes = sampler.peak();
                captureStructure(arm, windowFactory, rows);
            } finally {
                sampler.watch(null);
            }
        }
        return arm;
    }

    /**
     * The three lists a report row is read off, taken once per arm because they are the factory's
     * own and outlive its cursors. The counters behind them are not: they live on the
     * {@link WindowMapState}s, which a close resets, so {@code captureStructure} still has to run
     * while the cursor is open.
     */
    private static final class WindowProbe {
        final ObjList<WindowFunction> functions;
        final ObjList<WindowAccumulatorPlan> plans;
        final ObjList<WindowMapState> states;

        WindowProbe(
                ObjList<WindowFunction> functions,
                ObjList<WindowAccumulatorPlan> plans,
                ObjList<WindowMapState> states
        ) {
            this.functions = functions;
            this.plans = plans;
            this.states = states;
        }
    }

    private static final class Arm {
        String mapImplementation = "none";
        long argumentEvaluations;
        long checksum;
        int components;
        int groups;
        long lookups;
        long nanos;
        int openMaps;
        long peakBytes;
        int plans;
        long retainedBytes;
        long rows;
        long skips;
        int slots;
        long updates;
    }

    /**
     * A {@link DefaultCairoConfiguration} whose two window-fusion knobs move between arms. Both
     * are read at compile time, and every arm compiles its own factory, so a field is all the
     * indirection this needs.
     */
    private static final class BenchmarkConfiguration extends DefaultCairoConfiguration {
        private int maxEntrySize = 16;
        private boolean windowCachedLightEnabled = false;
        private boolean windowMapFusionEnabled = true;

        BenchmarkConfiguration(String root) {
            super(root);
        }

        /**
         * Off, so the report is a table rather than a table with a query log through it. Every
         * drain is one registered query and would otherwise log a line into the middle of the
         * row it belongs to.
         */
        @Override
        public boolean getLogSqlQueryProgressExe() {
            return false;
        }

        @Override
        public int getSqlUnorderedMapMaxEntrySize() {
            return maxEntrySize;
        }

        @Override
        public boolean isSqlWindowCachedLightEnabled() {
            return windowCachedLightEnabled;
        }

        @Override
        public boolean isSqlWindowMapFusionEnabled() {
            return windowMapFusionEnabled;
        }

        void setSqlUnorderedMapMaxEntrySize(int maxEntrySize) {
            this.maxEntrySize = maxEntrySize;
        }

        void setSqlWindowCachedLightEnabled(boolean enabled) {
            this.windowCachedLightEnabled = enabled;
        }

        void setSqlWindowMapFusionEnabled(boolean enabled) {
            this.windowMapFusionEnabled = enabled;
        }
    }

    /**
     * The cursor an arm measures. It is a property of the run rather than of the shape - the same
     * SELECT list reaches all three, given the window clause each of them needs - so the report
     * carries it as a column and the arm asserts the query landed on it.
     */
    private enum Cursor {
        CACHED("cached"),
        CACHED_LIGHT("cached-light"),
        STREAMING("streaming");

        private final String name;

        Cursor(String name) {
            this.name = name;
        }

        static Cursor of(String name) {
            for (Cursor cursor : values()) {
                if (cursor.name.equals(name)) {
                    return cursor;
                }
            }
            throw new IllegalArgumentException(
                    "--cursor must be one of streaming, cached, cached-light: " + name
            );
        }
    }

    /**
     * The partition key's type. It decides the map implementation as much as the value width
     * does - {@code MapFactory} weighs 4 bytes for an INT or a SYMBOL and 16 for a VARCHAR, and
     * puts every STRING key on an {@code OrderedMap} whatever the value - so a claim about the
     * fused implementation belongs to one of these and not to all four. The two that can leave
     * an unordered map as the value widens leave a different one: an INT or SYMBOL key drops
     * {@code Unordered4Map}, a VARCHAR key drops {@code UnorderedVarcharMap}.
     */
    private enum KeyType {
        INT("int"),
        STRING("string"),
        SYMBOL("symbol"),
        VARCHAR("varchar");

        private final String name;

        KeyType(String name) {
            this.name = name;
        }

        static KeyType of(String name) {
            for (KeyType keyType : values()) {
                if (keyType.name.equals(name)) {
                    return keyType;
                }
            }
            throw new IllegalArgumentException("--key-type must be one of int, symbol, string, varchar: " + name);
        }

        String keyExpression(String keyId) {
            return switch (this) {
                case INT -> keyId + "::int";
                case STRING -> "('k' || " + keyId + ")::string";
                case SYMBOL -> "('k' || " + keyId + ")::symbol";
                case VARCHAR -> "('k' || " + keyId + ")::varchar";
            };
        }
    }

    /**
     * Samples one tracker's used bytes on a side thread and keeps the highest reading since the
     * last {@link #watch}. A query's map grows and is handed back inside the cursor's lifetime,
     * so nothing a caller reads after the close can see the peak.
     */
    private static final class PeakSampler extends Thread implements QuietCloseable {
        private final Object lock = new Object();
        private long peak;
        private volatile boolean running = true;
        private MemoryTracker tracker;

        PeakSampler() {
            super("window-map-fusion-peak-sampler");
            setDaemon(true);
            start();
        }

        @Override
        public void close() {
            running = false;
        }

        public long peak() {
            synchronized (lock) {
                return peak;
            }
        }

        @Override
        public void run() {
            while (running) {
                synchronized (lock) {
                    if (tracker != null) {
                        final long used = tracker.getUsed();
                        if (used > peak) {
                            peak = used;
                        }
                    }
                }
                Os.sleep(1);
            }
        }

        /**
         * Starts sampling {@code tracker}, or stops sampling when it is null. Holding the lock is
         * what makes the stop synchronous: the sampling thread cannot be inside a
         * {@link MemoryTracker#getUsed()} on a tracker the caller is about to recycle.
         */
        public void watch(MemoryTracker tracker) {
            synchronized (lock) {
                this.tracker = tracker;
                if (tracker != null) {
                    peak = 0;
                }
            }
        }
    }

    /**
     * The SELECT list a run measures - one per numbered case of the acceptance plan. Cases 8, 9
     * and 10 vary the data rather than the query and are {@code --keys} and {@code --key-type};
     * case 11 is {@code --cursor}, which runs these same lists on the cached factories.
     */
    private enum Shape {
        /**
         * Case 4: two counters over one narrow key, which fuse across a Map-implementation change
         * at {@code --entry-size=16} and without one at 32.
         */
        COUNT_COUNT("count-count", false),
        /**
         * Case 5: four dispersion projections plus the {@code count} that folds onto their
         * counter - one three-slot Welford component serving five outputs.
         */
        DISPERSION("dispersion", false),
        /**
         * The whole-partition {@code avg} on its own: one map and two probes a row, and no group,
         * because one fusible function is not a group - moving a map is not removing one. It is the
         * map work a fused {@code partition-sum-avg-count} is left with, so it is that shape's
         * floor and, like the single-sum control, a row whose two arms run the same code.
         */
        PARTITION_AVG("partition-avg", true),
        /**
         * Two whole-partition two-pass functions over one argument: two maps and four probes a row
         * unfused against one map and two fused. The narrower of the two fusing whole-partition
         * shapes, and the one the refused-row skip moves most - its whole group is the single
         * {@code (sum, nonNullCount)} component, so a row with a non-finite {@code x} is a row the
         * group has nothing at all to do for.
         */
        PARTITION_SUM_AVG("partition-sum-avg", true),
        /**
         * Three whole-partition two-pass functions over one argument: three maps, six probes a row
         * - three in pass 1 and three in pass 2 - and three copies of a {@code (sum, count)} pair
         * that a shared component makes one. It is what the non-destructive {@code preparePass2}
         * step exists to fuse.
         */
        PARTITION_SUM_AVG_COUNT("partition-sum-avg-count", true),
        /**
         * {@link #PARTITION_SUM_AVG} with the two calls reading different columns, which is the
         * one thing that changes: the same arithmetic over two arguments is two
         * {@code (sum, nonNullCount)} components rather than one, so the group co-locates them
         * behind one key instead of merging them. It is the row that separates the merge from
         * co-location, and the one where the refused-row skip is a joint decision - a row counts
         * as refused only when neither column offers a finite value, so
         * {@code --null-pct=90} skips rather less than 0.90 of the rows here and the
         * {@code skips/row} column says how much less.
         * <p>
         * A wider fused value than {@link #PARTITION_SUM_AVG}'s, so an INT-keyed group crosses
         * {@code --entry-size=32} onto an {@code OrderedMap} while its unfused members keep an
         * {@code Unordered4Map} each. Run it at 64 as well to hold the implementation constant.
         */
        PARTITION_SUM_AVG_TWO_ARGS("partition-sum-avg-two-args", true),
        /**
         * Case 6: the row-count family, and a {@code count} over the window's own partition key,
         * which is a guarded reading of it wherever the key type admits the guard.
         */
        ROW_COUNT("row-count", false),
        /**
         * The bounded-ROWS counterpart of {@link #SUM_AVG_COUNT}'s merge: two readings of one
         * moving {@code (sum, count)} over the last {@code BOUNDED_ROWS_PRECEDING} rows.
         * <p>
         * It is the first shape here whose state is not all in the map value. Unfused it is two
         * maps, two probes, two evaluations of {@code x} and <b>two rings</b> of the frame's own
         * values; fused it is one of each, with the ring one cell longer than the unfused one -
         * which is what a bound contributor pays for leaving the current frame's total in the slice
         * instead of the carry the unfused row ends on. The retained-bytes column is where that
         * trade is legible: the arena is tracked like the maps are.
         */
        ROWS_FRAME_SUM_AVG("rows-frame-sum-avg", false),
        /**
         * The same frame with a counter beside it: two components, seven slots and two rings fused,
         * against three maps and three rings unfused. Nothing folds between a bounded sum and a
         * bounded count - the count's own state continues into a ring of flags - so this is the
         * shape that prices physical co-location for the ring families with the merge held
         * constant.
         */
        ROWS_FRAME_SUM_AVG_COUNT("rows-frame-sum-avg-count", false),
        /**
         * The bounded-RANGE spelling of {@link #ROWS_FRAME_SUM_AVG}: the same merge over a frame
         * whose length is the timestamps' rather than the query's.
         * <p>
         * What it prices that the ROWS shape does not is the resizable ring. Its slice is six slots
         * where the ROWS one is four - the address, the read cursor, the length and the capacity -
         * and each of its cells is a {@code (timestamp, value)} pair rather than a bare value, so
         * the retained-bytes column carries twice the arena per row of frame. The frame's span is
         * chosen to hold about {@link #BOUNDED_ROWS_PRECEDING} rows, so the two shapes are
         * comparable.
         */
        RANGE_FRAME_SUM_AVG("range-frame-sum-avg", false),
        /**
         * The same frame with a counter beside it: two components, eleven slots and two rings
         * fused, against three maps and three rings unfused. Nothing folds between a bounded sum
         * and a bounded count, so this prices physical co-location for the RANGE ring families with
         * the merge held constant.
         */
        RANGE_FRAME_SUM_AVG_COUNT("range-frame-sum-avg-count", false),
        /**
         * Case 1: the single-function control. It forms no group - moving one map is not removing
         * one - so both arms run the same path, and a difference between them is the noise floor
         * every other row of the report is read against.
         */
        SINGLE_SUM("sum", false),
        /**
         * Case 2: three projections onto one {@code (sum, nonNullCount)} component. The
         * structural-acceptance shape: three maps, five slots and three argument evaluations a row
         * become one, two and one.
         */
        SUM_AVG_COUNT("sum-avg-count", false),
        /**
         * Case 3: two components behind one key. The counters do not merge; the lookup does.
         */
        SUM_COUNT("sum-count", false),
        /**
         * Case 12: the same trade as case 4 one limit up. Two {@code (sum, nonNullCount)} pairs
         * are {@code 4 + 16 = 20} each on their own and {@code 4 + 32 = 36} fused, so over an INT
         * key at {@code --entry-size=32} - a server's default - the group is one {@code OrderedMap}
         * against two {@code Unordered4Map}s.
         */
        SUM_SUM("sum-sum", false),
        /**
         * Case 7: one partition domain, two frames. The two windows are two traversals and so two
         * groups, which is what says co-location is per window rather than per key.
         */
        TWO_FRAMES("two-frames", false);

        private final String name;
        /**
         * Whether every call in the list is a whole-partition two-pass function. Such a shape
         * declines the streaming fast path by itself, so it needs no forcing and has no streaming
         * arm.
         */
        private final boolean wholePartition;

        Shape(String name, boolean wholePartition) {
            this.name = name;
            this.wholePartition = wholePartition;
        }

        static Shape of(String name) {
            for (Shape shape : values()) {
                if (shape.name.equals(name)) {
                    return shape;
                }
            }
            throw new IllegalArgumentException("unknown --shape: " + name);
        }

        /**
         * @param cursor        the factory the arm intends to measure. A cumulative shape reaches
         *                      a cached one only because this method makes it - see
         *                      {@code orderedBucket} - and a whole-partition shape reaches it
         *                      whatever is asked for
         * @param orderedBucket on a cached cursor, whether to force the path with a sort the base
         *                      cursor does not already produce (the group is then traversed in its
         *                      own sort bucket and the SELECT list is the streaming one exactly) or
         *                      with a residual whole-partition call (the group is then traversed
         *                      with the scan that fills the chain). Ignored for the streaming
         *                      cursor, for a whole-partition shape and for a bounded RANGE one,
         *                      none of which has a choice to make
         * @param keys          the table's key cardinality, which a bounded RANGE frame's span is a
         *                      function of: one partition's rows are that many table rows apart
         */
        String sql(String table, Cursor cursor, boolean orderedBucket, long keys) {
            final String projections = switch (this) {
                case COUNT_COUNT -> "count(x) over w, count(y) over w";
                case DISPERSION -> "stddev_samp(x) over w, stddev_pop(x) over w, var_samp(x) over w, "
                        + "var_pop(x) over w, count(x) over w";
                case PARTITION_AVG -> "avg(x) over w";
                case PARTITION_SUM_AVG -> "sum(x) over w, avg(x) over w";
                case PARTITION_SUM_AVG_TWO_ARGS -> "sum(x) over w, avg(y) over w";
                case PARTITION_SUM_AVG_COUNT -> "sum(x) over w, avg(x) over w, count(x) over w";
                case RANGE_FRAME_SUM_AVG -> "sum(x) over w, avg(x) over w";
                case RANGE_FRAME_SUM_AVG_COUNT -> "sum(x) over w, avg(x) over w, count(x) over w";
                case ROW_COUNT -> "count(*) over w, row_number() over w, count(k) over w";
                case ROWS_FRAME_SUM_AVG -> "sum(x) over w, avg(x) over w";
                case ROWS_FRAME_SUM_AVG_COUNT -> "sum(x) over w, avg(x) over w, count(x) over w";
                case SINGLE_SUM -> "sum(x) over w";
                case SUM_AVG_COUNT -> "sum(x) over w, avg(x) over w, count(x) over w";
                case SUM_COUNT -> "sum(x) over w, count(y) over w";
                case SUM_SUM -> "sum(x) over w, sum(y) over w";
                case TWO_FRAMES -> "sum(x) over w, count(y) over w, sum(x) over w2, count(y) over w2";
            };
            if (wholePartition) {
                return "select " + projections + " from " + table
                        + " window w as (partition by k)";
            }
            final boolean cached = cursor != Cursor.STREAMING;
            final boolean boundedRange = this == RANGE_FRAME_SUM_AVG || this == RANGE_FRAME_SUM_AVG_COUNT;
            // Descending on the designated timestamp rather than on an ordinary column: no two
            // rows tie, so the cumulative answers stay a function of the data alone and the two
            // fusion arms remain comparable as answers. A bounded RANGE frame has no such choice -
            // it is compiled only where the window's order was dismissed against the base cursor -
            // so it always takes the ascending spelling and always needs the residual forcing call.
            final String order = cached && orderedBucket && !boundedRange ? "order by ts desc" : "order by ts";
            final String forcing =
                    cached && (!orderedBucket || boundedRange) ? ", avg(x) over (partition by k)" : "";
            // A bounded low bound is what makes the state ring-backed, and the span is small on
            // purpose: the ring is one cell per row of the frame per key, so a wide frame would
            // measure the arena's size rather than the group's saving. The RANGE span is the same
            // number of rows expressed in the table's own timestamp step, so the two bounded
            // shapes hold the same frame and differ only in the state that holds it.
            final boolean boundedRows = this == ROWS_FRAME_SUM_AVG || this == ROWS_FRAME_SUM_AVG_COUNT;
            final String frame;
            if (boundedRange) {
                // Consecutive rows of one partition are `keys` rows apart in the table, so this is
                // the span BOUNDED_ROWS_PRECEDING of them occupy - which is what makes the two
                // bounded shapes hold the same frame over the same data.
                frame = "range between " + (BOUNDED_ROWS_PRECEDING * keys * TS_STEP_MICROS)
                        + " microseconds preceding and current row";
            } else if (boundedRows) {
                frame = "rows between " + BOUNDED_ROWS_PRECEDING + " preceding and current row";
            } else {
                frame = "rows between unbounded preceding and current row";
            }
            final String windows = "window w as (partition by k " + order + " " + frame + ")"
                    + (this == TWO_FRAMES
                    ? ", w2 as (partition by k " + order + " range between unbounded preceding and current row)"
                    : "");
            return "select " + projections + forcing + " from " + table + " " + windows;
        }
    }
}
