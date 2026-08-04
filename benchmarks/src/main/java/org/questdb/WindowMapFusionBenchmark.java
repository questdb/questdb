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
 * </pre>
 */
public class WindowMapFusionBenchmark {

    private static final long START_TS = 1_704_067_200_000_000L;
    private static final long TS_STEP_MICROS = 1_000L;

    public static void main(String[] args) throws Exception {
        long rows = 2_000_000L;
        String keysArg = "1000,1000000";
        String keyTypesArg = "int,symbol,string,varchar";
        String entrySizesArg = "16,32";
        String shapesArg = "all";
        String fusionArg = "both";
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
                    "# rows=%d keys=%s keyTypes=%s entrySizes=%s shapes=%s fusion=%s warmups=%d runs=%d%n",
                    rows, keysArg, keyTypesArg, entrySizesArg, shapesArg, fusionArg, warmups, runs
            );

            for (KeyType keyType : keyTypes) {
                for (long keys : cardinalities) {
                    engine.execute(createTableSql(keyType, keys, rows), sqlCtx);
                }
            }

            final List<String> table = new ArrayList<>();
            table.add("shape\tkey\tkeys\tmaxEntry\tfusion\tplans\tgroups\tmaps\tmapImpl\tcomps\tslots"
                    + "\tlookups/row\tupdates/row\targs/row\tns/row\trows/s\tpeakKiB\tretainedKiB\tchecksum");
            System.out.println(table.get(0));

            try (PeakSampler sampler = new PeakSampler()) {
                for (Shape shape : shapes) {
                    for (KeyType keyType : keyTypes) {
                        for (long keys : cardinalities) {
                            for (int entrySize : entrySizes) {
                                final Arm[] best = new Arm[fusionSettings.length];
                                // Forward then backward over the settings, keeping each one's
                                // fastest drain. One arm always runs into a JIT state the other
                                // left behind, and a fixed order would charge that to whichever
                                // arm goes first every time; alternating gives each of them the
                                // warm position once. Pointless with a single setting, so the
                                // second pass only runs when there are two to alternate.
                                final int passes = fusionSettings.length > 1 ? 2 : 1;
                                for (int pass = 0; pass < passes; pass++) {
                                    for (int i = 0; i < fusionSettings.length; i++) {
                                        final int index = pass == 0 ? i : fusionSettings.length - 1 - i;
                                        configuration.setSqlUnorderedMapMaxEntrySize(entrySize);
                                        configuration.setSqlWindowMapFusionEnabled(fusionSettings[index]);
                                        final Arm arm = runArm(
                                                engine, sqlCtx, sampler, shape, keyType, keys, rows, warmups, runs
                                        );
                                        if (best[index] == null || arm.nanos < best[index].nanos) {
                                            best[index] = arm;
                                        }
                                    }
                                }
                                for (int i = 0; i < fusionSettings.length; i++) {
                                    if (best[i].checksum != best[0].checksum) {
                                        throw new IllegalStateException(
                                                "fused and unfused answers differ for " + shape.name + "/"
                                                        + keyType.name + "/keys=" + keys + "/maxEntry=" + entrySize
                                        );
                                    }
                                    final String row = row(shape, keyType, keys, entrySize, fusionSettings[i], best[i]);
                                    table.add(row);
                                    System.out.println(row);
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
    private static String createTableSql(KeyType keyType, long keys, long rows) {
        return "create table " + tableName(keyType, keys) + " as (select"
                + " (" + START_TS + " + rn * " + TS_STEP_MICROS + ")::timestamp as ts,"
                + " " + keyType.keyExpression("((rn - 1) % " + keys + ")") + " as k,"
                + " (rn % 997)::double as x,"
                + " (rn % 991)::double as y"
                + " from (select x as rn from long_sequence(" + rows + ")))"
                + " timestamp(ts) partition by day";
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

    private static String row(Shape shape, KeyType keyType, long keys, int entrySize, boolean fusion, Arm arm) {
        return String.format(
                Locale.ROOT,
                "%s\t%s\t%d\t%d\t%s\t%d\t%d\t%d\t%s\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.1f\t%.0f\t%d\t%d\t%d",
                shape.name,
                keyType.name,
                keys,
                entrySize,
                fusion ? "on" : "off",
                arm.plans,
                arm.groups,
                arm.openMaps,
                arm.mapImplementation,
                arm.components,
                arm.slots,
                arm.lookups / (double) arm.rows,
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
            KeyType keyType,
            long keys,
            long rows,
            int warmups,
            int runs
    ) throws Exception {
        final String sql = shape.sql(tableName(keyType, keys));
        RecordCursorFactory factory = null;
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            factory = compiler.compile(sql, sqlCtx).getRecordCursorFactory();
        } catch (Throwable th) {
            Misc.free(factory);
            throw th;
        }
        try {
            final WindowRecordCursorFactory windowFactory = windowFactory(factory, sql);
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

    private static String tableName(KeyType keyType, long keys) {
        return "w_" + keyType.name + "_" + keys;
    }

    /**
     * The window factory this query drives, or a failure. A silently cached or mis-routed plan
     * would measure a different cursor and report it under this benchmark's name.
     */
    private static WindowRecordCursorFactory windowFactory(RecordCursorFactory factory, String sql) {
        RecordCursorFactory root = factory;
        while (root != null && !(root instanceof WindowRecordCursorFactory)) {
            root = root.getBaseFactory();
        }
        if (root == null) {
            throw new IllegalStateException("expected a streaming WindowRecordCursorFactory for: " + sql);
        }
        return (WindowRecordCursorFactory) root;
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
    private static void captureStructure(Arm arm, WindowRecordCursorFactory factory, long rows) {
        final LinkedHashMap<String, Integer> implementations = new LinkedHashMap<>();
        final ObjList<WindowAccumulatorPlan> plans = factory.getWindowAccumulatorPlans();
        arm.plans = plans == null ? 0 : plans.size();
        final ObjList<WindowMapState> states = factory.getWindowMapStates();
        if (states != null) {
            for (int i = 0, n = states.size(); i < n; i++) {
                final WindowMapState state = states.getQuick(i);
                arm.groups++;
                arm.lookups += state.getLookupCount();
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
        final ObjList<WindowFunction> functions = factory.getWindowFunctions();
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            final Map map = function.getPartitionMap();
            if (map == null || !map.isOpen()) {
                continue;
            }
            arm.openMaps++;
            implementations.merge(map.getClass().getSimpleName(), 1, Integer::sum);
            arm.lookups += rows;
            arm.updates += rows;
            // Its own standalone image, which is what makes the components and slots column
            // comparable across the two arms: three private (sum, nonNullCount)-shaped states are
            // three components and five slots whether or not a group would fold them into one.
            arm.components++;
            arm.slots += WindowAccumulatorDescriptor.familySlotCount(function.windowAccumulatorFamily());
            // Exact for the families here, each of which reads its argument once per row inside
            // the same computeNext that probes. A residual family that reads an argument without
            // declaring one would be undercounted, and none of the shapes below is one.
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
            WindowRecordCursorFactory windowFactory,
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
        public boolean isSqlWindowMapFusionEnabled() {
            return windowMapFusionEnabled;
        }

        void setSqlUnorderedMapMaxEntrySize(int maxEntrySize) {
            this.maxEntrySize = maxEntrySize;
        }

        void setSqlWindowMapFusionEnabled(boolean enabled) {
            this.windowMapFusionEnabled = enabled;
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
     * case 11 needs the cached factories and does not exist yet.
     */
    private enum Shape {
        /**
         * Case 4: two counters over one narrow key, which fuse across a Map-implementation change
         * at {@code --entry-size=16} and without one at 32.
         */
        COUNT_COUNT("count-count"),
        /**
         * Case 5: four dispersion projections plus the {@code count} that folds onto their
         * counter - one three-slot Welford component serving five outputs.
         */
        DISPERSION("dispersion"),
        /**
         * Case 6: the row-count family, and a {@code count} over the window's own partition key,
         * which is a guarded reading of it wherever the key type admits the guard.
         */
        ROW_COUNT("row-count"),
        /**
         * Case 1: the single-function control. It forms no group - moving one map is not removing
         * one - so both arms run the same path, and a difference between them is the noise floor
         * every other row of the report is read against.
         */
        SINGLE_SUM("sum"),
        /**
         * Case 2: three projections onto one {@code (sum, nonNullCount)} component. The
         * structural-acceptance shape: three maps, five slots and three argument evaluations a row
         * become one, two and one.
         */
        SUM_AVG_COUNT("sum-avg-count"),
        /**
         * Case 3: two components behind one key. The counters do not merge; the lookup does.
         */
        SUM_COUNT("sum-count"),
        /**
         * Case 12: the same trade as case 4 one limit up. Two {@code (sum, nonNullCount)} pairs
         * are {@code 4 + 16 = 20} each on their own and {@code 4 + 32 = 36} fused, so over an INT
         * key at {@code --entry-size=32} - a server's default - the group is one {@code OrderedMap}
         * against two {@code Unordered4Map}s.
         */
        SUM_SUM("sum-sum"),
        /**
         * Case 7: one partition domain, two frames. The two windows are two traversals and so two
         * groups, which is what says co-location is per window rather than per key.
         */
        TWO_FRAMES("two-frames");

        private final String name;

        Shape(String name) {
            this.name = name;
        }

        static Shape of(String name) {
            for (Shape shape : values()) {
                if (shape.name.equals(name)) {
                    return shape;
                }
            }
            throw new IllegalArgumentException("unknown --shape: " + name);
        }

        String sql(String table) {
            final String projections = switch (this) {
                case COUNT_COUNT -> "count(x) over w, count(y) over w";
                case DISPERSION -> "stddev_samp(x) over w, stddev_pop(x) over w, var_samp(x) over w, "
                        + "var_pop(x) over w, count(x) over w";
                case ROW_COUNT -> "count(*) over w, row_number() over w, count(k) over w";
                case SINGLE_SUM -> "sum(x) over w";
                case SUM_AVG_COUNT -> "sum(x) over w, avg(x) over w, count(x) over w";
                case SUM_COUNT -> "sum(x) over w, count(y) over w";
                case SUM_SUM -> "sum(x) over w, sum(y) over w";
                case TWO_FRAMES -> "sum(x) over w, count(y) over w, sum(x) over w2, count(y) over w2";
            };
            final String windows = "window w as (partition by k order by ts "
                    + "rows between unbounded preceding and current row)"
                    + (this == TWO_FRAMES
                    ? ", w2 as (partition by k order by ts range between unbounded preceding and current row)"
                    : "");
            return "select " + projections + " from " + table + " " + windows;
        }
    }
}
