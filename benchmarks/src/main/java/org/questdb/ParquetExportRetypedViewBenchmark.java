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

import io.questdb.ServerMain;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.view.ViewCompilerJob;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cutlass.parquet.CopyExportRequestJob;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Parquet export of a view over a table that has Parquet partitions and a column whose type
 * changed by ALTER COLUMN TYPE.
 * <p>
 * {@code source=RETYPED} exports the view over such a table. Its export goes through a temp
 * table: the export copies every exported row into a native temp table, then encodes that.
 * {@code source=TWIN} exports the same view over a table that holds the same rows and partition
 * formats but was created with the final column type. Its export takes the path the export of
 * the RETYPED view took while StaleViewCheckFactory did not report the converted columns of the
 * scan it wraps: it streams the scan's page frames straight to the encoder (DIRECT_PAGE_FRAME),
 * or, when the scan supports no page frames, it encodes the rows the cursor returns
 * (CURSOR_BASED). The frames, their bytes and the decoded Parquet columns are the same for both
 * tables in every DIRECT_PAGE_FRAME shape below, and in the CURSOR_BASED shapes TWIN only skips
 * the row-wise conversion of the retyped column in Parquet partitions, so TWIN measures that path.
 * <p>
 * Shapes, each one that path exported right:
 * <ul>
 *   <li>NATIVE_PARTITIONS - the query reads only native partitions;</li>
 *   <li>LEADING_COLUMNS - the view reads every partition but only the columns before the
 *   retyped one;</li>
 *   <li>RETYPED_BEFORE_CONVERSION - the column changed type before its partitions went to
 *   Parquet, so they store it in its current type; it is the last column;</li>
 *   <li>LATEST_ON - the view is a LATEST ON query, whose scan supports no page frames;</li>
 *   <li>INDEXED_SYMBOL_FILTER - the query filters the view on an indexed SYMBOL, whose scan
 *   supports no page frames;</li>
 *   <li>SINGLE_ROW - the query reads the first row of the first native partition;</li>
 *   <li>EMPTY_RESULT - the query reads a native partition and returns no row.</li>
 * </ul>
 * Endpoints: {@code COPY (...) TO ... WITH FORMAT PARQUET}, run by the benchmark thread, and
 * HTTP {@code /exp?fmt=parquet} against a server on the same data.
 * <pre>
 *   mvn -pl core install -DskipTests &amp;&amp; mvn -pl benchmarks package -DskipTests
 *   java -jar benchmarks/target/benchmarks.jar ParquetExportRetypedViewBenchmark
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, jvmArgsAppend = {
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED"
})
public class ParquetExportRetypedViewBenchmark {
    private static final int DAYS = 4;
    private static final String EXPORT_FILE = "bench_export";
    private static final String TABLE = "trades";
    @Param({"COPY", "HTTP"})
    public String endpoint;
    // The temp table adds a fixed cost per export besides its per-row cost, so small exports slow down the most.
    @Param({"10000", "1000000"})
    public long rowsPerDay;
    @Param({
            "NATIVE_PARTITIONS",
            "LEADING_COLUMNS",
            "RETYPED_BEFORE_CONVERSION",
            "LATEST_ON",
            "INDEXED_SYMBOL_FILTER",
            "SINGLE_ROW",
            "EMPTY_RESULT"
    })
    public String shape;
    @Param({"RETYPED", "TWIN"})
    public String source;
    private SqlCompiler compiler;
    private CopyExportRequestJob copyJob;
    private String copySql;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private Path exportDir;
    private URI exportUri;
    private long queuedCopyExportCount;
    private Path root;
    private ServerMain serverMain;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ParquetExportRetypedViewBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public long export() throws Exception {
        return "COPY".equals(endpoint) ? copyExport() : httpExport();
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        // JMH skips the trial's @TearDown when its @Setup throws
        try {
            setUpTrial();
        } catch (Throwable th) {
            freeTrialResources();
            throw th;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        try {
            if (copyJob != null) {
                verifyCopyExportsFinished();
            }
        } finally {
            freeTrialResources();
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private String aggregates(String query) throws Exception {
        // Every shape exports ts, sym, price, qty, flags and note; LEADING_COLUMNS leaves out c, the retyped column.
        final String sql = "SELECT count(), min(ts), max(ts), sum(qty), sum(length(note)), count_distinct(sym),"
                + " sum(flags), min(price), max(price)" + ("LEADING_COLUMNS".equals(shape) ? "" : ", sum(c)")
                + " FROM (" + query + ")";
        final StringSink sink = new StringSink();
        try (
                RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
        }
        return sink.toString();
    }

    private long copyExport() throws Exception {
        try (
                RecordCursorFactory factory = compiler.compile(copySql, ctx).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            if (!cursor.hasNext()) {
                throw new IllegalStateException("COPY returned no export id");
            }
        }
        queuedCopyExportCount++;
        if (!copyJob.run()) {
            throw new IllegalStateException("the queued export did not run");
        }
        return Files.size(exportDir.resolve(EXPORT_FILE + ".parquet"));
    }

    private void createData() throws Exception {
        final boolean isRetyped = "RETYPED".equals(source);
        final boolean isRetypedBeforeConversion = isRetyped && "RETYPED_BEFORE_CONVERSION".equals(shape);
        engine.execute(
                "CREATE TABLE " + TABLE + " (ts TIMESTAMP, sym SYMBOL"
                        + ("INDEXED_SYMBOL_FILTER".equals(shape) ? " INDEX" : "")
                        + ", price DOUBLE, qty LONG, flags INT, note VARCHAR, c " + (isRetyped ? "INT" : "LONG")
                        + ") TIMESTAMP(ts) PARTITION BY DAY WAL",
                ctx
        );
        final long step = 86_400_000_000L / rowsPerDay;
        engine.execute(
                "INSERT INTO " + TABLE + " SELECT timestamp_sequence('2024-01-01', " + step + "),"
                        + " rnd_symbol('AAPL', 'MSFT', 'AMZN', 'GOOG', 'META', 'TSLA', 'NVDA', 'NFLX'),"
                        + " rnd_double(), rnd_long(1, 1_000_000, 0), rnd_int(), rnd_varchar(4, 24, 0),"
                        + " (x * 3)::" + (isRetyped ? "INT" : "LONG")
                        + " FROM long_sequence(" + rowsPerDay * DAYS + ")",
                ctx
        );
        drainWal();
        if (isRetypedBeforeConversion) {
            engine.execute("ALTER TABLE " + TABLE + " ALTER COLUMN c TYPE LONG", ctx);
            drainWal();
        }
        engine.execute("ALTER TABLE " + TABLE + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'", ctx);
        drainWal();
        if (isRetyped && !isRetypedBeforeConversion) {
            engine.execute("ALTER TABLE " + TABLE + " ALTER COLUMN c TYPE LONG", ctx);
            drainWal();
        }
        engine.execute("CREATE VIEW v AS (SELECT * FROM " + TABLE + ")", ctx);
        engine.execute("CREATE VIEW v_lead AS (SELECT ts, sym, price, qty, flags, note FROM " + TABLE + ")", ctx);
        engine.execute("CREATE VIEW v_latest AS (SELECT * FROM " + TABLE + " LATEST ON ts PARTITION BY sym)", ctx);
        drainWal();
        try (ViewCompilerJob viewCompilerJob = new ViewCompilerJob(0, engine)) {
            //noinspection StatementWithEmptyBody
            while (viewCompilerJob.run()) ;
        }
        try (TableReader reader = engine.getReader(TABLE)) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                final byte expected = i < 2 ? PartitionFormat.PARQUET : PartitionFormat.NATIVE;
                if (reader.getPartitionFormatFromMetadata(i) != expected) {
                    throw new IllegalStateException("partition " + i + " has format " + reader.getPartitionFormatFromMetadata(i));
                }
            }
        }
    }

    private void drainWal() {
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0)) {
            //noinspection StatementWithEmptyBody
            while (walApplyJob.run()) ;
            if (new CheckWalTransactionsJob(engine).run()) {
                //noinspection StatementWithEmptyBody
                while (walApplyJob.run()) ;
            }
        }
    }

    private ParquetExportMode expectedExportMode() {
        if ("RETYPED".equals(source)) {
            return ParquetExportMode.TEMP_TABLE;
        }
        return switch (shape) {
            case "LATEST_ON", "INDEXED_SYMBOL_FILTER" -> ParquetExportMode.CURSOR_BASED;
            default -> ParquetExportMode.DIRECT_PAGE_FRAME;
        };
    }

    private String exportedQuery() {
        return switch (shape) {
            case "NATIVE_PARTITIONS" -> "SELECT * FROM v WHERE ts >= '2024-01-03'";
            case "LEADING_COLUMNS" -> "SELECT * FROM v_lead";
            case "RETYPED_BEFORE_CONVERSION" -> "SELECT * FROM v";
            case "LATEST_ON" -> "SELECT * FROM v_latest";
            case "INDEXED_SYMBOL_FILTER" -> "SELECT * FROM v WHERE sym = 'AAPL'";
            case "SINGLE_ROW" -> "SELECT * FROM v WHERE ts IN '2024-01-03T00:00:00.000000Z'";
            case "EMPTY_RESULT" -> "SELECT * FROM v WHERE ts IN '2024-01-03T00:00:00.000001Z'";
            default -> throw new IllegalArgumentException("unknown shape: " + shape);
        };
    }

    private void freeTrialResources() throws IOException {
        copyJob = Misc.free(copyJob);
        compiler = Misc.free(compiler);
        ctx = Misc.free(ctx);
        if (serverMain != null) {
            serverMain = Misc.free(serverMain);
            engine = null;
        } else {
            engine = Misc.free(engine);
        }
        if (root != null) {
            try (Stream<Path> paths = Files.walk(root)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException ignore) {
                    }
                });
            }
            root = null;
        }
    }

    private long httpExport() throws IOException {
        return httpExport(OutputStream.nullOutputStream());
    }

    private long httpExport(OutputStream out) throws IOException {
        final HttpURLConnection connection = (HttpURLConnection) exportUri.toURL().openConnection();
        try {
            if (connection.getResponseCode() != 200) {
                throw new IllegalStateException("HTTP " + connection.getResponseCode() + " for " + exportUri);
            }
            long total = 0;
            final byte[] buffer = new byte[1 << 16];
            try (InputStream in = connection.getInputStream()) {
                int n;
                while ((n = in.read(buffer)) > 0) {
                    out.write(buffer, 0, n);
                    total += n;
                }
            }
            return total;
        } finally {
            connection.disconnect();
        }
    }

    private void setUpTrial() throws Exception {
        queuedCopyExportCount = 0;
        root = Files.createTempDirectory("parquet-export-retyped-view-bench-");
        exportDir = Files.createDirectories(root.resolve("export"));
        final Path dbRoot = Files.createDirectories(root.resolve("db"));
        final CairoConfiguration configuration = new DefaultCairoConfiguration(dbRoot.toString()) {
            @Override
            public CharSequence getSqlCopyExportRoot() {
                return exportDir.toString();
            }

            @Override
            public CharSequence getSqlCopyInputRoot() {
                return exportDir.toString();
            }
        };
        engine = new CairoEngine(configuration);
        engine.load();
        // Fixed seed: RETYPED and TWIN tables get identical rows.
        ctx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null, new Rnd(0x5f3759dfL, 0x2545f4914f6cdd1dL), -1, null
        );
        compiler = engine.getSqlCompiler();
        createData();

        final String query = exportedQuery();
        final ParquetExportMode expectedMode = expectedExportMode();
        try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
            final ParquetExportMode mode = ParquetExportMode.determineExportMode(factory, false, ctx);
            if (mode != expectedMode) {
                throw new IllegalStateException("source=" + source + " shape=" + shape + " exports via " + mode + ", expected " + expectedMode);
            }
        }
        final String expectedAggregates = aggregates(query);

        if ("COPY".equals(endpoint)) {
            copySql = "COPY (" + query + ") TO '" + EXPORT_FILE + "' WITH FORMAT PARQUET";
            copyJob = new CopyExportRequestJob(engine);
            copyExport();
            verifyExport(expectedAggregates, EXPORT_FILE + ".parquet");
        } else {
            compiler = Misc.free(compiler);
            ctx = Misc.free(ctx);
            engine = Misc.free(engine);
            final int port = freePort();
            final Map<String, String> env = new HashMap<>();
            env.put("QDB_HTTP_BIND_TO", "127.0.0.1:" + port);
            env.put("QDB_HTTP_MIN_ENABLED", "false");
            env.put("QDB_PG_ENABLED", "false");
            env.put("QDB_LINE_TCP_ENABLED", "false");
            env.put("QDB_LINE_UDP_ENABLED", "false");
            env.put("QDB_TELEMETRY_ENABLED", "false");
            env.put("QDB_CAIRO_SQL_COPY_ROOT", exportDir.toString());
            serverMain = ServerMain.create(root.toString(), env);
            serverMain.start();
            engine = serverMain.getEngine();
            ctx = new SqlExecutionContextImpl(engine, 1).with(
                    engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                    null, null, -1, null
            );
            compiler = engine.getSqlCompiler();
            exportUri = URI.create("http://127.0.0.1:" + port + "/exp?fmt=parquet&query="
                    + URLEncoder.encode(query, StandardCharsets.UTF_8));
            final String fileName = "bench_http.parquet";
            try (OutputStream out = Files.newOutputStream(exportDir.resolve(fileName))) {
                httpExport(out);
            }
            verifyExport(expectedAggregates, fileName);
        }
    }

    private void verifyCopyExportsFinished() throws Exception {
        // CopyExportRequestJob logs a failed export and still reports that it ran.
        final String sql = "SELECT count() FROM \"" + engine.getConfiguration().getSystemTableNamePrefix()
                + "copy_export_log\" WHERE phase = 'success' AND status = 'finished'";
        try (
                RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            final long finishedCount = cursor.hasNext() ? cursor.getRecord().getLong(0) : 0;
            if (finishedCount != queuedCopyExportCount) {
                throw new IllegalStateException(
                        finishedCount + " of " + queuedCopyExportCount + " queued exports finished"
                );
            }
        }
    }

    private void verifyExport(String expectedAggregates, String fileName) throws Exception {
        final String actual = aggregates("SELECT * FROM read_parquet('" + fileName + "')");
        if (!expectedAggregates.equals(actual)) {
            throw new IllegalStateException("export of " + shape + "/" + source + " differs from the query: "
                    + actual + " vs " + expectedAggregates);
        }
        System.out.println("[bench] endpoint=" + endpoint + " shape=" + shape + " source=" + source
                + " rows/day=" + rowsPerDay + " aggregates=" + actual.replace('\n', ' '));
    }
}
