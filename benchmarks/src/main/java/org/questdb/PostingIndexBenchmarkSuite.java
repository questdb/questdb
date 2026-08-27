package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.idx.AbstractParquetPostingIndexReader;
import io.questdb.cairo.idx.BitmapIndexBwdReader;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.idx.BitpackUtils;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.idx.ParquetIndexSeal;
import io.questdb.cairo.idx.ParquetPostingIndexBwdReader;
import io.questdb.cairo.idx.ParquetPostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexBwdReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexNative;
import io.questdb.cairo.idx.PostingIndexReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
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
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * JMH benchmark suite for posting and covering indices.
 * Covers index-comparison, decode, sidecar, SQL, and write benchmarks.
 * Designed to complete in ~5 minutes with {@code @Fork(0)}.
 *
 * <pre>
 * mvn install -pl questdb/core -DskipTests -q
 * mvn package -pl questdb/benchmarks -DskipTests -q
 * java -Xmx4g -Dquestdb.log.level=E -cp questdb/benchmarks/target/benchmarks.jar org.questdb.PostingIndexBenchmarkSuite
 * </pre>
 */
public class PostingIndexBenchmarkSuite {
    /**
     * Copied from core/pom.xml. Without these a forked SqlState or LimitState
     * dies on IllegalAccessError: jdk.internal.vm.ContinuationScope, while the
     * index arms fork fine -- a PARTIAL failure that reads as a successful run
     * with fewer arms.
     */
    private static final String[] JVM_EXPORTS = {
            "--enable-native-access=ALL-UNNAMED",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.time.zone=ALL-UNNAMED",
            "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
    };

    static {
        // Runs in the FORKED jvm as well as this one. main()'s haltInstance()
        // does not: JMH forks run ForkedMain, so from the moment execution
        // became forked every measured JVM had QuestDB's log writer thread
        // live, which the unforked setup had always halted. Quiet the log where
        // the measurement actually happens.
        LogFactory.haltInstance();
    }


    private static final double CLOUD_US_PER_PAGE = 1_000;
    private static final long COL_TXN = COLUMN_NAME_TXN_NONE;
    private static final int[] COVER_REQ_0 = new int[]{0};
    private static final double HDD_US_PER_PAGE = 4_000;
    private static final boolean IS_DELTA = "delta".equals(System.getProperty("questdb.posting.format", "ef"));
    private static final double NVME_US_PER_PAGE = 80;
    private static final int PAGE_SIZE = 4096;
    // The parquet arm names its artifacts <col>.pidx.<indexTxn>.{parquet,_im}.
    // Nothing here publishes a _pm, so the benchmark plays the part of the
    // token: it seals at this index_txn and binds the reader to the same one.
    private static final long PARQUET_INDEX_TXN = 1;
    private static final long PARQUET_PARTITION_TXN = -1;
    // SQL keyword for the posting index type: "POSTING" (EF default) or "POSTING DELTA"
    private static final String POSTING_SQL = IS_DELTA ? "POSTING DELTA" : "POSTING";

    private static final PrintStream out = System.err;

    public static void main(String[] args) throws Exception {
        verifyLadder();
        System.setProperty("questdb.log.level", "E");
        LogFactory.haltInstance();

        // Optional filter: -Dquestdb.suite.bench=<token>
        //   "all"  (default): every benchmark
        //   "core":            commitProfile, decode, index*, sidecarRead,
        //                      sqlQuery, writeInsert (the printSummary set)
        //   "wal":             walFastLag* and walLargePartition*
        //   "wal_o3":          walLargePartitionO3* — O3 commit paths
        //                      (commitDense + rebuildSidecarsByCopy)
        //   "wal_o3_spill":    walLargePartitionO3SpillReseal — reseal cost as
        //                      the spill budget forces mid-stream flushes
        //                      (commitDense's flushAllPendingDense vs seal())
        //   "io":              page-fault analysis only (skip JMH benchmarks)
        //   anything else:     literal regex body, expanded to
        //                      "PostingIndexBenchmarkSuite\.(<token>)$"
        String filter = System.getProperty("questdb.suite.bench", "all");
        String suiteName = PostingIndexBenchmarkSuite.class.getSimpleName();
        String includePattern = switch (filter) {
            case "all" -> suiteName + "\\.";
            case "core" -> suiteName + "\\.(commitProfile|decode|indexPointRead|indexScanRead|"
                    + "indexRangeRead|sidecarRead|sqlQuery|writeInsert)$";
            case "wal" -> suiteName + "\\.(walFastLag|walLargePartition).*";
            case "wal_o3" -> suiteName + "\\.walLargePartitionO3.*";
            case "wal_o3_append" -> suiteName + "\\.walLargePartitionO3AppendInsert.*";
            case "wal_o3_spill" -> suiteName + "\\.walLargePartitionO3SpillReseal.*";
            case "io" -> null;
            default -> suiteName + "\\.(" + filter + ")$";
        };

        Collection<RunResult> results;
        if (includePattern != null) {
            org.openjdk.jmh.runner.options.ChainedOptionsBuilder builder = new OptionsBuilder()
                    .include(includePattern)
                    .resultFormat(ResultFormatType.TEXT);
            // Pin @Param values from the command line, e.g.
            //   -Dquestdb.suite.bench.scenario=P400K
            //   -Dquestdb.suite.bench.format=POSTING,POSTING_PARQUET
            // Without this a focused arm comparison has to run every scenario
            // the state declares. Only pass a param the selected benchmark
            // actually declares; JMH rejects an unknown one.
            for (String param : new String[]{"scenario", "format", "mode", "columnType", "queryType", "storage", "direction"}) {
                String values = System.getProperty("questdb.suite.bench." + param);
                if (values != null) {
                    builder.param(param, values.split(","));
                }
            }
            if ("wal_o3".equals(filter) || "wal_o3_append".equals(filter) || "wal_o3_spill".equals(filter)) {
                // Same exports as the branch below. This branch forked already,
                // so it needed them just as much; the two only differ in
                // iteration counts.
                builder.forks(1)
                        .jvmArgsAppend(JVM_EXPORTS)
                        .warmupIterations(1)
                        .measurementIterations(2);
            } else {
                // Benchmarks contaminate each other in a shared JVM: a 5,000-key
                // scan measured 3.11x SLOWER alongside two others and 1.73x
                // FASTER alone, three runs agreeing within 2%. Forking makes a
                // trustworthy number the default rather than folklore.
                // Five, not three. Three is the minimum at which JMH computes a
                // score error at all, but the estimate is still poor enough to
                // report a CONFIDENT wrong verdict: at three iterations the
                // 16-key point read measured "3.24x slower" with disjoint
                // intervals, and at ten it is 1.07x (3,690+/-41 vs 3,434+/-38).
                // Disjointness is only as trustworthy as the error feeding it.
                // Five costs about two minutes across the suite.
                final int iters = Integer.getInteger("questdb.suite.bench.iterations", 5);
                builder.forks(1)
                        .jvmArgsAppend(JVM_EXPORTS)
                        // Three warmup iterations, not one. One second of
                        // warmup does not settle JIT on these cells, and the
                        // instability shows up as CONFIDENT wrong verdicts
                        // rather than wide error bars: the 16-key range read
                        // reported 3.18x slower with disjoint intervals where a
                        // careful ten-iteration run measures 1.14x
                        // (7,006+/-104 vs 6,172+/-55). More measurement
                        // iterations do not fix an unwarmed measurement.
                        .warmupIterations(3)
                        .warmupTime(org.openjdk.jmh.runner.options.TimeValue.seconds(1))
                        .measurementIterations(iters)
                        .measurementTime(org.openjdk.jmh.runner.options.TimeValue.seconds(1));
            }
            results = new Runner(builder.build()).run();
            printSummary(results);
        }
        // Page-fault projection is part of the broad/IO sweeps; skip it for
        // focused single-benchmark runs (e.g. -Dquestdb.suite.bench=sqlLimit)
        // so iteration stays fast.
        if ("all".equals(filter) || "core".equals(filter) || "io".equals(filter)) {
            runPageFaultAnalysis();
        }
    }

    /**
     * Per-commit write profiling: measures incremental add+commit cost (market data pattern).
     */
    @Benchmark
    public void commitProfile(CommitState s) {
        // Each invocation does one full write cycle: 56 commits of 512 keys × 128 values
        String dir = System.getProperty("java.io.tmpdir") + File.separator + "suite_commit_" + System.nanoTime();
        new File(dir).mkdirs();
        try {
            initPosting(s.config, dir);
            try (Path path = new Path().of(dir)) {
                PostingIndexWriter writer = new PostingIndexWriter(s.config);
                writer.of(path, "test", COL_TXN, false);
                int rowId = 0;
                for (int c = 0; c < CommitState.COMMITS; c++) {
                    for (int k = 0; k < CommitState.KEYS; k++) {
                        for (int v = 0; v < CommitState.VALUES_PER_COMMIT; v++) {
                            writer.add(k, rowId++);
                        }
                    }
                    writer.setMaxValue(rowId - 1);
                    writer.commit();
                }
                writer.seal();
                writer.close();
            }
        } finally {
            deleteDir(dir);
        }
    }

    // ==================================================================================
    // Summary: structured output for feedback/iteration cycles
    // ==================================================================================

    @Benchmark
    public void decode(DecodeState s) {
        BitpackUtils.unpackValuesFrom(s.packedAddr, 0, s.batchSize, s.bitWidth, s.minValue, s.destAddr);
    }

    // ==================================================================================
    // Post-JMH: Page fault projection for covering vs baseline
    // ==================================================================================

    @Benchmark
    public void indexPointRead(IndexState s) {
        try (Path path = new Path().of(s.dir)) {
            IndexReader reader = openReader(s, path);
            try {
                for (int key : s.pointKeys) {
                    try (RowCursor c = reader.getCursor(key, 0, Long.MAX_VALUE)) {
                        while (c.hasNext()) c.next();
                    }
                }
            } finally {
                Misc.free(reader);
            }
        }
    }

    // ==================================================================================
    // Section 1: Index Comparison — Legacy vs Posting, all 7 scenarios
    // ==================================================================================

    @Benchmark
    public void indexRangeRead(IndexState s) {
        try (Path path = new Path().of(s.dir)) {
            IndexReader reader = openReader(s, path);
            try {
                for (int key : s.rangeKeys) {
                    try (RowCursor c = reader.getCursor(key, s.maxRow / 4, s.maxRow * 3 / 4)) {
                        while (c.hasNext()) c.next();
                    }
                }
            } finally {
                Misc.free(reader);
            }
        }
    }

    @Benchmark
    public void indexScanRead(IndexState s) {
        try (Path path = new Path().of(s.dir)) {
            IndexReader reader = openReader(s, path);
            try {
                for (int key = 0; key < s.keyCount; key++) {
                    try (RowCursor c = reader.getCursor(key, 0, Long.MAX_VALUE)) {
                        while (c.hasNext()) c.next();
                    }
                }
            } finally {
                Misc.free(reader);
            }
        }
    }

    @Benchmark
    public long sidecarRead(SidecarState s) {
        long sum = 0;
        boolean covering = s.isParquet || "covering".equals(s.mode);
        int[] requiredCovers = covering ? COVER_REQ_0 : null;
        try (Path path = new Path().of(s.dir)) {
            try (PostingIndexReader reader = openSidecarReader(s, path)) {
                for (int key : s.readKeys) {
                    try (RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE, requiredCovers)) {
                        if (covering && cursor instanceof CoveringRowCursor crc && crc.isCoveredAvailable(0)) {
                            while (crc.hasNext()) {
                                crc.next();
                                sum += switch (s.columnType) {
                                    case "DOUBLE" -> (long) crc.getCoveredDouble(0);
                                    case "FLOAT" -> (long) crc.getCoveredFloat(0);
                                    case "LONG", "DECIMAL64" -> crc.getCoveredLong(0);
                                    case "INT", "DECIMAL32" -> crc.getCoveredInt(0);
                                    case "SHORT" -> crc.getCoveredShort(0);
                                    default -> 0;
                                };
                            }
                        } else {
                            while (cursor.hasNext()) {
                                long rowId = cursor.next();
                                sum += switch (s.columnType) {
                                    case "DOUBLE" -> (long) Unsafe.getDouble(s.colAddr + rowId * 8);
                                    case "FLOAT" -> (long) Unsafe.getFloat(s.colAddr + rowId * 4);
                                    case "LONG", "DECIMAL64" -> Unsafe.getLong(s.colAddr + rowId * 8);
                                    case "INT", "DECIMAL32" -> Unsafe.getInt(s.colAddr + rowId * 4);
                                    case "SHORT" -> Unsafe.getShort(s.colAddr + rowId * 2);
                                    default -> 0;
                                };
                            }
                        }
                    }
                }
            }
        }
        return sum;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public long sqlLimit(LimitState s) throws Exception {
        // One LIMIT query end to end. The first hasNext() triggers
        // fetchAllFrames(); for a covering LIMIT that includes buildAddressCache()
        // materializing the whole matching-key result set up front -- the M1 cost.
        // Reads every projected column so the VARCHAR copy is on the hot path too.
        long sum = 0;
        var meta = s.factory.getMetadata();
        int cols = meta.getColumnCount();
        try (RecordCursor cursor = s.factory.getCursor(LimitState.ctx)) {
            Record rec = cursor.getRecord();
            while (cursor.hasNext()) {
                for (int c = 0; c < cols; c++) {
                    switch (ColumnType.tagOf(meta.getColumnType(c))) {
                        case ColumnType.DOUBLE -> sum += (long) rec.getDouble(c);
                        case ColumnType.VARCHAR -> {
                            var v = rec.getVarcharA(c);
                            sum += v == null ? 0 : v.size();
                        }
                        default -> sum++;
                    }
                }
            }
        }
        return sum;
    }

    @Benchmark
    public long sqlQuery(SqlState s) throws Exception {
        long sum = 0;
        try (RecordCursorFactory factory = s.compiler.compile(s.sql, s.ctx).getRecordCursorFactory()) {
            if (factory == null) return 0;
            try (RecordCursor cursor = factory.getCursor(s.ctx)) {
                Record rec = cursor.getRecord();
                while (cursor.hasNext()) {
                    for (int c = 0, n = factory.getMetadata().getColumnCount(); c < n; c++) {
                        int type = factory.getMetadata().getColumnType(c);
                        sum += switch (ColumnType.tagOf(type)) {
                            case ColumnType.DOUBLE -> (long) rec.getDouble(c);
                            case ColumnType.LONG -> rec.getLong(c);
                            case ColumnType.INT -> rec.getInt(c);
                            default -> 1;
                        };
                    }
                }
            }
        }
        return sum;
    }

    // ==================================================================================
    // Section 2: Decode Throughput
    // ==================================================================================

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void walFastLagInsert(WalFastLagState s) throws Exception {
        // 1s spacing: at JMH's typical invocation rates a full trial
        // (~50000 invocations) advances ~14h of fake time, comfortably
        // within the single 2024-01-01 partition. Wider spacing would
        // cross DAY boundaries and contaminate the seal cost with
        // partition-switch work.
        int batchOffsetSeconds = (s.batchCounter++) + 1;
        String sql = "INSERT INTO walbench(ts, sym, " + s.extraColumns + ") " +
                "SELECT dateadd('u', x::INT, dateadd('s', " + batchOffsetSeconds + ", '2024-01-01T00:00:00.000000Z'::TIMESTAMP)), " +
                "rnd_symbol(" + s.keyCount + ", 4, 8, 0), " + s.extraValues + " " +
                "FROM long_sequence(" + s.batchRows + ")";
        s.engine.execute(sql, s.ctx);
        s.applyJob.drain(0);
        s.checkJob.run();
        s.applyJob.drain(0);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long walFastLagInsertAndQuery(WalFastLagState s) throws Exception {
        // Combined: one fast-lag commit followed by one point query against
        // the freshly-committed state. Headline number for "what does a
        // tick of work cost when a query lands in the unsealed window".
        walFastLagInsert(s);
        return walFastLagQuery(s);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public long walFastLagQuery(WalFastLagState s) throws Exception {
        // Single point query against the table state left by previous
        // invocations. Compiles fresh because the WAL apply path bumps
        // table metadata version and invalidates cached factories. The
        // batchRows axis is irrelevant here but JMH still varies it;
        // treat those rows as replicate measurements when reading results.
        String key = s.queryKeys[(s.queryCounter++) & (s.queryKeys.length - 1)];
        long sum = 0;
        try (RecordCursorFactory f = s.compiler.compile(
                "SELECT count() FROM walbench WHERE sym = '" + key + "'", s.ctx
        ).getRecordCursorFactory()) {
            try (RecordCursor c = f.getCursor(s.ctx)) {
                while (c.hasNext()) sum += c.getRecord().getLong(0);
            }
        }
        return sum;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public long walFastLagQueryAtGen(WalFastLagQueryGenState s) throws Exception {
        // Pure point-query against state pre-built to a target unsealed
        // gen count. Setup ran preload + unsealedGens fast-lag commits;
        // benchmark adds nothing, so gen count is stable across iters.
        // This isolates the per-key cursor cost as a function of unsealed
        // gens (each commit produces one gen via extendHead).
        String key = s.queryKeys[(s.queryCounter++) & (s.queryKeys.length - 1)];
        long sum = 0;
        try (RecordCursorFactory f = s.compiler.compile(
                "SELECT count() FROM walbench WHERE sym = '" + key + "'", s.ctx
        ).getRecordCursorFactory()) {
            try (RecordCursor c = f.getCursor(s.ctx)) {
                while (c.hasNext()) sum += c.getRecord().getLong(0);
            }
        }
        return sum;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void walLargePartitionInsert(WalLargePartitionState s) throws Exception {
        // One additional 100k-row commit on top of the preloaded partition.
        // Partition grows by ~1% per JMH iteration, so the measured cost
        // reflects insert latency at approximately partitionSize rows.
        int batchOffsetUs = (s.batchCounter++) + WalLargePartitionState.PRELOAD_TS_LIMIT_US + 1;
        String sql = "INSERT INTO walbench(ts, sym, " + s.extraColumns + ") " +
                "SELECT dateadd('u', x::INT + " + batchOffsetUs + ", '2024-01-01T00:00:00.000000Z'::TIMESTAMP), " +
                "rnd_symbol(" + s.keyCount + ", 4, 8, 0), " + s.extraValues + " " +
                "FROM long_sequence(" + WalLargePartitionState.BATCH_ROWS + ")";
        s.engine.execute(sql, s.ctx);
        s.applyJob.drain(0);
        s.checkJob.run();
        s.applyJob.drain(0);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 2)
    public void walLargePartitionO3AppendInsert(WalLargePartitionO3AppendState s) throws Exception {
        // Pure-append O3: insert BATCH_ROWS rows into the FIRST partition
        // (which is not the last partition, because a sentinel row in day 2
        // makes day 1 non-last). Timestamps land strictly after day 1's
        // existing max ts, so partitionMutates=false and
        // sealPostingIndexForPartition takes the canSkipRebuild=true branch:
        //   - covering: rollbackConditionally + rebuildSidecars
        //               (single dense gen -> rebuildSidecarsByCopy memcpy)
        //   - non-covering: rollbackConditionally + sealIfMultiGen(threshold)
        //                   (seal entirely skipped until gen count crosses
        //                   cairo.posting.seal.gen.threshold)
        // Per-iteration setup keeps day-1's pre-insert state stable.
        String sql = "INSERT INTO walbench(ts, sym, " + s.extraColumns + ") " +
                "SELECT dateadd('u', x::INT, dateadd('h', 14, '2024-01-01T00:00:00.000000Z'::TIMESTAMP)), " +
                "rnd_symbol(" + s.keyCount + ", 4, 8, 0), " + s.extraValues + " " +
                "FROM long_sequence(" + WalLargePartitionO3AppendState.BATCH_ROWS + ")";
        s.engine.execute(sql, s.ctx);
        s.applyJob.drain(0);
        s.checkJob.run();
        s.applyJob.drain(0);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 2)
    public void walLargePartitionO3Insert(WalLargePartitionO3State s) throws Exception {
        // O3 insert: BATCH_ROWS rows with timestamps spread randomly within
        // the preloaded partition's time range. Forces
        // sealPostingIndexForPartition's canSkipRebuild=false branch:
        //   - covering: discardForRebuild + index + commitDense +
        //               configureCovering + rebuildSidecars (single dense gen
        //               -> rebuildSidecarsByCopy memcpy)
        //   - non-covering: discardForRebuild + index + commitDense
        // Each iteration is a single shot on a freshly preloaded partition
        // (Level.Iteration setup), isolating the measurement from
        // partition-size growth and from JIT/GC/page-cache state.
        String sql = "INSERT INTO walbench(ts, sym, " + s.extraColumns + ") " +
                "SELECT dateadd('u', rnd_int(1, " + WalLargePartitionO3State.PRELOAD_TS_LIMIT_US +
                ", 0), '2024-01-01T00:00:00.000000Z'::TIMESTAMP), " +
                "rnd_symbol(" + s.keyCount + ", 4, 8, 0), " + s.extraValues + " " +
                "FROM long_sequence(" + WalLargePartitionO3State.BATCH_ROWS + ")";
        s.engine.execute(sql, s.ctx);
        s.applyJob.drain(0);
        s.checkJob.run();
        s.applyJob.drain(0);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 2)
    public void walLargePartitionO3SpillReseal(WalLargePartitionO3SpillState s) throws Exception {
        // Same O3-mutating insert as walLargePartitionO3Insert (forces
        // sealPostingIndexForPartition's canSkipRebuild=false reseal:
        // discardForRebuild + index + commitDense + rebuildSidecars), but with
        // cairo.posting.index.indexer.spill.bytes.max parameterised so the
        // reseal's index() loop spills a controlled number of times:
        //   - spillBytesMax=256MiB: no mid-stream flush. commitDense stays on
        //     flushAllPendingDense (the unchanged fast path) -- a no-op for the
        //     fix, so this column is the no-regression baseline.
        //   - spillBytesMax=2MiB / 256KiB: re-indexing the ~1M-row partition
        //     trips compactIfOverBudget, so commitDense routes through seal()
        //     (flushAllPending + sealFull consolidation). This is the path the
        //     fix added; before the fix it crashed (covering) or dropped rows
        //     (non-covering), so there is no pre-fix number to compare against.
        // Single shot on a freshly preloaded partition (Level.Iteration setup).
        String sql = "INSERT INTO walbench(ts, sym, " + s.extraColumns + ") " +
                "SELECT dateadd('u', rnd_int(1, " + WalLargePartitionO3SpillState.PRELOAD_TS_LIMIT_US +
                ", 0), '2024-01-01T00:00:00.000000Z'::TIMESTAMP), " +
                "rnd_symbol(" + s.keyCount + ", 4, 8, 0), " + s.extraValues + " " +
                "FROM long_sequence(" + WalLargePartitionO3SpillState.BATCH_ROWS + ")";
        s.engine.execute(sql, s.ctx);
        s.applyJob.drain(0);
        s.checkJob.run();
        s.applyJob.drain(0);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public long walLargePartitionQuery(WalLargePartitionState s) throws Exception {
        // Point-query against a partition pre-loaded to partitionSize rows.
        // No insert during measurement, so the partition stays at the
        // configured size across all iterations of a trial.
        String key = s.queryKeys[(s.queryCounter++) & (s.queryKeys.length - 1)];
        long sum = 0;
        try (RecordCursorFactory f = s.compiler.compile(
                "SELECT count() FROM walbench WHERE sym = '" + key + "'", s.ctx
        ).getRecordCursorFactory()) {
            try (RecordCursor c = f.getCursor(s.ctx)) {
                while (c.hasNext()) sum += c.getRecord().getLong(0);
            }
        }
        return sum;
    }

    /**
     * Cost of SEALING a covering index, native chain against parquet form, over
     * an identical set of postings.
     * <p>
     * The read benchmarks say what the parquet form costs to query; this is the
     * other half. It matters more here than for the native chain because a
     * parquet-form index is republished WHOLESALE -- there is no append path
     * into a parquet partition, so every commit that touches one re-seals the
     * whole thing.
     */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void sealCost(SealState s) {
        s.seal();
    }

    @Benchmark
    public void writeInsert(WriteState s) throws Exception {
        s.engine.execute(s.ddl, s.ctx);
        s.engine.execute(s.insertSql, s.ctx);
        s.engine.releaseAllWriters();
        s.engine.execute("DROP TABLE wbench", s.ctx);
    }

    private static DefaultCairoConfiguration benchConfig(String root) {
        return new DefaultCairoConfiguration(root) {
            @Override
            public int getPostingIndexParquetCompressionCodec() {
                // -Dquestdb.idx.codec=<parquet codec ordinal>, so a sweep can
                // vary the index's codec without a rebuild. 0 is UNCOMPRESSED.
                return Integer.getInteger("questdb.idx.codec", super.getPostingIndexParquetCompressionCodec());
            }

            @Override
            public int getPostingIndexParquetDataPageSize() {
                // Unset means "whatever the codec implies", which is the
                // pairing the product default encodes.
                final Integer explicit = Integer.getInteger("questdb.idx.page");
                return explicit != null ? explicit : super.getPostingIndexParquetDataPageSize();
            }

            @Override
            public int getPostingIndexParquetMaxKeysPerRowGroup() {
                return Integer.getInteger("questdb.idx.rgkeys", super.getPostingIndexParquetMaxKeysPerRowGroup());
            }

            @Override
            public int getPostingIndexParquetMinRowsPerRowGroup() {
                return Integer.getInteger("questdb.idx.rgminrows", super.getPostingIndexParquetMinRowsPerRowGroup());
            }

            @Override
            public byte getPostingIndexRowIdEncoding() {
                return IS_DELTA ? PostingIndexUtils.ENCODING_DELTA : PostingIndexUtils.ENCODING_ADAPTIVE;
            }
        };
    }

    private static GenericRecordMetadata buildCoverMetadata(int colType) {
        GenericRecordMetadata meta = new GenericRecordMetadata();
        for (int i = 0; i <= 2; i++) {
            int t = (i == 2) ? colType : ColumnType.LONG;
            meta.add(new TableColumnMetadata("c" + i, t, IndexType.NONE, 0, false, null, i, false));
        }
        return meta;
    }

    // ==================================================================================
    // Section 3: Covering Sidecar Compression
    // ==================================================================================

    private static int[] buildRoundRobin(int totalRows, int keyCount) {
        int[] a = new int[totalRows];
        for (int i = 0; i < totalRows; i++) a[i] = i % keyCount;
        return a;
    }

    private static int[] buildShuffled(int totalRows, int keyCount) {
        int[] a = new int[totalRows];
        for (int i = 0; i < totalRows; i++) a[i] = i % keyCount;
        Random rng = new Random(42);
        for (int i = totalRows - 1; i > 0; i--) {
            int j = rng.nextInt(i + 1);
            int tmp = a[i];
            a[i] = a[j];
            a[j] = tmp;
        }
        return a;
    }

    // ==================================================================================
    // Section 4: SQL Covering Queries
    // ==================================================================================

    private static int[] buildZipfian(int totalRows, int keyCount) {
        double[] cdf = new double[keyCount];
        double sum = 0;
        for (int i = 0; i < keyCount; i++) {
            sum += 1.0 / (i + 1);
            cdf[i] = sum;
        }
        for (int i = 0; i < keyCount; i++) cdf[i] /= sum;
        int[] a = new int[totalRows];
        Random rng = new Random(42);
        for (int i = 0; i < totalRows; i++) {
            int key = Arrays.binarySearch(cdf, rng.nextDouble());
            if (key < 0) key = -key - 1;
            a[i] = Math.min(key, keyCount - 1);
        }
        return a;
    }

    private static void deleteDir(String path) {
        File dir = new File(path);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) for (File f : files) f.delete();
            dir.delete();
        }
    }

    // ==================================================================================
    // Section 5: Write Overhead
    // ==================================================================================

    private static void deleteDirRecursive(File dir) {
        try {
            Files.walkFileTree(dir.toPath(), new SimpleFileVisitor<>() {
                @Override
                public @NotNull FileVisitResult postVisitDirectory(java.nio.file.@NotNull Path d, IOException e) throws IOException {
                    Files.delete(d);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public @NotNull FileVisitResult visitFile(java.nio.file.@NotNull Path file, @NotNull BasicFileAttributes a) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException ignored) {
        }
    }

    private static void doCovBaselineRead(CairoConfiguration config, String dir, int[] keys,
                                          long colAddr, CoverType ct) {
        try (Path path = new Path().of(dir)) {
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(config, path, "test", COL_TXN, 0, 0)) {
                for (int key : keys) {
                    try (RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE)) {
                        while (cursor.hasNext()) {
                            long rowId = cursor.next();
                            switch (ct) {
                                case DOUBLE -> Unsafe.getDouble(colAddr + rowId * 8);
                                case FLOAT -> Unsafe.getFloat(colAddr + rowId * 4);
                                case LONG -> Unsafe.getLong(colAddr + rowId * 8);
                                case INT -> Unsafe.getInt(colAddr + rowId * 4);
                            }
                        }
                    }
                }
            }
        }
    }

    private static void doCovCoveringRead(CairoConfiguration config, String dir, int[] keys, CoverType ct) {
        GenericRecordMetadata meta = buildCoverMetadata(ct.columnType);
        try (ColumnVersionReader cvr = new ColumnVersionReader();
             Path path = new Path().of(dir)) {
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    config, path, "test", COL_TXN, 0, 0, meta, cvr, 0)) {
                for (int key : keys) {
                    try (RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE, COVER_REQ_0)) {
                        if (cursor instanceof CoveringRowCursor crc && crc.isCoveredAvailable(0)) {
                            while (crc.hasNext()) {
                                crc.next();
                                switch (ct) {
                                    case DOUBLE -> crc.getCoveredDouble(0);
                                    case FLOAT -> crc.getCoveredFloat(0);
                                    case LONG -> crc.getCoveredLong(0);
                                    case INT -> crc.getCoveredInt(0);
                                }
                            }
                        } else {
                            while (cursor.hasNext()) cursor.next();
                        }
                    }
                }
            }
        }
    }

    // ==================================================================================
    // Shared utilities
    // ==================================================================================

    private static long getDirectorySize(String path) {
        File dir = new File(path);
        long size = 0;
        File[] files = dir.listFiles();
        if (files != null) for (File f : files) size += f.length();
        return size;
    }

    private static void initPosting(CairoConfiguration config, String dir) {
        try (Path path = new Path().of(dir)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(ff, PostingIndexUtils.keyFileName(path, "test", COL_TXN),
                    MemoryTag.MMAP_DEFAULT, config.getWriterFileOpenOpts())) {
                PostingIndexWriter.initKeyMemory(mem);
            }
            // Fresh file: sealTxn starts equal to postingColumnNameTxn (no seal performed yet).
            ff.touch(PostingIndexUtils.valueFileName(path.trimTo(plen), "test", COL_TXN, COL_TXN));
        }
    }

    private static IndexReader openReader(CairoConfiguration config, Path path, boolean posting) {
        return posting
                ? new PostingIndexFwdReader(config, path, "test", COL_TXN, -1, 0)
                : new BitmapIndexFwdReader(config, path, "test", COL_TXN, -1, 0);
    }

    /**
     * Opens the arm {@code s} was built for. The parquet arm is bound through
     * {@code ofParquet} because its artifacts are named by {@code index_txn},
     * which a plain {@code of()} has no way to carry - the native readers find
     * their files from the column name alone.
     */
    private static IndexReader openReader(IndexState s, Path path) {
        final boolean backward = "BACKWARD".equals(s.direction);
        if (!s.isParquet) {
            if (!backward) {
                return openReader(s.config, path, s.isPosting);
            }
            return s.isPosting
                    ? new PostingIndexBwdReader(s.config, path, "test", COL_TXN, -1, 0, null, null, 0)
                    : new BitmapIndexBwdReader(s.config, path, "test", COL_TXN, -1, 0);
        }
        AbstractParquetPostingIndexReader reader = backward
                ? new ParquetPostingIndexBwdReader()
                : new ParquetPostingIndexFwdReader();
        try {
            reader.ofParquet(
                    s.config, path, "test", COL_TXN, PARQUET_PARTITION_TXN, 0,
                    s.parquetMetadata, s.parquetCvr, 0, PARQUET_INDEX_TXN, s.imFileSize);
            assertDirection(reader, backward, s);
            return reader;
        } catch (Throwable th) {
            Misc.free(reader);
            throw th;
        }
    }

    /**
     * A direction axis that does not actually change the reader is 90 cells
     * measuring nothing twice. Assert the class, not the intent.
     */
    private static void assertDirection(IndexReader reader, boolean backward, IndexState s) {
        final boolean isBwd = reader instanceof ParquetPostingIndexBwdReader;
        if (isBwd != backward) {
            throw new IllegalStateException(
                    "arm " + s.format + "/" + s.scenario + "/" + s.direction
                            + " asked for direction=" + s.direction + " but bound a "
                            + reader.getClass().getSimpleName()
                            + "; this cell would measure the wrong direction while reporting the right one");
        }
    }

    /**
     * Storage arms for the SQL benchmarks, which decompose the parquet form's
     * cost into its two independent parts.
     * <p>
     * {@code native} is a native partition with a native index. {@code
     * parquet_data} converts the partition but keeps the native index, whose
     * sidecars are hard-linked into the parquet directory. {@code
     * parquet_index} converts the partition AND seals the index into parquet.
     * Comparing the first two isolates the cost of the parquet DATA format;
     * comparing the last two isolates the cost of the parquet INDEX form, which
     * is the thing under test. A single native-vs-parquet_index number confounds
     * the two.
     */
    /** Query types whose table covers a VARCHAR, which the parquet seal refuses. */
    private static final java.util.Set<String> VARCHAR_COVERED_QUERIES = java.util.Set.of(
            "varchar_fsst", "varchar_non_covering", "varchar_in_covering",
            "bulk_covering", "bulk_non_covering"
    );
    /** Timestamp of the one-row trailing partition every SQL table carries. */
    private static final String TRAILING_TS = "2024-06-01T00:00:00.000000Z";
    private static final String STORAGE_NATIVE = "native";
    private static final String STORAGE_PARQUET_DATA = "parquet_data";
    private static final String STORAGE_PARQUET_INDEX = "parquet_index";

    /**
     * Converts every partition of {@code table} to parquet.
     * <p>
     * Under {@code parquet_index} the seal writes the covering index into
     * parquet as part of the conversion, so this is also where a refusal would
     * surface -- which is why the caller verifies the artifacts afterwards
     * rather than trusting the statement to have done what the property asked.
     */
    private static void convertPartitionsToParquet(
            SqlCompilerImpl compiler, SqlExecutionContextImpl ctx, String table
    ) throws Exception {
        // Everything before the trailing partition, which stays native in every
        // arm because the active partition cannot be converted.
        compiler.compile("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '"
                + TRAILING_TS + '\'', ctx).execute(null).await();
    }

    /**
     * Fails unless {@code table}'s partitions carry a parquet-form covering
     * index, that is a {@code <col>.pidx.<indexTxn>.parquet} artifact.
     * <p>
     * The seal REFUSES var-size and symbol covered columns, and a refused seal
     * leaves the native chain in place. Without this check the
     * {@code parquet_index} arm would quietly measure the native index and
     * report it as the parquet one -- the arm would be vacuous, and vacuously
     * fast in exactly the way that looks like a good result.
     */
    private static void assertParquetIndexPresent(CairoEngine engine, String table) {
        final java.io.File tableDir = new java.io.File(engine.getConfiguration().getDbRoot(), engine.verifyTableName(table).getDirName());
        final java.io.File[] partitions = tableDir.listFiles(java.io.File::isDirectory);
        if (partitions == null) {
            throw new IllegalStateException("no partition directories under " + tableDir);
        }
        for (java.io.File partition : partitions) {
            final String[] pidx = partition.list((d, n) -> n.contains(".pidx.") && n.endsWith(".parquet"));
            if (pidx != null && pidx.length > 0) {
                return;
            }
        }
        throw new IllegalStateException(
                "table " + table + " has no parquet-form covering index; the seal refused it and left the"
                        + " native chain, so this arm would measure the native index while claiming to be parquet");
    }

    /**
     * Counts every posting the arm can reach and fails the trial if it is not
     * {@code expected}. Without this an arm that resolves no postings at all -
     * a mis-bound {@code index_txn}, a key space read as empty - reports as the
     * fastest in the suite, because returning nothing is very quick. Runs once
     * per trial, so it does not enter the measurement.
     */
    private static void verifyArmPostingCount(IndexState s, long expected) {
        long seen = 0;
        try (Path path = new Path().of(s.dir)) {
            IndexReader reader = openReader(s, path);
            try {
                for (int key = 0; key < s.keyCount; key++) {
                    try (RowCursor c = reader.getCursor(key, 0, Long.MAX_VALUE)) {
                        while (c.hasNext()) {
                            c.next();
                            seen++;
                        }
                    }
                }
            } finally {
                Misc.free(reader);
            }
        }
        if (seen != expected) {
            throw new IllegalStateException(
                    "arm " + s.format + "/" + s.scenario + " resolved " + seen
                            + " postings, expected " + expected
                            + " - the arms are not indexing the same rows, so any ratio from them is meaningless");
        }
    }

    /**
     * Opens the covered-gather arm {@code s} was built for: the native covering
     * reader for {@code baseline}/{@code covering}, the parquet-form one for
     * {@code covering_parquet}.
     */
    private static PostingIndexReader openSidecarReader(SidecarState s, Path path) {
        if (!s.isParquet) {
            return new PostingIndexFwdReader(
                    s.config, path, "test", COLUMN_NAME_TXN_NONE, 0, 0,
                    s.coverMetadata, s.cvr, 0);
        }
        ParquetPostingIndexFwdReader reader = new ParquetPostingIndexFwdReader();
        try {
            reader.ofParquet(
                    s.config, path, "test", COLUMN_NAME_TXN_NONE, PARQUET_PARTITION_TXN, 0,
                    s.coverMetadata, s.cvr, 0, PARQUET_INDEX_TXN, s.imFileSize);
            return reader;
        } catch (Throwable th) {
            Misc.free(reader);
            throw th;
        }
    }

    /**
     * Cumulative row counts for a synthetic {@code data.parquet}, one more entry
     * than row groups and starting at 0, cut at the same row-group size the
     * partition encoder would have used. The seal turns these into the zone maps
     * the reader prunes with, so a single whole-partition group would hand the
     * parquet arm a fixture with nothing to prune and flatter it.
     */
    private static LongList syntheticRowGroupBoundaries(CairoConfiguration config, long totalRows) {
        // 0 is not "one row per group" - it is the encoder's sentinel for "use
        // my own default", which is 512*512. Taking it literally builds one
        // boundary per row, and since the seal stores every boundary that alone
        // inflated the _im to 8 bytes/row and made the parquet arm look far
        // worse than it is.
        final int configured = config.getPartitionEncoderParquetRowGroupSize();
        final long groupSize = configured > 0 ? configured : 512L * 512L;
        // Cumulative row COUNTS, which start at 0 -- not row ids. Offsetting
        // them by firstRowId is what S8, whose rows start at 1e9, rejects with
        // "first data row group boundary must be 0".
        final LongList boundaries = new LongList();
        for (long cum = 0; cum < totalRows; cum += groupSize) {
            boundaries.add(cum);
        }
        boundaries.add(totalRows);
        return boundaries;
    }

    /**
     * Seals a parquet-form covering index over {@code keys} into {@code dir},
     * mirroring what the native arm writes with {@link PostingIndexWriter}.
     *
     * @return the committed {@code _im} size, which {@code ofParquet} cross-checks
     */
    private static long sealParquetArm(
            CairoConfiguration config,
            String dir,
            int[] keys,
            int totalRows,
            int keyCount,
            long firstRowId,
            ObjList<CharSequence> coveredNames,
            IntList coveredTypes,
            IntList coveredWriterIndices,
            LongList coveredAddrs,
            LongList coveredColumnTops
    ) {
        return sealParquetArm(config, dir, keys, totalRows, keyCount, firstRowId,
                coveredNames, coveredTypes, coveredWriterIndices, coveredAddrs, coveredColumnTops,
                PARQUET_INDEX_TXN);
    }

    private static long sealParquetArm(
            CairoConfiguration config,
            String dir,
            int[] keys,
            int totalRows,
            int keyCount,
            long firstRowId,
            ObjList<CharSequence> coveredNames,
            IntList coveredTypes,
            IntList coveredWriterIndices,
            LongList coveredAddrs,
            LongList coveredColumnTops,
            long indexTxn
    ) {
        final DirectIntList rowKeys = new DirectIntList(totalRows, MemoryTag.NATIVE_DEFAULT);
        try (Path path = new Path().of(dir)) {
            for (int i = 0; i < totalRows; i++) {
                rowKeys.add(keys[i]);
            }
            return ParquetIndexSeal.seal(
                    config,
                    config.getFilesFacade(),
                    path,
                    "test",
                    indexTxn,
                    keyCount,
                    rowKeys,
                    firstRowId,
                    totalRows,
                    coveredNames,
                    coveredTypes,
                    coveredWriterIndices,
                    coveredAddrs,
                    coveredColumnTops,
                    syntheticRowGroupBoundaries(config, totalRows)
            );
        } finally {
            Misc.free(rowKeys);
        }
    }

    /**
     * Every state class must agree on what a scenario label means. This runs at
     * startup rather than living in a test, because the benchmarks module has no
     * test harness -- and a check that runs on every invocation is a stronger
     * guarantee than a test nobody executes.
     */
    private static void verifyLadder() {
        for (Ladder l : Ladder.values()) {
            if (l.streaming()) {
                continue;
            }
            if (l.keyCount() <= 0 || l.totalRows() <= 0) {
                throw new IllegalStateException("ladder " + l + " has non-positive shape");
            }
            if (l.keyCount() > l.totalRows()) {
                throw new IllegalStateException(
                        "ladder " + l + " asks for more keys (" + l.keyCount()
                                + ") than rows (" + l.totalRows() + "), so most keys would hold no posting");
            }
            if (l.commitInterval() <= 0 || l.commitInterval() > l.totalRows()) {
                throw new IllegalStateException("ladder " + l + " has out-of-range commitInterval");
            }
        }
    }

    /**
     * The score of the one result whose key names {@code bench} and every one of
     * {@code must}, or null when absent. Matching whole {@code /}-delimited
     * SEGMENTS, not substrings -- a substring test made "POSTING" also match
     * "POSTING_PARQUET", so the native arm's lookup could return the parquet
     * row. Matching segments rather than rebuilding the key means adding a
     * @Param does not silently empty a table:
     * the key is params joined in declaration order, so a positional lookup
     * breaks the moment a new axis is inserted ahead of an existing one.
     *
     * @return {@code {score, error}}, or null if no key matched
     */
    private static double[] cell(Map<String, double[]> m, String bench, String... must) {
        outer:
        for (Map.Entry<String, double[]> e : m.entrySet()) {
            final String k = e.getKey();
            if (!k.startsWith(bench + "/")) {
                continue;
            }
            final String[] seg = k.split("/");
            for (String token : must) {
                boolean hit = false;
                for (int i = 1; i < seg.length; i++) {
                    if (seg[i].equals(token)) {
                        hit = true;
                        break;
                    }
                }
                if (!hit) {
                    continue outer;
                }
            }
            return e.getValue();
        }
        return null;
    }

    /**
     * A ratio of {@code arm} to {@code base}, or {@code "~"} when the two error
     * intervals overlap.
     * <p>
     * At three measurement iterations the intervals are still wide. Printing a bare
     * ratio would turn that noise into a reported regression, and detecting
     * regressions is the only reason this table exists.
     */
    private static String ratio(double[] base, double[] arm) {
        if (base == null || arm == null || base[0] <= 0 || arm[0] <= 0) {
            return "-";
        }
        final double r = arm[0] / base[0];
        final double spread = r >= 1 ? r : 1 / r;
        final boolean noError = Double.isNaN(base[1]) || Double.isNaN(arm[1]);
        // JMH computes no error below three iterations, and without one there is
        // no way to separate a difference from noise.
        final boolean disjoint = !noError
                && ((base[0] - base[1]) > (arm[0] + arm[1]) || (arm[0] - arm[1]) > (base[0] + base[1]));
        if (disjoint) {
            return r >= 1 ? String.format("%.2fF", r) : String.format("%.2fS", 1 / r);
        }
        // Overlapping intervals mean two different things and conflating them
        // misleads. A small gap really is parity. A LARGE gap whose intervals
        // still overlap is a noisy cell, not an equal one -- reporting 10,222 vs
        // 4,048 as "no signal" reads as parity when it is actually an unmeasured
        // 2.5x. Flag those for a deeper re-run instead.
        if (spread < 1.25) {
            return "~";
        }
        return String.format("%.1f%s?", spread, r >= 1 ? "F" : "S");
    }

    private static void printSummary(Collection<RunResult> results) {
        // Index results by benchmark name and params
        Map<String, Double> scores = new LinkedHashMap<>();
        Map<String, double[]> cells = new LinkedHashMap<>();
        for (RunResult rr : results) {
            String label = rr.getParams().getBenchmark().replace("org.questdb.PostingIndexBenchmarkSuite.", "");
            Map<String, String> params = rr.getParams().getParamsKeys().stream()
                    .collect(LinkedHashMap::new, (m, k) -> m.put(k, rr.getParams().getParam(k)), Map::putAll);
            String key = label + params.values().stream()
                    .filter(v -> !"N/A".equals(v))
                    .reduce("", (a, v) -> a + "/" + v);
            scores.put(key, rr.getPrimaryResult().getScore());
            cells.put(key, new double[]{rr.getPrimaryResult().getScore(), rr.getPrimaryResult().getScoreError()});
        }

        out.println();
        out.println("╔══════════════════════════════════════════════════════════════════════════════════╗");
        out.printf("║              POSTING INDEX BENCHMARK SUMMARY  [encoding: %-5s]                  ║%n", IS_DELTA ? "DELTA" : "EF");
        out.println("╚══════════════════════════════════════════════════════════════════════════════════╝");

        // --- Decode ---
        out.println();
        out.println("── Decode Throughput (ops/s, higher=better) ──────────────────────────────────────");
        out.printf("  %-8s", "batch");
        for (String bw : new String[]{"1", "8", "12", "16", "20", "32"}) out.printf(" %12s", bw + "-bit");
        out.println();
        for (String batch : new String[]{"64", "256", "1024"}) {
            out.printf("  %-8s", batch);
            for (String bw : new String[]{"1", "8", "12", "16", "20", "32"}) {
                Double v = scores.get("decode/" + batch + "/" + bw);
                out.printf(" %,12.0f", v != null ? v : 0);
            }
            out.println();
        }

        // --- What converting to parquet costs ---
        // The one question this suite exists to answer. F = parquet faster,
        // S = parquet slower, ~ = the two error intervals overlap so the
        // difference is not resolvable at this sample count (re-run that one
        // cell with -Dquestdb.suite.bench.iterations=10 to settle it).
        out.println();
        out.println("── Converting to parquet: native index vs parquet index ──────────────────────────");
        out.println("   ops/s, higher=better.  F=parquet faster  S=parquet SLOWER\n   ~=parity   N.NS?=apparent gap but too noisy to confirm; re-run that cell with\n   -Dquestdb.suite.bench.iterations=10");
        final String[] rungs = {"P400K", "S4", "S6", "S7"};
        final String[] keyLabels = {"16", "2,000", "200,000", "1,000,000"};
        for (String bench : new String[]{"indexPointRead", "indexScanRead", "indexRangeRead"}) {
            for (String dir : new String[]{"FORWARD", "BACKWARD"}) {
                out.printf("%n  %s (%s):%n", bench, dir);
                out.printf("  %-12s %12s %12s %8s%n", "keys", "native", "parquet", "verdict");
                for (int i = 0; i < rungs.length; i++) {
                    double[] nat = cell(cells, bench, "POSTING", rungs[i], dir);
                    double[] pq = cell(cells, bench, "POSTING_PARQUET", rungs[i], dir);
                    if (nat == null || pq == null) {
                        continue;
                    }
                    out.printf("  %-12s %,12.0f %,12.0f %8s%n", keyLabels[i], nat[0], pq[0], ratio(nat, pq));
                }
            }
        }

        // --- The same question at SQL level ---
        out.println();
        out.println("── Converting to parquet: SQL ────────────────────────────────────────────────────");
        out.println("   'data' = cost of the parquet DATA format (native -> parquet_data)");
        out.println("   'index' = cost of the parquet INDEX form (parquet_data -> parquet_index)");
        out.println("   Only 'index' is attributable to this branch.");
        out.printf("  %-16s %-10s %11s %11s %11s %9s %9s%n",
                "query", "keys", "native", "pq-data", "pq-index", "data", "index");
        for (String q : new String[]{"covering_where", "latest_on", "latest_on_indexed"}) {
            for (int i = 0; i < 3; i++) {
                double[] nat = cell(cells, "sqlQuery", q, rungs[i], STORAGE_NATIVE);
                double[] dat = cell(cells, "sqlQuery", q, rungs[i], STORAGE_PARQUET_DATA);
                double[] idx = cell(cells, "sqlQuery", q, rungs[i], STORAGE_PARQUET_INDEX);
                if (nat == null || dat == null || idx == null) {
                    continue;
                }
                out.printf("  %-16s %-10s %,11.0f %,11.0f %,11.0f %9s %9s%n",
                        q, keyLabels[i], nat[0], dat[0], idx[0], ratio(nat, dat), ratio(dat, idx));
            }
        }
        out.println();

        // --- Sidecar ---
        out.println("── Sidecar Read Throughput (ops/s, higher=better) ────────────────────────────────");
        out.printf("  %-8s %10s %10s %8s%n", "type", "baseline", "covering", "ratio");
        for (String t : new String[]{"DOUBLE", "FLOAT", "LONG", "INT", "DECIMAL64", "DECIMAL32", "SHORT"}) {
            Double base = scores.get("sidecarRead/" + t + "/baseline");
            Double cov = scores.get("sidecarRead/" + t + "/covering");
            if (base != null && cov != null) {
                out.printf("  %-8s %,10.0f %,10.0f %7.2fx%n", t, base, cov, cov / base);
            }
        }

        // --- SQL Queries ---
        out.println();
        out.println("── SQL Queries (ops/s, higher=better) ────────────────────────────────────────────");
        String[][] sqlGroups = {
                {"Point lookup", "covering_where", "non_covering_where"},
                {"Aggregation", "covering_agg", "covering_sum", "covering_count"},
                {"Filter", "residual_filter", "non_covering_filter", "no_index_filter"},
                {"Filter IN-list", "residual_filter_in", "non_covering_filter_in"},
                {"VARCHAR/FSST", "varchar_fsst", "varchar_non_covering", "varchar_in_covering"},
                {"O3", "o3_covering", "o3_non_covering", "o3_distinct"},
                {"Misc", "latest_on", "in_list", "wide_table"},
                {"Bulk throughput", "bulk_covering", "bulk_non_covering"},
        };
        for (String[] group : sqlGroups) {
            out.printf("  %s:%n", group[0]);
            for (int i = 1; i < group.length; i++) {
                Double v = scores.get("sqlQuery/" + group[i]);
                if (v != null) {
                    out.printf("    %-28s %,12.0f ops/s%n", group[i], v);
                }
            }
        }

        // --- Limit by covered-column type (M1: covering over-materialization) ---
        if (scores.get("sqlLimit/neg_cov_double") != null) {
            out.println();
            out.println("── LIMIT by covered type (us/op, lower=better; cov=covering, pl=plain) ────────────");
            out.printf("  %-9s %11s %11s %11s %11s%n", "shape", "neg cov", "neg pl", "pos cov", "pos pl");
            for (String shape : new String[]{"double", "varchar", "both"}) {
                Double nc = scores.get("sqlLimit/neg_cov_" + shape);
                Double np = scores.get("sqlLimit/neg_plain_" + shape);
                Double pc = scores.get("sqlLimit/pos_cov_" + shape);
                Double pp = scores.get("sqlLimit/pos_plain_" + shape);
                out.printf("  %-9s %,11.1f %,11.1f %,11.1f %,11.1f%n", shape,
                        nc != null ? nc : 0, np != null ? np : 0,
                        pc != null ? pc : 0, pp != null ? pp : 0);
            }
        }

        // --- Write ---
        out.println();
        out.println("── Write Overhead (ops/s, higher=better) ─────────────────────────────────────────");
        for (String cfg : new String[]{"no_index", "bitmap", "posting", "posting_covering", "posting_varchar"}) {
            Double v = scores.get("writeInsert/" + cfg);
            if (v != null) {
                out.printf("  %-22s %,10.1f ops/s%n", cfg, v);
            }
        }

        // --- Commit ---
        Double commit = scores.get("commitProfile");
        if (commit != null) {
            out.println();
            out.printf("── Commit Profile (512 keys × 56 commits × 128 v/commit + seal) ──────────────────%n");
            out.printf("  %-22s %,10.1f ops/s  (%.1f ms/cycle)%n", "full write+seal cycle", commit, 1000.0 / commit);
        }

        out.println();
    }

    /**
     * Sidecar files are named {@code <name>.pc<idx>.<colTxn>.<sealTxn>}. Each seal
     * publishes a new file at a higher sealTxn, but the previous one is normally
     * removed by a message-bus purge job that this bare-bones benchmark setup does
     * not run. Call this after {@code writer.seal()} so {@code getDirectorySize}
     * reflects only the live sidecar.
     */
    private static void purgeStaleSidecars(String dir) {
        File d = new File(dir);
        File[] files = d.listFiles();
        if (files == null) return;
        // group by "<name>.pc<idx>.<colTxn>" prefix; track max sealTxn per group.
        Map<String, Long> maxSeal = new LinkedHashMap<>();
        for (File f : files) {
            String n = f.getName();
            int pcAt = n.indexOf(".pc");
            if (pcAt < 0 || n.contains(".pci")) continue;
            int lastDot = n.lastIndexOf('.');
            if (lastDot <= pcAt) continue;
            String prefix = n.substring(0, lastDot);
            long sealTxn;
            try {
                sealTxn = Long.parseLong(n.substring(lastDot + 1));
            } catch (NumberFormatException e) {
                continue;
            }
            maxSeal.merge(prefix, sealTxn, Math::max);
        }
        for (File f : files) {
            String n = f.getName();
            int pcAt = n.indexOf(".pc");
            if (pcAt < 0 || n.contains(".pci")) continue;
            int lastDot = n.lastIndexOf('.');
            if (lastDot <= pcAt) continue;
            String prefix = n.substring(0, lastDot);
            long sealTxn;
            try {
                sealTxn = Long.parseLong(n.substring(lastDot + 1));
            } catch (NumberFormatException e) {
                continue;
            }
            Long max = maxSeal.get(prefix);
            if (max != null && sealTxn < max) {
                f.delete();
            }
        }
    }

    private static String resolveKey(SqlCompilerImpl compiler, SqlExecutionContextImpl ctx, String table) throws Exception {
        try (RecordCursorFactory f = compiler.compile("SELECT sym FROM " + table + " LIMIT 1", ctx).getRecordCursorFactory()) {
            try (RecordCursor c = f.getCursor(ctx)) {
                if (c.hasNext()) return c.getRecord().getSymA(0).toString();
            }
        }
        return "A";
    }

    private static String resolveNKeys(SqlCompilerImpl compiler, SqlExecutionContextImpl ctx, String table, int n) throws Exception {
        StringBuilder sb = new StringBuilder();
        try (RecordCursorFactory f = compiler.compile("SELECT DISTINCT sym FROM " + table + " LIMIT " + n, ctx).getRecordCursorFactory()) {
            try (RecordCursor c = f.getCursor(ctx)) {
                while (c.hasNext()) {
                    if (!sb.isEmpty()) sb.append(',');
                    sb.append('\'').append(c.getRecord().getSymA(0)).append('\'');
                }
            }
        }
        return sb.toString();
    }

    private static void runPageFaultAnalysis() {
        System.err.println();
        System.err.println("══════════════════════════════════════════════════════════════════════════════════════════════════");
        System.err.println("  Covering Index I/O Projection (CPU + predicted page faults)");
        System.err.println("══════════════════════════════════════════════════════════════════════════════════════════════════");

        int keys = SidecarState.KEYS;
        int rows = SidecarState.ROWS;
        int readKeys = SidecarState.READ_KEYS;
        int warmup = 3;
        int runs = 5;

        LogFactory.haltInstance();
        String tmpDir = System.getProperty("java.io.tmpdir");
        CairoConfiguration config = benchConfig(tmpDir);
        int[] keyAssignment = buildShuffled(rows, keys);
        int[] queryKeys = new int[readKeys];
        Random rng = new Random(99);
        for (int i = 0; i < readKeys; i++) queryKeys[i] = rng.nextInt(keys);

        System.err.printf("  %,d keys × %,d v/k = %,d rows, querying %,d keys%n%n", keys, SidecarState.VPK, rows, readKeys);
        System.err.printf("  %-14s  %6s  %6s  %8s  %8s  %8s  %8s  %8s%n",
                "type", "pages", "pages", "CPU ms", "CPU ms", "+ NVMe", "+ cloud", "+ HDD");
        System.err.printf("  %-14s  %6s  %6s  %8s  %8s  %8s  %8s  %8s%n",
                "", "base", "cover", "base", "cover", "cover", "cover", "cover");
        System.err.println("  " + "─".repeat(88));

        // Captured per-type so we can print a compression-ratio table
        // after the page-fault loop. Index by CoverType.ordinal().
        double[] rawMB = new double[CoverType.values().length];
        double[] sidecarMB = new double[CoverType.values().length];

        for (CoverType ct : CoverType.values()) {
            String baseDir = tmpDir + File.separator + "pf_base_" + ct + "_" + System.nanoTime();
            String covDir = tmpDir + File.separator + "pf_cov_" + ct + "_" + System.nanoTime();
            new File(baseDir).mkdirs();
            new File(covDir).mkdirs();

            long colAddr = Unsafe.malloc((long) rows * ct.size, MemoryTag.NATIVE_DEFAULT);
            Random dataRng = new Random(42);
            for (int i = 0; i < rows; i++) {
                switch (ct) {
                    case DOUBLE -> Unsafe.putDouble(colAddr + (long) i * 8, 100.0 + dataRng.nextInt(90_000) * 0.01);
                    case FLOAT -> Unsafe.putFloat(colAddr + (long) i * 4, 20.0f + dataRng.nextInt(500) * 0.01f);
                    case LONG ->
                            Unsafe.putLong(colAddr + (long) i * 8, 1_700_000_000_000_000L + dataRng.nextInt(1_000_000));
                    case INT -> Unsafe.putInt(colAddr + (long) i * 4, dataRng.nextInt(10_000));
                }
            }

            try {
                // Write baseline (no covering) and covering index
                writeCovIndex(config, baseDir, keyAssignment, null, 0, null, null, null);
                writeCovIndex(config, covDir, keyAssignment,
                        new long[]{colAddr}, 1, new int[]{ct.shift}, new int[]{ct.columnType}, new long[]{0});

                // Count distinct pages for baseline reads
                LongHashSet baselinePages = new LongHashSet();
                try (Path path = new Path().of(baseDir)) {
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(config, path, "test", COL_TXN, 0, 0)) {
                        for (int key : queryKeys) {
                            try (RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE)) {
                                while (cursor.hasNext()) {
                                    long rowId = cursor.next();
                                    long pageNum = (rowId * ct.size) / PAGE_SIZE;
                                    baselinePages.add(pageNum);
                                }
                            }
                        }
                    }
                }
                int basePages = baselinePages.size();

                // Covering pages: sidecar file size, proportional to queried fraction
                long baseSize = getDirectorySize(baseDir);
                long covSize = getDirectorySize(covDir);
                long sidecarBytes = covSize - baseSize;
                long sidecarTotalPages = (sidecarBytes + PAGE_SIZE - 1) / PAGE_SIZE;
                double fraction = (double) readKeys / keys;
                int covPages = Math.max(1, (int) Math.ceil(sidecarTotalPages * fraction));

                rawMB[ct.ordinal()] = (rows * (long) ct.size) / 1024.0 / 1024.0;
                sidecarMB[ct.ordinal()] = sidecarBytes / 1024.0 / 1024.0;

                // Measure CPU time
                for (int w = 0; w < warmup; w++) {
                    doCovBaselineRead(config, baseDir, queryKeys, colAddr, ct);
                    doCovCoveringRead(config, covDir, queryKeys, ct);
                }

                long baseCpuNs = 0;
                for (int i = 0; i < runs; i++) {
                    long t0 = System.nanoTime();
                    doCovBaselineRead(config, baseDir, queryKeys, colAddr, ct);
                    baseCpuNs += System.nanoTime() - t0;
                }
                double baseCpuMs = baseCpuNs / (runs * 1e6);

                long covCpuNs = 0;
                for (int i = 0; i < runs; i++) {
                    long t0 = System.nanoTime();
                    doCovCoveringRead(config, covDir, queryKeys, ct);
                    covCpuNs += System.nanoTime() - t0;
                }
                double covCpuMs = covCpuNs / (runs * 1e6);

                double nvmeMs = covCpuMs + covPages * NVME_US_PER_PAGE / 1_000.0;
                double cloudMs = covCpuMs + covPages * CLOUD_US_PER_PAGE / 1_000.0;
                double hddMs = covCpuMs + covPages * HDD_US_PER_PAGE / 1_000.0;
                double baseNvmeMs = baseCpuMs + basePages * NVME_US_PER_PAGE / 1_000.0;

                System.err.printf("  %-14s  %,6d  %,6d  %8.1f  %8.1f  %8.1f  %8.1f  %8.1f%n",
                        ct.name(), basePages, covPages, baseCpuMs, covCpuMs, nvmeMs, cloudMs, hddMs);
                System.err.printf("  %-14s  %6s  %6s  %8s  %8s  %8.1f  %8s  %8s    (baseline + NVMe for comparison)%n",
                        "", "", "", "", "", baseNvmeMs, "", "");
            } finally {
                Unsafe.free(colAddr, (long) rows * ct.size, MemoryTag.NATIVE_DEFAULT);
                deleteDir(baseDir);
                deleteDir(covDir);
            }
        }
        System.err.println();

        // Implied compression: raw column bytes vs the additional bytes
        // covering writes for the .pcN sidecar (covSize - baseSize, after
        // purgeStaleSidecars). Ratio > 1.0 means the sidecar encoding
        // compresses the raw column.
        System.err.println("══════════════════════════════════════════════════════════════════════════════════════════════════");
        System.err.println("  Implied Sidecar Compression (file size, raw column vs encoded sidecar)");
        System.err.println("══════════════════════════════════════════════════════════════════════════════════════════════════");
        System.err.printf("  %-14s  %12s  %12s  %12s%n", "type", "raw column", "sidecar", "ratio");
        System.err.println("  " + "─".repeat(56));
        for (CoverType ct : CoverType.values()) {
            double raw = rawMB[ct.ordinal()];
            double side = sidecarMB[ct.ordinal()];
            double ratio = side > 0 ? raw / side : Double.NaN;
            System.err.printf("  %-14s  %9.2f MB  %9.2f MB  %11.2fx%n",
                    ct.name(), raw, side, ratio);
        }
        System.err.println();
    }

    private static String[] sampleKeys(SqlCompilerImpl compiler, SqlExecutionContextImpl ctx, String table, int n) throws Exception {
        String[] result = new String[n];
        int idx = 0;
        try (RecordCursorFactory f = compiler.compile("SELECT DISTINCT sym FROM " + table + " LIMIT " + n, ctx).getRecordCursorFactory()) {
            try (RecordCursor c = f.getCursor(ctx)) {
                while (c.hasNext() && idx < n) {
                    result[idx++] = c.getRecord().getSymA(0).toString();
                }
            }
        }
        if (idx == 0) {
            for (int i = 0; i < n; i++) result[i] = "_";
        } else {
            for (int i = idx; i < n; i++) result[i] = result[i % idx];
        }
        return result;
    }

    private static int[] selectRandomKeys(int keyCount, int n) {
        int[] keys = new int[n];
        Random rng = new Random(99);
        for (int i = 0; i < n; i++) keys[i] = rng.nextInt(keyCount);
        return keys;
    }

    private static void writeCovIndex(CairoConfiguration config, String dir, int[] keyAssignment,
                                      long[] colAddrs, int coverCount,
                                      int[] shifts, int[] types, long[] tops) {
        try (Path path = new Path().of(dir)) {
            try (PostingIndexWriter writer = new PostingIndexWriter(config, path, "test", COLUMN_NAME_TXN_NONE)) {
                if (coverCount > 0) {
                    int[] indices = new int[coverCount];
                    for (int i = 0; i < coverCount; i++) indices[i] = i + 2;
                    writer.configureCovering(colAddrs, tops, shifts, indices, types, coverCount);
                }
                for (int rowId = 0; rowId < keyAssignment.length; rowId++) {
                    writer.add(keyAssignment[rowId], rowId);
                }
                writer.setMaxValue(keyAssignment.length - 1);
                writer.seal();
            }
        }
        purgeStaleSidecars(dir);
    }

    enum CoverType {
        DOUBLE(ColumnType.DOUBLE, 3, 8),
        FLOAT(ColumnType.FLOAT, 2, 4),
        LONG(ColumnType.LONG, 3, 8),
        INT(ColumnType.INT, 2, 4);

        final int columnType, shift, size;

        CoverType(int columnType, int shift, int size) {
            this.columnType = columnType;
            this.shift = shift;
            this.size = size;
        }
    }

    // ==================================================================================
    // Page fault analysis helpers
    // ==================================================================================

    /**
     * State for per-commit write profiling (market data pattern from IndexScenarioBenchmark).
     * 512 keys, 56 commits, 128 values per key per commit = 3.67M rows.
     * Measures the full add→commit→seal cycle cost.
     */
    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class CommitState {
        static final int COMMITS = 56;
        static final int KEYS = 512;
        static final int VALUES_PER_COMMIT = 128;

        CairoConfiguration config;

        @Setup(Level.Trial)
        public void setup() {
            String tmpDir = System.getProperty("java.io.tmpdir");
            config = benchConfig(tmpDir);
        }
    }

    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public static class DecodeState {
        static final int TOTAL = 65_536;
        @Param({"64", "256", "1024"})
        int batchSize;
        @Param({"1", "8", "12", "16", "20", "32"})
        int bitWidth;
        long destAddr;
        long minValue = 1_000_000L;
        long packedAddr;
        int packedSize;

        @Setup(Level.Trial)
        public void setup() {
            long maxOffset = (1L << bitWidth) - 1;
            packedSize = BitpackUtils.packedDataSize(TOTAL, bitWidth);
            packedAddr = Unsafe.malloc(packedSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.setMemory(packedAddr, packedSize, (byte) 0);

            long valuesSize = (long) TOTAL * Long.BYTES;
            long valuesAddr = Unsafe.malloc(valuesSize, MemoryTag.NATIVE_DEFAULT);
            for (int i = 0; i < TOTAL; i++) {
                Unsafe.putLong(valuesAddr + (long) i * Long.BYTES,
                        minValue + (i % (maxOffset + 1)));
            }
            PostingIndexNative.packValuesNativeFallback(valuesAddr, TOTAL, minValue, bitWidth, packedAddr);
            Unsafe.free(valuesAddr, valuesSize, MemoryTag.NATIVE_DEFAULT);

            destAddr = Unsafe.malloc((long) batchSize * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            Unsafe.free(destAddr, (long) batchSize * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(packedAddr, packedSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * The nine fixture shapes this suite measures across, defined once so that
     * "S7" means one million keys in EVERY arm. Four state classes parameterise
     * against this; a shared label that silently meant different things in
     * different arms would be worse than no sharing at all, which is what
     * verifyLadder() guards.
     */
    enum Dist { SHUFFLED, ROUND_ROBIN, ZIPFIAN, STREAMING }

    enum Ladder {
        S1(500_000, 2_000_000, 2_000_000, Dist.SHUFFLED, 0L),
        S2(512, 1_024_000, 65_536, Dist.ROUND_ROBIN, 0L),
        S3(10_000, 0, 0, Dist.STREAMING, 0L),
        S4(2_000, 2_000_000, 2_000_000, Dist.ROUND_ROBIN, 0L),
        S5(500, 1_000_000, 1_000_000, Dist.ZIPFIAN, 0L),
        S6(200_000, 1_200_000, 1_200_000, Dist.SHUFFLED, 0L),
        S7(1_000_000, 2_000_000, 2_000_000, Dist.SHUFFLED, 0L),
        S8(5_000, 1_000_000, 1_000_000, Dist.ROUND_ROBIN, 1_000_000_000L),
        P400K(16, 400_000, 400_000, Dist.ROUND_ROBIN, 0L);

        private final int commitInterval;
        private final Dist dist;
        private final int keyCount;
        private final long rowIdBase;
        private final int totalRows;

        Ladder(int keyCount, int totalRows, int commitInterval, Dist dist, long rowIdBase) {
            this.keyCount = keyCount;
            this.totalRows = totalRows;
            this.commitInterval = commitInterval;
            this.dist = dist;
            this.rowIdBase = rowIdBase;
        }

        static Ladder of(String name) {
            for (Ladder l : values()) {
                if (l.name().equals(name)) {
                    return l;
                }
            }
            throw new IllegalArgumentException("unknown scenario " + name);
        }

        int commitInterval() { return commitInterval; }

        Dist dist() { return dist; }

        int keyCount() { return keyCount; }

        long rowIdBase() { return rowIdBase; }

        /**
         * Whether this shape can be built by a static SQL insert. ZIPFIAN has no
         * weighted rnd_symbol, and STREAMING is 100 separate commits; a
         * hand-rolled approximation of either would be non-comparable with the
         * index-level arm while LOOKING comparable under the same label. The SQL
         * and sidecar arms skip these and say so.
         */
        boolean sqlExpressible() { return dist != Dist.ZIPFIAN && dist != Dist.STREAMING; }

        boolean streaming() { return dist == Dist.STREAMING; }

        int totalRows() { return totalRows; }
    }

    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class IndexState {
        CairoConfiguration config;
        // Written index directory
        String dir;
        @Param({"FORWARD", "BACKWARD"})
        String direction;
        // LEGACY (the old bitmap index) stays selectable but is not a default.
        // The question this suite answers is what converting a partition to
        // parquet costs, which is POSTING against POSTING_PARQUET; a third
        // default arm spends a third of the runtime on a different question.
        @Param({"POSTING", "POSTING_PARQUET"})
        String format;
        long imFileSize;
        boolean isParquet;
        boolean isPosting;
        int keyCount;
        long maxRow;
        ColumnVersionReader parquetCvr;
        GenericRecordMetadata parquetMetadata;
        int[] pointKeys;
        int[] rangeKeys;
        long rowIdBase;
        // Four cardinality rungs. These fixtures are built through the index
        // writer rather than SQL, so they stay cheap enough to keep the 1M-key
        // rung -- which is exactly where the parquet form regresses.
        @Param({"P400K", "S4", "S6", "S7"})
        String scenario;
        int totalRows;

        @Setup(Level.Trial)
        public void setup() {
            String tmpDir = System.getProperty("java.io.tmpdir");
            config = benchConfig(tmpDir);
            isParquet = "POSTING_PARQUET".equals(format);
            isPosting = "POSTING".equals(format);

            // Scaled data sizes: preserve distribution, ~1-2M rows for speed
            int commitInterval;
            int[] keys;
            final Ladder l = Ladder.of(scenario);
            keyCount = l.keyCount();
            if (l.streaming()) {
                setupStreaming();
                return;
            }
            totalRows = l.totalRows();
            commitInterval = l.commitInterval();
            rowIdBase = l.rowIdBase();
            keys = switch (l.dist()) {
                case SHUFFLED -> buildShuffled(totalRows, keyCount);
                case ROUND_ROBIN -> buildRoundRobin(totalRows, keyCount);
                case ZIPFIAN -> buildZipfian(totalRows, keyCount);
                case STREAMING -> throw new IllegalStateException("handled above");
            };

            maxRow = rowIdBase + totalRows - 1;
            pointKeys = selectRandomKeys(keyCount, Math.min(5_000, keyCount));
            rangeKeys = selectRandomKeys(keyCount, Math.min(500, keyCount));

            dir = tmpDir + File.separator + "suite_" + scenario + "_" + format + "_" + System.nanoTime();
            new File(dir).mkdirs();

            if (isParquet) {
                buildParquetArm(keys);
            } else if (isPosting) {
                initPosting(config, dir);
                try (Path path = new Path().of(dir)) {
                    PostingIndexWriter writer = new PostingIndexWriter(config);
                    writer.of(path, "test", COL_TXN, false);
                    for (int i = 0; i < totalRows; i++) {
                        writer.add(keys[i], rowIdBase + i);
                        if ((i + 1) % commitInterval == 0) {
                            writer.setMaxValue(rowIdBase + i);
                            writer.commit();
                        }
                    }
                    if (totalRows % commitInterval != 0) {
                        writer.setMaxValue(rowIdBase + totalRows - 1);
                        writer.commit();
                    }
                    writer.seal();
                    writer.close();
                }
            } else {
                int blockCap = Math.max(8, Numbers.ceilPow2(totalRows / Math.max(keyCount, 1)));
                try (Path path = new Path().of(dir)) {
                    try (BitmapIndexWriter w = new BitmapIndexWriter(config)) {
                        w.of(path, "test", COL_TXN, blockCap);
                        for (int i = 0; i < totalRows; i++) w.add(keys[i], rowIdBase + i);
                    }
                }
            }
            verifyArmPostingCount(this, totalRows);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            parquetCvr = Misc.free(parquetCvr);
            deleteDir(dir);
        }

        /**
         * Seals the parquet arm over the same key assignment the native arms
         * index, so the only difference between arms is the index form.
         * Uncovered: these three benchmarks walk row ids and never gather a
         * covered value, which is what {@code sidecarRead} measures instead.
         */
        private void buildParquetArm(int[] keys) {
            parquetMetadata = new GenericRecordMetadata();
            parquetMetadata.add(new TableColumnMetadata(
                    "test", ColumnType.SYMBOL, IndexType.POSTING, 0, false, null, 0, false));
            parquetCvr = new ColumnVersionReader();
            imFileSize = sealParquetArm(
                    config, dir, keys, totalRows, keyCount, rowIdBase,
                    new ObjList<>(), new IntList(), new IntList(), new LongList(), new LongList());
            if (imFileSize == 0) {
                throw new IllegalStateException("parquet seal wrote nothing for scenario " + scenario);
            }
        }

        private void setupStreaming() {
            int commits = 100;
            int activePerCommit = 200;
            int valsPerActive = 10;
            Rnd rnd = new Rnd(12345, 67890);

            int[][] activeKeyArrays = new int[commits][];
            totalRows = 0;
            for (int c = 0; c < commits; c++) {
                int[] pool = new int[keyCount];
                for (int i = 0; i < keyCount; i++) pool[i] = i;
                for (int i = 0; i < activePerCommit; i++) {
                    int j = i + rnd.nextPositiveInt() % (keyCount - i);
                    int tmp = pool[i];
                    pool[i] = pool[j];
                    pool[j] = tmp;
                }
                activeKeyArrays[c] = Arrays.copyOf(pool, activePerCommit);
                Arrays.sort(activeKeyArrays[c]);
                totalRows += activePerCommit * valsPerActive;
            }
            maxRow = totalRows - 1;
            pointKeys = selectRandomKeys(keyCount, 5_000);
            rangeKeys = selectRandomKeys(keyCount, 500);

            String tmpDir = System.getProperty("java.io.tmpdir");
            dir = tmpDir + File.separator + "suite_S3_" + format + "_" + System.nanoTime();
            new File(dir).mkdirs();

            if (isParquet) {
                // Flatten the per-commit key arrays into one row-ordered
                // assignment: the seal takes the whole partition at once, since
                // a parquet-form index is republished wholesale rather than
                // appended to commit by commit.
                int[] flat = new int[totalRows];
                int at = 0;
                for (int c = 0; c < commits; c++) {
                    for (int key : activeKeyArrays[c]) {
                        for (int v = 0; v < valsPerActive; v++) flat[at++] = key;
                    }
                }
                buildParquetArm(flat);
            } else if (isPosting) {
                initPosting(config, dir);
                try (Path path = new Path().of(dir)) {
                    PostingIndexWriter writer = new PostingIndexWriter(config);
                    writer.of(path, "test", COL_TXN, false);
                    int rowId = 0;
                    for (int c = 0; c < commits; c++) {
                        for (int key : activeKeyArrays[c]) {
                            for (int v = 0; v < valsPerActive; v++) writer.add(key, rowId++);
                        }
                        writer.setMaxValue(rowId - 1);
                        writer.commit();
                    }
                    writer.seal();
                    writer.close();
                }
            } else {
                try (Path path = new Path().of(dir)) {
                    try (BitmapIndexWriter w = new BitmapIndexWriter(config)) {
                        w.of(path, "test", COL_TXN, 64);
                        int rowId = 0;
                        for (int c = 0; c < commits; c++) {
                            for (int key : activeKeyArrays[c]) {
                                for (int v = 0; v < valsPerActive; v++) w.add(key, rowId++);
                            }
                        }
                    }
                }
            }
        }
    }

    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class SidecarState {
        static final int COVER_WRITER_IDX = 2;
        static final int KEYS = 500;
        static final int READ_KEYS = 200;
        // -Dquestdb.suite.bench.sidecar.vpk=<n>: rows per key. Default 2000
        // (8 MB DOUBLE column, 2 MB SHORT — partially cache-resident).
        // Set higher (e.g. 125_000 → 62.5M rows × 8 = 500 MB DOUBLE) to
        // push the baseline column firmly into DRAM.
        static final int VPK = Integer.getInteger("questdb.suite.bench.sidecar.vpk", 2_000);
        static final int ROWS = KEYS * VPK;
        long colAddr;
        int colType, colShift, colSize;
        @Param({"DOUBLE", "FLOAT", "LONG", "INT", "DECIMAL64", "DECIMAL32", "SHORT"})
        String columnType;
        CairoConfiguration config;
        GenericRecordMetadata coverMetadata;
        ColumnVersionReader cvr;
        String dir;
        long imFileSize;
        boolean isParquet;
        @Param({"baseline", "covering", "covering_parquet"})
        String mode;
        int[] readKeys;

        @Setup(Level.Trial)
        public void setup() {
            switch (columnType) {
                case "DOUBLE" -> {
                    colType = ColumnType.DOUBLE;
                    colShift = 3;
                    colSize = 8;
                }
                case "FLOAT" -> {
                    colType = ColumnType.FLOAT;
                    colShift = 2;
                    colSize = 4;
                }
                case "LONG" -> {
                    colType = ColumnType.LONG;
                    colShift = 3;
                    colSize = 8;
                }
                case "INT" -> {
                    colType = ColumnType.INT;
                    colShift = 2;
                    colSize = 4;
                }
                case "DECIMAL64" -> {
                    colType = ColumnType.DECIMAL64;
                    colShift = 3;
                    colSize = 8;
                }
                case "DECIMAL32" -> {
                    colType = ColumnType.DECIMAL32;
                    colShift = 2;
                    colSize = 4;
                }
                case "SHORT" -> {
                    colType = ColumnType.SHORT;
                    colShift = 1;
                    colSize = 2;
                }
                default -> throw new IllegalArgumentException(columnType);
            }

            String tmpDir = System.getProperty("java.io.tmpdir");
            config = benchConfig(tmpDir);
            dir = tmpDir + File.separator + "suite_cov_" + columnType + "_" + mode + "_" + System.nanoTime();
            new File(dir).mkdirs();

            coverMetadata = new GenericRecordMetadata();
            for (int i = 0; i <= COVER_WRITER_IDX; i++) {
                int t = (i == COVER_WRITER_IDX) ? colType : ColumnType.LONG;
                coverMetadata.add(new TableColumnMetadata(
                        "c" + i, t, IndexType.NONE, 0, false, null, i, false));
            }
            cvr = new ColumnVersionReader();

            colAddr = Unsafe.malloc((long) ROWS * colSize, MemoryTag.NATIVE_DEFAULT);
            Random rng = new Random(42);
            for (int i = 0; i < ROWS; i++) {
                switch (columnType) {
                    case "DOUBLE" -> Unsafe.putDouble(colAddr + (long) i * 8, 100.0 + rng.nextInt(90_000) * 0.01);
                    case "FLOAT" -> Unsafe.putFloat(colAddr + (long) i * 4, 20.0f + rng.nextInt(500) * 0.01f);
                    case "LONG" ->
                            Unsafe.putLong(colAddr + (long) i * 8, 1_700_000_000_000_000L + rng.nextInt(1_000_000));
                    case "INT" -> Unsafe.putInt(colAddr + (long) i * 4, rng.nextInt(10_000));
                    case "DECIMAL64" ->
                        // Prices with scale=2: 100.00–1000.00 → unscaled 10_000–100_000
                            Unsafe.putLong(colAddr + (long) i * 8, 10_000L + rng.nextInt(90_000));
                    case "DECIMAL32" ->
                        // Prices with scale=2: 20.00–25.00 → unscaled 2_000–2_500
                            Unsafe.putInt(colAddr + (long) i * 4, 2_000 + rng.nextInt(500));
                    case "SHORT" -> Unsafe.putShort(colAddr + (long) i * 2, (short) rng.nextInt(1_000));
                }
            }

            readKeys = new int[READ_KEYS];
            rng = new Random(99);
            for (int i = 0; i < READ_KEYS; i++) readKeys[i] = rng.nextInt(KEYS);

            int[] keyAssignment = buildShuffled(ROWS, KEYS);
            isParquet = "covering_parquet".equals(mode);
            boolean covering = "covering".equals(mode);
            if (isParquet) {
                // Same covered column, same key assignment as the "covering"
                // arm - only the index form differs, which is the comparison.
                final ObjList<CharSequence> coveredNames = new ObjList<>();
                coveredNames.add("c" + COVER_WRITER_IDX);
                final IntList coveredTypes = new IntList();
                coveredTypes.add(colType);
                final IntList coveredWriterIndices = new IntList();
                coveredWriterIndices.add(COVER_WRITER_IDX);
                final LongList coveredAddrs = new LongList();
                coveredAddrs.add(colAddr);
                final LongList coveredColumnTops = new LongList();
                coveredColumnTops.add(0);
                imFileSize = sealParquetArm(
                        config, dir, keyAssignment, ROWS, KEYS, 0,
                        coveredNames, coveredTypes, coveredWriterIndices,
                        coveredAddrs, coveredColumnTops);
                if (imFileSize == 0) {
                    throw new IllegalStateException("parquet seal wrote nothing for " + columnType);
                }
                return;
            }
            try (Path path = new Path().of(dir)) {
                try (PostingIndexWriter writer = new PostingIndexWriter(config, path, "test", COLUMN_NAME_TXN_NONE)) {
                    if (covering) {
                        writer.configureCovering(
                                new long[]{colAddr}, new long[]{0},
                                new int[]{colShift}, new int[]{2},
                                new int[]{colType}, 1);
                    }
                    for (int i = 0; i < ROWS; i++) writer.add(keyAssignment[i], i);
                    writer.setMaxValue(ROWS - 1);
                    writer.seal();
                }
            }
            purgeStaleSidecars(dir);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            Misc.free(cvr);
            Unsafe.free(colAddr, (long) ROWS * colSize, MemoryTag.NATIVE_DEFAULT);
            deleteDir(dir);
        }
    }

    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public static class SqlState {
        SqlCompilerImpl compiler;
        SqlExecutionContextImpl ctx;
        CairoEngine engine;
        @Param({"covering_where", "latest_on", "latest_on_indexed"})
        String queryType;
        // Three cardinality rungs, not the full ladder. The question this arm
        // answers is "does converting to parquet hurt", and 16 / 2,000 / 200,000
        // distinct keys spans the range where the index-level arm crosses over.
        // More rungs cost minutes each and add no answer.
        @Param({"P400K", "S4", "S6"})
        String scenario;
        String sql;
        // Three arms, not two. native -> parquet_data isolates the cost of the
        // parquet DATA format; parquet_data -> parquet_index isolates the cost
        // of the index form, which is the thing this branch changes. A single
        // native-vs-parquet_index number confounds them, and on latest_on that
        // confusion reads as an 8x index regression when the index accounts for
        // 9% of it.
        @Param({STORAGE_NATIVE, STORAGE_PARQUET_DATA, STORAGE_PARQUET_INDEX})
        String storage;
        java.nio.file.Path tmpDir;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            final Ladder l = Ladder.of(scenario);
            if (!l.sqlExpressible()) {
                throw new IllegalStateException(
                        "scenario " + scenario + " has no SQL form; it must not appear in this arm's @Param list");
            }
            tmpDir = Files.createTempDirectory("suite-sql");
            final boolean parquetIndex = STORAGE_PARQUET_INDEX.equals(storage);
            CairoConfiguration config = new DefaultCairoConfiguration(tmpDir.toString()) {
                // The same sweep knobs benchConfig honours. Without them the SQL
                // arm silently ran the default codec while the index benchmarks
                // ran whatever -Dquestdb.idx.codec asked for, and the two sets of
                // numbers were not comparable.
                @Override
                public int getPostingIndexParquetCompressionCodec() {
                    return Integer.getInteger("questdb.idx.codec", super.getPostingIndexParquetCompressionCodec());
                }

                @Override
                public int getPostingIndexParquetDataPageSize() {
                    final Integer explicit = Integer.getInteger("questdb.idx.page");
                    return explicit != null ? explicit : super.getPostingIndexParquetDataPageSize();
                }

                @Override
                public byte getPostingIndexParquetPartitionFormat() {
                    return parquetIndex
                            ? PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET
                            : PostingIndexUtils.PARQUET_INDEX_FORMAT_NATIVE;
                }

                @Override
                public byte getPostingIndexRowIdEncoding() {
                    return IS_DELTA ? PostingIndexUtils.ENCODING_DELTA : PostingIndexUtils.ENCODING_ADAPTIVE;
                }

                @Override
                public int getRndFunctionMemoryMaxPages() {
                    return 4096;
                }
            };
            engine = new CairoEngine(config);
            ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            compiler = new SqlCompilerImpl(engine);

            // bench / bench_noidx / bench_nc are ladder-driven: their cardinality
            // and row count come from the scenario, not a hardcoded 200/400000.
            // wide, vchar, bulk and o3bench stay fixed -- they serve query shapes
            // this arm's default @Param list no longer pins.
            final String keyExpr = switch (l.dist()) {
                case SHUFFLED -> "rnd_symbol(" + l.keyCount() + ", 4, 8, 0)";
                case ROUND_ROBIN -> "('s' || (x % " + l.keyCount() + "))::SYMBOL";
                default -> throw new IllegalStateException("not SQL-expressible: " + l);
            };
            // Fixture cost scales with ROWS; the thing under test is KEYS. Holding
            // rows at the original 400k keeps every rung the same price -- an S7
            // shaped cell measured 201.5 s, which buys no extra answer. Every rung
            // used here has keyCount <= 200,000, so at 400k rows the sparsest still
            // averages two rows per key.
            final String rows = String.valueOf(Math.min(l.totalRows(), 400_000));

            // Core table: sym with covering index on price (ladder keys, ladder rows)
            engine.execute("CREATE TABLE bench (" +
                    "ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + " INCLUDE (price), price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
            engine.execute("INSERT INTO bench SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP, " +
                    keyExpr + ", rnd_double() * 1000 FROM long_sequence(" + rows + ")", ctx);

            // No-index version of bench for full-scan filter comparison
            engine.execute("CREATE TABLE bench_noidx (" +
                    "ts TIMESTAMP, sym SYMBOL, price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
            engine.execute("INSERT INTO bench_noidx SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP, " +
                    keyExpr + ", rnd_double() * 1000 FROM long_sequence(" + rows + ")", ctx);

            // Non-covering version of bench (bitmap index, no INCLUDE)
            engine.execute("CREATE TABLE bench_nc (" +
                    "ts TIMESTAMP, sym SYMBOL INDEX, price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
            engine.execute("INSERT INTO bench_nc SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP, " +
                    keyExpr + ", rnd_double() * 1000 FROM long_sequence(" + rows + ")", ctx);

            // Wide table: 8 columns, covering 2 (200 keys, 200K rows)
            engine.execute("CREATE TABLE wide (" +
                    "ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + " INCLUDE (c1, c2), " +
                    "c1 DOUBLE, c2 INT, c3 DOUBLE, c4 INT, c5 DOUBLE, c6 INT, c7 DOUBLE, c8 INT" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
            engine.execute("INSERT INTO wide SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP, " +
                    "rnd_symbol(200, 4, 8, 0), rnd_double()*100, rnd_int(0,1000,0), " +
                    "rnd_double()*100, rnd_int(0,1000,0), rnd_double()*100, rnd_int(0,1000,0), " +
                    "rnd_double()*100, rnd_int(0,1000,0) FROM long_sequence(200000)", ctx);

            // VARCHAR/FSST table: covering includes VARCHAR (200 keys, 200K rows)
            engine.execute("CREATE TABLE vchar (" +
                    "ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + " INCLUDE (name, price), " +
                    "name VARCHAR, price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
            engine.execute("INSERT INTO vchar SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP, " +
                    "rnd_symbol(200, 4, 8, 0), rnd_varchar(10, 30, 0), rnd_double() * 1000 " +
                    "FROM long_sequence(200000)", ctx);

            // Bulk table: few keys, many rows per key — tests sustained output throughput
            // 20 keys × 50K rows = 1M rows (covering VARCHAR + DOUBLE, FSST compressed)
            engine.execute("CREATE TABLE bulk (" +
                    "ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + " INCLUDE (name, price), " +
                    "name VARCHAR, price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL", ctx);
            engine.execute("INSERT INTO bulk SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP, " +
                    "rnd_symbol(20, 4, 8, 0), rnd_varchar(10, 30, 0), rnd_double() * 1000 " +
                    "FROM long_sequence(1000000)", ctx);

            // O3 table: in-order insert then out-of-order insert
            engine.execute("CREATE TABLE o3bench (" +
                    "ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + " INCLUDE (price), price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
            engine.execute("INSERT INTO o3bench SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP, " +
                    "rnd_symbol(50, 4, 8, 0), rnd_double() * 1000 FROM long_sequence(100000)", ctx);
            // O3 insert: timestamps interleaved with existing data
            engine.execute("INSERT INTO o3bench SELECT dateadd('s', x::INT, '2024-01-01T00:00:00.500000')::TIMESTAMP, " +
                    "rnd_symbol(50, 4, 8, 0), rnd_double() * 1000 FROM long_sequence(100000)", ctx);

            // One row per table in a far-later partition, in EVERY arm so the
            // data is identical across them. QuestDB refuses to convert the
            // ACTIVE partition and reports nothing when it declines, so without
            // a trailing partition the conversion below is a silent no-op and
            // the parquet arms measure native storage under a parquet label.
            engine.execute("INSERT INTO bench VALUES ('" + TRAILING_TS + "', 'zzz', 1.0)", ctx);
            engine.execute("INSERT INTO bench_noidx VALUES ('" + TRAILING_TS + "', 'zzz', 1.0)", ctx);
            engine.execute("INSERT INTO bench_nc VALUES ('" + TRAILING_TS + "', 'zzz', 1.0)", ctx);
            engine.execute("INSERT INTO wide VALUES ('" + TRAILING_TS + "', 'zzz', 1.0, 1, 1.0, 1, 1.0, 1, 1.0, 1)", ctx);
            engine.execute("INSERT INTO vchar VALUES ('" + TRAILING_TS + "', 'zzz', 'zzz', 1.0)", ctx);
            engine.execute("INSERT INTO bulk VALUES ('" + TRAILING_TS + "', 'zzz', 'zzz', 1.0)", ctx);
            engine.execute("INSERT INTO o3bench VALUES ('" + TRAILING_TS + "', 'zzz', 1.0)", ctx);

            if (!STORAGE_NATIVE.equals(storage)) {
                // The seal refuses var-size covered columns, so vchar and bulk
                // -- both covering a VARCHAR -- have no parquet-index arm at
                // all. Failing here is deliberate: silently leaving them native
                // would report a native measurement under a parquet label.
                if (parquetIndex && VARCHAR_COVERED_QUERIES.contains(queryType)) {
                    throw new UnsupportedOperationException(
                            "query type " + queryType + " covers a VARCHAR, which the parquet index seal refuses;"
                                    + " run it under storage=native or storage=parquet_data only");
                }
                for (String table : new String[]{"bench", "bench_noidx", "bench_nc", "wide", "o3bench"}) {
                    convertPartitionsToParquet(compiler, ctx, table);
                }
                if (!parquetIndex) {
                    // Under parquet_data the VARCHAR tables convert too: the
                    // native index simply hard-links into the parquet directory.
                    for (String table : new String[]{"vchar", "bulk"}) {
                        convertPartitionsToParquet(compiler, ctx, table);
                    }
                } else {
                    // The conversion itself reseals the covering index in the
                    // configured format, so no rebuild is needed -- but a
                    // refusal would leave the native chain, so verify.
                    for (String table : new String[]{"bench", "wide", "o3bench"}) {
                        assertParquetIndexPresent(engine, table);
                    }
                }
            }

            engine.releaseAllWriters();

            String key = resolveKey(compiler, ctx, "bench");
            String ncKey = resolveKey(compiler, ctx, "bench_nc");
            String niKey = resolveKey(compiler, ctx, "bench_noidx");
            String wideKey = resolveKey(compiler, ctx, "wide");
            String vcharKey = resolveKey(compiler, ctx, "vchar");
            String o3Key = resolveKey(compiler, ctx, "o3bench");
            String bulkKey = resolveKey(compiler, ctx, "bulk");
            String tenKeys = resolveNKeys(compiler, ctx, "bench", 10);
            String tenNcKeys = resolveNKeys(compiler, ctx, "bench_nc", 10);

            sql = switch (queryType) {
                // Core covering
                case "covering_where" -> "SELECT price FROM bench WHERE sym = '" + key + "'";
                case "covering_agg" -> "SELECT avg(price) FROM bench WHERE sym = '" + key + "'";
                case "covering_sum" -> "SELECT sum(price) FROM bench WHERE sym = '" + key + "'";
                case "covering_count" -> "SELECT count() FROM bench WHERE sym = '" + key + "'";
                case "latest_on" -> "SELECT ts, sym, price FROM bench LATEST ON ts PARTITION BY sym";
                // latest_on above has NO WHERE, so it compiles to a frame
                // backward scan and never touches the index -- which is why the
                // index column read as parity for it while an indexed LATEST ON
                // was 2.9x SLOWER than using no index at all. Naming values is
                // what routes it to "CoveringIndex op: latest on". Uses
                // resolveNKeys so the list is real symbols; an IN list of
                // values that do not exist returns nothing and times as a very
                // fast empty result.
                case "latest_on_indexed" -> "SELECT ts, sym, price FROM bench WHERE sym IN ("
                        + tenKeys + ") LATEST ON ts PARTITION BY sym";
                case "in_list" -> "SELECT price FROM bench WHERE sym IN (" + tenKeys + ")";
                // Residual filter variants
                case "residual_filter" -> "SELECT price FROM bench WHERE sym = '" + key + "' AND price > 500";
                case "non_covering_filter" -> "SELECT price FROM bench_nc WHERE sym = '" + ncKey + "' AND price > 500";
                case "no_index_filter" -> "SELECT price FROM bench_noidx WHERE sym = '" + niKey + "' AND price > 500";
                case "residual_filter_in" -> "SELECT price FROM bench WHERE sym IN (" + tenKeys + ") AND price > 500";
                case "non_covering_filter_in" ->
                        "SELECT price FROM bench_nc WHERE sym IN (" + tenNcKeys + ") AND price > 500";
                // VARCHAR/FSST variants
                case "varchar_fsst" -> "SELECT name, price FROM vchar WHERE sym = '" + vcharKey + "'";
                case "varchar_non_covering" -> "SELECT ts, name, price FROM vchar WHERE sym = '" + vcharKey + "'";
                case "varchar_in_covering" ->
                        "SELECT name, price FROM vchar WHERE sym IN (" + resolveNKeys(compiler, ctx, "vchar", 5) + ")";
                // Wide table, O3, non-covering baseline
                case "wide_table" -> "SELECT c1, c2 FROM wide WHERE sym = '" + wideKey + "'";
                case "o3_covering" -> "SELECT price FROM o3bench WHERE sym = '" + o3Key + "'";
                case "o3_non_covering" -> "SELECT ts FROM o3bench WHERE sym = '" + o3Key + "'";
                case "o3_distinct" -> "SELECT DISTINCT sym FROM o3bench";
                case "non_covering_where" -> "SELECT ts FROM bench WHERE sym = '" + key + "'";
                // Bulk throughput
                case "bulk_covering" -> "SELECT name, price FROM bulk WHERE sym = '" + bulkKey + "'";
                case "bulk_non_covering" -> "SELECT ts, name, price FROM bulk WHERE sym = '" + bulkKey + "'";
                default -> throw new IllegalArgumentException(queryType);
            };
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            Misc.free(compiler);
            Misc.free(engine);
            deleteDirRecursive(tmpDir.toFile());
        }
    }

    /**
     * LIMIT over a covering index vs a plain index, across covered-column
     * shapes, on a reasonably large hot-key dataset: {@link #ROWS} rows over
     * {@link #KEYS} symbols spread across ~{@code ROWS/86400} DAY partitions,
     * so each key has ~{@code ROWS/KEYS} matching rows.
     * <p>
     * The grid is direction (neg/pos) x index (cov/plain) x shape
     * (double/varchar/both). A covering LIMIT materializes the whole
     * matching-key result set up front (buildAddressCache drains every
     * sub-frame before a row is returned); the plain twin shares the data
     * shape, so the gap is the covering materialization cost. The shape axis
     * exposes how that cost scales with the covered column type -- an 8-byte
     * DOUBLE store vs a variable-length VARCHAR copy (M1's worst case).
     * <p>
     * The covering table covers BOTH name (VARCHAR) and price (DOUBLE); the
     * plain twin has the same columns without INCLUDE. Both tables are built
     * ONCE and shared across every param combination, so the grid costs only a
     * query compile per cell, not another multi-million-row load. ROWS/KEYS are
     * overridable via {@code -Dquestdb.limit.bench.rows} /
     * {@code -Dquestdb.limit.bench.keys}.
     */
    @State(Scope.Benchmark)
    public static class LimitState {
        static final int KEYS = Integer.getInteger("questdb.limit.bench.keys", 16);
        static final int ROWS = Integer.getInteger("questdb.limit.bench.rows", 5_000_000);
        private static String covKey;
        private static SqlExecutionContextImpl ctx;
        private static String ncKey;
        private static java.nio.file.Path sharedDir;
        private static CairoEngine sharedEngine;

        RecordCursorFactory factory;
        @Param({
                "neg_cov_double", "neg_plain_double", "pos_cov_double", "pos_plain_double",
                "neg_cov_varchar", "neg_plain_varchar", "pos_cov_varchar", "pos_plain_varchar",
                "neg_cov_both", "neg_plain_both", "pos_cov_both", "pos_plain_both"
        })
        String queryType;

        // Build the two large tables exactly once and share them across every
        // param combination -- adding shapes/directions then costs only a
        // compile, not another load.
        private static synchronized void ensureData() throws Exception {
            if (sharedEngine != null) {
                return;
            }
            sharedDir = Files.createTempDirectory("suite-limit");
            CairoConfiguration config = new DefaultCairoConfiguration(sharedDir.toString()) {
                @Override
                public byte getPostingIndexRowIdEncoding() {
                    return IS_DELTA ? PostingIndexUtils.ENCODING_DELTA : PostingIndexUtils.ENCODING_ADAPTIVE;
                }

                @Override
                public int getRndFunctionMemoryMaxPages() {
                    return 8192;
                }
            };
            CairoEngine engine = new CairoEngine(config);
            ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                // Covering table: posting index on sym, INCLUDE covers both the
                // VARCHAR (name) and DOUBLE (price), so any projection is served
                // from the sidecar. Plain twin: same columns, bitmap index, no
                // INCLUDE -- projected/filtered columns come from the base.
                engine.execute("CREATE TABLE lim (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                        " INCLUDE (name, price), name VARCHAR, price DOUBLE) " +
                        "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                engine.execute("INSERT INTO lim SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP, " +
                        "rnd_symbol(" + KEYS + ", 4, 8, 0), rnd_varchar(10, 30, 0), rnd_double() * 1000 " +
                        "FROM long_sequence(" + ROWS + ")", ctx);
                engine.execute("CREATE TABLE lim_nc (ts TIMESTAMP, sym SYMBOL INDEX, name VARCHAR, price DOUBLE) " +
                        "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                engine.execute("INSERT INTO lim_nc SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP, " +
                        "rnd_symbol(" + KEYS + ", 4, 8, 0), rnd_varchar(10, 30, 0), rnd_double() * 1000 " +
                        "FROM long_sequence(" + ROWS + ")", ctx);
                engine.releaseAllWriters();
                covKey = resolveKey(compiler, ctx, "lim");
                ncKey = resolveKey(compiler, ctx, "lim_nc");
            }
            sharedEngine = engine;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                Misc.free(sharedEngine);
                deleteDirRecursive(sharedDir.toFile());
            }));
        }

        @Setup(Level.Trial)
        public void setup() throws Exception {
            ensureData();
            boolean covering = queryType.contains("_cov_");
            String table = covering ? "lim" : "lim_nc";
            String key = covering ? covKey : ncKey;
            String limit = queryType.startsWith("neg") ? " LIMIT -5" : " LIMIT 5";
            String proj;
            String filter;
            if (queryType.endsWith("double")) {
                proj = "price";
                filter = "price > 500";
            } else if (queryType.endsWith("varchar")) {
                proj = "name";
                filter = "name != 'x'";
            } else {
                proj = "name, price";
                filter = "price > 500";
            }
            String sql = "SELECT " + proj + " FROM " + table + " WHERE sym = '" + key + "' AND " + filter + limit;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(sharedEngine)) {
                factory = compiler.compile(sql, ctx).getRecordCursorFactory();
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            factory = Misc.free(factory);
        }
    }

    /**
     * S9: WAL fast-lag cost suite — per-commit seal cost, point-query cost in
     * the unsealed window, and combined insert+query cost.
     * <p>
     * {@code @Setup} preloads {@link #PRELOAD_ROWS} rows at sub-second
     * timestamps within {@code 2024-01-01}, then drains the WAL queue, so
     * every benchmark invocation operates on a warmed partition rather than
     * from empty. Each {@code walFastLagInsert} invocation appends
     * {@code batchRows} rows at offset {@code (batchCounter+1) * 1s} —
     * strictly monotone, single-partition, always taking the fast-lag branch
     * inside {@code applyLagToLastPartition}.
     * <p>
     * Read benchmarks ({@code walFastLagQuery}, {@code walFastLagInsertAndQuery})
     * dispatch to a fixed pool of pre-compiled {@code SELECT count() FROM
     * walbench WHERE sym = '...'} factories sampled from the preloaded keys.
     * Pre-compilation keeps parse/plan cost out of the measurement.
     * <p>
     * Axes:
     * <ul>
     *   <li>{@code indexType}: {@code no_index} and {@code bitmap} bracket the
     *       legacy / unindexed baselines; {@code posting} measures the
     *       non-covering posting index (currently seals on fast-lag);
     *       {@code posting_covering} / {@code posting_covering_2cols} measure
     *       the covering machinery (configureCovering + sidecar rebuild).</li>
     *   <li>{@code batchRows}: rows-per-fast-lag-commit. Drives the seal
     *       cost's dependence on per-batch volume.</li>
     *   <li>{@code keyCount}: distinct symbol values. At {@code 50} every key
     *       appears in every batch (full re-encode); at {@code 100000} most
     *       keys are quiet in any one batch (sparse-gen extension). Drives
     *       both seal cost and per-key match rate at query time.</li>
     * </ul>
     */
    /**
     * State for {@code walFastLagQueryAtGen}: preload + a configurable number
     * of fast-lag commits, then queries-only during measurement so the
     * unsealed gen count is stable across iterations. Each fast-lag commit
     * adds one gen via {@code extendHead}, so {@code unsealedGens} maps
     * (modulo a small preload contribution) to the gen count the chain
     * picker walks per query.
     * <p>
     * Fixed batchRows (100) and keyCount (10000) keep prebuild cost
     * bounded; the only axes that matter for the read-cost-vs-gens
     * hypothesis are {@code indexType} and {@code unsealedGens}.
     */
    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public static class WalFastLagQueryGenState {
        static final int PREBUILD_BATCH_ROWS = 100;
        static final int PRELOAD_ROWS = 10_000;
        static final int QUERY_KEY_POOL = 32;

        ApplyWal2TableJob applyJob;
        CheckWalTransactionsJob checkJob;
        SqlCompilerImpl compiler;
        SqlExecutionContextImpl ctx;
        CairoEngine engine;
        @Param({"no_index", "bitmap", "posting", "posting_covering", "posting_covering_2cols", "posting_covering_10cols"})
        String indexType;
        @Param({"50", "10000"})
        int keyCount;
        int queryCounter;
        String[] queryKeys;
        java.nio.file.Path tmpDir;
        @Param({"1", "16", "64", "142"})
        int unsealedGens;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            tmpDir = Files.createTempDirectory("suite-walfastlag-querygen");
            CairoConfiguration config = new DefaultCairoConfiguration(tmpDir.toString()) {
                @Override
                public byte getPostingIndexRowIdEncoding() {
                    return IS_DELTA ? PostingIndexUtils.ENCODING_DELTA : PostingIndexUtils.ENCODING_ADAPTIVE;
                }

                @Override
                public int getRndFunctionMemoryMaxPages() {
                    return 4096;
                }
            };
            engine = new CairoEngine(config);
            ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            compiler = new SqlCompilerImpl(engine);

            String ddl;
            String extraColumns;
            String extraValues;
            if ("posting_covering_10cols".equals(indexType)) {
                ddl = "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                        " INCLUDE (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10), " +
                        "c1 DOUBLE, c2 FLOAT, c3 LONG, c4 INT, c5 SHORT, c6 BYTE, " +
                        "c7 DOUBLE, c8 LONG, c9 INT, c10 FLOAT) " +
                        "TIMESTAMP(ts) PARTITION BY DAY WAL";
                extraColumns = "c1, c2, c3, c4, c5, c6, c7, c8, c9, c10";
                extraValues = "rnd_double() * 1000, rnd_float() * 100, " +
                        "rnd_long(1, 1000000, 0), rnd_int(0, 10000, 0), " +
                        "rnd_short(), rnd_byte(0, 127), " +
                        "rnd_double() * 1000, rnd_long(1, 1000000, 0), " +
                        "rnd_int(0, 10000, 0), rnd_float() * 100";
            } else {
                ddl = switch (indexType) {
                    case "no_index" -> "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL, price DOUBLE, name VARCHAR) " +
                            "TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "bitmap" ->
                            "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX, price DOUBLE, name VARCHAR) " +
                                    "TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "posting" -> "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                            ", price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "posting_covering" ->
                            "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                                    " INCLUDE (price), price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "posting_covering_2cols" ->
                            "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                                    " INCLUDE (price, name), price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL";
                    default -> throw new IllegalArgumentException(indexType);
                };
                extraColumns = "price, name";
                extraValues = "rnd_double() * 1000, rnd_varchar(10, 20, 0)";
            }
            engine.execute(ddl, ctx);
            applyJob = new ApplyWal2TableJob(engine, 0);
            checkJob = new CheckWalTransactionsJob(engine);

            // Small preload: one big INSERT that the WAL apply path drains
            // before our gen-driving commits start. Smaller than the insert
            // bench's preload because per-trial setup time matters more here
            // (24+ trials each doing setup).
            String preloadSql = "INSERT INTO walbench(ts, sym, " + extraColumns + ") " +
                    "SELECT dateadd('u', x::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP), " +
                    "rnd_symbol(" + keyCount + ", 4, 8, 0), " + extraValues + " " +
                    "FROM long_sequence(" + PRELOAD_ROWS + ")";
            engine.execute(preloadSql, ctx);
            applyJob.drain(0);
            checkJob.run();
            applyJob.drain(0);

            // Drive the chain to the target unsealed gen count. Each
            // fast-lag commit adds one gen via extendHead; auto-seal at
            // genCount >= 143 caps unsealedGens at 142 for "just before
            // seal" measurements.
            for (int i = 0; i < unsealedGens; i++) {
                int batchOffsetSeconds = i + 1;
                String batchSql = "INSERT INTO walbench(ts, sym, " + extraColumns + ") " +
                        "SELECT dateadd('u', x::INT, dateadd('s', " + batchOffsetSeconds +
                        ", '2024-01-01T00:00:00.000000Z'::TIMESTAMP)), " +
                        "rnd_symbol(" + keyCount + ", 4, 8, 0), " + extraValues + " " +
                        "FROM long_sequence(" + PREBUILD_BATCH_ROWS + ")";
                engine.execute(batchSql, ctx);
                applyJob.drain(0);
                checkJob.run();
                applyJob.drain(0);
            }

            queryKeys = sampleKeys(compiler, ctx, "walbench", QUERY_KEY_POOL);
            queryCounter = 0;
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            Misc.free(applyJob);
            Misc.free(compiler);
            Misc.free(engine);
            deleteDirRecursive(tmpDir.toFile());
        }
    }

    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class WalFastLagState {
        static final int PRELOAD_ROWS = 100_000;
        static final int QUERY_KEY_POOL = 32;

        ApplyWal2TableJob applyJob;
        int batchCounter;
        @Param({"100", "1000", "10000"})
        int batchRows;
        CheckWalTransactionsJob checkJob;
        SqlCompilerImpl compiler;
        SqlExecutionContextImpl ctx;
        CairoEngine engine;
        // Column list used in the INSERT statement, e.g. "price, name" or
        // "c1, c2, ..., c10". Computed in setup based on indexType so the
        // 10cols schema can opt out of the price/name layout.
        String extraColumns;
        // SELECT expression list matching extraColumns 1:1, e.g.
        // "rnd_double() * 1000, rnd_varchar(10, 20, 0)".
        String extraValues;
        @Param({"no_index", "bitmap", "posting", "posting_covering", "posting_covering_2cols", "posting_covering_10cols"})
        String indexType;
        @Param({"50", "1000", "10000", "100000"})
        int keyCount;
        int queryCounter;
        String[] queryKeys;
        java.nio.file.Path tmpDir;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            tmpDir = Files.createTempDirectory("suite-walfastlag");
            CairoConfiguration config = new DefaultCairoConfiguration(tmpDir.toString()) {
                @Override
                public byte getPostingIndexRowIdEncoding() {
                    return IS_DELTA ? PostingIndexUtils.ENCODING_DELTA : PostingIndexUtils.ENCODING_ADAPTIVE;
                }

                @Override
                public int getRndFunctionMemoryMaxPages() {
                    return 4096;
                }
            };
            engine = new CairoEngine(config);
            ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            compiler = new SqlCompilerImpl(engine);

            String ddl;
            if ("posting_covering_10cols".equals(indexType)) {
                ddl = "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                        " INCLUDE (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10), " +
                        "c1 DOUBLE, c2 FLOAT, c3 LONG, c4 INT, c5 SHORT, c6 BYTE, " +
                        "c7 DOUBLE, c8 LONG, c9 INT, c10 FLOAT) " +
                        "TIMESTAMP(ts) PARTITION BY DAY WAL";
                extraColumns = "c1, c2, c3, c4, c5, c6, c7, c8, c9, c10";
                extraValues = "rnd_double() * 1000, rnd_float() * 100, " +
                        "rnd_long(1, 1000000, 0), rnd_int(0, 10000, 0), " +
                        "rnd_short(), rnd_byte(0, 127), " +
                        "rnd_double() * 1000, rnd_long(1, 1000000, 0), " +
                        "rnd_int(0, 10000, 0), rnd_float() * 100";
            } else {
                ddl = switch (indexType) {
                    case "no_index" -> "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL, price DOUBLE, name VARCHAR) " +
                            "TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "bitmap" ->
                            "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX, price DOUBLE, name VARCHAR) " +
                                    "TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "posting" -> "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                            ", price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "posting_covering" ->
                            "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                                    " INCLUDE (price), price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "posting_covering_2cols" ->
                            "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                                    " INCLUDE (price, name), price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL";
                    default -> throw new IllegalArgumentException(indexType);
                };
                extraColumns = "price, name";
                extraValues = "rnd_double() * 1000, rnd_varchar(10, 20, 0)";
            }
            engine.execute(ddl, ctx);
            applyJob = new ApplyWal2TableJob(engine, 0);
            checkJob = new CheckWalTransactionsJob(engine);

            // Preload occupies offsets [1us, PRELOAD_ROWS us] (= 100ms span)
            // within 2024-01-01. Bench batches start at offset >= 1s, well
            // after the preload tail, so they always exercise the fast-lag
            // branch on a warmed partition.
            String preloadSql = "INSERT INTO walbench(ts, sym, " + extraColumns + ") " +
                    "SELECT dateadd('u', x::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP), " +
                    "rnd_symbol(" + keyCount + ", 4, 8, 0), " + extraValues + " " +
                    "FROM long_sequence(" + PRELOAD_ROWS + ")";
            engine.execute(preloadSql, ctx);
            applyJob.drain(0);
            checkJob.run();
            applyJob.drain(0);

            // Sample distinct symbols from the preloaded data for the read
            // benches. Pre-compiling factories does not survive WAL apply
            // (table metadata version bumps invalidate the cached plan), so
            // benches recompile per invocation as the existing SqlState
            // pattern does. Compile cost is constant across runs and
            // cancels in the differential analysis (baseline vs candidate).
            queryKeys = sampleKeys(compiler, ctx, "walbench", QUERY_KEY_POOL);

            batchCounter = 0;
            queryCounter = 0;
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            Misc.free(applyJob);
            Misc.free(compiler);
            Misc.free(engine);
            deleteDirRecursive(tmpDir.toFile());
        }
    }

    /**
     * State for walLargePartitionO3AppendInsert: preload day 1 with
     * partitionSize rows at strictly increasing timestamps, plus a single
     * sentinel row in day 2 so that day 1 is no longer the last partition.
     * The benchmark body then appends to day 1 at timestamps after its
     * existing max, taking sealPostingIndexForPartition's canSkipRebuild=true
     * branch (the sealIfMultiGen / rebuildSidecarsByCopy fast path).
     */
    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class WalLargePartitionO3AppendState {
        static final int BATCH_ROWS = 100_000;
        // Day 1's preload sits in [1us, PRELOAD_TS_LIMIT_US]. The bench body
        // inserts at +14h, which is well after preload max.
        static final int PRELOAD_TS_LIMIT_US = 200_000_000;

        ApplyWal2TableJob applyJob;
        CheckWalTransactionsJob checkJob;
        SqlCompilerImpl compiler;
        SqlExecutionContextImpl ctx;
        CairoEngine engine;
        String extraColumns;
        String extraValues;
        @Param({"no_index", "bitmap", "posting", "posting_covering", "posting_covering_10cols"})
        String indexType;
        int keyCount = 1000;
        @Param({"1000000"})
        int partitionSize;
        java.nio.file.Path tmpDir;

        @Setup(Level.Iteration)
        public void setup() throws Exception {
            tmpDir = Files.createTempDirectory("suite-largepart-o3-append");
            CairoConfiguration config = new DefaultCairoConfiguration(tmpDir.toString()) {
                @Override
                public byte getPostingIndexRowIdEncoding() {
                    return IS_DELTA ? PostingIndexUtils.ENCODING_DELTA : PostingIndexUtils.ENCODING_ADAPTIVE;
                }

                @Override
                public int getRndFunctionMemoryMaxPages() {
                    return 4096;
                }
            };
            engine = new CairoEngine(config);
            ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            compiler = new SqlCompilerImpl(engine);

            String ddl;
            if ("posting_covering_10cols".equals(indexType)) {
                ddl = "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                        " INCLUDE (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10), " +
                        "c1 DOUBLE, c2 FLOAT, c3 LONG, c4 INT, c5 SHORT, c6 BYTE, " +
                        "c7 DOUBLE, c8 LONG, c9 INT, c10 FLOAT) " +
                        "TIMESTAMP(ts) PARTITION BY DAY WAL";
                extraColumns = "c1, c2, c3, c4, c5, c6, c7, c8, c9, c10";
                extraValues = "rnd_double() * 1000, rnd_float() * 100, " +
                        "rnd_long(1, 1000000, 0), rnd_int(0, 10000, 0), " +
                        "rnd_short(), rnd_byte(0, 127), " +
                        "rnd_double() * 1000, rnd_long(1, 1000000, 0), " +
                        "rnd_int(0, 10000, 0), rnd_float() * 100";
            } else {
                ddl = switch (indexType) {
                    case "no_index" -> "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL, price DOUBLE, name VARCHAR) " +
                            "TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "bitmap" ->
                            "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX, price DOUBLE, name VARCHAR) " +
                                    "TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "posting" -> "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                            ", price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "posting_covering" ->
                            "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                                    " INCLUDE (price), price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL";
                    default -> throw new IllegalArgumentException(indexType);
                };
                extraColumns = "price, name";
                extraValues = "rnd_double() * 1000, rnd_varchar(10, 20, 0)";
            }
            engine.execute(ddl, ctx);
            applyJob = new ApplyWal2TableJob(engine, 0);
            checkJob = new CheckWalTransactionsJob(engine);

            // Preload day 1 with partitionSize rows at strictly increasing ts
            // within [1us, PRELOAD_TS_LIMIT_US] (well before +14h).
            int batches = partitionSize / BATCH_ROWS;
            for (int i = 0; i < batches; i++) {
                int batchOffsetUs = i * BATCH_ROWS + 1;
                String batchSql = "INSERT INTO walbench(ts, sym, " + extraColumns + ") " +
                        "SELECT dateadd('u', x::INT + " + batchOffsetUs + ", '2024-01-01T00:00:00.000000Z'::TIMESTAMP), " +
                        "rnd_symbol(" + keyCount + ", 4, 8, 0), " + extraValues + " " +
                        "FROM long_sequence(" + BATCH_ROWS + ")";
                engine.execute(batchSql, ctx);
                applyJob.drain(0);
                checkJob.run();
                applyJob.drain(0);
            }

            // Sentinel row in day 2 makes day 1 non-last; subsequent inserts
            // into day 1 take the O3 commit path.
            String sentinelSql = "INSERT INTO walbench(ts, sym, " + extraColumns + ") " +
                    "VALUES ('2024-01-02T00:00:00.000001Z', '_sentinel_', " +
                    sentinelExtraValues() + ")";
            engine.execute(sentinelSql, ctx);
            applyJob.drain(0);
            checkJob.run();
            applyJob.drain(0);
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            Misc.free(applyJob);
            Misc.free(compiler);
            Misc.free(engine);
            deleteDirRecursive(tmpDir.toFile());
        }

        private String sentinelExtraValues() {
            if ("posting_covering_10cols".equals(indexType)) {
                return "0.0, 0.0, 0, 0, 0::SHORT, 0::BYTE, 0.0, 0, 0, 0.0";
            }
            return "0.0, 'x'";
        }
    }

    /**
     * State for walLargePartitionO3Insert: preload a partition then have
     * each invocation insert a 100k-row batch with timestamps spread
     * randomly across the preloaded range. The interleaving forces
     * partitionMutates=true, taking sealPostingIndexForPartition's
     * canSkipRebuild=false branch (rebuild-from-data) — the heaviest
     * O3 commit path for POSTING indexes.
     */
    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class WalLargePartitionO3State {
        static final int BATCH_ROWS = 100_000;
        static final int PRELOAD_TS_LIMIT_US = 200_000_000;

        ApplyWal2TableJob applyJob;
        CheckWalTransactionsJob checkJob;
        SqlCompilerImpl compiler;
        SqlExecutionContextImpl ctx;
        CairoEngine engine;
        String extraColumns;
        String extraValues;
        @Param({"no_index", "bitmap", "posting", "posting_covering", "posting_covering_10cols"})
        String indexType;
        int keyCount = 1000;
        @Param({"1000000", "10000000", "10000000"})
        int partitionSize;
        java.nio.file.Path tmpDir;

        @Setup(Level.Iteration)
        public void setup() throws Exception {
            tmpDir = Files.createTempDirectory("suite-largepart-o3");
            CairoConfiguration config = new DefaultCairoConfiguration(tmpDir.toString()) {
                @Override
                public byte getPostingIndexRowIdEncoding() {
                    return IS_DELTA ? PostingIndexUtils.ENCODING_DELTA : PostingIndexUtils.ENCODING_ADAPTIVE;
                }

                @Override
                public int getRndFunctionMemoryMaxPages() {
                    return 4096;
                }
            };
            engine = new CairoEngine(config);
            ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            compiler = new SqlCompilerImpl(engine);

            String ddl;
            if ("posting_covering_10cols".equals(indexType)) {
                ddl = "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                        " INCLUDE (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10), " +
                        "c1 DOUBLE, c2 FLOAT, c3 LONG, c4 INT, c5 SHORT, c6 BYTE, " +
                        "c7 DOUBLE, c8 LONG, c9 INT, c10 FLOAT) " +
                        "TIMESTAMP(ts) PARTITION BY DAY WAL";
                extraColumns = "c1, c2, c3, c4, c5, c6, c7, c8, c9, c10";
                extraValues = "rnd_double() * 1000, rnd_float() * 100, " +
                        "rnd_long(1, 1000000, 0), rnd_int(0, 10000, 0), " +
                        "rnd_short(), rnd_byte(0, 127), " +
                        "rnd_double() * 1000, rnd_long(1, 1000000, 0), " +
                        "rnd_int(0, 10000, 0), rnd_float() * 100";
            } else {
                ddl = switch (indexType) {
                    case "no_index" -> "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL, price DOUBLE, name VARCHAR) " +
                            "TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "bitmap" ->
                            "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX, price DOUBLE, name VARCHAR) " +
                                    "TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "posting" -> "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                            ", price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "posting_covering" ->
                            "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                                    " INCLUDE (price), price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL";
                    default -> throw new IllegalArgumentException(indexType);
                };
                extraColumns = "price, name";
                extraValues = "rnd_double() * 1000, rnd_varchar(10, 20, 0)";
            }
            engine.execute(ddl, ctx);
            applyJob = new ApplyWal2TableJob(engine, 0);
            checkJob = new CheckWalTransactionsJob(engine);

            // Preload partitionSize rows in 100k-row batches at strictly
            // increasing timestamps within [1us, PRELOAD_TS_LIMIT_US].
            // The benchmark body then interleaves new rows across this
            // entire range, forcing O3 mutation on every commit.
            int batches = partitionSize / BATCH_ROWS;
            for (int i = 0; i < batches; i++) {
                int batchOffsetUs = i * BATCH_ROWS + 1;
                String batchSql = "INSERT INTO walbench(ts, sym, " + extraColumns + ") " +
                        "SELECT dateadd('u', x::INT + " + batchOffsetUs + ", '2024-01-01T00:00:00.000000Z'::TIMESTAMP), " +
                        "rnd_symbol(" + keyCount + ", 4, 8, 0), " + extraValues + " " +
                        "FROM long_sequence(" + BATCH_ROWS + ")";
                engine.execute(batchSql, ctx);
                applyJob.drain(0);
                checkJob.run();
                applyJob.drain(0);
            }
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            Misc.free(applyJob);
            Misc.free(compiler);
            Misc.free(engine);
            deleteDirRecursive(tmpDir.toFile());
        }
    }

    /**
     * State for {@code walLargePartitionO3SpillReseal}: preload a single ~1M-row
     * DAY partition, then let the benchmark O3-mutate it so the posting index
     * reseals from the column file. {@code keyCount} is small so each symbol is
     * hot and re-indexing the partition spills enough rowids to cross the small
     * {@link #spillBytesMax} budgets. {@code spillBytesMax} controls how often
     * the reseal's index() loop flushes mid-stream, selecting commitDense's
     * fast path (no flush) vs its seal() consolidation path (>=1 flush).
     */
    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class WalLargePartitionO3SpillState {
        static final int BATCH_ROWS = 100_000;
        static final int PARTITION_SIZE = 1_000_000;
        // Matches the preload span (1us spacing over PARTITION_SIZE rows) so the
        // benchmark's O3 rows land inside the existing data and force mutation
        // (canSkipRebuild=false), not a pure append.
        static final int PRELOAD_TS_LIMIT_US = PARTITION_SIZE;

        ApplyWal2TableJob applyJob;
        CheckWalTransactionsJob checkJob;
        SqlCompilerImpl compiler;
        SqlExecutionContextImpl ctx;
        CairoEngine engine;
        String extraColumns;
        String extraValues;
        @Param({"posting_covering", "posting"})
        String indexType;
        int keyCount = 100;
        // cairo.posting.index.indexer.spill.bytes.max. 256MiB never flushes
        // mid-stream (flushAllPendingDense fast path = no-regression baseline);
        // 2MiB and 256KiB trip compactIfOverBudget so commitDense routes through
        // seal() (the path the fix added).
        @Param({"268435456", "2097152", "262144"})
        long spillBytesMax;
        java.nio.file.Path tmpDir;

        @Setup(Level.Iteration)
        public void setup() throws Exception {
            tmpDir = Files.createTempDirectory("suite-largepart-o3-spill");
            CairoConfiguration config = new DefaultCairoConfiguration(tmpDir.toString()) {
                @Override
                public long getPostingIndexerSpillBytesMax() {
                    return spillBytesMax;
                }

                @Override
                public byte getPostingIndexRowIdEncoding() {
                    return IS_DELTA ? PostingIndexUtils.ENCODING_DELTA : PostingIndexUtils.ENCODING_ADAPTIVE;
                }

                @Override
                public int getRndFunctionMemoryMaxPages() {
                    return 4096;
                }

                @Override
                public boolean isPostingIndexAutoIncludeTimestamp() {
                    // Without this, POSTING auto-covers the designated timestamp,
                    // so the "posting" variant would also be covering and the two
                    // @Param values would measure the same path. Disabling it keeps
                    // "posting" on the non-covering reseal (commitDense -> seal,
                    // no rebuildSidecars) and "posting_covering" on the covering
                    // reseal via the explicit INCLUDE (price) below.
                    return false;
                }
            };
            engine = new CairoEngine(config);
            ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            compiler = new SqlCompilerImpl(engine);

            String ddl = switch (indexType) {
                case "posting" -> "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                        ", price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL";
                case "posting_covering" -> "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                        " INCLUDE (price), price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL";
                default -> throw new IllegalArgumentException(indexType);
            };
            extraColumns = "price, name";
            extraValues = "rnd_double() * 1000, rnd_varchar(10, 20, 0)";
            engine.execute(ddl, ctx);
            applyJob = new ApplyWal2TableJob(engine, 0);
            checkJob = new CheckWalTransactionsJob(engine);

            int batches = PARTITION_SIZE / BATCH_ROWS;
            for (int i = 0; i < batches; i++) {
                int batchOffsetUs = i * BATCH_ROWS + 1;
                String batchSql = "INSERT INTO walbench(ts, sym, " + extraColumns + ") " +
                        "SELECT dateadd('u', x::INT + " + batchOffsetUs + ", '2024-01-01T00:00:00.000000Z'::TIMESTAMP), " +
                        "rnd_symbol(" + keyCount + ", 4, 8, 0), " + extraValues + " " +
                        "FROM long_sequence(" + BATCH_ROWS + ")";
                engine.execute(batchSql, ctx);
                applyJob.drain(0);
                checkJob.run();
                applyJob.drain(0);
            }
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            Misc.free(applyJob);
            Misc.free(compiler);
            Misc.free(engine);
            deleteDirRecursive(tmpDir.toFile());
        }
    }

    /**
     * State for {@code walLargePartitionInsert} and {@code walLargePartitionQuery}:
     * preload the table with {@code partitionSize} rows in {@link #BATCH_ROWS}-row
     * batches (i.e. {@code partitionSize / 100_000} fast-lag commits) before any
     * measurement begins. Single DAY partition; commits are spaced by 1us so the
     * full 100M-row preload still fits inside one partition.
     * <p>
     * Auto-seal at gen >= 143 fires multiple times during preload for the larger
     * sizes (100M / 100k = 1000 commits → ~7 seal cycles), so the post-setup state
     * has both a sealed prefix on disk and a small unsealed tail.
     */
    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class WalLargePartitionState {
        static final int BATCH_ROWS = 100_000;
        // Reserve [1us, PRELOAD_TS_LIMIT_US] for setup INSERTs. After setup
        // completes, walLargePartitionInsert appends at offsets above this.
        static final int PRELOAD_TS_LIMIT_US = 200_000_000;
        static final int QUERY_KEY_POOL = 32;

        ApplyWal2TableJob applyJob;
        int batchCounter;
        CheckWalTransactionsJob checkJob;
        SqlCompilerImpl compiler;
        SqlExecutionContextImpl ctx;
        CairoEngine engine;
        String extraColumns;
        String extraValues;
        // posting (non-covering) and posting_covering exercise the
        // sealPostingIndexForPartition fast-path's non-covering
        // sealIfMultiGen branch and the covering rebuildSidecarsByCopy
        // memcpy path respectively. posting_covering_10cols hits
        // rebuildSidecarsByCopy with the widest sidecar footprint.
        @Param({"no_index", "bitmap", "posting", "posting_covering", "posting_covering_10cols"})
        String indexType;
        // Bench keeps the per-batch sym cardinality at 1000 so each batch
        // touches a sizeable subset of keys without exploding compile cost
        // for queries.
        int keyCount = 1000;
        @Param({"1000000", "10000000", "100000000"})
        int partitionSize;
        int queryCounter;
        String[] queryKeys;
        java.nio.file.Path tmpDir;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            tmpDir = Files.createTempDirectory("suite-largepart");
            CairoConfiguration config = new DefaultCairoConfiguration(tmpDir.toString()) {
                @Override
                public byte getPostingIndexRowIdEncoding() {
                    return IS_DELTA ? PostingIndexUtils.ENCODING_DELTA : PostingIndexUtils.ENCODING_ADAPTIVE;
                }

                @Override
                public int getRndFunctionMemoryMaxPages() {
                    return 4096;
                }
            };
            engine = new CairoEngine(config);
            ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            compiler = new SqlCompilerImpl(engine);

            String ddl;
            if ("posting_covering_10cols".equals(indexType)) {
                ddl = "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                        " INCLUDE (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10), " +
                        "c1 DOUBLE, c2 FLOAT, c3 LONG, c4 INT, c5 SHORT, c6 BYTE, " +
                        "c7 DOUBLE, c8 LONG, c9 INT, c10 FLOAT) " +
                        "TIMESTAMP(ts) PARTITION BY DAY WAL";
                extraColumns = "c1, c2, c3, c4, c5, c6, c7, c8, c9, c10";
                extraValues = "rnd_double() * 1000, rnd_float() * 100, " +
                        "rnd_long(1, 1000000, 0), rnd_int(0, 10000, 0), " +
                        "rnd_short(), rnd_byte(0, 127), " +
                        "rnd_double() * 1000, rnd_long(1, 1000000, 0), " +
                        "rnd_int(0, 10000, 0), rnd_float() * 100";
            } else {
                ddl = switch (indexType) {
                    case "no_index" -> "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL, price DOUBLE, name VARCHAR) " +
                            "TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "bitmap" ->
                            "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX, price DOUBLE, name VARCHAR) " +
                                    "TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "posting" -> "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                            ", price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL";
                    case "posting_covering" ->
                            "CREATE TABLE walbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL +
                                    " INCLUDE (price), price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL";
                    default -> throw new IllegalArgumentException(indexType);
                };
                extraColumns = "price, name";
                extraValues = "rnd_double() * 1000, rnd_varchar(10, 20, 0)";
            }
            engine.execute(ddl, ctx);
            applyJob = new ApplyWal2TableJob(engine, 0);
            checkJob = new CheckWalTransactionsJob(engine);

            // Preload partitionSize rows in 100k-row batches. Each batch is
            // one INSERT + WAL drain = one fast-lag commit. Auto-seal at
            // genCount>=143 fires multiple times during a 1000-batch
            // (100M-row) preload, producing a sealed prefix on disk plus a
            // small unsealed tail.
            int batches = partitionSize / BATCH_ROWS;
            for (int i = 0; i < batches; i++) {
                int batchOffsetUs = i * BATCH_ROWS + 1;
                String batchSql = "INSERT INTO walbench(ts, sym, " + extraColumns + ") " +
                        "SELECT dateadd('u', x::INT + " + batchOffsetUs + ", '2024-01-01T00:00:00.000000Z'::TIMESTAMP), " +
                        "rnd_symbol(" + keyCount + ", 4, 8, 0), " + extraValues + " " +
                        "FROM long_sequence(" + BATCH_ROWS + ")";
                engine.execute(batchSql, ctx);
                applyJob.drain(0);
                checkJob.run();
                applyJob.drain(0);
            }

            queryKeys = sampleKeys(compiler, ctx, "walbench", QUERY_KEY_POOL);
            batchCounter = 0;
            queryCounter = 0;
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            Misc.free(applyJob);
            Misc.free(compiler);
            Misc.free(engine);
            deleteDirRecursive(tmpDir.toFile());
        }
    }

    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class SealState {
        CairoConfiguration config;
        String dir;
        @Param({"POSTING", "POSTING_PARQUET"})
        String format;
        boolean isParquet;
        int[] keys;
        int keyCount;
        long coverAddr;
        int rowCount;
        @Param({"400000", "2000000"})
        int rows;
        @Param({"16", "2000"})
        int distinctKeys;
        private int sealSeq;

        @Setup(Level.Trial)
        public void setup() {
            String tmpDir = System.getProperty("java.io.tmpdir");
            config = benchConfig(tmpDir);
            isParquet = "POSTING_PARQUET".equals(format);
            rowCount = rows;
            keyCount = distinctKeys;
            keys = buildRoundRobin(rowCount, keyCount);
            // One fixed-width covered column, the shape the parquet seal
            // supports and the native chain writes to a .pc sidecar.
            coverAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            Random rng = new Random(7);
            for (int i = 0; i < rowCount; i++) {
                Unsafe.putDouble(coverAddr + (long) i * Double.BYTES, rng.nextDouble() * 1000);
            }
            dir = tmpDir + File.separator + "suite_seal_" + format + '_' + rows + '_' + distinctKeys
                    + '_' + System.nanoTime();
            new File(dir).mkdirs();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            Unsafe.free(coverAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            deleteDir(dir);
        }

        void seal() {
            // A fresh index txn per invocation: the artifacts are named by it,
            // so reusing one would have each seal overwrite the last and
            // measure a rewrite rather than a write.
            final int seq = ++sealSeq;
            if (isParquet) {
                final ObjList<CharSequence> names = new ObjList<>();
                names.add("price");
                final IntList types = new IntList();
                types.add(ColumnType.DOUBLE);
                final IntList writerIndices = new IntList();
                writerIndices.add(2);
                final LongList addrs = new LongList();
                addrs.add(coverAddr);
                final LongList tops = new LongList();
                tops.add(0);
                sealParquetArm(config, dir, keys, rowCount, keyCount, 0,
                        names, types, writerIndices, addrs, tops, seq);
            } else {
                try (Path path = new Path().of(dir)) {
                    try (PostingIndexWriter writer =
                                 new PostingIndexWriter(config, path, "test" + seq, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{coverAddr}, new long[]{0},
                                new int[]{3}, new int[]{2},
                                new int[]{ColumnType.DOUBLE}, 1);
                        for (int i = 0; i < rowCount; i++) {
                            writer.add(keys[i], i);
                        }
                        writer.setMaxValue(rowCount - 1);
                        writer.seal();
                    }
                }
            }
        }
    }

    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class WriteState {
        SqlCompilerImpl compiler;
        SqlExecutionContextImpl ctx;
        String ddl;
        CairoEngine engine;
        @Param({"no_index", "bitmap", "posting", "posting_covering", "posting_varchar"})
        String indexType;
        String insertSql;
        java.nio.file.Path tmpDir;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            tmpDir = Files.createTempDirectory("suite-write");
            CairoConfiguration config = new DefaultCairoConfiguration(tmpDir.toString()) {
                @Override
                public byte getPostingIndexRowIdEncoding() {
                    return IS_DELTA ? PostingIndexUtils.ENCODING_DELTA : PostingIndexUtils.ENCODING_ADAPTIVE;
                }

                @Override
                public int getRndFunctionMemoryMaxPages() {
                    return 4096;
                }
            };
            engine = new CairoEngine(config);
            ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            compiler = new SqlCompilerImpl(engine);

            ddl = switch (indexType) {
                case "no_index" -> "CREATE TABLE wbench (ts TIMESTAMP, sym SYMBOL, price DOUBLE, name VARCHAR) " +
                        "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL";
                case "bitmap" -> "CREATE TABLE wbench (ts TIMESTAMP, sym SYMBOL INDEX, price DOUBLE, name VARCHAR) " +
                        "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL";
                case "posting" ->
                        "CREATE TABLE wbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + ", price DOUBLE, name VARCHAR) " +
                                "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL";
                case "posting_covering" ->
                        "CREATE TABLE wbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + " INCLUDE (price), " +
                                "price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL";
                case "posting_varchar" ->
                        "CREATE TABLE wbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + " INCLUDE (price, name), " +
                                "price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL";
                default -> throw new IllegalArgumentException(indexType);
            };
            insertSql = "INSERT INTO wbench SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP, " +
                    "rnd_symbol(50, 4, 8, 0), rnd_double() * 1000, rnd_varchar(10, 20, 0) " +
                    "FROM long_sequence(50000)";
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            Misc.free(compiler);
            Misc.free(engine);
            deleteDirRecursive(tmpDir.toFile());
        }
    }
}
