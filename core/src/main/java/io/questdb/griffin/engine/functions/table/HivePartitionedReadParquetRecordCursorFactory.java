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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ParquetFileCache;
import io.questdb.cairo.ProjectableRecordCursorFactory;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.table.PageFrameRecordCursorImpl;
import io.questdb.griffin.engine.table.PageFrameRowCursorFactory;
import io.questdb.griffin.engine.table.PushdownFilterExtractor;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8StringList;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * Reads many parquet files matched by a glob pattern as a single result set.
 * Owns the underlying glob cursor factory and the single-file parquet cursor.
 * <p>
 * The factory's metadata is the parquet schema (first {@code parquetColumnCount}
 * columns) concatenated with hive partition columns derived from {@code key=value}
 * segments in the directory path. Partition column types are inferred from the
 * values encountered across all matched files.
 * <p>
 * <b>Snapshot contract.</b> The factory takes two snapshots at planning time and
 * caches them for its lifetime:
 * <ul>
 *   <li>The matched-file list (no per-query directory walks).</li>
 *   <li>For each file the cursor opens, its fd + mmap + parsed parquet footer
 *       (the per-file {@link CachedFile} cache, bounded by
 *       {@code cairo.sql.parquet.hive.max.open.files}).</li>
 * </ul>
 * Both caches are invalidated only on factory close. If a user rewrites or
 * truncates a matched parquet file while the factory is alive (e.g. a cached
 * prepared statement re-running against rewritten data), queries will continue
 * to read the snapshot bytes through the held mmap. To pick up fresh data,
 * recompile the query or close the prepared statement. This mirrors the
 * single-file {@link ReadParquetFunctionFactory} behaviour for an in-progress
 * cursor and is the price of skipping the open+mmap+footer-parse cost on every
 * cursor reuse - the dominant cost for repeated queries against many small files.
 * <p>
 * Two execution paths coexist:
 * <ul>
 *   <li>Page-frame path (default). Emits one frame per parquet row group across
 *       all files; partition values come from per-file native buffers via the
 *       virtual page overlay. Supports every inferred partition column type, and
 *       the surrounding async filter pipeline executes frames in parallel when
 *       worker threads are available.</li>
 *   <li>Sequential record cursor path, used when
 *       {@code cairo.sql.parquet.hive.parallel.enabled=false}. Walks files
 *       single-threaded and materialises partition values on the wrapping
 *       {@link io.questdb.cairo.sql.Record}. Provided as a safety fallback.</li>
 * </ul>
 */
public class HivePartitionedReadParquetRecordCursorFactory extends ProjectableRecordCursorFactory {
    private static final Log LOG = LogFactory.getLog(HivePartitionedReadParquetRecordCursorFactory.class);
    private final boolean canPageFrame;
    private final CairoConfiguration configuration;
    private final CharSequence globPattern;
    // The matched file list is enumerated once at planning time and held here for
    // the factory's lifetime. Both cursor backends iterate this list directly - no
    // per-getCursor / per-size() / per-iteration directory walks. Owned by the
    // factory; freed in _close. Critical prep for remote-backed reads where
    // re-enumeration is expensive.
    private final DirectUtf8StringList matchedFiles;
    private final int nonGlobRootByteLen;
    private final int parquetColumnCount;
    private final GenericRecordMetadata parquetMetadata;
    private final ObjList<String> partitionColumnNames;
    private final IntList partitionColumnTypes;
    private PageFrameRecordCursorImpl pageFrameRecordCursor;
    private HivePartitionedReadParquetPageFrameCursor pageFrameCursor;
    // Per pruned-column metadata, built by setQueryProjectedMetadata. For each
    // entry, exactly one of projectedToParquetWriterIdx / projectedToPartitionIdx
    // is >= 0 (the other is -1). Both null means no projection - cursor uses the
    // full schema with parquet columns first then partition virtual columns.
    private int[] projectedToParquetWriterIdx;
    private int[] projectedToPartitionIdx;
    private @Nullable ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions;
    // When true, the legacy serial cursor walks matchedFiles end-to-start AND
    // each file's underlying single-file cursor iterates rows in reverse order.
    // The parallel page-frame backend cannot honour reverse iteration because
    // each emitted PageFrame is consumed in row-group storage order downstream
    // and there is no row-reverse hook on the frame contract; in that mode
    // {@link #getScanDirection()} stays {@code FORWARD} and the planner gives
    // up the elision. Set by {@link #setReverseScan}; effective only when the
    // configuration disables hive parallel page frames or a subclass disables
    // it via {@link #canPageFrame}. Must be set BEFORE the first
    // {@code getCursor()} call - the cursor's reverse flag is synced once at
    // construction.
    private boolean reverseScan;
    // Total row count summed across every matched file, computed once and cached
    // for the factory's lifetime. -1 = not yet computed. Lazy because the planning
    // walk doesn't open parquet files (it parses partition keys from paths only),
    // so the first count(*) / size() pays the metadata-read cost; subsequent calls
    // on the same factory get O(1). Mutating writes are synchronized on `this`;
    // reads use the volatile flag.
    private volatile long cachedTotalRowCount = -1;
    // Parallel to matchedFiles: per-file footer row count, populated alongside
    // cachedTotalRowCount. Lets size() / calculateSize() compute the prune-aware
    // emitted row count without re-opening files. Reads under the synchronized
    // lock; published with cachedTotalRowCount.
    private final io.questdb.std.LongList cachedPerFileRowCounts = new io.questdb.std.LongList();
    // Engine-shared cache of opened+mmapped+footer-parsed parquet files. Each
    // cursor borrows entries via {@code acquire()}/{@code release()} - the
    // factory itself does not hold long-lived refs (prefetch acquires-and-releases
    // as a warm-up hint). Memory is bounded globally by cairo.parquet.cache.max.*
    // rather than per-factory.
    private final ParquetFileCache parquetCache;
    // Set true by _close to refuse new prefetch submissions after the factory has
    // been told to shut down. The shared cache itself is engine-owned and never
    // shuts down with the factory; this flag is only used to wind down the
    // prefetch executor cleanly.
    private volatile boolean closed;
    // Single-thread executor used to open files ahead of the cursor's iteration.
    // Lazy: only created if a query actually triggers a prefetch. Daemon thread so
    // it doesn't hold the JVM open. Synchronisation with the cursor's main-thread
    // open path happens naturally via openCachedFile's `synchronized` lock - either
    // the prefetch thread or the cursor wins the race and the loser observes the
    // cached entry.
    private volatile ExecutorService prefetchExecutor;

    public HivePartitionedReadParquetRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull ParquetFileCache parquetCache,
            @NotNull DirectUtf8StringList matchedFiles,
            @NotNull CharSequence globPattern,
            int nonGlobRootByteLen,
            @NotNull GenericRecordMetadata wrappingMetadata,
            @NotNull GenericRecordMetadata parquetMetadata,
            @NotNull ObjList<String> partitionColumnNames,
            @NotNull IntList partitionColumnTypes
    ) {
        super(wrappingMetadata);
        this.configuration = configuration;
        this.parquetCache = parquetCache;
        this.matchedFiles = matchedFiles;
        this.globPattern = globPattern.toString();
        this.nonGlobRootByteLen = nonGlobRootByteLen;
        this.parquetColumnCount = parquetMetadata.getColumnCount();
        this.parquetMetadata = parquetMetadata;
        this.partitionColumnNames = partitionColumnNames;
        this.partitionColumnTypes = partitionColumnTypes;
        this.canPageFrame = configuration.isSqlParquetHiveParallelEnabled();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (canPageFrame) {
            return getPageFrameBackedCursor(executionContext);
        }
        return getLegacyCursor(executionContext);
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        if (!canPageFrame) {
            return null;
        }
        if (pageFrameCursor == null) {
            pageFrameCursor = new HivePartitionedReadParquetPageFrameCursor(
                    configuration.getFilesFacade(),
                    matchedFiles,
                    parquetMetadata,
                    parquetColumnCount,
                    partitionColumnNames,
                    partitionColumnTypes,
                    nonGlobRootByteLen,
                    configuration.getParquetCacheMaxEntries(),
                    projectedToParquetWriterIdx,
                    projectedToPartitionIdx,
                    pushdownFilterConditions,
                    this
            );
        }
        pageFrameCursor.of(executionContext);
        return pageFrameCursor;
    }

    /**
     * Reports whether the planner can flip this factory's iteration order via
     * {@link #setReverseScan}. The parallel page-frame backend cannot - its
     * frames are consumed in row-group storage order downstream and there is
     * no row-reverse hook on the frame contract. Only the legacy serial
     * backend (gated by {@code cairo.sql.parquet.hive.parallel.enabled=false})
     * supports reverse iteration today.
     */
    public boolean canScanInReverse() {
        return !canPageFrame;
    }

    @Override
    public int getScanDirection() {
        // Reverse only takes effect when the cursor backend actually walks rows
        // in reverse. With the parallel page-frame backend active, ignore the
        // flag and report FORWARD so the planner's elision check declines and
        // a downstream SORT operator gets inserted - same behaviour as before
        // reverseScan existed.
        return (reverseScan && canScanInReverse()) ? SCAN_DIRECTION_BACKWARD : SCAN_DIRECTION_FORWARD;
    }

    @Override
    public boolean mayHaveParquetPartitions(SqlExecutionContext executionContext) {
        return true;
    }

    @Override
    public boolean isFilterFullyConsumedByPushdown(@Nullable ExpressionNode filterExpr) {
        // We can drop the row-level filter only when EVERY conjunct in the WHERE clause
        // is a pushdown-able predicate on a hive partition column with a precisely
        // comparable type. File-level pruning then guarantees every surviving row's
        // partition value satisfies the predicate - so re-evaluating it per row is
        // pure waste. Any disjunction, function call, or predicate on a parquet data
        // column means the predicate space is not fully covered and we must keep the
        // row-level filter.
        return filterExpr != null && allConjunctsArePartitionPredicates(filterExpr);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        // The page-frame-backed cursor wraps a PageFrameRecordCursorImpl which supports
        // random access. The sequential fallback walks files in order and does not.
        return canPageFrame;
    }

    @Override
    public void setPushdownFilterCondition(ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions) {
        this.pushdownFilterConditions = pushdownFilterConditions;
    }

    /**
     * Flips the planner-requested iteration direction. Only honoured when the
     * cursor backend can walk rows in reverse - see {@link #canScanInReverse()}.
     * Must be called BEFORE the first {@code getCursor()} call; the legacy
     * cursor's reverse flag is synced once at construction time, and flipping
     * afterwards would leave the file walk and the wrapped single-file cursor
     * out of sync with their already-seeded indices.
     */
    public void setReverseScan(boolean reverseScan) {
        this.reverseScan = reverseScan;
    }

    @Override
    public void setQueryProjectedMetadata(RecordMetadata metadata) {
        super.setQueryProjectedMetadata(metadata);
        // Compute the projection -> (parquet writer index, partition column index)
        // mapping for the cursor. Each pruned column is either a real parquet column
        // (look it up by name in parquetMetadata, store its position as the writer
        // index that PageFrameMemoryPool will use to find it in the file's column id
        // map) or a hive partition virtual column (store the original partition
        // index so the virtual page overlay can address the right per-file buffer).
        // Sentinels: -1 in the array we don't apply to a given column.
        final int n = metadata.getColumnCount();
        final int[] parquetWriterIdx = new int[n];
        final int[] partitionIdx = new int[n];
        for (int i = 0; i < n; i++) {
            CharSequence name = metadata.getColumnName(i);
            int pIdx = indexOfPartitionColumn(name);
            if (pIdx >= 0) {
                parquetWriterIdx[i] = -1;
                partitionIdx[i] = pIdx;
            } else {
                int pqIdx = parquetMetadata.getColumnIndexQuiet(name);
                // SqlCodeGenerator only projects columns that exist in the factory's
                // declared metadata, so a projected column we don't recognise here
                // is a contract violation worth surfacing rather than silently
                // dropping.
                assert pqIdx >= 0 : "projected column not in parquet or partition schema: " + name;
                parquetWriterIdx[i] = pqIdx;
                partitionIdx[i] = -1;
            }
        }
        this.projectedToParquetWriterIdx = parquetWriterIdx;
        this.projectedToPartitionIdx = partitionIdx;
    }

    /**
     * Returns the total row count across every matched parquet file. Walks file
     * metadata footers on first call and caches the sum for the factory's
     * lifetime - subsequent calls are O(1). Both the cursor's size() and the
     * cursor's calculateSize() route through here so a single eager walk amortises
     * across every getCursor() reuse.
     */
    public long getCachedTotalRowCount(FilesFacade ff) {
        long count = cachedTotalRowCount;
        if (count >= 0) {
            return count;
        }
        synchronized (this) {
            count = cachedTotalRowCount;
            if (count >= 0) {
                return count;
            }
            count = computeTotalRowCount(ff);
            cachedTotalRowCount = count;
            return count;
        }
    }

    private long computeTotalRowCount(FilesFacade ff) {
        // Populates cachedPerFileRowCounts in lockstep with summing. Both are read
        // together under the synchronized lock, so callers see either the populated
        // pair or the -1 sentinel on cachedTotalRowCount.
        cachedPerFileRowCounts.clear();
        long total = 0;
        try (Path tempPath = new Path(MemoryTag.NATIVE_PATH);
             ParquetFileDecoder probe = new ParquetFileDecoder()) {
            for (int i = 0, n = matchedFiles.size(); i < n; i++) {
                DirectUtf8Sequence filePath = matchedFiles.getQuick(i);
                tempPath.of(filePath);
                final long fd = TableUtils.openRO(ff, tempPath.$(), LOG);
                long addr = 0;
                long size = 0;
                try {
                    size = ff.length(fd);
                    addr = TableUtils.mapRO(ff, fd, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                    probe.of(addr, size, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                    long rc = probe.metadata().getRowCount();
                    cachedPerFileRowCounts.add(rc);
                    total += rc;
                } finally {
                    ff.close(fd);
                    if (addr != 0) {
                        ff.munmap(addr, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                    }
                }
            }
        }
        return total;
    }

    /**
     * Returns per-file footer row counts only if the cache has already been
     * populated by a prior {@link #getCachedTotalRowCount} call; returns
     * {@code null} otherwise without forcing the bulk walk. The cursor's
     * prune-aware count path uses this to avoid opening every matched file
     * when a partition filter would skip most of them - falling back to
     * opening only survivors via {@link #openCachedFile}.
     */
    /**
     * Borrowed reference to the planning-time snapshot of matched files. Used
     * by the footer MIN/MAX shortcut to walk every survivor file and read
     * per-row-group statistics without going through the iteration cursor.
     * <p>
     * Callers MUST treat the returned reference as valid only for the call
     * duration - this factory still owns the {@link DirectUtf8StringList}
     * and frees it in {@code _close()}.
     */
    public @NotNull DirectUtf8StringList getMatchedFiles() {
        return matchedFiles;
    }

    public synchronized io.questdb.std.LongList getCachedPerFileRowCountsIfPopulated() {
        if (cachedTotalRowCount < 0) {
            return null;
        }
        return cachedPerFileRowCounts;
    }

    /**
     * Returns per-file footer row counts in the same order as the matchedFiles list.
     * Triggers the full walk on first call (via {@link #getCachedTotalRowCount}); cheap
     * thereafter. The cursor uses this to compute prune-aware row totals without
     * having to re-open files - it just walks matchedFiles applying the partition
     * predicate and sums the indices that survive.
     */
    public io.questdb.std.LongList getCachedPerFileRowCounts(FilesFacade ff) {
        // Ensure the per-file list is populated by forcing the total computation.
        // Both fields are written together under the synchronized lock.
        getCachedTotalRowCount(ff);
        return cachedPerFileRowCounts;
    }

    // True iff the partition column whose runtime value is parsed from the directory
    // path can be exactly compared at planning time against constants of this type.
    // Must match HivePartitionedReadParquetPageFrameCursor.canCompareTyped - the cursor
    // skips conditions on non-matching types during prune evaluation, so we must too,
    // or the planner would drop the row-level filter even though the cursor would not
    // exactly prune.
    private static boolean isExactlyComparablePartitionType(int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.INT:
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                return true;
            default:
                return false;
        }
    }

    // True iff `node` is a single comparison / null-check that the hive cursor will
    // exactly prune at the file level. Mirrors PushdownFilterExtractor's recognised
    // operator vocabulary and the cursor's per-op evaluation in isPrunedByParsedPartition.
    private boolean isExactPartitionPredicate(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        final String op = node.token == null ? null : node.token.toString();
        if (op == null) {
            return false;
        }
        // BETWEEN: args[2] is column, args[0]/args[1] are bounds.
        if (SqlKeywords.isBetweenKeyword(op)) {
            if (node.args == null || node.args.size() < 3) {
                return false;
            }
            ExpressionNode col = node.args.getLast();
            return isExactPartitionColumnRef(col);
        }
        // IS NULL / IS NOT NULL: ExpressionNode token is "=" or "!=" with a NULL leaf
        // for the legacy form, but the modern path lowers them to dedicated tokens.
        // We accept neither here because the cursor's null handling treats unknown
        // operators as "do not prune"; safer to keep the row-level filter for them.
        // Simple binary comparisons. Intentionally exclude `!=`: the extractor only
        // pushes `!= NULL` (folded into OP_IS_NOT_NULL) and `!= literal` is not in
        // the cursor's prune vocabulary, so the cursor wouldn't actually consume it.
        // Conservative: keep the row-level filter when we can't prove the cursor
        // will fully evaluate the predicate at file level.
        if (op.equals("=") || op.equals("<") || op.equals("<=") || op.equals(">") || op.equals(">=")) {
            if (node.lhs == null || node.rhs == null) {
                return false;
            }
            // Column on one side, literal on the other - mirror PushdownFilterExtractor.tryExtractComparison.
            ExpressionNode colNode;
            if (node.lhs.type == ExpressionNode.LITERAL && node.rhs.type != ExpressionNode.LITERAL) {
                colNode = node.lhs;
            } else if (node.rhs.type == ExpressionNode.LITERAL && node.lhs.type != ExpressionNode.LITERAL) {
                colNode = node.rhs;
            } else {
                return false;
            }
            return isExactPartitionColumnRef(colNode);
        }
        return false;
    }

    private boolean isExactPartitionColumnRef(ExpressionNode colNode) {
        if (colNode == null || colNode.type != ExpressionNode.LITERAL || colNode.token == null) {
            return false;
        }
        int partIdx = indexOfPartitionColumn(colNode.token);
        if (partIdx < 0) {
            return false;
        }
        int type = partitionColumnTypes.getQuick(partIdx);
        return isExactlyComparablePartitionType(type);
    }

    // Recursively check that every conjunct in `node` is a comparison or null-check on
    // a hive partition column that the cursor can exactly prune. Mirrors the operators
    // and type rules in HivePartitionedReadParquetPageFrameCursor.isPrunedByParsedPartition
    // - changes to one must be reflected in the other or row-level filtering and
    // file-level pruning will disagree about what's left, producing wrong results.
    private boolean allConjunctsArePartitionPredicates(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (SqlKeywords.isAndKeyword(node.token)) {
            return allConjunctsArePartitionPredicates(node.lhs) && allConjunctsArePartitionPredicates(node.rhs);
        }
        return isExactPartitionPredicate(node);
    }

    private int indexOfPartitionColumn(CharSequence name) {
        for (int i = 0, n = partitionColumnNames.size(); i < n; i++) {
            if (Chars.equalsIgnoreCase(partitionColumnNames.getQuick(i), name)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return canPageFrame;
    }

    @Override
    public void toPlan(PlanSink sink) {
        final boolean reverseEffective = reverseScan && canScanInReverse();
        sink.type(reverseEffective ? "Parquet glob scan (reverse)" : "Parquet glob scan").attr("glob").val(globPattern);
        // Surface the URI scheme so an operator running EXPLAIN can tell at a
        // glance whether this read is local or remote. Useful in incident
        // response - "is this query slow because we're hitting S3?" answered
        // by the plan without grepping logs.
        sink.attr("scheme").val(schemeOf(globPattern));
        // matchedFiles is the planning-time snapshot - users running EXPLAIN want to
        // see how wide the glob actually opened up, especially for diagnosing whether
        // a glob accidentally matched the whole tree.
        sink.attr("files").val(matchedFiles.size());
    }

    /**
     * Extract the URI scheme prefix ({@code s3}, {@code gcs}, {@code azblob},
     * {@code fs}) or "local" for paths with no {@code ://} delimiter. The
     * pattern is the original user-typed glob, so scheme is whatever the
     * user wrote regardless of whether a remote provider later resolved it
     * to a local cache path.
     */
    private static String schemeOf(CharSequence pattern) {
        if (pattern == null) {
            return "local";
        }
        int n = pattern.length();
        for (int i = 0; i < n - 2; i++) {
            if (pattern.charAt(i) == ':' && pattern.charAt(i + 1) == '/' && pattern.charAt(i + 2) == '/') {
                return pattern.subSequence(0, i).toString();
            }
        }
        return "local";
    }

    @Override
    protected void _close() {
        ExecutorService toShutdown;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            toShutdown = prefetchExecutor;
            prefetchExecutor = null;
            Misc.free(matchedFiles);
            Misc.free(pageFrameRecordCursor);
            Misc.free(pageFrameCursor);
            Misc.freeObjListAndClear(pushdownFilterConditions);
        }
        if (toShutdown != null) {
            // Shut the executor down OUTSIDE the monitor so any prefetch task
            // mid-acquire on the shared cache can finish without contending on
            // our own lock. The cache is engine-scoped and lives beyond this
            // factory; nothing here needs to drain it.
            toShutdown.shutdownNow();
            try {
                toShutdown.awaitTermination(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public ParquetFileCache getParquetFileCache() {
        return parquetCache;
    }

    /**
     * Schedule asynchronous opens for every matched file up front. Best-effort
     * priming of the engine-shared {@link ParquetFileCache}: the prefetch task
     * does an acquire-release pair, so the entry lands in the cache LRU but
     * is immediately eligible for eviction under pressure. Under sufficient
     * cache budget the cursor's later acquire is a hit; under tight budget,
     * prefetched entries get displaced before consumption and prefetch is a
     * no-op - the right behaviour because the dataset does not fit anyway.
     * <p>
     * No-op when the file count would individually overrun the cache entry
     * cap; prefetching past it would only thrash. Safe to call concurrently
     * with cursor iteration.
     */
    public void schedulePrefetchAll(FilesFacade ff) {
        if (closed) {
            return;
        }
        final int n = matchedFiles.size();
        if (n == 0) {
            return;
        }
        for (int i = 0; i < n; i++) {
            schedulePrefetch(matchedFiles.getQuick(i), ff);
        }
    }

    public void schedulePrefetch(Utf8Sequence path, FilesFacade ff) {
        if (closed) {
            return;
        }
        ExecutorService executor = prefetchExecutor;
        if (executor == null) {
            synchronized (this) {
                if (closed) {
                    return;
                }
                executor = prefetchExecutor;
                if (executor == null) {
                    executor = Executors.newSingleThreadExecutor(r -> {
                        Thread t = new Thread(r, "hive-parquet-prefetch");
                        t.setDaemon(true);
                        return t;
                    });
                    prefetchExecutor = executor;
                }
            }
        }
        // Copy the path bytes - the original Utf8Sequence is a flyweight backed
        // by the DirectUtf8StringList that may advance under iteration.
        final Utf8String key = Utf8String.newInstance(path);
        try {
            executor.submit(() -> {
                ParquetFileCache.Entry entry = null;
                try {
                    entry = parquetCache.acquire(key, ff);
                } catch (Throwable ignored) {
                    // Best-effort prefetch; cursor's sync acquire surfaces real errors.
                } finally {
                    if (entry != null) {
                        parquetCache.release(entry);
                    }
                }
            });
        } catch (RejectedExecutionException ignored) {
            // Executor shutting down. Cursor's sync acquire path will handle.
        }
    }

    private RecordCursor getLegacyCursor(SqlExecutionContext executionContext) throws SqlException {
        ReadParquetRecordCursor parquetCursor = null;
        try {
            parquetCursor = new ReadParquetRecordCursor(
                    configuration.getFilesFacade(),
                    parquetCache,
                    parquetMetadata,
                    pushdownFilterConditions
            );
            HivePartitionedReadParquetRecordCursor cursor = new HivePartitionedReadParquetRecordCursor(
                    matchedFiles,
                    parquetCursor,
                    nonGlobRootByteLen,
                    parquetColumnCount,
                    partitionColumnNames,
                    partitionColumnTypes
            );
            // Sync the iteration direction with the factory's reverseScan flag
            // BEFORE of(): the cursor's of() seeds globIndex and propagates the
            // flag onto the wrapped single-file cursor, so a stale value would
            // leave the first hasNext misaligned with the file-walk direction.
            // canScanInReverse() rules out the parallel-page-frame path; we
            // only land here when the serial backend is active.
            cursor.setReverse(reverseScan);
            cursor.of(executionContext);
            return cursor;
        } catch (Throwable th) {
            Misc.free(parquetCursor);
            throw th;
        }
    }

    private RecordCursor getPageFrameBackedCursor(SqlExecutionContext executionContext) throws SqlException {
        if (pageFrameCursor == null) {
            pageFrameCursor = new HivePartitionedReadParquetPageFrameCursor(
                    configuration.getFilesFacade(),
                    matchedFiles,
                    parquetMetadata,
                    parquetColumnCount,
                    partitionColumnNames,
                    partitionColumnTypes,
                    nonGlobRootByteLen,
                    configuration.getParquetCacheMaxEntries(),
                    projectedToParquetWriterIdx,
                    projectedToPartitionIdx,
                    pushdownFilterConditions,
                    this
            );
        }
        if (pageFrameRecordCursor == null) {
            pageFrameRecordCursor = new PageFrameRecordCursorImpl(
                    configuration,
                    getMetadata(),
                    new PageFrameRowCursorFactory(ORDER_ASC),
                    true,
                    null
            );
        }
        pageFrameCursor.of(executionContext);
        try {
            pageFrameRecordCursor.of(pageFrameCursor, executionContext);
            return pageFrameRecordCursor;
        } catch (Throwable th) {
            pageFrameRecordCursor.close();
            throw th;
        }
    }
}
