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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
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
import io.questdb.std.QuietCloseable;
import io.questdb.std.Utf8SequenceObjHashMap;
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
    // Tracks sum of mapSize across all CachedFile entries currently in fileCache.
    // Maintained under the synchronized lock alongside fileCacheLruOrder so eviction
    // can stop adding new entries once the byte budget is exceeded, independent of
    // the entry-count cap.
    private long currentCacheBytes;
    // Per-file cache of mmap + parsed parquet footer + ParquetFileDecoder, owned by
    // the factory and bounded by maxConcurrentOpenFiles (LRU). Lets cursor reuse
    // skip the open+mmap+parse-footer cost when the same factory runs more than
    // one query against overlapping files - common for prepared statements and any
    // JMH-style replay. Cache eviction releases the fd back through FdCache and
    // the mmap back through MmapCache, so peak OS resource usage stays bounded.
    private final Utf8SequenceObjHashMap<CachedFile> fileCache = new Utf8SequenceObjHashMap<>();
    private final ObjList<CachedFile> fileCacheLruOrder = new ObjList<>();
    // Scratch Path reused inside the synchronized openCachedFile body so we do not
    // alloc-and-free a Path's native buffer per cursor file-open. Lazy-init on first
    // miss; nulled in _close.
    private Path scratchPath;
    // Set true by _close to refuse opens that win the synchronized lock after the
    // factory has been told to shut down. Without this, a prefetch task that was
    // queued before _close ran (or had just released the lock to retry) could
    // acquire the lock once _close released it, insert a fresh CachedFile into the
    // already-cleared map, and leak the fd + mmap. Volatile is enough: the read
    // happens under the synchronized lock, which provides the visibility barrier.
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
                    configuration.getSqlParquetHiveMaxOpenFiles(),
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

    /**
     * Cached open parquet file: fd, mmap, and parsed footer/decoder. Owned by
     * the factory's fileCache; the cursor borrows the {@link #decoder} reference
     * but must not free the entry. Close releases the fd via FdCache and the
     * mmap via MmapCache, both refcounted - if another cursor still holds the
     * underlying handles via a concurrent path open, the OS resources stay live.
     */
    public static final class CachedFile implements QuietCloseable {
        public final Utf8String cachedPath;
        public final ParquetFileDecoder decoder;
        public final long fd;
        public final FilesFacade ff;
        public final long mapAddr;
        public final long mapSize;

        CachedFile(FilesFacade ff, Utf8String path, long fd, long mapAddr, long mapSize, ParquetFileDecoder decoder) {
            this.ff = ff;
            this.cachedPath = path;
            this.fd = fd;
            this.mapAddr = mapAddr;
            this.mapSize = mapSize;
            this.decoder = decoder;
        }

        @Override
        public void close() {
            Misc.free(decoder);
            if (mapAddr != 0) {
                ff.munmap(mapAddr, mapSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            if (fd != -1) {
                ff.close(fd);
            }
        }
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
        // Two-phase shutdown. Phase 1: take the monitor briefly to (a) flip
        // the `closed` flag so subsequent schedulePrefetch / openCachedFile
        // calls short-circuit, (b) snapshot the executor reference, and (c)
        // free all owned state. Phase 2: shut the executor down OUTSIDE the
        // monitor so any prefetch task already parked on openCachedFile's
        // synchronized acquire can grab the lock, observe closed=true, and
        // exit fast via the CairoException path. Doing the shutdown inside
        // the synchronized block self-deadlocked: awaitTermination always
        // timed out at its 2-second cap because parked tasks could never
        // acquire the lock to make progress.
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
            for (int i = 0, n = fileCacheLruOrder.size(); i < n; i++) {
                Misc.free(fileCacheLruOrder.getQuick(i));
            }
            fileCacheLruOrder.clear();
            fileCache.clear();
            currentCacheBytes = 0;
            scratchPath = Misc.free(scratchPath);
        }
        if (toShutdown != null) {
            toShutdown.shutdownNow();
            // Best-effort wait. Most parked tasks bail in under a millisecond
            // once they acquire the monitor and see closed=true; anything
            // mid-mmap finishes in single-digit milliseconds. 100 ms is a
            // safety cap, not the expected cost. The daemon thread will exit
            // the JVM cleanly even if this times out.
            try {
                toShutdown.awaitTermination(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Schedule an asynchronous open of {@code path} so its decoder is hot in the
     * cache before the cursor reaches it. Returns immediately. Best-effort: if the
     * open fails (file deleted, schema drift) the prefetch is silently dropped and
     * the cursor's later synchronous open will surface the error.
     * <p>
     * The prefetch worker calls {@link #openCachedFile} which is synchronized -
     * concurrent prefetch and cursor opens for the same path serialise via that
     * lock, so the cursor's open observes a populated cache entry rather than
     * duplicating the work. Sharing also lands automatically at the OS layer
     * because both code paths go through TableUtils.openRO/mapRO which dedupe via
     * FdCache and MmapCache.
     */
    /**
     * Schedule asynchronous opens for every matched file up front. Intended for the
     * very first {@code getCursor()} call - it lets file open + mmap + footer parse
     * run in parallel with the cursor's first decoded frame, instead of being paid
     * serially on demand. Subsequent cursor calls find a populated cache and skip
     * the open cost entirely. No-op when the file count exceeds the cache cap
     * (would only thrash the LRU). Safe to call concurrently with cursor iteration.
     */
    public void schedulePrefetchAll(FilesFacade ff) {
        if (closed) {
            return;
        }
        final int n = matchedFiles.size();
        if (n == 0 || n > configuration.getSqlParquetHiveMaxOpenFiles()) {
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
                // Re-check closed under the lock - _close also sets `closed` while
                // holding the monitor, so this guard rules out the post-close racy
                // creation of a fresh executor whose daemon thread would then linger
                // for the JVM's lifetime with no work to do.
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
        // Copy the path bytes - the original Utf8Sequence is a flyweight backed by
        // the DirectUtf8StringList that may be advancing under iteration.
        final Utf8String key = Utf8String.newInstance(path);
        try {
            executor.submit(() -> {
                try {
                    openCachedFile(key, ff);
                } catch (Throwable ignored) {
                    // Best-effort prefetch; the cursor will retry synchronously on demand
                    // and surface any real error there.
                }
            });
        } catch (RejectedExecutionException ignored) {
            // Executor shutting down. Cursor's sync open path will handle.
        }
    }

    /**
     * Returns a cached open-and-parsed handle to {@code path}, opening it on demand
     * if missing. The cursor must NOT close the returned {@link CachedFile} - the
     * factory owns its lifetime. Used by the cursor's openNextFile in place of the
     * direct openRO/mapRO + ParquetFileDecoder.of sequence, so repeated queries
     * against the same factory skip the file-open + metadata-parse cost.
     */
    public synchronized CachedFile openCachedFile(Utf8Sequence path, FilesFacade ff) {
        if (closed) {
            // _close has already drained the cache. Either we're a prefetch task that
            // was queued before shutdown, or the caller has a bug holding the factory
            // open past close. Refusing here keeps the post-close map empty and the
            // CairoException surfaces the bug if the cursor is at fault.
            throw CairoException.nonCritical().put("hive parquet factory is closed");
        }
        final int idx = fileCache.keyIndex(path);
        if (idx < 0) {
            CachedFile hit = fileCache.valueAt(idx);
            // Touch LRU: move to tail. Linear scan - the list is bounded by
            // maxConcurrentOpenFiles so it stays small (4096 by default), and this
            // path runs from the cursor's single-threaded iteration only.
            final int n = fileCacheLruOrder.size();
            for (int i = 0; i < n; i++) {
                if (fileCacheLruOrder.getQuick(i) == hit) {
                    if (i != n - 1) {
                        fileCacheLruOrder.remove(i);
                        fileCacheLruOrder.add(hit);
                    }
                    break;
                }
            }
            return hit;
        }
        // Miss - open. Evict LRU until BOTH (a) the entry count is below the cap and
        // (b) the byte budget would still cover one more entry. The byte check is
        // best-effort: at the moment of decision we don't know the new entry's size,
        // so we leave one slot of headroom and re-check after insertion. Eviction
        // removes from BOTH the map and the order list, then frees the entry (which
        // releases fd and mmap back to FdCache / MmapCache and decrements the byte
        // counter).
        final int maxEntries = configuration.getSqlParquetHiveMaxOpenFiles();
        final long maxBytes = configuration.getSqlParquetHiveMaxCacheBytes();
        while (fileCacheLruOrder.size() >= maxEntries
                || (currentCacheBytes > maxBytes && fileCacheLruOrder.size() > 0)) {
            CachedFile victim = fileCacheLruOrder.getQuick(0);
            fileCacheLruOrder.remove(0);
            fileCache.remove(victim.cachedPath);
            currentCacheBytes -= victim.mapSize;
            Misc.free(victim);
        }
        final Utf8String key = Utf8String.newInstance(path);
        final CachedFile entry;
        if (scratchPath == null) {
            scratchPath = new Path(MemoryTag.NATIVE_PATH);
        }
        scratchPath.of(path);
        final long fd = TableUtils.openRO(ff, scratchPath.$(), LOG);
        long addr = 0;
        long size = 0;
        ParquetFileDecoder decoder = null;
        try {
            size = ff.length(fd);
            addr = TableUtils.mapRO(ff, fd, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            decoder = new ParquetFileDecoder();
            decoder.of(addr, size, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
        } catch (Throwable th) {
            if (addr != 0) {
                ff.munmap(addr, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            ff.close(fd);
            Misc.free(decoder);
            throw th;
        }
        entry = new CachedFile(ff, key, fd, addr, size, decoder);
        fileCache.putAt(idx, key, entry);
        fileCacheLruOrder.add(entry);
        currentCacheBytes += size;
        return entry;
    }

    private RecordCursor getLegacyCursor(SqlExecutionContext executionContext) throws SqlException {
        ReadParquetRecordCursor parquetCursor = null;
        try {
            parquetCursor = new ReadParquetRecordCursor(
                    configuration.getFilesFacade(),
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
                    configuration.getSqlParquetHiveMaxOpenFiles(),
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
