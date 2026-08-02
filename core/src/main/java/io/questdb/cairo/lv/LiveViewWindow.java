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

package io.questdb.cairo.lv;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.BitSet;
import io.questdb.std.BoolList;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.MicrosecondClock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Per-row driver that wires a named WINDOW's ANCHOR clause to live-view window
 * functions' {@link WindowFunction#resetPartition(Record)} contract.
 * <p>
 * Built once per refresh cycle when the live view's compiled SELECT contains an
 * anchored named WINDOW. Per-row flow:
 * <ol>
 *     <li>Build the row's partition-by key directly from the source record via
 *     {@link #partitionKeySink}.</li>
 *     <li>Evaluate {@link #anchorExpression} against the row.</li>
 *     <li>Compare to the per-partition last-seen anchor value held in
 *     {@link #anchorMap}.</li>
 *     <li>If the anchor changed (or the partition is brand new), dispatch
 *     {@link WindowFunction#resetPartition(Record)} to every function on this
 *     WINDOW, then update the partition's recorded anchor value.</li>
 * </ol>
 * <p>
 * Limitations:
 * <ul>
 *     <li>Anchor expressions must return a {@code TIMESTAMP}, {@code LONG}, or
 *     {@code INT} (the most common calendar-period anchor case). Other primitive
 *     and composite return types are rejected at CREATE.</li>
 *     <li>One {@code LiveViewWindow} per LV — multi-window LVs with different
 *     anchors are rejected at CREATE (deferred validation).</li>
 *     <li>{@link #functions} is the full set of window functions in the SELECT.
 *     Multi-window queries where only a subset belongs to the anchored WINDOW
 *     are out of scope until the per-WINDOW dispatch landing.</li>
 * </ul>
 */
public class LiveViewWindow implements QuietCloseable {
    // The dirty anchor map's two slots. Nothing reads an anchor value or a live
    // tombstone off that map - freezeCheckpointEntries goes to the live anchor map for
    // both - so carrying the three slots below would be padding on every key the
    // cadence touches.
    //
    // DIRTY_SLOT_EVICTED: 1 means the frontier sweep dropped this key from the anchor
    // map, so the seal freezes a removal for it instead of raising on the missing live
    // value. Per-key rather than per-sweep on purpose: a dirty key that lost its anchor
    // entry to anything but the sweep carries a 0 here and still raises.
    // DIRTY_SLOT_NEW_SINCE_CHECKPOINT: 1 means the key is absent from the durable
    // predecessor, which is what keeps the logical-size accounting exact without a probe
    // into the anchor root.
    private static final int DIRTY_SLOT_EVICTED = 1;
    private static final int DIRTY_SLOT_NEW_SINCE_CHECKPOINT = 0;
    // Slot 0: last-seen anchor value (LONG / TIMESTAMP).
    // Slot 1: byte flag — 0 means "uninitialized", 1 means "set". The MapValue's
    // intrinsic isNew() flips to false on first access; we use this explicit flag
    // so the live-view processRow can distinguish "first row of a partition" from
    // "anchor changed between rows."
    // Slot 2: byte tombstone — 0 means "alive" (partition saw a row recently), 1
    // means "stale" (anchor crossed and no follow-up row visited the partition
    // since). The anchor-map compaction trigger reclaims
    // tombstoned entries.
    private static final int SLOT_ANCHOR_VALUE = 0;
    private static final int SLOT_INITIALIZED = 1;
    private static final int SLOT_TOMBSTONE = 2;

    private final Function anchorExpression;
    // Reads the partition-by key columns straight off the anchor map's own MapRecord.
    // compact() hands this to every function so one whose partition map picked a
    // different Map implementation can still mirror the survivors into a probe of its
    // own implementation -- the sink writes through per-column putters and never casts.
    private final RecordSink anchorKeySink;
    private final int anchorValueType;
    private final CairoConfiguration cairoConfiguration;
    // The fixed segment boundary the compiler derived from the anchor expression, or
    // null when the anchor has none. Carried on the window because the segment is a
    // property of the anchor, not of any one function on it.
    private final @Nullable LiveViewCheckpointAnchorPlan checkpointAnchorPlan;
    // Per-function answer to "did this function accept every key the running sweep
    // dropped?", indexed by position in functions. Allocated with the window and
    // rewritten per sweep, so the sweep keeps its no-allocation property.
    private final BoolList checkpointRemovalsRecorded = new BoolList();
    // Anchor-map size above which a frontier sweep is attempted (mirrors
    // cairo.live.view.partition.compact.threshold). The sweep itself is gated on
    // the anchor having advanced since the last sweep, so it fires at most once
    // per bucket boundary rather than per row.
    private final int compactThreshold;
    private final ObjList<WindowFunction> functions;
    // True only when the anchor expression is provably monotone with the base
    // scan order (it derives solely from the base's designated timestamp, which
    // the incremental-refresh cursor emits in ascending order). Computed once at
    // build() time and used to gate frontier compaction preventively: an anchor
    // that reads any other column (for example a non-designated TIMESTAMP) can
    // dip back into an already-evicted bucket, so it must keep every partition.
    private final boolean isAnchorMonotone;
    // Per-view tracker charged for the anchor map and its compaction scratch. Owned by
    // LiveViewInstance, which closes it only after this window has freed both maps.
    private final @Nullable MemoryTracker memoryTracker;
    // Static reference to the anchor map's key-column types. Held so compact()
    // can allocate a replacement Map with the same shape without re-deriving
    // it from build()-time inputs.
    private final ColumnTypes partitionKeyTypes;
    private final RecordSink partitionKeySink;
    private final String windowName;
    private Map anchorMap;
    // Generation of the checkpoint root checkpointLogicalStateBytes and
    // checkpointDirtyAnchorMap are relative to. LONG_NULL until the first seal
    // publishes; a repair, truncate or compaction moves the timeline's generation
    // past it without this window having produced the new root, and the mismatch is
    // what keeps the next seal off the incremental path.
    private long checkpointBaselineGeneration = Numbers.LONG_NULL;
    // Deduplicated anchor keys touched since the last durable checkpoint, with the
    // per-key marker that says whether the key is new relative to that checkpoint.
    // One entry per distinct key, so the footprint scales with the checkpoint
    // cadence rather than with the batch: raising
    // cairo.live.view.checkpoint.max.duration.micros trades seal cost for both
    // latency and memory, charged to cairo.live.view.refresh.memory.limit.bytes. A
    // view whose max timestamp stops advancing never publishes and grows the map
    // until the tracker trips.
    private Map checkpointDirtyAnchorMap;
    private long checkpointLogicalStateBytes;
    private boolean isCheckpointFullScanRequired = true;
    // Frontier-gated compaction state. All mutated only on the refresh-worker
    // thread (processRow / compact / restore / toTop); not volatile.
    //
    // compactionViable starts true only when the anchor is provably monotone with
    // the base scan order (isAnchorMonotone, decided at build() time) AND has a
    // TIMESTAMP return type. It latches to false the moment an in-WAL-order row
    // produces an anchor value below the running maximum (or a NULL). A
    // behind-frontier partition is safe to drop only when the anchor advances
    // monotonically with the WAL stream: the partition's next in-order row then
    // necessarily lands in a new bucket and resets anyway, and any late
    // (out-of-order) row routes through O3 replay, which rebuilds state.
    // Event-style anchors (a flag toggling back to an earlier value) break that
    // guarantee, so they keep all partitions. The build()-time gate is the primary
    // guard; the runtime latch is a backstop for a monotone-looking anchor that
    // nonetheless produces a decrease at runtime.
    private boolean compactionViable;
    // Lifetime sweep instrumentation. Read by the live view benchmarks and by tests
    // to tell a sweep that reclaimed a large generation apart from one that found
    // little, and to price the sweep against the seal that follows it.
    private long compactedPartitionCount;
    private long compactionCount;
    private long compactionMicros;
    private long lastCompactionMapSize;
    private long currentBucketPartitionCount;
    private long previousBucketPartitionCount;
    private long stalePartitionCount;
    private boolean frontierInitialized;
    // True once a sweep has put evicted keys into the dirty anchor map and the seal has
    // not consumed them yet. What it decides is whether dropping the dirty set also hands
    // the backing memory back - see clearCheckpointDirtyAnchorMap.
    private boolean hasCheckpointEvictionsRecorded;
    private long lastCompactedFrontier = Long.MIN_VALUE;
    // Highest anchor value seen (the current bucket); prevFrontier is the bucket
    // before it. A sweep keeps partitions whose last anchor value is >= prevFrontier
    // (the current and previous buckets) and drops older ones.
    private long maxAnchorValue;
    private long prevFrontier = Long.MIN_VALUE;
    // Reusable second anchor map for the frontier sweep; ping-pongs with anchorMap
    // so a sweep never allocates. Allocated once on the first sweep.
    private Map scratchAnchorMap;
    // Anchor-map entry count flagged SLOT_TOMBSTONE = 1. Reset-driven tombstoning
    // is disabled (see processRow), so this stays 0; retained for the snapshot
    // live-count accounting and the catalogue.
    private long tombstoneCount;

    public LiveViewWindow(
            @NotNull CairoConfiguration cairoConfiguration,
            @NotNull String windowName,
            @NotNull Function anchorExpression,
            int anchorValueType,
            @NotNull ColumnTypes partitionKeyTypes,
            @NotNull Map anchorMap,
            @NotNull RecordSink partitionKeySink,
            @NotNull RecordSink anchorKeySink,
            @NotNull ObjList<WindowFunction> functions,
            boolean isAnchorMonotone,
            @Nullable LiveViewCheckpointAnchorPlan checkpointAnchorPlan,
            @Nullable MemoryTracker memoryTracker
    ) {
        this.cairoConfiguration = cairoConfiguration;
        this.windowName = windowName;
        this.anchorExpression = anchorExpression;
        this.anchorValueType = anchorValueType;
        this.partitionKeyTypes = partitionKeyTypes;
        this.anchorMap = anchorMap;
        this.partitionKeySink = partitionKeySink;
        this.anchorKeySink = anchorKeySink;
        this.functions = functions;
        this.isAnchorMonotone = isAnchorMonotone;
        this.checkpointAnchorPlan = checkpointAnchorPlan;
        this.memoryTracker = memoryTracker;
        this.compactThreshold = cairoConfiguration.getLiveViewPartitionCompactThreshold();
        // Frontier compaction is sound only when the anchor advances monotonically
        // with the WAL stream. A TIMESTAMP anchor derived from the ascending
        // designated timestamp (DAILY sugar, calendar-period timestamp_floor) gives
        // that; LONG/INT anchors (session ids, event flags) cannot be assumed
        // monotone, so they opt out structurally. A non-monotone TIMESTAMP anchor
        // (e.g. a non-designated ts column) is caught preventively by
        // isAnchorMonotone here - the runtime latch in trackFrontier alone cannot,
        // because it fires only AFTER a decrease is seen and an eviction may already
        // have dropped a partition that a later dip row revisits (silent undercount).
        this.compactionViable = isAnchorMonotone && ColumnType.tagOf(anchorValueType) == ColumnType.TIMESTAMP;
    }

    /**
     * Returns the column types the anchor map's value layout uses, so factories
     * can construct compatible Maps.
     */
    public static ColumnTypes anchorMapValueTypes() {
        return AnchorMapValueTypes.INSTANCE;
    }

    /**
     * Adds two logical byte counts, raising rather than wrapping. An incremental freeze
     * both adds and subtracts, and a subtraction that underflows would publish a root
     * charging a nonsense figure that every later cadence then builds on.
     */
    private static long checkedAdd(long a, long b) {
        try {
            return Math.addExact(a, b);
        } catch (ArithmeticException e) {
            throw CairoException.critical(0).put("live view checkpoint anchor byte count overflow");
        }
    }

    /**
     * Copies the first {@code length} bytes of the encoded key {@code keyBuffer} holds
     * into a fresh array, which is the form both the put and the removal channels carry
     * to publication.
     */
    private static byte[] copyEncodedKey(MemoryCARW keyBuffer, int length) {
        final byte[] key = new byte[length];
        for (int i = 0; i < length; i++) {
            key[i] = keyBuffer.getByte(i);
        }
        return key;
    }

    /**
     * Builds the sink {@link #compact()} hands to each window function so it can mirror
     * the rebuilt anchor map's surviving keys into a probe map of its own {@link Map}
     * implementation.
     * <p>
     * The sink reads from the anchor map's own {@code MapRecord}. Map records lay value
     * columns out first and key columns after them, so the record's column types are
     * {@link AnchorMapValueTypes} followed by the partition-by key types, and the filter
     * selects only the key tail. {@code ListColumnFilter} indices are 1-based.
     * <p>
     * No {@code writeSymbolAsString} bitset: the anchor map already stores SYMBOL
     * partition columns as STRING (see the key-type loop in {@code build}), so reading
     * them back off the map record needs no further conversion.
     */
    private static RecordSink createAnchorKeySink(
            @NotNull CairoConfiguration configuration,
            @NotNull BytecodeAssembler asm,
            @NotNull ColumnTypes partitionKeyTypes
    ) {
        final int keyStartIndex = AnchorMapValueTypes.INSTANCE.getColumnCount();
        final int keyColumnCount = partitionKeyTypes.getColumnCount();
        final ArrayColumnTypes anchorRecordTypes = new ArrayColumnTypes();
        for (int i = 0; i < keyStartIndex; i++) {
            anchorRecordTypes.add(AnchorMapValueTypes.INSTANCE.getColumnType(i));
        }
        for (int i = 0; i < keyColumnCount; i++) {
            anchorRecordTypes.add(partitionKeyTypes.getColumnType(i));
        }
        final ListColumnFilter keyFilter = new ListColumnFilter();
        for (int i = 0; i < keyColumnCount; i++) {
            keyFilter.add(keyStartIndex + i + 1);
        }
        return RecordSinkFactory.getInstance(configuration, asm, anchorRecordTypes, keyFilter);
    }

    /**
     * Creates an anchor-shaped map charged to the per-view tracker. Built lazily
     * ({@code openOnInit=false}) so the tracker is bound before the first allocation: a
     * malloc that predates the bind would be charged to nobody while its free is credited
     * AGAINST the tracker, driving the balance negative.
     */
    private static Map createTrackedAnchorMap(
            @NotNull CairoConfiguration configuration,
            @NotNull ColumnTypes keyTypes,
            @Nullable MemoryTracker memoryTracker
    ) {
        Map map = MapFactory.createUnorderedMap(configuration, keyTypes, anchorMapValueTypes(), false, false);
        map.setMemoryTracker(memoryTracker);
        map.reopen();
        return map;
    }

    /**
     * Creates the checkpoint dirty-key map: anchor keys under the one-byte value
     * layout {@link DirtyAnchorMapValueTypes} rather than the anchor map's own.
     * Charged to the per-view tracker on the same lazy terms as
     * {@link #createTrackedAnchorMap}.
     */
    private static Map createTrackedDirtyAnchorMap(
            @NotNull CairoConfiguration configuration,
            @NotNull ColumnTypes keyTypes,
            @Nullable MemoryTracker memoryTracker
    ) {
        Map map = MapFactory.createUnorderedMap(
                configuration,
                keyTypes,
                DirtyAnchorMapValueTypes.INSTANCE,
                false,
                false
        );
        map.setMemoryTracker(memoryTracker);
        map.reopen();
        return map;
    }

    /**
     * Constructs a {@code LiveViewWindow} bound to {@code projectedMetadata} —
     * the record shape produced by the live view's source-side cursor (the leaf
     * page-frame factory in the compiled SELECT). The {@code partitionColumnNames}
     * come from the persisted {@link LiveViewDefinition.LvAnchorSpec}.
     * <p>
     * Throws {@link CairoException} when:
     * <ul>
     *     <li>{@code partitionColumnNames} is empty (an anchored WINDOW requires at
     *     least one partition column).</li>
     *     <li>any partition column is not present in {@code projectedMetadata}.</li>
     *     <li>the anchor expression's return type is not TIMESTAMP, LONG, or INT.</li>
     * </ul>
     * <p>
     * {@code isAnchorMonotone} is the caller's determination (see
     * {@code LiveViewRefreshJob.isAnchorMonotoneWithBaseOrder}) that the anchor
     * derives solely from the base's designated timestamp, and so advances
     * monotonically with the incremental-refresh scan order. It gates frontier
     * compaction: only a monotone anchor may evict behind-frontier partitions.
     * <p>
     * {@code checkpointAnchorPlan} is the compiler's determination (see
     * {@code LiveViewCheckpointFunctionCompiler.anchorPlan}) of where this anchor's
     * segments begin and end, or null when the anchor has no fixed boundary. It carries
     * no runtime behavior; a localized out-of-order repair reads it to bound its work.
     */
    public static LiveViewWindow build(
            @NotNull CairoConfiguration configuration,
            @NotNull BytecodeAssembler asm,
            @NotNull String windowName,
            @NotNull RecordMetadata projectedMetadata,
            @NotNull ObjList<String> partitionColumnNames,
            @NotNull Function anchorExpression,
            @NotNull ObjList<WindowFunction> functions,
            boolean isAnchorMonotone,
            @Nullable LiveViewCheckpointAnchorPlan checkpointAnchorPlan,
            @Nullable MemoryTracker memoryTracker
    ) {
        int n = partitionColumnNames.size();
        if (n == 0) {
            throw CairoException.nonCritical()
                    .put("anchored live-view window requires PARTITION BY columns");
        }
        // The RecordSink contract: columnFilter holds 1-based indexes into the
        // source record's metadata, and the ColumnTypes argument carries the
        // FULL source metadata's types (the sink looks up types by source index,
        // not by filter slot). The map's key types — separately — must match
        // the filtered subset.
        // SYMBOL partition columns route through writeSymbolAsString so the
        // map key holds the resolved string rather than the segment-local
        // symbol index. WAL segments assign different local indices to the
        // same string, so a raw-int key would collide across incremental
        // refresh cycles whose rows come from different WAL segments. The map
        // key type for those columns becomes STRING to match the sink's writes.
        ListColumnFilter columnFilter = new ListColumnFilter();
        ArrayColumnTypes mapKeyTypes = new ArrayColumnTypes();
        BitSet writeSymbolAsString = null;
        for (int i = 0; i < n; i++) {
            String name = partitionColumnNames.getQuick(i);
            int idx = projectedMetadata.getColumnIndexQuiet(name);
            if (idx < 0) {
                throw CairoException.nonCritical()
                        .put("partition column not found in projected metadata [column=").put(name).put(']');
            }
            columnFilter.add(idx + 1);
            int columnType = projectedMetadata.getColumnType(idx);
            if (ColumnType.isSymbol(columnType)) {
                if (writeSymbolAsString == null) {
                    writeSymbolAsString = new BitSet();
                }
                writeSymbolAsString.set(idx);
                mapKeyTypes.add(ColumnType.STRING);
            } else {
                mapKeyTypes.add(columnType);
            }
        }
        ArrayColumnTypes sourceColumnTypes = new ArrayColumnTypes();
        for (int i = 0, m = projectedMetadata.getColumnCount(); i < m; i++) {
            sourceColumnTypes.add(projectedMetadata.getColumnType(i));
        }
        RecordSink sink = RecordSinkFactory.getInstance(configuration, asm, sourceColumnTypes, columnFilter, writeSymbolAsString);
        // Built before the anchor map so a failure here cannot strand a tracked
        // allocation: the map has no owner until the constructor below takes it.
        RecordSink anchorKeySink = createAnchorKeySink(configuration, asm, mapKeyTypes);
        // createUnorderedMap (not createOrderedMap) so the anchor map keeps the fastest
        // implementation its key shape and 10-byte value allow. It need not agree with
        // any window function's choice -- MapFactory also selects on value size, so a
        // function with a wider live-view payload legitimately lands elsewhere -- because
        // compact() hands each function anchorKeySink and the rebuild bridges the two
        // implementations through it. See retainPartitions.
        Map map = createTrackedAnchorMap(configuration, mapKeyTypes, memoryTracker);
        int returnType = anchorExpression.getType();
        int tag = ColumnType.tagOf(returnType);
        if (tag != ColumnType.TIMESTAMP && tag != ColumnType.LONG && tag != ColumnType.INT) {
            Misc.free(map);
            // Same wording as the CREATE-time check in
            // CairoEngine.validateAnchorReturnType. CREATE validates this, but
            // restart re-compiles the persisted anchor SQL without re-running
            // CREATE validation, so this guards a return type that changed
            // across releases (e.g. a function whose result type was widened).
            throw CairoException.nonCritical()
                    .put("ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got ")
                    .put(ColumnType.nameOf(returnType));
        }
        return new LiveViewWindow(configuration, windowName, anchorExpression, returnType, mapKeyTypes, map, sink, anchorKeySink, functions, isAnchorMonotone, checkpointAnchorPlan, memoryTracker);
    }

    /**
     * Drops every anchor entry and the frontier that tracks them so a checkpoint
     * restore can rehydrate the map through {@link #restoreCheckpointEntry}. The
     * caller validates the complete root first, so a framing failure cannot
     * leave the window with a half-restored map.
     * <p>
     * The window is left on the full scan, which is what a restore that abandons
     * midway or reads a root other than the timeline head needs. A restore from the
     * head calls {@link #onCheckpointPersisted(long, long)} once the map is whole to
     * put the window back on the incremental path.
     */
    public void beginCheckpointRestore() {
        checkpointBaselineGeneration = Numbers.LONG_NULL;
        isCheckpointFullScanRequired = true;
        checkpointLogicalStateBytes = 0;
        clearCheckpointDirtyAnchorMap();
        anchorMap.clear();
        tombstoneCount = 0;
        resetFrontier();
    }

    /**
     * @param generation the checkpoint generation the seal is freezing on top of
     * @return true when the seal may freeze only the keys
     * {@link #freezeCheckpointEntries} would name from the dirty map. False forces a
     * complete freeze: either something removed anchor keys since the last
     * publication, or {@code generation} is not the one this window's baseline was
     * recorded against - a repair, truncate or compaction published in between, and
     * the root the seal would build on top of is not the one this window produced
     */
    public boolean canFreezeCheckpointIncrementally(long generation) {
        return !isCheckpointFullScanRequired
                && checkpointDirtyAnchorMap != null
                && checkpointBaselineGeneration == generation;
    }

    /**
     * Clears the frontier sweep's eviction marker on up to {@code limit} dirty anchor
     * keys that carry one, and returns how many it cleared. Such a key is still absent
     * from the anchor map but no longer says why, which is the shape a bookkeeping bug
     * elsewhere in the runtime would produce: the seal must refuse to read it as a
     * removal rather than publish a root missing an entry no sweep took out.
     * <p>
     * No production path reaches this - it exists so a test can hold that refusal to its
     * contract on one key while the sweep's other keys keep their markers, which is what
     * a sweep-wide "something was evicted" flag would get wrong.
     */
    @TestOnly
    public int clearCheckpointEvictionMarkers(int limit) {
        if (checkpointDirtyAnchorMap == null || limit <= 0) {
            return 0;
        }
        final MapRecordCursor cursor = checkpointDirtyAnchorMap.getCursor();
        final MapRecord record = checkpointDirtyAnchorMap.getRecord();
        int cleared = 0;
        while (cleared < limit && cursor.hasNext()) {
            final MapValue value = record.getValue();
            if (value.getByte(DIRTY_SLOT_EVICTED) == 1) {
                value.putByte(DIRTY_SLOT_EVICTED, (byte) 0);
                cleared++;
            }
        }
        return cleared;
    }

    @Override
    public void close() {
        // The Map and RecordSink are exclusively owned by this object. The anchor
        // Function and the window-functions list are owned upstream
        // (LiveViewInstance and WindowRecordCursorFactory respectively); freeing
        // them here would double-free.
        Misc.free(anchorMap);
        Misc.free(checkpointDirtyAnchorMap);
        Misc.free(scratchAnchorMap);
    }

    /**
     * Encodes every live anchor entry for a complete freeze, or only keys touched
     * since the durable predecessor for an incremental freeze. Tombstoned entries
     * are skipped. The keys and anchor values remain index-aligned.
     * <p>
     * An incremental freeze also names the keys the frontier sweep dropped, in
     * {@code removedKeysOut}: the root the freeze builds on top of still holds their
     * entries, and nothing else in an incremental build would take them out. A complete
     * freeze leaves the list empty - it removes by omission instead, since its puts are
     * the whole truth.
     * <p>
     * {@code keyBuffer} is caller-owned scratch the key codec writes through; it
     * is rewound per entry and holds nothing once this returns.
     */
    public long freezeCheckpointEntries(
            @NotNull MemoryCARW keyBuffer,
            @NotNull ObjList<byte[]> keysOut,
            @NotNull LongList valuesOut,
            @NotNull ObjList<byte[]> removedKeysOut,
            boolean incremental
    ) {
        keysOut.clear();
        valuesOut.clear();
        removedKeysOut.clear();
        long logicalBytes = incremental ? checkpointLogicalStateBytes : 0;
        final Map scanMap = incremental ? checkpointDirtyAnchorMap : anchorMap;
        // A map record lays its value columns out ahead of its key columns, and the
        // two maps carry different value layouts, so the key tail starts at a
        // different index in each.
        final int keyStartIndex = incremental
                ? DirtyAnchorMapValueTypes.INSTANCE.getColumnCount()
                : AnchorMapValueTypes.INSTANCE.getColumnCount();
        final MapRecordCursor cursor = scanMap.getCursor();
        final MapRecord record = scanMap.getRecord();
        while (cursor.hasNext()) {
            final MapValue dirtyOrAnchorValue = record.getValue();
            final boolean isNewSinceCheckpoint = incremental
                    && dirtyOrAnchorValue.getByte(DIRTY_SLOT_NEW_SINCE_CHECKPOINT) == 1;
            final boolean isRecordedEviction = incremental
                    && dirtyOrAnchorValue.getByte(DIRTY_SLOT_EVICTED) == 1;
            keyBuffer.jumpTo(0);
            LiveViewSnapshotKeyCodec.writeKey(keyBuffer, record, partitionKeyTypes, keyStartIndex);
            final long length = keyBuffer.getAppendOffset();
            if (length <= 0 || length > Integer.MAX_VALUE) {
                throw CairoException.critical(0)
                        .put("live view checkpoint anchor key length out of bounds, bytes=").put(length);
            }
            final MapValue anchorValue;
            if (incremental) {
                final MapKey liveKey = anchorMap.withKey();
                LiveViewSnapshotKeyCodec.readKey(liveKey, keyBuffer, 0, partitionKeyTypes);
                anchorValue = liveKey.findValue();
                if (anchorValue == null) {
                    if (!isRecordedEviction) {
                        // compact() records every key it drops and the clear() sites all
                        // force a full scan first, so a dirty key the anchor map does not
                        // hold and that carries no eviction marker is a broken invariant
                        // rather than a removal. Dropping it here would leave the
                        // incremental root holding a stale anchor value for a key the live
                        // map has moved on from: an incremental build removes nothing it
                        // was not handed.
                        throw CairoException.critical(0)
                                .put("live view checkpoint dirty anchor key is missing from the anchor map");
                    }
                    final byte[] key = copyEncodedKey(keyBuffer, (int) length);
                    removedKeysOut.add(key);
                    if (!isNewSinceCheckpoint) {
                        // The predecessor root holds this key, so the build takes its
                        // entry out and the charge goes with it. A key created and evicted
                        // inside one cadence was never published, and un-charging it would
                        // drive the total below what the root actually holds.
                        logicalBytes = checkedAdd(
                                logicalBytes,
                                -((long) key.length + LiveViewCheckpointAnchorRoot.ENTRY_STATE_SIZE)
                        );
                    }
                    continue;
                }
            } else {
                anchorValue = dirtyOrAnchorValue;
            }
            if (anchorValue.getByte(SLOT_TOMBSTONE) == 1) {
                continue;
            }
            final byte[] key = copyEncodedKey(keyBuffer, (int) length);
            keysOut.add(key);
            valuesOut.add(anchorValue.getLong(SLOT_ANCHOR_VALUE));
            if (!incremental || isNewSinceCheckpoint) {
                logicalBytes = checkedAdd(
                        logicalBytes,
                        (long) key.length + LiveViewCheckpointAnchorRoot.ENTRY_STATE_SIZE
                );
            }
        }
        return logicalBytes;
    }

    /**
     * @return current live (non-tombstoned + tombstoned) entry count in the
     * anchor map. Useful for tests and the {@code live_views()} catalogue.
     */
    public long getAnchorMapSize() {
        return anchorMap.size();
    }

    /**
     * @return the column type the anchor expression evaluates to. Persisted in
     * the checkpoint anchor root and compared against the recompiled runtime on
     * restore, because a widened return type changes how the stored LONG slot
     * must be read back.
     */
    public int getAnchorValueType() {
        return anchorValueType;
    }

    /**
     * @return the fixed segment boundary this anchor resets on, or null when the
     * compiler could not derive one. A localized out-of-order repair bounds itself
     * with it; a null plan leaves the view on the from-boundary rebuild.
     */
    public @Nullable LiveViewCheckpointAnchorPlan getCheckpointAnchorPlan() {
        return checkpointAnchorPlan;
    }

    /**
     * @return the checkpoint generation this window's incremental baseline was
     * recorded against, or {@link Numbers#LONG_NULL} when it holds none
     */
    @TestOnly
    public long getCheckpointBaselineGeneration() {
        return checkpointBaselineGeneration;
    }

    /**
     * @return anchor keys touched since the last durable checkpoint. Zero before the
     * window has processed a row, and zero again immediately after a publication
     */
    @TestOnly
    public long getCheckpointDirtyAnchorMapSize() {
        return checkpointDirtyAnchorMap == null ? 0 : checkpointDirtyAnchorMap.size();
    }

    /**
     * @return the dirty anchor map's current key capacity, or 0 when it holds none. What
     * it exposes is the map's retained backing rather than what it holds: a publication
     * empties the map but a plain clear keeps the capacity, so this is where a sweep's
     * inflated peak would stay visible if nothing handed it back
     */
    @TestOnly
    public int getCheckpointDirtyAnchorMapKeyCapacity() {
        return checkpointDirtyAnchorMap == null ? 0 : checkpointDirtyAnchorMap.getKeyCapacity();
    }

    /**
     * @return how many dirty anchor keys currently carry the frontier sweep's eviction
     * marker, which is what the next seal turns into removals
     */
    @TestOnly
    public int getCheckpointEvictionMarkerCount() {
        if (checkpointDirtyAnchorMap == null) {
            return 0;
        }
        final MapRecordCursor cursor = checkpointDirtyAnchorMap.getCursor();
        final MapRecord record = checkpointDirtyAnchorMap.getRecord();
        int marked = 0;
        while (cursor.hasNext()) {
            if (record.getValue().getByte(DIRTY_SLOT_EVICTED) == 1) {
                marked++;
            }
        }
        return marked;
    }

    /**
     * @return what the last durably published root charges for the anchor map. An
     * incremental seal carries this figure forward and adjusts it by the keys it froze
     * and the ones it removed, while a restore recomputes it by walking the root it
     * read, so the two agreeing across a restart is what proves the running arithmetic
     * still describes what the root holds
     */
    @TestOnly
    public long getCheckpointLogicalStateBytes() {
        return checkpointLogicalStateBytes;
    }

    /**
     * @return the lifetime number of anchor entries the frontier sweep has dropped.
     * Divided by {@link #getCompactionCount()} it gives the mean reclaim per sweep,
     * which is what decides whether the seal that follows a sweep is worth
     * optimising. Survives a sweep; only a window rebuild resets it.
     */
    public long getCompactedPartitionCount() {
        return compactedPartitionCount;
    }

    /**
     * @return the lifetime number of frontier sweeps this window has run.
     */
    public long getCompactionCount() {
        return compactionCount;
    }

    /**
     * @return the lifetime wall time of the frontier sweeps, in micros. The sweep
     * walks the whole anchor map and rebuilds every function's partition map from
     * the survivors, so this is the cost the reclaim itself charges, separate from
     * the seal that follows it.
     */
    public long getCompactionMicros() {
        return compactionMicros;
    }

    public ObjList<WindowFunction> getFunctions() {
        return functions;
    }

    /**
     * @return the anchor map entry count the most recent frontier sweep started from,
     * or 0 when this window has swept none. Read together with
     * {@link #getCompactedPartitionCount()} it gives the survivor count the seal after
     * that sweep has to freeze.
     */
    public long getLastCompactionMapSize() {
        return lastCompactionMapSize;
    }

    /**
     * @return the anchor map's partition-key column types. Consumed by the
     * head-checkpoint capability gate ({@link LiveViewSnapshotKeyCodec#isAllTypesSupported})
     * and tests.
     */
    public ColumnTypes getPartitionKeyTypes() {
        return partitionKeyTypes;
    }

    /**
     * @return number of anchor-map entries currently marked tombstoned
     * (SLOT_TOMBSTONE == 1). Consumed by the compaction trigger.
     */
    public long getTombstoneCount() {
        return tombstoneCount;
    }

    /**
     * @return the user-facing name of the WINDOW clause this object drives.
     * At most one anchored WINDOW is allowed per live view (multi-anchored-window
     * LVs are rejected at CREATE); the name is persisted into the WINDOW_ANCHOR
     * checkpoint block so future restores can match by-name rather than by-position.
     */
    public String getWindowName() {
        return windowName;
    }

    /**
     * Initialises the anchor expression against {@code baseCursor} so that
     * bind variables, symbol tables, etc. resolve correctly for the rows this
     * window will process. Called once per refresh cycle by the wrapping cursor.
     */
    public void init(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        anchorExpression.init(baseCursor, executionContext);
    }

    /**
     * @return whether the next seal must freeze every live anchor entry rather than
     * the touched ones. {@link #canFreezeCheckpointIncrementally(long)} is the seal's
     * own gate and additionally demands the dirty map, which the first processed row
     * allocates; this reads the flag on its own
     */
    @TestOnly
    public boolean isCheckpointFullScanRequired() {
        return isCheckpointFullScanRequired;
    }

    /**
     * Adopts a durable root's state as this window's incremental baseline. Two
     * callers reach it:
     * <ul>
     *     <li>the seal, only after the checkpoint superblock is durably published, so
     *     a seal that fails anywhere before that leaves the dirty map and the previous
     *     baseline intact and the next seal repeats the work;</li>
     *     <li>the restore, once it has rehydrated the anchor map from the generation's
     *     head root - the map then equals that root entry for entry, which is the same
     *     position a seal leaves it in.</li>
     * </ul>
     *
     * @param logicalStateBytes what the root charges for the anchor map
     * @param generation        the generation the root belongs to. The next seal
     *                          freezes incrementally only when it is sealing on top of
     *                          exactly this generation
     */
    public void onCheckpointPersisted(long logicalStateBytes, long generation) {
        checkpointBaselineGeneration = generation;
        checkpointLogicalStateBytes = logicalStateBytes;
        isCheckpointFullScanRequired = false;
        clearCheckpointDirtyAnchorMap();
    }

    /**
     * Reopen hook for the wrapping {@link AnchorDispatchingCursor}: invoked
     * whenever the cursor stack issues {@code toTop()} between refresh ticks.
     * Resets only the anchor expression's per-cursor iteration state; the
     * anchor map and tombstone count are preserved so the in-memory
     * partition-anchor record survives across incremental refresh cycles
     * (and across the first post-restart tick following
     * {@link io.questdb.cairo.lv.LiveViewRefreshJob}'s restore from a head
     * checkpoint).
     * <p>
     * Full-reset semantics live on {@link #toTop()} and are invoked
     * explicitly by the head-miss replay path before reopening the cursor
     * stack.
     */
    public void onCursorReopen() {
        anchorExpression.toTop();
    }

    /**
     * Drives the per-row anchor-comparison + reset-dispatch logic for one input row.
     * Must be invoked before the row reaches the underlying window cursor's
     * {@code computeNext}.
     */
    public void processRow(Record record) {
        MapKey key = anchorMap.withKey();
        key.put(record, partitionKeySink);
        MapValue value = key.createValue();

        final boolean isNewPartition = value.isNew();
        markCheckpointPartitionDirty(record, isNewPartition);
        final byte initialized = isNewPartition ? 0 : value.getByte(SLOT_INITIALIZED);
        final long lastAnchor = initialized == 0 ? 0 : value.getLong(SLOT_ANCHOR_VALUE);
        final long currentAnchor = readAnchorValue(record);
        trackFrontier(currentAnchor);
        final boolean shouldReset = initialized == 0 || lastAnchor != currentAnchor;

        if (isNewPartition) {
            // First row for this partition - anchor map didn't carry it yet. Functions
            // either have no per-partition state yet (in which case resetPartition is
            // a no-op) or have stale state from a prior partition that was evicted -
            // resetting it is the safe default. Write a 0 tombstone slot explicitly
            // rather than relying on createValue() value-byte zero-fill, which
            // OrderedMap does not guarantee (Unsafe.realloc / Unsafe.malloc / clear()
            // can all leave stale bytes in the heap region the new entry lands on);
            // a stale 1 would make the anchor snapshot drop a live partition.
            value.putByte(SLOT_TOMBSTONE, (byte) 0);
        }

        if (shouldReset) {
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).resetPartition(record);
            }
            movePartitionToCurrentBucket(initialized == 0, lastAnchor);
            value.putLong(SLOT_ANCHOR_VALUE, currentAnchor);
            value.putByte(SLOT_INITIALIZED, (byte) 1);
        }

        // markPartitionAlive runs AFTER resetPartition so the anchor-cross row's reset
        // (which sets a per-function tombstone bit) is immediately cancelled. The
        // partition is alive in its new bucket: this same row's computeNext
        // repopulates the accumulator, so the partition's state is NOT identity and
        // must never be dropped or skipped by a snapshot. Reset-driven tombstones
        // therefore never persist. Map-growth reclamation instead uses the
        // frontier-gated sweep below, which drops a partition only once the anchor has
        // advanced past its bucket -- by which point its accumulator is no longer
        // needed (the next in-order row starts a fresh bucket; late rows replay).
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).markPartitionAlive(record);
        }

        // Frontier sweep is the last act on the row: compact() rebuilds the anchor map
        // and each function's partition map in fresh allocations and swaps references,
        // invalidating the local `value` handle.
        maybeCompact();
    }

    /**
     * Rehydrates the anchor map from a payload written by
     * {@link #snapshot(MemoryA)}. Clears the existing map, then walks the
     * serialised partition list and reinserts each entry with
     * {@code initialized=1, tombstone=0}.
     * <p>
     * Validates the recorded {@code windowName}, partition-key column count,
     * per-column types, and anchor value type against this window's static
     * shape; any mismatch throws {@link CairoException}, which the caller
     * (the checkpoint restore path) treats as corruption -- it retires the
     * unreadable timeline and the LV falls through to the head-miss replay
     * path. Same disposition as a failed metadata page checksum.
     */
    public void restore(MemoryR source) {
        restore(source, 0, Long.MAX_VALUE);
    }

    public void restore(MemoryR source, long offset) {
        restore(source, offset, Long.MAX_VALUE);
    }

    public void restore(MemoryR source, long offset, long payloadLength) {
        checkpointBaselineGeneration = Numbers.LONG_NULL;
        isCheckpointFullScanRequired = true;
        checkpointLogicalStateBytes = 0;
        clearCheckpointDirtyAnchorMap();
        final long payloadStart = offset;
        final CharSequence storedName = source.getStrA(offset);
        if (storedName == null || !storedName.toString().equals(windowName)) {
            throw CairoException.nonCritical()
                    .put("live view checkpoint anchor block window name mismatch [expected=")
                    .put(windowName)
                    .put(", got=")
                    .put(storedName)
                    .put(']');
        }
        // STR encoding is [INT length, length * CHAR].
        final int nameLen = source.getInt(offset);
        offset += Integer.BYTES + (long) nameLen * Character.BYTES;

        final int storedKeyColumnCount = source.getInt(offset);
        offset += Integer.BYTES;
        final int expectedKeyColumnCount = partitionKeyTypes.getColumnCount();
        if (storedKeyColumnCount != expectedKeyColumnCount) {
            throw CairoException.nonCritical()
                    .put("live view checkpoint anchor block key column count mismatch [expected=")
                    .put(expectedKeyColumnCount)
                    .put(", got=")
                    .put(storedKeyColumnCount)
                    .put(']');
        }
        for (int i = 0; i < storedKeyColumnCount; i++) {
            final int storedType = source.getInt(offset);
            offset += Integer.BYTES;
            final int expectedType = partitionKeyTypes.getColumnType(i);
            if (storedType != expectedType) {
                throw CairoException.nonCritical()
                        .put("live view checkpoint anchor block key column type mismatch [index=")
                        .put(i)
                        .put(", expected=")
                        .put(ColumnType.nameOf(expectedType))
                        .put(", got=")
                        .put(ColumnType.nameOf(storedType))
                        .put(']');
            }
        }
        final int storedAnchorValueType = source.getInt(offset);
        offset += Integer.BYTES;
        if (storedAnchorValueType != anchorValueType) {
            throw CairoException.nonCritical()
                    .put("live view checkpoint anchor block anchor value type mismatch [expected=")
                    .put(ColumnType.nameOf(anchorValueType))
                    .put(", got=")
                    .put(ColumnType.nameOf(storedAnchorValueType))
                    .put(']');
        }
        final long partitionCount = source.getLong(offset);
        offset += Long.BYTES;
        // Reject a negative count BEFORE clearing the anchor map: a negative count would wipe
        // the frontier and zero-iterate, and a header-only payload crafted to match
        // payloadLength would then pass the final length check - silently restoring empty
        // anchor state from a corrupt (but CRC-valid) checkpoint.
        if (partitionCount < 0) {
            throw CairoException.nonCritical()
                    .put("live view checkpoint anchor block negative partition count [count=")
                    .put(partitionCount)
                    .put(']');
        }
        // Reject a count that cannot fit in the remaining payload BEFORE clearing the frontier:
        // each entry consumes at least the anchor Long, so a crafted (CRC-valid) oversized count
        // would otherwise drive an out-of-bounds / long-running read that only the final length
        // check catches. Division avoids overflow; skipped when the length is unknown.
        if (payloadLength != Long.MAX_VALUE) {
            final long remainingBytes = payloadLength - (offset - payloadStart);
            if (remainingBytes < 0 || partitionCount > remainingBytes / Long.BYTES) {
                throw CairoException.nonCritical()
                        .put("live view checkpoint anchor block partition count exceeds payload [count=")
                        .put(partitionCount)
                        .put(", remainingBytes=")
                        .put(remainingBytes)
                        .put(']');
            }
        }

        anchorMap.clear();
        tombstoneCount = 0;
        // Reconstruct the two retained frontier generations while reading the
        // checkpoint so the first post-restore sweep has exact reclaimable counts.
        resetFrontier();
        for (long i = 0; i < partitionCount; i++) {
            MapKey key = anchorMap.withKey();
            offset = LiveViewSnapshotKeyCodec.readKey(key, source, offset, partitionKeyTypes);
            MapValue value = key.createValue();
            long restoredAnchor = source.getLong(offset);
            value.putLong(SLOT_ANCHOR_VALUE, restoredAnchor);
            value.putByte(SLOT_INITIALIZED, (byte) 1);
            value.putByte(SLOT_TOMBSTONE, (byte) 0);
            restoreFrontierEntry(restoredAnchor);
            offset += Long.BYTES;
        }
        final long consumed = offset - payloadStart;
        if (payloadLength != Long.MAX_VALUE && consumed != payloadLength) {
            throw CairoException.critical(0)
                    .put("live view anchor snapshot payload length mismatch [expected=")
                    .put(payloadLength)
                    .put(", consumed=")
                    .put(consumed)
                    .put(']');
        }
    }

    /**
     * Rehydrates one anchor entry read from a checkpoint anchor-map leaf.
     * {@code keySource} is the entry's encoded partition key, bounded to its
     * exact length; the decoder must consume all of it.
     * <p>
     * Callers restore a complete root, so {@link #beginCheckpointRestore()}
     * must precede the first entry. The two retained frontier generations are
     * reconstructed as the entries arrive, leaving the first post-restore sweep
     * with exact reclaimable counts.
     */
    public void restoreCheckpointEntry(@NotNull LiveViewStatePageReader keySource, long anchorValue) {
        final MapKey key = anchorMap.withKey();
        final long consumed = LiveViewSnapshotKeyCodec.readKey(key, keySource, 0, partitionKeyTypes);
        if (consumed != keySource.size()) {
            throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                    .put("live view checkpoint anchor key decoder did not consume the entry exactly [expected=")
                    .put(keySource.size()).put(", consumed=").put(consumed).put(']');
        }
        final MapValue value = key.createValue();
        if (!value.isNew()) {
            throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                    .put("live view checkpoint anchor contains a duplicate partition key");
        }
        value.putLong(SLOT_ANCHOR_VALUE, anchorValue);
        value.putByte(SLOT_INITIALIZED, (byte) 1);
        value.putByte(SLOT_TOMBSTONE, (byte) 0);
        restoreFrontierEntry(anchorValue);
    }

    /**
     * Serialises the anchor map's live entries (tombstoned entries are
     * skipped) into {@code sink}. {@link LiveViewCheckpointScratchOverlay} calls
     * this to take the published anchor state aside before a localized repair
     * replays over it, and {@link #restore(MemoryR, long, long)} reads the same
     * payload back.
     * <p>
     * Payload shape:
     * <pre>
     *   windowName: STR
     *   partitionKeyColumnCount: INT
     *   per key column: columnType: INT
     *   anchorValueType: INT
     *   partitionCount: LONG          (live entries only)
     *   per partition:
     *     per key column: keyValue    (LiveViewSnapshotKeyCodec)
     *     lastAnchorValue: LONG
     * </pre>
     */
    public void snapshot(MemoryA sink) {
        sink.putStr(windowName);
        final int keyColumnCount = partitionKeyTypes.getColumnCount();
        sink.putInt(keyColumnCount);
        for (int i = 0; i < keyColumnCount; i++) {
            sink.putInt(partitionKeyTypes.getColumnType(i));
        }
        sink.putInt(anchorValueType);
        final long liveCount = anchorMap.size() - tombstoneCount;
        sink.putLong(liveCount);

        // MapRecord column layout is [value0, value1, value2, key0, ..., keyN-1] - keys
        // sit after the three value slots (anchor LONG, initialized BYTE, tombstone BYTE).
        // The codec needs the key-start index to address them via record.getXxx(columnIndex).
        final int keyStartIndex = AnchorMapValueTypes.INSTANCE.getColumnCount();
        MapRecordCursor cursor = anchorMap.getCursor();
        MapRecord record = anchorMap.getRecord();
        long emitted = 0;
        while (cursor.hasNext()) {
            MapValue value = record.getValue();
            if (value.getByte(SLOT_TOMBSTONE) == 1) {
                continue;
            }
            LiveViewSnapshotKeyCodec.writeKey(sink, record, partitionKeyTypes, keyStartIndex);
            sink.putLong(value.getLong(SLOT_ANCHOR_VALUE));
            emitted++;
        }
        if (emitted != liveCount) {
            throw CairoException.critical(0)
                    .put("live view anchor snapshot live-count mismatch [expected=")
                    .put(liveCount)
                    .put(", emitted=")
                    .put(emitted)
                    .put(']');
        }
    }

    /**
     * Full reset: wipes the per-partition anchor map, zeroes the tombstone
     * counter, and re-initialises the anchor expression. Intended for
     * non-incremental rebuild paths (head-miss O3 replay, full bootstrap)
     * where downstream callers explicitly want a clean slate before the
     * cursor stack reopens.
     * <p>
     * The wrapping {@link AnchorDispatchingCursor} routes its cursor-stack
     * {@code toTop()} through {@link #onCursorReopen()} instead, so
     * back-to-back incremental refresh ticks (including the first tick
     * after a restart that just rehydrated this map from a head
     * checkpoint) preserve the recorded partition-anchor state.
     */
    public void toTop() {
        checkpointBaselineGeneration = Numbers.LONG_NULL;
        isCheckpointFullScanRequired = true;
        checkpointLogicalStateBytes = 0;
        clearCheckpointDirtyAnchorMap();
        anchorMap.clear();
        tombstoneCount = 0;
        resetFrontier();
        anchorExpression.toTop();
    }

    /**
     * Validates one anchor entry's encoded key without changing runtime state.
     */
    public void validateCheckpointEntry(@NotNull LiveViewStatePageReader keySource) {
        final long consumed = LiveViewSnapshotKeyCodec.validateKey(keySource, 0, partitionKeyTypes);
        if (consumed != keySource.size()) {
            throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                    .put("live view checkpoint anchor key decoder did not consume the entry exactly [expected=")
                    .put(keySource.size()).put(", consumed=").put(consumed).put(']');
        }
    }

    /**
     * Frontier-gated sweep: drops every partition whose last anchor value is below
     * {@code prevFrontier} (the bucket before the current one), keeping the current
     * and previous buckets. Allocates a fresh anchor {@link Map}, copies the
     * surviving entries, then hands the survivor map to each function's
     * {@link WindowFunction#retainPartitions(Map, RecordSink, boolean)} so the
     * per-function partition maps drop the same keys, finally swaps the reference and
     * frees the old map.
     * <p>
     * The eviction branch is also the only enumeration of the dropped keys the system
     * gets - both rebuilds are survivor-driven and never visit one - so it records each
     * key in the anchor's dirty set and in every function's, marked as an eviction. The
     * next seal freezes those removals and stays incremental; before, the sweep pinned
     * it to a complete freeze of every live key of every function. A function that
     * declines to record a key gets {@code false} and falls back to that complete
     * freeze on its own.
     * <p>
     * Safe only for a monotone anchor: a dropped partition's next in-WAL-order row
     * lands in a new bucket and resets anyway, and a late row routes through O3
     * replay, which rebuilds state from the base. {@link #trackFrontier} latches
     * {@code compactionViable=false} for non-monotone or NULL anchors, and this
     * method also returns early when no frontier advance has happened yet (so there
     * is no safe cutoff).
     * <p>
     * The anchor map and a function's partition map may legitimately use different
     * {@link Map} implementations, because {@code MapFactory.createUnorderedMap} selects
     * on value size as well as key shape. The sweep therefore passes {@link #anchorKeySink}
     * alongside the survivor map: the rebuild writes keys through that sink's per-column
     * putters instead of casting to a concrete implementation's key, so it never has to
     * reconcile the two implementations.
     * <p>
     * Wired into {@link #processRow(Record)} via {@link #maybeCompact()} once the
     * anchor advances past a bucket boundary and the map exceeds
     * {@code cairo.live.view.partition.compact.threshold}. Also directly callable
     * from tests.
     */
    public void compact() {
        if (!compactionViable || prevFrontier == Long.MIN_VALUE) {
            // Non-monotone/NULL anchor, or no frontier advance yet -> no safe cutoff.
            return;
        }
        final MicrosecondClock clock = cairoConfiguration.getMicrosecondClock();
        final long startMicros = clock.getTicks();
        lastCompactionMapSize = anchorMap.size();
        final long cutoff = prevFrontier;
        final int functionCount = functions.size();
        checkpointRemovalsRecorded.setAll(functionCount, true);
        for (int i = 0; i < functionCount; i++) {
            // A ring-shaped function's seal walks its whole map either way -
            // freezeFunction gates the dirty-map path on !isRingShaped - so populating a
            // removal set for it buys nothing and costs one map insert per evicted key.
            if (functions.getQuick(i).supportsCheckpointRingState()) {
                checkpointRemovalsRecorded.setQuick(i, false);
            }
        }
        if (scratchAnchorMap == null) {
            // Allocate the reusable second anchor map once; subsequent sweeps reuse it. The
            // sweep ping-pongs it with anchorMap, so it outlives the sweep and is charged to
            // the same per-view tracker.
            scratchAnchorMap = createTrackedAnchorMap(cairoConfiguration, partitionKeyTypes, memoryTracker);
        } else {
            // Clear before rebuild (not after swap) so the scratch stays consistent
            // even if a prior sweep threw mid-rebuild.
            scratchAnchorMap.clear();
        }
        MapRecordCursor cursor = anchorMap.getCursor();
        MapRecord record = anchorMap.getRecord();
        long evictedCount = 0;
        while (cursor.hasNext()) {
            MapValue srcValue = record.getValue();
            if (srcValue.getLong(SLOT_ANCHOR_VALUE) < cutoff) {
                evictedCount++;
                // The sweep is the only enumeration of the evicted keys anyone gets: the
                // survivor-driven rebuild below never visits one. Recording them here is
                // what lets the next seal freeze the removals instead of walking the whole
                // live domain to discover them.
                markCheckpointPartitionEvicted(record);
                for (int i = 0; i < functionCount; i++) {
                    if (checkpointRemovalsRecorded.get(i)) {
                        checkpointRemovalsRecorded.setQuick(
                                i,
                                functions.getQuick(i).markCheckpointPartitionEvicted(record, anchorKeySink)
                        );
                    }
                }
                continue;
            }
            long srcKeyHash = record.keyHashCode();
            MapKey dstKey = scratchAnchorMap.withKey();
            record.copyToKey(dstKey);
            MapValue dstValue = dstKey.createValue(srcKeyHash);
            record.copyValue(dstValue);
        }
        // Each function keeps only the partitions still in the survivor anchor map.
        // anchorKeySink is what lets a function whose partition map picked a different
        // Map implementation than the anchor map (MapFactory selects on value size as
        // well as key shape) probe it anyway; see WindowFunction.retainPartitions.
        for (int i = 0; i < functionCount; i++) {
            functions.getQuick(i).retainPartitions(
                    scratchAnchorMap,
                    anchorKeySink,
                    checkpointRemovalsRecorded.get(i)
            );
        }
        // Ping-pong: survivor map becomes live; the old anchor map becomes the
        // scratch for the next sweep. No allocation, no free.
        Map old = anchorMap;
        anchorMap = scratchAnchorMap;
        scratchAnchorMap = old;
        tombstoneCount = 0;
        stalePartitionCount = 0;
        lastCompactedFrontier = maxAnchorValue;
        compactionCount++;
        compactedPartitionCount += evictedCount;
        compactionMicros += clock.getTicks() - startMicros;
    }

    /**
     * Empties the checkpoint dirty set, handing its backing memory back when the frontier
     * sweep is what grew it.
     * <p>
     * {@link Map#clear()} keeps the capacity, which is what a cadence wants: the dirty set
     * holds roughly the same touched-key count every time, so re-growing it per cadence
     * would be pure churn. A sweep breaks that - it adds one entry per evicted key on top
     * of the touched ones, and the trigger fires only when at least half the anchor map is
     * reclaimable, so the peak is a multiple of the steady state and then stays resident
     * against {@code cairo.live.view.refresh.memory.limit.bytes} for the view's lifetime.
     * {@link Map#restoreInitialCapacity()} is the only primitive that gives it back -
     * {@code setKeyCapacity} grows only - so the sweep-inflated cadence pays a re-grow next
     * time and every other cadence keeps today's behaviour exactly.
     */
    private void clearCheckpointDirtyAnchorMap() {
        if (checkpointDirtyAnchorMap == null) {
            return;
        }
        if (hasCheckpointEvictionsRecorded && checkpointDirtyAnchorMap.isOpen()) {
            checkpointDirtyAnchorMap.restoreInitialCapacity();
        }
        // Unconditionally, and after the shrink rather than instead of it: OrderedMap's
        // restoreInitialCapacity() clears only as a side effect of actually reallocating,
        // so a map already at its initial capacity would keep every entry and the next
        // seal would freeze the same removals a second time.
        checkpointDirtyAnchorMap.clear();
        hasCheckpointEvictionsRecorded = false;
    }

    /**
     * Adds one partition key to the checkpoint dirty set and records whether it was new
     * relative to the last durable checkpoint. The marker keeps logical-size accounting
     * exact without probing the persistent anchor root.
     */
    private void markCheckpointPartitionDirty(Record record, boolean isNewPartition) {
        if (checkpointDirtyAnchorMap == null) {
            checkpointDirtyAnchorMap = createTrackedDirtyAnchorMap(
                    cairoConfiguration,
                    partitionKeyTypes,
                    memoryTracker
            );
        }
        final MapKey key = checkpointDirtyAnchorMap.withKey();
        key.put(record, partitionKeySink);
        final MapValue value = key.createValue();
        if (value.isNew()) {
            value.putByte(DIRTY_SLOT_NEW_SINCE_CHECKPOINT, isNewPartition ? (byte) 1 : (byte) 0);
        }
        // Unconditionally, including on an entry that already existed: this row is what
        // turns a key the sweep evicted earlier in the cadence back into an upsert.
        // Writing it on a fresh entry also keeps the marker off whatever bytes the map's
        // backing happened to hold - createValue() zero-fills on no implementation.
        value.putByte(DIRTY_SLOT_EVICTED, (byte) 0);
    }

    /**
     * Adds one anchor key the frontier sweep has just dropped to the checkpoint dirty
     * set, marked as an eviction so the next seal freezes a removal for it rather than
     * raising on the missing live value.
     * <p>
     * {@code record} is the anchor map's own {@link MapRecord}, whose key columns sit
     * after the value slots, so the key goes in through {@link #anchorKeySink} rather
     * than {@link #partitionKeySink}. A key already in the dirty set keeps the
     * new-since-checkpoint marker the row that put it there wrote: whether the
     * predecessor root holds the key is what that marker says, and an eviction does not
     * change it.
     */
    private void markCheckpointPartitionEvicted(Record record) {
        hasCheckpointEvictionsRecorded = true;
        if (checkpointDirtyAnchorMap == null) {
            checkpointDirtyAnchorMap = createTrackedDirtyAnchorMap(
                    cairoConfiguration,
                    partitionKeyTypes,
                    memoryTracker
            );
        }
        final MapKey key = checkpointDirtyAnchorMap.withKey();
        key.put(record, anchorKeySink);
        final MapValue value = key.createValue();
        if (value.isNew()) {
            // The sweep never drops a key the current bucket touched, so a key that
            // reaches here without a dirty entry was last written before the predecessor
            // root was published and that root holds it.
            value.putByte(DIRTY_SLOT_NEW_SINCE_CHECKPOINT, (byte) 0);
        }
        value.putByte(DIRTY_SLOT_EVICTED, (byte) 1);
    }

    /**
     * Tracks the running anchor maximum (the current bucket) and the bucket before
     * it, and latches off the frontier sweep for anchors that are not monotone with
     * the WAL stream. See {@link #compactionViable}.
     */
    private void trackFrontier(long currentAnchor) {
        if (!compactionViable) {
            return;
        }
        if (currentAnchor == Numbers.LONG_NULL) {
            // NULL is a stable bucket that can recur; frontier reasoning fails.
            compactionViable = false;
            return;
        }
        if (!frontierInitialized) {
            maxAnchorValue = currentAnchor;
            frontierInitialized = true;
        } else if (currentAnchor > maxAnchorValue) {
            stalePartitionCount += previousBucketPartitionCount;
            previousBucketPartitionCount = currentBucketPartitionCount;
            currentBucketPartitionCount = 0;
            prevFrontier = maxAnchorValue;
            maxAnchorValue = currentAnchor;
        } else if (currentAnchor < maxAnchorValue) {
            compactionViable = false;
        }
    }

    private void maybeCompact() {
        final long mapSize = anchorMap.size();
        final long halfMapSize = mapSize - mapSize / 2;
        if (compactionViable
                && prevFrontier != Long.MIN_VALUE
                && maxAnchorValue > lastCompactedFrontier
                && mapSize > compactThreshold
                && stalePartitionCount >= compactThreshold
                && stalePartitionCount >= halfMapSize) {
            compact();
        }
    }

    private void resetFrontier() {
        frontierInitialized = false;
        maxAnchorValue = 0;
        currentBucketPartitionCount = 0;
        previousBucketPartitionCount = 0;
        stalePartitionCount = 0;
        prevFrontier = Long.MIN_VALUE;
        lastCompactedFrontier = Long.MIN_VALUE;
        compactionViable = isAnchorMonotone && ColumnType.tagOf(anchorValueType) == ColumnType.TIMESTAMP;
    }

    private void movePartitionToCurrentBucket(boolean untracked, long lastAnchor) {
        if (!compactionViable) {
            return;
        }
        if (untracked) {
            currentBucketPartitionCount++;
        } else if (lastAnchor != maxAnchorValue) {
            if (lastAnchor == prevFrontier) {
                previousBucketPartitionCount--;
            } else {
                stalePartitionCount--;
            }
            currentBucketPartitionCount++;
        }
    }

    private void restoreFrontierEntry(long anchor) {
        if (!compactionViable) {
            return;
        }
        if (anchor == Numbers.LONG_NULL) {
            compactionViable = false;
            return;
        }
        if (!frontierInitialized) {
            maxAnchorValue = anchor;
            currentBucketPartitionCount = 1;
            frontierInitialized = true;
        } else if (anchor == maxAnchorValue) {
            currentBucketPartitionCount++;
        } else if (anchor > maxAnchorValue) {
            stalePartitionCount += previousBucketPartitionCount;
            prevFrontier = maxAnchorValue;
            previousBucketPartitionCount = currentBucketPartitionCount;
            maxAnchorValue = anchor;
            currentBucketPartitionCount = 1;
        } else if (prevFrontier == Long.MIN_VALUE) {
            prevFrontier = anchor;
            previousBucketPartitionCount = 1;
        } else if (anchor == prevFrontier) {
            previousBucketPartitionCount++;
        } else if (anchor > prevFrontier) {
            stalePartitionCount += previousBucketPartitionCount;
            prevFrontier = anchor;
            previousBucketPartitionCount = 1;
        } else {
            stalePartitionCount++;
        }
    }

    private long readAnchorValue(Record record) {
        // build() restricts anchorValueType to TIMESTAMP, LONG, or INT; INT
        // widens cleanly into the LONG slot via getInt's int-to-long promotion.
        switch (ColumnType.tagOf(anchorValueType)) {
            case ColumnType.TIMESTAMP:
                return anchorExpression.getTimestamp(record);
            case ColumnType.INT:
                return anchorExpression.getInt(record);
            default:
                return anchorExpression.getLong(record);
        }
    }

    /**
     * Static singleton {@link ColumnTypes} for the anchor map's value layout —
     * exposed via {@link #anchorMapValueTypes()} so callers don't have to know
     * the slot order.
     */
    private static final class AnchorMapValueTypes implements ColumnTypes {
        static final AnchorMapValueTypes INSTANCE = new AnchorMapValueTypes();

        @Override
        public int getColumnCount() {
            return 3;
        }

        @Override
        public int getColumnType(int columnIndex) {
            switch (columnIndex) {
                case SLOT_ANCHOR_VALUE:
                    return ColumnType.LONG;
                case SLOT_INITIALIZED:
                    return ColumnType.BYTE;
                case SLOT_TOMBSTONE:
                    return ColumnType.BYTE;
                default:
                    throw new IndexOutOfBoundsException();
            }
        }
    }

    /**
     * Value layout of the checkpoint dirty-key map: two bytes, the
     * {@link #DIRTY_SLOT_NEW_SINCE_CHECKPOINT} and {@link #DIRTY_SLOT_EVICTED}
     * markers. The map's whole job is to name keys and say how each one got there,
     * and {@link #freezeCheckpointEntries} reads every anchor value it publishes out
     * of the live anchor map, so nothing else belongs here.
     */
    private static final class DirtyAnchorMapValueTypes implements ColumnTypes {
        static final DirtyAnchorMapValueTypes INSTANCE = new DirtyAnchorMapValueTypes();

        @Override
        public int getColumnCount() {
            return 2;
        }

        @Override
        public int getColumnType(int columnIndex) {
            if (columnIndex == DIRTY_SLOT_NEW_SINCE_CHECKPOINT || columnIndex == DIRTY_SLOT_EVICTED) {
                return ColumnType.BYTE;
            }
            throw new IndexOutOfBoundsException();
        }
    }
}
