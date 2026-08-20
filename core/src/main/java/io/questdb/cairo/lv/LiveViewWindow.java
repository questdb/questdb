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
import io.questdb.std.IntList;
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
    // both - so carrying the four slots below would be padding on every key the
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
    // The cadence value no key is ever marked with, so a value slot that was never
    // written - and a map implementation zero-fills none of them - cannot read as
    // "already dirty in this cadence". The counter starts at 1 and never emits it.
    private static final short EPOCH_NONE = 0;
    // The freeze walk emits the window's own fused payload rather than one member's
    // whole-state image.
    private static final int NO_MEMBER_PROJECTION = -1;
    // Slot 0: last-seen anchor value (LONG / TIMESTAMP).
    // Slot 1: byte flag — 0 means "uninitialized", 1 means "set". The MapValue's
    // intrinsic isNew() flips to false on first access; we use this explicit flag
    // so the live-view processRow can distinguish "first row of a partition" from
    // "anchor changed between rows."
    // Slot 2: byte tombstone — 0 means "alive" (partition saw a row recently), 1
    // means "stale" (anchor crossed and no follow-up row visited the partition
    // since). The anchor-map compaction trigger reclaims
    // tombstoned entries.
    // Slot 3: short cadence - the checkpoint cadence this key was last entered into
    // the dirty map in, or EPOCH_NONE. Reading it off the value processRow has already
    // loaded is what lets a repeat row skip re-serializing and re-probing the key into
    // that second map. SHORT rather than LONG deliberately: MapFactory selects the map
    // implementation on the raw key+value byte sum against
    // cairo.sql.unordered.map.max.entry.size, and two more bytes land inside the
    // alignment padding every unordered map already pays, where eight would push an
    // INT-keyed fused view and a VARCHAR-keyed one onto OrderedMap.
    private static final int SLOT_ANCHOR_VALUE = 0;
    private static final int SLOT_DIRTY_EPOCH = 3;
    private static final int SLOT_INITIALIZED = 1;
    private static final int SLOT_TOMBSTONE = 2;

    private final Function anchorExpression;
    // Reads the partition-by key columns straight off the anchor map's own MapRecord.
    // compact() hands this to every function so one whose partition map picked a
    // different Map implementation can still mirror the survivors into a probe of its
    // own implementation -- the sink writes through per-column putters and never casts.
    //
    // One per value layout, because a map record lays its value columns out ahead of its
    // key columns: the narrow layout's key tail starts four slots in, the fused one's
    // after the components too. The active layout picks between them.
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
    // Share of the anchor map, in percent, a sweep must be able to reclaim before it
    // fires (mirrors cairo.live.view.partition.compact.stale.percent). The only arm of
    // the trigger that scales with the map, so it is the one that binds once the map is
    // large; 0 turns it off and leaves the two count arms to decide.
    private final int compactStalePercent;
    // Anchor-map size above which a frontier sweep is attempted, and the stale count it
    // needs (mirrors cairo.live.view.partition.compact.threshold). The sweep itself is
    // gated on the anchor having advanced since the last sweep, so it fires at most once
    // per bucket boundary rather than per row.
    private final int compactThreshold;
    // The fused window-state plan the compiler produced for this view, or null when the
    // factory carries no fusible group. Non-owning: the compiled factory owns the plan
    // and every function named by it, exactly as it owns `functions`. Held separately
    // from checkpointWindowStatePlan because the layouts both sides of adoption have to
    // be buildable at build() time - a RecordSink needs a BytecodeAssembler, and this
    // window outlives the compiler that lends it one.
    private final @Nullable LiveViewWindowStatePlan compiledWindowStatePlan;
    // The fused value layout's key sink, or null when the view compiled no plan.
    private final @Nullable RecordSink fusedKeySink;
    // The fused map's value layout: the window's own four slots, then every component's
    // slots in the plan's canonical order. Null when the view compiled no plan.
    private final @Nullable ColumnTypes fusedValueTypes;
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
    // The key sink and key-tail start index of whichever value layout anchorMap
    // currently carries. Everything that reads a key off the anchor map's own record -
    // the sweep, the eviction marker, the freeze, the snapshot - goes through these
    // rather than through the narrow layout's, which stops being the live one the moment
    // the plan is adopted.
    private RecordSink activeKeySink;
    private int activeKeyStartIndex;
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
    // The cadence the dirty map is currently accumulating. Every clear of that map moves
    // it on, which is what invalidates every SLOT_DIRTY_EPOCH the anchor map still holds
    // in one store rather than by walking them. Never EPOCH_NONE.
    private short checkpointDirtyEpoch = 1;
    // Keys entered into the dirty map. Incremented once per insert attempt rather than
    // per row, so a test can hold a cadence to one mark per distinct key.
    private long checkpointDirtyMarkCount;
    // Walks of the key domain this window has made for a freeze. Incremented once per
    // walk, not per key or per row, so a test can hold the seal to a walk count that does
    // not grow with the number of runtime-only members sharing it.
    private long checkpointFreezeScanCount;
    // Keys imaged over every freeze this window has made, so a test can price a whole
    // chain of them rather than only the one that happened to end it.
    private long checkpointFreezeKeyCountTotal;
    private long checkpointLastFreezeKeyCount;
    private long checkpointLogicalStateBytes;
    // The plan this window has adopted, or null when it holds none - because the factory
    // compiled none, because the plan's key layout is not this window's, or because
    // something declined it. Adopting it moves the group's runtime state into the anchor
    // map's own value and turns every grouped function's hot-path state method into a
    // no-op; declining puts each function back on the map it owns. Both directions are
    // state migrations, and both go through bindCheckpointWindowStatePlan.
    private @Nullable LiveViewWindowStatePlan checkpointWindowStatePlan;
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
            @Nullable LiveViewWindowStatePlan compiledWindowStatePlan,
            @Nullable ColumnTypes fusedValueTypes,
            @Nullable RecordSink fusedKeySink,
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
        this.compiledWindowStatePlan = compiledWindowStatePlan;
        this.fusedValueTypes = fusedValueTypes;
        this.fusedKeySink = fusedKeySink;
        this.activeKeySink = anchorKeySink;
        this.activeKeyStartIndex = AnchorMapValueTypes.INSTANCE.getColumnCount();
        this.functions = functions;
        this.isAnchorMonotone = isAnchorMonotone;
        this.checkpointAnchorPlan = checkpointAnchorPlan;
        this.memoryTracker = memoryTracker;
        this.compactStalePercent = cairoConfiguration.getLiveViewPartitionCompactStalePercent();
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
            @NotNull ColumnTypes partitionKeyTypes,
            @NotNull ColumnTypes valueTypes
    ) {
        final int keyStartIndex = valueTypes.getColumnCount();
        final int keyColumnCount = partitionKeyTypes.getColumnCount();
        final ArrayColumnTypes anchorRecordTypes = new ArrayColumnTypes();
        for (int i = 0; i < keyStartIndex; i++) {
            anchorRecordTypes.add(valueTypes.getColumnType(i));
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
            @NotNull ColumnTypes valueTypes,
            @Nullable MemoryTracker memoryTracker
    ) {
        Map map = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes, false, false);
        map.setMemoryTracker(memoryTracker);
        map.reopen();
        return map;
    }

    /**
     * Builds the fused map's value layout: the window's own four slots, then every
     * component's, in the plan's canonical order.
     * <p>
     * The layout widens the value, and {@code MapFactory.createUnorderedMap} selects on
     * {@code keySize + valueSize <= cairo.sql.unordered.map.max.entry.size}. That limit is
     * not one number: {@link io.questdb.cairo.DefaultCairoConfiguration} returns 16, which
     * embedded use and the benchmarks take, while {@code PropServerConfiguration} defaults
     * the property to 32, and the fused shape sits in the gap between the two. Only an
     * INT-keyed view is affected - it moves from {@code Unordered4Map} to
     * {@code OrderedMap} at 16 and stays put at 32, while every wider key was already past
     * both - so a claim about the {@link Map} implementation has to name the limit it holds
     * for. See {@link #getAnchorMapImplementation()}.
     */
    private static ColumnTypes fusedMapValueTypes(@NotNull LiveViewWindowStatePlan plan) {
        final ArrayColumnTypes types = new ArrayColumnTypes();
        for (int i = 0, n = AnchorMapValueTypes.INSTANCE.getColumnCount(); i < n; i++) {
            types.add(AnchorMapValueTypes.INSTANCE.getColumnType(i));
        }
        for (int c = 0, m = plan.getComponentCount(); c < m; c++) {
            final LiveViewAccumulatorDescriptor component = plan.getComponent(c);
            for (int s = 0, k = component.getSlotCount(); s < k; s++) {
                types.add(component.getSlotColumnType(s));
            }
        }
        return types;
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
            @Nullable LiveViewWindowStatePlan windowStatePlan,
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
        RecordSink anchorKeySink = createAnchorKeySink(configuration, asm, mapKeyTypes, AnchorMapValueTypes.INSTANCE);
        // The fused layout is built here rather than at adoption because a RecordSink
        // needs the compiler's BytecodeAssembler and this window outlives the compiler.
        // A plan whose components are keyed differently is dropped now: the fused entry
        // is keyed by this map, so such a plan describes state it cannot address.
        // cairo.sql.window.map.fusion.enabled drops one here for the same reason it
        // withholds a runtime from a generic group in WindowMapState.createGroups: the
        // switch gates the binding, not the compile. The compiler still works the group
        // out and the factory still carries it; what a dropped plan costs this view is
        // the fused map value and the fused root, so every function keeps the private
        // map and the legacy root it has outside a group. A view sealed while the switch
        // was on and restarted with it off is not silently misread - the restore finds a
        // window root with no plan to restore into and reports it as recoverable
        // corruption, which falls back to a predecessor or rebuilds from the base.
        LiveViewWindowStatePlan compiledPlan =
                windowStatePlan != null
                        && configuration.isSqlWindowMapFusionEnabled()
                        && windowStatePlan.isKeyLayoutCompatible(mapKeyTypes)
                        ? windowStatePlan
                        : null;
        ColumnTypes fusedValueTypes = compiledPlan == null ? null : fusedMapValueTypes(compiledPlan);
        RecordSink fusedKeySink = fusedValueTypes == null
                ? null
                : createAnchorKeySink(configuration, asm, mapKeyTypes, fusedValueTypes);
        // createUnorderedMap (not createOrderedMap) so the anchor map keeps the fastest
        // implementation its key shape and value width allow. It need not agree with
        // any residual window function's choice -- MapFactory also selects on value size,
        // so a function with a wider live-view payload legitimately lands elsewhere --
        // because compact() hands each function the active key sink and the rebuild
        // bridges the two implementations through it. See retainPartitions.
        Map map = createTrackedAnchorMap(configuration, mapKeyTypes, AnchorMapValueTypes.INSTANCE, memoryTracker);
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
        return new LiveViewWindow(
                configuration,
                windowName,
                anchorExpression,
                returnType,
                mapKeyTypes,
                map,
                sink,
                anchorKeySink,
                compiledPlan,
                fusedValueTypes,
                fusedKeySink,
                functions,
                isAnchorMonotone,
                checkpointAnchorPlan,
                memoryTracker
        );
    }

    /**
     * Puts back the incremental-seal bookkeeping {@link #detachCheckpointSealState} took
     * aside, re-stamped against the generation the caller has since published.
     * <p>
     * The dirty set goes back as the same map it left as, so the keys the cadence had
     * named before the repair are the keys the next seal freezes - together with
     * whatever the rows since have added. Whatever the replay built in its place is
     * freed here rather than kept: it names the keys of a state nothing holds any more.
     * <p>
     * The anchor entries the {@link LiveViewCheckpointScratchOverlay} restored beside
     * this carry no cadence stamp - the restore writes {@code EPOCH_NONE} over every one
     * of them - so the first row to touch a key marks it again. That is a redundant map
     * insert per key per cadence and nothing worse: the set already holds the key, and
     * the failure direction of a lost stamp is an extra mark rather than a missing one.
     *
     * @param generation the generation the newest root this baseline names belongs to.
     *                   The caller has proved that root sits at or above the repair's
     *                   convergence boundary, so its payload is the one this window's
     *                   restored state was frozen from
     */
    public void attachCheckpointSealState(@NotNull LiveViewCheckpointSealState state, long generation) {
        Misc.free(checkpointDirtyAnchorMap);
        checkpointDirtyAnchorMap = state.takeDirtySet();
        hasCheckpointEvictionsRecorded = state.hasEvictionsRecorded();
        checkpointLogicalStateBytes = state.getLogicalStateBytes();
        checkpointBaselineGeneration = generation;
        isCheckpointFullScanRequired = false;
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
     * Adopts the compiler's fused window-state plan, or declines it. Declining is the
     * fail-safe direction and costs the view only the fused root: every function goes
     * back to the private map and the legacy root it has outside a group.
     * <p>
     * Adopting moves runtime ownership. The anchor map is rebuilt with the fused value
     * layout, each grouped function's accumulator is copied into the component slots the
     * plan assigned it, and the private maps are closed - from here one loaded value per
     * row serves the anchor and every projection on it. Declining reverses exactly that.
     * Both directions migrate the state rather than dropping it, because a view may be
     * rebound while it holds a live frontier and losing it would silently restart every
     * partition's accumulator at zero.
     * <p>
     * Only the plan this window was built with may be adopted: the fused value layout
     * and its key sink are built at {@code build()} time, where the compiler's
     * {@link BytecodeAssembler} is still available. Anything else - a null, a plan whose
     * key layout is not this window's, a plan from another factory - declines.
     *
     * @return true when the plan was adopted
     */
    public boolean bindCheckpointWindowStatePlan(@Nullable LiveViewWindowStatePlan plan) {
        final LiveViewWindowStatePlan adopted = plan != null && plan == compiledWindowStatePlan ? plan : null;
        if (adopted == checkpointWindowStatePlan) {
            return adopted != null;
        }
        if (adopted != null) {
            adoptWindowStatePlan(adopted);
        } else {
            declineWindowStatePlan();
        }
        // The durable shape changed under the runtime - a legacy anchor root and a fused
        // window root never share a leaf - so the next seal converts whole. Its own
        // predecessor test would reach the same answer; forcing it here keeps the
        // logical-byte baseline, which is charged per entry at a width that just moved,
        // from being carried across the change. The same has just been done to each
        // member's own root, which the window's flags do not reach.
        checkpointBaselineGeneration = Numbers.LONG_NULL;
        isCheckpointFullScanRequired = true;
        checkpointLogicalStateBytes = 0;
        clearCheckpointDirtyAnchorMap();
        return checkpointWindowStatePlan != null;
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

    /**
     * Moves the dirty-set cadence counter to {@code epoch}, so a test can stand a key's
     * stamp and the counter's turn against each other without driving 32766 seals.
     * <p>
     * No production path reaches this. What it exposes is the one arm that can leave a
     * stamp matching a cadence the dirty set no longer holds - the counter is a SHORT, so
     * it comes back around - and that arm is reached in the field on a timescale of days
     * rather than of rows, because a seal fires on a wall-clock cadence as well as on a
     * row one. Setting the counter is what lets a case put a key's stamp exactly where the
     * turn will land rather than somewhere it happens not to.
     */
    @TestOnly
    public void setCheckpointDirtyEpoch(short epoch) {
        checkpointDirtyEpoch = epoch;
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
     * Hands this window's incremental-seal bookkeeping to {@code state} and leaves the
     * window owing a complete freeze, so a converging repair can wipe and replay through
     * it without the bookkeeping being lost with the state it describes.
     * <p>
     * A window already owing one holds no baseline to carry and fills nothing in; the
     * carryover then leaves it exactly where it is. Everything else leaves here in the
     * same position a {@code requireCheckpointFullScan} would put it - baseline dropped,
     * flag raised - except that the dirty set travels rather than being emptied. That is
     * what makes the exchange safe at every point in between: a repair unwinding before
     * {@link #attachCheckpointSealState} leaves a window that full-scans, never one
     * holding a baseline whose dirty set went missing.
     */
    public void detachCheckpointSealState(@NotNull LiveViewCheckpointSealState state) {
        if (isCheckpointFullScanRequired || checkpointBaselineGeneration == Numbers.LONG_NULL) {
            return;
        }
        // Always tracking: the window creates its dirty set on the first key a cadence
        // touches and never opts out of marking, so there is no per-window equivalent of a
        // function's hasCheckpointDirtyTracking to carry.
        state.of(checkpointDirtyAnchorMap, true, hasCheckpointEvictionsRecorded, checkpointLogicalStateBytes);
        checkpointDirtyAnchorMap = null;
        hasCheckpointEvictionsRecorded = false;
        checkpointBaselineGeneration = Numbers.LONG_NULL;
        checkpointLogicalStateBytes = 0;
        isCheckpointFullScanRequired = true;
        // The map the stamps answered to is gone, so the stamps must stop matching in the
        // same act. Leaving them standing would have the rows between here and the attach
        // read keys as already marked in a cadence whose set no longer holds them.
        advanceCheckpointDirtyEpoch();
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
            boolean isIncremental
    ) {
        return freezeCheckpointEntries(
                keyBuffer,
                keysOut,
                valuesOut,
                removedKeysOut,
                isIncremental,
                LiveViewCheckpointAnchorRoot.ENTRY_STATE_SIZE,
                null
        );
    }

    /**
     * As {@link #freezeCheckpointEntries(MemoryCARW, ObjList, LongList, ObjList, boolean)},
     * but charging {@code entryStateBytes} of state per key rather than the anchor
     * value's own eight, and - when {@code payloadsOut} is non-null - emitting each
     * key's whole fused scalar payload from the same walk.
     * <p>
     * A fused seal writes one entry per key holding the anchor value <b>and</b> every
     * grouped accumulator component, so that entry is what the window's running logical
     * total has to describe: the grouped functions no longer charge anything of their
     * own. The two figures have to be produced by the same walk, because an incremental
     * freeze adds and subtracts against a total an earlier seal left behind, and a width
     * that changed between them would leave the running total describing neither root.
     * <p>
     * The payload comes out of the same loaded map value the anchor value does, which is
     * the whole point of owning the group's runtime state: the seal reads one entry per
     * key rather than probing a map per component.
     *
     * @param entryStateBytes the state bytes one published entry carries for a key
     * @param payloadsOut     the fused scalar payloads, index-aligned with
     *                        {@code keysOut}, or null for the legacy anchor-only shape
     */
    public long freezeCheckpointEntries(
            @NotNull MemoryCARW keyBuffer,
            @NotNull ObjList<byte[]> keysOut,
            @NotNull LongList valuesOut,
            @NotNull ObjList<byte[]> removedKeysOut,
            boolean isIncremental,
            int entryStateBytes,
            @Nullable ObjList<byte[]> payloadsOut
    ) {
        // One member, allocated locally: this runs once per seal, where the batched member
        // walk below runs once per seal for R members and is the one worth pooling.
        final IntList stateBytes = new IntList(1);
        stateBytes.add(entryStateBytes);
        final IntList projectionIndexes = new IntList(1);
        projectionIndexes.add(NO_MEMBER_PROJECTION);
        final LongList logicalBytes = new LongList(1);
        logicalBytes.add(isIncremental ? checkpointLogicalStateBytes : 0);
        ObjList<ObjList<byte[]>> payloads = null;
        if (payloadsOut != null) {
            payloads = new ObjList<>(1);
            payloads.add(payloadsOut);
        }
        freezeCheckpointEntries(
                keyBuffer,
                keysOut,
                valuesOut,
                removedKeysOut,
                isIncremental,
                stateBytes,
                payloads,
                projectionIndexes,
                logicalBytes
        );
        return logicalBytes.getQuick(0);
    }

    /**
     * Freezes every <b>runtime-only member</b> named by {@code projectionIndexes} out of the
     * group's map, in one walk: the same key domain, the same dirty set and the same
     * removals the window's own seal walks, with each member's whole-state image in place
     * of the fused payload.
     * <p>
     * This is what "the checkpoint addresses a group Map plus a function slice" means. A
     * member's state is not in a map of its own - the window closed that one when it
     * adopted the plan - so the walk is the anchor map's and the image is read out of each
     * entry at {@link LiveViewAccumulatorProjection#getFunctionSlotBase()} through the
     * component codec, which produces exactly the bytes the function's own
     * {@code freezeCheckpointState} would have written from a private map value.
     * <p>
     * Dirty and removal tracking is the group's for the same reason: a bound function marks
     * nothing of its own - see {@code BasePartitionedWindowFunction.markPartitionAlive} -
     * so the keys an incremental seal names, and the keys the frontier sweep dropped, are
     * the window's one dirty set's. That is also why one walk serves every member: those
     * keys, and the encoding they are named in, belong to the window rather than to any
     * member. What stays each member's own is only its state image and what its root
     * charges - the logical byte baseline it is incremental against.
     * <p>
     * The anchor value is not among what this emits, unlike the window's own seal: a
     * member publishes its function's state image, and the anchor entry the same keys
     * carry is the window root's, written by that seal from its own walk.
     *
     * @param projectionIndexes the members' projections in the adopted plan
     * @param imagesOut         one image list per member, index-aligned with
     *                          {@code projectionIndexes}. May hold more lists than there
     *                          are members - a pooled caller keeps the ones it has grown -
     *                          and only the first {@code projectionIndexes.size()} are read
     *                          or written
     * @param logicalBytesInOut each member's running logical total, index-aligned with
     *                          {@code projectionIndexes}: seeded by the caller with the
     *                          member root's own logical size, charged in place here, and
     *                          reset to zero for a complete freeze, which builds on nothing
     */
    public void freezeCheckpointMemberEntries(
            @NotNull MemoryCARW keyBuffer,
            @NotNull IntList projectionIndexes,
            @NotNull ObjList<byte[]> keysOut,
            @NotNull ObjList<ObjList<byte[]>> imagesOut,
            @NotNull ObjList<byte[]> removedKeysOut,
            boolean isIncremental,
            @NotNull LongList logicalBytesInOut
    ) {
        final LiveViewWindowStatePlan plan = checkpointWindowStatePlan;
        if (plan == null) {
            throw CairoException.critical(0)
                    .put("live view checkpoint member freeze without an adopted plan");
        }
        final int memberCount = projectionIndexes.size();
        // The caller's image lists are pooled and so may outnumber this walk's members -
        // a bucket narrower than a previous seal's keeps the lists it grew. Only the first
        // memberCount are read or written; anything past them is left alone.
        if (memberCount > imagesOut.size() || memberCount != logicalBytesInOut.size()) {
            throw CairoException.critical(0)
                    .put("live view checkpoint member freeze is not aligned with its members, members=")
                    .put(memberCount);
        }
        // The state width is read once per member rather than once per member per key: it
        // is a property of the projection, and the inner loop below runs K times.
        final IntList entryStateBytes = new IntList(memberCount);
        for (int m = 0; m < memberCount; m++) {
            entryStateBytes.add(plan.getProjection(projectionIndexes.getQuick(m)).getFunctionStateLength());
            if (!isIncremental) {
                logicalBytesInOut.setQuick(m, 0);
            }
        }
        freezeCheckpointEntries(
                keyBuffer,
                keysOut,
                null,
                removedKeysOut,
                isIncremental,
                entryStateBytes,
                imagesOut,
                projectionIndexes,
                logicalBytesInOut
        );
    }

    /**
     * Walks the key domain once and emits one image per member per key.
     * <p>
     * Every member of one walk shares the encoded key, the anchor-map probe an incremental
     * freeze makes to find the live entry, and the removal set - all three are properties
     * of the key rather than of the member, so a walk per member re-derived each of them R
     * times over. What stays each member's own is only the state image, which is read out
     * of the loaded value at that member's own slot base, and the logical charge, which is
     * seeded from that member's own root and charged at that member's own width.
     * <p>
     * Members must agree on {@code isIncremental}, because it selects the map that is walked
     * and the layout its records carry. They can disagree - a state-format version bump
     * leaves one member without a matching predecessor root while its siblings keep theirs
     * - so the caller buckets them by that flag and issues one walk per bucket, which is
     * two in the worst case and one in practice.
     * <p>
     * The single {@code byte[]} each key is encoded into is handed to every member. That is
     * safe because a frozen partition never mutates its key: {@code FrozenPartition.key} is
     * final and the directory and partition-map writers only read it.
     *
     * @param entryStateBytes   the state bytes one published entry carries, per member
     * @param valuesOut         the per-key anchor values, index-aligned with
     *                          {@code keysOut}, or null for a member walk, whose keys are
     *                          the window root's to publish an anchor value for
     * @param payloadsOut       the per-key images, per member, or null for the legacy
     *                          anchor-only shape that publishes no payload
     * @param logicalBytesInOut each member's running logical total: seeded by the caller
     *                          with the root the freeze builds on, charged in place here
     */
    private void freezeCheckpointEntries(
            @NotNull MemoryCARW keyBuffer,
            @NotNull ObjList<byte[]> keysOut,
            @Nullable LongList valuesOut,
            @NotNull ObjList<byte[]> removedKeysOut,
            boolean isIncremental,
            @NotNull IntList entryStateBytes,
            @Nullable ObjList<ObjList<byte[]>> payloadsOut,
            @NotNull IntList memberProjectionIndexes,
            @NotNull LongList logicalBytesInOut
    ) {
        checkpointFreezeScanCount++;
        final int memberCount = entryStateBytes.size();
        // Read once rather than per key: the walk below runs K times and a member walk
        // publishes no anchor value at all.
        final boolean isAnchorValueEmitted = valuesOut != null;
        keysOut.clear();
        if (isAnchorValueEmitted) {
            valuesOut.clear();
        }
        removedKeysOut.clear();
        if (payloadsOut != null) {
            for (int m = 0; m < memberCount; m++) {
                payloadsOut.getQuick(m).clear();
            }
            if (checkpointWindowStatePlan == null) {
                throw CairoException.critical(0)
                        .put("live view checkpoint window state freeze without an adopted plan");
            }
        }
        final Map scanMap = isIncremental ? checkpointDirtyAnchorMap : anchorMap;
        // A map record lays its value columns out ahead of its key columns, and the
        // two maps carry different value layouts, so the key tail starts at a
        // different index in each.
        final int keyStartIndex = isIncremental
                ? DirtyAnchorMapValueTypes.INSTANCE.getColumnCount()
                : activeKeyStartIndex;
        final MapRecordCursor cursor = scanMap.getCursor();
        final MapRecord record = scanMap.getRecord();
        while (cursor.hasNext()) {
            final MapValue dirtyOrAnchorValue = record.getValue();
            final boolean isNewSinceCheckpoint = isIncremental
                    && dirtyOrAnchorValue.getByte(DIRTY_SLOT_NEW_SINCE_CHECKPOINT) == 1;
            final boolean isRecordedEviction = isIncremental
                    && dirtyOrAnchorValue.getByte(DIRTY_SLOT_EVICTED) == 1;
            keyBuffer.jumpTo(0);
            LiveViewSnapshotKeyCodec.writeKey(keyBuffer, record, partitionKeyTypes, keyStartIndex);
            final long length = keyBuffer.getAppendOffset();
            if (length <= 0 || length > Integer.MAX_VALUE) {
                throw CairoException.critical(0)
                        .put("live view checkpoint anchor key length out of bounds, bytes=").put(length);
            }
            final MapValue anchorValue;
            if (isIncremental) {
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
                        for (int m = 0; m < memberCount; m++) {
                            logicalBytesInOut.setQuick(m, checkedAdd(
                                    logicalBytesInOut.getQuick(m),
                                    -((long) key.length + entryStateBytes.getQuick(m))
                            ));
                        }
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
            if (isAnchorValueEmitted) {
                valuesOut.add(anchorValue.getLong(SLOT_ANCHOR_VALUE));
            }
            final boolean isCharged = !isIncremental || isNewSinceCheckpoint;
            for (int m = 0; m < memberCount; m++) {
                final int stateBytes = entryStateBytes.getQuick(m);
                if (payloadsOut != null) {
                    final int memberProjectionIndex = memberProjectionIndexes.getQuick(m);
                    payloadsOut.getQuick(m).add(memberProjectionIndex == NO_MEMBER_PROJECTION
                            ? encodeWindowStatePayload(anchorValue, stateBytes)
                            : encodeMemberStateImage(
                            memberProjectionIndex,
                            anchorValue,
                            record,
                            keyStartIndex,
                            stateBytes
                    ));
                }
                if (isCharged) {
                    logicalBytesInOut.setQuick(
                            m,
                            checkedAdd(logicalBytesInOut.getQuick(m), (long) key.length + stateBytes)
                    );
                }
            }
        }
        checkpointLastFreezeKeyCount = keysOut.size() + removedKeysOut.size();
        checkpointFreezeKeyCountTotal += checkpointLastFreezeKeyCount;
    }

    /**
     * @return the compiled ANCHOR expression this window dispatches on. Owned upstream -
     * the window neither initialises nor frees it - and handed back so a caller that
     * builds a window can adopt the function beside it rather than carrying the pair
     * through its own out-parameter
     */
    public Function getAnchorExpression() {
        return anchorExpression;
    }

    /**
     * @return the {@link Map} implementation the window's one partition map landed on.
     * {@code MapFactory} selects on {@code keySize + valueSize} against
     * {@code cairo.sql.unordered.map.max.entry.size}, so fusing the components into the
     * value can move an INT-keyed view off the fastest shape; this is what a benchmark
     * reports per fused group to see whether it did
     */
    public String getAnchorMapImplementation() {
        return anchorMap.getClass().getSimpleName();
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
     * @return how many keys this window has entered into the dirty set over its lifetime.
     * One per distinct key per cadence rather than one per row, so a cadence that
     * processes many rows over few partitions advances it by the partitions
     */
    @TestOnly
    public long getCheckpointDirtyMarkCount() {
        return checkpointDirtyMarkCount;
    }

    /**
     * @return how many keys every freeze this window has made imaged, added up. A repair
     * that keeps the checkpoint ladder freezes a boundary per logical position its
     * replay crosses, and the property that matters across the chain is not what any one
     * of them cost but what all of them cost together: the keys the replay touched, once,
     * rather than the live domain once per boundary. Only a difference between two
     * readings means anything, so a case takes one before the correction and one after.
     */
    @TestOnly
    public long getCheckpointFreezeKeyCountTotal() {
        return checkpointFreezeKeyCountTotal;
    }

    /**
     * @return how many times a freeze has walked this window's key domain. The seal shares
     * one walk across every runtime-only member that agrees on the incremental disposition,
     * so this counts dispositions rather than members and does not grow with the SELECT
     * list's width
     */
    @TestOnly
    public long getCheckpointFreezeScanCount() {
        return checkpointFreezeScanCount;
    }

    /**
     * @return how many keys the last freeze imaged - the ones it wrote plus the ones it
     * removed. This is what separates an incremental seal from a complete one, and
     * nothing in the published artifacts does: both leave a root naming the whole live
     * domain, because the incremental one keeps every key it did not touch from its
     * predecessor. A repair that resumes from a boundary and then seals must land near
     * the keys its replay touched rather than near the domain size.
     */
    @TestOnly
    public long getCheckpointLastFreezeKeyCount() {
        return checkpointLastFreezeKeyCount;
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
     * @return the fused window-state plan this window adopted, or null when it holds
     * none. Read by tests and by the window-state root; the runtime hot path does not
     * consult it
     */
    public @Nullable LiveViewWindowStatePlan getCheckpointWindowStatePlan() {
        return checkpointWindowStatePlan;
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
     * Converts the {@link LiveViewCheckpointContracts#REPAIR_BASELINE_GENERATION
     * provisional repair stamp} this window carries into the real generation the
     * repair's splice has just published, keeping the dirty keys.
     * <p>
     * A repair freezes a chain of boundaries out of the running state, resetting the
     * dirty set at each one, and publishes the lot as a single generation once its
     * replacement is durable. What the window holds at the end is the newest of those
     * roots plus the keys the replay touched above it, so the stamp has to move while
     * that set stays where it is - which is the one thing
     * {@link #onCheckpointPersisted(long, long)} cannot do, because it clears the set
     * the post-repair head seal is about to freeze.
     * <p>
     * Only a window still carrying the provisional stamp moves; anything else keeps
     * what it has. The same is handed to every checkpoint-capable function - see
     * {@link WindowFunction#onCheckpointRepairBaselinePublished(long)} - because a
     * residual function keeps a dirty set and a baseline of its own.
     *
     * @param generation the generation the splice published
     */
    public void onCheckpointRepairBaselinePublished(long generation) {
        if (checkpointBaselineGeneration == LiveViewCheckpointContracts.REPAIR_BASELINE_GENERATION) {
            checkpointBaselineGeneration = generation;
        }
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
     * Drives the per-row anchor-comparison + reset-dispatch logic for one input row,
     * and - once the window owns the group's state - the group's whole accumulator
     * update and output materialization. Must be invoked before the row reaches the
     * underlying window cursor's {@code computeNext}.
     * <p>
     * Under an adopted plan this is the only partition-map lookup the fused group makes
     * per row: one loaded value carries the anchor, every accumulator component and the
     * bucket bookkeeping, so the crossing reset, the contributor updates and each
     * output's projection all run against bytes already in hand. Doing it here rather
     * than from the functions' own {@code computeNext} is also what removes any
     * dependency on SELECT-list order - every accumulator is whole before the first
     * output reads one.
     */
    public void processRow(Record record) {
        MapKey key = anchorMap.withKey();
        key.put(record, partitionKeySink);
        MapValue value = key.createValue();

        final boolean isNewPartition = value.isNew();
        if (isNewPartition) {
            // First row for this partition - the anchor map didn't carry it yet. Write both
            // flag slots explicitly rather than relying on createValue() value-byte
            // zero-fill, which MapKey.createValue() promises nowhere and OrderedMap - the
            // implementation MapFactory hands back for every key shape its unordered maps
            // do not cover, multi-column partition keys included - does not provide: its
            // clear() rewinds the heap append pointer and zeroes only the offsets table,
            // and the Unsafe.malloc / Unsafe.realloc behind that heap return whatever the
            // region already held, so a new entry can land on a departed entry's bytes.
            // The unordered maps do memset their region today, but on their own terms
            // rather than under a contract this code can lean on. A stale tombstone would
            // make the anchor snapshot drop a live partition, and a stale stamp reading as
            // the live cadence would have this row's own retry skip every mark the stamp
            // stands for.
            //
            // Ahead of the first call that can throw rather than next to the reset the
            // partition is about to take. createValue() has already published the entry, so
            // it outlives a throw from the marks below and the retry reads these two slots
            // off it - by which point isNew() answers false and says nothing about them.
            value.putByte(SLOT_TOMBSTONE, (byte) 0);
            value.putShort(SLOT_DIRTY_EPOCH, EPOCH_NONE);
        }
        // The dirty map only has to name each key once per cadence, and the anchor value
        // in hand already says whether this key was named. Skipping on a match is what
        // keeps a repeat row from serializing the partition key a second time through the
        // sink, hashing it again and probing a second map for an entry that is already
        // there. isNewPartition short-circuits ahead of the load: the block above has just
        // seeded a new entry's stamp with EPOCH_NONE, which no cadence matches, so the load
        // would only pay for an answer the flag already has. That is also what keeps the
        // sweep's eviction-and-revival path honest: eviction takes the anchor entry out, so
        // a revived key arrives new and re-enters the dirty map, clearing the eviction
        // marker it left behind.
        //
        // Every dirty set the plan leaves standing answers to this same stamp - see the
        // flag handed to markPartitionAlive below - so this one anchor-value load stands
        // in for the view's whole per-row dirty marking rather than for the anchor's
        // alone. A function whose component this window adopted keeps no set of its own
        // and marks nothing (BasePartitionedWindowFunction.markPartitionAlive returns on
        // isWindowStateOwned); a residual one keeps the set it always had. A view with R
        // residual functions used to serialize the partition key and probe a second map
        // R + 1 times per row; it now does so R + 1 times per key per cadence, and a view
        // the plan adopted whole still saves the anchor's own mark.
        //
        // What one stamp can stand for R + 1 sets is that no row is processed between a
        // function's dirty set being emptied and this counter moving on. Every path that
        // empties a function's set either moves this counter on in the same synchronous
        // block or latches that function onto a complete freeze. The seal
        // (LiveViewCheckpointTimelineStoreWriter) hands onCheckpointPersisted to the anchor
        // and to each checkpoint-capable function, and this window's own
        // clearCheckpointDirtyAnchorMap moves the counter on there. The checkpoint restore
        // (LiveViewCheckpointTimelineStoreReader) and the repair overlay
        // (LiveViewCheckpointScratchOverlay, through LiveViewFunctionSnapshot) each hand
        // every checkpoint-capable function onCheckpointRestoreBegin, which latches the full
        // scan, and rewind this window through beginCheckpointRestore / restore, which move
        // the counter on; the reader alone then lifts that latch again with
        // onCheckpointPersisted, on the anchor and on each function other than the
        // ring-shaped and scalar ones, and only when it read this generation's timeline
        // head. The head-miss replay (LiveViewRefreshJob.clearWindowState) rewinds each
        // function through toTop() and this window through toTop(), which does the same. The
        // cursor-stack toTop() empties every function's set and leaves this map, its stamps
        // and this counter standing - AnchorDispatchingCursor routes it to onCursorReopen -
        // but BasePartitionedWindowFunction.toTop() latches isCheckpointFullScanRequired and
        // drops the baseline generation there, and freezeFunction reads both before it takes
        // a dirty map, so the seal full-scans that function instead. A function rebound on
        // its own - reset() frees its dirty set without reaching this window at all -
        // latches the same flag, so the set it builds before the next onCheckpointPersisted
        // is one the seal never reads.
        final boolean isFirstCadenceTouch = isNewPartition
                || value.getShort(SLOT_DIRTY_EPOCH) != checkpointDirtyEpoch;
        if (isFirstCadenceTouch) {
            markCheckpointPartitionDirty(record, isNewPartition);
        }
        final byte initialized = isNewPartition ? 0 : value.getByte(SLOT_INITIALIZED);
        final long lastAnchor = initialized == 0 ? 0 : value.getLong(SLOT_ANCHOR_VALUE);
        final long currentAnchor = readAnchorValue(record);
        trackFrontier(currentAnchor);
        final boolean shouldReset = initialized == 0 || lastAnchor != currentAnchor;

        // The branch takes a row that crossed to a new anchor value, and - through
        // initialized == 0 - every new partition. Resetting a new partition is deliberate:
        // its functions either have no per-partition state yet (in which case
        // resetPartition is a no-op) or carry stale state from a prior partition the sweep
        // evicted, and resetting it is the safe default.
        if (shouldReset) {
            // Grouped functions no-op here; their component is zeroed in the loaded
            // value instead. Residual ones keep the dispatch they have always had.
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).resetPartition(record);
            }
            resetWindowStateComponents(value);
            movePartitionToCurrentBucket(initialized == 0, lastAnchor);
            value.putLong(SLOT_ANCHOR_VALUE, currentAnchor);
            value.putByte(SLOT_INITIALIZED, (byte) 1);
        }

        updateWindowState(record, value);

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
            functions.getQuick(i).markPartitionAlive(record, isFirstCadenceTouch);
        }

        // The stamp goes in last, once every mark the flag stood for has been made rather
        // than before the loop that makes them. A function's dirty set is allocated on
        // first use through the per-view tracker, so the mark can throw on a breach of
        // cairo.live.view.refresh.memory.limit.bytes; a stamp already standing would have
        // the retry read this row as marked and leave that function's set one key short of
        // what its rows moved - which the seal cannot tell from a complete set. A key
        // created by this row leans on the same ordering through the EPOCH_NONE its own
        // block above seeded: without it the retry would read whatever the heap held.
        // Nothing between the load above and here touches the anchor map, so the handle
        // still addresses this key's entry.
        if (isFirstCadenceTouch) {
            value.putShort(SLOT_DIRTY_EPOCH, checkpointDirtyEpoch);
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
        final int storedComponentStateBytes = source.getInt(offset);
        offset += Integer.BYTES;
        final int componentStateBytes = overlayComponentStateBytes();
        if (storedComponentStateBytes != componentStateBytes) {
            throw CairoException.nonCritical()
                    .put("live view checkpoint anchor block component state width mismatch [expected=")
                    .put(componentStateBytes)
                    .put(", got=")
                    .put(storedComponentStateBytes)
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
            if (remainingBytes < 0 || partitionCount > remainingBytes / (Long.BYTES + componentStateBytes)) {
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
        final byte[] image = componentStateBytes > 0 ? new byte[componentStateBytes] : null;
        for (long i = 0; i < partitionCount; i++) {
            MapKey key = anchorMap.withKey();
            offset = LiveViewSnapshotKeyCodec.readKey(key, source, offset, partitionKeyTypes);
            MapValue value = key.createValue();
            long restoredAnchor = source.getLong(offset);
            value.putLong(SLOT_ANCHOR_VALUE, restoredAnchor);
            value.putByte(SLOT_INITIALIZED, (byte) 1);
            value.putByte(SLOT_TOMBSTONE, (byte) 0);
            value.putShort(SLOT_DIRTY_EPOCH, EPOCH_NONE);
            restoreFrontierEntry(restoredAnchor);
            offset += Long.BYTES;
            if (image != null) {
                for (int b = 0; b < componentStateBytes; b++) {
                    image[b] = source.getByte(offset + b);
                }
                offset += componentStateBytes;
                restoreWindowStateRuntimeImage(image, value);
            }
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
        value.putShort(SLOT_DIRTY_EPOCH, EPOCH_NONE);
        // A legacy anchor root carries no components, so the group's slots start at
        // identity and the per-function roots restored after it fill them in. Writing
        // them explicitly is what keeps a fresh map value's uninitialized bytes from
        // being read as an accumulator.
        resetWindowStateComponents(value);
        restoreFrontierEntry(anchorValue);
    }

    /**
     * Rehydrates one fused entry read from a window-state root's leaf: the anchor value
     * and every grouped component, out of one payload and into one map value.
     * <p>
     * {@code keySource} is the entry's encoded partition key, bounded to its exact
     * length; the decoder must consume all of it. {@code payload} is the entry's scalar
     * state, already proved to be exactly the manifest's width by
     * {@link LiveViewCheckpointWindowRoot#readWindowState}.
     * <p>
     * Callers restore a complete root, so {@link #beginCheckpointRestore()} must precede
     * the first entry.
     */
    public void restoreCheckpointWindowEntry(@NotNull LiveViewStatePageReader keySource, byte @NotNull [] payload) {
        final LiveViewWindowStatePlan plan = checkpointWindowStatePlan;
        if (plan == null) {
            throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                    .put("live view checkpoint window state restore without an adopted plan");
        }
        final MapKey key = anchorMap.withKey();
        final long consumed = LiveViewSnapshotKeyCodec.readKey(key, keySource, 0, partitionKeyTypes);
        if (consumed != keySource.size()) {
            throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                    .put("live view checkpoint window state key decoder did not consume the entry exactly [expected=")
                    .put(keySource.size()).put(", consumed=").put(consumed).put(']');
        }
        final MapValue value = key.createValue();
        if (!value.isNew()) {
            throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                    .put("live view checkpoint window state contains a duplicate partition key");
        }
        final long anchorValue = LiveViewCheckpointWindowRoot.readAnchorValue(payload);
        value.putLong(SLOT_ANCHOR_VALUE, anchorValue);
        value.putByte(SLOT_INITIALIZED, (byte) 1);
        value.putByte(SLOT_TOMBSTONE, (byte) 0);
        value.putShort(SLOT_DIRTY_EPOCH, EPOCH_NONE);
        final LiveViewWindowStateManifest manifest = plan.getManifest();
        final int durableComponentCount = plan.getDurableComponentCount();
        for (int c = 0; c < durableComponentCount; c++) {
            plan.getComponent(c).restoreStateFrom(
                    payload,
                    manifest.getComponentStateOffset(c),
                    value,
                    plan.getComponentSlotBase(c)
            );
        }
        // A runtime-only member's bytes are on its own function root, which is restored
        // after this walk has created the entry it writes into. Identity here rather than
        // whatever the map's backing held: an entry the member's root turns out not to name
        // is one whose accumulator is empty, and reading uninitialized slots as state is the
        // one way that could go unnoticed.
        for (int c = durableComponentCount, n = plan.getComponentCount(); c < n; c++) {
            plan.getComponent(c).resetState(value, plan.getComponentSlotBase(c));
        }
        restoreFrontierEntry(anchorValue);
    }

    /**
     * Rehydrates one entry of a runtime-only member's own function root into the group's
     * map value - the restore half of {@link #freezeCheckpointMemberEntries}.
     * <p>
     * The window-state root is restored first and created the entry this writes into, so a
     * key the member's root names and the window's does not is a disagreement between two
     * roots of one boundary rather than a key to insert: inserting would add an entry the
     * fused walk decided was not live, with an anchor value nothing wrote.
     * <p>
     * Only the component's {@link LiveViewWindowStatePlan#isContributor(int) contributor}
     * writes. Every other member reading that component holds a root of its own - a derived
     * {@code count}'s eight bytes, a guarded one's corrected number - and neither is the
     * component's state; the contributor's image is, and restoring it restores every output
     * that reads it. That is the same rule {@code endLegacyComponentRestore} applies when it
     * hoists a legacy root, one root shape later.
     */
    public void restoreCheckpointMemberEntry(
            int projectionIndex,
            @NotNull LiveViewStatePageReader keySource,
            byte @NotNull [] image
    ) {
        final LiveViewWindowStatePlan plan = checkpointWindowStatePlan;
        if (plan == null) {
            throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                    .put("live view checkpoint member restore without an adopted plan");
        }
        if (!plan.isContributor(projectionIndex)) {
            return;
        }
        final MapKey key = anchorMap.withKey();
        final long consumed = LiveViewSnapshotKeyCodec.readKey(key, keySource, 0, partitionKeyTypes);
        if (consumed != keySource.size()) {
            throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                    .put("live view checkpoint member key decoder did not consume the entry exactly [expected=")
                    .put(keySource.size()).put(", consumed=").put(consumed).put(']');
        }
        final MapValue value = key.findValue();
        if (value == null) {
            throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                    .put("live view checkpoint member root holds a key the window state root does not");
        }
        final LiveViewAccumulatorProjection projection = plan.getProjection(projectionIndex);
        projection.getFunctionComponent().restoreStateFrom(
                image,
                0,
                value,
                projection.getFunctionSlotBase()
        );
    }

    /**
     * Opens the grouped functions' private maps so a legacy per-function root can be
     * restored into them, and reports whether anything needs it.
     * <p>
     * This is the upgrade adapter's first half. A checkpoint written before the fused
     * root existed holds one root per function, and the shortest correct way to read it
     * into a fused runtime is to let each function's own restore run exactly as it
     * always has and then hoist the result - rather than teach every decoder a second
     * destination.
     *
     * @return true when the window owns a group, and so the caller must pair this with
     * {@link #endLegacyComponentRestore()}
     */
    public boolean beginLegacyComponentRestore() {
        final LiveViewWindowStatePlan plan = checkpointWindowStatePlan;
        if (plan == null) {
            return false;
        }
        plan.reopenProjectionMaps();
        return true;
    }

    /**
     * Copies every grouped component out of the private maps a legacy restore just
     * filled and into the fused value, then closes those maps again. The second half of
     * {@link #beginLegacyComponentRestore()}.
     */
    public void endLegacyComponentRestore() {
        final LiveViewWindowStatePlan plan = checkpointWindowStatePlan;
        if (plan == null) {
            return;
        }
        try {
            final MapRecordCursor cursor = anchorMap.getCursor();
            final MapRecord record = anchorMap.getRecord();
            while (cursor.hasNext()) {
                final MapValue value = record.getValue();
                for (int c = 0, n = plan.getComponentCount(); c < n; c++) {
                    hoistComponentInto(plan, c, record, activeKeySink, value);
                }
            }
        } finally {
            plan.releaseProjectionMaps();
        }
    }

    /**
     * Serialises the anchor map's live entries (tombstoned entries are
     * skipped) into {@code sink}. {@link LiveViewCheckpointScratchOverlay} calls
     * this to take the published anchor state aside before a localized repair
     * replays over it, and {@link #restore(MemoryR, long, long)} reads the same
     * payload back.
     * <p>
     * Once the window owns a fused group, the grouped functions have no state of their
     * own for the overlay to capture, so their accumulators travel here too - which is
     * what "capture the window state once" means with runtime fusion. The component bytes
     * are <b>every</b> component the group carries, durable or runtime-only, in the plan's
     * canonical order, and a width of zero is a window that adopted no plan. All of them
     * rather than the leaf's prefix, because a repair replays over the runtime and consults
     * no function root: a runtime-only member's accumulator is in this map value and
     * nowhere else the overlay can reach.
     * <p>
     * Payload shape:
     * <pre>
     *   windowName: STR
     *   partitionKeyColumnCount: INT
     *   per key column: columnType: INT
     *   anchorValueType: INT
     *   componentStateBytes: INT      (0 when no plan is adopted)
     *   partitionCount: LONG          (live entries only)
     *   per partition:
     *     per key column: keyValue    (LiveViewSnapshotKeyCodec)
     *     lastAnchorValue: LONG
     *     componentState: componentStateBytes bytes
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
        final int componentStateBytes = overlayComponentStateBytes();
        sink.putInt(componentStateBytes);
        final long liveCount = anchorMap.size() - tombstoneCount;
        sink.putLong(liveCount);

        // MapRecord column layout is [value0, value1, value2, value3, key0, ..., keyN-1] - keys
        // sit after the four value slots (anchor LONG, initialized BYTE, tombstone BYTE,
        // dirty-cadence SHORT).
        // The codec needs the key-start index to address them via record.getXxx(columnIndex).
        final int keyStartIndex = activeKeyStartIndex;
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
            if (componentStateBytes > 0) {
                final byte[] image = encodeWindowStateRuntimeImage(value, componentStateBytes);
                for (int i = 0; i < componentStateBytes; i++) {
                    sink.putByte(image[i]);
                }
            }
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
     * Wired into {@link #processRow(Record)} via {@link #maybeCompact()} once the anchor
     * advances past a bucket boundary, the map exceeds
     * {@code cairo.live.view.partition.compact.threshold} and the stale partitions are
     * both that many and {@code cairo.live.view.partition.compact.stale.percent} of the
     * map. Also directly callable from tests.
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
            // the same per-view tracker. It carries the live value layout, which adoption
            // and decline both drop it over.
            scratchAnchorMap = createTrackedAnchorMap(
                    cairoConfiguration,
                    partitionKeyTypes,
                    activeValueTypes(),
                    memoryTracker
            );
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
                                functions.getQuick(i).markCheckpointPartitionEvicted(record, activeKeySink)
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
                    activeKeySink,
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
     * Rebuilds the anchor map under the fused value layout, carrying every live entry's
     * anchor value and every grouped function's accumulator into it, then closes the
     * private maps the group no longer writes to.
     * <p>
     * The copy probes each component's contributor through {@link #anchorKeySink} - the
     * narrow layout's, since that is the map being read - because the two maps may be
     * different {@link Map} implementations and the sink writes through per-column
     * putters rather than casting to either. A key the contributor does not hold takes
     * the identity state: outside a fused group a function creates its map entry lazily
     * on the row that first contributes, so an anchor key with no entry is one whose
     * accumulator is empty rather than one whose state went missing.
     */
    private void adoptWindowStatePlan(@NotNull LiveViewWindowStatePlan plan) {
        assert fusedValueTypes != null && fusedKeySink != null;
        final Map fused = createTrackedAnchorMap(
                cairoConfiguration,
                partitionKeyTypes,
                fusedValueTypes,
                memoryTracker
        );
        try {
            final MapRecordCursor cursor = anchorMap.getCursor();
            final MapRecord record = anchorMap.getRecord();
            while (cursor.hasNext()) {
                final MapValue src = record.getValue();
                final MapKey dstKey = fused.withKey();
                dstKey.put(record, anchorKeySink);
                final MapValue dst = dstKey.createValue();
                dst.putLong(SLOT_ANCHOR_VALUE, src.getLong(SLOT_ANCHOR_VALUE));
                dst.putByte(SLOT_INITIALIZED, src.getByte(SLOT_INITIALIZED));
                dst.putByte(SLOT_TOMBSTONE, src.getByte(SLOT_TOMBSTONE));
                // Carried rather than reset: the rebuild does not empty the dirty map, so
                // a key already marked in this cadence is still marked after it. The
                // caller's clear moves the cadence on afterwards either way, which is what
                // makes both answers safe - but a fresh value's slot is written by nobody
                // and would otherwise be read as whatever the backing held.
                dst.putShort(SLOT_DIRTY_EPOCH, src.getShort(SLOT_DIRTY_EPOCH));
                for (int c = 0, n = plan.getComponentCount(); c < n; c++) {
                    hoistComponentInto(plan, c, record, anchorKeySink, dst);
                }
            }
        } catch (Throwable t) {
            Misc.free(fused);
            throw t;
        }
        Misc.free(anchorMap);
        anchorMap = fused;
        // The sweep's second map carries the value layout that just changed, so it goes
        // back to the allocator rather than being reused against a different shape.
        scratchAnchorMap = Misc.free(scratchAnchorMap);
        checkpointWindowStatePlan = plan;
        activeKeySink = fusedKeySink;
        activeKeyStartIndex = fusedValueTypes.getColumnCount();
        plan.bindProjectionFunctions();
        plan.releaseProjectionMaps();
        // The keys a member touches are the group's dirty set's from here, so whatever
        // baseline its own root carried is against a set that stops moving. See
        // LiveViewWindowStatePlan.requireProjectionCheckpointFullScan.
        plan.requireProjectionCheckpointFullScan();
    }

    /**
     * Rebuilds the anchor map under the narrow value layout and hands each grouped
     * function's accumulator back to the private map it owns outside a group. The exact
     * inverse of {@link #adoptWindowStatePlan}.
     * <p>
     * A projection takes back the slice its own state is, which is the whole component
     * only while the projection is not derived: a {@code count} folded onto a sum's
     * counter reads that one slot and nothing else, exactly as its restore does.
     */
    private void declineWindowStatePlan() {
        final LiveViewWindowStatePlan plan = checkpointWindowStatePlan;
        if (plan == null) {
            return;
        }
        final Map narrow = createTrackedAnchorMap(
                cairoConfiguration,
                partitionKeyTypes,
                AnchorMapValueTypes.INSTANCE,
                memoryTracker
        );
        try {
            plan.reopenProjectionMaps();
            final MapRecordCursor cursor = anchorMap.getCursor();
            final MapRecord record = anchorMap.getRecord();
            while (cursor.hasNext()) {
                final MapValue src = record.getValue();
                final MapKey dstKey = narrow.withKey();
                dstKey.put(record, fusedKeySink);
                final MapValue dst = dstKey.createValue();
                dst.putLong(SLOT_ANCHOR_VALUE, src.getLong(SLOT_ANCHOR_VALUE));
                dst.putByte(SLOT_INITIALIZED, src.getByte(SLOT_INITIALIZED));
                dst.putByte(SLOT_TOMBSTONE, src.getByte(SLOT_TOMBSTONE));
                // Carried rather than reset: the rebuild does not empty the dirty map, so
                // a key already marked in this cadence is still marked after it. The
                // caller's clear moves the cadence on afterwards either way, which is what
                // makes both answers safe - but a fresh value's slot is written by nobody
                // and would otherwise be read as whatever the backing held.
                dst.putShort(SLOT_DIRTY_EPOCH, src.getShort(SLOT_DIRTY_EPOCH));
                for (int p = 0, n = plan.getProjectionCount(); p < n; p++) {
                    lowerProjectionInto(plan, p, record, src);
                }
            }
        } catch (Throwable t) {
            Misc.free(narrow);
            throw t;
        }
        Misc.free(anchorMap);
        anchorMap = narrow;
        scratchAnchorMap = Misc.free(scratchAnchorMap);
        plan.unbindProjectionFunctions();
        // And the other direction of the same thing: a runtime-only member takes its root
        // back with a baseline the group's dirty set was feeding, and an incremental seal
        // on top of it would name only the keys touched after this point.
        plan.requireProjectionCheckpointFullScan();
        checkpointWindowStatePlan = null;
        activeKeySink = anchorKeySink;
        activeKeyStartIndex = AnchorMapValueTypes.INSTANCE.getColumnCount();
    }

    /**
     * Copies one component's accumulator out of its contributor's private map and into
     * the fused value's slots. {@code keySink} reads the partition key off
     * {@code srcRecord}, which is a record of whichever map the caller is walking.
     */
    private void hoistComponentInto(
            @NotNull LiveViewWindowStatePlan plan,
            int componentIndex,
            @NotNull MapRecord srcRecord,
            @NotNull RecordSink keySink,
            @NotNull MapValue dst
    ) {
        final LiveViewAccumulatorDescriptor component = plan.getComponent(componentIndex);
        final int slotBase = plan.getComponentSlotBase(componentIndex);
        final Map map = plan.getContributor(componentIndex).getPartitionMap();
        MapValue src = null;
        if (map != null && map.isOpen()) {
            final MapKey key = map.withKey();
            key.put(srcRecord, keySink);
            src = key.findValue();
        }
        if (src == null) {
            component.resetState(dst, slotBase);
            return;
        }
        // A private partition map lays the component's fields out from slot 0 - that
        // equality is the same one the durable image rests on, and the plan already
        // required the contributor's declared width to be the family's.
        component.copyState(src, 0, dst, slotBase);
    }

    /**
     * Whether the partition key of the entry {@code record} is positioned on is present,
     * which for a guarded {@code count(k)} is exactly whether that call counts its
     * partition's rows or none of them.
     * <p>
     * The key rather than the argument, because this walk has no base row to evaluate the
     * argument against. The two agree by construction: the compiler admits the guarded
     * form only for a SYMBOL or VARCHAR argument, whose {@code count} contributes on a
     * plain null test, and a SYMBOL partition column reaches the map as its resolved
     * STRING - null for the null symbol. That agreement is what the type check below
     * holds the two sides to; anything else means they have drifted apart, and reading
     * some other type's null sentinel would silently answer for a different set of rows.
     */
    private boolean isPartitionKeyPresent(@NotNull MapRecord record) {
        return isPartitionKeyPresent(record, activeKeyStartIndex);
    }

    /**
     * As {@link #isPartitionKeyPresent(MapRecord)}, for a walk positioned on a map whose
     * value layout is not the anchor map's - the dirty set's, whose key tail starts after
     * its own two marker bytes.
     */
    private boolean isPartitionKeyPresent(@NotNull MapRecord record, int keyIndex) {
        switch (ColumnType.tagOf(partitionKeyTypes.getColumnType(0))) {
            case ColumnType.STRING:
                return record.getStrA(keyIndex) != null;
            case ColumnType.VARCHAR:
                return record.getVarcharA(keyIndex) != null;
            default:
                throw CairoException.critical(0)
                        .put("live view window state cannot test a partition key of this type [type=")
                        .put(ColumnType.nameOf(partitionKeyTypes.getColumnType(0))).put(']');
        }
    }

    /**
     * Writes one projection's own accumulator back into the private map its function
     * owns outside a fused group, taking it from {@code fusedValue}'s slots.
     * <p>
     * A {@link LiveViewAccumulatorProjection#isPartitionKeyGuarded() guarded} count is
     * the one projection whose own state is not the slots it reads: it emits
     * {@code partition-key-is-null ? 0 : rowCount}, so copying the row count into its map
     * would leave the NULL-key partition counting rows it never counted. The guard is
     * applied here from the entry's own key, which is where that key is available - the
     * function reads it off a base row and this walk has none.
     */
    private void lowerProjectionInto(
            @NotNull LiveViewWindowStatePlan plan,
            int projectionIndex,
            @NotNull MapRecord fusedRecord,
            @NotNull MapValue fusedValue
    ) {
        final LiveViewAccumulatorProjection projection = plan.getProjection(projectionIndex);
        final WindowFunction function = plan.getProjectionFunction(projectionIndex);
        final Map map = function.getPartitionMap();
        if (map == null || !map.isOpen()) {
            return;
        }
        final MapKey key = map.withKey();
        key.put(fusedRecord, fusedKeySink);
        final MapValue value = key.createValue();
        if (projection.isPartitionKeyGuarded()) {
            value.putLong(
                    0,
                    isPartitionKeyPresent(fusedRecord)
                            ? fusedValue.getLong(projection.getNonNullCountSlot())
                            : 0L
            );
        } else {
            projection.getFunctionComponent().copyState(
                    fusedValue,
                    projection.getFunctionSlotBase(),
                    value,
                    0
            );
        }
        final int tombstoneIndex = function.getTombstoneValueIndex();
        if (tombstoneIndex >= 0) {
            value.putByte(tombstoneIndex, (byte) 0);
        }
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
        // Ahead of the null guard rather than behind it. A live stamp does imply a
        // non-null dirty map today - the only writer of one is preceded by the mark that
        // creates the map, and every restore path writes EPOCH_NONE - so the guard would
        // not skip a bump that matters. It is placed here anyway because what it protects
        // is severe and silent: an emptied dirty set that left stamps standing has later
        // rows skip a mark the map no longer holds, and the next incremental seal
        // publishes a root missing those keys.
        advanceCheckpointDirtyEpoch();
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
     * Moves the dirty set on to a cadence no anchor entry has been stamped with, so every
     * stamp the anchor map still holds stops matching in one store rather than by walking
     * the map.
     * <p>
     * A SHORT counter wraps, and on the wrap the stamps left over from 32766 cadences ago
     * would start matching again - which would have later rows skip a mark the dirty set
     * no longer holds, and an incremental seal publish a root missing those keys. That is
     * what the scan is for, and it is the only place the anchor map is walked for this.
     * <p>
     * The wrap is not remote. A seal fires on the row cadence <b>or</b> on
     * {@code cairo.live.view.checkpoint.max.duration.micros}, five minutes by default, so
     * a quiet view turns the counter over in about 114 days and one sealing every ten
     * seconds in under four. It is reachable in the field rather than in theory, which is
     * why {@link #setCheckpointDirtyEpoch(short)} exists to reach it in a test.
     */
    private void advanceCheckpointDirtyEpoch() {
        if (checkpointDirtyEpoch == Short.MAX_VALUE) {
            checkpointDirtyEpoch = 1;
            if (anchorMap.isOpen()) {
                final MapRecordCursor cursor = anchorMap.getCursor();
                final MapRecord record = anchorMap.getRecord();
                while (cursor.hasNext()) {
                    record.getValue().putShort(SLOT_DIRTY_EPOCH, EPOCH_NONE);
                }
            }
            return;
        }
        checkpointDirtyEpoch++;
    }

    /**
     * Adds one partition key to the checkpoint dirty set and records whether it was new
     * relative to the last durable checkpoint. The marker keeps logical-size accounting
     * exact without probing the persistent anchor root.
     * <p>
     * Reached once per key per cadence rather than once per row - see the epoch test in
     * {@link #processRow} - so what it costs scales with the key domain the cadence
     * touches rather than with the rows it processes.
     */
    private void markCheckpointPartitionDirty(Record record, boolean isNewPartition) {
        checkpointDirtyMarkCount++;
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
        key.put(record, activeKeySink);
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
        // Integer arithmetic on both sides rather than a rounded entry count: at the 50
        // default this is exactly the ceil(mapSize / 2) the arm has always used, and every
        // other setting stays exact too. mapSize is bounded by the anchor map's entry
        // count, so the multiply cannot overflow a long.
        if (compactionViable
                && prevFrontier != Long.MIN_VALUE
                && maxAnchorValue > lastCompactedFrontier
                && mapSize > compactThreshold
                && stalePartitionCount >= compactThreshold
                && stalePartitionCount * 100L >= mapSize * compactStalePercent) {
            compact();
        }
    }

    /**
     * Returns the value layout the anchor map currently carries.
     */
    private ColumnTypes activeValueTypes() {
        return fusedValueTypes != null && checkpointWindowStatePlan != null
                ? fusedValueTypes
                : AnchorMapValueTypes.INSTANCE;
    }

    /**
     * Builds one entry's fused scalar payload - the anchor value, then every <b>durable</b>
     * component at the manifest's offset - out of the map value the walk already holds.
     * <p>
     * The runtime-only members the leaf budget left out are deliberately absent: the leaf
     * carries the manifest's components and nothing else, and their bytes go to the function
     * roots they keep. Their slots are still in the value this reads, which is what lets
     * both images come off one loaded entry.
     */
    private byte[] encodeWindowStatePayload(MapValue value, int entryStateBytes) {
        final LiveViewWindowStatePlan plan = checkpointWindowStatePlan;
        assert plan != null;
        if (entryStateBytes != plan.getTotalInlineStateBytes()) {
            throw CairoException.critical(0)
                    .put("live view checkpoint window state payload width does not match the plan [expected=")
                    .put(plan.getTotalInlineStateBytes()).put(", requested=").put(entryStateBytes).put(']');
        }
        final byte[] payload = new byte[entryStateBytes];
        LiveViewCheckpointWindowRoot.encodeAnchorValue(value.getLong(SLOT_ANCHOR_VALUE), payload);
        final LiveViewWindowStateManifest manifest = plan.getManifest();
        for (int c = 0, n = plan.getDurableComponentCount(); c < n; c++) {
            plan.getComponent(c).freezeStateInto(
                    value,
                    plan.getComponentSlotBase(c),
                    payload,
                    manifest.getComponentStateOffset(c)
            );
        }
        return payload;
    }

    /**
     * The width of one entry's whole component image in the repair overlay's payload -
     * every component the group carries, or zero for a window that adopted no plan.
     * <p>
     * It equals the fused payload's width minus the anchor value whenever the leaf carries
     * the whole group, which is every shape but an overflowing one, and the bytes are the
     * same bytes: the manifest's offsets accumulate the same widths in the same order this
     * image does.
     */
    private int overlayComponentStateBytes() {
        return checkpointWindowStatePlan == null ? 0 : checkpointWindowStatePlan.getTotalRuntimeStateBytes();
    }

    /**
     * Builds one entry's whole component image - every component in the plan's canonical
     * order, at cumulative offsets - out of the map value the walk already holds. The
     * overlay's counterpart of {@link #encodeWindowStatePayload}, which stops at the leaf's
     * prefix.
     */
    private byte[] encodeWindowStateRuntimeImage(MapValue value, int componentStateBytes) {
        final LiveViewWindowStatePlan plan = checkpointWindowStatePlan;
        assert plan != null;
        final byte[] image = new byte[componentStateBytes];
        int offset = 0;
        for (int c = 0, n = plan.getComponentCount(); c < n; c++) {
            final LiveViewAccumulatorDescriptor component = plan.getComponent(c);
            component.freezeStateInto(value, plan.getComponentSlotBase(c), image, offset);
            offset += component.getStateLength();
        }
        return image;
    }

    /**
     * Builds one runtime-only member's own whole-state image out of the group's map value -
     * its function slice, through the component codec, which is byte for byte what the
     * function's own {@code freezeCheckpointState} writes from a private map value.
     * <p>
     * A {@link LiveViewAccumulatorProjection#isPartitionKeyGuarded() guarded} count is the
     * one member whose own image is not its slice: it emits
     * {@code partition-key-is-null ? 0 : rowCount}, exactly as {@link #lowerProjectionInto}
     * writes it back into a private map, and the guard is applied here from the entry's own
     * key because this walk has no base row to evaluate the argument against. A
     * NULL-key partition's image is the eight zero bytes a fresh array already holds.
     *
     * @param keyRecord     the record of whichever map the walk is on - the anchor map's
     *                      for a complete freeze, the dirty set's for an incremental one
     * @param keyStartIndex where that record's key columns begin
     */
    private byte[] encodeMemberStateImage(
            int projectionIndex,
            MapValue value,
            MapRecord keyRecord,
            int keyStartIndex,
            int stateBytes
    ) {
        final LiveViewWindowStatePlan plan = checkpointWindowStatePlan;
        assert plan != null;
        final LiveViewAccumulatorProjection projection = plan.getProjection(projectionIndex);
        final byte[] image = new byte[stateBytes];
        if (projection.isPartitionKeyGuarded()) {
            if (isPartitionKeyPresent(keyRecord, keyStartIndex)) {
                final long count = value.getLong(projection.getNonNullCountSlot());
                for (int i = 0; i < Long.BYTES; i++) {
                    image[i] = (byte) (count >>> (i * Byte.SIZE));
                }
            }
            return image;
        }
        projection.getFunctionComponent().freezeStateInto(
                value,
                projection.getFunctionSlotBase(),
                image,
                0
        );
        return image;
    }

    /**
     * Puts every component of one entry back from an overlay image. The exact inverse of
     * {@link #encodeWindowStateRuntimeImage}.
     */
    private void restoreWindowStateRuntimeImage(byte @NotNull [] image, MapValue value) {
        final LiveViewWindowStatePlan plan = checkpointWindowStatePlan;
        assert plan != null;
        int offset = 0;
        for (int c = 0, n = plan.getComponentCount(); c < n; c++) {
            final LiveViewAccumulatorDescriptor component = plan.getComponent(c);
            component.restoreStateFrom(image, offset, value, plan.getComponentSlotBase(c));
            offset += component.getStateLength();
        }
    }

    /**
     * Puts every grouped component in {@code value} back to identity on an anchor
     * crossing, and on the first row of a partition - a fresh map value's slots are not
     * zero-filled by any implementation. A no-op for a view that adopted no plan.
     */
    private void resetWindowStateComponents(MapValue value) {
        final LiveViewWindowStatePlan plan = checkpointWindowStatePlan;
        if (plan == null) {
            return;
        }
        for (int c = 0, n = plan.getComponentCount(); c < n; c++) {
            plan.getComponent(c).resetState(value, plan.getComponentSlotBase(c));
        }
    }

    /**
     * Absorbs the row into every grouped component - once per component, through the
     * contributor the plan chose - and then materializes each output's value from the
     * updated slots. A no-op for a view that adopted no plan.
     * <p>
     * The two loops are separate on purpose: several projections may read one component,
     * and one of them updating it while another has already read would make the outputs
     * depend on the order the SELECT list happens to be in.
     */
    private void updateWindowState(Record record, MapValue value) {
        final LiveViewWindowStatePlan plan = checkpointWindowStatePlan;
        if (plan == null) {
            return;
        }
        for (int c = 0, n = plan.getComponentCount(); c < n; c++) {
            plan.getContributor(c).accumulateWindowState(record, value);
        }
        for (int p = 0, n = plan.getProjectionCount(); p < n; p++) {
            plan.getProjectionFunction(p).projectWindowState(record, value);
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
            // Read back from the plan rather than restated, so the slot the components
            // start at and the slots the window keeps are one decision.
            return LiveViewWindowStatePlan.WINDOW_VALUE_SLOT_COUNT;
        }

        @Override
        public int getColumnType(int columnIndex) {
            return switch (columnIndex) {
                case SLOT_ANCHOR_VALUE -> ColumnType.LONG;
                case SLOT_INITIALIZED -> ColumnType.BYTE;
                case SLOT_TOMBSTONE -> ColumnType.BYTE;
                case SLOT_DIRTY_EPOCH -> ColumnType.SHORT;
                default -> throw new IndexOutOfBoundsException();
            };
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
