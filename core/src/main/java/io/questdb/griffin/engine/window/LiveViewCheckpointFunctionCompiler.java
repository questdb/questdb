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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.lv.LiveViewAccumulatorDescriptor;
import io.questdb.cairo.lv.LiveViewCheckpointAnchorPlan;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.DependencyKind;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointDependency.NumericConvergence;
import io.questdb.cairo.lv.LiveViewCheckpointDependency.StructuralConvergence;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointKeyProjector;
import io.questdb.cairo.lv.LiveViewCheckpointRangePlan;
import io.questdb.cairo.lv.LiveViewCheckpointRowsPlan;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewWindowStatePlan;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.date.TimestampFloorFromOffsetUtcFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampFloorFunctionFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.BitSet;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Compiler-only builder for stable live-view checkpoint function metadata.
 */
public final class LiveViewCheckpointFunctionCompiler {
    private static final String STATE_PAGE_CODEC_FAMILY = "live-view-state-page";
    // The UTC offset desugarDailyAnchor emits beside the zone name; any other value is a
    // hand-written anchor this does not describe.
    private static final String UTC_ZERO_OFFSET = "+00:00";

    private LiveViewCheckpointFunctionCompiler() {
    }

    /**
     * Builds the fixed segment boundary an anchored live view resets on, or null when the anchor
     * has none or does not govern the whole view. It reads the anchor expression rather than any
     * frame and recognizes a calendar-period floor of the designated timestamp in three forms:
     * <ul>
     *     <li>{@code timestamp_floor('<stride><unit>', ts)} - a hand-written
     *     {@code ANCHOR EXPRESSION}, or {@code ANCHOR DAILY} at UTC midnight, on epoch-aligned
     *     buckets;</li>
     *     <li>{@code timestamp_floor('1d', ts, <origin>)} - {@code ANCHOR DAILY} at any other time
     *     of day;</li>
     *     <li>{@code timestamp_floor_utc('1d', ts, <origin>, '+00:00', '<zone>')} -
     *     {@code ANCHOR DAILY} with an IANA time zone, whose buckets
     *     {@link LiveViewCheckpointAnchorPlan#ofTimeZone} follows across DST transitions.</li>
     * </ul>
     * The last two carry their origin as a cast expression, so it is read from the definition's
     * {@code anchorDailyTimeUs} instead, which is why they are accepted only for a DAILY anchor.
     * Everything else declines, which costs the view only its localized repair path.
     * <p>
     * The plan is withheld unless {@link #isAnchorSegmentLocal} confirms the anchor resets every
     * anchored window function. A bounded ROWS/RANGE window beside the anchored one is
     * {@link #rowsPlan}'s or {@link #rangePlan}'s to bound, and {@link #isDependencyComplete}
     * proves the plans between them cover the factory.
     *
     * @param anchorNode                the parsed anchor expression, already desugared for a
     *                                  DAILY anchor
     * @param projectedMetadata         the metadata the runtime anchor function is compiled
     *                                  against
     * @param anchorableWindowFunctions the subset the anchor dispatches {@code resetPartition} to
     */
    public static @Nullable LiveViewCheckpointAnchorPlan anchorPlan(
            @NotNull LiveViewDefinition.LvAnchorSpec spec,
            @Nullable ExpressionNode anchorNode,
            @NotNull RecordMetadata projectedMetadata,
            @NotNull ObjList<WindowFunction> windowFunctions,
            @Nullable ObjList<WindowFunction> anchorableWindowFunctions
    ) {
        final int timestampIndex = projectedMetadata.getTimestampIndex();
        if (timestampIndex == -1 || anchorNode == null || anchorNode.type != ExpressionNode.FUNCTION
                || anchorNode.token == null
                || !isAnchorSegmentLocal(windowFunctions, anchorableWindowFunctions)) {
            return null;
        }
        final boolean isZonedFloor =
                Chars.equalsIgnoreCase(anchorNode.token, TimestampFloorFromOffsetUtcFunctionFactory.NAME);
        if (!isZonedFloor && !Chars.equalsIgnoreCase(anchorNode.token, TimestampFloorFunctionFactory.NAME)) {
            return null;
        }
        final ExpressionNode unitNode;
        final ExpressionNode timestampNode;
        final long segmentOffset;
        final CharSequence timeZone;
        final int timestampType = projectedMetadata.getTimestampType();
        if (!isZonedFloor && anchorNode.paramCount == 2) {
            // Two children live in lhs/rhs, in the order they were written.
            unitNode = anchorNode.lhs;
            timestampNode = anchorNode.rhs;
            segmentOffset = 0;
            timeZone = null;
        } else if (!isZonedFloor && anchorNode.paramCount == 3 && anchorNode.args.size() == 3
                && spec.anchorKind == WindowExpression.ANCHOR_KIND_DAILY) {
            // Three or more children live in args, inverted, so the first written
            // argument is the last entry.
            unitNode = anchorNode.args.getQuick(2);
            timestampNode = anchorNode.args.getQuick(1);
            if (!ColumnType.isTimestamp(timestampType)) {
                return null;
            }
            segmentOffset = dailyAnchorOrigin(spec, timestampType);
            timeZone = null;
        } else if (isZonedFloor && anchorNode.paramCount == 5 && anchorNode.args.size() == 5
                && spec.anchorKind == WindowExpression.ANCHOR_KIND_DAILY
                && spec.anchorDailyTimeZone != null) {
            // The same inverted args, two entries deeper: unit, ts, origin, offset, zone.
            unitNode = anchorNode.args.getQuick(4);
            timestampNode = anchorNode.args.getQuick(3);
            // A hand-written ANCHOR EXPRESSION could name another zone or offset, and a plan
            // ignoring them would describe a grid the runtime does not use.
            if (!ColumnType.isTimestamp(timestampType)
                    || !isQuotedConstant(anchorNode.args.getQuick(1), UTC_ZERO_OFFSET)
                    || !isQuotedConstant(anchorNode.args.getQuick(0), spec.anchorDailyTimeZone)) {
                return null;
            }
            segmentOffset = dailyAnchorOrigin(spec, timestampType);
            timeZone = spec.anchorDailyTimeZone;
        } else {
            return null;
        }
        if (timestampNode == null || timestampNode.type != ExpressionNode.LITERAL
                || timestampNode.token == null
                || SqlUtil.getColumnIndexQuiet(projectedMetadata, timestampNode.token) != timestampIndex) {
            return null;
        }
        return segmentPlan(unitNode, segmentOffset, timestampType, timeZone);
    }

    public static void configure(
            @NotNull WindowFunction function,
            @NotNull WindowExpression window,
            @NotNull CharSequence factorySignature,
            int outputPosition,
            @NotNull RecordMetadata baseMetadata
    ) throws SqlException {
        final int timestampType = baseMetadata.getTimestampType();
        final String partitionSignature = expressionListSignature(window.getPartitionBy(), null);
        final String orderSignature = expressionListSignature(window.getOrderBy(), window.getOrderByDirection());
        final boolean anchored = window.getAnchorKind() != WindowExpression.ANCHOR_KIND_NONE
                || window.isResolvedWindowAnchored();
        // A stateless function's kind follows the function rather than the frame, which it does not
        // read.
        final boolean isStateless = function.isCheckpointStateless();
        final DependencyKind kind = isStateless
                ? DependencyKind.STATELESS_CURRENT_ROW
                : dependencyKind(function.getName(), window);
        final boolean keyed = function.getCheckpointKeyColumnTypes() != null;
        // validateRange() has already ensured a RANGE frame is ordered by the designated timestamp
        // ascending, so its width reads as a timestamp offset.
        final boolean isRange = kind == DependencyKind.RANGE_W_PRECEDING_BOUNDED_HI;
        // A fixed row-count state extent the function declares for itself, independent of its
        // frame - lag's offset. Long.MIN_VALUE means it has none and the frame decides the extent.
        final long rowsStateExtentOverride = function.checkpointRowsStateExtentOverride();
        final boolean hasRowsStateExtentOverride = rowsStateExtentOverride != Long.MIN_VALUE;
        final long frameLo;
        final long frameHi;
        final long stateExtentLo;
        if (isStateless) {
            // Zeros: the declared frame describes no row this function reads, and zero needs no
            // unit.
            frameLo = 0;
            frameHi = 0;
            stateExtentLo = 0;
        } else if (hasRowsStateExtentOverride && !isRange) {
            // lag ignores its frame and reads the row `offset` back, so it is described as ROWS
            // BETWEEN offset PRECEDING AND CURRENT ROW, extent included.
            frameLo = rowsStateExtentOverride;
            frameHi = 0;
            stateExtentLo = rowsStateExtentOverride;
        } else {
            frameLo = isRange
                    ? rangeFrameLo(function.getName(), window, timestampType)
                    : window.getRowsLo();
            // Both RANGE bounds convert from the user's unit so they stay commensurable; ROWS
            // bounds count rows and stay as recorded.
            frameHi = isRange
                    ? rangeFrameHi(function.getName(), window, timestampType)
                    : effectiveRowsHi(window);
            // An accumulator's extent is the frame's low bound; last_value reads only the row its
            // high bound names, so its extent is that lag.
            stateExtentLo = hasHighBoundStateExtent(function.getName(), window, frameHi)
                    ? frameHi
                    : frameLo;
        }
        // A row-count extent (lag) cannot bound a RANGE frame, whose repair works in timestamp
        // width, so the frame-local claim is declined there.
        final boolean hasFrameLocalState = function.hasFrameLocalCheckpointState()
                && !(hasRowsStateExtentOverride && isRange);
        final LiveViewCheckpointDependency dependency = new LiveViewCheckpointDependency(
                kind,
                partitionSignature,
                orderSignature,
                frameLo,
                frameHi,
                stateExtentLo,
                timestampType,
                hasFrameLocalState,
                keyed,
                keyed && anchored,
                StructuralConvergence.EXACT,
                numericConvergence(function)
        );
        final String canonicalWindowName = window.getResolvedWindowName() == null
                ? ""
                : Chars.toLowerCaseAscii(window.getResolvedWindowName());
        final String codecIdentity = STATE_PAGE_CODEC_FAMILY
                + "/" + factorySignature
                + "/v" + function.checkpointStateFormatVersion();
        final LiveViewCheckpointFunctionIdentity identity = new LiveViewCheckpointFunctionIdentity(
                canonicalWindowName,
                factorySignature,
                outputPosition,
                partitionSignature,
                orderSignature,
                codecIdentity,
                function.getCheckpointKeyColumnTypes()
        );
        function.setCheckpointCompilerMetadata(identity, dependency);
    }

    /**
     * Whether the three dependency plans between them cover every window function in the factory. A
     * repair needs that before it bounds them all at once: its replacement is timestamp-global, so
     * a function whose state the replay cannot reconstruct would be re-emitted wrong. A function is
     * uncovered when its kind has no plan, when it carries no dependency contract, or when no plan
     * can bound its kind. A factory with no window function answers false.
     */
    public static boolean isDependencyComplete(
            @NotNull ObjList<WindowFunction> windowFunctions,
            boolean hasRangePlan,
            boolean hasRowsPlan,
            boolean hasAnchorPlan
    ) {
        final int functionCount = windowFunctions.size();
        if (functionCount == 0) {
            return false;
        }
        for (int i = 0; i < functionCount; i++) {
            final LiveViewCheckpointDependency dependency = windowFunctions.getQuick(i).checkpointDependency();
            if (dependency == null) {
                return false;
            }
            final boolean covered;
            // The RANGE plan covers stateless functions too, at zero width.
            if (dependency.isStateless() || dependency.isFiniteRange()) {
                covered = hasRangePlan;
            } else if (dependency.isFiniteRows()) {
                covered = hasRowsPlan;
            } else {
                covered = hasAnchorPlan && dependency.getKind() == DependencyKind.FIXED_ANCHOR_SEGMENT;
            }
            if (!covered) {
                return false;
            }
        }
        return true;
    }

    /**
     * Builds the union of the finite RANGE dependencies a streaming live-view factory carries, or
     * null when it carries none or one of them lacks
     * {@link LiveViewCheckpointDependency#hasFrameLocalState() frame-local state}. Two RANGE
     * functions must agree on the key/order domain, or the compile fails, so no repair interval
     * mixes incompatible descriptors.
     * <p>
     * A {@link LiveViewCheckpointDependency#isStateless() stateless} function joins as a zero-width
     * arm: it never widens the union and agrees with every domain, and on its own it yields
     * {@code L = R} and {@code H = changeMaxTs + 1}.
     */
    @Nullable
    public static LiveViewCheckpointRangePlan rangePlan(
            @NotNull ObjList<Function> functions,
            @NotNull ObjList<QueryColumn> columns
    ) throws SqlException {
        LiveViewCheckpointDependency firstRange = null;
        LiveViewCheckpointDependency firstStateless = null;
        LiveViewCheckpointFunctionIdentity firstIdentity = null;
        boolean allFrameLocal = true;
        int rangeFunctionCount = 0;
        int statelessFunctionCount = 0;
        long maxFrameWidth = 0;

        for (int i = 0, n = functions.size(); i < n; i++) {
            final Function function = functions.getQuick(i);
            if (!(function instanceof WindowFunction windowFunction)) {
                continue;
            }
            final LiveViewCheckpointDependency dependency = windowFunction.checkpointDependency();
            if (dependency != null && dependency.isStateless()) {
                // A zero-width arm: it widens nothing and agrees with every domain, but makes the
                // plan exist for a view carrying nothing else.
                if (firstStateless == null) {
                    firstStateless = dependency;
                }
                allFrameLocal &= dependency.hasFrameLocalState();
                statelessFunctionCount++;
                continue;
            }
            if (dependency == null || !dependency.isFiniteRange()) {
                // Another kind's function, or one with no contract at all. Either way it
                // is not this plan's to describe.
                continue;
            }
            final LiveViewCheckpointFunctionIdentity identity = windowFunction.checkpointFunctionIdentity();
            if (firstRange == null) {
                firstRange = dependency;
                firstIdentity = identity;
            } else if (!firstRange.getPartitionSignature().equals(dependency.getPartitionSignature())
                    || !firstRange.getOrderSignature().equals(dependency.getOrderSignature())
                    || firstRange.getTimestampType() != dependency.getTimestampType()) {
                throw SqlException.$(
                                columns.getQuick(i).getAst().position,
                                "live view RANGE window functions must use the same PARTITION BY and ORDER BY domain"
                        )
                        .put(" [first=").put(functionLabel(firstIdentity))
                        .put(", incompatible=").put(functionLabel(identity)).put(']');
            }
            allFrameLocal &= dependency.hasFrameLocalState();
            maxFrameWidth = Math.max(maxFrameWidth, dependency.getRangeFrameWidth());
            rangeFunctionCount++;
        }

        if (!allFrameLocal) {
            return null;
        }
        // A stateless-only factory reports its own function's domain, which nothing downstream
        // reads.
        final LiveViewCheckpointDependency first = firstRange != null ? firstRange : firstStateless;
        if (first == null) {
            return null;
        }
        return new LiveViewCheckpointRangePlan(
                rangeFunctionCount + statelessFunctionCount,
                maxFrameWidth,
                first.getPartitionSignature(),
                first.getOrderSignature(),
                first.getTimestampType()
        );
    }

    /**
     * Builds the union of the finite ROWS dependencies a streaming live-view factory carries, or
     * null when the view has no bounded ROWS repair contract. Unlike {@link #rangePlan}, an
     * unusable shape declines the plan rather than failing the compile: every ROWS shape is
     * accepted at CREATE, and declining costs the view only its localized repair path.
     * <p>
     * The plan declines when a ROWS function lacks frame-local state, when two of them disagree on
     * the key/order domain, when a window is not ordered by the designated timestamp ascending,
     * when the frame is keyless or zero-wide, or when a key expression is
     * {@link Function#isNonDeterministic() non-deterministic}. Other expression keys project
     * through compiled functions; see {@link #keyProjector}.
     *
     * @param sharedProjector the view's shared key projector, used when it matches the ROWS
     *                        partition signature; otherwise the plan compiles and owns its own
     * @param functionParser  compiles an expression key into the plan's own function, separate
     *                        from the copy the window runtime partitions by
     */
    @Nullable
    public static LiveViewCheckpointRowsPlan rowsPlan(
            @NotNull ObjList<Function> functions,
            @NotNull ObjList<QueryColumn> columns,
            @NotNull RecordMetadata baseMetadata,
            @Nullable LiveViewCheckpointKeyProjector sharedProjector,
            @NotNull CairoConfiguration configuration,
            @NotNull BytecodeAssembler asm,
            @NotNull FunctionParser functionParser,
            @NotNull SqlExecutionContext executionContext
    ) throws SqlException {
        final int timestampIndex = baseMetadata.getTimestampIndex();
        if (timestampIndex == -1) {
            return null;
        }
        LiveViewCheckpointDependency firstRows = null;
        ObjList<ExpressionNode> partitionBy = null;
        int rowsFunctionCount = 0;
        long maxPrecedingRows = 0;

        for (int i = 0, n = functions.size(); i < n; i++) {
            final Function function = functions.getQuick(i);
            if (!(function instanceof WindowFunction windowFunction)) {
                continue;
            }
            final LiveViewCheckpointDependency dependency = windowFunction.checkpointDependency();
            if (dependency == null || !dependency.isFiniteRows()) {
                // Another kind's function, or one with no contract at all. Either way it
                // is not this plan's to describe.
                continue;
            }
            if (!dependency.hasFrameLocalState()) {
                return null;
            }
            if (!(columns.getQuick(i) instanceof WindowExpression window)
                    || !isOrderedByDesignatedTimestampAsc(window, baseMetadata)) {
                return null;
            }
            if (firstRows == null) {
                firstRows = dependency;
                partitionBy = window.getPartitionBy();
            } else if (!firstRows.getPartitionSignature().equals(dependency.getPartitionSignature())
                    || !firstRows.getOrderSignature().equals(dependency.getOrderSignature())
                    || firstRows.getTimestampType() != dependency.getTimestampType()) {
                return null;
            }
            maxPrecedingRows = Math.max(maxPrecedingRows, dependency.getRowsPrecedingCount());
            rowsFunctionCount++;
        }

        // An empty PARTITION BY or a zero-wide frame has nothing to bound; neither reaches here
        // through a checkpoint-capable function today.
        if (firstRows == null || partitionBy.size() == 0 || maxPrecedingRows < 1) {
            return null;
        }
        LiveViewCheckpointKeyProjector projector = sharedProjector;
        if (projector == null || !projector.getPartitionSignature().equals(firstRows.getPartitionSignature())) {
            // No shared identity matches the ROWS functions' partitioning, so the plan compiles and
            // owns its own projector.
            projector = keyProjector(
                    partitionBy,
                    firstRows.getPartitionSignature(),
                    baseMetadata,
                    configuration,
                    asm,
                    functionParser,
                    executionContext
            );
        }
        if (projector == null) {
            return null;
        }
        final boolean isProjectorOwned = projector != sharedProjector;
        try {
            return new LiveViewCheckpointRowsPlan(
                    rowsFunctionCount,
                    maxPrecedingRows,
                    firstRows.getPartitionSignature(),
                    firstRows.getOrderSignature(),
                    projector,
                    isProjectorOwned,
                    timestampIndex,
                    firstRows.getTimestampType()
            );
        } catch (Throwable th) {
            if (isProjectorOwned) {
                Misc.free(projector);
            }
            throw th;
        }
    }

    /**
     * Compiles the partition identity {@code partitionBy} names into the projector every repair
     * of the view reads its keys through, or returns null when it cannot be projected.
     * <ul>
     *     <li><b>Plain columns</b> project through a sink over their column indexes, which also
     *     lets a repair seek one key's rows through a SYMBOL index. A SYMBOL projects as its
     *     table-local integer, stable for one reader's lifetime - the scope one repair runs in -
     *     and a second sink writes the resolved string a checkpoint partition map keys by.</li>
     *     <li><b>Expressions</b>: one non-column term puts every term on a compiled key function.
     *     A SYMBOL-typed key function projects its resolved string, since its integers index its
     *     own map rather than the reader's.</li>
     * </ul>
     * A non-deterministic expression key is refused: two cursors reading one base row must land
     * on the same key.
     */
    public static @Nullable LiveViewCheckpointKeyProjector keyProjector(
            @NotNull ObjList<ExpressionNode> partitionBy,
            @NotNull CharSequence partitionSignature,
            @NotNull RecordMetadata baseMetadata,
            @NotNull CairoConfiguration configuration,
            @NotNull BytecodeAssembler asm,
            @NotNull FunctionParser functionParser,
            @NotNull SqlExecutionContext executionContext
    ) throws SqlException {
        if (partitionBy.size() == 0) {
            // A window with no PARTITION BY compiles to a scalar function that carries no
            // per-key state, so there is no identity here to name.
            return null;
        }
        final IntList partitionByColumnIndexes = new IntList(partitionBy.size());
        final ListColumnFilter keyColumnFilter = new ListColumnFilter(partitionBy.size());
        final ArrayColumnTypes keyColumnTypes = new ArrayColumnTypes();
        // The checkpoint projector's key types: this list with every SYMBOL resolved to STRING.
        // Null while no key column is a SYMBOL, when the two projectors are the same.
        ArrayColumnTypes checkpointKeyColumnTypes = null;
        BitSet writeSymbolAsString = null;
        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            final ExpressionNode node = partitionBy.getQuick(i);
            final int columnIndex = node.type == ExpressionNode.LITERAL
                    ? SqlUtil.getColumnIndexQuiet(baseMetadata, node.token)
                    : -1;
            if (columnIndex == -1) {
                // One non-column term puts every term on a key function, so SYMBOL keys share one
                // space.
                return expressionKeyProjector(
                        partitionBy,
                        partitionSignature,
                        baseMetadata,
                        configuration,
                        asm,
                        functionParser,
                        executionContext
                );
            }
            final int columnType = baseMetadata.getColumnType(columnIndex);
            partitionByColumnIndexes.add(columnIndex);
            keyColumnFilter.add(columnIndex + 1);
            keyColumnTypes.add(columnType);
            if (ColumnType.isSymbol(columnType)) {
                if (checkpointKeyColumnTypes == null) {
                    checkpointKeyColumnTypes = new ArrayColumnTypes();
                    for (int j = 0; j < i; j++) {
                        checkpointKeyColumnTypes.add(keyColumnTypes.getColumnType(j));
                    }
                    writeSymbolAsString = new BitSet();
                }
                writeSymbolAsString.set(columnIndex);
                checkpointKeyColumnTypes.add(ColumnType.STRING);
            } else if (checkpointKeyColumnTypes != null) {
                checkpointKeyColumnTypes.add(columnType);
            }
        }
        // SYMBOL keys project as table-local integers here.
        final RecordSink keySink = RecordSinkFactory.getInstance(configuration, asm, baseMetadata, keyColumnFilter);
        // The checkpoint sink writes SYMBOL keys as strings, to compare equal to the keys the
        // window functions' own partition maps hold.
        final RecordSink checkpointKeySink = writeSymbolAsString == null
                ? keySink
                : RecordSinkFactory.getInstance(configuration, asm, baseMetadata, keyColumnFilter, writeSymbolAsString);
        return new LiveViewCheckpointKeyProjector(
                partitionByColumnIndexes,
                null,
                keyColumnTypes,
                keySink,
                checkpointKeyColumnTypes != null ? checkpointKeyColumnTypes : keyColumnTypes,
                checkpointKeySink,
                partitionSignature,
                baseMetadata
        );
    }

    /**
     * Compiles the partition identity every checkpoint-capable window function in the factory
     * shares, or returns null when they do not share one, which
     * {@link io.questdb.cairo.lv.LiveViewSegmentRepairEnvelope#keyedScanGate} reports as
     * {@code GATE_MIXED_PARTITION_KEYS}. Such a view keeps every other repair route.
     */
    public static @Nullable LiveViewCheckpointKeyProjector sharedKeyProjector(
            @NotNull ObjList<Function> functions,
            @NotNull ObjList<QueryColumn> columns,
            @NotNull RecordMetadata baseMetadata,
            @NotNull CairoConfiguration configuration,
            @NotNull BytecodeAssembler asm,
            @NotNull FunctionParser functionParser,
            @NotNull SqlExecutionContext executionContext
    ) throws SqlException {
        String signature = null;
        ObjList<ExpressionNode> partitionBy = null;
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (!(functions.getQuick(i) instanceof WindowFunction windowFunction)) {
                continue;
            }
            final LiveViewCheckpointDependency dependency = windowFunction.checkpointDependency();
            if (dependency == null) {
                return null;
            }
            if (signature == null) {
                if (!(columns.getQuick(i) instanceof WindowExpression window)) {
                    return null;
                }
                signature = dependency.getPartitionSignature();
                partitionBy = window.getPartitionBy();
            } else if (!signature.equals(dependency.getPartitionSignature())) {
                return null;
            }
        }
        if (signature == null) {
            return null;
        }
        return keyProjector(partitionBy, signature, baseMetadata, configuration, asm, functionParser, executionContext);
    }

    /**
     * Compiles the fused window-state plan for a live view: the accumulator components its
     * durable state is made of, the projections that read them, and the residual functions that
     * keep their own legacy roots. Returns null when nothing can join a fused group.
     * <p>
     * A function contributes a component only when the anchor resets it, its dependency is the
     * anchor segment with key-resettable state, it holds whole-state, non-ring checkpoint state of
     * a fixed width within the inline budget, it declares an accumulator family and projection,
     * and its argument is a direct column reference whose contribution predicate
     * {@link WindowAccumulatorDescriptor} can name, or it takes none. Anything else, an
     * implicitly cast argument included, is a residual.
     * <p>
     * Sharing is proved from the component identity alone, never from SQL text: {@code sum(x)}
     * and {@code avg(x)} merge on the same {@code (family, argument, predicate, codec)}, and
     * {@code count(*)} joins as a row count. A {@code count(k)} over the window's own partition
     * key joins that row count too; see {@link WindowAccumulatorCandidate} and
     * {@link #isCountOverTheWindowsOwnPartitionKey}.
     *
     * @param functions                 every SELECT-list function, in output order
     * @param anchorableWindowFunctions the subset the anchor dispatches {@code resetPartition}
     *                                  to, or null for a factory with no anchored window
     */
    public static @Nullable LiveViewWindowStatePlan windowStatePlan(
            @NotNull ObjList<Function> functions,
            @Nullable ObjList<WindowFunction> anchorableWindowFunctions,
            @NotNull RecordMetadata baseMetadata
    ) {
        if (anchorableWindowFunctions == null || anchorableWindowFunctions.size() == 0) {
            return null;
        }
        // Read before any projection is added: a count over the partition key may precede its
        // row-count host in the SELECT list.
        final WindowFunction rowCountHost = rowCountHost(functions, anchorableWindowFunctions);
        // A live view compiles no WindowMapSpec, so the guard proves the key from each function's
        // checkpoint key layout and PARTITION BY functions.
        final WindowAccumulatorCandidate.PartitionKeyGuard partitionKeyGuard =
                (function, argumentColumnIndex, host) ->
                        isCountOverTheWindowsOwnPartitionKey(function, argumentColumnIndex, host, baseMetadata);
        final LiveViewWindowStatePlan.Builder builder = new LiveViewWindowStatePlan.Builder();
        // Name the window first, so a window whose every function is a residual still gets the
        // storage plan its checkpoints are laid out by.
        if (!nameAnchoredWindow(builder, anchorableWindowFunctions)) {
            return null;
        }
        for (int i = 0, n = functions.size(); i < n; i++) {
            final Function function = functions.getQuick(i);
            if (!(function instanceof WindowFunction windowFunction)) {
                continue;
            }
            if (!addAccumulatorProjection(
                    builder,
                    windowFunction,
                    i,
                    anchorableWindowFunctions,
                    baseMetadata,
                    rowCountHost,
                    partitionKeyGuard
            )) {
                builder.addResidual(windowFunction);
            }
        }
        return builder.build();
    }

    /**
     * Rejects at CREATE a {@code RANGE W PRECEDING} frame ending at or below the current row that
     * is not ordered by the designated timestamp ascending, since its width would not bound a
     * repair. Other RANGE shapes produce no finite RANGE descriptor and keep their behavior. The
     * caller runs this before parsing the function, so the error names the frame's position.
     */
    public static void validateRange(
            @NotNull WindowExpression window,
            @NotNull CharSequence functionName,
            @NotNull RecordMetadata baseMetadata
    ) throws SqlException {
        if (dependencyKind(functionName, window) == DependencyKind.RANGE_W_PRECEDING_BOUNDED_HI) {
            validateRangeOrder(functionName, window, baseMetadata);
        }
    }

    /**
     * Names the anchored window on {@code builder} from the first anchorable function carrying
     * checkpoint metadata. A view has one {@code LiveViewWindow}, so that function names the window
     * for all of them, and the builder's identity test declines any that disagree.
     *
     * @return false when no anchorable function carries an identity and key layout
     */
    private static boolean nameAnchoredWindow(
            LiveViewWindowStatePlan.Builder builder,
            ObjList<WindowFunction> anchorableWindowFunctions
    ) {
        for (int i = 0, n = anchorableWindowFunctions.size(); i < n; i++) {
            final WindowFunction function = anchorableWindowFunctions.getQuick(i);
            final LiveViewCheckpointFunctionIdentity identity = function.checkpointFunctionIdentity();
            final ColumnTypes keyColumnTypes = function.getCheckpointKeyColumnTypes();
            if (identity == null || keyColumnTypes == null || keyColumnTypes.getColumnCount() == 0) {
                continue;
            }
            builder.ofWindow(
                    LiveViewWindowStatePlan.encodeWindowIdentity(
                            identity.getCanonicalWindowName(),
                            identity.getPartitionSignature(),
                            identity.getOrderSignature()
                    ),
                    keyColumnTypes
            );
            return true;
        }
        return false;
    }

    /**
     * Offers {@code function} to the fused group and reports whether it joined; a rejected function
     * becomes a residual.
     */
    private static boolean addAccumulatorProjection(
            LiveViewWindowStatePlan.Builder builder,
            WindowFunction function,
            int outputPosition,
            ObjList<WindowFunction> anchorableWindowFunctions,
            RecordMetadata baseMetadata,
            @Nullable WindowFunction rowCountHost,
            WindowAccumulatorCandidate.PartitionKeyGuard partitionKeyGuard
    ) {
        if (!isFusibleAccumulator(function, anchorableWindowFunctions)) {
            return false;
        }
        final WindowAccumulatorCandidate candidate = WindowAccumulatorCandidate.of(
                function,
                baseMetadata,
                rowCountHost,
                partitionKeyGuard
        );
        if (candidate == null) {
            return false;
        }
        final LiveViewAccumulatorDescriptor component = LiveViewAccumulatorDescriptor.of(candidate.getComponent());
        if (component == null) {
            return false;
        }
        final LiveViewCheckpointFunctionIdentity identity = function.checkpointFunctionIdentity();
        if (identity == null) {
            return false;
        }
        return builder.addProjection(
                function,
                component,
                candidate.getProjectionKind(),
                outputPosition,
                LiveViewWindowStatePlan.encodeWindowIdentity(
                        identity.getCanonicalWindowName(),
                        identity.getPartitionSignature(),
                        identity.getOrderSignature()
                ),
                function.getCheckpointKeyColumnTypes()
        );
    }

    /**
     * Points a frame-bound error at the unit token the user wrote, falling back to the window
     * function itself for a bound whose unit position the parser did not record.
     */
    private static int boundPosition(WindowExpression window, int unitPosition) {
        return unitPosition > 0 ? unitPosition : window.getAst().position;
    }

    /**
     * Classifies a frame into the dependency kind whose bounds a repair can prove, reading the high
     * bound the runtime evaluates ({@link #effectiveRowsHi}).
     * <p>
     * The eligible kinds admit any high bound at or below the current row: a lagging frame reads a
     * subset of what the same-width frame ending at the current row reads, so the bounds derived
     * from the look-behind stay valid, only looser. The look-behind is the state extent's
     * ({@link #hasFiniteStateLookBehind}) rather than the frame's. A {@code FOLLOWING} high bound
     * falls through to {@code FOLLOWING_OR_DATA_DEPENDENT}, since a base row then joins the frames
     * of earlier output.
     */
    private static DependencyKind dependencyKind(CharSequence functionName, WindowExpression window) {
        if (window.getAnchorKind() != WindowExpression.ANCHOR_KIND_NONE || window.isResolvedWindowAnchored()) {
            return DependencyKind.FIXED_ANCHOR_SEGMENT;
        }
        final long rowsHi = effectiveRowsHi(window);
        if (isRanking(functionName)
                && window.getRowsLo() == Long.MIN_VALUE
                && rowsHi == Long.MAX_VALUE) {
            return DependencyKind.UNANCHORED_RANK;
        }
        // SqlOptimiser.normalizeWindowFrame() can negate a literal Long.MAX_VALUE PRECEDING high
        // bound into Long.MIN_VALUE, the unbounded encoding, which names no finite lag. A RANGE end
        // of that width negates to Long.MIN_VALUE + 1 instead.
        if (hasFiniteStateLookBehind(functionName, window, rowsHi)
                && rowsHi != Long.MIN_VALUE && rowsHi <= 0
                && hasSupportedExclusion(window)) {
            if (window.getFramingMode() == WindowExpression.FRAMING_ROWS) {
                return DependencyKind.ROWS_N_PRECEDING_BOUNDED_HI;
            }
            if (window.getFramingMode() == WindowExpression.FRAMING_RANGE
                    && window.getRowsLoKind() == WindowExpression.PRECEDING
                    && (window.getRowsHiKind() == WindowExpression.CURRENT
                    || window.getRowsHiKind() == WindowExpression.PRECEDING)) {
                return DependencyKind.RANGE_W_PRECEDING_BOUNDED_HI;
            }
        }
        if (window.getRowsLo() == Long.MIN_VALUE && rowsHi == 0) {
            return DependencyKind.UNBOUNDED_CUMULATIVE_NO_RESET;
        }
        return DependencyKind.FOLLOWING_OR_DATA_DEPENDENT;
    }

    /**
     * Returns the frame's high bound as the runtime evaluates it.
     * {@code WindowContextImpl.getRowsHi()} rewrites an {@code EXCLUDE CURRENT ROW} frame's
     * {@code 0} to {@code -1} - one tick for RANGE, one row for ROWS - while
     * {@link WindowExpression#getRowsHi()} still answers {@code 0}. Folding it here lets
     * {@link #dependencyKind} treat the exclusion as the smallest lagging bound;
     * {@link #hasSupportedExclusion} keeps the other exclusion modes out.
     */
    private static long effectiveRowsHi(WindowExpression window) {
        return effectiveRowsHi(window, window.getRowsHi());
    }

    /**
     * Folds {@code EXCLUDE CURRENT ROW} into a high bound already converted into the designated
     * timestamp's units. That is the runtime's order: the {@code -1} is already normalized, and a
     * later conversion would scale it again.
     */
    private static long effectiveRowsHi(WindowExpression window, long rowsHi) {
        return window.getExclusionKind() == WindowExpression.EXCLUDE_CURRENT_ROW && rowsHi == 0
                ? -1
                : rowsHi;
    }

    /**
     * Builds the projector of a view whose PARTITION BY holds an expression, compiling every term
     * into a key function of the projector's own, separate from the window runtime's copies. This
     * path has no index seek. The functions are freed unless the projector takes ownership of them.
     */
    private static @Nullable LiveViewCheckpointKeyProjector expressionKeyProjector(
            ObjList<ExpressionNode> partitionBy,
            CharSequence partitionSignature,
            RecordMetadata baseMetadata,
            CairoConfiguration configuration,
            BytecodeAssembler asm,
            FunctionParser functionParser,
            SqlExecutionContext executionContext
    ) throws SqlException {
        ObjList<Function> keyFunctions = new ObjList<>(partitionBy.size());
        try {
            final ArrayColumnTypes keyColumnTypes = new ArrayColumnTypes();
            for (int i = 0, n = partitionBy.size(); i < n; i++) {
                final Function function = functionParser.parseFunction(partitionBy.getQuick(i), baseMetadata, executionContext);
                keyFunctions.add(function);
                if (function.isNonDeterministic()) {
                    // The forward pass and the backward search read one base row through two
                    // cursors, so the key must not change between reads.
                    return null;
                }
                final int type = function.getType();
                keyColumnTypes.add(ColumnType.isSymbol(type) ? ColumnType.STRING : type);
            }
            final RecordSink keySink = RecordSinkFactory.getInstance(
                    configuration,
                    asm,
                    baseMetadata,
                    new ListColumnFilter(),
                    keyFunctions,
                    null
            );
            final LiveViewCheckpointKeyProjector projector = new LiveViewCheckpointKeyProjector(
                    null,
                    keyFunctions,
                    keyColumnTypes,
                    keySink,
                    // SYMBOL keys already project as strings, so the checkpoint form is the same.
                    keyColumnTypes,
                    keySink,
                    partitionSignature,
                    baseMetadata
            );
            keyFunctions = null;
            return projector;
        } finally {
            Misc.freeObjList(keyFunctions);
        }
    }

    private static String expressionListSignature(ObjList<ExpressionNode> expressions, IntList directions) {
        final StringSink sink = new StringSink();
        sink.put(expressions.size()).putAscii(':');
        for (int i = 0, n = expressions.size(); i < n; i++) {
            final StringSink expressionSink = new StringSink();
            expressions.getQuick(i).toSink(expressionSink);
            sink.put(expressionSink.length()).putAscii(':').put(expressionSink);
            if (directions != null) {
                sink.putAscii(':').put(directions.getQuick(i));
            }
            sink.putAscii(';');
        }
        return sink.toString();
    }

    private static CharSequence functionLabel(LiveViewCheckpointFunctionIdentity identity) {
        if (identity == null) {
            return "unknown";
        }
        return identity.getCanonicalWindowName().isEmpty()
                ? identity.getFactorySignature()
                : identity.getFactorySignature() + " OVER " + identity.getCanonicalWindowName();
    }

    /**
     * Whether the frame names a finite look-behind for the function's state: the frame's low bound
     * for an accumulator, or the lag for a function whose state the high bound bounds.
     */
    private static boolean hasFiniteStateLookBehind(CharSequence functionName, WindowExpression window, long rowsHi) {
        return hasHighBoundStateExtent(functionName, window, rowsHi)
                || (window.getRowsLo() != Long.MIN_VALUE && window.getRowsLo() <= 0);
    }

    /**
     * Whether the function's state extent is the frame's high bound rather than its low one. It
     * holds only for {@code last_value} over a ROWS frame ending below the current row without
     * {@code IGNORE NULLS}: it emits the single row its high bound names, so the lag is all a
     * warm-up replays. The test names the function because an accumulator over the same frame would
     * be under-replayed.
     * <p>
     * {@code IGNORE NULLS} scans back to the frame's start like an accumulator, a high bound at the
     * current row compiles to a stateless projection, and a RANGE lag names a timestamp offset
     * whose moved outputs the data rather than the lag bounds. The function's own
     * {@link WindowFunction#hasFrameLocalCheckpointState()} still has to agree.
     */
    private static boolean hasHighBoundStateExtent(CharSequence functionName, WindowExpression window, long rowsHi) {
        return rowsHi < 0
                && window.getFramingMode() == WindowExpression.FRAMING_ROWS
                && !window.isIgnoreNulls()
                && Chars.equalsIgnoreCase(functionName, "last_value");
    }

    /**
     * Whether the frame exclusion is one the window runtime implements: {@code EXCLUDE NO OTHERS}
     * or {@code EXCLUDE CURRENT ROW}, which {@link #effectiveRowsHi} has already folded into the
     * high bound. {@code WindowContextImpl.validate()} rejects the other two.
     */
    private static boolean hasSupportedExclusion(WindowExpression window) {
        return window.getExclusionKind() == WindowExpression.EXCLUDE_NO_OTHERS
                || window.getExclusionKind() == WindowExpression.EXCLUDE_CURRENT_ROW;
    }

    /**
     * Whether the anchor's segment determines the state of every anchored window function, so a
     * repair can rebuild that state by replaying one segment. It is checked both ways: every
     * anchored function must be in the subset the anchor resets, with key-resettable state, and
     * every member of that subset must be anchored or checkpoint-stateless, or the anchor resets
     * state it does not own. A bounded ROWS/RANGE window beside the anchored one is its own frame
     * plan's to bound. A factory with no anchored function answers false.
     */
    private static boolean isAnchorSegmentLocal(
            ObjList<WindowFunction> windowFunctions,
            ObjList<WindowFunction> anchorableWindowFunctions
    ) {
        if (anchorableWindowFunctions == null) {
            return false;
        }
        for (int i = 0, n = anchorableWindowFunctions.size(); i < n; i++) {
            final WindowFunction function = anchorableWindowFunctions.getQuick(i);
            if (function.isCheckpointStateless()) {
                continue;
            }
            final LiveViewCheckpointDependency dependency = function.checkpointDependency();
            if (dependency == null || dependency.getKind() != DependencyKind.FIXED_ANCHOR_SEGMENT) {
                return false;
            }
        }
        int anchoredCount = 0;
        for (int i = 0, n = windowFunctions.size(); i < n; i++) {
            final WindowFunction function = windowFunctions.getQuick(i);
            final LiveViewCheckpointDependency dependency = function.checkpointDependency();
            if (dependency == null || dependency.getKind() != DependencyKind.FIXED_ANCHOR_SEGMENT) {
                continue;
            }
            if (!dependency.supportsKeyReset() || anchorableWindowFunctions.indexOf(function) < 0) {
                return false;
            }
            anchoredCount++;
        }
        return anchoredCount > 0;
    }

    /**
     * Whether the window is ordered by the designated timestamp ascending - the order the replay's
     * cursor produces and both repair bounds are expressed in.
     */
    private static boolean isOrderedByDesignatedTimestampAsc(WindowExpression window, RecordMetadata baseMetadata) {
        final ObjList<ExpressionNode> orderBy = window.getOrderBy();
        final int timestampIndex = baseMetadata.getTimestampIndex();
        return timestampIndex != -1
                && orderBy.size() == 1
                && window.getOrderByDirection().getQuick(0) == IQueryModel.ORDER_DIRECTION_ASCENDING
                && SqlUtil.getColumnIndexQuiet(baseMetadata, orderBy.getQuick(0).token) == timestampIndex;
    }

    /**
     * The live view's {@link WindowAccumulatorCandidate.PartitionKeyGuard}: whether
     * {@code function}'s window partitions by exactly the one column {@code argumentColumnIndex}
     * names and {@code rowCountHost} belongs to that same window. The column is proved through
     * {@link WindowAccumulatorDescriptor#directColumnIndex} rather than the rendered signature, the
     * encoded key must be one column, and the host is compared directly because the builder has not
     * latched a window identity yet.
     */
    private static boolean isCountOverTheWindowsOwnPartitionKey(
            WindowFunction function,
            int argumentColumnIndex,
            WindowFunction rowCountHost,
            RecordMetadata baseMetadata
    ) {
        final ColumnTypes keyColumnTypes = function.getCheckpointKeyColumnTypes();
        if (keyColumnTypes == null || keyColumnTypes.getColumnCount() != 1) {
            return false;
        }
        final ObjList<? extends Function> partitionBy = function.checkpointPartitionByFunctions();
        if (partitionBy == null || partitionBy.size() != 1) {
            return false;
        }
        if (WindowAccumulatorDescriptor.directColumnIndex(partitionBy.getQuick(0), baseMetadata) != argumentColumnIndex) {
            return false;
        }
        return isSameWindowGroup(function, rowCountHost);
    }

    /**
     * The gates a function passes before any part of its accumulator identity is read.
     * Shared by the projection walk and by the row-count pre-pass, so the pre-pass cannot
     * name a host the walk would have declined.
     */
    private static boolean isFusibleAccumulator(
            WindowFunction function,
            ObjList<WindowFunction> anchorableWindowFunctions
    ) {
        if (anchorableWindowFunctions.indexOf(function) < 0) {
            return false;
        }
        final LiveViewCheckpointDependency dependency = function.checkpointDependency();
        if (dependency == null
                || dependency.getKind() != DependencyKind.FIXED_ANCHOR_SEGMENT
                || !dependency.supportsKeyReset()) {
            return false;
        }
        if (!function.supportsCheckpointState()
                || function.supportsCheckpointRingState()
                || function.isCheckpointStateless()) {
            return false;
        }
        return LiveViewCheckpointContracts.isInlineableStateLength(function.checkpointStateFixedLength());
    }

    private static boolean isRanking(CharSequence name) {
        return Chars.equalsIgnoreCase(name, "row_number")
                || Chars.equalsIgnoreCase(name, "rank")
                || Chars.equalsIgnoreCase(name, "dense_rank");
    }

    /**
     * Whether two functions belong to one fused window group: the same named window, partition and
     * order domains, and encoded key layout. It is the test {@code LiveViewWindowStatePlan.Builder}
     * applies, read directly because the row-count pre-pass runs before the builder sees either.
     */
    private static boolean isSameWindowGroup(WindowFunction left, WindowFunction right) {
        final LiveViewCheckpointFunctionIdentity a = left.checkpointFunctionIdentity();
        final LiveViewCheckpointFunctionIdentity b = right.checkpointFunctionIdentity();
        if (a == null || b == null) {
            return false;
        }
        if (!Chars.equals(a.getCanonicalWindowName(), b.getCanonicalWindowName())
                || !Chars.equals(a.getPartitionSignature(), b.getPartitionSignature())
                || !Chars.equals(a.getOrderSignature(), b.getOrderSignature())) {
            return false;
        }
        final ColumnTypes leftKey = left.getCheckpointKeyColumnTypes();
        final ColumnTypes rightKey = right.getCheckpointKeyColumnTypes();
        if (leftKey == null || rightKey == null) {
            return false;
        }
        final int n = leftKey.getColumnCount();
        if (n != rightKey.getColumnCount()) {
            return false;
        }
        for (int i = 0; i < n; i++) {
            if (leftKey.getColumnType(i) != rightKey.getColumnType(i)) {
                return false;
            }
        }
        return true;
    }

    private static NumericConvergence numericConvergence(WindowFunction function) {
        // Floating accumulators may leave a sub-ULP difference after a localized replay; the same
        // functions over DECIMAL are exact. The stddev/variance and covariance/correlation
        // families stay EXACT: their formulas subtract nearly equal running sums, which would
        // amplify that difference past the contract's ceiling, so their bounded frames decline
        // frame-local state and no localized replay reaches them. Every repair they take starts
        // from state the plain query also holds - a sealed root carried verbatim, an anchor's
        // reset or the view's boundary - and re-emits the same bits.
        final CharSequence name = function.getName();
        return ColumnType.tagOf(function.getType()) == ColumnType.DOUBLE
                && (Chars.equalsIgnoreCase(name, "avg")
                || Chars.equalsIgnoreCase(name, "sum")
                || Chars.equalsIgnoreCase(name, "ksum")
                || Chars.equalsIgnoreCase(name, "nsum"))
                ? NumericConvergence.FLOATING_TOLERANCE
                : NumericConvergence.EXACT;
    }

    /**
     * Converts one RANGE frame bound from the user's unit into the designated timestamp's units,
     * exactly as {@code WindowContextImpl.of()} builds the runtime frame, so the descriptor and the
     * compiled frame agree bit-for-bit. It rejects a bound wider than
     * {@link TimestampDriver#getMaxUnitValue(char)}, the ceiling the runtime guards with, and a
     * nonzero bound finer than the timestamp's resolution, which the runtime would round to zero
     * and a repair would then be bounded by.
     */
    private static long rangeFrameBound(
            CharSequence functionName,
            WindowExpression window,
            int timestampType,
            long bound,
            char unit,
            int unitPosition,
            CharSequence boundName
    ) throws SqlException {
        if (unit == 0 || !ColumnType.isTimestamp(timestampType)) {
            return bound;
        }
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        final long maxUnitValue = driver.getMaxUnitValue(unit);
        // A PRECEDING bound arrives negated, and the widest count the parser accepts,
        // Long.MAX_VALUE, negates onto Long.MIN_VALUE - which negates back onto itself.
        final long width = bound == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bound);
        if (bound < -maxUnitValue || bound > maxUnitValue) {
            throw SqlException.$(boundPosition(window, unitPosition), "live view RANGE frame ").put(boundName)
                    .put(" is out of range for the designated timestamp [function=")
                    .put(functionName).put("(), width=").put(width).put(unit)
                    .put(", max=").put(maxUnitValue).put(unit).put(']');
        }
        final long converted = driver.from(bound, unit);
        if (converted == 0 && bound != 0) {
            throw SqlException.$(boundPosition(window, unitPosition), "live view RANGE frame ").put(boundName)
                    .put(" is below the designated timestamp resolution [function=")
                    .put(functionName).put("(), width=").put(width).put(unit).put(']');
        }
        return converted;
    }

    /**
     * Resolves the descriptor's {@code frameHi} to the negated finite lag {@code V} in the
     * designated timestamp's units, folding {@code EXCLUDE CURRENT ROW} in after the conversion.
     */
    private static long rangeFrameHi(
            CharSequence functionName,
            WindowExpression window,
            int timestampType
    ) throws SqlException {
        return effectiveRowsHi(window, rangeFrameBound(
                functionName,
                window,
                timestampType,
                window.getRowsHi(),
                window.getRowsHiExprTimeUnit(),
                window.getRowsHiExprPos(),
                "end lag"
        ));
    }

    /**
     * Resolves the descriptor's {@code frameLo} to the negated finite RANGE width {@code W}
     * in the designated timestamp's native units, which is what the repair floor
     * {@code L = R - W} subtracts from a timestamp.
     */
    private static long rangeFrameLo(
            CharSequence functionName,
            WindowExpression window,
            int timestampType
    ) throws SqlException {
        return rangeFrameBound(
                functionName,
                window,
                timestampType,
                window.getRowsLo(),
                window.getRowsLoExprTimeUnit(),
                window.getRowsLoExprPos(),
                "width"
        );
    }

    /**
     * The first function that would join the group as an unguarded
     * {@link WindowAccumulatorDescriptor#FAMILY_ROW_COUNT row count} - {@code count(*)} or a
     * partitioned {@code row_number()} - or null. It is searched before any projection is added,
     * because a {@code count} over the partition key may precede its host in the SELECT list, and
     * applies the same gates as {@link #addAccumulatorProjection}.
     */
    private static @Nullable WindowFunction rowCountHost(
            ObjList<Function> functions,
            ObjList<WindowFunction> anchorableWindowFunctions
    ) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            final Function function = functions.getQuick(i);
            if (!(function instanceof WindowFunction windowFunction)
                    || !isFusibleAccumulator(windowFunction, anchorableWindowFunctions)
                    || !WindowAccumulatorCandidate.isRowCountHost(windowFunction)
                    || windowFunction.checkpointFunctionIdentity() == null) {
                continue;
            }
            final LiveViewAccumulatorDescriptor component = LiveViewAccumulatorDescriptor.of(
                    WindowAccumulatorDescriptor.FAMILY_ROW_COUNT,
                    WindowAccumulatorDescriptor.NO_ARGUMENT_COLUMN_INDEX,
                    ColumnType.UNDEFINED
            );
            if (component != null
                    && component.getStateLength() == windowFunction.checkpointStateFixedLength()) {
                return windowFunction;
            }
        }
        return null;
    }

    /**
     * Reads a {@code timestamp_floor} period literal - {@code '1d'}, {@code '15m'} - into a segment
     * plan, split as {@code TimestampFloorFunctionFactory} splits it, on the zone's civil grid when
     * {@code timeZone} is non-null. An unreadable token declines rather than throws.
     */
    private static LiveViewCheckpointAnchorPlan segmentPlan(
            ExpressionNode unitNode,
            long segmentOffset,
            int timestampType,
            @Nullable CharSequence timeZone
    ) {
        if (unitNode == null || unitNode.type != ExpressionNode.CONSTANT || unitNode.token == null) {
            return null;
        }
        final CharSequence token = unitNode.token;
        if (!Chars.isQuoted(token) || token.length() < 3) {
            return null;
        }
        final int lo = 1;
        final int hi = token.length() - 1;
        final char unit = token.charAt(hi - 1);
        int stride = 1;
        if (hi - lo > 1) {
            try {
                stride = Numbers.parseInt(token, lo, hi - 1);
            } catch (NumericException e) {
                return null;
            }
        }
        return timeZone == null
                ? LiveViewCheckpointAnchorPlan.of(unit, stride, segmentOffset, timestampType)
                : LiveViewCheckpointAnchorPlan.ofTimeZone(unit, stride, segmentOffset, timestampType, timeZone);
    }

    /**
     * The origin a DAILY anchor's buckets align to, in the designated timestamp's units, read off
     * the definition's captured wall time because the desugared expression carries it as a cast.
     */
    private static long dailyAnchorOrigin(LiveViewDefinition.LvAnchorSpec spec, int timestampType) {
        return ColumnType.getTimestampDriver(timestampType).from(spec.anchorDailyTimeUs, ColumnType.TIMESTAMP_MICRO);
    }

    /**
     * Whether {@code node} is the quoted string literal {@code expected}.
     */
    private static boolean isQuotedConstant(@Nullable ExpressionNode node, @NotNull CharSequence expected) {
        if (node == null || node.type != ExpressionNode.CONSTANT || node.token == null) {
            return false;
        }
        final CharSequence token = node.token;
        return Chars.isQuoted(token)
                && Chars.equals(token, 1, token.length() - 1, expected, 0, expected.length());
    }

    private static void validateRangeOrder(
            CharSequence functionName,
            WindowExpression window,
            RecordMetadata baseMetadata
    ) throws SqlException {
        if (!isOrderedByDesignatedTimestampAsc(window, baseMetadata)) {
            final ObjList<ExpressionNode> orderBy = window.getOrderBy();
            final int position = orderBy.size() > 0 ? orderBy.getQuick(0).position : window.getAst().position;
            throw SqlException.$(position, "live view RANGE window function must ORDER BY the designated timestamp ASC [function=")
                    .put(functionName).put("()]");
        }
    }
}
