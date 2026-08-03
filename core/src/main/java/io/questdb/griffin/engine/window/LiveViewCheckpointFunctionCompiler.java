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
import io.questdb.cairo.lv.LiveViewAccumulatorProjection;
import io.questdb.cairo.lv.LiveViewCheckpointAnchorPlan;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.DependencyKind;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointDependency.NumericConvergence;
import io.questdb.cairo.lv.LiveViewCheckpointDependency.StructuralConvergence;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
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
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
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

    private LiveViewCheckpointFunctionCompiler() {
    }

    /**
     * Builds the fixed segment boundary an anchored live view resets on, or null when
     * the anchor has none or does not govern the whole view. The anchor counterpart of
     * {@link #rangePlan} and {@link #rowsPlan}: those read the frame descriptors the
     * window functions carry, this one reads the anchor expression the definition
     * carries, because an anchored window's dependency is the segment rather than any
     * function's frame.
     * <p>
     * The recognized shape is a calendar-period floor of the base's designated
     * timestamp, in the two forms the definition can hold it:
     * <ul>
     *     <li>{@code timestamp_floor('<stride><unit>', ts)} - a hand-written
     *     {@code ANCHOR EXPRESSION}, and equally what {@code ANCHOR DAILY} at UTC
     *     midnight desugars to. The buckets are epoch-aligned;</li>
     *     <li>{@code timestamp_floor('1d', ts, <origin>)} - what {@code ANCHOR DAILY}
     *     at any other time of day desugars to. The origin is a constant the AST
     *     carries as a cast expression, so this reads it from the definition's own
     *     {@code anchorDailyTimeUs} instead of folding the node, which is also why the
     *     three-argument form is accepted only for a DAILY anchor.</li>
     * </ul>
     * Everything else declines, including the time-zone-aware daily anchor: it desugars
     * to {@code timestamp_floor_utc}, whose buckets change width at a DST transition and
     * so have no closed-form end. Declining costs a view only the localized repair path.
     * <p>
     * The segment arithmetic is only half the contract. It bounds a repair because the
     * anchor resets state at every boundary, so the plan is withheld unless
     * {@link #isAnchorSegmentLocal} confirms that every <i>anchored</i> window function
     * is one the anchor actually resets. A bounded ROWS/RANGE window declared beside the
     * anchored one keeps sliding across bucket crossings and is no more segment-local
     * than the anchor is frame-local, so it is not this plan's to bound - it is
     * {@link #rowsPlan}'s or {@link #rangePlan}'s, and
     * {@link #isDependencyComplete} is what proves the three plans between them cover
     * the whole factory.
     *
     * @param spec                      the definition's captured anchored window
     * @param anchorNode                the parsed anchor expression, already desugared
     *                                  for a DAILY anchor
     * @param projectedMetadata         the metadata the anchor expression resolves
     *                                  against - the same one the runtime anchor
     *                                  function is compiled with
     * @param windowFunctions           every window function the compiled factory
     *                                  carries
     * @param anchorableWindowFunctions the subset the anchor dispatches
     *                                  {@code resetPartition} to
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
                || !Chars.equalsIgnoreCase(anchorNode.token, TimestampFloorFunctionFactory.NAME)
                || !isAnchorSegmentLocal(windowFunctions, anchorableWindowFunctions)) {
            return null;
        }
        final ExpressionNode unitNode;
        final ExpressionNode timestampNode;
        final long segmentOffset;
        final int timestampType = projectedMetadata.getTimestampType();
        if (anchorNode.paramCount == 2) {
            // Two children live in lhs/rhs, in the order they were written.
            unitNode = anchorNode.lhs;
            timestampNode = anchorNode.rhs;
            segmentOffset = 0;
        } else if (anchorNode.paramCount == 3 && anchorNode.args.size() == 3
                && spec.anchorKind == WindowExpression.ANCHOR_KIND_DAILY) {
            // Three or more children live in args, inverted, so the first written
            // argument is the last entry.
            unitNode = anchorNode.args.getQuick(2);
            timestampNode = anchorNode.args.getQuick(1);
            if (!ColumnType.isTimestamp(timestampType)) {
                return null;
            }
            segmentOffset = ColumnType.getTimestampDriver(timestampType)
                    .from(spec.anchorDailyTimeUs, ColumnType.TIMESTAMP_MICRO);
        } else {
            return null;
        }
        if (timestampNode == null || timestampNode.type != ExpressionNode.LITERAL
                || timestampNode.token == null
                || SqlUtil.getColumnIndexQuiet(projectedMetadata, timestampNode.token) != timestampIndex) {
            return null;
        }
        return segmentPlan(unitNode, segmentOffset, timestampType);
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
        // The kind of a stateless function follows the function rather than the frame, because
        // the frame is precisely what such a function does not read: last_value over
        // ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW and over ROWS BETWEEN 10 PRECEDING
        // AND CURRENT ROW compile to one class whose computeNext reads the row it was handed.
        final boolean isStateless = function.isCheckpointStateless();
        final DependencyKind kind = isStateless
                ? DependencyKind.STATELESS_CURRENT_ROW
                : dependencyKind(function.getName(), window);
        final boolean keyed = function.getCheckpointKeyColumnTypes() != null;
        // The RANGE kind is only assigned to a W PRECEDING frame ending at or below the
        // current row, and SqlCodeGenerator has already run validateRange() over every
        // live-view window expression, so such a frame is known to be ordered by the
        // designated timestamp ascending and its width is safe to read as a timestamp offset.
        final boolean isRange = kind == DependencyKind.RANGE_W_PRECEDING_BOUNDED_HI;
        // A fixed row-count state extent the function declares for itself, independent of its
        // frame - lag's offset. Long.MIN_VALUE means it has none and the frame decides the extent.
        final long rowsStateExtentOverride = function.checkpointRowsStateExtentOverride();
        final boolean hasRowsStateExtentOverride = rowsStateExtentOverride != Long.MIN_VALUE;
        final long frameLo;
        final long frameHi;
        final long stateExtentLo;
        if (isStateless) {
            // Zeros throughout rather than the bounds the user wrote: the declared frame
            // describes no row this function reads and no bound a repair derives, and a zero
            // needs no unit to be commensurable with the other two.
            frameLo = 0;
            frameHi = 0;
            stateExtentLo = 0;
        } else if (hasRowsStateExtentOverride && !isRange) {
            // lag ignores its frame outright: it reads the row `offset` back and emits at the
            // current row, whatever ROWS frame it was written over. Describe it as exactly that -
            // low bound and extent both -offset, high bound the current row - so the ROWS
            // discovery bounds it the way it bounds an accumulator over
            // ROWS BETWEEN offset PRECEDING AND CURRENT ROW. The declared frame's own bounds (a
            // lagging high bound, an EXCLUDE clause) are decorative for lag and are not carried.
            frameLo = rowsStateExtentOverride;
            frameHi = 0;
            stateExtentLo = rowsStateExtentOverride;
        } else {
            frameLo = isRange
                    ? rangeFrameLo(function.getName(), window, timestampType)
                    : window.getRowsLo();
            // Both RANGE bounds carry the unit the user wrote, so both go through the same
            // conversion, and the descriptor holds two commensurable timestamp offsets. A ROWS
            // frame counts rows at either end and carries no unit, so its bounds stay as the
            // model records them.
            frameHi = isRange
                    ? rangeFrameHi(function.getName(), window, timestampType)
                    : effectiveRowsHi(window);
            // An accumulator's state is the frame's own contents, so the look-behind that feeds
            // the frame is also the one a warm-up replays and the extent is the frame's low
            // bound. last_value reads a single row instead, the one its high bound names, so its
            // extent is that lag however far back the frame nominally starts.
            stateExtentLo = hasHighBoundStateExtent(function.getName(), window, frameHi)
                    ? frameHi
                    : frameLo;
        }
        // A function whose extent is a fixed row count (lag) cannot bound a RANGE frame, whose
        // repair works in timestamp width: decline the frame-local claim there so rangePlan
        // falls back to the from-boundary rebuild rather than localize against wrong units.
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
                codecIdentity
        );
        function.setCheckpointCompilerMetadata(identity, dependency);
    }

    /**
     * Whether the three dependency plans between them cover every window function in the
     * factory, which is what lets a repair bound them all at once.
     * <p>
     * Each plan describes the functions of its own kind and says nothing about the
     * others: {@link #rangePlan} bounds the finite RANGE frames, {@link #rowsPlan} the
     * finite ROWS ones, {@link #anchorPlan} the anchored ones. A localized repair takes
     * the union - the earliest {@code L} and the latest {@code H} any of them proves -
     * and a union is only a bound over the view's output when every function that
     * produced that output is inside it. One uncovered function is therefore a whole
     * repair declined: the replacement over {@code [R, H)} is timestamp-global, so it
     * re-emits every function's output from the same replay, and a function whose state
     * that replay cannot reconstruct would be re-emitted wrong.
     * <p>
     * A function is uncovered when its kind has no plan (its own contract failed, or the
     * factory's functions of that kind disagreed on the key/order domain), when it
     * carries no dependency contract at all - a window function without checkpoint state
     * support - or when its kind is one no plan can bound. A factory with no window
     * function answers false: there is nothing for a dependency contract to be about.
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
            // The RANGE plan describes a stateless function too, at the zero width its empty
            // extent proves. Such a function always contributes that arm, so the plan is
            // missing here only when another RANGE function in the same factory declined it -
            // and then nothing localizes, which is the answer this returns.
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
     * Builds the union of the finite RANGE dependencies a streaming live-view factory
     * carries, or null when it carries none or one of them fails its own contract. The
     * plan describes the RANGE functions and only those - a factory mixing them with ROWS
     * or anchored windows still gets one, and {@link #isDependencyComplete} is what
     * decides whether the three plans together bound the whole view. Two RANGE functions
     * must nevertheless agree on the key/order domain, so a later planner can never
     * combine incompatible descriptors into one repair interval.
     * <p>
     * The frame shape is necessary but not sufficient: every RANGE function must also hold
     * {@link LiveViewCheckpointDependency#hasFrameLocalState() frame-local state}, since a
     * repair warms up over the declared state extent and nothing below it. The domain check
     * still runs for a factory a non-frame-local function declines, so an incompatible pair
     * is named at CREATE either way.
     * <p>
     * A {@link LiveViewCheckpointDependency#isStateless() stateless} function joins the union
     * as a zero-width arm, which is why this plan and not a fourth one describes it. Zero is
     * the identity of the width the union maximizes, so such a function never widens the
     * interval another one proves; the bounds it is left with when it is all the factory
     * carries are {@code L = R} and {@code H = changeMaxTs + 1}, which is what its empty state
     * extent proves. It sits outside the domain check for the same reason - a function reading
     * one row agrees with every key and order domain - so declaring one beside a RANGE window
     * over a different domain is not a compile error.
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
                // A zero-width arm. It widens nothing - zero is the identity of the width
                // union - and it takes no part in the domain check either, because a function
                // that reads one row agrees with every key and order domain there is. What it
                // does is make the plan exist for a view carrying nothing else.
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
        // The domain the plan reports is a RANGE function's when the factory has one; a
        // stateless-only factory reports the one its own function was declared over, which
        // nothing downstream reads - the repair takes only the width - and which keeps the
        // plan's fields describing a window the view actually carries.
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
     * Builds the union of the finite ROWS dependencies a streaming live-view factory
     * carries, or null when the view has no bounded ROWS repair contract. The ROWS
     * counterpart of {@link #rangePlan}, with one deliberate difference: an unusable
     * shape declines the plan instead of failing the compile.
     * <p>
     * That asymmetry is the point. {@code validateRange} already narrowed what
     * {@code CREATE LIVE VIEW} accepts for RANGE, so a mismatched RANGE domain is a
     * shape the product has decided to reject. Every ROWS shape below is accepted today
     * and must stay accepted; declining the plan costs such a view only the localized
     * repair path, which is what it has now.
     * <p>
     * Like the RANGE plan, this describes the finite ROWS functions and only those; a
     * mixed factory still gets one, and {@link #isDependencyComplete} decides whether the
     * plans together bound the whole view. The plan is declined when a ROWS function does
     * not hold {@link LiveViewCheckpointDependency#hasFrameLocalState() frame-local state}
     * and so reads rows the warm-up over {@code [L, R)} never feeds it, when two of them
     * disagree on the key/order domain, when a window is not ordered by the designated
     * timestamp ascending (the row positions {@code Nmax} counts would then be positions
     * in an order the replay's cursor does not produce), or when the frame is keyless or
     * zero-wide.
     * <p>
     * A PARTITION BY term that is not a plain base column is projected through a compiled
     * function instead, so an expression key costs the view its index seek rather than its
     * repair bound (see {@link LiveViewCheckpointRowsPlan}). The one expression the plan
     * still declines is a {@link Function#isNonDeterministic() non-deterministic} one: the
     * two searches read the same row from two cursors and have to land on the same key
     * both times, and a key that answers {@code now()} does not.
     *
     * @param baseMetadata     the base factory's metadata, which the key projector's
     *                         columns, its key functions and the designated timestamp are
     *                         resolved against
     * @param configuration    for the projector's codegen
     * @param asm              the compiler's bytecode assembler
     * @param functionParser   compiles an expression key into the plan's own function,
     *                         separate from the copy the window runtime partitions by
     * @param executionContext the live-view compile context the key functions parse under
     */
    @Nullable
    public static LiveViewCheckpointRowsPlan rowsPlan(
            @NotNull ObjList<Function> functions,
            @NotNull ObjList<QueryColumn> columns,
            @NotNull RecordMetadata baseMetadata,
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

        // An empty PARTITION BY leaves nothing to count per key, and a zero-wide frame
        // has no look-behind to bound. Neither is reachable through a checkpoint-capable
        // function today - both compile to scalar window functions that carry no
        // checkpoint state - so declining them costs no view its repair path.
        if (firstRows == null || partitionBy.size() == 0 || maxPrecedingRows < 1) {
            return null;
        }
        final IntList partitionByColumnIndexes = new IntList(partitionBy.size());
        final ListColumnFilter keyColumnFilter = new ListColumnFilter(partitionBy.size());
        final ArrayColumnTypes keyColumnTypes = new ArrayColumnTypes();
        // The second projector's shape: what the view's window functions key their own
        // maps by, which is this list with every SYMBOL resolved to a STRING. Left null
        // while no key column is a SYMBOL, because the two projectors are then the same
        // one and generating a second sink would buy nothing.
        ArrayColumnTypes checkpointKeyColumnTypes = null;
        BitSet writeSymbolAsString = null;
        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            final ExpressionNode node = partitionBy.getQuick(i);
            final int columnIndex = node.type == ExpressionNode.LITERAL
                    ? SqlUtil.getColumnIndexQuiet(baseMetadata, node.token)
                    : -1;
            if (columnIndex == -1) {
                // One term the sink cannot read off a page-frame record puts every term on
                // a key function, so the projector stays one shape rather than two halves
                // whose SYMBOL keys would live in different spaces.
                return expressionKeyedPlan(
                        partitionBy,
                        firstRows,
                        rowsFunctionCount,
                        maxPrecedingRows,
                        timestampIndex,
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
        // No writeSymbolAsString is set, so a SYMBOL key column is projected as its
        // table-local integer. That is stable for one reader's lifetime, which is exactly
        // the scope one repair plans and replays in.
        final RecordSink keySink = RecordSinkFactory.getInstance(configuration, asm, baseMetadata, keyColumnFilter);
        // The checkpoint projector does set it, because the key it writes has to compare
        // equal to the one a window function's partition map already holds, and those
        // maps key a SYMBOL partition column by its resolved string. A plan with no
        // SYMBOL key column needs no second sink at all.
        final RecordSink checkpointKeySink = writeSymbolAsString == null
                ? keySink
                : RecordSinkFactory.getInstance(configuration, asm, baseMetadata, keyColumnFilter, writeSymbolAsString);
        return new LiveViewCheckpointRowsPlan(
                rowsFunctionCount,
                maxPrecedingRows,
                firstRows.getPartitionSignature(),
                firstRows.getOrderSignature(),
                partitionByColumnIndexes,
                null,
                keyColumnTypes,
                keySink,
                checkpointKeyColumnTypes != null ? checkpointKeyColumnTypes : keyColumnTypes,
                checkpointKeySink,
                timestampIndex,
                firstRows.getTimestampType()
        );
    }

    /**
     * Compiles the fused window-state plan for a live view: the accumulator components
     * its durable state is made of, the projections that read them, and the residual
     * functions that keep their own legacy roots. Returns null when nothing in the
     * factory can join a fused group, which is an ordinary answer and leaves every
     * function exactly where it is today.
     * <p>
     * Six things have to hold of a function before it may contribute a component, and
     * each is a separate way of not being fusible:
     * <ul>
     *     <li>it is one of the functions the anchor resets. {@code SqlCodeGenerator}
     *     has already collected that subset on the frame shape plus the anchor's
     *     ownership of it, so membership here is both tests at once, read off the same
     *     list the runtime dispatches through;</li>
     *     <li>its dependency is the anchor segment and its per-partition state is one
     *     the anchor can put back to identity;</li>
     *     <li>it holds whole-state, non-ring checkpoint state - a ring-backed function's
     *     root names chunk pages rather than an image, and a stateless one has no image
     *     at all;</li>
     *     <li>that image is fixed width and fits the per-component inline budget;</li>
     *     <li>it declares an accumulator family and a projection off it;</li>
     *     <li>its argument is a direct compiled column reference of a type whose
     *     contribution predicate {@link LiveViewAccumulatorDescriptor} can name - or,
     *     for a family that takes no argument, it has no argument at all.</li>
     * </ul>
     * Anything else is a residual. In particular an argument reached through an implicit
     * cast is not a direct column reference. {@code count(*)} joins as a
     * {@link LiveViewAccumulatorDescriptor#FAMILY_ROW_COUNT row count} rather than as a
     * {@code count(x)}, and the two never merge: one counts rows and the other counts a
     * column's non-null values, which agree only on data where that column is never
     * null.
     * <p>
     * One {@code count(x)} nevertheless joins that row count, and it is the case the
     * window rather than the argument settles: {@code x} being the very column the window
     * partitions by makes it constant across a partition, so the call reads
     * {@code partition-key-is-null ? 0 : rowCount}. See
     * {@link #isCountOverTheWindowsOwnPartitionKey}, which is also where the four
     * conditions that keep the equality honest are set out.
     * <p>
     * Sharing is proved from the component identity alone and never from SQL text.
     * {@code sum(x)} and {@code avg(x)} over one window merge because both declare the
     * same {@code (family, argument, contribution predicate, codec)}, and a
     * {@code count(x)} over the same argument folds onto their counter because that
     * counter is provably the one it would have persisted alone. The target shape's
     * {@code sum(amt)} and {@code count(acct)} do neither, because their arguments
     * differ and so, on any row where exactly one is null, do their counters. Which of
     * the two relations applies is {@link LiveViewWindowStatePlan.Builder}'s to decide;
     * this method's job is to hand it a component only where every part of the identity
     * can be named.
     *
     * @param functions                 every SELECT-list function, in output order
     * @param anchorableWindowFunctions the subset the anchor dispatches
     *                                  {@code resetPartition} to, or null for a factory
     *                                  with no anchored window
     * @param baseMetadata              the metadata the window functions and their
     *                                  arguments were compiled against
     */
    public static @Nullable LiveViewWindowStatePlan windowStatePlan(
            @NotNull ObjList<Function> functions,
            @Nullable ObjList<WindowFunction> anchorableWindowFunctions,
            @NotNull RecordMetadata baseMetadata
    ) {
        if (anchorableWindowFunctions == null || anchorableWindowFunctions.size() == 0) {
            return null;
        }
        // Read before a single projection is added, because whether a count over the
        // partition key may join a row count depends on the group holding an unguarded
        // reading of that same row count - and the count may precede it in the SELECT
        // list. Everything else about a projection follows from the function alone.
        final WindowFunction rowCountHost = rowCountHost(functions, anchorableWindowFunctions);
        final LiveViewWindowStatePlan.Builder builder = new LiveViewWindowStatePlan.Builder();
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
                    rowCountHost
            )) {
                builder.addResidual(windowFunction);
            }
        }
        return builder.build();
    }

    /**
     * Validates the ordering domain of a {@code RANGE W PRECEDING} frame ending at or below
     * the current row - the RANGE shapes whose forward influence boundary {@code H} follows
     * from timestamp arithmetic, and therefore the only ones this phase plans a localized
     * repair against. The width is meaningless unless the frame is ordered by the designated
     * timestamp ascending, so a frame that claims the shape but orders by something else is
     * turned away at CREATE rather than silently given a bound that does not describe it.
     * <p>
     * Every other RANGE shape - an unbounded look-behind, a {@code FOLLOWING} bound, an
     * exclusion mode the runtime does not implement - keeps its existing behavior. Those
     * frames simply do not produce a finite RANGE descriptor, so no repair plan claims them;
     * narrowing what a live view accepts is a separate, deliberate scope decision.
     * <p>
     * The caller runs this for every live-view window expression, before the function itself
     * is parsed, so an unsupported frame is named at its own position rather than surfacing
     * later as a missing-checkpoint-metadata failure.
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
     * Offers {@code function} to the fused group, and reports whether it joined. Every
     * rejection below is an ordinary answer: the caller records the function as a
     * residual and it keeps the legacy root it has today.
     */
    private static boolean addAccumulatorProjection(
            LiveViewWindowStatePlan.Builder builder,
            WindowFunction function,
            int outputPosition,
            ObjList<WindowFunction> anchorableWindowFunctions,
            RecordMetadata baseMetadata,
            @Nullable WindowFunction rowCountHost
    ) {
        if (!isFusibleAccumulator(function, anchorableWindowFunctions)) {
            return false;
        }
        int projectionKind = function.checkpointAccumulatorProjection();
        if (projectionKind == LiveViewAccumulatorProjection.PROJECTION_NONE) {
            return false;
        }
        int family = function.checkpointAccumulatorFamily();
        int argumentColumnIndex;
        int argumentColumnType;
        if (LiveViewAccumulatorDescriptor.familyTakesArgument(family)) {
            argumentColumnIndex = directColumnIndex(function.checkpointAccumulatorArgument(), baseMetadata);
            if (argumentColumnIndex < 0) {
                return false;
            }
            argumentColumnType = baseMetadata.getColumnType(argumentColumnIndex);
            if (isCountOverTheWindowsOwnPartitionKey(
                    function,
                    family,
                    projectionKind,
                    argumentColumnIndex,
                    argumentColumnType,
                    baseMetadata,
                    rowCountHost
            )) {
                family = LiveViewAccumulatorDescriptor.FAMILY_ROW_COUNT;
                projectionKind = LiveViewAccumulatorProjection.PROJECTION_COUNT_PARTITION_KEY;
                argumentColumnIndex = LiveViewAccumulatorDescriptor.NO_ARGUMENT_COLUMN_INDEX;
                argumentColumnType = ColumnType.UNDEFINED;
            }
        } else {
            // A row-count component counts rows and nothing about a column, so its
            // identity has no argument to carry. A function that declares the family and
            // still holds an argument is describing state the identity does not, and is
            // turned away rather than fused under a key that omits it.
            if (function.checkpointAccumulatorArgument() != null) {
                return false;
            }
            argumentColumnIndex = LiveViewAccumulatorDescriptor.NO_ARGUMENT_COLUMN_INDEX;
            argumentColumnType = ColumnType.UNDEFINED;
        }
        final LiveViewAccumulatorDescriptor component = LiveViewAccumulatorDescriptor.of(
                family,
                argumentColumnIndex,
                argumentColumnType
        );
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
                projectionKind,
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
     * Classifies a frame into the dependency kind whose bounds a repair can prove, reading
     * the high bound the runtime evaluates rather than the one the model records - see
     * {@link #effectiveRowsHi}.
     * <p>
     * The two eligible kinds admit any high bound at or below the current row, not just the
     * current row itself. A frame that ends {@code V} below its own row reads a subset of
     * what the same-width frame ending at that row reads, so the bounds both plans derive
     * from the look-behind alone stay valid: the RANGE floor {@code R - W} still feeds every
     * base row the frame admits, the RANGE ceiling {@code changeMaxTs + W + 1} still sits
     * above every output a changed row can reach, and the ROWS discovery still converges
     * from a key's {@code (Nmax + 1)}-th row above the change because a lagging bound only
     * removes rows from the affected set. Both are looser than a lagging frame needs, which
     * widens the repair interval and never narrows it.
     * <p>
     * The look-behind the eligible kinds require is the state extent's rather than the
     * frame's, which is why {@link #hasFiniteStateLookBehind} rather than the frame's own low
     * bound decides it: {@code last_value} over a ROWS frame reads the one row its high bound
     * names, so an unbounded frame start still leaves a finite extent and both bounds follow
     * from it.
     * <p>
     * A {@code FOLLOWING} high bound is what must keep falling through to
     * {@code FOLLOWING_OR_DATA_DEPENDENT}: a base row at {@code m} then joins the frame of
     * output below {@code m}, and neither bound holds. That case stays a visible branch here
     * rather than a sign folded into the eligible test.
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
        // Long.MIN_VALUE is the encoding an unbounded look-behind uses, and
        // SqlOptimiser.normalizeWindowFrame() reaches it on the high bound too - a literal
        // Long.MAX_VALUE PRECEDING negates into it, leaving a frame that ends below its own
        // start. Such a bound names no finite lag, so it is turned away here alongside the
        // unbounded frame starts.
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
     * Returns the frame's high bound as the runtime evaluates it, which is what the
     * descriptor has to describe. {@link WindowExpression#getRowsHi()} answers the raw model
     * value, so an {@code EXCLUDE CURRENT ROW} frame still reads {@code 0} there even though
     * {@code WindowContextImpl.getRowsHi()} rewrites it to {@code -1} before any factory sees
     * it - the runtime frame ends one unit below the current row, one tick for RANGE and one
     * row for ROWS.
     * <p>
     * Reading the model value instead leaves the descriptor claiming a frame the runtime does
     * not evaluate, and it stops being harmless the moment a bound is derived from
     * {@link LiveViewCheckpointDependency#getFrameHi()} itself. Folding the exclusion into the
     * high bound here is also what lets {@link #dependencyKind} classify the exclusion with
     * one test on both arms rather than turning it away on the RANGE arm alone: an
     * {@code EXCLUDE CURRENT ROW} frame is a lagging high bound with the smallest possible lag,
     * and the eligible kinds already admit those.
     * <p>
     * {@link #hasSupportedExclusion} is what keeps the other two exclusion modes out; this
     * method describes only the one the runtime turns into a frame adjustment.
     */
    private static long effectiveRowsHi(WindowExpression window) {
        return effectiveRowsHi(window, window.getRowsHi());
    }

    /**
     * Resolves {@code argument} to the base column it reads, or {@code -1} when it is
     * not a direct compiled column reference of that column's own type.
     * <p>
     * The type check is not redundant with the {@code instanceof}. It is what keeps the
     * argument key's type the one the runtime really evaluates the predicate against, so
     * a column function carrying a type its base column does not have cannot key a
     * component under the wrong contribution semantics.
     * <p>
     * A signature match handing a narrower column straight to a wider factory - a LONG
     * column reaching {@code sum(D)} and {@code count(D)} - passes both halves, because
     * no cast wrapper is inserted and the column function is still the column's own
     * type. Whether that widening is fusible is not this method's answer but
     * {@link LiveViewAccumulatorDescriptor#contributionKindFor}'s, which names a
     * predicate only for the types it has been proved over.
     */
    private static int directColumnIndex(@Nullable Function argument, RecordMetadata baseMetadata) {
        if (!(argument instanceof ColumnFunction columnFunction)) {
            return -1;
        }
        final int index = columnFunction.getColumnIndex();
        if (index < 0 || index >= baseMetadata.getColumnCount()) {
            return -1;
        }
        return argument.getType() == baseMetadata.getColumnType(index) ? index : -1;
    }

    /**
     * Folds {@code EXCLUDE CURRENT ROW} into a high bound a caller has already converted into
     * the designated timestamp's units, which is the order the runtime applies the two
     * adjustments in: {@code WindowContextImpl.of()} converts the unit, and its
     * {@code getRowsHi()} rewrites the {@code 0} afterwards.
     * <p>
     * That order is what the {@code -1} requires. It is already a normalized quantity - one
     * tick for RANGE, one row for ROWS - so a conversion running after it would scale it a
     * second time. Converting first happens to agree today because {@code CURRENT ROW} carries
     * no time unit and {@link #rangeFrameBound} returns early without one, but the two stop
     * agreeing the moment the parser attaches a unit to a bound that reads as zero.
     */
    private static long effectiveRowsHi(WindowExpression window, long rowsHi) {
        return window.getExclusionKind() == WindowExpression.EXCLUDE_CURRENT_ROW && rowsHi == 0
                ? -1
                : rowsHi;
    }

    /**
     * Builds the projector of a view whose PARTITION BY holds at least one expression, by
     * compiling every term into a key function of the plan's own. The window runtime keeps
     * its separate copies, so the two never share evaluation state.
     * <p>
     * There is no index seek on this path and no column list to name one: the plan's
     * column indexes stay empty, and the discovery falls back to the unrestricted backward
     * walk. The key types follow what the generated sink writes rather than what the
     * function returns - a SYMBOL-typed key function is written through its resolved
     * string, because the integers it hands out index its own map rather than the
     * reader's, and two cursors would not agree on them.
     * <p>
     * The functions are freed unless the plan takes ownership of them, so a parse failure,
     * a declined key or a codegen failure leaves nothing behind.
     */
    private static @Nullable LiveViewCheckpointRowsPlan expressionKeyedPlan(
            ObjList<ExpressionNode> partitionBy,
            LiveViewCheckpointDependency firstRows,
            int rowsFunctionCount,
            long maxPrecedingRows,
            int timestampIndex,
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
                    // The forward pass and the backward search read the same base row from
                    // two cursors, so a key that answers differently each time would count
                    // one row's predecessors against another row's key.
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
            final LiveViewCheckpointRowsPlan plan = new LiveViewCheckpointRowsPlan(
                    rowsFunctionCount,
                    maxPrecedingRows,
                    firstRows.getPartitionSignature(),
                    firstRows.getOrderSignature(),
                    null,
                    keyFunctions,
                    keyColumnTypes,
                    keySink,
                    // The expression projector already resolves a SYMBOL key function
                    // through its string, which is what a window function's own map
                    // holds, so the checkpoint form is the same projector.
                    keyColumnTypes,
                    keySink,
                    timestampIndex,
                    firstRows.getTimestampType()
            );
            keyFunctions = null;
            return plan;
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
     * Whether the frame names a finite look-behind for the function's <i>state</i>, which is
     * what the eligible kinds need and what a warm-up replays. For an accumulator that is the
     * frame's own low bound; for a function whose state the high bound bounds it is the lag,
     * and the frame's start does not participate.
     * <p>
     * Keeping the two arms apart is what confines the widening to the second: an unbounded
     * frame start still leaves every accumulator with no floor to discover, and its own arm
     * turns it away here exactly as before.
     */
    private static boolean hasFiniteStateLookBehind(CharSequence functionName, WindowExpression window, long rowsHi) {
        return hasHighBoundStateExtent(functionName, window, rowsHi)
                || (window.getRowsLo() != Long.MIN_VALUE && window.getRowsLo() <= 0);
    }

    /**
     * Whether the function's state extent is the frame's high bound rather than its low one.
     * <p>
     * This reads the function and not just the frame, which inverts the rule the CREATE-time
     * reject follows, and the inversion is the point. A frame bounds what an accumulator
     * depends on, so reading the frame alone covers every accumulator written later without
     * listing any of them. {@code last_value} does not accumulate: it emits the single row its
     * high bound names, so the lag is the whole of what a warm-up has to replay and the
     * frame's start says nothing about it. Reading {@code -frameHi} for an accumulator sharing
     * that same frame would under-replay it and emit a wrong value, which is why the test
     * names the function.
     * <p>
     * Three things narrow it to the shape whose state really is the ring of the last
     * {@code K} values. {@code IGNORE NULLS} scans the whole frame for the last non-null and
     * is bounded by the frame's start like any accumulator. A high bound at the current row -
     * as the runtime evaluates it, so an {@code EXCLUDE CURRENT ROW} frame is a lag of one
     * rather than one of these - leaves no ring at all: that shape compiles to a stateless
     * per-row projection, which carries no checkpoint surface and never reaches this compiler.
     * <p>
     * And a RANGE frame ends at a timestamp offset rather than at a row, so its lag names no
     * row for this to read. Over an unbounded start the emitted value is the newest base row
     * at or below {@code t - V}, and a row inserted at {@code m} moves every output from
     * {@code m + V} up to the {@code + V} of the next base row that supersedes it - a distance
     * the data sets rather than the lag. Rows at 0s, 100s and 200s under a one-second lag put
     * a change at 50s and the moved output at 100s, which neither {@code changeMaxTs + V + 1}
     * nor any other closed form off {@code V} reaches; the state runs as far back as that
     * superseded row, so the floor misses it too. The RANGE family therefore keeps the
     * CREATE-time reject over an unbounded start. What it does have is the bounded start,
     * where the frame's own width bounds both sides and the extent stays {@code -frameLo} -
     * that arm needs nothing from this method.
     * <p>
     * The function's own {@link WindowFunction#hasFrameLocalCheckpointState()} stands behind
     * this: the partitioned ROWS-frame {@code last_value} implementations declare it outright
     * and the RANGE-frame ones declare it only for a bounded frame start, so a shape this
     * admits that compiles to some other class declines the plan rather than taking one
     * against an extent it does not hold.
     */
    private static boolean hasHighBoundStateExtent(CharSequence functionName, WindowExpression window, long rowsHi) {
        return rowsHi < 0
                && window.getFramingMode() == WindowExpression.FRAMING_ROWS
                && !window.isIgnoreNulls()
                && Chars.equalsIgnoreCase(functionName, "last_value");
    }

    /**
     * Whether the frame exclusion is one the window runtime implements.
     * {@code WindowContextImpl.validate()} accepts {@code EXCLUDE NO OTHERS} and
     * {@code EXCLUDE CURRENT ROW} and rejects the other two, so a descriptor claiming an
     * {@code EXCLUDE GROUP} or {@code EXCLUDE TIES} frame would describe a frame no factory
     * evaluates. The two the runtime does implement need no separate handling here:
     * {@link #effectiveRowsHi} has already folded {@code EXCLUDE CURRENT ROW} into the high
     * bound, which is the whole of what it does to the frame.
     */
    private static boolean hasSupportedExclusion(WindowExpression window) {
        return window.getExclusionKind() == WindowExpression.EXCLUDE_NO_OTHERS
                || window.getExclusionKind() == WindowExpression.EXCLUDE_CURRENT_ROW;
    }

    /**
     * Whether the anchor's segment determines the state of every <i>anchored</i> window
     * function in the factory, which is what lets a repair reconstruct that state by
     * replaying one segment. Two independent things have to hold of each of them, and
     * each has its own way of failing:
     * <ul>
     *     <li>the anchor actually resets it. The runtime dispatches
     *     {@code resetPartition} only to the functions whose frame is
     *     {@code UNBOUNDED PRECEDING ... CURRENT ROW}, so an anchored function outside
     *     that subset carries state reaching below the segment start;</li>
     *     <li>its state is keyed and resettable. A per-partition state the anchor cannot
     *     put back to identity is not described by the reset the replay relies on, and
     *     {@code supportsKeyReset} is the descriptor for exactly that.</li>
     * </ul>
     * A bounded ROWS/RANGE window declared beside the anchored one keeps sliding across
     * bucket crossings, so it is not the segment's to bound - its own frame plan bounds
     * it, and {@link #isDependencyComplete} checks that one exists. A factory with no
     * anchored function at all answers false: there is nothing for the anchor to be a
     * dependency contract over.
     * <p>
     * The containment is checked in both directions. Forward: every anchored function has
     * to be in the subset, or the anchor does not in fact reset it. Reverse: every member
     * of the subset has to be anchored or checkpoint-stateless, because a member that is
     * neither carries per-partition state the anchor resets without owning - the segment
     * then does not describe that function's state either, and the replay a repair builds
     * on it would rehydrate the wrong values. {@code SqlCodeGenerator} already collects on
     * exactly that rule; this is the checkpoint side refusing to plan over a subset that
     * broke it.
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
     * Whether the window's rows arrive in the order both repair bounds are expressed
     * in: the base table's designated timestamp, ascending. A frame ordered by anything
     * else counts row positions the replay's own ts-ordered cursor does not reproduce,
     * so neither the RANGE width nor the ROWS count describes the frame the user asked
     * for.
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
     * Whether {@code function} is a {@code count(k)} whose argument is the single column
     * its own window partitions by, and whose group already holds an unguarded reading of
     * the row count it would join.
     * <p>
     * Every row of a partition carries the same {@code k}, so such a call's counter is
     * the partition's row count wherever {@code k} is present and zero where it is not.
     * That makes it a
     * {@link LiveViewAccumulatorProjection#PROJECTION_COUNT_PARTITION_KEY guarded reading}
     * of the {@link LiveViewAccumulatorDescriptor#FAMILY_ROW_COUNT} component
     * {@code count(*)} and {@code row_number()} maintain, rather than a counter of its
     * own - which is what takes {@code count(*) + count(k)} from two components to one.
     * <p>
     * Four things narrow it, and each closes a way the equality could fail:
     * <ul>
     *     <li><b>The argument's contribution predicate is the type's own null test.</b>
     *     A widened-to-double argument contributes on {@code Numbers.isFinite}, so a
     *     partition keyed by a DOUBLE {@code +Infinity} would be a present key that the
     *     count does not count. SYMBOL and VARCHAR are the two types whose predicate is
     *     exactly "the key is absent";</li>
     *     <li><b>the partition key is one term and that term is the argument's own
     *     column</b>, proved through {@link #directColumnIndex} rather than through the
     *     rendered partition signature - two spellings of one column are still one
     *     column, and one spelling of two is not;</li>
     *     <li><b>the encoded key is one column too</b>, so the guard has one value to
     *     read rather than a tuple whose null-ness is a different question;</li>
     *     <li><b>the group holds a row-count projection that is not itself guarded.</b>
     *     That projection is what the component's contributor will be, and it is the
     *     only kind that maintains a true row count. Without it the component would be
     *     maintained by the guarded count alone - correct for every output the group has
     *     today, but a leaf carrying a partition's {@code count(k)} where the manifest
     *     says row count, which a later recompile adding {@code count(*)} would read
     *     back at face value under the very same manifest.</li>
     * </ul>
     */
    private static boolean isCountOverTheWindowsOwnPartitionKey(
            WindowFunction function,
            int family,
            int projectionKind,
            int argumentColumnIndex,
            int argumentColumnType,
            RecordMetadata baseMetadata,
            @Nullable WindowFunction rowCountHost
    ) {
        if (family != LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT
                || projectionKind != LiveViewAccumulatorProjection.PROJECTION_COUNT
                || rowCountHost == null) {
            return false;
        }
        final int tag = ColumnType.tagOf(argumentColumnType);
        if (tag != ColumnType.SYMBOL && tag != ColumnType.VARCHAR) {
            return false;
        }
        final ColumnTypes keyColumnTypes = function.getCheckpointKeyColumnTypes();
        if (keyColumnTypes == null || keyColumnTypes.getColumnCount() != 1) {
            return false;
        }
        final ObjList<? extends Function> partitionBy = function.checkpointPartitionByFunctions();
        if (partitionBy == null || partitionBy.size() != 1) {
            return false;
        }
        if (directColumnIndex(partitionBy.getQuick(0), baseMetadata) != argumentColumnIndex) {
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
     * Whether two functions belong to one fused window group: the same named window, the
     * same partition and order domains, and the same encoded partition-key layout. It is
     * the test {@code LiveViewWindowStatePlan.Builder} applies when a projection joins,
     * read here from the two identities directly because the row-count pre-pass runs
     * before any of them has been offered to the builder.
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
        // These functions maintain floating accumulators whose add/remove order can
        // leave an allowed sub-ULP suffix difference after localized replay. The type
        // check is what confines the tolerance to that case: the same functions over a
        // DECIMAL add and subtract scaled integers, which is exact, so their state and
        // output converge bit-for-bit and they stay EXACT here.
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
     * Converts one RANGE frame bound from the unit the user wrote into the designated
     * timestamp's native units, repeating exactly the conversion {@code WindowContextImpl.of()}
     * applies to build the runtime frame. Both bounds run through here because the descriptor
     * is only sound while it and the compiled frame agree bit-for-bit, and because a
     * descriptor mixing a converted low bound with a raw high one holds two numbers that
     * cannot be compared to each other.
     * <p>
     * Two ways of writing a bound produce a runtime frame that is not the one the user asked
     * for, and the compiler turns both away rather than checkpointing a view against a frame
     * nobody wrote:
     * <ul>
     *     <li>a bound wider than the timestamp's units can carry, which {@code from()} wraps or
     *     narrows onto a legal-looking width. The ceiling the test reads is
     *     {@link TimestampDriver#getMaxUnitValue(char)}, the same primitive
     *     {@code WindowContextImpl.of()} guards the compiled frame with - one ceiling serves
     *     both, because two parallel range checks would agree only by inspection;</li>
     *     <li>a bound finer than the timestamp's resolution, which divides down to zero. The
     *     runtime accepts that one: it rounds the frame rather than corrupting it, and a query
     *     asking for a nanosecond lag over a micros column keeps working. A live view cannot,
     *     because the descriptor would record a dependency the user did not ask for and the
     *     repair would then be bounded by it.</li>
     * </ul>
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
     * Resolves the descriptor's {@code frameHi} to the negated finite lag {@code V} the frame
     * ends at, in the designated timestamp's native units, with {@code EXCLUDE CURRENT ROW}
     * folded in after the conversion - see {@link #effectiveRowsHi(WindowExpression, long)}
     * for why that order is the one that holds.
     * <p>
     * A frame ending at the current row carries no unit and reaches 0 unchanged, so the
     * conversion costs nothing for the shape that was admitted before the lagging ones were.
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
     * The first function of {@code functions} that would join the group as an unguarded
     * {@link LiveViewAccumulatorDescriptor#FAMILY_ROW_COUNT row count} - a
     * {@code count(*)} or a partitioned {@code row_number()} - or null when the factory
     * holds none.
     * <p>
     * It is the host a {@code count} over the window's own partition key may join, and
     * the reason the search runs before any projection is added: such a count may precede
     * its host in the SELECT list, and whether it fuses must not depend on that.
     * <p>
     * The gates are the same ones {@link #addAccumulatorProjection} applies, so this
     * cannot name a host that walk would decline. Its own arm is deliberately narrow: the
     * family must be the argumentless one and the function must actually hold no argument,
     * which is what keeps a {@code count(x)} from being read as a row count here.
     */
    private static @Nullable WindowFunction rowCountHost(
            ObjList<Function> functions,
            ObjList<WindowFunction> anchorableWindowFunctions
    ) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            final Function function = functions.getQuick(i);
            if (!(function instanceof WindowFunction windowFunction)
                    || !isFusibleAccumulator(windowFunction, anchorableWindowFunctions)
                    || windowFunction.checkpointAccumulatorFamily() != LiveViewAccumulatorDescriptor.FAMILY_ROW_COUNT
                    || windowFunction.checkpointAccumulatorArgument() != null
                    || windowFunction.checkpointAccumulatorProjection() == LiveViewAccumulatorProjection.PROJECTION_NONE
                    || windowFunction.checkpointFunctionIdentity() == null) {
                continue;
            }
            final LiveViewAccumulatorDescriptor component = LiveViewAccumulatorDescriptor.of(
                    LiveViewAccumulatorDescriptor.FAMILY_ROW_COUNT,
                    LiveViewAccumulatorDescriptor.NO_ARGUMENT_COLUMN_INDEX,
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
     * Reads a {@code timestamp_floor} period literal - {@code '1d'}, {@code '15m'} - into
     * the segment plan it describes, or null when the token is not one. The literal is the
     * function's own first argument, so this repeats the split
     * {@code TimestampFloorFunctionFactory} applies to it: a trailing unit character, and an
     * optional leading count that defaults to one. A non-constant, unquoted, or
     * unparseable token declines rather than throws - an anchor this cannot read is an
     * anchor with no fixed segment, which is an answer rather than a compile error.
     */
    private static LiveViewCheckpointAnchorPlan segmentPlan(
            ExpressionNode unitNode,
            long segmentOffset,
            int timestampType
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
        return LiveViewCheckpointAnchorPlan.of(unit, stride, segmentOffset, timestampType);
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
