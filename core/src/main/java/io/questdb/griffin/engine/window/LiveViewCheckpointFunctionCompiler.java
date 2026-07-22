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
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.lv.LiveViewCheckpointAnchorPlan;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.DependencyKind;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointDependency.NumericConvergence;
import io.questdb.cairo.lv.LiveViewCheckpointDependency.StructuralConvergence;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointRangePlan;
import io.questdb.cairo.lv.LiveViewCheckpointRowsBounds;
import io.questdb.cairo.lv.LiveViewCheckpointRowsPlan;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.date.TimestampFloorFunctionFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.WindowExpression;
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
        final DependencyKind kind = dependencyKind(function.getName(), window);
        final boolean keyed = function.getCheckpointKeyColumnTypes() != null;
        // The RANGE kind is only assigned to a W PRECEDING ... CURRENT ROW frame, and
        // SqlCodeGenerator has already run validateRange() over every live-view window
        // expression, so such a frame is known to be ordered by the designated timestamp
        // ascending and its width is safe to read as a timestamp offset.
        final long frameLo = kind == DependencyKind.RANGE_W_PRECEDING_CURRENT_ROW
                ? rangeFrameLo(function.getName(), window, timestampType)
                : window.getRowsLo();
        final LiveViewCheckpointDependency dependency = new LiveViewCheckpointDependency(
                kind,
                partitionSignature,
                orderSignature,
                frameLo,
                effectiveRowsHi(window),
                timestampType,
                function.hasFrameLocalCheckpointState(),
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
            if (dependency.isFiniteRange()) {
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
     * repair warms up over the frame's extent and nothing below it. The domain check still
     * runs for a factory a non-frame-local function declines, so an incompatible pair is
     * named at CREATE either way.
     */
    @Nullable
    public static LiveViewCheckpointRangePlan rangePlan(
            @NotNull ObjList<Function> functions,
            @NotNull ObjList<QueryColumn> columns
    ) throws SqlException {
        LiveViewCheckpointDependency firstRange = null;
        LiveViewCheckpointFunctionIdentity firstIdentity = null;
        boolean allFrameLocal = true;
        int rangeFunctionCount = 0;
        long maxFrameWidth = 0;

        for (int i = 0, n = functions.size(); i < n; i++) {
            final Function function = functions.getQuick(i);
            if (!(function instanceof WindowFunction windowFunction)) {
                continue;
            }
            final LiveViewCheckpointDependency dependency = windowFunction.checkpointDependency();
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

        if (!allFrameLocal || firstRange == null) {
            return null;
        }
        return new LiveViewCheckpointRangePlan(
                rangeFunctionCount,
                maxFrameWidth,
                firstRange.getPartitionSignature(),
                firstRange.getOrderSignature(),
                firstRange.getTimestampType()
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
            partitionByColumnIndexes.add(columnIndex);
            keyColumnFilter.add(columnIndex + 1);
            keyColumnTypes.add(baseMetadata.getColumnType(columnIndex));
        }
        // No writeSymbolAsString is set, so a SYMBOL key column is projected as its
        // table-local integer. That is stable for one reader's lifetime, which is exactly
        // the scope one repair plans and replays in.
        final RecordSink keySink = RecordSinkFactory.getInstance(configuration, asm, baseMetadata, keyColumnFilter);
        return new LiveViewCheckpointRowsPlan(
                rowsFunctionCount,
                maxPrecedingRows,
                firstRows.getPartitionSignature(),
                firstRows.getOrderSignature(),
                partitionByColumnIndexes,
                null,
                keyColumnTypes,
                keySink,
                timestampIndex,
                firstRows.getTimestampType()
        );
    }

    /**
     * Validates the ordering domain of a {@code RANGE W PRECEDING ... CURRENT ROW} frame -
     * the one RANGE shape whose forward influence boundary {@code H} follows from timestamp
     * arithmetic, and therefore the only one this phase plans a localized repair against.
     * The width is meaningless unless the frame is ordered by the designated timestamp
     * ascending, so a frame that claims the shape but orders by something else is turned
     * away at CREATE rather than silently given a bound that does not describe it.
     * <p>
     * Every other RANGE shape - a frame ending before the current row, an unbounded
     * look-behind, a FOLLOWING bound, a frame exclusion - keeps its existing behavior. Those
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
        if (dependencyKind(functionName, window) == DependencyKind.RANGE_W_PRECEDING_CURRENT_ROW) {
            validateRangeOrder(functionName, window, baseMetadata);
        }
    }

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
        if (window.getRowsLo() != Long.MIN_VALUE && window.getRowsLo() <= 0 && rowsHi == 0) {
            if (window.getFramingMode() == WindowExpression.FRAMING_ROWS) {
                return DependencyKind.ROWS_N_PRECEDING_CURRENT_ROW;
            }
            if (window.getFramingMode() == WindowExpression.FRAMING_RANGE
                    && window.getRowsLoKind() == WindowExpression.PRECEDING
                    && window.getRowsHiKind() == WindowExpression.CURRENT
                    && window.getExclusionKind() == WindowExpression.EXCLUDE_NO_OTHERS) {
                return DependencyKind.RANGE_W_PRECEDING_CURRENT_ROW;
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
     * not evaluate, and the two kinds disagree about it: the RANGE arm of
     * {@link #dependencyKind} turns the exclusion away through its
     * {@code EXCLUDE_NO_OTHERS} test while the ROWS arm admits it and records
     * {@code frameHi = 0}. That over-states the ROWS frame, which is conservative rather than
     * wrong - a repair bound derived from a wider frame still covers the narrower one - but it
     * is accidental, and it stops being harmless the moment a bound is derived from
     * {@link LiveViewCheckpointDependency#getFrameHi()} itself.
     * <p>
     * {@code WindowExpression} carries no other exclusion mode this far: only
     * {@code EXCLUDE NO OTHERS} and {@code EXCLUDE CURRENT ROW} pass
     * {@code WindowContextImpl.validate()}.
     */
    private static long effectiveRowsHi(WindowExpression window) {
        return window.getExclusionKind() == WindowExpression.EXCLUDE_CURRENT_ROW && window.getRowsHi() == 0
                ? -1
                : window.getRowsHi();
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
     */
    private static boolean isAnchorSegmentLocal(
            ObjList<WindowFunction> windowFunctions,
            ObjList<WindowFunction> anchorableWindowFunctions
    ) {
        if (anchorableWindowFunctions == null) {
            return false;
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

    private static boolean isRanking(CharSequence name) {
        return Chars.equalsIgnoreCase(name, "row_number")
                || Chars.equalsIgnoreCase(name, "rank")
                || Chars.equalsIgnoreCase(name, "dense_rank");
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
     * Resolves the descriptor's {@code frameLo} to the negated finite RANGE width {@code W}
     * in the designated timestamp's native units. {@link WindowExpression#getRowsLo()} still
     * carries the width in the units the user wrote, so this repeats exactly the conversion
     * {@code WindowContextImpl.of()} applies to build the runtime frame - the repair floor
     * {@code L = R - W} is only sound while the descriptor and the compiled frame agree
     * bit-for-bit.
     */
    private static long rangeFrameLo(
            CharSequence functionName,
            WindowExpression window,
            int timestampType
    ) throws SqlException {
        final long rowsLo = window.getRowsLo();
        final char unit = window.getRowsLoExprTimeUnit();
        if (unit == 0 || !ColumnType.isTimestamp(timestampType)) {
            return rowsLo;
        }
        final long frameLo = ColumnType.getTimestampDriver(timestampType).from(rowsLo, unit);
        // from(long, char) yields 0 for an unrecognized unit, and narrows minutes/hours/days
        // to int, so a width beyond that range silently collapses to zero or flips sign.
        // Such a frame has no usable dependency bound - reject it instead of checkpointing a
        // view whose runtime frame is not the one the user asked for.
        if (frameLo >= 0 && rowsLo < 0) {
            final int position = window.getRowsLoExprPos() > 0
                    ? window.getRowsLoExprPos()
                    : window.getAst().position;
            throw SqlException.$(position, "live view RANGE width is out of range for the designated timestamp [function=")
                    .put(functionName).put("(), width=").put(-rowsLo).put(unit).put(']');
        }
        return frameLo;
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
