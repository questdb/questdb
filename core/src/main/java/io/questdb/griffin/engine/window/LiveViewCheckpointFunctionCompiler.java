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
import io.questdb.cairo.lv.LiveViewCheckpointContracts.DependencyKind;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointDependency.NumericConvergence;
import io.questdb.cairo.lv.LiveViewCheckpointDependency.StructuralConvergence;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointRangePlan;
import io.questdb.cairo.lv.LiveViewCheckpointRowsBounds;
import io.questdb.cairo.lv.LiveViewCheckpointRowsPlan;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Compiler-only builder for stable live-view checkpoint function metadata. */
public final class LiveViewCheckpointFunctionCompiler {
    private static final String STATE_PAGE_CODEC_FAMILY = "live-view-state-page";

    private LiveViewCheckpointFunctionCompiler() {
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
                window.getRowsHi(),
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
     * Builds the union of the finite RANGE dependencies a streaming live-view factory
     * carries, or null when any window function is not a finite RANGE - a mixed
     * ROWS/anchor factory stays valid but has no RANGE repair plan. Two RANGE functions
     * must nevertheless agree on the key/order domain even in a mixed factory, so a later
     * planner can never combine incompatible descriptors into one repair interval.
     * <p>
     * The frame shape is necessary but not sufficient: every function must also hold
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
        boolean allRange = true;
        int rangeFunctionCount = 0;
        long maxFrameWidth = 0;

        for (int i = 0, n = functions.size(); i < n; i++) {
            final Function function = functions.getQuick(i);
            if (!(function instanceof WindowFunction windowFunction)) {
                continue;
            }
            final LiveViewCheckpointDependency dependency = windowFunction.checkpointDependency();
            if (dependency == null || !dependency.isFiniteRange()) {
                allRange = false;
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

        if (!allRange || !allFrameLocal || firstRange == null) {
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
     * The plan is declined when any window function is not a finite ROWS frame (a mixed
     * ROWS/RANGE factory has no single contract), when a function does not hold
     * {@link LiveViewCheckpointDependency#hasFrameLocalState() frame-local state} and so
     * reads rows the warm-up over {@code [L, R)} never feeds it, when two functions
     * disagree on the key/order domain, when a window is not ordered by the designated
     * timestamp ascending (the row positions {@code Nmax} counts would then be positions
     * in an order the replay's cursor does not produce), when the frame is keyless or
     * zero-wide, or when a PARTITION BY expression is not a plain base column. The last
     * is a projector limitation rather than a contract one:
     * {@link LiveViewCheckpointRowsBounds} reads keys straight out of a page-frame
     * record, and an arbitrary expression would need the window's own partition-by
     * functions rebound to the discovery cursor and back.
     *
     * @param baseMetadata  the base factory's metadata, which the key projector's column
     *                      indexes and the designated timestamp are resolved against
     * @param configuration for the projector's codegen
     * @param asm           the compiler's bytecode assembler
     */
    @Nullable
    public static LiveViewCheckpointRowsPlan rowsPlan(
            @NotNull ObjList<Function> functions,
            @NotNull ObjList<QueryColumn> columns,
            @NotNull RecordMetadata baseMetadata,
            @NotNull CairoConfiguration configuration,
            @NotNull BytecodeAssembler asm
    ) {
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
            if (dependency == null || !dependency.isFiniteRows() || !dependency.hasFrameLocalState()) {
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
            if (node.type != ExpressionNode.LITERAL) {
                return null;
            }
            final int columnIndex = SqlUtil.getColumnIndexQuiet(baseMetadata, node.token);
            if (columnIndex == -1) {
                return null;
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
        if (isRanking(functionName)
                && window.getRowsLo() == Long.MIN_VALUE
                && window.getRowsHi() == Long.MAX_VALUE) {
            return DependencyKind.UNANCHORED_RANK;
        }
        if (window.getRowsLo() != Long.MIN_VALUE && window.getRowsLo() <= 0 && window.getRowsHi() == 0) {
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
        if (window.getRowsLo() == Long.MIN_VALUE && window.getRowsHi() == 0) {
            return DependencyKind.UNBOUNDED_CUMULATIVE_NO_RESET;
        }
        return DependencyKind.FOLLOWING_OR_DATA_DEPENDENT;
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
        // leave an allowed sub-ULP suffix difference after localized replay.
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
