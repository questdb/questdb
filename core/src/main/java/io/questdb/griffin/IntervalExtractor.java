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

package io.questdb.griffin;

import io.questdb.ParanoiaState;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.model.DateExpressionEvaluator;
import io.questdb.griffin.model.IntervalOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.model.RuntimeIntervalModelBuilder;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Interval;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.StringSink;

/**
 * Timestamp bounds for a single generated scan occurrence.
 */
public final class IntervalExtractor implements Mutable {
    private static final int MAX_SPECULATIVE_SCALAR_BOUND_DEPTH = 4;
    private final CairoConfiguration configuration;
    private final StringSink intervalText;
    private final Interval intervalValue = new Interval();
    private final RuntimeIntervalModelBuilder intervals = new RuntimeIntervalModelBuilder() {
        @Override
        protected Function compileTickExpr(CharSequence seq, int lo, int lim, int position) throws SqlException {
            return bounds.compileTickExpr(ColumnType.getTimestampDriver(timestampType), configuration, seq, lo, lim, position);
        }
    };
    private final Interval inverted = new Interval();
    private final LongList parsedIntervals;
    private IntervalAnalysis analysis;
    private IntervalBoundSource bounds;
    private boolean isBoundSpeculationAllowed;
    private boolean isIntrinsicFalse;
    private boolean isNestedOffset;
    private IntervalExtractor offsetIntervals;
    private int timestampType;

    public IntervalExtractor(CairoConfiguration configuration, StringSink intervalText, LongList parsedIntervals) {
        this.configuration = configuration;
        this.intervalText = intervalText;
        this.parsedIntervals = parsedIntervals;
    }

    /**
     * A string literal spelled in SQL as itself rather than folded from an expression.
     */
    public static boolean isStringLiteral(ConstantExpression constant) {
        return isQuotedLiteral(constant) && constant.getDataType() == ColumnType.STRING;
    }

    public RuntimeIntrinsicIntervalModel build(int partitionBy) {
        intervals.of(timestampType, partitionBy, configuration);
        return intervals.hasIntervalFilters() ? intervals.build() : null;
    }

    @Override
    public void clear() {
        isIntrinsicFalse = false;
        final Throwable failure = Misc.clearBestEffort(null, offsetIntervals);
        intervals.clear();
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Returns only the conjuncts the intervals do not implement.
     *
     * @param depth the generation depth of the scan's query level, which caps sub-query bound speculation
     */
    public BoundExpression extract(BoundExpression predicate, int timestampColumnId, OutputSchema input, IntervalBoundSource bounds,
                                   BoundExpressionRewriter rewriter, int depth, SqlExecutionContext executionContext) throws SqlException {
        final int timestampType = input.getColumnType(input.getColumnIndexById(timestampColumnId));
        final BoundExpression residual = extract(predicate, timestampColumnId, timestampType, depth < MAX_SPECULATIVE_SCALAR_BOUND_DEPTH,
                bounds, input, rewriter, executionContext);
        if (ParanoiaState.PLAN_PARANOIA_MODE) {
            verifyAnalysis(predicate, timestampColumnId, timestampType, depth, rewriter, residual);
        }
        return residual;
    }

    public boolean isIntrinsicFalse() {
        return isIntrinsicFalse;
    }

    /**
     * Merges the static intervals of a window join master with the same timestamp type, see
     * {@link RuntimeIntervalModelBuilder#merge(TimestampDriver, LongList, long, long)}.
     */
    public void merge(LongList masterIntervals, long lo, long hi) {
        intervals.merge(ColumnType.getTimestampDriver(timestampType), masterIntervals, lo, hi);
        isIntrinsicFalse |= intervals.isEmptySet();
    }

    public void of(int timestampType) {
        clear();
        this.timestampType = timestampType;
        intervals.of(timestampType, PartitionBy.NONE, configuration);
    }

    /**
     * Intersects the timestamp range the execution context imposes on the table, such as a mat view refresh range.
     */
    public void override(TableToken tableToken, SqlExecutionContext executionContext) {
        executionContext.overrideWhereIntervals(tableToken, intervals, timestampType);
        isIntrinsicFalse |= intervals.isEmptySet();
    }

    private static long constantBoundValue(ConstantExpression constant, TimestampDriver driver) throws NumericException {
        final int type = constant.getDataType();
        if (ColumnType.isTimestamp(type)) {
            // Text truncates fractional digits; a typed negative epoch truncates toward zero.
            return constant.getTimestampText() != null ? driver.parseFloorLiteral(constant.getTimestampText())
                    : driver.from(constant.getLongValue(), ColumnType.getTimestampType(type));
        }
        return switch (type) {
            case ColumnType.NULL -> Numbers.LONG_NULL;
            case ColumnType.INT -> Numbers.intToLong((int) constant.getLongValue());
            default -> constant.getLongValue();
        };
    }

    private static CharSequence constantText(ConstantExpression constant) {
        return constant.getDataType() == ColumnType.VARCHAR
                ? constant.getVarcharValue() == null ? null : constant.getVarcharValue().asAsciiCharSequence()
                : constant.getStrValue();
    }

    private static FunctionExpression findProjectedOffset(BoundExpression expression) {
        if (!(expression instanceof FunctionExpression call)) {
            return null;
        }
        if (call.isProjectedOffset()) {
            return call;
        }
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            final FunctionExpression offset = findProjectedOffset(call.argumentAt(i));
            if (offset != null) {
                return offset;
            }
        }
        return null;
    }

    private static boolean hasMixedProjectedOffsets(BoundExpression expression, FunctionExpression offset) {
        if (!(expression instanceof FunctionExpression call)) {
            return false;
        }
        if (call.isProjectedOffset()) {
            return !isSameOffset(call, offset);
        }
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            if (hasMixedProjectedOffsets(call.argumentAt(i), offset)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A constant BETWEEN bound finer than a computed operand: binding rounds such a bound only when the
     * other bound is a constant too, so this one stays in the filter, which compares it exactly.
     */
    private static boolean isFinerBound(BoundExpression bound, int operandType) {
        return bound instanceof ConstantExpression && ColumnType.isTimestampNano(bound.getDataType())
                && !ColumnType.isTimestampNano(operandType);
    }

    private static boolean isIntegralType(int type) {
        final int tag = ColumnType.tagOf(type);
        return tag == ColumnType.INT || tag == ColumnType.LONG || tag == ColumnType.SHORT || tag == ColumnType.BYTE;
    }

    private static boolean isLosslessPrecisionBound(ColumnExpression column, ConstantExpression constant, int timestampType) {
        if (!column.isDirectReference() || column.getDataType() != timestampType
                || !ColumnType.isTimestamp(constant.getDataType()) || constant.getDataType() == timestampType
                || constant.getTimestampText() != null || constant.getLongValue() == Numbers.LONG_NULL) {
            return false;
        }
        final long value = constant.getLongValue();
        return ColumnType.isTimestampNano(timestampType)
                ? value > Long.MIN_VALUE / Micros.MICRO_NANOS && value < Long.MAX_VALUE / Micros.MICRO_NANOS
                : value % Micros.MICRO_NANOS == 0;
    }

    private static boolean isMonotonicOperand(BoundExpression expression) {
        return expression instanceof FunctionExpression
                || expression instanceof ColumnExpression column && !column.isDirectReference();
    }

    private static boolean isQuotedLiteral(ConstantExpression constant) {
        return constant.isLiteral() && constant.getSource() == null;
    }

    private static boolean isSameOffset(BoundExpression left, BoundExpression right) {
        if (left instanceof ColumnExpression l && right instanceof ColumnExpression r) {
            return l.getColumnId() == r.getColumnId();
        }
        return left instanceof FunctionExpression l && right instanceof FunctionExpression r
                && l.isProjectedOffset() && r.isProjectedOffset()
                && ((ConstantExpression) l.argumentAt(0)).getLongValue() == ((ConstantExpression) r.argumentAt(0)).getLongValue()
                && ((ConstantExpression) l.argumentAt(1)).getLongValue() == ((ConstantExpression) r.argumentAt(1)).getLongValue()
                && isSameOffset(l.argumentAt(2), r.argumentAt(2));
    }

    private static boolean isSameResidual(BoundExpression residual, BoundExpression other) {
        return residual == other || residual instanceof FunctionExpression call && other instanceof FunctionExpression that
                && call.isAnd() && that.isAnd() && isSameResidual(call.argumentAt(0), that.argumentAt(0))
                && isSameResidual(call.argumentAt(1), that.argumentAt(1));
    }

    private static boolean isTimestampCursor(CursorExpression cursor) {
        final OutputSchema output = cursor.getPlan().getOutput();
        return !cursor.isBoolean() && output.getColumnCount() == 1 && ColumnType.isTimestamp(output.getColumnType(0));
    }

    // A literal bound must parse as a date; a computed bound must cast to TIMESTAMP.
    private static void validateRangeBound(BoundExpression bound) throws SqlException {
        if (bound instanceof ConstantExpression constant && constant.getSource() == null) {
            if (constant.getLiteralText() != null) {
                throw SqlException.invalidDate(constant.getLiteralText(), bound.getPosition());
            }
            return;
        }
        if (bound instanceof ColumnExpression || bound instanceof CursorExpression) {
            return;
        }
        switch (ColumnType.tagOf(bound.getDataType())) {
            case ColumnType.UNDEFINED, ColumnType.NULL, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.STRING,
                 ColumnType.SYMBOL,
                 ColumnType.INT, ColumnType.LONG, ColumnType.VARCHAR -> {
            }
            default -> throw SqlException.invalidDate(bound.getPosition());
        }
    }

    private Function boundFunction(BoundExpression bound, OutputSchema input, SqlExecutionContext executionContext) throws SqlException {
        return bound instanceof CursorExpression cursor ? bounds.instantiateTimestampCursor(cursor, executionContext)
                : bounds.instantiate(bound, input, executionContext);
    }

    private BoundExpression extract(BoundExpression predicate, int timestampColumnId, int timestampType, boolean isBoundSpeculationAllowed,
                                    IntervalBoundSource bounds, OutputSchema input, BoundExpressionRewriter rewriter,
                                    SqlExecutionContext executionContext) throws SqlException {
        of(timestampType);
        this.isBoundSpeculationAllowed = isBoundSpeculationAllowed;
        this.bounds = bounds;
        return intersect(predicate, timestampColumnId, input, rewriter, executionContext);
    }

    private BoundExpression intersect(BoundExpression predicate, int timestampColumnId, OutputSchema input,
                                      BoundExpressionRewriter rewriter, SqlExecutionContext executionContext) throws SqlException {
        if (!(predicate instanceof FunctionExpression call)) {
            return predicate;
        }
        if (!call.isAnd()) {
            final FunctionExpression offset = findProjectedOffset(call);
            if (offset != null) {
                return intersectOffset(call, offset, timestampColumnId, input, rewriter, executionContext);
            }
        }
        final String operator = call.getName();
        if ("between".equals(operator) && call.getArgumentCount() == 3) {
            return intersectBetween(call, false, timestampColumnId, input, executionContext);
        }
        if ("in".equals(operator) && call.getArgumentCount() >= 2) {
            return intersectIn(call, false, timestampColumnId, input, executionContext);
        }
        if ("not".equals(operator) && call.getArgumentCount() == 1
                && call.argumentAt(0) instanceof FunctionExpression negated) {
            final BoundExpression remaining;
            if ("between".equals(negated.getName()) && negated.getArgumentCount() == 3) {
                remaining = intersectBetween(negated, true, timestampColumnId, input, executionContext);
            } else if ("in".equals(negated.getName()) && negated.getArgumentCount() >= 2) {
                remaining = intersectIn(negated, true, timestampColumnId, input, executionContext);
            } else {
                return predicate;
            }
            return remaining == null ? null : predicate;
        }
        if (call.getArgumentCount() != 2) {
            return predicate;
        }
        if (call.isAnd()) {
            final BoundExpression left;
            final BoundExpression right;
            // Match intrinsic extraction's right-leaf-first order: an earlier
            // interval prevents a later OR from becoming a union.
            if (call.argumentAt(1) instanceof FunctionExpression nested && nested.isAnd()) {
                left = intersect(call.argumentAt(0), timestampColumnId, input, rewriter, executionContext);
                right = intersect(call.argumentAt(1), timestampColumnId, input, rewriter, executionContext);
            } else {
                right = intersect(call.argumentAt(1), timestampColumnId, input, rewriter, executionContext);
                left = intersect(call.argumentAt(0), timestampColumnId, input, rewriter, executionContext);
            }
            // Rewrite descriptions only. The original preparation still owns its
            // complete closure; generation independently instantiates the residual.
            return rewriter.replaceConjunction(call, left, right);
        }
        if (call.isOr()) {
            // A union must not widen bounds already extracted from another conjunct.
            if (!intervals.hasIntervalFilters() && isTimestampUnion(call, timestampColumnId, timestampType)) {
                unionTimestampPredicates(call, input, executionContext);
                return null;
            }
            return predicate;
        }
        final BoundExpression left = call.argumentAt(0);
        final BoundExpression right = call.argumentAt(1);
        final BoundExpression bound;
        final boolean isReversed;
        if (left instanceof ColumnExpression column && column.getColumnId() == timestampColumnId) {
            bound = right;
            isReversed = false;
        } else if (right instanceof ColumnExpression column && column.getColumnId() == timestampColumnId) {
            bound = left;
            isReversed = true;
        } else {
            return intersectMonotonic(call, timestampColumnId, input, executionContext);
        }
        if (left instanceof ColumnExpression l && right instanceof ColumnExpression r
                && l.getColumnId() == r.getColumnId() && l.isDirectReference() && r.isDirectReference()) {
            switch (operator) {
                case "=" -> {
                    return null;
                }
                case "!=", "<>" -> {
                    intersectEmpty();
                    return null;
                }
                default -> {
                    return predicate;
                }
            }
        }
        final ColumnExpression column = (ColumnExpression) (isReversed ? right : left);
        if (column.isDirectReference() && column.getDataType() == timestampType
                && (operator.startsWith("<") || operator.startsWith(">")) && !"<>".equals(operator)) {
            validateRangeBound(bound);
        }
        final boolean isIntegralBound = bound instanceof ConstantExpression && column.isDirectReference()
                && column.getDataType() == timestampType && isIntegralType(bound.getDataType());
        final boolean isCursorBound = bound instanceof CursorExpression cursor && isTimestampCursor(cursor)
                && column.isDirectReference() && column.getDataType() == timestampType;
        final boolean isRuntimeTimestampBound = isCursorBound || !(bound instanceof ConstantExpression) && column.isDirectReference()
                && column.getDataType() == timestampType
                && (ColumnType.isTimestamp(bound.getDataType()) || isIntegralType(bound.getDataType()))
                && (bound.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) != 0;
        final long convertedBound = bound instanceof ConstantExpression constantBound && isLosslessPrecisionBound(column, constantBound, timestampType)
                ? ColumnType.getTimestampDriver(timestampType).from(constantBound.getLongValue(), ColumnType.getTimestampType(bound.getDataType()))
                : Numbers.LONG_NULL;
        // Precision conversion can round or overflow; retain those constant bounds in the filter.
        if (!isIntegralBound && !isRuntimeTimestampBound && convertedBound == Numbers.LONG_NULL
                && (left.getDataType() != timestampType || right.getDataType() != timestampType)) {
            if (!column.isDirectReference() && bound instanceof ConstantExpression) {
                return intersectMonotonic(call, timestampColumnId, input, executionContext);
            }
            return predicate;
        }
        final boolean isEquality = "=".equals(operator);
        final boolean isExclusion = "!=".equals(operator) || "<>".equals(operator);
        final boolean isLower;
        final boolean isStrict;
        switch (operator) {
            case "=", "<", "<=" -> {
                isLower = isReversed;
                isStrict = "<".equals(operator);
            }
            case ">", ">=" -> {
                isLower = !isReversed;
                isStrict = ">".equals(operator);
            }
            case "!=", "<>" -> {
                isLower = false;
                isStrict = false;
            }
            default -> {
                return predicate;
            }
        }
        if (bound instanceof ConstantExpression constant) {
            final long value = convertedBound != Numbers.LONG_NULL ? convertedBound
                    : isIntegralBound && ColumnType.tagOf(constant.getDataType()) != ColumnType.LONG
                      ? Numbers.intToLong((int) constant.getLongValue()) : constant.getLongValue();
            if (value == Numbers.LONG_NULL && !isExclusion) {
                if (isNestedOffset) {
                    intersectEmpty();
                    return null;
                }
                return predicate;
            }
            if (isExclusion) {
                intervals.subtractInterval(value, value);
                isIntrinsicFalse |= intervals.isEmptySet();
            } else if (isEquality) {
                intervals.intersect(value, value);
                if (!isIntegralBound && constant.getTimestampText() != null) {
                    isIntrinsicFalse |= intervals.isEmptySet();
                }
            } else {
                intersectBound(value, isLower, isStrict);
            }
        } else if (isCursorBound || (bound.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) != 0) {
            final Function function = isCursorBound ? bounds.instantiateTimestampCursor((CursorExpression) bound, executionContext)
                    : bounds.instantiate(bound, input, executionContext);
            if (isExclusion) {
                intervals.subtractEquals(function, bound.getPosition());
            } else if (isEquality) {
                intervals.intersectRuntimeTimestamp(function, bound.getPosition());
            } else if (isLower) {
                intervals.intersect(function, Long.MAX_VALUE, (short) (isStrict ? 1 : 0), bound.getPosition());
            } else {
                intervals.intersect(Long.MIN_VALUE, function, (short) (isStrict ? -1 : 0), bound.getPosition());
            }
        } else {
            return predicate;
        }
        return null;
    }

    private BoundExpression intersectBetween(
            FunctionExpression predicate,
            boolean negated,
            int timestampColumnId,
            OutputSchema input,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final BoundExpression operand = predicate.argumentAt(0);
        final BoundExpression lo = predicate.argumentAt(1);
        final BoundExpression hi = predicate.argumentAt(2);
        if (isUnusableTimestampBound(lo) || isUnusableTimestampBound(hi)) {
            return predicate;
        }
        if (operand instanceof ColumnExpression column && column.getColumnId() != timestampColumnId) {
            return predicate;
        }
        if (!(operand instanceof ColumnExpression column) || !column.isDirectReference()) {
            if (negated || !isScalarBound(lo) || !isScalarBound(hi)
                    || isFinerBound(lo, operand.getDataType()) || isFinerBound(hi, operand.getDataType())) {
                return predicate;
            }
            return intersectMonotonicRange(predicate, operand, lo, hi, (short) 0, (short) 0,
                    true, timestampColumnId, input, executionContext);
        }
        Throwable failure = null;
        try {
            intervals.setBetweenNegated(negated);
            setBetweenBoundary(lo, input, executionContext);
            setBetweenBoundary(hi, input, executionContext);
            isIntrinsicFalse |= intervals.isEmptySet();
            return null;
        } catch (NumericException e) {
            final SqlException invalidDate = SqlException.invalidDate(predicate.getPosition());
            failure = invalidDate;
            throw invalidDate;
        } catch (Throwable th) {
            failure = th;
            throw th;
        } finally {
            final Throwable closeFailure = intervals.clearBetweenParsing(failure);
            if (failure == null) {
                CairoException.rethrowCleanupFailure(closeFailure);
            }
        }
    }

    private void intersectBound(long value, boolean isLower, boolean isStrict) {
        if (isLower) {
            if (isStrict && value == Long.MAX_VALUE) {
                intersectEmpty();
            } else {
                intervals.intersect(isStrict ? value + 1 : value, Long.MAX_VALUE);
            }
        } else {
            // LONG_NULL was excluded before arithmetic; subtracting one cannot wrap.
            intervals.intersect(Long.MIN_VALUE, isStrict ? value - 1 : value);
        }
    }

    private void intersectEmpty() {
        intervals.intersectEmpty();
        isIntrinsicFalse = true;
    }

    private BoundExpression intersectIn(
            FunctionExpression predicate,
            boolean negated,
            int timestampColumnId,
            OutputSchema input,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (!(predicate.argumentAt(0) instanceof ColumnExpression column)
                || column.getColumnId() != timestampColumnId || column.getDataType() != timestampType
                || negated && !column.isDirectReference()) {
            return negated ? predicate : intersectMonotonicIn(predicate, timestampColumnId, input, executionContext);
        }
        final int count = predicate.getArgumentCount();
        if (count == 2) {
            final BoundExpression bound = predicate.argumentAt(1);
            if (ColumnType.isInterval(bound.getDataType())) {
                return intersectInterval(predicate, bound, negated, input, executionContext);
            }
            if (isUnusableTimestampBound(bound) || bound instanceof CursorExpression) {
                return predicate;
            }
            if (bound instanceof ConstantExpression constant) {
                final CharSequence text = ColumnType.isVarcharOrString(bound.getDataType()) ? constantText(constant) : null;
                if (text != null) {
                    final CharSequence seq = isStringLiteral(constant) ? quote(text) : text;
                    final int lo = seq == text ? 0 : 1;
                    if (negated) {
                        intervals.subtractIntervals(seq, lo, seq.length() - lo, bound.getPosition());
                    } else {
                        intervals.intersectIntervals(seq, lo, seq.length() - lo, bound.getPosition());
                    }
                    isIntrinsicFalse |= intervals.isEmptySet();
                } else {
                    final CharSequence seq = intervalElement(constant);
                    if (seq != null) {
                        final int lo = seq == intervalText ? 1 : 0;
                        if (negated) {
                            intervals.subtractIntervals(seq, lo, seq.length() - lo, bound.getPosition());
                        } else {
                            intervals.intersectIntervals(seq, lo, seq.length() - lo, bound.getPosition());
                        }
                        isIntrinsicFalse |= intervals.isEmptySet();
                        return null;
                    }
                    final long value = timestampValue(constant);
                    if (negated) {
                        intervals.subtractInterval(value, value);
                        isIntrinsicFalse |= intervals.isEmptySet();
                    } else if (value == Numbers.LONG_NULL && ColumnType.isTimestamp(constant.getDataType())) {
                        intersectEmpty();
                    } else {
                        intervals.intersect(value, value);
                    }
                }
            } else {
                final Function function = bounds.instantiate(bound, input, executionContext);
                if (ColumnType.isVarcharOrString(bound.getDataType())) {
                    if (negated) {
                        intervals.subtractRuntimeIntervals(function, bound.getPosition());
                    } else {
                        intervals.intersectRuntimeIntervals(function, bound.getPosition());
                    }
                } else if (negated) {
                    intervals.subtractEquals(function, bound.getPosition());
                } else {
                    intervals.intersectRuntimeTimestamp(function, bound.getPosition());
                }
                isIntrinsicFalse |= intervals.isEmptySet();
            }
            return null;
        }
        if (!negated && intervals.hasIntervalFilters()) {
            return predicate;
        }
        for (int i = 1; i < count; i++) {
            if (!(predicate.argumentAt(i) instanceof ConstantExpression) || isUnusableTimestampBound(predicate.argumentAt(i))) {
                return predicate;
            }
        }
        for (int i = count - 1; i > 0; i--) {
            final ConstantExpression constant = (ConstantExpression) predicate.argumentAt(i);
            final CharSequence seq = intervalElement(constant);
            if (negated) {
                if (seq != null) {
                    final int lo = seq == intervalText ? 1 : 0;
                    intervals.subtractIntervals(seq, lo, seq.length() - lo, constant.getPosition());
                } else {
                    final long value = timestampValue(constant);
                    intervals.subtractInterval(value, value);
                }
                isIntrinsicFalse |= intervals.isEmptySet();
            } else {
                unionElement(constant, seq);
            }
        }
        return null;
    }

    private BoundExpression intersectInterval(
            FunctionExpression predicate,
            BoundExpression bound,
            boolean negated,
            OutputSchema input,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (bound instanceof ConstantExpression constant) {
            final Interval interval = ColumnType.getTimestampDriver(timestampType)
                    .fixInterval(intervalValue.of(constant.getLongValue(), constant.getIntervalHi()), constant.getDataType());
            if (negated) {
                intervals.subtractInterval(interval.getLo(), interval.getHi());
            } else {
                intervals.intersect(interval.getLo(), interval.getHi());
            }
        } else if ((bound.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) != 0) {
            final Function function = bounds.instantiate(bound, input, executionContext);
            if (negated) {
                intervals.subtractRuntimeIntervals(function, bound.getPosition());
            } else {
                intervals.intersectRuntimeIntervals(function, bound.getPosition());
            }
        } else {
            return predicate;
        }
        isIntrinsicFalse |= intervals.isEmptySet();
        return null;
    }

    private BoundExpression intersectMonotonic(
            FunctionExpression predicate,
            int timestampColumnId,
            OutputSchema input,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final String operator = predicate.getName();
        final boolean isEquality = "=".equals(operator);
        final boolean isLess = "<".equals(operator) || "<=".equals(operator);
        final boolean isGreater = ">".equals(operator) || ">=".equals(operator);
        if (!isEquality && !isLess && !isGreater) {
            return predicate;
        }
        final BoundExpression operand;
        final BoundExpression bound;
        final boolean isReversed;
        if (isMonotonicOperand(predicate.argumentAt(0)) && isScalarBound(predicate.argumentAt(1))) {
            operand = predicate.argumentAt(0);
            bound = predicate.argumentAt(1);
            isReversed = false;
        } else if (isMonotonicOperand(predicate.argumentAt(1)) && isScalarBound(predicate.argumentAt(0))) {
            operand = predicate.argumentAt(1);
            bound = predicate.argumentAt(0);
            isReversed = true;
        } else {
            return predicate;
        }
        if (isEquality && bound instanceof CursorExpression) {
            return predicate;
        }
        final boolean isLower = isGreater != isReversed;
        final short adjustment = "<".equals(operator) || ">".equals(operator) ? (short) (isLower ? 1 : -1) : 0;
        return intersectMonotonicRange(predicate, operand, isEquality || isLower ? bound : null,
                isEquality || !isLower ? bound : null, isLower ? adjustment : 0, !isLower ? adjustment : 0,
                false, timestampColumnId, input, executionContext);
    }

    private BoundExpression intersectMonotonicIn(
            FunctionExpression predicate,
            int timestampColumnId,
            OutputSchema input,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (predicate.getArgumentCount() != 2 || !isMonotonicOperand(predicate.argumentAt(0))
                || !ColumnType.isTimestamp(predicate.argumentAt(0).getDataType())
                || !(predicate.argumentAt(1) instanceof ConstantExpression constant)
                || constant.getDataType() != ColumnType.STRING || constant.getStrValue() == null) {
            return predicate;
        }
        final CharSequence text = constant.getStrValue();
        for (int i = 0, n = text.length(); i < n; i++) {
            if (DateExpressionEvaluator.isDateVariable(text, i, n)) {
                return predicate;
            }
        }
        final Function head = bounds.instantiate(predicate.argumentAt(0), input, executionContext);
        Throwable failure = null;
        try {
            if (bounds.monotonicColumnId(predicate.argumentAt(0), head) != timestampColumnId) {
                return predicate;
            }
            parsedIntervals.clear();
            intervalText.clear();
            try {
                IntervalUtils.parseTickExpr(ColumnType.getTimestampDriver(predicate.argumentAt(0).getDataType()), configuration,
                        text, 0, text.length(), constant.getPosition(), parsedIntervals,
                        IntervalOperation.INTERSECT, intervalText, true);
            } catch (SqlException | CairoException e) {
                return predicate;
            }
            if (parsedIntervals.size() != 2) {
                return predicate;
            }
            inverted.of(parsedIntervals.getQuick(0), parsedIntervals.getQuick(1));
            final int soundness;
            try {
                soundness = bounds.invertMonotonic(predicate.argumentAt(0), head, inverted);
            } catch (CairoException e) {
                return predicate;
            }
            if (soundness == MonotonicTimestampFunction.NONE) {
                return predicate;
            }
            if (inverted.getLo() > inverted.getHi()) {
                intersectEmpty();
                return null;
            }
            intervals.intersect(inverted.getLo(), inverted.getHi());
            return soundness == MonotonicTimestampFunction.EXACT ? null : predicate;
        } catch (Throwable th) {
            failure = th;
            throw th;
        } finally {
            final Throwable closeFailure = Misc.freeBestEffort(failure, head);
            if (failure == null) {
                CairoException.rethrowCleanupFailure(closeFailure);
            }
        }
    }

    private BoundExpression intersectMonotonicRange(
            FunctionExpression predicate,
            BoundExpression operand,
            BoundExpression lo,
            BoundExpression hi,
            short loAdjustment,
            short hiAdjustment,
            boolean isBetween,
            int timestampColumnId,
            OutputSchema input,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final int outputType = operand.getDataType();
        final boolean isTimestamp = ColumnType.isTimestamp(outputType);
        if (!isTimestamp && outputType != ColumnType.INT && outputType != ColumnType.LONG
                || !isTimestamp && (lo != null && (ColumnType.isTimestamp(lo.getDataType()) || lo instanceof CursorExpression)
                || hi != null && (ColumnType.isTimestamp(hi.getDataType()) || hi instanceof CursorExpression))) {
            return predicate;
        }
        Function head = bounds.instantiate(operand, input, executionContext);
        Function loFunction = null;
        Function hiFunction = null;
        Throwable failure = null;
        try {
            if (bounds.monotonicColumnId(operand, head) != timestampColumnId) {
                return predicate;
            }
            final TimestampDriver driver = ColumnType.getTimestampDriver(isTimestamp ? outputType : timestampType);
            long loValue = Long.MIN_VALUE;
            long hiValue = Long.MAX_VALUE;
            if (lo instanceof ConstantExpression constant) {
                loValue = constantBoundValue(constant, driver);
                if (loValue == Numbers.LONG_NULL || loAdjustment > 0 && loValue == Long.MAX_VALUE) {
                    intersectEmpty();
                    return null;
                }
                loValue += loAdjustment;
            } else if (lo != null) {
                loFunction = boundFunction(lo, input, executionContext);
            }
            if (hi instanceof ConstantExpression constant) {
                hiValue = constantBoundValue(constant, driver);
                if (hiValue == Numbers.LONG_NULL) {
                    intersectEmpty();
                    return null;
                }
                hiValue += hiAdjustment;
            } else if (hi != null) {
                hiFunction = hi == lo ? loFunction : boundFunction(hi, input, executionContext);
            }
            if (loFunction == null && hiFunction == null) {
                inverted.of(isBetween ? Math.min(loValue, hiValue) : loValue,
                        isBetween ? Math.max(loValue, hiValue) : hiValue);
                final int soundness = bounds.invertMonotonic(operand, head, inverted);
                if (soundness == MonotonicTimestampFunction.NONE) {
                    return predicate;
                }
                if (inverted.getLo() > inverted.getHi()) {
                    intersectEmpty();
                    return null;
                }
                intervals.intersect(inverted.getLo(), inverted.getHi());
                return soundness == MonotonicTimestampFunction.EXACT ? null : predicate;
            }
            inverted.of(Long.MIN_VALUE, Long.MAX_VALUE);
            if (bounds.invertMonotonic(operand, head, inverted) == MonotonicTimestampFunction.NONE) {
                return predicate;
            }
            final Function inverter = bounds.newMonotonicInverter(
                    head, loFunction, loFunction != null ? loAdjustment : 0, loValue,
                    hiFunction, hiFunction != null ? hiAdjustment : 0, hiValue, isBetween, driver
            );
            bounds.publishScalarBound(lo, loFunction);
            if (hi != lo) {
                bounds.publishScalarBound(hi, hiFunction);
            }
            head = loFunction = hiFunction = null;
            intervals.intersectMonotonicTimestamp(inverter);
            isIntrinsicFalse |= intervals.isEmptySet();
            // A runtime value can make inversion decline at cursor open.
            return predicate;
        } catch (NumericException e) {
            return predicate;
        } catch (Throwable th) {
            failure = th;
            throw th;
        } finally {
            Throwable closeFailure = Misc.freeBestEffort(failure, head);
            // The retained residual adopts a declined sub-query bound instead of generating it again.
            if (failure == null) {
                loFunction = bounds.parkDeclinedBound(lo, loFunction);
                hiFunction = hi == lo ? null : bounds.parkDeclinedBound(hi, hiFunction);
            }
            closeFailure = Misc.freeBestEffort(closeFailure, loFunction);
            if (hiFunction != loFunction) {
                closeFailure = Misc.freeBestEffort(closeFailure, hiFunction);
            }
            if (failure == null) {
                CairoException.rethrowCleanupFailure(closeFailure);
            }
        }
    }

    /**
     * Extracts the intervals of the predicate with the innermost offset removed, then shifts them
     * back by that offset, so a chain inverts outermost first. Calendar units clamp the day of month, so their shifted intervals widen and the
     * predicate stays as a residual.
     */
    private BoundExpression intersectOffset(
            FunctionExpression predicate,
            FunctionExpression outerOffset,
            int timestampColumnId,
            OutputSchema input,
            BoundExpressionRewriter rewriter,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (hasMixedProjectedOffsets(predicate, outerOffset)) {
            return predicate;
        }
        FunctionExpression offset = outerOffset;
        while (offset.argumentAt(2) instanceof FunctionExpression inner) {
            offset = inner;
        }
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        final char unit = (char) ((ConstantExpression) offset.argumentAt(0)).getLongValue();
        final int stride = (int) ((ConstantExpression) offset.argumentAt(1)).getLongValue();
        final TimestampDriver.TimestampAddMethod addMethod = driver.getAddMethod(unit);
        if (addMethod == null || stride == Numbers.INT_NULL) {
            return predicate;
        }
        if (offsetIntervals == null) {
            offsetIntervals = new IntervalExtractor(configuration, intervalText, parsedIntervals);
        }
        final IntervalExtractor source = offsetIntervals;
        source.isNestedOffset = true;
        final BoundExpression residual;
        try {
            residual = source.extract(rewriter.unwrapProjectedOffsets(predicate), timestampColumnId, timestampType,
                    isBoundSpeculationAllowed, bounds, input, rewriter, executionContext);
        } catch (Throwable th) {
            Misc.clearBestEffort(th, source);
            throw th;
        }
        if (source.isIntrinsicFalse) {
            source.intervals.freeAndClear();
            intersectEmpty();
            return null;
        }
        if (!source.intervals.hasIntervalFilters()) {
            source.intervals.freeAndClear();
            return residual == null ? null : predicate;
        }
        final boolean isInjective = stride == 0 || unit != 'M' && unit != 'y';
        final long ceiling = isNestedOffset ? Long.MAX_VALUE : driver.getMaxDesignatedTimestamp();
        final boolean isConsumed = intervals.mergeWithAddMethod(source.intervals, addMethod, -stride, isInjective, ceiling);
        if (!isConsumed) {
            source.intervals.freeAndClear();
        }
        isIntrinsicFalse |= intervals.isEmptySet();
        return isConsumed && residual == null && isInjective ? null : predicate;
    }

    /**
     * The spelling of an IN list element that is text spelling intervals rather than a timestamp, a string literal
     * quoted as in SQL, or null for any other element. The binder rejects text that spells neither.
     */
    private CharSequence intervalElement(ConstantExpression constant) {
        final int tag = ColumnType.tagOf(constant.getDataType());
        final CharSequence text = tag == ColumnType.STRING || tag == ColumnType.SYMBOL || tag == ColumnType.VARCHAR
                ? constantText(constant) : null;
        if (text == null) {
            return null;
        }
        try {
            ColumnType.getTimestampDriver(timestampType).parseFloorLiteral(text);
            return null;
        } catch (NumericException e) {
            return isStringLiteral(constant) ? quote(text) : text;
        }
    }

    private boolean isScalarBound(BoundExpression expression) {
        if (expression instanceof CursorExpression cursor) {
            return isBoundSpeculationAllowed && isTimestampCursor(cursor);
        }
        final int type = ColumnType.tagOf(expression.getDataType());
        return (expression instanceof ConstantExpression || (expression.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) != 0)
                && (type == ColumnType.TIMESTAMP || type == ColumnType.INT || type == ColumnType.LONG
                || type == ColumnType.SHORT || type == ColumnType.BYTE || type == ColumnType.NULL);
    }

    private boolean isTimestampPoint(ColumnExpression column, BoundExpression bound, int timestampColumnId, int timestampType) {
        return column.getColumnId() == timestampColumnId && column.getDataType() == timestampType
                && (bound instanceof ConstantExpression && (bound.getDataType() == timestampType || bound.getDataType() == ColumnType.NULL)
                || bound.getDataType() == timestampType && (bound.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) != 0
                || bound instanceof CursorExpression cursor && isBoundSpeculationAllowed && isTimestampCursor(cursor)
                && cursor.getPlan().getOutput().getColumnType(0) == timestampType);
    }

    private boolean isTimestampUnion(BoundExpression expression, int timestampColumnId, int timestampType) {
        if (!(expression instanceof FunctionExpression call)) {
            return false;
        }
        if (call.isOr() && call.getArgumentCount() == 2) {
            return isTimestampUnion(call.argumentAt(0), timestampColumnId, timestampType)
                    && isTimestampUnion(call.argumentAt(1), timestampColumnId, timestampType);
        }
        if ("in".equals(call.getName()) && call.getArgumentCount() >= 2) {
            if (!(call.argumentAt(0) instanceof ColumnExpression column) || !column.isDirectReference()
                    || column.getColumnId() != timestampColumnId || column.getDataType() != timestampType) {
                return false;
            }
            for (int i = 1, n = call.getArgumentCount(); i < n; i++) {
                final BoundExpression bound = call.argumentAt(i);
                if (bound instanceof ConstantExpression) {
                    if (isUnusableTimestampBound(bound) || ColumnType.isTimestamp(bound.getDataType()) && bound.getDataType() != timestampType) {
                        return false;
                    }
                } else if (bound.getDataType() != timestampType
                        || (bound.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) == 0) {
                    return false;
                }
            }
            return true;
        }
        if (!"=".equals(call.getName()) || call.getArgumentCount() != 2) {
            return false;
        }
        final BoundExpression left = call.argumentAt(0);
        final BoundExpression right = call.argumentAt(1);
        if (left instanceof ColumnExpression column) {
            return isTimestampPoint(column, right, timestampColumnId, timestampType);
        }
        return right instanceof ColumnExpression column
                && isTimestampPoint(column, left, timestampColumnId, timestampType);
    }

    private boolean isUnusableTimestampBound(BoundExpression expression) {
        final int type = ColumnType.tagOf(expression.getDataType());
        return !isScalarBound(expression) && (!(expression instanceof ConstantExpression)
                && (expression.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) == 0
                || type != ColumnType.STRING && type != ColumnType.VARCHAR && type != ColumnType.SYMBOL && type != ColumnType.DATE);
    }

    /**
     * The SQL spelling of a string literal. Interval parsing reads a bare number as an epoch only
     * when it is the whole sequence, so a quoted literal spelling such as '1583077401000000' is an
     * invalid date while the same text computed by a function is an epoch.
     */
    private CharSequence quote(CharSequence text) {
        intervalText.clear();
        intervalText.put('\'').put(text).put('\'');
        return intervalText;
    }

    private void setBetweenBoundary(BoundExpression bound, OutputSchema input, SqlExecutionContext executionContext)
            throws SqlException, NumericException {
        if (bound instanceof ConstantExpression constant) {
            final int type = constant.getDataType();
            if (ColumnType.isTimestamp(type) && type != timestampType) {
                final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
                final int constantType = ColumnType.getTimestampType(type);
                intervals.setBetweenBoundary(driver.ceilFrom(constant.getLongValue(), constantType),
                        driver.floorFrom(constant.getLongValue(), constantType));
            } else {
                final long value = timestampValue(constant);
                intervals.setBetweenBoundary(value, value);
            }
            return;
        }
        final Function function = boundFunction(bound, input, executionContext);
        try {
            intervals.setBetweenBoundary(function, bound.getPosition());
        } catch (Throwable th) {
            if (!intervals.isBetweenBoundaryFunctionConsumed()) {
                Misc.free(function, th);
            }
            throw th;
        }
    }

    private long timestampValue(ConstantExpression constant) throws SqlException {
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        try {
            return switch (ColumnType.tagOf(constant.getDataType())) {
                case ColumnType.STRING, ColumnType.SYMBOL, ColumnType.VARCHAR -> {
                    final CharSequence text = constantText(constant);
                    yield text == null ? Numbers.LONG_NULL : driver.parseFloorLiteral(text);
                }
                case ColumnType.DATE -> driver.fromDate(constant.getLongValue());
                default -> constantBoundValue(constant, driver);
            };
        } catch (NumericException e) {
            throw SqlException.invalidDate(constant.getPosition());
        }
    }

    /**
     * Unions an IN list element: the intervals of its {@link #intervalElement spelling} when it has one, otherwise
     * the timestamp it spells.
     */
    private void unionElement(ConstantExpression constant, CharSequence seq) throws SqlException {
        if (seq != null) {
            final int lo = seq == intervalText ? 1 : 0;
            intervals.unionIntervals(seq, lo, seq.length() - lo, constant.getPosition());
        } else {
            final long value = timestampValue(constant);
            intervals.union(value, value);
        }
    }

    private void unionTimestampPredicates(FunctionExpression call, OutputSchema input, SqlExecutionContext executionContext) throws SqlException {
        if (call.isOr()) {
            unionTimestampPredicates((FunctionExpression) call.argumentAt(0), input, executionContext);
            unionTimestampPredicates((FunctionExpression) call.argumentAt(1), input, executionContext);
        } else if ("in".equals(call.getName())) {
            final CharSequence text = call.getArgumentCount() == 2 && call.argumentAt(1) instanceof ConstantExpression constant
                    && ColumnType.isVarcharOrString(constant.getDataType()) ? constantText(constant) : null;
            if (text != null) {
                final CharSequence seq = isStringLiteral((ConstantExpression) call.argumentAt(1)) ? quote(text) : text;
                final int lo = seq == text ? 0 : 1;
                intervals.unionIntervals(seq, lo, seq.length() - lo, call.argumentAt(1).getPosition());
            } else {
                for (int i = call.getArgumentCount() - 1; i > 0; i--) {
                    final BoundExpression bound = call.argumentAt(i);
                    if (bound instanceof ConstantExpression constant) {
                        unionElement(constant, intervalElement(constant));
                    } else {
                        intervals.unionRuntimeTimestamp(bounds.instantiate(bound, input, executionContext), bound.getPosition());
                    }
                }
            }
        } else {
            final BoundExpression bound = call.argumentAt(0) instanceof ColumnExpression ? call.argumentAt(1) : call.argumentAt(0);
            if (bound instanceof ConstantExpression constant) {
                final long value = constant.getDataType() == ColumnType.NULL ? Numbers.LONG_NULL : constant.getLongValue();
                intervals.union(value, value);
            } else {
                intervals.unionRuntimeTimestamp(boundFunction(bound, input, executionContext), bound.getPosition());
            }
        }
    }

    private void verifyAnalysis(BoundExpression predicate, int timestampColumnId, int timestampType, int depth,
                                BoundExpressionRewriter rewriter, BoundExpression residual) {
        if (analysis == null) {
            analysis = new IntervalAnalysis(configuration);
        }
        try {
            analysis.analyse(predicate, timestampColumnId, timestampType, depth, rewriter);
        } catch (SqlException | CairoException e) {
            throw new AssertionError("interval analysis fails where extraction succeeds", e);
        }
        if (analysis.hasIntervalFilters() != intervals.hasIntervalFilters() || analysis.isStatic() != intervals.isStatic()
                || !analysis.getStaticIntervals().equals(intervals.getStaticIntervals())
                || !isSameResidual(analysis.getResidual(), residual)) {
            throw new AssertionError("interval analysis differs from the extracted intervals");
        }
    }

    /**
     * The residual of the predicate; throws as {@link #extract} would.
     */
    BoundExpression analyse(BoundExpression predicate, int timestampColumnId, int timestampType, int depth, IntervalBoundSource bounds,
                            BoundExpressionRewriter rewriter) throws SqlException {
        return extract(predicate, timestampColumnId, timestampType, depth < MAX_SPECULATIVE_SCALAR_BOUND_DEPTH, bounds, null, rewriter, null);
    }

    RuntimeIntervalModelBuilder getIntervals() {
        return intervals;
    }
}
