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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.engine.functions.ScalarSubQueryTimestampFunction;
import io.questdb.griffin.model.DateExpressionEvaluator;
import io.questdb.griffin.model.IntervalOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.model.RuntimeIntervalModel;
import io.questdb.griffin.model.RuntimeIntervalModelBuilder;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.griffin.model.ScalarTimestampBoundHolder;
import io.questdb.griffin.model.TimestampMonotonicInverter;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.DeferredErrorExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Chars;
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
final class IntervalExtractor implements Mutable {
    private static final int MAX_SPECULATIVE_SCALAR_BOUND_DEPTH = 4;
    private final CairoConfiguration configuration;
    private final StringSink intervalText;
    private final Interval intervalValue = new Interval();
    private final RuntimeIntervalModelBuilder intervals = new RuntimeIntervalModelBuilder();
    private final Interval inverted = new Interval();
    private final LongList parsedIntervals;
    private boolean intrinsicFalse;
    private boolean isBoundSpeculationAllowed;
    private boolean isNestedOffset;
    private IntervalExtractor offsetIntervals;
    private int timestampType;

    IntervalExtractor(CairoConfiguration configuration, StringSink intervalText, LongList parsedIntervals) {
        this.configuration = configuration;
        this.intervalText = intervalText;
        this.parsedIntervals = parsedIntervals;
    }

    @Override
    public void clear() {
        intrinsicFalse = false;
        intervals.clear();
    }

    private static Function boundFunction(BoundExpression bound, OutputSchema input, FunctionInstantiator instantiator,
                                          SqlExecutionContext executionContext) throws SqlException {
        return bound instanceof CursorExpression ? timestampCursor(bound, instantiator, executionContext)
                : instantiator.instantiate(bound, input, executionContext);
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

    /**
     * A designated timestamp bound or element whose text does not parse: a quoted literal reports only its
     * position, other text reports itself.
     */
    private static SqlException invalidDate(ConstantExpression constant) {
        return isQuotedLiteral(constant) ? SqlException.invalidDate(constant.getPosition())
                : SqlException.invalidDate(constant.getTimestampText(), constant.getPosition());
    }

    private static boolean isQuotedLiteral(ConstantExpression constant) {
        return constant.isLiteral() && constant.getSource() == null;
    }

    private static boolean isStringLiteral(ConstantExpression constant) {
        return isQuotedLiteral(constant) && constant.getDataType() == ColumnType.STRING;
    }

    private static boolean isTimestampCursor(CursorExpression cursor) {
        final OutputSchema output = cursor.getPlan().getOutput();
        return !cursor.isBoolean() && output.getColumnCount() == 1 && ColumnType.isTimestamp(output.getColumnType(0));
    }

    private static Function parkDeclinedBound(BoundExpression bound, Function function, FunctionInstantiator instantiator) {
        if (bound instanceof CursorExpression cursor && function instanceof ScalarSubQueryTimestampFunction owner) {
            instantiator.parkSubquery(cursor, owner.releaseCursorFunction());
            owner.close();
            return null;
        }
        return function;
    }

    private static void shareScalarBound(BoundExpression bound, Function function, FunctionInstantiator instantiator) {
        if (bound instanceof CursorExpression cursor && function instanceof ScalarSubQueryTimestampFunction owner) {
            final ScalarTimestampBoundHolder holder = new ScalarTimestampBoundHolder(owner.getType());
            owner.setPublishHolder(holder);
            instantiator.shareScalarBound(cursor, holder);
        }
    }

    private static Function timestampCursor(BoundExpression bound, FunctionInstantiator instantiator, SqlExecutionContext executionContext)
            throws SqlException {
        final Function cursor = instantiator.instantiateSubquery((CursorExpression) bound, executionContext);
        try {
            return new ScalarSubQueryTimestampFunction(cursor, bound.getPosition());
        } catch (Throwable th) {
            Misc.free(cursor, th);
            throw th;
        }
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

    private BoundExpression intersect(BoundExpression predicate, int timestampColumnId, OutputSchema input, FunctionInstantiator instantiator,
                                      BoundExpressionRewriter rewriter, SqlExecutionContext executionContext) throws SqlException {
        if (predicate instanceof DeferredErrorExpression deferred && deferred.getComparedColumnId() == timestampColumnId) {
            throw deferred.raise();
        }
        if (!(predicate instanceof FunctionExpression call)) {
            return predicate;
        }
        if (!call.isAnd()) {
            final FunctionExpression offset = findProjectedOffset(call);
            if (offset != null) {
                return intersectOffset(call, offset, timestampColumnId, input, instantiator, rewriter, executionContext);
            }
        }
        final String operator = call.getName();
        if ("between".equals(operator) && call.getArgumentCount() == 3) {
            return intersectBetween(call, false, timestampColumnId, input, instantiator, executionContext);
        }
        if ("in".equals(operator) && call.getArgumentCount() >= 2) {
            return intersectIn(call, false, timestampColumnId, input, instantiator, executionContext);
        }
        if ("not".equals(operator) && call.getArgumentCount() == 1
                && call.argumentAt(0) instanceof FunctionExpression negated) {
            final BoundExpression remaining;
            if ("between".equals(negated.getName()) && negated.getArgumentCount() == 3) {
                remaining = intersectBetween(negated, true, timestampColumnId, input, instantiator, executionContext);
            } else if ("in".equals(negated.getName()) && negated.getArgumentCount() >= 2) {
                remaining = intersectIn(negated, true, timestampColumnId, input, instantiator, executionContext);
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
                left = intersect(call.argumentAt(0), timestampColumnId, input, instantiator, rewriter, executionContext);
                right = intersect(call.argumentAt(1), timestampColumnId, input, instantiator, rewriter, executionContext);
            } else {
                right = intersect(call.argumentAt(1), timestampColumnId, input, instantiator, rewriter, executionContext);
                left = intersect(call.argumentAt(0), timestampColumnId, input, instantiator, rewriter, executionContext);
            }
            // Rewrite descriptions only. The original preparation still owns its
            // complete closure; generation independently instantiates the residual.
            return rewriter.replaceConjunction(call, left, right);
        }
        if (call.isOr()) {
            // A union must not widen bounds already extracted from another conjunct.
            if (!intervals.hasIntervalFilters() && isTimestampUnion(call, timestampColumnId, timestampType)) {
                unionTimestampPredicates(call, input, instantiator, executionContext);
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
            return intersectMonotonic(call, timestampColumnId, input, instantiator, executionContext);
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
        if (bound instanceof ConstantExpression constant) {
            if (column.isDirectReference() && column.getDataType() == timestampType) {
                if (isTextComparisonConsumed(operator, constant)) {
                    return null;
                }
            } else if (constant.isUnparsedTimestamp()) {
                return predicate;
            }
        }
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
                return intersectMonotonic(call, timestampColumnId, input, instantiator, executionContext);
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
            final CharSequence text = isIntegralBound ? null : constant.getTimestampText();
            if (isExclusion) {
                if (text != null) {
                    intervals.subtractIntervals(text, 0, text.length(), bound.getPosition());
                } else {
                    intervals.subtractInterval(value, value);
                }
                intrinsicFalse |= intervals.isEmptySet();
            } else if (isEquality) {
                intervals.intersect(value, value);
                if (text != null) {
                    intrinsicFalse |= intervals.isEmptySet();
                }
            } else {
                intersectBound(value, isLower, isStrict);
            }
        } else if (isCursorBound || (bound.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) != 0) {
            final Function function = isCursorBound ? timestampCursor(bound, instantiator, executionContext)
                    : instantiator.instantiate(bound, input, executionContext);
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
            FunctionInstantiator instantiator,
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
                    true, timestampColumnId, input, instantiator, executionContext);
        }
        Throwable failure = null;
        try {
            intervals.setBetweenNegated(negated);
            setBetweenBoundary(lo, input, instantiator, executionContext);
            setBetweenBoundary(hi, input, instantiator, executionContext);
            intrinsicFalse |= intervals.isEmptySet();
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
        intrinsicFalse = true;
    }

    private BoundExpression intersectIn(
            FunctionExpression predicate,
            boolean negated,
            int timestampColumnId,
            OutputSchema input,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (!(predicate.argumentAt(0) instanceof ColumnExpression column)
                || column.getColumnId() != timestampColumnId || column.getDataType() != timestampType
                || negated && !column.isDirectReference()) {
            return negated ? predicate : intersectMonotonicIn(predicate, timestampColumnId, input, instantiator, executionContext);
        }
        final int count = predicate.getArgumentCount();
        if (count == 2) {
            final BoundExpression bound = predicate.argumentAt(1);
            if (ColumnType.isInterval(bound.getDataType())) {
                return intersectInterval(predicate, bound, negated, input, instantiator, executionContext);
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
                    intrinsicFalse |= intervals.isEmptySet();
                } else {
                    final long value = timestampValue(constant);
                    if (negated) {
                        intervals.subtractInterval(value, value);
                        intrinsicFalse |= intervals.isEmptySet();
                    } else if (value == Numbers.LONG_NULL && ColumnType.isTimestamp(constant.getDataType())) {
                        intersectEmpty();
                    } else {
                        intervals.intersect(value, value);
                    }
                }
            } else {
                final Function function = instantiator.instantiate(bound, input, executionContext);
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
                intrinsicFalse |= intervals.isEmptySet();
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
            final long value = timestampValue((ConstantExpression) predicate.argumentAt(i));
            if (negated) {
                intervals.subtractInterval(value, value);
                intrinsicFalse |= intervals.isEmptySet();
            } else {
                intervals.union(value, value);
            }
        }
        return null;
    }

    private BoundExpression intersectInterval(
            FunctionExpression predicate,
            BoundExpression bound,
            boolean negated,
            OutputSchema input,
            FunctionInstantiator instantiator,
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
            final Function function = instantiator.instantiate(bound, input, executionContext);
            if (negated) {
                intervals.subtractRuntimeIntervals(function, bound.getPosition());
            } else {
                intervals.intersectRuntimeIntervals(function, bound.getPosition());
            }
        } else {
            return predicate;
        }
        intrinsicFalse |= intervals.isEmptySet();
        return null;
    }

    private BoundExpression intersectMonotonic(
            FunctionExpression predicate,
            int timestampColumnId,
            OutputSchema input,
            FunctionInstantiator instantiator,
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
                false, timestampColumnId, input, instantiator, executionContext);
    }

    private BoundExpression intersectMonotonicIn(
            FunctionExpression predicate,
            int timestampColumnId,
            OutputSchema input,
            FunctionInstantiator instantiator,
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
        final Function head = instantiator.instantiate(predicate.argumentAt(0), input, executionContext);
        Throwable failure = null;
        try {
            if (FunctionInstantiator.monotonicTimestampColumnId(head) != timestampColumnId) {
                return predicate;
            }
            parsedIntervals.clear();
            intervalText.clear();
            try {
                IntervalUtils.parseTickExpr(ColumnType.getTimestampDriver(head.getType()), configuration,
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
                soundness = invert(head);
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
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final int outputType = operand.getDataType();
        final boolean isTimestamp = ColumnType.isTimestamp(outputType);
        if (!isTimestamp && outputType != ColumnType.INT && outputType != ColumnType.LONG
                || !isTimestamp && (lo != null && (ColumnType.isTimestamp(lo.getDataType()) || lo instanceof CursorExpression)
                || hi != null && (ColumnType.isTimestamp(hi.getDataType()) || hi instanceof CursorExpression))) {
            return predicate;
        }
        Function head = instantiator.instantiate(operand, input, executionContext);
        Function loFunction = null;
        Function hiFunction = null;
        Throwable failure = null;
        try {
            if (FunctionInstantiator.monotonicTimestampColumnId(head) != timestampColumnId) {
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
                loFunction = boundFunction(lo, input, instantiator, executionContext);
            }
            if (hi instanceof ConstantExpression constant) {
                hiValue = constantBoundValue(constant, driver);
                if (hiValue == Numbers.LONG_NULL) {
                    intersectEmpty();
                    return null;
                }
                hiValue += hiAdjustment;
            } else if (hi != null) {
                hiFunction = hi == lo ? loFunction : boundFunction(hi, input, instantiator, executionContext);
            }
            if (loFunction == null && hiFunction == null) {
                inverted.of(isBetween ? Math.min(loValue, hiValue) : loValue,
                        isBetween ? Math.max(loValue, hiValue) : hiValue);
                final int soundness = invert(head);
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
            // The retained residual reads the bound this inverter evaluates; that is sound only
            // when repeated opens of the sub-query agree within one execution.
            if (loFunction instanceof ScalarSubQueryTimestampFunction && !loFunction.isStableWithinExecution()
                    || hiFunction instanceof ScalarSubQueryTimestampFunction && !hiFunction.isStableWithinExecution()) {
                return predicate;
            }
            inverted.of(Long.MIN_VALUE, Long.MAX_VALUE);
            if (invert(head) == MonotonicTimestampFunction.NONE) {
                return predicate;
            }
            final TimestampMonotonicInverter inverter = new TimestampMonotonicInverter(
                    head, loFunction, loFunction != null ? loAdjustment : 0, loValue,
                    hiFunction, hiFunction != null ? hiAdjustment : 0, hiValue, isBetween, driver
            );
            shareScalarBound(lo, loFunction, instantiator);
            if (hi != lo) {
                shareScalarBound(hi, hiFunction, instantiator);
            }
            head = loFunction = hiFunction = null;
            intervals.intersectMonotonicTimestamp(inverter);
            intrinsicFalse |= intervals.isEmptySet();
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
                loFunction = parkDeclinedBound(lo, loFunction, instantiator);
                hiFunction = hi == lo ? null : parkDeclinedBound(hi, hiFunction, instantiator);
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
            FunctionInstantiator instantiator,
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
        final BoundExpression residual = source.extract(rewriter.unwrapProjectedOffsets(predicate),
                timestampColumnId, input, instantiator, rewriter, executionContext);
        if (source.intrinsicFalse) {
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
        intrinsicFalse |= intervals.isEmptySet();
        return isConsumed && residual == null && isInjective ? null : predicate;
    }

    private int invert(Function head) {
        int soundness = MonotonicTimestampFunction.EXACT;
        while (head instanceof MonotonicTimestampFunction function) {
            final int grade = function.invertTimestampInterval(inverted);
            if (grade == MonotonicTimestampFunction.NONE) {
                return grade;
            }
            soundness = Math.min(soundness, grade);
            if (inverted.getLo() > inverted.getHi()) {
                break;
            }
            head = function.getTimestampArg();
        }
        return soundness;
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

    /**
     * Validates a comparison of the designated timestamp with constant text that does not parse as a
     * timestamp: an exclusion of a quoted literal subtracts the intervals the literal spells, any other
     * such comparison raises its operator's error. Returns false when the constant is not such text or the
     * call is no comparison.
     */
    private boolean isTextComparisonConsumed(String operator, ConstantExpression constant) throws SqlException {
        final CharSequence text = unparsedText(constant);
        if (text == null) {
            return false;
        }
        final int position = constant.getPosition();
        final boolean isQuoted = isQuotedLiteral(constant);
        switch (operator) {
            case "<", "<=", ">", ">=" -> throw SqlException.invalidDate(isQuoted ? quote(text) : text, position);
            case "=", "!=", "<>" -> {
                if (isQuoted && !"=".equals(operator)) {
                    final CharSequence seq = quote(text);
                    intervals.subtractIntervals(seq, 1, seq.length() - 1, position);
                    intrinsicFalse |= intervals.isEmptySet();
                    return true;
                }
                if (Chars.indexOf(text, ';') >= 0) {
                    throw SqlException.$(position, isQuoted ? "not a timestamp, use IN keyword with intervals" : "Not a date, use IN keyword with intervals");
                }
                throw isQuoted ? SqlException.$(position, "invalid timestamp") : SqlException.invalidDate(text, position);
            }
            default -> {
                return false;
            }
        }
    }

    private boolean isTimestampPoint(ColumnExpression column, BoundExpression bound, int timestampColumnId, int timestampType) {
        return column.getColumnId() == timestampColumnId && column.getDataType() == timestampType
                && (bound instanceof ConstantExpression constant && (bound.getDataType() == timestampType || bound.getDataType() == ColumnType.NULL
                || constant.isUnparsedTimestamp())
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

    private void setBetweenBoundary(BoundExpression bound, OutputSchema input, FunctionInstantiator instantiator, SqlExecutionContext executionContext)
            throws SqlException, NumericException {
        if (bound instanceof ConstantExpression constant) {
            final int type = constant.getDataType();
            if (constant.isUnparsedTimestamp()) {
                throw invalidDate(constant);
            }
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
        final Function function = boundFunction(bound, input, instantiator, executionContext);
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
        if (constant.isUnparsedTimestamp()) {
            throw invalidDate(constant);
        }
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

    private void unionTimestampPredicates(FunctionExpression call, OutputSchema input, FunctionInstantiator instantiator, SqlExecutionContext executionContext) throws SqlException {
        if (call.isOr()) {
            unionTimestampPredicates((FunctionExpression) call.argumentAt(0), input, instantiator, executionContext);
            unionTimestampPredicates((FunctionExpression) call.argumentAt(1), input, instantiator, executionContext);
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
                        final long value = timestampValue(constant);
                        intervals.union(value, value);
                    } else {
                        intervals.unionRuntimeTimestamp(instantiator.instantiate(bound, input, executionContext), bound.getPosition());
                    }
                }
            }
        } else {
            final BoundExpression bound = call.argumentAt(0) instanceof ColumnExpression ? call.argumentAt(1) : call.argumentAt(0);
            if (bound instanceof ConstantExpression constant) {
                if (constant.isUnparsedTimestamp()) {
                    throw invalidDate(constant);
                }
                final long value = constant.getDataType() == ColumnType.NULL ? Numbers.LONG_NULL : constant.getLongValue();
                intervals.union(value, value);
            } else {
                intervals.unionRuntimeTimestamp(boundFunction(bound, input, instantiator, executionContext), bound.getPosition());
            }
        }
    }

    /**
     * The text of a constant that does not parse as the timestamp it is compared with, or null.
     */
    private CharSequence unparsedText(ConstantExpression constant) {
        if (constant.isUnparsedTimestamp()) {
            return constant.getTimestampText();
        }
        if (constant.getDataType() != ColumnType.SYMBOL || constant.getStrValue() == null) {
            return null;
        }
        try {
            ColumnType.getTimestampDriver(timestampType).parseFloorLiteral(constant.getStrValue());
            return null;
        } catch (NumericException e) {
            return constant.getStrValue();
        }
    }

    RuntimeIntrinsicIntervalModel build(int partitionBy) {
        // The scan supplies the validated reader's partition scheme immediately
        // before construction; the preceding intersections need no reader.
        intervals.of(timestampType, partitionBy, configuration);
        return intervals.hasIntervalFilters() ? intervals.build() : null;
    }

    /**
     * Returns only the conjuncts the intervals do not implement.
     */
    BoundExpression extract(BoundExpression predicate, int timestampColumnId, OutputSchema input, FunctionInstantiator instantiator,
                            BoundExpressionRewriter rewriter, SqlExecutionContext executionContext) throws SqlException {
        of(input.getColumnType(input.getColumnIndexById(timestampColumnId)));
        // Each pruning bound generates its sub-query once more, so cap the nesting that speculates.
        isBoundSpeculationAllowed = instantiator.getScalarBoundDepth() < MAX_SPECULATIVE_SCALAR_BOUND_DEPTH;
        return intersect(predicate, timestampColumnId, input, instantiator, rewriter, executionContext);
    }

    boolean isIntrinsicFalse() {
        return intrinsicFalse;
    }

    void merge(RuntimeIntervalModel model, long lo, long hi) {
        intervals.merge(model, lo, hi);
        intrinsicFalse |= intervals.isEmptySet();
    }

    void of(int timestampType) {
        clear();
        this.timestampType = timestampType;
        intervals.of(timestampType, PartitionBy.NONE, configuration);
    }

    /**
     * Intersects the timestamp range the execution context imposes on the table, such as a mat view refresh range.
     */
    void override(TableToken tableToken, SqlExecutionContext executionContext) {
        executionContext.overrideWhereIntervals(tableToken, intervals, timestampType);
        intrinsicFalse |= intervals.isEmptySet();
    }
}
