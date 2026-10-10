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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.model.RuntimeIntervalModel;
import io.questdb.griffin.model.RuntimeIntervalModelBuilder;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Interval;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

/**
 * The timestamp intervals a scan predicate implies, computed from the bound plan without instantiating any function.
 * The facts match the {@link RuntimeIntervalModel} codegen builds from the same predicate.
 */
public final class IntervalAnalysis {
    private final PlaceholderBounds bounds = new PlaceholderBounds();
    private final IntervalExtractor extractor;
    private BoundExpression residual;
    private int timestampType;

    public IntervalAnalysis(CairoConfiguration configuration) {
        extractor = new IntervalExtractor(configuration, new StringSink(), new LongList());
    }

    /**
     * Whether the intervals read at most one partition of a table partitioned by {@code partitionBy}.
     */
    public boolean allIntervalsHitOnePartition(int partitionBy) {
        final RuntimeIntervalModelBuilder intervals = extractor.getIntervals();
        return RuntimeIntervalModel.allIntervalsHitOnePartition(ColumnType.getTimestampDriver(timestampType), partitionBy,
                intervals.getStaticIntervals(), intervals.isStatic());
    }

    /**
     * Analyses the predicate over the designated timestamp; raises the error extraction raises for the predicate.
     *
     * @param depth the generation depth of the scan's query level, which caps sub-query bound speculation
     */
    public void analyse(BoundExpression predicate, int timestampColumnId, int timestampType, int depth, BoundExpressionRewriter rewriter) throws SqlException {
        this.timestampType = timestampType;
        bounds.clear();
        residual = extractor.analyse(predicate, timestampColumnId, timestampType, depth, bounds, rewriter);
    }

    /**
     * The conjuncts of the analysed predicate the intervals do not implement, null when they implement all.
     */
    public BoundExpression getResidual() {
        return residual;
    }

    /**
     * The static intervals as sorted [lo, hi] pairs; valid only when {@link #isStatic()}.
     */
    public LongList getStaticIntervals() {
        return extractor.getIntervals().getStaticIntervals();
    }

    /**
     * Whether the predicate constrains the timestamp at all; codegen builds no interval model otherwise.
     */
    public boolean hasIntervalFilters() {
        return extractor.getIntervals().hasIntervalFilters();
    }

    /**
     * Whether the intervals are an empty set, so the scan reads no row.
     */
    public boolean isIntrinsicFalse() {
        return extractor.isIntrinsicFalse();
    }

    /**
     * Whether the intervals are known before execution: no bind variable, runtime function or sub-query bounds them.
     */
    public boolean isStatic() {
        return extractor.getIntervals().isStatic();
    }

    /**
     * Narrows the intervals to the static intervals of a window join master expanded by the window, as the scan of
     * the slave does.
     */
    public void merge(LongList masterIntervals, long lo, long hi) {
        extractor.merge(masterIntervals, lo, hi);
    }

    /**
     * Starts an analysis that constrains nothing, for a scan without a predicate over the designated timestamp.
     */
    public void of(int timestampType) {
        this.timestampType = timestampType;
        residual = null;
        extractor.of(timestampType);
    }

    /**
     * Intersects the timestamp range the execution context imposes on the table, see
     * {@link SqlExecutionContext#overrideWhereIntervals}.
     */
    public void override(TableToken tableToken, SqlExecutionContext executionContext) {
        extractor.override(tableToken, executionContext);
    }

    /**
     * Stands in for each runtime bound with a distinct inert function, since the builder tracks bound ownership by
     * identity, and resolves monotonic operands through the declarations of their factories.
     */
    private static final class PlaceholderBounds implements IntervalBoundSource {
        private final MonotonicTimestampFunctionFactory.ConstantArguments arguments = new MonotonicTimestampFunctionFactory.ConstantArguments();
        private final ObjList<Function> placeholders = new ObjList<>();
        private int placeholderCount;

        private static MonotonicTimestampFunctionFactory factory(BoundExpression expression) {
            return expression instanceof FunctionExpression call && call.getOverload() != null
                    && call.getOverload().getFactory() instanceof MonotonicTimestampFunctionFactory factory ? factory : null;
        }

        @Override
        public Function compileTickExpr(TimestampDriver timestampDriver, CairoConfiguration configuration, CharSequence seq, int lo, int lim, int position) {
            return placeholder();
        }

        @Override
        public Function instantiate(BoundExpression expression, OutputSchema input, SqlExecutionContext executionContext) {
            return placeholder();
        }

        @Override
        public Function instantiateTimestampCursor(CursorExpression cursor, SqlExecutionContext executionContext) {
            return placeholder();
        }

        @Override
        public int invertMonotonic(BoundExpression operand, Function head, Interval io) throws SqlException {
            int soundness = MonotonicTimestampFunction.EXACT;
            BoundExpression layer = skipIdentities(operand);
            int index;
            while ((index = timestampArgumentIndex(layer)) >= 0) {
                final FunctionExpression call = (FunctionExpression) layer;
                final BoundExpression argument = skipIdentities(call.argumentAt(index));
                final int grade = factory(call).invertTimestampInterval(call, io, timestampArgumentIndex(argument) >= 0, arguments);
                if (grade == MonotonicTimestampFunction.NONE) {
                    return grade;
                }
                soundness = Math.min(soundness, grade);
                if (io.getLo() > io.getHi()) {
                    break;
                }
                layer = argument;
            }
            return soundness;
        }

        @Override
        public int monotonicColumnId(BoundExpression operand, Function head) throws SqlException {
            BoundExpression layer = operand;
            int index;
            while ((index = timestampArgumentIndex(layer)) >= 0) {
                layer = ((FunctionExpression) layer).argumentAt(index);
            }
            return layer instanceof ColumnExpression column && ColumnType.isTimestamp(column.getDataType()) ? column.getColumnId() : -1;
        }

        @Override
        public Function newMonotonicInverter(Function head, Function lo, short loAdjustment, long loValue, Function hi, short hiAdjustment,
                                             long hiValue, boolean isBetween, TimestampDriver timestampDriver) {
            return placeholder();
        }

        @Override
        public Function parkDeclinedBound(BoundExpression bound, Function function) {
            return function;
        }

        @Override
        public void publishScalarBound(BoundExpression bound, Function function) {
        }

        private void clear() {
            placeholderCount = 0;
        }

        private Function placeholder() {
            if (placeholderCount == placeholders.size()) {
                placeholders.add(new TimestampConstant(Numbers.LONG_NULL, ColumnType.TIMESTAMP_MICRO));
            }
            return placeholders.getQuick(placeholderCount++);
        }

        private BoundExpression skipIdentities(BoundExpression expression) throws SqlException {
            MonotonicTimestampFunctionFactory factory;
            while ((factory = factory(expression)) != null) {
                final FunctionExpression call = (FunctionExpression) expression;
                if (factory.getTimestampArgumentIndex(call, arguments) < 0 || !factory.isIdentity(call, arguments)) {
                    break;
                }
                expression = call.argumentAt(factory.getTimestampArgumentIndex(call, arguments));
            }
            return expression;
        }

        private int timestampArgumentIndex(BoundExpression expression) throws SqlException {
            final MonotonicTimestampFunctionFactory factory = factory(expression);
            return factory != null ? factory.getTimestampArgumentIndex((FunctionExpression) expression, arguments) : -1;
        }
    }
}
