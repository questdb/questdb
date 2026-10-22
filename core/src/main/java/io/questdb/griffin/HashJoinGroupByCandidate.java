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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.bool.AndFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.HashJoinGroupByAggregates;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKey;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/** The physical fusion of a two-input hash join and its aggregate, expressed in bound column ids. */
public final class HashJoinGroupByCandidate {
    private final AggregatePlan aggregate;
    private final CharSequence buildAlias;
    private final LogicalPlan buildInput;
    private final BoundExpression buildOnFilter;
    private final boolean isInputSwapped;
    private final boolean isKeyCapacityPresized;
    private final HashJoinGroupByKeys keys;
    private final JoinKind logicalJoinType;
    private final ObjList<BoundExpression> postJoinFilters;
    private final CharSequence probeAlias;
    private final LogicalPlan probeInput;
    private final IntList requiredBuildColumns;
    private HashJoinGroupByCandidate alternate;

    private HashJoinGroupByCandidate(AggregatePlan aggregate, LogicalPlan probeInput, LogicalPlan buildInput,
                                    CharSequence probeAlias, CharSequence buildAlias, JoinKind logicalJoinType,
                                    HashJoinGroupByKeys keys, IntList requiredBuildColumns, BoundExpression buildOnFilter,
                                    ObjList<BoundExpression> postJoinFilters, boolean isInputSwapped, boolean isKeyCapacityPresized) {
        this.aggregate = aggregate;
        this.probeInput = probeInput;
        this.buildInput = buildInput;
        this.probeAlias = probeAlias;
        this.buildAlias = buildAlias;
        this.logicalJoinType = logicalJoinType;
        this.keys = keys;
        this.requiredBuildColumns = requiredBuildColumns;
        this.buildOnFilter = buildOnFilter;
        this.postJoinFilters = postJoinFilters;
        this.isInputSwapped = isInputSwapped;
        this.isKeyCapacityPresized = isKeyCapacityPresized;
    }

    @Nullable
    public static HashJoinGroupByCandidate analyse(AggregatePlan aggregate, FunctionInstantiator instantiator,
                                                   SqlExecutionContext executionContext) throws SqlException {
        return analyse(aggregate, instantiator, executionContext, false);
    }

    @Nullable
    public static HashJoinGroupByCandidate analyse(AggregatePlan aggregate, FunctionInstantiator instantiator,
                                                   SqlExecutionContext executionContext, boolean isFlipped) throws SqlException {
        if (aggregate.getSharedSource() != null || aggregate.getSharedConsumerCount() > 0 || aggregate.getAggregates().size() == 0) {
            return null;
        }
        LogicalPlan input = aggregate.getChild();
        while (input instanceof ProjectPlan || input instanceof FilterPlan) {
            if (input instanceof FilterPlan filter && LogicalPlans.skipRenames(filter.getChild()) instanceof ProjectPlan project
                    && LogicalPlans.isComputedProjection(project)) {
                return null;
            }
            input = input.inputAt(0);
        }
        if (!(input instanceof JoinPlan join) || join.getOrderedInputs().size() != 2) {
            return null;
        }
        JoinInput first = join.getOrderedInputs().getQuick(0);
        JoinInput second = join.getOrderedInputs().getQuick(1);
        JoinKind kind = second.getJoinType();
        if (kind != JoinKind.INNER && kind != JoinKind.LEFT_OUTER && kind != JoinKind.RIGHT_OUTER
                || second.getKeys().size() == 0 || first.isDependent() || second.isDependent()
                || PlanToFactoryRules.isSlaveEmptied(second) || PlanToFactoryRules.isMasterEmptied(second)) {
            return null;
        }
        LogicalPlan left = frameSource(first.getInput());
        LogicalPlan right = frameSource(second.getInput());
        if (left == null || right == null) {
            return null;
        }
        ScanPlan leftScan = scan(left);
        ScanPlan rightScan = scan(right);
        try (TableReader leftReader = executionContext.getReader(leftScan.getTableToken(), leftScan.getMetadataVersion());
             TableReader rightReader = executionContext.getReader(rightScan.getTableToken(), rightScan.getMetadataVersion())) {
            boolean isSwapped = kind == JoinKind.RIGHT_OUTER || kind == JoinKind.INNER && leftReader.size() < rightReader.size();
            if (isFlipped) {
                if (kind != JoinKind.INNER) {
                    return null;
                }
                isSwapped = !isSwapped;
            }
            LogicalPlan build = isSwapped ? left : right;
            LogicalPlan probe = isSwapped ? right : left;
            TableReader buildReader = isSwapped ? leftReader : rightReader;
            TableReader probeReader = isSwapped ? rightReader : leftReader;
            if (kind == JoinKind.RIGHT_OUTER && (buildReader.size() > probeReader.size()
                    || hasFilter(probe) || executionContext.isOverriddenIntrinsics(probeReader.getTableToken())
                    || buildReader.getTableToken().isLiveView())) {
                return null;
            }
            final OutputSchema buildOutput = frameOutput(build);
            final OutputSchema probeOutput = frameOutput(probe);
            HashJoinGroupByKeys keys = new HashJoinGroupByKeys();
            for (int i = 0; i < second.getKeys().size(); i++) {
                JoinKey key = second.getKeys().getQuick(i);
                BoundExpression master = resolveColumn(key.getMasterColumnId(), first.getInput());
                BoundExpression slave = resolveColumn(key.getSlaveColumnId(), second.getInput());
                if (!(master instanceof ColumnExpression a) || !(slave instanceof ColumnExpression b)) {
                    return null;
                }
                ColumnExpression p = isSwapped ? b : a;
                ColumnExpression s = isSwapped ? a : b;
                if (!keys.add(p.getColumnId(), p.getDataType(), s.getColumnId(), s.getDataType())) {
                    return null;
                }
                if (ColumnType.isSymbol(p.getDataType()) && !probeOutput.isSymbolTableStatic(probeOutput.getColumnIndexById(p.getColumnId()))
                        || ColumnType.isSymbol(s.getDataType()) && !buildOutput.isSymbolTableStatic(buildOutput.getColumnIndexById(s.getColumnId()))) {
                    return null;
                }
            }
            if (keys.hasTranslatedSymbol() && buildReader.getTableToken().isLiveView()) {
                return null;
            }
            AggregatePlan resolved = new AggregatePlan().of(join, aggregate.getPosition());
            resolved.getOutput().copyFrom(aggregate.getOutput());
            IntList required = new IntList();
            for (int i = 0; i < aggregate.getGroupingExpressions().size(); i++) {
                BoundExpression key = resolve(aggregate.getGroupingExpressions().getQuick(i), aggregate.getChild());
                if (key == null || !isSafe(key) || !HashJoinGroupByKeys.isSupportedKeyType(key.getDataType())
                        || !collectPayload(key, buildOutput, required)) {
                    return null;
                }
                resolved.getGroupingExpressions().add(key);
            }
            OutputSchema joined = new OutputSchema();
            joined.copyFrom(probeOutput);
            for (int i = 0; i < buildOutput.getColumnCount(); i++) {
                joined.addColumnFrom(buildOutput, i);
            }
            final GenericRecordMetadata joinedMetadata = metadata(joined);
            for (int i = 0; i < aggregate.getAggregates().size(); i++) {
                BoundExpression expression = resolve(aggregate.getAggregates().getQuick(i), aggregate.getChild());
                if (!(expression instanceof FunctionExpression call) || !isSafe(call) || !collectPayload(call, buildOutput, required)) {
                    return null;
                }
                try (Function function = instantiator.instantiateAggregate(call, joined, joinedMetadata, executionContext)) {
                    if (!isSupportedAggregate(function)) {
                        return null;
                    }
                }
                resolved.getAggregates().add(call);
            }
            ObjList<BoundExpression> postFilters = new ObjList<>();
            for (int i = 0; i < 2; i++) {
                JoinInput step = join.getOrderedInputs().getQuick(i);
                if (step.getPostJoinFilter() != null) {
                    BoundExpression predicate = resolve(step.getPostJoinFilter(), join);
                    if (predicate == null || !isPostFilter(predicate, probeOutput, buildOutput) || !collectPayload(predicate, buildOutput, required)) {
                        return null;
                    }
                    postFilters.add(predicate);
                }
            }
            ObjList<FilterPlan> outerFilters = new ObjList<>();
            LogicalPlan current = aggregate.getChild();
            while (current != join) {
                if (current instanceof FilterPlan filter) {
                    outerFilters.add(filter);
                }
                current = current.inputAt(0);
            }
            for (int i = outerFilters.size() - 1; i >= 0; i--) {
                FilterPlan filter = outerFilters.getQuick(i);
                BoundExpression predicate = resolve(filter.getPredicate(), filter.getChild());
                if (predicate == null || !isPostFilter(predicate, probeOutput, buildOutput) || !collectPayload(predicate, buildOutput, required)) {
                    return null;
                }
                postFilters.add(predicate);
            }
            BoundExpression on = resolve(second.getOnResidual(), join);
            BoundExpression keyFilter = resolve(second.getKeyFilter(), join);
            if (second.getOnResidual() != null && on == null || second.getKeyFilter() != null && keyFilter == null) {
                return null;
            }
            // A slave-only key constraint is a build predicate only when the physical build is that slave.
            if (keyFilter != null) {
                if (!isSafe(keyFilter) || !onlyReads(keyFilter, buildOutput)) {
                    return null;
                }
                on = and(on, keyFilter);
            }
            if (kind == JoinKind.RIGHT_OUTER) {
                for (int i = 0; i < postFilters.size(); i++) {
                    if (!onlyReads(postFilters.getQuick(i), buildOutput)) {
                        return null;
                    }
                }
            }
            if (kind == JoinKind.INNER && on != null) {
                if (!isPostFilter(on, probeOutput, buildOutput) || !collectPayload(on, buildOutput, required)) {
                    return null;
                }
                postFilters.insert(0, 1, on);
                on = null;
            }
            boolean isPresized = buildReader.size() <= probeReader.size();
            if (on != null && (!isSafe(on) || !onlyReads(on, buildOutput)
                    || !isPresized && isExpensive(on))) {
                return null;
            }
            return new HashJoinGroupByCandidate(resolved, probe, build,
                    (isSwapped ? second : first).getBindingAlias(), (isSwapped ? first : second).getBindingAlias(),
                    kind, keys, required, on, postFilters, isSwapped, isPresized);
        }
    }

    public AggregatePlan getAggregate() {
        return aggregate;
    }

    public HashJoinGroupByCandidate getAlternate() {
        return alternate;
    }

    public CharSequence getBuildAlias() {
        return buildAlias;
    }

    public LogicalPlan getBuildInput() {
        return buildInput;
    }

    public BoundExpression getBuildOnFilter() {
        return buildOnFilter;
    }

    public HashJoinGroupByKeys getKeys() {
        return keys;
    }

    public JoinKind getLogicalJoinType() {
        return logicalJoinType;
    }

    public JoinKind getPhysicalJoinType() {
        return logicalJoinType == JoinKind.INNER ? JoinKind.INNER : JoinKind.LEFT_OUTER;
    }

    public BoundExpression getPostJoinFilter() throws SqlException {
        BoundExpression result = null;
        for (int i = 0; i < postJoinFilters.size(); i++) {
            result = and(result, postJoinFilters.getQuick(i));
        }
        return result;
    }

    public ObjList<BoundExpression> getPostJoinFilters() {
        return postJoinFilters;
    }

    public CharSequence getProbeAlias() {
        return probeAlias;
    }

    public LogicalPlan getProbeInput() {
        return probeInput;
    }

    public IntList getRequiredBuildColumns() {
        return requiredBuildColumns;
    }

    public boolean isInputSwapped() {
        return isInputSwapped;
    }

    public boolean isKeyCapacityPresized() {
        return isKeyCapacityPresized;
    }

    public void setAlternate(HashJoinGroupByCandidate alternate) {
        this.alternate = alternate;
    }

    public static OutputSchema frameOutput(LogicalPlan input) {
        return input instanceof FilterPlan filter ? filter.getChild().getOutput() : input.getOutput();
    }

    private static BoundExpression and(BoundExpression left, BoundExpression right) throws SqlException {
        if (left == null) {
            return right;
        }
        ObjList<BoundExpression> args = new ObjList<>(2);
        args.add(left);
        args.add(right);
        IntList positions = new IntList(2);
        positions.add(left.getPosition());
        positions.add(right.getPosition());
        return new FunctionExpression().of(new FunctionFactoryDescriptor(new AndFunctionFactory()), args, positions,
                ColumnType.BOOLEAN, BoundExpression.STABLE_WITHIN_EXECUTION, left.getPosition());
    }

    private static GenericRecordMetadata metadata(OutputSchema input) {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0; i < input.getColumnCount(); i++) {
            metadata.add(new TableColumnMetadata("c" + input.getColumnId(i), input.getColumnType(i), IndexType.NONE, 0,
                    input.isSymbolTableStatic(i), null));
        }
        return metadata;
    }

    private static boolean collectPayload(BoundExpression expression, OutputSchema build, IntList required) {
        return expression.walk(node -> {
            if (node instanceof ColumnExpression column && build.getColumnIndexById(column.getColumnId()) >= 0) {
                if (!isSupportedValueType(column.getDataType())) {
                    return TreeWalk.STOP;
                }
                if (!required.contains(column.getColumnId())) {
                    required.add(column.getColumnId());
                }
            }
            return TreeWalk.CONTINUE;
        });
    }

    private static LogicalPlan frameSource(LogicalPlan input) {
        while (input instanceof ProjectPlan project) {
            if (LogicalPlans.isComputedProjection(project)) {
                return null;
            }
            input = project.getChild();
        }
        ScanPlan scan = scan(input);
        if (scan == null || scan.getAccessPath() != ScanPlan.AccessPath.PAGE_FRAMES
                || scan.getLimitLo() != null || scan.getLimitHi() != null) {
            return null;
        }
        BoundExpression predicate = scan.getResidual();
        if (input instanceof FilterPlan filter && !PlanToFactoryRules.isFusedFilter(filter)) {
            predicate = filter.getPredicate();
            if (!isConstantGate(predicate) && filter.getAlgorithm() != FilterPlan.Algorithm.PARALLEL) {
                return null;
            }
        } else if (predicate != null && !isConstantGate(predicate) && scan.getResidualAlgorithm() != FilterPlan.Algorithm.PARALLEL) {
            return null;
        }
        return predicate == null || isSafe(predicate) ? input : null;
    }

    public static boolean isConstantGate(BoundExpression predicate) {
        return predicate != null && (predicate.getFunctionFlags() & (BoundExpression.CONSTANT | BoundExpression.RUNTIME_CONSTANT)) != 0;
    }

    private static boolean hasFilter(LogicalPlan input) {
        ScanPlan scan = scan(input);
        return input instanceof FilterPlan || scan.getResidual() != null || scan.getJoinIntervals().size() > 0;
    }

    private static boolean isCheap(BoundExpression expression) {
        if (!(expression instanceof FunctionExpression call)) {
            return false;
        }
        String name = call.getName();
        if (call.isAnd() || call.isOr() || SqlKeywords.isNotKeyword(name)) {
            for (int i = 0; i < call.getArgumentCount(); i++) {
                if (!isCheap(call.argumentAt(i))) {
                    return false;
                }
            }
            return true;
        }
        if (call.getArgumentCount() != 2 || !(Chars.equals(name, "=") || Chars.equals(name, "!=") || Chars.equals(name, "<>")
                || Chars.equals(name, "<") || Chars.equals(name, "<=") || Chars.equals(name, ">") || Chars.equals(name, ">="))) {
            return false;
        }
        for (int i = 0; i < 2; i++) {
            BoundExpression arg = call.argumentAt(i);
            if (!(arg instanceof ColumnExpression || arg instanceof ConstantExpression || arg instanceof BindVariableExpression)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isExpensive(BoundExpression expression) {
        if (isCheap(expression)) {
            return false;
        }
        return !expression.walk(node -> node instanceof ColumnExpression && !isSupportedValueType(node.getDataType())
                ? TreeWalk.STOP : TreeWalk.CONTINUE) || hasVariableArgument(expression, false);
    }

    private static boolean hasVariableArgument(BoundExpression expression, boolean isArgument) {
        if (!(expression instanceof FunctionExpression call)) {
            return false;
        }
        final String name = call.getName();
        final boolean isBooleanOperation = call.isAnd() || call.isOr() || SqlKeywords.isNotKeyword(name)
                || Chars.equals(name, "=") || Chars.equals(name, "!=") || Chars.equals(name, "<>")
                || Chars.equals(name, "<") || Chars.equals(name, "<=") || Chars.equals(name, ">") || Chars.equals(name, ">=");
        if (isArgument && !isBooleanOperation && ColumnType.isVarSize(call.getDataType())
                && !call.walk(node -> node instanceof ColumnExpression ? TreeWalk.STOP : TreeWalk.CONTINUE)) {
            return true;
        }
        for (int i = 0; i < call.getArgumentCount(); i++) {
            if (hasVariableArgument(call.argumentAt(i), !isBooleanOperation)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isParallelSafe(Function function) {
        return function.supportsParallelism() && function.isStableWithinExecution();
    }

    private static boolean isSafe(BoundExpression expression) {
        return (expression.getFunctionFlags() & (BoundExpression.NO_PARALLELISM | BoundExpression.STABLE_WITHIN_EXECUTION))
                == BoundExpression.STABLE_WITHIN_EXECUTION
                && expression.walk(node -> node instanceof CursorExpression || node instanceof OuterColumnExpression ? TreeWalk.STOP : TreeWalk.CONTINUE);
    }

    private static boolean isPostFilter(BoundExpression expression, OutputSchema probe, OutputSchema build) {
        if (!isSafe(expression) || (expression.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) != 0
                && !LogicalPlans.isConstant(expression)) {
            return false;
        }
        if (expression instanceof ConstantExpression constant && constant.getLongValue() == 0) {
            return false;
        }
        return isPostConjunct(expression, probe, build);
    }

    private static boolean isPostConjunct(BoundExpression expression, OutputSchema probe, OutputSchema build) {
        if (expression instanceof FunctionExpression call && call.isAnd()) {
            for (int i = 0; i < call.getArgumentCount(); i++) {
                if (!isPostConjunct(call.argumentAt(i), probe, build)) {
                    return false;
                }
            }
            return true;
        }
        return onlyReads(expression, probe) || onlyReads(expression, build);
    }

    private static boolean onlyReads(BoundExpression expression, OutputSchema schema) {
        return expression.walk(node -> node instanceof ColumnExpression column && schema.getColumnIndexById(column.getColumnId()) < 0
                ? TreeWalk.STOP : TreeWalk.CONTINUE);
    }

    private static BoundExpression resolve(BoundExpression expression, LogicalPlan input) {
        if (expression instanceof ColumnExpression column) {
            BoundExpression resolved = resolveColumn(column.getColumnId(), input);
            return column.isCast() && (resolved == null || column.getDataType() != resolved.getDataType()) ? null : resolved;
        }
        if (expression instanceof FunctionExpression call) {
            ObjList<BoundExpression> arguments = new ObjList<>(call.getArgumentCount());
            for (int i = 0; i < call.getArgumentCount(); i++) {
                BoundExpression argument = resolve(call.argumentAt(i), input);
                if (argument == null) {
                    return null;
                }
                arguments.add(argument);
            }
            return new FunctionExpression().of(call, arguments);
        }
        return expression;
    }

    private static BoundExpression resolveColumn(int id, LogicalPlan input) {
        int index = input.getOutput().getColumnIndexById(id);
        if (index < 0) {
            return null;
        }
        if (input instanceof ProjectPlan project) {
            return resolve(project.getExpressions().getQuick(index), project.getChild());
        }
        if (input instanceof FilterPlan filter) {
            return resolveColumn(id, filter.getChild());
        }
        if (input instanceof JoinPlan join) {
            for (int i = 0; i < join.getOrderedInputs().size(); i++) {
                LogicalPlan source = join.getOrderedInputs().getQuick(i).getInput();
                if (source.getOutput().getColumnIndexById(id) >= 0) {
                    return resolveColumn(id, source);
                }
            }
            return null;
        }
        if (input instanceof ScanPlan) {
            return new ColumnExpression().of(id, input.getOutput().getColumnType(index), input.getPosition());
        }
        return null;
    }

    private static ScanPlan scan(LogicalPlan input) {
        return input instanceof ScanPlan scan ? scan : input instanceof FilterPlan filter && filter.getChild() instanceof ScanPlan scan ? scan : null;
    }

    public static boolean isSupportedAggregate(Function function) {
        if (!isParallelSafe(function)) {
            return false;
        }
        Class<?> type = function.getClass();
        if (function instanceof UnaryFunction unary) {
            Function arg = unary.getArg();
            return isParallelSafe(arg) && HashJoinGroupByAggregates.isSupportedUnary(type, arg.getType());
        }
        if (function instanceof BinaryFunction binary) {
            return isParallelSafe(binary.getLeft()) && isParallelSafe(binary.getRight())
                    && HashJoinGroupByAggregates.isSupportedBinary(type);
        }
        return HashJoinGroupByAggregates.isSupportedNullary(type);
    }

    private static boolean isSupportedInputFactory(RecordCursorFactory factory) {
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current instanceof CoveringIndexRecordCursorFactory) {
                return false;
            }
        }
        return true;
    }

    public static boolean isSupportedFramedInput(RecordCursorFactory factory) {
        if (!isSupportedInputFactory(factory)) {
            return false;
        }
        if (factory.supportsPageFrameCursor()) {
            return true;
        }
        return factory.getBaseFactory() != null
                && factory.getBaseFactory().supportsPageFrameCursor()
                && factory.getFilter() != null
                && factory.getFilter().supportsParallelism()
                && factory.getFilter().isStableWithinExecution();
    }

    public static boolean isSupportedTranslatedSymbolBuild(RecordCursorFactory factory) {
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current instanceof LiveViewRecordCursorFactory) {
                return false;
            }
        }
        return true;
    }

    public static boolean isSupportedValueType(int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT, ColumnType.CHAR,
                 ColumnType.INT, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP,
                 ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.SYMBOL, ColumnType.IPv4,
                 ColumnType.UUID, ColumnType.LONG256, ColumnType.GEOBYTE, ColumnType.GEOSHORT,
                 ColumnType.GEOINT, ColumnType.GEOLONG, ColumnType.DECIMAL8, ColumnType.DECIMAL16,
                 ColumnType.DECIMAL32, ColumnType.DECIMAL64, ColumnType.DECIMAL128,
                 ColumnType.DECIMAL256 -> true;
            // STRING, VARCHAR, BINARY and ARRAY are variable-size, so the row heap cannot copy
            // them. INTERVAL is not persisted, so no base-table column carries one.
            default -> false;
        };
    }

}
