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

package io.questdb.griffin.plan.logical;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.map.RecordValueSinkFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.groupby.vect.VectorAggregateConstructors;
import io.questdb.griffin.engine.groupby.vect.VectorAggregateFunctionConstructor;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * The shapes codegen builds from plan nodes: which nodes one factory covers (a filter fused into its scan, a LATEST
 * BY or DISTINCT built into a scan, a filter a parallel consumer steals, the base a GROUP BY, SAMPLE BY or LATEST BY
 * reads) and the layout of the factory it builds (its column names, designated timestamp and vector form). Operator
 * planning, plan verification, {@link PhysicalProperties} and codegen read them; {@link LogicalPlans} holds the plan
 * facts they are built on and never reads them.
 */
public final class PlanToFactoryRules {
    /**
     * The most slots per master row, offsets times slaves, for which a HORIZON JOIN projection over a master with
     * random access runs in parallel, see {@link #isHorizonWithinSlotCap}.
     */
    public static final int MAX_HORIZON_SLOTS_PER_MASTER_ROW = 100;

    private PlanToFactoryRules() {
    }

    /**
     * The input the generator builds a GROUP BY over: the aggregate's input past renames, without a projection that
     * only declares the designated timestamp.
     */
    public static LogicalPlan aggregateBase(AggregatePlan aggregate) {
        return timestampDeclarationBase(LogicalPlans.skipRenames(aggregate.getChild()));
    }

    /**
     * The factory the generator builds for the aggregate.
     */
    public static AggregateFactory aggregateFactory(AggregatePlan aggregate) {
        if (aggregate.getAlgorithm() == AggregatePlan.Algorithm.HASH_JOIN) {
            return AggregateFactory.GROUP_BY;
        }
        if (postingDistinctScan(aggregate) != null) {
            return AggregateFactory.POSTING_DISTINCT;
        }
        if (aggregate.getChild() instanceof HorizonJoinPlan horizon && horizon.getConsumer() == HorizonJoinPlan.Consumer.AGGREGATE) {
            return AggregateFactory.HORIZON;
        }
        if (LogicalPlans.isCount(aggregate)) {
            return AggregateFactory.COUNT;
        }
        return isSingleRowTimestamp(aggregate) ? AggregateFactory.TIMESTAMP_ENDPOINT : AggregateFactory.GROUP_BY;
    }

    /**
     * The name the generator's factory gives column {@code index} of the plan: a count aggregate, read through
     * filters, names its column {@code count} under any spelling of that name.
     */
    public static CharSequence factoryColumnName(LogicalPlan plan, int index) {
        final CharSequence name = plan.getOutput().getColumnName(index);
        return LogicalPlans.skipFilters(plan) instanceof AggregatePlan aggregate && LogicalPlans.isCount(aggregate) && SqlKeywords.isCountKeyword(name)
                ? "count" : name;
    }

    /**
     * The index, in the output of the temporal join step's slave, of the first column the full-fat ASOF or LT join
     * cannot carry: a column that is not a slave key, which the join stores in its map value, of a type the map value
     * cannot hold, such as a variable-size type. -1 when the join carries every slave column.
     */
    public static int fullFatTemporalRejectedColumn(JoinInput step) {
        final OutputSchema slave = step.getInput().getOutput();
        for (int i = 0, n = slave.getColumnCount(); i < n; i++) {
            if (!RecordValueSinkFactory.isSupportedColumnType(slave.getColumnType(i)) && !step.hasKeyOn(slave.getColumnId(i))) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The table scan the generator builds the filter into, or null when it filters the factory of its input.
     */
    public static ScanPlan fusedScan(FilterPlan filter) {
        return filter.getChild() instanceof ScanPlan scan && !scan.isWalClientUpdate() ? scan : null;
    }

    /**
     * The plan the generator builds the factory of {@code plan} from: a projection that passes its input through
     * and a filter it folds to true build none.
     */
    public static LogicalPlan generatedPlan(LogicalPlan plan) {
        while (true) {
            if (plan instanceof ProjectPlan project && isIdentityProjection(project)) {
                plan = project.getChild();
            } else if (plan instanceof FilterPlan filter && !isFusedFilter(filter)
                    && filter.getPredicate() instanceof ConstantExpression constant && constant.getLongValue() != 0) {
                plan = filter.getChild();
            } else {
                return plan;
            }
        }
    }

    /**
     * The window the generator builds the factory of {@code plan} from: the window itself, or the window under a
     * projection of its output, which the window factory builds; null otherwise.
     */
    public static WindowPlan generatedWindow(LogicalPlan plan) {
        if (plan instanceof WindowPlan window) {
            return window;
        }
        return plan instanceof ProjectPlan project && projectionFactory(project) == ProjectionFactory.WINDOW ? (WindowPlan) project.getChild() : null;
    }

    /**
     * The window join under a column-only projection of its output, which the window join factory builds; null
     * otherwise.
     */
    public static WindowJoinPlan generatedWindowJoin(ProjectPlan project) {
        return projectionFactory(project) == ProjectionFactory.WINDOW_JOIN ? (WindowJoinPlan) project.getChild() : null;
    }

    /**
     * True when the order a sort requests reaches a filtered table scan, directly, through a window, or as
     * the master of a join that preserves master order.
     */
    public static boolean hasAdvisedInput(LogicalPlan plan) {
        plan = LogicalPlans.skipProjects(plan);
        if (plan instanceof WindowPlan) {
            return hasNativeFilterInput(plan.inputAt(0));
        }
        return hasNativeFilterInput(plan) || hasOrderedJoinMasterInput(plan);
    }

    /**
     * True when every GeoHash prefix the within() call tests is a constant the column takes, so that
     * {@link #withinPrefixes} collects them.
     */
    public static boolean hasConstantWithinPrefixes(FunctionExpression within) {
        final int columnType = within.argumentAt(0).getDataType();
        for (int i = 1, n = within.getArgumentCount(); i < n; i++) {
            if (!(within.argumentAt(i) instanceof ConstantExpression prefix)
                    || !GeoHashes.isNormalizableGeoPrefix(prefix.getLongValue(), prefix.getDataType(), columnType)) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the plan, under any projections, filters a table scan, so the scan's access path serves the filter.
     */
    public static boolean hasNativeFilterInput(LogicalPlan plan) {
        plan = LogicalPlans.skipProjects(plan);
        return plan instanceof FilterPlan filter && filter.getChild() instanceof ScanPlan;
    }

    /**
     * True when the plan, under any projections, is a join that keeps its master's order over a filtered table scan.
     */
    public static boolean hasOrderedJoinMasterInput(LogicalPlan plan) {
        plan = LogicalPlans.skipProjects(plan);
        return plan instanceof JoinPlan join && LogicalPlans.isMasterOrderPreserved(join)
                && hasNativeFilterInput(join.getOrderedInputs().getQuick(0).getInput());
    }

    /**
     * Whether the aggregate has a single vector key and a vector implementation of every aggregate call.
     */
    public static boolean hasVectorShape(AggregatePlan plan) {
        final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
        if (keys.size() != 1 || vectorKey(keys.getQuick(0)) == null) {
            return false;
        }
        for (int i = 0, n = plan.getAggregates().size(); i < n; i++) {
            if (vectorConstructor(plan.getAggregates().getQuick(i)) == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * The selection a HORIZON JOIN reads the filter of its slave through: the {@link #temporalSlaveProjection} that
     * keeps the names of its input columns and the designated timestamp of its input; null otherwise.
     */
    public static ProjectPlan horizonSlaveProjection(LogicalPlan slave) {
        final ProjectPlan project = temporalSlaveProjection(slave);
        if (project == null) {
            return null;
        }
        final int timestampIndex = PhysicalProperties.timestampIndex(project);
        if (timestampIndex < 0) {
            return null;
        }
        final OutputSchema output = project.getOutput();
        final OutputSchema input = project.getChild().getOutput();
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            final int index = input.getColumnIndexById(((ColumnExpression) project.getExpressions().getQuick(i)).getColumnId());
            if (!Chars.equals(output.getColumnName(i), input.getColumnName(index))) {
                return null;
            }
            if (i == timestampIndex && index != PhysicalProperties.timestampIndex(project.getChild())) {
                return null;
            }
        }
        return project;
    }

    /**
     * The filter a HORIZON JOIN steals from its slave and applies to the slave rows before it matches them: the filter
     * the generator builds the slave from, or the one under the selection {@link #horizonSlaveProjection} reads it
     * through; null when there is none, or when access path planning left a fused filter no residual, whose scan then
     * applies the whole predicate itself.
     */
    public static FilterPlan horizonStolenFilter(LogicalPlan slave) {
        final ProjectPlan projection = horizonSlaveProjection(slave);
        final FilterPlan filter = stolenFilter(projection != null ? projection.getChild() : slave);
        if (filter == null) {
            return null;
        }
        final ScanPlan scan = fusedScan(filter);
        return scan == null || scan.getResidual() != null ? filter : null;
    }

    /**
     * True when the generator builds the filter into the factory of the table scan under it.
     */
    public static boolean isFusedFilter(FilterPlan filter) {
        return fusedScan(filter) != null;
    }

    /**
     * True when the HORIZON JOIN matches at most {@link #MAX_HORIZON_SLOTS_PER_MASTER_ROW} slaves and offsets per
     * master row. Past that, a projection over a master with random access runs serially: the parallel projection
     * cuts the master into frames that shrink as the slots grow, and every frame costs a reduce task.
     */
    public static boolean isHorizonWithinSlotCap(HorizonJoinPlan horizon) {
        return (long) horizon.getOffsetValues().size() * horizon.getSlaves().size() <= MAX_HORIZON_SLOTS_PER_MASTER_ROW;
    }

    /**
     * True when the generator builds no factory for the projection over the factory of its input, see
     * {@link #isIdentityProjection(ProjectPlan, RecordMetadata, int, int)}, whose columns operator planning takes to be
     * the input's output schema. Over a join of several inputs, or a node that forwards its columns, such as a filter
     * or a sort over one, the factory names its columns qualifier.name, so the generator builds a selection where this
     * answers true; the planner reads the answer only to look through the projection for a filter, a scan or page
     * frames, which such a join exposes no more than the selection does.
     */
    public static boolean isIdentityProjection(ProjectPlan project) {
        return isIdentityProjection(project, null, PhysicalProperties.timestampIndex(project),
                PhysicalProperties.timestampIndex(project.getChild()));
    }

    /**
     * True when the generator builds no factory for the projection: it selects every input column, in order, under
     * the name and type the input's factory gives it, {@code layout}, or the input's output schema when null, and
     * designates the timestamp the input's factory designates. A SELECT list over GROUP BY keeps the key spelling when
     * it only changes the name case.
     */
    public static boolean isIdentityProjection(ProjectPlan project, @Nullable RecordMetadata layout, int timestampIndex, int inputTimestampIndex) {
        if (projectionFactory(project) != ProjectionFactory.SELECTION) {
            return false;
        }
        final LogicalPlan input = project.getChild();
        final OutputSchema output = project.getOutput();
        final OutputSchema inputOutput = input.getOutput();
        if (output.getColumnCount() != (layout == null ? inputOutput.getColumnCount() : layout.getColumnCount())
                || timestampIndex != inputTimestampIndex) {
            return false;
        }
        final boolean isKeySpellingKept = LogicalPlans.skipFilters(input) instanceof AggregatePlan aggregate
                && aggregate.hasKeySpellingKept() && !(aggregate.getChild() instanceof HorizonJoinPlan);
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            final ColumnExpression column = (ColumnExpression) project.getExpressions().getQuick(i);
            final CharSequence name = output.getColumnName(i);
            final CharSequence inputName = layout == null ? factoryColumnName(input, i) : layout.getColumnName(i);
            if (inputOutput.getColumnIndexById(column.getColumnId()) != i
                    || output.getColumnType(i) != (layout == null ? inputOutput.getColumnType(i) : layout.getColumnType(i))
                    || !(isKeySpellingKept ? Chars.equalsIgnoreCase(name, inputName) : Chars.equals(name, inputName))) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the temporal join step matches on keys: a sole key pairing the master's designated timestamp, at
     * {@code masterTimestampIndex} of {@code masterOutput}, with the slave's, at {@code slaveTimestampIndex}, adds no
     * key, and the generator drops it.
     */
    public static boolean isKeyedTemporalJoin(JoinInput step, OutputSchema masterOutput, int masterTimestampIndex, int slaveTimestampIndex) {
        final ObjList<JoinKey> keys = step.getKeys();
        if (keys.size() != 1) {
            return keys.size() > 0;
        }
        return masterOutput.getColumnIndexById(keys.getQuick(0).getMasterColumnId()) != masterTimestampIndex
                || step.getInput().getOutput().getColumnIndexById(keys.getQuick(0).getSlaveColumnId()) != slaveTimestampIndex;
    }

    /**
     * Whether the generator hash-joins an empty master to the step's input instead of the join's master: a keyed RIGHT
     * join whose ON residual is constant false, so no master row matches and the join emits each slave row once.
     */
    public static boolean isMasterEmptied(JoinInput step) {
        return step.getJoinType() == JoinKind.RIGHT_OUTER && isNeverMatchingKeyedStep(step);
    }

    /**
     * True when the factory of the relational join step at {@code position} of the join's ordered inputs can keep its
     * master's designated timestamp: the join leads with its first input, see {@link LogicalPlans#isFirstInputLeading},
     * and the step neither null-extends its master nor drives the join with the smaller input. A markout step
     * designates none.
     */
    public static boolean isMasterTimestampKept(JoinPlan join, int position) {
        final JoinInput step = join.getOrderedInputs().getQuick(position);
        return LogicalPlans.isFirstInputLeading(join) && !step.getJoinType().isMasterNulling()
                && step.getMasterSide() != JoinInput.MasterSide.SMALLER;
    }

    /**
     * True when the page frames of the scan follow the order advice of its consumer: a backward scan delivers its
     * designated timestamp descending and no other key, so it follows a consumer that orders by that key alone.
     */
    public static boolean isPageFrameAdviceFollowed(ScanPlan scan) {
        final SortKeys order = scan.getRequestedOrder();
        return scan.getScanDirection() == SortDirection.DESCENDING && order.size() == 1
                && order.getColumnIds().getQuick(0) == scan.getOutput().getTimestampColumnId()
                && order.getDirections().getQuick(0) == SortDirection.DESCENDING;
    }

    /**
     * True when the generator builds a projection factory for the plan, which a parallel top-K can build over itself.
     */
    public static boolean isPeelableProjection(LogicalPlan plan) {
        if (!(plan instanceof ProjectPlan project)) {
            return false;
        }
        final ProjectionFactory factory = projectionFactory(project);
        return factory == ProjectionFactory.VIRTUAL || factory == ProjectionFactory.SELECTION;
    }

    /**
     * Whether the generator folds the constant filter the join step applies to its joined rows: any constant after
     * UNNEST, only a literal after another join.
     */
    public static boolean isPostJoinFilterFolded(JoinInput step, BoundExpression filter) {
        return step.getJoinType() == JoinKind.UNNEST || filter instanceof ConstantExpression constant && constant.isLiteral();
    }

    /**
     * Whether the generator hash-joins the master to an empty slave instead of the step's input: a keyed LEFT join
     * whose ON residual is constant false, so no slave row matches and the join emits each master row once.
     */
    public static boolean isSlaveEmptied(JoinInput step) {
        return step.getJoinType() == JoinKind.LEFT_OUTER && isNeverMatchingKeyedStep(step);
    }

    /**
     * True when operator planning may drop the master's designated timestamp from the projection: a projection over a
     * window join that the window join factory does not build, see {@link #isWindowJoinTimestampKept}.
     */
    public static boolean isWindowJoinTimestampDroppable(ProjectPlan project) {
        return project.getChild() instanceof WindowJoinPlan && projectionFactory(project) != ProjectionFactory.WINDOW_JOIN;
    }

    /**
     * True when a computing projection over the window join keeps the master's designated timestamp: the columns it
     * reads, in the order the SELECT list names them, list the master timestamp at its position among the master's
     * columns. {@code order} and {@code readIds} are scratch lists.
     */
    public static boolean isWindowJoinTimestampKept(ProjectPlan project, WindowJoinPlan windowJoin, IntList order, IntList readIds) {
        final OutputSchema input = project.getChild().getOutput();
        final ObjList<BoundExpression> expressions = project.getExpressions();
        order.clear();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            final int position = expressions.getQuick(i).getPosition();
            int index = order.size();
            while (index > 0 && position < expressions.getQuick(order.getQuick(index - 1)).getPosition()) {
                index--;
            }
            order.insert(index, i);
        }
        readIds.clear();
        for (int i = 0, n = order.size(); i < n; i++) {
            LogicalPlans.collectInputColumnIds(expressions.getQuick(order.getQuick(i)), input, readIds);
        }
        final OutputSchema master = windowJoin.getMaster().getOutput();
        final int index = master.getTimestampIndex();
        return index >= 0 && index < readIds.size() && readIds.getQuick(index) == master.getColumnId(index);
    }

    /**
     * True when the light cached window under the filter selects the filtered rows itself: the filter reads, at its
     * first position, the BOOLEAN result of the window's sole function, a row-selecting window function SUBSAMPLE
     * binds as its keep flag, which the projection above the filter drops.
     */
    public static boolean isWindowKeepFlag(FilterPlan filter) {
        final LogicalPlan input = filter.getChild();
        final WindowPlan window = generatedWindow(input);
        if (!(filter.getPredicate() instanceof ColumnExpression column) || window == null
                || window.getAlgorithm() != WindowPlan.Algorithm.CACHED_LIGHT || window.getFunctions().size() != 1
                || !window.getSpecs().getQuick(0).isSubsampleKeepFlag()) {
            return false;
        }
        final FunctionExpression function = window.getFunctions().getQuick(0);
        final int functionColumnId = window.getFunctionColumnIds().getQuick(0);
        final int functionIndex = input instanceof ProjectPlan project ? LogicalPlans.projectedColumnIndex(project, functionColumnId)
                : input.getOutput().getColumnIndexById(functionColumnId);
        return functionIndex >= 0 && functionIndex == input.getOutput().getColumnIndexById(column.getColumnId())
                && (function.getFunctionFlags() & BoundExpression.ROW_SELECTING) != 0 && function.getDataType() == ColumnType.BOOLEAN;
    }

    /**
     * A projection the window factory can emit directly: plain column references that select every
     * window output once, at unchanged types.
     */
    public static boolean isWindowOutputProjection(ProjectPlan project, WindowPlan window) {
        if (project.hasTimestampDeclaration()) {
            return false;
        }
        final OutputSchema input = window.getOutput();
        final int functionCount = window.getFunctionColumnIds().size();
        final int distinctWindowCount = project.getDistinctWindowColumnCount();
        if (distinctWindowCount >= 0 && distinctWindowCount != functionCount) {
            return false;
        }
        final int firstFunctionIndex = input.getColumnCount() - functionCount;
        int windowCount = 0;
        int lastFunctionIndex = -1;
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column)) {
                return false;
            }
            final int index = input.getColumnIndexById(column.getColumnId());
            if (index < 0 || column.isCast() || !column.isDirectReference() || input.getColumnType(index) != project.getOutput().getColumnType(i)) {
                return false;
            }
            if (index >= firstFunctionIndex) {
                if (distinctWindowCount < 0 && index <= lastFunctionIndex && LogicalPlans.isColumnSelectedBefore(project, i, column.getColumnId())) {
                    return false;
                }
                lastFunctionIndex = Math.max(lastFunctionIndex, index);
                if (++windowCount > functionCount) {
                    return false;
                }
            }
        }
        return windowCount == functionCount;
    }

    /**
     * The input the generator builds a LATEST BY over when it reads no table scan directly: its input, without a
     * projection that only declares the designated timestamp.
     */
    public static LogicalPlan latestByBase(LatestByPlan latest) {
        return timestampDeclarationBase(latest.getChild());
    }

    /**
     * The table scan a LATEST BY reads directly, optionally through one filter, which the generator builds the LATEST
     * BY into; null otherwise.
     */
    public static ScanPlan latestByScan(LatestByPlan latest) {
        return LogicalPlans.scanThroughFilter(latest.getChild());
    }

    /**
     * The join whose master rows a LIMIT reads a growing suffix of: the join under the LIMIT and its projections, when
     * the LIMIT may be negative and the join may emit no row for a master row, but emits the rows of each master row
     * together, in master order. The last {@code n} rows of such a join are the last {@code n} rows it emits for any
     * suffix of master rows it emits at least {@code n} rows for. Null otherwise.
     */
    public static JoinPlan masterSuffixJoin(LimitPlan limit) {
        return limit.getHi() == null && LogicalPlans.mayBeNegativeLimit(limit.getLo())
                && LogicalPlans.skipProjects(limit.getChild()) instanceof JoinPlan join
                && !LogicalPlans.isMasterRowPreserved(join) && LogicalPlans.isMasterRowGrouped(join) ? join : null;
    }

    /**
     * The master scan of {@link #masterSuffixJoin} the LIMIT reaches, whose parallel filter, when the scan has one,
     * returns as many of its last rows as the LIMIT asks for; null otherwise.
     */
    public static ScanPlan masterSuffixScan(LimitPlan limit) {
        final JoinPlan join = masterSuffixJoin(limit);
        if (join == null) {
            return null;
        }
        final ScanPlan scan = LogicalPlans.scanThroughFilter(LogicalPlans.skipProjects(join.getOrderedInputs().getQuick(0).getInput()));
        return scan != null && scan.getLimitLo() == limit.getLo() ? scan : null;
    }

    /**
     * The projection a parallel top-K over the sort's input builds over itself: the one projection the generator
     * builds for the input, unless the projection's input is another; null otherwise.
     */
    public static ProjectPlan parallelTopKProjection(SortPlan sort) {
        final LogicalPlan base = generatedPlan(sort.getChild());
        if (!isPeelableProjection(base)) {
            return null;
        }
        final ProjectPlan projection = (ProjectPlan) base;
        return isPeelableProjection(generatedPlan(projection.getChild())) ? null : projection;
    }

    /**
     * The input a parallel top-K reads: the input of the projection it builds over itself, else the sort's input.
     */
    public static LogicalPlan parallelTopKSource(SortPlan sort) {
        final ProjectPlan projection = parallelTopKProjection(sort);
        return projection != null ? projection.getChild() : sort.getChild();
    }

    /**
     * The scan of a posting index the generator builds a DISTINCT of the aggregate's single SYMBOL key from, read
     * directly or through one filter, whose predicate the scan's intervals implement whole; null otherwise.
     */
    public static ScanPlan postingDistinctScan(AggregatePlan aggregate) {
        final ScanPlan scan = LogicalPlans.scanThroughFilter(aggregate.getChild());
        return scan != null && scan.getAccessPath() == ScanPlan.AccessPath.POSTING_DISTINCT ? scan : null;
    }

    /**
     * The designated timestamp of the factory of a projection over an input whose factory designates
     * {@code inputTimestampIndex}: the one it declares, else the column its requested order reaches when that column
     * is the input's timestamp, else the timestamp it selects, lost when that column only passes through an input
     * that designates none, or when operator planning dropped it from a computing projection over a window join.
     */
    public static int projectedTimestampIndex(ProjectPlan project, int inputTimestampIndex) {
        final LogicalPlan input = project.getChild();
        final int timestampIndex = requestedTimestampIndex(project, inputTimestampIndex);
        if (timestampIndex < 0) {
            return -1;
        }
        if (!project.hasTimestampDeclaration() && inputTimestampIndex < 0 && !LogicalPlans.hasExplicitJoinTimestamp(input)
                && project.getExpressions().getQuick(timestampIndex) instanceof ColumnExpression timestamp
                && (input.getOutput().getTimestampIndex() < 0 || timestamp.getColumnId() == input.getOutput().getTimestampColumnId())) {
            return -1;
        }
        return project.isTimestampDropped() && isWindowJoinTimestampDroppable(project) ? -1 : timestampIndex;
    }

    /**
     * The factory the generator builds for the projection: the window factory for a projection of the window's output,
     * the window join factory for a column-only projection of the window join's output, else a virtual projection
     * when it computes a column and a selection otherwise.
     */
    public static ProjectionFactory projectionFactory(ProjectPlan project) {
        final LogicalPlan input = project.getChild();
        if (input instanceof WindowPlan window && isWindowOutputProjection(project, window)) {
            return ProjectionFactory.WINDOW;
        }
        if (input instanceof WindowJoinPlan && LogicalPlans.isColumnOnlyProjection(project)) {
            return ProjectionFactory.WINDOW_JOIN;
        }
        return LogicalPlans.isComputedProjection(project) ? ProjectionFactory.VIRTUAL : ProjectionFactory.SELECTION;
    }

    /**
     * The input the generator builds a SAMPLE BY over: its input, without a projection that only declares the
     * designated timestamp unless the SAMPLE BY reads the timestamp that projection declares.
     */
    public static LogicalPlan sampleByBase(SampleByPlan sample) {
        final LogicalPlan sampled = sample.getChild();
        return sample.isTimestampRequired() ? sampled : timestampDeclarationBase(sampled);
    }

    /**
     * The filter node the generator builds the factory of {@code plan} from, which a parallel consumer of the plan
     * steals; null when it builds another factory.
     */
    public static FilterPlan stolenFilter(LogicalPlan plan) {
        return generatedPlan(plan) instanceof FilterPlan filter ? filter : null;
    }

    /**
     * The selection an ASOF or HORIZON JOIN reads the filter of its slave through, or null when it reads the filter
     * directly.
     */
    public static ProjectPlan temporalSlaveProjection(LogicalPlan slave) {
        return generatedPlan(slave) instanceof ProjectPlan project && isPeelableProjection(project)
                && !LogicalPlans.isComputedProjection(project) ? project : null;
    }

    /**
     * The filter node an ASOF or HORIZON JOIN reads its slave under: the filter the generator builds the slave from,
     * or the one under the selection it builds the slave from.
     */
    public static FilterPlan temporalStolenFilter(LogicalPlan slave) {
        final ProjectPlan projection = temporalSlaveProjection(slave);
        return stolenFilter(projection != null ? projection.getChild() : slave);
    }

    /**
     * The vector implementation of an aggregate call over a direct column, or of {@code count()}; null when it has none.
     */
    public static VectorAggregateFunctionConstructor vectorConstructor(FunctionExpression call) {
        if (!call.isAggregate()) {
            return null;
        }
        final int count = call.getArgumentCount();
        if (count == 0) {
            return VectorAggregateConstructors.of(call.getName(), ColumnType.UNDEFINED, true);
        }
        if (count == 1 && call.argumentAt(0) instanceof ColumnExpression column) {
            return VectorAggregateConstructors.of(call.getName(), column.getDataType(), false);
        }
        return null;
    }

    /**
     * The column a vector GROUP BY keys on: an INT or SYMBOL column, or the timestamp under {@code hour()}; null when
     * the key has no vector form.
     */
    public static ColumnExpression vectorKey(BoundExpression key) {
        if (key instanceof ColumnExpression column) {
            return column.getDataType() == ColumnType.INT || column.getDataType() == ColumnType.SYMBOL ? column : null;
        }
        if (key instanceof FunctionExpression call && SqlKeywords.isHourKeyword(call.getName())
                && call.getArgumentCount() == 1 && call.argumentAt(0) instanceof ColumnExpression column
                && ColumnType.isTimestamp(column.getDataType())) {
            return column;
        }
        return null;
    }

    /**
     * The designated timestamp a column-only projection over a window join keeps: the master timestamp at
     * {@code timestampIndex}, while the projection selects only master columns, those below {@code splitIndex},
     * before it.
     */
    public static int windowJoinProjectionTimestampIndex(ProjectPlan projection, OutputSchema output, int timestampIndex, int splitIndex) {
        for (int i = 0, n = projection.getExpressions().size(); i < n; i++) {
            final int index = output.getColumnIndexById(((ColumnExpression) projection.getExpressions().getQuick(i)).getColumnId());
            if (index == timestampIndex) {
                return i;
            }
            if (index >= splitIndex) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Appends the output index and type of the column a within() call tests, then the GeoHash prefixes it matches
     * normalised to the column's precision; when a prefix is not a constant the column takes, restores the list and
     * returns false.
     */
    public static boolean withinPrefixes(FunctionExpression within, OutputSchema output, LongList prefixes) {
        final ColumnExpression column = (ColumnExpression) within.argumentAt(0);
        final int columnType = column.getDataType();
        final int start = prefixes.size();
        prefixes.add(output.getColumnIndexById(column.getColumnId()));
        prefixes.add(columnType);
        for (int i = 1, n = within.getArgumentCount(); i < n; i++) {
            if (!(within.argumentAt(i) instanceof ConstantExpression prefix)) {
                prefixes.setPos(start);
                return false;
            }
            try {
                GeoHashes.addNormalizedGeoPrefix(prefix.getLongValue(), prefix.getDataType(), columnType, prefixes);
            } catch (NumericException e) {
                prefixes.setPos(start);
                return false;
            }
        }
        return true;
    }

    private static boolean isNeverMatchingKeyedStep(JoinInput step) {
        return step.getKeys().size() > 0 && LogicalPlans.constantTruth(step.getOnResidual()) == 0;
    }

    /**
     * Whether the aggregate returns a TIMESTAMP column of the only row of its input, a LIMIT 1, see
     * {@link LogicalPlans#endpointColumn}: the shape {@link LogicalPlans#isTimestampEndpoint timestamp endpoints}
     * take once planned.
     */
    private static boolean isSingleRowTimestamp(AggregatePlan aggregate) {
        final ColumnExpression column = LogicalPlans.endpointColumn(aggregate);
        return column != null && ColumnType.isTimestamp(column.getDataType()) && aggregate.getChild() instanceof LimitPlan limit
                && limit.getHi() == null && limit.getLo() instanceof ConstantExpression lo && LogicalPlans.limitValue(lo) == 1;
    }

    /**
     * The designated timestamp a projection declares over an input whose factory designates
     * {@code inputTimestampIndex}: its own, else the column its requested order reaches when that column is the
     * input's timestamp.
     */
    private static int requestedTimestampIndex(ProjectPlan project, int inputTimestampIndex) {
        if (!project.getRequestedOrder().isEmpty() && inputTimestampIndex < 0 && hasNativeFilterInput(project)) {
            return -1;
        }
        final int orderColumnId = project.getRequestedOrderColumnId();
        final int inputOrderId = LogicalPlans.projectedSourceColumnId(project, orderColumnId);
        return project.getOutput().getTimestampIndex() < 0 && inputOrderId >= 0
                && inputTimestampIndex == project.getChild().getOutput().getColumnIndexById(inputOrderId)
                ? project.getOutput().getColumnIndexById(orderColumnId) : project.getOutput().getTimestampIndex();
    }

    private static LogicalPlan timestampDeclarationBase(LogicalPlan plan) {
        return LogicalPlans.isTimestampDeclarationOnly(plan) ? plan.inputAt(0) : plan;
    }

    /**
     * The factory the generator builds for an aggregate, see {@link #aggregateFactory}.
     */
    public enum AggregateFactory {
        /**
         * The DISTINCT of a single SYMBOL key read from a posting index, see {@link #postingDistinctScan}.
         */
        POSTING_DISTINCT,
        /**
         * The aggregate over a horizon join, built with the join.
         */
        HORIZON,
        /**
         * The count of the input rows, see {@link LogicalPlans#isCount}.
         */
        COUNT,
        /**
         * The timestamp the aggregate reads from the only row of its input, or NULL without one, see
         * {@link #isSingleRowTimestamp}.
         */
        TIMESTAMP_ENDPOINT,
        /**
         * The GROUP BY by the algorithm operator planning records.
         */
        GROUP_BY
    }

    /**
     * The factory the generator builds for a projection, see {@link #projectionFactory}.
     */
    public enum ProjectionFactory {
        /**
         * The window factory, which emits the projected window output itself.
         */
        WINDOW,
        /**
         * The window join factory, which emits the projected window join output itself.
         */
        WINDOW_JOIN,
        /**
         * A projection that computes its columns over its input.
         */
        VIRTUAL,
        /**
         * A selection of input columns, or none when it selects every column in place.
         */
        SELECTION
    }
}
