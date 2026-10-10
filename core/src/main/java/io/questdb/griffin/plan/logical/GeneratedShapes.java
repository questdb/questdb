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
public final class GeneratedShapes {

    private GeneratedShapes() {
    }

    /**
     * The input the generator builds a GROUP BY over: the aggregate's input past renames, without a projection that
     * only declares the designated timestamp.
     */
    public static LogicalPlan aggregateBase(AggregatePlan aggregate) {
        return timestampDeclarationBase(LogicalPlans.skipRenames(aggregate.getInput()));
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
     * The table scan the generator builds the filter into, or null when it filters the factory of its input.
     */
    public static ScanPlan fusedScan(FilterPlan filter) {
        return filter.getInput() instanceof ScanPlan scan && !scan.isWalClientUpdate() ? scan : null;
    }

    /**
     * The plan the generator builds the factory of {@code plan} from: a projection that passes its input through
     * and a filter it folds to true build none.
     */
    public static LogicalPlan generatedPlan(LogicalPlan plan) {
        while (true) {
            if (plan instanceof ProjectPlan project && isIdentityProjection(project)) {
                plan = project.getInput();
            } else if (plan instanceof FilterPlan filter && !isFusedFilter(filter)
                    && filter.getPredicate() instanceof ConstantExpression constant && constant.getLongValue() != 0) {
                plan = filter.getInput();
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
        return plan instanceof ProjectPlan project && project.getInput() instanceof WindowPlan window
                && isWindowOutputProjection(project, window) ? window : null;
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
     * True when the plan, under any projections, filters a table scan, so the scan's access path serves the filter.
     */
    public static boolean hasNativeFilterInput(LogicalPlan plan) {
        plan = LogicalPlans.skipProjects(plan);
        return plan instanceof FilterPlan filter && filter.getInput() instanceof ScanPlan;
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
     * True when the generator builds the filter into the factory of the table scan under it.
     */
    public static boolean isFusedFilter(FilterPlan filter) {
        return fusedScan(filter) != null;
    }

    /**
     * True when the generator builds no factory for the projection over the factory of its input, see
     * {@link #isIdentityProjection(ProjectPlan, RecordMetadata, int, int)}, whose columns operator planning takes to be
     * the input's output schema. Over a join of several inputs, or a filter over one, the factory names its columns
     * qualifier.name, so the generator builds a selection where this answers true; the planner reads the answer only
     * to look through the projection for a filter, a scan or page frames, which such a join exposes no more than the
     * selection does.
     */
    public static boolean isIdentityProjection(ProjectPlan project) {
        return isIdentityProjection(project, null, PhysicalProperties.timestampIndex(project),
                PhysicalProperties.timestampIndex(project.getInput()));
    }

    /**
     * True when the generator builds no factory for the projection: it selects every input column, in order, under
     * the name and type the input's factory gives it, {@code layout}, or the input's output schema when null, and
     * designates the timestamp the input's factory designates. A SELECT list over GROUP BY keeps the key spelling when
     * it only changes the name case.
     */
    public static boolean isIdentityProjection(ProjectPlan project, @Nullable RecordMetadata layout, int timestampIndex, int inputTimestampIndex) {
        final LogicalPlan input = project.getInput();
        if (LogicalPlans.isComputedProjection(project) || input instanceof WindowPlan window && isWindowOutputProjection(project, window)
                || input instanceof WindowJoinPlan && LogicalPlans.isColumnOnlyProjection(project)) {
            return false;
        }
        final OutputSchema output = project.getOutput();
        final OutputSchema inputOutput = input.getOutput();
        if (output.getColumnCount() != (layout == null ? inputOutput.getColumnCount() : layout.getColumnCount())
                || timestampIndex != inputTimestampIndex) {
            return false;
        }
        final boolean isKeySpellingKept = LogicalPlans.skipFilters(input) instanceof AggregatePlan aggregate
                && aggregate.hasKeySpellingKept() && !(aggregate.getInput() instanceof HorizonJoinPlan);
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
     * True when the generator builds a projection factory for the plan, which a parallel top-K can build over itself.
     */
    public static boolean isPeelableProjection(LogicalPlan plan) {
        if (!(plan instanceof ProjectPlan project)) {
            return false;
        }
        final LogicalPlan input = project.getInput();
        return !(input instanceof WindowPlan window && isWindowOutputProjection(project, window))
                && !(input instanceof WindowJoinPlan && LogicalPlans.isColumnOnlyProjection(project));
    }

    /**
     * Whether the generator folds the constant filter the join step applies to its joined rows: any constant after
     * UNNEST, only a literal after another join.
     */
    public static boolean isPostJoinFilterFolded(JoinInput step, BoundExpression filter) {
        return step.getJoinType() == JoinKind.UNNEST || filter instanceof ConstantExpression constant && constant.isLiteral();
    }

    /**
     * True when a computing projection over the window join keeps the master's designated timestamp: the columns it
     * reads, in the order the SELECT list names them, list the master timestamp at its position among the master's
     * columns. {@code order} and {@code readIds} are scratch lists.
     */
    public static boolean isWindowJoinTimestampKept(ProjectPlan project, WindowJoinPlan windowJoin, IntList order, IntList readIds) {
        final OutputSchema input = project.getInput().getOutput();
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
     * A projection the window factory can emit directly: plain column references that select every
     * window output once, at unchanged types.
     */
    public static boolean isWindowOutputProjection(ProjectPlan project, WindowPlan window) {
        if (project.hasTimestampDeclaration()) {
            return false;
        }
        final OutputSchema input = window.getOutput();
        int windowCount = 0;
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column)) {
                return false;
            }
            final int index = input.getColumnIndexById(column.getColumnId());
            if (index < 0 || column.isCast() || !column.isDirectReference() || input.getColumnType(index) != project.getOutput().getColumnType(i)) {
                return false;
            }
            if (window.getFunctionColumnIds().indexOf(column.getColumnId(), 0, window.getFunctionColumnIds().size()) >= 0) {
                if (LogicalPlans.isColumnSelectedBefore(project, i, column.getColumnId())) {
                    return false;
                }
                windowCount++;
            }
        }
        return windowCount == window.getFunctionColumnIds().size();
    }

    /**
     * The input the generator builds a LATEST BY over when it reads no table scan directly: its input, without a
     * projection that only declares the designated timestamp.
     */
    public static LogicalPlan latestByBase(LatestByPlan latest) {
        return timestampDeclarationBase(latest.getInput());
    }

    /**
     * The table scan a LATEST BY reads directly, optionally through one filter, which the generator builds the LATEST
     * BY into; null otherwise.
     */
    public static ScanPlan latestByScan(LatestByPlan latest) {
        return LogicalPlans.scanThroughFilter(latest.getInput());
    }

    /**
     * The projection a parallel top-K over the sort's input builds over itself: the one projection the generator
     * builds for the input, unless the projection's input is another; null otherwise.
     */
    public static ProjectPlan parallelTopKProjection(SortPlan sort) {
        final LogicalPlan base = generatedPlan(sort.getInput());
        if (!isPeelableProjection(base)) {
            return null;
        }
        final ProjectPlan projection = (ProjectPlan) base;
        return isPeelableProjection(generatedPlan(projection.getInput())) ? null : projection;
    }

    /**
     * The input a parallel top-K reads: the input of the projection it builds over itself, else the sort's input.
     */
    public static LogicalPlan parallelTopKSource(SortPlan sort) {
        final ProjectPlan projection = parallelTopKProjection(sort);
        return projection != null ? projection.getInput() : sort.getInput();
    }

    /**
     * The scan of a posting index the generator builds a DISTINCT of the aggregate's single SYMBOL key from, read
     * directly or through one filter, whose predicate the scan's intervals implement whole; null otherwise.
     */
    public static ScanPlan postingDistinctScan(AggregatePlan aggregate) {
        final ScanPlan scan = LogicalPlans.scanThroughFilter(aggregate.getInput());
        return scan != null && scan.getAccessPath() == ScanPlan.AccessPath.POSTING_DISTINCT ? scan : null;
    }

    /**
     * The designated timestamp of the factory of a projection over an input whose factory designates
     * {@code inputTimestampIndex}: the one it declares, else the column its requested order reaches when that column
     * is the input's timestamp, else the timestamp it selects, lost when that column only passes through an input
     * that designates none, or when operator planning dropped it from a computing projection over a window join.
     */
    public static int projectedTimestampIndex(ProjectPlan project, int inputTimestampIndex) {
        final LogicalPlan input = project.getInput();
        final int timestampIndex = requestedTimestampIndex(project, inputTimestampIndex);
        if (timestampIndex < 0) {
            return -1;
        }
        if (!project.hasTimestampDeclaration() && inputTimestampIndex < 0 && !LogicalPlans.hasExplicitJoinTimestamp(input)
                && project.getExpressions().getQuick(timestampIndex) instanceof ColumnExpression timestamp
                && (input.getOutput().getTimestampIndex() < 0 || timestamp.getColumnId() == input.getOutput().getTimestampColumnId())) {
            return -1;
        }
        return input instanceof WindowJoinPlan && !LogicalPlans.isColumnOnlyProjection(project) && project.isTimestampDropped() ? -1 : timestampIndex;
    }

    /**
     * The input the generator builds a SAMPLE BY over: its input, without a projection that only declares the
     * designated timestamp unless the SAMPLE BY reads the timestamp that projection declares.
     */
    public static LogicalPlan sampleByBase(SampleByPlan sample) {
        final LogicalPlan sampled = sample.getInput();
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
     * The selection a temporal join reads the stolen filter of its slave through, or null when it reads the filter
     * directly.
     */
    public static ProjectPlan temporalSlaveProjection(LogicalPlan slave) {
        return generatedPlan(slave) instanceof ProjectPlan project && isPeelableProjection(project)
                && !LogicalPlans.isComputedProjection(project) ? project : null;
    }

    /**
     * The filter node a temporal join steals from its slave: the filter the generator builds the slave from, or the
     * one under the selection it builds the slave from.
     */
    public static FilterPlan temporalStolenFilter(LogicalPlan slave) {
        final ProjectPlan projection = temporalSlaveProjection(slave);
        return stolenFilter(projection != null ? projection.getInput() : slave);
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
                && inputTimestampIndex == project.getInput().getOutput().getColumnIndexById(inputOrderId)
                ? project.getOutput().getColumnIndexById(orderColumnId) : project.getOutput().getTimestampIndex();
    }

    private static LogicalPlan timestampDeclarationBase(LogicalPlan plan) {
        return LogicalPlans.isTimestampDeclarationOnly(plan) ? plan.inputAt(0) : plan;
    }
}
