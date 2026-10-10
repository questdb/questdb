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

package io.questdb.griffin.optimiser;

import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.OuterColumnReads;
import io.questdb.griffin.PlanNodePools;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.ForwardingPlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.Chars;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Rewrite state {@link Decorrelation} shares with its collaborators: the master join of the step being
 * rewritten, the mapping from outer columns to the columns that satisfy them, column ids and names.
 */
final class DecorrelationContext implements Mutable {
    static final String OUTER_REF_PREFIX = "__qdb_outer_ref__";
    final ObjList<JoinInput> carrierSteps = new ObjList<>();
    final ObjList<LogicalPlan> chain;
    final IntList chainOuterIds = new IntList();
    final CharacterStore characterStore;
    final OptimiserContext context;
    final LogicalPlanCopier copier;
    final IntList mappedColumnIds;
    final IntList mappedOuterIds;
    final IntList masterOuterIds = new IntList();
    final IntIntHashMap outerAliases = new IntIntHashMap();
    final OuterColumnReads outerColumnReads = new OuterColumnReads();
    final PlanNodePools planNodes;
    final IntList tmpColumnIds;
    final IntIntHashMap substitution = new IntIntHashMap();
    private final OutputSchema tmpSchema;
    private IntList columnIdSink;
    private IntList readIds;
    private final ExpressionVisitor columnIdReads = expression -> expression instanceof ColumnExpression column
            && readIds.contains(column.getColumnId()) ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private final ExpressionVisitor outerIdReads = expression -> expression instanceof OuterColumnExpression outer
            && readIds.contains(outer.getColumnId()) ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private int readMappingBase;
    private OutputSchema readOutput;
    private final ExpressionVisitor outputColumnReads = expression -> expression instanceof ColumnExpression column
            && readOutput.getColumnIndexById(column.getColumnId()) > -1 ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private final PlanVisitor outerRefNames = this::findOuterRefName;
    private final ExpressionVisitor unmappedOuterReads = this::findUnmappedOuter;
    private final ExpressionVisitor columnIdCollector = expression -> {
        if (expression instanceof ColumnExpression column && !columnIdSink.contains(column.getColumnId())) {
            columnIdSink.add(column.getColumnId());
        }
        return TreeWalk.CONTINUE;
    };
    int carrierSequence;
    JoinPlan master;
    int masterLimit;
    int outerRefSequence = -1;

    DecorrelationContext(
            OptimiserContext context,
            PlanNodePools planNodes,
            CharacterStore characterStore,
            IntList tmpColumnIds,
            IntList mappedOuterIds,
            IntList mappedColumnIds,
            ObjList<LogicalPlan> chain,
            OutputSchema tmpSchema
    ) {
        this.context = context;
        this.planNodes = planNodes;
        this.characterStore = characterStore;
        this.tmpColumnIds = tmpColumnIds;
        this.mappedOuterIds = mappedOuterIds;
        this.mappedColumnIds = mappedColumnIds;
        this.chain = chain;
        this.tmpSchema = tmpSchema;
        this.copier = new LogicalPlanCopier(context, planNodes);
    }

    @Override
    public void clear() {
        releaseCarriers();
        chainOuterIds.clear();
        copier.clear();
        mappedColumnIds.clear();
        mappedOuterIds.clear();
        masterOuterIds.clear();
        outerAliases.clear();
        substitution.clear();
        tmpColumnIds.clear();
        carrierSequence = 0;
        master = null;
        masterLimit = 0;
        outerRefSequence = -1;
    }

    private static int parseOuterRefSequence(CharSequence name) {
        if (!Chars.startsWith(name, OUTER_REF_PREFIX)) {
            return -1;
        }
        int sequence = -1;
        for (int i = OUTER_REF_PREFIX.length(), n = name.length(); i < n; i++) {
            final char c = name.charAt(i);
            if (c < '0' || c > '9') {
                break;
            }
            sequence = Math.max(sequence, 0) * 10 + c - '0';
        }
        return sequence;
    }

    private static boolean reads(BoundExpression expression, ExpressionVisitor visitor) {
        return expression != null && !expression.walk(visitor);
    }

    /**
     * Lays out a window join over its decorrelated master: master columns, then the step aggregates, with each
     * step's scopes rebuilt over that layout.
     */
    private void alignWindowJoin(WindowJoinPlan windowJoin) {
        final OutputSchema output = windowJoin.getOutput();
        final OutputSchema master = windowJoin.getMaster().getOutput();
        alignColumns(output, master);
        int prefix = master.getColumnCount();
        for (int s = 0, m = windowJoin.getSteps().size(); s < m; s++) {
            final WindowJoinStep step = windowJoin.getSteps().getQuick(s);
            final OutputSchema masterScope = step.getMasterScope();
            masterScope.clear();
            for (int i = 0; i < prefix; i++) {
                masterScope.addColumnFrom(output, i);
            }
            final OutputSchema scope = step.getScope();
            scope.copyFrom(masterScope);
            scope.addColumnsFrom(step.getSlave().getOutput(), step.getSlaveAlias());
            prefix += step.getAggregateColumnIds().size();
        }
    }

    private int findOuterRefName(LogicalPlan plan) {
        final OutputSchema output = plan.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (parseOuterRefSequence(output.getColumnName(i)) == outerRefSequence) {
                return TreeWalk.STOP;
            }
        }
        return TreeWalk.CONTINUE;
    }

    private int findUnmappedOuter(BoundExpression expression) {
        return expression instanceof OuterColumnExpression outer && masterOuterIds.contains(outer.getColumnId())
                && mappedColumn(outer.getColumnId(), readMappingBase, mappedOuterIds.size()) < 0 ? TreeWalk.STOP : TreeWalk.CONTINUE;
    }

    /**
     * Records, as carriers of the step, the columns {@link #substitution} maps the outer columns its ON condition and
     * key filter read to.
     */
    private void recordCarriers(JoinInput step) {
        recordCarriers(step, step.getOnResidual());
        recordCarriers(step, step.getKeyFilter());
    }

    private void recordCarriers(JoinInput step, BoundExpression expression) {
        if (expression == null) {
            return;
        }
        final int base = tmpColumnIds.size();
        outerColumnReads.collect(expression, tmpColumnIds);
        for (int i = base, n = tmpColumnIds.size(); i < n; i++) {
            final int carrierId = substitution.get(tmpColumnIds.getQuick(i));
            if (carrierId > -1) {
                addCarrier(step, carrierId);
            }
        }
        tmpColumnIds.setPos(base);
    }

    static boolean isTrue(BoundExpression condition) {
        return condition == null || condition instanceof ConstantExpression constant && constant.getLongValue() != 0;
    }

    static int pairIndex(IntList pairs, int key) {
        for (int i = 0, n = pairs.size(); i < n; i += 2) {
            if (pairs.getQuick(i) == key) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Records a carrier of the step for {@link PlanVerifier}, which reads it right after decorrelation; the step's
     * carriers stay until {@link #releaseCarriers}.
     */
    void addCarrier(JoinInput step, int columnId) {
        final IntList carriers = step.getCarrierColumnIds();
        if (carriers.size() == 0) {
            carrierSteps.add(step);
        }
        if (!carriers.contains(columnId)) {
            carriers.add(columnId);
        }
    }

    void addMapping(int outerId, int columnId) {
        mappedOuterIds.add(outerId);
        mappedColumnIds.add(columnId);
    }

    /**
     * Lays out {@code output} as the columns of {@code input} in input order, adding the missing ones hidden, followed
     * by the columns the node itself defines; the designated timestamp keeps its column.
     */
    void alignColumns(OutputSchema output, OutputSchema input) {
        final OutputSchema copy = tmpSchema;
        copy.copyFrom(output);
        final int timestampId = output.getTimestampColumnId();
        output.clear();
        for (int i = 0, n = input.getColumnCount(); i < n; i++) {
            final int index = copy.getColumnIndexById(input.getColumnId(i));
            if (index < 0) {
                output.add(input.getColumnId(i), input.getColumnName(i), input.getColumnType(i), input.getMetadata(i), false,
                        input.getColumnQualifier(i));
            } else {
                output.addColumnFrom(copy, index);
            }
        }
        for (int i = 0, n = copy.getColumnCount(); i < n; i++) {
            if (input.getColumnIndexById(copy.getColumnId(i)) < 0) {
                output.addColumnFrom(copy, i);
            }
        }
        output.setTimestampColumnId(timestampId);
        copy.clear();
    }

    /**
     * CASE WHEN {@code condition} THEN {@code then} ELSE {@code otherwise} END over the input.
     */
    BoundExpression caseWhen(BoundExpression condition, BoundExpression then, BoundExpression otherwise, OutputSchema input, int position)
            throws SqlException {
        final ObjList<BoundExpression> arguments = context.getCallArguments();
        arguments.clear();
        arguments.add(condition);
        arguments.add(then);
        arguments.add(otherwise);
        return context.bindCall("case", position, input);
    }

    void collectColumnIds(BoundExpression expression, IntList sink) {
        columnIdSink = sink;
        try {
            expression.walk(columnIdCollector);
        } finally {
            columnIdSink = null;
        }
    }

    ColumnExpression column(OutputSchema output, int columnId, int position) {
        return planNodes.columns.next().of(columnId, output.getColumnType(output.getColumnIndexById(columnId)), position);
    }

    /**
     * Appends a hidden column of the projection that reads {@code columnId} of the input; returns its id.
     */
    int exposeColumn(ProjectPlan project, OutputSchema input, int columnId, CharSequence name, int position) {
        final int type = input.getColumnType(input.getColumnIndexById(columnId));
        project.getExpressions().add(planNodes.columns.next().of(columnId, type, position));
        final int exposedId = context.newColumnId();
        project.getOutput().add(exposedId, name, type, false);
        return exposedId;
    }

    /**
     * A projection that re-projects every column of the input under a fresh id, attribute for attribute and in
     * input order; {@code remap}, when given, maps each column to its fresh id.
     */
    ProjectPlan forwardingProjection(LogicalPlan input, @Nullable IntIntHashMap remap, int position) {
        final ProjectPlan project = planNodes.projects.next().of(input, position);
        final OutputSchema source = input.getOutput();
        final OutputSchema output = project.getOutput();
        for (int i = 0, n = source.getColumnCount(); i < n; i++) {
            final int columnId = source.getColumnId(i);
            final int type = source.getColumnType(i);
            project.getExpressions().add(planNodes.columns.next().of(columnId, type, position));
            final int forwardedId = context.newColumnId();
            output.add(forwardedId, source.getColumnName(i), type, source.getMetadata(i), source.isVisible(i), source.getColumnQualifier(i));
            if (remap != null) {
                remap.put(columnId, forwardedId);
            }
        }
        return project;
    }

    boolean hasOuterRefName(LogicalPlan plan) {
        return !plan.walkTopDown(outerRefNames);
    }

    void insertColumn(OutputSchema output, int index, int columnId, CharSequence name, int type) {
        final OutputSchema copy = tmpSchema;
        copy.copyFrom(output);
        final int timestampId = output.getTimestampColumnId();
        output.clear();
        for (int i = 0, n = copy.getColumnCount(); i <= n; i++) {
            if (i == index) {
                output.add(columnId, name, type, false);
            }
            if (i < n) {
                output.addColumnFrom(copy, i);
            }
        }
        output.setTimestampColumnId(timestampId);
        copy.clear();
    }

    CharSequence joinedName(OutputSchema output, int columnId) {
        final int index = output.getColumnIndexById(columnId);
        return qualifiedName(output.getColumnQualifier(index), output.getColumnName(index));
    }

    /**
     * Loads {@link #substitution} with the mapping above {@code base}: each outer column to its mapped column.
     */
    void loadMappedSubstitution(int base) {
        substitution.clear();
        for (int i = base, n = mappedOuterIds.size(); i < n; i++) {
            substitution.put(mappedOuterIds.getQuick(i), mappedColumnIds.getQuick(i));
        }
    }

    int mappedColumn(int outerId, int lo, int hi) {
        for (int i = lo; i < hi; i++) {
            if (mappedOuterIds.getQuick(i) == outerId) {
                return mappedColumnIds.getQuick(i);
            }
        }
        return -1;
    }

    int mappedIndex(int columnId, int lo, int hi) {
        for (int i = lo; i < hi; i++) {
            if (mappedColumnIds.getQuick(i) == columnId) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The master column an outer column reads: itself, or the master column the join's WHERE equates an outer
     * column of an enclosing lateral to.
     */
    int masterColumn(int outerId) {
        final int alias = outerAliases.get(outerId);
        return alias < 0 ? outerId : alias;
    }

    CharSequence masterColumnName(int outerId) {
        final OutputSchema output = master.getOutput();
        final int index = output.getColumnIndexById(masterColumn(outerId));
        return qualifiedName(output.getColumnQualifier(index), output.getColumnName(index));
    }

    /**
     * The expression over the master columns: outer columns read the master columns they stand for.
     */
    BoundExpression masterExpression(BoundExpression expression) {
        if (expression == null || !LogicalPlans.hasOuterColumn(expression)) {
            return expression;
        }
        final int columnBase = tmpColumnIds.size();
        outerColumnReads.collect(expression, tmpColumnIds);
        substitution.clear();
        for (int i = columnBase, n = tmpColumnIds.size(); i < n; i++) {
            substitution.put(tmpColumnIds.getQuick(i), masterColumn(tmpColumnIds.getQuick(i)));
        }
        tmpColumnIds.setPos(columnBase);
        return context.getRewriter().remapColumns(expression, substitution);
    }

    int masterInput(int outerId) {
        for (int i = 0; i < masterLimit; i++) {
            if (master.getInputs().getQuick(i).getSourceOutput().getColumnIndexById(masterColumn(outerId)) > -1) {
                return i;
            }
        }
        return -1;
    }

    CharSequence outerRefName(int outerId) {
        final OutputSchema output = master.getOutput();
        final CharacterStoreEntry name = characterStore.newEntry();
        final int index = output.getColumnIndexById(masterColumn(outerId));
        final CharSequence columnName = output.getColumnName(index);
        name.put(OUTER_REF_PREFIX).put(outerRefSequence).put('_').put(columnName);
        int sameNameCount = 0;
        for (int i = 0; i < index; i++) {
            if (Chars.equalsIgnoreCase(output.getColumnName(i), columnName)) {
                sameNameCount++;
            }
        }
        if (sameNameCount > 0) {
            name.put(sameNameCount);
        }
        return name.toImmutable();
    }

    CharSequence qualifiedName(CharSequence qualifier, CharSequence name) {
        if (qualifier == null) {
            return name;
        }
        final CharacterStoreEntry entry = characterStore.newEntry();
        entry.put(qualifier).put('.').put(name);
        return entry.toImmutable();
    }

    /**
     * True when the expression reads one of the columns.
     */
    boolean readsAnyColumn(BoundExpression expression, IntList columnIds) {
        readIds = columnIds;
        return reads(expression, columnIdReads);
    }

    /**
     * True when the expression reads a column of the output.
     */
    boolean readsAnyColumn(BoundExpression expression, OutputSchema output) {
        readOutput = output;
        return reads(expression, outputColumnReads);
    }

    /**
     * True when the expression reads one of the outer columns.
     */
    boolean readsAnyOuter(BoundExpression expression, IntList outerIds) {
        readIds = outerIds;
        return reads(expression, outerIdReads);
    }

    /**
     * True when the expression reads an outer column of the master that no mapping above {@code base} satisfies.
     */
    boolean readsUnmappedOuter(BoundExpression expression, int base) {
        readMappingBase = base;
        return reads(expression, unmappedOuterReads);
    }

    /**
     * Lays the node's output out again over the changed output of its input.
     */
    void realign(LogicalPlan node) {
        switch (node) {
            case ForwardingPlan forwarding -> forwarding.deriveOutput();
            case WindowJoinPlan windowJoin -> alignWindowJoin(windowJoin);
            case WindowPlan _, HorizonJoinPlan _ -> alignColumns(node.getOutput(), node.inputAt(0).getOutput());
            default -> {
            }
        }
    }

    /**
     * Realigns every node of the single-input chain from {@code top} down to, excluding, {@code bottom}, lowest first.
     */
    void realignAbove(LogicalPlan top, LogicalPlan bottom) {
        final int base = chain.size();
        for (LogicalPlan node = top; node != bottom; node = node.inputAt(0)) {
            chain.add(node);
        }
        for (int i = chain.size() - 1; i >= base; i--) {
            realign(chain.getQuick(i));
        }
        chain.setPos(base);
    }

    /**
     * Empties the carriers of every step decorrelation recorded them on: their column ids hold only until the next
     * pass renames columns.
     */
    void releaseCarriers() {
        for (int i = 0, n = carrierSteps.size(); i < n; i++) {
            carrierSteps.getQuick(i).getCarrierColumnIds().clear();
        }
        carrierSteps.clear();
    }

    /**
     * Renames, in the node itself, the outer columns of the mapping above {@code base} to their mapped columns, and
     * records the mapped columns a join step's ON condition reads as the step's carriers.
     */
    void remapMapped(LogicalPlan node, int base) {
        loadMappedSubstitution(base);
        if (node instanceof JoinPlan join) {
            for (int i = 0, n = join.getInputs().size(); i < n; i++) {
                recordCarriers(join.getInputs().getQuick(i));
            }
        }
        copier.remap(node, substitution);
    }

    /**
     * Records the step's carriers and renames, in its ON condition, key filter and, when {@code isFilterRead}, post-join
     * filter, the columns {@link #substitution} holds.
     */
    void remapStepConditions(JoinInput step, boolean isFilterRead) {
        recordCarriers(step);
        step.setOnResidual(context.getRewriter().remapColumns(step.getOnResidual(), substitution));
        step.setKeyFilter(context.getRewriter().remapColumns(step.getKeyFilter(), substitution));
        if (isFilterRead) {
            step.setPostJoinFilter(context.getRewriter().remapColumns(step.getPostJoinFilter(), substitution));
        }
    }

    /**
     * Gives each column of the output the name {@link OptimiserContext#uniqueName} assigns it after the columns before it.
     */
    void uniqueNames(OutputSchema output) {
        final OutputSchema seen = tmpSchema;
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final CharSequence name = context.uniqueName(seen, output.getColumnName(i));
            output.setColumnName(i, name, output.getColumnQualifier(i));
            seen.add(output.getColumnId(i), name, output.getColumnType(i), false);
        }
        seen.clear();
    }
}
