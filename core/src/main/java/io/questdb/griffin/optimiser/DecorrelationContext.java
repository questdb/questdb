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

import io.questdb.cairo.ColumnType;
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
import io.questdb.griffin.plan.logical.HorizonJoinSlave;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKey;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.UniqueColumnNames;
import io.questdb.griffin.plan.logical.UnnestSpec;
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
 * Rewrite state {@link Decorrelation} shares with its collaborators: the step being rewritten, its master join and
 * the outer columns of the master its body reads, the names decorrelation generates, and the carriers it records
 * for {@link DecorrelationVerifier}. Builds the expressions and columns the rewrite adds.
 */
final class DecorrelationContext implements Mutable {
    static final String OUTER_REF_PREFIX = "__qdb_outer_ref__";
    private static final String ROW_MARKER_PREFIX = "__qdb_row_marker__";
    @Nullable
    private final DecorrelationCarriers carriers;
    private final ObjList<LogicalPlan> chain;
    private final CharacterStore characterStore;
    private final OptimiserContext context;
    private final LogicalPlanCopier copier;
    private final OuterColumnMapping mapping;
    private final IntList masterOuterIds;
    private final IntIntHashMap outerAliases = new IntIntHashMap();
    private final OuterColumnReads outerColumnReads;
    private final PlanNodePools planNodes;
    private final IntIntHashMap substitution;
    private final UniqueColumnNames takenNames;
    private final IntList tmpColumnIds;
    private final OutputSchema tmpSchema;
    private int carrierSequence;
    private IntList columnIdSink;
    private int domainSequence;
    private JoinPlan master;
    private final PlanVisitor masterOuterCollector = this::collectNodeMasterOuterIds;
    private int masterLimit;
    private int outerRefSequence = -1;
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

    DecorrelationContext(
            OptimiserContext context,
            PlanNodePools planNodes,
            CharacterStore characterStore,
            OuterColumnMapping mapping,
            ObjList<LogicalPlan> chain,
            IntList tmpColumnIds,
            IntList masterOuterIds,
            OutputSchema tmpSchema,
            IntIntHashMap substitution,
            UniqueColumnNames takenNames,
            OuterColumnReads outerColumnReads,
            @Nullable DecorrelationCarriers carriers
    ) {
        this.context = context;
        this.planNodes = planNodes;
        this.characterStore = characterStore;
        this.mapping = mapping;
        this.chain = chain;
        this.tmpColumnIds = tmpColumnIds;
        this.masterOuterIds = masterOuterIds;
        this.tmpSchema = tmpSchema;
        this.substitution = substitution;
        this.takenNames = takenNames;
        this.outerColumnReads = outerColumnReads;
        this.carriers = carriers;
        this.copier = new LogicalPlanCopier(context, planNodes, substitution);
    }

    @Override
    public void clear() {
        if (carriers != null) {
            carriers.clear();
        }
        copier.clear();
        outerAliases.clear();
        carrierSequence = 0;
        domainSequence = 0;
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
     * Lays out {@code output} as the columns of {@code input} in input order, adding the missing ones hidden, followed
     * by the columns the node itself defines; the designated timestamp keeps its column.
     */
    private void alignColumns(OutputSchema output, OutputSchema input) {
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
     * Lays out a horizon join over its decorrelated inputs: master columns, the horizon offset and timestamp, then each
     * slave's columns.
     */
    private void alignHorizonJoin(HorizonJoinPlan horizon) {
        final OutputSchema output = horizon.getOutput();
        final OutputSchema master = horizon.getMaster().getOutput();
        alignColumns(output, master);
        final OutputSchema copy = tmpSchema;
        copy.copyFrom(output);
        output.clear();
        for (int i = 0, n = master.getColumnCount() + 2; i < n; i++) {
            output.addColumnFrom(copy, i);
        }
        for (int s = 0, m = horizon.getSlaves().size(); s < m; s++) {
            final HorizonJoinSlave slave = horizon.getSlaves().getQuick(s);
            output.addColumnsFrom(slave.getInput().getOutput(), slave.getAlias());
        }
        output.setTimestampColumnId(copy.getTimestampColumnId());
        copy.clear();
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

    private int collectNodeMasterOuterIds(LogicalPlan plan) {
        final int base = tmpColumnIds.size();
        outerColumnReads.collect(plan, tmpColumnIds);
        for (int i = base, n = tmpColumnIds.size(); i < n; i++) {
            final int outerId = tmpColumnIds.getQuick(i);
            if (masterInput(outerId) > -1 && !masterOuterIds.contains(outerId)) {
                masterOuterIds.add(outerId);
            }
        }
        tmpColumnIds.setPos(base);
        return TreeWalk.CONTINUE;
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
                && mapping.mappedColumn(outer.getColumnId(), readMappingBase) < 0 ? TreeWalk.STOP : TreeWalk.CONTINUE;
    }

    private boolean hasOuterRefName(LogicalPlan plan) {
        return !plan.walkTopDown(outerRefNames);
    }

    /**
     * Records, as carriers of the step, the columns {@link #substitution} maps the outer columns its ON condition, key
     * filter and UNNEST arguments read to.
     */
    private void recordCarriers(JoinInput step) {
        if (carriers == null) {
            return;
        }
        recordCarriers(step, step.getOnResidual());
        recordCarriers(step, step.getKeyFilter());
        final UnnestSpec unnest = step.getUnnest();
        if (unnest != null) {
            for (int i = 0, n = unnest.getExpressions().size(); i < n; i++) {
                recordCarriers(step, unnest.getExpressions().getQuick(i));
            }
        }
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
     * Records a carrier of the step for {@link DecorrelationVerifier}, which reads it right after decorrelation.
     */
    void addCarrier(JoinInput step, int columnId) {
        if (carriers != null) {
            carriers.add(step, columnId);
        }
    }

    void beginStep(JoinPlan join, int index) {
        master = join;
        masterLimit = index;
        outerAliases.clear();
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

    /**
     * Collects the outer columns of the master that the body reads, see {@link #masterOuterIds()}.
     */
    void collectMasterOuterIds(LogicalPlan body) {
        body.walkTopDown(masterOuterCollector);
    }

    ColumnExpression column(OutputSchema output, int columnId, int position) {
        return planNodes.columns.next().of(columnId, output.getColumnType(output.getColumnIndexById(columnId)), position);
    }

    /**
     * A copy of the plan under fresh column ids.
     */
    LogicalPlan copy(LogicalPlan plan) {
        return copier.copy(plan);
    }

    /**
     * The alias of the next domain of the step: the step's outer reference prefix, numbered after its first domain.
     */
    CharSequence domainAlias() {
        final CharacterStoreEntry alias = characterStore.newEntry();
        alias.put(OUTER_REF_PREFIX).put(outerRefSequence);
        if (domainSequence++ > 0) {
            alias.put('_').put(domainSequence - 1);
        }
        return alias.toImmutable();
    }

    void endStep(int masterBase) {
        masterOuterIds.setPos(masterBase);
        master = null;
        masterLimit = 0;
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

    boolean hasOuterAlias(int outerId) {
        return outerAliases.keyIndex(outerId) < 0;
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

    boolean isMasterOuter(int outerId) {
        return masterOuterIds.contains(outerId);
    }

    /**
     * True when a step the join orders after the step being decorrelated null-extends it.
     */
    boolean isStepNullExtendedLater() {
        return LogicalPlans.lastMasterNullingStep(master) > LogicalPlans.orderedSteps(master).indexOf(step());
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
        for (int i = base, n = mapping.mark(); i < n; i++) {
            substitution.put(mapping.outerIdAt(i), mapping.columnIdAt(i));
        }
    }

    /**
     * Appends a hidden TRUE column to the projection, which a row a join null-extends reads FALSE; returns its id.
     */
    int markRows(ProjectPlan project, int position) {
        project.getExpressions().add(planNodes.constants.next().ofBoolean(true, position));
        final int markerId = context.newColumnId();
        project.getOutput().add(markerId, sequencedName(ROW_MARKER_PREFIX).toImmutable(), ColumnType.BOOLEAN, false);
        return markerId;
    }

    JoinPlan master() {
        return master;
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
     * The expression over the master columns: the outer columns of the master read the master columns they stand
     * for, those of an enclosing LATERAL stay outer columns.
     */
    BoundExpression masterExpression(BoundExpression expression) {
        if (expression == null || !LogicalPlans.hasOuterColumn(expression)) {
            return expression;
        }
        final int columnBase = tmpColumnIds.size();
        outerColumnReads.collect(expression, tmpColumnIds);
        substitution.clear();
        for (int i = columnBase, n = tmpColumnIds.size(); i < n; i++) {
            final int outerId = tmpColumnIds.getQuick(i);
            if (masterInput(outerId) > -1) {
                substitution.put(outerId, masterColumn(outerId));
            }
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

    /**
     * The outer columns of the master that the body of the step reads, in the order the body reads them first. The
     * caller only reads them.
     */
    IntList masterOuterIds() {
        return masterOuterIds;
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

    /**
     * Takes, for the names of the step, the first outer reference sequence that no column of the body or of the join
     * names, and restarts its domain aliases.
     */
    void pickOuterRefSequence(LogicalPlan body, JoinPlan join) {
        do {
            outerRefSequence++;
        } while (hasOuterRefName(body) || hasOuterRefName(join));
        domainSequence = 0;
    }

    void putOuterAlias(int outerId, int columnId) {
        outerAliases.put(outerId, columnId);
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
     * True when the step reads a column of the output as it joins: in its keys, ON condition, key filter or UNNEST
     * arguments.
     */
    boolean readsAnyColumn(JoinInput step, OutputSchema output) {
        final ObjList<JoinKey> keys = step.getKeys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final JoinKey key = keys.getQuick(i);
            if (output.getColumnIndexById(key.getMasterColumnId()) > -1 || output.getColumnIndexById(key.getSlaveColumnId()) > -1) {
                return true;
            }
        }
        if (readsAnyColumn(step.getOnResidual(), output) || readsAnyColumn(step.getKeyFilter(), output)) {
            return true;
        }
        final UnnestSpec unnest = step.getUnnest();
        for (int i = 0, n = unnest != null ? unnest.getExpressions().size() : 0; i < n; i++) {
            if (readsAnyColumn(unnest.getExpressions().getQuick(i), output)) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when the expression reads one of the outer columns.
     */
    boolean readsAnyOuter(BoundExpression expression, IntList outerIds) {
        readIds = outerIds;
        return reads(expression, outerIdReads);
    }

    /**
     * True when the step reads one of the outer columns as it joins: in its ON condition, key filter or UNNEST
     * arguments.
     */
    boolean readsAnyOuter(JoinInput step, IntList outerIds) {
        if (readsAnyOuter(step.getOnResidual(), outerIds) || readsAnyOuter(step.getKeyFilter(), outerIds)) {
            return true;
        }
        final UnnestSpec unnest = step.getUnnest();
        for (int i = 0, n = unnest != null ? unnest.getExpressions().size() : 0; i < n; i++) {
            if (readsAnyOuter(unnest.getExpressions().getQuick(i), outerIds)) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when the expression reads an outer column of the master that no mapping above {@code base} satisfies.
     */
    boolean readsUnmappedOuter(BoundExpression expression, int base) {
        readMappingBase = base;
        return reads(expression, unmappedOuterReads);
    }

    /**
     * Lays the node's output out again over the changed outputs of its inputs.
     */
    void realign(LogicalPlan node) {
        switch (node) {
            case ForwardingPlan forwarding -> forwarding.deriveOutput();
            case WindowJoinPlan windowJoin -> alignWindowJoin(windowJoin);
            case HorizonJoinPlan horizon -> alignHorizonJoin(horizon);
            case WindowPlan window -> window.deriveOutput(tmpSchema);
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
     * Renames, in the node itself, the columns {@code columnIds} holds, and with them the carriers of a join's steps.
     */
    void remap(LogicalPlan node, IntIntHashMap columnIds) {
        copier.remap(node, columnIds);
        if (carriers != null && node instanceof JoinPlan join) {
            carriers.remap(join, columnIds);
        }
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
        remap(node, substitution);
    }

    /**
     * Records the step's carriers and renames, in its ON condition, key filter, UNNEST arguments and, when
     * {@code isFilterRead}, post-join filter, the columns {@link #substitution} holds.
     */
    void remapStepReads(JoinInput step, boolean isFilterRead) {
        recordCarriers(step);
        step.setOnResidual(context.getRewriter().remapColumns(step.getOnResidual(), substitution));
        step.setKeyFilter(context.getRewriter().remapColumns(step.getKeyFilter(), substitution));
        if (isFilterRead) {
            step.setPostJoinFilter(context.getRewriter().remapColumns(step.getPostJoinFilter(), substitution));
        }
        final UnnestSpec unnest = step.getUnnest();
        if (unnest != null) {
            final ObjList<BoundExpression> arguments = unnest.getExpressions();
            for (int i = 0, n = arguments.size(); i < n; i++) {
                arguments.setQuick(i, context.getRewriter().remapColumns(arguments.getQuick(i), substitution));
            }
        }
    }

    /**
     * A new name of the prefix and the next carrier sequence, to which the caller may append.
     */
    CharacterStoreEntry sequencedName(CharSequence prefix) {
        final CharacterStoreEntry name = characterStore.newEntry();
        name.put(prefix).put(carrierSequence++);
        return name;
    }

    JoinInput step() {
        return master.getInputs().getQuick(masterLimit);
    }

    int stepIndex() {
        return masterLimit;
    }

    /**
     * Gives each column of the output its name, or the name with the lowest numeric suffix, that no column before it
     * has in any case.
     */
    void uniqueNames(OutputSchema output) {
        takenNames.clear();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            output.setColumnName(i, takenNames.take(output.getColumnName(i), characterStore), output.getColumnQualifier(i));
        }
    }
}
