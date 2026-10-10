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
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.std.Chars;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * Rewrite state {@link Decorrelation} shares with its collaborators: the master join of the step being
 * rewritten, the mapping from outer columns to the columns that satisfy them, column ids, names and bound
 * calls.
 */
final class DecorrelationContext implements Mutable {
    static final String OUTER_REF_PREFIX = "__qdb_outer_ref__";
    final ObjList<BoundExpression> callArguments;
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
    private final PlanVisitor outerRefNames = this::findOuterRefName;
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
            ObjList<BoundExpression> callArguments,
            IntList tmpColumnIds,
            IntList mappedOuterIds,
            IntList mappedColumnIds,
            ObjList<LogicalPlan> chain,
            OutputSchema tmpSchema
    ) {
        this.context = context;
        this.planNodes = planNodes;
        this.characterStore = characterStore;
        this.callArguments = callArguments;
        this.tmpColumnIds = tmpColumnIds;
        this.mappedOuterIds = mappedOuterIds;
        this.mappedColumnIds = mappedColumnIds;
        this.chain = chain;
        this.tmpSchema = tmpSchema;
        this.copier = new LogicalPlanCopier(context, planNodes);
    }

    @Override
    public void clear() {
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

    private int findOuterRefName(LogicalPlan plan) {
        final OutputSchema output = plan.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (parseOuterRefSequence(output.getColumnName(i)) == outerRefSequence) {
                return TreeWalk.STOP;
            }
        }
        return TreeWalk.CONTINUE;
    }

    static void appendMissingColumns(OutputSchema target, OutputSchema source) {
        for (int i = 0, n = source.getColumnCount(); i < n; i++) {
            if (target.getColumnIndexById(source.getColumnId(i)) < 0) {
                target.add(source.getColumnId(i), source.getColumnName(i), source.getColumnType(i), source.getMetadata(i), false,
                        source.getColumnQualifier(i));
            }
        }
    }

    /**
     * True when step {@code step} of the join can emit the columns of input {@code index} as NULL: the input's
     * own step null-extends it, or a later step null-extends its master.
     */
    static boolean isNullingStep(JoinPlan join, int step, int index) {
        final JoinKind type = join.getInputs().getQuick(step).getJoinType();
        return step == index ? type.isSlaveNulling() : step > index && type.isMasterNulling();
    }

    static boolean isTrue(BoundExpression condition) {
        return condition == null || condition instanceof ConstantExpression constant && constant.getLongValue() != 0;
    }

    /**
     * The aggregate key column that a projection's exposed column reads.
     */
    static int keyColumnId(ProjectPlan project, int exposedId) {
        final BoundExpression expression = project.getExpressions().getQuick(project.getOutput().getColumnIndexById(exposedId));
        return expression instanceof ColumnExpression column ? column.getColumnId() : -1;
    }

    static int pairIndex(IntList pairs, int key) {
        for (int i = 0, n = pairs.size(); i < n; i += 2) {
            if (pairs.getQuick(i) == key) {
                return i;
            }
        }
        return -1;
    }

    static void rebuildJoinOutput(JoinPlan join) {
        final OutputSchema output = join.getOutput();
        for (int i = 0, n = join.getInputs().size(); i < n; i++) {
            final JoinInput input = join.getInputs().getQuick(i);
            final OutputSchema source = input.getSourceOutput();
            for (int c = 0, m = source.getColumnCount(); c < m; c++) {
                if (output.getColumnIndexById(source.getColumnId(c)) < 0) {
                    output.add(source.getColumnId(c), source.getColumnName(c), source.getColumnType(c), source.getMetadata(c), false,
                            input.getBindingAlias());
                }
            }
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

    BoundExpression bindCall(CharSequence name, int position, OutputSchema input) throws SqlException {
        final BoundExpression bound = context.bindCall(name, position, callArguments, input);
        callArguments.clear();
        return bound;
    }

    BoundExpression bindCall(CharSequence name, int position, BoundExpression left, BoundExpression right, OutputSchema input) throws SqlException {
        callArguments.clear();
        callArguments.add(left);
        callArguments.add(right);
        return bindCall(name, position, input);
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

    void exposeColumn(ProjectPlan project, OutputSchema input, int columnId, CharSequence name, int position) {
        final int type = input.getColumnType(input.getColumnIndexById(columnId));
        project.getExpressions().add(planNodes.columns.next().of(columnId, type, position));
        project.getOutput().add(context.newColumnId(), name, type, false);
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

    boolean readsAnyColumn(BoundExpression expression, OutputSchema output) {
        if (expression == null) {
            return false;
        }
        final int columnBase = tmpColumnIds.size();
        collectColumnIds(expression, tmpColumnIds);
        boolean isFound = false;
        for (int i = columnBase, n = tmpColumnIds.size(); i < n && !isFound; i++) {
            isFound = output.getColumnIndexById(tmpColumnIds.getQuick(i)) > -1;
        }
        tmpColumnIds.setPos(columnBase);
        return isFound;
    }

    /**
     * Renames, in the node itself, the outer columns of the mapping above {@code base} to their mapped columns.
     */
    void remapMapped(LogicalPlan node, int base) {
        substitution.clear();
        for (int k = base, n = mappedOuterIds.size(); k < n; k++) {
            substitution.put(mappedOuterIds.getQuick(k), mappedColumnIds.getQuick(k));
        }
        copier.remap(node, substitution);
    }
}
