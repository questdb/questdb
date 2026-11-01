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

import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

/**
 * Sources retain occurrence order; the optimiser selects the execution order of a join that carries a
 * {@link JoinGraph} and places the graph's filter conjuncts.
 */
public final class JoinPlan extends LogicalPlan {
    public static final ObjectFactory<JoinPlan> FACTORY = JoinPlan::new;
    private final IntList filterConjunctOrigins = new IntList();
    private final ObjList<BoundExpression> filterConjuncts = new ObjList<>();
    private final ObjList<JoinInput> inputs = new ObjList<>();
    private final ObjList<JoinInput> orderedInputs = new ObjList<>();
    private JoinGraph graph;
    private boolean hasExplicitTimestamp;
    private boolean isLeadingCrossJoinFixed;

    /**
     * Appends an input, last in occurrence and execution order alike.
     */
    public void addInput(JoinInput input) {
        inputs.add(input);
        orderedInputs.add(input);
    }

    /**
     * Appends, hidden and under the input's binding alias, every column of an input's source the output lacks.
     */
    public void addMissingInputColumns() {
        final OutputSchema output = getOutput();
        for (int i = 0, n = inputs.size(); i < n; i++) {
            final JoinInput input = inputs.getQuick(i);
            final OutputSchema source = input.getSourceOutput();
            for (int c = 0, m = source.getColumnCount(); c < m; c++) {
                if (output.getColumnIndexById(source.getColumnId(c)) < 0) {
                    output.add(source.getColumnId(c), source.getColumnName(c), source.getColumnType(c), source.getMetadata(c), false,
                            input.getBindingAlias());
                }
            }
        }
    }

    @Override
    public void clear() {
        super.clear();
        filterConjunctOrigins.clear();
        filterConjuncts.clear();
        inputs.clear();
        orderedInputs.clear();
        graph = null;
        hasExplicitTimestamp = false;
        isLeadingCrossJoinFixed = false;
    }

    /**
     * The input whose ON clause states each filter conjunct, or -1 for WHERE.
     */
    public IntList getFilterConjunctOrigins() {
        return filterConjunctOrigins;
    }

    /**
     * The WHERE and ON conjuncts bound into this join's ON residuals and filters, in binding order.
     */
    public ObjList<BoundExpression> getFilterConjuncts() {
        return filterConjuncts;
    }

    /**
     * The graph binding left for the optimiser to order the join by, or null once the join is ordered.
     */
    public JoinGraph getGraph() {
        return graph;
    }

    public ObjList<JoinInput> getInputs() {
        return inputs;
    }

    public ObjList<JoinInput> getOrderedInputs() {
        return orderedInputs;
    }

    public boolean hasExplicitTimestamp() {
        return hasExplicitTimestamp;
    }

    @Override
    public LogicalPlan inputAt(int index) {
        return inputOccurrenceAt(index).getInput();
    }

    @Override
    public int inputCount() {
        int count = 0;
        for (int i = 0, n = inputs.size(); i < n; i++) {
            if (inputs.getQuick(i).getInput() != null) {
                count++;
            }
        }
        return count;
    }

    public boolean isLeadingCrossJoinFixed() {
        return isLeadingCrossJoinFixed;
    }

    /**
     * Replaces the steps from {@code start} to {@code end} of the execution order, {@code end} after {@code start},
     * with {@code step}, which takes occurrence {@code occurrence} among the inputs this join keeps. {@code nested}
     * holds the inputs of those steps in occurrence order and takes over the filter conjuncts their ON clauses state.
     */
    public void nestSteps(int start, int end, JoinPlan nested, JoinInput step, int occurrence) {
        final ObjList<JoinInput> nestedInputs = nested.inputs;
        int keptCount = 0;
        for (int i = 0, n = filterConjuncts.size(); i < n; i++) {
            final BoundExpression conjunct = filterConjuncts.getQuick(i);
            final int origin = filterConjunctOrigins.getQuick(i);
            final int nestedOrigin = origin < 0 ? -1 : nestedInputs.indexOf(inputs.getQuick(origin));
            if (nestedOrigin > -1) {
                nested.filterConjuncts.add(conjunct);
                nested.filterConjunctOrigins.add(nestedOrigin);
            } else {
                filterConjuncts.setQuick(keptCount, conjunct);
                filterConjunctOrigins.setQuick(keptCount++, origin < 0 ? -1 : keptOccurrence(origin, nestedInputs, occurrence));
            }
        }
        filterConjuncts.setPos(keptCount);
        filterConjunctOrigins.setPos(keptCount);
        for (int i = inputs.size() - 1; i > -1; i--) {
            if (nestedInputs.indexOf(inputs.getQuick(i)) > -1) {
                inputs.remove(i);
            }
        }
        inputs.insert(occurrence, 1, step);
        orderedInputs.setQuick(start, step);
        orderedInputs.remove(start + 1, end);
    }

    public JoinPlan of(int position) {
        setPosition(position);
        return this;
    }

    @Override
    public void replaceInput(int index, LogicalPlan input) {
        inputOccurrenceAt(index).setInput(input);
    }

    public void setExplicitTimestamp(boolean hasExplicitTimestamp) {
        this.hasExplicitTimestamp = hasExplicitTimestamp;
    }

    public void setGraph(JoinGraph graph) {
        this.graph = graph;
    }

    public void setLeadingCrossJoinFixed(boolean isLeadingCrossJoinFixed) {
        this.isLeadingCrossJoinFixed = isLeadingCrossJoinFixed;
    }

    @Override
    public void visitReads(PlanExpressionVisitor visitor) {
        PlanReads.expressions(filterConjuncts, visitor);
        for (int i = 0, n = inputs.size(); i < n; i++) {
            inputs.getQuick(i).visitReads(visitor);
        }
        if (graph != null) {
            graph.visitReads(visitor);
        }
    }

    private JoinInput inputOccurrenceAt(int index) {
        for (int i = 0, n = inputs.size(); i < n && index >= 0; i++) {
            final JoinInput occurrence = inputs.getQuick(i);
            if (occurrence.getInput() != null && index-- == 0) {
                return occurrence;
            }
        }
        throw new IndexOutOfBoundsException();
    }

    /**
     * The occurrence that input {@code index} takes once the inputs {@code nestedInputs} holds leave this join and
     * the step that nests them takes occurrence {@code occurrence}.
     */
    private int keptOccurrence(int index, ObjList<JoinInput> nestedInputs, int occurrence) {
        int keptBefore = 0;
        for (int i = 0; i < index; i++) {
            if (nestedInputs.indexOf(inputs.getQuick(i)) < 0) {
                keptBefore++;
            }
        }
        return keptBefore < occurrence ? keptBefore : keptBefore + 1;
    }
}
