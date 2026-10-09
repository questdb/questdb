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

    @Override
    public void clear() {
        super.clear();
        filterConjunctOrigins.clear();
        filterConjuncts.clear();
        inputs.clear();
        orderedInputs.clear();
        graph = null;
        hasExplicitTimestamp = false;
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
}
