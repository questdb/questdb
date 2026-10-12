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

package io.questdb.griffin.bind;

import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.PreparedFunctions;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.JoinEquality;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.Subquery;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * The binding state of one nesting depth: what the query binding at that depth holds while a sub-query of it binds
 * one depth deeper, namely the column-id counter and the scope facts of its query blocks, the block lists its area
 * binders fill and read across expression binding, and the state of the expression {@link FunctionBinder} binds
 * now. {@link BindScopeStack} keeps one per depth; the binder services read the current one. Lists of two areas
 * whose windows never overlap within a depth are one list under two names: a block binds its windows before its
 * aggregation, DISTINCT and cursor columns, and a PIVOT names its keys and combinations outside both.
 */
public final class BindScope implements Mutable {
    final ObjList<ExpressionNode> aggregateNodes = new ObjList<>();
    final ObjList<ExpressionNode> aggregateOrderExpressions;
    final IntList aggregateProjectionIndexes;
    final ObjList<ExpressionNode> aggregateSelectExpressions;
    final LowerCaseCharSequenceIntHashMap aliasSequences = new LowerCaseCharSequenceIntHashMap();
    final LowerCaseCharSequenceHashSet aliases = new LowerCaseCharSequenceHashSet();
    final IntHashSet ambiguousTimestampColumnIds = new IntHashSet();
    final ObjList<ColumnExpression> cursorColumns = new ObjList<>();
    final ObjList<CharSequence> cursorNames;
    final ObjList<ExpressionNode> cursorNodes = new ObjList<>();
    final IntList cursorProjectionSources;
    final IntList cursorSourceIndexes;
    final ObjList<ExpressionNode> cursorSources;
    final ObjList<BoundExpression> fillBindings = new ObjList<>();
    final ObjList<ExpressionNode> groupingNodes = new ObjList<>();
    final ObjList<QueryModel> hintAliasModels = new ObjList<>();
    final ObjList<CharSequence> hintAliases = new ObjList<>();
    final IntHashSet intrinsicTimestampColumnIds = new IntHashSet();
    /**
     * The equalities between two columns of one join input that the join graph derived, pending binding as filters.
     */
    final ObjList<JoinEquality> joinDerivedEqualities = new ObjList<>();
    final IntHashSet joinNativeTimestampIds = new IntHashSet();
    /**
     * Per join input, the residual of its outer ON clause, pending binding.
     */
    final ObjList<ExpressionNode> joinOnNodes = new ObjList<>();
    final ObjList<ExpressionNode> joinResidualNodes = new ObjList<>();
    final IntList joinResidualOrigins = new IntList();
    final IntHashSet keySubqueryColumnIds = new IntHashSet();
    final IntList orderOutputIndexes = new IntList();
    /**
     * Ids of the outer columns every binding at this depth resolved, in resolution order and with repeats; a caller
     * reads the range its own binding appended.
     */
    final IntList outerColumnIds = new IntList();
    final ObjList<OutputSchema> outerScopes = new ObjList<>();
    final ObjList<CharSequence> pivotForAliases = new ObjList<>();
    final IntList pivotIndexes;
    final ObjList<CharSequence> pivotKeyAliases;
    final IntList pivotKeyIndexes;
    /**
     * The dateadd calls the binding root's and_offset rewrite wraps the timestamp column in.
     */
    final ObjList<ExpressionNode> projectedOffsets = new ObjList<>();
    final IntList projectionAliasIndexes = new IntList();
    final OutputSchema referenceScope = new OutputSchema();
    final IntList sourceProjectionIndexes = new IntList();
    final ObjList<ColumnExpression> substitutionColumns = new ObjList<>();
    final ObjList<ExpressionNode> substitutionNodes = new ObjList<>();
    /**
     * Models of the SAMPLE BY range bounds that are scalar sub-queries; each binds as a runtime-constant timestamp
     * of {@link #timestampSubqueryType}. Binding may clone the bound's node, so the model identifies it.
     */
    final ObjList<QueryModel> timestampSubqueries = new ObjList<>();
    final TranslatingAliases translatingAliases = new TranslatingAliases();
    final IntHashSet translatingCopyIds = new IntHashSet();
    final ObjList<ExpressionNode> windowAliasCopies = new ObjList<>();
    final IntList windowAliasCopyColumns = new IntList();
    final IntList windowAliasIds = new IntList();
    final IntList windowAliasReferenceColumns = new IntList();
    final ObjList<ExpressionNode> windowAliasReferences = new ObjList<>();
    final ObjList<ExpressionNode> windowAliasResolutions = new ObjList<>();
    final OutputSchema windowBindingSchema = new OutputSchema();
    final IntList windowColumnIds = new IntList();
    final ObjList<ExpressionNode> windowCopies = new ObjList<>();
    final ObjList<ExpressionNode> windowCopyOrigins = new ObjList<>();
    final IntList windowJoinAggregateSteps = new IntList();
    final ObjList<PivotBinder.WindowJoinPivotAggregate> windowJoinPivotAggregates = new ObjList<>();
    final ObjList<WindowSpec> windowLevelSpecs = new ObjList<>();
    final IntList windowLevels = new IntList();
    final ObjList<CharSequence> windowNames = new ObjList<>();
    final ObjList<ExpressionNode> windowNodes = new ObjList<>();
    final ObjList<ExpressionNode> windowOrderExpressions = new ObjList<>();
    /**
     * Per window call, the first of its ORDER BY expressions that is not a column, otherwise null.
     */
    final ObjList<ExpressionNode> windowOrderViolations = new ObjList<>();
    final ObjList<ExpressionNode> windowSelectExpressions = new ObjList<>();
    final LowerCaseCharSequenceIntHashMap windowSelfReferences = new LowerCaseCharSequenceIntHashMap();
    final LongList withinPrefixes = new LongList();
    private final IntList aggregateHashes = new IntList();
    /**
     * Per plan aggregate of the block being bound, by its index in {@link GroupingPlan#getAggregates()} rather than in
     * {@link #aggregateNodes}, the integer literal {@code k} of a {@code sum(c op k)} or {@code sum(k op c)} the binder
     * bound as {@code sum(c)} pending normalisation, otherwise null. The plan methods below change both lists together.
     */
    private final ObjList<ConstantExpression> pendingSumLiterals = new ObjList<>();
    FunctionBinder.BindingRole bindingRole;
    ExpressionNode bindingRoot;
    Subquery boundLowerBound;
    ExpressionNode boundLowerBoundNode;
    LowerCaseCharSequenceObjHashMap<CharSequence> currentHints;
    PreparedFunctions.Entry currentPreparation;
    OutputSchema expressionInput;
    CharSequence expressionInputAlias;
    boolean isInsideJoin;
    QueryModel namedWindowScope;
    IntHashSet nativeTimestampIds;
    int nestedWindowPosition;
    ObjList<ColumnExpression> replacementColumns;
    ObjList<ExpressionNode> replacementNodes;
    int timestampSubqueryType;
    LogicalPlan windowInput;

    public BindScope() {
        aggregateOrderExpressions = windowAliasCopies;
        aggregateProjectionIndexes = windowLevels;
        aggregateSelectExpressions = windowAliasReferences;
        cursorNames = windowNames;
        cursorProjectionSources = windowAliasCopyColumns;
        cursorSourceIndexes = windowAliasReferenceColumns;
        cursorSources = windowCopyOrigins;
        pivotIndexes = projectionAliasIndexes;
        pivotKeyAliases = windowNames;
        pivotKeyIndexes = sourceProjectionIndexes;
    }

    @Override
    public void clear() {
        clearAggregateNodes();
        aliasSequences.clear();
        aliases.clear();
        ambiguousTimestampColumnIds.clear();
        cursorColumns.clear();
        cursorNodes.clear();
        fillBindings.clear();
        groupingNodes.clear();
        hintAliasModels.clear();
        hintAliases.clear();
        intrinsicTimestampColumnIds.clear();
        joinDerivedEqualities.clear();
        joinNativeTimestampIds.clear();
        joinOnNodes.clear();
        joinResidualNodes.clear();
        joinResidualOrigins.clear();
        keySubqueryColumnIds.clear();
        orderOutputIndexes.clear();
        outerColumnIds.clear();
        outerScopes.clear();
        pivotForAliases.clear();
        projectedOffsets.clear();
        projectionAliasIndexes.clear();
        referenceScope.clear();
        sourceProjectionIndexes.clear();
        substitutionColumns.clear();
        substitutionNodes.clear();
        timestampSubqueries.clear();
        translatingAliases.clear();
        translatingCopyIds.clear();
        windowAliasCopies.clear();
        windowAliasCopyColumns.clear();
        windowAliasIds.clear();
        windowAliasReferenceColumns.clear();
        windowAliasReferences.clear();
        windowAliasResolutions.clear();
        windowBindingSchema.clear();
        windowColumnIds.clear();
        windowCopies.clear();
        windowCopyOrigins.clear();
        windowJoinAggregateSteps.clear();
        windowJoinPivotAggregates.clear();
        windowLevelSpecs.clear();
        windowLevels.clear();
        windowNames.clear();
        windowNodes.clear();
        windowOrderExpressions.clear();
        windowOrderViolations.clear();
        windowSelectExpressions.clear();
        windowSelfReferences.clear();
        withinPrefixes.clear();
        bindingRole = null;
        bindingRoot = null;
        boundLowerBound = null;
        boundLowerBoundNode = null;
        currentHints = null;
        currentPreparation = null;
        expressionInput = null;
        expressionInputAlias = null;
        isInsideJoin = false;
        namedWindowScope = null;
        nativeTimestampIds = null;
        nestedWindowPosition = 0;
        replacementColumns = null;
        replacementNodes = null;
        timestampSubqueryType = 0;
        windowInput = null;
    }

    /**
     * Adds an aggregate of the block being bound, see {@link #findAggregateNode}.
     */
    void addAggregateNode(ExpressionNode node) {
        aggregateNodes.add(node);
        aggregateHashes.add(ExpressionNode.hashNodesExact(node));
    }

    /**
     * Appends an aggregate to the plan of the block, with the literal of the sum it holds pending normalisation, or null.
     */
    void addPlanAggregate(GroupingPlan aggregate, FunctionExpression function, ConstantExpression pendingSumLiteral) {
        assert pendingSumLiterals.size() == aggregate.getAggregates().size();
        aggregate.getAggregates().add(function);
        pendingSumLiterals.add(pendingSumLiteral);
    }

    void clearAggregateNodes() {
        aggregateNodes.clear();
        aggregateHashes.clear();
        pendingSumLiterals.clear();
    }

    /**
     * The index of the aggregate of the block that is {@code node} or computes the same value: the same node, an equal
     * expression tree, or another row count; -1 when there is none.
     */
    int findAggregateNode(ExpressionNode node) {
        final int identity = aggregateNodes.indexOf(node);
        if (identity >= 0) {
            return identity;
        }
        final boolean isRowCount = BindContext.isRowCount(node);
        final int hash = ExpressionNode.hashNodesExact(node);
        for (int i = 0, n = aggregateNodes.size(); i < n; i++) {
            final ExpressionNode aggregate = aggregateNodes.getQuick(i);
            if (aggregateHashes.getQuick(i) == hash && ExpressionNode.compareNodesExact(node, aggregate)
                    || isRowCount && BindContext.isRowCount(aggregate)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The literal of the sum the plan aggregate at {@code index} holds pending normalisation, or null.
     */
    ConstantExpression pendingSumLiteral(GroupingPlan aggregate, int index) {
        assert pendingSumLiterals.size() == aggregate.getAggregates().size();
        return pendingSumLiterals.getQuick(index);
    }

    void removeAggregateNode(int index) {
        aggregateNodes.remove(index);
        aggregateHashes.removeIndex(index);
    }

    /**
     * Removes the plan aggregate at {@code index} with its output column.
     */
    void removePlanAggregate(GroupingPlan aggregate, int index) {
        assert pendingSumLiterals.size() == aggregate.getAggregates().size();
        aggregate.getAggregates().remove(index);
        pendingSumLiterals.remove(index);
        aggregate.getOutput().remove(aggregate.getGroupingExpressions().size() + index);
    }

    /**
     * Starts a projection's alias space afresh.
     */
    void resetAliases() {
        aliases.clear();
        aliasSequences.clear();
    }

    /**
     * Starts a projection's alias space afresh, with the names of {@code seed} taken.
     */
    void resetAliases(OutputSchema seed) {
        resetAliases();
        for (int i = 0, n = seed.getColumnCount(); i < n; i++) {
            aliases.add(seed.getColumnName(i));
        }
    }

    /**
     * Replaces the plan aggregate at {@code index} with one no normalisation is pending for.
     */
    void setPlanAggregate(GroupingPlan aggregate, int index, FunctionExpression function) {
        assert pendingSumLiterals.size() == aggregate.getAggregates().size();
        aggregate.getAggregates().setQuick(index, function);
        pendingSumLiterals.setQuick(index, null);
    }

    /**
     * Projection aliases: the first reference to a column fixes its alias.
     */
    static final class TranslatingAliases implements Mutable {
        private final IntHashSet columnIds = new IntHashSet();
        private final IntList inputColumnIds = new IntList();
        private final LowerCaseCharSequenceHashSet names = new LowerCaseCharSequenceHashSet();
        private final LowerCaseCharSequenceIntHashMap sequences = new LowerCaseCharSequenceIntHashMap();

        @Override
        public void clear() {
            columnIds.clear();
            inputColumnIds.clear();
            names.clear();
            sequences.clear();
        }

        CharSequence add(int columnId, CharSequence name, CharacterStore store) {
            if (!columnIds.add(columnId)) {
                return name;
            }
            final CharSequence alias = SqlUtil.createColumnAlias(store, name, -1, names, sequences, false);
            names.add(alias);
            return alias;
        }

        void addArguments(BoundExpression expression, OutputSchema input, CharacterStore store) {
            inputColumnIds.clear();
            LogicalPlans.collectInputColumnIds(expression, input, inputColumnIds);
            for (int i = 0, n = inputColumnIds.size(); i < n; i++) {
                final int columnId = inputColumnIds.getQuick(i);
                add(columnId, input.getColumnName(input.getColumnIndexById(columnId)), store);
            }
        }
    }
}
