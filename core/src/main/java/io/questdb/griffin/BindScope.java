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

import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
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
final class BindScope implements Mutable {
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
    final ObjList<ExpressionNode> joinFilterNodes = new ObjList<>();
    final IntHashSet joinNativeTimestampIds = new IntHashSet();
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
    final IntList projectionAliasIndexes = new IntList();
    final OutputSchema referenceScope = new OutputSchema();
    final IntList sourceProjectionIndexes = new IntList();
    final ObjList<ColumnExpression> substitutionColumns = new ObjList<>();
    final ObjList<ExpressionNode> substitutionNodes = new ObjList<>();
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
    ExpressionNode aggregateRoot;
    ExpressionNode bindingRoot;
    Subquery boundLowerBound;
    ExpressionNode boundLowerBoundNode;
    LowerCaseCharSequenceObjHashMap<CharSequence> currentHints;
    PreparedFunctions.Entry currentPreparation;
    OutputSchema expressionInput;
    CharSequence expressionInputAlias;
    boolean isBindingGroupByExpression;
    boolean isBindingPredicate;
    boolean isInsideJoin;
    IntHashSet nativeTimestampIds;
    int nestedWindowPosition;
    int nextColumnId;
    ObjList<? extends BoundExpression> replacementExpressions;
    ObjList<ExpressionNode> replacementNodes;
    LogicalPlan windowInput;
    ExpressionNode windowRoot;

    BindScope() {
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
        aggregateNodes.clear();
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
        joinFilterNodes.clear();
        joinNativeTimestampIds.clear();
        joinResidualNodes.clear();
        joinResidualOrigins.clear();
        keySubqueryColumnIds.clear();
        orderOutputIndexes.clear();
        outerColumnIds.clear();
        outerScopes.clear();
        pivotForAliases.clear();
        projectionAliasIndexes.clear();
        referenceScope.clear();
        sourceProjectionIndexes.clear();
        substitutionColumns.clear();
        substitutionNodes.clear();
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
        aggregateRoot = null;
        bindingRoot = null;
        boundLowerBound = null;
        boundLowerBoundNode = null;
        currentHints = null;
        currentPreparation = null;
        expressionInput = null;
        expressionInputAlias = null;
        isBindingGroupByExpression = false;
        isBindingPredicate = false;
        isInsideJoin = false;
        nativeTimestampIds = null;
        nestedWindowPosition = 0;
        nextColumnId = 0;
        replacementExpressions = null;
        replacementNodes = null;
        windowInput = null;
        windowRoot = null;
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
