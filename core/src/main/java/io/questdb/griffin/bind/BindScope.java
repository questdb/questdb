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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.PreparedFunctions;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.JoinEquality;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.Subquery;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjIntHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.ObjObjHashMap;

/**
 * The binding state of one nesting depth: what the query binding at that depth holds while a sub-query of it binds
 * one depth deeper, namely the column-id counter and the scope facts of its query blocks, the block lists its area
 * binders fill and read across expression binding, and the state of the expression {@link FunctionBinder} binds
 * now. {@link BindScopeStack} keeps one per depth; the binder services read the current one. Lists of areas whose
 * windows never overlap within a depth are one list under several names: a block binds its join conditions, then its
 * windows, then its aggregation, DISTINCT and cursor columns, each starting from an empty list; a PIVOT and a WINDOW
 * JOIN bind their keys, aggregates and combinations outside windows; no substitution spans the cursor columns;
 * only a block without windows translates its projection's aliases; a block looks up its SELECT aliases outside the
 * binding of its windows' self references and the translation of its aliases; and its window calls bind before its
 * projection reads earlier aliases.
 */
final class BindScope implements Mutable {
    final IntIntHashMap aggregateColumnMappings = new IntIntHashMap();
    final ObjList<ExpressionNode> aggregateNodes;
    final ObjList<ExpressionNode> aggregateOrderExpressions;
    final ObjObjHashMap<ExpressionNode, BoundExpression> aggregateProjectionExpressions = new ObjObjHashMap<>();
    final IntList aggregateProjectionIndexes;
    final ObjList<ExpressionNode> aggregateSelectExpressions;
    final ObjList<BoundExpression> aggregateSubstitutions = new ObjList<>();
    final LowerCaseCharSequenceIntHashMap aliasSequences = new LowerCaseCharSequenceIntHashMap();
    final LowerCaseCharSequenceHashSet aliases = new LowerCaseCharSequenceHashSet();
    final IntHashSet ambiguousTimestampColumnIds = new IntHashSet();
    final ObjList<ColumnExpression> cursorColumns;
    final ObjList<CharSequence> cursorNames;
    final ObjList<ExpressionNode> cursorNodes;
    final IntList cursorProjectionSources;
    final IntList cursorSourceIndexes;
    final ObjList<ExpressionNode> cursorSources = new ObjList<>();
    final ObjList<BoundExpression> fillBindings = new ObjList<>();
    final ObjList<ExpressionNode> groupingNodes;
    final ObjList<QueryModel> hintAliasModels = new ObjList<>();
    final ObjList<CharSequence> hintAliases = new ObjList<>();
    final IntHashSet intrinsicTimestampColumnIds = new IntHashSet();
    /**
     * The equalities between two columns of one join input that the join graph derived, pending binding as filters.
     */
    final ObjList<JoinEquality> joinDerivedEqualities = new ObjList<>();
    final IntHashSet joinNativeTimestampIds = new IntHashSet();
    /**
     * Per join input, the ON clause terms pending binding: the residual of an outer ON clause; the constant terms of an
     * inner one, which filter the input's own step when the join has an outer or temporal step, and the last step
     * otherwise.
     */
    final ObjList<ExpressionNode> joinOnNodes;
    final ObjList<ExpressionNode> joinResidualNodes;
    final IntList joinResidualOrigins;
    final IntHashSet keySubqueryColumnIds = new IntHashSet();
    final IntHashSet nonTimestampReferenceIds = new IntHashSet();
    final IntList orderOutputIndexes = new IntList();
    final IntList outerColumnIds = new IntList();
    final ObjList<OutputSchema> outerScopes = new ObjList<>();
    final ObjList<CharSequence> pivotForAliases = new ObjList<>();
    final IntList pivotIndexes;
    final ObjList<CharSequence> pivotKeyAliases;
    final IntList pivotKeyIndexes;
    //The dateadd calls the binding root's and_offset rewrite wraps the timestamp column in.
    final ObjList<ExpressionNode> projectedOffsets = new ObjList<>();
    final IntList projectionAliasIndexes = new IntList();
    final OutputSchema referenceScope = new OutputSchema();
    final LowerCaseCharSequenceIntHashMap selectAliasIndexes;
    final IntList sourceProjectionIndexes = new IntList();
    final ObjList<ColumnExpression> substitutionColumns = new ObjList<>();
    final ObjList<ExpressionNode> substitutionNodes = new ObjList<>();
    final TranslatingAliases translatingAliases;
    final IntHashSet translatingCopyIds = new IntHashSet();
    final ObjList<ExpressionNode> windowAliasCopies = new ObjList<>();
    final IntList windowAliasCopyColumns = new IntList();
    final IntList windowAliasIds = new IntList();
    final IntList windowAliasReferenceColumns = new IntList();
    final ObjList<ExpressionNode> windowAliasReferences = new ObjList<>();
    final ObjList<ExpressionNode> windowAliasResolutions = new ObjList<>();
    final OutputSchema windowBindingSchema = new OutputSchema();
    final IntList windowColumnIds = new IntList();
    final OutputSchema windowInputScope;
    final IntList windowJoinAggregateSteps;
    final ObjList<PivotBinder.WindowJoinPivotAggregate> windowJoinPivotAggregates = new ObjList<>();
    final ArrayColumnTypes windowKeyTypes = new ArrayColumnTypes();
    final ObjList<WindowSpec> windowLevelSpecs = new ObjList<>();
    final IntList windowLevels = new IntList();
    final ObjList<CharSequence> windowNames = new ObjList<>();
    /**
     * The index in {@link #windowNodes} of the window node each window call occurrence binds as.
     */
    final ObjIntHashMap<ExpressionNode> windowNodeIndexes = new ObjIntHashMap<>();
    final ObjList<ExpressionNode> windowNodes = new ObjList<>();
    /**
     * The window call, as written in the query, that each bound copy of a window call stands for.
     */
    final ObjObjHashMap<ExpressionNode, ExpressionNode> windowOccurrences = new ObjObjHashMap<>();
    final ObjList<ExpressionNode> windowOrderExpressions = new ObjList<>();
    final ObjList<ExpressionNode> windowSelectExpressions = new ObjList<>();
    final ObjIntHashMap<ExpressionNode> windowSelectIndexes = new ObjIntHashMap<>();
    final LowerCaseCharSequenceIntHashMap windowSelfReferences = new LowerCaseCharSequenceIntHashMap();
    private final IntList aggregateHashes;
    FunctionBinder.BindingRole bindingRole;
    ExpressionNode bindingRoot;
    Subquery boundLowerBound;
    ExpressionNode boundLowerBoundNode;
    LowerCaseCharSequenceObjHashMap<CharSequence> currentHints;
    PreparedFunctions.Entry currentPreparation;
    OutputSchema expressionInput;
    CharSequence expressionInputAlias;
    boolean hasWithin;
    boolean isImplicitAggregation;
    boolean isInsideJoin;
    QueryModel namedWindowScope;
    IntHashSet nativeTimestampIds;
    int nestedWindowPosition;
    // A horizon projection prefers exact source spelling, otherwise its first resolved timestamp.
    int preferredTimestampColumnId = -1;
    ObjList<? extends BoundExpression> replacementColumns;
    ObjList<ExpressionNode> replacementNodes;
    int sampleByTimestampColumnId = -1;
    QueryModel timestampFromSubquery;
    int timestampSubqueryType;
    QueryModel timestampToSubquery;
    LogicalPlan windowInput;
    LogicalPlan windowInputScopePlan;

    BindScope() {
        aggregateHashes = windowAliasIds;
        aggregateNodes = windowAliasResolutions;
        aggregateOrderExpressions = windowAliasCopies;
        aggregateProjectionIndexes = windowLevels;
        aggregateSelectExpressions = windowAliasReferences;
        cursorColumns = substitutionColumns;
        cursorNames = windowNames;
        cursorNodes = substitutionNodes;
        cursorProjectionSources = windowAliasCopyColumns;
        cursorSourceIndexes = windowAliasReferenceColumns;
        groupingNodes = windowNodes;
        joinOnNodes = windowNodes;
        joinResidualNodes = substitutionNodes;
        joinResidualOrigins = windowColumnIds;
        pivotIndexes = projectionAliasIndexes;
        pivotKeyAliases = windowNames;
        pivotKeyIndexes = sourceProjectionIndexes;
        selectAliasIndexes = windowSelfReferences;
        translatingAliases = new TranslatingAliases(joinNativeTimestampIds, windowColumnIds, windowSelfReferences);
        windowInputScope = referenceScope;
        windowJoinAggregateSteps = windowColumnIds;
    }

    @Override
    public void clear() {
        clearAggregateNodes();
        aggregateColumnMappings.clear();
        aggregateProjectionExpressions.clear();
        aggregateSubstitutions.clear();
        aliasSequences.clear();
        aliases.clear();
        ambiguousTimestampColumnIds.clear();
        cursorSources.clear();
        fillBindings.clear();
        hintAliasModels.clear();
        hintAliases.clear();
        intrinsicTimestampColumnIds.clear();
        joinDerivedEqualities.clear();
        joinNativeTimestampIds.clear();
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
        translatingAliases.clear();
        translatingCopyIds.clear();
        nonTimestampReferenceIds.clear();
        preferredTimestampColumnId = -1;
        windowAliasCopies.clear();
        windowAliasCopyColumns.clear();
        windowAliasIds.clear();
        windowAliasReferenceColumns.clear();
        windowAliasReferences.clear();
        windowAliasResolutions.clear();
        windowBindingSchema.clear();
        windowColumnIds.clear();
        windowJoinPivotAggregates.clear();
        windowKeyTypes.clear();
        windowLevelSpecs.clear();
        windowLevels.clear();
        windowNames.clear();
        windowNodeIndexes.clear();
        windowNodes.clear();
        if (windowOccurrences.size() > 0) {
            windowOccurrences.clear();
        }
        windowOrderExpressions.clear();
        windowSelectExpressions.clear();
        windowSelectIndexes.clear();
        windowSelfReferences.clear();
        bindingRole = null;
        bindingRoot = null;
        boundLowerBound = null;
        boundLowerBoundNode = null;
        currentHints = null;
        currentPreparation = null;
        expressionInput = null;
        expressionInputAlias = null;
        hasWithin = false;
        isImplicitAggregation = false;
        isInsideJoin = false;
        namedWindowScope = null;
        nativeTimestampIds = null;
        nestedWindowPosition = 0;
        replacementColumns = null;
        replacementNodes = null;
        sampleByTimestampColumnId = -1;
        timestampFromSubquery = null;
        timestampSubqueryType = 0;
        timestampToSubquery = null;
        windowInput = null;
        windowInputScopePlan = null;
    }

    /**
     * Adds an aggregate of the block being bound, see {@link #findAggregateNode}.
     */
    void addAggregateNode(ExpressionNode node) {
        aggregateNodes.add(node);
        aggregateHashes.add(ExpressionNode.hashNodesExact(node));
    }

    void clearAggregateNodes() {
        aggregateNodes.clear();
        aggregateHashes.clear();
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

    void removeAggregateNode(int index) {
        aggregateNodes.remove(index);
        aggregateHashes.removeIndex(index);
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
     * Projection aliases: the first reference to a column fixes its alias.
     */
    static final class TranslatingAliases implements Mutable {
        private final IntHashSet columnIds;
        private final IntList inputColumnIds;
        private final LowerCaseCharSequenceHashSet names = new LowerCaseCharSequenceHashSet();
        private final LowerCaseCharSequenceIntHashMap sequences;

        private TranslatingAliases(IntHashSet columnIds, IntList inputColumnIds, LowerCaseCharSequenceIntHashMap sequences) {
            this.columnIds = columnIds;
            this.inputColumnIds = inputColumnIds;
            this.sequences = sequences;
        }

        @Override
        public void clear() {
            columnIds.clear();
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
