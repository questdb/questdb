/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.engine.functions.catalogue.*;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.table.AllTablesFunctionFactory;
import io.questdb.griffin.engine.table.ShowColumnsRecordCursorFactory;
import io.questdb.griffin.engine.table.ShowPartitionsRecordCursorFactory;
import io.questdb.griffin.model.*;
import io.questdb.std.*;
import io.questdb.std.str.FlyweightCharSequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

import static io.questdb.griffin.SqlKeywords.isHourKeyword;
import static io.questdb.griffin.model.ExpressionNode.*;

public class SqlOptimiser implements Mutable {

    private static final int JOIN_OP_AND = 2;
    private static final int JOIN_OP_EQUAL = 1;
    private static final int JOIN_OP_OR = 3;
    private static final int JOIN_OP_REGEX = 4;
    private static final int NOT_OP_AND = 2;
    private static final int NOT_OP_EQUAL = 8;
    private static final int NOT_OP_GREATER = 4;
    private static final int NOT_OP_GREATER_EQ = 5;
    private static final int NOT_OP_LESS = 6;
    private static final int NOT_OP_LESS_EQ = 7;
    private static final int NOT_OP_NOT = 1;
    private static final int NOT_OP_NOT_EQ = 9;
    private static final int NOT_OP_OR = 3;
    private static final IntHashSet flexColumnModelTypes = new IntHashSet();
    //list of join types that don't support all optimisations (e.g. pushing table-specific predicates to both left and right table)
    private final static IntHashSet joinBarriers;
    private static final CharSequenceIntHashMap joinOps = new CharSequenceIntHashMap();
    private static final boolean[] joinsRequiringTimestamp = {false, false, false, false, true, true, true};

    private static final IntHashSet limitTypes = new IntHashSet();
    private static final CharSequenceIntHashMap notOps = new CharSequenceIntHashMap();
    private final static CharSequenceHashSet nullConstants = new CharSequenceHashSet();
    protected final ObjList<CharSequence> literalCollectorANames = new ObjList<>();
    private final CharacterStore characterStore;
    private final IntList clausesToSteal = new IntList();
    private final ColumnPrefixEraser columnPrefixEraser = new ColumnPrefixEraser();
    private final CairoConfiguration configuration;
    private final CharSequenceIntHashMap constNameToIndex = new CharSequenceIntHashMap();
    private final CharSequenceObjHashMap<ExpressionNode> constNameToNode = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<CharSequence> constNameToToken = new CharSequenceObjHashMap<>();
    private final ObjectPool<JoinContext> contextPool;
    private final IntHashSet deletedContexts = new IntHashSet();
    private final ObjectPool<ExpressionNode> expressionNodePool;
    private final FunctionParser functionParser;
    //list of group-by-model-level expressions with prefixes
    //we've to use it because group by is likely to contain rewritten/aliased expressions that make matching input expressions by pure AST unreliable
    private final ObjList<CharSequence> groupByAliases = new ObjList<>();
    private final ObjList<ExpressionNode> groupByNodes = new ObjList<>();
    private final BoolList groupByUsed = new BoolList();
    private final ObjectPool<IntHashSet> intHashSetPool = new ObjectPool<>(IntHashSet::new, 16);
    private final ObjList<JoinContext> joinClausesSwap1 = new ObjList<>();
    private final ObjList<JoinContext> joinClausesSwap2 = new ObjList<>();
    private final LiteralCheckingVisitor literalCheckingVisitor = new LiteralCheckingVisitor();
    private final LiteralCollector literalCollector = new LiteralCollector();
    private final IntHashSet literalCollectorAIndexes = new IntHashSet();
    private final IntHashSet literalCollectorBIndexes = new IntHashSet();
    private final ObjList<CharSequence> literalCollectorBNames = new ObjList<>();
    private final LiteralRewritingVisitor literalRewritingVisitor = new LiteralRewritingVisitor();
    private final int maxRecursion;
    private final ObjList<ExpressionNode> orderByAdvice = new ObjList<>();
    private final IntPriorityQueue orderingStack = new IntPriorityQueue();
    private final Path path;
    private final IntHashSet postFilterRemoved = new IntHashSet();
    private final ObjList<IntHashSet> postFilterTableRefs = new ObjList<>();
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    private final ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
    private final ObjList<RecordCursorFactory> tableFactoriesInFlight = new ObjList<>();
    private final FlyweightCharSequence tableLookupSequence = new FlyweightCharSequence();
    private final IntHashSet tablesSoFar = new IntHashSet();
    private final IntList tempCrossIndexes = new IntList();
    private final IntList tempCrosses = new IntList();
    private final IntList tempList = new IntList();
    private final LowerCaseCharSequenceObjHashMap<QueryColumn> tmpCursorAliases = new LowerCaseCharSequenceObjHashMap<>();
    private final PostOrderTreeTraversalAlgo traversalAlgo;
    private int defaultAliasCount = 0;
    private ObjList<JoinContext> emittedJoinClauses;
    private CharSequence tempColumnAlias;
    private QueryModel tempQueryModel;

    public SqlOptimiser(
            CairoConfiguration configuration,
            CharacterStore characterStore,
            ObjectPool<ExpressionNode> expressionNodePool,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<QueryModel> queryModelPool,
            PostOrderTreeTraversalAlgo traversalAlgo,
            FunctionParser functionParser,
            Path path
    ) {
        this.configuration = configuration;
        this.expressionNodePool = expressionNodePool;
        this.characterStore = characterStore;
        this.traversalAlgo = traversalAlgo;
        this.queryModelPool = queryModelPool;
        this.queryColumnPool = queryColumnPool;
        this.functionParser = functionParser;
        this.contextPool = new ObjectPool<>(JoinContext.FACTORY, configuration.getSqlJoinContextPoolCapacity());
        this.path = path;
        this.maxRecursion = configuration.getSqlWindowMaxRecursion();
    }

    public void clear() {
        clearForUnionModelInJoin();
        contextPool.clear();
        intHashSetPool.clear();
        joinClausesSwap1.clear();
        joinClausesSwap2.clear();
        literalCollectorAIndexes.clear();
        literalCollectorBIndexes.clear();
        literalCollectorANames.clear();
        literalCollectorBNames.clear();
        defaultAliasCount = 0;
        expressionNodePool.clear();
        characterStore.clear();
        tablesSoFar.clear();
        clausesToSteal.clear();
        tmpCursorAliases.clear();
        tableFactoriesInFlight.clear();
        groupByAliases.clear();
        groupByNodes.clear();
        groupByUsed.clear();
        tempColumnAlias = null;
        tempQueryModel = null;
    }

    public void clearForUnionModelInJoin() {
        constNameToIndex.clear();
        constNameToNode.clear();
        constNameToToken.clear();
    }

    public CharSequence findColumnByAst(ObjList<ExpressionNode> groupByNodes, ObjList<CharSequence> groupByAlises, ExpressionNode node) {
        for (int i = 0, max = groupByNodes.size(); i < max; i++) {
            ExpressionNode n = groupByNodes.getQuick(i);
            if (ExpressionNode.compareNodesExact(node, n)) {
                return groupByAlises.getQuick(i);
            }
        }
        return null;
    }

    public int findColumnIdxByAst(ObjList<ExpressionNode> groupByNodes, ExpressionNode node) {
        for (int i = 0, max = groupByNodes.size(); i < max; i++) {
            ExpressionNode n = groupByNodes.getQuick(i);
            if (ExpressionNode.compareNodesExact(node, n)) {
                return i;
            }
        }
        return -1;
    }

    private static boolean isOrderedByDesignatedTimestamp(QueryModel model) {
        return model.getTimestamp() != null && model.getOrderBy().size() == 1
                && Chars.equals(model.getOrderBy().getQuick(0).token, model.getTimestamp().token);
    }

    private static boolean isSymbolColumn(ExpressionNode countDistinctExpr, QueryModel nested) {
        return countDistinctExpr.rhs.type == LITERAL
                && nested.getAliasToColumnMap().get(countDistinctExpr.rhs.token) != null
                && nested.getAliasToColumnMap().get(countDistinctExpr.rhs.token).getColumnType() == ColumnType.SYMBOL;
    }

    private static void linkDependencies(QueryModel model, int parent, int child) {
        model.getJoinModels().getQuick(parent).addDependency(child);
    }

    private static boolean modelIsFlex(QueryModel model) {
        return model != null && flexColumnModelTypes.contains(model.getSelectModelType());
    }

    private static void unlinkDependencies(QueryModel model, int parent, int child) {
        model.getJoinModels().getQuick(parent).removeDependency(child);
    }

    /*
     * Uses validating model to determine if column name exists and non-ambiguous in case of using joins.
     */
    private void addColumnToTranslatingModel(
            QueryColumn column,
            QueryModel translatingModel,
            QueryModel validatingModel
    ) throws SqlException {
        if (validatingModel != null) {
            CharSequence refColumn = column.getAst().token;
            final int dot = Chars.indexOf(refColumn, '.');
            validateColumnAndGetModelIndex(validatingModel, refColumn, dot, column.getAst().position);
            // when we have only one model, e.g. this is not a join
            // and there is table alias to lookup column
            // we will remove this alias as unneeded
            if (dot != -1 && validatingModel.getJoinModels().size() == 1) {
                ExpressionNode base = column.getAst();
                column.of(
                        column.getAlias(),
                        expressionNodePool.next().of(
                                base.type,
                                base.token.subSequence(dot + 1, base.token.length()),
                                base.precedence,
                                base.position
                        )
                );
            }
        }
        translatingModel.addBottomUpColumn(column);
    }

    private QueryColumn addCursorFunctionAsCrossJoin(
            ExpressionNode node,
            @Nullable CharSequence alias,
            QueryModel cursorModel,
            @Nullable QueryModel innerVirtualModel,
            QueryModel translatingModel,
            QueryModel baseModel,
            SqlExecutionContext sqlExecutionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        QueryColumn qc = cursorModel.findBottomUpColumnByAst(node);
        if (qc == null) {

            // we are about to add new column as a join model to the base model
            // the name of this column must not clash with any name of the base mode
            CharSequence baseAlias;
            if (alias != null) {
                baseAlias = createColumnAlias(alias, baseModel);
            } else {
                baseAlias = createColumnAlias(node, baseModel);
            }

            // add to temp aliases so that two cursors cannot use the same alias!
            baseAlias = SqlUtil.createColumnAlias(characterStore, baseAlias, -1, tmpCursorAliases);

            final QueryColumn crossColumn = queryColumnPool.next().of(baseAlias, node);

            final QueryModel cross = queryModelPool.next();
            cross.setJoinType(QueryModel.JOIN_CROSS);
            cross.setSelectModelType(QueryModel.SELECT_MODEL_CURSOR);
            cross.setAlias(makeJoinAlias());

            final QueryModel crossInner = queryModelPool.next();
            crossInner.setTableNameExpr(node);
            parseFunctionAndEnumerateColumns(crossInner, sqlExecutionContext, sqlParserCallback);
            cross.setNestedModel(crossInner);

            cross.addBottomUpColumn(crossColumn);
            baseModel.addJoinModel(cross);

            // keep track of duplicates
            tmpCursorAliases.put(baseAlias, crossColumn);

            // now we need to make alias in the translating column

            CharSequence translatingAlias;
            translatingAlias = createColumnAlias(baseAlias, translatingModel);

            // add trackable expression to cursor model
            cursorModel.addBottomUpColumn(queryColumnPool.next().of(baseAlias, node));

            qc = queryColumnPool.next().of(translatingAlias, nextLiteral(baseAlias));
            translatingModel.addBottomUpColumn(qc);

        } else {
            final CharSequence al = translatingModel.getColumnNameToAliasMap().get(qc.getAlias());
            if (alias != null && !Chars.equals(al, alias)) {
                QueryColumn existing = translatingModel.getAliasToColumnMap().get(alias);
                if (existing == null) {
                    // create new column
                    qc = nextColumn(alias, al);
                    translatingModel.addBottomUpColumn(qc);

                    if (innerVirtualModel != null) {
                        innerVirtualModel.addBottomUpColumn(qc);
                    }

                    return qc;
                }

                if (ExpressionNode.compareNodesExact(node, existing.getAst())) {
                    return existing;
                }

                throw SqlException.invalidColumn(node.position, "duplicate alias");
            }

            // check if column is in inner virtual model as requested
            qc = translatingModel.getAliasToColumnMap().get(al);
        }
        if (innerVirtualModel != null) {
            innerVirtualModel.addBottomUpColumn(qc);
        }
        return qc;
    }

    private void addFilterOrEmitJoin(QueryModel parent, int idx, int ai, CharSequence an, ExpressionNode ao, int bi, CharSequence bn, ExpressionNode bo) {
        if (ai == bi && Chars.equals(an, bn)) {
            deletedContexts.add(idx);
            return;
        }

        if (ai == bi) {
            // (same table)
            ExpressionNode node = expressionNodePool.next().of(ExpressionNode.OPERATION, "=", 0, 0);
            node.paramCount = 2;
            node.lhs = ao;
            node.rhs = bo;
            addWhereNode(parent, ai, node);
        } else {
            // (different tables)
            JoinContext jc = contextPool.next();
            jc.aIndexes.add(ai);
            jc.aNames.add(an);
            jc.aNodes.add(ao);
            jc.bIndexes.add(bi);
            jc.bNames.add(bn);
            jc.bNodes.add(bo);
            jc.slaveIndex = Math.max(ai, bi);
            jc.parents.add(Math.min(ai, bi));
            emittedJoinClauses.add(jc);
        }

        deletedContexts.add(idx);
    }

    private void addFunction(
            QueryColumn qc,
            QueryModel validatingModel,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel windowModel,
            QueryModel groupByModel,
            QueryModel outerModel,
            QueryModel distinctModel
    ) throws SqlException {
        // there were no aggregation functions emitted therefore
        // this is just a function that goes into virtual model
        innerModel.addBottomUpColumn(qc);

        // we also create column that references this inner layer from outer layer,
        // for example when we have:
        // select a, b+c ...
        // it should translate to:
        // select a, x from (select a, b+c x from (select a,b,c ...))
        final QueryColumn innerColumn = nextColumn(qc.getAlias());

        // pull literals only into translating model
        emitLiterals(qc.getAst(), translatingModel, null, validatingModel, false);
        groupByModel.addBottomUpColumn(innerColumn);
        windowModel.addBottomUpColumn(innerColumn);
        outerModel.addBottomUpColumn(innerColumn);
        distinctModel.addBottomUpColumn(innerColumn);
    }

    private void addJoinContext(QueryModel parent, JoinContext context) {
        QueryModel jm = parent.getJoinModels().getQuick(context.slaveIndex);
        JoinContext other = jm.getContext();
        if (other == null) {
            jm.setContext(context);
        } else {
            jm.setContext(mergeContexts(parent, other, context));
        }
    }

    //add table prefix to all column references to make it easier to compare expressions
    private void addMissingTablePrefixes(ExpressionNode node, QueryModel baseModel) throws SqlException {
        sqlNodeStack.clear();

        ExpressionNode temp = replaceIfUnaliasedLiteral(node, baseModel);
        if (temp != node) {
            node.of(LITERAL, temp.token, node.precedence, node.position);
            return;
        }

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.paramCount < 3) {
                    if (node.rhs != null) {
                        temp = replaceIfUnaliasedLiteral(node.rhs, baseModel);
                        if (node.rhs == temp) {
                            sqlNodeStack.push(node.rhs);
                        } else {
                            node.rhs = temp;
                        }
                    }

                    if (node.lhs != null) {
                        temp = replaceIfUnaliasedLiteral(node.lhs, baseModel);
                        if (temp == node.lhs) {
                            node = node.lhs;
                        } else {
                            node.lhs = temp;
                            node = null;
                        }
                    } else {
                        node = null;
                    }
                } else {
                    for (int i = 1, k = node.paramCount; i < k; i++) {
                        ExpressionNode e = node.args.getQuick(i);
                        temp = replaceIfUnaliasedLiteral(e, baseModel);
                        if (e == temp) {
                            sqlNodeStack.push(e);
                        } else {
                            node.args.setQuick(i, temp);
                        }
                    }

                    ExpressionNode e = node.args.getQuick(0);
                    temp = replaceIfUnaliasedLiteral(e, baseModel);
                    if (e == temp) {
                        node = e;
                    } else {
                        node.args.setQuick(0, temp);
                        node = null;
                    }
                }
            } else {
                node = sqlNodeStack.poll();
            }
        }
    }

    private void addOuterJoinExpression(QueryModel parent, QueryModel model, int joinIndex, ExpressionNode node) {
        model.setOuterJoinExpressionClause(concatFilters(model.getOuterJoinExpressionClause(), node));
        // add dependency to prevent previous model reordering (left joins are not symmetric)
        if (joinIndex > 0) {
            linkDependencies(parent, joinIndex - 1, joinIndex);
        }
    }

    private void addPostJoinWhereClause(QueryModel model, ExpressionNode node) {
        model.setPostJoinWhereClause(concatFilters(model.getPostJoinWhereClause(), node));
    }

    private void addTopDownColumn(@Transient ExpressionNode node, QueryModel model) {
        if (node != null && node.type == LITERAL) {
            final CharSequence columnName = node.token;
            final int dotIndex = Chars.indexOf(columnName, '.');
            if (dotIndex == -1) {
                // When there is no dot in column name it is still possible that column comes from
                // one of the join models. What we need to do here is to assign column to that model
                // which already have this column in alias map
                addTopDownColumn(columnName, model);
            } else {
                int modelIndex = model.getModelAliasIndex(node.token, 0, dotIndex);
                if (modelIndex < 0) {
                    // alias cannot be resolved, we will trust that the calling side will handle this
                    // in this context we do not have model that is able to resolve table alias
                    return;
                }

                addTopDownColumn0(
                        node,
                        model.getJoinModels().getQuick(modelIndex),
                        node.token.subSequence(dotIndex + 1, node.token.length())
                );
            }
        }
    }

    private void addTopDownColumn(CharSequence columnName, QueryModel model) {
        final ObjList<QueryModel> joinModels = model.getJoinModels();
        final int joinCount = joinModels.size();
        for (int i = 0; i < joinCount; i++) {
            final QueryModel m = joinModels.getQuick(i);
            final QueryColumn column = m.getAliasToColumnMap().get(columnName);
            if (column != null) {
                if (m.getSelectModelType() == QueryModel.SELECT_MODEL_NONE) {
                    m.addTopDownColumn(
                            queryColumnPool.next().of(
                                    columnName,
                                    nextLiteral(columnName)
                            ),
                            columnName
                    );
                } else {
                    m.addTopDownColumn(column, columnName);
                }
                break;
            }
        }
    }

    private void addTopDownColumn0(@Transient ExpressionNode node, QueryModel model, CharSequence name) {
        if (model.isTopDownNameMissing(name)) {
            model.addTopDownColumn(
                    queryColumnPool.next().of(
                            name,
                            expressionNodePool.next().of(node.type, name, node.precedence, node.position)
                    )
                    , name
            );
        }
    }

    /**
     * Adds filters derived from transitivity of equals operation, for example
     * if there is filter:
     * <p>
     * a.x = b.x and b.x = 10
     * <p>
     * derived filter would be:
     * <p>
     * a.x = 10
     * <p>
     * this filter is not explicitly mentioned, but it might help pre-filtering record sources
     * before hashing.
     */
    private void addTransitiveFilters(QueryModel model) {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            JoinContext jc = joinModels.getQuick(i).getContext();
            if (jc != null) {
                for (int k = 0, kn = jc.bNames.size(); k < kn; k++) {
                    CharSequence name = jc.bNames.getQuick(k);
                    if (constNameToIndex.get(name) == jc.bIndexes.getQuick(k)) {
                        ExpressionNode node = expressionNodePool.next().of(ExpressionNode.OPERATION, constNameToToken.get(name), 0, 0);
                        node.lhs = jc.aNodes.getQuick(k);
                        node.rhs = constNameToNode.get(name);
                        node.paramCount = 2;
                        addWhereNode(model, jc.slaveIndex, node);
                    }
                }
            }
        }
    }

    private void addWhereNode(QueryModel model, int joinModelIndex, ExpressionNode node) {
        addWhereNode(model.getJoinModels().getQuick(joinModelIndex), node);
    }

    private void addWhereNode(QueryModel model, ExpressionNode node) {
        model.setWhereClause(concatFilters(model.getWhereClause(), node));
    }

    /**
     * Move fields that belong to slave table to left and parent fields
     * to right of equals operator.
     */
    private void alignJoinClauses(QueryModel parent) {
        ObjList<QueryModel> joinModels = parent.getJoinModels();
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            JoinContext jc = joinModels.getQuick(i).getContext();
            if (jc != null) {
                int index = jc.slaveIndex;
                for (int k = 0, kc = jc.aIndexes.size(); k < kc; k++) {
                    if (jc.aIndexes.getQuick(k) != index) {
                        int idx = jc.aIndexes.getQuick(k);
                        CharSequence name = jc.aNames.getQuick(k);
                        ExpressionNode node = jc.aNodes.getQuick(k);

                        jc.aIndexes.setQuick(k, jc.bIndexes.getQuick(k));
                        jc.aNames.setQuick(k, jc.bNames.getQuick(k));
                        jc.aNodes.setQuick(k, jc.bNodes.getQuick(k));

                        jc.bIndexes.setQuick(k, idx);
                        jc.bNames.setQuick(k, name);
                        jc.bNodes.setQuick(k, node);
                    }
                }
            }
        }
    }

    //checks join equality condition and pushes it to optimal join contexts (could be a different join context)
    //NOTE on LEFT JOIN :
    // - left join condition MUST remain as is otherwise it'll produce wrong results
    // - only predicates relating to LEFT table may be pushed down
    // - predicates on both or right table may be added to post join clause as long as they're marked properly (via ExpressionNode.isOuterJoinPredicate)
    private void analyseEquals(QueryModel parent, ExpressionNode node, boolean innerPredicate, QueryModel joinModel) throws SqlException {
        traverseNamesAndIndices(parent, node);
        int aSize = literalCollectorAIndexes.size();
        int bSize = literalCollectorBIndexes.size();

        JoinContext jc;
        boolean canMovePredicate = joinBarriers.excludes(joinModel.getJoinType());
        int joinIndex = parent.getJoinModels().indexOf(joinModel);

        //switch code below assumes expression are simple column references
        if (literalCollector.functionCount > 0) {
            node.innerPredicate = innerPredicate;
            if (canMovePredicate) {
                parent.addParsedWhereNode(node, innerPredicate);
            } else {
                addOuterJoinExpression(parent, joinModel, joinIndex, node);
            }
            return;
        }

        switch (aSize) {
            case 0:
                if (!canMovePredicate) {
                    addOuterJoinExpression(parent, joinModel, joinIndex, node);
                    break;
                }
                if (bSize == 1
                        && literalCollector.nullCount == 0
                        // table must not be OUTER or ASOF joined
                        && joinBarriers.excludes(parent.getJoinModels().get(literalCollectorBIndexes.get(0)).getJoinType())
                ) {
                    // single table reference + constant
                    jc = contextPool.next();
                    jc.slaveIndex = literalCollectorBIndexes.get(0);

                    addWhereNode(parent, jc.slaveIndex, node);
                    addJoinContext(parent, jc);

                    CharSequence cs = literalCollectorBNames.getQuick(0);
                    constNameToIndex.put(cs, jc.slaveIndex);
                    constNameToNode.put(cs, node.lhs);
                    constNameToToken.put(cs, node.token);
                } else {
                    parent.addParsedWhereNode(node, innerPredicate);
                }
                break;
            case 1:
                jc = contextPool.next();
                int lhi = literalCollectorAIndexes.get(0);
                if (bSize == 1) {
                    int rhi = literalCollectorBIndexes.get(0);
                    if (lhi == rhi) {
                        // single table reference
                        jc.slaveIndex = lhi;
                        if (canMovePredicate) {
                            // we can't push anything into other left join
                            if (jc.slaveIndex != joinIndex &&
                                    joinBarriers.contains(parent.getJoinModels().get(jc.slaveIndex).getJoinType())) {
                                addPostJoinWhereClause(parent.getJoinModels().getQuick(jc.slaveIndex), node);
                            } else {
                                addWhereNode(parent, lhi, node);
                            }
                            return;
                        }
                    } else if (lhi < rhi) {
                        // we must align "a" nodes with slave index
                        // compiler will always be checking "a" columns
                        // against metadata of the slave the context is assigned to
                        jc.aNodes.add(node.lhs);
                        jc.bNodes.add(node.rhs);
                        jc.aNames.add(literalCollectorANames.getQuick(0));
                        jc.bNames.add(literalCollectorBNames.getQuick(0));
                        jc.aIndexes.add(lhi);
                        jc.bIndexes.add(rhi);
                        jc.slaveIndex = rhi;
                        jc.parents.add(lhi);
                    } else {
                        jc.aNodes.add(node.rhs);
                        jc.bNodes.add(node.lhs);
                        jc.aNames.add(literalCollectorBNames.getQuick(0));
                        jc.bNames.add(literalCollectorANames.getQuick(0));
                        jc.aIndexes.add(rhi);
                        jc.bIndexes.add(lhi);
                        jc.slaveIndex = lhi;
                        jc.parents.add(rhi);
                    }

                    if (canMovePredicate || jc.slaveIndex == joinIndex) {
                        //we can't push anything into other left join
                        if (jc.slaveIndex != joinIndex && joinBarriers.contains(parent.getJoinModels().get(jc.slaveIndex).getJoinType())) {
                            addPostJoinWhereClause(parent.getJoinModels().getQuick(jc.slaveIndex), node);
                        } else {
                            addJoinContext(parent, jc);
                            if (lhi != rhi) {
                                linkDependencies(parent, Math.min(lhi, rhi), Math.max(lhi, rhi));
                            }
                        }
                    } else {
                        addOuterJoinExpression(parent, joinModel, joinIndex, node);
                    }
                } else if (bSize == 0
                        && literalCollector.nullCount == 0
                        && joinBarriers.excludes(parent.getJoinModels().get(literalCollectorAIndexes.get(0)).getJoinType())) {
                    // single table reference + constant
                    if (!canMovePredicate) {
                        addOuterJoinExpression(parent, joinModel, joinIndex, node);
                        break;
                    }
                    jc.slaveIndex = lhi;
                    addWhereNode(parent, lhi, node);
                    addJoinContext(parent, jc);

                    CharSequence cs = literalCollectorANames.getQuick(0);
                    constNameToIndex.put(cs, lhi);
                    constNameToNode.put(cs, node.rhs);
                    constNameToToken.put(cs, node.token);
                } else {
                    if (canMovePredicate) {
                        parent.addParsedWhereNode(node, innerPredicate);
                    } else {
                        addOuterJoinExpression(parent, joinModel, joinIndex, node);
                    }
                }
                break;
            default:
                if (canMovePredicate) {
                    node.innerPredicate = innerPredicate;
                    parent.addParsedWhereNode(node, innerPredicate);
                } else {
                    addOuterJoinExpression(parent, joinModel, joinIndex, node);
                }

                break;
        }
    }

    private void analyseRegex(QueryModel parent, ExpressionNode node) throws SqlException {
        traverseNamesAndIndices(parent, node);

        if (literalCollector.nullCount == 0) {
            int aSize = literalCollectorAIndexes.size();
            int bSize = literalCollectorBIndexes.size();
            if (aSize == 1 && bSize == 0) {
                CharSequence name = literalCollectorANames.getQuick(0);
                constNameToIndex.put(name, literalCollectorAIndexes.get(0));
                constNameToNode.put(name, node.rhs);
                constNameToToken.put(name, node.token);
            }
        }
    }

    private void assignFilters(QueryModel parent) throws SqlException {
        tablesSoFar.clear();
        postFilterRemoved.clear();
        postFilterTableRefs.clear();

        literalCollector.withModel(parent);
        ObjList<ExpressionNode> filterNodes = parent.getParsedWhere();
        // collect table indexes from each part of global filter
        int pc = filterNodes.size();
        for (int i = 0; i < pc; i++) {
            IntHashSet indexes = intHashSetPool.next();
            literalCollector.resetCounts();
            traversalAlgo.traverse(filterNodes.getQuick(i), literalCollector.to(indexes));
            postFilterTableRefs.add(indexes);
        }

        IntList ordered = parent.getOrderedJoinModels();
        // match table references to set of table in join order
        for (int i = 0, n = ordered.size(); i < n; i++) {
            int index = ordered.getQuick(i);
            tablesSoFar.add(index);

            for (int k = 0; k < pc; k++) {
                if (postFilterRemoved.contains(k)) {
                    continue;
                }

                final ExpressionNode node = filterNodes.getQuick(k);

                IntHashSet refs = postFilterTableRefs.getQuick(k);
                int rs = refs.size();
                if (rs == 0) {
                    // condition has no table references
                    // must evaluate as constant
                    postFilterRemoved.add(k);
                    parent.setConstWhereClause(concatFilters(parent.getConstWhereClause(), node));
                } else if (rs == 1 && // single table reference and this table is not joined via OUTER or ASOF
                        joinBarriers.excludes(parent.getJoinModels().getQuick(refs.get(0)).getJoinType())) {
                    // get single table reference out of the way right away
                    // we don't have to wait until "our" table comes along
                    addWhereNode(parent, refs.get(0), node);
                    postFilterRemoved.add(k);
                } else {
                    boolean qualifies = true;
                    // check if filter references table processed so far
                    for (int y = 0; y < rs; y++) {
                        if (tablesSoFar.excludes(refs.get(y))) {
                            qualifies = false;
                            break;
                        }
                    }
                    if (qualifies) {
                        postFilterRemoved.add(k);
                        QueryModel m = parent.getJoinModels().getQuick(index);
                        m.setPostJoinWhereClause(concatFilters(m.getPostJoinWhereClause(), node));
                    }
                }
            }
        }
        assert postFilterRemoved.size() == pc;
    }

    // The model for the following SQL:
    // select * from t1 union all select * from t2 order by x
    // will have "order by" clause on the last model of the union linked list.
    // Semantically, order by must be executed after union. To get there, we will
    // create outer model(s) for the union block and move "order by" there.
    private QueryModel bubbleUpOrderByAndLimitFromUnion(QueryModel model) throws SqlException {
        QueryModel m = model.getUnionModel();
        QueryModel nested = model.getNestedModel();
        if (nested != null) {
            QueryModel _n = bubbleUpOrderByAndLimitFromUnion(nested);
            if (_n != nested) {
                model.setNestedModel(_n);
            }
        }

        if (m != null) {
            // find order by clauses
            if (m.getNestedModel() != null) {
                final QueryModel m1 = bubbleUpOrderByAndLimitFromUnion(m.getNestedModel());
                if (m1 != m) {
                    m.setNestedModel(m1);
                }
            }

            do {
                if (m.getUnionModel() == null) {
                    // last model in the linked list
                    QueryModel un = m.getNestedModel();
                    int n = un.getOrderBy().size();
                    // order by clause is on the nested model
                    final ObjList<ExpressionNode> orderBy = un.getOrderBy();
                    final IntList orderByDirection = un.getOrderByDirection();
                    // limit is on the parent model
                    final ExpressionNode limitLo = m.getLimitLo();
                    final ExpressionNode limitHi = m.getLimitHi();

                    if (n > 0 || limitHi != null || limitLo != null) {
                        // we have some order by clauses to move
                        QueryModel _nested = queryModelPool.next();
                        for (int i = 0; i < n; i++) {
                            _nested.addOrderBy(orderBy.getQuick(i), orderByDirection.getQuick(i));
                        }
                        orderBy.clear();
                        orderByDirection.clear();

                        m.setLimit(null, null);

                        _nested.setNestedModel(model);
                        QueryModel _model = queryModelPool.next();
                        _model.setNestedModel(_nested);
                        SqlUtil.addSelectStar(_model, queryColumnPool, expressionNodePool);
                        _model.setLimit(limitLo, limitHi);
                        return _model;
                    }
                    break;
                }

                m = m.getUnionModel();
            } while (true);
        }
        return model;
    }

    //pushing predicates to sample by model is only allowed for sample by fill none align to calendar and expressions on non-timestamp columns
    //pushing for other fill options or sample by first observation could alter result
    private boolean canPushToSampleBy(final QueryModel model, ObjList<CharSequence> expressionColumns) {
        ObjList<ExpressionNode> fill = model.getSampleByFill();
        int fillCount = fill.size();
        boolean isFillNone = fillCount == 0 || (fillCount == 1 && SqlKeywords.isNoneKeyword(fill.getQuick(0).token));

        if (!isFillNone || model.getSampleByOffset() == null) {
            return false;
        }

        CharSequence timestamp = findTimestamp(model);
        if (timestamp == null) {
            return true;
        }

        for (int i = 0, n = expressionColumns.size(); i < n; i++) {
            if (Chars.equalsIgnoreCase(expressionColumns.get(i), timestamp)) {
                return false;
            }
        }

        return true;
    }

    private boolean checkForAggregates(ExpressionNode node) {
        sqlNodeStack.clear();
        while (node != null) {
            if (node.rhs != null) {
                if (functionParser.getFunctionFactoryCache().isGroupBy(node.rhs.token)) {
                    return true;
                }
                sqlNodeStack.push(node.rhs);
            }

            if (node.lhs != null) {
                if (functionParser.getFunctionFactoryCache().isGroupBy(node.lhs.token)) {
                    return true;
                }
                node = node.lhs;
            } else {
                if (!sqlNodeStack.isEmpty()) {
                    node = sqlNodeStack.poll();
                } else {
                    node = null;
                }
            }
        }

        return false;
    }

    private void checkIsNotAggregateOrWindowFunction(ExpressionNode node, QueryModel model) throws SqlException {
        if (node.type == FUNCTION) {
            if (functionParser.getFunctionFactoryCache().isGroupBy(node.token)) {
                throw SqlException.$(node.position, "aggregate functions are not allowed in GROUP BY");
            }
            if (node.type == FUNCTION && functionParser.getFunctionFactoryCache().isWindow(node.token)) {
                throw SqlException.$(node.position, "window functions are not allowed in GROUP BY");
            }
        }
        if (node.type == LITERAL) {
            QueryColumn column = model.getAliasToColumnMap().get(node.token);
            if (column != null) {
                checkForAggregates(column.getAst());
            }
        }
    }

    private QueryColumn checkSimpleIntegerColumn(ExpressionNode column, QueryModel model) {
        if (column == null || column.type != LITERAL) {
            return null;
        }

        CharSequence tok = column.token;
        final int dot = Chars.indexOf(tok, '.');
        QueryColumn qc = getQueryColumn(model, tok, dot);

        if (qc != null &&
                (qc.getColumnType() == ColumnType.BYTE ||
                        qc.getColumnType() == ColumnType.SHORT ||
                        qc.getColumnType() == ColumnType.INT ||
                        qc.getColumnType() == ColumnType.LONG)) {
            return qc;
        }
        return null;
    }

    private void collectModelAlias(QueryModel parent, int modelIndex, QueryModel model) throws SqlException {
        final ExpressionNode alias = model.getAlias() != null ? model.getAlias() : model.getTableNameExpr();
        if (parent.addModelAliasIndex(alias, modelIndex)) {
            return;
        }
        throw SqlException.position(alias.position).put("Duplicate table or alias: ").put(alias.token);
    }

    private ExpressionNode concatFilters(ExpressionNode old, ExpressionNode filter) {
        if (old == null) {
            return filter;
        } else {
            ExpressionNode n = expressionNodePool.next().of(ExpressionNode.OPERATION, "and", 0, filter.position);
            n.paramCount = 2;
            n.lhs = old;
            n.rhs = filter;
            return n;
        }
    }

    private void copyColumnTypesFromMetadata(QueryModel model, TableRecordMetadata m) {
        // TODO: optimise by copying column indexes, types of the columns used in SET clause in the UPDATE only
        for (int i = 0, k = m.getColumnCount(); i < k; i++) {
            model.addUpdateTableColumnMetadata(m.getColumnType(i), m.getColumnName(i));
        }
    }

    private void copyColumnsFromMetadata(QueryModel model, RecordMetadata m, boolean nonLiteral) throws SqlException {
        // column names are not allowed to have a dot

        for (int i = 0, k = m.getColumnCount(); i < k; i++) {
            CharSequence columnName = createColumnAlias(m.getColumnName(i), model, nonLiteral);
            QueryColumn column = queryColumnPool.next().of(columnName, expressionNodePool.next().of(LITERAL, columnName, 0, 0), true, m.getColumnType(i));
            model.addField(column);
        }

        // validate explicitly defined timestamp, if it exists
        ExpressionNode timestamp = model.getTimestamp();
        if (timestamp == null) {
            if (m.getTimestampIndex() != -1) {
                model.setTimestamp(expressionNodePool.next().of(LITERAL, m.getColumnName(m.getTimestampIndex()), 0, 0));
            }
        } else {
            int index = m.getColumnIndexQuiet(timestamp.token);
            if (index == -1) {
                throw SqlException.invalidColumn(timestamp.position, timestamp.token);
            } else if (!ColumnType.isTimestamp(m.getColumnType(index))) {
                throw SqlException.$(timestamp.position, "not a TIMESTAMP");
            }
        }
    }

    private CharSequence createColumnAlias(CharSequence name, QueryModel model, boolean nonLiteral) {
        return SqlUtil.createColumnAlias(characterStore, name, -1, model.getAliasToColumnMap(), nonLiteral);
    }

    private CharSequence createColumnAlias(CharSequence name, QueryModel model) {
        return SqlUtil.createColumnAlias(characterStore, name, -1, model.getAliasToColumnMap());
    }

    private CharSequence createColumnAlias(ExpressionNode node, QueryModel model) {
        return SqlUtil.createColumnAlias(characterStore, node.token, Chars.indexOf(node.token, '.'), model.getAliasToColumnMap());
    }

    // use only if input is a column literal!
    private QueryColumn createGroupByColumn(
            CharSequence columnName,
            ExpressionNode columnAst,
            QueryModel validatingModel,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel windowModel,
            QueryModel groupByModel
    ) throws SqlException {
        // add duplicate column names only to group-by model
        // taking into account that column is pre-aliased, e.g.
        // "col, col" will look like "col, col col1"

        LowerCaseCharSequenceObjHashMap<CharSequence> translatingAliasMap = translatingModel.getColumnNameToAliasMap();
        int index = translatingAliasMap.keyIndex(columnAst.token);
        if (index < 0) {
            // column is already being referenced by translating model
            final CharSequence translatedColumnName = translatingAliasMap.valueAtQuick(index);
            final CharSequence innerAlias = createColumnAlias(columnName, groupByModel);
            final QueryColumn translatedColumn = nextColumn(innerAlias, translatedColumnName);
            innerModel.addBottomUpColumn(columnAst.position, translatedColumn, true);
            groupByModel.addBottomUpColumn(translatedColumn);
            return translatedColumn;
        } else {
            final CharSequence alias = createColumnAlias(columnName, translatingModel);
            addColumnToTranslatingModel(
                    queryColumnPool.next().of(
                            alias,
                            columnAst
                    ),
                    translatingModel,
                    validatingModel
            );

            final QueryColumn translatedColumn = nextColumn(alias);
            // create column that references inner alias we just created
            innerModel.addBottomUpColumn(translatedColumn);
            windowModel.addBottomUpColumn(translatedColumn);
            groupByModel.addBottomUpColumn(translatedColumn);
            return translatedColumn;
        }
    }

    /**
     * Creates dependencies via implied columns, typically timestamp.
     * Dependencies like that are not explicitly expressed in SQL query and
     * therefore are not created by analyzing "where" clause.
     * <p>
     * Explicit dependencies however are required for table ordering.
     *
     * @param parent the parent model
     */
    private void createImpliedDependencies(QueryModel parent) {
        ObjList<QueryModel> models = parent.getJoinModels();
        JoinContext jc;
        for (int i = 0, n = models.size(); i < n; i++) {
            QueryModel m = models.getQuick(i);
            if (joinsRequiringTimestamp[m.getJoinType()]) {
                linkDependencies(parent, 0, i);
                if (m.getContext() == null) {
                    m.setContext(jc = contextPool.next());
                    jc.parents.add(0);
                    jc.slaveIndex = i;
                }
            }
        }
    }

    // order hash is used to determine redundant order when parsing window function definition
    private void createOrderHash(QueryModel model) {
        LowerCaseCharSequenceIntHashMap hash = model.getOrderHash();
        hash.clear();

        final ObjList<ExpressionNode> orderBy = model.getOrderBy();
        final int n = orderBy.size();

        if (n > 0) {
            final IntList orderByDirection = model.getOrderByDirection();
            for (int i = 0; i < n; i++) {
                // if a column appears multiple times in the ORDER BY clause then we use only the first occurrence.
                // why? consider this clause: ORDER BY A DESC, A ASC. In this case we want to order to be "A DESC" only.
                // why? "A ASC" is a lower in priority, and it's applicable if and only if the items within first clause
                // are equal. but if the items are already equal then there is no point in ordering by the same column again.
                // unconditional put() would be a bug as a lower priority ordering would replace ordering with a higher priority.
                hash.putIfAbsent(orderBy.getQuick(i).token, orderByDirection.getQuick(i));
            }
        }

        final QueryModel nestedModel = model.getNestedModel();
        if (nestedModel != null) {
            createOrderHash(nestedModel);
        }

        final ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, z = joinModels.size(); i < z; i++) {
            createOrderHash(joinModels.getQuick(i));
        }

        final QueryModel union = model.getUnionModel();
        if (union != null) {
            createOrderHash(union);
        }
    }

    // add existing group by column to outer & distinct models
    private boolean createSelectColumn(
            CharSequence alias,
            CharSequence columnName,
            QueryModel groupByModel,
            QueryModel outerModel,
            QueryModel distinctModel
    ) throws SqlException {
        QueryColumn groupByColumn = groupByModel.getAliasToColumnMap().get(columnName);
        QueryColumn outerColumn = nextColumn(alias, groupByColumn.getAlias());
        outerColumn = ensureAliasUniqueness(outerModel, outerColumn);
        outerModel.addBottomUpColumn(outerColumn);

        boolean sameAlias = Chars.equalsIgnoreCase(groupByColumn.getAlias(), outerColumn.getAlias());
        if (distinctModel != null) {
            if (sameAlias) {
                distinctModel.addBottomUpColumn(outerColumn);
            } else { // we've to use alias from outer model
                QueryColumn distinctColumn = nextColumn(outerColumn.getAlias());
                distinctColumn = ensureAliasUniqueness(distinctModel, distinctColumn);
                distinctModel.addBottomUpColumn(distinctColumn);
            }
        }

        return sameAlias;
    }

    private void createSelectColumn(
            CharSequence columnName,
            ExpressionNode columnAst,
            boolean allowDuplicates,
            QueryModel validatingModel,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel windowModel,
            QueryModel groupByModel,
            QueryModel outerModel,
            QueryModel distinctModel
    ) throws SqlException {
        // add duplicate column names only to group-by model
        // taking into account that column is pre-aliased, e.g.
        // "col, col" will look like "col, col col1"

        final LowerCaseCharSequenceObjHashMap<CharSequence> translatingAliasMap = translatingModel.getColumnNameToAliasMap();
        final int index = translatingAliasMap.keyIndex(columnAst.token);
        if (index < 0) {
            // check if the column is a duplicate, i.e. already referenced by the group-by model
            if (!allowDuplicates && groupByModel.getAliasToColumnMap().contains(columnName)) {
                throw SqlException.duplicateColumn(columnAst.position, columnName);
            }
            // column is already being referenced by translating model
            final CharSequence translatedColumnName = translatingAliasMap.valueAtQuick(index);
            final CharSequence innerAlias = createColumnAlias(columnName, groupByModel);
            final QueryColumn translatedColumn = nextColumn(innerAlias, translatedColumnName);
            innerModel.addBottomUpColumn(columnAst.position, translatedColumn, true);
            groupByModel.addBottomUpColumn(translatedColumn);

            // window model is used together with inner model
            final CharSequence windowAlias = createColumnAlias(innerAlias, windowModel);
            final QueryColumn windowColumn = nextColumn(windowAlias, innerAlias);
            windowModel.addBottomUpColumn(windowColumn);
            outerModel.addBottomUpColumn(translatedColumn);
            if (distinctModel != null) {
                distinctModel.addBottomUpColumn(translatedColumn);
            }
        } else {
            final CharSequence alias;
            if (groupByModel.getAliasToColumnMap().contains(columnName)) {
                // the column is not yet translated, but another column is referenced via the same name
                if (!allowDuplicates) {
                    throw SqlException.duplicateColumn(columnAst.position, columnName);
                }
                alias = createColumnAlias(columnName, groupByModel);
            } else {
                alias = createColumnAlias(columnName, translatingModel);
            }
            addColumnToTranslatingModel(
                    queryColumnPool.next().of(
                            alias,
                            columnAst
                    ),
                    translatingModel,
                    validatingModel
            );

            final QueryColumn translatedColumn = nextColumn(alias);

            // create column that references inner alias we just created
            innerModel.addBottomUpColumn(translatedColumn);
            windowModel.addBottomUpColumn(translatedColumn);
            groupByModel.addBottomUpColumn(translatedColumn);
            outerModel.addBottomUpColumn(translatedColumn);
            if (distinctModel != null) {
                distinctModel.addBottomUpColumn(translatedColumn);
            }
        }
    }

    private void createSelectColumn0(
            CharSequence columnName,
            ExpressionNode columnAst,
            QueryModel validatingModel,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel windowModel
    ) throws SqlException {
        // add duplicate column names only to group-by model
        // taking into account that column is pre-aliased, e.g.
        // "col, col" will look like "col, col col1"

        LowerCaseCharSequenceObjHashMap<CharSequence> translatingAliasMap = translatingModel.getColumnNameToAliasMap();
        int index = translatingAliasMap.keyIndex(columnAst.token);
        if (index < 0) {
            // column is already being referenced by translating model
            final CharSequence translatedColumnName = translatingAliasMap.valueAtQuick(index);
            final CharSequence innerAlias = createColumnAlias(columnName, innerModel);
            final QueryColumn translatedColumn = nextColumn(innerAlias, translatedColumnName);
            innerModel.addBottomUpColumn(translatedColumn);

            // window model is used together with inner model
            final CharSequence windowAlias = createColumnAlias(innerAlias, windowModel);
            final QueryColumn windowColumn = nextColumn(windowAlias, innerAlias);
            windowModel.addBottomUpColumn(windowColumn);
        } else {
            final CharSequence alias = createColumnAlias(columnName, translatingModel);
            addColumnToTranslatingModel(
                    queryColumnPool.next().of(
                            alias,
                            columnAst
                    ),
                    translatingModel,
                    validatingModel
            );

            final QueryColumn translatedColumn = nextColumn(alias);

            // create column that references inner alias we just created
            innerModel.addBottomUpColumn(translatedColumn);
            windowModel.addBottomUpColumn(translatedColumn);
        }
    }

    private void createSelectColumnsForWildcard(
            QueryColumn qc,
            boolean hasJoins,
            QueryModel baseModel,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel windowModel,
            QueryModel groupByModel,
            QueryModel outerModel,
            QueryModel distinctModel
    ) throws SqlException {
        // this could be a wildcard, such as '*' or 'a.*'
        int dot = Chars.indexOf(qc.getAst().token, '.');
        if (dot > -1) {
            int index = baseModel.getModelAliasIndex(qc.getAst().token, 0, dot);
            if (index == -1) {
                throw SqlException.$(qc.getAst().position, "invalid table alias");
            }

            // we are targeting single table
            createSelectColumnsForWildcard0(
                    baseModel.getJoinModels().getQuick(index),
                    hasJoins,
                    qc.getAst().position,
                    translatingModel,
                    innerModel,
                    windowModel,
                    groupByModel,
                    outerModel,
                    distinctModel
            );
        } else {
            ObjList<QueryModel> models = baseModel.getJoinModels();
            for (int j = 0, z = models.size(); j < z; j++) {
                createSelectColumnsForWildcard0(
                        models.getQuick(j),
                        hasJoins,
                        qc.getAst().position,
                        translatingModel,
                        innerModel,
                        windowModel,
                        groupByModel,
                        outerModel,
                        distinctModel
                );
            }
        }
    }

    private void createSelectColumnsForWildcard0(
            QueryModel srcModel,
            boolean hasJoins,
            int wildcardPosition,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel windowModel,
            QueryModel groupByModel,
            QueryModel outerModel,
            QueryModel distinctModel
    ) throws SqlException {
        createSelectColumnsForWildcardFromColumnNames(
                srcModel,
                hasJoins,
                wildcardPosition,
                translatingModel,
                innerModel,
                windowModel,
                groupByModel,
                outerModel,
                distinctModel
        );
    }

    private void createSelectColumnsForWildcardFromColumnNames(
            QueryModel srcModel,
            boolean hasJoins,
            int wildcardPosition,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel windowModel,
            QueryModel groupByModel,
            QueryModel outerModel,
            QueryModel distinctModel
    ) throws SqlException {
        final ObjList<CharSequence> columnNames = srcModel.getBottomUpColumnNames();
        for (int j = 0, z = columnNames.size(); j < z; j++) {
            CharSequence name = columnNames.getQuick(j);
            // this is a check to see if column has to be added to wildcard list
            QueryColumn qc = srcModel.getAliasToColumnMap().get(name);
            if (qc.isIncludeIntoWildcard()) {
                CharSequence token;
                if (hasJoins) {
                    CharacterStoreEntry characterStoreEntry = characterStore.newEntry();
                    characterStoreEntry.put(srcModel.getName());
                    characterStoreEntry.put('.');
                    characterStoreEntry.put(name);
                    token = characterStoreEntry.toImmutable();
                } else {
                    token = name;
                }
                createSelectColumn(
                        name,
                        nextLiteral(token, wildcardPosition),
                        true,
                        null, // do not validate
                        translatingModel,
                        innerModel,
                        windowModel,
                        groupByModel,
                        outerModel,
                        distinctModel
                );
            }
        }
    }

    private int doReorderTables(QueryModel parent, IntList ordered) {
        tempCrossIndexes.clear();
        ordered.clear();
        this.orderingStack.clear();
        ObjList<QueryModel> joinModels = parent.getJoinModels();

        int cost = 0;

        for (int i = 0, n = joinModels.size(); i < n; i++) {
            QueryModel q = joinModels.getQuick(i);
            if (q.getJoinType() == QueryModel.JOIN_CROSS || q.getContext() == null || q.getContext().parents.size() == 0) {
                if (q.getDependencies().size() > 0) {
                    orderingStack.push(i);
                } else {
                    tempCrossIndexes.add(i);
                }
            } else {
                q.getContext().inCount = q.getContext().parents.size();
            }
        }

        while (orderingStack.notEmpty()) {
            //remove a node n from orderingStack
            int index = orderingStack.pop();

            ordered.add(index);

            QueryModel m = joinModels.getQuick(index);

            if (m.getJoinType() == QueryModel.JOIN_CROSS) {
                cost += 10;
            } else {
                cost += 5;
            }

            IntHashSet dependencies = m.getDependencies();

            //for each node m with an edge e from n to m do
            for (int i = 0, k = dependencies.size(); i < k; i++) {
                int depIndex = dependencies.get(i);
                JoinContext jc = joinModels.getQuick(depIndex).getContext();
                if (jc != null && --jc.inCount == 0) {
                    orderingStack.push(depIndex);
                }
            }
        }

        //Check to see if all edges are removed
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            QueryModel m = joinModels.getQuick(i);
            if (m.getContext() != null && m.getContext().inCount > 0) {
                return Integer.MAX_VALUE;
            }
        }

        // add pure crosses at end of ordered table list
        for (int i = 0, n = tempCrossIndexes.size(); i < n; i++) {
            ordered.add(tempCrossIndexes.getQuick(i));
        }

        return cost;
    }

    private ExpressionNode doReplaceLiteral(
            @Transient ExpressionNode node,
            QueryModel translatingModel,
            @Nullable QueryModel innerModel,
            QueryModel validatingModel,
            boolean windowCall
    ) throws SqlException {
        if (windowCall) {
            assert innerModel != null;
            ExpressionNode n = doReplaceLiteral0(node, translatingModel, null, validatingModel);
            LowerCaseCharSequenceObjHashMap<CharSequence> map = innerModel.getColumnNameToAliasMap();
            int index = map.keyIndex(n.token);
            if (index > -1) {
                // column is not referenced by inner model
                CharSequence alias = createColumnAlias(n.token, innerModel);
                innerModel.addBottomUpColumn(queryColumnPool.next().of(alias, n));
                // when alias is not the same as token, e.g. column aliases as "token" is already on the list
                // we have to create a new expression node that uses this alias
                if (alias != n.token) {
                    return nextLiteral(alias);
                } else {
                    return n;
                }
            } else {
                // column is already referenced
                return nextLiteral(map.valueAt(index), node.position);
            }
        }
        return doReplaceLiteral0(node, translatingModel, innerModel, validatingModel);
    }

    private ExpressionNode doReplaceLiteral0(
            ExpressionNode node,
            QueryModel translatingModel,
            @Nullable QueryModel innerModel,
            QueryModel validatingModel
    ) throws SqlException {

        final LowerCaseCharSequenceObjHashMap<CharSequence> map = translatingModel.getColumnNameToAliasMap();
        int index = map.keyIndex(node.token);
        final CharSequence alias;
        if (index > -1) {
            // there is a possibility that column references join table, but in a different way
            // for example. main column could be tab1.y and the "missing" one just "y"
            // which is the same thing.
            // To disambiguate this situation we need to go over all join tables and see if the
            // column matches any of join tables unambiguously.

            final int joinCount = validatingModel.getJoinModels().size();
            if (joinCount > 1) {
                boolean found = false;
                final StringSink sink = Misc.getThreadLocalSink();
                for (int i = 0; i < joinCount; i++) {
                    final QueryModel jm = validatingModel.getJoinModels().getQuick(i);
                    if (jm.getAliasToColumnMap().keyIndex(node.token) < 0) {
                        if (found) {
                            throw SqlException.ambiguousColumn(node.position, node.token);
                        }
                        if (jm.getAlias() != null) {
                            sink.put(jm.getAlias().token);
                        } else {
                            sink.put(jm.getTableName());
                        }

                        sink.put('.');
                        sink.put(node.token);

                        if ((index = map.keyIndex(sink)) < 0) {
                            found = true;
                        }
                    }
                }

                if (found) {
                    return nextLiteral(map.valueAtQuick(index), node.position);
                }
            }

            // this is the first time we see this column and must create alias
            alias = createColumnAlias(node, translatingModel);
            QueryColumn column = queryColumnPool.next().of(alias, node);
            // add column to both models
            addColumnToTranslatingModel(column, translatingModel, validatingModel);
            if (innerModel != null) {
                ExpressionNode innerToken = expressionNodePool.next().of(LITERAL, alias, node.precedence, node.position);
                QueryColumn innerColumn = queryColumnPool.next().of(alias, innerToken);
                innerModel.addBottomUpColumn(innerColumn);
            }
        } else {
            // It might be the case that we previously added the column to
            // the translating model, but not to the inner one.
            alias = map.valueAtQuick(index);
            if (innerModel != null && innerModel.getColumnNameToAliasMap().excludes(alias)) {
                innerModel.addBottomUpColumn(nextColumn(alias), true);
            }
        }
        return nextLiteral(alias, node.position);
    }

    private void doRewriteOrderByPositionForUnionModels(QueryModel model, QueryModel parent, QueryModel next) throws SqlException {
        final int columnCount = model.getBottomUpColumns().size();
        while (next != null) {
            if (next.getBottomUpColumns().size() != columnCount) {
                throw SqlException.$(next.getModelPosition(), "queries have different number of columns");
            }
            rewriteOrderByPosition(next);
            parent.setUnionModel(next);
            parent = next;
            next = next.getUnionModel();
        }
    }

    private void emitAggregatesAndLiterals(
            @Transient ExpressionNode node,
            QueryModel groupByModel,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel validatingModel,
            ObjList<ExpressionNode> groupByNodes,
            ObjList<CharSequence> groupByAliases
    ) throws SqlException {
        sqlNodeStack.clear();
        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.rhs != null) {
                    ExpressionNode n = replaceIfAggregateOrLiteral(node.rhs, groupByModel, translatingModel, innerModel, validatingModel, groupByNodes, groupByAliases);
                    if (node.rhs == n) {
                        sqlNodeStack.push(node.rhs);
                    } else {
                        node.rhs = n;
                    }
                }

                ExpressionNode n = replaceIfAggregateOrLiteral(node.lhs, groupByModel, translatingModel, innerModel, validatingModel, groupByNodes, groupByAliases);
                if (n == node.lhs) {
                    node = node.lhs;
                } else {
                    node.lhs = n;
                    node = null;
                }
            } else {
                node = sqlNodeStack.poll();
            }
        }
    }

    private void emitColumnLiteralsTopDown(ObjList<QueryColumn> columns, QueryModel target) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn qc = columns.getQuick(i);
            emitLiteralsTopDown(qc.getAst(), target);
            if (qc.isWindowColumn()) {
                final WindowColumn ac = (WindowColumn) qc;
                emitLiteralsTopDown(ac.getPartitionBy(), target);
                emitLiteralsTopDown(ac.getOrderBy(), target);
            }
        }
    }

    // This method will create CROSS join models in the "baseModel" for all unique cursor
    // function it finds on the node. The "translatingModel" is used to ensure uniqueness
    private boolean emitCursors(
            @Transient ExpressionNode node,
            QueryModel cursorModel,
            @Nullable QueryModel innerVirtualModel,
            QueryModel translatingModel,
            QueryModel baseModel,
            SqlExecutionContext sqlExecutionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        boolean replaced = false;
        sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {

                if (node.rhs != null) {
                    final ExpressionNode n = replaceIfCursor(
                            node.rhs,
                            cursorModel,
                            innerVirtualModel,
                            translatingModel,
                            baseModel,
                            sqlExecutionContext,
                            sqlParserCallback
                    );
                    if (node.rhs == n) {
                        sqlNodeStack.push(node.rhs);
                    } else {
                        node.rhs = n;
                        replaced = true;
                    }
                }

                final ExpressionNode n = replaceIfCursor(
                        node.lhs,
                        cursorModel,
                        innerVirtualModel,
                        translatingModel,
                        baseModel,
                        sqlExecutionContext,
                        sqlParserCallback
                );
                if (n == node.lhs) {
                    node = node.lhs;
                } else {
                    node.lhs = n;
                    node = null;
                    replaced = true;
                }
            } else {
                node = sqlNodeStack.poll();
            }
        }
        return replaced;
    }

    //warning: this method replaces literal with aliases (changes node)
    private void emitLiterals(
            @Transient ExpressionNode node,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel validatingModel,
            boolean windowCall
    ) throws SqlException {
        sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.paramCount < 3) {
                    if (node.rhs != null) {
                        ExpressionNode n = replaceLiteral(node.rhs, translatingModel, innerModel, validatingModel, windowCall);
                        if (node.rhs == n) {
                            sqlNodeStack.push(node.rhs);
                        } else {
                            node.rhs = n;
                        }
                    }

                    ExpressionNode n = replaceLiteral(node.lhs, translatingModel, innerModel, validatingModel, windowCall);
                    if (n == node.lhs) {
                        node = node.lhs;
                    } else {
                        node.lhs = n;
                        node = null;
                    }
                } else {
                    for (int i = 1, k = node.paramCount; i < k; i++) {
                        ExpressionNode e = node.args.getQuick(i);
                        ExpressionNode n = replaceLiteral(e, translatingModel, innerModel, validatingModel, windowCall);
                        if (e == n) {
                            sqlNodeStack.push(e);
                        } else {
                            node.args.setQuick(i, n);
                        }
                    }

                    ExpressionNode e = node.args.getQuick(0);
                    ExpressionNode n = replaceLiteral(e, translatingModel, innerModel, validatingModel, windowCall);
                    if (e == n) {
                        node = e;
                    } else {
                        node.args.setQuick(0, n);
                        node = null;
                    }
                }
            } else {
                node = sqlNodeStack.poll();
            }
        }
    }

    private void emitLiteralsTopDown(@Transient ExpressionNode node, QueryModel model) {
        sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        addTopDownColumn(node, model);

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.paramCount < 3) {
                    if (node.rhs != null) {
                        addTopDownColumn(node.rhs, model);
                        sqlNodeStack.push(node.rhs);
                    }

                    if (node.lhs != null) {
                        addTopDownColumn(node.lhs, model);
                    }
                    node = node.lhs;
                } else {
                    for (int i = 1, k = node.paramCount; i < k; i++) {
                        ExpressionNode e = node.args.getQuick(i);
                        addTopDownColumn(e, model);
                        sqlNodeStack.push(e);
                    }

                    final ExpressionNode e = node.args.getQuick(0);
                    addTopDownColumn(e, model);
                    node = e;
                }
            } else {
                node = sqlNodeStack.poll();
            }
        }
    }

    private void emitLiteralsTopDown(ObjList<ExpressionNode> list, QueryModel nested) {
        for (int i = 0, m = list.size(); i < m; i++) {
            emitLiteralsTopDown(list.getQuick(i), nested);
        }
    }

    private QueryColumn ensureAliasUniqueness(QueryModel model, QueryColumn qc) {
        CharSequence alias = createColumnAlias(qc.getAlias(), model);
        if (alias != qc.getAlias()) {
            qc = queryColumnPool.next().of(alias, qc.getAst());
        }
        return qc;
    }

    private void enumerateColumns(QueryModel model, TableRecordMetadata metadata) throws SqlException {
        model.setMetadataVersion(metadata.getMetadataVersion());
        model.setTableId(metadata.getTableId());
        copyColumnsFromMetadata(model, metadata, false);
        if (model.isUpdate()) {
            copyColumnTypesFromMetadata(model, metadata);
        }
    }

    private void enumerateTableColumns(QueryModel model, SqlExecutionContext executionContext, SqlParserCallback sqlParserCallback) throws SqlException {
        final ObjList<QueryModel> jm = model.getJoinModels();

        // we have plain tables and possibly joins
        // deal with _this_ model first, it will always be the first element in join model list
        final ExpressionNode tableNameExpr = model.getTableNameExpr();
        if (tableNameExpr != null || model.getSelectModelType() == QueryModel.SELECT_MODEL_SHOW) {
            if (model.getSelectModelType() == QueryModel.SELECT_MODEL_SHOW || (tableNameExpr != null && tableNameExpr.type == ExpressionNode.FUNCTION)) {
                parseFunctionAndEnumerateColumns(model, executionContext, sqlParserCallback);
            } else {
                openReaderAndEnumerateColumns(executionContext, model, sqlParserCallback);
            }
        } else {
            final QueryModel nested = model.getNestedModel();
            if (nested != null) {
                enumerateTableColumns(nested, executionContext, sqlParserCallback);
                if (model.isUpdate()) {
                    model.copyUpdateTableMetadata(nested);
                }
            }
        }
        for (int i = 1, n = jm.size(); i < n; i++) {
            enumerateTableColumns(jm.getQuick(i), executionContext, sqlParserCallback);
        }

        if (model.getUnionModel() != null) {
            enumerateTableColumns(model.getUnionModel(), executionContext, sqlParserCallback);
        }
    }

    private void eraseColumnPrefixInWhereClauses(QueryModel model) throws SqlException {

        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            QueryModel m = joinModels.getQuick(i);
            ExpressionNode where = m.getWhereClause();

            // join models can have "where" clause
            // although in context of SQL where is executed after joins, this model
            // always localises "where" to a single table and therefore "where" is
            // applied before join. Please see post-join-where for filters that
            // executed in line with standard SQL behaviour.

            if (where != null) {
                if (where.type == LITERAL) {
                    m.setWhereClause(columnPrefixEraser.rewrite(where));
                } else {
                    traversalAlgo.traverse(where, columnPrefixEraser);
                }
            }

            QueryModel nested = m.getNestedModel();
            if (nested != null) {
                eraseColumnPrefixInWhereClauses(nested);
            }

            nested = m.getUnionModel();
            if (nested != null) {
                eraseColumnPrefixInWhereClauses(nested);
            }
        }
    }

    private long evalNonNegativeLongConstantOrDie(ExpressionNode expr, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (expr != null) {
            final Function loFunc = functionParser.parseFunction(expr, EmptyRecordMetadata.INSTANCE, sqlExecutionContext);
            if (!loFunc.isConstant()) {
                Misc.free(loFunc);
                throw SqlException.$(expr.position, "constant expression expected");
            }

            try {
                long value;
                if (!(loFunc instanceof CharConstant)) {
                    value = loFunc.getLong(null);
                } else {
                    long tmp = (byte) (loFunc.getChar(null) - '0');
                    value = tmp > -1 && tmp < 10 ? tmp : Numbers.LONG_NaN;
                }

                if (value < 0) {
                    throw SqlException.$(expr.position, "non-negative integer expression expected");
                }
                return value;
            } catch (UnsupportedOperationException | ImplicitCastException e) {
                throw SqlException.$(expr.position, "integer expression expected");
            } finally {
                Misc.free(loFunc);
            }
        }
        return Long.MAX_VALUE;
    }

    private CharSequence findQueryColumnByAst(ObjList<QueryColumn> bottomUpColumns, ExpressionNode node) {
        for (int i = 0, max = bottomUpColumns.size(); i < max; i++) {
            QueryColumn qc = bottomUpColumns.getQuick(i);
            if (ExpressionNode.compareNodesExact(qc.getAst(), node)) {
                return qc.getAlias();
            }
        }
        return null;

    }

    private CharSequence findTimestamp(QueryModel model) {
        if (model != null) {
            CharSequence timestamp;
            if (model.getTimestamp() != null) {
                timestamp = model.getTimestamp().token;
            } else {
                timestamp = findTimestamp(model.getNestedModel());
            }

            if (timestamp != null) {
                return model.getColumnNameToAliasMap().get(timestamp);
            }
        }
        return null;
    }

    private Function getLoFunction(ExpressionNode limit, SqlExecutionContext executionContext) throws SqlException {
        final Function func = functionParser.parseFunction(limit, EmptyRecordMetadata.INSTANCE, executionContext);
        final int type = func.getType();
        if (limitTypes.excludes(type)) {
            return null;
        }
        return func;
    }

    private ObjList<ExpressionNode> getOrderByAdvice(QueryModel model, int orderByMnemonic) {
        orderByAdvice.clear();
        ObjList<ExpressionNode> orderBy = model.getOrderBy();
        int len = orderBy.size();
        if (len == 0) {
            // propagate advice in case nested model can implement it efficiently (e.g. with backward scan)
            if (orderByMnemonic == OrderByMnemonic.ORDER_BY_INVARIANT && model.getOrderByAdvice().size() > 0) {
                orderBy = model.getOrderByAdvice();
                len = orderBy.size();
            } else {
                return orderByAdvice;
            }
        }

        LowerCaseCharSequenceObjHashMap<QueryColumn> map = model.getAliasToColumnMap();
        for (int i = 0; i < len; i++) {
            ExpressionNode orderByNode = orderBy.getQuick(i);
            QueryColumn queryColumn = map.get(orderByNode.token);
            if (queryColumn == null) { // order by can't be pushed down
                orderByAdvice.clear();
                break;
            }

            if (queryColumn.getAst().type == LITERAL) {
                orderByAdvice.add(queryColumn.getAst());
            } else {
                orderByAdvice.clear();
                break;
            }
        }
        return orderByAdvice;
    }

    private IntList getOrderByAdviceDirection(QueryModel model, int orderByMnemonic) {
        IntList orderByDirection = model.getOrderByDirection();
        if (model.getOrderBy().size() == 0
                && orderByMnemonic == OrderByMnemonic.ORDER_BY_INVARIANT) {
            return model.getOrderByDirectionAdvice();
        }
        return orderByDirection;
    }

    private QueryColumn getQueryColumn(QueryModel model, CharSequence columnName, int dot) {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        QueryColumn column = null;
        if (dot == -1) {
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                QueryColumn qc = joinModels.getQuick(i).getAliasToColumnMap().get(columnName);
                if (qc == null) {
                    continue;
                }
                if (column != null) {
                    return null;//more than one column match
                }
                column = qc;
            }
        } else {
            int index = model.getModelAliasIndex(columnName, 0, dot);
            if (index == -1) {
                return null;
            }
            return joinModels.getQuick(index).getAliasToColumnMap().get(columnName, dot + 1, columnName.length());
        }
        return column;
    }

    private CharSequence getTranslatedColumnAlias(QueryModel model, QueryModel stopModel, CharSequence token) {
        if (model == stopModel) {
            return token;
        }

        CharSequence nestedAlias = getTranslatedColumnAlias(model.getNestedModel(), stopModel, token);
        if (nestedAlias != null) {
            CharSequence alias = model.getColumnNameToAliasMap().get(nestedAlias);
            if (alias == null) {
                tempQueryModel = model;
                tempColumnAlias = nestedAlias;
            }
            return alias;
        } else {
            return null;
        }
    }

    private boolean hasAggregateQueryColumn(QueryModel model) {
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();

        for (int i = 0, k = columns.size(); i < k; i++) {
            QueryColumn qc = columns.getQuick(i);
            if (qc.getAst().type != LITERAL) {
                if (qc.getAst().type == ExpressionNode.FUNCTION) {
                    if (functionParser.getFunctionFactoryCache().isGroupBy(qc.getAst().token)) {
                        return true;
                    } else if (functionParser.getFunctionFactoryCache().isCursor(qc.getAst().token)) {
                        continue;
                    }
                }

                if (checkForAggregates(qc.getAst())) {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean hasAggregates(ExpressionNode node) {
        sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                switch (node.type) {
                    case LITERAL:
                        node = null;
                        continue;
                    case ExpressionNode.FUNCTION:
                        if (functionParser.getFunctionFactoryCache().isGroupBy(node.token)) {
                            return true;
                        }
                        break;
                    default:
                        if (node.rhs != null) {
                            sqlNodeStack.push(node.rhs);
                        }
                        break;
                }

                node = node.lhs;
            } else {
                node = sqlNodeStack.poll();
            }
        }
        return false;
    }

    private void homogenizeCrossJoins(QueryModel parent) {
        ObjList<QueryModel> joinModels = parent.getJoinModels();
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            QueryModel m = joinModels.getQuick(i);
            JoinContext c = m.getContext();

            if (m.getJoinType() == QueryModel.JOIN_CROSS) {
                if (c != null && c.parents.size() > 0) {
                    m.setJoinType(QueryModel.JOIN_INNER);
                }
            } else if (m.getJoinType() == QueryModel.JOIN_OUTER &&
                    c == null &&
                    m.getJoinCriteria() != null) {
                m.setJoinType(QueryModel.JOIN_CROSS_LEFT);
            } else if (m.getJoinType() != QueryModel.JOIN_ASOF &&
                    m.getJoinType() != QueryModel.JOIN_SPLICE &&
                    (c == null || c.parents.size() == 0)
            ) {
                m.setJoinType(QueryModel.JOIN_CROSS);
            }
        }
    }

    private boolean isEffectivelyConstantExpression(ExpressionNode node) {
        sqlNodeStack.clear();
        while (node != null) {
            if (node.type != ExpressionNode.OPERATION
                    && node.type != ExpressionNode.CONSTANT
                    && !(node.type == ExpressionNode.FUNCTION && functionParser.getFunctionFactoryCache().isRuntimeConstant(node.token))) {
                return false;
            }

            if (node.lhs != null) {
                sqlNodeStack.push(node.lhs);
            }

            if (node.rhs != null) {
                node = node.rhs;
            } else {
                if (!sqlNodeStack.isEmpty()) {
                    node = this.sqlNodeStack.poll();
                } else {
                    node = null;
                }
            }
        }

        return true;
    }

    private boolean isIntegerConstant(ExpressionNode n) {
        if (n.type != CONSTANT) {
            return false;
        }

        try {
            Numbers.parseLong(n.token);
            return true;
        } catch (NumericException ne) {
            return false;
        }
    }

    private boolean isSimpleIntegerColumn(ExpressionNode column, QueryModel model) {
        return checkSimpleIntegerColumn(column, model) != null;
    }

    private ExpressionNode makeJoinAlias() {
        CharacterStoreEntry characterStoreEntry = characterStore.newEntry();
        characterStoreEntry.put(QueryModel.SUB_QUERY_ALIAS_PREFIX).put(defaultAliasCount++);
        return nextLiteral(characterStoreEntry.toImmutable());
    }

    private ExpressionNode makeModelAlias(CharSequence modelAlias, ExpressionNode node) {
        CharacterStoreEntry characterStoreEntry = characterStore.newEntry();
        characterStoreEntry.put(modelAlias).put('.').put(node.token);
        return nextLiteral(characterStoreEntry.toImmutable(), node.position);
    }

    private ExpressionNode makeOperation(CharSequence token, ExpressionNode lhs, ExpressionNode rhs) {
        ExpressionNode expr = expressionNodePool.next().of(ExpressionNode.OPERATION, token, 0, 0);
        expr.paramCount = 2;
        expr.lhs = lhs;
        expr.rhs = rhs;
        return expr;
    }

    private JoinContext mergeContexts(QueryModel parent, JoinContext a, JoinContext b) {
        assert a.slaveIndex == b.slaveIndex;

        deletedContexts.clear();
        JoinContext r = contextPool.next();
        // check if we are merging a.x = b.x to a.y = b.y
        // or a.x = b.x to a.x = b.y, e.g. one of columns in the same table
        for (int i = 0, n = b.aNames.size(); i < n; i++) {

            CharSequence ban = b.aNames.getQuick(i);
            int bai = b.aIndexes.getQuick(i);
            ExpressionNode bao = b.aNodes.getQuick(i);

            CharSequence bbn = b.bNames.getQuick(i);
            int bbi = b.bIndexes.getQuick(i);
            ExpressionNode bbo = b.bNodes.getQuick(i);

            for (int k = 0, z = a.aNames.size(); k < z; k++) {
                final CharSequence aan = a.aNames.getQuick(k);
                final int aai = a.aIndexes.getQuick(k);
                final ExpressionNode aao = a.aNodes.getQuick(k);
                final CharSequence abn = a.bNames.getQuick(k);
                final int abi = a.bIndexes.getQuick(k);
                final ExpressionNode abo = a.bNodes.getQuick(k);

                if (aai == bai && Chars.equals(aan, ban)) {
                    // a.x = ?.x
                    //  |     ?
                    // a.x = ?.y
                    addFilterOrEmitJoin(parent, k, abi, abn, abo, bbi, bbn, bbo);
                    break;
                } else if (abi == bai && Chars.equals(abn, ban)) {
                    // a.y = b.x
                    //    /
                    // b.x = a.x
                    addFilterOrEmitJoin(parent, k, aai, aan, aao, bbi, bbn, bbo);
                    break;
                } else if (aai == bbi && Chars.equals(aan, bbn)) {
                    // a.x = b.x
                    //     \
                    // b.y = a.x
                    addFilterOrEmitJoin(parent, k, abi, abn, abo, bai, ban, bao);
                    break;
                } else if (abi == bbi && Chars.equals(abn, bbn)) {
                    // a.x = b.x
                    //        |
                    // a.y = b.x
                    addFilterOrEmitJoin(parent, k, aai, aan, aao, bai, ban, bao);
                    break;
                }
            }
            r.aIndexes.add(bai);
            r.aNames.add(ban);
            r.aNodes.add(bao);
            r.bIndexes.add(bbi);
            r.bNames.add(bbn);
            r.bNodes.add(bbo);
            int max = Math.max(bai, bbi);
            int min = Math.min(bai, bbi);
            r.slaveIndex = max;
            r.parents.add(min);
            linkDependencies(parent, min, max);
        }

        // add remaining a nodes
        for (int i = 0, n = a.aNames.size(); i < n; i++) {
            int aai, abi, min, max;

            aai = a.aIndexes.getQuick(i);
            abi = a.bIndexes.getQuick(i);

            if (aai < abi) {
                min = aai;
                max = abi;
            } else {
                min = abi;
                max = aai;
            }

            if (deletedContexts.contains(i)) {
                if (r.parents.excludes(min)) {
                    unlinkDependencies(parent, min, max);
                }
            } else {
                r.aNames.add(a.aNames.getQuick(i));
                r.bNames.add(a.bNames.getQuick(i));
                r.aIndexes.add(aai);
                r.bIndexes.add(abi);
                r.aNodes.add(a.aNodes.getQuick(i));
                r.bNodes.add(a.bNodes.getQuick(i));

                r.parents.add(min);
                r.slaveIndex = max;
                linkDependencies(parent, min, max);
            }
        }
        return r;
    }

    private void mergeInnerVirtualModel(QueryModel innerModel, QueryModel groupByModel) {
        for (int i = 0, n = innerModel.getBottomUpColumns().size(); i < n; i++) {
            QueryColumn qc = innerModel.getBottomUpColumns().getQuick(i);
            groupByModel.mergeInnerColumn(qc);
        }
    }

    private JoinContext moveClauses(QueryModel parent, JoinContext from, JoinContext to, IntList positions) {
        int p = 0;
        int m = positions.size();

        JoinContext result = contextPool.next();
        result.slaveIndex = from.slaveIndex;

        for (int i = 0, n = from.aIndexes.size(); i < n; i++) {
            // logically those clauses we move away from "from" context
            // should no longer exist in "from", but instead of implementing
            // "delete" function, which would be manipulating underlying array
            // on every invocation, we copy retained clauses to new context,
            // which is "result".
            // hence, whenever exists in "positions" we copy clause to "to"
            // otherwise copy to "result"
            JoinContext t = p < m && i == positions.getQuick(p) ? to : result;
            int ai = from.aIndexes.getQuick(i);
            int bi = from.bIndexes.getQuick(i);
            t.aIndexes.add(ai);
            t.aNames.add(from.aNames.getQuick(i));
            t.aNodes.add(from.aNodes.getQuick(i));
            t.bIndexes.add(bi);
            t.bNames.add(from.bNames.getQuick(i));
            t.bNodes.add(from.bNodes.getQuick(i));

            // either ai or bi is definitely belongs to this context
            if (ai != t.slaveIndex) {
                t.parents.add(ai);
                linkDependencies(parent, ai, bi);
            } else {
                t.parents.add(bi);
                linkDependencies(parent, bi, ai);
            }
        }

        return result;
    }

    private QueryModel moveOrderByFunctionsIntoOuterSelect(QueryModel model) {
        // at this point order by should be on the nested model of this model :)
        QueryModel unionModel = model.getUnionModel();
        if (unionModel != null) {
            model.setUnionModel(moveOrderByFunctionsIntoOuterSelect(unionModel));
        }

        QueryModel nested = model.getNestedModel();
        if (nested != null) {
            for (int jm = 0, jmn = nested.getJoinModels().size(); jm < jmn; jm++) {
                QueryModel joinModel = nested.getJoinModels().getQuick(jm);
                if (joinModel != nested && joinModel.getNestedModel() != null) {
                    joinModel.setNestedModel(moveOrderByFunctionsIntoOuterSelect(joinModel.getNestedModel()));
                }
            }

            QueryModel nestedNested = nested.getNestedModel();
            if (nestedNested != null) {
                nested.setNestedModel(moveOrderByFunctionsIntoOuterSelect(nestedNested));
            }

            final ObjList<ExpressionNode> orderBy = nested.getOrderBy();
            final int n = orderBy.size();
            final int columnCount = model.getBottomUpColumns().size();
            boolean moved = false;
            for (int i = 0; i < n; i++) {
                ExpressionNode node = orderBy.getQuick(i);
                if (node.type == FUNCTION || node.type == OPERATION) {
                    CharSequence alias = findQueryColumnByAst(model.getBottomUpColumns(), node);
                    if (alias == null) {
                        // add this function to bottom-up columns and replace this expression with index
                        alias = SqlUtil.createColumnAlias(characterStore, node.token, Chars.indexOf(node.token, '.'), model.getAliasToColumnMap(), true);
                        QueryColumn qc = queryColumnPool.next().of(
                                alias,
                                node,
                                false
                        );
                        model.getAliasToColumnMap().put(alias, qc);
                        model.getBottomUpColumns().add(qc);
                    }
                    orderBy.setQuick(i, nextLiteral(alias));
                    moved = true;
                }
            }

            if (moved) {
                // these are early stages of model processing
                // to create outer query, we will need a pair of models
                QueryModel _model = queryModelPool.next();
                QueryModel _nested = queryModelPool.next();

                // nest them
                _model.setNestedModel(_nested);
                _nested.setNestedModel(model);

                // then create columns on the outermost model
                for (int i = 0; i < columnCount; i++) {
                    QueryColumn qcFrom = model.getBottomUpColumns().getQuick(i);
                    QueryColumn qcTo = queryColumnPool.next().of(
                            qcFrom.getAlias(),
                            nextLiteral(qcFrom.getAlias())
                    );
                    _model.getBottomUpColumns().add(qcTo);
                }

                return _model;
            }
        }
        return model;
    }

    private void moveTimestampToChooseModel(QueryModel model) {
        QueryModel nested = model.getNestedModel();
        if (nested != null) {
            moveTimestampToChooseModel(nested);
            ExpressionNode timestamp = nested.getTimestamp();
            if (
                    timestamp != null
                            && nested.getSelectModelType() == QueryModel.SELECT_MODEL_NONE
                            && nested.getTableName() == null
                            && nested.getTableNameFunction() == null
                            && nested.getLatestBy().size() == 0
            ) {
                model.setTimestamp(timestamp);
                model.setExplicitTimestamp(nested.isExplicitTimestamp());
                nested.setTimestamp(null);
                nested.setExplicitTimestamp(false);
            }
        }

        final ObjList<QueryModel> joinModels = model.getJoinModels();
        if (joinModels.size() > 1) {
            for (int i = 1, n = joinModels.size(); i < n; i++) {
                moveTimestampToChooseModel(joinModels.getQuick(i));
            }
        }

        nested = model.getUnionModel();
        if (nested != null) {
            moveTimestampToChooseModel(nested);
        }
    }

    private void moveWhereInsideSubQueries(QueryModel model) throws SqlException {
        if (model.getSelectModelType() != QueryModel.SELECT_MODEL_DISTINCT &&
                // in theory, we could push down predicates as long as they align with ALL partition by clauses and remove whole partition(s)
                model.getSelectModelType() != QueryModel.SELECT_MODEL_WINDOW) {
            model.getParsedWhere().clear();
            final ObjList<ExpressionNode> nodes = model.parseWhereClause();
            model.setWhereClause(null);

            final int n = nodes.size();
            if (n > 0) {
                for (int i = 0; i < n; i++) {
                    final ExpressionNode node = nodes.getQuick(i);
                    // collect table references this where clause element
                    literalCollectorAIndexes.clear();
                    literalCollectorANames.clear();
                    literalCollector.withModel(model);
                    literalCollector.resetCounts();
                    traversalAlgo.traverse(node, literalCollector.lhs());

                    tempList.clear();
                    for (int j = 0; j < literalCollectorAIndexes.size(); j++) {
                        int tableExpressionReference = literalCollectorAIndexes.get(j);
                        int position = tempList.binarySearchUniqueList(tableExpressionReference);
                        if (position < 0) {
                            tempList.insert(-(position + 1), tableExpressionReference);
                        }
                    }

                    int distinctIndexes = tempList.size();

                    // at this point we must not have constant conditions in where clause
                    // this could be either referencing constant of a sub-query
                    if (literalCollectorAIndexes.size() == 0) {
                        // keep condition with this model
                        addWhereNode(model, node);
                        continue;
                    } else if (distinctIndexes > 1) {
                        int greatest = tempList.get(distinctIndexes - 1);
                        final QueryModel m = model.getJoinModels().get(greatest);
                        m.setPostJoinWhereClause(concatFilters(m.getPostJoinWhereClause(), nodes.getQuick(i)));
                        continue;
                    }

                    // by now all where clause must reference single table only and all column references have to be valid
                    // they would have been rewritten and validated as join analysis stage
                    final int tableIndex = literalCollectorAIndexes.get(0);
                    final QueryModel parent = model.getJoinModels().getQuick(tableIndex);

                    // Do not move where clauses inside outer join models because that'd change result
                    int joinType = parent.getJoinType();
                    if (tableIndex > 0
                            && (joinBarriers.contains(joinType))
                    ) {
                        QueryModel joinModel = model.getJoinModels().getQuick(tableIndex);
                        joinModel.setPostJoinWhereClause(concatFilters(joinModel.getPostJoinWhereClause(), node));
                        continue;
                    }

                    final QueryModel nested = parent.getNestedModel();
                    if (nested == null
                            || nested.getLatestBy().size() > 0
                            || nested.getLimitLo() != null
                            || nested.getLimitHi() != null
                            || nested.getUnionModel() != null
                            || (nested.getSampleBy() != null && !canPushToSampleBy(nested, literalCollectorANames))
                    ) {
                        // there is no nested model for this table, keep where clause element with this model
                        addWhereNode(parent, node);
                    } else {
                        // now that we have identified sub-query we have to rewrite our where clause
                        // to potentially replace all column references with actual literals used inside
                        // sub-query, for example:
                        // (select a x, b from T) where x = 10
                        // we can't move "x" inside sub-query because it is not a field.
                        // Instead, we have to translate "x" to actual column expression, which is "a":
                        // select a x, b from T where a = 10

                        // because we are rewriting SqlNode in-place we need to make sure that
                        // none of expression literals reference non-literals in nested query, e.g.
                        // (select a+b x from T) where x > 10
                        // does not warrant inlining of "x > 10" because "x" is not a column
                        //
                        // at this step we would throw exception if one of our literals hits non-literal
                        // in sub-query

                        try {
                            traversalAlgo.traverse(node, literalCheckingVisitor.of(parent.getAliasToColumnMap()));

                            // go ahead and rewrite expression
                            traversalAlgo.traverse(node, literalRewritingVisitor.of(parent.getAliasToColumnNameMap()));

                            // whenever nested model has explicitly defined columns it must also
                            // have its own nested model, where we assign new "where" clauses
                            addWhereNode(nested, node);
                            // we do not have to deal with "union" models here
                            // because "where" clause is made to apply to the result of the union
                        } catch (NonLiteralException ignore) {
                            // keep node where it is
                            addWhereNode(parent, node);
                        }
                    }
                }
                model.getParsedWhere().clear();
            }
        }

        QueryModel nested = model.getNestedModel();
        if (nested != null) {
            moveWhereInsideSubQueries(nested);
        }

        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, m = joinModels.size(); i < m; i++) {
            nested = joinModels.getQuick(i);
            if (nested != model) {
                moveWhereInsideSubQueries(nested);
            }
        }

        nested = model.getUnionModel();
        if (nested != null) {
            moveWhereInsideSubQueries(nested);
        }
    }

    private QueryColumn nextColumn(CharSequence name) {
        return SqlUtil.nextColumn(queryColumnPool, expressionNodePool, name, name);
    }

    private QueryColumn nextColumn(CharSequence alias, CharSequence column) {
        return SqlUtil.nextColumn(queryColumnPool, expressionNodePool, alias, column);
    }

    private ExpressionNode nextLiteral(CharSequence token, int position) {
        return SqlUtil.nextLiteral(expressionNodePool, token, position);
    }

    private ExpressionNode nextLiteral(CharSequence token) {
        return nextLiteral(token, 0);
    }

    private void openReaderAndEnumerateColumns(
            SqlExecutionContext executionContext,
            QueryModel model,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        final ExpressionNode tableNameExpr = model.getTableNameExpr();

        // table name must not contain quotes by now
        final CharSequence tableName = tableNameExpr.token;
        final int tableNamePosition = tableNameExpr.position;

        int lo = 0;
        int hi = tableName.length();
        if (Chars.startsWith(tableName, QueryModel.NO_ROWID_MARKER)) {
            lo += QueryModel.NO_ROWID_MARKER.length();
        }

        if (lo == hi) {
            throw SqlException.$(tableNamePosition, "come on, where is table name?");
        }

        final TableToken tableToken = executionContext.getTableTokenIfExists(tableName, lo, hi);
        int status = executionContext.getTableStatus(path, tableToken);

        if (status == TableUtils.TABLE_DOES_NOT_EXIST) {
            try {
                model.getTableNameExpr().type = ExpressionNode.FUNCTION;
                parseFunctionAndEnumerateColumns(model, executionContext, sqlParserCallback);
                return;
            } catch (SqlException e) {
                throw SqlException.tableDoesNotExist(tableNamePosition, tableName);
            }
        }

        if (status == TableUtils.TABLE_RESERVED) {
            throw SqlException.$(tableNamePosition, "table directory is of unknown format [table=").put(tableName).put(']');
        }

        if (model.isUpdate()) {
            assert lo == 0;
            try {
                try (TableRecordMetadata metadata = executionContext.getMetadataForWrite(tableToken, model.getMetadataVersion())) {
                    enumerateColumns(model, metadata);
                }
            } catch (CairoException e) {
                throw SqlException.position(tableNamePosition).put(e);
            }
        } else {
            try (TableReader reader = executionContext.getReader(tableToken)) {
                enumerateColumns(model, reader.getMetadata());
            } catch (EntryLockedException e) {
                throw SqlException.position(tableNamePosition).put("table is locked: ").put(tableToken.getTableName());
            } catch (CairoException e) {
                throw SqlException.position(tableNamePosition).put(e);
            }
        }
    }

    private ExpressionNode optimiseBooleanNot(final ExpressionNode node, boolean reverse) {
        if (node.token != null) {
            switch (notOps.get(node.token)) {
                case NOT_OP_NOT:
                    if (reverse) {
                        return optimiseBooleanNot(node.rhs, false);
                    } else {
                        switch (node.rhs.type) {
                            case LITERAL:
                            case ExpressionNode.CONSTANT:
                                break;
                            default:
                                return optimiseBooleanNot(node.rhs, true);
                        }
                    }
                    break;
                case NOT_OP_AND:
                    if (reverse) {
                        node.token = "or";
                    }
                    node.lhs = optimiseBooleanNot(node.lhs, reverse);
                    node.rhs = optimiseBooleanNot(node.rhs, reverse);
                    break;
                case NOT_OP_OR:
                    if (reverse) {
                        node.token = "and";
                    }
                    node.lhs = optimiseBooleanNot(node.lhs, reverse);
                    node.rhs = optimiseBooleanNot(node.rhs, reverse);
                    break;
                case NOT_OP_GREATER:
                    if (reverse) {
                        node.token = "<=";
                    }
                    break;
                case NOT_OP_GREATER_EQ:
                    if (reverse) {
                        node.token = "<";
                    }
                    break;
                case NOT_OP_LESS:
                    if (reverse) {
                        node.token = ">=";
                    }
                    break;
                case NOT_OP_LESS_EQ:
                    if (reverse) {
                        node.token = ">";
                    }
                    break;
                case NOT_OP_EQUAL:
                    if (reverse) {
                        node.token = "!=";
                    }
                    break;
                case NOT_OP_NOT_EQ:
                    if (reverse) {
                        node.token = "=";
                    } else {
                        node.token = "!=";
                    }
                    break;
                default:
                    if (reverse) {
                        ExpressionNode n = expressionNodePool.next();
                        n.token = "not";
                        n.paramCount = 1;
                        n.rhs = node;
                        n.type = ExpressionNode.OPERATION;
                        return n;
                    }
                    break;
            }
        }
        return node;
    }

    private void optimiseBooleanNot(QueryModel model) {
        ExpressionNode where = model.getWhereClause();
        if (where != null) {
            model.setWhereClause(optimiseBooleanNot(where, false));
        }

        if (model.getNestedModel() != null) {
            optimiseBooleanNot(model.getNestedModel());
        }

        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            optimiseBooleanNot(joinModels.getQuick(i));
        }

        if (model.getUnionModel() != null) {
            optimiseBooleanNot(model.getNestedModel());
        }
    }

    private void optimiseExpressionModels(
            QueryModel model,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        ObjList<ExpressionNode> expressionModels = model.getExpressionModels();
        final int n = expressionModels.size();
        if (n > 0) {
            for (int i = 0; i < n; i++) {
                final ExpressionNode node = expressionModels.getQuick(i);
                // for expression models that have been converted to
                // the joins, the query model will be set to null.
                if (node.queryModel != null) {
                    QueryModel optimised = optimise(node.queryModel, executionContext, sqlParserCallback);
                    if (optimised != node.queryModel) {
                        node.queryModel = optimised;
                    }
                }
            }
        }

        if (model.getNestedModel() != null) {
            optimiseExpressionModels(model.getNestedModel(), executionContext, sqlParserCallback);
        }

        final ObjList<QueryModel> joinModels = model.getJoinModels();
        final int m = joinModels.size();
        // as usual, we already optimised self (index=0), now optimised others
        if (m > 1) {
            for (int i = 1; i < m; i++) {
                optimiseExpressionModels(joinModels.getQuick(i), executionContext, sqlParserCallback);
            }
        }

        // call out to union models
        if (model.getUnionModel() != null) {
            optimiseExpressionModels(model.getUnionModel(), executionContext, sqlParserCallback);
        }
    }

    private void optimiseJoins(QueryModel model) throws SqlException {
        ObjList<QueryModel> joinModels = model.getJoinModels();

        int n = joinModels.size();
        if (n > 1) {
            emittedJoinClauses = joinClausesSwap1;
            emittedJoinClauses.clear();

            // for sake of clarity, "model" model is the first in the list of
            // joinModels, e.g. joinModels.get(0) == model
            // only "model" model is allowed to have "where" clause,
            // so we can assume that "where" clauses of joinModel elements are all null (except for element 0).
            // in case one of joinModels is suburb, its entire query model will be set as
            // nestedModel, e.g. "where" clause is still null there as well

            ExpressionNode where = model.getWhereClause();

            // clear where clause of model so that
            // optimiser can assign there correct nodes

            model.setWhereClause(null);
            processJoinConditions(model, where, false, model, -1);

            for (int i = 1; i < n; i++) {
                processJoinConditions(model, joinModels.getQuick(i).getJoinCriteria(), true, joinModels.getQuick(i), i);
            }

            processEmittedJoinClauses(model);
            createImpliedDependencies(model);
            homogenizeCrossJoins(model);
            reorderTables(model);
            assignFilters(model);
            alignJoinClauses(model);
            addTransitiveFilters(model);
        }

        for (int i = 0; i < n; i++) {
            QueryModel m = model.getJoinModels().getQuick(i).getNestedModel();
            if (m != null) {
                optimiseJoins(m);
            }

            m = model.getJoinModels().getQuick(i).getUnionModel();
            if (m != null) {
                clearForUnionModelInJoin();
                optimiseJoins(m);
            }
        }
    }

    // removes redundant order by clauses from sub-queries (only those that don't force materialization of other order by clauses )
    private void optimiseOrderBy(QueryModel model, int topLevelOrderByMnemonic) {
        ObjList<QueryColumn> columns = model.getBottomUpColumns();
        int orderByMnemonic;
        int n = columns.size();

        //limit x,y forces order materialization; we can't push order by past it and need to discover actual nested ordering
        if (model.getLimitLo() != null) {
            topLevelOrderByMnemonic = OrderByMnemonic.ORDER_BY_UNKNOWN;
        }
        //if model has explicit timestamp then we should detect and preserve actual order because it might be used for asof/lt/splice join
        if (model.getTimestamp() != null) {
            topLevelOrderByMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
        }

        // keep order by on model with window functions to speed up query (especially when it matches window order by)
        if (model.getSelectModelType() == QueryModel.SELECT_MODEL_WINDOW && model.getOrderBy().size() > 0) {
            topLevelOrderByMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
        }

        // determine if ordering is required
        switch (topLevelOrderByMnemonic) {
            case OrderByMnemonic.ORDER_BY_UNKNOWN:
                // we have sample by, so expect sub-query has to be ordered
                if (model.getOrderBy().size() > 0 && model.getSampleBy() == null) {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_INVARIANT;
                } else {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
                }
                if (model.getSampleBy() == null
                        && orderByMnemonic != OrderByMnemonic.ORDER_BY_INVARIANT) {
                    for (int i = 0; i < n; i++) {
                        QueryColumn col = columns.getQuick(i);
                        if (hasAggregates(col.getAst())) {
                            orderByMnemonic = OrderByMnemonic.ORDER_BY_INVARIANT;
                            break;
                        }
                    }
                }
                break;
            case OrderByMnemonic.ORDER_BY_REQUIRED:
                // parent requires order
                // if this model forces ordering - sub-query ordering is not needed
                if (model.getOrderBy().size() > 0) {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_INVARIANT;
                } else {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
                }
                break;
            default:
                // sub-query ordering is not needed, but we'd like to propagate order by advice (if possible)
                model.getOrderBy().clear();
                if (model.getSampleBy() != null) {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
                } else {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_INVARIANT;
                }
                break;
        }

        final ObjList<ExpressionNode> orderByAdvice = getOrderByAdvice(model, orderByMnemonic);
        final IntList orderByDirectionAdvice = getOrderByAdviceDirection(model, orderByMnemonic);

        if (model.getSelectModelType() == QueryModel.SELECT_MODEL_WINDOW
                && model.getOrderBy().size() > 0
                && model.getOrderByAdvice().size() > 0
                && model.getLimitLo() == null) {

            boolean orderChanges = false;
            if (orderByAdvice.size() != model.getOrderByAdvice().size()) {
                orderChanges = true;
            } else {
                for (int i = 0, max = orderByAdvice.size(); i < max; i++) {
                    if (!orderByAdvice.getQuick(i).equals(model.getOrderBy().getQuick(i)) ||
                            orderByDirectionAdvice.getQuick(i) != model.getOrderByDirection().getQuick(i)) {
                        orderChanges = true;
                    }
                }
            }

            if (orderChanges) {
                // Set artificial limit to trigger LimitRCF use, so that parent models don't use the followedOrderByAdvice flag and skip necessary sort
                // Currently the only way to delineate order by advice is through use of factory that returns false for followedOrderByAdvice().
                // TODO: factories should provide order metadata to enable better sort-skipping
                model.setLimit(expressionNodePool.next().of(CONSTANT, "" + Long.MAX_VALUE, Integer.MIN_VALUE, 0), null);
            }
        }

        final ObjList<QueryModel> jm = model.getJoinModels();
        for (int i = 0, k = jm.size(); i < k; i++) {
            QueryModel qm = jm.getQuick(i).getNestedModel();
            if (qm != null) {
                if (model.getGroupBy().size() == 0
                        && model.getSampleBy() == null
                        && model.getSelectModelType() != QueryModel.SELECT_MODEL_DISTINCT) { // order by should not copy through group by, sample by or distinct
                    qm.setOrderByAdviceMnemonic(orderByMnemonic);
                    qm.copyOrderByAdvice(orderByAdvice);
                    qm.copyOrderByDirectionAdvice(orderByDirectionAdvice);
                }
                optimiseOrderBy(qm, orderByMnemonic);
            }
        }

        final QueryModel union = model.getUnionModel();
        if (union != null) {
            union.copyOrderByAdvice(orderByAdvice);
            union.copyOrderByDirectionAdvice(orderByDirectionAdvice);
            union.setOrderByAdviceMnemonic(orderByMnemonic);
            optimiseOrderBy(union, orderByMnemonic);
        }
    }

    private void parseFunctionAndEnumerateColumns(
            @NotNull QueryModel model,
            @NotNull SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        final RecordCursorFactory tableFactory;
        TableToken tableToken;
        if (model.getSelectModelType() == QueryModel.SELECT_MODEL_SHOW) {
            switch (model.getShowKind()) {
                case QueryModel.SHOW_TABLES:
                    tableFactory = new ShowTablesFunctionFactory.ShowTablesCursorFactory(configuration, AllTablesFunctionFactory.METADATA, AllTablesFunctionFactory.SIGNATURE);
                    break;
                case QueryModel.SHOW_COLUMNS:
                    tableToken = executionContext.getTableTokenIfExists(model.getTableNameExpr().token);
                    if (executionContext.getTableStatus(path, tableToken) != TableUtils.TABLE_EXISTS) {
                        throw SqlException.tableDoesNotExist(model.getTableNameExpr().position, model.getTableNameExpr().token);
                    }
                    tableFactory = new ShowColumnsRecordCursorFactory(tableToken, model.getTableNameExpr().position);
                    break;
                case QueryModel.SHOW_PARTITIONS:
                    tableToken = executionContext.getTableTokenIfExists(model.getTableNameExpr().token);
                    if (executionContext.getTableStatus(path, tableToken) != TableUtils.TABLE_EXISTS) {
                        throw SqlException.tableDoesNotExist(model.getTableNameExpr().position, model.getTableNameExpr().token);
                    }
                    tableFactory = new ShowPartitionsRecordCursorFactory(tableToken);
                    break;
                case QueryModel.SHOW_TRANSACTION:
                case QueryModel.SHOW_TRANSACTION_ISOLATION_LEVEL:
                    tableFactory = new ShowTransactionIsolationLevelCursorFactory();
                    break;
                case QueryModel.SHOW_MAX_IDENTIFIER_LENGTH:
                    tableFactory = new ShowMaxIdentifierLengthCursorFactory();
                    break;
                case QueryModel.SHOW_STANDARD_CONFORMING_STRINGS:
                    tableFactory = new ShowStandardConformingStringsCursorFactory();
                    break;
                case QueryModel.SHOW_SEARCH_PATH:
                    tableFactory = new ShowSearchPathCursorFactory();
                    break;
                case QueryModel.SHOW_DATE_STYLE:
                    tableFactory = new ShowDateStyleCursorFactory();
                    break;
                case QueryModel.SHOW_TIME_ZONE:
                    tableFactory = new ShowTimeZoneFactory();
                    break;
                case QueryModel.SHOW_PARAMETERS:
                    tableFactory = new ShowParametersCursorFactory();
                    break;
                case QueryModel.SHOW_SERVER_VERSION:
                    tableFactory = new ShowServerVersionCursorFactory();
                    break;
                default:
                    tableFactory = sqlParserCallback.generateShowSqlFactory(model);
                    break;
            }
            model.setTableFactory(tableFactory);
        } else {
            assert model.getTableNameFunction() == null;
            tableFactory = TableUtils.createCursorFunction(functionParser, model, executionContext).getRecordCursorFactory();
            model.setTableFactory(tableFactory);
            tableFactoriesInFlight.add(tableFactory);
        }
        copyColumnsFromMetadata(model, tableFactory.getMetadata(), true);
    }

    private void processEmittedJoinClauses(QueryModel model) {
        // pick up join clauses emitted at initial analysis stage
        // as we merge contexts at this level no more clauses is to be emitted
        for (int i = 0, k = emittedJoinClauses.size(); i < k; i++) {
            addJoinContext(model, emittedJoinClauses.getQuick(i));
        }
    }

    /**
     * Splits "where" clauses into "and" concatenated list of boolean expressions.
     *
     * @param node expression n
     */
    private void processJoinConditions(
            QueryModel parent,
            ExpressionNode node,
            boolean innerPredicate,
            QueryModel joinModel,
            int joinIndex
    ) throws SqlException {
        ExpressionNode n = node;
        // pre-order traversal
        sqlNodeStack.clear();
        while (!sqlNodeStack.isEmpty() || n != null) {
            if (n != null) {
                switch (joinOps.get(n.token)) {
                    case JOIN_OP_EQUAL:
                        analyseEquals(parent, n, innerPredicate, joinModel);
                        n = null;
                        break;
                    case JOIN_OP_AND:
                        if (n.rhs != null) {
                            sqlNodeStack.push(n.rhs);
                        }
                        n = n.lhs;
                        break;
                    case JOIN_OP_REGEX:
                        analyseRegex(parent, n);
                        if (joinBarriers.contains(joinModel.getJoinType())) {
                            addOuterJoinExpression(parent, joinModel, joinIndex, n);
                        } else {
                            parent.addParsedWhereNode(n, innerPredicate);
                        }
                        n = null;
                        break;
                    default:
                        if (joinBarriers.contains(joinModel.getJoinType())) {
                            addOuterJoinExpression(parent, joinModel, joinIndex, n);
                        } else {
                            parent.addParsedWhereNode(n, innerPredicate);
                        }
                        n = null;
                        break;
                }
            } else {
                n = sqlNodeStack.poll();
            }
        }
    }

    private void propagateTopDownColumns(QueryModel model, boolean allowColumnChange) {
        propagateTopDownColumns0(model, true, null, allowColumnChange);
    }

    /**
     * Pushes columns from top to bottom models.
     * <p>
     * Adding or removing columns to/from union, except, intersect should not happen!
     * UNION/INTERSECT/EXCEPT-ed columns MUST be exactly as specified in the query, otherwise they might produce different result, e.g.
     * <pre>
     * SELECT a
     * FROM (
     *   SELECT 1 as a, 'b' as status
     *   UNION
     *   SELECT 1 as a, 'c' as status
     * );
     * </pre>
     * Now if we push a top-to-bottom and remove b from union column list then we'll get a single '1' but we should get two!
     * Same thing applies to INTERSECT & EXCEPT
     * The only thing that'd be safe to add SET models is a constant literal (but what's the point?).
     * Column/expression pushdown should (probably) ONLY happen for UNION with ALL!
     * <p>
     * allowColumnsChange - determines whether changing columns of given model is acceptable.
     * It is not for columns used in distinct, except, intersect, union (even transitively for the latter three!).
     */
    private void propagateTopDownColumns0(QueryModel model, boolean topLevel, @Nullable QueryModel papaModel, boolean allowColumnsChange) {
        // copy columns to 'protect' column list that shouldn't be modified
        if (!allowColumnsChange && model.getBottomUpColumns().size() > 0) {
            model.copyBottomToTopColumns();
        }

        // skip over NONE model that does not have table name
        final QueryModel nested = skipNoneTypeModels(model.getNestedModel());
        model.setNestedModel(nested);
        final boolean nestedIsFlex = modelIsFlex(nested);
        final boolean nestedAllowsColumnChange = nested != null && nested.allowsColumnsChange()
                && model.allowsNestedColumnsChange();

        final QueryModel union = skipNoneTypeModels(model.getUnionModel());
        if (!topLevel && modelIsFlex(union)) {
            emitColumnLiteralsTopDown(model.getColumns(), union);
        }

        // process join models and their join conditions
        final ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            final QueryModel jm = joinModels.getQuick(i);
            final JoinContext jc = jm.getContext();
            if (jc != null && jc.aIndexes.size() > 0) {
                // join clause
                for (int k = 0, z = jc.aIndexes.size(); k < z; k++) {
                    emitLiteralsTopDown(jc.aNodes.getQuick(k), model);
                    emitLiteralsTopDown(jc.bNodes.getQuick(k), model);

                    emitLiteralsTopDown(jc.aNodes.getQuick(k), jm);
                    emitLiteralsTopDown(jc.bNodes.getQuick(k), jm);

                    if (papaModel != null) {
                        emitLiteralsTopDown(jc.aNodes.getQuick(k), papaModel);
                        emitLiteralsTopDown(jc.bNodes.getQuick(k), papaModel);
                    }
                }
            }

            // process post-join-where
            final ExpressionNode postJoinWhere = jm.getPostJoinWhereClause();
            if (postJoinWhere != null) {
                emitLiteralsTopDown(postJoinWhere, jm);
                emitLiteralsTopDown(postJoinWhere, model);
            }

            final ExpressionNode leftJoinWhere = jm.getOuterJoinExpressionClause();
            if (leftJoinWhere != null) {
                emitLiteralsTopDown(leftJoinWhere, jm);
                emitLiteralsTopDown(leftJoinWhere, model);
            }
        }

        final ExpressionNode postJoinWhere = model.getPostJoinWhereClause();
        if (postJoinWhere != null) {
            emitLiteralsTopDown(postJoinWhere, model);
        }

        // propagate join models columns in separate loop to catch columns added to models prior to the current one
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            final QueryModel jm = joinModels.getQuick(i);
            propagateTopDownColumns0(jm, false, model, true);
        }

        // If this is group by model we need to add all non-selected keys, only if this is sub-query
        // For top level models top-down column list will be empty
        if (model.getSelectModelType() == QueryModel.SELECT_MODEL_GROUP_BY && model.getTopDownColumns().size() > 0) {
            final ObjList<QueryColumn> bottomUpColumns = model.getBottomUpColumns();
            for (int i = 0, n = bottomUpColumns.size(); i < n; i++) {
                QueryColumn qc = bottomUpColumns.getQuick(i);
                if (qc.getAst().type != FUNCTION || !functionParser.getFunctionFactoryCache().isGroupBy(qc.getAst().token)) {
                    model.addTopDownColumn(qc, qc.getAlias());
                }
            }
        }

        // latest on
        if (model.getLatestBy().size() > 0) {
            emitLiteralsTopDown(model.getLatestBy(), model);
        }

        // propagate explicit timestamp declaration
        if (model.getTimestamp() != null && nestedIsFlex && nestedAllowsColumnChange) {
            emitLiteralsTopDown(model.getTimestamp(), nested);

            QueryModel unionModel = nested.getUnionModel();
            while (unionModel != null) {
                emitLiteralsTopDown(model.getTimestamp(), unionModel);
                unionModel = unionModel.getUnionModel();
            }
        }

        if (model.getWhereClause() != null) {
            if (allowColumnsChange) {
                emitLiteralsTopDown(model.getWhereClause(), model);
            }
            if (nestedAllowsColumnChange) {
                emitLiteralsTopDown(model.getWhereClause(), nested);

                QueryModel unionModel = nested.getUnionModel();
                while (unionModel != null) {
                    emitLiteralsTopDown(model.getWhereClause(), unionModel);
                    unionModel = unionModel.getUnionModel();
                }
            }
        }

        // propagate 'order by'
        if (!topLevel) {
            emitLiteralsTopDown(model.getOrderBy(), model);
        }

        if (nestedIsFlex && nestedAllowsColumnChange) {
            emitColumnLiteralsTopDown(model.getColumns(), nested);

            final IntList unionColumnIndexes = tempList;
            unionColumnIndexes.clear();
            ObjList<QueryColumn> nestedTopDownColumns = nested.getTopDownColumns();
            for (int i = 0, n = nestedTopDownColumns.size(); i < n; i++) {
                unionColumnIndexes.add(nested.getColumnAliasIndex(nestedTopDownColumns.getQuick(i).getAlias()));
            }

            QueryModel unionModel = nested.getUnionModel();
            while (unionModel != null) {
                // promote bottom-up columns to top-down columns, which
                // indexes correspond to the chosen columns in the "nested" model
                ObjList<QueryColumn> cols = unionModel.getBottomUpColumns();
                for (int i = 0, n = unionColumnIndexes.size(); i < n; i++) {
                    QueryColumn qc = cols.getQuick(unionColumnIndexes.getQuick(i));
                    unionModel.addTopDownColumn(qc, qc.getAlias());
                }
                unionModel = unionModel.getUnionModel();
            }
        }

        // go down the nested path
        if (nested != null) {
            propagateTopDownColumns0(nested, false, null, nestedAllowsColumnChange);
        }

        final QueryModel unionModel = model.getUnionModel();
        if (unionModel != null) {
            // we've to use this value because union-ed models don't have a back-reference and might not know they participate in set operation
            propagateTopDownColumns(unionModel, allowColumnsChange);
        }
    }

    private ExpressionNode pushOperationOutsideAgg(ExpressionNode agg, ExpressionNode op, ExpressionNode column, ExpressionNode constant, QueryModel model) {
        final QueryColumn qc = checkSimpleIntegerColumn(column, model);
        if (qc == null) {
            return agg;
        }

        agg.rhs = column;

        ExpressionNode count = expressionNodePool.next();
        count.token = "COUNT";
        count.type = FUNCTION;
        // INT and LONG are nullable, so we need to use COUNT(column) for them.
        if (qc.getColumnType() == ColumnType.INT || qc.getColumnType() == ColumnType.LONG) {
            count.paramCount = 1;
            count.rhs = column;
        } else {
            count.paramCount = 0;
        }
        count.position = agg.position;

        ExpressionNode mul = expressionNodePool.next();
        mul.token = "*";
        mul.type = OPERATION;
        mul.position = agg.position;
        mul.paramCount = 2;
        mul.precedence = 3;
        mul.lhs = count;
        mul.rhs = constant;

        if (op.lhs == column) { // maintain order for subtraction
            op.lhs = agg;
            op.rhs = mul;
        } else {
            op.lhs = mul;
            op.rhs = agg;
        }

        return op;
    }

    /**
     * Identify joined tables without join clause and try to find other reversible join clauses
     * that may be applied to it. For example when these tables joined"
     * <p>
     * from a
     * join b on c.x = b.x
     * join c on c.y = a.y
     * <p>
     * the system that prefers child table with the lowest index will attribute c.x = b.x clause to
     * table "c" leaving "b" without clauses.
     */
    @SuppressWarnings({"StatementWithEmptyBody"})
    private void reorderTables(QueryModel model) {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        int n = joinModels.size();

        tempCrosses.clear();
        // collect crosses
        for (int i = 0; i < n; i++) {
            QueryModel q = joinModels.getQuick(i);
            if (q.getContext() == null || q.getContext().parents.size() == 0) {
                tempCrosses.add(i);
            }
        }

        int cost = Integer.MAX_VALUE;
        int root = -1;

        // analyse state of tree for each set of n-1 crosses
        for (int z = 0, zc = tempCrosses.size(); z < zc; z++) {
            for (int i = 0; i < zc; i++) {
                if (z != i) {
                    int to = tempCrosses.getQuick(i);
                    final JoinContext jc = joinModels.getQuick(to).getContext();
                    // look above i up to OUTER join
                    for (int k = i - 1; k > -1 && swapJoinOrder(model, to, k, jc); k--) ;
                    // look below i for up to OUTER join
                    for (int k = i + 1; k < n && swapJoinOrder(model, to, k, jc); k++) ;
                }
            }

            IntList ordered = model.nextOrderedJoinModels();
            int thisCost = doReorderTables(model, ordered);

            // we have to have root, even if it is expensive
            // so the first iteration sets the root regardless
            // the following iterations might improve it
            if (thisCost < cost || root == -1) {
                root = z;
                cost = thisCost;
                model.setOrderedJoinModels(ordered);
            }
        }

        assert root != -1;
    }

    private ExpressionNode replaceIfAggregateOrLiteral(
            @Transient ExpressionNode node,
            QueryModel groupByModel,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel validatingModel,
            ObjList<ExpressionNode> groupByNodes,
            ObjList<CharSequence> groupByAliases
    ) throws SqlException {
        if (node != null &&
                ((node.type == FUNCTION && functionParser.getFunctionFactoryCache().isGroupBy(node.token)) || node.type == LITERAL)) {
            CharSequence alias = findColumnByAst(groupByNodes, groupByAliases, node);
            if (alias == null) {
                QueryColumn qc = queryColumnPool.next().of(createColumnAlias(node, groupByModel), node);
                groupByModel.addBottomUpColumn(qc);
                alias = qc.getAlias();

                groupByNodes.add(deepClone(expressionNodePool, node));
                groupByAliases.add(alias);

                if (node.type == LITERAL) {
                    doReplaceLiteral(node, translatingModel, innerModel, validatingModel, false);
                }
            }

            return nextLiteral(alias);
        }
        return node;
    }

    private ExpressionNode replaceIfCursor(
            @Transient ExpressionNode node,
            QueryModel cursorModel,
            @Nullable QueryModel innerVirtualModel,
            QueryModel translatingModel,
            QueryModel baseModel,
            SqlExecutionContext sqlExecutionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        if (node != null && functionParser.getFunctionFactoryCache().isCursor(node.token)) {
            return nextLiteral(
                    addCursorFunctionAsCrossJoin(
                            node,
                            null,
                            cursorModel,
                            innerVirtualModel,
                            translatingModel,
                            baseModel,
                            sqlExecutionContext,
                            sqlParserCallback
                    ).getAlias()
            );
        }
        return node;
    }

    private ExpressionNode replaceIfGroupByExpressionOrAggregate(
            ExpressionNode node,
            QueryModel groupByModel,
            ObjList<ExpressionNode> groupByNodes,
            ObjList<CharSequence> groupByAliases
    ) throws SqlException {
        CharSequence alias = findColumnByAst(groupByNodes, groupByAliases, node);
        if (alias != null) {
            return nextLiteral(alias);
        } else if (node.type == FUNCTION && functionParser.getFunctionFactoryCache().isGroupBy(node.token)) {
            QueryColumn qc = queryColumnPool.next().of(createColumnAlias(node, groupByModel), node);
            groupByModel.addBottomUpColumn(qc);
            return nextLiteral(qc.getAlias());
        }

        if (node.type == LITERAL) {
            throw SqlException.$(node.position, "column must appear in GROUP BY clause or aggregate function");
        }

        return node;
    }

    private ExpressionNode replaceIfUnaliasedLiteral(ExpressionNode node, QueryModel baseModel) throws SqlException {
        if (node != null && node.type == LITERAL) {
            CharSequence col = node.token;
            final int dot = Chars.indexOf(col, '.');
            int modelIndex = validateColumnAndGetModelIndex(baseModel, col, dot, node.position);

            boolean addAlias = dot == -1 && baseModel.getJoinModels().size() > 1;
            boolean removeAlias = dot > -1 && baseModel.getJoinModels().size() == 1;

            if (addAlias || removeAlias) {
                CharacterStoreEntry entry = characterStore.newEntry();
                if (addAlias) {
                    CharSequence alias = baseModel.getModelAliasIndexes().keys().get(modelIndex);
                    entry.put(alias).put('.').put(col);
                } else {
                    entry.put(col, dot + 1, col.length());
                }
                CharSequence prefixedCol = entry.toImmutable();
                return expressionNodePool.next().of(LITERAL, prefixedCol, node.precedence, node.position);
            }
        }

        return node;
    }

    private ExpressionNode replaceLiteral(
            @Transient ExpressionNode node,
            QueryModel translatingModel,
            @Nullable QueryModel innerModel,
            QueryModel validatingModel,
            boolean windowCall
    ) throws SqlException {
        if (node != null && node.type == LITERAL) {
            try {
                return doReplaceLiteral(node, translatingModel, innerModel, validatingModel, windowCall);
            } catch (SqlException e) {
                if (functionParser.findNoArgFunction(node)) {
                    node.type = FUNCTION;
                } else {
                    throw e;
                }
            }
        }
        return node;
    }

    private void replaceLiteralList(QueryModel innerVirtualModel, QueryModel translatingModel, QueryModel baseModel, ObjList<ExpressionNode> list) throws SqlException {
        for (int j = 0, n = list.size(); j < n; j++) {
            final ExpressionNode node = list.getQuick(j);
            emitLiterals(node, translatingModel, innerVirtualModel, baseModel, true);
            list.setQuick(j, replaceLiteral(node, translatingModel, innerVirtualModel, baseModel, true));
        }
    }

    private void resolveJoinColumns(QueryModel model) throws SqlException {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        final int size = joinModels.size();
        final CharSequence modelAlias = setAndGetModelAlias(model);
        // collect own alias
        collectModelAlias(model, 0, model);
        if (size > 1) {
            for (int i = 1; i < size; i++) {
                final QueryModel jm = joinModels.getQuick(i);
                final ObjList<ExpressionNode> jc = jm.getJoinColumns();
                final int joinColumnsSize = jc.size();

                if (joinColumnsSize > 0) {
                    final CharSequence jmAlias = setAndGetModelAlias(jm);
                    ExpressionNode joinCriteria = jm.getJoinCriteria();
                    for (int j = 0; j < joinColumnsSize; j++) {
                        ExpressionNode node = jc.getQuick(j);
                        ExpressionNode eq = makeOperation("=", makeModelAlias(modelAlias, node), makeModelAlias(jmAlias, node));
                        if (joinCriteria == null) {
                            joinCriteria = eq;
                        } else {
                            joinCriteria = makeOperation("and", joinCriteria, eq);
                        }
                    }
                    jm.setJoinCriteria(joinCriteria);
                }
                resolveJoinColumns(jm);
                collectModelAlias(model, i, jm);
            }
        }

        if (model.getNestedModel() != null) {
            resolveJoinColumns(model.getNestedModel());
        }

        // and union models too
        if (model.getUnionModel() != null) {
            resolveJoinColumns(model.getUnionModel());
        }
    }

    // Rewrite:
    // sum(x*10) into sum(x) * 10, etc.
    // sum(x+10) into sum(x) + count(x)*10
    // sum(x-10) into sum(x) - count(x)*10
    private ExpressionNode rewriteAggregate(ExpressionNode agg, QueryModel model) {
        if (agg == null) {
            return null;
        }

        ExpressionNode op = agg.rhs;

        if (op != null &&
                agg.type == FUNCTION &&
                functionParser.getFunctionFactoryCache().isGroupBy(agg.token) &&
                Chars.equalsIgnoreCase("sum", agg.token) &&
                op.type == OPERATION) {
            if (Chars.equals(op.token, '*')) { // sum(x*10) == sum(x)*10
                if (isIntegerConstant(op.rhs) && isSimpleIntegerColumn(op.lhs, model)) {
                    agg.rhs = op.lhs;
                    op.lhs = agg;
                    return op;
                } else if (isIntegerConstant(op.lhs) && isSimpleIntegerColumn(op.rhs, model)) {
                    agg.rhs = op.rhs;
                    op.rhs = agg;
                    return op;
                }
            } else if (Chars.equals(op.token, '+') || Chars.equals(op.token, '-')) { // sum(x+10) == sum(x)+count(x)*10 , sum(x-10) == sum(x)-count(x)*10
                if (isIntegerConstant(op.rhs)) {
                    return pushOperationOutsideAgg(agg, op, op.lhs, op.rhs, model);
                } else if (isIntegerConstant(op.lhs)) {
                    return pushOperationOutsideAgg(agg, op, op.rhs, op.lhs, model);
                }
            }
        }

        return agg;
    }

    /**
     * Rewrites expressions such as :
     * SELECT count_distinct(s) FROM tab WHERE s like '%a' ;
     * into more parallel-friendly :
     * SELECT count(*) FROM (SELECT s FROM tab WHERE s like '%a' AND s IS NOT NULL GROUP BY s);
     */
    private void rewriteCountDistinct(QueryModel model) throws SqlException {
        QueryModel nested = model.getNestedModel();
        ExpressionNode countDistinctExpr;

        if (
                model.getColumns().size() == 1
                        && (countDistinctExpr = model.getColumns().getQuick(0).getAst()).type == ExpressionNode.FUNCTION
                        && Chars.equalsIgnoreCase("count_distinct", countDistinctExpr.token)
                        && countDistinctExpr.paramCount == 1
                        && !isSymbolColumn(countDistinctExpr, nested) // don't rewrite for symbol column because there's a separate optimization in count_distinct
                        && model.getJoinModels().size() == 1
                        && model.getUnionModel() == null
                        && nested != null
                        && nested.getNestedModel() == null
                        && model.getWhereClause() == null
                        && nested.getTableName() != null
                        && model.getSampleBy() == null
                        && model.getGroupBy().size() == 0
        ) {
            ExpressionNode distinctExpr = countDistinctExpr.rhs;

            QueryModel middle = queryModelPool.next();
            middle.setNestedModel(nested);
            middle.setSelectModelType(QueryModel.SELECT_MODEL_GROUP_BY);
            model.setNestedModel(middle);

            CharSequence innerAlias = createColumnAlias(distinctExpr.token, middle, true);
            QueryColumn qc = queryColumnPool.next().of(innerAlias, distinctExpr);
            middle.addBottomUpColumn(qc);

            ExpressionNode nullExpr = expressionNodePool.next();
            nullExpr.type = CONSTANT;
            nullExpr.token = "null";
            nullExpr.precedence = 0;

            ExpressionNode node = expressionNodePool.next();
            node.type = OPERATION;
            node.token = "!=";
            node.paramCount = 2;
            node.lhs = nullExpr;
            node.rhs = distinctExpr;
            node.precedence = 7;

            nested.setWhereClause(concatFilters(nested.getWhereClause(), node));
            middle.addGroupBy(distinctExpr);

            countDistinctExpr.token = "count";
            countDistinctExpr.paramCount = 0;
            countDistinctExpr.rhs = null;

            // if rewrite applies to this model then there's no point trying to apply it to nested, joined or union-ed models
            return;
        }

        if (nested != null) {
            rewriteCountDistinct(nested);
        }

        final QueryModel union = model.getUnionModel();
        if (union != null) {
            rewriteCountDistinct(union);
        }

        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            rewriteCountDistinct(joinModels.getQuick(i));
        }
    }

    // push aggregate function calls to group by model, replace key column expressions with group by aliases
    // raise error if raw column usage doesn't match one of expressions on group by list
    private ExpressionNode rewriteGroupBySelectExpression(
            final @Transient ExpressionNode topLevelNode,
            QueryModel groupByModel,
            ObjList<ExpressionNode> groupByNodes,
            ObjList<CharSequence> groupByAliases
    ) throws SqlException {
        sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        ExpressionNode temp = replaceIfGroupByExpressionOrAggregate(topLevelNode, groupByModel, groupByNodes, groupByAliases);
        if (temp != topLevelNode) {
            return temp;
        }

        ExpressionNode node = topLevelNode;

        while (!sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.paramCount < 3) {
                    if (node.rhs != null) {
                        temp = replaceIfGroupByExpressionOrAggregate(node.rhs, groupByModel, groupByNodes, groupByAliases);
                        if (node.rhs == temp) {
                            sqlNodeStack.push(node.rhs);
                        } else {
                            node.rhs = temp;
                        }
                    }

                    if (node.lhs != null) {
                        temp = replaceIfGroupByExpressionOrAggregate(node.lhs, groupByModel, groupByNodes, groupByAliases);
                        if (temp == node.lhs) {
                            node = node.lhs;
                        } else {
                            node.lhs = temp;
                            node = null;
                        }
                    } else {
                        node = null;
                    }
                } else {
                    for (int i = 1, k = node.paramCount; i < k; i++) {
                        ExpressionNode e = node.args.getQuick(i);
                        temp = replaceIfGroupByExpressionOrAggregate(e, groupByModel, groupByNodes, groupByAliases);
                        if (e == temp) {
                            sqlNodeStack.push(e);
                        } else {
                            node.args.setQuick(i, temp);
                        }
                    }

                    ExpressionNode e = node.args.getQuick(0);
                    temp = replaceIfGroupByExpressionOrAggregate(e, groupByModel, groupByNodes, groupByAliases);
                    if (e == temp) {
                        node = e;
                    } else {
                        node.args.setQuick(0, temp);
                        node = null;
                    }
                }
            } else {
                node = sqlNodeStack.poll();
            }
        }

        return topLevelNode;
    }

    /**
     * For queries on tables with designated timestamp, no where clause and no order by or order by ts only :
     * <p>
     * For example:
     * select a,b from X limit -10 -> select a,b from (select * from X order by ts desc limit 10) order by ts asc
     * select a,b from X order by ts limit -10 -> select a,b  from (select * from X order by ts desc limit 10) order by ts asc
     * select a,b from X order by ts desc limit -10 -> select a,b from (select * from X order by ts asc limit 10) order by ts desc
     *
     * @param model            input model
     * @param executionContext execution context
     */
    private void rewriteNegativeLimit(final QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        if (model != null) {
            final QueryModel nested = model.getNestedModel();
            Function loFunction;
            ObjList<ExpressionNode> orderBy;
            long limitValue;

            if (
                    model.getLimitLo() != null
                            && model.getLimitHi() == null
                            && model.getUnionModel() == null
                            && model.getJoinModels().size() == 1
                            && model.getGroupBy().size() == 0
                            && model.getSampleBy() == null
                            && !hasAggregateQueryColumn(model)
                            && !model.isDistinct()
                            && nested != null
                            && nested.getJoinModels().size() == 1
                            && nested.getTimestamp() != null
                            && nested.getWhereClause() == null
                            && (loFunction = getLoFunction(model.getLimitLo(), executionContext)) != null
                            && loFunction.isConstant()
                            && (limitValue = loFunction.getLong(null)) < 0
                            && (limitValue >= -executionContext.getCairoEngine().getConfiguration().getSqlMaxNegativeLimit())
                            && ((orderBy = nested.getOrderBy()).size() == 0 ||
                            (
                                    orderBy.size() == 1
                                            && nested.isOrderByTimestamp(orderBy.getQuick(0).token)
                            )
                    )
            ) {
                final CharacterStoreEntry characterStoreEntry = characterStore.newEntry();
                characterStoreEntry.put(-limitValue);
                ExpressionNode limitNode = nextLiteral(characterStoreEntry.toImmutable());
                // override limit node type
                limitNode.type = CONSTANT;

                // insert two models between "model" and "nested", like so:
                // model -> order -> limit -> nested + order

                // the outer model loses its limit
                model.setLimit(null, null);

                QueryModel limitModel = queryModelPool.next();
                limitModel.setSelectModelType(QueryModel.SELECT_MODEL_CHOOSE);
                limitModel.setNestedModel(nested);
                limitModel.setLimit(limitNode, null);

                // copy columns to the "limit" model from the "nested" model
                ObjList<CharSequence> nestedColumnNames = nested.getBottomUpColumnNames();
                for (int i = 0, n = nestedColumnNames.size(); i < n; i++) {
                    limitModel.addBottomUpColumn(nextColumn(nestedColumnNames.getQuick(i)));
                }

                QueryModel order = null;
                if (limitValue < -1) {
                    order = queryModelPool.next();
                    order.setNestedModel(limitModel);
                    ObjList<QueryColumn> limitColumns = limitModel.getBottomUpColumns();
                    for (int i = 0, n = limitColumns.size(); i < n; i++) {
                        order.addBottomUpColumn(limitColumns.getQuick(i));
                    }
                    order.setNestedModelIsSubQuery(true);
                    model.setNestedModel(order);
                }

                if (orderBy.size() == 0) {
                    if (order != null) {
                        order.addOrderBy(nested.getTimestamp(), QueryModel.ORDER_DIRECTION_ASCENDING);
                    }

                    nested.addOrderBy(nested.getTimestamp(), QueryModel.ORDER_DIRECTION_DESCENDING);
                } else {
                    final IntList orderByDirection = nested.getOrderByDirection();
                    if (order != null) {
                        // it is possible that order by column has not been selected in the order model
                        // for that reason we must add column lookup to allow order-by optimisation to resolve the column name
                        final ExpressionNode orderByNode = nested.getTimestamp();
                        final CharSequence orderByToken = orderByNode.token;
                        // We are here because order-by matched timestamp, what we don't know is
                        // if we matched timestamp via index or name. If we parse order-by token
                        // AGAIN, that would be our index, otherwise - it is a name
                        if (order.getAliasToColumnMap().excludes(orderByToken)) {
                            order.getAliasToColumnMap().put(orderByToken, nextColumn(orderByToken));
                        }
                        order.addOrderBy(orderByNode, orderByDirection.getQuick(0));
                    }

                    int newOrder;
                    if (orderByDirection.getQuick(0) == QueryModel.ORDER_DIRECTION_ASCENDING) {
                        newOrder = QueryModel.ORDER_DIRECTION_DESCENDING;
                    } else {
                        newOrder = QueryModel.ORDER_DIRECTION_ASCENDING;
                    }
                    orderByDirection.setQuick(0, newOrder);
                }

                if (order == null) {
                    model.setNestedModel(limitModel);
                }
            }
            // assign different nested model because it might need to be re-written
            rewriteNegativeLimit(nested, executionContext);
        }
    }

    /**
     * Rewrites order by clause to achieve simple column resolution for model parser.
     * Order by must never reference column that doesn't exist in its own select list.
     * <p>
     * Because order by clause logically executes after "select" it must be able to
     * reference results of arithmetic expression, aggregation function results, arithmetic with
     * aggregation results and window functions. Somewhat contradictory to this order by must
     * also be able to reference columns of table or sub-query that are not even in select clause.
     *
     * @param model inbound model
     * @return outbound model
     * @throws SqlException when column names are ambiguous or not found at all.
     */
    private QueryModel rewriteOrderBy(final QueryModel model) throws SqlException {
        // find base model and check if there is "group-by" model in between
        // when we are dealing with "group by" model some implicit "order by" columns have to be dropped,
        // However, in the following example
        // select a, b from T order by c
        // ordering does affect query result
        QueryModel result = model;
        QueryModel base = model;
        QueryModel baseParent = model;
        QueryModel wrapper = null;
        QueryModel limitModel = model;//bottom-most model which contains limit, order by can't be moved past it
        final int modelColumnCount = model.getBottomUpColumns().size();
        boolean groupByOrDistinct = false;

        while (base.getBottomUpColumns().size() > 0 && !base.isNestedModelIsSubQuery()) {
            baseParent = base;

            final QueryModel union = base.getUnionModel();
            if (union != null) {
                final QueryModel rewritten = rewriteOrderBy(union);
                if (rewritten != union) {
                    base.setUnionModel(rewritten);
                }
            }

            base = base.getNestedModel();
            if (base.getLimitLo() != null) {
                limitModel = base;
            }
            final int selectModelType = baseParent.getSelectModelType();
            groupByOrDistinct = groupByOrDistinct
                    || selectModelType == QueryModel.SELECT_MODEL_GROUP_BY
                    || selectModelType == QueryModel.SELECT_MODEL_DISTINCT;
        }

        // find out how "order by" columns are referenced
        ObjList<ExpressionNode> orderByNodes = base.getOrderBy();
        int sz = orderByNodes.size();
        if (sz > 0) {
            // for each order by column check how deep we need to go between "model" and "base"
            for (int i = 0; i < sz; i++) {
                final ExpressionNode orderBy = orderByNodes.getQuick(i);
                CharSequence column = orderBy.token;
                int dot = Chars.indexOf(column, '.');
                // is this a table reference?
                if (dot > -1 || model.getAliasToColumnMap().excludes(column)) {
                    // validate column
                    validateColumnAndGetModelIndex(base, column, dot, orderBy.position);
                    // good news, our column matched base model
                    // this condition is to ignore order by columns that are not in select and behind group by
                    if (base != model) {
                        // check if column is aliased as either
                        // "x y" or "tab.x y" or "t.x y", where "t" is alias of table "tab"
                        final LowerCaseCharSequenceObjHashMap<CharSequence> map = baseParent.getColumnNameToAliasMap();
                        CharSequence alias = null;
                        int index = map.keyIndex(column);
                        if (index > -1 && dot > -1) {
                            // we have the following that are true:
                            // 1. column does have table alias, e.g. tab.x
                            // 2. column definitely exists
                            // 3. column is _not_ referenced as select tab.x from tab
                            //
                            // lets check if column is referenced as select x from tab
                            // this will determine is column is referenced by select at all
                            index = map.keyIndex(column, dot + 1, column.length());
                        }

                        if (index > -1) {
                            QueryColumn qc = this.getQueryColumn(baseParent, column, dot);
                            if (qc != null) {
                                index = map.keyIndex(qc.getAst().token);
                            }
                        }


                        if (index < 0) {
                            // we have found alias, rewrite order by column
                            orderBy.token = map.valueAtQuick(index);

                            tempQueryModel = null;
                            tempColumnAlias = null;

                            // if necessary, propagate column to limit model that'll receive order by
                            if (limitModel != baseParent) {
                                CharSequence translatedColumnAlias = getTranslatedColumnAlias(limitModel, baseParent, orderBy.token);

                                if (translatedColumnAlias == null) {
                                    //add column ref to the most-nested model that doesn't have it
                                    alias = SqlUtil.createColumnAlias(characterStore, tempColumnAlias, Chars.indexOf(tempColumnAlias, '.'), tempQueryModel.getAliasToColumnMap());
                                    tempQueryModel.addBottomUpColumn(nextColumn(alias, tempColumnAlias));

                                    //and then push to upper models
                                    QueryModel m = limitModel;
                                    while (m != tempQueryModel) {
                                        m.addBottomUpColumn(nextColumn(alias));
                                        m = m.getNestedModel();
                                    }

                                    tempQueryModel = null;
                                    tempColumnAlias = null;
                                    orderBy.token = alias;

                                    //if necessary, add external model to maintain output
                                    if (limitModel == model && wrapper == null) {
                                        wrapper = queryModelPool.next();
                                        wrapper.setSelectModelType(QueryModel.SELECT_MODEL_CHOOSE);
                                        for (int j = 0; j < modelColumnCount; j++) {
                                            wrapper.addBottomUpColumn(nextColumn(model.getBottomUpColumns().getQuick(j).getAlias()));
                                        }
                                        result = wrapper;
                                        wrapper.setNestedModel(model);
                                    }
                                } else {
                                    orderBy.token = translatedColumnAlias;
                                }
                            }
                        } else {
                            if (dot > -1 && !base.getModelAliasIndexes().contains(column, 0, dot)) {
                                throw SqlException.invalidColumn(orderBy.position, column);
                            }

                            // we must attempt to ascend order by column
                            // when we have group-by or distinct model, ascent is not possible
                            if (groupByOrDistinct) {
                                throw SqlException.position(orderBy.position)
                                        .put("ORDER BY expressions must appear in select list. ")
                                        .put("Invalid column: ")
                                        .put(column);
                            } else {
                                if (dot > -1
                                        && base.getModelAliasIndexes().contains(column, 0, dot)
                                        && base.getModelAliasIndexes().size() == 1) {
                                    column = column.subSequence(dot + 1, column.length());//remove alias
                                    dot = -1;
                                }

                                if (baseParent.getSelectModelType() != QueryModel.SELECT_MODEL_CHOOSE) {
                                    QueryModel synthetic = queryModelPool.next();
                                    synthetic.setSelectModelType(QueryModel.SELECT_MODEL_CHOOSE);
                                    for (int j = 0, z = baseParent.getBottomUpColumns().size(); j < z; j++) {
                                        QueryColumn qc = baseParent.getBottomUpColumns().getQuick(j);
                                        if (qc.getAst().type == ExpressionNode.FUNCTION || qc.getAst().type == ExpressionNode.OPERATION) {
                                            emitLiterals(qc.getAst(), synthetic, null, baseParent.getNestedModel(), false);
                                        } else {
                                            synthetic.addBottomUpColumn(qc);
                                        }
                                    }
                                    synthetic.setNestedModel(base);
                                    baseParent.setNestedModel(synthetic);
                                    baseParent = synthetic;

                                    // the column may appear in the list after literals from expressions have been emitted
                                    index = synthetic.getColumnNameToAliasMap().keyIndex(column);

                                    if (index < 0) {
                                        alias = synthetic.getColumnNameToAliasMap().valueAtQuick(index);
                                    }
                                }

                                if (alias == null) {
                                    alias = SqlUtil.createColumnAlias(characterStore, column, dot, baseParent.getAliasToColumnMap());
                                    baseParent.addBottomUpColumn(nextColumn(alias, column));
                                }

                                // do we have more than one parent model?
                                if (model != baseParent) {
                                    QueryModel m = model;
                                    do {
                                        m.addBottomUpColumn(nextColumn(alias));
                                        m = m.getNestedModel();
                                    } while (m != baseParent);
                                }

                                orderBy.token = alias;

                                if (wrapper == null) {
                                    wrapper = queryModelPool.next();
                                    wrapper.setSelectModelType(QueryModel.SELECT_MODEL_CHOOSE);
                                    for (int j = 0; j < modelColumnCount; j++) {
                                        wrapper.addBottomUpColumn(nextColumn(model.getBottomUpColumns().getQuick(j).getAlias()));
                                    }
                                    result = wrapper;
                                    wrapper.setNestedModel(model);
                                }
                            }
                        }
                    }
                }
                //order by can't be pushed through limit clause because it'll produce bad results
                if (base != baseParent && base != limitModel) {
                    limitModel.addOrderBy(orderBy, base.getOrderByDirection().getQuick(i));
                }
            }

            if (base != model && base != limitModel) {
                base.clearOrderBy();
            }
        }

        final QueryModel nested = base.getNestedModel();
        if (nested != null) {
            final QueryModel rewritten = rewriteOrderBy(nested);
            if (rewritten != nested) {
                base.setNestedModel(rewritten);
            }
        }

        final QueryModel union = base.getUnionModel();
        if (union != null) {
            final QueryModel rewritten = rewriteOrderBy(union);
            if (rewritten != union) {
                base.setUnionModel(rewritten);
            }
        }

        ObjList<QueryModel> joinModels = base.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            // we can ignore result of order by rewrite for because
            // 1. when join model is not a sub-query it will always have all the fields, so order by wouldn't
            //    introduce synthetic model (no column needs to be hidden)
            // 2. when join model is a sub-query it will have nested model, which can be rewritten. Parent model
            //    would remain the same again.
            rewriteOrderBy(joinModels.getQuick(i));
        }

        return result;
    }

    private void rewriteOrderByPosition(final QueryModel model) throws SqlException {
        QueryModel base = model;
        QueryModel baseParent = model;
        QueryModel baseGroupBy = null;
        QueryModel baseOuter = null;
        QueryModel baseDistinct = null;
        // while order by is initially kept in the base model (most inner one)
        // columns used in order by could be stored in one of many models : inner model, group by model, window or outer model
        // here we've to descend and keep track of all of those
        while (base.getBottomUpColumns().size() > 0) {
            // Check if the model contains the full list of selected columns and, thus, can be used as the parent.
            if (!base.isSelectTranslation()) {
                baseParent = base;
            }
            switch (base.getSelectModelType()) {
                case QueryModel.SELECT_MODEL_DISTINCT:
                    baseDistinct = base;
                    break;
                case QueryModel.SELECT_MODEL_GROUP_BY:
                    baseGroupBy = base;
                    break;
                case QueryModel.SELECT_MODEL_VIRTUAL:
                case QueryModel.SELECT_MODEL_CHOOSE:
                    QueryModel nested = base.getNestedModel();
                    if (nested != null && nested.getSelectModelType() == QueryModel.SELECT_MODEL_GROUP_BY) {
                        baseOuter = base;
                    }
                    break;
            }
            base = base.getNestedModel();
        }

        if (baseDistinct != null) {
            baseParent = baseDistinct;
        } else if (baseOuter != null) {
            baseParent = baseOuter;
        } else if (baseGroupBy != null) {
            baseParent = baseGroupBy;
        }

        ObjList<ExpressionNode> orderByNodes = base.getOrderBy();
        int sz = orderByNodes.size();
        if (sz > 0) {
            final ObjList<QueryColumn> columns = baseParent.getBottomUpColumns();
            final int columnCount = columns.size();
            for (int i = 0; i < sz; i++) {
                final ExpressionNode orderBy = orderByNodes.getQuick(i);
                final CharSequence column = orderBy.token;

                char first = column.charAt(0);
                if (first < '0' || first > '9') {
                    continue;
                }

                try {
                    final int position = Numbers.parseInt(column);
                    if (position < 1 || position > columnCount) {
                        throw SqlException.$(orderBy.position, "order column position is out of range [max=").put(columnCount).put(']');
                    }
                    orderByNodes.setQuick(
                            i,
                            expressionNodePool.next().of(
                                    LITERAL,
                                    columns.get(position - 1).getName(),
                                    -1,
                                    orderBy.position
                            )
                    );
                } catch (NumericException e) {
                    throw SqlException.invalidColumn(orderBy.position, column);
                }
            }
        }

        QueryModel nested = base.getNestedModel();
        if (nested != null) {
            rewriteOrderByPosition(nested);
        }

        ObjList<QueryModel> joinModels = base.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            // we can ignore result of order by rewrite for because
            // 1. when join model is not a sub-query it will always have all the fields, so order by wouldn't
            //    introduce synthetic model (no column needs to be hidden)
            // 2. when join model is a sub-query it will have nested model, which can be rewritten. Parent model
            //    would remain the same again.
            rewriteOrderByPosition(joinModels.getQuick(i));
        }
    }

    private void rewriteOrderByPositionForUnionModels(final QueryModel model) throws SqlException {
        QueryModel next = model.getUnionModel();
        if (next != null) {
            doRewriteOrderByPositionForUnionModels(model, model, next);
        }

        next = model.getNestedModel();
        if (next != null) {
            rewriteOrderByPositionForUnionModels(next);
        }
    }

    // flatParent = true means that parent model does not have selected columns
    private QueryModel rewriteSelectClause(
            QueryModel model,
            boolean flatParent,
            SqlExecutionContext sqlExecutionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {

        if (model.getUnionModel() != null) {
            QueryModel rewrittenUnionModel = rewriteSelectClause(
                    model.getUnionModel(),
                    true,
                    sqlExecutionContext,
                    sqlParserCallback
            );
            if (rewrittenUnionModel != model.getUnionModel()) {
                model.setUnionModel(rewrittenUnionModel);
            }
        }

        ObjList<QueryModel> models = model.getJoinModels();
        for (int i = 0, n = models.size(); i < n; i++) {
            final QueryModel m = models.getQuick(i);
            final boolean flatModel = m.getBottomUpColumns().size() == 0;
            final QueryModel nestedModel = m.getNestedModel();
            if (nestedModel != null) {
                QueryModel rewritten = rewriteSelectClause(nestedModel, flatModel, sqlExecutionContext, sqlParserCallback);
                if (rewritten != nestedModel) {
                    m.setNestedModel(rewritten);
                    // since we have rewritten nested model we also have to update column hash
                    m.copyColumnsFrom(rewritten, queryColumnPool, expressionNodePool);
                }
            }

            if (flatModel) {
                if (flatParent && m.getSampleBy() != null) {
                    throw SqlException.$(m.getSampleBy().position, "'sample by' must be used with 'select' clause, which contains aggregate expression(s)");
                }
            } else {
                model.replaceJoinModel(i, rewriteSelectClause0(m, sqlExecutionContext, sqlParserCallback));
            }
        }

        // "model" is always first in its own list of join models
        return models.getQuick(0);
    }

    @NotNull
    private QueryModel rewriteSelectClause0(
            final QueryModel model,
            SqlExecutionContext sqlExecutionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        assert model.getNestedModel() != null;

        groupByAliases.clear();
        groupByNodes.clear();

        final QueryModel groupByModel = queryModelPool.next();
        groupByModel.setSelectModelType(QueryModel.SELECT_MODEL_GROUP_BY);
        final QueryModel distinctModel = queryModelPool.next();
        distinctModel.setSelectModelType(QueryModel.SELECT_MODEL_DISTINCT);
        final QueryModel outerVirtualModel = queryModelPool.next();
        outerVirtualModel.setSelectModelType(QueryModel.SELECT_MODEL_VIRTUAL);
        final QueryModel innerVirtualModel = queryModelPool.next();
        innerVirtualModel.setSelectModelType(QueryModel.SELECT_MODEL_VIRTUAL);
        final QueryModel windowModel = queryModelPool.next();
        windowModel.setSelectModelType(QueryModel.SELECT_MODEL_WINDOW);
        final QueryModel translatingModel = queryModelPool.next();
        translatingModel.setSelectModelType(QueryModel.SELECT_MODEL_CHOOSE);
        // this is dangling model, which isn't chained with any other
        // we use it to ensure expression and alias uniqueness
        final QueryModel cursorModel = queryModelPool.next();

        boolean useInnerModel = false;
        boolean useWindowModel = false;
        boolean useGroupByModel = false;
        boolean useOuterModel = false;
        final boolean useDistinctModel = model.isDistinct();

        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        final QueryModel baseModel = model.getNestedModel();
        final boolean hasJoins = baseModel.getJoinModels().size() > 1;

        // sample by clause should be promoted to all the models as well as validated
        final ExpressionNode sampleBy = baseModel.getSampleBy();
        if (sampleBy != null) {
            // move sample by to group by model
            groupByModel.moveSampleByFrom(baseModel);
        }

        if (baseModel.getGroupBy().size() > 0) {
            groupByModel.moveGroupByFrom(baseModel);
            useGroupByModel = true; // group by should be implemented even if there are no aggregate functions
        }

        // cursor model should have all columns that base model has to properly resolve duplicate names
        cursorModel.getAliasToColumnMap().putAll(baseModel.getAliasToColumnMap());
        // create virtual columns from select list

        for (int i = 0, k = columns.size(); i < k; i++) {
            QueryColumn qc = columns.getQuick(i);
            final boolean window = qc.isWindowColumn();

            // fail-fast if this is an arithmetic expression where we expect window function
            if (window && qc.getAst().type != ExpressionNode.FUNCTION) {
                throw SqlException.$(qc.getAst().position, "Window function expected");
            }

            if (qc.getAst().type == ExpressionNode.BIND_VARIABLE) {
                useInnerModel = true;
            } else if (qc.getAst().type != LITERAL) {
                if (qc.getAst().type == ExpressionNode.FUNCTION) {
                    if (window) {
                        useWindowModel = true;
                        continue;
                    } else if (functionParser.getFunctionFactoryCache().isGroupBy(qc.getAst().token)) {
                        useGroupByModel = true;

                        if (groupByModel.getSampleByFill().size() > 0) { // fill breaks if column is de-duplicated
                            continue;
                        }

                        ExpressionNode repl = rewriteAggregate(qc.getAst(), baseModel);
                        if (repl == qc.getAst()) { // no rewrite
                            if (!useOuterModel) { // so try to push duplicate aggregates to nested model
                                for (int j = i + 1; j < k; j++) {
                                    if (ExpressionNode.compareNodesExact(qc.getAst(), columns.get(j).getAst())) {
                                        useOuterModel = true;
                                        break;
                                    }
                                }
                            }
                            continue;
                        }

                        useOuterModel = true;
                        qc.of(qc.getAlias(), repl);
                    } else if (functionParser.getFunctionFactoryCache().isCursor(qc.getAst().token)) {
                        continue;
                    }
                }

                if (checkForAggregates(qc.getAst())) {
                    useGroupByModel = true;
                    useOuterModel = true;
                } else {
                    useInnerModel = true;
                }
            }
        }

        boolean outerVirtualIsSelectChoose = true;
        // if there are explicit group by columns then nothing else should go to group by model
        // select columns should either match group by columns exactly or go to outer virtual model
        ObjList<ExpressionNode> groupBy = groupByModel.getGroupBy();
        boolean explicitGroupBy = groupBy.size() > 0;

        if (explicitGroupBy) {
            // Outer model is not needed only if select clauses is the same as group by plus aggregate function calls
            for (int i = 0, n = groupBy.size(); i < n; i++) {
                ExpressionNode node = groupBy.getQuick(i);
                CharSequence alias = null;
                int originalNodePosition = -1;

                //group by select clause alias
                if (node.type == LITERAL) {
                    // If literal is select clause alias then use its AST //sym1 -> ccy x -> a
                    // NOTE: this is merely a shortcut and doesn't mean that alias exists at group by stage !
                    // while
                    //   select a as d  from t group by d
                    // works, the following does not
                    //  select a as d  from t group by d + d
                    QueryColumn qc = model.getAliasToColumnMap().get(node.token);
                    if (qc != null && (qc.getAst().type != LITERAL || !Chars.equals(node.token, qc.getAst().token))) {
                        originalNodePosition = node.position;
                        node = qc.getAst();
                        alias = qc.getAlias();
                    }
                } else if (node.type == CONSTANT) { // group by column index
                    try {
                        int columnIdx = Numbers.parseInt(node.token);
                        // group by column index is 1-based
                        if (columnIdx < 1 || columnIdx > columns.size()) {
                            throw SqlException.$(node.position, "GROUP BY position ").put(columnIdx).put(" is not in select list");
                        }
                        columnIdx--;

                        QueryColumn qc = columns.getQuick(columnIdx);

                        originalNodePosition = node.position;
                        node = qc.getAst();
                        alias = qc.getAlias();
                    } catch (NumericException e) {
                        // ignore
                    }
                }

                if (node.type == LITERAL && Chars.endsWith(node.token, '*')) {
                    throw SqlException.$(node.position, "'*' is not allowed in GROUP BY");
                }

                addMissingTablePrefixes(node, baseModel);
                // ignore duplicates in group by
                if (findColumnByAst(groupByNodes, groupByAliases, node) != null) {
                    continue;
                }

                validateGroupByExpression(node, groupByModel, originalNodePosition);

                if (node.type == LITERAL) {
                    if (alias == null) {
                        alias = createColumnAlias(node, groupByModel);
                    } else {
                        alias = createColumnAlias(alias, innerVirtualModel, true);
                    }
                    QueryColumn groupByColumn = createGroupByColumn(
                            alias,
                            node,
                            baseModel,
                            translatingModel,
                            innerVirtualModel,
                            windowModel,
                            groupByModel
                    );

                    groupByNodes.add(node);
                    groupByAliases.add(groupByColumn.getAlias());
                }
                // If there's at least one other group by column then we can ignore constant expressions,
                // otherwise we've to include not to affect the outcome, e.g.
                // if table t is empty then
                // select count(*) from t  returns 0  but
                // select count(*) from t group by 12+3 returns empty result
                // if we removed 12+3 then we'd affect result
                else if (!(isEffectivelyConstantExpression(node) && i > 0)) {
                    // add expression
                    // if group by element is an expression then we've to use inner model to compute it
                    useInnerModel = true;

                    // expressions in GROUP BY clause should be pushed to inner model
                    CharSequence innerAlias = createColumnAlias(node.token, innerVirtualModel, true);
                    QueryColumn qc = queryColumnPool.next().of(innerAlias, node);
                    innerVirtualModel.addBottomUpColumn(qc);

                    if (alias != null) {
                        alias = createColumnAlias(alias, groupByModel, true);
                    } else {
                        alias = qc.getAlias();
                    }

                    final QueryColumn groupByColumn = nextColumn(alias, qc.getAlias());
                    groupByModel.addBottomUpColumn(groupByColumn);

                    groupByNodes.add(deepClone(expressionNodePool, node));
                    groupByAliases.add(groupByColumn.getAlias());

                    emitLiterals(qc.getAst(), translatingModel, null, baseModel, false);
                }
            }
        }

        groupByUsed.setAll(groupBy.size(), false);
        int nonAggSelectCount = 0;

        // create virtual columns from select list
        for (int i = 0, k = columns.size(); i < k; i++) {
            QueryColumn qc = columns.getQuick(i);
            final boolean window = qc.isWindowColumn();

            if (qc.getAst().type == LITERAL) {
                if (Chars.endsWith(qc.getAst().token, '*')) {
                    // in general sense we need to create new column in case
                    // there is change of alias, for example we may have something as simple as
                    // select a.f, b.f from ....
                    createSelectColumnsForWildcard(
                            qc,
                            hasJoins,
                            baseModel,
                            translatingModel,
                            innerVirtualModel,
                            windowModel,
                            groupByModel,
                            outerVirtualModel,
                            distinctModel
                    );
                } else {
                    if (explicitGroupBy) {
                        nonAggSelectCount++;
                        addMissingTablePrefixes(qc.getAst(), baseModel);
                        int matchingColIdx = findColumnIdxByAst(groupByNodes, qc.getAst());
                        if (matchingColIdx == -1) {
                            throw SqlException.$(qc.getAst().position, "column must appear in GROUP BY clause or aggregate function");
                        }

                        boolean sameAlias = createSelectColumn(
                                qc.getAlias(),
                                groupByAliases.get(matchingColIdx),
                                groupByModel,
                                outerVirtualModel,
                                useDistinctModel ? distinctModel : null
                        );
                        if (sameAlias && i == matchingColIdx) {
                            groupByUsed.set(matchingColIdx, true);
                        } else {
                            useOuterModel = true;
                        }
                    } else {
                        // groupByModel is populated in createSelectColumn.
                        // groupByModel must be used as it is the only model that is populated with duplicate column names in createSelectColumn.
                        // The below if-statement will only evaluate to true when using wildcards in a join with duplicate column names.
                        // Because the other column aliases are not known at the time qc's alias gets set, we must wait until this point
                        // (when we know the other column aliases) to alter it if a duplicate has occurred.
                        if (groupByModel.getAliasToColumnMap().contains(qc.getAlias())) {
                            CharSequence newAlias = createColumnAlias(qc.getAst(), groupByModel);
                            qc.setAlias(newAlias);
                        }
                        createSelectColumn(
                                qc.getAlias(),
                                qc.getAst(),
                                false,
                                baseModel,
                                translatingModel,
                                innerVirtualModel,
                                windowModel,
                                groupByModel,
                                outerVirtualModel,
                                useDistinctModel ? distinctModel : null
                        );
                    }
                }
            } else if (qc.getAst().type == ExpressionNode.BIND_VARIABLE) {
                if (explicitGroupBy) {
                    useOuterModel = true;
                    outerVirtualIsSelectChoose = false;
                    outerVirtualModel.addBottomUpColumn(qc);
                    distinctModel.addBottomUpColumn(qc);
                } else {
                    addFunction(
                            qc,
                            baseModel,
                            translatingModel,
                            innerVirtualModel,
                            windowModel,
                            groupByModel,
                            outerVirtualModel,
                            distinctModel
                    );
                }
            } else {
                // when column is direct call to aggregation function, such as
                // select sum(x) ...
                // we can add it to group-by model right away
                if (qc.getAst().type == ExpressionNode.FUNCTION) {
                    if (window) {
                        windowModel.addBottomUpColumn(qc);

                        QueryColumn ref = nextColumn(qc.getAlias());
                        outerVirtualModel.addBottomUpColumn(ref);
                        distinctModel.addBottomUpColumn(ref);
                        // ensure literals referenced by window column are present in nested models
                        emitLiterals(qc.getAst(), translatingModel, innerVirtualModel, baseModel, true);
                        continue;
                    } else if (functionParser.getFunctionFactoryCache().isGroupBy(qc.getAst().token)) {
                        addMissingTablePrefixes(qc.getAst(), baseModel);
                        CharSequence matchingCol = findColumnByAst(groupByNodes, groupByAliases, qc.getAst());
                        if (useOuterModel && matchingCol != null) {
                            QueryColumn ref = nextColumn(qc.getAlias(), matchingCol);
                            ref = ensureAliasUniqueness(outerVirtualModel, ref);
                            outerVirtualModel.addBottomUpColumn(ref);
                            distinctModel.addBottomUpColumn(ref);
                            emitLiterals(qc.getAst(), translatingModel, innerVirtualModel, baseModel, false);
                            continue;
                        }

                        qc = ensureAliasUniqueness(groupByModel, qc);
                        groupByModel.addBottomUpColumn(qc);

                        groupByNodes.add(deepClone(expressionNodePool, qc.getAst()));
                        groupByAliases.add(qc.getAlias());

                        // group-by column references might be needed when we have
                        // outer model supporting arithmetic such as:
                        // select sum(a)+sum(b) ...
                        QueryColumn ref = nextColumn(qc.getAlias());
                        outerVirtualModel.addBottomUpColumn(ref);
                        distinctModel.addBottomUpColumn(ref);
                        emitLiterals(qc.getAst(), translatingModel, innerVirtualModel, baseModel, false);
                        continue;
                    } else if (functionParser.getFunctionFactoryCache().isCursor(qc.getAst().token)) {
                        addCursorFunctionAsCrossJoin(
                                qc.getAst(),
                                qc.getAlias(),
                                cursorModel,
                                innerVirtualModel,
                                translatingModel,
                                baseModel,
                                sqlExecutionContext,
                                sqlParserCallback
                        );
                        continue;
                    }
                }

                if (explicitGroupBy) {
                    nonAggSelectCount++;
                    if (isEffectivelyConstantExpression(qc.getAst())) {
                        useOuterModel = true;
                        outerVirtualIsSelectChoose = false;
                        outerVirtualModel.addBottomUpColumn(qc);
                        distinctModel.addBottomUpColumn(qc);
                        continue;
                    }

                    addMissingTablePrefixes(qc.getAst(), baseModel);
                    final int beforeSplit = groupByModel.getBottomUpColumns().size();
                    // if there is explicit GROUP BY clause then we've to replace matching expressions with aliases in  outer virtual model
                    ExpressionNode en = rewriteGroupBySelectExpression(qc.getAst(), groupByModel, groupByNodes, groupByAliases);
                    if (qc.getAst() == en) {
                        useOuterModel = true;
                    } else {
                        if (Chars.equals(qc.getAst().token, qc.getAlias())) {
                            int idx = groupByAliases.indexOf(qc.getAst().token);
                            if (i != idx) {
                                useOuterModel = true;
                            }
                            groupByUsed.set(idx, true);
                        } else {
                            useOuterModel = true;
                        }
                        qc.of(qc.getAlias(), en, qc.isIncludeIntoWildcard(), qc.getColumnType());
                    }

                    emitCursors(qc.getAst(), cursorModel, innerVirtualModel, translatingModel, baseModel, sqlExecutionContext, sqlParserCallback);
                    qc = ensureAliasUniqueness(outerVirtualModel, qc);
                    outerVirtualModel.addBottomUpColumn(qc);
                    distinctModel.addBottomUpColumn(nextColumn(qc.getAlias()));

                    for (int j = beforeSplit, n = groupByModel.getBottomUpColumns().size(); j < n; j++) {
                        emitLiterals(groupByModel.getBottomUpColumns().getQuick(j).getAst(), translatingModel, innerVirtualModel, baseModel, false);
                    }
                    continue;
                }

                // this is not a direct call to aggregation function, in which case
                // we emit aggregation function into group-by model and leave the rest in outer model
                final int beforeSplit = groupByModel.getBottomUpColumns().size();
                if (checkForAggregates(qc.getAst())) {
                    // push aggregates and literals outside aggregate functions
                    emitAggregatesAndLiterals(qc.getAst(), groupByModel, translatingModel, innerVirtualModel, baseModel, groupByNodes, groupByAliases);
                    emitCursors(qc.getAst(), cursorModel, innerVirtualModel, translatingModel, baseModel, sqlExecutionContext, sqlParserCallback);
                    qc = ensureAliasUniqueness(outerVirtualModel, qc);
                    outerVirtualModel.addBottomUpColumn(qc);
                    distinctModel.addBottomUpColumn(nextColumn(qc.getAlias()));
                    for (int j = beforeSplit, n = groupByModel.getBottomUpColumns().size(); j < n; j++) {
                        emitLiterals(groupByModel.getBottomUpColumns().getQuick(j).getAst(), translatingModel, innerVirtualModel, baseModel, false);
                    }
                } else {
                    if (emitCursors(qc.getAst(), cursorModel, null, translatingModel, baseModel, sqlExecutionContext, sqlParserCallback)) {
                        qc = ensureAliasUniqueness(innerVirtualModel, qc);
                    }
                    if (useGroupByModel) {
                        if (isEffectivelyConstantExpression(qc.getAst())) {
                            outerVirtualIsSelectChoose = false;
                            outerVirtualModel.addBottomUpColumn(qc);
                            distinctModel.addBottomUpColumn(qc);
                            continue;
                        }
                    }

                    addFunction(
                            qc,
                            baseModel,
                            translatingModel,
                            innerVirtualModel,
                            windowModel,
                            groupByModel,
                            outerVirtualModel,
                            distinctModel
                    );
                }
            }
        }

        if (explicitGroupBy && !useOuterModel && (nonAggSelectCount != groupBy.size() || groupByUsed.getTrueCount() != groupBy.size())) {
            useOuterModel = true;
        }

        // fail if we have both window and group-by models
        if (useWindowModel && useGroupByModel) {
            throw SqlException.$(0, "Window function is not allowed in context of aggregation. Use sub-query.");
        }

        boolean forceTranslatingModel = false;
        if (useWindowModel) {
            // We need one more pass for window model to emit potentially missing columns.
            // For example, 'SELECT row_number() over (partition by col_c order by col_c), col_a, col_b FROM tab'
            // needs col_c to be emitted.
            for (int i = 0, k = columns.size(); i < k; i++) {
                QueryColumn qc = columns.getQuick(i);
                final boolean window = qc.isWindowColumn();

                if (window & qc.getAst().type == ExpressionNode.FUNCTION) {
                    // Window model can be after either translation model directly
                    // or after inner virtual model, which can be sandwiched between
                    // translation model and window model.
                    // To make sure columns, referenced by the window model
                    // are rendered correctly we will emit them into a dedicated
                    // translation model for the window model.
                    // When we're able to determine which combination of models precedes the
                    // window model, we can copy columns from window_translation model to
                    // either only to translation model or both translation model and the
                    // inner virtual models.
                    final WindowColumn ac = (WindowColumn) qc;
                    int innerColumnsPre = innerVirtualModel.getBottomUpColumns().size();
                    replaceLiteralList(innerVirtualModel, translatingModel, baseModel, ac.getPartitionBy());
                    replaceLiteralList(innerVirtualModel, translatingModel, baseModel, ac.getOrderBy());
                    int innerColumnsPost = innerVirtualModel.getBottomUpColumns().size();
                    // window model might require columns it doesn't explicitly contain (e.g. used for order by or partition by  in over() clause  )
                    // skipping translating model will trigger 'invalid column' exceptions
                    forceTranslatingModel |= innerColumnsPre != innerColumnsPost;
                }
            }
        }

        if (useInnerModel) {
            final ObjList<QueryColumn> innerColumns = innerVirtualModel.getBottomUpColumns();
            useInnerModel = false;
            boolean columnsAndFunctionsOnly = true;
            // hour(column) is the only function key in supported by Rosti, so we need to detect it
            int hourFunctionKeyCount = 0;
            int totalFunctionKeyCount = 0;
            for (int i = 0, k = innerColumns.size(); i < k; i++) {
                QueryColumn qc = innerColumns.getQuick(i);
                if (qc.getAst().type != LITERAL) {
                    useInnerModel = true;
                }
                if (qc.getAst().type != LITERAL && qc.getAst().type != FUNCTION && qc.getAst().type != OPERATION) {
                    columnsAndFunctionsOnly = false;
                }
                if (qc.getAst().type == FUNCTION
                        && isHourKeyword(qc.getAst().token) && qc.getAst().paramCount == 1 && qc.getAst().rhs.type == LITERAL) {
                    hourFunctionKeyCount++;
                }
                if (qc.getAst().type == FUNCTION || qc.getAst().type == OPERATION) {
                    totalFunctionKeyCount++;
                }
            }
            boolean singleHourFunctionKey = totalFunctionKeyCount == 1 && hourFunctionKeyCount == 1;
            if (useInnerModel
                    && useGroupByModel && groupByModel.getSampleBy() == null
                    && columnsAndFunctionsOnly && !singleHourFunctionKey
                    && SqlUtil.isPlainSelect(baseModel)) {
                // we can "steal" all keys from inner model in case of group-by
                // this is necessary in case of further parallel execution
                mergeInnerVirtualModel(innerVirtualModel, groupByModel);
                useInnerModel = false;
            }
        }

        // check if translating model is redundant, e.g.
        // that it neither chooses between tables nor renames columns
        boolean translationIsRedundant = (useInnerModel || useGroupByModel || useWindowModel) && !forceTranslatingModel;
        if (translationIsRedundant) {
            for (int i = 0, n = translatingModel.getBottomUpColumns().size(); i < n; i++) {
                QueryColumn column = translatingModel.getBottomUpColumns().getQuick(i);
                if (!column.getAst().token.equals(column.getAlias())) {
                    translationIsRedundant = false;
                    break;
                }
            }
        }

        if (sampleBy != null && baseModel.getTimestamp() != null) {
            CharSequence timestamp = baseModel.getTimestamp().token;
            // does model already select timestamp column?
            if (innerVirtualModel.getColumnNameToAliasMap().excludes(timestamp)) {
                // no, do we rename columns? does model select timestamp under a new name?
                if (translationIsRedundant) {
                    // columns were not renamed
                    createSelectColumn0(
                            baseModel.getTimestamp().token,
                            baseModel.getTimestamp(),
                            baseModel,
                            translatingModel,
                            innerVirtualModel,
                            windowModel
                    );
                } else {
                    // columns were renamed,
                    if (translatingModel.getColumnNameToAliasMap().excludes(timestamp)) {
                        // make alias name
                        CharacterStoreEntry e = characterStore.newEntry();
                        e.put(baseModel.getName()).put('.').put(timestamp);
                        if (translatingModel.getColumnNameToAliasMap().excludes(e.toImmutable())) {
                            createSelectColumn0(
                                    baseModel.getTimestamp().token,
                                    baseModel.getTimestamp(),
                                    baseModel,
                                    translatingModel,
                                    innerVirtualModel,
                                    windowModel
                            );
                        }
                    }
                }
            }
        }

        QueryModel root;
        QueryModel limitSource;

        if (translationIsRedundant) {
            root = baseModel;
            limitSource = model;
        } else {
            root = translatingModel;
            limitSource = translatingModel;
            translatingModel.setNestedModel(baseModel);

            // translating model has limits to ensure clean factory separation
            // during code generation. However, in some cases limit could also
            // be implemented by nested model. Nested model must not implement limit
            // when parent model is order by or join.
            // Only exception is when order by is by designated timestamp because it'll be implemented as forward or backward scan (no sorting required) .
            if ((baseModel.getOrderBy().size() == 0 || isOrderedByDesignatedTimestamp(baseModel)) && !useDistinctModel) {
                baseModel.setLimitAdvice(model.getLimitLo(), model.getLimitHi());
            }

            translatingModel.moveLimitFrom(model);
            translatingModel.moveJoinAliasFrom(model);
            translatingModel.setSelectTranslation(true);
        }

        if (useInnerModel) {
            innerVirtualModel.setNestedModel(root);
            innerVirtualModel.moveLimitFrom(limitSource);
            innerVirtualModel.moveJoinAliasFrom(limitSource);
            root = innerVirtualModel;
            limitSource = innerVirtualModel;
        }

        if (useWindowModel) {
            windowModel.setNestedModel(root);
            windowModel.moveLimitFrom(limitSource);
            windowModel.moveJoinAliasFrom(limitSource);
            root = windowModel;
            limitSource = windowModel;
        } else if (useGroupByModel) {
            groupByModel.setNestedModel(root);
            groupByModel.moveLimitFrom(limitSource);
            groupByModel.moveJoinAliasFrom(limitSource);
            root = groupByModel;
            limitSource = groupByModel;
        }

        if (useOuterModel) {
            outerVirtualModel.setNestedModel(root);
            outerVirtualModel.moveLimitFrom(limitSource);
            outerVirtualModel.moveJoinAliasFrom(limitSource);
            root = outerVirtualModel;
        } else if (root != outerVirtualModel && root.getBottomUpColumns().size() < outerVirtualModel.getBottomUpColumns().size()) {
            outerVirtualModel.setNestedModel(root);
            outerVirtualModel.moveLimitFrom(limitSource);
            outerVirtualModel.moveJoinAliasFrom(limitSource);
            outerVirtualModel.setSelectModelType(outerVirtualIsSelectChoose ? QueryModel.SELECT_MODEL_CHOOSE : QueryModel.SELECT_MODEL_VIRTUAL);
            root = outerVirtualModel;
        }

        if (useDistinctModel) {
            distinctModel.setNestedModel(root);
            distinctModel.moveLimitFrom(root);
            root = distinctModel;
        }

        if (!useGroupByModel && groupByModel.getSampleBy() != null) {
            throw SqlException.$(groupByModel.getSampleBy().position, "at least one aggregation function must be present in 'select' clause");
        }

        if (model != root) {
            root.setUnionModel(model.getUnionModel());
            root.setSetOperationType(model.getSetOperationType());
            root.setModelPosition(model.getModelPosition());
            if (model.isUpdate()) {
                root.setIsUpdate(true);
                root.copyUpdateTableMetadata(model);
            }
        }
        return root;
    }

    // the intent is to either validate top-level columns in select columns or replace them with function calls
    // if columns do not exist
    private void rewriteTopLevelLiteralsToFunctions(QueryModel model) {
        final QueryModel nested = model.getNestedModel();
        if (nested != null) {
            rewriteTopLevelLiteralsToFunctions(nested);
            final ObjList<QueryColumn> columns = model.getColumns();
            final int n = columns.size();
            if (n > 0) {
                for (int i = 0; i < n; i++) {
                    final QueryColumn qc = columns.getQuick(i);
                    final ExpressionNode node = qc.getAst();
                    if (node.type == LITERAL) {
                        if (nested.getAliasToColumnMap().contains(node.token)) {
                            continue;
                        }

                        if (functionParser.getFunctionFactoryCache().isValidNoArgFunction(node)) {
                            node.type = FUNCTION;
                        }
                    } else {
                        model.addField(qc);
                    }
                }
            } else {
                model.copyColumnsFrom(nested, queryColumnPool, expressionNodePool);
            }
        }
    }

    private CharSequence setAndGetModelAlias(QueryModel model) {
        CharSequence name = model.getName();
        if (name != null) {
            return name;
        }
        ExpressionNode alias = makeJoinAlias();
        model.setAlias(alias);
        return alias.token;
    }

    private QueryModel skipNoneTypeModels(QueryModel model) {
        while (
                model != null
                        && model.getSelectModelType() == QueryModel.SELECT_MODEL_NONE
                        && model.getTableName() == null
                        && model.getTableNameFunction() == null
                        && model.getJoinModels().size() == 1
                        && model.getWhereClause() == null
                        && model.getLatestBy().size() == 0
        ) {
            model = model.getNestedModel();
        }
        return model;
    }

    /**
     * Moves reversible join clauses, such as a.x = b.x from table "from" to table "to".
     *
     * @param to      target table index
     * @param from    source table index
     * @param context context of target table index
     * @return false if "from" is outer joined table, otherwise - true
     */
    private boolean swapJoinOrder(QueryModel parent, int to, int from, final JoinContext context) {
        ObjList<QueryModel> joinModels = parent.getJoinModels();
        QueryModel jm = joinModels.getQuick(from);
        if (joinBarriers.contains(jm.getJoinType())) {
            return false;
        }

        final JoinContext that = jm.getContext();
        if (that != null && that.parents.contains(to)) {
            swapJoinOrder0(parent, jm, to, context);
        }
        return true;
    }

    private void swapJoinOrder0(QueryModel parent, QueryModel jm, int to, JoinContext jc) {
        final JoinContext that = jm.getContext();
        clausesToSteal.clear();
        int zc = that.aIndexes.size();
        for (int z = 0; z < zc; z++) {
            if (that.aIndexes.getQuick(z) == to || that.bIndexes.getQuick(z) == to) {
                clausesToSteal.add(z);
            }
        }

        // we check that parent contains "to", so we must have something to do
        assert clausesToSteal.size() > 0;

        if (clausesToSteal.size() < zc) {
            QueryModel target = parent.getJoinModels().getQuick(to);
            if (jc == null) {
                target.setContext(jc = contextPool.next());
            }
            jc.slaveIndex = to;
            jm.setContext(moveClauses(parent, that, jc, clausesToSteal));
            if (target.getJoinType() == QueryModel.JOIN_CROSS) {
                target.setJoinType(QueryModel.JOIN_INNER);
            }
        }
    }

    private void traverseNamesAndIndices(QueryModel parent, ExpressionNode node) throws SqlException {
        literalCollectorAIndexes.clear();
        literalCollectorBIndexes.clear();

        literalCollectorANames.clear();
        literalCollectorBNames.clear();

        literalCollector.withModel(parent);
        literalCollector.resetCounts();
        traversalAlgo.traverse(node.lhs, literalCollector.lhs());
        traversalAlgo.traverse(node.rhs, literalCollector.rhs());
    }

    private int validateColumnAndGetModelIndex(QueryModel model, CharSequence columnName, int dot, int position) throws SqlException {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        int index = -1;
        if (dot == -1) {
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                if (joinModels.getQuick(i).getAliasToColumnMap().excludes(columnName)) {
                    continue;
                }

                if (index != -1) {
                    throw SqlException.ambiguousColumn(position, columnName);
                }

                index = i;
            }

            if (index == -1) {
                throw SqlException.invalidColumn(position, columnName);
            }

        } else {
            index = model.getModelAliasIndex(columnName, 0, dot);

            if (index == -1) {
                throw SqlException.$(position, "Invalid table name or alias");
            }

            if (joinModels.getQuick(index).getAliasToColumnMap().excludes(columnName, dot + 1, columnName.length())) {
                throw SqlException.invalidColumn(position, columnName);
            }

        }
        return index;
    }

    /* Throws exception if given node tree contains reference to aggregate or window function that are not allowed in GROUP BY clause. */
    private void validateGroupByExpression(@Transient ExpressionNode node, QueryModel model, int originalNodePosition) throws SqlException {
        try {
            checkIsNotAggregateOrWindowFunction(node, model);

            sqlNodeStack.clear();
            while (node != null) {
                if (node.paramCount < 3) {
                    if (node.rhs != null) {
                        checkIsNotAggregateOrWindowFunction(node.rhs, model);
                        sqlNodeStack.push(node.rhs);
                    }

                    if (node.lhs != null) {
                        checkIsNotAggregateOrWindowFunction(node.lhs, model);
                        node = node.lhs;
                    } else {
                        if (!sqlNodeStack.isEmpty()) {
                            node = sqlNodeStack.poll();
                        } else {
                            node = null;
                        }
                    }
                } else {
                    for (int i = 1, k = node.paramCount; i < k; i++) {
                        ExpressionNode e = node.args.getQuick(i);
                        checkIsNotAggregateOrWindowFunction(e, model);
                        sqlNodeStack.push(e);
                    }

                    ExpressionNode e = node.args.getQuick(0);
                    checkIsNotAggregateOrWindowFunction(e, model);
                    node = e;
                }
            }
        } catch (SqlException sqle) {
            if (originalNodePosition > -1) {
                sqle.setPosition(originalNodePosition);
            }
            throw sqle;
        }
    }

    private void validateWindowFunctions(QueryModel model, SqlExecutionContext sqlExecutionContext, int recursionLevel) throws SqlException {
        if (model == null) {
            return;
        }

        if (recursionLevel > maxRecursion) {
            throw SqlException.$(0, "SQL model is too complex to evaluate");
        }

        if (model.getSelectModelType() == QueryModel.SELECT_MODEL_WINDOW) {
            final ObjList<QueryColumn> queryColumns = model.getColumns();
            for (int i = 0, n = queryColumns.size(); i < n; i++) {
                QueryColumn qc = queryColumns.getQuick(i);
                if (qc.isWindowColumn()) {
                    final WindowColumn ac = (WindowColumn) qc;
                    // preceding and following accept non-negative values only
                    long rowsLo = evalNonNegativeLongConstantOrDie(ac.getRowsLoExpr(), sqlExecutionContext);
                    long rowsHi = evalNonNegativeLongConstantOrDie(ac.getRowsHiExpr(), sqlExecutionContext);

                    switch (ac.getRowsLoKind()) {
                        case WindowColumn.PRECEDING:
                            rowsLo = rowsLo != Long.MAX_VALUE ? -rowsLo : Long.MIN_VALUE;
                            break;
                        case WindowColumn.FOLLOWING:
                            //rowsLo = rowsLo;
                            break;
                        default:
                            // CURRENT ROW
                            rowsLo = 0;
                            break;
                    }

                    switch (ac.getRowsHiKind()) {
                        case WindowColumn.PRECEDING:
                            rowsHi = rowsHi != Long.MAX_VALUE ? -rowsHi : Long.MIN_VALUE;
                            break;
                        case WindowColumn.FOLLOWING:
                            //rowsHi = rowsHi;
                            break;
                        default:
                            // CURRENT ROW
                            rowsHi = 0;
                            break;
                    }

                    ac.setRowsLo(rowsLo * ac.getRowsLoExprTimeUnit());
                    ac.setRowsHi(rowsHi * ac.getRowsHiExprTimeUnit());

                    if (rowsLo > rowsHi) {
                        throw SqlException.$(ac.getRowsLoExpr().position, "start of window must be lower than end of window");
                    }
                }
            }
        }

        validateWindowFunctions(model.getNestedModel(), sqlExecutionContext, recursionLevel + 1);

        // join models
        for (int i = 1, n = model.getJoinModels().size(); i < n; i++) {
            validateWindowFunctions(model.getJoinModels().getQuick(i), sqlExecutionContext, recursionLevel + 1);
        }

        validateWindowFunctions(model.getUnionModel(), sqlExecutionContext, recursionLevel + 1);
    }

    protected void authorizeColumnAccess(SqlExecutionContext executionContext, QueryModel model) {
    }

    protected void authorizeUpdate(QueryModel updateQueryModel, TableToken token) {
    }

    QueryModel optimise(
            @Transient final QueryModel model,
            @Transient SqlExecutionContext sqlExecutionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        QueryModel rewrittenModel = model;
        try {
            rewrittenModel = bubbleUpOrderByAndLimitFromUnion(rewrittenModel);
            optimiseExpressionModels(rewrittenModel, sqlExecutionContext, sqlParserCallback);
            enumerateTableColumns(rewrittenModel, sqlExecutionContext, sqlParserCallback);
            rewriteTopLevelLiteralsToFunctions(rewrittenModel);
            rewrittenModel = moveOrderByFunctionsIntoOuterSelect(rewrittenModel);
            resolveJoinColumns(rewrittenModel);
            optimiseBooleanNot(rewrittenModel);
            rewrittenModel = rewriteSelectClause(rewrittenModel, true, sqlExecutionContext, sqlParserCallback);
            optimiseJoins(rewrittenModel);
            rewriteCountDistinct(rewrittenModel);
            rewriteNegativeLimit(rewrittenModel, sqlExecutionContext);
            rewriteOrderByPosition(rewrittenModel);
            rewriteOrderByPositionForUnionModels(rewrittenModel);
            rewrittenModel = rewriteOrderBy(rewrittenModel);
            optimiseOrderBy(rewrittenModel, OrderByMnemonic.ORDER_BY_UNKNOWN);
            createOrderHash(rewrittenModel);
            moveWhereInsideSubQueries(rewrittenModel);
            eraseColumnPrefixInWhereClauses(rewrittenModel);
            moveTimestampToChooseModel(rewrittenModel);
            propagateTopDownColumns(rewrittenModel, rewrittenModel.allowsColumnsChange());
            validateWindowFunctions(rewrittenModel, sqlExecutionContext, 0);
            authorizeColumnAccess(sqlExecutionContext, rewrittenModel);
            return rewrittenModel;
        } catch (Throwable e) {
            // at this point models may have functions than need to be freed
            Misc.freeObjListAndClear(tableFactoriesInFlight);
            throw e;
        }
    }

    void optimiseUpdate(
            QueryModel updateQueryModel,
            SqlExecutionContext sqlExecutionContext,
            TableRecordMetadata metadata,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        final QueryModel selectQueryModel = updateQueryModel.getNestedModel();
        selectQueryModel.setIsUpdate(true);
        QueryModel optimisedNested = optimise(selectQueryModel, sqlExecutionContext, sqlParserCallback);
        assert optimisedNested.isUpdate();
        updateQueryModel.setNestedModel(optimisedNested);

        // And then generate plan for UPDATE top level QueryModel
        validateUpdateColumns(updateQueryModel, metadata, sqlExecutionContext);
    }

    void validateUpdateColumns(QueryModel updateQueryModel, TableRecordMetadata metadata, SqlExecutionContext sqlExecutionContext) throws SqlException {
        try {
            literalCollectorANames.clear();
            tempList.clear(metadata.getColumnCount());
            tempList.setPos(metadata.getColumnCount());
            int timestampIndex = metadata.getTimestampIndex();
            int updateSetColumnCount = updateQueryModel.getUpdateExpressions().size();
            for (int i = 0; i < updateSetColumnCount; i++) {

                // SET left hand side expressions are stored in top level UPDATE QueryModel
                ExpressionNode columnExpression = updateQueryModel.getUpdateExpressions().get(i);
                int position = columnExpression.position;
                int columnIndex = metadata.getColumnIndexQuiet(columnExpression.token);

                // SET right hand side expressions are stored in the Nested SELECT QueryModel as columns
                QueryColumn queryColumn = updateQueryModel.getNestedModel().getColumns().get(i);
                if (columnIndex < 0) {
                    throw SqlException.invalidColumn(position, queryColumn.getName());
                }
                if (columnIndex == timestampIndex) {
                    throw SqlException.$(position, "Designated timestamp column cannot be updated");
                }
                if (tempList.getQuick(columnIndex) == 1) {
                    throw SqlException.$(position, "Duplicate column ").put(queryColumn.getName()).put(" in SET clause");
                }

                // When column name case does not match table column name in left side of SET
                // for example if table "tbl" column name is "Col" but update uses
                // UPDATE tbl SET coL = 1
                // we need to replace to match metadata name exactly
                CharSequence exactColName = metadata.getColumnName(columnIndex);
                queryColumn.of(exactColName, queryColumn.getAst());
                tempList.set(columnIndex, 1);
                literalCollectorANames.add(exactColName);

                ExpressionNode rhs = queryColumn.getAst();
                if (rhs.type == FUNCTION) {
                    if (functionParser.getFunctionFactoryCache().isGroupBy(rhs.token)) {
                        throw SqlException.$(rhs.position, "Unsupported function in SET clause");
                    }
                }
            }

            TableToken tableToken = metadata.getTableToken();
            sqlExecutionContext.getSecurityContext().authorizeTableUpdate(tableToken, literalCollectorANames);

            if (!sqlExecutionContext.isWalApplication() && !Chars.equalsIgnoreCase(tableToken.getTableName(), updateQueryModel.getTableName())) {
                // Table renamed
                throw TableReferenceOutOfDateException.of(updateQueryModel.getTableName());
            }
            updateQueryModel.setUpdateTableToken(tableToken);
            authorizeUpdate(updateQueryModel, tableToken);
        } catch (EntryLockedException e) {
            throw SqlException.position(updateQueryModel.getModelPosition()).put("table is locked: ").put(tableLookupSequence);
        } catch (CairoException e) {
            if (e.isAuthorizationError()) {
                throw e;
            }
            throw SqlException.position(updateQueryModel.getModelPosition()).put(e);
        }
    }

    private static class LiteralCheckingVisitor implements PostOrderTreeTraversalAlgo.Visitor {
        private LowerCaseCharSequenceObjHashMap<QueryColumn> nameTypeMap;

        @Override
        public void visit(ExpressionNode node) {
            if (node.type == LITERAL) {
                final int len = node.token.length();
                final int dot = Chars.indexOf(node.token, 0, len, '.');
                int index = nameTypeMap.keyIndex(node.token, dot + 1, len);
                // these columns are pre-validated
                assert index < 0;
                if (nameTypeMap.valueAt(index).getAst().type != LITERAL) {
                    throw NonLiteralException.INSTANCE;
                }
            }
        }

        PostOrderTreeTraversalAlgo.Visitor of(LowerCaseCharSequenceObjHashMap<QueryColumn> nameTypeMap) {
            this.nameTypeMap = nameTypeMap;
            return this;
        }
    }

    private static class LiteralRewritingVisitor implements PostOrderTreeTraversalAlgo.Visitor {
        private LowerCaseCharSequenceObjHashMap<CharSequence> aliasToColumnMap;

        @Override
        public void visit(ExpressionNode node) {
            if (node.type == LITERAL) {
                int dot = Chars.indexOf(node.token, '.');
                int index = dot == -1 ? aliasToColumnMap.keyIndex(node.token) : aliasToColumnMap.keyIndex(node.token, dot + 1, node.token.length());
                // we have table column hit when alias is not found
                // in this case expression rewrite is unnecessary
                if (index < 0) {
                    CharSequence column = aliasToColumnMap.valueAtQuick(index);
                    assert column != null;
                    // it is also unnecessary to rewrite literal if target value is the same
                    if (!Chars.equals(node.token, column)) {
                        node.token = column;
                    }
                }
            }
        }

        PostOrderTreeTraversalAlgo.Visitor of(LowerCaseCharSequenceObjHashMap<CharSequence> aliasToColumnMap) {
            this.aliasToColumnMap = aliasToColumnMap;
            return this;
        }
    }

    private static class NonLiteralException extends RuntimeException {
        private static final NonLiteralException INSTANCE = new NonLiteralException();
    }

    private class ColumnPrefixEraser implements PostOrderTreeTraversalAlgo.Visitor {

        @Override
        public void visit(ExpressionNode node) {
            switch (node.type) {
                case ExpressionNode.FUNCTION:
                case ExpressionNode.OPERATION:
                case ExpressionNode.SET_OPERATION:
                    if (node.paramCount < 3) {
                        node.lhs = rewrite(node.lhs);
                        node.rhs = rewrite(node.rhs);
                    } else {
                        for (int i = 0, n = node.paramCount; i < n; i++) {
                            node.args.setQuick(i, rewrite(node.args.getQuick(i)));
                        }
                    }
                    break;
                default:
                    break;
            }
        }

        private ExpressionNode rewrite(ExpressionNode node) {
            if (node != null && node.type == LITERAL) {
                final int dot = Chars.indexOf(node.token, '.');
                if (dot != -1) {
                    return nextLiteral(node.token.subSequence(dot + 1, node.token.length()));
                }
            }
            return node;
        }
    }

    private class LiteralCollector implements PostOrderTreeTraversalAlgo.Visitor {
        private int functionCount;
        private IntHashSet indexes;
        private QueryModel model;
        private ObjList<CharSequence> names;
        private int nullCount;

        @Override
        public void visit(ExpressionNode node) throws SqlException {
            switch (node.type) {
                case LITERAL:
                    int dot = Chars.indexOf(node.token, '.');
                    CharSequence name = dot == -1 ? node.token : node.token.subSequence(dot + 1, node.token.length());
                    indexes.add(validateColumnAndGetModelIndex(model, node.token, dot, node.position));
                    if (names != null) {
                        names.add(name);
                    }
                    break;
                case ExpressionNode.CONSTANT:
                    if (nullConstants.contains(node.token)) {
                        nullCount++;
                    }
                    break;
                case FUNCTION:
                case OPERATION:
                    functionCount++;
                    break;
                default:
                    break;
            }
        }

        private PostOrderTreeTraversalAlgo.Visitor lhs() {
            indexes = literalCollectorAIndexes;
            names = literalCollectorANames;
            return this;
        }

        private void resetCounts() {
            nullCount = 0;
            functionCount = 0;
        }

        private PostOrderTreeTraversalAlgo.Visitor rhs() {
            indexes = literalCollectorBIndexes;
            names = literalCollectorBNames;
            return this;
        }

        private PostOrderTreeTraversalAlgo.Visitor to(IntHashSet indexes) {
            this.indexes = indexes;
            this.names = null;
            return this;
        }

        private void withModel(QueryModel model) {
            this.model = model;
        }
    }

    static {
        notOps.put("not", NOT_OP_NOT);
        notOps.put("and", NOT_OP_AND);
        notOps.put("or", NOT_OP_OR);
        notOps.put(">", NOT_OP_GREATER);
        notOps.put(">=", NOT_OP_GREATER_EQ);
        notOps.put("<", NOT_OP_LESS);
        notOps.put("<=", NOT_OP_LESS_EQ);
        notOps.put("=", NOT_OP_EQUAL);
        notOps.put("!=", NOT_OP_NOT_EQ);
        notOps.put("<>", NOT_OP_NOT_EQ);

        joinBarriers = new IntHashSet();
        joinBarriers.add(QueryModel.JOIN_OUTER);
        joinBarriers.add(QueryModel.JOIN_CROSS_LEFT);
        joinBarriers.add(QueryModel.JOIN_ASOF);
        joinBarriers.add(QueryModel.JOIN_SPLICE);
        joinBarriers.add(QueryModel.JOIN_LT);

        nullConstants.add("null");
        nullConstants.add("NaN");

        joinOps.put("=", JOIN_OP_EQUAL);
        joinOps.put("and", JOIN_OP_AND);
        joinOps.put("or", JOIN_OP_OR);
        joinOps.put("~", JOIN_OP_REGEX);

        flexColumnModelTypes.add(QueryModel.SELECT_MODEL_CHOOSE);
        flexColumnModelTypes.add(QueryModel.SELECT_MODEL_NONE);
        flexColumnModelTypes.add(QueryModel.SELECT_MODEL_DISTINCT);
        flexColumnModelTypes.add(QueryModel.SELECT_MODEL_VIRTUAL);
        flexColumnModelTypes.add(QueryModel.SELECT_MODEL_WINDOW);
        flexColumnModelTypes.add(QueryModel.SELECT_MODEL_GROUP_BY);
    }

    static {
        limitTypes.add(ColumnType.LONG);
        limitTypes.add(ColumnType.BYTE);
        limitTypes.add(ColumnType.SHORT);
        limitTypes.add(ColumnType.INT);
    }
}
