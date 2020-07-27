/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.model.*;
import io.questdb.std.*;
import io.questdb.std.str.FlyweightCharSequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

class SqlOptimiser {

    private static final CharSequenceIntHashMap notOps = new CharSequenceIntHashMap();
    private static final boolean[] joinsRequiringTimestamp = {false, false, false, false, true, true, true};
    private static final int NOT_OP_NOT = 1;
    private static final int NOT_OP_AND = 2;
    private static final int NOT_OP_OR = 3;
    private static final int NOT_OP_GREATER = 4;
    private static final int NOT_OP_GREATER_EQ = 5;
    private static final int NOT_OP_LESS = 6;
    private static final int NOT_OP_LESS_EQ = 7;
    private static final int NOT_OP_EQUAL = 8;
    private static final int NOT_OP_NOT_EQ = 9;
    private final static IntHashSet joinBarriers;
    private final static CharSequenceHashSet nullConstants = new CharSequenceHashSet();
    private static final CharSequenceIntHashMap joinOps = new CharSequenceIntHashMap();
    private static final int JOIN_OP_EQUAL = 1;
    private static final int JOIN_OP_AND = 2;
    private static final int JOIN_OP_OR = 3;
    private static final int JOIN_OP_REGEX = 4;
    private static final IntHashSet flexColumnModelTypes = new IntHashSet();

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
        flexColumnModelTypes.add(QueryModel.SELECT_MODEL_VIRTUAL);
        flexColumnModelTypes.add(QueryModel.SELECT_MODEL_GROUP_BY);
    }

    private final CairoEngine engine;
    private final FlyweightCharSequence tableLookupSequence = new FlyweightCharSequence();
    private final ObjectPool<ExpressionNode> expressionNodePool;
    private final CharacterStore characterStore;
    private final ObjList<JoinContext> joinClausesSwap1 = new ObjList<>();
    private final ObjList<JoinContext> joinClausesSwap2 = new ObjList<>();
    private final IntList tempCrosses = new IntList();
    private final IntList tempList = new IntList();
    private final LiteralCollector literalCollector = new LiteralCollector();
    private final IntHashSet tablesSoFar = new IntHashSet();
    private final IntHashSet postFilterRemoved = new IntHashSet();
    private final IntList nullCounts = new IntList();
    private final ObjList<IntList> postFilterTableRefs = new ObjList<>();
    private final LiteralCheckingVisitor literalCheckingVisitor = new LiteralCheckingVisitor();
    private final LiteralRewritingVisitor literalRewritingVisitor = new LiteralRewritingVisitor();
    private final IntList literalCollectorAIndexes = new IntList();
    private final ObjList<CharSequence> literalCollectorANames = new ObjList<>();
    private final IntList literalCollectorBIndexes = new IntList();
    private final ObjList<CharSequence> literalCollectorBNames = new ObjList<>();
    private final PostOrderTreeTraversalAlgo traversalAlgo;
    private final ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
    private final ObjectPool<JoinContext> contextPool;
    private final IntHashSet deletedContexts = new IntHashSet();
    private final CharSequenceObjHashMap<CharSequence> constNameToToken = new CharSequenceObjHashMap<>();
    private final CharSequenceIntHashMap constNameToIndex = new CharSequenceIntHashMap();
    private final CharSequenceObjHashMap<ExpressionNode> constNameToNode = new CharSequenceObjHashMap<>();
    private final IntList tempCrossIndexes = new IntList();
    private final IntList clausesToSteal = new IntList();
    private final ObjectPool<IntList> intListPool = new ObjectPool<>(IntList::new, 16);
    private final ObjectPool<QueryModel> queryModelPool;
    private final IntPriorityQueue orderingStack = new IntPriorityQueue();
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final FunctionParser functionParser;
    private final ColumnPrefixEraser columnPrefixEraser = new ColumnPrefixEraser();
    private final Path path;
    private final ObjList<ExpressionNode> orderByAdvice = new ObjList<>();
    private int defaultAliasCount = 0;
    private ObjList<JoinContext> emittedJoinClauses;

    SqlOptimiser(
            CairoConfiguration configuration,
            CairoEngine engine,
            CharacterStore characterStore,
            ObjectPool<ExpressionNode> expressionNodePool,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<QueryModel> queryModelPool,
            PostOrderTreeTraversalAlgo traversalAlgo,
            FunctionParser functionParser,
            Path path
    ) {
        this.engine = engine;
        this.expressionNodePool = expressionNodePool;
        this.characterStore = characterStore;
        this.traversalAlgo = traversalAlgo;
        this.queryModelPool = queryModelPool;
        this.queryColumnPool = queryColumnPool;
        this.functionParser = functionParser;
        this.contextPool = new ObjectPool<>(JoinContext.FACTORY, configuration.getSqlJoinContextPoolCapacity());
        this.path = path;
    }

    private static void linkDependencies(QueryModel model, int parent, int child) {
        model.getJoinModels().getQuick(parent).addDependency(child);
    }

    private static void unlinkDependencies(QueryModel model, int parent, int child) {
        model.getJoinModels().getQuick(parent).removeDependency(child);
    }

    private static boolean isNotBindVariable(CharSequence token) {
        int len = token.length();
        if (len < 1) {
            return true;
        }

        final char first = token.charAt(0);
        if (first == ':') {
            return false;
        }

        if (first == '?' && len == 1) {
            return false;
        }

        if (first == '$') {
            try {
                Numbers.parseInt(token, 1, len);
                return false;
            } catch (NumericException e) {
                return true;
            }
        }

        return true;
    }

    private static boolean modelIsFlex(QueryModel model) {
        return model != null && flexColumnModelTypes.contains(model.getSelectModelType());
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
            getIndexOfTableForColumn(validatingModel, refColumn, dot, column.getAst().position);
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
            QueryModel baseModel,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel analyticModel,
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
        emitLiterals(qc.getAst(), translatingModel, null, baseModel);
        groupByModel.addBottomUpColumn(innerColumn);
        analyticModel.addBottomUpColumn(innerColumn);
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

    private void addTopDownColumn(@Transient ExpressionNode node, QueryModel model) {
        if (node != null && node.type == ExpressionNode.LITERAL) {
            final ObjList<QueryModel> joinModels = model.getJoinModels();
            final int joinCount = joinModels.size();
            final CharSequence columnName = node.token;
            final int dotIndex = Chars.indexOf(columnName, '.');
            if (dotIndex == -1) {
                // When there is no dot in column name it is still possible that column comes from
                // one of the join models. What we need to do here is to assign column to that model
                // which already have this column in alias map
                for (int i = 0; i < joinCount; i++) {
                    final QueryModel m = joinModels.getQuick(i);
                    final QueryColumn column = m.getAliasToColumnMap().get(columnName);
                    if (column != null) {
                        m.addTopDownColumn(column, columnName);
                        break;
                    }
                }
            } else {
                int modelIndex = model.getAliasIndex(node.token, 0, dotIndex);
                if (modelIndex < 0) {
                    // alias cannot be resolved, we will trust that the calling side will handle this
                    // in this context we do not have model that is able to resolve table alias
                    return;
                }

                addTopDownColumn0(
                        node,
                        joinModels.getQuick(modelIndex),
                        node.token.subSequence(dotIndex + 1, node.token.length())
                );
            }
        }
    }

    private void addTopDownColumn0(@Transient ExpressionNode node, QueryModel model, CharSequence name) {
        if (model.isTopDownNameMissing(name)) {
            model.addTopDownColumn(
                    queryColumnPool.next().of(name, expressionNodePool.next().of(node.type, name, node.precedence, node.position))
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
     * this filter is not explicitly mentioned but it might help pre-filtering record sources
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

    private void analyseEquals(QueryModel parent, ExpressionNode node) throws SqlException {
        traverseNamesAndIndices(parent, node);

        int aSize = literalCollectorAIndexes.size();
        int bSize = literalCollectorBIndexes.size();

        JoinContext jc;

        switch (aSize) {
            case 0:
                if (bSize == 1
                        && literalCollector.nullCount == 0
                        // table must not be OUTER or ASOF joined
                        && joinBarriers.excludes(parent.getJoinModels().get(literalCollectorBIndexes.getQuick(0)).getJoinType())) {
                    // single table reference + constant
                    jc = contextPool.next();
                    jc.slaveIndex = literalCollectorBIndexes.getQuick(0);

                    addWhereNode(parent, jc.slaveIndex, node);
                    addJoinContext(parent, jc);

                    CharSequence cs = literalCollectorBNames.getQuick(0);
                    constNameToIndex.put(cs, jc.slaveIndex);
                    constNameToNode.put(cs, node.lhs);
                    constNameToToken.put(cs, node.token);
                } else {
                    parent.addParsedWhereNode(node);
                }
                break;
            case 1:
                jc = contextPool.next();
                int lhi = literalCollectorAIndexes.getQuick(0);
                if (bSize == 1) {
                    int rhi = literalCollectorBIndexes.getQuick(0);
                    if (lhi == rhi) {
                        // single table reference
                        jc.slaveIndex = lhi;
                        addWhereNode(parent, lhi, node);
                    } else {
                        jc.aNodes.add(node.lhs);
                        jc.bNodes.add(node.rhs);
                        jc.aNames.add(literalCollectorANames.getQuick(0));
                        jc.bNames.add(literalCollectorBNames.getQuick(0));
                        jc.aIndexes.add(lhi);
                        jc.bIndexes.add(rhi);
                        int max = Math.max(lhi, rhi);
                        int min = Math.min(lhi, rhi);
                        jc.slaveIndex = max;
                        jc.parents.add(min);
                        linkDependencies(parent, min, max);
                    }
                    addJoinContext(parent, jc);
                } else if (bSize == 0
                        && literalCollector.nullCount == 0
                        && joinBarriers.excludes(parent.getJoinModels().get(literalCollectorAIndexes.getQuick(0)).getJoinType())) {
                    // single table reference + constant
                    jc.slaveIndex = lhi;
                    addWhereNode(parent, lhi, node);
                    addJoinContext(parent, jc);

                    CharSequence cs = literalCollectorANames.getQuick(0);
                    constNameToIndex.put(cs, lhi);
                    constNameToNode.put(cs, node.rhs);
                    constNameToToken.put(cs, node.token);
                } else {
                    parent.addParsedWhereNode(node);
                }
                break;
            default:
                parent.addParsedWhereNode(node);
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
                constNameToIndex.put(name, literalCollectorAIndexes.getQuick(0));
                constNameToNode.put(name, node.rhs);
                constNameToToken.put(name, node.token);
            }
        }
    }

    private void assignFilters(QueryModel parent) throws SqlException {
        tablesSoFar.clear();
        postFilterRemoved.clear();
        postFilterTableRefs.clear();
        nullCounts.clear();

        literalCollector.withModel(parent);
        ObjList<ExpressionNode> filterNodes = parent.getParsedWhere();
        // collect table indexes from each part of global filter
        int pc = filterNodes.size();
        for (int i = 0; i < pc; i++) {
            IntList indexes = intListPool.next();
            literalCollector.resetNullCount();
            traversalAlgo.traverse(filterNodes.getQuick(i), literalCollector.to(indexes));
            postFilterTableRefs.add(indexes);
            nullCounts.add(literalCollector.nullCount);
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

                IntList refs = postFilterTableRefs.getQuick(k);
                int rs = refs.size();
                if (rs == 0) {
                    // condition has no table references
                    // must evaluate as constant
                    postFilterRemoved.add(k);
                    parent.setConstWhereClause(concatFilters(parent.getConstWhereClause(), filterNodes.getQuick(k)));
                } else if (rs == 1
                        && nullCounts.getQuick(k) == 0
                        // single table reference and this table is not joined via OUTER or ASOF
                        && joinBarriers.excludes(parent.getJoinModels().getQuick(refs.getQuick(0)).getJoinType())) {
                    // get single table reference out of the way right away
                    // we don't have to wait until "our" table comes along
                    addWhereNode(parent, refs.getQuick(0), filterNodes.getQuick(k));
                    postFilterRemoved.add(k);
                } else {
                    boolean qualifies = true;
                    // check if filter references table processed so far
                    for (int y = 0; y < rs; y++) {
                        if (tablesSoFar.excludes(refs.getQuick(y))) {
                            qualifies = false;
                            break;
                        }
                    }
                    if (qualifies) {
                        postFilterRemoved.add(k);
                        QueryModel m = parent.getJoinModels().getQuick(index);
                        m.setPostJoinWhereClause(concatFilters(m.getPostJoinWhereClause(), filterNodes.getQuick(k)));
                    }
                }
            }
        }
        assert postFilterRemoved.size() == pc;
    }

    void clear() {
        contextPool.clear();
        intListPool.clear();
        joinClausesSwap1.clear();
        joinClausesSwap2.clear();
        constNameToIndex.clear();
        constNameToNode.clear();
        constNameToToken.clear();
        literalCollectorAIndexes.clear();
        literalCollectorBIndexes.clear();
        literalCollectorANames.clear();
        literalCollectorBNames.clear();
        defaultAliasCount = 0;
        expressionNodePool.clear();
        characterStore.clear();
        tablesSoFar.clear();
        clausesToSteal.clear();
    }

    private void collectAlias(QueryModel parent, int modelIndex, QueryModel model) throws SqlException {
        final ExpressionNode alias = model.getAlias() != null ? model.getAlias() : model.getTableName();
        if (parent.addAliasIndex(alias, modelIndex)) {
            return;
        }
        throw SqlException.position(alias.position).put("duplicate table or alias: ").put(alias.token);
    }

    private ExpressionNode concatFilters(ExpressionNode old, ExpressionNode filter) {
        if (old == null) {
            return filter;
        } else {
            ExpressionNode n = expressionNodePool.next().of(ExpressionNode.OPERATION, "and", 0, 0);
            n.paramCount = 2;
            n.lhs = old;
            n.rhs = filter;
            return n;
        }
    }

    private void copyColumnsFromMetadata(QueryModel model, RecordMetadata m) throws SqlException {
        // column names are not allowed to have dot

        for (int i = 0, k = m.getColumnCount(); i < k; i++) {
            CharSequence columnName = createColumnAlias(m.getColumnName(i), model);
            model.addField(queryColumnPool.next().of(columnName, expressionNodePool.next().of(ExpressionNode.LITERAL, columnName, 0, 0)));
        }

        // validate explicitly defined timestamp, if it exists
        ExpressionNode timestamp = model.getTimestamp();
        if (timestamp == null) {
            if (m.getTimestampIndex() != -1) {
                model.setTimestamp(expressionNodePool.next().of(ExpressionNode.LITERAL, m.getColumnName(m.getTimestampIndex()), 0, 0));
            }
        } else {
            int index = m.getColumnIndexQuiet(timestamp.token);
            if (index == -1) {
                throw SqlException.invalidColumn(timestamp.position, timestamp.token);
            } else if (m.getColumnType(index) != ColumnType.TIMESTAMP) {
                throw SqlException.$(timestamp.position, "not a TIMESTAMP");
            }
        }
    }

    private CharSequence createColumnAlias(CharSequence name, QueryModel model) {
        return SqlUtil.createColumnAlias(characterStore, name, -1, model.getAliasToColumnMap());
    }

    private CharSequence createColumnAlias(ExpressionNode node, QueryModel model) {
        return SqlUtil.createColumnAlias(characterStore, node.token, Chars.indexOf(node.token, '.'), model.getAliasToColumnMap());
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

    // order hash is used to determine redundant order when parsing analytic function definition
    private void createOrderHash(QueryModel model) {
        CharSequenceIntHashMap hash = model.getOrderHash();
        hash.clear();

        final ObjList<ExpressionNode> orderBy = model.getOrderBy();
        final int n = orderBy.size();
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        final int m = columns.size();
        final QueryModel nestedModel = model.getNestedModel();

        if (n > 0) {
            final IntList orderByDirection = model.getOrderByDirection();
            for (int i = 0; i < n; i++) {
                hash.put(orderBy.getQuick(i).token, orderByDirection.getQuick(i));
            }
        }

        if (nestedModel != null) {
            createOrderHash(nestedModel);
            if (m > 0) {
                CharSequenceIntHashMap thatHash = nestedModel.getOrderHash();
                if (thatHash.size() > 0) {
                    for (int i = 0; i < m; i++) {
                        QueryColumn column = columns.getQuick(i);
                        ExpressionNode node = column.getAst();
                        if (node.type == ExpressionNode.LITERAL) {
                            int direction = thatHash.get(node.token);
                            if (direction != -1) {
                                hash.put(column.getName(), direction);
                            }
                        }
                    }
                }
            }
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

    private void createSelectColumn(
            CharSequence columnName,
            ExpressionNode columnAst,
            QueryModel validatingModel,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel analyticModel,
            QueryModel groupByModel,
            QueryModel outerModel,
            QueryModel distinctModel
    ) throws SqlException {
        // add duplicate column names only to group-by model
        // taking into account that column is pre-aliased, e.g.
        // "col, col" will look like "col, col col1"

        CharSequenceObjHashMap<CharSequence> translatingAliasMap = translatingModel.getColumnNameToAliasMap();
        int index = translatingAliasMap.keyIndex(columnAst.token);
        if (index < 0) {
            // column is already being referenced by translating model
            final CharSequence translatedColumnName = translatingAliasMap.valueAtQuick(index);
            final CharSequence alias = createColumnAlias(columnName, groupByModel);
            final QueryColumn translatedColumn = nextColumn(alias, translatedColumnName);
            innerModel.addBottomUpColumn(translatedColumn);
            groupByModel.addBottomUpColumn(translatedColumn);
            analyticModel.addBottomUpColumn(translatedColumn);
            outerModel.addBottomUpColumn(translatedColumn);
            distinctModel.addBottomUpColumn(translatedColumn);
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
            analyticModel.addBottomUpColumn(translatedColumn);
            groupByModel.addBottomUpColumn(translatedColumn);
            outerModel.addBottomUpColumn(translatedColumn);
            distinctModel.addBottomUpColumn(translatedColumn);
        }
    }

    private void createSelectColumnsForWildcard(
            QueryColumn qc,
            boolean hasJoins,
            QueryModel baseModel,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel analyticModel,
            QueryModel groupByModel,
            QueryModel outerModel,
            QueryModel distinctModel
    ) throws SqlException {
        // this could be a wildcard, such as '*' or 'a.*'
        int dot = Chars.indexOf(qc.getAst().token, '.');
        if (dot > -1) {
            int index = baseModel.getAliasIndex(qc.getAst().token, 0, dot);
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
                    analyticModel,
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
                        analyticModel,
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
            QueryModel analyticModel,
            QueryModel groupByModel,
            QueryModel outerModel,
            QueryModel distinctModel
    ) throws SqlException {
        final ObjList<CharSequence> columnNames = srcModel.getBottomUpColumnNames();
        for (int j = 0, z = columnNames.size(); j < z; j++) {
            CharSequence name = columnNames.getQuick(j);
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
                    null, // do not validate
                    translatingModel,
                    innerModel,
                    analyticModel,
                    groupByModel,
                    outerModel,
                    distinctModel
            );
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
                if (--jc.inCount == 0) {
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

    private ExpressionNode doReplaceLiteral(@Transient ExpressionNode node, QueryModel translatingModel, QueryModel innerModel, QueryModel validatingModel) throws SqlException {
        final CharSequenceObjHashMap<CharSequence> map = translatingModel.getColumnNameToAliasMap();
        int index = map.keyIndex(node.token);
        if (index > -1) {
            // there is a possibility that column references join table, but in a different way
            // for example. main column could be tab1.y and the "missing" one just "y"
            // which is the same thing.
            // To disambiguate this situation we need to go over all join tables and see if the
            // column matches any of join tables unambiguously.

            final int joinCount = validatingModel.getJoinModels().size();
            if (joinCount > 1) {
                boolean found = false;
                final StringSink sink = Misc.getThreadLocalBuilder();
                sink.clear();
                for (int i = 0; i < joinCount; i++) {
                    final QueryModel jm = validatingModel.getJoinModels().getQuick(i);
                    if (jm.getAliasToColumnMap().keyIndex(node.token) < 0) {
                        if (found) {
                            throw SqlException.ambiguousColumn(node.position);
                        }
                        if (jm.getAlias() != null) {
                            sink.put(jm.getAlias().token);
                        } else {
                            sink.put(jm.getTableName().token);
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
            CharSequence alias = createColumnAlias(node, translatingModel);
            QueryColumn column = queryColumnPool.next().of(alias, node);
            // add column to both models
            addColumnToTranslatingModel(column, translatingModel, validatingModel);
            if (innerModel != null) {
                innerModel.addBottomUpColumn(column);
            }
            return nextLiteral(alias, node.position);
        }
        return nextLiteral(map.valueAtQuick(index), node.position);
    }

    private void doRewriteOrderByPositionForUnionModels(QueryModel model, QueryModel parent, QueryModel next) throws SqlException {
        final int columnCount = model.getBottomUpColumns().size();
        while (next != null) {
            if (next.getBottomUpColumns().size() != columnCount) {
                throw SqlException.$(next.getModelPosition(), "queries have different number of columns");
            }
            parent.setUnionModel(rewriteOrderByPosition(next));
            parent = next;
            next = next.getUnionModel();
        }
    }

    private void emitAggregates(@Transient ExpressionNode node, QueryModel model) {

        this.sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!this.sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {

                if (node.rhs != null) {
                    ExpressionNode n = replaceIfAggregate(node.rhs, model);
                    if (node.rhs == n) {
                        this.sqlNodeStack.push(node.rhs);
                    } else {
                        node.rhs = n;
                    }
                }

                ExpressionNode n = replaceIfAggregate(node.lhs, model);
                if (n == node.lhs) {
                    node = node.lhs;
                } else {
                    node.lhs = n;
                    node = null;
                }
            } else {
                node = this.sqlNodeStack.poll();
            }
        }
    }

    private void emitLiterals(
            @Transient ExpressionNode node,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel validatingModel
    ) throws SqlException {

        this.sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!this.sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.paramCount < 3) {
                    if (node.rhs != null) {
                        ExpressionNode n = replaceLiteral(node.rhs, translatingModel, innerModel, validatingModel);
                        if (node.rhs == n) {
                            this.sqlNodeStack.push(node.rhs);
                        } else {
                            node.rhs = n;
                        }
                    }

                    ExpressionNode n = replaceLiteral(node.lhs, translatingModel, innerModel, validatingModel);
                    if (n == node.lhs) {
                        node = node.lhs;
                    } else {
                        node.lhs = n;
                        node = null;
                    }
                } else {
                    for (int i = 1, k = node.paramCount; i < k; i++) {
                        ExpressionNode e = node.args.getQuick(i);
                        ExpressionNode n = replaceLiteral(e, translatingModel, innerModel, validatingModel);
                        if (e == n) {
                            this.sqlNodeStack.push(e);
                        } else {
                            node.args.setQuick(i, n);
                        }
                    }

                    ExpressionNode e = node.args.getQuick(0);
                    ExpressionNode n = replaceLiteral(e, translatingModel, innerModel, validatingModel);
                    if (e == n) {
                        node = e;
                    } else {
                        node.args.setQuick(0, n);
                        node = null;
                    }
                }
            } else {
                node = this.sqlNodeStack.poll();
            }
        }
    }

    private void emitLiteralsTopDown(@Transient ExpressionNode node, QueryModel model) {
        this.sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        addTopDownColumn(node, model);

        while (!this.sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                if (node.paramCount < 3) {
                    if (node.rhs != null) {
                        addTopDownColumn(node.rhs, model);
                        this.sqlNodeStack.push(node.rhs);
                    }

                    if (node.lhs != null) {
                        addTopDownColumn(node.lhs, model);
                    }
                    node = node.lhs;
                } else {
                    for (int i = 1, k = node.paramCount; i < k; i++) {
                        ExpressionNode e = node.args.getQuick(i);
                        addTopDownColumn(e, model);
                        this.sqlNodeStack.push(e);
                    }

                    final ExpressionNode e = node.args.getQuick(0);
                    addTopDownColumn(e, model);
                    node = e;
                }
            } else {
                node = this.sqlNodeStack.poll();
            }
        }
    }

    private void enumerateTableColumns(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final ObjList<QueryModel> jm = model.getJoinModels();

        // we have plain tables and possibly joins
        // deal with _this_ model first, it will always be the first element in join model list
        final ExpressionNode tableName = model.getTableName();
        if (tableName != null) {
            if (tableName.type == ExpressionNode.FUNCTION) {
                parseFunctionAndEnumerateColumns(model, executionContext);
            } else {
                openReaderAndEnumerateColumns(executionContext, model);
            }
        } else {
            if (model.getNestedModel() != null) {
                enumerateTableColumns(model.getNestedModel(), executionContext);
                // copy columns of nested model onto parent one
                // we must treat sub-query just like we do a table
                model.copyColumnsFrom(model.getNestedModel());
            }
        }
        for (int i = 1, n = jm.size(); i < n; i++) {
            enumerateTableColumns(jm.getQuick(i), executionContext);
        }

        if (model.getUnionModel() != null) {
            enumerateTableColumns(model.getUnionModel(), executionContext);
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
            // is applied before join. Please see post-join-where for filters that
            // executed in line with standard SQL behaviour.

            if (where != null) {
                if (where.type == ExpressionNode.LITERAL) {
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

    private int getIndexOfTableForColumn(QueryModel model, CharSequence column, int dot, int position) throws SqlException {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        int index = -1;
        if (dot == -1) {
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                if (joinModels.getQuick(i).getAliasToColumnMap().excludes(column)) {
                    continue;
                }

                if (index != -1) {
                    throw SqlException.ambiguousColumn(position);
                }

                index = i;
            }

            if (index == -1) {
                throw SqlException.invalidColumn(position, column);
            }

        } else {
            index = model.getAliasIndex(column, 0, dot);

            if (index == -1) {
                throw SqlException.$(position, "Invalid table name or alias");
            }

            if (joinModels.getQuick(index).getAliasToColumnMap().excludes(column, dot + 1, column.length())) {
                throw SqlException.invalidColumn(position, column);
            }

        }
        return index;
    }

    private ObjList<ExpressionNode> getOrderByAdvice(QueryModel model) {
        orderByAdvice.clear();
        final ObjList<ExpressionNode> orderBy = model.getOrderBy();
        final int len = orderBy.size();
        if (len == 0) {
            return orderByAdvice;
        }

        CharSequenceObjHashMap<QueryColumn> map = model.getAliasToColumnMap();
        for (int i = 0; i < len; i++) {
            QueryColumn queryColumn = map.get(orderBy.getQuick(i).token);
            if (queryColumn.getAst().type == ExpressionNode.LITERAL) {
                orderByAdvice.add(nextLiteral(queryColumn.getAst().token));
            } else {
                orderByAdvice.clear();
                break;
            }
        }
        return orderByAdvice;
    }

    private boolean hasAggregates(ExpressionNode node) {

        this.sqlNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!this.sqlNodeStack.isEmpty() || node != null) {
            if (node != null) {
                switch (node.type) {
                    case ExpressionNode.LITERAL:
                        node = null;
                        continue;
                    case ExpressionNode.FUNCTION:
                        if (functionParser.isGroupBy(node.token)) {
                            return true;
                        }
                        break;
                    default:
                        if (node.rhs != null) {
                            this.sqlNodeStack.push(node.rhs);
                        }
                        break;
                }

                node = node.lhs;
            } else {
                node = this.sqlNodeStack.poll();
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
            } else if (
                    m.getJoinType() != QueryModel.JOIN_ASOF &&
                            m.getJoinType() != QueryModel.JOIN_SPLICE &&
                            (c == null || c.parents.size() == 0)
            ) {
                m.setJoinType(QueryModel.JOIN_CROSS);
            }
        }
    }

    private ExpressionNode makeJoinAlias(int index) {
        CharacterStoreEntry characterStoreEntry = characterStore.newEntry();
        characterStoreEntry.put(QueryModel.SUB_QUERY_ALIAS_PREFIX).put(index);
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
        // check if we merging a.x = b.x to a.y = b.y
        // or a.x = b.x to a.x = b.y, e.g. one of columns in the same
        for (int i = 0, n = b.aNames.size(); i < n; i++) {

            CharSequence ban = b.aNames.getQuick(i);
            int bai = b.aIndexes.getQuick(i);
            ExpressionNode bao = b.aNodes.getQuick(i);

            CharSequence bbn = b.bNames.getQuick(i);
            int bbi = b.bIndexes.getQuick(i);
            ExpressionNode bbo = b.bNodes.getQuick(i);

            for (int k = 0, z = a.aNames.size(); k < z; k++) {

                // don't seem to be adding indexes outside of main loop
//                if (deletedContexts.contains(k)) {
//                    continue;
//                }

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
                linkDependencies(parent, min, max);
            }
        }

        return r;
    }

    private JoinContext moveClauses(QueryModel parent, JoinContext from, JoinContext to, IntList positions) {
        int p = 0;
        int m = positions.size();

        JoinContext result = contextPool.next();
        result.slaveIndex = from.slaveIndex;

        for (int i = 0, n = from.aIndexes.size(); i < n; i++) {
            // logically those clauses we move away from "from" context
            // should not longer exist in "from", but instead of implementing
            // "delete" function, which would be manipulating underlying array
            // on every invocation, we copy retained clauses to new context,
            // which is "result".
            // hence whenever exists in "positions" we copy clause to "to"
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

    private void moveTimestampToChooseModel(QueryModel model) {
        QueryModel nested = model.getNestedModel();
        if (nested != null) {
            moveTimestampToChooseModel(nested);
            ExpressionNode timestamp = nested.getTimestamp();
            if (timestamp != null && nested.getSelectModelType() == QueryModel.SELECT_MODEL_NONE && nested.getTableName() == null && nested.getTableNameFunction() == null) {
                model.setTimestamp(timestamp);
                nested.setTimestamp(null);
            }
        }

        nested = model.getUnionModel();
        if (nested != null) {
            moveTimestampToChooseModel(nested);
        }
    }

    private void moveWhereInsideSubQueries(QueryModel model) throws SqlException {
        if (model.getSelectModelType() != QueryModel.SELECT_MODEL_DISTINCT) {
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
                    literalCollector.resetNullCount();
                    traversalAlgo.traverse(node, literalCollector.lhs());

                    tempList.clear();
                    for (int j = 0; j < literalCollectorAIndexes.size(); j++) {
                        int tableExpressionReference = literalCollectorAIndexes.getQuick(j);
                        int position = tempList.binarySearch(tableExpressionReference);
                        if (position < 0) {
                            tempList.add(-(position + 1), tableExpressionReference);
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
                    final int tableIndex = literalCollectorAIndexes.getQuick(0);
                    final QueryModel parent = model.getJoinModels().getQuick(tableIndex);

                    // Do not move where clauses that contain references
                    // to NULL constant inside outer join models.
                    // Outer join can produce nulls in slave model columns.
                    int joinType = parent.getJoinType();
                    if (tableIndex > 0
                            && (joinBarriers.contains(joinType))
                            && literalCollector.nullCount > 0
                    ) {
                        model.getJoinModels().getQuick(tableIndex).setPostJoinWhereClause(concatFilters(model.getPostJoinWhereClause(), node));
                        continue;
                    }

                    final QueryModel nested = parent.getNestedModel();
                    if (nested == null || nested.getLatestBy().size() > 0) {
                        // there is no nested model for this table, keep where clause element with this model
                        addWhereNode(parent, node);
                    } else {
                        // now that we have identified sub-query we have to rewrite our where clause
                        // to potentially replace all of column references with actual literals used inside
                        // sub-query, for example:
                        // (select a x, b from T) where x = 10
                        // we can't move "x" inside sub-query because it is not a field.
                        // Instead we have to translate "x" to actual column expression, which is "a":
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

    private void openReaderAndEnumerateColumns(SqlExecutionContext executionContext, QueryModel model) throws SqlException {
        final ExpressionNode tableNameNode = model.getTableName();

        // table name must not contain quotes by now
        final CharSequence tableName = tableNameNode.token;
        final int tableNamePosition = tableNameNode.position;

        int lo = 0;
        int hi = tableName.length();
        if (Chars.startsWith(tableName, QueryModel.NO_ROWID_MARKER)) {
            lo += QueryModel.NO_ROWID_MARKER.length();
        }

        if (lo == hi) {
            throw SqlException.$(tableNamePosition, "come on, where is table name?");
        }

        int status = engine.getStatus(executionContext.getCairoSecurityContext(), path, tableName, lo, hi);

        if (status == TableUtils.TABLE_DOES_NOT_EXIST) {
            try {
                model.getTableName().type = ExpressionNode.FUNCTION;
                parseFunctionAndEnumerateColumns(model, executionContext);
                return;
            } catch (SqlException e) {
                throw SqlException.$(tableNamePosition, "table does not exist [name=").put(tableName).put(']');
            }
        }

        if (status == TableUtils.TABLE_RESERVED) {
            throw SqlException.$(tableNamePosition, "table directory is of unknown format");
        }

        try (TableReader r = engine.getReader(
                executionContext.getCairoSecurityContext(),
                tableLookupSequence.of(tableName, lo, hi - lo),
                TableUtils.ANY_TABLE_VERSION
        )) {
            model.setTableVersion(r.getVersion());
            copyColumnsFromMetadata(model, r.getMetadata());
        } catch (EntryLockedException e) {
            throw SqlException.position(tableNamePosition).put("table is locked: ").put(tableLookupSequence);
        } catch (CairoException e) {
            throw SqlException.position(tableNamePosition).put(e);
        }
    }

    QueryModel optimise(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        optimiseExpressionModels(model, executionContext);
        enumerateTableColumns(model, executionContext);
        resolveJoinColumns(model);
        optimiseBooleanNot(model);
        final QueryModel rewrittenModel = rewriteOrderBy(
                rewriteOrderByPositionForUnionModels(
                        rewriteOrderByPosition(
                                rewriteSelectClause(
                                        model, true
                                )
                        )
                )
        );
        optimiseOrderBy(rewrittenModel, OrderByMnemonic.ORDER_BY_UNKNOWN);
        createOrderHash(rewrittenModel);
        optimiseJoins(rewrittenModel);
        moveWhereInsideSubQueries(rewrittenModel);
        eraseColumnPrefixInWhereClauses(rewrittenModel);
        moveTimestampToChooseModel(rewrittenModel);
        propagateTopDownColumns(rewrittenModel, true, null);
        return rewrittenModel;
    }

    private ExpressionNode optimiseBooleanNot(final ExpressionNode node, boolean reverse) {
        switch (notOps.get(node.token)) {
            case NOT_OP_NOT:
                if (reverse) {
                    return optimiseBooleanNot(node.rhs, false);
                } else {
                    switch (node.rhs.type) {
                        case ExpressionNode.LITERAL:
                        case ExpressionNode.CONSTANT:
                            return node;
                        default:
                            return optimiseBooleanNot(node.rhs, true);
                    }
                }
            case NOT_OP_AND:
                if (reverse) {
                    node.token = "or";
                }
                node.lhs = optimiseBooleanNot(node.lhs, reverse);
                node.rhs = optimiseBooleanNot(node.rhs, reverse);
                return node;
            case NOT_OP_OR:
                if (reverse) {
                    node.token = "and";
                }
                node.lhs = optimiseBooleanNot(node.lhs, reverse);
                node.rhs = optimiseBooleanNot(node.rhs, reverse);
                return node;
            case NOT_OP_GREATER:
                if (reverse) {
                    node.token = "<=";
                }
                return node;
            case NOT_OP_GREATER_EQ:
                if (reverse) {
                    node.token = "<";
                }
                return node;

            case NOT_OP_LESS:
                if (reverse) {
                    node.token = ">=";
                }
                return node;
            case NOT_OP_LESS_EQ:
                if (reverse) {
                    node.token = ">";
                }
                return node;
            case NOT_OP_EQUAL:
                if (reverse) {
                    node.token = "!=";
                }
                return node;
            case NOT_OP_NOT_EQ:
                if (reverse) {
                    node.token = "=";
                } else {
                    node.token = "!=";
                }
                return node;
            default:
                if (reverse) {
                    ExpressionNode n = expressionNodePool.next();
                    n.token = "not";
                    n.paramCount = 1;
                    n.rhs = node;
                    n.type = ExpressionNode.OPERATION;
                    return n;
                }
                return node;
        }
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

    private void optimiseExpressionModels(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        ObjList<ExpressionNode> expressionModels = model.getExpressionModels();
        final int n = expressionModels.size();
        if (n > 0) {
            for (int i = 0; i < n; i++) {
                final ExpressionNode node = expressionModels.getQuick(i);
                assert node.queryModel != null;
                QueryModel optimised = optimise(node.queryModel, executionContext);
                if (optimised != node.queryModel) {
                    node.queryModel = optimised;
                }
            }
        }

        if (model.getNestedModel() != null) {
            optimiseExpressionModels(model.getNestedModel(), executionContext);
        }

        final ObjList<QueryModel> joinModels = model.getJoinModels();
        final int m = joinModels.size();
        // as usual, we already optimised self (index=0), now optimised others
        if (m > 1) {
            for (int i = 1; i < m; i++) {
                optimiseExpressionModels(joinModels.getQuick(i), executionContext);
            }
        }

        // call out to union models
        if (model.getUnionModel() != null) {
            optimiseExpressionModels(model.getUnionModel(), executionContext);
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
            // only model model is allowed to have "where" clause
            // so we can assume that "where" clauses of joinModel elements are all null (except for element 0).
            // in case one of joinModels is subquery, its entire query model will be set as
            // nestedModel, e.g. "where" clause is still null there as well

            ExpressionNode where = model.getWhereClause();

            // clear where clause of model so that
            // optimiser can assign there correct nodes

            model.setWhereClause(null);
            processJoinConditions(model, where);

            for (int i = 1; i < n; i++) {
                processJoinConditions(model, joinModels.getQuick(i).getJoinCriteria());
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
                optimiseJoins(m);
            }
        }
    }

    // removes redundant order by clauses from sub-queries
    private void optimiseOrderBy(QueryModel model, final int topLevelOrderByMnemonic) {
        ObjList<QueryColumn> columns = model.getBottomUpColumns();
        int orderByMnemonic;
        int n = columns.size();
        // determine if ordering is required
        switch (topLevelOrderByMnemonic) {
            case OrderByMnemonic.ORDER_BY_UNKNOWN:
                // we have sample by, so expect sub-query has to be ordered
                if (model.getOrderBy().size() > 0 && model.getSampleBy() == null) {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_INVARIANT;
                } else {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
                }
                if (model.getSampleBy() == null) {
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
                // sub-query ordering is not needed
                model.getOrderBy().clear();
                if (model.getSampleBy() != null) {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
                } else {
                    orderByMnemonic = OrderByMnemonic.ORDER_BY_INVARIANT;
                }
                break;
        }

        final ObjList<ExpressionNode> orderByAdvice = getOrderByAdvice(model);
        final IntList orderByDirectionAdvice = model.getOrderByDirection();
        final ObjList<QueryModel> jm = model.getJoinModels();
        for (int i = 0, k = jm.size(); i < k; i++) {
            QueryModel qm = jm.getQuick(i).getNestedModel();
            if (qm != null) {
                qm.setOrderByAdviceMnemonic(orderByMnemonic);
                qm.copyOrderByAdvice(orderByAdvice);
                qm.copyOrderByDirectionAdvice(orderByDirectionAdvice);
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

    private void parseFunctionAndEnumerateColumns(@NotNull QueryModel model, @NotNull SqlExecutionContext executionContext) throws SqlException {
        assert model.getTableNameFunction() == null;
        Function function = functionParser.parseFunction(model.getTableName(), EmptyRecordMetadata.INSTANCE, executionContext);
        if (function.getType() != TypeEx.CURSOR) {
            throw SqlException.$(model.getTableName().position, "function must return CURSOR");
        }
        model.setTableNameFunction(function);
        copyColumnsFromMetadata(model, function.getMetadata());
    }

    private void processEmittedJoinClauses(QueryModel model) {
        // pick up join clauses emitted at initial analysis stage
        // as we merge contexts at this level no more clauses is be emitted
        for (int i = 0, k = emittedJoinClauses.size(); i < k; i++) {
            addJoinContext(model, emittedJoinClauses.getQuick(i));
        }
    }

    /**
     * Splits "where" clauses into "and" concatenated list of boolean expressions.
     *
     * @param node expression n
     */
    private void processJoinConditions(QueryModel parent, ExpressionNode node) throws SqlException {
        ExpressionNode n = node;
        // pre-order traversal
        sqlNodeStack.clear();
        while (!sqlNodeStack.isEmpty() || n != null) {
            if (n != null) {
                switch (joinOps.get(n.token)) {
                    case JOIN_OP_EQUAL:
                        analyseEquals(parent, n);
                        n = null;
                        break;
                    case JOIN_OP_AND:
                        if (n.rhs != null) {
                            sqlNodeStack.push(n.rhs);
                        }
                        n = n.lhs;
                        break;
                    case JOIN_OP_OR:
                        processOrConditions(parent, n);
                        n = null;
                        break;
                    case JOIN_OP_REGEX:
                        analyseRegex(parent, n);
                        // intentional fallthrough
                    default:
                        parent.addParsedWhereNode(n);
                        n = null;
                        break;
                }
            } else {
                n = sqlNodeStack.poll();
            }
        }
    }

    /**
     * There are two ways "or" conditions can go:
     * - all "or" conditions have at least one fields in common
     * e.g. a.x = b.x or a.x = b.y
     * this can be implemented as a hash join where master table is "b"
     * and slave table is "a" keyed on "a.x" so that
     * if HashTable contains all rows of "a" keyed on "a.x"
     * hash join algorithm can do:
     * rows = HashTable.get(b.x);
     * if (rows == null) {
     * rows = HashTable.get(b.y);
     * }
     * <p>
     * in this case tables can be reordered as long as "b" is processed
     * before "a"
     * <p>
     * - second possibility is where all "or" conditions are random
     * in which case query like this:
     * <p>
     * from a
     * join c on a.x = c.x
     * join b on a.x = b.x or c.y = b.y
     * <p>
     * can be rewritten to:
     * <p>
     * from a
     * join c on a.x = c.x
     * join b on a.x = b.x
     * union
     * from a
     * join c on a.x = c.x
     * join b on c.y = b.y
     */
    private void processOrConditions(QueryModel parent, ExpressionNode node) {
        // stub: use filter
        parent.addParsedWhereNode(node);
    }

    private void propagateTopDownColumns(QueryModel model, boolean topLevel, @Nullable QueryModel papaModel) {

        // skip over NONE model that does not have table name
        final QueryModel nested = skipNoneTypeModels(model.getNestedModel());
        final boolean nestedIsFlex = modelIsFlex(nested);

        if (nestedIsFlex) {
            final ObjList<QueryColumn> columns = model.getColumns();
            for (int i = 0, n = columns.size(); i < n; i++) {
                emitLiteralsTopDown(columns.getQuick(i).getAst(), nested);
            }
        }

        final QueryModel union = skipNoneTypeModels(model.getUnionModel());

        if (modelIsFlex(union)) {
            final ObjList<QueryColumn> columns = model.getColumns();
            for (int i = 0, n = columns.size(); i < n; i++) {
                emitLiteralsTopDown(columns.getQuick(i).getAst(), union);
            }
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

                    if (papaModel != null) {
                        emitLiteralsTopDown(jc.aNodes.getQuick(k), papaModel);
                        emitLiteralsTopDown(jc.bNodes.getQuick(k), papaModel);
                    }
                }
            }
            propagateTopDownColumns(jm, false, model);

            // process post-join-where
            final ExpressionNode postJoinWhere = jm.getPostJoinWhereClause();
            if (postJoinWhere != null) {
                emitLiteralsTopDown(postJoinWhere, jm);
                emitLiteralsTopDown(postJoinWhere, model);
            }
        }


        // latest by
        final ObjList<ExpressionNode> latestBy = model.getLatestBy();
        final int latestBySize = latestBy.size();
        if (latestBySize > 0) {
            for (int i = 0; i < latestBySize; i++) {
                emitLiteralsTopDown(latestBy.getQuick(i), model);
            }
        }

        // propagate explicit timestamp declaration
        if (model.getTimestamp() != null && nestedIsFlex) {
            emitLiteralsTopDown(model.getTimestamp(), nested);
        }

        // where clause
        if (model.getWhereClause() != null) {
            emitLiteralsTopDown(model.getWhereClause(), model);
        }

        // propagate 'order by'
        if (!topLevel) {
            final ObjList<ExpressionNode> orderBy = model.getOrderBy();
            final int orderBySize = orderBy.size();
            if (orderBySize > 0) {
                for (int i = 0; i < orderBySize; i++) {
                    emitLiteralsTopDown(orderBy.getQuick(i), model);
                }
            }
        }

        // go down the nested path
        if (nested != null) {
            propagateTopDownColumns(nested, false, null);
        }

        final QueryModel unionModel = model.getUnionModel();
        if (unionModel != null) {
            propagateTopDownColumns(unionModel, true, null);
        }
    }

    /**
     * Identify joined tables without join clause and try to find other reversible join clauses
     * that may be applied to it. For example when these tables joined"
     * <p>
     * from a
     * join b on c.x = b.x
     * join c on c.y = a.y
     * <p>
     * the system that prefers child table with lowest index will attribute c.x = b.x clause to
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
            if (thisCost < cost) {
                root = z;
                cost = thisCost;
                model.setOrderedJoinModels(ordered);
            }
        }

        assert root != -1;
    }

    private ExpressionNode replaceIfAggregate(@Transient ExpressionNode node, QueryModel model) {
        if (node != null && functionParser.isGroupBy(node.token)) {
            QueryColumn c = queryColumnPool.next().of(createColumnAlias(node, model), node);
            model.addBottomUpColumn(c);
            return nextLiteral(c.getAlias());
        }
        return node;
    }

    private ExpressionNode replaceLiteral(
            @Transient ExpressionNode node,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel validatingModel
    ) throws SqlException {
        return node != null && node.type == ExpressionNode.LITERAL ? doReplaceLiteral(node, translatingModel, innerModel, validatingModel) : node;
    }

    private void resolveJoinColumns(QueryModel model) throws SqlException {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        final int size = joinModels.size();
        final CharSequence modelAlias = setAndGetModelAlias(model);
        // collect own alias
        collectAlias(model, 0, model);
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
                collectAlias(model, i, jm);
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

    /**
     * Rewrites order by clause to achieve simple column resolution for model parser.
     * Order by must never reference column that doesn't exist in its own select list.
     * <p>
     * Because order by clause logically executes after "select" it must be able to
     * reference results of arithmetic expression, aggregation function results, arithmetic with
     * aggregation results and analytic functions. Somewhat contradictory to this order by must
     * also be able to reference columns of table or sub-query that are not even in select clause.
     *
     * @param model inbound model
     * @return outbound model
     * @throws SqlException when column names are ambiguous or not found at all.
     */
    private QueryModel rewriteOrderBy(QueryModel model) throws SqlException {
        // find base model and check if there is "group-by" model in between
        // when we are dealing with "group by" model some of the implicit "order by" columns have to be dropped,
        // for example:
        // select a, sum(b) from T order by c
        //
        // above is valid but sorting on "c" would be redundant. However in the following example
        //
        // select a, b from T order by c
        //
        // ordering is does affect query result
        QueryModel result = model;
        QueryModel base = model;
        QueryModel baseParent = model;
        QueryModel wrapper = null;
        final int modelColumnCount = model.getBottomUpColumns().size();
        boolean groupBy = false;

        while (base.getBottomUpColumns().size() > 0 && !base.isNestedModelIsSubQuery()) {
            baseParent = base;
            base = base.getNestedModel();
            groupBy = groupBy || baseParent.getSelectModelType() == QueryModel.SELECT_MODEL_GROUP_BY;
        }

        // find out how "order by" columns are referenced
        ObjList<ExpressionNode> orderByNodes = base.getOrderBy();
        int sz = orderByNodes.size();
        if (sz > 0) {
            boolean ascendColumns = true;
            // for each order by column check how deep we need to go between "model" and "base"
            for (int i = 0; i < sz; i++) {
                final ExpressionNode orderBy = orderByNodes.getQuick(i);
                final CharSequence column = orderBy.token;
                final int dot = Chars.indexOf(column, '.');
                // is this a table reference?
                if (dot > -1 || model.getAliasToColumnMap().excludes(column)) {
                    // validate column
                    int indexOfTableForColumn = getIndexOfTableForColumn(base, column, dot, orderBy.position);
                    if (dot < 0 && baseParent.getAliasToColumnNameMap().get(base.getBottomUpColumnNames().get(indexOfTableForColumn)) == null) {
                        throw SqlException.invalidColumn(orderBy.position, column);
                    }

                    // good news, our column matched base model
                    // this condition is to ignore order by columns that are not in select and behind group by
                    if (ascendColumns && base != model) {
                        // check if column is aliased as either
                        // "x y" or "tab.x y" or "t.x y", where "t" is alias of table "tab"
                        final CharSequenceObjHashMap<CharSequence> map = baseParent.getColumnNameToAliasMap();
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

                        if (index < 0) {
                            // we have found alias, rewrite order by column
                            orderBy.token = map.valueAtQuick(index);
                        } else {
                            if (dot > -1) {
                                throw SqlException.invalidColumn(orderBy.position, column);
                            }

                            // we must attempt to ascend order by column
                            // when we have group by model, ascent is not possible
                            if (groupBy) {
                                ascendColumns = false;
                            } else {
                                if (baseParent.getSelectModelType() != QueryModel.SELECT_MODEL_CHOOSE) {
                                    QueryModel synthetic = queryModelPool.next();
                                    synthetic.setSelectModelType(QueryModel.SELECT_MODEL_CHOOSE);
                                    for (int j = 0, z = baseParent.getBottomUpColumns().size(); j < z; j++) {
                                        QueryColumn qc = baseParent.getBottomUpColumns().getQuick(j);
                                        if (qc.getAst().type == ExpressionNode.FUNCTION || qc.getAst().type == ExpressionNode.OPERATION) {
                                            emitLiterals(qc.getAst(), synthetic, null, baseParent.getNestedModel());
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
                if (ascendColumns && base != baseParent) {
                    model.addOrderBy(orderBy, base.getOrderByDirection().getQuick(i));
                }
            }

            if (base != model) {
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

    private QueryModel rewriteOrderByPosition(QueryModel model) throws SqlException {
        QueryModel base = model;
        QueryModel baseParent = model;

        while (base.getBottomUpColumns().size() > 0) {
            baseParent = base;
            base = base.getNestedModel();
        }

        ObjList<ExpressionNode> orderByNodes = base.getOrderBy();
        int sz = orderByNodes.size();
        if (sz > 0) {
            final ObjList<QueryColumn> columns = baseParent.getBottomUpColumns();
            final int columnCount = columns.size();
            // for each order by column check how deep we need to go between "model" and "base"
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
                                    ExpressionNode.LITERAL,
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

        return model;
    }

    private QueryModel rewriteOrderByPositionForUnionModels(QueryModel model) throws SqlException {
        QueryModel next = model.getUnionModel();
        if (next != null) {
            doRewriteOrderByPositionForUnionModels(model, model, next);
        }

        next = model.getNestedModel();
        if (next != null) {
            rewriteOrderByPositionForUnionModels(next);
        }
        return model;
    }

    // flatParent = true means that parent model does not have selected columns
    private QueryModel rewriteSelectClause(QueryModel model, boolean flatParent) throws SqlException {

        if (model.getUnionModel() != null) {
            QueryModel rewrittenUnionModel = rewriteSelectClause(model.getUnionModel(), true);
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
                QueryModel rewritten = rewriteSelectClause(nestedModel, flatModel);
                if (rewritten != nestedModel) {
                    m.setNestedModel(rewritten);
                    // since we have rewritten nested model we also have to update column hash
                    m.copyColumnsFrom(rewritten);
                }
            }

            if (flatModel) {
                if (flatParent && m.getSampleBy() != null) {
                    throw SqlException.$(m.getSampleBy().position, "'sample by' must be used with 'select' clause, which contains aggerate expression(s)");
                }
            } else {
                model.replaceJoinModel(i, rewriteSelectClause0(m));
            }
        }

        // "model" is always first in its own list of join models
        return models.getQuick(0);
    }

    @NotNull
    private QueryModel rewriteSelectClause0(QueryModel model) throws SqlException {
        assert model.getNestedModel() != null;

        QueryModel groupByModel = queryModelPool.next();
        groupByModel.setSelectModelType(QueryModel.SELECT_MODEL_GROUP_BY);
        QueryModel distinctModel = queryModelPool.next();
        distinctModel.setSelectModelType(QueryModel.SELECT_MODEL_DISTINCT);
        QueryModel outerModel = queryModelPool.next();
        outerModel.setSelectModelType(QueryModel.SELECT_MODEL_VIRTUAL);
        QueryModel innerModel = queryModelPool.next();
        innerModel.setSelectModelType(QueryModel.SELECT_MODEL_VIRTUAL);
        QueryModel analyticModel = queryModelPool.next();
        analyticModel.setSelectModelType(QueryModel.SELECT_MODEL_ANALYTIC);
        QueryModel translatingModel = queryModelPool.next();
        translatingModel.setSelectModelType(QueryModel.SELECT_MODEL_CHOOSE);
        boolean useInnerModel = false;
        boolean useAnalyticModel = false;
        boolean useGroupByModel = false;
        boolean useOuterModel = false;
        boolean useDistinctModel = model.isDistinct();

        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        final QueryModel baseModel = model.getNestedModel();
        final boolean hasJoins = baseModel.getJoinModels().size() > 1;

        // sample by clause should be promoted to all of the models as well as validated
        final ExpressionNode sampleBy = baseModel.getSampleBy();
        if (sampleBy != null) {
            // move sample by to group by model
            groupByModel.moveSampleByFrom(baseModel);
        }

        // create virtual columns from select list
        for (int i = 0, k = columns.size(); i < k; i++) {
            QueryColumn qc = columns.getQuick(i);
            final boolean analytic = qc instanceof AnalyticColumn;

            // fail-fast if this is an arithmetic expression where we expect analytic function
            if (analytic && qc.getAst().type != ExpressionNode.FUNCTION) {
                throw SqlException.$(qc.getAst().position, "Analytic function expected");
            }

            if (qc.getAst().type == ExpressionNode.LITERAL) {
                if (!isNotBindVariable(qc.getAst().token)) {
                    addFunction(
                            qc,
                            baseModel,
                            translatingModel,
                            innerModel,
                            analyticModel,
                            groupByModel,
                            outerModel,
                            distinctModel
                    );
                    useInnerModel = true;
                } else if (Chars.endsWith(qc.getAst().token, '*')) {
                    // in general sense we need to create new column in case
                    // there is change of alias, for example we may have something as simple as
                    // select a.f, b.f from ....
                    createSelectColumnsForWildcard(
                            qc,
                            hasJoins,
                            baseModel,
                            translatingModel,
                            innerModel,
                            analyticModel,
                            groupByModel,
                            outerModel,
                            distinctModel
                    );
                } else {
                    createSelectColumn(
                            qc.getAlias(),
                            qc.getAst(),
                            baseModel,
                            translatingModel,
                            innerModel,
                            analyticModel,
                            groupByModel,
                            outerModel,
                            distinctModel
                    );
                }
            } else {
                // when column is direct call to aggregation function, such as
                // select sum(x) ...
                // we can add it to group-by model right away
                if (qc.getAst().type == ExpressionNode.FUNCTION) {
                    if (analytic) {
                        analyticModel.addBottomUpColumn(qc);

                        // ensure literals referenced by analytic column are present in nested models
                        emitLiterals(qc.getAst(), translatingModel, innerModel, baseModel);
                        useAnalyticModel = true;
                        continue;
                    } else if (functionParser.isGroupBy(qc.getAst().token)) {
                        CharSequence alias = createColumnAlias(qc.getAlias(), groupByModel);
                        if (alias != qc.getAlias()) {
                            qc = queryColumnPool.next().of(alias, qc.getAst());
                        }
                        groupByModel.addBottomUpColumn(qc);
                        // group-by column references might be needed when we have
                        // outer model supporting arithmetic such as:
                        // select sum(a)+sum(b) ....
                        QueryColumn aggregateRef = nextColumn(qc.getAlias());
                        outerModel.addBottomUpColumn(aggregateRef);
                        distinctModel.addBottomUpColumn(aggregateRef);
                        // pull out literals
                        emitLiterals(qc.getAst(), translatingModel, innerModel, baseModel);
                        useGroupByModel = true;
                        continue;
                    }
                }

                // this is not a direct call to aggregation function, in which case
                // we emit aggregation function into group-by model and leave the
                // rest in outer model
                int beforeSplit = groupByModel.getBottomUpColumns().size();
                emitAggregates(qc.getAst(), groupByModel);
                if (beforeSplit < groupByModel.getBottomUpColumns().size()) {
                    outerModel.addBottomUpColumn(qc);
                    distinctModel.addBottomUpColumn(nextColumn(qc.getAlias()));

                    // pull literals from newly created group-by columns into both of underlying models
                    for (int j = beforeSplit, n = groupByModel.getBottomUpColumns().size(); j < n; j++) {
                        emitLiterals(groupByModel.getBottomUpColumns().getQuick(i).getAst(), translatingModel, innerModel, baseModel);
                    }

                    useGroupByModel = true;
                    useOuterModel = true;
                } else {
                    addFunction(
                            qc,
                            baseModel,
                            translatingModel,
                            innerModel,
                            analyticModel,
                            groupByModel,
                            outerModel,
                            distinctModel
                    );
                    useInnerModel = true;
                }
            }
        }

        // fail if we have both analytic and group-by models
        if (useAnalyticModel && useGroupByModel) {
            throw SqlException.$(0, "Analytic function is not allowed in context of aggregation. Use sub-query.");
        }

        // check if translating model is redundant, e.g.
        // that it neither chooses between tables nor renames columns
        boolean translationIsRedundant = useInnerModel || useGroupByModel || useAnalyticModel;
        if (translationIsRedundant) {
            for (int i = 0, n = translatingModel.getBottomUpColumns().size(); i < n; i++) {
                QueryColumn column = translatingModel.getBottomUpColumns().getQuick(i);
                if (!column.getAst().token.equals(column.getAlias())) {
                    translationIsRedundant = false;
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
            translatingModel.moveLimitFrom(model);
        }

        if (useInnerModel) {
            innerModel.setNestedModel(root);
            innerModel.moveLimitFrom(limitSource);
            root = innerModel;
            limitSource = innerModel;
        }

        if (useAnalyticModel) {
            analyticModel.setNestedModel(root);
            analyticModel.moveLimitFrom(limitSource);
            root = analyticModel;
            limitSource = analyticModel;
        } else if (useGroupByModel) {
            groupByModel.setNestedModel(root);
            groupByModel.moveLimitFrom(limitSource);
            root = groupByModel;
            limitSource = groupByModel;
            if (useOuterModel) {
                outerModel.setNestedModel(root);
                outerModel.moveLimitFrom(limitSource);
                root = outerModel;
                limitSource = outerModel;
            }
        }

        if (root != outerModel && root.getBottomUpColumns().size() < outerModel.getBottomUpColumns().size()) {
            outerModel.setNestedModel(root);
            outerModel.moveLimitFrom(limitSource);
            // in this case outer model should be of "choose" type
            outerModel.setSelectModelType(QueryModel.SELECT_MODEL_CHOOSE);
            root = outerModel;
        }

        if (useDistinctModel) {
            distinctModel.setNestedModel(root);
            root = distinctModel;
        }

        if (!useGroupByModel && groupByModel.getSampleBy() != null) {
            throw SqlException.$(groupByModel.getSampleBy().position, "at least one aggregation function must be present in 'select' clause");
        }

        if (model != root) {
            root.setUnionModel(model.getUnionModel());
            root.setSetOperationType(model.getSetOperationType());
            root.setModelPosition(model.getModelPosition());
        }
        return root;
    }

    private CharSequence setAndGetModelAlias(QueryModel model) {
        CharSequence name = model.getName();
        if (name != null) {
            return name;
        }
        ExpressionNode alias = makeJoinAlias(defaultAliasCount++);
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
            target.getDependencies().clear();
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
        literalCollector.resetNullCount();
        traversalAlgo.traverse(node.lhs, literalCollector.lhs());
        traversalAlgo.traverse(node.rhs, literalCollector.rhs());
    }

    private static class NonLiteralException extends RuntimeException {
        private static final NonLiteralException INSTANCE = new NonLiteralException();
    }

    private static class LiteralCheckingVisitor implements PostOrderTreeTraversalAlgo.Visitor {
        private CharSequenceObjHashMap<QueryColumn> nameTypeMap;

        @Override
        public void visit(ExpressionNode node) {
            if (node.type == ExpressionNode.LITERAL) {
                final int dot = Chars.indexOf(node.token, '.');
                int index = dot == -1 ? nameTypeMap.keyIndex(node.token) : nameTypeMap.keyIndex(node.token, dot + 1, node.token.length());
                // these columns are pre-validated
                assert index < 0;
                if (nameTypeMap.valueAt(index).getAst().type != ExpressionNode.LITERAL) {
                    throw NonLiteralException.INSTANCE;
                }
            }
        }

        PostOrderTreeTraversalAlgo.Visitor of(CharSequenceObjHashMap<QueryColumn> nameTypeMap) {
            this.nameTypeMap = nameTypeMap;
            return this;
        }
    }

    private static class LiteralRewritingVisitor implements PostOrderTreeTraversalAlgo.Visitor {
        private CharSequenceObjHashMap<CharSequence> aliasToColumnMap;

        @Override
        public void visit(ExpressionNode node) {
            if (node.type == ExpressionNode.LITERAL) {
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

        PostOrderTreeTraversalAlgo.Visitor of(CharSequenceObjHashMap<CharSequence> aliasToColumnMap) {
            this.aliasToColumnMap = aliasToColumnMap;
            return this;
        }
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
            if (node != null && node.type == ExpressionNode.LITERAL) {
                final int dot = Chars.indexOf(node.token, '.');
                if (dot != -1) {
                    return nextLiteral(node.token.subSequence(dot + 1, node.token.length()));
                }
            }
            return node;
        }
    }

    private class LiteralCollector implements PostOrderTreeTraversalAlgo.Visitor {
        private IntList indexes;
        private ObjList<CharSequence> names;
        private int nullCount;
        private QueryModel model;

        @Override
        public void visit(ExpressionNode node) throws SqlException {
            switch (node.type) {
                case ExpressionNode.LITERAL:
                    // ignore bind variables
                    if (isNotBindVariable(node.token)) {
                        int dot = Chars.indexOf(node.token, '.');
                        CharSequence name = extractColumnName(node.token, dot);
                        indexes.add(getIndexOfTableForColumn(model, node.token, dot, node.position));
                        if (names != null) {
                            names.add(name);
                        }
                    }
                    break;
                case ExpressionNode.CONSTANT:
                    if (nullConstants.contains(node.token)) {
                        nullCount++;
                    }
                    break;
                default:
                    break;
            }
        }

        private CharSequence extractColumnName(CharSequence token, int dot) {
            return dot == -1 ? token : token.subSequence(dot + 1, token.length());
        }

        private PostOrderTreeTraversalAlgo.Visitor lhs() {
            indexes = literalCollectorAIndexes;
            names = literalCollectorANames;
            return this;
        }

        private void resetNullCount() {
            nullCount = 0;
        }

        private PostOrderTreeTraversalAlgo.Visitor rhs() {
            indexes = literalCollectorBIndexes;
            names = literalCollectorBNames;
            return this;
        }

        private PostOrderTreeTraversalAlgo.Visitor to(IntList indexes) {
            this.indexes = indexes;
            this.names = null;
            return this;
        }

        private void withModel(QueryModel model) {
            this.model = model;
        }
    }
}
