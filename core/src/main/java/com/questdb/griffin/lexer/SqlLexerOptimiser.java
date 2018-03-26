/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.lexer;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.common.ColumnType;
import com.questdb.common.NumericException;
import com.questdb.common.PartitionBy;
import com.questdb.common.RecordMetadata;
import com.questdb.griffin.common.ExprNode;
import com.questdb.griffin.lexer.model.*;
import com.questdb.ql.ops.FunctionFactories;
import com.questdb.std.*;
import com.questdb.std.str.FlyweightCharSequence;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;

public final class SqlLexerOptimiser {

    public static final int MAX_ORDER_BY_COLUMNS = 1560;
    private static final CharSequenceHashSet tableAliasStop = new CharSequenceHashSet();
    private static final CharSequenceHashSet columnAliasStop = new CharSequenceHashSet();
    private static final CharSequenceHashSet groupByStopSet = new CharSequenceHashSet();
    private static final CharSequenceIntHashMap joinStartSet = new CharSequenceIntHashMap();
    private static final int ORDER_BY_UNKNOWN = 0;
    private static final int ORDER_BY_REQUIRED = 1;
    private static final int ORDER_BY_INVARIANT = 2;
    private final static IntHashSet joinBarriers;
    private final static CharSequenceHashSet nullConstants = new CharSequenceHashSet();
    private static final CharSequenceHashSet disallowedAliases = new CharSequenceHashSet();
    private static final CharSequenceIntHashMap joinOps = new CharSequenceIntHashMap();
    private static final int JOIN_OP_EQUAL = 1;
    private static final int JOIN_OP_AND = 2;
    private static final int JOIN_OP_OR = 3;
    private static final int JOIN_OP_REGEX = 4;
    private static final CharSequenceIntHashMap notOps = new CharSequenceIntHashMap();
    private static final int NOT_OP_NOT = 1;
    private static final int NOT_OP_AND = 2;
    private static final int NOT_OP_OR = 3;
    private static final int NOT_OP_GREATER = 4;
    private static final int NOT_OP_GREATER_EQ = 5;
    private static final int NOT_OP_LESS = 6;
    private static final int NOT_OP_LESS_EQ = 7;
    private static final int NOT_OP_EQUAL = 8;
    private static final int NOT_OP_NOT_EQ = 9;
    private final ObjectPool<ExprNode> exprNodePool = new ObjectPool<>(ExprNode.FACTORY, 128);
    private final ExprAstBuilder astBuilder = new ExprAstBuilder();
    private final ObjectPool<QueryModel> queryModelPool = new ObjectPool<>(QueryModel.FACTORY, 8);
    private final ObjectPool<QueryColumn> queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 64);
    private final ObjectPool<AnalyticColumn> analyticColumnPool = new ObjectPool<>(AnalyticColumn.FACTORY, 8);
    private final ObjectPool<CreateTableModel> createTableModelPool = new ObjectPool<>(CreateTableModel.FACTORY, 4);
    private final ObjectPool<ColumnCastModel> columnCastModelPool = new ObjectPool<>(ColumnCastModel.FACTORY, 8);
    private final ObjectPool<RenameTableModel> renameTableModelPool = new ObjectPool<>(RenameTableModel.FACTORY, 8);
    private final ObjectPool<WithClauseModel> withClauseModelPool = new ObjectPool<>(WithClauseModel.FACTORY, 16);
    private final Lexer2 secondaryLexer = new Lexer2();
    private final ExprParser exprParser = new ExprParser(exprNodePool);
    private final CairoConfiguration configuration;
    private final ArrayDeque<ExprNode> exprNodeStack = new ArrayDeque<>();
    private final PostOrderTreeTraversalAlgo traversalAlgo = new PostOrderTreeTraversalAlgo();
    private final CairoEngine engine;
    private final IntList literalCollectorAIndexes = new IntList();
    private final ObjList<CharSequence> literalCollectorANames = new ObjList<>();
    private final IntList literalCollectorBIndexes = new IntList();
    private final ObjList<CharSequence> literalCollectorBNames = new ObjList<>();
    private final LiteralCollector literalCollector = new LiteralCollector();
    private final ObjectPool<JoinContext> contextPool = new ObjectPool<>(JoinContext.FACTORY, 16);
    private final IntHashSet deletedContexts = new IntHashSet();
    private final ObjList<JoinContext> joinClausesSwap1 = new ObjList<>();
    private final ObjList<JoinContext> joinClausesSwap2 = new ObjList<>();
    private final CharSequenceIntHashMap constNameToIndex = new CharSequenceIntHashMap();
    private final CharSequenceObjHashMap<ExprNode> constNameToNode = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<CharSequence> constNameToToken = new CharSequenceObjHashMap<>();
    private final IntList tempCrosses = new IntList();
    private final IntList tempCrossIndexes = new IntList();
    private final IntList clausesToSteal = new IntList();
    private final ObjList<IntList> postFilterTableRefs = new ObjList<>();
    private final ObjectPool<IntList> intListPool = new ObjectPool<>(IntList::new, 16);
    private final IntHashSet tablesSoFar = new IntHashSet();
    private final IntHashSet postFilterRemoved = new IntHashSet();
    private final IntList nullCounts = new IntList();
    private final IntPriorityQueue orderingStack = new IntPriorityQueue();
    //    private final StringSink columnNameAssembly = new StringSink();
    private final LiteralCheckingVisitor literalCheckingVisitor = new LiteralCheckingVisitor();
    private final LiteralRewritingVisitor literalRewritingVisitor = new LiteralRewritingVisitor();
    private final ObjList<ExprNode> tempExprNodes = new ObjList<>();
    private final FlyweightCharSequence tableLookupSequence = new FlyweightCharSequence();
    private Lexer2 lexer = new Lexer2();
    private ObjList<JoinContext> emittedJoinClauses;
    private int defaultAliasCount = 0;
    private boolean subQueryMode = false;

    public SqlLexerOptimiser(CairoEngine engine, CairoConfiguration configuration) {
        this.engine = engine;
        this.configuration = configuration;

        ExprParser.configureLexer(lexer);
        ExprParser.configureLexer(secondaryLexer);
    }

    public void enumerateTableColumns(QueryModel model) throws ParserException {
        final ObjList<QueryModel> jm = model.getJoinModels();

        // we have plain tables and possibly joins
        // deal with _this_ model first, it will always be the first element in join model list
        if (model.getTableName() != null) {
            RecordMetadata m = model.getTableMetadata(engine, tableLookupSequence);
            // column names are not allowed to have dot
            // todo: test that this is the case
            for (int i = 0, k = m.getColumnCount(); i < k; i++) {
                model.addField(createColumnAlias(m.getColumnName(i), model));
            }

            // validate explicitly defined timestamp, if it exists
            ExprNode timestamp = model.getTimestamp();
            if (timestamp == null) {
                if (m.getTimestampIndex() != -1) {
                    model.setTimestamp(exprNodePool.next().of(ExprNode.LITERAL, m.getColumnQuick(m.getTimestampIndex()).getName(), 0, 0));
                }
            } else {
                int index = m.getColumnIndexQuiet(timestamp.token);
                if (index == -1) {
                    throw ParserException.invalidColumn(timestamp.position, timestamp.token);
                } else if (m.getColumnQuick(index).getType() != ColumnType.TIMESTAMP) {
                    throw ParserException.$(timestamp.position, "not a TIMESTAMP");
                }
            }
        } else {
            if (model.getNestedModel() != null) {
                enumerateTableColumns(model.getNestedModel());
                // copy columns of nested model onto parent one
                // we must treat sub-query just like we do a table
                model.copyColumnsFrom(model.getNestedModel());
            }
        }
        for (int i = 1, n = jm.size(); i < n; i++) {
            enumerateTableColumns(jm.getQuick(i));
        }
    }

    public ParsedModel parse(CharSequence query) throws ParserException {
        clear();
        lexer.setContent(query);
        CharSequence tok = tok("'create', 'rename' or 'select'");

        if (Chars.equals(tok, "select")) {
            return parseSelect();
        }

        if (Chars.equals(tok, "create")) {
            return parseCreateStatement();
        }

        if (Chars.equals(tok, "rename")) {
            return parseRenameStatement();
        }

        return parseSelect();
    }

    private static void assertNotNull(ExprNode node, int position, String message) throws ParserException {
        if (node == null) {
            throw ParserException.$(position, message);
        }
    }

    private static void linkDependencies(QueryModel model, int parent, int child) {
        model.getJoinModels().getQuick(parent).addDependency(child);
    }

    private static void unlinkDependencies(QueryModel model, int parent, int child) {
        model.getJoinModels().getQuick(parent).removeDependency(child);
    }

    /*
     * Uses validating model to determine if column name exists and non-ambiguous in case of using joins.
     */
    private void addColumnToTranslatingModel(QueryColumn column, QueryModel translatingModel, QueryModel validatingModel) throws ParserException {
        if (validatingModel != null) {
            CharSequence refColumn = column.getAst().token;
            getIndexOfTableForColumn(validatingModel, refColumn, Chars.indexOf(refColumn, '.'), column.getAst().position);
        }
        translatingModel.addColumn(column);
    }

    private void addFilterOrEmitJoin(QueryModel parent, int idx, int ai, CharSequence an, ExprNode ao, int bi, CharSequence bn, ExprNode bo) {
        if (ai == bi && Chars.equals(an, bn)) {
            deletedContexts.add(idx);
            return;
        }

        if (ai == bi) {
            // (same table)
            ExprNode node = exprNodePool.next().of(ExprNode.OPERATION, "=", 0, 0);
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
            jc.slaveIndex = ai > bi ? ai : bi;
            jc.parents.add(ai < bi ? ai : bi);
            emittedJoinClauses.add(jc);
        }

        deletedContexts.add(idx);
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
    private void addTransitiveFilters(QueryModel parent) {
        ObjList<QueryModel> joinModels = parent.getJoinModels();
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            JoinContext jc = joinModels.getQuick(i).getContext();
            if (jc != null) {
                for (int k = 0, kn = jc.bNames.size(); k < kn; k++) {
                    CharSequence name = jc.bNames.getQuick(k);
                    if (constNameToIndex.get(name) == jc.bIndexes.getQuick(k)) {
                        ExprNode node = exprNodePool.next().of(ExprNode.OPERATION, constNameToToken.get(name), 0, 0);
                        node.lhs = jc.aNodes.getQuick(k);
                        node.rhs = constNameToNode.get(name);
                        node.paramCount = 2;
                        addWhereNode(parent, jc.slaveIndex, node);
                    }
                }
            }
        }
    }

    private void addWhereNode(QueryModel model, int joinModelIndex, ExprNode node) {
        addWhereNode(model.getJoinModels().getQuick(joinModelIndex), node);
    }

    private void addWhereNode(QueryModel model, ExprNode node) {
        if (model.getColumns().size() > 0) {
            model.getNestedModel().setWhereClause(concatFilters(model.getNestedModel().getWhereClause(), node));
        } else {
            // otherwise assign directly to model
            model.setWhereClause(concatFilters(model.getWhereClause(), node));
        }
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
                        ExprNode node = jc.aNodes.getQuick(k);

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

    private void analyseEquals(QueryModel parent, ExprNode node) throws ParserException {
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
                        int max = lhi > rhi ? lhi : rhi;
                        int min = lhi < rhi ? lhi : rhi;
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

    private void analyseRegex(QueryModel parent, ExprNode node) throws ParserException {
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

    private void assignFilters(QueryModel parent) throws ParserException {

        tablesSoFar.clear();
        postFilterRemoved.clear();
        postFilterTableRefs.clear();
        nullCounts.clear();

        literalCollector.withModel(parent);
        ObjList<ExprNode> filterNodes = parent.getParsedWhere();
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

    private void clear() {
        queryModelPool.clear();
        queryColumnPool.clear();
        exprNodePool.clear();
        analyticColumnPool.clear();
        createTableModelPool.clear();
        columnCastModelPool.clear();
        renameTableModelPool.clear();
        withClauseModelPool.clear();
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
        subQueryMode = false;
        defaultAliasCount = 0;
    }

    private void collectAlias(QueryModel parent, int modelIndex, QueryModel model) throws ParserException {
        final ExprNode alias = model.getAlias() != null ? model.getAlias() : model.getTableName();
        if (parent.addAliasIndex(alias, modelIndex)) {
            return;
        }
        throw ParserException.position(alias.position).put("duplicate table or alias: ").put(alias.token);
    }

    private ExprNode concatFilters(ExprNode old, ExprNode filter) {
        if (old == null) {
            return filter;
        } else {
            ExprNode n = exprNodePool.next().of(ExprNode.OPERATION, "and", 0, 0);
            n.paramCount = 2;
            n.lhs = old;
            n.rhs = filter;
            return n;
        }
    }

    private CharSequence createColumnAlias(ExprNode node, QueryModel model) {
        return createColumnAlias(node.token, Chars.indexOf(node.token, '.'), model.getColumnNameTypeMap());
    }

    private CharSequence createColumnAlias(CharSequence name, QueryModel model) {
        return createColumnAlias(name, -1, model.getColumnNameTypeMap());
    }

    private CharSequence createColumnAlias(CharSequence base, int indexOfDot, CharSequenceIntHashMap nameTypeMap) {
        final boolean disallowed = disallowedAliases.contains(base);

        // short and sweet version
        if (indexOfDot == -1 && !disallowed && nameTypeMap.excludes(base)) {
            return base;
        }

        Lexer2.NameAssembler nameAssembler = lexer.getNameAssembler();
        if (indexOfDot == -1) {
            if (disallowed) {
                nameAssembler.put("column");
            } else {
                nameAssembler.put(base);
            }
        } else {
            nameAssembler.put(base, indexOfDot + 1, base.length());
        }


        int len = nameAssembler.size();
        int sequence = 0;
        while (true) {
            if (sequence > 0) {
                nameAssembler.setSize(len);
                nameAssembler.put(sequence);
            }
            sequence++;
            CharSequence alias = nameAssembler.toImmutable();
            if (nameTypeMap.excludes(alias)) {
                return alias;
            }
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
            if (m.getJoinType() == QueryModel.JOIN_ASOF) {
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

        final ObjList<ExprNode> orderBy = model.getOrderBy();
        final int n = orderBy.size();
        final ObjList<QueryColumn> columns = model.getColumns();
        final int m = columns.size();
        final QueryModel nestedModel = model.getNestedModel();

        if (n > 0) {
            final IntList orderByDirection = model.getOrderByDirection();
            for (int i = 0; i < n; i++) {
                hash.put(orderBy.getQuick(i).token, orderByDirection.getQuick(i));
            }
        } else if (nestedModel != null && m > 0) {
            createOrderHash(nestedModel);
            CharSequenceIntHashMap thatHash = nestedModel.getOrderHash();
            if (thatHash.size() > 0) {
                for (int i = 0; i < m; i++) {
                    QueryColumn column = columns.getQuick(i);
                    ExprNode node = column.getAst();
                    if (node.type == ExprNode.LITERAL) {
                        int direction = thatHash.get(node.token);
                        if (direction != -1) {
                            hash.put(column.getName(), direction);
                        }
                    }
                }
            }
        }
    }

    private void createSelectColumn(
            CharSequence columnName,
            ExprNode columnAst,
            QueryModel validatingModel,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel groupByModel,
            QueryModel analyticModel,
            QueryModel outerModel
    ) throws ParserException {
        final CharSequence alias = createColumnAlias(columnName, translatingModel);

        addColumnToTranslatingModel(
                queryColumnPool.next().of(
                        alias,
                        columnAst
                ), translatingModel, validatingModel);

        final QueryColumn translatedColumn = nextColumn(alias);

        // create column that references inner alias we just created
        innerModel.addColumn(translatedColumn);
        groupByModel.addColumn(translatedColumn);
        analyticModel.addColumn(translatedColumn);
        outerModel.addColumn(translatedColumn);
    }

    private void createSelectColumnsForWildcard(
            QueryColumn qc,
            QueryModel baseModel,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel groupByModel,
            QueryModel analyticModel,
            QueryModel outerModel,
            boolean hasJoins
    ) throws ParserException {
        // this could be a wildcard, such as '*' or 'a.*'
        int dot = Chars.indexOf(qc.getAst().token, '.');
        if (dot > -1) {
            int index = baseModel.getAliasIndex(qc.getAst().token, 0, dot);
            if (index == -1) {
                throw ParserException.$(qc.getAst().position, "invalid table alias");
            }

            // we are targeting single table
            createSelectColumnsForWildcard0(
                    baseModel.getJoinModels().getQuick(index),
                    qc.getAst().position,
                    translatingModel,
                    innerModel,
                    groupByModel,
                    analyticModel,
                    outerModel,
                    hasJoins
            );
        } else {
            ObjList<QueryModel> models = baseModel.getJoinModels();
            for (int j = 0, z = models.size(); j < z; j++) {
                createSelectColumnsForWildcard0(
                        models.getQuick(j),
                        qc.getAst().position,
                        translatingModel,
                        innerModel,
                        groupByModel,
                        analyticModel,
                        outerModel,
                        hasJoins
                );
            }
        }
    }

    private void createSelectColumnsForWildcard0(
            QueryModel srcModel,
            int wildcardPosition,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel groupByModel,
            QueryModel analyticModel,
            QueryModel outerModel,
            boolean hasJoins
    ) throws ParserException {
        final ObjList<CharSequence> columnNames = srcModel.getColumnNames();
        for (int j = 0, z = columnNames.size(); j < z; j++) {
            CharSequence name = columnNames.getQuick(j);
            CharSequence token;
            if (hasJoins) {
                Lexer2.NameAssembler nameAssembler = lexer.getNameAssembler();
                nameAssembler.put(srcModel.getName());
                nameAssembler.put('.');
                nameAssembler.put(name);
                token = nameAssembler.toImmutable();
            } else {
                token = name;
            }
            createSelectColumn(
                    name,
                    nextLiteral(token, wildcardPosition),
                    null, // do not validate
                    translatingModel,
                    innerModel,
                    groupByModel,
                    analyticModel,
                    outerModel
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

            switch (m.getJoinType()) {
                case QueryModel.JOIN_CROSS:
                    cost += 10;
                    break;
                default:
                    cost += 5;
                    break;
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

    private void emitAggregates(@Transient ExprNode node, QueryModel model) {

        this.exprNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!this.exprNodeStack.isEmpty() || node != null) {
            if (node != null) {

                if (node.rhs != null) {
                    ExprNode n = replaceIfAggregate(node.rhs, model);
                    if (node.rhs == n) {
                        this.exprNodeStack.push(node.rhs);
                    } else {
                        node.rhs = n;
                    }
                }

                ExprNode n = replaceIfAggregate(node.lhs, model);
                if (n == node.lhs) {
                    node = node.lhs;
                } else {
                    node.lhs = n;
                    node = null;
                }
            } else {
                node = this.exprNodeStack.poll();
            }
        }
    }

    private void emitLiterals(
            @Transient ExprNode node,
            QueryModel translatingModel,
            QueryModel innerModel,
            QueryModel validatingModel
    ) throws ParserException {

        this.exprNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!this.exprNodeStack.isEmpty() || node != null) {
            if (node != null) {

                if (node.rhs != null) {
                    ExprNode n = replaceLiteral(node.rhs, translatingModel, innerModel, validatingModel);
                    if (node.rhs == n) {
                        this.exprNodeStack.push(node.rhs);
                    } else {
                        node.rhs = n;
                    }
                }

                ExprNode n = replaceLiteral(node.lhs, translatingModel, innerModel, validatingModel);
                if (n == node.lhs) {
                    node = node.lhs;
                } else {
                    node.lhs = n;
                    node = null;
                }
            } else {
                node = this.exprNodeStack.poll();
            }
        }
    }

    private ParserException err(String msg) {
        return ParserException.$(lexer.position(), msg);
    }

    private ParserException errUnexpected(CharSequence token) {
        return ParserException.unexpectedToken(lexer.position(), token);
    }

    private ExprNode expectExpr() throws ParserException {
        ExprNode n = expr();
        if (n == null) {
            throw ParserException.$(lexer.position(), "Expression expected");
        }
        return n;
    }

    private int expectInt() throws ParserException {
        try {
            return Numbers.parseInt(tok("integer"));
        } catch (NumericException e) {
            throw err("bad integer");
        }
    }

    private ExprNode expectLiteral() throws ParserException {
        CharSequence tok = tok("literal");
        int pos = lexer.position();
        validateLiteral(pos, tok);
        return nextLiteral(lexer.toImmutable(tok), pos);
    }

    private CharSequence expectTableNameOrSubQuery() throws ParserException {
        return tok("table name or sub-query");
    }

    private void expectTok(CharSequence tok, CharSequence expected) throws ParserException {
        if (tok == null || !Chars.equals(tok, expected)) {
            throw ParserException.position(lexer.position()).put('\'').put(expected).put("' expected");
        }
    }

    private void expectTok(CharSequence expected) throws ParserException {
        CharSequence tok = optTok();
        if (tok == null) {
            throw ParserException.position(lexer.position()).put('\'').put(expected).put("' expected");
        }
        expectTok(tok, expected);
    }

    private void expectTok(char expected) throws ParserException {
        CharSequence tok = optTok();
        if (tok == null) {
            throw ParserException.position(lexer.position()).put(expected).put(" expected");
        }
        expectTok(tok, lexer.position(), expected);
    }

    private void expectTok(CharSequence tok, int pos, char expected) throws ParserException {
        if (tok == null || !Chars.equals(tok, expected)) {
            throw ParserException.position(pos).put('\'').put(expected).put("' expected");
        }
    }

    private ExprNode expr() throws ParserException {
        astBuilder.reset();
        exprParser.parseExpr(lexer, astBuilder);
        return rewriteCase(astBuilder.poll());
    }

    private CharSequence extractColumnName(CharSequence token, int dot) {
        return dot == -1 ? token : lexer.toImmutable(token, dot + 1, token.length());
    }

    private int getCreateTableColumnIndex(CreateTableModel model, CharSequence columnName, int position) throws ParserException {
        int index = model.getColumnIndex(columnName);
        if (index == -1) {
            throw ParserException.invalidColumn(position, columnName);
        }
        return index;
    }

    private int getIndexOfTableForColumn(QueryModel model, CharSequence column, int dot, int position) throws ParserException {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        int index = -1;
        if (dot == -1) {
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                if (joinModels.getQuick(i).getColumnNameTypeMap().excludes(column)) {
                    continue;
                }

                if (index != -1) {
                    throw ParserException.ambiguousColumn(position);
                }

                index = i;
            }

            if (index == -1) {
                throw ParserException.invalidColumn(position, column);
            }

            return index;
        } else {
            index = model.getAliasIndex(column, 0, dot);

            if (index == -1) {
                throw ParserException.$(position, "Invalid table name or alias");
            }

            if (joinModels.getQuick(index).getColumnNameTypeMap().excludes(column, dot + 1, column.length())) {
                throw ParserException.invalidColumn(position, column);
            }

            return index;
        }
    }

    private boolean hasAggregates(ExprNode node) {

        this.exprNodeStack.clear();

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        while (!this.exprNodeStack.isEmpty() || node != null) {
            if (node != null) {
                switch (node.type) {
                    case ExprNode.LITERAL:
                        node = null;
                        continue;
                    case ExprNode.FUNCTION:
                        if (FunctionFactories.isAggregate(node.token)) {
                            return true;
                        }
                        break;
                    default:
                        if (node.rhs != null) {
                            this.exprNodeStack.push(node.rhs);
                        }
                        break;
                }

                node = node.lhs;
            } else {
                node = this.exprNodeStack.poll();
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
            } else if (m.getJoinType() != QueryModel.JOIN_ASOF && (c == null || c.parents.size() == 0)) {
                m.setJoinType(QueryModel.JOIN_CROSS);
            }
        }
    }

    private boolean isFieldTerm(CharSequence tok) {
        return Chars.equals(tok, ')') || Chars.equals(tok, ',');
    }

    private ExprNode literal(CharSequence name) {
        // this can never be null in its current contexts
        // every time this function is called is after lexer.unparse(), which ensures non-null token.
        return exprNodePool.next().of(ExprNode.LITERAL, lexer.unquote(name), 0, lexer.position());
    }

    private ExprNode makeJoinAlias(int index) {
        Lexer2.NameAssembler nameAssembler = lexer.getNameAssembler();
        nameAssembler.put(QueryModel.SUB_QUERY_ALIAS_PREFIX).put(index);
        return nextLiteral(nameAssembler.toImmutable());
    }

    private ExprNode makeModelAlias(CharSequence modelAlias, ExprNode node) {
        Lexer2.NameAssembler nameAssembler = lexer.getNameAssembler();
        nameAssembler.put(modelAlias).put('.').put(node.token);
        return nextLiteral(nameAssembler.toImmutable(), node.position);
    }

    private ExprNode makeOperation(CharSequence token, ExprNode lhs, ExprNode rhs) {
        ExprNode expr = exprNodePool.next().of(ExprNode.OPERATION, token, 0, 0);
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
            ExprNode bao = b.aNodes.getQuick(i);

            CharSequence bbn = b.bNames.getQuick(i);
            int bbi = b.bIndexes.getQuick(i);
            ExprNode bbo = b.bNodes.getQuick(i);

            for (int k = 0, z = a.aNames.size(); k < z; k++) {

                // don't seem to be adding indexes outside of main loop
//                if (deletedContexts.contains(k)) {
//                    continue;
//                }

                final CharSequence aan = a.aNames.getQuick(k);
                final int aai = a.aIndexes.getQuick(k);
                final ExprNode aao = a.aNodes.getQuick(k);
                final CharSequence abn = a.bNames.getQuick(k);
                final int abi = a.bIndexes.getQuick(k);
                final ExprNode abo = a.bNodes.getQuick(k);

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
            int max = bai > bbi ? bai : bbi;
            int min = bai < bbi ? bai : bbi;
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

    private void moveWhereInsideSubQueries(QueryModel model) throws ParserException {
        model.getParsedWhere().clear();
        final ObjList<ExprNode> nodes = model.parseWhereClause();
        model.setWhereClause(null);

        final int n = nodes.size();
        if (n > 0) {
            for (int i = 0; i < n; i++) {
                final ExprNode node = nodes.getQuick(i);
                // collect table references this where clause element
                literalCollectorAIndexes.clear();
                literalCollectorANames.clear();
                literalCollector.withModel(model);
                literalCollector.resetNullCount();
                traversalAlgo.traverse(node, literalCollector.lhs());

                // at this point we must not have constant conditions in where clause
                assert literalCollectorAIndexes.size() > 0;

                // by now all where clause must reference single table only and all column references have to be valid
                // they would have been rewritten and validated as join analysis stage
                final int tableIndex = literalCollectorAIndexes.getQuick(0);
                final QueryModel parent = model.getJoinModels().getQuick(tableIndex);
                final QueryModel nested = parent.getNestedModel();
                if (nested == null) {
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

                    // because we are rewriting ExprNode in-place we need to make sure that
                    // none of expression literals reference non-literals in nested query, e.g.
                    // (select a+b x from T) where x > 10
                    // does not warrant inlining of "x > 10" because "x" is not a column
                    //
                    // at this step we would throw exception if one of our literals hits non-literal
                    // in sub-query

                    try {
                        traversalAlgo.traverse(node, literalCheckingVisitor.of(nested.getColumnNameTypeMap()));

                        // go ahead and rewrite expression
                        traversalAlgo.traverse(node, literalRewritingVisitor.of(nested.getAliasToColumnMap()));

                        // whenever nested model has explicitly defined columns it must also
                        // have its owen nested model, where we assign new "where" clauses
                        addWhereNode(nested, node);
                    } catch (NonLiteralException ignore) {
                        // keep node where it is
                        addWhereNode(parent, node);
                    }
                }
            }
            model.getParsedWhere().clear();
        }

        if (model.getNestedModel() != null) {
            moveWhereInsideSubQueries(model.getNestedModel());
        }

        ObjList<QueryModel> joinModels = model.getJoinModels();
        for (int i = 0, m = joinModels.size(); i < m; i++) {
            QueryModel nested = joinModels.getQuick(i);
            if (nested != model) {
                moveWhereInsideSubQueries(nested);
            }
        }
    }

    private QueryColumn nextColumn(CharSequence alias, CharSequence name) {
        return queryColumnPool.next().of(alias, nextLiteral(name, 0));
    }

    private QueryColumn nextColumn(CharSequence name) {
        return nextColumn(name, name);
    }

    private ExprNode nextLiteral(CharSequence token, int position) {
        return exprNodePool.next().of(ExprNode.LITERAL, token, 0, position);
    }

    private ExprNode nextLiteral(CharSequence token) {
        return nextLiteral(token, 0);
    }

    private CharSequence notTermTok() throws ParserException {
        CharSequence tok = tok("')' or ','");
        if (isFieldTerm(tok)) {
            throw err("missing column definition");
        }
        return tok;
    }

    private CharSequence optTok() {
        CharSequence tok = lexer.optionTok();
        if (tok == null || (subQueryMode && Chars.equals(tok, ')'))) {
            return null;
        }
        return tok;
    }

    private QueryModel optimise(QueryModel model) throws ParserException {
        enumerateTableColumns(model);
        resolveJoinColumns(model);
        optimiseBooleanNot(model);
        final QueryModel rewrittenModel = rewriteOrderBy(rewriteSelectClause(model, true));
        optimiseOrderBy(rewrittenModel, ORDER_BY_UNKNOWN);
        createOrderHash(rewrittenModel);
        optimiseJoins(rewrittenModel);
        moveWhereInsideSubQueries(rewrittenModel);
        return rewrittenModel;
    }

    private ExprNode optimiseBooleanNot(final ExprNode node, boolean reverse) throws ParserException {
        switch (notOps.get(node.token)) {
            case NOT_OP_NOT:
                if (reverse) {
                    return optimiseBooleanNot(node.rhs, false);
                } else {
                    switch (node.rhs.type) {
                        case ExprNode.LITERAL:
                        case ExprNode.CONSTANT:
                            return node;
                        default:
                            assertNotNull(node.rhs, node.position, "Missing right argument");
                            return optimiseBooleanNot(node.rhs, true);
                    }
                }
            case NOT_OP_AND:
                if (reverse) {
                    node.token = "or";
                }
                assertNotNull(node.lhs, node.position, "Missing right argument");
                assertNotNull(node.rhs, node.position, "Missing left argument");
                node.lhs = optimiseBooleanNot(node.lhs, reverse);
                node.rhs = optimiseBooleanNot(node.rhs, reverse);
                return node;
            case NOT_OP_OR:
                if (reverse) {
                    node.token = "and";
                }
                assertNotNull(node.lhs, node.position, "Missing right argument");
                assertNotNull(node.rhs, node.position, "Missing left argument");
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
                }
                return node;
            default:
                if (reverse) {
                    ExprNode n = exprNodePool.next();
                    n.token = "not";
                    n.paramCount = 1;
                    n.rhs = node;
                    n.type = ExprNode.OPERATION;
                    return n;
                }
                return node;
        }
    }

    private void optimiseBooleanNot(QueryModel model) throws ParserException {
        ExprNode where = model.getWhereClause();
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
    }

    private void optimiseJoins(QueryModel model) throws ParserException {
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

            ExprNode where = model.getWhereClause();

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
        }
    }

    // removes redundant order by clauses from sub-queries
    private void optimiseOrderBy(QueryModel model, int orderByState) {
        ObjList<QueryColumn> columns = model.getColumns();
        int subQueryOrderByState;

        int n = columns.size();
        // determine if ordering is required
        switch (orderByState) {
            case ORDER_BY_UNKNOWN:
                // we have sample by, so expect sub-query has to be ordered
                subQueryOrderByState = ORDER_BY_REQUIRED;
                if (model.getSampleBy() == null) {
                    for (int i = 0; i < n; i++) {
                        QueryColumn col = columns.getQuick(i);
                        if (hasAggregates(col.getAst())) {
                            subQueryOrderByState = ORDER_BY_INVARIANT;
                            break;
                        }
                    }
                }
                break;
            case ORDER_BY_REQUIRED:
                // parent requires order
                // if this model forces ordering - sub-query ordering is not needed
                if (model.getOrderBy().size() > 0) {
                    subQueryOrderByState = ORDER_BY_INVARIANT;
                } else {
                    subQueryOrderByState = ORDER_BY_REQUIRED;
                }
                break;
            default:
                // sub-query ordering is not needed
                model.getOrderBy().clear();
                if (model.getSampleBy() != null) {
                    subQueryOrderByState = ORDER_BY_REQUIRED;
                } else {
                    subQueryOrderByState = ORDER_BY_INVARIANT;
                }
                break;
        }

        ObjList<QueryModel> jm = model.getJoinModels();
        for (int i = 0, k = jm.size(); i < k; i++) {
            QueryModel qm = jm.getQuick(i).getNestedModel();
            if (qm != null) {
                optimiseOrderBy(qm, subQueryOrderByState);
            }
        }
    }

    private ParsedModel parseCreateStatement() throws ParserException {
        expectTok("table");
        return parseCreateTable();
    }

    private ParsedModel parseCreateTable() throws ParserException {
        final CreateTableModel model = createTableModelPool.next();
        model.setName(nextLiteral(lexer.unquote(tok("table name")), lexer.position()));

        CharSequence tok = tok("'(' or 'as'");

        if (Chars.equals(tok, '(')) {
            lexer.unparse();
            parseCreateTableColumns(model);
        } else if (Chars.equals(tok, "as")) {
            parseCreateTableAsSelect(model);
        } else {
            throw errUnexpected(tok);
        }

        while ((tok = optTok()) != null && Chars.equals(tok, ',')) {
            tok = tok("'index' or 'cast'");
            if (Chars.equals(tok, "index")) {
                parseCreateTableIndexDef(model);
            } else if (Chars.equals(tok, "cast")) {
                parseCreateTableCastDef(model);
            } else {
                throw errUnexpected(tok);
            }
        }

        ExprNode timestamp = parseTimestamp(tok);
        if (timestamp != null) {
            // ignore index, validate column
            getCreateTableColumnIndex(model, timestamp.token, timestamp.position);
            model.setTimestamp(timestamp);
            tok = optTok();
        }

        ExprNode partitionBy = parseCreateTablePartition(tok);
        if (partitionBy != null) {
            if (PartitionBy.fromString(partitionBy.token) == -1) {
                throw ParserException.$(partitionBy.position, "'NONE', 'DAY', 'MONTH' or 'YEAR' expected");
            }
            model.setPartitionBy(partitionBy);
            tok = optTok();
        }

        if (tok != null) {
            throw errUnexpected(tok);
        }
        return model;
    }

    private void parseCreateTableAsSelect(CreateTableModel model) throws ParserException {
        expectTok('(');
        QueryModel queryModel = optimise(parseDml());
        ObjList<QueryColumn> columns = queryModel.getColumns();
        assert columns.size() > 0;

        // we do not know types of columns at this stage
        // compiler must put table together using query metadata.
        for (int i = 0, n = columns.size(); i < n; i++) {
            model.addColumn(columns.getQuick(i).getName(), -1, configuration.getCutlassSymbolCapacity());
        }

        model.setQueryModel(queryModel);
        expectTok(')');
    }

    private void parseCreateTableCastDef(CreateTableModel model) throws ParserException {
        if (model.getQueryModel() == null) {
            throw ParserException.$(lexer.position(), "cast is only supported in 'create table as ...' context");
        }
        expectTok('(');
        ColumnCastModel columnCastModel = columnCastModelPool.next();

        columnCastModel.setName(expectLiteral());
        expectTok("as");

        final ExprNode node = expectLiteral();
        final int type = toColumnType(node.token);
        columnCastModel.setType(type, node.position);

        if (type == ColumnType.SYMBOL) {
            if (Chars.equals(optTok(), "capacity")) {
                columnCastModel.setSymbolCapacity(expectInt());
            } else {
                lexer.unparse();
                columnCastModel.setSymbolCapacity(configuration.getCutlassSymbolCapacity());
            }
        }

        expectTok(')');

        if (!model.addColumnCastModel(columnCastModel)) {
            throw ParserException.$(columnCastModel.getName().position, "duplicate cast");
        }
    }

    private void parseCreateTableColumns(CreateTableModel model) throws ParserException {
        expectTok('(');

        while (true) {
            final int position = lexer.position();
            final CharSequence name = lexer.toImmutable(notTermTok());
            final int type = toColumnType(notTermTok());

            if (!model.addColumn(name, type, configuration.getCutlassSymbolCapacity())) {
                throw ParserException.$(position, "Duplicate column");
            }

            CharSequence tok;
            switch (type) {
                case ColumnType.SYMBOL:
                    tok = tok("'capacity', 'nocache', 'cache', 'index' or ')'");

                    if (Chars.equals(tok, "capacity")) {
                        model.symbolCapacity(expectInt());
                        tok = tok("'nocache', 'cache', 'index' or ')'");
                    }

                    if (Chars.equals(tok, "nocache")) {
                        model.cached(false);
                    } else if (Chars.equals(tok, "cache")) {
                        model.cached(true);
                    } else {
                        lexer.unparse();
                    }
                    tok = parseCreateTableInlineIndexDef(model);
                    break;
                default:
                    tok = null;
                    break;
            }

            if (tok == null) {
                tok = tok("',' or ')'");
            }

            if (Chars.equals(tok, ')')) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw err("',' or ')' expected");
            }
        }
    }

    private void parseCreateTableIndexDef(CreateTableModel model) throws ParserException {
        expectTok('(');
        final int columnIndex = getCreateTableColumnIndex(model, expectLiteral().token, lexer.position());

        if (Chars.equals(tok("'capacity'"), "capacity")) {
            model.setIndexFlags(columnIndex, true, Numbers.ceilPow2(expectInt()) - 1);
        } else {
            model.setIndexFlags(columnIndex, true, configuration.getIndexValueBlockSize());
            lexer.unparse();
        }
        expectTok(')');
    }

    private CharSequence parseCreateTableInlineIndexDef(CreateTableModel model) throws ParserException {
        CharSequence tok = tok("')', or 'index'");

        if (isFieldTerm(tok)) {
            model.setIndexFlags(false, configuration.getIndexValueBlockSize());
            return tok;
        }

        expectTok(tok, "index");

        if (isFieldTerm(tok = tok(") | , expected"))) {
            model.setIndexFlags(true, configuration.getIndexValueBlockSize());
            return tok;
        }

        expectTok(tok, "capacity");
        model.setIndexFlags(true, expectInt());
        return null;
    }

    private ExprNode parseCreateTablePartition(CharSequence tok) throws ParserException {
        if (Chars.equalsNc("partition", tok)) {
            expectTok("by");
            return expectLiteral();
        }
        return null;
    }

    private QueryModel parseDml() throws ParserException {

        CharSequence tok;
        QueryModel model = queryModelPool.next();

        tok = tok("'select', 'with' or table name expected");

        if (Chars.equals(tok, "with")) {
            parseWithClauses(model);
            tok = tok("'select' or table name expected");
        }

        // [select]
        if (Chars.equals(tok, "select")) {
            parseSelectClause(model);
        } else {
            lexer.unparse();
            // do not default to wildcard column selection when
            // dealing with sub-queries
            if (subQueryMode) {
                parseFromClause(model, model);
                return model;
            }
            model.addColumn(nextColumn("*"));
        }
        QueryModel nestedModel = queryModelPool.next();
        parseFromClause(nestedModel, model);
        model.setSelectModelType(QueryModel.SELECT_MODEL_CHOOSE);
        model.setNestedModel(nestedModel);
        return model;
    }

    private void parseFromClause(QueryModel model, QueryModel masterModel) throws ParserException {
        CharSequence tok = expectTableNameOrSubQuery();
        // expect "(" in case of sub-query

        if (Chars.equals(tok, '(')) {
            model.setNestedModel(parseSubQuery());

            // expect closing bracket
            expectTok(')');

            tok = optTok();

            // check if tok is not "where" - should be alias

            if (tok != null && tableAliasStop.excludes(tok)) {
                model.setAlias(literal(tok));
                tok = optTok();
            }

            // expect [timestamp(column)]

            ExprNode timestamp = parseTimestamp(tok);
            if (timestamp != null) {
                model.setTimestamp(timestamp);
                tok = optTok();
            }
        } else {

            parseSelectFrom(model, tok, masterModel);

            tok = optTok();

            if (tok != null && tableAliasStop.excludes(tok)) {
                model.setAlias(literal(tok));
                tok = optTok();
            }

            // expect [timestamp(column)]

            ExprNode timestamp = parseTimestamp(tok);
            if (timestamp != null) {
                model.setTimestamp(timestamp);
                tok = optTok();
            }

            // expect [latest by]

            if (Chars.equalsNc("latest", tok)) {
                parseLatestBy(model);
                tok = optTok();
            }
        }

        // expect multiple [[inner | outer | cross] join]

        int joinType;
        while (tok != null && (joinType = joinStartSet.get(tok)) != -1) {
            model.addJoinModel(parseJoin(tok, joinType, masterModel));
            tok = optTok();
        }

        // expect [where]

        if (tok != null && Chars.equals(tok, "where")) {
            model.setWhereClause(expr());
            tok = optTok();
        }

        // expect [group by]

        if (tok != null && Chars.equals(tok, "sample")) {
            expectTok("by");
            model.setSampleBy(expectLiteral());
            tok = optTok();
        }

        // expect [order by]

        if (tok != null && Chars.equals(tok, "order")) {
            expectTok("by");
            do {
                ExprNode n = expectLiteral();

                tok = optTok();

                if (tok != null && Chars.equalsIgnoreCase(tok, "desc")) {

                    model.addOrderBy(n, QueryModel.ORDER_DIRECTION_DESCENDING);
                    tok = optTok();

                } else {

                    model.addOrderBy(n, QueryModel.ORDER_DIRECTION_ASCENDING);

                    if (tok != null && Chars.equalsIgnoreCase(tok, "asc")) {
                        tok = optTok();
                    }
                }

                if (model.getOrderBy().size() >= MAX_ORDER_BY_COLUMNS) {
                    throw err("Too many columns");
                }

            } while (tok != null && Chars.equals(tok, ','));
        }

        // expect [limit]
        if (tok != null && Chars.equals(tok, "limit")) {
            ExprNode lo = expr();
            ExprNode hi = null;

            tok = optTok();
            if (tok != null && Chars.equals(tok, ',')) {
                hi = expr();
            } else {
                lexer.unparse();
            }
            model.setLimit(lo, hi);
        } else {
            lexer.unparse();
        }
    }

    private QueryModel parseJoin(CharSequence tok, int joinType, QueryModel parent) throws ParserException {
        QueryModel joinModel = queryModelPool.next();
        joinModel.setJoinType(joinType);

        if (!Chars.equals(tok, "join")) {
            expectTok("join");
        }

        tok = expectTableNameOrSubQuery();

        if (Chars.equals(tok, '(')) {
            joinModel.setNestedModel(parseSubQuery());
            expectTok(')');
        } else {
            parseSelectFrom(joinModel, tok, parent);
        }

        tok = optTok();

        if (tok != null && tableAliasStop.excludes(tok)) {
            lexer.unparse();
            joinModel.setAlias(expr());
        } else {
            lexer.unparse();
        }

        tok = optTok();

        if (joinType == QueryModel.JOIN_CROSS && tok != null && Chars.equals(tok, "on")) {
            throw ParserException.$(lexer.position(), "Cross joins cannot have join clauses");
        }

        switch (joinType) {
            case QueryModel.JOIN_ASOF:
                if (tok == null || !Chars.equals("on", tok)) {
                    lexer.unparse();
                    break;
                }
                // intentional fall through
            case QueryModel.JOIN_INNER:
            case QueryModel.JOIN_OUTER:
                expectTok(tok, "on");
                astBuilder.reset();
                exprParser.parseExpr(lexer, astBuilder);
                ExprNode expr;
                switch (astBuilder.size()) {
                    case 0:
                        throw ParserException.$(lexer.position(), "Expression expected");
                    case 1:
                        expr = astBuilder.poll();
                        if (expr.type == ExprNode.LITERAL) {
                            do {
                                joinModel.addJoinColumn(expr);
                            } while ((expr = astBuilder.poll()) != null);
                        } else {
                            joinModel.setJoinCriteria(expr);
                        }
                        break;
                    default:
                        // this code handles "join on (a,b,c)", e.g. list of columns
                        while ((expr = astBuilder.poll()) != null) {
                            if (expr.type != ExprNode.LITERAL) {
                                throw ParserException.$(lexer.position(), "Column name expected");
                            }
                            joinModel.addJoinColumn(expr);
                        }
                        break;
                }
                break;
            default:
                lexer.unparse();
        }

        return joinModel;
    }

    private void parseLatestBy(QueryModel model) throws ParserException {
        expectTok("by");
        model.setLatestBy(expr());
    }

    private ParsedModel parseRenameStatement() throws ParserException {
        expectTok("table");
        RenameTableModel model = renameTableModelPool.next();
        ExprNode e = expectExpr();
        if (e.type != ExprNode.LITERAL && e.type != ExprNode.CONSTANT) {
            throw ParserException.$(e.position, "literal or constant expected");
        }
        model.setFrom(e);
        expectTok("to");

        e = expectExpr();
        if (e.type != ExprNode.LITERAL && e.type != ExprNode.CONSTANT) {
            throw ParserException.$(e.position, "literal or constant expected");
        }
        model.setTo(e);
        return model;
    }

    private ParsedModel parseSelect() throws ParserException {
        lexer.unparse();
        final QueryModel model = parseDml();
        final CharSequence tok = optTok();
        if (tok != null) {
            throw errUnexpected(tok);
        }
        return optimise(model);
    }

    private void parseSelectClause(QueryModel model) throws ParserException {
        while (true) {
            CharSequence tok = tok("column");

            final ExprNode expr;
            // this is quite dramatic workaround for lexer
            // because lexer tokenizes expressions, for something like 'a.*' it would
            // produce two tokens, 'a.' and '*'
            // we should be able to tell if they are together or there is whitespace between them
            // for example "a.  *' would also produce two token and it must be a error
            // to determine if wildcard is correct we would rely on token position
            final char last = tok.charAt(tok.length() - 1);
            if (last == '*') {
                expr = nextLiteral(lexer.toImmutable(tok), lexer.position());
            } else if (last == '.') {
                // stash 'a.' token
                final int pos = lexer.position() + tok.length();
                Lexer2.NameAssembler nameAssembler = lexer.getNameAssembler();
                nameAssembler.put(tok);
                tok = tok("*");
                if (Chars.equals(tok, '*')) {
                    if (lexer.position() > pos) {
                        throw ParserException.$(pos, "whitespace is not allowed");
                    }
                    nameAssembler.put('*');
                    expr = nextLiteral(nameAssembler.toImmutable(), lexer.position());
                } else {
                    throw ParserException.$(pos, "'*' expected");
                }
            } else {
                lexer.unparse();
                expr = expr();

                if (expr == null) {
                    throw ParserException.$(lexer.position(), "missing expression");
                }
            }

            CharSequence alias;

            tok = tok("',', 'from', 'over' or literal");

            if (columnAliasStop.excludes(tok)) {
                if (Chars.indexOf(tok, '.') != -1) {
                    throw ParserException.$(lexer.position(), "'.' is not allowed here");
                }
                alias = lexer.toImmutable(tok);
                tok = tok("',', 'from' or 'over'");
            } else {
                alias = createColumnAlias(expr, model);
            }

            if (Chars.equals(tok, "over")) {
                // analytic
                expectTok('(');

                AnalyticColumn col = analyticColumnPool.next().of(alias, expr);
                tok = tok("'");

                if (Chars.equals(tok, "partition")) {
                    expectTok("by");

                    ObjList<ExprNode> partitionBy = col.getPartitionBy();

                    do {
                        partitionBy.add(expectLiteral());
                        tok = tok("'order' or ')'");
                    } while (Chars.equals(tok, ','));
                }

                if (Chars.equals(tok, "order")) {
                    expectTok("by");

                    do {
                        ExprNode e = expectLiteral();
                        tok = tok("'asc' or 'desc'");

                        if (Chars.equalsIgnoreCase(tok, "desc")) {
                            col.addOrderBy(e, QueryModel.ORDER_DIRECTION_DESCENDING);
                            tok = tok("',' or ')'");
                        } else {
                            col.addOrderBy(e, QueryModel.ORDER_DIRECTION_ASCENDING);
                            if (Chars.equalsIgnoreCase(tok, "asc")) {
                                tok = tok("',' or ')'");
                            }
                        }
                    } while (Chars.equals(tok, ','));
                }
                expectTok(tok, lexer.position(), ')');
                model.addColumn(col);
                tok = tok("'from' or ','");
            } else {
                model.addColumn(queryColumnPool.next().of(alias, expr));
            }

            if (Chars.equals(tok, "from")) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw err("',' or 'from' expected");
            }
        }
    }

    private void parseSelectFrom(QueryModel model, CharSequence name, QueryModel masterModel) throws ParserException {
        final ExprNode literal = literal(name);
        final WithClauseModel withClause = masterModel.getWithClause(name);
        if (withClause != null) {
            model.setNestedModel(parseWith(withClause));
            model.setAlias(literal);
        } else {
            model.setTableName(literal);
        }
    }

    private QueryModel parseSubQuery() throws ParserException {
        this.subQueryMode = true;
        try {
            return parseDml();
        } finally {
            this.subQueryMode = false;
        }
    }

    private ExprNode parseTimestamp(CharSequence tok) throws ParserException {
        if (Chars.equalsNc("timestamp", tok)) {
            expectTok('(');
            final ExprNode result = expectLiteral();
            expectTok(')');
            return result;
        }
        return null;
    }

    private QueryModel parseWith(WithClauseModel wcm) throws ParserException {
        QueryModel m = wcm.popModel();
        if (m != null) {
            return m;
        }

        secondaryLexer.setContent(lexer.getContent(), wcm.getLo(), wcm.getHi());

        // we would have parsed this before without exceptions
        Lexer2 tmp = this.lexer;
        this.lexer = secondaryLexer;
        QueryModel model = parseSubQuery();
        lexer = tmp;
        return model;
    }

    private void parseWithClauses(QueryModel model) throws ParserException {
        do {
            ExprNode name = expectLiteral();

            if (model.getWithClause(name.token) != null) {
                throw ParserException.$(name.position, "duplicate name");
            }

            expectTok("as");
            expectTok('(');
            int lo, hi;
            lo = lexer.position();
            QueryModel m = parseSubQuery();
            hi = lexer.position();
            WithClauseModel wcm = withClauseModelPool.next();
            wcm.of(lo + 1, hi, m);
            expectTok(')');
            model.addWithClause(name.token, wcm);

            CharSequence tok = optTok();
            if (tok == null || !Chars.equals(tok, ',')) {
                lexer.unparse();
                break;
            }
        } while (true);
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
    private void processJoinConditions(QueryModel parent, ExprNode node) throws ParserException {
        ExprNode n = node;
        // pre-order traversal
        exprNodeStack.clear();
        while (!exprNodeStack.isEmpty() || n != null) {
            if (n != null) {
                switch (joinOps.get(n.token)) {
                    case JOIN_OP_EQUAL:
                        analyseEquals(parent, n);
                        n = null;
                        break;
                    case JOIN_OP_AND:
                        if (n.rhs != null) {
                            exprNodeStack.push(n.rhs);
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
                n = exprNodeStack.poll();
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
    private void processOrConditions(QueryModel parent, ExprNode node) {
        // stub: use filter
        parent.addParsedWhereNode(node);
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
                    JoinContext jc = joinModels.getQuick(to).getContext();
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

    private ExprNode replaceIfAggregate(@Transient ExprNode node, QueryModel model) {
        if (node != null && FunctionFactories.isAggregate(node.token)) {
            QueryColumn c = queryColumnPool.next().of(createColumnAlias(node, model), node);
            model.addColumn(c);
            return nextLiteral(c.getAlias());
        }
        return node;
    }

    private ExprNode replaceLiteral(@Transient ExprNode node, QueryModel translatingModel, QueryModel
            innerModel, QueryModel validatingModel) throws ParserException {
        if (node != null && node.type == ExprNode.LITERAL) {
            final CharSequenceObjHashMap<CharSequence> map = translatingModel.getColumnToAliasMap();
            int index = map.keyIndex(node.token);
            if (index > -1) {
                // this is the first time we see this column and must create alias
                CharSequence alias = createColumnAlias(node, translatingModel);
                QueryColumn column = queryColumnPool.next().of(alias, node);
                // add column to both models
                addColumnToTranslatingModel(column, translatingModel, validatingModel);
                if (innerModel != null) {
                    innerModel.addColumn(column);
                }
                return nextLiteral(alias, node.position);
            }
            return nextLiteral(map.valueAt(index), node.position);
        }
        return node;
    }

    private void resolveJoinColumns(QueryModel model) throws ParserException {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        final int size = joinModels.size();
        final CharSequence modelAlias = setAndGetModelAlias(model);
        // collect own alias
        collectAlias(model, 0, model);
        if (size > 1) {
            for (int i = 1; i < size; i++) {
                final QueryModel jm = joinModels.getQuick(i);
                final ObjList<ExprNode> jc = jm.getJoinColumns();
                final int joinColumnsSize = jc.size();

                if (joinColumnsSize > 0) {
                    final CharSequence jmAlias = setAndGetModelAlias(jm);
                    ExprNode joinCriteria = jm.getJoinCriteria();
                    for (int j = 0; j < joinColumnsSize; j++) {
                        ExprNode node = jc.getQuick(j);
                        ExprNode eq = makeOperation("=", makeModelAlias(modelAlias, node), makeModelAlias(jmAlias, node));
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
    }

    private ExprNode rewriteCase(ExprNode node) throws ParserException {
        traversalAlgo.traverse(node, node1 -> {
            if (node1.type == ExprNode.FUNCTION && Chars.equals("case", node1.token)) {
                tempExprNodes.clear();
                ExprNode literal = null;
                ExprNode elseExpr;
                boolean convertToSwitch = true;
                final int paramCount = node1.paramCount;
                final int lim;
                if ((paramCount & 1) == 1) {
                    elseExpr = node1.args.getQuick(0);
                    lim = 0;
                } else {
                    elseExpr = null;
                    lim = -1;
                }

                for (int i = paramCount - 1; i > lim; i--) {
                    if ((i & 1) == 1) {
                        // this is "then" clause, copy it as as
                        tempExprNodes.add(node1.args.getQuick(i));
                        continue;
                    }
                    ExprNode where = node1.args.getQuick(i);
                    if (where.type == ExprNode.OPERATION && where.token.charAt(0) == '=') {
                        ExprNode thisConstant;
                        ExprNode thisLiteral;
                        if (where.lhs.type == ExprNode.CONSTANT && where.rhs.type == ExprNode.LITERAL) {
                            thisConstant = where.lhs;
                            thisLiteral = where.rhs;
                        } else if (where.lhs.type == ExprNode.LITERAL && where.rhs.type == ExprNode.CONSTANT) {
                            thisConstant = where.rhs;
                            thisLiteral = where.lhs;
                        } else {
                            convertToSwitch = false;
                            // not supported
                            break;
                        }

                        if (literal == null) {
                            literal = thisLiteral;
                            tempExprNodes.add(thisConstant);
                        } else if (Chars.equals(literal.token, thisLiteral.token)) {
                            tempExprNodes.add(thisConstant);
                        } else {
                            convertToSwitch = false;
                            // not supported
                            break;
                        }
                    } else {
                        convertToSwitch = false;
                        // not supported
                        break;
                    }
                }

                if (convertToSwitch) {
                    node1.token = "switch";
                    node1.args.clear();
                    node1.args.add(literal);
                    node1.args.add(elseExpr);
                    node1.args.addAll(tempExprNodes);
                }
            }
        });
        return node;
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
     * @throws ParserException when column names are ambiguos or not found at all.
     */
    private QueryModel rewriteOrderBy(QueryModel model) throws ParserException {
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
        final int modelColumnCount = model.getColumns().size();
        boolean groupBy = false;

        while (base.getColumns().size() > 0) {
            baseParent = base;
            base = base.getNestedModel();
            groupBy = groupBy || baseParent.getSelectModelType() == QueryModel.SELECT_MODEL_GROUP_BY;
        }

        // find out how "order by" columns are referenced
        ObjList<ExprNode> orderByNodes = base.getOrderBy();
        int sz = orderByNodes.size();
        if (sz > 0) {
            boolean ascendColumns = true;
            // for each order by column check how deep we need to go between "model" and "base"
            for (int i = 0; i < sz; i++) {
                final ExprNode orderBy = orderByNodes.getQuick(i);
                final CharSequence column = orderBy.token;
                final int dot = Chars.indexOf(column, '.');
                // is this a table reference?
                if (dot > -1 || model.getColumnNameTypeMap().excludes(column)) {
                    // validate column
                    getIndexOfTableForColumn(base, column, dot, orderBy.position);

                    // good news, our column matched base model
                    // this condition is to ignore order by columns that are not in select and behind group by
                    if (ascendColumns && base != model) {
                        // check if column is aliased as either
                        // "x y" or "tab.x y" or "t.x y", where "t" is alias of table "tab"
                        final CharSequenceObjHashMap<CharSequence> map = baseParent.getColumnToAliasMap();
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
                            orderBy.token = map.valueAt(index);
                        } else {
                            // we must attempt to ascend order by column
                            // when we have group by model, ascent is not possible
                            if (groupBy) {
                                ascendColumns = false;
                            } else {
                                if (baseParent.getSelectModelType() != QueryModel.SELECT_MODEL_CHOOSE) {
                                    QueryModel synthetic = queryModelPool.next();
                                    synthetic.setSelectModelType(QueryModel.SELECT_MODEL_CHOOSE);
                                    for (int j = 0, z = baseParent.getColumns().size(); j < z; j++) {
                                        synthetic.addColumn(baseParent.getColumns().getQuick(j));
                                    }
                                    synthetic.setNestedModel(base);
                                    baseParent.setNestedModel(synthetic);
                                    baseParent = synthetic;
                                }

                                // if base parent model is already "choose" type, use that and ascend alias all the way up
                                CharSequence alias = createColumnAlias(column, dot, baseParent.getColumnNameTypeMap());
                                baseParent.addColumn(nextColumn(alias, column));

                                // do we have more than one parent model?
                                if (model != baseParent) {
                                    QueryModel m = model;
                                    do {
                                        m.addColumn(nextColumn(alias));
                                        m = m.getNestedModel();
                                    } while (m != baseParent);
                                }

                                orderBy.token = alias;

                                if (wrapper == null) {
                                    wrapper = queryModelPool.next();
                                    wrapper.setSelectModelType(QueryModel.SELECT_MODEL_CHOOSE);
                                    for (int j = 0; j < modelColumnCount; j++) {
                                        wrapper.addColumn(nextColumn(model.getColumns().getQuick(j).getAlias()));
                                    }
                                    result = wrapper;
                                    wrapper.setNestedModel(model);
                                }
                            }
                        }
                    }
                }
                if (ascendColumns && base != model) {
                    model.addOrderBy(orderBy, base.getOrderByDirection().getQuick(i));
                }
            }

            if (base != model) {
                base.clearOrderBy();
            }
        }

        QueryModel nested = base.getNestedModel();
        if (nested != null) {
            QueryModel rewritten = rewriteOrderBy(nested);
            if (rewritten != nested) {
                base.setNestedModel(rewritten);
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

    // flatParent = true means that parent model does not have selected columns
    private QueryModel rewriteSelectClause(QueryModel model, boolean flatParent) throws ParserException {
        ObjList<QueryModel> models = model.getJoinModels();
        for (int i = 0, n = models.size(); i < n; i++) {
            final QueryModel m = models.getQuick(i);
            final boolean flatModel = m.getColumns().size() == 0;
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
                    throw ParserException.$(m.getSampleBy().position, "'sample by' must be used with 'select' clause, which contains aggerate expression(s)");
                }
            } else {
                model.replaceJoinModel(i, rewriteSelectClause0(m));
            }
        }

        // "model" is always first in its own list of join models
        return models.getQuick(0);
    }

    @NotNull
    private QueryModel rewriteSelectClause0(QueryModel model) throws ParserException {
        assert model.getNestedModel() != null;

        QueryModel groupByModel = queryModelPool.next();
        groupByModel.setSelectModelType(QueryModel.SELECT_MODEL_GROUP_BY);
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

        final ObjList<QueryColumn> columns = model.getColumns();
        final QueryModel baseModel = model.getNestedModel();
        final boolean hasJoins = baseModel.getJoinModels().size() > 1;

        // sample by clause should be promoted to all of the models as well as validated
        final ExprNode sampleBy = baseModel.getSampleBy();
        if (sampleBy != null) {
            final ExprNode timestamp = baseModel.getTimestamp();
            if (timestamp == null) {
                throw ParserException.$(sampleBy.position, "TIMESTAMP column is not defined");
            }

            groupByModel.setSampleBy(sampleBy);
            groupByModel.setTimestamp(timestamp);
            baseModel.setSampleBy(null);
            baseModel.setTimestamp(null);

            createSelectColumn(
                    timestamp.token,
                    timestamp,
                    baseModel,
                    translatingModel,
                    innerModel,
                    groupByModel,
                    analyticModel,
                    outerModel
            );
        }

        // create virtual columns from select list
        for (int i = 0, k = columns.size(); i < k; i++) {
            final QueryColumn qc = columns.getQuick(i);
            final boolean analytic = qc instanceof AnalyticColumn;

            // fail-fast if this is an arithmetic expression where we expect analytic function
            if (analytic && qc.getAst().type != ExprNode.FUNCTION) {
                throw ParserException.$(qc.getAst().position, "Analytic function expected");
            }

            if (qc.getAst().type == ExprNode.LITERAL) {
                // in general sense we need to create new column in case
                // there is change of alias, for example we may have something as simple as
                // select a.f, b.f from ....
                if (Chars.endsWith(qc.getAst().token, '*')) {
                    createSelectColumnsForWildcard(qc, baseModel, translatingModel, innerModel, groupByModel, analyticModel, outerModel, hasJoins);
                } else {
                    createSelectColumn(
                            qc.getAlias(),
                            qc.getAst(),
                            baseModel,
                            translatingModel,
                            innerModel,
                            groupByModel,
                            analyticModel,
                            outerModel
                    );
                }
            } else {
                // when column is direct call to aggregation function, such as
                // select sum(x) ...
                // we can add it to group-by model right away
                if (qc.getAst().type == ExprNode.FUNCTION) {
                    if (analytic) {
                        analyticModel.addColumn(qc);

                        // ensure literals referenced by analytic column are present in nested models
                        emitLiterals(qc.getAst(), translatingModel, innerModel, baseModel);
                        useAnalyticModel = true;
                        continue;
                    } else if (FunctionFactories.isAggregate(qc.getAst().token)) {
                        groupByModel.addColumn(qc);
                        // group-by column references might be needed when we have
                        // outer model supporting arithmetic such as:
                        // select sum(a)+sum(b) ....
                        outerModel.addColumn(nextColumn(qc.getAlias()));
                        // pull out literals
                        emitLiterals(qc.getAst(), translatingModel, innerModel, baseModel);
                        useGroupByModel = true;
                        continue;
                    }
                }

                // this is not a direct call to aggregation function, in which case
                // we emit aggregation function into group-by model and leave the
                // rest in outer model
                int beforeSplit = groupByModel.getColumns().size();
                emitAggregates(qc.getAst(), groupByModel);
                if (beforeSplit < groupByModel.getColumns().size()) {
                    outerModel.addColumn(qc);

                    // pull literals from newly created group-by columns into both of underlying models
                    for (int j = beforeSplit, n = groupByModel.getColumns().size(); j < n; j++) {
                        emitLiterals(groupByModel.getColumns().getQuick(i).getAst(), translatingModel, innerModel, baseModel);
                    }

                    useGroupByModel = true;
                    useOuterModel = true;
                } else {
                    // there were no aggregation functions emitted therefore
                    // this is just a function that goes into virtual model
                    innerModel.addColumn(qc);
                    useInnerModel = true;

                    // we also create column that references this inner layer from outer layer,
                    // for example when we have:
                    // select a, b+c ...
                    // it should translate to:
                    // select a, x from (select a, b+c x from (select a,b,c ...))
                    final QueryColumn innerColumn = nextColumn(qc.getAlias());

                    // pull literals only into translating model
                    emitLiterals(qc.getAst(), translatingModel, null, baseModel);
                    groupByModel.addColumn(innerColumn);
                    analyticModel.addColumn(innerColumn);
                    outerModel.addColumn(innerColumn);
                }
            }
        }

        // fail if we have both analytic and group-by models
        if (useAnalyticModel && useGroupByModel) {
            throw ParserException.$(0, "Analytic function is not allowed in context of aggregation. Use sub-query.");
        }

        // check if translating model is redundant, e.g.
        // that it neither chooses between tables nor renames columns
        boolean translationIsRedundant = useInnerModel || useGroupByModel || useAnalyticModel;
        if (translationIsRedundant) {
            for (int i = 0, n = translatingModel.getColumns().size(); i < n; i++) {
                QueryColumn column = translatingModel.getColumns().getQuick(i);
                if (!column.getAst().token.equals(column.getAlias())) {
                    translationIsRedundant = false;
                }
            }
        }

        QueryModel root;

        if (translationIsRedundant) {
            root = baseModel;
        } else {
            root = translatingModel;
            translatingModel.setNestedModel(baseModel);
        }

        if (useInnerModel) {
            innerModel.setNestedModel(root);
            root = innerModel;
        }

        if (useAnalyticModel) {
            analyticModel.setNestedModel(root);
            root = analyticModel;
        } else if (useGroupByModel) {
            groupByModel.setNestedModel(root);
            root = groupByModel;
            if (useOuterModel) {
                outerModel.setNestedModel(root);
                root = outerModel;
            }
        }

        if (!useGroupByModel && groupByModel.getSampleBy() != null) {
            throw ParserException.$(groupByModel.getSampleBy().position, "at least one aggregation function must be present in 'select' clause");
        }

        return root;
    }

    private CharSequence setAndGetModelAlias(QueryModel model) {
        CharSequence name = model.getName();
        if (name != null) {
            return name;
        }
        ExprNode alias = makeJoinAlias(defaultAliasCount++);
        model.setAlias(alias);
        return alias.token;
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

    private int toColumnType(CharSequence tok) throws ParserException {
        final int type = ColumnType.columnTypeOf(tok);
        if (type == -1) {
            throw ParserException.$(lexer.position(), "unsupported column type: ").put(tok);
        }
        return type;
    }

    private CharSequence tok(String expectedList) throws ParserException {
        CharSequence tok = optTok();
        if (tok == null) {
            throw ParserException.position(lexer.position()).put(expectedList).put(" expected");
        }
        return tok;
    }

    private void traverseNamesAndIndices(QueryModel parent, ExprNode node) throws ParserException {
        literalCollectorAIndexes.clear();
        literalCollectorBIndexes.clear();

        literalCollectorANames.clear();
        literalCollectorBNames.clear();

        literalCollector.withModel(parent);
        literalCollector.resetNullCount();
        traversalAlgo.traverse(node.lhs, literalCollector.lhs());
        traversalAlgo.traverse(node.rhs, literalCollector.rhs());
    }


    private void validateLiteral(int pos, CharSequence tok) throws ParserException {
        switch (tok.charAt(0)) {
            case '(':
            case ')':
            case ',':
            case '`':
            case '"':
            case '\'':
                throw ParserException.$(pos, "literal expected");
            default:
                break;

        }
    }

    private static class LiteralCheckingVisitor implements PostOrderTreeTraversalAlgo.Visitor {
        private CharSequenceIntHashMap nameTypeMap;

        @Override
        public void visit(ExprNode node) throws ParserException {
            if (node.type == ExprNode.LITERAL) {
                final int dot = Chars.indexOf(node.token, '.');
                int index = dot == -1 ? nameTypeMap.keyIndex(node.token) : nameTypeMap.keyIndex(node.token, dot + 1, node.token.length());
                if (index < 0) {
                    if (nameTypeMap.valueAt(index) != ExprNode.LITERAL) {
                        throw NonLiteralException.INSTANCE;
                    }
                } else {
                    throw ParserException.invalidColumn(node.position, node.token);
                }
            }
        }

        PostOrderTreeTraversalAlgo.Visitor of(CharSequenceIntHashMap nameTypeMap) {
            this.nameTypeMap = nameTypeMap;
            return this;
        }


    }

    private static class LiteralRewritingVisitor implements PostOrderTreeTraversalAlgo.Visitor {
        private CharSequenceObjHashMap<CharSequence> nameTypeMap;

        public PostOrderTreeTraversalAlgo.Visitor of(CharSequenceObjHashMap<CharSequence> aliasToColumnMap) {
            this.nameTypeMap = aliasToColumnMap;
            return this;
        }

        @Override
        public void visit(ExprNode node) {
            if (node.type == ExprNode.LITERAL) {
                int dot = Chars.indexOf(node.token, '.');
                int index = dot == -1 ? nameTypeMap.keyIndex(node.token) : nameTypeMap.keyIndex(node.token, dot + 1, node.token.length());
                // we have table column hit when alias is not found
                // in this case expression rewrite is unnecessary
                if (index < 0) {
                    CharSequence column = nameTypeMap.valueAt(index);
                    assert column != null;
                    // it is also unnecessary to rewrite literal if target value is the same
                    if (!Chars.equals(node.token, column)) {
                        node.token = column;
                    }
                }
            }
        }


    }

    private static class NonLiteralException extends RuntimeException {
        private static final NonLiteralException INSTANCE = new NonLiteralException();
    }

    private class LiteralCollector implements PostOrderTreeTraversalAlgo.Visitor {
        private IntList indexes;
        private ObjList<CharSequence> names;
        private int nullCount;
        private QueryModel model;

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

        @Override
        public void visit(ExprNode node) throws ParserException {
            switch (node.type) {
                case ExprNode.LITERAL:
                    int dot = Chars.indexOf(node.token, '.');
                    CharSequence name = extractColumnName(node.token, dot);
                    indexes.add(getIndexOfTableForColumn(model, node.token, dot, node.position));
                    if (names != null) {
                        names.add(name);
                    }
                    break;
                case ExprNode.CONSTANT:
                    if (nullConstants.contains(node.token)) {
                        nullCount++;
                    }
                    break;
                default:
                    break;
            }
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
    }

    static {
        joinOps.put("=", JOIN_OP_EQUAL);
        joinOps.put("and", JOIN_OP_AND);
        joinOps.put("or", JOIN_OP_OR);
        joinOps.put("~", JOIN_OP_REGEX);
    }

    static {
        for (int i = 0, n = ExprOperator.operators.size(); i < n; i++) {
            disallowedAliases.add(ExprOperator.operators.getQuick(i).token);
        }
    }

    static {
        joinBarriers = new IntHashSet();
        joinBarriers.add(QueryModel.JOIN_OUTER);
        joinBarriers.add(QueryModel.JOIN_ASOF);

        nullConstants.add("null");
        nullConstants.add("NaN");

        tableAliasStop.add("where");
        tableAliasStop.add("latest");
        tableAliasStop.add("join");
        tableAliasStop.add("inner");
        tableAliasStop.add("outer");
        tableAliasStop.add("asof");
        tableAliasStop.add("cross");
        tableAliasStop.add("sample");
        tableAliasStop.add("order");
        tableAliasStop.add("on");
        tableAliasStop.add("timestamp");
        tableAliasStop.add("limit");
        tableAliasStop.add(")");
        //
        columnAliasStop.add("from");
        columnAliasStop.add(",");
        columnAliasStop.add("over");
        //
        groupByStopSet.add("order");
        groupByStopSet.add(")");
        groupByStopSet.add(",");

        joinStartSet.put("join", QueryModel.JOIN_INNER);
        joinStartSet.put("inner", QueryModel.JOIN_INNER);
        joinStartSet.put("outer", QueryModel.JOIN_OUTER);
        joinStartSet.put("cross", QueryModel.JOIN_CROSS);
        joinStartSet.put("asof", QueryModel.JOIN_ASOF);
    }
}
