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

package io.questdb.griffin.model;

import io.questdb.cairo.sql.Function;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

import java.util.ArrayDeque;

import static io.questdb.griffin.SqlKeywords.isAndKeyword;

public class QueryModel implements Mutable, ExecutionModel, AliasTranslator, Sinkable {
    public static final QueryModelFactory FACTORY = new QueryModelFactory();
    public static final int ORDER_DIRECTION_ASCENDING = 0;
    public static final int ORDER_DIRECTION_DESCENDING = 1;
    public static final String NO_ROWID_MARKER = "*!*";
    public static final int JOIN_INNER = 1;
    public static final int JOIN_OUTER = 2;
    public static final int JOIN_CROSS = 3;
    public static final int JOIN_ASOF = 4;
    public static final int JOIN_SPLICE = 5;
    public static final int JOIN_LT = 6;
    public static final String SUB_QUERY_ALIAS_PREFIX = "_xQdbA";
    public static final int SELECT_MODEL_NONE = 0;
    public static final int SELECT_MODEL_CHOOSE = 1;
    public static final int SELECT_MODEL_VIRTUAL = 2;
    public static final int SELECT_MODEL_ANALYTIC = 3;
    public static final int SELECT_MODEL_GROUP_BY = 4;
    public static final int SELECT_MODEL_DISTINCT = 5;
    public static final int SELECT_MODEL_CURSOR = 6;
    public static final int SET_OPERATION_UNION_ALL = 0;
    public static final int SET_OPERATION_UNION = 1;
    public static final int SET_OPERATION_EXCEPT = 2;
    public static final int SET_OPERATION_INTERSECT = 3;
    private static final ObjList<String> modelTypeName = new ObjList<>();
    private final ObjList<QueryColumn> bottomUpColumns = new ObjList<>();
    private final LowerCaseCharSequenceHashSet topDownNameSet = new LowerCaseCharSequenceHashSet();
    private final ObjList<QueryColumn> topDownColumns = new ObjList<>();
    private final LowerCaseCharSequenceObjHashMap<CharSequence> aliasToColumnNameMap = new LowerCaseCharSequenceObjHashMap<>();
    private final LowerCaseCharSequenceObjHashMap<CharSequence> columnNameToAliasMap = new LowerCaseCharSequenceObjHashMap<>();
    private final LowerCaseCharSequenceObjHashMap<QueryColumn> aliasToColumnMap = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjList<CharSequence> bottomUpColumnNames = new ObjList<>();
    private final ObjList<QueryModel> joinModels = new ObjList<>();
    private final ObjList<ExpressionNode> orderBy = new ObjList<>();
    private final ObjList<ExpressionNode> groupBy = new ObjList<>();
    private final IntList orderByDirection = new IntList();
    private final IntHashSet dependencies = new IntHashSet();
    private final IntList orderedJoinModels1 = new IntList();
    private final IntList orderedJoinModels2 = new IntList();
    private final LowerCaseCharSequenceIntHashMap aliasIndexes = new LowerCaseCharSequenceIntHashMap();
    private final ObjList<ExpressionNode> expressionModels = new ObjList<>();
    // collect frequency of column names from each join model
    // and check if any of columns with frequency > 0 are selected
    // column name frequency of 1 corresponds to map value 0
    // column name frequency of 0 corresponds to map value -1
    // list of "and" concatenated expressions
    private final ObjList<ExpressionNode> parsedWhere = new ObjList<>();
    private final IntHashSet parsedWhereConsts = new IntHashSet();
    private final ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
    private final LowerCaseCharSequenceIntHashMap orderHash = new LowerCaseCharSequenceIntHashMap(4, 0.5, -1);
    private final ObjList<ExpressionNode> joinColumns = new ObjList<>(4);
    private final LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjList<ExpressionNode> sampleByFill = new ObjList<>();
    private ExpressionNode sampleByTimezoneName = null;
    private ExpressionNode sampleByOffset = null;
    private final ObjList<ExpressionNode> latestBy = new ObjList<>();
    private final ObjList<ExpressionNode> orderByAdvice = new ObjList<>();
    private final IntList orderByDirectionAdvice = new IntList();
    private ExpressionNode whereClause;
    private ExpressionNode postJoinWhereClause;
    private ExpressionNode constWhereClause;
    private QueryModel nestedModel;
    private ExpressionNode tableName;
    private long tableVersion;
    private Function tableNameFunction;
    private ExpressionNode alias;
    private ExpressionNode timestamp;
    private ExpressionNode sampleBy;
    private JoinContext context;
    private ExpressionNode joinCriteria;
    private int joinType;
    private int joinKeywordPosition;
    private IntList orderedJoinModels = orderedJoinModels2;
    private ExpressionNode limitLo;
    private ExpressionNode limitHi;
    private int selectModelType = SELECT_MODEL_NONE;
    private boolean nestedModelIsSubQuery = false;
    private boolean distinct = false;
    private QueryModel unionModel;
    private int setOperationType;
    private int modelPosition = 0;
    private int orderByAdviceMnemonic;
    private int tableId;

    private QueryModel() {
        joinModels.add(this);
    }

    public boolean addAliasIndex(ExpressionNode node, int index) {
        return aliasIndexes.put(node.token, index);
    }

    public void addBottomUpColumn(QueryColumn column) {
        bottomUpColumns.add(column);
        addField(column);
    }

    public void addDependency(int index) {
        dependencies.add(index);
    }

    public void addExpressionModel(ExpressionNode node) {
        assert node.queryModel != null;
        expressionModels.add(node);
    }

    public void addField(QueryColumn column) {
        final CharSequence alias = column.getAlias();
        final ExpressionNode ast = column.getAst();
        assert alias != null;
        aliasToColumnNameMap.put(alias, ast.token);
        columnNameToAliasMap.put(ast.token, alias);
        bottomUpColumnNames.add(alias);
        aliasToColumnMap.put(alias, column);
    }

    public void addGroupBy(ExpressionNode node) {
        groupBy.add(node);
    }

    public void addJoinColumn(ExpressionNode node) {
        joinColumns.add(node);
    }

    public void addJoinModel(QueryModel model) {
        joinModels.add(model);
    }

    public void addLatestBy(ExpressionNode latestBy) {
        this.latestBy.add(latestBy);
    }

    public void addOrderBy(ExpressionNode node, int direction) {
        orderBy.add(node);
        orderByDirection.add(direction);
    }

    public void addParsedWhereNode(ExpressionNode node, boolean innerPredicate) {
        node.innerPredicate = innerPredicate;
        parsedWhere.add(node);
    }

    public void addSampleByFill(ExpressionNode sampleByFill) {
        this.sampleByFill.add(sampleByFill);
    }

    public void addTopDownColumn(QueryColumn column, CharSequence alias) {
        if (topDownNameSet.add(alias)) {
            topDownColumns.add(column);
        }
    }

    public void addWithClause(CharSequence name, WithClauseModel model) {
        withClauses.put(name, model);
    }

    public void addWithClauses(LowerCaseCharSequenceObjHashMap<WithClauseModel> parentWithClauses) {
        withClauses.putAll(parentWithClauses);
    }

    public void clear() {
        bottomUpColumns.clear();
        aliasToColumnNameMap.clear();
        joinModels.clear();
        joinModels.add(this);
        clearSampleBy();
        orderBy.clear();
        orderByDirection.clear();
        groupBy.clear();
        dependencies.clear();
        parsedWhere.clear();
        whereClause = null;
        constWhereClause = null;
        nestedModel = null;
        tableName = null;
        alias = null;
        latestBy.clear();
        joinCriteria = null;
        joinType = JOIN_INNER;
        joinKeywordPosition = 0;
        orderedJoinModels1.clear();
        orderedJoinModels2.clear();
        parsedWhereConsts.clear();
        aliasIndexes.clear();
        postJoinWhereClause = null;
        context = null;
        orderedJoinModels = orderedJoinModels2;
        limitHi = null;
        limitLo = null;
        timestamp = null;
        sqlNodeStack.clear();
        joinColumns.clear();
        withClauses.clear();
        selectModelType = SELECT_MODEL_NONE;
        columnNameToAliasMap.clear();
        tableNameFunction = null;
        tableId = -1;
        tableVersion = -1;
        bottomUpColumnNames.clear();
        expressionModels.clear();
        distinct = false;
        nestedModelIsSubQuery = false;
        unionModel = null;
        orderHash.clear();
        modelPosition = 0;
        topDownColumns.clear();
        topDownNameSet.clear();
        aliasToColumnMap.clear();
    }

    public void clearColumnMapStructs() {
        this.aliasToColumnNameMap.clear();
        this.bottomUpColumnNames.clear();
        this.aliasToColumnMap.clear();
    }

    public void clearOrderBy() {
        orderBy.clear();
        orderByDirection.clear();
    }

    public void clearSampleBy() {
        sampleBy = null;
        sampleByFill.clear();
        sampleByTimezoneName = null;
        sampleByOffset = null;
    }

    public void copyColumnsFrom(
            QueryModel other,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<ExpressionNode> expressionNodePool
    ) {
        clearColumnMapStructs();


        // copy only literal columns and convert functions to literal while copying
        final ObjList<CharSequence> aliases = other.aliasToColumnMap.keys();
        for (int i = 0, n = aliases.size(); i < n; i++) {
            final CharSequence alias = aliases.getQuick(i);
            QueryColumn qc = other.aliasToColumnMap.get(alias);
            if (qc.getAst().type == ExpressionNode.LITERAL) {
                this.aliasToColumnMap.put(alias, qc);
            } else {
                this.aliasToColumnMap.put(alias, queryColumnPool.next().of(alias, expressionNodePool.next().of(ExpressionNode.LITERAL, alias, 0, qc.getAst().position)));
            }
        }
        ObjList<CharSequence> columnNames = other.bottomUpColumnNames;
        this.bottomUpColumnNames.addAll(columnNames);
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            final CharSequence name = columnNames.getQuick(i);
            this.aliasToColumnNameMap.put(name, name);
        }
    }

    public void copyOrderByAdvice(ObjList<ExpressionNode> orderByAdvice) {
        this.orderByAdvice.clear();
        this.orderByAdvice.addAll(orderByAdvice);
    }

    public void copyOrderByDirectionAdvice(IntList orderByDirection) {
        this.orderByDirectionAdvice.clear();
        this.orderByDirectionAdvice.addAll(orderByDirection);
    }

    public QueryColumn findBottomUpColumnByAst(ExpressionNode node) {
        for (int i = 0, n = bottomUpColumns.size(); i < n; i++) {
            QueryColumn qc = bottomUpColumns.getQuick(i);
            if (ExpressionNode.compareNodesExact(node, qc.getAst())) {
                return qc;
            }
        }
        return null;
    }

    public ExpressionNode getAlias() {
        return alias;
    }

    public int getTableId() {
        return tableId;
    }

    public void moveAliasFrom(QueryModel that) {
        final ExpressionNode alias = that.alias;
        if (alias != null && !Chars.startsWith(alias.token, SUB_QUERY_ALIAS_PREFIX)) {
            setAlias(alias);
            addAliasIndex(alias, 0);
        }
    }

    public void setAlias(ExpressionNode alias) {
        this.alias = alias;
    }

    public int getAliasIndex(CharSequence column, int start, int end) {
        int index = aliasIndexes.keyIndex(column, start, end);
        if (index < 0) {
            return aliasIndexes.valueAt(index);
        }
        return -1;
    }

    public LowerCaseCharSequenceIntHashMap getAliasIndexes() {
        return aliasIndexes;
    }

    public LowerCaseCharSequenceObjHashMap<QueryColumn> getAliasToColumnMap() {
        return aliasToColumnMap;
    }

    public LowerCaseCharSequenceObjHashMap<CharSequence> getAliasToColumnNameMap() {
        return aliasToColumnNameMap;
    }

    public ObjList<CharSequence> getBottomUpColumnNames() {
        return bottomUpColumnNames;
    }

    public ObjList<QueryColumn> getBottomUpColumns() {
        return bottomUpColumns;
    }

    public LowerCaseCharSequenceObjHashMap<CharSequence> getColumnNameToAliasMap() {
        return columnNameToAliasMap;
    }

    public ObjList<QueryColumn> getColumns() {
        return topDownColumns.size() > 0 ? topDownColumns : bottomUpColumns;
    }

    public ExpressionNode getConstWhereClause() {
        return constWhereClause;
    }

    public void setConstWhereClause(ExpressionNode constWhereClause) {
        this.constWhereClause = constWhereClause;
    }

    public JoinContext getContext() {
        return context;
    }

    public void setContext(JoinContext context) {
        this.context = context;
    }

    public IntHashSet getDependencies() {
        return dependencies;
    }

    public ObjList<ExpressionNode> getExpressionModels() {
        return expressionModels;
    }

    public ObjList<ExpressionNode> getGroupBy() {
        return groupBy;
    }

    public ObjList<ExpressionNode> getJoinColumns() {
        return joinColumns;
    }

    public ExpressionNode getJoinCriteria() {
        return joinCriteria;
    }

    public void setJoinCriteria(ExpressionNode joinCriteria) {
        this.joinCriteria = joinCriteria;
    }

    public int getJoinKeywordPosition() {
        return joinKeywordPosition;
    }

    public void setJoinKeywordPosition(int position) {
        this.joinKeywordPosition = position;
    }

    public ObjList<QueryModel> getJoinModels() {
        return joinModels;
    }

    public int getJoinType() {
        return joinType;
    }

    public void setJoinType(int joinType) {
        this.joinType = joinType;
    }

    public ObjList<ExpressionNode> getLatestBy() {
        return latestBy;
    }

    public ExpressionNode getLimitHi() {
        return limitHi;
    }

    public ExpressionNode getLimitLo() {
        return limitLo;
    }

    public int getModelPosition() {
        return modelPosition;
    }

    public void setModelPosition(int modelPosition) {
        this.modelPosition = modelPosition;
    }

    @Override
    public int getModelType() {
        return ExecutionModel.QUERY;
    }

    public CharSequence getName() {
        if (alias != null) {
            return alias.token;
        }

        if (tableName != null) {
            return tableName.token;
        }

        return null;
    }

    public QueryModel getNestedModel() {
        return nestedModel;
    }

    public void setNestedModel(QueryModel nestedModel) {
        this.nestedModel = nestedModel;
    }

    public ObjList<ExpressionNode> getOrderBy() {
        return orderBy;
    }

    public ObjList<ExpressionNode> getOrderByAdvice() {
        return orderByAdvice;
    }

    public int getOrderByAdviceMnemonic() {
        return orderByAdviceMnemonic;
    }

    public void setOrderByAdviceMnemonic(int orderByAdviceMnemonic) {
        this.orderByAdviceMnemonic = orderByAdviceMnemonic;
    }

    public IntList getOrderByDirection() {
        return orderByDirection;
    }

    public IntList getOrderByDirectionAdvice() {
        return orderByDirectionAdvice;
    }

    public LowerCaseCharSequenceIntHashMap getOrderHash() {
        return orderHash;
    }

    public IntList getOrderedJoinModels() {
        return orderedJoinModels;
    }

    public void setOrderedJoinModels(IntList that) {
        assert that == orderedJoinModels1 || that == orderedJoinModels2;
        this.orderedJoinModels = that;
    }

    public ObjList<ExpressionNode> getParsedWhere() {
        return parsedWhere;
    }

    public ExpressionNode getPostJoinWhereClause() {
        return postJoinWhereClause;
    }

    public void setPostJoinWhereClause(ExpressionNode postJoinWhereClause) {
        this.postJoinWhereClause = postJoinWhereClause;
    }

    public ExpressionNode getSampleBy() {
        return sampleBy;
    }

    public void setSampleBy(ExpressionNode sampleBy) {
        this.sampleBy = sampleBy;
    }

    public ExpressionNode getSampleByTimezoneName() {
        return sampleByTimezoneName;
    }

    public void setSampleByTimezoneName(ExpressionNode sampleByTimezoneName) {
        this.sampleByTimezoneName = sampleByTimezoneName;
    }

    public ExpressionNode getSampleByOffset() {
        return sampleByOffset;
    }

    public void setSampleByOffset(ExpressionNode sampleByOffset) {
        this.sampleByOffset = sampleByOffset;
    }

    public ObjList<ExpressionNode> getSampleByFill() {
        return sampleByFill;
    }

    public int getSelectModelType() {
        return selectModelType;
    }

    public void setSelectModelType(int selectModelType) {
        this.selectModelType = selectModelType;
    }

    public int getSetOperationType() {
        return setOperationType;
    }

    public void setSetOperationType(int setOperationType) {
        this.setOperationType = setOperationType;
    }

    public ExpressionNode getTableName() {
        return tableName;
    }

    public void setTableId(int id) {
        this.tableId = id;
    }

    public void setTableName(ExpressionNode tableName) {
        this.tableName = tableName;
    }

    public Function getTableNameFunction() {
        return tableNameFunction;
    }

    public void setTableNameFunction(Function function) {
        this.tableNameFunction = function;
    }

    public long getTableVersion() {
        return tableVersion;
    }

    public void setTableVersion(long tableVersion) {
        this.tableVersion = tableVersion;
    }

    public ExpressionNode getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(ExpressionNode timestamp) {
        this.timestamp = timestamp;
    }

    public ObjList<QueryColumn> getTopDownColumns() {
        return topDownColumns;
    }

    public QueryModel getUnionModel() {
        return unionModel;
    }

    public void setUnionModel(QueryModel unionModel) {
        this.unionModel = unionModel;
    }

    public ExpressionNode getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(ExpressionNode whereClause) {
        this.whereClause = whereClause;
    }

    public WithClauseModel getWithClause(CharSequence name) {
        return withClauses.get(name);
    }

    public LowerCaseCharSequenceObjHashMap<WithClauseModel> getWithClauses() {
        return withClauses;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public boolean isNestedModelIsSubQuery() {
        return nestedModelIsSubQuery;
    }

    public void setNestedModelIsSubQuery(boolean nestedModelIsSubQuery) {
        this.nestedModelIsSubQuery = nestedModelIsSubQuery;
    }

    public boolean isTopDownNameMissing(CharSequence columnName) {
        return topDownNameSet.excludes(columnName);
    }

    public void moveGroupByFrom(QueryModel model) {
        this.groupBy.addAll(model.groupBy);
        // clear the source
        model.groupBy.clear();
    }

    public void moveLimitFrom(QueryModel baseModel) {
        this.limitLo = baseModel.getLimitLo();
        this.limitHi = baseModel.getLimitHi();
        baseModel.setLimit(null, null);
    }

    public void moveSampleByFrom(QueryModel model) {
        this.sampleBy = model.sampleBy;
        this.sampleByFill.clear();
        this.sampleByFill.addAll(model.sampleByFill);
        this.sampleByTimezoneName = model.sampleByTimezoneName;
        this.sampleByOffset = model.sampleByOffset;

        // clear the source
        model.clearSampleBy();
    }

    /**
     * Optimiser may be attempting to order join clauses several times.
     * Every time ordering takes place optimiser will keep at most two lists:
     * one is last known order the other is new order. If new order cost is better
     * optimiser will replace last known order with new one.
     * <p>
     * To facilitate this behaviour the function will always return non-current list.
     *
     * @return non current order list.
     */
    public IntList nextOrderedJoinModels() {
        IntList ordered = orderedJoinModels == orderedJoinModels1 ? orderedJoinModels2 : orderedJoinModels1;
        ordered.clear();
        return ordered;
    }

    /*
     * Splits "where" clauses into "and" chunks
     */
    public ObjList<ExpressionNode> parseWhereClause() {
        ExpressionNode n = getWhereClause();
        // pre-order traversal
        sqlNodeStack.clear();
        while (!sqlNodeStack.isEmpty() || n != null) {
            if (n != null && n.token != null) {
                if (isAndKeyword(n.token)) {
                    if (n.rhs != null) {
                        sqlNodeStack.push(n.rhs);
                    }
                    n = n.lhs;
                } else {
                    addParsedWhereNode(n, false);
                    n = null;

                }
            } else {
                n = sqlNodeStack.poll();
            }
        }
        return getParsedWhere();
    }

    public void removeDependency(int index) {
        dependencies.remove(index);
    }

    public void replaceJoinModel(int pos, QueryModel model) {
        joinModels.setQuick(pos, model);
    }

    public void setLimit(ExpressionNode lo, ExpressionNode hi) {
        this.limitLo = lo;
        this.limitHi = hi;
    }

    @Override
    public void toSink(CharSink sink) {
        toSink0(sink, false);
    }

    @Override
    public CharSequence translateAlias(CharSequence column) {
        return aliasToColumnNameMap.get(column);
    }

    private static void aliasToSink(CharSequence alias, CharSink sink) {
        sink.put(' ');
        boolean quote = Chars.indexOf(alias, ' ') != -1;
        if (quote) {
            sink.put('\'').put(alias).put('\'');
        } else {
            sink.put(alias);
        }
    }

    private String getSelectModelTypeText() {
        return modelTypeName.get(selectModelType);
    }

    private void sinkColumns(CharSink sink, ObjList<QueryColumn> columns) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            QueryColumn column = columns.getQuick(i);
            CharSequence name = column.getName();
            CharSequence alias = column.getAlias();
            ExpressionNode ast = column.getAst();
            if (column instanceof AnalyticColumn || name == null) {
                ast.toSink(sink);

                if (alias != null) {
                    aliasToSink(alias, sink);
                }

                // this can only be analytic column
                if (name != null) {
                    AnalyticColumn ac = (AnalyticColumn) column;
                    sink.put(" over (");
                    final ObjList<ExpressionNode> partitionBy = ac.getPartitionBy();
                    if (partitionBy.size() > 0) {
                        sink.put("partition by ");
                        for (int k = 0, z = partitionBy.size(); k < z; k++) {
                            if (k > 0) {
                                sink.put(", ");
                            }
                            partitionBy.getQuick(k).toSink(sink);
                        }
                    }

                    final ObjList<ExpressionNode> orderBy = ac.getOrderBy();
                    if (orderBy.size() > 0) {
                        if (partitionBy.size() > 0) {
                            sink.put(' ');
                        }
                        sink.put("order by ");
                        for (int k = 0, z = orderBy.size(); k < z; k++) {
                            if (k > 0) {
                                sink.put(", ");
                            }
                            orderBy.getQuick(k).toSink(sink);
                            if (ac.getOrderByDirection().getQuick(k) == 1) {
                                sink.put(" desc");
                            }
                        }
                    }
                    sink.put(')');
                }
            } else {
                ast.toSink(sink);
                // do not repeat alias when it is the same as AST token, provided AST is a literal
                if (alias != null && (ast.type != ExpressionNode.LITERAL || !ast.token.equals(alias))) {
                    aliasToSink(alias, sink);
                }
            }
        }
    }

    private void toSink0(CharSink sink, boolean joinSlave) {
        final boolean hasColumns = this.topDownColumns.size() > 0 || this.bottomUpColumns.size() > 0;
        if (hasColumns) {
            sink.put(getSelectModelTypeText());
            if (this.topDownColumns.size() > 0) {
                sink.put(' ');
                sink.put('[');
                sinkColumns(sink, this.topDownColumns);
                sink.put(']');
            }
            if (this.bottomUpColumns.size() > 0) {
                sink.put(' ');
                sinkColumns(sink, this.bottomUpColumns);
            }
            sink.put(" from ");
        }
        if (tableName != null) {
            tableName.toSink(sink);
        } else {
            sink.put('(');
            nestedModel.toSink(sink);
            sink.put(')');
        }
        if (alias != null) {
            aliasToSink(alias.token, sink);
        }

        if (timestamp != null) {
            sink.put(" timestamp (");
            timestamp.toSink(sink);
            sink.put(')');
        }

        if (getLatestBy().size() > 0) {
            sink.put(" latest by ");
            for (int i = 0, n = getLatestBy().size(); i < n; i++) {
                if (i > 0) {
                    sink.put(',');
                }
                getLatestBy().getQuick(i).toSink(sink);
            }
        }

        if (orderedJoinModels.size() > 1) {
            for (int i = 0, n = orderedJoinModels.size(); i < n; i++) {
                QueryModel model = joinModels.getQuick(orderedJoinModels.getQuick(i));
                if (model != this) {
                    switch (model.getJoinType()) {
                        case JOIN_OUTER:
                            sink.put(" outer join ");
                            break;
                        case JOIN_ASOF:
                            sink.put(" asof join ");
                            break;
                        case JOIN_SPLICE:
                            sink.put(" splice join ");
                            break;
                        case JOIN_CROSS:
                            sink.put(" cross join ");
                            break;
                        case JOIN_LT:
                            sink.put(" lt join ");
                            break;
                        default:
                            sink.put(" join ");
                            break;
                    }

                    if (model.getWhereClause() != null) {
                        sink.put('(');
                        model.toSink0(sink, true);
                        sink.put(')');
                        if (model.getAlias() != null) {
                            aliasToSink(model.getAlias().token, sink);
                        } else if (model.getTableName() != null) {
                            aliasToSink(model.getTableName().token, sink);
                        }
                    } else {
                        model.toSink0(sink, true);
                    }

                    JoinContext jc = model.getContext();
                    if (jc != null && jc.aIndexes.size() > 0) {
                        // join clause
                        sink.put(" on ");
                        for (int k = 0, z = jc.aIndexes.size(); k < z; k++) {
                            if (k > 0) {
                                sink.put(" and ");
                            }
                            jc.aNodes.getQuick(k).toSink(sink);
                            sink.put(" = ");
                            jc.bNodes.getQuick(k).toSink(sink);
                        }
                    }

                    if (model.getPostJoinWhereClause() != null) {
                        sink.put(" post-join-where ");
                        model.getPostJoinWhereClause().toSink(sink);
                    }
                }
            }
        }

        if (whereClause != null) {
            sink.put(" where ");
            whereClause.toSink(sink);
        }

        if (constWhereClause != null) {
            sink.put(" const-where ");
            constWhereClause.toSink(sink);
        }

        if (!joinSlave && postJoinWhereClause != null) {
            sink.put(" post-join-where ");
            postJoinWhereClause.toSink(sink);
        }

        if (sampleBy != null) {
            sink.put(" sample by ");
            sampleBy.toSink(sink);

            final int fillCount = sampleByFill.size();
            if (fillCount > 0) {
                sink.put(" fill(");
                sink.put(sampleByFill.getQuick(0));

                if (fillCount > 1) {
                    for (int i = 1; i < fillCount; i++) {
                        sink.put(',');
                        sink.put(sampleByFill.getQuick(i));
                    }
                }
                sink.put(')');
            }

            if (sampleByTimezoneName != null || sampleByOffset != null) {
                sink.put(" align to calendar");
                if (sampleByTimezoneName != null) {
                    sink.put(" time zone ");
                    sink.put(sampleByTimezoneName);
                }

                if(sampleByOffset != null) {
                    sink.put(" with offset ");
                    sink.put(sampleByOffset);
                }
            }
        }

        if (orderHash.size() > 0 && orderBy.size() > 0) {
            sink.put(" order by ");

            ObjList<CharSequence> columnNames = orderHash.keys();
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                if (i > 0) {
                    sink.put(", ");
                }

                CharSequence key = columnNames.getQuick(i);
                sink.put(key);
                if (orderHash.get(key) == 1) {
                    sink.put(" desc");
                }
            }
        }

        if (getLimitLo() != null || getLimitHi() != null) {
            sink.put(" limit ");
            if (getLimitLo() != null) {
                getLimitLo().toSink(sink);
            }
            if (getLimitHi() != null) {
                sink.put(',');
                getLimitHi().toSink(sink);
            }
        }

        if (unionModel != null) {
            if (setOperationType == QueryModel.SET_OPERATION_INTERSECT) {
                sink.put(" intersect ");
            } else if (setOperationType == QueryModel.SET_OPERATION_EXCEPT) {
                sink.put(" except ");
            } else {
                sink.put(" union ");
                if (setOperationType == QueryModel.SET_OPERATION_UNION_ALL) {
                    sink.put("all ");
                }
            }
            unionModel.toSink0(sink, false);
        }
    }

    public static final class QueryModelFactory implements ObjectFactory<QueryModel> {
        @Override
        public QueryModel newInstance() {
            return new QueryModel();
        }
    }

    static {
        modelTypeName.extendAndSet(SELECT_MODEL_NONE, "select");
        modelTypeName.extendAndSet(SELECT_MODEL_CHOOSE, "select-choose");
        modelTypeName.extendAndSet(SELECT_MODEL_VIRTUAL, "select-virtual");
        modelTypeName.extendAndSet(SELECT_MODEL_ANALYTIC, "select-analytic");
        modelTypeName.extendAndSet(SELECT_MODEL_GROUP_BY, "select-group-by");
        modelTypeName.extendAndSet(SELECT_MODEL_DISTINCT, "select-distinct");
        modelTypeName.extendAndSet(SELECT_MODEL_CURSOR, "select-cursor");
    }
}