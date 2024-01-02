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

package io.questdb.griffin.model;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.OrderByMnemonic;
import io.questdb.griffin.SqlException;
import io.questdb.std.*;
import io.questdb.std.str.CharSinkBase;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Objects;

import static io.questdb.griffin.SqlKeywords.isAndKeyword;

/**
 * Important note: Make sure to update clear, equals and hashCode methods, as well as
 * the unit tests, when you're adding a new field to this class. Instances of QueryModel
 * are reused across query compilation, so making sure that we reset all fields correctly
 * is important.
 */
public class QueryModel implements Mutable, ExecutionModel, AliasTranslator, Sinkable {
    public static final QueryModelFactory FACTORY = new QueryModelFactory();
    public static final int JOIN_ASOF = 4;
    public static final int JOIN_CROSS = 3;
    public static final int JOIN_CROSS_LEFT = 8;
    public static final int JOIN_INNER = 1;
    public static final int JOIN_LT = 6;
    public static final int JOIN_MAX = JOIN_CROSS_LEFT;
    public static final int JOIN_ONE = 7;
    public static final int JOIN_OUTER = 2;
    public static final int JOIN_SPLICE = 5;
    public static final int LATEST_BY_DEPRECATED = 1;
    public static final int LATEST_BY_NEW = 2;
    public static final int LATEST_BY_NONE = 0;
    public static final String NO_ROWID_MARKER = "*!*";
    public static final int ORDER_DIRECTION_ASCENDING = 0;
    public static final int ORDER_DIRECTION_DESCENDING = 1;
    public static final int SELECT_MODEL_CHOOSE = 1;
    public static final int SELECT_MODEL_CURSOR = 6;
    public static final int SELECT_MODEL_DISTINCT = 5;
    public static final int SELECT_MODEL_GROUP_BY = 4;
    public static final int SELECT_MODEL_NONE = 0;
    public static final int SELECT_MODEL_SHOW = 7;
    public static final int SELECT_MODEL_VIRTUAL = 2;
    public static final int SELECT_MODEL_WINDOW = 3;
    public static final int SET_OPERATION_EXCEPT = 2;
    public static final int SET_OPERATION_EXCEPT_ALL = 3;
    public static final int SET_OPERATION_INTERSECT = 4;
    public static final int SET_OPERATION_INTERSECT_ALL = 5;
    public static final int SET_OPERATION_UNION = 1;
    // types of set operations between this and union model
    public static final int SET_OPERATION_UNION_ALL = 0;
    public static final int SHOW_COLUMNS = 2;
    public static final int SHOW_DATE_STYLE = 9;
    public static final int SHOW_MAX_IDENTIFIER_LENGTH = 6;
    public static final int SHOW_PARAMETERS = 11;
    public static final int SHOW_PARTITIONS = 3;
    public static final int SHOW_SEARCH_PATH = 8;
    public static final int SHOW_SERVER_VERSION = 12;
    public static final int SHOW_STANDARD_CONFORMING_STRINGS = 7;
    public static final int SHOW_TABLES = 1;
    public static final int SHOW_TIME_ZONE = 10;
    public static final int SHOW_TRANSACTION = 4;
    public static final int SHOW_TRANSACTION_ISOLATION_LEVEL = 5;
    public static final String SUB_QUERY_ALIAS_PREFIX = "_xQdbA";
    private static final ObjList<String> modelTypeName = new ObjList<>();
    private final LowerCaseCharSequenceObjHashMap<QueryColumn> aliasToColumnMap = new LowerCaseCharSequenceObjHashMap<>();
    private final LowerCaseCharSequenceObjHashMap<CharSequence> aliasToColumnNameMap = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjList<CharSequence> bottomUpColumnNames = new ObjList<>();
    private final ObjList<QueryColumn> bottomUpColumns = new ObjList<>();
    private final LowerCaseCharSequenceIntHashMap columnAliasIndexes = new LowerCaseCharSequenceIntHashMap();
    private final LowerCaseCharSequenceObjHashMap<CharSequence> columnNameToAliasMap = new LowerCaseCharSequenceObjHashMap<>();
    private final IntHashSet dependencies = new IntHashSet();
    private final ObjList<ExpressionNode> expressionModels = new ObjList<>();
    private final ObjList<ExpressionNode> groupBy = new ObjList<>();
    private final ObjList<ExpressionNode> joinColumns = new ObjList<>(4);
    private final ObjList<QueryModel> joinModels = new ObjList<>();
    private final ObjList<ExpressionNode> latestBy = new ObjList<>();
    private final LowerCaseCharSequenceIntHashMap modelAliasIndexes = new LowerCaseCharSequenceIntHashMap();
    private final ObjList<ExpressionNode> orderBy = new ObjList<>();
    private final ObjList<ExpressionNode> orderByAdvice = new ObjList<>();
    private final IntList orderByDirection = new IntList();
    private final IntList orderByDirectionAdvice = new IntList();
    private final LowerCaseCharSequenceIntHashMap orderHash = new LowerCaseCharSequenceIntHashMap(4, 0.5, -1);
    private final IntList orderedJoinModels1 = new IntList();
    private final IntList orderedJoinModels2 = new IntList();
    // collect frequency of column names from each join model
    // and check if any of columns with frequency > 0 are selected
    // column name frequency of 1 corresponds to map value 0
    // column name frequency of 0 corresponds to map value -1
    // list of "and" concatenated expressions
    private final ObjList<ExpressionNode> parsedWhere = new ObjList<>();
    private final IntHashSet parsedWhereConstants = new IntHashSet();
    private final ObjList<ExpressionNode> sampleByFill = new ObjList<>();
    private final ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
    private final ObjList<QueryColumn> topDownColumns = new ObjList<>();
    private final LowerCaseCharSequenceHashSet topDownNameSet = new LowerCaseCharSequenceHashSet();
    private final ObjList<ExpressionNode> updateSetColumns = new ObjList<>();
    private final ObjList<CharSequence> updateTableColumnNames = new ObjList<>();
    private final IntList updateTableColumnTypes = new IntList();
    private final LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauseModel = new LowerCaseCharSequenceObjHashMap<>();
    private ExpressionNode alias;
    private boolean artificialStar;
    // Used to store a deep copy of the whereClause field
    // since whereClause can be changed during optimization/generation stage.
    private ExpressionNode backupWhereClause;
    private ExpressionNode constWhereClause;
    private JoinContext context;
    private boolean distinct = false;
    private boolean explicitTimestamp;
    //simple flag to mark when limit x,y in current model (part of query) is already taken care of by existing factories e.g. LimitedSizeSortedLightRecordCursorFactory
    //and doesn't need to be enforced by LimitRecordCursor. We need it to detect whether current factory implements limit from this or inner query .
    private boolean isLimitImplemented;
    // A flag to mark intermediate SELECT translation models. Such models do not contain the full list of selected
    // columns (e.g. they lack virtual columns), so they should be skipped when rewriting positional ORDER BY.
    private boolean isSelectTranslation = false;
    private boolean isUpdateModel;
    private ExpressionNode joinCriteria;
    private int joinKeywordPosition;
    private int joinType = JOIN_INNER;
    private int latestByType = LATEST_BY_NONE;
    private ExpressionNode limitAdviceHi;
    private ExpressionNode limitAdviceLo;
    private ExpressionNode limitHi;
    private ExpressionNode limitLo;
    // position of the limit clause token
    private int limitPosition;
    private long metadataVersion = -1;
    private int modelPosition = 0;
    private int modelType = ExecutionModel.QUERY;
    private QueryModel nestedModel;
    private boolean nestedModelIsSubQuery = false;
    private int orderByAdviceMnemonic = OrderByMnemonic.ORDER_BY_UNKNOWN;
    // position of the order by clause token
    private int orderByPosition;
    private IntList orderedJoinModels = orderedJoinModels2;
    // Expression clause that is actually part of left/outer join but not in join model.
    // Inner join expressions
    private ExpressionNode outerJoinExpressionClause;
    private ExpressionNode postJoinWhereClause;
    private ExpressionNode sampleBy;
    private ExpressionNode sampleByOffset = null;
    private ExpressionNode sampleByTimezoneName = null;
    private ExpressionNode sampleByUnit;
    private int selectModelType = SELECT_MODEL_NONE;
    private int setOperationType;
    private int showKind = -1;
    private int tableId = -1;
    private ExpressionNode tableNameExpr;
    private RecordCursorFactory tableNameFunction;
    private ExpressionNode timestamp;
    private QueryModel unionModel;
    private QueryModel updateTableModel;
    private TableToken updateTableToken;
    private ExpressionNode whereClause;

    private QueryModel() {
        joinModels.add(this);
    }

    /**
     * Recursively clones the current value of whereClause for the model and its sub-models into the backupWhereClause field.
     */
    public static void backupWhereClause(final ObjectPool<ExpressionNode> pool, final QueryModel model) {
        QueryModel current = model;
        while (current != null) {
            if (current.unionModel != null) {
                backupWhereClause(pool, current.unionModel);
            }
            if (current.updateTableModel != null) {
                backupWhereClause(pool, current.updateTableModel);
            }
            for (int i = 1, n = current.joinModels.size(); i < n; i++) {
                final QueryModel m = current.joinModels.get(i);
                if (m != null && current != m) {
                    backupWhereClause(pool, m);
                }
            }
            current.backupWhereClause = ExpressionNode.deepClone(pool, current.whereClause);
            current = current.nestedModel;
        }
    }

    /**
     * Recursively restores the whereClause field from backupWhereClause for the model and its sub-models.
     */
    public static void restoreWhereClause(final ObjectPool<ExpressionNode> pool, final QueryModel model) {
        QueryModel current = model;
        while (current != null) {
            if (current.unionModel != null) {
                restoreWhereClause(pool, current.unionModel);
            }
            if (current.updateTableModel != null) {
                restoreWhereClause(pool, current.updateTableModel);
            }
            for (int i = 1, n = current.joinModels.size(); i < n; i++) {
                final QueryModel m = current.joinModels.get(i);
                if (m != null && current != m) {
                    restoreWhereClause(pool, m);
                }
            }
            current.whereClause = ExpressionNode.deepClone(pool, current.backupWhereClause);
            current = current.nestedModel;
        }
    }

    public void addBottomUpColumn(QueryColumn column) throws SqlException {
        addBottomUpColumn(0, column, false, null);
    }

    public void addBottomUpColumn(QueryColumn column, boolean allowDuplicates) throws SqlException {
        addBottomUpColumn(0, column, allowDuplicates, null);
    }

    public void addBottomUpColumn(int position, QueryColumn column, boolean allowDuplicates) throws SqlException {
        addBottomUpColumn(position, column, allowDuplicates, null);
    }

    public void addBottomUpColumn(int position, QueryColumn column, boolean allowDuplicates, CharSequence additionalMessage) throws SqlException {
        if (!allowDuplicates && aliasToColumnMap.contains(column.getName())) {
            throw SqlException.duplicateColumn(position, column.getName(), additionalMessage);
        }
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
        columnAliasIndexes.put(alias, bottomUpColumnNames.size() - 1);
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

    public boolean addModelAliasIndex(ExpressionNode node, int index) {
        return modelAliasIndexes.put(node.token, index);
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

    public void addUpdateTableColumnMetadata(int columnType, String columnName) {
        updateTableColumnTypes.add(columnType);
        updateTableColumnNames.add(columnName);
    }

    /**
     * Determines whether this model allows pushing columns from parent model(s).
     * If this is a UNION, EXCEPT or INTERSECT or contains a SELECT DISTINCT then it can't be done safely.
     */
    public boolean allowsColumnsChange() {
        QueryModel union = this;
        while (union != null) {
            if (union.getSetOperationType() != QueryModel.SET_OPERATION_UNION_ALL
                    || union.getSelectModelType() == QueryModel.SELECT_MODEL_DISTINCT) {
                return false;
            }
            union = union.getUnionModel();
        }
        return true;
    }

    /**
     * Determines whether this model allows pushing columns to nested models.
     * If this is a SELECT DISTINCT then we don't push since the parent model contains the necessary columns.
     */
    public boolean allowsNestedColumnsChange() {
        return this.getSelectModelType() != QueryModel.SELECT_MODEL_DISTINCT;
    }

    @Override
    public void clear() {
        bottomUpColumns.clear();
        aliasToColumnNameMap.clear();
        joinModels.clear();
        joinModels.add(this);
        clearSampleBy();
        orderBy.clear();
        orderByDirection.clear();
        orderByAdvice.clear();
        orderByDirectionAdvice.clear();
        orderByPosition = 0;
        orderByAdviceMnemonic = OrderByMnemonic.ORDER_BY_UNKNOWN;
        isSelectTranslation = false;
        groupBy.clear();
        dependencies.clear();
        parsedWhere.clear();
        whereClause = null;
        constWhereClause = null;
        nestedModel = null;
        tableNameExpr = null;
        alias = null;
        latestByType = LATEST_BY_NONE;
        latestBy.clear();
        joinCriteria = null;
        joinType = JOIN_INNER;
        joinKeywordPosition = 0;
        orderedJoinModels1.clear();
        orderedJoinModels2.clear();
        parsedWhereConstants.clear();
        columnAliasIndexes.clear();
        modelAliasIndexes.clear();
        postJoinWhereClause = null;
        outerJoinExpressionClause = null;
        context = null;
        orderedJoinModels = orderedJoinModels2;
        limitHi = null;
        limitLo = null;
        limitAdviceHi = null;
        limitAdviceLo = null;
        limitPosition = 0;
        isLimitImplemented = false;
        timestamp = null;
        sqlNodeStack.clear();
        joinColumns.clear();
        withClauseModel.clear();
        selectModelType = SELECT_MODEL_NONE;
        columnNameToAliasMap.clear();
        tableNameFunction = null;
        tableId = -1;
        metadataVersion = -1;
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
        isUpdateModel = false;
        modelType = ExecutionModel.QUERY;
        updateSetColumns.clear();
        updateTableColumnTypes.clear();
        updateTableColumnNames.clear();
        updateTableModel = null;
        updateTableToken = null;
        setOperationType = SET_OPERATION_UNION_ALL;
        artificialStar = false;
        explicitTimestamp = false;
        showKind = -1;
    }

    public void clearColumnMapStructs() {
        this.aliasToColumnNameMap.clear();
        this.bottomUpColumnNames.clear();
        this.aliasToColumnMap.clear();
        this.bottomUpColumns.clear();
    }

    public void clearOrderBy() {
        orderBy.clear();
        orderByDirection.clear();
    }

    public void clearSampleBy() {
        sampleBy = null;
        sampleByUnit = null;
        sampleByFill.clear();
        sampleByTimezoneName = null;
        sampleByOffset = null;
    }

    public boolean containsJoin() {
        QueryModel current = this;
        do {
            if (current.getJoinModels().size() > 1) {
                return true;
            }
        } while ((current = current.getNestedModel()) != null);
        return false;
    }

    public void copyBottomToTopColumns() {
        topDownColumns.clear();
        topDownNameSet.clear();
        for (int i = 0, n = bottomUpColumns.size(); i < n; i++) {
            QueryColumn column = bottomUpColumns.getQuick(i);
            addTopDownColumn(column, column.getAlias());
        }
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
            if (qc.getAst().type != ExpressionNode.LITERAL) {
                qc = queryColumnPool.next().of(
                        alias,
                        expressionNodePool.next().of(
                                ExpressionNode.LITERAL,
                                alias,
                                0,
                                qc.getAst().position
                        ),
                        qc.isIncludeIntoWildcard()
                );
            }
            this.aliasToColumnMap.put(alias, qc);
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

    public void copyUpdateTableMetadata(QueryModel updateTableModel) {
        this.updateTableModel = updateTableModel;
        this.tableId = updateTableModel.tableId;
        this.metadataVersion = updateTableModel.metadataVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryModel that = (QueryModel) o;
        // joinModels always contain this as the first element, so we need to compare them manually.
        if (joinModels.size() != that.joinModels.size()) {
            return false;
        }
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            if (!joinModels.getQuick(i).equals(that.joinModels.getQuick(i))) {
                return false;
            }
        }
        // ArrayDeque doesn't implement equals and hashCode, so we deal with sqlNodeStack separately.
        if (sqlNodeStack.size() != that.sqlNodeStack.size()) {
            return false;
        }
        Iterator<ExpressionNode> i1 = sqlNodeStack.iterator();
        Iterator<ExpressionNode> i2 = that.sqlNodeStack.iterator();
        while (i1.hasNext() && i2.hasNext()) {
            ExpressionNode n1 = i1.next();
            ExpressionNode n2 = i2.next();
            if (!Objects.equals(n1, n2)) {
                return false;
            }
        }
        return orderByPosition == that.orderByPosition
                && latestByType == that.latestByType
                && metadataVersion == that.metadataVersion
                && joinType == that.joinType
                && joinKeywordPosition == that.joinKeywordPosition
                && limitPosition == that.limitPosition
                && isLimitImplemented == that.isLimitImplemented
                && isSelectTranslation == that.isSelectTranslation
                && selectModelType == that.selectModelType
                && nestedModelIsSubQuery == that.nestedModelIsSubQuery
                && distinct == that.distinct
                && setOperationType == that.setOperationType
                && modelPosition == that.modelPosition
                && orderByAdviceMnemonic == that.orderByAdviceMnemonic
                && tableId == that.tableId
                && isUpdateModel == that.isUpdateModel
                && modelType == that.modelType
                && artificialStar == that.artificialStar
                && Objects.equals(bottomUpColumns, that.bottomUpColumns)
                && Objects.equals(topDownNameSet, that.topDownNameSet)
                && Objects.equals(topDownColumns, that.topDownColumns)
                && Objects.equals(aliasToColumnNameMap, that.aliasToColumnNameMap)
                && Objects.equals(columnNameToAliasMap, that.columnNameToAliasMap)
                && Objects.equals(aliasToColumnMap, that.aliasToColumnMap)
                && Objects.equals(bottomUpColumnNames, that.bottomUpColumnNames)
                && Objects.equals(orderBy, that.orderBy)
                && Objects.equals(groupBy, that.groupBy)
                && Objects.equals(orderByDirection, that.orderByDirection)
                && Objects.equals(dependencies, that.dependencies)
                && Objects.equals(orderedJoinModels1, that.orderedJoinModels1)
                && Objects.equals(orderedJoinModels2, that.orderedJoinModels2)
                && Objects.equals(columnAliasIndexes, that.columnAliasIndexes)
                && Objects.equals(modelAliasIndexes, that.modelAliasIndexes)
                && Objects.equals(expressionModels, that.expressionModels)
                && Objects.equals(parsedWhere, that.parsedWhere)
                && Objects.equals(parsedWhereConstants, that.parsedWhereConstants)
                && Objects.equals(orderHash, that.orderHash)
                && Objects.equals(joinColumns, that.joinColumns)
                && Objects.equals(sampleByFill, that.sampleByFill)
                && Objects.equals(latestBy, that.latestBy)
                && Objects.equals(orderByAdvice, that.orderByAdvice)
                && Objects.equals(orderByDirectionAdvice, that.orderByDirectionAdvice)
                && Objects.equals(withClauseModel, that.withClauseModel)
                && Objects.equals(updateSetColumns, that.updateSetColumns)
                && Objects.equals(updateTableColumnTypes, that.updateTableColumnTypes)
                && Objects.equals(updateTableColumnNames, that.updateTableColumnNames)
                && Objects.equals(sampleByTimezoneName, that.sampleByTimezoneName)
                && Objects.equals(sampleByOffset, that.sampleByOffset)
                && Objects.equals(whereClause, that.whereClause)
                && Objects.equals(backupWhereClause, that.backupWhereClause)
                && Objects.equals(postJoinWhereClause, that.postJoinWhereClause)
                && Objects.equals(outerJoinExpressionClause, that.outerJoinExpressionClause)
                && Objects.equals(constWhereClause, that.constWhereClause)
                && Objects.equals(nestedModel, that.nestedModel)
                && Objects.equals(tableNameExpr, that.tableNameExpr)
                && Objects.equals(tableNameFunction, that.tableNameFunction)
                && Objects.equals(alias, that.alias)
                && Objects.equals(timestamp, that.timestamp)
                && Objects.equals(sampleBy, that.sampleBy)
                && Objects.equals(sampleByUnit, that.sampleByUnit)
                && Objects.equals(context, that.context)
                && Objects.equals(joinCriteria, that.joinCriteria)
                && Objects.equals(orderedJoinModels, that.orderedJoinModels)
                && Objects.equals(limitLo, that.limitLo)
                && Objects.equals(limitHi, that.limitHi)
                && Objects.equals(limitAdviceLo, that.limitAdviceLo)
                && Objects.equals(limitAdviceHi, that.limitAdviceHi)
                && Objects.equals(unionModel, that.unionModel)
                && Objects.equals(updateTableModel, that.updateTableModel)
                && Objects.equals(updateTableToken, that.updateTableToken);
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

    public int getColumnAliasIndex(CharSequence alias) {
        return columnAliasIndexes.get(alias);
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

    public JoinContext getContext() {
        return context;
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

    public int getJoinKeywordPosition() {
        return joinKeywordPosition;
    }

    public ObjList<QueryModel> getJoinModels() {
        return joinModels;
    }

    public int getJoinType() {
        return joinType;
    }

    public ObjList<ExpressionNode> getLatestBy() {
        return latestBy;
    }

    public int getLatestByType() {
        return latestByType;
    }

    public ExpressionNode getLimitAdviceHi() {
        return limitAdviceHi;
    }

    public ExpressionNode getLimitAdviceLo() {
        return limitAdviceLo;
    }

    public ExpressionNode getLimitHi() {
        return limitHi;
    }

    public ExpressionNode getLimitLo() {
        return limitLo;
    }

    public int getLimitPosition() {
        return limitPosition;
    }

    public long getMetadataVersion() {
        return metadataVersion;
    }

    public int getModelAliasIndex(CharSequence column, int start, int end) {
        int index = modelAliasIndexes.keyIndex(column, start, end);
        if (index < 0) {
            return modelAliasIndexes.valueAt(index);
        }
        return -1;
    }

    public LowerCaseCharSequenceIntHashMap getModelAliasIndexes() {
        return modelAliasIndexes;
    }

    public int getModelPosition() {
        return modelPosition;
    }

    @Override
    public int getModelType() {
        return modelType;
    }

    public CharSequence getName() {
        if (alias != null) {
            return alias.token;
        }

        if (tableNameExpr != null) {
            return tableNameExpr.token;
        }

        return null;
    }

    public QueryModel getNestedModel() {
        return nestedModel;
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

    public IntList getOrderByDirection() {
        return orderByDirection;
    }

    public IntList getOrderByDirectionAdvice() {
        return orderByDirectionAdvice;
    }

    public int getOrderByPosition() {
        return orderByPosition;
    }

    public LowerCaseCharSequenceIntHashMap getOrderHash() {
        return orderHash;
    }

    public IntList getOrderedJoinModels() {
        return orderedJoinModels;
    }

    public ExpressionNode getOuterJoinExpressionClause() {
        return outerJoinExpressionClause;
    }

    public ObjList<ExpressionNode> getParsedWhere() {
        return parsedWhere;
    }

    public ExpressionNode getPostJoinWhereClause() {
        return postJoinWhereClause;
    }

    @Override
    public QueryModel getQueryModel() {
        return this;
    }

    public ExpressionNode getSampleBy() {
        return sampleBy;
    }

    public ObjList<ExpressionNode> getSampleByFill() {
        return sampleByFill;
    }

    public ExpressionNode getSampleByOffset() {
        return sampleByOffset;
    }

    public ExpressionNode getSampleByTimezoneName() {
        return sampleByTimezoneName;
    }

    public ExpressionNode getSampleByUnit() {
        return sampleByUnit;
    }

    public int getSelectModelType() {
        return selectModelType;
    }

    public int getSetOperationType() {
        return setOperationType;
    }

    public int getShowKind() {
        return showKind;
    }

    public int getTableId() {
        return tableId;
    }

    public CharSequence getTableName() {
        return tableNameExpr != null ? tableNameExpr.token : null;
    }

    @Override
    public ExpressionNode getTableNameExpr() {
        return tableNameExpr;
    }

    public RecordCursorFactory getTableNameFunction() {
        return tableNameFunction;
    }

    public ExpressionNode getTimestamp() {
        return timestamp;
    }

    public ObjList<QueryColumn> getTopDownColumns() {
        return topDownColumns;
    }

    public QueryModel getUnionModel() {
        return unionModel;
    }

    public ObjList<ExpressionNode> getUpdateExpressions() {
        return updateSetColumns;
    }

    public ObjList<CharSequence> getUpdateTableColumnNames() {
        return this.updateTableModel != null ? this.updateTableModel.getUpdateTableColumnNames() : updateTableColumnNames;
    }

    public IntList getUpdateTableColumnTypes() {
        return this.updateTableModel != null ? this.updateTableModel.getUpdateTableColumnTypes() : updateTableColumnTypes;
    }

    public TableToken getUpdateTableToken() {
        return updateTableToken;
    }

    public ExpressionNode getWhereClause() {
        return whereClause;
    }

    public LowerCaseCharSequenceObjHashMap<WithClauseModel> getWithClauses() {
        return withClauseModel;
    }

    public boolean hasExplicitTimestamp() {
        return timestamp != null && explicitTimestamp;
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        // joinModels always contain this as the first element, so we need to hash them manually.
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            hash = 31 * hash + Objects.hash(joinModels.getQuick(i));
        }
        // ArrayDeque doesn't implement equals and hashCode, so we deal with sqlNodeStack separately.
        for (ExpressionNode node : sqlNodeStack) {
            hash = 31 * hash + Objects.hash(node);
        }
        return 31 * hash + Objects.hash(
                bottomUpColumns, topDownNameSet, topDownColumns,
                aliasToColumnNameMap, columnNameToAliasMap, aliasToColumnMap,
                bottomUpColumnNames, orderBy,
                orderByPosition, groupBy, orderByDirection,
                dependencies, orderedJoinModels1, orderedJoinModels2,
                columnAliasIndexes, modelAliasIndexes, expressionModels,
                parsedWhere, parsedWhereConstants,
                orderHash, joinColumns, sampleByFill,
                latestBy, orderByAdvice, orderByDirectionAdvice,
                withClauseModel, updateSetColumns, updateTableColumnTypes,
                updateTableColumnNames, sampleByTimezoneName, sampleByOffset,
                latestByType, whereClause, backupWhereClause,
                postJoinWhereClause, outerJoinExpressionClause, constWhereClause, nestedModel,
                tableNameExpr, metadataVersion, tableNameFunction,
                alias, timestamp, sampleBy,
                sampleByUnit, context, joinCriteria,
                joinType, joinKeywordPosition, orderedJoinModels,
                limitLo, limitHi, limitPosition,
                limitAdviceLo, limitAdviceHi, isLimitImplemented,
                isSelectTranslation, selectModelType, nestedModelIsSubQuery,
                distinct, unionModel, setOperationType,
                modelPosition, orderByAdviceMnemonic, tableId,
                isUpdateModel, modelType, updateTableModel,
                updateTableToken, artificialStar
        );
    }

    public boolean isArtificialStar() {
        return artificialStar;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public boolean isExplicitTimestamp() {
        return explicitTimestamp;
    }

    public boolean isLimitImplemented() {
        return isLimitImplemented;
    }

    public boolean isNestedModelIsSubQuery() {
        return nestedModelIsSubQuery;
    }

    public boolean isOrderByTimestamp(CharSequence orderByToken) {
        if (Chars.equalsIgnoreCase(orderByToken, timestamp.token)) {
            return true;
        }

        try {
            int columnIndex = Numbers.parseInt(orderByToken);
            if (columnIndex < 1 && columnIndex > bottomUpColumns.size()) {
                return false;
            }
            return Chars.equalsIgnoreCase(bottomUpColumnNames.getQuick(columnIndex - 1), timestamp.token);
        } catch (NumericException e) {
            return false;
        }
    }

    public boolean isSelectTranslation() {
        return isSelectTranslation;
    }

    @SuppressWarnings("unused")
    public boolean isTemporalJoin() {
        return joinType >= JOIN_ASOF && joinType <= JOIN_LT;
    }

    public boolean isTopDownNameMissing(CharSequence columnName) {
        return topDownNameSet.excludes(columnName);
    }

    public boolean isUpdate() {
        return isUpdateModel;
    }

    public void mergeInnerColumn(QueryColumn innerQC) {
        final CharSequence nestedAlias = innerQC.getAlias();
        final ExpressionNode nestedAst = innerQC.getAst();
        assert nestedAlias != null;
        final CharSequence alias = columnNameToAliasMap.get(nestedAlias);
        if (alias == null) {
            return;
        }
        final QueryColumn oldQC = aliasToColumnMap.get(alias);
        if (oldQC == null) {
            return;
        }
        final ExpressionNode oldAst = oldQC.getAst();
        aliasToColumnNameMap.put(alias, nestedAst.token);
        columnNameToAliasMap.remove(oldAst.token);
        columnNameToAliasMap.put(nestedAst.token, alias);
        aliasToColumnMap.put(alias, innerQC);
        int colIndex = columnAliasIndexes.get(alias);
        assert colIndex >= 0;
        bottomUpColumns.set(colIndex, innerQC);
        bottomUpColumnNames.set(colIndex, alias);
        innerQC.setAlias(alias);
    }

    public void moveGroupByFrom(QueryModel model) {
        groupBy.addAll(model.groupBy);
        // clear the source
        model.groupBy.clear();
    }

    public void moveJoinAliasFrom(QueryModel that) {
        final ExpressionNode alias = that.alias;
        if (alias != null && !Chars.startsWith(alias.token, SUB_QUERY_ALIAS_PREFIX)) {
            setAlias(alias);
            addModelAliasIndex(alias, 0);
        }
    }

    public void moveLimitFrom(QueryModel baseModel) {
        this.limitLo = baseModel.getLimitLo();
        this.limitHi = baseModel.getLimitHi();
        baseModel.setLimit(null, null);
    }

    public void moveSampleByFrom(QueryModel model) {
        this.sampleBy = model.sampleBy;
        this.sampleByUnit = model.sampleByUnit;
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

    public void setAlias(ExpressionNode alias) {
        this.alias = alias;
    }

    public void setArtificialStar(boolean artificialStar) {
        this.artificialStar = artificialStar;
    }

    public void setConstWhereClause(ExpressionNode constWhereClause) {
        this.constWhereClause = constWhereClause;
    }

    public void setContext(JoinContext context) {
        this.context = context;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public void setExplicitTimestamp(boolean explicitTimestamp) {
        this.explicitTimestamp = explicitTimestamp;
    }

    public void setIsUpdate(boolean isUpdate) {
        this.isUpdateModel = isUpdate;
    }

    public void setJoinCriteria(ExpressionNode joinCriteria) {
        this.joinCriteria = joinCriteria;
    }

    public void setJoinKeywordPosition(int position) {
        this.joinKeywordPosition = position;
    }

    public void setJoinType(int joinType) {
        this.joinType = joinType;
    }

    public void setLatestByType(int latestByType) {
        this.latestByType = latestByType;
    }

    public void setLimit(ExpressionNode lo, ExpressionNode hi) {
        this.limitLo = lo;
        this.limitHi = hi;
    }

    public void setLimitAdvice(ExpressionNode lo, ExpressionNode hi) {
        this.limitAdviceLo = lo;
        this.limitAdviceHi = hi;
    }

    public void setLimitImplemented(boolean limitImplemented) {
        isLimitImplemented = limitImplemented;
    }

    public void setLimitPosition(int limitPosition) {
        this.limitPosition = limitPosition;
    }

    public void setMetadataVersion(long metadataVersion) {
        this.metadataVersion = metadataVersion;
    }

    public void setModelPosition(int modelPosition) {
        this.modelPosition = modelPosition;
    }

    public void setModelType(int modelType) {
        this.modelType = modelType;
    }

    public void setNestedModel(QueryModel nestedModel) {
        this.nestedModel = nestedModel;
    }

    public void setNestedModelIsSubQuery(boolean nestedModelIsSubQuery) {
        this.nestedModelIsSubQuery = nestedModelIsSubQuery;
    }

    public void setOrderByAdviceMnemonic(int orderByAdviceMnemonic) {
        this.orderByAdviceMnemonic = orderByAdviceMnemonic;
    }

    public void setOrderByPosition(int orderByPosition) {
        this.orderByPosition = orderByPosition;
    }

    public void setOrderedJoinModels(IntList that) {
        assert that == orderedJoinModels1 || that == orderedJoinModels2;
        this.orderedJoinModels = that;
    }

    public void setOuterJoinExpressionClause(ExpressionNode outerJoinExpressionClause) {
        this.outerJoinExpressionClause = outerJoinExpressionClause;
    }

    public void setPostJoinWhereClause(ExpressionNode postJoinWhereClause) {
        this.postJoinWhereClause = postJoinWhereClause;
    }

    public void setSampleBy(ExpressionNode sampleBy) {
        this.sampleBy = sampleBy;
    }

    public void setSampleBy(ExpressionNode sampleBy, ExpressionNode sampleByUnit) {
        this.sampleBy = sampleBy;
        this.sampleByUnit = sampleByUnit;
    }

    public void setSampleByOffset(ExpressionNode sampleByOffset) {
        this.sampleByOffset = sampleByOffset;
    }

    public void setSampleByTimezoneName(ExpressionNode sampleByTimezoneName) {
        this.sampleByTimezoneName = sampleByTimezoneName;
    }

    public void setSelectModelType(int selectModelType) {
        this.selectModelType = selectModelType;
    }

    public void setSelectTranslation(boolean isSelectTranslation) {
        this.isSelectTranslation = isSelectTranslation;
    }

    public void setSetOperationType(int setOperationType) {
        this.setOperationType = setOperationType;
    }

    public void setShowKind(int showKind) {
        this.showKind = showKind;
    }

    public void setTableFactory(RecordCursorFactory function) {
        this.tableNameFunction = function;
    }

    public void setTableId(int id) {
        this.tableId = id;
    }

    public void setTableNameExpr(ExpressionNode tableNameExpr) {
        this.tableNameExpr = tableNameExpr;
    }

    public void setTimestamp(ExpressionNode timestamp) {
        this.timestamp = timestamp;
    }

    public void setUnionModel(QueryModel unionModel) {
        this.unionModel = unionModel;
    }

    public void setUpdateTableToken(TableToken tableName) {
        this.updateTableToken = tableName;
    }

    public void setWhereClause(ExpressionNode whereClause) {
        this.whereClause = whereClause;
    }

    @Override
    public void toSink(@NotNull CharSinkBase<?> sink) {
        if (modelType == ExecutionModel.QUERY) {
            toSink0(sink, false, false);
        } else if (modelType == ExecutionModel.UPDATE) {
            updateToSink(sink);
        }
    }

    // method to make debugging easier 
    // not using toString name to prevent debugger from trying to use it on all model variables (because toSink0 can fail).
    @SuppressWarnings("unused")
    public String toString0() {
        StringSink sink = Misc.getThreadLocalSink();
        this.toSink0(sink, true, true);
        return sink.toString();
    }

    @Override
    public CharSequence translateAlias(CharSequence column) {
        return aliasToColumnNameMap.get(column);
    }

    private static void aliasToSink(CharSequence alias, CharSinkBase<?> sink) {
        sink.putAscii(' ');
        boolean quote = Chars.indexOf(alias, ' ') != -1;
        if (quote) {
            sink.putAscii('\'').put(alias).putAscii('\'');
        } else {
            sink.put(alias);
        }
    }

    private static void unitToSink(CharSinkBase<?> sink, long timeUnit) {
        if (timeUnit == WindowColumn.TIME_UNIT_MICROSECOND) {
            sink.putAscii(" microsecond");
        } else if (timeUnit == WindowColumn.TIME_UNIT_MILLISECOND) {
            sink.putAscii(" millisecond");
        } else if (timeUnit == WindowColumn.TIME_UNIT_SECOND) {
            sink.putAscii(" second");
        } else if (timeUnit == WindowColumn.TIME_UNIT_MINUTE) {
            sink.putAscii(" minute");
        } else if (timeUnit == WindowColumn.TIME_UNIT_HOUR) {
            sink.putAscii(" hour");
        } else if (timeUnit == WindowColumn.TIME_UNIT_DAY) {
            sink.putAscii(" day");
        } else {
            sink.putAscii(" [unknown unit]");
        }
    }

    private String getSelectModelTypeText() {
        return modelTypeName.get(selectModelType);
    }

    private void sinkColumns(CharSinkBase<?> sink, ObjList<QueryColumn> columns) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (i > 0) {
                sink.putAscii(", ");
            }
            QueryColumn column = columns.getQuick(i);
            CharSequence name = column.getName();
            CharSequence alias = column.getAlias();
            ExpressionNode ast = column.getAst();
            ast.toSink(sink);
            if (column.isWindowColumn() || name == null) {

                if (alias != null) {
                    aliasToSink(alias, sink);
                }

                // this can only be window column
                if (name != null) {
                    WindowColumn ac = (WindowColumn) column;
                    sink.putAscii(" over (");
                    final ObjList<ExpressionNode> partitionBy = ac.getPartitionBy();
                    if (partitionBy.size() > 0) {
                        sink.putAscii("partition by ");
                        for (int k = 0, z = partitionBy.size(); k < z; k++) {
                            if (k > 0) {
                                sink.putAscii(", ");
                            }
                            partitionBy.getQuick(k).toSink(sink);
                        }
                    }

                    final ObjList<ExpressionNode> orderBy = ac.getOrderBy();
                    if (orderBy.size() > 0) {
                        if (partitionBy.size() > 0) {
                            sink.put(' ');
                        }
                        sink.putAscii("order by ");
                        for (int k = 0, z = orderBy.size(); k < z; k++) {
                            if (k > 0) {
                                sink.putAscii(", ");
                            }
                            orderBy.getQuick(k).toSink(sink);
                            if (ac.getOrderByDirection().getQuick(k) == 1) {
                                sink.putAscii(" desc");
                            }
                        }
                    }

                    if (ac.isNonDefaultFrame()) {
                        switch (ac.getFramingMode()) {
                            case WindowColumn.FRAMING_ROWS:
                                sink.putAscii(" rows");
                                break;
                            case WindowColumn.FRAMING_RANGE:
                                sink.putAscii(" range");
                                break;
                            case WindowColumn.FRAMING_GROUPS:
                                sink.putAscii(" groups");
                                break;
                            default:
                                break;
                        }
                        sink.put(" between ");
                        if (ac.getRowsLoExpr() != null) {
                            ac.getRowsLoExpr().toSink(sink);
                            if (ac.getFramingMode() == WindowColumn.FRAMING_RANGE) {
                                unitToSink(sink, ac.getRowsLoExprTimeUnit());
                            }

                            switch (ac.getRowsLoKind()) {
                                case WindowColumn.PRECEDING:
                                    sink.putAscii(" preceding");
                                    break;
                                case WindowColumn.FOLLOWING:
                                    sink.putAscii(" following");
                                    break;
                                default:
                                    break;
                            }
                        } else {
                            switch (ac.getRowsLoKind()) {
                                case WindowColumn.PRECEDING:
                                    sink.putAscii("unbounded preceding");
                                    break;
                                case WindowColumn.FOLLOWING:
                                    sink.putAscii("unbounded following");
                                    break;
                                default:
                                    // CURRENT
                                    sink.putAscii("current row");
                                    break;
                            }
                        }
                        sink.putAscii(" and ");

                        if (ac.getRowsHiExpr() != null) {
                            ac.getRowsHiExpr().toSink(sink);
                            if (ac.getFramingMode() == WindowColumn.FRAMING_RANGE) {
                                unitToSink(sink, ac.getRowsHiExprTimeUnit());
                            }

                            switch (ac.getRowsHiKind()) {
                                case WindowColumn.PRECEDING:
                                    sink.putAscii(" preceding");
                                    break;
                                case WindowColumn.FOLLOWING:
                                    sink.putAscii(" following");
                                    break;
                                default:
                                    assert false;
                                    break;
                            }
                        } else {
                            switch (ac.getRowsHiKind()) {
                                case WindowColumn.PRECEDING:
                                    sink.putAscii("unbounded preceding");
                                    break;
                                case WindowColumn.FOLLOWING:
                                    sink.putAscii("unbounded following");
                                    break;
                                default:
                                    // CURRENT
                                    sink.put("current row");
                                    break;
                            }
                        }

                        switch (ac.getExclusionKind()) {
                            case WindowColumn.EXCLUDE_CURRENT_ROW:
                                sink.putAscii(" exclude current row");
                                break;
                            case WindowColumn.EXCLUDE_GROUP:
                                sink.putAscii(" exclude group");
                                break;
                            case WindowColumn.EXCLUDE_TIES:
                                sink.putAscii(" exclude ties");
                                break;
                            case WindowColumn.EXCLUDE_NO_OTHERS:
                                sink.putAscii(" exclude no others");
                                break;
                            default:
                                assert false;
                                break;
                        }
                    }
                    sink.putAscii(')');
                }
            } else {
                // do not repeat alias when it is the same as AST token, provided AST is a literal
                if (alias != null && (ast.type != ExpressionNode.LITERAL || !ast.token.equals(alias))) {
                    aliasToSink(alias, sink);
                }
            }
        }
    }

    // returns textual description of this model, e.g. select-choose [top-down-columns] bottom-up-columns from X ...
    private void toSink0(CharSinkBase<?> sink, boolean joinSlave, boolean showOrderBy) {
        if (selectModelType == QueryModel.SELECT_MODEL_SHOW) {
            sink.put(getSelectModelTypeText());
        } else {
            final boolean hasColumns = topDownColumns.size() > 0 || bottomUpColumns.size() > 0;
            if (hasColumns) {
                sink.put(getSelectModelTypeText());
                if (topDownColumns.size() > 0) {
                    sink.putAscii(' ');
                    sink.putAscii('[');
                    sinkColumns(sink, topDownColumns);
                    sink.putAscii(']');
                }
                if (this.bottomUpColumns.size() > 0) {
                    sink.putAscii(' ');
                    sinkColumns(sink, bottomUpColumns);
                }
                sink.putAscii(" from ");
            }
            if (tableNameExpr != null) {
                tableNameExpr.toSink(sink);
            } else {
                sink.putAscii('(');
                nestedModel.toSink0(sink, false, showOrderBy);
                sink.putAscii(')');
            }
            if (alias != null) {
                aliasToSink(alias.token, sink);
            }

            if (getLatestByType() != LATEST_BY_NEW && timestamp != null) {
                sink.putAscii(" timestamp (");
                timestamp.toSink(sink);
                sink.putAscii(')');
            }

            if (getLatestByType() == LATEST_BY_DEPRECATED && getLatestBy().size() > 0) {
                sink.putAscii(" latest by ");
                for (int i = 0, n = getLatestBy().size(); i < n; i++) {
                    if (i > 0) {
                        sink.putAscii(',');
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
                                sink.putAscii(" left join ");
                                break;
                            case JOIN_ASOF:
                                sink.putAscii(" asof join ");
                                break;
                            case JOIN_SPLICE:
                                sink.putAscii(" splice join ");
                                break;
                            case JOIN_CROSS:
                                sink.putAscii(" cross join ");
                                break;
                            case JOIN_LT:
                                sink.putAscii(" lt join ");
                                break;
                            default:
                                sink.putAscii(" join ");
                                break;
                        }

                        if (model.getWhereClause() != null) {
                            sink.putAscii('(');
                            model.toSink0(sink, true, showOrderBy);
                            sink.putAscii(')');
                            if (model.getAlias() != null) {
                                aliasToSink(model.getAlias().token, sink);
                            } else if (model.getTableName() != null) {
                                aliasToSink(model.getTableName(), sink);
                            }
                        } else {
                            model.toSink0(sink, true, showOrderBy);
                        }

                        JoinContext jc = model.getContext();
                        if (jc != null && jc.aIndexes.size() > 0) {
                            // join clause
                            sink.putAscii(" on ");
                            for (int k = 0, z = jc.aIndexes.size(); k < z; k++) {
                                if (k > 0) {
                                    sink.putAscii(" and ");
                                }
                                jc.aNodes.getQuick(k).toSink(sink);
                                sink.putAscii(" = ");
                                jc.bNodes.getQuick(k).toSink(sink);
                            }
                        }

                        if (model.getOuterJoinExpressionClause() != null) {
                            sink.putAscii(" outer-join-expression ");
                            model.getOuterJoinExpressionClause().toSink(sink);
                        }

                        if (model.getPostJoinWhereClause() != null) {
                            sink.putAscii(" post-join-where ");
                            model.getPostJoinWhereClause().toSink(sink);
                        }
                    }
                }
            }
        }

        if (getWhereClause() != null) {
            sink.putAscii(" where ");
            whereClause.toSink(sink);
        }

        if (constWhereClause != null) {
            sink.putAscii(" const-where ");
            constWhereClause.toSink(sink);
        }

        if (!joinSlave && postJoinWhereClause != null) {
            sink.putAscii(" post-join-where ");
            postJoinWhereClause.toSink(sink);
        }

        if (!joinSlave && outerJoinExpressionClause != null) {
            sink.putAscii(" outer-join-expressions ");
            outerJoinExpressionClause.toSink(sink);
        }

        if (getLatestByType() == LATEST_BY_NEW && getLatestBy().size() > 0) {
            sink.putAscii(" latest on ");
            timestamp.toSink(sink);
            sink.putAscii(" partition by ");
            for (int i = 0, n = getLatestBy().size(); i < n; i++) {
                if (i > 0) {
                    sink.put(',');
                }
                getLatestBy().getQuick(i).toSink(sink);
            }
        }

        if (sampleBy != null) {
            sink.putAscii(" sample by ");
            sampleBy.toSink(sink);

            final int fillCount = sampleByFill.size();
            if (fillCount > 0) {
                sink.putAscii(" fill(");
                sink.put(sampleByFill.getQuick(0));

                if (fillCount > 1) {
                    for (int i = 1; i < fillCount; i++) {
                        sink.putAscii(',');
                        sink.put(sampleByFill.getQuick(i));
                    }
                }
                sink.putAscii(')');
            }

            if (sampleByTimezoneName != null || sampleByOffset != null) {
                sink.putAscii(" align to calendar");
                if (sampleByTimezoneName != null) {
                    sink.putAscii(" time zone ");
                    sink.put(sampleByTimezoneName);
                }

                if (sampleByOffset != null) {
                    sink.putAscii(" with offset ");
                    sink.put(sampleByOffset);
                }
            }
        }

        if (showOrderBy && orderBy.size() > 0) {
            sink.putAscii(" order by ");
            for (int i = 0, n = orderBy.size(); i < n; i++) {
                if (i > 0) {
                    sink.putAscii(", ");
                }
                sink.put(orderBy.get(i));
                if (orderByDirection.get(i) == 1) {
                    sink.putAscii(" desc");
                }
            }
        } else if (orderHash.size() > 0 && orderBy.size() > 0) {
            sink.putAscii(" order by ");

            ObjList<CharSequence> columnNames = orderHash.keys();
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                if (i > 0) {
                    sink.putAscii(", ");
                }

                CharSequence key = columnNames.getQuick(i);
                sink.put(key);
                if (orderHash.get(key) == 1) {
                    sink.putAscii(" desc");
                }
            }
        }

        if (getLimitLo() != null || getLimitHi() != null) {
            sink.putAscii(" limit ");
            if (getLimitLo() != null) {
                getLimitLo().toSink(sink);
            }
            if (getLimitHi() != null) {
                sink.putAscii(',');
                getLimitHi().toSink(sink);
            }
        }

        if (unionModel != null) {
            if (setOperationType == QueryModel.SET_OPERATION_INTERSECT) {
                sink.putAscii(" intersect ");
            } else if (setOperationType == QueryModel.SET_OPERATION_INTERSECT_ALL) {
                sink.putAscii(" intersect all ");
            } else if (setOperationType == QueryModel.SET_OPERATION_EXCEPT) {
                sink.putAscii(" except ");
            } else if (setOperationType == QueryModel.SET_OPERATION_EXCEPT_ALL) {
                sink.putAscii(" except all ");
            } else {
                sink.putAscii(" union ");
                if (setOperationType == QueryModel.SET_OPERATION_UNION_ALL) {
                    sink.putAscii("all ");
                }
            }
            unionModel.toSink0(sink, false, showOrderBy);
        }
    }

    private void updateToSink(CharSinkBase<?> sink) {
        sink.putAscii("update ");
        tableNameExpr.toSink(sink);
        if (alias != null) {
            sink.putAscii(" as");
            aliasToSink(alias.token, sink);
        }
        sink.putAscii(" set ");
        for (int i = 0, n = getUpdateExpressions().size(); i < n; i++) {
            if (i > 0) {
                sink.putAscii(',');
            }
            CharSequence columnExpr = getUpdateExpressions().get(i).token;
            sink.put(columnExpr);
            sink.putAscii(" = ");
            QueryColumn setColumn = getNestedModel().getColumns().getQuick(i);
            setColumn.getAst().toSink(sink);
        }

        if (getNestedModel() != null) {
            sink.putAscii(" from (");
            getNestedModel().toSink(sink);
            sink.putAscii(")");
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
        modelTypeName.extendAndSet(SELECT_MODEL_WINDOW, "select-window");
        modelTypeName.extendAndSet(SELECT_MODEL_GROUP_BY, "select-group-by");
        modelTypeName.extendAndSet(SELECT_MODEL_DISTINCT, "select-distinct");
        modelTypeName.extendAndSet(SELECT_MODEL_CURSOR, "select-cursor");
        modelTypeName.extendAndSet(SELECT_MODEL_SHOW, "show");
    }
}
