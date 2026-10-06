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

package io.questdb.griffin.model;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.ShowCreateDatabaseRecordCursorFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.SqlParser.ZERO_OFFSET;

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
    public static final int JOIN_CROSS_FULL = 12;
    public static final int JOIN_CROSS_LEFT = 8;
    public static final int JOIN_CROSS_RIGHT = 11;
    public static final int JOIN_FULL_OUTER = 10;
    public static final int JOIN_HORIZON = 13;
    public static final int JOIN_INNER = 1;
    public static final int JOIN_LATERAL_CROSS = 16;
    public static final int JOIN_LATERAL_INNER = 14;
    public static final int JOIN_LATERAL_LEFT = 15;
    public static final int JOIN_LEFT_OUTER = 2;
    public static final int JOIN_LT = 6;
    public static final int JOIN_NONE = 0;
    public static final int JOIN_RIGHT_OUTER = 9;
    public static final int JOIN_SPLICE = 5;
    public static final int JOIN_UNNEST = 17;
    public static final int JOIN_WINDOW = 7;
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
    public static final int SELECT_MODEL_HORIZON_JOIN = 9;
    public static final int SELECT_MODEL_NONE = 0;
    public static final int SELECT_MODEL_SHOW = 7;
    public static final int SELECT_MODEL_VIRTUAL = 2;
    public static final int SELECT_MODEL_WINDOW = 3;
    public static final int SELECT_MODEL_WINDOW_JOIN = 8;
    public static final int SET_OPERATION_EXCEPT = 2;
    public static final int SET_OPERATION_EXCEPT_ALL = 3;
    public static final int SET_OPERATION_INTERSECT = 4;
    public static final int SET_OPERATION_INTERSECT_ALL = 5;
    public static final int SET_OPERATION_UNION = 1;
    // types of set operations between this and union model
    public static final int SET_OPERATION_UNION_ALL = 0;
    public static final int SHOW_COLUMNS = 2;
    public static final int SHOW_CREATE_DATABASE = 18;
    public static final int SHOW_CREATE_LIVE_VIEW = 19;
    public static final int SHOW_CREATE_MAT_VIEW = 15;
    public static final int SHOW_CREATE_TABLE = 14;
    public static final int SHOW_CREATE_VIEW = 17;
    public static final int SHOW_DATE_STYLE = 9;
    public static final int SHOW_DEFAULT_TRANSACTION_READ_ONLY = 16;
    public static final int SHOW_MAX_IDENTIFIER_LENGTH = 6;
    public static final int SHOW_PARAMETERS = 11;
    public static final int SHOW_PARTITIONS = 3;
    public static final int SHOW_SEARCH_PATH = 8;
    public static final int SHOW_SERVER_VERSION = 12;
    public static final int SHOW_SERVER_VERSION_NUM = 13;
    public static final int SHOW_STANDARD_CONFORMING_STRINGS = 7;
    public static final int SHOW_TABLES = 1;
    public static final int SHOW_TIME_ZONE = 10;
    public static final int SHOW_TRANSACTION = 4;
    public static final int SHOW_TRANSACTION_ISOLATION_LEVEL = 5;
    public static final String SUB_QUERY_ALIAS_PREFIX = "_xQdbA";
    private static final ObjList<String> modelTypeName = new ObjList<>();
    private final LowerCaseCharSequenceObjHashMap<QueryColumn> aliasToColumnMap = new LowerCaseCharSequenceObjHashMap<>();
    private final LowerCaseCharSequenceObjHashMap<CharSequence> aliasToColumnNameMap = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjList<QueryColumn> bottomUpColumns = new ObjList<>();
    private final LowerCaseCharSequenceIntHashMap columnAliasIndexes = new LowerCaseCharSequenceIntHashMap();
    private final LowerCaseCharSequenceObjHashMap<CharSequence> columnNameToAliasMap = new LowerCaseCharSequenceObjHashMap<>();
    private final LowerCaseCharSequenceObjHashMap<ExpressionNode> decls = new LowerCaseCharSequenceObjHashMap<>();
    private final IntHashSet dependencies = new IntHashSet();
    private final ObjList<ExpressionNode> expressionModels = new ObjList<>();
    private final ObjList<ExpressionNode> groupBy = new ObjList<>();
    private final LowerCaseCharSequenceObjHashMap<CharSequence> hintsMap = new LowerCaseCharSequenceObjHashMap<>();
    private final HorizonJoinContext horizonJoinContext = new HorizonJoinContext();
    private final ObjList<ExpressionNode> joinColumns = new ObjList<>(4);
    private final ObjList<QueryModel> joinModels = new ObjList<>();
    private final ObjList<ExpressionNode> latestBy = new ObjList<>();
    // Named window definitions from WINDOW clause (e.g., WINDOW w AS (PARTITION BY ...))
    private final LowerCaseCharSequenceObjHashMap<WindowExpression> namedWindows = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjList<ExpressionNode> orderBy = new ObjList<>();
    private final IntList orderByDirection = new IntList();
    private final LowerCaseCharSequenceHashSet overridableDecls = new LowerCaseCharSequenceHashSet();
    // collect frequency of column names from each join model
    // and check if any of columns with frequency > 0 are selected
    // column name frequency of 1 corresponds to map value 0
    // column name frequency of 0 corresponds to map value -1
    private final ObjList<PivotForColumn> pivotForColumns = new ObjList<>();
    private final ObjList<QueryColumn> pivotGroupByColumns = new ObjList<>();
    private final ObjList<ViewDefinition> referencedViews = new ObjList<>();
    private final ObjList<ExpressionNode> sampleByFill = new ObjList<>();
    private final ObjList<QueryColumn> topDownColumns = new ObjList<>();
    private final ObjList<CharSequence> unnestColumnAliases = new ObjList<>();
    private final ObjList<ExpressionNode> unnestExpressions = new ObjList<>();
    private final ObjList<ObjList<CharSequence>> unnestJsonColumnNames = new ObjList<>();
    private final ObjList<IntList> unnestJsonColumnTypes = new ObjList<>();
    private final ObjList<ExpressionNode> updateSetColumns = new ObjList<>();
    private final ObjList<CharSequence> updateTableColumnNames = new ObjList<>();
    private final IntList updateTableColumnTypes = new IntList();
    private final ObjList<CharSequence> wildcardColumnNames = new ObjList<>();
    private final WindowJoinContext windowJoinContext = new WindowJoinContext();
    private final LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauseModel = new LowerCaseCharSequenceObjHashMap<>();
    // used for the parallel sample by rewrite. In the future, if we deprecate original SAMPLE BY, then these will
    // be the only fields for these values.
    private ExpressionNode alias;
    // used to block pushing down of order by advice to lower model
    private boolean artificialStar;
    private ExpressionNode asOfJoinTolerance = null;
    // Used to store a deep copy of the whereClause field
    // since whereClause can be changed during optimization/generation stage.
    private boolean cacheable = true;
    private boolean distinct = false;
    private boolean explicitTimestamp;
    private boolean isCommaJoin;
    private boolean isCteModel;
    private boolean isUpdateModel;
    private ExpressionNode joinCriteria;
    private int joinKeywordPosition;
    private int joinType = JOIN_NONE;
    private int latestByType = LATEST_BY_NONE;
    private ExpressionNode limitHi;
    private ExpressionNode limitLo;
    // position of the limit clause token
    private int limitPosition;
    private long metadataVersion = -1;
    private int modelPosition = 0;
    private int modelType = ExecutionModel.QUERY;
    private QueryModel nestedModel;
    private boolean nestedModelIsSubQuery = false;
    // position of the order by clause token
    private int orderByPosition;
    private ExpressionNode originatingViewNameExpr;
    // Expression clause that is actually part of left/outer join but not in join model.
    // Inner join expressions
    private boolean pivotGroupByColumnHasNoAlias = false;
    private ExpressionNode sampleBy;
    private ExpressionNode sampleByFrom;
    private ExpressionNode sampleByOffset = ZERO_OFFSET;
    private ExpressionNode sampleByTimezoneName = null;
    private ExpressionNode sampleByTo;
    private ExpressionNode sampleByUnit;
    private int selectModelType = SELECT_MODEL_NONE;
    private int setOperationType;
    private int showCreateDatabaseInclude = ShowCreateDatabaseRecordCursorFactory.INCLUDE_ALL;
    private int showKind = -1;
    private boolean standaloneUnnest;
    private ExpressionNode subsample;
    private int subsamplePosition;
    private int tableId = -1;
    private ExpressionNode tableNameExpr;
    private ExpressionNode timestamp;
    private QueryModel unionModel;
    private boolean unnestOrdinality;
    private TableToken updateTableToken;
    private ExpressionNode viewNameExpr;
    private ExpressionNode whereClause;

    private QueryModel() {
        joinModels.add(this);
    }

    public static boolean isLateralJoin(int joinType) {
        return joinType == JOIN_LATERAL_INNER
                || joinType == JOIN_LATERAL_LEFT
                || joinType == JOIN_LATERAL_CROSS;
    }

    public void addBottomUpColumn(QueryColumn column) throws SqlException {
        addBottomUpColumn(0, column, false, null);
    }

    public void addBottomUpColumn(int position, QueryColumn column, boolean allowDuplicates) throws SqlException {
        addBottomUpColumn(position, column, allowDuplicates, null);
    }

    public void addBottomUpColumn(
            int position,
            QueryColumn column,
            boolean allowDuplicates,
            CharSequence additionalMessage
    ) throws SqlException {
        if (!allowDuplicates && aliasToColumnMap.contains(column.getName())) {
            throw SqlException.duplicateColumn(position, column.getName(), additionalMessage);
        }
        addBottomUpColumnIfNotExists(column);
    }

    public void addBottomUpColumnIfNotExists(QueryColumn column) {
        if (addField(column)) {
            bottomUpColumns.add(column);
        }
    }

    public void addExpressionModel(ExpressionNode node) {
        assert node.queryModel != null;
        expressionModels.add(node);
    }

    public boolean addField(QueryColumn column) {
        final CharSequence alias = column.getAlias();
        final ExpressionNode ast = column.getAst();
        assert alias != null;
        aliasToColumnMap.put(alias, column);
        int aliasKeyIndex = aliasToColumnNameMap.keyIndex(alias);
        if (aliasKeyIndex > -1) {
            aliasToColumnNameMap.putAt(aliasKeyIndex, alias, ast.token);
            wildcardColumnNames.add(alias);
            columnNameToAliasMap.put(ast.token, alias);
            columnAliasIndexes.put(alias, wildcardColumnNames.size() - 1);
            return true;
        }
        return false;
    }

    public void addGroupBy(ExpressionNode node) {
        groupBy.add(node);
    }

    public void addHint(CharSequence key, CharSequence value) {
        hintsMap.put(key, value);
    }

    public void addJoinColumn(ExpressionNode node) {
        joinColumns.add(node);
    }

    public void addJoinModel(QueryModel joinModel) {
        joinModels.add(joinModel);
        if (joinModel != null && viewNameExpr != null) {
            joinModel.setViewNameExpr(viewNameExpr);
        }
    }

    public void addLatestBy(ExpressionNode latestBy) {
        this.latestBy.add(latestBy);
    }

    public void addOrderBy(ExpressionNode node, int direction) {
        orderBy.add(node);
        orderByDirection.add(direction);
    }

    public void addPivotForColumn(PivotForColumn column) {
        pivotForColumns.add(column);
    }

    public void addPivotGroupByColumn(QueryColumn column) {
        pivotGroupByColumns.add(column);
    }

    public void addSampleByFill(ExpressionNode sampleByFill) {
        this.sampleByFill.add(sampleByFill);
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
        orderByPosition = 0;
        groupBy.clear();
        dependencies.clear();
        whereClause = null;
        nestedModel = null;
        tableNameExpr = null;
        viewNameExpr = null;
        originatingViewNameExpr = null;
        alias = null;
        latestByType = LATEST_BY_NONE;
        latestBy.clear();
        joinCriteria = null;
        joinType = JOIN_NONE;
        joinKeywordPosition = 0;
        columnAliasIndexes.clear();
        limitHi = null;
        limitLo = null;
        limitPosition = 0;
        timestamp = null;
        joinColumns.clear();
        withClauseModel.clear();
        namedWindows.clear();
        selectModelType = SELECT_MODEL_NONE;
        columnNameToAliasMap.clear();
        tableId = -1;
        metadataVersion = -1;
        wildcardColumnNames.clear();
        expressionModels.clear();
        distinct = false;
        nestedModelIsSubQuery = false;
        unionModel = null;
        modelPosition = 0;
        topDownColumns.clear();
        aliasToColumnMap.clear();
        // TODO: replace booleans with an enum-like type: UPDATE/MAT_VIEW/INSERT_AS_SELECT/SELECT
        //  default is SELECT
        isUpdateModel = false;
        isCommaJoin = false;
        isCteModel = false;
        modelType = ExecutionModel.QUERY;
        updateSetColumns.clear();
        updateTableColumnTypes.clear();
        standaloneUnnest = false;
        unnestColumnAliases.clear();
        unnestExpressions.clear();
        unnestJsonColumnNames.clear();
        unnestJsonColumnTypes.clear();
        unnestOrdinality = false;
        updateTableColumnNames.clear();
        updateTableToken = null;
        setOperationType = SET_OPERATION_UNION_ALL;
        artificialStar = false;
        explicitTimestamp = false;
        showCreateDatabaseInclude = ShowCreateDatabaseRecordCursorFactory.INCLUDE_ALL;
        showKind = -1;
        sampleByOffset = ZERO_OFFSET;
        sampleByTo = null;
        sampleByFrom = null;
        subsample = null;
        subsamplePosition = 0;
        decls.clear();
        overridableDecls.clear();
        hintsMap.clear();
        asOfJoinTolerance = null;
        horizonJoinContext.clear();
        windowJoinContext.clear();
        pivotGroupByColumns.clear();
        pivotForColumns.clear();
        cacheable = true;
        pivotGroupByColumnHasNoAlias = false;
        referencedViews.clear();
    }

    public void clearSampleBy() {
        sampleBy = null;
        sampleByUnit = null;
        sampleByFill.clear();
        sampleByTimezoneName = null;
        sampleByOffset = null;
        sampleByTo = null;
        sampleByFrom = null;
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

    public void copyDeclsFrom(QueryModel model, boolean overrideDeclares) throws SqlException {
        copyDeclsFrom(model.getDecls(), overrideDeclares);
    }

    public void copyDeclsFrom(LowerCaseCharSequenceObjHashMap<ExpressionNode> decls, boolean overrideDeclares) throws SqlException {
        if (decls != null && decls.size() > 0) {
            final ObjList<CharSequence> keys = decls.keys();
            if (overrideDeclares) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    final CharSequence key = keys.getQuick(i);
                    // Only allow override if the variable is marked as OVERRIDABLE
                    if (!this.overridableDecls.contains(key) && this.decls.contains(key)) {
                        ExpressionNode existing = decls.get(key);
                        int position = existing != null ? existing.position : 0;
                        throw SqlException.$(position, "variable is not overridable: ").put(key);
                    }
                }
                this.decls.putAll(decls);
            } else {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    final CharSequence key = keys.getQuick(i);
                    this.decls.putIfAbsent(key, decls.get(key));
                }
            }
        }
    }

    public ExpressionNode getAlias() {
        return alias;
    }

    public LowerCaseCharSequenceObjHashMap<QueryColumn> getAliasToColumnMap() {
        return aliasToColumnMap;
    }

    @Nullable
    public ExpressionNode getAsOfJoinTolerance() {
        return asOfJoinTolerance;
    }

    public ObjList<QueryColumn> getBottomUpColumns() {
        return bottomUpColumns;
    }

    public ObjList<QueryColumn> getColumns() {
        return topDownColumns.size() > 0 ? topDownColumns : bottomUpColumns;
    }

    public LowerCaseCharSequenceObjHashMap<ExpressionNode> getDecls() {
        return decls;
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

    @NotNull
    public LowerCaseCharSequenceObjHashMap<CharSequence> getHints() {
        return hintsMap;
    }

    public HorizonJoinContext getHorizonJoinContext() {
        return horizonJoinContext;
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

    public LowerCaseCharSequenceObjHashMap<WindowExpression> getNamedWindows() {
        return namedWindows;
    }

    public QueryModel getNestedModel() {
        return nestedModel;
    }

    public ObjList<ExpressionNode> getOrderBy() {
        return orderBy;
    }

    public IntList getOrderByDirection() {
        return orderByDirection;
    }

    public int getOrderByPosition() {
        return orderByPosition;
    }

    public ExpressionNode getOriginatingViewNameExpr() {
        return originatingViewNameExpr;
    }

    public LowerCaseCharSequenceHashSet getOverridableDecls() {
        return overridableDecls;
    }

    public ObjList<PivotForColumn> getPivotForColumns() {
        return pivotForColumns;
    }

    public ObjList<QueryColumn> getPivotGroupByColumns() {
        return pivotGroupByColumns;
    }

    @Override
    public QueryModel getQueryModel() {
        return this;
    }

    public ObjList<ViewDefinition> getReferencedViews() {
        return referencedViews;
    }

    public ExpressionNode getSampleBy() {
        return sampleBy;
    }

    public ObjList<ExpressionNode> getSampleByFill() {
        return sampleByFill;
    }

    public ExpressionNode getSampleByFrom() {
        return sampleByFrom;
    }

    public ExpressionNode getSampleByOffset() {
        return sampleByOffset;
    }

    public ExpressionNode getSampleByTimezoneName() {
        return sampleByTimezoneName;
    }

    public ExpressionNode getSampleByTo() {
        return sampleByTo;
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

    public int getShowCreateDatabaseInclude() {
        return showCreateDatabaseInclude;
    }

    public int getShowKind() {
        return showKind;
    }

    public ExpressionNode getSubsample() {
        return subsample;
    }

    public int getSubsamplePosition() {
        return subsamplePosition;
    }

    public int getTableId() {
        return tableId;
    }

    @Override
    public CharSequence getTableName() {
        return tableNameExpr != null ? tableNameExpr.token : null;
    }

    @Override
    public ExpressionNode getTableNameExpr() {
        return tableNameExpr;
    }

    public ExpressionNode getTimestamp() {
        return timestamp;
    }

    public QueryModel getUnionModel() {
        return unionModel;
    }

    public ObjList<CharSequence> getUnnestColumnAliases() {
        return unnestColumnAliases;
    }

    public ObjList<ExpressionNode> getUnnestExpressions() {
        return unnestExpressions;
    }

    public ObjList<ObjList<CharSequence>> getUnnestJsonColumnNames() {
        return unnestJsonColumnNames;
    }

    public ObjList<IntList> getUnnestJsonColumnTypes() {
        return unnestJsonColumnTypes;
    }

    /**
     * Returns the total number of output columns across all UNNEST sources.
     * Array sources contribute 1 column each; JSON sources contribute N
     * columns (one per COLUMNS declaration).
     */
    public int getUnnestOutputColumnCount() {
        int total = 0;
        for (int i = 0, n = unnestExpressions.size(); i < n; i++) {
            if (isUnnestJsonSource(i)) {
                total += unnestJsonColumnNames.getQuick(i).size();
            } else {
                total++;
            }
        }
        return total;
    }

    public ObjList<ExpressionNode> getUpdateExpressions() {
        return updateSetColumns;
    }

    public TableToken getUpdateTableToken() {
        return updateTableToken;
    }

    public ExpressionNode getViewNameExpr() {
        return viewNameExpr;
    }

    public ExpressionNode getWhereClause() {
        return whereClause;
    }

    public WindowJoinContext getWindowJoinContext() {
        return windowJoinContext;
    }

    public LowerCaseCharSequenceObjHashMap<WithClauseModel> getWithClauses() {
        return withClauseModel;
    }

    public boolean hasExplicitTimestamp() {
        return timestamp != null && explicitTimestamp;
    }

    public boolean isArtificialStar() {
        return artificialStar;
    }

    public boolean isCacheable() {
        if (nestedModel != null) {
            return cacheable && nestedModel.isCacheable();
        }
        return cacheable;
    }

    public boolean isCommaJoin() {
        return isCommaJoin;
    }

    public boolean isCteModel() {
        return isCteModel;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public boolean isExplicitTimestamp() {
        return explicitTimestamp;
    }

    public boolean isNestedModelIsSubQuery() {
        return nestedModelIsSubQuery;
    }

    public boolean isPivot() {
        return pivotForColumns.size() > 0;
    }

    public boolean isPivotGroupByColumnHasNoAlias() {
        return pivotGroupByColumnHasNoAlias;
    }

    public boolean isStandaloneUnnest() {
        return standaloneUnnest;
    }

    @SuppressWarnings("unused")
    public boolean isTemporalJoin() {
        return joinType >= JOIN_ASOF && joinType <= JOIN_LT;
    }

    public boolean isUnnestJsonSource(int index) {
        return index < unnestJsonColumnNames.size()
                && unnestJsonColumnNames.getQuick(index) != null;
    }

    public boolean isUnnestOrdinality() {
        return unnestOrdinality;
    }

    public boolean isUpdate() {
        return isUpdateModel;
    }

    public void recordViews(LowerCaseCharSequenceObjHashMap<ViewDefinition> viewDefinitions) {
        final ObjList<CharSequence> keys = viewDefinitions.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final ViewDefinition viewDefinition = viewDefinitions.get(keys.getQuick(i));
            if (!referencedViews.contains(viewDefinition)) {
                referencedViews.add(viewDefinition);
            }
        }
    }

    /**
     * Removes column from the model by index. This method also removes all references to column alias, but
     * leaves out column name to alias mapping. This is because the mapping is ambiguous.
     *
     * @param columnIndex of the column to remove. This index is based on bottomUpColumns list.
     */
    public void removeColumn(int columnIndex) {
        CharSequence columnAlias = bottomUpColumns.getQuick(columnIndex).getAlias();
        bottomUpColumns.remove(columnIndex);
        wildcardColumnNames.remove(columnAlias);
        aliasToColumnMap.remove(columnAlias);
        aliasToColumnNameMap.remove(columnAlias);
        columnAliasIndexes.remove(columnAlias);
    }

    public void setAlias(ExpressionNode alias) {
        this.alias = alias;
    }

    public void setArtificialStar(boolean artificialStar) {
        this.artificialStar = artificialStar;
    }

    public void setAsOfJoinTolerance(ExpressionNode asOfJoinTolerance) {
        this.asOfJoinTolerance = asOfJoinTolerance;
    }

    public void setCacheable(boolean b) {
        cacheable = b;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public void setExplicitTimestamp(boolean explicitTimestamp) {
        this.explicitTimestamp = explicitTimestamp;
    }

    public void setIsCommaJoin(boolean isCommaJoin) {
        this.isCommaJoin = isCommaJoin;
    }

    public void setIsCteModel(boolean isCteModel) {
        this.isCteModel = isCteModel;
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
        if (nestedModel != null && viewNameExpr != null) {
            nestedModel.setViewNameExpr(viewNameExpr);
        }
    }

    public void setNestedModelIsSubQuery(boolean nestedModelIsSubQuery) {
        this.nestedModelIsSubQuery = nestedModelIsSubQuery;
    }

    public void setOrderByPosition(int orderByPosition) {
        this.orderByPosition = orderByPosition;
    }

    public void setOriginatingViewNameExpr(ExpressionNode originatingViewNameExpr) {
        this.originatingViewNameExpr = originatingViewNameExpr;
    }

    public void setPivotGroupByColumnHasNoAlias(boolean pivotGroupByColumnHasNoAlias) {
        this.pivotGroupByColumnHasNoAlias = pivotGroupByColumnHasNoAlias;
    }

    public void setSampleBy(ExpressionNode sampleBy) {
        this.sampleBy = sampleBy;
    }

    public void setSampleBy(ExpressionNode sampleBy, ExpressionNode sampleByUnit) {
        this.sampleBy = sampleBy;
        this.sampleByUnit = sampleByUnit;
    }

    public void setSampleByFromTo(ExpressionNode from, ExpressionNode to) {
        this.sampleByFrom = from;
        this.sampleByTo = to;
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

    public void setSetOperationType(int setOperationType) {
        this.setOperationType = setOperationType;
    }

    public void setShowCreateDatabaseInclude(int includeMask) {
        this.showCreateDatabaseInclude = includeMask;
    }

    public void setShowKind(int showKind) {
        this.showKind = showKind;
    }

    public void setStandaloneUnnest(boolean standaloneUnnest) {
        this.standaloneUnnest = standaloneUnnest;
    }

    public void setSubsample(ExpressionNode subsample, int position) {
        this.subsample = subsample;
        this.subsamplePosition = position;
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
        if (unionModel != null && viewNameExpr != null) {
            unionModel.setViewNameExpr(viewNameExpr);
        }
    }

    public void setUnnestOrdinality(boolean unnestOrdinality) {
        this.unnestOrdinality = unnestOrdinality;
    }

    public void setViewNameExpr(ExpressionNode viewNameExpr) {
        this.viewNameExpr = viewNameExpr;
        if (viewNameExpr != null) {
            if (nestedModel != null) {
                nestedModel.setViewNameExpr(viewNameExpr);
            }
            if (unionModel != null) {
                unionModel.setViewNameExpr(viewNameExpr);
            }
            for (int i = 1, n = joinModels.size(); i < n; i++) {
                joinModels.getQuick(i).setViewNameExpr(viewNameExpr);
            }
        }
    }

    public void setWhereClause(ExpressionNode whereClause) {
        this.whereClause = whereClause;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        if (modelType == ExecutionModel.QUERY) {
            toSink0(sink, false);
        } else if (modelType == ExecutionModel.UPDATE) {
            updateToSink(sink);
        }
    }

    // returns textual description of this model, e.g. select-choose [top-down-columns] bottom-up-columns from X ...
    public void toSink0(CharSink<?> sink, boolean showOrderBy) {
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
                if (bottomUpColumns.size() > 0) {
                    sink.putAscii(' ');
                    sinkColumns(sink, bottomUpColumns);
                }
                sink.putAscii(" from ");
            }
            if (tableNameExpr != null) {
                tableNameExpr.toSink(sink);
            } else if (nestedModel != null) {
                sink.putAscii('(');
                nestedModel.toSink0(sink, showOrderBy);
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
        }

        if (getWhereClause() != null) {
            sink.putAscii(" where ");
            whereClause.toSink(sink);
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

            if (sampleByFrom != null) {
                sink.putAscii(" from ");
                sampleByFrom.toSink(sink);
            }

            if (sampleByTo != null) {
                sink.putAscii(" to ");
                sampleByTo.toSink(sink);
            }

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

        if (groupBy.size() > 0 && selectModelType != SELECT_MODEL_GROUP_BY) {
            sink.putAscii(" group by ");
            for (int i = 0, n = groupBy.size(); i < n; i++) {
                if (i > 0) {
                    sink.putAscii(", ");
                }
                sink.put(groupBy.get(i));
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
            unionModel.toSink0(sink, showOrderBy);
        }

        if (hintsMap.size() > 0) {
            sink.putAscii(" hints[");
            boolean first = true;
            for (int i = 0, n = hintsMap.getKeyCount(); i < n; i++) {
                CharSequence hint = hintsMap.getKey(i);
                if (hint == null) {
                    continue;
                }
                if (!first) {
                    sink.putAscii(", ");
                }
                sink.put(hint);
                CharSequence params = hintsMap.valueAt(-i - 1);
                if (params != null) {
                    sink.putAscii("(");
                    sink.put(params);
                    sink.putAscii(")");
                }
                first = false;
            }
            sink.putAscii(']');
        }
    }

    // method to make debugging easier
    // not using toString name to prevent debugger from trying to use it on all model variables (because toSink0 can fail).
    @SuppressWarnings("unused")
    public String toString0() {
        StringSink sink = Misc.getThreadLocalSink();
        this.toSink0(sink, true);
        return sink.toString();
    }

    @Override
    public CharSequence translateAlias(CharSequence column) {
        return aliasToColumnNameMap.get(column);
    }

    private static void aliasToSink(CharSequence alias, CharSink<?> sink) {
        sink.putAscii(' ');
        boolean quote = !Chars.isQuoted(alias) && Chars.indexOf(alias, ' ') != -1;
        if (quote) {
            sink.putAscii('\'').put(alias).putAscii('\'');
        } else {
            sink.put(alias);
        }
    }

    private static void unitToSink(CharSink<?> sink, char timeUnit) {
        if (timeUnit != 0) {
            sink.putAscii(' ').putAscii(WindowExpression.timeUnitName(timeUnit));
        }
    }

    private String getSelectModelTypeText() {
        return modelTypeName.get(selectModelType);
    }

    private void sinkColumns(CharSink<?> sink, ObjList<QueryColumn> columns) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (i > 0) {
                sink.putAscii(", ");
            }
            QueryColumn column = columns.getQuick(i);
            CharSequence name = column.getName();
            CharSequence alias = column.getAlias();
            ExpressionNode ast = column.getAst();
            ast.toSink(sink);
            if (column.isWindowExpression() || name == null) {

                if (alias != null) {
                    aliasToSink(alias, sink);
                }

                // this can only be window column
                if (name != null) {
                    WindowExpression ac = (WindowExpression) column;
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
                            case WindowExpression.FRAMING_ROWS:
                                sink.putAscii(" rows");
                                break;
                            case WindowExpression.FRAMING_RANGE:
                                sink.putAscii(" range");
                                break;
                            case WindowExpression.FRAMING_GROUPS:
                                sink.putAscii(" groups");
                                break;
                            default:
                                break;
                        }
                        sink.put(" between ");
                        if (ac.getRowsLoExpr() != null) {
                            ac.getRowsLoExpr().toSink(sink);
                            if (ac.getFramingMode() == WindowExpression.FRAMING_RANGE) {
                                unitToSink(sink, ac.getRowsLoExprTimeUnit());
                            }

                            switch (ac.getRowsLoKind()) {
                                case WindowExpression.PRECEDING:
                                    sink.putAscii(" preceding");
                                    break;
                                case WindowExpression.FOLLOWING:
                                    sink.putAscii(" following");
                                    break;
                                default:
                                    break;
                            }
                        } else {
                            switch (ac.getRowsLoKind()) {
                                case WindowExpression.PRECEDING:
                                    sink.putAscii("unbounded preceding");
                                    break;
                                case WindowExpression.FOLLOWING:
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
                            if (ac.getFramingMode() == WindowExpression.FRAMING_RANGE) {
                                unitToSink(sink, ac.getRowsHiExprTimeUnit());
                            }

                            switch (ac.getRowsHiKind()) {
                                case WindowExpression.PRECEDING:
                                    sink.putAscii(" preceding");
                                    break;
                                case WindowExpression.FOLLOWING:
                                    sink.putAscii(" following");
                                    break;
                                default:
                                    assert false;
                                    break;
                            }
                        } else {
                            switch (ac.getRowsHiKind()) {
                                case WindowExpression.PRECEDING:
                                    sink.putAscii("unbounded preceding");
                                    break;
                                case WindowExpression.FOLLOWING:
                                    sink.putAscii("unbounded following");
                                    break;
                                default:
                                    // CURRENT
                                    sink.put("current row");
                                    break;
                            }
                        }

                        switch (ac.getExclusionKind()) {
                            case WindowExpression.EXCLUDE_CURRENT_ROW:
                                sink.putAscii(" exclude current row");
                                break;
                            case WindowExpression.EXCLUDE_GROUP:
                                sink.putAscii(" exclude group");
                                break;
                            case WindowExpression.EXCLUDE_TIES:
                                sink.putAscii(" exclude ties");
                                break;
                            case WindowExpression.EXCLUDE_NO_OTHERS:
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

    private void updateToSink(CharSink<?> sink) {
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
        modelTypeName.extendAndSet(SELECT_MODEL_WINDOW_JOIN, "select-window-join");
        modelTypeName.extendAndSet(SELECT_MODEL_HORIZON_JOIN, "select-horizon-join");
    }
}
