/*******************************************************************************
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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.OrderByMnemonic;
import io.questdb.griffin.SqlException;
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
import io.questdb.std.ObjectPool;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

import static io.questdb.griffin.SqlKeywords.isAndKeyword;
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
    public static final int JOIN_LEFT_OUTER = 2;
    public static final int JOIN_LT = 6;
    public static final int JOIN_MAX = JOIN_HORIZON;
    public static final int JOIN_NONE = 0;
    public static final int JOIN_RIGHT_OUTER = 9;
    public static final int JOIN_SPLICE = 5;
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
    public static final int SELECT_MODEL_NONE = 0;
    public static final int SELECT_MODEL_SHOW = 7;
    public static final int SELECT_MODEL_VIRTUAL = 2;
    public static final int SELECT_MODEL_WINDOW = 3;
    public static final int SELECT_MODEL_WINDOW_JOIN = 8;
    public static final int SELECT_MODEL_HORIZON_JOIN = 9;
    public static final int SET_OPERATION_EXCEPT = 2;
    public static final int SET_OPERATION_EXCEPT_ALL = 3;
    public static final int SET_OPERATION_INTERSECT = 4;
    public static final int SET_OPERATION_INTERSECT_ALL = 5;
    public static final int SET_OPERATION_UNION = 1;
    // types of set operations between this and union model
    public static final int SET_OPERATION_UNION_ALL = 0;
    public static final int SHOW_COLUMNS = 2;
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
    // Tracks next sequence number for alias generation to achieve O(1) amortized complexity
    private final LowerCaseCharSequenceIntHashMap aliasSequenceMap = new LowerCaseCharSequenceIntHashMap();
    private final LowerCaseCharSequenceObjHashMap<QueryColumn> aliasToColumnMap = new LowerCaseCharSequenceObjHashMap<>();
    private final LowerCaseCharSequenceObjHashMap<CharSequence> aliasToColumnNameMap = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjList<QueryColumn> bottomUpColumns = new ObjList<>();
    private final LowerCaseCharSequenceIntHashMap columnAliasIndexes = new LowerCaseCharSequenceIntHashMap();
    private final LowerCaseCharSequenceIntHashMap columnAliasRefCounts = new LowerCaseCharSequenceIntHashMap(8, 0.4, 0);
    private final LowerCaseCharSequenceObjHashMap<CharSequence> columnNameToAliasMap = new LowerCaseCharSequenceObjHashMap<>();
    private final LowerCaseCharSequenceObjHashMap<ExpressionNode> decls = new LowerCaseCharSequenceObjHashMap<>();
    private final IntHashSet dependencies = new IntHashSet();
    private final ObjList<ExpressionNode> expressionModels = new ObjList<>();
    private final ObjList<ExpressionNode> groupBy = new ObjList<>();
    private final LowerCaseCharSequenceObjHashMap<CharSequence> hintsMap = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjList<ExpressionNode> joinColumns = new ObjList<>(4);
    private final ObjList<QueryModel> joinModels = new ObjList<>();
    private final ObjList<ExpressionNode> latestBy = new ObjList<>();
    private final LowerCaseCharSequenceIntHashMap modelAliasIndexes = new LowerCaseCharSequenceIntHashMap();
    // Named window definitions from WINDOW clause (e.g., WINDOW w AS (PARTITION BY ...))
    private final LowerCaseCharSequenceObjHashMap<WindowExpression> namedWindows = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjList<ExpressionNode> orderBy = new ObjList<>();
    private final ObjList<ExpressionNode> orderByAdvice = new ObjList<>();
    private final IntList orderByDirection = new IntList();
    private final IntList orderByDirectionAdvice = new IntList();
    private final LowerCaseCharSequenceIntHashMap orderHash = new LowerCaseCharSequenceIntHashMap(4, 0.5, -1);
    private final IntList orderedJoinModels1 = new IntList();
    private final IntList orderedJoinModels2 = new IntList();
    private final LowerCaseCharSequenceHashSet overridableDecls = new LowerCaseCharSequenceHashSet();
    // collect frequency of column names from each join model
    // and check if any of columns with frequency > 0 are selected
    // column name frequency of 1 corresponds to map value 0
    // column name frequency of 0 corresponds to map value -1
    // list of "and" concatenated expressions
    private final ObjList<ExpressionNode> parsedWhere = new ObjList<>();
    private final IntHashSet parsedWhereConstants = new IntHashSet();
    private final ObjList<PivotForColumn> pivotForColumns = new ObjList<>();
    private final ObjList<QueryColumn> pivotGroupByColumns = new ObjList<>();
    private final ObjList<ViewDefinition> referencedViews = new ObjList<>();
    private final ObjList<ExpressionNode> sampleByFill = new ObjList<>();
    private final ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
    private final ObjList<QueryColumn> topDownColumns = new ObjList<>();
    private final LowerCaseCharSequenceHashSet topDownNameSet = new LowerCaseCharSequenceHashSet();
    private final ObjList<ExpressionNode> updateSetColumns = new ObjList<>();
    private final ObjList<CharSequence> updateTableColumnNames = new ObjList<>();
    private final IntList updateTableColumnTypes = new IntList();
    private final ObjList<CharSequence> wildcardColumnNames = new ObjList<>();
    private final HorizonJoinContext horizonJoinContext = new HorizonJoinContext();
    private final WindowJoinContext windowJoinContext = new WindowJoinContext();
    private final LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauseModel = new LowerCaseCharSequenceObjHashMap<>();
    // used for the parallel sample by rewrite. In the future, if we deprecate original SAMPLE BY, then these will
    // be the only fields for these values.
    private ExpressionNode alias;
    // used to block pushing down of order by advice to lower model
    // this is used for negative limits optimisations
    private boolean allowPropagationOfOrderByAdvice = true;
    private boolean artificialStar;
    private ExpressionNode asOfJoinTolerance = null;
    // Used to store a deep copy of the whereClause field
    // since whereClause can be changed during optimization/generation stage.
    private ExpressionNode backupWhereClause;
    private boolean cacheable = true;
    // where clause expressions that do not reference any tables, not necessarily constants
    private ExpressionNode constWhereClause;
    private JoinContext context;
    private boolean distinct = false;
    private boolean explicitTimestamp;
    private ExpressionNode fillFrom;
    private ExpressionNode fillStride;
    private ExpressionNode fillTo;
    private ObjList<ExpressionNode> fillValues;
    private boolean forceBackwardScan;
    private boolean isCteModel;
    // A flag to mark intermediate SELECT translation models. Such models do not contain the full list of selected
    // columns (e.g. they lack virtual columns), so they should be skipped when rewriting positional ORDER BY.
    private boolean isSelectTranslation = false;
    private boolean isUpdateModel;
    private ExpressionNode joinCriteria;
    private int joinKeywordPosition;
    private int joinType = JOIN_NONE;
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
    private boolean orderDescendingByDesignatedTimestampOnly;
    private IntList orderedJoinModels = orderedJoinModels2;
    private ExpressionNode originatingViewNameExpr;
    // Expression clause that is actually part of left/outer join but not in join model.
    // Inner join expressions
    private ExpressionNode outerJoinExpressionClause;
    private boolean pivotGroupByColumnHasNoAlias = false;
    private ExpressionNode postJoinWhereClause;
    private ExpressionNode sampleBy;
    private ExpressionNode sampleByFrom;
    private ExpressionNode sampleByOffset = ZERO_OFFSET;
    private ExpressionNode sampleByTimezoneName = null;
    private ExpressionNode sampleByTo;
    private ExpressionNode sampleByUnit;
    private int selectModelType = SELECT_MODEL_NONE;
    private int setOperationType;
    private int showKind = -1;
    private boolean skipped;
    private int tableId = -1;
    private ExpressionNode tableNameExpr;
    private RecordCursorFactory tableNameFunction;
    private ExpressionNode timestamp;
    private CharSequence timestampOffsetAlias;  // The alias name for the transformed timestamp (e.g., "ts")
    // Timestamp offset information for virtual models where timestamp is computed via dateadd.
    // Used to enable timestamp predicate pushdown with appropriate offset adjustment.
    // NOTE: The optimizer intrinsically understands dateadd(char, int, timestamp) and pushes
    // predicates through it. The offset type must match dateadd's signature (int).
    // See TimestampAddFunctionFactory for the function definition.
    private char timestampOffsetUnit;           // 'h', 'd', 'm', 's', etc. (0 means no offset)
    private int timestampOffsetValue;           // The offset value (inverse, e.g., +1 for dateadd -1)
    private CharSequence timestampSourceColumn; // The original column name before dateadd transformation
    private int timestampColumnIndex = -1;      // Index of the timestamp column in virtual models (-1 means not set)
    private QueryModel unionModel;
    private QueryModel updateTableModel;
    private TableToken updateTableToken;
    private ExpressionNode viewNameExpr;
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

    // Window join must be the last join in the query; no other join types can follow it.
    // This constraint is enforced at the SQL parser stage.
    public static boolean isWindowJoin(QueryModel model) {
        if (model == null) {
            return false;
        }
        ObjList<QueryModel> ms = model.getJoinModels();
        return ms.size() > 1 && ms.get(ms.size() - 1).getJoinType() == JOIN_WINDOW;
    }

    // Horizon join must be the only join in the query level; no other join types can be combined with it.
    // This constraint is enforced at the SQL parser stage.
    public static boolean isHorizonJoin(QueryModel model) {
        if (model == null) {
            return false;
        }
        ObjList<QueryModel> ms = model.getJoinModels();
        return ms.size() > 1 && ms.get(ms.size() - 1).getJoinType() == JOIN_HORIZON;
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

    public void addDependency(int index) {
        dependencies.add(index);
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

    public void addPivotForColumn(PivotForColumn column) {
        pivotForColumns.add(column);
    }

    public void addPivotGroupByColumn(QueryColumn column) {
        pivotGroupByColumns.add(column);
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
        backupWhereClause = null;
        constWhereClause = null;
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
        timestamp = null;
        timestampOffsetUnit = 0;
        timestampOffsetValue = 0;
        timestampSourceColumn = null;
        timestampOffsetAlias = null;
        timestampColumnIndex = -1;
        sqlNodeStack.clear();
        joinColumns.clear();
        withClauseModel.clear();
        namedWindows.clear();
        selectModelType = SELECT_MODEL_NONE;
        columnNameToAliasMap.clear();
        tableNameFunction = null;
        tableId = -1;
        metadataVersion = -1;
        wildcardColumnNames.clear();
        expressionModels.clear();
        distinct = false;
        nestedModelIsSubQuery = false;
        unionModel = null;
        orderHash.clear();
        modelPosition = 0;
        topDownColumns.clear();
        topDownNameSet.clear();
        aliasToColumnMap.clear();
        aliasSequenceMap.clear();
        // TODO: replace booleans with an enum-like type: UPDATE/MAT_VIEW/INSERT_AS_SELECT/SELECT
        //  default is SELECT
        isUpdateModel = false;
        isCteModel = false;
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
        sampleByOffset = ZERO_OFFSET;
        sampleByTo = null;
        sampleByFrom = null;
        fillFrom = null;
        fillTo = null;
        fillStride = null;
        fillValues = null;
        skipped = false;
        allowPropagationOfOrderByAdvice = true;
        decls.clear();
        overridableDecls.clear();
        orderDescendingByDesignatedTimestampOnly = false;
        forceBackwardScan = false;
        hintsMap.clear();
        asOfJoinTolerance = null;
        horizonJoinContext.clear();
        windowJoinContext.clear();
        pivotGroupByColumns.clear();
        pivotForColumns.clear();
        cacheable = true;
        pivotGroupByColumnHasNoAlias = false;
        referencedViews.clear();
        columnAliasRefCounts.clear();
    }

    public void clearColumnMapStructs() {
        this.aliasToColumnNameMap.clear();
        this.wildcardColumnNames.clear();
        this.aliasToColumnMap.clear();
        this.bottomUpColumns.clear();
        this.columnAliasIndexes.clear();
        this.columnNameToAliasMap.clear();
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
            aliasToColumnMap.put(alias, qc);
        }
        ObjList<CharSequence> columnNames = other.wildcardColumnNames;
        this.wildcardColumnNames.addAll(columnNames);
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            final CharSequence name = columnNames.getQuick(i);
            this.aliasToColumnNameMap.put(name, name);
        }
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

    public void copyHints(LowerCaseCharSequenceObjHashMap<CharSequence> hints) {
        // do not copy hints to self
        if (hintsMap != hints) {
            this.hintsMap.putAll(hints);
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

    public LowerCaseCharSequenceIntHashMap getAliasSequenceMap() {
        return aliasSequenceMap;
    }

    public LowerCaseCharSequenceObjHashMap<QueryColumn> getAliasToColumnMap() {
        return aliasToColumnMap;
    }

    public LowerCaseCharSequenceObjHashMap<CharSequence> getAliasToColumnNameMap() {
        return aliasToColumnNameMap;
    }

    public boolean getAllowPropagationOfOrderByAdvice() {
        return allowPropagationOfOrderByAdvice;
    }

    @Nullable
    public ExpressionNode getAsOfJoinTolerance() {
        return asOfJoinTolerance;
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

    public LowerCaseCharSequenceObjHashMap<ExpressionNode> getDecls() {
        return decls;
    }

    public IntHashSet getDependencies() {
        return dependencies;
    }

    public ObjList<ExpressionNode> getExpressionModels() {
        return expressionModels;
    }

    public ExpressionNode getFillFrom() {
        return fillFrom;
    }

    public ExpressionNode getFillStride() {
        return fillStride;
    }

    public ExpressionNode getFillTo() {
        return fillTo;
    }

    public ObjList<ExpressionNode> getFillValues() {
        return fillValues;
    }

    public ObjList<ExpressionNode> getGroupBy() {
        return groupBy;
    }

    @NotNull
    public LowerCaseCharSequenceObjHashMap<CharSequence> getHints() {
        return hintsMap;
    }

    public ObjList<ExpressionNode> getJoinColumns() {
        return joinColumns;
    }

    public JoinContext getJoinContext() {
        return context;
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

    public int getModelAliasIndex(CharSequence modelAlias, int start, int end) {
        if (modelAlias.charAt(start) == '"' && modelAlias.charAt(end - 1) == '"') {
            start++;
            end--;
        }
        int index = modelAliasIndexes.keyIndex(modelAlias, start, end);
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

    public LowerCaseCharSequenceObjHashMap<WindowExpression> getNamedWindows() {
        return namedWindows;
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

    public ExpressionNode getOriginatingViewNameExpr() {
        return originatingViewNameExpr;
    }

    public ExpressionNode getOuterJoinExpressionClause() {
        return outerJoinExpressionClause;
    }

    public LowerCaseCharSequenceHashSet getOverridableDecls() {
        return overridableDecls;
    }

    public ObjList<ExpressionNode> getParsedWhere() {
        return parsedWhere;
    }

    public ObjList<PivotForColumn> getPivotForColumns() {
        return pivotForColumns;
    }

    public ObjList<QueryColumn> getPivotGroupByColumns() {
        return pivotGroupByColumns;
    }

    public ExpressionNode getPostJoinWhereClause() {
        return postJoinWhereClause;
    }

    @Override
    public QueryModel getQueryModel() {
        return this;
    }

    public int getRefCount(CharSequence alias) {
        return columnAliasRefCounts.get(alias);
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

    public int getShowKind() {
        return showKind;
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

    public RecordCursorFactory getTableNameFunction() {
        return tableNameFunction;
    }

    public ExpressionNode getTimestamp() {
        return timestamp;
    }

    public CharSequence getTimestampOffsetAlias() {
        return timestampOffsetAlias;
    }

    public char getTimestampOffsetUnit() {
        return timestampOffsetUnit;
    }

    public int getTimestampOffsetValue() {
        return timestampOffsetValue;
    }

    public CharSequence getTimestampSourceColumn() {
        return timestampSourceColumn;
    }

    public boolean hasTimestampOffset() {
        return timestampOffsetUnit != 0;
    }

    public int getTimestampColumnIndex() {
        return timestampColumnIndex;
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
        return updateTableModel != null ? updateTableModel.getUpdateTableColumnNames() : updateTableColumnNames;
    }

    public IntList getUpdateTableColumnTypes() {
        return updateTableModel != null ? updateTableModel.getUpdateTableColumnTypes() : updateTableColumnTypes;
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

    public ObjList<CharSequence> getWildcardColumnNames() {
        return wildcardColumnNames;
    }

    public HorizonJoinContext getHorizonJoinContext() {
        return horizonJoinContext;
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

    public void incrementColumnRefCount(CharSequence alias, int refCount) {
        if (columnAliasIndexes.get(alias) > -1) {
            int keyIndex = columnAliasRefCounts.keyIndex(alias);
            if (keyIndex < 0) {
                int old = columnAliasRefCounts.valueAt(keyIndex);
                columnAliasRefCounts.putAt(keyIndex, alias, old + refCount);
            } else {
                columnAliasRefCounts.putAt(keyIndex, alias, refCount);
            }
        }
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

    public boolean isCteModel() {
        return isCteModel;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public boolean isExplicitTimestamp() {
        return explicitTimestamp;
    }

    public boolean isForceBackwardScan() {
        return forceBackwardScan;
    }

    public boolean isNestedModelIsSubQuery() {
        return nestedModelIsSubQuery;
    }

    public boolean isOrderDescendingByDesignatedTimestampOnly() {
        return orderDescendingByDesignatedTimestampOnly;
    }

    public boolean isPivot() {
        return pivotForColumns.size() > 0;
    }

    public boolean isPivotGroupByColumnHasNoAlias() {
        return pivotGroupByColumnHasNoAlias;
    }

    public boolean isSelectTranslation() {
        return isSelectTranslation;
    }

    public boolean isSkipped() {
        return skipped;
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

    /**
     * The goal of this method is to dismiss the baseModel as
     * the layer between this and the baseModel is referencing. We do that by copying ASTs from
     * the baseModel onto the current baseModel and also maintaining all the maps in sync.
     * <p>
     * The caller is responsible for checking if baseModel is suitable for the removal. E.g. it does not
     * contain arithmetic expressions. Although this method does not validate if baseModel has arithmetic.
     *
     * @param baseModel baseModel containing columns mapped into the referenced baseModel.
     */
    public void mergePartially(QueryModel baseModel, ObjectPool<QueryColumn> queryColumnPool) {
        for (int i = 0, n = bottomUpColumns.size(); i < n; i++) {
            QueryColumn thisColumn = bottomUpColumns.getQuick(i);
            if (thisColumn.getAst().type == ExpressionNode.LITERAL) {
                QueryColumn thatColumn = baseModel.getAliasToColumnMap().get(thisColumn.getAst().token);
                // skip if baseModel does not have this column
                if (thatColumn == null) {
                    continue;
                }
                // We cannot mutate the column on this baseModel, because columns might be shared between
                // models. The bottomUpColumns are also referenced by `aliasToColumnMap`. Typically,
                // `thisColumn` alias should let us lookup, the column's reference
                QueryColumn col = queryColumnPool.next();
                col.of(thisColumn.getAlias(), thatColumn.getAst());
                bottomUpColumns.setQuick(i, col);

                int index = aliasToColumnMap.keyIndex(thisColumn.getAlias());
                assert index < 0;
                final CharSequence immutableAlias = aliasToColumnMap.keyAt(index);
                aliasToColumnMap.putAt(index, immutableAlias, col);
                // maintain sync between alias and column name
                aliasToColumnNameMap.put(immutableAlias, thatColumn.getAst().token);
            }
        }

        if (baseModel.getOrderBy().size() > 0) {
            assert getOrderBy().size() == 0;
            for (int i = 0, n = baseModel.getOrderBy().size(); i < n; i++) {
                addOrderBy(baseModel.getOrderBy().getQuick(i), baseModel.getOrderByDirection().getQuick(i));
            }
            baseModel.getOrderBy().clear();
            baseModel.getOrderByDirection().clear();
        }

        // If baseModel has limits, the outer baseModel must not have different limits.
        // We are merging models affecting "select" clause and not the row count.
        if (baseModel.limitLo != null || baseModel.limitHi != null) {
            limitLo = baseModel.limitLo;
            limitHi = baseModel.limitHi;
            limitPosition = baseModel.limitPosition;
            limitAdviceLo = baseModel.limitAdviceLo;
            limitAdviceHi = baseModel.limitAdviceHi;
        }
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

    public void moveOrderByFrom(QueryModel model) {
        orderBy.addAll(model.getOrderBy());
        orderByDirection.addAll(model.getOrderByDirection());
        model.clearOrderBy();
    }

    public void moveSampleByFrom(QueryModel model) {
        this.sampleBy = model.sampleBy;
        this.sampleByUnit = model.sampleByUnit;
        this.sampleByFill.clear();
        this.sampleByFill.addAll(model.sampleByFill);
        this.sampleByTimezoneName = model.sampleByTimezoneName;
        this.sampleByOffset = model.sampleByOffset;
        this.sampleByTo = model.sampleByTo;
        this.sampleByFrom = model.sampleByFrom;

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
     * @return non-current order list.
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

    public void recordViews(LowerCaseCharSequenceObjHashMap<ViewDefinition> viewDefinitions) {
        final ObjList<CharSequence> keys = viewDefinitions.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final ViewDefinition viewDefinition = viewDefinitions.get(keys.getQuick(i));
            if (!referencedViews.contains(viewDefinition)) {
                referencedViews.add(viewDefinition);
            }
        }
    }

    public void recordViews(ObjList<ViewDefinition> viewDefinitions) {
        for (int i = 0, n = viewDefinitions.size(); i < n; i++) {
            final ViewDefinition viewDefinition = viewDefinitions.getQuick(i);
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

    public void removeDependency(int index) {
        dependencies.remove(index);
    }

    public void replaceColumn(int columnIndex, QueryColumn newColumn) {
        if (topDownColumns.size() > 0) {
            topDownColumns.setQuick(columnIndex, newColumn);
        } else {
            bottomUpColumns.setQuick(columnIndex, newColumn);
        }
    }

    public void replaceColumnNameMap(CharSequence alias, CharSequence oldToken, CharSequence newToken) {
        aliasToColumnNameMap.put(alias, newToken);
        columnNameToAliasMap.remove(oldToken);
        columnNameToAliasMap.put(newToken, alias);
    }

    public void replaceJoinModel(int pos, QueryModel model) {
        joinModels.setQuick(pos, model);
    }

    public void setAlias(ExpressionNode alias) {
        this.alias = alias;
    }

    public void setAllowPropagationOfOrderByAdvice(boolean value) {
        allowPropagationOfOrderByAdvice = value;
    }

    public void setArtificialStar(boolean artificialStar) {
        this.artificialStar = artificialStar;
    }

    public void setAsOfJoinTolerance(ExpressionNode asOfJoinTolerance) {
        this.asOfJoinTolerance = asOfJoinTolerance;
    }

    public void setBackupWhereClause(ExpressionNode backupWhereClause) {
        this.backupWhereClause = backupWhereClause;
    }

    public void setCacheable(boolean b) {
        cacheable = b;
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

    public void setFillFrom(ExpressionNode fillFrom) {
        this.fillFrom = fillFrom;
    }

    public void setFillStride(ExpressionNode fillStride) {
        this.fillStride = fillStride;
    }

    public void setFillTo(ExpressionNode fillTo) {
        this.fillTo = fillTo;
    }

    public void setFillValues(ObjList<ExpressionNode> fillValues) {
        this.fillValues = fillValues;
    }

    public void setForceBackwardScan(boolean forceBackwardScan) {
        this.forceBackwardScan = forceBackwardScan;
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

    public void setLimitAdvice(ExpressionNode lo, ExpressionNode hi) {
        this.limitAdviceLo = lo;
        this.limitAdviceHi = hi;
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

    public void setOrderByAdviceMnemonic(int orderByAdviceMnemonic) {
        this.orderByAdviceMnemonic = orderByAdviceMnemonic;
    }

    public void setOrderByPosition(int orderByPosition) {
        this.orderByPosition = orderByPosition;
    }

    public void setOrderDescendingByDesignatedTimestampOnly(boolean orderDescendingByDesignatedTimestampOnly) {
        this.orderDescendingByDesignatedTimestampOnly = orderDescendingByDesignatedTimestampOnly;
    }

    public void setOrderedJoinModels(IntList that) {
        assert that == orderedJoinModels1 || that == orderedJoinModels2;
        this.orderedJoinModels = that;
    }

    public void setOriginatingViewNameExpr(ExpressionNode originatingViewNameExpr) {
        this.originatingViewNameExpr = originatingViewNameExpr;
    }

    public void setOuterJoinExpressionClause(ExpressionNode outerJoinExpressionClause) {
        this.outerJoinExpressionClause = outerJoinExpressionClause;
    }

    public void setPivotGroupByColumnHasNoAlias(boolean pivotGroupByColumnHasNoAlias) {
        this.pivotGroupByColumnHasNoAlias = pivotGroupByColumnHasNoAlias;
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

    public void setSelectTranslation(boolean isSelectTranslation) {
        this.isSelectTranslation = isSelectTranslation;
    }

    public void setSetOperationType(int setOperationType) {
        this.setOperationType = setOperationType;
    }

    public void setShowKind(int showKind) {
        this.showKind = showKind;
    }

    public void setSkipped(boolean skipped) {
        this.skipped = skipped;
    }

    public void setTableId(int id) {
        this.tableId = id;
    }

    public void setTableNameExpr(ExpressionNode tableNameExpr) {
        this.tableNameExpr = tableNameExpr;
    }

    public void setTableNameFunction(RecordCursorFactory function) {
        this.tableNameFunction = function;
    }

    public void setTimestamp(ExpressionNode timestamp) {
        this.timestamp = timestamp;
    }

    public void setTimestampOffsetAlias(CharSequence alias) {
        this.timestampOffsetAlias = alias;
    }

    public void setTimestampOffsetUnit(char unit) {
        this.timestampOffsetUnit = unit;
    }

    public void setTimestampOffsetValue(int value) {
        this.timestampOffsetValue = value;
    }

    public void setTimestampSourceColumn(CharSequence col) {
        this.timestampSourceColumn = col;
    }

    public void setTimestampColumnIndex(int index) {
        this.timestampColumnIndex = index;
    }

    public void setUnionModel(QueryModel unionModel) {
        this.unionModel = unionModel;
        if (unionModel != null && viewNameExpr != null) {
            unionModel.setViewNameExpr(viewNameExpr);
        }
    }

    public void setUpdateTableToken(TableToken tableName) {
        this.updateTableToken = tableName;
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

    public void updateColumnAliasIndexes() {
        columnAliasIndexes.clear();
        for (int i = 0, n = bottomUpColumns.size(); i < n; i++) {
            columnAliasIndexes.put(wildcardColumnNames.getQuick(i), i);
        }
    }

    public boolean windowStopPropagate() {
        if (selectModelType != SELECT_MODEL_WINDOW) {
            return false;
        }
        for (int i = 0, size = getColumns().size(); i < size; i++) {
            QueryColumn column = getColumns().getQuick(i);
            if (column.isWindowExpression() && ((WindowExpression) column).stopOrderByPropagate(getOrderBy(), getOrderByDirection())) {
                return true;
            }
        }
        return false;
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
        switch (timeUnit) {
            case 0:
                break;
            case WindowExpression.TIME_UNIT_NANOSECOND:
                sink.putAscii(" nanosecond");
                break;
            case WindowExpression.TIME_UNIT_MICROSECOND:
                sink.putAscii(" microsecond");
                break;
            case WindowExpression.TIME_UNIT_MILLISECOND:
                sink.putAscii(" millisecond");
                break;
            case WindowExpression.TIME_UNIT_SECOND:
                sink.putAscii(" second");
                break;
            case WindowExpression.TIME_UNIT_MINUTE:
                sink.putAscii(" minute");
                break;
            case WindowExpression.TIME_UNIT_HOUR:
                sink.putAscii(" hour");
                break;
            case WindowExpression.TIME_UNIT_DAY:
                sink.putAscii(" day");
                break;
            default:
                sink.putAscii(" [unknown unit]");
                break;
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

    // returns textual description of this model, e.g. select-choose [top-down-columns] bottom-up-columns from X ...
    private void toSink0(CharSink<?> sink, boolean joinSlave, boolean showOrderBy) {
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

            // Output timestamp offset info if present (for dateadd-transformed timestamps)
            if (hasTimestampOffset()) {
                sink.putAscii(" ts_offset ('");
                sink.putAscii(timestampOffsetUnit);
                sink.putAscii("', ");
                sink.put(timestampOffsetValue);
                sink.putAscii(", ");
                sink.put(timestampColumnIndex);
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
                            case JOIN_LEFT_OUTER:
                                sink.putAscii(" left join ");
                                break;
                            case JOIN_WINDOW:
                                sink.putAscii(" window join ");
                                break;
                            case JOIN_RIGHT_OUTER:
                                sink.putAscii(" right join ");
                                break;
                            case JOIN_FULL_OUTER:
                                sink.putAscii(" full join ");
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
                            case JOIN_HORIZON:
                                sink.putAscii(" horizon join ");
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

                        JoinContext jc = model.getJoinContext();
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

                        WindowJoinContext wjc = model.getWindowJoinContext();
                        if (model.joinType == JOIN_WINDOW) {
                            sink.put(" between ");
                            if (wjc.getLoExpr() != null) {
                                wjc.getLoExpr().toSink(sink);
                                unitToSink(sink, wjc.getLoExprTimeUnit());
                                switch (wjc.getLoKind()) {
                                    case WindowJoinContext.PRECEDING:
                                        sink.putAscii(" preceding");
                                        break;
                                    case WindowJoinContext.FOLLOWING:
                                        sink.putAscii(" following");
                                        break;
                                    default:
                                        sink.putAscii("current row");
                                        break;
                                }
                            } else {
                                switch (wjc.getLoKind()) {
                                    case WindowJoinContext.PRECEDING:
                                        sink.putAscii("unbounded preceding");
                                        break;
                                    case WindowJoinContext.FOLLOWING:
                                        sink.putAscii("unbounded following");
                                        break;
                                    default:
                                        sink.putAscii("current row");
                                        break;
                                }
                            }
                            sink.putAscii(" and ");

                            if (wjc.getHiExpr() != null) {
                                wjc.getHiExpr().toSink(sink);
                                unitToSink(sink, wjc.getHiExprTimeUnit());
                                switch (wjc.getHiKind()) {
                                    case WindowJoinContext.PRECEDING:
                                        sink.putAscii(" preceding");
                                        break;
                                    case WindowJoinContext.FOLLOWING:
                                        sink.putAscii(" following");
                                        break;
                                    default:
                                        sink.putAscii("current row");
                                        break;
                                }
                            } else {
                                switch (wjc.getHiKind()) {
                                    case WindowJoinContext.PRECEDING:
                                        sink.putAscii("unbounded preceding");
                                        break;
                                    case WindowJoinContext.FOLLOWING:
                                        sink.putAscii("unbounded following");
                                        break;
                                    default:
                                        sink.put("current row");
                                        break;
                                }
                            }

                            if (wjc.isIncludePrevailing()) {
                                sink.putAscii(" include prevailing");
                            } else {
                                sink.putAscii(" exclude prevailing");
                            }
                        }

                        HorizonJoinContext hjc = model.getHorizonJoinContext();
                        if (hjc.getMode() != HorizonJoinContext.MODE_NONE) {
                            if (hjc.getMode() == HorizonJoinContext.MODE_RANGE) {
                                sink.putAscii(" range from ");
                                hjc.getRangeFrom().toSink(sink);
                                sink.putAscii(" to ");
                                hjc.getRangeTo().toSink(sink);
                                sink.putAscii(" step ");
                                hjc.getRangeStep().toSink(sink);
                            } else if (hjc.getMode() == HorizonJoinContext.MODE_LIST) {
                                sink.putAscii(" list (");
                                ObjList<ExpressionNode> offsets = hjc.getListOffsets();
                                for (int k = 0, z = offsets.size(); k < z; k++) {
                                    if (k > 0) {
                                        sink.putAscii(", ");
                                    }
                                    offsets.getQuick(k).toSink(sink);
                                }
                                sink.putAscii(")");
                            }
                            if (hjc.getAlias() != null) {
                                sink.putAscii(" as ");
                                hjc.getAlias().toSink(sink);
                            }
                        }

                        if (model.asOfJoinTolerance != null) {
                            assert model.joinType == JOIN_ASOF;
                            sink.putAscii(" tolerance ");
                            model.asOfJoinTolerance.toSink(sink);
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

        if (fillValues != null && fillValues.size() > 0) {
            sink.putAscii(" fill(");
            for (int i = 0, n = fillValues.size(); i < n; i++) {
                if (i > 0) {
                    sink.put(',');
                }
                sink.put(fillValues.getQuick(i));
            }
            sink.put(')');
        }

        if (fillFrom != null || fillTo != null) {
            if (fillFrom != null) {
                sink.putAscii(" from ");
                sink.put(fillFrom);
            }
            if (fillTo != null) {
                sink.putAscii(" to ");
                sink.put(fillTo);
            }
        }

        if (fillStride != null) {
            sink.putAscii(" stride ");
            sink.put(fillStride);
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
