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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.OrderByMnemonic;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.CharSink;
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
public class QueryModel implements IQueryModel {
    public static final QueryModelFactory FACTORY = new QueryModelFactory();
    private static final ObjList<String> modelTypeName = new ObjList<>();
    // Tracks next sequence number for alias generation to achieve O(1) amortized complexity
    private final LowerCaseCharSequenceIntHashMap aliasSequenceMap = new LowerCaseCharSequenceIntHashMap();
    private final LowerCaseCharSequenceObjHashMap<QueryColumn> aliasToColumnMap = new LowerCaseCharSequenceObjHashMap<>();
    private final LowerCaseCharSequenceObjHashMap<CharSequence> aliasToColumnNameMap = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjList<QueryColumn> bottomUpColumns = new ObjList<>();
    private final LowerCaseCharSequenceIntHashMap columnAliasIndexes = new LowerCaseCharSequenceIntHashMap();
    private final LowerCaseCharSequenceIntHashMap columnAliasRefCounts = new LowerCaseCharSequenceIntHashMap(8, 0.4, 0);
    private final LowerCaseCharSequenceObjHashMap<CharSequence> columnNameToAliasMap = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjList<LowerCaseCharSequenceIntHashMap> correlatedColumns = new ObjList<>();
    private final IntIntHashMap correlatedDepths = new IntIntHashMap();
    private final LowerCaseCharSequenceObjHashMap<ExpressionNode> decls = new LowerCaseCharSequenceObjHashMap<>();
    private final IntHashSet dependencies = new IntHashSet();
    private final ObjList<ExpressionNode> expressionModels = new ObjList<>();
    private final ObjList<ExpressionNode> groupBy = new ObjList<>();
    private final LowerCaseCharSequenceObjHashMap<CharSequence> hintsMap = new LowerCaseCharSequenceObjHashMap<>();
    private final HorizonJoinContext horizonJoinContext = new HorizonJoinContext();
    private final ObjList<ExpressionNode> joinColumns = new ObjList<>(4);
    private final ObjList<IQueryModel> joinModels = new ObjList<>();
    private final ObjList<CharSequence> lateralCountColumns = new ObjList<>();
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
    private final ObjList<QueryModelWrapper> sharedRefs = new ObjList<>();
    private final ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
    private final ObjList<QueryColumn> topDownColumns = new ObjList<>();
    private final LowerCaseCharSequenceHashSet topDownNameSet = new LowerCaseCharSequenceHashSet();
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
    private IQueryModel nestedModel;
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
    private int sharedRefByParentCount = 0;
    private int showKind = -1;
    private boolean skipped;
    private boolean standaloneUnnest;
    private int tableId = -1;
    private ExpressionNode tableNameExpr;
    private RecordCursorFactory tableNameFunction;
    private ExpressionNode timestamp;
    private int timestampColumnIndex = -1;      // Index of the timestamp column in virtual models (-1 means not set)
    private CharSequence timestampOffsetAlias;  // The alias name for the transformed timestamp (e.g., "ts")
    // Timestamp offset information for virtual models where timestamp is computed via dateadd.
    // Used to enable timestamp predicate pushdown with appropriate offset adjustment.
    // NOTE: The optimizer intrinsically understands dateadd(char, int, timestamp) and pushes
    // predicates through it. The offset type must match dateadd's signature (int).
    // See TimestampAddFunctionFactory for the function definition.
    private char timestampOffsetUnit;           // 'h', 'd', 'm', 's', etc. (0 means no offset)
    private int timestampOffsetValue;           // The offset value (inverse, e.g., +1 for dateadd -1)
    private CharSequence timestampSourceColumn; // The original column name before dateadd transformation
    private IQueryModel unionModel;
    private boolean unnestOrdinality;
    private IQueryModel updateTableModel;
    private TableToken updateTableToken;
    private ExpressionNode viewNameExpr;
    private ExpressionNode whereClause;

    private QueryModel() {
        joinModels.add(this);
    }

    @Override
    public void addBottomUpColumn(QueryColumn column) throws SqlException {
        addBottomUpColumn(0, column, false, null);
    }

    @Override
    public void addBottomUpColumn(QueryColumn column, boolean allowDuplicates) throws SqlException {
        addBottomUpColumn(0, column, allowDuplicates, null);
    }

    @Override
    public void addBottomUpColumn(int position, QueryColumn column, boolean allowDuplicates) throws SqlException {
        addBottomUpColumn(position, column, allowDuplicates, null);
    }

    @Override
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

    @Override
    public void addBottomUpColumnIfNotExists(QueryColumn column) {
        if (addField(column)) {
            bottomUpColumns.add(column);
        }
    }

    @Override
    public void addDependency(int index) {
        dependencies.add(index);
    }

    @Override
    public void addExpressionModel(ExpressionNode node) {
        assert node.queryModel != null;
        expressionModels.add(node);
    }

    @Override
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

    @Override
    public void addGroupBy(ExpressionNode node) {
        groupBy.add(node);
    }

    @Override
    public void addHint(CharSequence key, CharSequence value) {
        hintsMap.put(key, value);
    }

    @Override
    public void addJoinColumn(ExpressionNode node) {
        joinColumns.add(node);
    }

    @Override
    public void addJoinModel(IQueryModel joinModel) {
        joinModels.add(joinModel);
        if (joinModel != null && viewNameExpr != null) {
            joinModel.setViewNameExpr(viewNameExpr);
        }
    }

    @Override
    public void addLatestBy(ExpressionNode latestBy) {
        this.latestBy.add(latestBy);
    }

    @Override
    public boolean addModelAliasIndex(ExpressionNode node, int index) {
        return modelAliasIndexes.put(node.token, index);
    }

    @Override
    public void addOrderBy(ExpressionNode node, int direction) {
        orderBy.add(node);
        orderByDirection.add(direction);
    }

    @Override
    public void addParsedWhereNode(ExpressionNode node, boolean innerPredicate) {
        node.innerPredicate = innerPredicate;
        parsedWhere.add(node);
    }

    @Override
    public void addPivotForColumn(PivotForColumn column) {
        pivotForColumns.add(column);
    }

    @Override
    public void addPivotGroupByColumn(QueryColumn column) {
        pivotGroupByColumns.add(column);
    }

    @Override
    public void addSampleByFill(ExpressionNode sampleByFill) {
        this.sampleByFill.add(sampleByFill);
    }

    @Override
    public void addTopDownColumn(QueryColumn column, CharSequence alias) {
        if (topDownNameSet.add(alias)) {
            topDownColumns.add(column);
        }
    }

    @Override
    public void addUpdateTableColumnMetadata(int columnType, String columnName) {
        updateTableColumnTypes.add(columnType);
        updateTableColumnNames.add(columnName);
    }

    /**
     * Determines whether this model allows pushing columns from parent model(s).
     * If this is a UNION, EXCEPT or INTERSECT or contains a SELECT DISTINCT then it can't be done safely.
     */
    @Override
    public boolean allowsColumnsChange() {
        if (hasSharedRefs()) {
            return false;
        }
        IQueryModel union = this;
        while (union != null) {
            if (union.getSetOperationType() != IQueryModel.SET_OPERATION_UNION_ALL
                    || union.getSelectModelType() == IQueryModel.SELECT_MODEL_DISTINCT) {
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
    @Override
    public boolean allowsNestedColumnsChange() {
        return (nestedModel == null || !nestedModel.hasSharedRefs())
                && this.getSelectModelType() != IQueryModel.SELECT_MODEL_DISTINCT;
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
        lateralCountColumns.clear();
        updateSetColumns.clear();
        updateTableColumnTypes.clear();
        standaloneUnnest = false;
        unnestColumnAliases.clear();
        unnestExpressions.clear();
        unnestJsonColumnNames.clear();
        unnestJsonColumnTypes.clear();
        unnestOrdinality = false;
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
        correlatedDepths.clear();
        Misc.clearObjList(correlatedColumns);
        sharedRefs.clear();
        sharedRefByParentCount = 0;
    }

    @Override
    public void clearColumnMapStructs() {
        this.aliasToColumnNameMap.clear();
        this.wildcardColumnNames.clear();
        this.aliasToColumnMap.clear();
        this.bottomUpColumns.clear();
        this.columnAliasIndexes.clear();
        this.columnNameToAliasMap.clear();
    }

    @Override
    public void clearOrderBy() {
        orderBy.clear();
        orderByDirection.clear();
    }

    @Override
    public void clearSampleBy() {
        sampleBy = null;
        sampleByUnit = null;
        sampleByFill.clear();
        sampleByTimezoneName = null;
        sampleByOffset = null;
        sampleByTo = null;
        sampleByFrom = null;
    }

    @Override
    public void clearSharedRefs() {
        sharedRefs.clear();
    }

    @Override
    public boolean containsJoin() {
        IQueryModel current = this;
        do {
            if (current.getJoinModels().size() > 1) {
                return true;
            }
        } while ((current = current.getNestedModel()) != null);
        return false;
    }

    @Override
    public void copyBottomToTopColumns() {
        topDownColumns.clear();
        topDownNameSet.clear();
        for (int i = 0, n = bottomUpColumns.size(); i < n; i++) {
            QueryColumn column = bottomUpColumns.getQuick(i);
            addTopDownColumn(column, column.getAlias());
        }
    }

    @Override
    public void copyColumnsFrom(
            IQueryModel other,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<ExpressionNode> expressionNodePool
    ) {
        clearColumnMapStructs();

        // copy only literal columns and convert functions to literal while copying
        LowerCaseCharSequenceObjHashMap<QueryColumn> otherMap = other.getAliasToColumnMap();
        final ObjList<CharSequence> aliases = otherMap.keys();
        for (int i = 0, n = aliases.size(); i < n; i++) {
            final CharSequence alias = aliases.getQuick(i);
            QueryColumn qc = otherMap.get(alias);
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
        ObjList<CharSequence> columnNames = other.getWildcardColumnNames();
        this.wildcardColumnNames.addAll(columnNames);
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            final CharSequence name = columnNames.getQuick(i);
            this.aliasToColumnNameMap.put(name, name);
        }
    }

    @Override
    public void copyDeclsFrom(IQueryModel model, boolean overrideDeclares) throws SqlException {
        copyDeclsFrom(model.getDecls(), overrideDeclares);
    }

    @Override
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

    @Override
    public void copyHints(LowerCaseCharSequenceObjHashMap<CharSequence> hints) {
        // do not copy hints to self
        if (hintsMap != hints) {
            this.hintsMap.putAll(hints);
        }
    }

    @Override
    public void copyOrderByAdvice(ObjList<ExpressionNode> orderByAdvice) {
        this.orderByAdvice.clear();
        this.orderByAdvice.addAll(orderByAdvice);
    }

    @Override
    public void copyOrderByDirectionAdvice(IntList orderByDirection) {
        this.orderByDirectionAdvice.clear();
        this.orderByDirectionAdvice.addAll(orderByDirection);
    }

    @Override
    public void copySharedRefs(IQueryModel model) {
        sharedRefs.clear();
        for (int i = 0, n = model.getSharedRefs().size(); i < n; i++) {
            QueryModelWrapper wrapper = model.getSharedRefs().getQuick(i);
            wrapper.setDelegate(this);
        }
        sharedRefs.addAll(model.getSharedRefs());
        model.clearSharedRefs();
    }

    @Override
    public void copyUpdateTableMetadata(IQueryModel updateTableModel) {
        this.updateTableModel = updateTableModel;
        this.tableId = updateTableModel.getTableId();
        this.metadataVersion = updateTableModel.getMetadataVersion();
    }

    @Override
    public QueryColumn findBottomUpColumnByAst(ExpressionNode node) {
        for (int i = 0, n = bottomUpColumns.size(); i < n; i++) {
            QueryColumn qc = bottomUpColumns.getQuick(i);
            if (ExpressionNode.compareNodesExact(node, qc.getAst())) {
                return qc;
            }
        }
        return null;
    }

    @Override
    public ExpressionNode getAlias() {
        return alias;
    }

    @Override
    public LowerCaseCharSequenceIntHashMap getAliasSequenceMap() {
        return aliasSequenceMap;
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<QueryColumn> getAliasToColumnMap() {
        return aliasToColumnMap;
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<CharSequence> getAliasToColumnNameMap() {
        return aliasToColumnNameMap;
    }

    @Override
    public boolean getAllowPropagationOfOrderByAdvice() {
        return allowPropagationOfOrderByAdvice;
    }

    @Nullable
    @Override
    public ExpressionNode getAsOfJoinTolerance() {
        return asOfJoinTolerance;
    }

    @Override
    public ExpressionNode getBackupWhereClause() {
        return backupWhereClause;
    }

    @Override
    public ObjList<QueryColumn> getBottomUpColumns() {
        return bottomUpColumns;
    }

    @Override
    public int getColumnAliasIndex(CharSequence alias) {
        return columnAliasIndexes.get(alias);
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<CharSequence> getColumnNameToAliasMap() {
        return columnNameToAliasMap;
    }

    @Override
    public ObjList<QueryColumn> getColumns() {
        return topDownColumns.size() > 0 ? topDownColumns : bottomUpColumns;
    }

    @Override
    public ExpressionNode getConstWhereClause() {
        return constWhereClause;
    }

    @Override
    public ObjList<LowerCaseCharSequenceIntHashMap> getCorrelatedColumns() {
        return correlatedColumns;
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<ExpressionNode> getDecls() {
        return decls;
    }

    @Override
    public IntHashSet getDependencies() {
        return dependencies;
    }

    @Override
    public ObjList<ExpressionNode> getExpressionModels() {
        return expressionModels;
    }

    @Override
    public ExpressionNode getFillFrom() {
        return fillFrom;
    }

    @Override
    public ExpressionNode getFillStride() {
        return fillStride;
    }

    @Override
    public ExpressionNode getFillTo() {
        return fillTo;
    }

    @Override
    public ObjList<ExpressionNode> getFillValues() {
        return fillValues;
    }

    @Override
    public ObjList<ExpressionNode> getGroupBy() {
        return groupBy;
    }

    @NotNull
    @Override
    public LowerCaseCharSequenceObjHashMap<CharSequence> getHints() {
        return hintsMap;
    }

    @Override
    public HorizonJoinContext getHorizonJoinContext() {
        return horizonJoinContext;
    }

    @Override
    public ObjList<ExpressionNode> getJoinColumns() {
        return joinColumns;
    }

    @Override
    public JoinContext getJoinContext() {
        return context;
    }

    @Override
    public ExpressionNode getJoinCriteria() {
        return joinCriteria;
    }

    @Override
    public int getJoinKeywordPosition() {
        return joinKeywordPosition;
    }

    @Override
    public ObjList<IQueryModel> getJoinModels() {
        return joinModels;
    }

    @Override
    public int getJoinType() {
        return joinType;
    }

    @Override
    public ObjList<CharSequence> getLateralCountColumns() {
        return lateralCountColumns;
    }

    @Override
    public ObjList<ExpressionNode> getLatestBy() {
        return latestBy;
    }

    @Override
    public int getLatestByType() {
        return latestByType;
    }

    @Override
    public ExpressionNode getLimitAdviceHi() {
        return limitAdviceHi;
    }

    @Override
    public ExpressionNode getLimitAdviceLo() {
        return limitAdviceLo;
    }

    @Override
    public ExpressionNode getLimitHi() {
        return limitHi;
    }

    @Override
    public ExpressionNode getLimitLo() {
        return limitLo;
    }

    @Override
    public int getLimitPosition() {
        return limitPosition;
    }

    @Override
    public long getMetadataVersion() {
        return metadataVersion;
    }

    @Override
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

    @Override
    public LowerCaseCharSequenceIntHashMap getModelAliasIndexes() {
        return modelAliasIndexes;
    }

    @Override
    public int getModelPosition() {
        return modelPosition;
    }

    @Override
    public int getModelType() {
        return modelType;
    }

    @Override
    public CharSequence getName() {
        if (alias != null) {
            return alias.token;
        }

        if (tableNameExpr != null) {
            return tableNameExpr.token;
        }

        return null;
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<WindowExpression> getNamedWindows() {
        return namedWindows;
    }

    @Override
    public IQueryModel getNestedModel() {
        return nestedModel;
    }

    @Override
    public ObjList<ExpressionNode> getOrderBy() {
        return orderBy;
    }

    @Override
    public ObjList<ExpressionNode> getOrderByAdvice() {
        return orderByAdvice;
    }

    @Override
    public int getOrderByAdviceMnemonic() {
        return orderByAdviceMnemonic;
    }

    @Override
    public IntList getOrderByDirection() {
        return orderByDirection;
    }

    @Override
    public IntList getOrderByDirectionAdvice() {
        return orderByDirectionAdvice;
    }

    @Override
    public int getOrderByPosition() {
        return orderByPosition;
    }

    @Override
    public LowerCaseCharSequenceIntHashMap getOrderHash() {
        return orderHash;
    }

    @Override
    public IntList getOrderedJoinModels() {
        return orderedJoinModels;
    }

    @Override
    public ExpressionNode getOriginatingViewNameExpr() {
        return originatingViewNameExpr;
    }

    @Override
    public ExpressionNode getOuterJoinExpressionClause() {
        return outerJoinExpressionClause;
    }

    @Override
    public LowerCaseCharSequenceHashSet getOverridableDecls() {
        return overridableDecls;
    }

    @Override
    public ObjList<ExpressionNode> getParsedWhere() {
        return parsedWhere;
    }

    @Override
    public ObjList<PivotForColumn> getPivotForColumns() {
        return pivotForColumns;
    }

    @Override
    public ObjList<QueryColumn> getPivotGroupByColumns() {
        return pivotGroupByColumns;
    }

    @Override
    public ExpressionNode getPostJoinWhereClause() {
        return postJoinWhereClause;
    }

    @Override
    public IQueryModel getQueryModel() {
        return this;
    }

    @Override
    public int getRefCount(CharSequence alias) {
        return columnAliasRefCounts.get(alias);
    }

    @Override
    public ObjList<ViewDefinition> getReferencedViews() {
        return referencedViews;
    }

    @Override
    public ExpressionNode getSampleBy() {
        return sampleBy;
    }

    @Override
    public ObjList<ExpressionNode> getSampleByFill() {
        return sampleByFill;
    }

    @Override
    public ExpressionNode getSampleByFrom() {
        return sampleByFrom;
    }

    @Override
    public ExpressionNode getSampleByOffset() {
        return sampleByOffset;
    }

    @Override
    public ExpressionNode getSampleByTimezoneName() {
        return sampleByTimezoneName;
    }

    @Override
    public ExpressionNode getSampleByTo() {
        return sampleByTo;
    }

    @Override
    public ExpressionNode getSampleByUnit() {
        return sampleByUnit;
    }

    @Override
    public int getSelectModelType() {
        return selectModelType;
    }

    @Override
    public int getSetOperationType() {
        return setOperationType;
    }

    @Override
    public int getSharedRefCount() {
        return sharedRefs.size() + sharedRefByParentCount;
    }

    @Override
    public ObjList<QueryModelWrapper> getSharedRefs() {
        return sharedRefs;
    }

    @Override
    public int getShowKind() {
        return showKind;
    }

    @Override
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

    @Override
    public RecordCursorFactory getTableNameFunction() {
        return tableNameFunction;
    }

    @Override
    public ExpressionNode getTimestamp() {
        return timestamp;
    }

    @Override
    public int getTimestampColumnIndex() {
        return timestampColumnIndex;
    }

    @Override
    public CharSequence getTimestampOffsetAlias() {
        return timestampOffsetAlias;
    }

    @Override
    public char getTimestampOffsetUnit() {
        return timestampOffsetUnit;
    }

    @Override
    public int getTimestampOffsetValue() {
        return timestampOffsetValue;
    }

    @Override
    public CharSequence getTimestampSourceColumn() {
        return timestampSourceColumn;
    }

    @Override
    public ObjList<QueryColumn> getTopDownColumns() {
        return topDownColumns;
    }

    @Override
    public IQueryModel getUnionModel() {
        return unionModel;
    }

    @Override
    public ObjList<CharSequence> getUnnestColumnAliases() {
        return unnestColumnAliases;
    }

    @Override
    public ObjList<ExpressionNode> getUnnestExpressions() {
        return unnestExpressions;
    }

    @Override
    public ObjList<ObjList<CharSequence>> getUnnestJsonColumnNames() {
        return unnestJsonColumnNames;
    }

    @Override
    public ObjList<IntList> getUnnestJsonColumnTypes() {
        return unnestJsonColumnTypes;
    }

    /**
     * Returns the total number of output columns across all UNNEST sources.
     * Array sources contribute 1 column each; JSON sources contribute N
     * columns (one per COLUMNS declaration).
     */
    @Override
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

    @Override
    public ObjList<ExpressionNode> getUpdateExpressions() {
        return updateSetColumns;
    }

    @Override
    public ObjList<CharSequence> getUpdateTableColumnNames() {
        return updateTableModel != null ? updateTableModel.getUpdateTableColumnNames() : updateTableColumnNames;
    }

    @Override
    public IntList getUpdateTableColumnTypes() {
        return updateTableModel != null ? updateTableModel.getUpdateTableColumnTypes() : updateTableColumnTypes;
    }

    @Override
    public IQueryModel getUpdateTableModel() {
        return updateTableModel;
    }

    @Override
    public TableToken getUpdateTableToken() {
        return updateTableToken;
    }

    @Override
    public ExpressionNode getViewNameExpr() {
        return viewNameExpr;
    }

    @Override
    public ExpressionNode getWhereClause() {
        return whereClause;
    }

    @Override
    public ObjList<CharSequence> getWildcardColumnNames() {
        return wildcardColumnNames;
    }

    @Override
    public WindowJoinContext getWindowJoinContext() {
        return windowJoinContext;
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<WithClauseModel> getWithClauses() {
        return withClauseModel;
    }

    @Override
    public boolean hasExplicitTimestamp() {
        return timestamp != null && explicitTimestamp;
    }

    @Override
    public boolean hasSharedRefs() {
        return sharedRefs.size() > 0;
    }

    @Override
    public boolean hasTimestampOffset() {
        return timestampOffsetUnit != 0;
    }

    @Override
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

    @Override
    public boolean isArtificialStar() {
        return artificialStar;
    }

    @Override
    public boolean isCacheable() {
        if (nestedModel != null) {
            return cacheable && nestedModel.isCacheable();
        }
        return cacheable;
    }

    @Override
    public boolean isCorrelatedAtDepth(int depth) {
        if (nestedModel != null) {
            if (nestedModel.isCorrelatedAtDepth(depth)) {
                return true;
            }
        }

        for (int i = 1, j = joinModels.size(); i < j; i++) {
            IQueryModel m = joinModels.getQuick(i);
            if (m != null) {
                if (m.isCorrelatedAtDepth(depth)) {
                    return true;
                }
            }
        }

        if (unionModel != null) {
            if (unionModel.isCorrelatedAtDepth(depth)) {
                return true;
            }
        }
        return correlatedDepths.keyIndex(depth) < 0;
    }

    @Override
    public boolean isCteModel() {
        return isCteModel;
    }

    @Override
    public boolean isDistinct() {
        return distinct;
    }

    @Override
    public boolean isExplicitTimestamp() {
        return explicitTimestamp;
    }

    @Override
    public boolean isForceBackwardScan() {
        return forceBackwardScan;
    }

    @Override
    public boolean isNestedModelIsSubQuery() {
        return nestedModelIsSubQuery;
    }

    @Override
    public boolean isOptimisable() {
        return true;
    }

    @Override
    public boolean isOrderDescendingByDesignatedTimestampOnly() {
        return orderDescendingByDesignatedTimestampOnly;
    }

    @Override
    public boolean isOwnCorrelatedAtDepth(int depth, int flag) {
        int ki = correlatedDepths.keyIndex(depth);
        if (ki >= 0) {
            return false;
        }
        return (correlatedDepths.valueAt(ki) & flag) != 0;
    }

    @Override
    public boolean isPivot() {
        return pivotForColumns.size() > 0;
    }

    @Override
    public boolean isPivotGroupByColumnHasNoAlias() {
        return pivotGroupByColumnHasNoAlias;
    }

    @Override
    public boolean isSelectTranslation() {
        return isSelectTranslation;
    }

    @Override
    public boolean isSkipped() {
        return skipped;
    }

    @Override
    public boolean isStandaloneUnnest() {
        return standaloneUnnest;
    }

    @SuppressWarnings("unused")
    @Override
    public boolean isTemporalJoin() {
        return joinType >= JOIN_ASOF && joinType <= JOIN_LT;
    }

    @Override
    public boolean isTopDownNameMissing(CharSequence columnName) {
        return topDownNameSet.excludes(columnName);
    }

    @Override
    public boolean isUnnestJsonSource(int index) {
        return index < unnestJsonColumnNames.size()
                && unnestJsonColumnNames.getQuick(index) != null;
    }

    @Override
    public boolean isUnnestOrdinality() {
        return unnestOrdinality;
    }

    @Override
    public boolean isUpdate() {
        return isUpdateModel;
    }

    @Override
    public void makeCorrelatedAtDepth(int depth, int flags) {
        int ki = correlatedDepths.keyIndex(depth);
        if (ki < 0) {
            correlatedDepths.putAt(ki, depth, correlatedDepths.valueAt(ki) | flags);
        } else {
            correlatedDepths.putAt(ki, depth, flags);
        }
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
    @Override
    public void mergePartially(IQueryModel baseModel, ObjectPool<QueryColumn> queryColumnPool) {
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
                col.of(thisColumn.getAlias(), thatColumn.getAst(), thisColumn.isIncludeIntoWildcard());
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
        if (baseModel.getLimitLo() != null || baseModel.getLimitHi() != null) {
            limitLo = baseModel.getLimitLo();
            limitHi = baseModel.getLimitHi();
            limitPosition = baseModel.getLimitPosition();
            limitAdviceLo = baseModel.getLimitAdviceLo();
            limitAdviceHi = baseModel.getLimitAdviceHi();
        }
    }

    @Override
    public void moveGroupByFrom(IQueryModel model) {
        ObjList<ExpressionNode> thatGroupBy = model.getGroupBy();
        groupBy.addAll(thatGroupBy);
        // clear the source
        thatGroupBy.clear();
    }

    @Override
    public void moveJoinAliasFrom(IQueryModel that) {
        final ExpressionNode alias = that.getAlias();
        if (alias != null && !Chars.startsWith(alias.token, SUB_QUERY_ALIAS_PREFIX)) {
            setAlias(alias);
            addModelAliasIndex(alias, 0);
        }
    }

    @Override
    public void moveLimitFrom(IQueryModel baseModel) {
        this.limitLo = baseModel.getLimitLo();
        this.limitHi = baseModel.getLimitHi();
        baseModel.setLimit(null, null);
    }

    @Override
    public void moveOrderByFrom(IQueryModel model) {
        orderBy.addAll(model.getOrderBy());
        orderByDirection.addAll(model.getOrderByDirection());
        model.clearOrderBy();
    }

    @Override
    public void moveSampleByFrom(IQueryModel model) {
        this.sampleBy = model.getSampleBy();
        this.sampleByUnit = model.getSampleByUnit();
        this.sampleByFill.clear();
        this.sampleByFill.addAll(model.getSampleByFill());
        this.sampleByTimezoneName = model.getSampleByTimezoneName();
        this.sampleByOffset = model.getSampleByOffset();
        this.sampleByTo = model.getSampleByTo();
        this.sampleByFrom = model.getSampleByFrom();

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
    @Override
    public IntList nextOrderedJoinModels() {
        IntList ordered = orderedJoinModels == orderedJoinModels1 ? orderedJoinModels2 : orderedJoinModels1;
        ordered.clear();
        return ordered;
    }

    /*
     * Splits "where" clauses into "and" chunks
     */
    @Override
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

    @Override
    public void recordViews(LowerCaseCharSequenceObjHashMap<ViewDefinition> viewDefinitions) {
        final ObjList<CharSequence> keys = viewDefinitions.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final ViewDefinition viewDefinition = viewDefinitions.get(keys.getQuick(i));
            if (!referencedViews.contains(viewDefinition)) {
                referencedViews.add(viewDefinition);
            }
        }
    }

    @Override
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
    @Override
    public void removeColumn(int columnIndex) {
        CharSequence columnAlias = bottomUpColumns.getQuick(columnIndex).getAlias();
        bottomUpColumns.remove(columnIndex);
        wildcardColumnNames.remove(columnAlias);
        aliasToColumnMap.remove(columnAlias);
        aliasToColumnNameMap.remove(columnAlias);
        columnAliasIndexes.remove(columnAlias);
    }

    @Override
    public void removeDependency(int index) {
        dependencies.remove(index);
    }

    @Override
    public void replaceColumn(int columnIndex, QueryColumn newColumn) {
        if (topDownColumns.size() > 0) {
            topDownColumns.setQuick(columnIndex, newColumn);
        } else {
            bottomUpColumns.setQuick(columnIndex, newColumn);
        }
    }

    @Override
    public void replaceColumnNameMap(CharSequence alias, CharSequence oldToken, CharSequence newToken) {
        aliasToColumnNameMap.put(alias, newToken);
        columnNameToAliasMap.remove(oldToken);
        columnNameToAliasMap.put(newToken, alias);
    }

    @Override
    public void replaceJoinModel(int pos, IQueryModel model) {
        joinModels.setQuick(pos, model);
    }

    @Override
    public void setAlias(ExpressionNode alias) {
        this.alias = alias;
    }

    @Override
    public void setAllowPropagationOfOrderByAdvice(boolean value) {
        allowPropagationOfOrderByAdvice = value;
    }

    @Override
    public void setArtificialStar(boolean artificialStar) {
        this.artificialStar = artificialStar;
    }

    @Override
    public void setAsOfJoinTolerance(ExpressionNode asOfJoinTolerance) {
        this.asOfJoinTolerance = asOfJoinTolerance;
    }

    @Override
    public void setBackupWhereClause(ExpressionNode backupWhereClause) {
        this.backupWhereClause = backupWhereClause;
    }

    @Override
    public void setCacheable(boolean b) {
        cacheable = b;
    }

    @Override
    public void setConstWhereClause(ExpressionNode constWhereClause) {
        this.constWhereClause = constWhereClause;
    }

    @Override
    public void setContext(JoinContext context) {
        this.context = context;
    }

    @Override
    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    @Override
    public void setExplicitTimestamp(boolean explicitTimestamp) {
        this.explicitTimestamp = explicitTimestamp;
    }

    @Override
    public void setFillFrom(ExpressionNode fillFrom) {
        this.fillFrom = fillFrom;
    }

    @Override
    public void setFillStride(ExpressionNode fillStride) {
        this.fillStride = fillStride;
    }

    @Override
    public void setFillTo(ExpressionNode fillTo) {
        this.fillTo = fillTo;
    }

    @Override
    public void setFillValues(ObjList<ExpressionNode> fillValues) {
        this.fillValues = fillValues;
    }

    @Override
    public void setForceBackwardScan(boolean forceBackwardScan) {
        this.forceBackwardScan = forceBackwardScan;
    }

    @Override
    public void setIsCteModel(boolean isCteModel) {
        this.isCteModel = isCteModel;
    }

    @Override
    public void setIsUpdate(boolean isUpdate) {
        this.isUpdateModel = isUpdate;
    }

    @Override
    public void setJoinCriteria(ExpressionNode joinCriteria) {
        this.joinCriteria = joinCriteria;
    }

    @Override
    public void setJoinKeywordPosition(int position) {
        this.joinKeywordPosition = position;
    }

    @Override
    public void setJoinType(int joinType) {
        this.joinType = joinType;
    }

    @Override
    public void setLatestByType(int latestByType) {
        this.latestByType = latestByType;
    }

    @Override
    public void setLimit(ExpressionNode lo, ExpressionNode hi) {
        this.limitLo = lo;
        this.limitHi = hi;
    }

    @Override
    public void setLimitAdvice(ExpressionNode lo, ExpressionNode hi) {
        this.limitAdviceLo = lo;
        this.limitAdviceHi = hi;
    }

    @Override
    public void setLimitPosition(int limitPosition) {
        this.limitPosition = limitPosition;
    }

    @Override
    public void setMetadataVersion(long metadataVersion) {
        this.metadataVersion = metadataVersion;
    }

    @Override
    public void setModelPosition(int modelPosition) {
        this.modelPosition = modelPosition;
    }

    @Override
    public void setModelType(int modelType) {
        this.modelType = modelType;
    }

    @Override
    public void setNestedModel(IQueryModel nestedModel) {
        this.nestedModel = nestedModel;
        if (nestedModel != null && viewNameExpr != null) {
            nestedModel.setViewNameExpr(viewNameExpr);
        }
    }

    @Override
    public void setNestedModelIsSubQuery(boolean nestedModelIsSubQuery) {
        this.nestedModelIsSubQuery = nestedModelIsSubQuery;
    }

    @Override
    public void setOrderByAdviceMnemonic(int orderByAdviceMnemonic) {
        this.orderByAdviceMnemonic = orderByAdviceMnemonic;
    }

    @Override
    public void setOrderByPosition(int orderByPosition) {
        this.orderByPosition = orderByPosition;
    }

    @Override
    public void setOrderDescendingByDesignatedTimestampOnly(boolean orderDescendingByDesignatedTimestampOnly) {
        this.orderDescendingByDesignatedTimestampOnly = orderDescendingByDesignatedTimestampOnly;
    }

    @Override
    public void setOrderedJoinModels(IntList that) {
        assert that == orderedJoinModels1 || that == orderedJoinModels2;
        this.orderedJoinModels = that;
    }

    @Override
    public void setOriginatingViewNameExpr(ExpressionNode originatingViewNameExpr) {
        this.originatingViewNameExpr = originatingViewNameExpr;
    }

    @Override
    public void setOuterJoinExpressionClause(ExpressionNode outerJoinExpressionClause) {
        this.outerJoinExpressionClause = outerJoinExpressionClause;
    }

    @Override
    public void setPivotGroupByColumnHasNoAlias(boolean pivotGroupByColumnHasNoAlias) {
        this.pivotGroupByColumnHasNoAlias = pivotGroupByColumnHasNoAlias;
    }

    @Override
    public void setPostJoinWhereClause(ExpressionNode postJoinWhereClause) {
        this.postJoinWhereClause = postJoinWhereClause;
    }

    @Override
    public void setSampleBy(ExpressionNode sampleBy) {
        this.sampleBy = sampleBy;
    }

    @Override
    public void setSampleBy(ExpressionNode sampleBy, ExpressionNode sampleByUnit) {
        this.sampleBy = sampleBy;
        this.sampleByUnit = sampleByUnit;
    }

    @Override
    public void setSampleByFromTo(ExpressionNode from, ExpressionNode to) {
        this.sampleByFrom = from;
        this.sampleByTo = to;
    }

    @Override
    public void setSampleByOffset(ExpressionNode sampleByOffset) {
        this.sampleByOffset = sampleByOffset;
    }

    @Override
    public void setSampleByTimezoneName(ExpressionNode sampleByTimezoneName) {
        this.sampleByTimezoneName = sampleByTimezoneName;
    }

    @Override
    public void setSelectModelType(int selectModelType) {
        this.selectModelType = selectModelType;
    }

    @Override
    public void setSelectTranslation(boolean isSelectTranslation) {
        this.isSelectTranslation = isSelectTranslation;
    }

    @Override
    public void setSetOperationType(int setOperationType) {
        this.setOperationType = setOperationType;
    }

    public void setSharedRefByParentCount(int sharedRefByParentCount) {
        this.sharedRefByParentCount = sharedRefByParentCount;
    }

    @Override
    public void setShowKind(int showKind) {
        this.showKind = showKind;
    }

    @Override
    public void setSkipped(boolean skipped) {
        this.skipped = skipped;
    }

    @Override
    public void setStandaloneUnnest(boolean standaloneUnnest) {
        this.standaloneUnnest = standaloneUnnest;
    }

    @Override
    public void setTableId(int id) {
        this.tableId = id;
    }

    @Override
    public void setTableNameExpr(ExpressionNode tableNameExpr) {
        this.tableNameExpr = tableNameExpr;
    }

    @Override
    public void setTableNameFunction(RecordCursorFactory function) {
        this.tableNameFunction = function;
    }

    @Override
    public void setTimestamp(ExpressionNode timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public void setTimestampColumnIndex(int index) {
        this.timestampColumnIndex = index;
    }

    @Override
    public void setTimestampOffsetAlias(CharSequence alias) {
        this.timestampOffsetAlias = alias;
    }

    @Override
    public void setTimestampOffsetUnit(char unit) {
        this.timestampOffsetUnit = unit;
    }

    @Override
    public void setTimestampOffsetValue(int value) {
        this.timestampOffsetValue = value;
    }

    @Override
    public void setTimestampSourceColumn(CharSequence col) {
        this.timestampSourceColumn = col;
    }

    @Override
    public void setUnionModel(IQueryModel unionModel) {
        this.unionModel = unionModel;
        if (unionModel != null && viewNameExpr != null) {
            unionModel.setViewNameExpr(viewNameExpr);
        }
    }

    @Override
    public void setUnnestOrdinality(boolean unnestOrdinality) {
        this.unnestOrdinality = unnestOrdinality;
    }

    @Override
    public void setUpdateTableToken(TableToken tableName) {
        this.updateTableToken = tableName;
    }

    @Override
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

    @Override
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

    // returns textual description of this model, e.g. select-choose [top-down-columns] bottom-up-columns from X ...
    @Override
    public void toSink0(CharSink<?> sink, boolean joinSlave, boolean showOrderBy) {
        if (selectModelType == IQueryModel.SELECT_MODEL_SHOW) {
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
                    IQueryModel model = joinModels.getQuick(orderedJoinModels.getQuick(i));
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
                            case JOIN_UNNEST:
                                sink.putAscii(", unnest(");
                                for (int k = 0, z = model.getUnnestExpressions().size(); k < z; k++) {
                                    if (k > 0) {
                                        sink.putAscii(", ");
                                    }
                                    model.getUnnestExpressions().getQuick(k).toSink(sink);
                                    if (model.isUnnestJsonSource(k)) {
                                        ObjList<CharSequence> colNames =
                                                model.getUnnestJsonColumnNames().getQuick(k);
                                        IntList colTypes =
                                                model.getUnnestJsonColumnTypes().getQuick(k);
                                        sink.putAscii(" columns(");
                                        for (int c = 0, cn = colNames.size(); c < cn; c++) {
                                            if (c > 0) {
                                                sink.putAscii(", ");
                                            }
                                            sink.put(colNames.getQuick(c));
                                            sink.putAscii(' ');
                                            sink.putAscii(ColumnType.nameOf(colTypes.getQuick(c)));
                                        }
                                        sink.putAscii(')');
                                    }
                                }
                                sink.putAscii(')');
                                if (model.isUnnestOrdinality()) {
                                    sink.putAscii(" with ordinality");
                                }
                                aliasToSink(model.getAlias().token, sink);
                                if (model.getUnnestColumnAliases().size() > 0) {
                                    sink.putAscii('(');
                                    for (int k = 0, z = model.getUnnestColumnAliases().size(); k < z; k++) {
                                        if (k > 0) {
                                            sink.putAscii(", ");
                                        }
                                        sink.put(model.getUnnestColumnAliases().getQuick(k));
                                    }
                                    sink.putAscii(')');
                                }
                                if (model.getPostJoinWhereClause() != null) {
                                    sink.putAscii(" post-join-where ");
                                    model.getPostJoinWhereClause().toSink(sink);
                                }
                                continue;
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
                        if (model.getJoinType() == JOIN_WINDOW) {
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

                        if (model.getAsOfJoinTolerance() != null) {
                            assert model.getJoinType() == JOIN_ASOF;
                            sink.putAscii(" tolerance ");
                            model.getAsOfJoinTolerance().toSink(sink);
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
            if (setOperationType == IQueryModel.SET_OPERATION_INTERSECT) {
                sink.putAscii(" intersect ");
            } else if (setOperationType == IQueryModel.SET_OPERATION_INTERSECT_ALL) {
                sink.putAscii(" intersect all ");
            } else if (setOperationType == IQueryModel.SET_OPERATION_EXCEPT) {
                sink.putAscii(" except ");
            } else if (setOperationType == IQueryModel.SET_OPERATION_EXCEPT_ALL) {
                sink.putAscii(" except all ");
            } else {
                sink.putAscii(" union ");
                if (setOperationType == IQueryModel.SET_OPERATION_UNION_ALL) {
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

    // method to make debugging easier
    // not using toString name to prevent debugger from trying to use it on all model variables (because toSink0 can fail).
    @SuppressWarnings("unused")
    @Override
    public String toString0() {
        StringSink sink = Misc.getThreadLocalSink();
        this.toSink0(sink, true, true);
        return sink.toString();
    }

    @Override
    public CharSequence translateAlias(CharSequence column) {
        return aliasToColumnNameMap.get(column);
    }

    @Override
    public void updateColumnAliasIndexes() {
        columnAliasIndexes.clear();
        for (int i = 0, n = bottomUpColumns.size(); i < n; i++) {
            columnAliasIndexes.put(wildcardColumnNames.getQuick(i), i);
        }
    }

    @Override
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
