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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.SqlException;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

/**
 * Read-only, lightweight wrapper around a QueryModel that avoids deep-cloning
 * and owning complex member variables. Used by SharedRecordCursorFactory to
 * share a single QueryModel instance across multiple references. The shareId
 * identifies the shared group. The optimizer skips wrapped models during
 * optimization. Getter methods delegate to the underlying model; all mutation
 * methods throw {@link UnsupportedOperationException} to prevent accidental
 * modification of the shared delegate.
 */
public class QueryModelWrapper implements IQueryModel {
    public static final WrapperFactory FACTORY = new WrapperFactory();

    private QueryModel delegate;
    private int shareId;

    public QueryModelWrapper() {
    }

    @Override
    public void addBottomUpColumn(QueryColumn column) throws SqlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addBottomUpColumn(QueryColumn column, boolean allowDuplicates) throws SqlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addBottomUpColumn(int position, QueryColumn column, boolean allowDuplicates) throws SqlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addBottomUpColumn(int position, QueryColumn column, boolean allowDuplicates, CharSequence additionalMessage) throws SqlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addBottomUpColumnIfNotExists(QueryColumn column) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addDependency(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addExpressionModel(ExpressionNode node) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addField(QueryColumn column) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addGroupBy(ExpressionNode node) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addHint(CharSequence key, CharSequence value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addJoinColumn(ExpressionNode node) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addJoinModel(IQueryModel joinModel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addLatestBy(ExpressionNode latestBy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addModelAliasIndex(ExpressionNode node, int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addOrderBy(ExpressionNode node, int direction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addParsedWhereNode(ExpressionNode node, boolean innerPredicate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addPivotForColumn(PivotForColumn column) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addPivotGroupByColumn(QueryColumn column) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addSampleByFill(ExpressionNode sampleByFill) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addTopDownColumn(QueryColumn column, CharSequence alias) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addUpdateTableColumnMetadata(int columnType, String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean allowsColumnsChange() {
        return false;
    }

    @Override
    public boolean allowsNestedColumnsChange() {
        return false;
    }

    @Override
    public void clear() {
        delegate = null;
        shareId = 0;
    }

    @Override
    public void clearColumnMapStructs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearOrderBy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearSampleBy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearSharedRefs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsJoin() {
        return delegate.containsJoin();
    }

    @Override
    public void copyBottomToTopColumns() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyColumnsFrom(IQueryModel other, ObjectPool<QueryColumn> queryColumnPool, ObjectPool<ExpressionNode> expressionNodePool) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyDeclsFrom(IQueryModel model, boolean overrideDeclares) throws SqlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyDeclsFrom(LowerCaseCharSequenceObjHashMap<ExpressionNode> decls, boolean overrideDeclares) throws SqlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyHints(LowerCaseCharSequenceObjHashMap<CharSequence> hints) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyOrderByAdvice(ObjList<ExpressionNode> orderByAdvice) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyOrderByDirectionAdvice(IntList orderByDirection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copySharedRefs(IQueryModel model) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyUpdateTableMetadata(IQueryModel updateTableModel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryColumn findBottomUpColumnByAst(ExpressionNode node) {
        return delegate.findBottomUpColumnByAst(node);
    }

    @Override
    public ExpressionNode getAlias() {
        return delegate.getAlias();
    }

    @Override
    public LowerCaseCharSequenceIntHashMap getAliasSequenceMap() {
        return delegate.getAliasSequenceMap();
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<QueryColumn> getAliasToColumnMap() {
        return delegate.getAliasToColumnMap();
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<CharSequence> getAliasToColumnNameMap() {
        return delegate.getAliasToColumnNameMap();
    }

    @Override
    public boolean getAllowPropagationOfOrderByAdvice() {
        return delegate.getAllowPropagationOfOrderByAdvice();
    }

    @Override
    public ExpressionNode getAsOfJoinTolerance() {
        return delegate.getAsOfJoinTolerance();
    }

    @Override
    public ExpressionNode getBackupWhereClause() {
        return delegate.getBackupWhereClause();
    }

    @Override
    public ObjList<QueryColumn> getBottomUpColumns() {
        return delegate.getBottomUpColumns();
    }

    @Override
    public int getColumnAliasIndex(CharSequence alias) {
        return delegate.getColumnAliasIndex(alias);
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<CharSequence> getColumnNameToAliasMap() {
        return delegate.getColumnNameToAliasMap();
    }

    @Override
    public ObjList<QueryColumn> getColumns() {
        return delegate.getColumns();
    }

    @Override
    public ExpressionNode getConstWhereClause() {
        return delegate.getConstWhereClause();
    }

    @Override
    public ObjList<LowerCaseCharSequenceIntHashMap> getCorrelatedColumns() {
        return delegate.getCorrelatedColumns();
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<ExpressionNode> getDecls() {
        return delegate.getDecls();
    }

    public QueryModel getDelegate() {
        return delegate;
    }

    @Override
    public IntHashSet getDependencies() {
        return delegate.getDependencies();
    }

    @Override
    public ObjList<ExpressionNode> getExpressionModels() {
        return delegate.getExpressionModels();
    }

    @Override
    public ExpressionNode getFillFrom() {
        return delegate.getFillFrom();
    }

    @Override
    public ExpressionNode getFillStride() {
        return delegate.getFillStride();
    }

    @Override
    public ExpressionNode getFillTo() {
        return delegate.getFillTo();
    }

    @Override
    public ObjList<ExpressionNode> getFillValues() {
        return delegate.getFillValues();
    }

    @Override
    public ObjList<ExpressionNode> getGroupBy() {
        return delegate.getGroupBy();
    }

    @Override
    public ObjList<IntList> getGroupingSets() {
        return delegate.getGroupingSets();
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<CharSequence> getHints() {
        return delegate.getHints();
    }

    @Override
    public HorizonJoinContext getHorizonJoinContext() {
        return delegate.getHorizonJoinContext();
    }

    @Override
    public ObjList<ExpressionNode> getJoinColumns() {
        return delegate.getJoinColumns();
    }

    @Override
    public JoinContext getJoinContext() {
        return delegate.getJoinContext();
    }

    @Override
    public ExpressionNode getJoinCriteria() {
        return delegate.getJoinCriteria();
    }

    @Override
    public int getJoinKeywordPosition() {
        return delegate.getJoinKeywordPosition();
    }

    @Override
    public ObjList<IQueryModel> getJoinModels() {
        return delegate.getJoinModels();
    }

    @Override
    public int getJoinType() {
        return delegate.getJoinType();
    }

    @Override
    public ObjList<CharSequence> getLateralCountColumns() {
        return delegate.getLateralCountColumns();
    }

    @Override
    public ObjList<ExpressionNode> getLatestBy() {
        return delegate.getLatestBy();
    }

    @Override
    public int getLatestByType() {
        return delegate.getLatestByType();
    }

    @Override
    public ExpressionNode getLimitAdviceHi() {
        return delegate.getLimitAdviceHi();
    }

    @Override
    public ExpressionNode getLimitAdviceLo() {
        return delegate.getLimitAdviceLo();
    }

    @Override
    public ExpressionNode getLimitHi() {
        return delegate.getLimitHi();
    }

    @Override
    public ExpressionNode getLimitLo() {
        return delegate.getLimitLo();
    }

    @Override
    public int getLimitPosition() {
        return delegate.getLimitPosition();
    }

    @Override
    public long getMetadataVersion() {
        return delegate.getMetadataVersion();
    }

    @Override
    public int getModelAliasIndex(CharSequence modelAlias, int start, int end) {
        return delegate.getModelAliasIndex(modelAlias, start, end);
    }

    @Override
    public LowerCaseCharSequenceIntHashMap getModelAliasIndexes() {
        return delegate.getModelAliasIndexes();
    }

    @Override
    public int getModelPosition() {
        return delegate.getModelPosition();
    }

    @Override
    public int getModelType() {
        return delegate.getModelType();
    }

    @Override
    public CharSequence getName() {
        return delegate.getName();
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<WindowExpression> getNamedWindows() {
        return delegate.getNamedWindows();
    }

    @Override
    public IQueryModel getNestedModel() {
        return delegate.getNestedModel();
    }

    @Override
    public ObjList<ExpressionNode> getOrderBy() {
        return delegate.getOrderBy();
    }

    @Override
    public ObjList<ExpressionNode> getOrderByAdvice() {
        return delegate.getOrderByAdvice();
    }

    @Override
    public int getOrderByAdviceMnemonic() {
        return delegate.getOrderByAdviceMnemonic();
    }

    @Override
    public IntList getOrderByDirection() {
        return delegate.getOrderByDirection();
    }

    @Override
    public IntList getOrderByDirectionAdvice() {
        return delegate.getOrderByDirectionAdvice();
    }

    @Override
    public int getOrderByPosition() {
        return delegate.getOrderByPosition();
    }

    @Override
    public LowerCaseCharSequenceIntHashMap getOrderHash() {
        return delegate.getOrderHash();
    }

    @Override
    public IntList getOrderedJoinModels() {
        return delegate.getOrderedJoinModels();
    }

    @Override
    public ExpressionNode getOriginatingViewNameExpr() {
        return delegate.getOriginatingViewNameExpr();
    }

    @Override
    public ExpressionNode getOuterJoinExpressionClause() {
        return delegate.getOuterJoinExpressionClause();
    }

    @Override
    public LowerCaseCharSequenceHashSet getOverridableDecls() {
        return delegate.getOverridableDecls();
    }

    @Override
    public ObjList<ExpressionNode> getParsedWhere() {
        return delegate.getParsedWhere();
    }

    @Override
    public ObjList<PivotForColumn> getPivotForColumns() {
        return delegate.getPivotForColumns();
    }

    @Override
    public ObjList<QueryColumn> getPivotGroupByColumns() {
        return delegate.getPivotGroupByColumns();
    }

    @Override
    public ExpressionNode getPostJoinWhereClause() {
        return delegate.getPostJoinWhereClause();
    }

    @Override
    public IQueryModel getQueryModel() {
        return delegate.getQueryModel();
    }

    @Override
    public int getRefCount(CharSequence alias) {
        return delegate.getRefCount(alias);
    }

    @Override
    public ObjList<ViewDefinition> getReferencedViews() {
        return delegate.getReferencedViews();
    }

    @Override
    public ExpressionNode getSampleBy() {
        return delegate.getSampleBy();
    }

    @Override
    public ObjList<ExpressionNode> getSampleByFill() {
        return delegate.getSampleByFill();
    }

    @Override
    public ExpressionNode getSampleByFrom() {
        return delegate.getSampleByFrom();
    }

    @Override
    public ExpressionNode getSampleByOffset() {
        return delegate.getSampleByOffset();
    }

    @Override
    public ExpressionNode getSampleByTimezoneName() {
        return delegate.getSampleByTimezoneName();
    }

    @Override
    public ExpressionNode getSampleByTo() {
        return delegate.getSampleByTo();
    }

    @Override
    public ExpressionNode getSampleByUnit() {
        return delegate.getSampleByUnit();
    }

    @Override
    public int getSelectModelType() {
        return delegate.getSelectModelType();
    }

    @Override
    public int getSetOperationType() {
        return delegate.getSetOperationType();
    }

    public int getShareId() {
        return shareId;
    }

    @Override
    public int getSharedRefCount() {
        return delegate.getSharedRefCount();
    }

    @Override
    public ObjList<QueryModelWrapper> getSharedRefs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getShowKind() {
        return delegate.getShowKind();
    }

    @Override
    public int getTableId() {
        return delegate.getTableId();
    }

    @Override
    public CharSequence getTableName() {
        return delegate.getTableName();
    }

    @Override
    public ExpressionNode getTableNameExpr() {
        return delegate.getTableNameExpr();
    }

    @Override
    public RecordCursorFactory getTableNameFunction() {
        return delegate.getTableNameFunction();
    }

    @Override
    public ExpressionNode getTimestamp() {
        return delegate.getTimestamp();
    }

    @Override
    public int getTimestampColumnIndex() {
        return delegate.getTimestampColumnIndex();
    }

    @Override
    public CharSequence getTimestampOffsetAlias() {
        return delegate.getTimestampOffsetAlias();
    }

    @Override
    public char getTimestampOffsetUnit() {
        return delegate.getTimestampOffsetUnit();
    }

    @Override
    public int getTimestampOffsetValue() {
        return delegate.getTimestampOffsetValue();
    }

    @Override
    public CharSequence getTimestampSourceColumn() {
        return delegate.getTimestampSourceColumn();
    }

    @Override
    public ObjList<QueryColumn> getTopDownColumns() {
        return delegate.getTopDownColumns();
    }

    @Override
    public IQueryModel getUnionModel() {
        return delegate.getUnionModel();
    }

    @Override
    public ObjList<CharSequence> getUnnestColumnAliases() {
        return delegate.getUnnestColumnAliases();
    }

    @Override
    public ObjList<ExpressionNode> getUnnestExpressions() {
        return delegate.getUnnestExpressions();
    }

    @Override
    public ObjList<ObjList<CharSequence>> getUnnestJsonColumnNames() {
        return delegate.getUnnestJsonColumnNames();
    }

    @Override
    public ObjList<IntList> getUnnestJsonColumnTypes() {
        return delegate.getUnnestJsonColumnTypes();
    }

    @Override
    public int getUnnestOutputColumnCount() {
        return delegate.getUnnestOutputColumnCount();
    }

    @Override
    public ObjList<ExpressionNode> getUpdateExpressions() {
        return delegate.getUpdateExpressions();
    }

    @Override
    public ObjList<CharSequence> getUpdateTableColumnNames() {
        return delegate.getUpdateTableColumnNames();
    }

    @Override
    public IntList getUpdateTableColumnTypes() {
        return delegate.getUpdateTableColumnTypes();
    }

    @Override
    public IQueryModel getUpdateTableModel() {
        return delegate.getUpdateTableModel();
    }

    @Override
    public TableToken getUpdateTableToken() {
        return delegate.getUpdateTableToken();
    }

    @Override
    public ExpressionNode getViewNameExpr() {
        return delegate.getViewNameExpr();
    }

    @Override
    public ExpressionNode getWhereClause() {
        return delegate.getWhereClause();
    }

    @Override
    public ObjList<CharSequence> getWildcardColumnNames() {
        return delegate.getWildcardColumnNames();
    }

    @Override
    public WindowJoinContext getWindowJoinContext() {
        return delegate.getWindowJoinContext();
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<WithClauseModel> getWithClauses() {
        return delegate.getWithClauses();
    }

    @Override
    public boolean hasExplicitTimestamp() {
        return delegate.hasExplicitTimestamp();
    }

    @Override
    public boolean hasGroupingSets() {
        return delegate.hasGroupingSets();
    }

    @Override
    public boolean hasSharedRefs() {
        return false;
    }

    @Override
    public boolean hasTimestampOffset() {
        return delegate.hasTimestampOffset();
    }

    @Override
    public void incrementColumnRefCount(CharSequence alias, int refCount) {
        // Delegates so function memoization sees usage from all wrapper references.
        delegate.incrementColumnRefCount(alias, refCount);
    }

    @Override
    public boolean isArtificialStar() {
        return delegate.isArtificialStar();
    }

    @Override
    public boolean isCacheable() {
        return delegate.isCacheable();
    }

    @Override
    public boolean isCorrelatedAtDepth(int depth) {
        return delegate.isCorrelatedAtDepth(depth);
    }

    @Override
    public boolean isCteModel() {
        return delegate.isCteModel();
    }

    @Override
    public boolean isDistinct() {
        return delegate.isDistinct();
    }

    @Override
    public boolean isExplicitTimestamp() {
        return delegate.isExplicitTimestamp();
    }

    @Override
    public boolean isForceBackwardScan() {
        return delegate.isForceBackwardScan();
    }

    @Override
    public boolean isNestedModelIsSubQuery() {
        return delegate.isNestedModelIsSubQuery();
    }

    @Override
    public boolean isOptimisable() {
        return false;
    }

    @Override
    public boolean isOrderDescendingByDesignatedTimestampOnly() {
        return delegate.isOrderDescendingByDesignatedTimestampOnly();
    }

    @Override
    public boolean isOwnCorrelatedAtDepth(int depth, int flag) {
        return delegate.isOwnCorrelatedAtDepth(depth, flag);
    }

    @Override
    public boolean isPivot() {
        return delegate.isPivot();
    }

    @Override
    public boolean isPivotGroupByColumnHasNoAlias() {
        return delegate.isPivotGroupByColumnHasNoAlias();
    }

    @Override
    public boolean isSelectTranslation() {
        return delegate.isSelectTranslation();
    }

    @Override
    public boolean isSkipped() {
        return delegate.isSkipped();
    }

    @Override
    public boolean isStandaloneUnnest() {
        return delegate.isStandaloneUnnest();
    }

    @Override
    public boolean isTemporalJoin() {
        return delegate.isTemporalJoin();
    }

    @Override
    public boolean isTopDownNameMissing(CharSequence columnName) {
        return delegate.isTopDownNameMissing(columnName);
    }

    @Override
    public boolean isUnnestJsonSource(int index) {
        return delegate.isUnnestJsonSource(index);
    }

    @Override
    public boolean isUnnestOrdinality() {
        return delegate.isUnnestOrdinality();
    }

    @Override
    public boolean isUpdate() {
        return delegate.isUpdate();
    }

    @Override
    public void makeCorrelatedAtDepth(int depth, int flags) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mergePartially(IQueryModel baseModel, ObjectPool<QueryColumn> queryColumnPool) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void moveGroupByFrom(IQueryModel model) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void moveJoinAliasFrom(IQueryModel that) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void moveLimitFrom(IQueryModel baseModel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void moveOrderByFrom(IQueryModel model) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void moveSampleByFrom(IQueryModel model) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IntList nextOrderedJoinModels() {
        throw new UnsupportedOperationException();
    }

    public QueryModelWrapper of(QueryModel delegate, int shareId) {
        this.delegate = delegate;
        this.shareId = shareId;
        return this;
    }

    @Override
    public ObjList<ExpressionNode> parseWhereClause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void recordViews(LowerCaseCharSequenceObjHashMap<ViewDefinition> viewDefinitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void recordViews(ObjList<ViewDefinition> viewDefinitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeColumn(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeDependency(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceColumn(int columnIndex, QueryColumn newColumn) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceColumnNameMap(CharSequence alias, CharSequence oldToken, CharSequence newToken) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceJoinModel(int pos, IQueryModel model) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAlias(ExpressionNode alias) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAllowPropagationOfOrderByAdvice(boolean value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setArtificialStar(boolean artificialStar) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAsOfJoinTolerance(ExpressionNode asOfJoinTolerance) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBackupWhereClause(ExpressionNode backupWhereClause) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCacheable(boolean b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setConstWhereClause(ExpressionNode constWhereClause) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setContext(JoinContext context) {
        throw new UnsupportedOperationException();
    }

    public void setDelegate(QueryModel delegate) {
        this.delegate = delegate;
    }

    @Override
    public void setDistinct(boolean distinct) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setExplicitTimestamp(boolean explicitTimestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFillFrom(ExpressionNode fillFrom) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFillStride(ExpressionNode fillStride) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFillTo(ExpressionNode fillTo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFillValues(ObjList<ExpressionNode> fillValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setForceBackwardScan(boolean forceBackwardScan) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setIsCteModel(boolean isCteModel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setIsUpdate(boolean isUpdate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setJoinCriteria(ExpressionNode joinCriteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setJoinKeywordPosition(int position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setJoinType(int joinType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLatestByType(int latestByType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLimit(ExpressionNode lo, ExpressionNode hi) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLimitAdvice(ExpressionNode lo, ExpressionNode hi) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLimitPosition(int limitPosition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMetadataVersion(long metadataVersion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setModelPosition(int modelPosition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setModelType(int modelType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNestedModel(IQueryModel nestedModel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNestedModelIsSubQuery(boolean nestedModelIsSubQuery) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOrderByAdviceMnemonic(int orderByAdviceMnemonic) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOrderByPosition(int orderByPosition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOrderDescendingByDesignatedTimestampOnly(boolean orderDescendingByDesignatedTimestampOnly) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOrderedJoinModels(IntList that) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOriginatingViewNameExpr(ExpressionNode originatingViewNameExpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOuterJoinExpressionClause(ExpressionNode outerJoinExpressionClause) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPivotGroupByColumnHasNoAlias(boolean pivotGroupByColumnHasNoAlias) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPostJoinWhereClause(ExpressionNode postJoinWhereClause) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSampleBy(ExpressionNode sampleBy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSampleBy(ExpressionNode sampleBy, ExpressionNode sampleByUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSampleByFromTo(ExpressionNode from, ExpressionNode to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSampleByOffset(ExpressionNode sampleByOffset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSampleByTimezoneName(ExpressionNode sampleByTimezoneName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSelectModelType(int selectModelType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSelectTranslation(boolean isSelectTranslation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSetOperationType(int setOperationType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setShowKind(int showKind) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSkipped(boolean skipped) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setStandaloneUnnest(boolean standaloneUnnest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTableId(int id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTableNameExpr(ExpressionNode tableNameExpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTableNameFunction(RecordCursorFactory function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTimestamp(ExpressionNode timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTimestampColumnIndex(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTimestampOffsetAlias(CharSequence alias) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTimestampOffsetUnit(char unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTimestampOffsetValue(int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTimestampSourceColumn(CharSequence col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setUnionModel(IQueryModel unionModel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setUnnestOrdinality(boolean unnestOrdinality) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setUpdateTableToken(TableToken tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setViewNameExpr(ExpressionNode viewNameExpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setWhereClause(ExpressionNode whereClause) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        delegate.toSink(sink);
    }

    @Override
    public void toSink0(CharSink<?> sink, boolean joinSlave, boolean showOrderBy) {
        delegate.toSink0(sink, joinSlave, showOrderBy);
    }

    @Override
    public String toString() {
        return "QueryModelWrapper{shareId=" + shareId + ", delegate=" + delegate + "}";
    }

    @Override
    public String toString0() {
        return delegate.toString0();
    }

    @Override
    public CharSequence translateAlias(CharSequence column) {
        return delegate.translateAlias(column);
    }

    @Override
    public void updateColumnAliasIndexes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean windowStopPropagate() {
        return delegate.windowStopPropagate();
    }

    public static class WrapperFactory implements ObjectFactory<QueryModelWrapper> {
        @Override
        public QueryModelWrapper newInstance() {
            return new QueryModelWrapper();
        }
    }
}
