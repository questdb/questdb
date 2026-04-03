/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _  *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
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
import io.questdb.std.ObjectPool;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

/**
 * Lightweight wrapper around a QueryModel that avoids deep-cloning.
 * Used by SharedRecordCursorFactory to share a single QueryModel instance
 * across multiple references. The shareId identifies the shared group.
 * The optimizer skips wrapped models during optimization.
 */
public class QueryModelWrapper implements IQueryModel {
    private QueryModel delegate;
    private int shareId;

    @Override
    public void addBottomUpColumn(QueryColumn column) throws SqlException {
        delegate.addBottomUpColumn(column);
    }

    @Override
    public void addBottomUpColumn(QueryColumn column, boolean allowDuplicates) throws SqlException {
        delegate.addBottomUpColumn(column, allowDuplicates);
    }

    @Override
    public void addBottomUpColumn(int position, QueryColumn column, boolean allowDuplicates) throws SqlException {
        delegate.addBottomUpColumn(position, column, allowDuplicates);
    }

    @Override
    public void addBottomUpColumn(int position, QueryColumn column, boolean allowDuplicates, CharSequence additionalMessage) throws SqlException {
        delegate.addBottomUpColumn(position, column, allowDuplicates, additionalMessage);
    }

    @Override
    public void addBottomUpColumnIfNotExists(QueryColumn column) {
        delegate.addBottomUpColumnIfNotExists(column);
    }

    @Override
    public void addDependency(int index) {
        delegate.addDependency(index);
    }

    @Override
    public void addExpressionModel(ExpressionNode node) {
        delegate.addExpressionModel(node);
    }

    @Override
    public boolean addField(QueryColumn column) {
        return delegate.addField(column);
    }

    @Override
    public void addGroupBy(ExpressionNode node) {
        delegate.addGroupBy(node);
    }

    @Override
    public void addHint(CharSequence key, CharSequence value) {
        delegate.addHint(key, value);
    }

    @Override
    public void addJoinColumn(ExpressionNode node) {
        delegate.addJoinColumn(node);
    }

    @Override
    public void addJoinModel(IQueryModel joinModel) {
        delegate.addJoinModel(joinModel);
    }

    @Override
    public void addLatestBy(ExpressionNode latestBy) {
        delegate.addLatestBy(latestBy);
    }

    @Override
    public boolean addModelAliasIndex(ExpressionNode node, int index) {
        return delegate.addModelAliasIndex(node, index);
    }

    @Override
    public void addOrderBy(ExpressionNode node, int direction) {
        delegate.addOrderBy(node, direction);
    }

    @Override
    public void addParsedWhereNode(ExpressionNode node, boolean innerPredicate) {
        delegate.addParsedWhereNode(node, innerPredicate);
    }

    @Override
    public void addPivotForColumn(PivotForColumn column) {
        delegate.addPivotForColumn(column);
    }

    @Override
    public void addPivotGroupByColumn(QueryColumn column) {
        delegate.addPivotGroupByColumn(column);
    }

    @Override
    public void addSampleByFill(ExpressionNode sampleByFill) {
        delegate.addSampleByFill(sampleByFill);
    }

    @Override
    public void addTopDownColumn(QueryColumn column, CharSequence alias) {
        delegate.addTopDownColumn(column, alias);
    }

    @Override
    public void addUpdateTableColumnMetadata(int columnType, String columnName) {
        delegate.addUpdateTableColumnMetadata(columnType, columnName);
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
        delegate.clear();
    }

    @Override
    public void clearColumnMapStructs() {
        delegate.clearColumnMapStructs();
    }

    @Override
    public void clearOrderBy() {
        delegate.clearOrderBy();
    }

    @Override
    public void clearDependents() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearSampleBy() {
        delegate.clearSampleBy();
    }

    @Override
    public boolean containsJoin() {
        return delegate.containsJoin();
    }

    @Override
    public void copyBottomToTopColumns() {
        delegate.copyBottomToTopColumns();
    }

    @Override
    public void copyColumnsFrom(IQueryModel other, ObjectPool<QueryColumn> queryColumnPool, ObjectPool<ExpressionNode> expressionNodePool) {
        delegate.copyColumnsFrom(other, queryColumnPool, expressionNodePool);
    }

    @Override
    public void copyDeclsFrom(IQueryModel model, boolean overrideDeclares) throws SqlException {
        delegate.copyDeclsFrom(model, overrideDeclares);
    }

    @Override
    public void copyDeclsFrom(LowerCaseCharSequenceObjHashMap<ExpressionNode> decls, boolean overrideDeclares) throws SqlException {
        delegate.copyDeclsFrom(decls, overrideDeclares);
    }

    @Override
    public void copyHints(LowerCaseCharSequenceObjHashMap<CharSequence> hints) {
        delegate.copyHints(hints);
    }

    @Override
    public void copyOrderByAdvice(ObjList<ExpressionNode> orderByAdvice) {
        delegate.copyOrderByAdvice(orderByAdvice);
    }

    @Override
    public void copyOrderByDirectionAdvice(IntList orderByDirection) {
        delegate.copyOrderByDirectionAdvice(orderByDirection);
    }

    @Override
    public void copyDependents(IQueryModel model) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyUpdateTableMetadata(IQueryModel updateTableModel) {
        delegate.copyUpdateTableMetadata(updateTableModel);
    }

    @Override
    public IQueryModel deepClone(ObjectPool<QueryModel> queryModelPool, ObjectPool<QueryColumn> queryColumnPool, ObjectPool<ExpressionNode> expressionNodePool, ObjectPool<WindowExpression> windowExpressionPool) {
        return delegate.deepClone(queryModelPool, queryColumnPool, expressionNodePool, windowExpressionPool);
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
    public ObjList<QueryModelWrapper> getDependents() {
        throw new UnsupportedOperationException();
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
    public boolean hasDependents() {
        return delegate.hasDependents();
    }

    @Override
    public boolean hasTimestampOffset() {
        return delegate.hasTimestampOffset();
    }

    @Override
    public void incrementColumnRefCount(CharSequence alias, int refCount) {
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
    public boolean isTemporalJoin() {
        return delegate.isTemporalJoin();
    }

    @Override
    public boolean isTopDownNameMissing(CharSequence columnName) {
        return delegate.isTopDownNameMissing(columnName);
    }

    @Override
    public boolean isUpdate() {
        return delegate.isUpdate();
    }

    @Override
    public void makeCorrelatedAtDepth(int depth, int flags) {
        delegate.makeCorrelatedAtDepth(depth, flags);
    }

    @Override
    public void mergePartially(IQueryModel baseModel, ObjectPool<QueryColumn> queryColumnPool) {
        delegate.mergePartially(baseModel, queryColumnPool);
    }

    @Override
    public void moveGroupByFrom(IQueryModel model) {
        delegate.moveGroupByFrom(model);
    }

    @Override
    public void moveJoinAliasFrom(IQueryModel that) {
        delegate.moveJoinAliasFrom(that);
    }

    @Override
    public void moveLimitFrom(IQueryModel baseModel) {
        delegate.moveLimitFrom(baseModel);
    }

    @Override
    public void moveOrderByFrom(IQueryModel model) {
        delegate.moveOrderByFrom(model);
    }

    @Override
    public void moveSampleByFrom(IQueryModel model) {
        delegate.moveSampleByFrom(model);
    }

    @Override
    public IntList nextOrderedJoinModels() {
        return delegate.nextOrderedJoinModels();
    }

    @Override
    public ObjList<ExpressionNode> parseWhereClause() {
        return delegate.parseWhereClause();
    }

    @Override
    public void recordViews(LowerCaseCharSequenceObjHashMap<ViewDefinition> viewDefinitions) {
        delegate.recordViews(viewDefinitions);
    }

    @Override
    public void recordViews(ObjList<ViewDefinition> viewDefinitions) {
        delegate.recordViews(viewDefinitions);
    }

    @Override
    public void removeColumn(int columnIndex) {
        delegate.removeColumn(columnIndex);
    }

    @Override
    public void removeDependency(int index) {
        delegate.removeDependency(index);
    }

    @Override
    public void replaceColumn(int columnIndex, QueryColumn newColumn) {
        delegate.replaceColumn(columnIndex, newColumn);
    }

    @Override
    public void replaceColumnNameMap(CharSequence alias, CharSequence oldToken, CharSequence newToken) {
        delegate.replaceColumnNameMap(alias, oldToken, newToken);
    }

    @Override
    public void replaceJoinModel(int pos, IQueryModel model) {
        delegate.replaceJoinModel(pos, model);
    }

    @Override
    public void setAlias(ExpressionNode alias) {
        delegate.setAlias(alias);
    }

    @Override
    public void setAllowPropagationOfOrderByAdvice(boolean value) {
        delegate.setAllowPropagationOfOrderByAdvice(value);
    }

    @Override
    public void setArtificialStar(boolean artificialStar) {
        delegate.setArtificialStar(artificialStar);
    }

    @Override
    public void setAsOfJoinTolerance(ExpressionNode asOfJoinTolerance) {
        delegate.setAsOfJoinTolerance(asOfJoinTolerance);
    }

    @Override
    public void setBackupWhereClause(ExpressionNode backupWhereClause) {
        delegate.setBackupWhereClause(backupWhereClause);
    }

    @Override
    public void setCacheable(boolean b) {
        delegate.setCacheable(b);
    }

    @Override
    public void setConstWhereClause(ExpressionNode constWhereClause) {
        delegate.setConstWhereClause(constWhereClause);
    }

    @Override
    public void setContext(JoinContext context) {
        delegate.setContext(context);
    }

    public void setDelegate(QueryModel delegate) {
        this.delegate = delegate;
    }

    @Override
    public void setDistinct(boolean distinct) {
        delegate.setDistinct(distinct);
    }

    @Override
    public void setExplicitTimestamp(boolean explicitTimestamp) {
        delegate.setExplicitTimestamp(explicitTimestamp);
    }

    @Override
    public void setFillFrom(ExpressionNode fillFrom) {
        delegate.setFillFrom(fillFrom);
    }

    @Override
    public void setFillStride(ExpressionNode fillStride) {
        delegate.setFillStride(fillStride);
    }

    @Override
    public void setFillTo(ExpressionNode fillTo) {
        delegate.setFillTo(fillTo);
    }

    @Override
    public void setFillValues(ObjList<ExpressionNode> fillValues) {
        delegate.setFillValues(fillValues);
    }

    @Override
    public void setForceBackwardScan(boolean forceBackwardScan) {
        delegate.setForceBackwardScan(forceBackwardScan);
    }

    @Override
    public void setIsCteModel(boolean isCteModel) {
        delegate.setIsCteModel(isCteModel);
    }

    @Override
    public void setIsUpdate(boolean isUpdate) {
        delegate.setIsUpdate(isUpdate);
    }

    @Override
    public void setJoinCriteria(ExpressionNode joinCriteria) {
        delegate.setJoinCriteria(joinCriteria);
    }

    @Override
    public void setJoinKeywordPosition(int position) {
        delegate.setJoinKeywordPosition(position);
    }

    @Override
    public void setJoinType(int joinType) {
        delegate.setJoinType(joinType);
    }

    @Override
    public void setLatestByType(int latestByType) {
        delegate.setLatestByType(latestByType);
    }

    @Override
    public void setLimit(ExpressionNode lo, ExpressionNode hi) {
        delegate.setLimit(lo, hi);
    }

    @Override
    public void setLimitAdvice(ExpressionNode lo, ExpressionNode hi) {
        delegate.setLimitAdvice(lo, hi);
    }

    @Override
    public void setLimitPosition(int limitPosition) {
        delegate.setLimitPosition(limitPosition);
    }

    @Override
    public void setMetadataVersion(long metadataVersion) {
        delegate.setMetadataVersion(metadataVersion);
    }

    @Override
    public void setModelPosition(int modelPosition) {
        delegate.setModelPosition(modelPosition);
    }

    @Override
    public void setModelType(int modelType) {
        delegate.setModelType(modelType);
    }

    @Override
    public void setNestedModel(IQueryModel nestedModel) {
        delegate.setNestedModel(nestedModel);
    }

    @Override
    public void setNestedModelIsSubQuery(boolean nestedModelIsSubQuery) {
        delegate.setNestedModelIsSubQuery(nestedModelIsSubQuery);
    }

    @Override
    public void setOrderByAdviceMnemonic(int orderByAdviceMnemonic) {
        delegate.setOrderByAdviceMnemonic(orderByAdviceMnemonic);
    }

    @Override
    public void setOrderByPosition(int orderByPosition) {
        delegate.setOrderByPosition(orderByPosition);
    }

    @Override
    public void setOrderDescendingByDesignatedTimestampOnly(boolean orderDescendingByDesignatedTimestampOnly) {
        delegate.setOrderDescendingByDesignatedTimestampOnly(orderDescendingByDesignatedTimestampOnly);
    }

    @Override
    public void setOrderedJoinModels(IntList that) {
        delegate.setOrderedJoinModels(that);
    }

    @Override
    public void setOriginatingViewNameExpr(ExpressionNode originatingViewNameExpr) {
        delegate.setOriginatingViewNameExpr(originatingViewNameExpr);
    }

    @Override
    public void setOuterJoinExpressionClause(ExpressionNode outerJoinExpressionClause) {
        delegate.setOuterJoinExpressionClause(outerJoinExpressionClause);
    }

    @Override
    public void setPivotGroupByColumnHasNoAlias(boolean pivotGroupByColumnHasNoAlias) {
        delegate.setPivotGroupByColumnHasNoAlias(pivotGroupByColumnHasNoAlias);
    }

    @Override
    public void setPostJoinWhereClause(ExpressionNode postJoinWhereClause) {
        delegate.setPostJoinWhereClause(postJoinWhereClause);
    }

    @Override
    public void setSampleBy(ExpressionNode sampleBy) {
        delegate.setSampleBy(sampleBy);
    }

    @Override
    public void setSampleBy(ExpressionNode sampleBy, ExpressionNode sampleByUnit) {
        delegate.setSampleBy(sampleBy, sampleByUnit);
    }

    @Override
    public void setSampleByFromTo(ExpressionNode from, ExpressionNode to) {
        delegate.setSampleByFromTo(from, to);
    }

    @Override
    public void setSampleByOffset(ExpressionNode sampleByOffset) {
        delegate.setSampleByOffset(sampleByOffset);
    }

    @Override
    public void setSampleByTimezoneName(ExpressionNode sampleByTimezoneName) {
        delegate.setSampleByTimezoneName(sampleByTimezoneName);
    }

    @Override
    public void setSelectModelType(int selectModelType) {
        delegate.setSelectModelType(selectModelType);
    }

    @Override
    public void setSelectTranslation(boolean isSelectTranslation) {
        delegate.setSelectTranslation(isSelectTranslation);
    }

    @Override
    public void setSetOperationType(int setOperationType) {
        delegate.setSetOperationType(setOperationType);
    }

    public void setShareId(int shareId) {
        this.shareId = shareId;
    }

    @Override
    public void setShowKind(int showKind) {
        delegate.setShowKind(showKind);
    }

    @Override
    public void setSkipped(boolean skipped) {
        delegate.setSkipped(skipped);
    }

    @Override
    public void setTableId(int id) {
        delegate.setTableId(id);
    }

    @Override
    public void setTableNameExpr(ExpressionNode tableNameExpr) {
        delegate.setTableNameExpr(tableNameExpr);
    }

    @Override
    public void setTableNameFunction(RecordCursorFactory function) {
        delegate.setTableNameFunction(function);
    }

    @Override
    public void setTimestamp(ExpressionNode timestamp) {
        delegate.setTimestamp(timestamp);
    }

    @Override
    public void setTimestampColumnIndex(int index) {
        delegate.setTimestampColumnIndex(index);
    }

    @Override
    public void setTimestampOffsetAlias(CharSequence alias) {
        delegate.setTimestampOffsetAlias(alias);
    }

    @Override
    public void setTimestampOffsetUnit(char unit) {
        delegate.setTimestampOffsetUnit(unit);
    }

    @Override
    public void setTimestampOffsetValue(int value) {
        delegate.setTimestampOffsetValue(value);
    }

    @Override
    public void setTimestampSourceColumn(CharSequence col) {
        delegate.setTimestampSourceColumn(col);
    }

    @Override
    public void setUnionModel(IQueryModel unionModel) {
        delegate.setUnionModel(unionModel);
    }

    @Override
    public void setUpdateTableToken(TableToken tableName) {
        delegate.setUpdateTableToken(tableName);
    }

    @Override
    public void setViewNameExpr(ExpressionNode viewNameExpr) {
        delegate.setViewNameExpr(viewNameExpr);
    }

    @Override
    public void setWhereClause(ExpressionNode whereClause) {
        delegate.setWhereClause(whereClause);
    }

    @Override
    public boolean supportOptimise() {
        return false;
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
        delegate.updateColumnAliasIndexes();
    }

    @Override
    public boolean windowStopPropagate() {
        return delegate.windowStopPropagate();
    }
}
