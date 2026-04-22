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
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;

public interface IQueryModel extends Mutable, ExecutionModel, AliasTranslator, Sinkable {

    int JOIN_ASOF = 4;
    int JOIN_CROSS = 3;
    int JOIN_CROSS_FULL = 12;
    int JOIN_CROSS_LEFT = 8;
    int JOIN_CROSS_RIGHT = 11;
    int JOIN_FULL_OUTER = 10;
    int JOIN_HORIZON = 13;
    int JOIN_INNER = 1;
    int JOIN_LATERAL_CROSS = 16;
    int JOIN_LATERAL_INNER = 14;
    int JOIN_LATERAL_LEFT = 15;
    int JOIN_LEFT_OUTER = 2;
    int JOIN_LT = 6;
    int JOIN_NONE = 0;
    int JOIN_RIGHT_OUTER = 9;
    int JOIN_SPLICE = 5;
    int JOIN_UNNEST = 17;
    int JOIN_MAX = JOIN_UNNEST;
    int JOIN_WINDOW = 7;
    int LATEST_BY_DEPRECATED = 1;
    int LATEST_BY_NEW = 2;
    int LATEST_BY_NONE = 0;
    String NO_ROWID_MARKER = "*!*";
    int ORDER_DIRECTION_ASCENDING = 0;
    int ORDER_DIRECTION_DESCENDING = 1;
    int SELECT_MODEL_CHOOSE = 1;
    int SELECT_MODEL_CURSOR = 6;
    int SELECT_MODEL_DISTINCT = 5;
    int SELECT_MODEL_GROUP_BY = 4;
    int SELECT_MODEL_HORIZON_JOIN = 9;
    int SELECT_MODEL_NONE = 0;
    int SELECT_MODEL_SHOW = 7;
    int SELECT_MODEL_VIRTUAL = 2;
    int SELECT_MODEL_WINDOW = 3;
    int SELECT_MODEL_WINDOW_JOIN = 8;
    int SET_OPERATION_EXCEPT = 2;
    int SET_OPERATION_EXCEPT_ALL = 3;
    int SET_OPERATION_INTERSECT = 4;
    int SET_OPERATION_INTERSECT_ALL = 5;
    int SET_OPERATION_UNION = 1;
    // types of set operations between this and union model
    int SET_OPERATION_UNION_ALL = 0;
    int SHOW_COLUMNS = 2;
    int SHOW_CREATE_MAT_VIEW = 15;
    int SHOW_CREATE_TABLE = 14;
    int SHOW_CREATE_VIEW = 17;
    int SHOW_DATE_STYLE = 9;
    int SHOW_DEFAULT_TRANSACTION_READ_ONLY = 16;
    int SHOW_MAX_IDENTIFIER_LENGTH = 6;
    int SHOW_PARAMETERS = 11;
    int SHOW_PARTITIONS = 3;
    int SHOW_SEARCH_PATH = 8;
    int SHOW_SERVER_VERSION = 12;
    int SHOW_SERVER_VERSION_NUM = 13;
    int SHOW_STANDARD_CONFORMING_STRINGS = 7;
    int SHOW_TABLES = 1;
    int SHOW_TIME_ZONE = 10;
    int SHOW_TRANSACTION = 4;
    int SHOW_TRANSACTION_ISOLATION_LEVEL = 5;
    String SUB_QUERY_ALIAS_PREFIX = "_xQdbA";

    /**
     * Recursively clones the current value of whereClause for the model and its sub-models into the backupWhereClause field.
     */
    static void backupWhereClause(final ObjectPool<ExpressionNode> pool, final IQueryModel model) {
        IQueryModel current = model;
        while (current != null && current.isOptimisable()) {
            if (current.getUnionModel() != null) {
                backupWhereClause(pool, current.getUnionModel());
            }
            if (current.getUpdateTableModel() != null) {
                backupWhereClause(pool, current.getUpdateTableModel());
            }
            for (int i = 1, n = current.getJoinModels().size(); i < n; i++) {
                final IQueryModel m = current.getJoinModels().get(i);
                if (m != null && current != m) {
                    backupWhereClause(pool, m);
                }
            }
            current.setBackupWhereClause(ExpressionNode.deepClone(pool, current.getWhereClause()));
            current = current.getNestedModel();
        }
    }

    // Horizon join must be the only join in the query level; no other join types can be combined with it.
    // This constraint is enforced at the SQL parser stage.
    static boolean isHorizonJoin(IQueryModel model) {
        if (model == null) {
            return false;
        }
        ObjList<IQueryModel> ms = model.getJoinModels();
        return ms.size() > 1 && ms.get(ms.size() - 1).getJoinType() == JOIN_HORIZON;
    }

    static boolean isLateralJoin(int joinType) {
        return joinType == IQueryModel.JOIN_LATERAL_INNER
                || joinType == IQueryModel.JOIN_LATERAL_LEFT
                || joinType == IQueryModel.JOIN_LATERAL_CROSS;
    }

    // Window join must be the last join in the query; no other join types can follow it.
    // This constraint is enforced at the SQL parser stage.
    static boolean isWindowJoin(IQueryModel model) {
        if (model == null) {
            return false;
        }
        ObjList<IQueryModel> ms = model.getJoinModels();
        return ms.size() > 1 && ms.get(ms.size() - 1).getJoinType() == JOIN_WINDOW;
    }

    /**
     * Recursively restores the whereClause field from backupWhereClause for the model and its sub-models.
     */
    static void restoreWhereClause(final ObjectPool<ExpressionNode> pool, final IQueryModel model) {
        IQueryModel current = model;
        while (current != null && current.isOptimisable()) {
            if (current.getUnionModel() != null) {
                restoreWhereClause(pool, current.getUnionModel());
            }
            if (current.getUpdateTableModel() != null) {
                restoreWhereClause(pool, current.getUpdateTableModel());
            }
            for (int i = 1, n = current.getJoinModels().size(); i < n; i++) {
                final IQueryModel m = current.getJoinModels().get(i);
                if (m != null && current != m) {
                    restoreWhereClause(pool, m);
                }
            }
            current.setWhereClause(ExpressionNode.deepClone(pool, current.getBackupWhereClause()));
            current = current.getNestedModel();
        }
    }

    void addBottomUpColumn(QueryColumn column) throws SqlException;

    void addBottomUpColumn(QueryColumn column, boolean allowDuplicates) throws SqlException;

    void addBottomUpColumn(int position, QueryColumn column, boolean allowDuplicates) throws SqlException;

    void addBottomUpColumn(int position, QueryColumn column, boolean allowDuplicates, CharSequence additionalMessage) throws SqlException;

    void addBottomUpColumnIfNotExists(QueryColumn column);

    void addDependency(int index);

    void addExpressionModel(ExpressionNode node);

    boolean addField(QueryColumn column);

    void addGroupBy(ExpressionNode node);

    void addHint(CharSequence key, CharSequence value);

    void addJoinColumn(ExpressionNode node);

    void addJoinModel(IQueryModel joinModel);

    void addLatestBy(ExpressionNode latestBy);

    boolean addModelAliasIndex(ExpressionNode node, int index);

    void addOrderBy(ExpressionNode node, int direction);

    void addParsedWhereNode(ExpressionNode node, boolean innerPredicate);

    void addPivotForColumn(PivotForColumn column);

    void addPivotGroupByColumn(QueryColumn column);

    void addSampleByFill(ExpressionNode sampleByFill);

    void addTopDownColumn(QueryColumn column, CharSequence alias);

    void addUpdateTableColumnMetadata(int columnType, String columnName);

    boolean allowsColumnsChange();

    boolean allowsNestedColumnsChange();

    void clearColumnMapStructs();

    void clearOrderBy();

    void clearSampleBy();

    void clearSharedRefs();

    boolean containsJoin();

    void copyBottomToTopColumns();

    void copyColumnsFrom(IQueryModel other, ObjectPool<QueryColumn> queryColumnPool, ObjectPool<ExpressionNode> expressionNodePool);

    void copyDeclsFrom(IQueryModel model, boolean overrideDeclares) throws SqlException;

    void copyDeclsFrom(LowerCaseCharSequenceObjHashMap<ExpressionNode> decls, boolean overrideDeclares) throws SqlException;

    void copyHints(LowerCaseCharSequenceObjHashMap<CharSequence> hints);

    void copyOrderByAdvice(ObjList<ExpressionNode> orderByAdvice);

    void copyOrderByDirectionAdvice(IntList orderByDirection);

    void copySharedRefs(IQueryModel model);

    void copyUpdateTableMetadata(IQueryModel updateTableModel);

    QueryColumn findBottomUpColumnByAst(ExpressionNode node);

    ExpressionNode getAlias();

    LowerCaseCharSequenceIntHashMap getAliasSequenceMap();

    LowerCaseCharSequenceObjHashMap<QueryColumn> getAliasToColumnMap();

    LowerCaseCharSequenceObjHashMap<CharSequence> getAliasToColumnNameMap();

    boolean getAllowPropagationOfOrderByAdvice();

    ExpressionNode getAsOfJoinTolerance();

    ExpressionNode getBackupWhereClause();

    ObjList<QueryColumn> getBottomUpColumns();

    int getColumnAliasIndex(CharSequence alias);

    LowerCaseCharSequenceObjHashMap<CharSequence> getColumnNameToAliasMap();

    ObjList<QueryColumn> getColumns();

    ExpressionNode getConstWhereClause();

    ObjList<LowerCaseCharSequenceIntHashMap> getCorrelatedColumns();

    LowerCaseCharSequenceObjHashMap<ExpressionNode> getDecls();

    IntHashSet getDependencies();

    ObjList<ExpressionNode> getExpressionModels();

    ExpressionNode getFillFrom();

    ExpressionNode getFillStride();

    ExpressionNode getFillTo();

    ObjList<ExpressionNode> getFillValues();

    ObjList<ExpressionNode> getGroupBy();

    ObjList<IntList> getGroupingSets();

    LowerCaseCharSequenceObjHashMap<CharSequence> getHints();

    HorizonJoinContext getHorizonJoinContext();

    ObjList<ExpressionNode> getJoinColumns();

    JoinContext getJoinContext();

    ExpressionNode getJoinCriteria();

    int getJoinKeywordPosition();

    ObjList<IQueryModel> getJoinModels();

    int getJoinType();

    ObjList<CharSequence> getLateralCountColumns();

    ObjList<ExpressionNode> getLatestBy();

    int getLatestByType();

    ExpressionNode getLimitAdviceHi();

    ExpressionNode getLimitAdviceLo();

    ExpressionNode getLimitHi();

    ExpressionNode getLimitLo();

    int getLimitPosition();

    long getMetadataVersion();

    int getModelAliasIndex(CharSequence modelAlias, int start, int end);

    LowerCaseCharSequenceIntHashMap getModelAliasIndexes();

    int getModelPosition();

    CharSequence getName();

    LowerCaseCharSequenceObjHashMap<WindowExpression> getNamedWindows();

    IQueryModel getNestedModel();

    ObjList<ExpressionNode> getOrderBy();

    ObjList<ExpressionNode> getOrderByAdvice();

    int getOrderByAdviceMnemonic();

    IntList getOrderByDirection();

    IntList getOrderByDirectionAdvice();

    int getOrderByPosition();

    LowerCaseCharSequenceIntHashMap getOrderHash();

    IntList getOrderedJoinModels();

    ExpressionNode getOriginatingViewNameExpr();

    ExpressionNode getOuterJoinExpressionClause();

    LowerCaseCharSequenceHashSet getOverridableDecls();

    ObjList<ExpressionNode> getParsedWhere();

    ObjList<PivotForColumn> getPivotForColumns();

    ObjList<QueryColumn> getPivotGroupByColumns();

    ExpressionNode getPostJoinWhereClause();

    int getRefCount(CharSequence alias);

    ObjList<ViewDefinition> getReferencedViews();

    // getQueryModel() is inherited from ExecutionModel with return type QueryModel

    ExpressionNode getSampleBy();

    ObjList<ExpressionNode> getSampleByFill();

    ExpressionNode getSampleByFrom();

    ExpressionNode getSampleByOffset();

    ExpressionNode getSampleByTimezoneName();

    ExpressionNode getSampleByTo();

    ExpressionNode getSampleByUnit();

    int getSelectModelType();

    int getSetOperationType();

    int getSharedRefCount();

    ObjList<QueryModelWrapper> getSharedRefs();

    int getShowKind();

    int getTableId();

    CharSequence getTableName();

    ExpressionNode getTableNameExpr();

    RecordCursorFactory getTableNameFunction();

    ExpressionNode getTimestamp();

    int getTimestampColumnIndex();

    CharSequence getTimestampOffsetAlias();

    char getTimestampOffsetUnit();

    int getTimestampOffsetValue();

    CharSequence getTimestampSourceColumn();

    ObjList<QueryColumn> getTopDownColumns();

    IQueryModel getUnionModel();

    ObjList<CharSequence> getUnnestColumnAliases();

    ObjList<ExpressionNode> getUnnestExpressions();

    ObjList<ObjList<CharSequence>> getUnnestJsonColumnNames();

    ObjList<IntList> getUnnestJsonColumnTypes();

    int getUnnestOutputColumnCount();

    ObjList<ExpressionNode> getUpdateExpressions();

    ObjList<CharSequence> getUpdateTableColumnNames();

    IntList getUpdateTableColumnTypes();

    IQueryModel getUpdateTableModel();

    TableToken getUpdateTableToken();

    ExpressionNode getViewNameExpr();

    ExpressionNode getWhereClause();

    ObjList<CharSequence> getWildcardColumnNames();

    WindowJoinContext getWindowJoinContext();

    LowerCaseCharSequenceObjHashMap<WithClauseModel> getWithClauses();

    boolean hasExplicitTimestamp();

    boolean hasGroupingSets();

    boolean hasSharedRefs();

    boolean hasTimestampOffset();

    void incrementColumnRefCount(CharSequence alias, int refCount);

    boolean isArtificialStar();

    boolean isCacheable();

    boolean isCorrelatedAtDepth(int depth);

    boolean isCteModel();

    boolean isDistinct();

    boolean isExplicitTimestamp();

    boolean isForceBackwardScan();

    boolean isNestedModelIsSubQuery();

    boolean isOptimisable();

    boolean isOrderDescendingByDesignatedTimestampOnly();

    boolean isOwnCorrelatedAtDepth(int depth, int flag);

    boolean isPivot();

    boolean isPivotGroupByColumnHasNoAlias();

    boolean isSelectTranslation();

    boolean isSkipped();

    boolean isStandaloneUnnest();

    boolean isTemporalJoin();

    boolean isTopDownNameMissing(CharSequence columnName);

    boolean isUnnestJsonSource(int index);

    boolean isUnnestOrdinality();

    boolean isUpdate();

    void makeCorrelatedAtDepth(int depth, int flags);

    void mergePartially(IQueryModel baseModel, ObjectPool<QueryColumn> queryColumnPool);

    void moveGroupByFrom(IQueryModel model);

    void moveJoinAliasFrom(IQueryModel that);

    void moveLimitFrom(IQueryModel baseModel);

    void moveOrderByFrom(IQueryModel model);

    void moveSampleByFrom(IQueryModel model);

    IntList nextOrderedJoinModels();

    ObjList<ExpressionNode> parseWhereClause();

    void recordViews(LowerCaseCharSequenceObjHashMap<ViewDefinition> viewDefinitions);

    void recordViews(ObjList<ViewDefinition> viewDefinitions);

    void removeColumn(int columnIndex);

    void removeDependency(int index);

    void replaceColumn(int columnIndex, QueryColumn newColumn);

    void replaceColumnNameMap(CharSequence alias, CharSequence oldToken, CharSequence newToken);

    void replaceJoinModel(int pos, IQueryModel model);

    void setAlias(ExpressionNode alias);

    void setAllowPropagationOfOrderByAdvice(boolean value);

    void setArtificialStar(boolean artificialStar);

    void setAsOfJoinTolerance(ExpressionNode asOfJoinTolerance);

    void setBackupWhereClause(ExpressionNode backupWhereClause);

    void setCacheable(boolean b);

    void setConstWhereClause(ExpressionNode constWhereClause);

    void setContext(JoinContext context);

    void setDistinct(boolean distinct);

    void setExplicitTimestamp(boolean explicitTimestamp);

    void setFillFrom(ExpressionNode fillFrom);

    void setFillStride(ExpressionNode fillStride);

    void setFillTo(ExpressionNode fillTo);

    void setFillValues(ObjList<ExpressionNode> fillValues);

    void setForceBackwardScan(boolean forceBackwardScan);

    void setIsCteModel(boolean isCteModel);

    void setIsUpdate(boolean isUpdate);

    void setJoinCriteria(ExpressionNode joinCriteria);

    void setJoinKeywordPosition(int position);

    void setJoinType(int joinType);

    void setLatestByType(int latestByType);

    void setLimit(ExpressionNode lo, ExpressionNode hi);

    void setLimitAdvice(ExpressionNode lo, ExpressionNode hi);

    void setLimitPosition(int limitPosition);

    void setMetadataVersion(long metadataVersion);

    void setModelPosition(int modelPosition);

    void setModelType(int modelType);

    void setNestedModel(IQueryModel nestedModel);

    void setNestedModelIsSubQuery(boolean nestedModelIsSubQuery);

    void setOrderByAdviceMnemonic(int orderByAdviceMnemonic);

    void setOrderByPosition(int orderByPosition);

    void setOrderDescendingByDesignatedTimestampOnly(boolean orderDescendingByDesignatedTimestampOnly);

    void setOrderedJoinModels(IntList that);

    void setOriginatingViewNameExpr(ExpressionNode originatingViewNameExpr);

    void setOuterJoinExpressionClause(ExpressionNode outerJoinExpressionClause);

    void setPivotGroupByColumnHasNoAlias(boolean pivotGroupByColumnHasNoAlias);

    void setPostJoinWhereClause(ExpressionNode postJoinWhereClause);

    void setSampleBy(ExpressionNode sampleBy);

    void setSampleBy(ExpressionNode sampleBy, ExpressionNode sampleByUnit);

    void setSampleByFromTo(ExpressionNode from, ExpressionNode to);

    void setSampleByOffset(ExpressionNode sampleByOffset);

    void setSampleByTimezoneName(ExpressionNode sampleByTimezoneName);

    void setSelectModelType(int selectModelType);

    void setSelectTranslation(boolean isSelectTranslation);

    void setSetOperationType(int setOperationType);

    void setShowKind(int showKind);

    void setSkipped(boolean skipped);

    void setStandaloneUnnest(boolean standaloneUnnest);

    void setTableId(int id);

    void setTableNameExpr(ExpressionNode tableNameExpr);

    void setTableNameFunction(RecordCursorFactory function);

    void setTimestamp(ExpressionNode timestamp);

    void setTimestampColumnIndex(int index);

    void setTimestampOffsetAlias(CharSequence alias);

    void setTimestampOffsetUnit(char unit);

    void setTimestampOffsetValue(int value);

    void setTimestampSourceColumn(CharSequence col);

    void setUnionModel(IQueryModel unionModel);

    void setUnnestOrdinality(boolean unnestOrdinality);

    void setUpdateTableToken(TableToken tableName);

    void setViewNameExpr(ExpressionNode viewNameExpr);

    void setWhereClause(ExpressionNode whereClause);

    void toSink0(CharSink<?> sink, boolean joinSlave, boolean showOrderBy);

    String toString0();

    void updateColumnAliasIndexes();

    boolean windowStopPropagate();
}
