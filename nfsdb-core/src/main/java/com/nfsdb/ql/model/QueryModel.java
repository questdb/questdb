/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.model;

import com.nfsdb.collections.*;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.ql.RecordSource;

public class QueryModel implements Mutable {
    public static final QueryModelFactory FACTORY = new QueryModelFactory();

    private final ObjList<QueryColumn> columns = new ObjList<>();
    private final ObjList<QueryModel> joinModels = new ObjList<>();
    private final ObjList<String> groupBy = new ObjList<>();
    private final ObjList<ExprNode> orderBy = new ObjList<>();
    private final IntHashSet dependencies = new IntHashSet();
    private final IntList orderedJoinModels1 = new IntList();
    private final IntList orderedJoinModels2 = new IntList();
    private final StringSink planSink = new StringSink();
    private final CharSequenceIntHashMap aliasIndexes = new CharSequenceIntHashMap();
    // list of "and" concatenated expressions
    private final ObjList<ExprNode> parsedWhere = new ObjList<>();
    private final IntHashSet parsedWhereConsts = new IntHashSet();
    private ExprNode whereClause;
    private ExprNode postJoinWhereClause;
    private QueryModel nestedModel;
    private ExprNode journalName;
    private ExprNode alias;
    private ExprNode latestBy;
    private RecordSource<? extends Record> recordSource;
    private RecordMetadata metadata;
    private JoinContext context;
    private ExprNode joinCriteria;
    private JoinType joinType;
    private IntList orderedJoinModels = orderedJoinModels2;

    protected QueryModel() {
        joinModels.add(this);
    }

    public boolean addAliasIndex(ExprNode node, int index) {
        return aliasIndexes.put(node.token, index);
    }

    public void addColumn(QueryColumn column) {
        columns.add(column);
    }

    public void addDependency(int index) {
        dependencies.add(index);
    }

    public void addGroupBy(String name) {
        groupBy.add(name);
    }

    public void addJoinModel(QueryModel model) {
        joinModels.add(model);
    }

    public void addOrderBy(ExprNode node) {
        orderBy.add(node);
    }

    public void addParsedWhereConst(int index) {
        parsedWhereConsts.add(index);
    }

    public void addParsedWhereNode(ExprNode node) {
        parsedWhere.add(node);
    }

    public void clear() {
        columns.clear();
        joinModels.clear();
        joinModels.add(this);
        groupBy.clear();
        orderBy.clear();
        dependencies.clear();
        parsedWhere.clear();
        whereClause = null;
        nestedModel = null;
        journalName = null;
        alias = null;
        latestBy = null;
        recordSource = null;
        metadata = null;
        joinCriteria = null;
        joinType = JoinType.INNER;
        orderedJoinModels1.clear();
        orderedJoinModels2.clear();
    }

    public ExprNode getAlias() {
        return alias;
    }

    public void setAlias(ExprNode alias) {
        this.alias = alias;
    }

    public int getAliasIndex(CharSequence alias) {
        return aliasIndexes.get(alias);
    }

    public ObjList<QueryColumn> getColumns() {
        return columns;
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

    public ObjList<String> getGroupBy() {
        return groupBy;
    }

    public ExprNode getJoinCriteria() {
        return joinCriteria;
    }

    public void setJoinCriteria(ExprNode joinCriteria) {
        this.joinCriteria = joinCriteria;
    }

    public ObjList<QueryModel> getJoinModels() {
        return joinModels;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

    public ExprNode getJournalName() {
        return journalName;
    }

    public void setJournalName(ExprNode journalName) {
        this.journalName = journalName;
    }

    public ExprNode getLatestBy() {
        return latestBy;
    }

    public void setLatestBy(ExprNode latestBy) {
        this.latestBy = latestBy;
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(RecordMetadata metadata) {
        this.metadata = metadata;
    }

    public QueryModel getNestedModel() {
        return nestedModel;
    }

    public void setNestedModel(QueryModel nestedModel) {
        this.nestedModel = nestedModel;
    }

    public ObjList<ExprNode> getOrderBy() {
        return orderBy;
    }

    public IntList getOrderedJoinModels() {
        return orderedJoinModels;
    }

    public void setOrderedJoinModels(IntList that) {
        if (that != orderedJoinModels1 && that != orderedJoinModels2) {
            throw new JournalRuntimeException("Passing foreign list breaks convention");
        }
        this.orderedJoinModels = that;
    }

    public ObjList<ExprNode> getParsedWhere() {
        return parsedWhere;
    }

    public ExprNode getPostJoinWhereClause() {
        return postJoinWhereClause;
    }

    public void setPostJoinWhereClause(ExprNode postJoinWhereClause) {
        this.postJoinWhereClause = postJoinWhereClause;
    }

    public RecordSource<? extends Record> getRecordSource() {
        return recordSource;
    }

    public void setRecordSource(RecordSource<? extends Record> recordSource) {
        this.recordSource = recordSource;
    }

    public ExprNode getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(ExprNode whereClause) {
        this.whereClause = whereClause;
    }

    public boolean isCrossJoin() {
        return joinType == JoinType.CROSS || context == null || context.parents.size() == 0;
    }

    /**
     * Optimiser may be attempting to order join clauses several times.
     * Every time ordering takes place optimiser will keep at most two lists:
     * one is last known order the other is new order. If new order cost is better
     * optimiser will replace last known order with new one.
     * <p/>
     * To facilitate this behaviour the function will always return non-current list.
     *
     * @return non current order list.
     */
    public IntList nextOrderedJoinModels() {
        IntList ordered = orderedJoinModels == orderedJoinModels1 ? orderedJoinModels2 : orderedJoinModels1;
        ordered.clear();
        return ordered;
    }

    public CharSequence plan() {
        planSink.clear();
        plan(planSink, 0);
        return planSink;
    }

    public void removeDependency(int index) {
        dependencies.remove(index);
    }

    @Override
    public String toString() {
        return alias != null ? alias.token : (journalName != null ? journalName.token : "{" + nestedModel.toString() + "}");
    }

    private void plan(StringSink sink, int pad) {
        ObjList<QueryModel> joinModels = getJoinModels();
        if (joinModels.size() > 1) {
            IntList ordered = getOrderedJoinModels();
            for (int i = 0, n = ordered.size(); i < n; i++) {
                final int index = ordered.getQuick(i);
                final QueryModel m = joinModels.getQuick(index);
                final JoinContext jc = m.getContext();

                final boolean cross = jc == null || jc.parents.size() == 0;
                sink.put(' ', pad).put('+').put(' ').put(index);

                // join type
                sink.put('[').put(' ');
                if (m.getJoinType() == QueryModel.JoinType.CROSS || cross) {
                    sink.put("cross");
                } else if (m.getJoinType() == QueryModel.JoinType.INNER) {
                    sink.put("inner");
                } else {
                    sink.put("outer");
                }
                sink.put(' ').put(']').put(' ');

                // journal name/alias
                if (m.getAlias() != null) {
                    sink.put(m.getAlias().token);
                } else if (m.getJournalName() != null) {
                    sink.put(m.getJournalName().token);
                } else {
                    sink.put('{').put('\n');
                    m.getNestedModel().plan(sink, pad + 2);
                    sink.put('}');
                }

                // pre-filter
                ExprNode filter = getJoinModels().getQuick(index).getWhereClause();
                if (filter != null) {
                    sink.put(" (filter: ");
                    filter.toString(sink);
                    sink.put(')');
                }

                // join clause
                if (!cross && jc.aIndexes.size() > 0) {
                    sink.put(" ON ");
                    for (int k = 0, z = jc.aIndexes.size(); k < z; k++) {
                        if (k > 0) {
                            sink.put(" and ");
                        }
                        jc.aNodes.getQuick(k).toString(sink);
                        sink.put(" = ");
                        jc.bNodes.getQuick(k).toString(sink);
                    }
                }

                // post-filter
                filter = m.getPostJoinWhereClause();
                if (filter != null) {
                    sink.put(" (post-filter: ");
                    filter.toString(sink);
                    sink.put(')');
                }
                sink.put('\n');
            }
        } else {
            sink.put(' ', pad);
            // journal name/alias
            if (getAlias() != null) {
                sink.put(getAlias().token);
            } else if (getJournalName() != null) {
                sink.put(getJournalName().token);
            } else {
                getNestedModel().plan(sink, pad + 2);
            }

            // pre-filter
            ExprNode filter = getWhereClause();
            if (filter != null) {
                sink.put(" (filter: ");
                filter.toString(sink);
                sink.put(')');
            }

            ExprNode latestBy = getLatestBy();
            if (latestBy != null) {
                sink.put(" (latest by: ");
                latestBy.toString(sink);
                sink.put(')');
            }

        }
        sink.put('\n');
    }

    public enum JoinType {
        INNER, OUTER, CROSS
    }

    public static final class QueryModelFactory implements ObjectPoolFactory<QueryModel> {
        @Override
        public QueryModel newInstance() {
            return new QueryModel();
        }
    }
}