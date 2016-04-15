/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.model;

import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.ops.Parameter;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.std.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class QueryModel implements Mutable {
    public static final QueryModelFactory FACTORY = new QueryModelFactory();
    public static final int ORDER_DIRECTION_ASCENDING = 0;
    public static final int ORDER_DIRECTION_DESCENDING = 1;

    private final ObjList<QueryColumn> columns = new ObjList<>();
    private final ObjList<QueryModel> joinModels = new ObjList<>();
    private final ObjList<ExprNode> orderBy = new ObjList<>();
    private final IntList orderByDirection = new IntList();
    private final IntHashSet dependencies = new IntHashSet();
    private final IntList orderedJoinModels1 = new IntList();
    private final IntList orderedJoinModels2 = new IntList();
    private final IntList orderColumnIndices = new IntList();
    private final StringSink planSink = new StringSink();
    private final CharSequenceIntHashMap aliasIndexes = new CharSequenceIntHashMap();
    // collect frequency of column names from each join model
    // and check if any of columns with frequency > 0 are selected
    // column name frequency of 1 corresponds to map value 0
    // column name frequency of 0 corresponds to map value -1
    private final CharSequenceIntHashMap columnNameFrequencyMap = new CharSequenceIntHashMap();
    // list of "and" concatenated expressions
    private final ObjList<ExprNode> parsedWhere = new ObjList<>();
    private final IntHashSet parsedWhereConsts = new IntHashSet();
    private CharSequenceObjHashMap<Parameter> parameterMap = new CharSequenceObjHashMap<>();
    private ExprNode whereClause;
    private ExprNode postJoinWhereClause;
    private QueryModel nestedModel;
    private ExprNode journalName;
    private ExprNode alias;
    private ExprNode latestBy;
    private ExprNode timestamp;
    private ExprNode sampleBy;
    private RecordSource recordSource;
    private RecordMetadata metadata;
    private JoinContext context;
    private ExprNode joinCriteria;
    private JoinType joinType;
    private IntList orderedJoinModels = orderedJoinModels2;
    private ExprNode limitLo;
    private ExprNode limitHi;
    private VirtualColumn limitLoVc;
    private VirtualColumn limitHiVc;

    private QueryModel() {
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

    public void addJoinModel(QueryModel model) {
        joinModels.add(model);
    }

    public void addOrderBy(ExprNode node, int direction) {
        orderBy.add(node);
        orderByDirection.add(direction);
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
        sampleBy = null;
        orderBy.clear();
        orderByDirection.clear();
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
        parsedWhereConsts.clear();
        aliasIndexes.clear();
        postJoinWhereClause = null;
        context = null;
        orderedJoinModels = orderedJoinModels2;
        limitHi = null;
        limitLo = null;
        limitHiVc = null;
        limitLoVc = null;
        columnNameFrequencyMap.clear();
        parameterMap.clear();
        timestamp = null;
        orderColumnIndices.clear();
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

    public CharSequenceIntHashMap getColumnNameHistogram() {
        return columnNameFrequencyMap;
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

    public ExprNode getLimitHi() {
        return limitHi;
    }

    public VirtualColumn getLimitHiVc() {
        return limitHiVc;
    }

    public ExprNode getLimitLo() {
        return limitLo;
    }

    public VirtualColumn getLimitLoVc() {
        return limitLoVc;
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

    public IntList getOrderByDirection() {
        return orderByDirection;
    }

    public IntList getOrderColumnIndices() {
        return orderColumnIndices;
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

    public CharSequenceObjHashMap<Parameter> getParameterMap() {
        return parameterMap;
    }

    public void setParameterMap(CharSequenceObjHashMap<Parameter> parameterMap) {
        this.parameterMap = parameterMap;
    }

    public ObjList<ExprNode> getParsedWhere() {
        return parsedWhere;
    }

    public IntHashSet getParsedWhereConsts() {
        return parsedWhereConsts;
    }

    public ExprNode getPostJoinWhereClause() {
        return postJoinWhereClause;
    }

    public void setPostJoinWhereClause(ExprNode postJoinWhereClause) {
        this.postJoinWhereClause = postJoinWhereClause;
    }

    public RecordSource getRecordSource() {
        return recordSource;
    }

    public void setRecordSource(RecordSource recordSource) {
        this.recordSource = recordSource;
    }

    public ExprNode getSampleBy() {
        return sampleBy;
    }

    public void setSampleBy(ExprNode sampleBy) {
        this.sampleBy = sampleBy;
    }

    public ExprNode getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(ExprNode timestamp) {
        this.timestamp = timestamp;
    }

    public ExprNode getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(ExprNode whereClause) {
        this.whereClause = whereClause;
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

    public CharSequence plan() {
        planSink.clear();
        plan(planSink, 0);
        return planSink;
    }

    public void removeDependency(int index) {
        dependencies.remove(index);
    }

    public void setLimit(ExprNode lo, ExprNode hi) {
        this.limitLo = lo;
        this.limitHi = hi;
    }

    public void setLimitVc(VirtualColumn lo, VirtualColumn hi) {
        this.limitLoVc = lo;
        this.limitHiVc = hi;
    }

    @SuppressFBWarnings("ISB_TOSTRING_APPENDING")
    @Override
    public String toString() {
        return alias != null ? alias.token : (journalName != null ? journalName.token : '{' + nestedModel.toString() + '}');
    }

    private void plan(StringSink sink, int pad) {
        ObjList<QueryModel> joinModels = getJoinModels();
        if (joinModels.size() > 1) {
            IntList ordered = getOrderedJoinModels();
            for (int i = 0, n = ordered.size(); i < n; i++) {
                final int index = ordered.getQuick(i);
                final QueryModel m = joinModels.getQuick(index);
                final JoinContext jc = m.getContext();

//                final boolean cross = jc == null || jc.parents.size() == 0;
                sink.put(' ', pad).put('+').put(' ').put(index);

                // join type
                sink.put('[').put(' ');
                switch (m.getJoinType()) {
                    case CROSS:
                        sink.put("cross");
                        break;
                    case INNER:
                        sink.put("inner");
                        break;
                    case OUTER:
                        sink.put("outer");
                        break;
                    case ASOF:
                        sink.put("asof");
                        break;
                    default:
                        sink.put("unknown");
                        break;
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
                if (jc != null && jc.aIndexes.size() > 0) {
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
        INNER, OUTER, CROSS, ASOF
    }

    public static final class QueryModelFactory implements ObjectFactory<QueryModel> {
        @Override
        public QueryModel newInstance() {
            return new QueryModel();
        }
    }
}