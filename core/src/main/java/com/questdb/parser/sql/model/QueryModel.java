/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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
 ******************************************************************************/

package com.questdb.parser.sql.model;

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.QueryError;
import com.questdb.ql.RecordSource;
import com.questdb.ql.ops.Parameter;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.ql.sys.SysFactories;
import com.questdb.ql.sys.SystemViewFactory;
import com.questdb.std.*;
import com.questdb.std.ex.JournalException;
import com.questdb.std.str.StringSink;
import com.questdb.store.JournalRuntimeException;
import com.questdb.store.RecordMetadata;
import com.questdb.store.factory.Factory;
import com.questdb.store.factory.configuration.JournalConfiguration;
import com.questdb.store.factory.configuration.JournalMetadata;

import java.util.ArrayDeque;

public class QueryModel implements Mutable, ParsedModel, AliasTranslator {
    public static final QueryModelFactory FACTORY = new QueryModelFactory();
    public static final int ORDER_DIRECTION_ASCENDING = 0;
    public static final int ORDER_DIRECTION_DESCENDING = 1;
    public static final String NO_ROWID_MARKER = "*!*";
    public static final int JOIN_INNER = 1;
    public static final int JOIN_OUTER = 2;
    public static final int JOIN_CROSS = 3;
    public static final int JOIN_ASOF = 4;
    private final ObjList<QueryColumn> columns = new ObjList<>();
    private final CharSequenceObjHashMap<QueryColumn> aliasToColumnMap = new CharSequenceObjHashMap<>();
    private final ObjList<QueryModel> joinModels = new ObjList<>();
    private final ObjList<ExprNode> orderBy = new ObjList<>();
    private final IntList orderByDirection = new IntList();
    private final IntHashSet dependencies = new IntHashSet();
    private final IntList orderedJoinModels1 = new IntList();
    private final IntList orderedJoinModels2 = new IntList();
    private final StringSink planSink = new StringSink();
    private final CharSequenceIntHashMap aliasIndexes = new CharSequenceIntHashMap();
    // collect frequency of column names from each join model
    // and check if any of columns with frequency > 0 are selected
    // column name frequency of 1 corresponds to map value 0
    // column name frequency of 0 corresponds to map value -1
    private final CharSequenceIntHashMap columnNameHistogram = new CharSequenceIntHashMap();
    // list of "and" concatenated expressions
    private final ObjList<ExprNode> parsedWhere = new ObjList<>();
    private final IntHashSet parsedWhereConsts = new IntHashSet();
    private final ArrayDeque<ExprNode> exprNodeStack = new ArrayDeque<>();
    private final CharSequenceIntHashMap orderHash = new CharSequenceIntHashMap(4, 0.5, -1);
    private final ObjList<ExprNode> joinColumns = new ObjList<>(4);
    private final CharSequenceObjHashMap<WithClauseModel> withClauses = new CharSequenceObjHashMap<>();
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
    private int joinType;
    private IntList orderedJoinModels = orderedJoinModels2;
    private ExprNode limitLo;
    private ExprNode limitHi;
    private VirtualColumn limitLoVc;
    private VirtualColumn limitHiVc;
    private JournalMetadata journalMetadata;

    private QueryModel() {
        joinModels.add(this);
    }

    public static boolean hasMarker(String name) {
        return name.indexOf(NO_ROWID_MARKER) == 0;
    }

    public static String stripMarker(String name) {
        return hasMarker(name) ? name.substring(NO_ROWID_MARKER.length()) : name;
    }

    public boolean addAliasIndex(ExprNode node, int index) {
        return aliasIndexes.put(node.token, index);
    }

    public void addColumn(QueryColumn column) {
        columns.add(column);
        if (column.getAlias() != null) {
            aliasToColumnMap.put(column.getAlias(), column);
        }
    }

    public void addDependency(int index) {
        dependencies.add(index);
    }

    public void addJoinColumn(ExprNode node) {
        joinColumns.add(node);
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

    public void addWithClause(String name, WithClauseModel model) {
        withClauses.put(name, model);
    }

    public void clear() {
        columns.clear();
        aliasToColumnMap.clear();
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
        joinType = JOIN_INNER;
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
        columnNameHistogram.clear();
        parameterMap.clear();
        timestamp = null;
        exprNodeStack.clear();
        journalMetadata = null;
        joinColumns.clear();
        withClauses.clear();
    }

    public RecordMetadata collectJournalMetadata(Factory factory) throws ParserException {
        if (journalMetadata != null) {
            return journalMetadata;
        }

        ExprNode readerNode = getJournalName();
        if (readerNode.type != ExprNode.LITERAL && readerNode.type != ExprNode.CONSTANT) {
            throw QueryError.$(readerNode.position, "Journal name must be either literal or string constant");
        }

        String reader = stripMarker(Chars.stripQuotes(readerNode.token));

        SystemViewFactory systemViewFactory = SysFactories.getFactory(reader);

        if (systemViewFactory != null) {
            return systemViewFactory.getMetadata();
        }

        int status = factory.getConfiguration().exists(reader);

        if (status == JournalConfiguration.DOES_NOT_EXIST) {
            throw QueryError.$(readerNode.position, "Journal does not exist");
        }

        if (status == JournalConfiguration.EXISTS_FOREIGN) {
            throw QueryError.$(readerNode.position, "Journal directory is of unknown format");
        }

        try {
            return journalMetadata = factory.getConfiguration().readMetadata(reader);
        } catch (JournalException e) {
            throw QueryError.$(readerNode.position, e.getMessage());
        }
    }

    public void createColumnNameHistogram(Factory factory) throws ParserException {
        columnNameHistogram.clear();
        createColumnNameHistogram0(columnNameHistogram, this, factory, false);
    }

    public void createColumnNameHistogram(RecordSource rs) {
        RecordMetadata m = rs.getMetadata();
        for (int i = 0, n = m.getColumnCount(); i < n; i++) {
            columnNameHistogram.increment(m.getColumnName(i));
        }
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
        return columnNameHistogram;
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

    public ObjList<ExprNode> getJoinColumns() {
        return joinColumns;
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

    public int getJoinType() {
        return joinType;
    }

    public void setJoinType(int joinType) {
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

    @Override
    public int getModelType() {
        return ParsedModel.QUERY;
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

    public CharSequenceIntHashMap getOrderHash() {
        return orderHash;
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

    public WithClauseModel getWithClause(CharSequence name) {
        return withClauses.get(name);
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
    public ObjList<ExprNode> parseWhereClause() {
        ExprNode n = getWhereClause();
        // pre-order traversal
        exprNodeStack.clear();
        while (!exprNodeStack.isEmpty() || n != null) {
            if (n != null) {
                switch (n.token) {
                    case "and":
                        if (n.rhs != null) {
                            exprNodeStack.push(n.rhs);
                        }
                        n = n.lhs;
                        break;
                    default:
                        addParsedWhereNode(n);
                        n = null;
                        break;
                }
            } else {
                n = exprNodeStack.poll();
            }
        }
        return getParsedWhere();
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

    @Override
    public String toString() {
        return alias != null ? alias.token : (journalName != null ? journalName.token : '{' + nestedModel.toString() + '}');
    }

    @Override
    public CharSequence translateAlias(CharSequence column) {
        QueryColumn referent = aliasToColumnMap.get(column);
        if (referent != null
                && !(referent instanceof AnalyticColumn)
                && referent.getAst().type == ExprNode.LITERAL) {
            return referent.getAst().token;
        } else {
            return column;
        }
    }

    private static void createColumnNameHistogram0(CharSequenceIntHashMap histogram, QueryModel model, Factory factory, boolean ignoreJoins) throws ParserException {
        ObjList<QueryModel> jm = model.getJoinModels();
        int jmSize = ignoreJoins ? 0 : jm.size();

        int n = model.getColumns().size();
        if (n > 0) {
            for (int i = 0; i < n; i++) {
                QueryColumn qc = model.getColumns().getQuick(i);
                switch (qc.getAst().type) {
                    case ExprNode.LITERAL:
                        histogram.increment(qc.getName());
                        break;
                    default:
                        break;
                }
            }
        } else if (jmSize > 0) {
            for (int i = 0; i < jmSize; i++) {
                createColumnNameHistogram0(histogram, jm.getQuick(i), factory, true);
            }
        } else if (model.getJournalName() != null) {
            RecordMetadata m = model.collectJournalMetadata(factory);
            for (int i = 0, k = m.getColumnCount(); i < k; i++) {
                histogram.increment(m.getColumnName(i));
            }
        } else {
            createColumnNameHistogram0(histogram, model.getNestedModel(), factory, false);
        }
    }

    private void plan(StringSink sink, int pad) {
        ObjList<QueryModel> joinModels = getJoinModels();
        if (joinModels.size() > 1) {
            IntList ordered = getOrderedJoinModels();
            for (int i = 0, n = ordered.size(); i < n; i++) {
                final int index = ordered.getQuick(i);
                final QueryModel m = joinModels.getQuick(index);
                final JoinContext jc = m.getContext();

                sink.put(' ', pad).put('+').put(' ').put(index);

                // join type
                sink.put('[').put(' ');
                switch (m.getJoinType()) {
                    case JOIN_CROSS:
                        sink.put("cross");
                        break;
                    case JOIN_INNER:
                        sink.put("inner");
                        break;
                    case JOIN_OUTER:
                        sink.put("outer");
                        break;
                    case JOIN_ASOF:
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
                    filter.toSink(sink);
                    sink.put(')');
                }

                // join clause
                if (jc != null && jc.aIndexes.size() > 0) {
                    sink.put(" ON ");
                    for (int k = 0, z = jc.aIndexes.size(); k < z; k++) {
                        if (k > 0) {
                            sink.put(" and ");
                        }
                        jc.aNodes.getQuick(k).toSink(sink);
                        sink.put(" = ");
                        jc.bNodes.getQuick(k).toSink(sink);
                    }
                }

                // post-filter
                filter = m.getPostJoinWhereClause();
                if (filter != null) {
                    sink.put(" (post-filter: ");
                    filter.toSink(sink);
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
                filter.toSink(sink);
                sink.put(')');
            }

            ExprNode latestBy = getLatestBy();
            if (latestBy != null) {
                sink.put(" (latest by: ");
                latestBy.toSink(sink);
                sink.put(')');
            }

        }
        sink.put('\n');
    }

    private static final class QueryModelFactory implements ObjectFactory<QueryModel> {
        @Override
        public QueryModel newInstance() {
            return new QueryModel();
        }
    }
}