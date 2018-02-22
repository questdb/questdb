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

package com.questdb.griffin.lexer.model;

import com.questdb.cairo.CairoException;
import com.questdb.cairo.TableReader;
import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.common.JournalRuntimeException;
import com.questdb.common.RecordMetadata;
import com.questdb.griffin.common.ExprNode;
import com.questdb.griffin.common.VirtualColumn;
import com.questdb.griffin.compiler.Parameter;
import com.questdb.griffin.lexer.ParserException;
import com.questdb.ql.RecordSource;
import com.questdb.std.*;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;

import java.util.ArrayDeque;

public class QueryModel implements Mutable, ParsedModel, AliasTranslator, Sinkable {
    public static final QueryModelFactory FACTORY = new QueryModelFactory();
    public static final int ORDER_DIRECTION_ASCENDING = 0;
    public static final int ORDER_DIRECTION_DESCENDING = 1;
    public static final String NO_ROWID_MARKER = "*!*";
    public static final int JOIN_INNER = 1;
    public static final int JOIN_OUTER = 2;
    public static final int JOIN_CROSS = 3;
    public static final int JOIN_ASOF = 4;
    public static final String SUB_QUERY_ALIAS_PREFIX = "_xQdbA";
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
    private ExprNode tableName;
    private ExprNode alias;
    private ExprNode latestBy;
    private ExprNode timestamp;
    private ExprNode sampleBy;
    private RecordSource recordSource;
    private JoinContext context;
    private ExprNode joinCriteria;
    private int joinType;
    private IntList orderedJoinModels = orderedJoinModels2;
    private ExprNode limitLo;
    private ExprNode limitHi;
    private VirtualColumn limitLoVc;
    private VirtualColumn limitHiVc;

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
        tableName = null;
        alias = null;
        latestBy = null;
        recordSource = null;
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
        joinColumns.clear();
        withClauses.clear();
    }

    public void createColumnNameHistogram(CairoEngine engine) throws ParserException {
        columnNameHistogram.clear();

        // We cannot have selected columns and multiple join models at the same time
        // when this is the case in SQL expression join models have to be segregated from
        // selected columns into nested model. This way both join result and selected
        // columns can have their own column histograms.

        ObjList<QueryModel> jm = getJoinModels();
        int n = getColumns().size();
        assert (jm.size() <= 1 || n <= 0);

        if (n > 0) {
            assert getNestedModel() != null;

            for (int i = 0; i < n; i++) {
                QueryColumn qc = getColumns().getQuick(i);
                switch (qc.getAst().type) {
                    case ExprNode.LITERAL:
                        String name = qc.getName();
                        int dot = name.indexOf('.');
                        if (dot != -1) {
                            qc.of(name.substring(dot + 1), qc.getAliasPosition(), qc.getAst());
                        }
                        // name is the alias if it exists, deliberately not using local variable
                        columnNameHistogram.increment(qc.getName());
                        break;
                    default:
                        break;
                }
            }

            // also also build histogram for nested model
            getNestedModel().createColumnNameHistogram(engine);

        } else {
            // we have plain tables and possibly joins
            // deal with _this_ model first, it will always be the first element in join model list
            if (getTableName() != null) {
                RecordMetadata m = getTableMetadata(engine);
                for (int i = 0, k = m.getColumnCount(); i < k; i++) {
                    columnNameHistogram.increment(m.getColumnName(i));
                }
            } else {
                assert getNestedModel() != null;
                getNestedModel().createColumnNameHistogram(engine);
                // copy columns of nested model onto parent one
                // we must treat sub-query just like we do a table
                CharSequenceIntHashMap nameHistogram = getColumnNameHistogram();
                nameHistogram.clear();
                nameHistogram.putAll(getNestedModel().getColumnNameHistogram());
            }

            n = jm.size();
            for (int i = 1; i < n; i++) {
                jm.getQuick(i).createColumnNameHistogram(engine);
            }
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

    public RecordMetadata getTableMetadata(CairoEngine engine) throws ParserException {

        // todo: this method generates garbage - fix pls

        ExprNode readerNode = getTableName();
        if (readerNode.type != ExprNode.LITERAL && readerNode.type != ExprNode.CONSTANT) {
            throw ParserException.$(readerNode.position, "Journal name must be either literal or string constant");
        }

        String reader = stripMarker(Chars.stripQuotes(readerNode.token));

        int status = engine.getStatus(reader);

        if (status == TableUtils.TABLE_DOES_NOT_EXIST) {
            throw ParserException.$(readerNode.position, "table does not exist");
        }

        if (status == TableUtils.TABLE_RESERVED) {
            throw ParserException.$(readerNode.position, "table directory is of unknown format");
        }

        try (TableReader r = engine.getReader(reader)) {
            return r.getMetadata();
        } catch (CairoException e) {
            throw ParserException.$(readerNode.position, e.getMessage());
        }
    }

    public ExprNode getTableName() {
        return tableName;
    }

    public void setTableName(ExprNode tableName) {
        this.tableName = tableName;
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
    public void toSink(CharSink sink) {
        if (columns.size() > 0) {
            sink.put("select ");
            for (int i = 0, n = columns.size(); i < n; i++) {
                if (i > 0) {
                    sink.put(", ");
                }
                QueryColumn column = columns.getQuick(i);
                String name = column.getName();
                String alias = column.getAlias();
                if (column instanceof AnalyticColumn || name == null) {
                    column.getAst().toSink(sink);

                    if (alias != null) {
                        aliasToSink(alias, sink);
                    }

                    // this can only be analytic column
                    if (name != null) {
                        AnalyticColumn ac = (AnalyticColumn) column;
                        sink.put(" over (");
                        final ObjList<ExprNode> partitionBy = ac.getPartitionBy();
                        if (partitionBy.size() > 0) {
                            sink.put("partition by ");
                            for (int k = 0, z = partitionBy.size(); k < z; k++) {
                                if (k > 0) {
                                    sink.put(", ");
                                }
                                partitionBy.getQuick(k).toSink(sink);
                            }
                        }

                        final ObjList<ExprNode> orderBy = ac.getOrderBy();
                        if (orderBy.size() > 0) {
                            if (partitionBy.size() > 0) {
                                sink.put(' ');
                            }
                            sink.put("order by ");
                            for (int k = 0, z = orderBy.size(); k < z; k++) {
                                if (k > 0) {
                                    sink.put(", ");
                                }
                                orderBy.getQuick(k).toSink(sink);
                                if (ac.getOrderByDirection().getQuick(k) == 1) {
                                    sink.put(" desc");
                                }
                            }
                        }
                        sink.put(')');
                    }
                } else {
                    if (column.getAst() != null) {
                        column.getAst().toSink(sink);
                    } else {
                        sink.put(name);
                    }

                    if (alias != null) {
                        aliasToSink(alias, sink);
                    }
                }
            }
            sink.put(" from ");
        }
        if (tableName != null) {
            sink.put(tableName.token);
            if (alias != null) {
                aliasToSink(alias.token, sink);
            }
        } else {
            sink.put('(');
            nestedModel.toSink(sink);
            sink.put(')');
            if (alias != null) {
                aliasToSink(alias.token, sink);
            }
        }

        if (timestamp != null) {
            sink.put(" timestamp (");
            timestamp.toSink(sink);
            sink.put(')');
        }

        if (latestBy != null) {
            sink.put(" latest by ");
            latestBy.toSink(sink);
        }

        if (orderedJoinModels.size() > 1) {
            for (int i = 0, n = orderedJoinModels.size(); i < n; i++) {
                int index = orderedJoinModels.getQuick(i);
                QueryModel model = joinModels.getQuick(index);
                if (model == this) {
                    continue;
                }
                switch (model.getJoinType()) {
                    case JOIN_OUTER:
                        sink.put(" outer join ");
                        break;
                    case JOIN_ASOF:
                        sink.put(" asof join ");
                        break;
                    case JOIN_CROSS:
                        sink.put(" cross join ");
                        break;
                    default:
                        sink.put(" join ");
                }

                model.toSink(sink);

                if (model.getJoinCriteria() != null) {
                    sink.put(" on ");
                    model.getJoinCriteria().toSink(sink);
                }
            }
        }

        if (whereClause != null) {
            sink.put(" where ");
            whereClause.toSink(sink);
        }

        if (sampleBy != null) {
            sink.put(" sample by ");
            sampleBy.toSink(sink);
        }

        if (orderBy.size() > 0) {
            sink.put(" order by ");
            for (int i = 0, n = orderBy.size(); i < n; i++) {
                if (i > 0) {
                    sink.put(", ");
                }
                orderBy.getQuick(i).toSink(sink);
                if (orderByDirection.getQuick(i) == 1) {
                    sink.put(" desc");
                }
            }
        }

        if (limitLo != null || limitHi != null) {
            sink.put(" limit ");
            if (limitLo != null) {
                limitLo.toSink(sink);
            }
            if (limitHi != null) {
                sink.put(',');
                limitHi.toSink(sink);
            }
        }
    }

    @Override
    public String toString() {
        return alias != null ? alias.token : (tableName != null ? tableName.token : '{' + nestedModel.toString() + '}');
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

    private static void aliasToSink(CharSequence alias, CharSink sink) {
        if (Chars.startsWith(alias, SUB_QUERY_ALIAS_PREFIX)) {
            return;
        }
        sink.put(' ');
        boolean quote = Chars.indexOf(alias, ' ') != -1;
        if (quote) {
            sink.put('\'').put(alias).put('\'');
        } else {
            sink.put(alias);
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
                } else if (m.getTableName() != null) {
                    sink.put(m.getTableName().token);
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
            } else if (getTableName() != null) {
                sink.put(getTableName().token);
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