/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/
package com.nfsdb.ql.model;

import com.nfsdb.collections.ObjList;

public class QueryModel {
    private final ObjList<QueryColumn> columns = new ObjList<>();
    private final ObjList<JoinModel> joinModels = new ObjList<>();
    private final ObjList<String> groupBy = new ObjList<>();
    private final ObjList<ExprNode> orderBy = new ObjList<>();
    private ExprNode whereClause;
    private QueryModel nestedModel;
    private ExprNode journalName;
    private String alias;
    private ExprNode latestBy;

    public void addColumn(QueryColumn column) {
        columns.add(column);
    }

    public void addGroupBy(String name) {
        groupBy.add(name);
    }

    public void addJoinModel(JoinModel model) {
        joinModels.add(model);
    }

    public void addOrderBy(ExprNode node) {
        orderBy.add(node);
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public ObjList<QueryColumn> getColumns() {
        return columns;
    }

    public ObjList<String> getGroupBy() {
        return groupBy;
    }

    public ObjList<JoinModel> getJoinModels() {
        return joinModels;
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

    public QueryModel getNestedModel() {
        return nestedModel;
    }

    public void setNestedModel(QueryModel nestedModel) {
        this.nestedModel = nestedModel;
    }

    public ObjList<ExprNode> getOrderBy() {
        return orderBy;
    }

    public ExprNode getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(ExprNode whereClause) {
        this.whereClause = whereClause;
    }
}
