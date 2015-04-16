/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb.ql.model;

import com.nfsdb.collections.ObjList;

public class QueryModel {
    private final ObjList<QueryColumn> columns = new ObjList<>();
    private final ObjList<String> groupByValues = new ObjList<>();
    private final ObjList<ExprNode> whereClauses = new ObjList<>();
    private String journalName;
    private ExprNode mostRecentBy;

    public void addColumn(QueryColumn column) {
        columns.add(column);
    }

    public void addWhereClause(ExprNode node) {
        whereClauses.add(node);
    }

    public ObjList<QueryColumn> getColumns() {
        return columns;
    }

    public String getJournalName() {
        return journalName;
    }

    public void setJournalName(String journalName) {
        this.journalName = journalName;
    }

    public ExprNode getMostRecentBy() {
        return mostRecentBy;
    }

    public void setMostRecentBy(ExprNode mostRecentBy) {
        this.mostRecentBy = mostRecentBy;
    }

    public ObjList<ExprNode> getWhereClauses() {
        return whereClauses;
    }
}
