/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

public class JoinModel {
    private QueryModel nestedQuery;
    private ExprNode journalName;
    private String alias;
    private ExprNode joinCriteria;

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public ExprNode getJoinCriteria() {
        return joinCriteria;
    }

    public void setJoinCriteria(ExprNode joinCriteria) {
        this.joinCriteria = joinCriteria;
    }

    public ExprNode getJournalName() {
        return journalName;
    }

    public void setJournalName(ExprNode journalName) {
        this.journalName = journalName;
    }

    public QueryModel getNestedQuery() {
        return nestedQuery;
    }

    public void setNestedQuery(QueryModel nestedQuery) {
        this.nestedQuery = nestedQuery;
    }
}
