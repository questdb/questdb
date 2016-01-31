/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

import com.nfsdb.std.Mutable;
import com.nfsdb.std.ObjectFactory;

public final class QueryColumn implements Mutable {
    public final static ObjectFactory<QueryColumn> FACTORY = new ObjectFactory<QueryColumn>() {
        @Override
        public QueryColumn newInstance() {
            return new QueryColumn();
        }
    };
    private String alias;
    private ExprNode ast;

    private QueryColumn() {
    }

    @Override
    public void clear() {
        alias = null;
        ast = null;
    }

    public String getAlias() {
        return alias;
    }

    public ExprNode getAst() {
        return ast;
    }

    @Override
    public int hashCode() {
        return alias.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryColumn that = (QueryColumn) o;

        return alias.equals(that.alias);

    }

    public QueryColumn of(String alias, ExprNode ast) {
        this.alias = alias;
        this.ast = ast;
        return this;
    }
}
