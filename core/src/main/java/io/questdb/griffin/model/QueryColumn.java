/*******************************************************************************
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

import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

public class QueryColumn implements Mutable, Sinkable {
    public static final ObjectFactory<QueryColumn> FACTORY = QueryColumn::new;
    public static final int SYNTHESIZED_ALIAS_POSITION = -1;
    private CharSequence alias;
    private int aliasPosition;
    private ExpressionNode ast;
    private int columnType = -1;
    private boolean includeIntoWildcard = true;

    public QueryColumn() {
    }

    @Override
    public void clear() {
        alias = null;
        aliasPosition = 0;
        ast = null;
        includeIntoWildcard = true;
        columnType = -1;
    }

    public CharSequence getAlias() {
        return alias;
    }

    public int getAliasPosition() {
        return aliasPosition;
    }

    public ExpressionNode getAst() {
        return ast;
    }

    public int getColumnType() {
        return columnType;
    }

    public CharSequence getName() {
        return alias != null ? alias : ast.token;
    }

    public boolean isIncludeIntoWildcard() {
        return includeIntoWildcard;
    }

    public boolean isWindowExpression() {
        return false;
    }

    public QueryColumn of(CharSequence alias, ExpressionNode ast) {
        return of(alias, ast, true);
    }

    public QueryColumn of(CharSequence alias, ExpressionNode ast, boolean includeIntoWildcard) {
        return of(alias, ast, includeIntoWildcard, -1);
    }

    public QueryColumn of(CharSequence alias, ExpressionNode ast, boolean includeIntoWildcard, int type) {
        this.alias = alias;
        this.ast = ast;
        this.includeIntoWildcard = includeIntoWildcard;
        this.columnType = type;
        return this;
    }

    public void setAlias(CharSequence alias, int aliasPosition) {
        if (this.alias == alias || Chars.equalsNc(alias, this.alias)) {
            return;
        }
        this.alias = alias;
        this.aliasPosition = aliasPosition;
    }

    public void setIncludeIntoWildcard(boolean includeIntoWildcard) {
        this.includeIntoWildcard = includeIntoWildcard;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put(ast).putAscii(" as ").put(alias);
    }
}
