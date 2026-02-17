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

import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

public final class PivotForColumn implements Mutable {
    public final static ObjectFactory<PivotForColumn> FACTORY = PivotForColumn::new;
    private final ObjList<CharSequence> valueAliases = new ObjList<>();
    private final ObjList<ExpressionNode> valueList = new ObjList<>();
    private ExpressionNode inExpr;
    private CharSequence inExprAlias;
    private boolean isValueList = true;
    private ExpressionNode selectSubqueryExpr;

    public void addValue(ExpressionNode valueExpr, CharSequence valueAlias) {
        valueList.add(valueExpr);
        valueAliases.add(valueAlias);
    }

    @Override
    public void clear() {
        valueAliases.clear();
        valueList.clear();
        inExpr = null;
        selectSubqueryExpr = null;
        isValueList = true;
        inExprAlias = null;
    }

    public ExpressionNode getInExpr() {
        return inExpr;
    }

    public CharSequence getInExprAlias() {
        return inExprAlias;
    }

    public ExpressionNode getSelectSubqueryExpr() {
        return selectSubqueryExpr;
    }

    public ObjList<CharSequence> getValueAliases() {
        return valueAliases;
    }

    public ObjList<ExpressionNode> getValueList() {
        return valueList;
    }

    public boolean isValueList() {
        return isValueList;
    }

    public PivotForColumn of(ExpressionNode inExpr, boolean isValueList) {
        this.inExpr = inExpr;
        this.isValueList = isValueList;
        return this;
    }

    public void setInExprAlias(CharSequence inExprAlias) {
        this.inExprAlias = inExprAlias;
    }

    public void setIsValueList(boolean isValueList) {
        this.isValueList = isValueList;
    }

    public void setSelectSubqueryExpr(ExpressionNode selectSubqueryExpr) {
        this.selectSubqueryExpr = selectSubqueryExpr;
    }
}