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

import com.questdb.griffin.common.ExprNode;
import com.questdb.std.IntList;
import com.questdb.std.ObjList;
import com.questdb.std.ObjectFactory;

public final class AnalyticColumn extends QueryColumn {
    public final static ObjectFactory<AnalyticColumn> FACTORY = AnalyticColumn::new;
    private final ObjList<ExprNode> partitionBy = new ObjList<>(2);
    private final ObjList<ExprNode> orderBy = new ObjList<>(2);
    private final IntList orderByDirection = new IntList(2);

    private AnalyticColumn() {
    }

    public void addOrderBy(ExprNode node, int direction) {
        orderBy.add(node);
        orderByDirection.add(direction);
    }

    @Override
    public void clear() {
        super.clear();
        partitionBy.clear();
        orderBy.clear();
        orderByDirection.clear();
    }

    @Override
    public AnalyticColumn of(String alias, ExprNode ast) {
        return (AnalyticColumn) super.of(alias, ast);
    }

    public ObjList<ExprNode> getOrderBy() {
        return orderBy;
    }

    public IntList getOrderByDirection() {
        return orderByDirection;
    }

    public ObjList<ExprNode> getPartitionBy() {
        return partitionBy;
    }
}
