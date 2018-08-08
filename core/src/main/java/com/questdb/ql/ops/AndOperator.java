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

package com.questdb.ql.ops;

import com.questdb.ql.ops.constant.BooleanConstant;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;

public class AndOperator extends AbstractBinaryOperator {

    public static final VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new AndOperator(position);

    private AndOperator(int position) {
        super(ColumnType.BOOLEAN, position);
    }

    @Override
    public boolean getBool(Record rec) {
        return lhs.getBool(rec) && rhs.getBool(rec);
    }

    @Override
    public boolean isConstant() {
        if (rhs.isConstant() && !rhs.getBool(null)) {
            lhs = new BooleanConstant(false, rhs.getPosition());
            return true;
        }
        return (lhs.isConstant() && !lhs.getBool(null)) || (lhs.isConstant() && rhs.isConstant());
    }
}
