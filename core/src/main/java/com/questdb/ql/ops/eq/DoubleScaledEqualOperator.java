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

package com.questdb.ql.ops.eq;

import com.questdb.ql.ops.AbstractVirtualColumn;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;
import com.questdb.store.StorageFacade;

public class DoubleScaledEqualOperator extends AbstractVirtualColumn implements Function {

    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new DoubleScaledEqualOperator(position);
    private VirtualColumn lhs;
    private VirtualColumn rhs;
    private VirtualColumn scale;

    private DoubleScaledEqualOperator(int position) {
        super(ColumnType.BOOLEAN, position);
    }

    @Override
    public boolean getBool(Record rec) {
        double d = lhs.getDouble(rec) - rhs.getDouble(rec);
        return d > 0 ? d < this.scale.getDouble(rec) : d > -this.scale.getDouble(rec);
    }

    @Override
    public boolean isConstant() {
        return lhs.isConstant() && rhs.isConstant() && scale.isConstant();
    }

    @Override
    public void prepare(StorageFacade facade) {
        lhs.prepare(facade);
        rhs.prepare(facade);
        scale.prepare(facade);
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) {
        switch (pos) {
            case 0:
                lhs = arg;
                break;
            case 1:
                rhs = arg;
                break;
            default:
                scale = arg;
                break;
        }
    }
}
