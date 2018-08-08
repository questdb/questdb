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

package com.questdb.ql.ops.neq;

import com.questdb.ql.ops.AbstractBinaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.std.Numbers;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;
import com.questdb.store.StorageFacade;
import com.questdb.store.SymbolTable;

public class SymNotEqualStrOperator extends AbstractBinaryOperator {

    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new SymNotEqualStrOperator(position);

    private int key = -2;

    private SymNotEqualStrOperator(int position) {
        super(ColumnType.BOOLEAN, position);
    }

    @Override
    public boolean getBool(Record rec) {
        int k = lhs.getInt(rec);
        return (k != key && (key != SymbolTable.VALUE_IS_NULL || k != Numbers.INT_NaN));
    }

    @Override
    public void prepare(StorageFacade facade) {
        super.prepare(facade);
        this.key = lhs.getSymbolTable().getQuick(rhs.getFlyweightStr(null));
    }
}
