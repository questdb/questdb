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

package com.questdb.ql.ops.count;

import com.questdb.ql.AggregatorFunction;
import com.questdb.ql.map.DirectMapValues;
import com.questdb.ql.ops.AbstractVirtualColumn;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;
import com.questdb.store.RecordColumnMetadata;
import com.questdb.store.StorageFacade;

public final class CountAggregator extends AbstractVirtualColumn implements AggregatorFunction, Function {

    public static final VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new CountAggregator(position);
    private int index;

    private CountAggregator(int position) {
        super(ColumnType.LONG, position);
    }

    @Override
    public void calculate(Record rec, DirectMapValues values) {
        if (values.isNew()) {
            values.putLong(index, 1);
        } else {
            values.putLong(index, values.getLong(index) + 1);
        }
    }

    @Override
    public void clear() {
    }

    @Override
    public void prepare(ObjList<RecordColumnMetadata> columns, int offset) {
        columns.add(this);
        index = offset;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void prepare(StorageFacade facade) {
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) {
    }
}
