/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package io.questdb.griffin.engine.analytic.next;

import com.questdb.misc.Misc;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.impl.map.DirectMap;
import com.questdb.ql.impl.map.DirectMapValues;
import com.questdb.ql.impl.map.MapUtils;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;

public class NextPartitionedAnalyticFunction extends AbstractNextAnalyticFunction {
    private final DirectMap map;
    private final ObjList<VirtualColumn> partitionBy;

    public NextPartitionedAnalyticFunction(int pageSize, ObjList<VirtualColumn> partitionBy, VirtualColumn valueColumn) {
        super(pageSize, valueColumn);
        this.partitionBy = partitionBy;
        this.map = new DirectMap(pageSize, partitionBy.size(), MapUtils.toTypeList(ColumnType.LONG));
    }

    @Override
    public void add(Record record) {
        DirectMapValues values = MapUtils.getMapValues(map, record, partitionBy);
        // allocateOffset memory where we would eventually write "next" value
        final long address = pages.allocate(8);
        if (!values.isNew()) {
            Unsafe.getUnsafe().putLong(values.getLong(0), record.getRowId());
        }
        values.putLong(0, address);
        Unsafe.getUnsafe().putLong(address, -1);
    }

    @Override
    public void close() {
        super.close();
        Misc.free(map);
    }

    @Override
    public void reset() {
        super.reset();
        map.clear();
    }

    @Override
    public void toTop() {
        super.toTop();
        map.clear();
    }
}
