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

package com.questdb.ql.impl.analytic.next;

import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.impl.map.DirectHashMap;
import com.questdb.ql.impl.map.MapUtils;
import com.questdb.ql.impl.map.MapValues;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.ObjList;

public class NextRowAnalyticFunction extends AbstractNextRowAnalyticFunction {
    private final DirectHashMap map;
    private final ObjList<VirtualColumn> partitionBy;

    public NextRowAnalyticFunction(int pageSize, ObjList<VirtualColumn> partitionBy, VirtualColumn valueColumn) {
        super(pageSize, valueColumn);
        this.partitionBy = partitionBy;
        this.map = new DirectHashMap(pageSize, partitionBy.size(), MapUtils.ROWID_MAP_VALUES);
    }

    @Override
    public void addRecord(Record record, long rowid) {
        DirectHashMap.KeyWriter kw = map.keyWriter();
        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            MapUtils.writeVirtualColumn(kw, record, partitionBy.getQuick(i));
        }
        MapValues values = map.getOrCreateValues(kw);
        // allocateOffset memory where we would eventually write "next" value
        final long address = pages.allocate(8);
        if (!values.isNew()) {
            Unsafe.getUnsafe().putLong(values.getLong(0), rowid);
        }
        values.putLong(0, address);
        Unsafe.getUnsafe().putLong(address, -1);
    }

    @Override
    public void close() {
        super.close();
        map.close();
    }

    @Override
    public void reset() {
        super.reset();
        map.clear();
    }
}
