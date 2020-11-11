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

package io.questdb.griffin.engine.analytic.prev;

import com.questdb.misc.Misc;
import com.questdb.ql.Record;
import com.questdb.ql.impl.analytic.AbstractOrderedAnalyticFunction;
import com.questdb.ql.impl.map.DirectMap;
import com.questdb.ql.impl.map.DirectMapValues;
import com.questdb.ql.impl.map.MapUtils;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.ObjList;

import java.io.IOException;

public class PrevOrderedPartitionedAnalyticFunction extends AbstractOrderedAnalyticFunction {

    private final DirectMap prevMap;
    private final ObjList<VirtualColumn> partitionBy;

    public PrevOrderedPartitionedAnalyticFunction(int pageSize, ObjList<VirtualColumn> partitionBy, VirtualColumn valueColumn) {
        super(pageSize, valueColumn);
        this.prevMap = new DirectMap(pageSize, 1, MapUtils.ROWID_MAP_VALUES);
        this.partitionBy = partitionBy;
    }

    @Override
    public void add(Record record) {
        long row = record.getRowId();
        long prevRow;
        DirectMapValues prevValues = MapUtils.getMapValues(prevMap, record, partitionBy);
        if (prevValues.isNew()) {
            prevRow = -1;
        } else {
            prevRow = prevValues.getLong(0);
        }

        prevValues.putLong(0, row);
        DirectMap.KeyWriter kw2 = map.keyWriter();
        kw2.putLong(row);
        map.getOrCreateValues(kw2).putLong(0, prevRow);
    }

    @Override
    public void toTop() {
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        Misc.free(prevMap);
        super.close();
    }

    @Override
    public void reset() {
        prevMap.clear();
        super.reset();
    }
}
