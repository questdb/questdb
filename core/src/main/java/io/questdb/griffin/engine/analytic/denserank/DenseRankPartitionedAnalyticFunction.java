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

package io.questdb.griffin.engine.analytic.denserank;

import com.questdb.ql.Record;
import com.questdb.ql.impl.map.DirectMap;
import com.questdb.ql.impl.map.DirectMapValues;
import com.questdb.ql.impl.map.MapUtils;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;

import java.io.Closeable;
import java.io.IOException;

public class DenseRankPartitionedAnalyticFunction extends AbstractRankAnalyticFunction implements Closeable {

    private final DirectMap map;
    private final ObjList<VirtualColumn> partitionBy;

    public DenseRankPartitionedAnalyticFunction(int pageSize, String name, ObjList<VirtualColumn> partitionBy) {
        super(name);
        this.partitionBy = partitionBy;
        this.map = new DirectMap(pageSize, partitionBy.size(), MapUtils.toTypeList(ColumnType.LONG));
    }

    @Override
    public void close() throws IOException {
        map.close();
    }

    @Override
    public void prepareFor(Record rec) {
        DirectMapValues values = MapUtils.getMapValues(map, rec, partitionBy);
        if (values.isNew()) {
            rank = 0;
            values.putLong(0, 1);
        } else {
            rank = values.getLong(0);
            values.putLong(0, rank + 1);
        }
    }

    @Override
    public void reset() {
        map.clear();
    }

    @Override
    public void toTop() {
        reset();
    }
}
