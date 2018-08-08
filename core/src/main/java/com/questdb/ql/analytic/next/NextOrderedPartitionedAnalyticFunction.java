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

package com.questdb.ql.analytic.next;

import com.questdb.ql.analytic.AbstractOrderedAnalyticFunction;
import com.questdb.ql.map.*;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.Misc;
import com.questdb.std.ObjList;
import com.questdb.std.ThreadLocal;
import com.questdb.store.Record;

import java.io.IOException;

public class NextOrderedPartitionedAnalyticFunction extends AbstractOrderedAnalyticFunction {

    private final static ThreadLocal<VirtualColumnTypeResolver> tlPartitionByTypeResolver = new VirtualColumnTypeResolver.ResolverThreadLocal();
    private final DirectMap prevMap;
    private final ObjList<VirtualColumn> partitionBy;

    public NextOrderedPartitionedAnalyticFunction(int pageSize, ObjList<VirtualColumn> partitionBy, VirtualColumn valueColumn) {
        super(pageSize, valueColumn);
        this.prevMap = new DirectMap(pageSize, tlPartitionByTypeResolver.get().of(partitionBy), LongResolver.INSTANCE);
        this.partitionBy = partitionBy;
    }

    @Override
    public void add(Record record) {
        long row = record.getRowId();
        DirectMapValues prevValues = MapUtils.getMapValues(prevMap, record, partitionBy);
        if (!prevValues.isNew()) {
//            DirectMap.KeyWriter kw2 = map.keyWriter();
//            kw2.putLong(prevValues.getLong(0));
            map.locate(prevValues.getLong(0));
            map.getOrCreateValues().putLong(0, row);
        }
        prevValues.putLong(0, row);
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
