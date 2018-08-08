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

package com.questdb.ql.analytic.denserank;

import com.questdb.ql.map.*;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.Misc;
import com.questdb.std.ObjList;
import com.questdb.std.ThreadLocal;
import com.questdb.store.Record;

import java.io.IOException;

public class DenseRankOPAnalyticFunction extends AbstractRankOrderedAnalyticFunction {
    private final static ThreadLocal<VirtualColumnTypeResolver> tlPartitionByTypeResolver = new VirtualColumnTypeResolver.ResolverThreadLocal();
    private final DirectMap partitionMap;
    private final ObjList<VirtualColumn> partitionBy;

    public DenseRankOPAnalyticFunction(int pageSize, String name, ObjList<VirtualColumn> partitionBy) {
        super(pageSize, name);
        this.partitionMap = new DirectMap(pageSize, tlPartitionByTypeResolver.get().of(partitionBy), LongResolver.INSTANCE);
        this.partitionBy = partitionBy;
    }

    @Override
    public void add(Record record) {
        long row = record.getRowId();
        long rank;
        DirectMapValues values = MapUtils.getMapValues(partitionMap, record, partitionBy);
        if (values.isNew()) {
            values.putLong(0, rank = 0);
        } else {
            rank = values.getLong(0) + 1;
            values.putLong(0, rank);
        }
        map.locate(row);
        map.getOrCreateValues().putLong(0, rank);
    }

    @Override
    public void close() throws IOException {
        super.close();
        Misc.free(partitionMap);
    }

    @Override
    public void reset() {
        super.reset();
        partitionMap.clear();
    }
}
