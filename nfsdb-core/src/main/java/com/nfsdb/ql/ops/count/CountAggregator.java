/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.ops.count;

import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.ParserException;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.ql.AggregatorFunction;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.impl.map.MapValues;
import com.nfsdb.ql.ops.AbstractVirtualColumn;
import com.nfsdb.ql.ops.Function;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.storage.ColumnType;

public final class CountAggregator extends AbstractVirtualColumn implements AggregatorFunction, Function {

    public static final CountAggregator FACTORY = new CountAggregator();

    private int index;

    private CountAggregator() {
        super(ColumnType.LONG);
    }

    @Override
    public void calculate(Record rec, MapValues values) {
        if (values.isNew()) {
            values.putLong(index, 1);
        } else {
            values.putLong(index, values.getLong(index) + 1);
        }
    }

    @Override
    public RecordColumnMetadata[] getColumns() {
        return new RecordColumnMetadata[]{this};
    }

    @Override
    public void mapColumn(int k, int i) {
        index = i;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void prepare(StorageFacade facade) {
    }

    @Override
    public Function newInstance(ObjList<VirtualColumn> args) {
        return new CountAggregator();
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) throws ParserException {
    }
}
