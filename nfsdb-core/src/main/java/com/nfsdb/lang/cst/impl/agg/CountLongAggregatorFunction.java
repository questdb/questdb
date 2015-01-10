/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb.lang.cst.impl.agg;

import com.nfsdb.collections.mmap.MapValues;
import com.nfsdb.column.ColumnType;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.lang.cst.impl.qry.Record;
import com.nfsdb.lang.cst.impl.qry.RecordSource;

public class CountLongAggregatorFunction extends AbstractAggregatorFunction {

    private final ColumnMetadata[] meta = new ColumnMetadata[1];

    public CountLongAggregatorFunction(String name) {
        this.meta[0] = new ColumnMetadata();
        this.meta[0].name = name;
        this.meta[0].type = ColumnType.LONG;
    }

    @Override
    protected ColumnMetadata[] getColumnsInternal() {
        return meta;
    }

    @Override
    public void calculate(Record rec, MapValues values) {
        if (values.isNew()) {
            values.putLong(map(0), 1);
        } else {
            values.putLong(map(0), values.getLong(map(0)) + 1);
        }
    }

    @Override
    public void prepareSource(RecordSource<? extends Record> source) {

    }
}
