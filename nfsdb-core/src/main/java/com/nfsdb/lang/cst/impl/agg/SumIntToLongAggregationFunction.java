/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.lang.cst.impl.qry.Record;
import com.nfsdb.storage.ColumnType;

public class SumIntToLongAggregationFunction extends AbstractSingleColumnAggregatorFunction {
    public SumIntToLongAggregationFunction(ColumnMetadata meta) {
        super(new ColumnMetadata().copy(meta).setType(ColumnType.LONG));
    }

    @Override
    public void calculate(Record rec, MapValues values) {
        if (values.isNew()) {
            values.putLong(valueIndex, rec.getInt(recordIndex));
        } else {
            values.putLong(valueIndex, values.getLong(valueIndex) + rec.getInt(recordIndex));
        }
    }
}
