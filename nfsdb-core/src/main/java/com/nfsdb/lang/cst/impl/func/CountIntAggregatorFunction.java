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

package com.nfsdb.lang.cst.impl.func;

import com.nfsdb.collections.mmap.MapValues;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordSource;
import com.nfsdb.storage.ColumnType;

public class CountIntAggregatorFunction extends AbstractSingleColumnAggregatorFunction {


    public CountIntAggregatorFunction(String name) {
        super(new ColumnMetadata().setName(name).setType(ColumnType.INT));
    }

    @Override
    public void calculate(Record rec, MapValues values) {
        if (values.isNew()) {
            values.putInt(valueIndex, 1);
        } else {
            values.putInt(valueIndex, values.getInt(valueIndex) + 1);
        }
    }

    @Override
    public void prepareSource(RecordSource<? extends Record> source) {
        // do not call parent method, which will be trying to lookup column in record source.
    }
}
