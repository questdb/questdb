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

package com.nfsdb.ql.ops;

import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadataFacade;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.collections.MapValues;
import com.nfsdb.storage.ColumnType;

public class SumIntToLongAggregationFunction extends AbstractSingleColumnAggregatorFunction {
    public SumIntToLongAggregationFunction(RecordColumnMetadata meta) {
        super(new RecordColumnMetadataFacade(meta, ColumnType.LONG));
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
