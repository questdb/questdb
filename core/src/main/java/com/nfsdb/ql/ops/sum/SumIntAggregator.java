/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.ql.ops.sum;

import com.nfsdb.ql.Record;
import com.nfsdb.ql.impl.map.MapValues;
import com.nfsdb.ql.ops.AbstractUnaryAggregator;
import com.nfsdb.ql.ops.Function;
import com.nfsdb.store.ColumnType;

public final class SumIntAggregator extends AbstractUnaryAggregator {
    public static final SumIntAggregator FACTORY = new SumIntAggregator();

    private SumIntAggregator() {
        super(ColumnType.INT);
    }

    @Override
    public void calculate(Record rec, MapValues values) {
        if (values.isNew()) {
            values.putInt(valueIndex, value.getInt(rec));
        } else {
            values.putInt(valueIndex, values.getInt(valueIndex) + value.getInt(rec));
        }
    }

    @Override
    public Function newInstance() {
        return new SumIntAggregator();
    }
}
