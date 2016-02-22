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

package com.nfsdb.ql.ops;

import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.AggregatorFunction;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.impl.map.MapValues;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.ColumnType;
import com.nfsdb.store.VariableColumn;

public final class NullCountAggregator implements AggregatorFunction {

    public static final NullCountAggregator FACTORY = new NullCountAggregator();

    private int offset;
    private RecordMetadata metadata;

    private NullCountAggregator() {
    }

    @Override
    public void calculate(Record rec, MapValues values) {
        int slot = offset;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++, slot++) {
            boolean increment;
            switch (metadata.getColumnQuick(i).getType()) {
                case DATE:
                    increment = rec.getDate(i) == Long.MAX_VALUE;
                    break;
                case DOUBLE:
                    double d = rec.getDouble(i);
                    increment = d != d;
                    break;
                case FLOAT:
                    float f = rec.getFloat(i);
                    increment = f != f;
                    break;
                case INT:
                    increment = rec.getInt(i) == Integer.MAX_VALUE;
                    break;
                case STRING:
                    increment = rec.getStrLen(i) == VariableColumn.NULL_LEN;
                    break;
                case SYMBOL:
                    increment = rec.getSym(i) == null;
                    break;
                case LONG:
                    increment = rec.getLong(i) == Long.MIN_VALUE;
                    break;
                default:
                    increment = false;
                    break;
            }

            if (increment) {
                values.incrementLong(slot++);
            }
        }
    }

    @Override
    public void prepare(RecordMetadata metadata, ObjList<RecordColumnMetadata> columns, int offset) {
        this.offset = offset;
        this.metadata = metadata;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            RecordColumnMetadata m = metadata.getColumnQuick(i);
            columns.add(new ColumnMetadata().setName(m.getName()).setType(ColumnType.LONG));
        }
    }
}
