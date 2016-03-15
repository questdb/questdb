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

package com.nfsdb.ql.impl.map;

import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.misc.Chars;
import com.nfsdb.ql.Record;
import com.nfsdb.std.IntList;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.ColumnType;

public class GenericRecordComparator implements RecordComparator {
    private static final double D_TOLERANCE = 1E-10;
    private static final float F_TOLERANCE = 1E-10f;
    private final ObjList<ColumnType> types;
    private Record record;
    private IntList keyIndices;


    public GenericRecordComparator(RecordMetadata metadata, IntList keyIndices) {
        this.types = new ObjList<>(keyIndices.size());
        this.keyIndices = keyIndices;
        for (int i = 0, n = keyIndices.size(); i < n; i++) {
            types.add(metadata.getColumnQuick(keyIndices.getQuick(i)).getType());
        }
    }

    @Override
    public void setLeft(Record record) {
        this.record = record;
    }

    @Override
    public int compare(Record that) {
        int z;
        for (int i = 0, n = keyIndices.size(); i < n; i++) {
            int column = keyIndices.getQuick(i);
            switch (types.getQuick(i)) {
                case BOOLEAN:
                    z = cmpBool(record.getBool(column), that.getBool(column));
                    break;
                case BYTE:
                    z = cmpLong(record.get(column), that.get(column));
                    break;
                case DOUBLE:
                    z = cmp(record.getDouble(column), that.getDouble(column));
                    break;
                case FLOAT:
                    z = cmp(record.getFloat(column), that.getFloat(column));
                    break;
                case INT:
                    z = cmpLong(record.getInt(column), that.getInt(column));
                    break;
                case LONG:
                case DATE:
                    z = cmpLong(record.getLong(column), that.getLong(column));
                    break;
                case SHORT:
                    z = cmpLong(record.getShort(column), that.getShort(column));
                    break;
                case STRING:
                    z = Chars.compare(record.getFlyweightStr(column), that.getFlyweightStr(column));
                    break;
                case SYMBOL:
                    z = Chars.compare(record.getSym(column), that.getSym(column));
                    break;
                default:
                    z = 0;
                    break;
            }
            if (z != 0) {
                return z;
            }
        }
        return 0;
    }

    private static int cmpBool(boolean b1, boolean b2) {
        if (b1 && !b2) {
            return -1;
        } else if (!b1 && b2) {
            return 1;
        }
        return 0;
    }

    private static int cmpLong(long l1, long l2) {
        if (l1 < l2) {
            return -1;
        } else if (l1 > l2) {
            return 1;
        }
        return 0;
    }

    private static int cmp(double d1, double d2) {
        if (d1 + D_TOLERANCE < d2) {
            return -1;
        } else if (d2 + D_TOLERANCE < d1) {
            return 1;
        }
        return 0;
    }

    private static int cmp(float d1, float d2) {
        if (d1 + D_TOLERANCE < d2) {
            return -1;
        } else if (d2 + F_TOLERANCE < d1) {
            return 1;
        }
        return 0;
    }

}
