/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.impl.sort;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Chars;
import com.questdb.ql.Record;
import com.questdb.std.IntList;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;

public class GenericRecordComparator implements RecordComparator {
    private static final double D_TOLERANCE = 1E-10;
    private static final float F_TOLERANCE = 1E-10f;
    private final ObjList<ColumnType> types;
    private final IntList keyIndices;
    private Record record;


    public GenericRecordComparator(RecordMetadata metadata, IntList keyIndices) {
        this.types = new ObjList<>(keyIndices.size());
        this.keyIndices = keyIndices;
        for (int i = 0, n = keyIndices.size(); i < n; i++) {
            types.add(metadata.getColumnQuick(keyIndices.getQuick(i)).getType());
        }
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

    @Override
    public void setLeft(Record record) {
        this.record = record;
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
