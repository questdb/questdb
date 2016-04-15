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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.impl.sort;

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
