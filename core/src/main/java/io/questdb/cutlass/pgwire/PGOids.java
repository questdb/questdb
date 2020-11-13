/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cutlass.pgwire;

import io.questdb.cairo.ColumnType;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;

public class PGOids {

    public static final int PG_VARCHAR = 1043;
    public static final int PG_TIMESTAMP = 1114;
    public static final int PG_TIMESTAMPZ = 1184;
    public static final int PG_FLOAT8 = 701;
    public static final int PG_FLOAT4 = 700;
    public static final int PG_INT4 = 23;
    public static final int PG_INT2 = 21;
    public static final int PG_INT8 = 20;
    public static final int PG_NUMERIC = 1700;
    public static final int PG_BOOL = 16;
    public static final int PG_CHAR = 18;
    public static final int PG_DATE = 1082;
    public static final int PG_BYTEA = 17;
    public static final int PG_UNSPECIFIED = 0;
    public static final IntList TYPE_OIDS = new IntList();
    public static final IntHashSet PG_TYPE_OIDS = new IntHashSet();
    public static final IntIntHashMap PG_TYPE_TO_INTERNAL_TYPE_MAP = new IntIntHashMap();
    public static final IntObjHashMap<CharSequence> PG_TYPE_TO_NAME = new IntObjHashMap<>();

    static {
        TYPE_OIDS.extendAndSet(ColumnType.STRING, PG_VARCHAR); // VARCHAR
        TYPE_OIDS.extendAndSet(ColumnType.TIMESTAMP, PG_TIMESTAMP); // TIMESTAMP
        TYPE_OIDS.extendAndSet(ColumnType.DOUBLE, PG_FLOAT8); // FLOAT8
        TYPE_OIDS.extendAndSet(ColumnType.FLOAT, PG_FLOAT4); // FLOAT4
        TYPE_OIDS.extendAndSet(ColumnType.INT, PG_INT4); // INT4
        TYPE_OIDS.extendAndSet(ColumnType.SHORT, PG_INT2); // INT2
        TYPE_OIDS.extendAndSet(ColumnType.CHAR, PG_CHAR);
        TYPE_OIDS.extendAndSet(ColumnType.SYMBOL, PG_VARCHAR); // NAME
        TYPE_OIDS.extendAndSet(ColumnType.LONG, PG_INT8); // INT8
        TYPE_OIDS.extendAndSet(ColumnType.BYTE, PG_INT2); // INT2
        TYPE_OIDS.extendAndSet(ColumnType.BOOLEAN, PG_BOOL); // BOOL
        TYPE_OIDS.extendAndSet(ColumnType.DATE, PG_TIMESTAMP); // DATE
        TYPE_OIDS.extendAndSet(ColumnType.BINARY, PG_BYTEA); // BYTEA
        TYPE_OIDS.extendAndSet(ColumnType.LONG256, PG_NUMERIC); // NUMERIC

        for (int i = 0, n = TYPE_OIDS.size(); i < n; i++) {
            PG_TYPE_OIDS.add(TYPE_OIDS.getQuick(i));
        }

        PG_TYPE_TO_INTERNAL_TYPE_MAP.put(PG_VARCHAR, ColumnType.STRING);
        PG_TYPE_TO_INTERNAL_TYPE_MAP.put(PG_TIMESTAMP, ColumnType.TIMESTAMP); // TIMESTAMP
        PG_TYPE_TO_INTERNAL_TYPE_MAP.put(PG_FLOAT8, ColumnType.DOUBLE); // FLOAT8
        PG_TYPE_TO_INTERNAL_TYPE_MAP.put(PG_FLOAT4, ColumnType.FLOAT); // FLOAT4
        PG_TYPE_TO_INTERNAL_TYPE_MAP.put(PG_INT4, ColumnType.INT); // INT4
        PG_TYPE_TO_INTERNAL_TYPE_MAP.put(PG_INT2, ColumnType.SHORT); // INT2
        PG_TYPE_TO_INTERNAL_TYPE_MAP.put(PG_CHAR, ColumnType.CHAR);
        PG_TYPE_TO_INTERNAL_TYPE_MAP.put(PG_INT8, ColumnType.LONG); // INT8
        PG_TYPE_TO_INTERNAL_TYPE_MAP.put(PG_INT2, ColumnType.BYTE); // INT2
        PG_TYPE_TO_INTERNAL_TYPE_MAP.put(PG_BOOL, ColumnType.BOOLEAN); // BOOL
        PG_TYPE_TO_INTERNAL_TYPE_MAP.put(PG_BYTEA, ColumnType.BINARY); // BYTEA
        PG_TYPE_TO_INTERNAL_TYPE_MAP.put(PG_NUMERIC, ColumnType.LONG256);

        PG_TYPE_TO_NAME.put(PG_VARCHAR, "VARCHAR");
        PG_TYPE_TO_NAME.put(PG_TIMESTAMP, "TIMESTAMP"); // TIMESTAMP
        PG_TYPE_TO_NAME.put(PG_FLOAT8, "FLOAT8"); // FLOAT8
        PG_TYPE_TO_NAME.put(PG_FLOAT4, "FLOAT4"); // FLOAT4
        PG_TYPE_TO_NAME.put(PG_INT4, "INT8"); // INT4
        PG_TYPE_TO_NAME.put(PG_INT2, "INT2"); // INT2
        PG_TYPE_TO_NAME.put(PG_CHAR, "CHAR");
        PG_TYPE_TO_NAME.put(PG_INT8, "INT8"); // INT8
        PG_TYPE_TO_NAME.put(PG_BOOL, "BOOL"); // BOOL
        PG_TYPE_TO_NAME.put(PG_BYTEA, "BINARY"); // BYTEA
        PG_TYPE_TO_NAME.put(PG_NUMERIC, "NUMERIC"); // NUMERIC
    }
}
