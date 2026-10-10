/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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


package io.questdb.cairo;

/**
 * The PostgreSQL type OIDs (the numbers of the {@code pg_type} catalog). The type drivers return
 * them ({@link TypeDriver#getPgOid()}, {@link TypeDriver#getPgArrayOid()}) and {@code PGOids}
 * re-exports them, so {@code cairo} depends on no protocol package and each OID is defined once.
 */
public final class PgTypeOids {
    public static final int PG_ARR_BOOL = 1000;
    public static final int PG_ARR_BYTEA = 1001;
    public static final int PG_ARR_DATE = 1182;
    public static final int PG_ARR_FLOAT4 = 1021;
    public static final int PG_ARR_FLOAT8 = 1022;
    public static final int PG_ARR_INET = 1041;
    public static final int PG_ARR_INT2 = 1005;
    public static final int PG_ARR_INT4 = 1007;
    public static final int PG_ARR_INT8 = 1016;
    public static final int PG_ARR_INTERVAL = 1187;
    public static final int PG_ARR_JSONB = 3807;
    public static final int PG_ARR_NUMERIC = 1231;
    public static final int PG_ARR_TEXT = 1009;
    public static final int PG_ARR_TIME = 1183;
    public static final int PG_ARR_TIMESTAMP = 1115;
    public static final int PG_ARR_TIMESTAMP_TZ = 1185;
    public static final int PG_ARR_UUID = 2951;
    public static final int PG_ARR_VARCHAR = 1015;
    public static final int PG_BOOL = 16;
    public static final int PG_BYTEA = 17;
    public static final int PG_CHAR = 1042;
    public static final int PG_DATE = 1082;
    public static final int PG_FLOAT4 = 700;
    public static final int PG_FLOAT8 = 701;
    public static final int PG_INET = 869;
    public static final int PG_INT2 = 21;
    public static final int PG_INT4 = 23;
    public static final int PG_INT8 = 20;
    public static final int PG_INTERNAL = 2281;
    public static final int PG_INTERVAL = 1186;
    public static final int PG_JSONB = 3802;
    public static final int PG_NUMERIC = 1700;
    public static final int PG_OID = 26;
    public static final int PG_TEXT = 25;
    public static final int PG_TIME = 1083;
    public static final int PG_TIMESTAMP = 1114;
    public static final int PG_TIMESTAMP_TZ = 1184;
    public static final int PG_UNSPECIFIED = 0;
    public static final int PG_UUID = 2950;
    public static final int PG_VARCHAR = 1043;
    public static final int PG_VOID = 2278;

    private PgTypeOids() {
    }
}
