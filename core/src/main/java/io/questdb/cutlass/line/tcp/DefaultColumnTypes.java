/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.ColumnType;

class DefaultColumnTypes {
    static final int[] DEFAULT_COLUMN_TYPES = new int[LineTcpParser.N_ENTITY_TYPES];

    static {
        // if not set it defaults to ColumnType.UNDEFINED
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_TAG] = ColumnType.SYMBOL;
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_FLOAT] = ColumnType.DOUBLE;
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_INTEGER] = ColumnType.LONG;
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_STRING] = ColumnType.STRING;
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_SYMBOL] = ColumnType.SYMBOL;
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_BOOLEAN] = ColumnType.BOOLEAN;
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_LONG256] = ColumnType.LONG256;
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_GEOBYTE] = ColumnType.getGeoHashTypeWithBits(8);
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_GEOSHORT] = ColumnType.getGeoHashTypeWithBits(16);
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_GEOINT] = ColumnType.getGeoHashTypeWithBits(32);
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_GEOLONG] = ColumnType.getGeoHashTypeWithBits(60);
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_TIMESTAMP] = ColumnType.TIMESTAMP;
    }
}
