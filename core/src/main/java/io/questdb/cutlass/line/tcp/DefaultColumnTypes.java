/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;

public class DefaultColumnTypes {
    final int[] DEFAULT_COLUMN_TYPES = new int[LineTcpParser.N_ENTITY_TYPES];
    final int[] MAPPED_COLUMN_TYPES = new int[LineTcpParser.N_MAPPED_ENTITY_TYPES];

    public DefaultColumnTypes(LineTcpReceiverConfiguration configuration) {
        // if not set it defaults to ColumnType.UNDEFINED
        this(
                configuration.getDefaultColumnTypeForTimestamp(),
                configuration.getDefaultColumnTypeForFloat(),
                configuration.getDefaultColumnTypeForInteger(),
                configuration.isUseLegacyStringDefault()
        );
    }

    public DefaultColumnTypes(LineHttpProcessorConfiguration configuration) {
        // if not set it defaults to ColumnType.UNDEFINED
        this(
                configuration.getDefaultColumnTypeForTimestamp(),
                configuration.getDefaultColumnTypeForFloat(),
                configuration.getDefaultColumnTypeForInteger(),
                configuration.isUseLegacyStringDefault()
        );
    }

    private DefaultColumnTypes(
            int defaultTimestampType,
            short defaultColumnTypeForFloat,
            short defaultColumnTypeForInteger,
            boolean useLegacyStringDefault
    ) {
        // if not set it defaults to ColumnType.UNDEFINED
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_TAG] = ColumnType.SYMBOL;
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_FLOAT] = defaultColumnTypeForFloat;
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_INTEGER] = defaultColumnTypeForInteger;
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_STRING] =
                useLegacyStringDefault ? ColumnType.STRING : ColumnType.VARCHAR;
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_SYMBOL] = ColumnType.SYMBOL;
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_BOOLEAN] = ColumnType.BOOLEAN;
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_LONG256] = ColumnType.LONG256;
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_GEOBYTE] = ColumnType.getGeoHashTypeWithBits(8);
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_GEOSHORT] = ColumnType.getGeoHashTypeWithBits(16);
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_GEOINT] = ColumnType.getGeoHashTypeWithBits(32);
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_GEOLONG] = ColumnType.getGeoHashTypeWithBits(60);
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_TIMESTAMP] = defaultTimestampType;
        DEFAULT_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_ARRAY] = ColumnType.ARRAY;

        // we could remove this mapping by sending the column type to the writer
        // currently we are passing the ILP entity type instead
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_TAG] = ColumnType.SYMBOL;
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_FLOAT] = ColumnType.FLOAT;
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_DOUBLE] = ColumnType.DOUBLE;
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_BYTE] = ColumnType.BYTE;
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_SHORT] = ColumnType.SHORT;
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_INTEGER] = ColumnType.INT;
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_LONG] = ColumnType.LONG;
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_DATE] = ColumnType.DATE;
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_CHAR] = ColumnType.CHAR;
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_STRING] =
                useLegacyStringDefault ? ColumnType.STRING : ColumnType.VARCHAR;
        //MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_SYMBOL] = ColumnType.SYMBOL;
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_BOOLEAN] = ColumnType.BOOLEAN;
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_LONG256] = ColumnType.LONG256;
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_GEOBYTE] = ColumnType.getGeoHashTypeWithBits(8);
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_GEOSHORT] = ColumnType.getGeoHashTypeWithBits(16);
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_GEOINT] = ColumnType.getGeoHashTypeWithBits(32);
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_GEOLONG] = ColumnType.getGeoHashTypeWithBits(60);
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_TIMESTAMP] = defaultTimestampType;
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_ARRAY] = ColumnType.ARRAY;
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_UUID] = ColumnType.UUID;
        MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_VARCHAR] = ColumnType.VARCHAR;
    }
}
