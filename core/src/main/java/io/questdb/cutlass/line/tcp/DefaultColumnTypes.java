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
    final int[] defaultColumnTypes = new int[LineTcpParser.N_ENTITY_TYPES];
    final int defaultTimestampColumnType;
    final int[] mappedColumnTypes = new int[LineTcpParser.N_MAPPED_ENTITY_TYPES];

    public DefaultColumnTypes(LineTcpReceiverConfiguration configuration) {
        // if not set it defaults to ColumnType.UNDEFINED
        this(
                configuration.getDefaultCreateTimestampColumnType(),
                configuration.getDefaultColumnTypeForFloat(),
                configuration.getDefaultColumnTypeForInteger(),
                configuration.isUseLegacyStringDefault()
        );
    }

    public DefaultColumnTypes(LineHttpProcessorConfiguration configuration) {
        // if not set it defaults to ColumnType.UNDEFINED
        this(
                configuration.getDefaultTimestampColumnType(),
                configuration.getDefaultColumnTypeForFloat(),
                configuration.getDefaultColumnTypeForInteger(),
                configuration.isUseLegacyStringDefault()
        );
    }

    private DefaultColumnTypes(
            int defaultTimestampColumnType,
            short defaultColumnTypeForFloat,
            short defaultColumnTypeForInteger,
            boolean useLegacyStringDefault
    ) {
        this.defaultTimestampColumnType = defaultTimestampColumnType;

        // if not set it defaults to ColumnType.UNDEFINED
        defaultColumnTypes[LineTcpParser.ENTITY_TYPE_TAG] = ColumnType.SYMBOL;
        defaultColumnTypes[LineTcpParser.ENTITY_TYPE_FLOAT] = defaultColumnTypeForFloat;
        defaultColumnTypes[LineTcpParser.ENTITY_TYPE_INTEGER] = defaultColumnTypeForInteger;
        defaultColumnTypes[LineTcpParser.ENTITY_TYPE_STRING] =
                useLegacyStringDefault ? ColumnType.STRING : ColumnType.VARCHAR;
        defaultColumnTypes[LineTcpParser.ENTITY_TYPE_SYMBOL] = ColumnType.SYMBOL;
        defaultColumnTypes[LineTcpParser.ENTITY_TYPE_BOOLEAN] = ColumnType.BOOLEAN;
        defaultColumnTypes[LineTcpParser.ENTITY_TYPE_LONG256] = ColumnType.LONG256;
        defaultColumnTypes[LineTcpParser.ENTITY_TYPE_GEOBYTE] = ColumnType.getGeoHashTypeWithBits(8);
        defaultColumnTypes[LineTcpParser.ENTITY_TYPE_GEOSHORT] = ColumnType.getGeoHashTypeWithBits(16);
        defaultColumnTypes[LineTcpParser.ENTITY_TYPE_GEOINT] = ColumnType.getGeoHashTypeWithBits(32);
        defaultColumnTypes[LineTcpParser.ENTITY_TYPE_GEOLONG] = ColumnType.getGeoHashTypeWithBits(60);
        defaultColumnTypes[LineTcpParser.ENTITY_TYPE_TIMESTAMP] = ColumnType.TIMESTAMP;
        defaultColumnTypes[LineTcpParser.ENTITY_TYPE_ARRAY] = ColumnType.ARRAY;
        defaultColumnTypes[LineTcpParser.ENTITY_TYPE_DECIMAL] = ColumnType.DECIMAL;

        // we could remove this mapping by sending the column type to the writer
        // currently we are passing the ILP entity type instead
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_TAG] = ColumnType.SYMBOL;
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_FLOAT] = ColumnType.FLOAT;
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_DOUBLE] = ColumnType.DOUBLE;
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_BYTE] = ColumnType.BYTE;
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_SHORT] = ColumnType.SHORT;
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_INTEGER] = ColumnType.INT;
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_LONG] = ColumnType.LONG;
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_DATE] = ColumnType.DATE;
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_CHAR] = ColumnType.CHAR;
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_STRING] =
                useLegacyStringDefault ? ColumnType.STRING : ColumnType.VARCHAR;
        //MAPPED_COLUMN_TYPES[LineTcpParser.ENTITY_TYPE_SYMBOL] = ColumnType.SYMBOL;
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_BOOLEAN] = ColumnType.BOOLEAN;
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_LONG256] = ColumnType.LONG256;
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_GEOBYTE] = ColumnType.getGeoHashTypeWithBits(8);
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_GEOSHORT] = ColumnType.getGeoHashTypeWithBits(16);
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_GEOINT] = ColumnType.getGeoHashTypeWithBits(32);
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_GEOLONG] = ColumnType.getGeoHashTypeWithBits(60);
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_TIMESTAMP] = ColumnType.TIMESTAMP;
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_ARRAY] = ColumnType.ARRAY;
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_UUID] = ColumnType.UUID;
        mappedColumnTypes[LineTcpParser.ENTITY_TYPE_VARCHAR] = ColumnType.VARCHAR;
    }
}
