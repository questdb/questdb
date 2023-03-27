/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cairo.map;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.griffin.EmptyRecordMetadata;
import io.questdb.std.Chars;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class MapFactory {

    /**
     * Creates a Map pre-allocated to a large capacity to be used in JOIN queries.
     */
    public static Map createMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @NotNull ColumnTypes valueTypes
    ) {
        return createMap(false, configuration, keyTypes, valueTypes);
    }

    /**
     * Creates a Map pre-allocated to a large capacity to be used in JOIN queries.
     */
    public static Map createMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes
    ) {
        return createMap(false, configuration, keyTypes);
    }

    /**
     * Creates a Map pre-allocated to a small capacity to be used in SAMPLE BY, GROUP BY queries, but not only.
     */
    public static Map createSmallMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes
    ) {
        return createMap(true, configuration, keyTypes);
    }

    /**
     * Creates a Map pre-allocated to a small capacity to be used in SAMPLE BY, GROUP BY queries, but not only.
     */
    public static Map createSmallMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @NotNull ColumnTypes valueTypes
    ) {
        return createMap(true, configuration, keyTypes, valueTypes);
    }

    private static Map createMap(
            boolean smallMap,
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes
    ) {
        final int keyCapacity = smallMap ? configuration.getSqlSmallMapKeyCapacity() : configuration.getSqlMapKeyCapacity();
        final int pageSize = smallMap ? configuration.getSqlSmallMapPageSize() : configuration.getSqlMapPageSize();
        CharSequence mapType = configuration.getDefaultMapType();
        if (Chars.equalsLowerCaseAscii(mapType, "fast")) {
            return new FastMap(
                    pageSize,
                    keyTypes,
                    keyCapacity,
                    configuration.getSqlFastMapLoadFactor(),
                    configuration.getSqlMapMaxResizes()
            );
        }

        if (Chars.equalsLowerCaseAscii(mapType, "compact")) {
            return new CompactMap(
                    pageSize,
                    keyTypes,
                    EmptyRecordMetadata.INSTANCE,
                    keyCapacity,
                    configuration.getSqlCompactMapLoadFactor(),
                    configuration.getSqlMapMaxResizes(),
                    configuration.getSqlMapMaxPages()
            );
        }
        throw CairoException.critical(0).put("unknown map type: ").put(mapType);
    }

    private static Map createMap(
            boolean smallMap,
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @NotNull ColumnTypes valueTypes
    ) {
        final int keyCapacity = smallMap ? configuration.getSqlSmallMapKeyCapacity() : configuration.getSqlMapKeyCapacity();
        final int pageSize = smallMap ? configuration.getSqlSmallMapPageSize() : configuration.getSqlMapPageSize();
        final CharSequence mapType = configuration.getDefaultMapType();
        if (Chars.equalsLowerCaseAscii(mapType, "fast")) {
            return new FastMap(
                    pageSize,
                    keyTypes,
                    valueTypes,
                    keyCapacity,
                    configuration.getSqlFastMapLoadFactor(),
                    configuration.getSqlMapMaxResizes()
            );
        }

        if (Chars.equalsLowerCaseAscii(mapType, "compact")) {
            return new CompactMap(
                    pageSize,
                    keyTypes,
                    valueTypes,
                    keyCapacity,
                    configuration.getSqlCompactMapLoadFactor(),
                    configuration.getSqlMapMaxResizes(),
                    configuration.getSqlMapMaxPages()
            );
        }
        throw CairoException.critical(0).put("unknown map type: ").put(mapType);
    }
}
