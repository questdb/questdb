/*******************************************************************************
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

package io.questdb.cairo.map;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MapFactory {

    /**
     * Creates a Map pre-allocated to a small capacity to be used in SAMPLE BY, GROUP BY queries, but not only.
     */
    public static Map createOrderedMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes
    ) {
        final int keyCapacity = configuration.getSqlSmallMapKeyCapacity();
        final long pageSize = configuration.getSqlSmallMapPageSize();
        return new OrderedMap(
                pageSize,
                keyTypes,
                keyCapacity,
                configuration.getSqlFastMapLoadFactor(),
                configuration.getSqlMapMaxResizes()
        );
    }

    /**
     * Creates a Map pre-allocated to a small capacity to be used in SAMPLE BY, GROUP BY queries, but not only.
     */
    public static Map createOrderedMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes
    ) {
        final int keyCapacity = configuration.getSqlSmallMapKeyCapacity();
        final long pageSize = configuration.getSqlSmallMapPageSize();
        return new OrderedMap(
                pageSize,
                keyTypes,
                valueTypes,
                keyCapacity,
                configuration.getSqlFastMapLoadFactor(),
                configuration.getSqlMapMaxResizes()
        );
    }

    /**
     * Creates an unordered Map pre-allocated to a small capacity to be used in GROUP BY queries, but not only.
     * <p>
     * The returned map may actually preserve insertion order, i.e. it may be ordered, depending on the types
     * of key and value columns.
     */
    public static Map createUnorderedMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes
    ) {
        final int keyCapacity = configuration.getSqlSmallMapKeyCapacity();
        final long pageSize = configuration.getSqlSmallMapPageSize();
        return createUnorderedMap(configuration, keyTypes, valueTypes, keyCapacity, pageSize);
    }

    /**
     * Creates an unordered Map pre-allocated to a small capacity to be used in GROUP BY queries, but not only.
     * <p>
     * The returned map may actually preserve insertion order, i.e. it may be ordered, depending on the types
     * of key and value columns.
     */
    public static Map createUnorderedMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            long pageSize
    ) {
        final int maxEntrySize = configuration.getSqlUnorderedMapMaxEntrySize();

        final int valueSize = ColumnTypes.sizeInBytes(valueTypes);
        if (keyTypes.getColumnCount() == 1) {
            final int keyType = keyTypes.getColumnType(0);
            if (Unordered4Map.isSupportedKeyType(keyType) && Integer.BYTES + valueSize <= maxEntrySize) {
                return new Unordered4Map(
                        keyType,
                        valueTypes,
                        keyCapacity,
                        configuration.getSqlFastMapLoadFactor(),
                        configuration.getSqlMapMaxResizes()
                );
            } else if (Unordered8Map.isSupportedKeyType(keyType) && Long.BYTES + valueSize <= maxEntrySize) {
                return new Unordered8Map(
                        keyType,
                        valueTypes,
                        keyCapacity,
                        configuration.getSqlFastMapLoadFactor(),
                        configuration.getSqlMapMaxResizes()
                );
            } else if (keyType == ColumnType.VARCHAR && 2 * Long.BYTES + valueSize <= maxEntrySize) {
                return new UnorderedVarcharMap(
                        valueTypes,
                        keyCapacity,
                        configuration.getSqlFastMapLoadFactor(),
                        configuration.getSqlMapMaxResizes(),
                        configuration.getGroupByAllocatorDefaultChunkSize(),
                        configuration.getGroupByAllocatorMaxChunkSize()
                );
            }
        }

        return new OrderedMap(
                pageSize,
                keyTypes,
                valueTypes,
                keyCapacity,
                configuration.getSqlFastMapLoadFactor(),
                configuration.getSqlMapMaxResizes()
        );
    }
}
