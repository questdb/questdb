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
        final int pageSize = configuration.getSqlSmallMapPageSize();
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
        final int pageSize = configuration.getSqlSmallMapPageSize();
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
        final int pageSize = configuration.getSqlSmallMapPageSize();
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
            int pageSize
    ) {
        final int maxEntrySize = configuration.getSqlUnorderedMapMaxEntrySize();

        final int keySize = totalSize(keyTypes);
        final int valueSize = totalSize(keyTypes);
        if (keySize > 0) {
            if (keySize <= Short.BYTES && valueSize <= maxEntrySize) {
                return new Unordered2Map(keyTypes, valueTypes);
            } else if (keySize <= Integer.BYTES && Integer.BYTES + valueSize <= maxEntrySize) {
                return new Unordered4Map(
                        keyTypes,
                        valueTypes,
                        keyCapacity,
                        configuration.getSqlFastMapLoadFactor(),
                        configuration.getSqlMapMaxResizes()
                );
            } else if (keySize <= Long.BYTES && Long.BYTES + valueSize <= maxEntrySize) {
                return new Unordered8Map(
                        keyTypes,
                        valueTypes,
                        keyCapacity,
                        configuration.getSqlFastMapLoadFactor(),
                        configuration.getSqlMapMaxResizes()
                );
            } else if (keySize <= 2 * Long.BYTES && 2 * Long.BYTES + valueSize <= maxEntrySize) {
                return new Unordered16Map(
                        keyTypes,
                        valueTypes,
                        keyCapacity,
                        configuration.getSqlFastMapLoadFactor(),
                        configuration.getSqlMapMaxResizes()
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

    /**
     * Returns total size in case of all fixed-size columns
     * or -1 if there is a var-size column in the given list.
     */
    private static int totalSize(ColumnTypes types) {
        int keySize = 0;
        for (int i = 0, n = types.getColumnCount(); i < n; i++) {
            final int columnType = types.getColumnType(i);
            final int size = ColumnType.sizeOf(columnType);
            if (size > 0) {
                keySize += size;
            } else {
                return -1;
            }
        }
        return keySize;
    }
}
