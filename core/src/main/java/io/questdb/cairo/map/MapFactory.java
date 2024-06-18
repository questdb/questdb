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

package io.questdb.cairo.map;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.std.MemoryTag;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MapFactory {

    /**
     * Creates a Map pre-allocated to a small capacity to be used in SAMPLE BY, GROUP BY queries, but not only.
     * <p>
     * The returned map preserves insertion order in its cursor, i.e. when iterating the items.
     */
    public static Map createOrderedMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes
    ) {
        return createOrderedMap(
                configuration.getSqlSmallMapPageSize(),
                keyTypes,
                null,
                configuration.getSqlSmallMapKeyCapacity(),
                configuration.getSqlVarSizeMapLoadFactor(),
                configuration.getSqlMapMaxResizes(),
                MemoryTag.NATIVE_FAST_MAP
        );
    }

    /**
     * Creates a Map pre-allocated to a small capacity to be used in SAMPLE BY, GROUP BY queries, but not only.
     * <p>
     * The returned map preserves insertion order in its cursor, i.e. when iterating the items.
     */
    public static Map createOrderedMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes
    ) {
        return createOrderedMap(
                configuration.getSqlSmallMapPageSize(),
                keyTypes,
                valueTypes,
                configuration.getSqlSmallMapKeyCapacity(),
                configuration.getSqlVarSizeMapLoadFactor(),
                configuration.getSqlMapMaxResizes(),
                MemoryTag.NATIVE_FAST_MAP
        );
    }

    /**
     * The returned map preserves insertion order in its cursor, i.e. when iterating the items.
     */
    public static Map createOrderedMap(
            long heapSize,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes
    ) {
        return createOrderedMap(
                heapSize,
                keyTypes,
                valueTypes,
                keyCapacity,
                loadFactor,
                maxResizes,
                MemoryTag.NATIVE_FAST_MAP
        );
    }

    /**
     * The returned map preserves insertion order in its cursor, i.e. when iterating the items.
     */
    public static Map createOrderedMap(
            long heapSize,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            int memoryTag
    ) {
        final int keySize = totalSize(keyTypes);
        if (keySize > 0) {
            return new FixedSizeMap(
                    heapSize,
                    keyTypes,
                    valueTypes,
                    keyCapacity,
                    loadFactor,
                    maxResizes,
                    memoryTag
            );
        }

        return new VarSizeMap(
                heapSize,
                keyTypes,
                valueTypes,
                keyCapacity,
                loadFactor,
                maxResizes,
                memoryTag
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

        final int keySize = totalSize(keyTypes);
        final int valueSize = totalSize(valueTypes);

        if (keyTypes.getColumnCount() == 1) {
            final int keyType = keyTypes.getColumnType(0);
            if ((keyType == ColumnType.INT || keyType == ColumnType.IPv4 || keyType == ColumnType.GEOINT)
                    && keySize + valueSize <= maxEntrySize) {
                return new Unordered4Map(
                        keyTypes,
                        valueTypes,
                        keyCapacity,
                        configuration.getSqlFixedSizeMapLoadFactor(),
                        configuration.getSqlMapMaxResizes()
                );
            } else if ((keyType == ColumnType.LONG || keyType == ColumnType.GEOLONG)
                    && keySize + valueSize <= maxEntrySize) {
                return new Unordered8Map(
                        keyTypes,
                        valueTypes,
                        keyCapacity,
                        configuration.getSqlFixedSizeMapLoadFactor(),
                        configuration.getSqlMapMaxResizes()
                );
            } else if (keyType == ColumnType.VARCHAR && 2 * Long.BYTES + valueSize <= maxEntrySize) {
                return new UnorderedVarcharMap(
                        valueTypes,
                        keyCapacity,
                        configuration.getSqlVarSizeMapLoadFactor(),
                        configuration.getSqlMapMaxResizes(),
                        configuration.getGroupByAllocatorDefaultChunkSize(),
                        configuration.getGroupByAllocatorMaxChunkSize()
                );
            }
        }

        if (keySize > 0) {
            return new FixedSizeMap(
                    pageSize,
                    keyTypes,
                    valueTypes,
                    keyCapacity,
                    configuration.getSqlFixedSizeMapLoadFactor(),
                    configuration.getSqlMapMaxResizes()
            );
        }

        return new VarSizeMap(
                pageSize,
                keyTypes,
                valueTypes,
                keyCapacity,
                configuration.getSqlVarSizeMapLoadFactor(),
                configuration.getSqlMapMaxResizes()
        );
    }

    /**
     * Returns total size in case of all fixed-size columns
     * or -1 if there is a var-size column in the given list.
     */
    private static int totalSize(ColumnTypes types) {
        if (types == null) {
            return 0;
        }
        int totalSize = 0;
        for (int i = 0, n = types.getColumnCount(); i < n; i++) {
            final int columnType = types.getColumnType(i);
            final int size = ColumnType.sizeOf(columnType);
            if (size > 0) {
                totalSize += size;
            } else {
                return -1;
            }
        }
        return totalSize;
    }
}
