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
        return createOrderedMap(configuration, keyTypes, valueTypes, true);
    }

    /**
     * Creates a Map for SAMPLE BY, GROUP BY queries with control over initial allocation.
     *
     * @param openOnInit when {@code true}, the map's native backing is allocated by the constructor
     *                   (existing eager behavior). When {@code false}, the map is constructed in a
     *                   closed state and the first {@link Map#reopen()} call allocates the backing.
     *                   Lazy mode is used by owning factories that need to bind a per-query
     *                   {@code MemoryTracker} before any allocation happens, so malloc and free are
     *                   charged symmetrically on the per-query counter from the first cursor.
     */
    public static Map createOrderedMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            boolean openOnInit
    ) {
        final int keyCapacity = configuration.getSqlSmallMapKeyCapacity();
        final long pageSize = configuration.getSqlSmallMapPageSize();
        return new OrderedMap(
                pageSize,
                keyTypes,
                valueTypes,
                keyCapacity,
                configuration.getSqlFastMapLoadFactor(),
                configuration.getSqlMapMaxResizes(),
                openOnInit
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
        return createUnorderedMap(configuration, keyTypes, valueTypes, false, true);
    }

    /**
     * Creates an unordered Map pre-allocated to a small capacity to be used in GROUP BY queries, but not only.
     * <p>
     * The returned map may actually preserve insertion order, i.e. it may be ordered, depending on the types
     * of key and value columns.
     *
     * @param isDeferredKeyCopy when true, UnorderedVarcharMap skips the defensive copy of unstable varchar keys
     *                          in putVarchar() and copyFrom(), deferring the copy to asNew(). Only safe when the
     *                          caller guarantees the key pointer remains valid from putVarchar() through createValue().
     */
    public static Map createUnorderedMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            boolean isDeferredKeyCopy
    ) {
        return createUnorderedMap(configuration, keyTypes, valueTypes, isDeferredKeyCopy, true);
    }

    /**
     * Variant of {@link #createUnorderedMap(CairoConfiguration, ColumnTypes, ColumnTypes, boolean)}
     * with control over initial allocation.
     *
     * @param openOnInit when {@code true}, the map's native backing is allocated by the constructor
     *                   (existing eager behavior). When {@code false}, the map is constructed in a
     *                   closed state and the first {@link Map#reopen()} call allocates the backing.
     *                   Lazy mode is used by owning factories that need to bind a per-query
     *                   {@code MemoryTracker} before any allocation happens, so malloc and free are
     *                   charged symmetrically on the per-query counter from the first cursor.
     */
    public static Map createUnorderedMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            boolean isDeferredKeyCopy,
            boolean openOnInit
    ) {
        final int keyCapacity = configuration.getSqlSmallMapKeyCapacity();
        final long pageSize = configuration.getSqlSmallMapPageSize();
        return createUnorderedMap(configuration, keyTypes, valueTypes, keyCapacity, pageSize, isDeferredKeyCopy, openOnInit);
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
        return createUnorderedMap(configuration, keyTypes, valueTypes, keyCapacity, pageSize, false, true);
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
            long pageSize,
            boolean isDeferredKeyCopy
    ) {
        return createUnorderedMap(configuration, keyTypes, valueTypes, keyCapacity, pageSize, isDeferredKeyCopy, true);
    }

    /**
     * Variant of {@link #createUnorderedMap(CairoConfiguration, ColumnTypes, ColumnTypes, int, long, boolean)}
     * with control over initial allocation. See the {@code openOnInit} parameter for details.
     */
    public static Map createUnorderedMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            long pageSize,
            boolean isDeferredKeyCopy,
            boolean openOnInit
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
                        configuration.getSqlMapMaxResizes(),
                        openOnInit
                );
            } else if (Unordered8Map.isSupportedKeyType(keyType) && Long.BYTES + valueSize <= maxEntrySize) {
                return new Unordered8Map(
                        keyType,
                        valueTypes,
                        keyCapacity,
                        configuration.getSqlFastMapLoadFactor(),
                        configuration.getSqlMapMaxResizes(),
                        openOnInit
                );
            } else if (keyType == ColumnType.VARCHAR && 2 * Long.BYTES + valueSize <= maxEntrySize) {
                return new UnorderedVarcharMap(
                        valueTypes,
                        keyCapacity,
                        configuration.getSqlFastMapLoadFactor(),
                        configuration.getSqlMapMaxResizes(),
                        configuration.getGroupByAllocatorDefaultChunkSize(),
                        configuration.getGroupByAllocatorMaxChunkSize(),
                        isDeferredKeyCopy,
                        openOnInit
                );
            }
        }

        return new OrderedMap(
                pageSize,
                keyTypes,
                valueTypes,
                keyCapacity,
                configuration.getSqlFastMapLoadFactor(),
                configuration.getSqlMapMaxResizes(),
                openOnInit
        );
    }
}
