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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.map.MapValue;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.Unsafe;

public class InterpolationUtil {
    static final InterpolatorFunction INTERPOLATE_BYTE = InterpolationUtil::interpolateByte;
    static final InterpolatorFunction INTERPOLATE_DOUBLE = InterpolationUtil::interpolateDouble;
    static final InterpolatorFunction INTERPOLATE_FLOAT = InterpolationUtil::interpolateFloat;
    static final InterpolatorFunction INTERPOLATE_INT = InterpolationUtil::interpolateInt;
    static final InterpolatorFunction INTERPOLATE_LONG = InterpolationUtil::interpolateLong;
    static final InterpolatorFunction INTERPOLATE_SHORT = InterpolationUtil::interpolateShort;
    static final StoreYFunction STORE_Y_BYTE = InterpolationUtil::storeYByte;
    static final StoreYFunction STORE_Y_DOUBLE = InterpolationUtil::storeYDouble;
    static final StoreYFunction STORE_Y_FLOAT = InterpolationUtil::storeYFloat;
    static final StoreYFunction STORE_Y_INT = InterpolationUtil::storeYInt;
    static final StoreYFunction STORE_Y_LONG = InterpolationUtil::storeYLong;
    static final StoreYFunction STORE_Y_SHORT = InterpolationUtil::storeYShort;

    public static double interpolate(long x, long x1, double y1, long x2, double y2) {
        return (y1 * (x2 - x) + y2 * (x - x1)) / (x2 - x1);
    }

    public static void interpolateByte(
            GroupByFunction function,
            MapValue mapValue,
            long x,
            long x1,
            long x2,
            long y1Address,
            long y2Address
    ) {
        function.setByte(
                mapValue,
                (byte) interpolate(
                        x,
                        x1,
                        Unsafe.getUnsafe().getByte(y1Address),
                        x2,
                        Unsafe.getUnsafe().getByte(y2Address)
                )
        );
    }

    public static void interpolateFloat(
            GroupByFunction function,
            MapValue mapValue,
            long x,
            long x1,
            long x2,
            long y1Address,
            long y2Address
    ) {
        function.setFloat(
                mapValue,
                (float) interpolate(
                        x,
                        x1,
                        Unsafe.getUnsafe().getFloat(y1Address),
                        x2,
                        Unsafe.getUnsafe().getFloat(y2Address)
                )
        );
    }

    public static void interpolateInt(
            GroupByFunction function,
            MapValue mapValue,
            long x,
            long x1,
            long x2,
            long y1Address,
            long y2Address
    ) {
        function.setInt(
                mapValue,
                (int) interpolate(
                        x,
                        x1,
                        Unsafe.getUnsafe().getInt(y1Address),
                        x2,
                        Unsafe.getUnsafe().getInt(y2Address)
                )
        );
    }

    public static void interpolateLong(
            GroupByFunction function,
            MapValue mapValue,
            long x,
            long x1,
            long x2,
            long y1Address,
            long y2Address
    ) {
        function.setLong(
                mapValue,
                (long) interpolate(
                        x,
                        x1,
                        Unsafe.getUnsafe().getLong(y1Address),
                        x2,
                        Unsafe.getUnsafe().getLong(y2Address)
                )
        );
    }

    public static void interpolateShort(
            GroupByFunction function,
            MapValue mapValue,
            long x,
            long x1,
            long x2,
            long y1Address,
            long y2Address
    ) {
        function.setShort(
                mapValue,
                (short) interpolate(
                        x,
                        x1,
                        Unsafe.getUnsafe().getShort(y1Address),
                        x2,
                        Unsafe.getUnsafe().getShort(y2Address)
                )
        );
    }

    static void interpolateBoundary(
            GroupByFunction function,
            long boundaryTimestamp,
            MapValue mapValue1,
            MapValue mapValue2,
            boolean isEndOfBoundary
    ) {
        try {
            function.interpolateBoundary(
                    mapValue1,
                    mapValue2,
                    boundaryTimestamp,
                    isEndOfBoundary
            );
        } catch (UnsupportedOperationException e) {
            throw CairoException.nonCritical().put("interpolation is not supported for function: ").put(function.getClass().getName());
        }
    }

    static void interpolateDouble(
            GroupByFunction function,
            MapValue mapValue,
            long x,
            long x1,
            long x2,
            long y1Address,
            long y2Address
    ) {
        function.setDouble(
                mapValue,
                interpolate(
                        x,
                        x1,
                        Unsafe.getUnsafe().getDouble(y1Address),
                        x2,
                        Unsafe.getUnsafe().getDouble(y2Address)
                )
        );
    }

    static void interpolateGap(
            GroupByFunction function,
            MapValue mapValue,
            long x,
            MapValue mapValue1,
            MapValue mapValue2
    ) {
        try {
            function.interpolateGap(
                    mapValue,
                    mapValue1,
                    mapValue2,
                    x
            );
        } catch (UnsupportedOperationException e) {
            throw CairoException.nonCritical().put("interpolation is not supported for function: ").put(function.getClass().getName());
        }
    }

    static void storeYByte(GroupByFunction function, MapValue mapValue, long targetAddress) {
        Unsafe.getUnsafe().putByte(targetAddress, function.getByte(mapValue));
    }

    static void storeYDouble(GroupByFunction function, MapValue mapValue, long targetAddress) {
        Unsafe.getUnsafe().putDouble(targetAddress, function.getDouble(mapValue));
    }

    static void storeYFloat(GroupByFunction function, MapValue mapValue, long targetAddress) {
        Unsafe.getUnsafe().putFloat(targetAddress, function.getFloat(mapValue));
    }

    static void storeYInt(GroupByFunction function, MapValue mapValue, long targetAddress) {
        Unsafe.getUnsafe().putInt(targetAddress, function.getInt(mapValue));
    }

    static void storeYLong(GroupByFunction function, MapValue mapValue, long targetAddress) {
        Unsafe.getUnsafe().putLong(targetAddress, function.getLong(mapValue));
    }

    static void storeYShort(GroupByFunction function, MapValue mapValue, long targetAddress) {
        Unsafe.getUnsafe().putShort(targetAddress, function.getShort(mapValue));
    }

    interface InterpolatorFunction {
        void interpolateAndStore(GroupByFunction function, MapValue mapValue, long x, long x1, long x2, long y1Address, long y2Address);
    }

    @FunctionalInterface
    interface StoreYFunction {
        void store(GroupByFunction function, MapValue mapValue, long targetAddress);
    }
}
