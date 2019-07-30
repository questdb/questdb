/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.groupby;

import com.questdb.cairo.map.MapValue;
import com.questdb.griffin.engine.functions.GroupByFunction;
import com.questdb.std.Unsafe;

public class InterpolationUtil {
    static final StoreYFunction STORE_Y_DOUBLE = InterpolationUtil::storeYDouble;
    static final StoreYFunction STORE_Y_FLOAT = InterpolationUtil::storeYFloat;
    static final StoreYFunction STORE_Y_BYTE = InterpolationUtil::storeYByte;
    static final StoreYFunction STORE_Y_SHORT = InterpolationUtil::storeYShort;
    static final StoreYFunction STORE_Y_INT = InterpolationUtil::storeYInt;
    static final StoreYFunction STORE_Y_LONG = InterpolationUtil::storeYLong;

    static final InterpolatorFunction INTERPOLATE_DOUBLE = InterpolationUtil::interpolateDouble;
    static final InterpolatorFunction INTERPOLATE_FLOAT = InterpolationUtil::interpolateFloat;
    static final InterpolatorFunction INTERPOLATE_BYTE = InterpolationUtil::interpolateByte;
    static final InterpolatorFunction INTERPOLATE_SHORT = InterpolationUtil::interpolateShort;
    static final InterpolatorFunction INTERPOLATE_INT = InterpolationUtil::interpolateInt;
    static final InterpolatorFunction INTERPOLATE_LONG = InterpolationUtil::interpolateLong;

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

    private static double interpolate(long x, long x1, double y1, long x2, double y2) {
        return (y1 * (x2 - x) + y2 * (x - x1)) / (x2 - x1);
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

    static void storeYDouble(GroupByFunction function, MapValue mapValue, long targetAddress) {
        Unsafe.getUnsafe().putDouble(targetAddress, function.getDouble(mapValue));
    }

    static void storeYFloat(GroupByFunction function, MapValue mapValue, long targetAddress) {
        Unsafe.getUnsafe().putFloat(targetAddress, function.getFloat(mapValue));
    }

    static void storeYByte(GroupByFunction function, MapValue mapValue, long targetAddress) {
        Unsafe.getUnsafe().putByte(targetAddress, function.getByte(mapValue));
    }

    static void storeYShort(GroupByFunction function, MapValue mapValue, long targetAddress) {
        Unsafe.getUnsafe().putShort(targetAddress, function.getShort(mapValue));
    }

    static void storeYInt(GroupByFunction function, MapValue mapValue, long targetAddress) {
        Unsafe.getUnsafe().putInt(targetAddress, function.getInt(mapValue));
    }

    static void storeYLong(GroupByFunction function, MapValue mapValue, long targetAddress) {
        Unsafe.getUnsafe().putLong(targetAddress, function.getLong(mapValue));
    }

    @FunctionalInterface
    interface StoreYFunction {
        void store(GroupByFunction function, MapValue mapValue, long targetAddress);
    }

    interface InterpolatorFunction {
        void interpolateAndStore(GroupByFunction function, MapValue mapValue, long x, long x1, long x2, long y1Address, long y2Address);
    }
}
