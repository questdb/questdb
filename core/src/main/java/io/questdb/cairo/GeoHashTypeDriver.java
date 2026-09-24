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

package io.questdb.cairo;

import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.Vect;

/**
 * Type driver for the geohash family: GEOBYTE, GEOSHORT, GEOINT and GEOLONG are one type
 * stored at four widths, so they share one class with one instance per tag. The number of
 * bits is part of the encoded column type and is passed as an argument where a method
 * needs it. NULL is -1 at every width.
 */
public final class GeoHashTypeDriver extends FixedSizeTypeDriver {
    public static final GeoHashTypeDriver GEOBYTE = new GeoHashTypeDriver(ColumnTypeTag.GEOBYTE, 0);
    public static final GeoHashTypeDriver GEOINT = new GeoHashTypeDriver(ColumnTypeTag.GEOINT, 2);
    public static final GeoHashTypeDriver GEOLONG = new GeoHashTypeDriver(ColumnTypeTag.GEOLONG, 3);
    public static final GeoHashTypeDriver GEOSHORT = new GeoHashTypeDriver(ColumnTypeTag.GEOSHORT, 1);

    private GeoHashTypeDriver(ColumnTypeTag tag, int pow2Width) {
        super(tag, pow2Width);
    }

    @Override
    public long getNullLong(int longIndex) {
        return GeoHashes.NULL;
    }

    @Override
    public boolean hasNullSentinel() {
        return true;
    }

    @Override
    public Runnable newNullAppender(MemoryA dataMem, MemoryA auxMem) {
        return switch (getPow2Width()) {
            case 0 -> () -> dataMem.putByte(GeoHashes.BYTE_NULL);
            case 1 -> () -> dataMem.putShort(GeoHashes.SHORT_NULL);
            case 2 -> () -> dataMem.putInt(GeoHashes.INT_NULL);
            case 3 -> () -> dataMem.putLong(GeoHashes.NULL);
            default -> throw new IllegalStateException("no geohash width " + getPow2Width());
        };
    }

    @Override
    public void setNull(long addr, long count) {
        switch (getPow2Width()) {
            case 0 -> Vect.memset(addr, count, GeoHashes.BYTE_NULL);
            case 1 -> Vect.setMemoryShort(addr, GeoHashes.SHORT_NULL, count);
            case 2 -> Vect.setMemoryInt(addr, GeoHashes.INT_NULL, count);
            case 3 -> Vect.setMemoryLong(addr, GeoHashes.NULL, count);
            default -> throw new IllegalStateException("no geohash width " + getPow2Width());
        }
    }
}
