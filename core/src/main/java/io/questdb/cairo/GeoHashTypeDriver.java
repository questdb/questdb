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

/**
 * Type driver for the geohash family: GEOBYTE, GEOSHORT, GEOINT and GEOLONG are one type
 * stored at four widths, so they share one class with one instance per tag. The number of
 * bits is part of the encoded column type and is passed as an argument where a method
 * needs it.
 */
public final class GeoHashTypeDriver extends FixedSizeTypeDriver {
    public static final GeoHashTypeDriver GEOBYTE = new GeoHashTypeDriver(ColumnTypeTag.GEOBYTE, 0);
    public static final GeoHashTypeDriver GEOINT = new GeoHashTypeDriver(ColumnTypeTag.GEOINT, 2);
    public static final GeoHashTypeDriver GEOLONG = new GeoHashTypeDriver(ColumnTypeTag.GEOLONG, 3);
    public static final GeoHashTypeDriver GEOSHORT = new GeoHashTypeDriver(ColumnTypeTag.GEOSHORT, 1);

    private GeoHashTypeDriver(ColumnTypeTag tag, int pow2Width) {
        super(tag, pow2Width);
    }
}
