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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.TypeConstant;
import io.questdb.griffin.engine.functions.GeoByteFunction;

public class GeoHashTypeConstant extends GeoByteFunction implements TypeConstant {
    private final static GeoHashTypeConstant[] INSTANCES = new GeoHashTypeConstant[ColumnType.GEOLONG_MAX_BITS];

    private GeoHashTypeConstant(int typep) {
        super(typep);
    }

    public static GeoHashTypeConstant getInstanceByPrecision(int bitPrecision) {
        return INSTANCES[bitPrecision - 1];
    }

    @Override
    public byte getGeoByte(Record rec) {
        return GeoHashes.BYTE_NULL;
    }

    @Override
    public int getGeoInt(Record rec) {
        return GeoHashes.INT_NULL;
    }

    @Override
    public long getGeoLong(Record rec) {
        return GeoHashes.NULL;
    }

    @Override
    public short getGeoShort(Record rec) {
        return GeoHashes.SHORT_NULL;
    }

    static {
        for (int i = 0; i < ColumnType.GEOLONG_MAX_BITS; i++) {
            INSTANCES[i] = new GeoHashTypeConstant(ColumnType.getGeoHashTypeWithBits(i + 1));
        }
    }
}
