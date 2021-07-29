/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.GeoHashExtra;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GeoHashFunction;
import io.questdb.griffin.engine.functions.geohash.GeoHashNative;
import io.questdb.std.Chars;
import io.questdb.std.NumericException;

public class GeoHashConstant extends GeoHashFunction implements ConstantFunction {
    public static final GeoHashConstant NULL = new GeoHashConstant(GeoHashExtra.NULL, ColumnType.GEOHASH);
    private final long hash; // does NOT encode size

    public GeoHashConstant(long hash, int typep) {
        super(typep);
        this.hash = hash;
    }

    public static GeoHashConstant newInstance(long hash, int type) {
        return new GeoHashConstant(hash, type);
    }

    @Override
    public byte getByte(Record rec) {
        return (byte) getLong(rec);
    }

    @Override
    public short getShort(Record rec) {
        return (short) getLong(rec);
    }

    @Override
    public int getInt(Record rec) {
        return (int) getLong(rec);
    }

    @Override
    public final long getLong(Record rec) {
        return hash;
    }
}
