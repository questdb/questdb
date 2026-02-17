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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.AbstractGeoHashFunction;
import io.questdb.std.Mutable;

class GeoHashBindVariable extends AbstractGeoHashFunction implements Mutable {
    long value;

    public GeoHashBindVariable() {
        super(ColumnType.GEOLONG);
    }

    @Override
    public void clear() {
        this.value = GeoHashes.NULL;
    }

    @Override
    public byte getGeoByte(Record rec) {
        return (byte) value;
    }

    @Override
    public int getGeoInt(Record rec) {
        return (int) value;
    }

    @Override
    public long getGeoLong(Record rec) {
        return value;
    }

    @Override
    public short getGeoShort(Record rec) {
        return (short) value;
    }

    @Override
    public boolean isNonDeterministic() {
        return true;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("?::geohash");
    }

    void setType(int type) {
        this.type = type;
    }
}
