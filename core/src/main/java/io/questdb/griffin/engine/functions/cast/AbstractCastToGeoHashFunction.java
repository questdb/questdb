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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GeoByteFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;

import static io.questdb.cairo.ColumnType.GEOLONG_MAX_BITS;

public abstract class AbstractCastToGeoHashFunction extends GeoByteFunction implements UnaryFunction {
    protected final Function arg;
    protected final int bitsPrecision;
    protected final int position;

    public AbstractCastToGeoHashFunction(int geoType, Function arg, int position) {
        super(geoType);
        this.arg = arg;
        this.position = position;
        this.bitsPrecision = ColumnType.getGeoHashBits(geoType);
        assert this.bitsPrecision > 0 && this.bitsPrecision < GEOLONG_MAX_BITS + 1;
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public byte getGeoByte(Record rec) {
        assert bitsPrecision < 8;
        return (byte) getGeoHashLong0(rec);
    }

    @Override
    public int getGeoInt(Record rec) {
        assert bitsPrecision >= 16 && bitsPrecision < 32;
        return (int) getGeoHashLong0(rec);
    }

    @Override
    public long getGeoLong(Record rec) {
        assert bitsPrecision >= 32;
        return getGeoHashLong0(rec);
    }

    @Override
    public short getGeoShort(Record rec) {
        assert bitsPrecision >= 8 && bitsPrecision < 16;
        return (short) getGeoHashLong0(rec);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(arg).val("::geohash");
    }

    protected abstract long getGeoHashLong0(Record rec);
}
