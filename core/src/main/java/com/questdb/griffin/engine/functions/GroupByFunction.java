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

package com.questdb.griffin.engine.functions;

import com.questdb.cairo.ArrayColumnTypes;
import com.questdb.cairo.map.MapValue;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;

public interface GroupByFunction extends Function {
    void computeFirst(MapValue mapValue, Record record);

    void computeNext(MapValue mapValue, Record record);

    void pushValueTypes(ArrayColumnTypes columnTypes);

    default void setByte(MapValue mapValue, byte value) {
        throw new UnsupportedOperationException();
    }

    default void setDate(MapValue mapValue, long value) {
        throw new UnsupportedOperationException();
    }

    default void setDouble(MapValue mapValue, double value) {
        throw new UnsupportedOperationException();
    }

    default void setFloat(MapValue mapValue, float value) {
        throw new UnsupportedOperationException();
    }

    default void setInt(MapValue mapValue, int value) {
        throw new UnsupportedOperationException();
    }

    default void setLong(MapValue mapValue, long value) {
        throw new UnsupportedOperationException();
    }

    void setNull(MapValue mapValue);

    default void setShort(MapValue mapValue, short value) {
        throw new UnsupportedOperationException();
    }

    default void setTimestamp(MapValue mapValue, long value) {
        throw new UnsupportedOperationException();
    }
}
