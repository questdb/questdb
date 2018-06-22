/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.cairo.map;

import com.questdb.cairo.sql.Record;
import com.questdb.std.BinarySequence;

public interface MapKey {

    default boolean create() {
        return createValue().isNew();
    }

    MapValue createValue();

    MapValue findValue();

    default boolean notFound() {
        return findValue() == null;
    }

    void putBin(BinarySequence value);

    void putBool(boolean value);

    void putByte(byte value);

    void putDate(long value);

    void putDouble(double value);

    void putFloat(float value);

    void putInt(int value);

    void putLong(long value);

    void putRecord(Record record, RecordSink sink);

    void putShort(short value);

    void putStr(CharSequence value);

    @SuppressWarnings("unused")
    void putTimestamp(long value);
}
