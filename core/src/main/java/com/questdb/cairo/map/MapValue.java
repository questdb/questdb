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

public interface MapValue {

    boolean getBool(int columnIndex);

    byte getByte(int index);

    long getDate(int columnIndex);

    double getDouble(int index);

    float getFloat(int index);

    int getInt(int index);

    long getLong(int index);

    short getShort(int index);

    long getTimestamp(int columnIndex);

    boolean isNew();

    void putBool(int columnIndex, boolean value);

    void putByte(int index, byte value);

    void putDate(int index, long value);

    void putDouble(int index, double value);

    void putFloat(int index, float value);

    void putInt(int index, int value);

    void putLong(int index, long value);

    void putShort(int index, short value);

    void putTimestamp(int columnIndex, long value);
}
