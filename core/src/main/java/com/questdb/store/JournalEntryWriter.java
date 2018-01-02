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

package com.questdb.store;

import com.questdb.std.ex.JournalException;
import com.questdb.std.str.DirectBytes;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface JournalEntryWriter {
    void append() throws JournalException;

    void put(int index, byte value);

    void putBin(int index, InputStream value);

    OutputStream putBin(int index);

    void putBin(int index, ByteBuffer buf);

    void putBool(int index, boolean value);

    void putDate(int index, long value);

    void putDouble(int index, double value);

    void putFloat(int index, float value);

    void putInt(int index, int value);

    void putLong(int index, long value);

    void putNull(int index);

    void putShort(int index, short value);

    void putStr(int index, CharSequence value);

    void putStr(int index, DirectBytes value);

    void putSym(int index, CharSequence value);
}
