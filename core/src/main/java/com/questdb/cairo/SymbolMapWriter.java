/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.cairo;

import com.questdb.std.Chars;
import com.questdb.std.Hash;
import com.questdb.std.Misc;
import com.questdb.std.Numbers;
import com.questdb.std.str.Path;

import java.io.Closeable;

public class SymbolMapWriter implements Closeable {
    private final BitmapIndexWriter writer;
    private final ReadWriteMemory charMem;
    private final ReadWriteMemory offsetMem;
    private final int maxHash;

    public SymbolMapWriter(CairoConfiguration configuration, Path path, CharSequence name, int symbolCapacity) {
        final int plen = path.length();
        final long mapPageSize = configuration.getFilesFacade().getMapPageSize();

        this.writer = new BitmapIndexWriter(configuration, path, name, 4);
        this.charMem = new ReadWriteMemory(configuration.getFilesFacade(), path.trimTo(plen).concat(name).put(".c").$(), mapPageSize);
        this.offsetMem = new ReadWriteMemory(configuration.getFilesFacade(), path.trimTo(plen).concat(name).put(".o").$(), mapPageSize);
        this.maxHash = Numbers.ceilPow2(symbolCapacity / 2) - 1;
    }

    @Override
    public void close() {
        Misc.free(writer);
        Misc.free(charMem);
        Misc.free(offsetMem);
    }

    public long put(CharSequence symbol) {
        int key = Hash.boundedHash(symbol, maxHash);
        BitmapIndexCursor cursor = writer.getCursor(key);
        while (cursor.hasNext()) {
            long offsetOffset = cursor.next();
            long offset = offsetMem.getLong(offsetOffset);
            if (Chars.equals(symbol, charMem.getStr(offset))) {
                return offsetOffset / 8;
            }
        }

        long offset = charMem.putStr(symbol);
        long offsetOffset = offsetMem.getAppendOffset();
        offsetMem.putLong(offset);
        writer.add(key, offsetOffset);
        return offsetOffset / 8;
    }
}
