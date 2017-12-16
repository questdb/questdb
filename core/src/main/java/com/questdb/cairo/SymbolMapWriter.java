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

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.*;
import com.questdb.std.str.Path;

import java.io.Closeable;

public class SymbolMapWriter implements Closeable {
    private static final int HEADER_SIZE = 64;
    private static final Log LOG = LogFactory.getLog(SymbolMapWriter.class);

    private final BitmapIndexWriter writer;
    private final ReadWriteMemory charMem;
    private final ReadWriteMemory offsetMem;
    private final CharSequenceLongHashMap cache;
    private final int maxHash;

    public SymbolMapWriter(CairoConfiguration configuration, Path path, CharSequence name, boolean useCache) {
        final int plen = path.length();
        try {
            final long mapPageSize = configuration.getFilesFacade().getMapPageSize();

            path.trimTo(plen).concat(name).put(".o").$();
            if (!configuration.getFilesFacade().exists(path)) {
                LOG.error().$(path).$(" is not found").$();
                throw CairoException.instance(0).put("SymbolMap does not exist: ").put(path);
            }

            long len = configuration.getFilesFacade().length(path);
            if (len < HEADER_SIZE) {
                LOG.error().$(path).$(" is too short [len=").$(len).$(']').$();
                throw CairoException.instance(0).put("SymbolMap is too short: ").put(path);
            }

            this.offsetMem = new ReadWriteMemory(configuration.getFilesFacade(), path, mapPageSize);
            final int symbolCapacity = offsetMem.getInt(0);

            this.writer = new BitmapIndexWriter(configuration, path.trimTo(plen), name, 4);
            this.charMem = new ReadWriteMemory(configuration.getFilesFacade(), path.trimTo(plen).concat(name).put(".c").$(), mapPageSize);
            this.maxHash = Numbers.ceilPow2(symbolCapacity / 2) - 1;
            if (useCache) {
                this.cache = new CharSequenceLongHashMap(symbolCapacity);
            } else {
                this.cache = null;
            }
            LOG.info().$("open [name=").$(path.trimTo(plen).concat(name).$()).$(", fd=").$(this.offsetMem.getFd()).$(", cache=").$(cache != null).$(", capacity=").$(symbolCapacity).$(']').$();
        } catch (CairoException e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    public static void create(CairoConfiguration configuration, Path path, CharSequence name, int symbolCapacity) {
        int plen = path.length();
        try (ReadWriteMemory mem = new ReadWriteMemory(configuration.getFilesFacade(), path.concat(name).put(".o").$(), configuration.getFilesFacade().getMapPageSize())) {
            mem.putInt(symbolCapacity);
            mem.jumpTo(HEADER_SIZE);
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void close() {
        Misc.free(writer);
        Misc.free(charMem);
        if (this.offsetMem != null) {
            long fd = this.offsetMem.getFd();
            Misc.free(offsetMem);
            LOG.info().$("closed [fd=").$(fd).$(']').$();
        }
    }

    public long put(CharSequence symbol) {
        if (cache != null) {
            long result = cache.get(symbol);
            if (result != -1) {
                return result;
            }
            result = lookupAndPut(symbol);
            cache.put(symbol.toString(), result);
            return result;
        }
        return lookupAndPut(symbol);
    }

    private long lookupAndPut(CharSequence symbol) {
        int hash = Hash.boundedHash(symbol, maxHash);
        BitmapIndexCursor cursor = writer.getCursor(hash);
        while (cursor.hasNext()) {
            long offsetOffset = cursor.next();
            if (Chars.equals(symbol, charMem.getStr(offsetMem.getLong(offsetOffset)))) {
                return (offsetOffset - HEADER_SIZE) / 8;
            }
        }
        return put0(symbol, hash);
    }

    private long put0(CharSequence symbol, int hash) {
        long offsetOffset = offsetMem.getAppendOffset();
        offsetMem.putLong(charMem.putStr(symbol));
        writer.add(hash, offsetOffset);
        return (offsetOffset - HEADER_SIZE) / 8;
    }
}
