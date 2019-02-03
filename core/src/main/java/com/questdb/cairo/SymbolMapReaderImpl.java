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

package com.questdb.cairo;

import com.questdb.cairo.sql.RowCursor;
import com.questdb.cairo.sql.SymbolTable;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.*;
import com.questdb.std.str.Path;

import java.io.Closeable;

import static com.questdb.cairo.SymbolMapWriter.*;

public class SymbolMapReaderImpl implements Closeable, SymbolMapReader {
    private static final Log LOG = LogFactory.getLog(SymbolMapReaderImpl.class);
    private final BitmapIndexBwdReader indexReader = new BitmapIndexBwdReader();
    private final ReadOnlyMemory charMem = new ReadOnlyMemory();
    private final ReadOnlyMemory offsetMem = new ReadOnlyMemory();
    private final ObjList<String> cache = new ObjList<>();
    private int maxHash;
    private boolean cached;
    private int symbolCount;
    private long maxOffset;
    private int symbolCapacity;

    public SymbolMapReaderImpl(CairoConfiguration configuration, Path path, CharSequence name, int symbolCount) {
        of(configuration, path, name, symbolCount);
    }

    @Override
    public void close() {
        Misc.free(indexReader);
        Misc.free(charMem);
        this.cache.clear();
        long fd = this.offsetMem.getFd();
        Misc.free(offsetMem);
        LOG.info().$("closed [fd=").$(fd).$(']').$();
    }

    @Override
    public int getQuick(CharSequence symbol) {
        if (symbol == null) {
            return SymbolTable.VALUE_IS_NULL;
        }

        int hash = Hash.boundedHash(symbol, maxHash);
        RowCursor cursor = indexReader.getCursor(true, hash, 0, maxOffset);
        while (cursor.hasNext()) {
            long offsetOffset = cursor.next();
            if (Chars.equals(symbol, charMem.getStr(offsetMem.getLong(offsetOffset)))) {
                return offsetToKey(offsetOffset);
            }
        }
        return SymbolTable.VALUE_NOT_FOUND;
    }

    @Override
    public int size() {
        return symbolCount;
    }

    @Override
    public CharSequence value(int key) {
        if (key > -1 && key < symbolCount) {
            if (cached) {
                return cachedValue(key);
            }
            return uncachedValue(key);
        }
        return null;
    }

    @Override
    public int getSymbolCapacity() {
        return symbolCapacity;
    }

    @Override
    public boolean isCached() {
        return cached;
    }

    @Override
    public boolean isDeleted() {
        return offsetMem.isDeleted();
    }

    public void of(CairoConfiguration configuration, Path path, CharSequence name, int symbolCount) {
        FilesFacade ff = configuration.getFilesFacade();
        this.symbolCount = symbolCount;
        this.maxOffset = keyToOffset(symbolCount - 1);
        final int plen = path.length();
        try {
            final long mapPageSize = configuration.getFilesFacade().getMapPageSize();

            // this constructor does not create index. Index must exist
            // and we use "offset" file to store "header"
            offsetFileName(path.trimTo(plen), name);
            if (!ff.exists(path)) {
                LOG.error().$(path).$(" is not found").$();
                throw CairoException.instance(0).put("SymbolMap does not exist: ").put(path);
            }

            // is there enough length in "offset" file for "header"?
            long len = ff.length(path);
            if (len < HEADER_SIZE) {
                LOG.error().$(path).$(" is too short [len=").$(len).$(']').$();
                throw CairoException.instance(0).put("SymbolMap is too short: ").put(path);
            }

            // open "offset" memory and make sure we start appending from where
            // we left off. Where we left off is stored externally to symbol map
            this.offsetMem.of(ff, path, mapPageSize, keyToOffset(symbolCount));
            symbolCapacity = offsetMem.getInt(0);
            this.cached = offsetMem.getBool(4);
            this.offsetMem.grow(maxOffset);

            // index writer is used to identify attempts to store duplicate symbol value
            this.indexReader.of(configuration, path.trimTo(plen), name, 0);

            // this is the place where symbol values are stored
            this.charMem.of(ff, charFileName(path.trimTo(plen), name), mapPageSize, 0);

            // move append pointer for symbol values in the correct place
            growCharMemToSymbolCount(symbolCount);

            // we use index hash maximum equals to half of symbol capacity, which
            // theoretically should require 2 value cells in index per hash
            // we use 4 cells to compensate for occasionally unlucky hash distribution
            this.maxHash = Numbers.ceilPow2(symbolCapacity / 2) - 1;
            if (cached) {
                this.cache.setPos(symbolCapacity);
            }
            this.cache.clear();
            LOG.info().$("open [name=").$(path.trimTo(plen).concat(name).$()).$(", fd=").$(this.offsetMem.getFd()).$(", capacity=").$(symbolCapacity).$(']').$();
        } catch (CairoException e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void updateSymbolCount(int symbolCount) {
        if (symbolCount > this.symbolCount) {
            this.symbolCount = symbolCount;
            this.maxOffset = keyToOffset(symbolCount - 1);
            this.offsetMem.grow(maxOffset);
            growCharMemToSymbolCount(symbolCount);
        }
    }

    private CharSequence cachedValue(int key) {
        String symbol = cache.getQuiet(key);
        return symbol != null ? symbol : fetchAndCache(key);
    }

    private CharSequence fetchAndCache(int key) {
        String symbol;
        CharSequence cs = charMem.getStr(offsetMem.getLong(keyToOffset(key)));
        assert cs != null;
        cache.extendAndSet(key, symbol = cs.toString());
        return symbol;
    }

    private void growCharMemToSymbolCount(int symbolCount) {
        if (symbolCount > 0) {
            long lastSymbolOffset = this.offsetMem.getLong(keyToOffset(symbolCount - 1));
            this.charMem.grow(lastSymbolOffset + 4);
            this.charMem.grow(lastSymbolOffset + this.charMem.getStrLen(lastSymbolOffset) * 2 + 4);
        } else {
            this.charMem.grow(0);
        }
    }

    private CharSequence uncachedValue(int key) {
        return charMem.getStr(offsetMem.getLong(keyToOffset(key)));
    }
}
