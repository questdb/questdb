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

import com.questdb.common.SymbolTable;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.Chars;
import com.questdb.std.Hash;
import com.questdb.std.Misc;
import com.questdb.std.Numbers;
import com.questdb.std.str.Path;

import java.io.Closeable;

public class SymbolMapReader implements Closeable {
    private static final Log LOG = LogFactory.getLog(SymbolMapReader.class);

    private final BitmapIndexBackwardReader indexReader;
    private final ReadOnlyMemory charMem;
    private final ReadOnlyMemory offsetMem;
    private final int maxHash;
    private long symbolCount;
    private long maxOffset;

    public SymbolMapReader(CairoConfiguration configuration, Path path, CharSequence name, long symbolCount) {
        this.symbolCount = symbolCount;
        this.maxOffset = SymbolMapWriter.keyToOffset(symbolCount - 1);
        final int plen = path.length();
        try {
            final long mapPageSize = configuration.getFilesFacade().getMapPageSize();

            // this constructor does not create index. Index must exist
            // and we use "offset" file to store "header"
            path.trimTo(plen).concat(name).put(".o").$();
            if (!configuration.getFilesFacade().exists(path)) {
                LOG.error().$(path).$(" is not found").$();
                throw CairoException.instance(0).put("SymbolMap does not exist: ").put(path);
            }

            // is there enough length in "offset" file for "header"?
            long len = configuration.getFilesFacade().length(path);
            if (len < SymbolMapWriter.HEADER_SIZE) {
                LOG.error().$(path).$(" is too short [len=").$(len).$(']').$();
                throw CairoException.instance(0).put("SymbolMap is too short: ").put(path);
            }

            // open "offset" memory and make sure we start appending from where
            // we left off. Where we left off is stored externally to symbol map
            this.offsetMem = new ReadOnlyMemory(configuration.getFilesFacade(), path, mapPageSize);
            final int symbolCapacity = offsetMem.getInt(0);
            this.offsetMem.grow(maxOffset);

            // index writer is used to identify attempts to store duplicate symbol value
            this.indexReader = new BitmapIndexBackwardReader(configuration, path.trimTo(plen), name);

            // this is the place where symbol values are stored
            this.charMem = new ReadOnlyMemory(configuration.getFilesFacade(), path.trimTo(plen).concat(name).put(".c").$(), mapPageSize);

            // move append pointer for symbol values in the correct place
            growCharMemToSymbolCount(symbolCount);

            // we use index hash maximum equals to half of symbol capacity, which
            // theoretically should require 2 value cells in index per hash
            // we use 4 cells to compensate for occasionally unlucky hash distribution
            this.maxHash = Numbers.ceilPow2(symbolCapacity / 2) - 1;
            LOG.info().$("open [name=").$(path.trimTo(plen).concat(name).$()).$(", fd=").$(this.offsetMem.getFd()).$(", capacity=").$(symbolCapacity).$(']').$();
        } catch (CairoException e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void close() {
        Misc.free(indexReader);
        Misc.free(charMem);
        if (this.offsetMem != null) {
            long fd = this.offsetMem.getFd();
            Misc.free(offsetMem);
            LOG.info().$("closed [fd=").$(fd).$(']').$();
        }
    }

    public long getQuick(CharSequence symbol) {
        if (symbol == null) {
            return SymbolTable.VALUE_IS_NULL;
        }

        int hash = Hash.boundedHash(symbol, maxHash);
        BitmapIndexCursor cursor = indexReader.getCursor(hash, maxOffset);
        while (cursor.hasNext()) {
            long offsetOffset = cursor.next();
            if (Chars.equals(symbol, charMem.getStr(offsetMem.getLong(offsetOffset)))) {
                return SymbolMapWriter.offsetToKey(offsetOffset);
            }
        }
        return SymbolTable.VALUE_NOT_FOUND;
    }

    public void updateSymbolCount(long symbolCount) {
        if (symbolCount > this.symbolCount) {
            this.symbolCount = symbolCount;
            this.maxOffset = SymbolMapWriter.keyToOffset(symbolCount - 1);
            this.offsetMem.grow(maxOffset);
            growCharMemToSymbolCount(symbolCount);
        }
    }

    public CharSequence value(long key) {
        if (key < 0) {
            return null;
        }

        if (key < symbolCount) {
            return charMem.getStr(offsetMem.getLong(SymbolMapWriter.keyToOffset(key)));
        }
        throw CairoException.instance(0).put("Invalid key: ").put(key);
    }

    private void growCharMemToSymbolCount(long symbolCount) {
        if (symbolCount > 0) {
            long lastSymbolOffset = this.offsetMem.getLong(SymbolMapWriter.keyToOffset(symbolCount - 1));
            int l = VirtualMemory.getStorageLength(this.charMem.getStr(lastSymbolOffset));
            this.charMem.grow(lastSymbolOffset + l);
        } else {
            this.charMem.grow(0);
        }
    }
}
