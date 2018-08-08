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

import com.questdb.ex.JournalInvalidSymbolValueException;
import com.questdb.std.*;
import com.questdb.std.ex.JournalException;

import java.io.Closeable;
import java.io.File;

public class MMappedSymbolTable implements Closeable, SymbolTable {

    private static final String DATA_FILE_SUFFIX = ".symd";
    private static final String INDEX_FILE_SUFFIX = ".symi";
    private static final String HASH_INDEX_FILE_SUFFIX = ".symr";
    private static final double CACHE_LOAD_FACTOR = 0.2;
    private final int hashKeyCount;
    private final String column;
    private final CharSequenceIntHashMap valueCache;
    private final ObjList<String> keyCache;
    private final boolean noCache;
    private final Iter iter = new Iter();
    private final VariableColumn data;
    private final KVIndex index;
    private int size;
    private boolean open = true;

    public MMappedSymbolTable(
            int keyCount,
            int avgStringSize,
            int txCountHint,
            File directory,
            String column,
            int journalMode,
            int size,
            long indexTxAddress,
            boolean noCache,
            boolean sequentialAccess) throws JournalException {
        // number of hash keys stored in index
        // assume it is 20% of stated capacity
        this.hashKeyCount = Numbers.ceilPow2(Math.max(2, (int) (keyCount * CACHE_LOAD_FACTOR))) - 1;
        this.column = column;
        this.noCache = noCache;

        MemoryFile dataFile = new MemoryFile(new File(directory, column + DATA_FILE_SUFFIX), ByteBuffers.getBitHint(avgStringSize * 2 + 4, keyCount), journalMode, sequentialAccess);
        MemoryFile indexFile;
        try {
            indexFile = new MemoryFile(new File(directory, column + INDEX_FILE_SUFFIX), ByteBuffers.getBitHint(8, keyCount), journalMode, sequentialAccess);
        } catch (JournalException e) {
            dataFile.close();
            throw e;
        }

        this.data = new VariableColumn(dataFile, indexFile);
        this.size = size;

        try {
            this.index = new KVIndex(new File(directory, column + HASH_INDEX_FILE_SUFFIX), this.hashKeyCount, keyCount, txCountHint, journalMode, indexTxAddress, sequentialAccess);
        } catch (JournalException e) {
            this.data.close();
            throw e;
        }
        this.valueCache = new CharSequenceIntHashMap(noCache ? 0 : keyCount, 0.5, VALUE_NOT_FOUND);
        this.keyCache = new ObjList<>(noCache ? 0 : keyCount);
    }

    public void alignSize() {
        this.size = (int) data.size();
    }

    public void applyTx(int size, long indexTxAddress) {
        this.size = size;
        this.index.setTxAddress(indexTxAddress);
    }

    @Override
    public void close() {
        if (open) {
            Misc.free(data);
            Misc.free(index);
            open = false;
        }
    }

    public void commit() {
        data.commit();
        index.commit();
    }

    public void force() {
        data.force();
        index.force();
    }

    public int get(CharSequence value) {
        int result = getQuick(value);
        if (result == VALUE_NOT_FOUND) {
            throw new JournalInvalidSymbolValueException("Invalid value %s for symbol %s", value, column);
        } else {
            return result;
        }
    }

    public VariableColumn getDataColumn() {
        return data;
    }

    public long getIndexTxAddress() {
        return index.getTxAddress();
    }

    @Override
    public int getQuick(CharSequence value) {
        if (value == null) {
            return VALUE_IS_NULL;
        }

        if (!noCache) {
            int key = valueCache.get(value);
            if (key != VALUE_NOT_FOUND) {
                return key;
            }
        }

        return get0(value);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public String value(int key) {
        if (key < 0) {
            return null;
        }

        if (key < size) {
            String value = key < keyCache.size() ? keyCache.getQuick(key) : null;
            if (value == null) {
                cache(key, value = data.getStr(key));
            }
            return value;
        }
        throw new JournalRuntimeException("Invalid symbol key: " + key);

    }

    public MMappedSymbolTable preLoad() {
        for (int key = 0, size = (int) data.size(); key < size; key++) {
            String value = data.getStr(key);
            valueCache.putIfAbsent(value, key);
            keyCache.add(value);

        }
        return this;
    }

    public int put(CharSequence value) {
        int key = getQuick(value);
        if (key == VALUE_NOT_FOUND) {
            key = (int) data.putStr(value);
            data.commit();
            index.add(hashKey(value), key);
            size++;
            cache(key, value);
        }
        return key;
    }

    public void setSequentialAccess(boolean sequentialAccess) {
        this.data.setSequentialAccess(sequentialAccess);
        this.index.setSequentialAccess(sequentialAccess);
    }

    public void truncate() {
        truncate(0);
    }

    public void truncate(int size) {
        if (size() > size) {
            data.truncate(size);
            index.truncate(size);
            data.commit();
            clearCache();
            this.size = size;
        }
    }

    public void updateIndex(int oldSize, int newSize) {
        if (oldSize < newSize) {
            for (int i = oldSize; i < newSize; i++) {
                index.add(hashKey(data.getStr(i)), i);
            }
        }
    }

    public boolean valueExists(CharSequence value) {
        return getQuick(value) != VALUE_NOT_FOUND;
    }

    public Iterable<Entry> values() {
        iter.pos = 0;
        iter.size = size();
        return iter;
    }

    private void cache(int key, CharSequence value) {
        if (noCache) {
            return;
        }

        final String str = value.toString();
        valueCache.put(str, key);
        keyCache.extendAndSet(key, str);
    }

    private void clearCache() {
        valueCache.clear();
        keyCache.clear();
    }

    private int get0(CharSequence value) {
        int hashKey = hashKey(value);

        if (!index.contains(hashKey)) {
            return VALUE_NOT_FOUND;
        }

        IndexCursor cursor = index.cursor(hashKey);
        while (cursor.hasNext()) {
            int key;
            if (data.cmpStr((key = (int) cursor.next()), value)) {
                cache(key, value);
                return key;
            }
        }
        return VALUE_NOT_FOUND;
    }

    private int hashKey(CharSequence value) {
        return Hash.boundedHash(value, hashKeyCount);
    }

    public static class Entry {
        public int key;
        public CharSequence value;
    }

    private class Iter implements com.questdb.std.ImmutableIterator<Entry> {
        private final Entry e = new Entry();
        private int pos;
        private int size;

        @Override
        public boolean hasNext() {
            return pos < size;
        }

        @Override
        public Entry next() {
            e.key = pos;
            e.value = data.getFlyweightStr(pos++);
            return e;
        }
    }
}
