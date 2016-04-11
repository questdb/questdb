/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.store;

import com.nfsdb.JournalMode;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalInvalidSymbolValueException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Hash;
import com.nfsdb.misc.Misc;
import com.nfsdb.misc.Numbers;
import com.nfsdb.std.AbstractImmutableIterator;
import com.nfsdb.std.CharSequenceIntHashMap;
import com.nfsdb.std.ObjList;

import java.io.Closeable;
import java.io.File;

public class SymbolTable implements Closeable {

    public static final int VALUE_NOT_FOUND = -2;
    public static final int VALUE_IS_NULL = -1;
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
    private VariableColumn data;
    private KVIndex index;
    private int size;

    public SymbolTable(int keyCount, int avgStringSize, int txCountHint, File directory, String column, JournalMode mode, int size, long indexTxAddress, boolean noCache) throws JournalException {
        // number of hash keys stored in index
        // assume it is 20% of stated capacity
        this.hashKeyCount = Numbers.ceilPow2(Math.max(2, (int) (keyCount * CACHE_LOAD_FACTOR))) - 1;
        this.column = column;
        this.noCache = noCache;
        JournalMode m;

        switch (mode) {
            case BULK_APPEND:
                m = JournalMode.APPEND;
                break;
            case BULK_READ:
                m = JournalMode.READ;
                break;
            default:
                m = mode;
                break;
        }

        MemoryFile dataFile = new MemoryFile(new File(directory, column + DATA_FILE_SUFFIX), ByteBuffers.getBitHint(avgStringSize * 2 + 4, keyCount), m);
        MemoryFile indexFile = new MemoryFile(new File(directory, column + INDEX_FILE_SUFFIX), ByteBuffers.getBitHint(8, keyCount), m);

        this.data = new VariableColumn(dataFile, indexFile);
        this.size = size;

        this.index = new KVIndex(new File(directory, column + HASH_INDEX_FILE_SUFFIX), this.hashKeyCount, keyCount, txCountHint, mode, indexTxAddress);
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

    public void close() {
        data = Misc.free(data);
        index = Misc.free(index);
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

    public SymbolTable preLoad() {
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
            cache(key, value.toString());
        }
        return key;
    }

    public int size() {
        return size;
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

    public String value(int key) {
        if (key == VALUE_IS_NULL) {
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

    public boolean valueExists(CharSequence value) {
        return getQuick(value) != VALUE_NOT_FOUND;
    }

    public Iterable<Entry> values() {
        iter.pos = 0;
        iter.size = size();
        return iter;
    }

    private void cache(int key, String value) {
        if (noCache) {
            return;
        }

        valueCache.put(value, key);
        keyCache.extendAndSet(key, value);
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
                String s = value.toString();
                cache(key, s);
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

    private class Iter extends AbstractImmutableIterator<Entry> {
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
