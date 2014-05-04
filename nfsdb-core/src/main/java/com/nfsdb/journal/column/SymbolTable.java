/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.column;

import com.nfsdb.journal.JournalMode;
import com.nfsdb.journal.collections.LongArrayList;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalImmutableIteratorException;
import com.nfsdb.journal.exceptions.JournalInvalidSymbolValueException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.utils.ByteBuffers;
import com.nfsdb.journal.utils.Lists;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

public class SymbolTable implements Closeable {

    public static final int VALUE_NOT_FOUND = -2;
    public static final int VALUE_IS_NULL = -1;
    private static final String DATA_FILE_SUFFIX = ".symd";
    private static final String INDEX_FILE_SUFFIX = ".symi";
    private static final String HASH_INDEX_FILE_SUFFIX = ".symr";
    private static final int HASH_GROUPING_RATE = 25;
    private static final float CACHE_LOAD_FACTOR = 0.2f;
    private final int capacity;
    private final String column;
    private final TObjectIntHashMap<String> valueCache;
    private final ArrayList<String> keyCache;
    private VarcharColumn data;
    private SymbolIndex index;
    private int size;

    public SymbolTable(int capacity, int maxLen, File directory, String column, JournalMode mode, int size, long indexTxAddress) throws JournalException {
        this.capacity = capacity;
        this.column = column;
        MappedFile dataFile = new MappedFileImpl(new File(directory, column + DATA_FILE_SUFFIX), ByteBuffers.getBitHint(maxLen, capacity), mode);
        MappedFile indexFile = new MappedFileImpl(new File(directory, column + INDEX_FILE_SUFFIX), ByteBuffers.getBitHint(8, capacity), mode);

        this.data = new VarcharColumn(dataFile, indexFile, maxLen);
        this.size = size;

        this.index = new SymbolIndex(new File(directory, column + HASH_INDEX_FILE_SUFFIX), capacity, capacity * HASH_GROUPING_RATE, mode, indexTxAddress);
        this.valueCache = new TObjectIntHashMap<>(capacity, CACHE_LOAD_FACTOR, VALUE_NOT_FOUND);
        this.keyCache = new ArrayList<>(capacity);
    }

    public void applyTx(int size, long indexTxAddress) {
        this.size = size;
        this.index.setTxAddress(indexTxAddress);
    }

    public void alignSize() {
        this.size = (int) data.size();
    }

    public int put(String value) throws JournalException {

        int key = getQuick(value);
        if (key == VALUE_NOT_FOUND) {
            data.putString(value);
            data.commit();
            key = (int) (data.size() - 1);
            index.put(hashKey(value), key);
            size++;
            cache(key, value);
        }

        return key;
    }

    public int getQuick(String value) {
        int key = value == null ? VALUE_IS_NULL : this.valueCache.get(value);

        if (key != VALUE_NOT_FOUND) {
            return key;
        }

        int hashKey = hashKey(value);

        if (!index.contains(hashKey)) {
            return VALUE_NOT_FOUND;
        }


        LongArrayList values = index.getValues(hashKey);
        for (int i = 0; i < values.size(); i++) {
            key = (int) values.get(i);
            String s = data.getString(key);
            if (value.equals(s)) {
                cache(key, value);
                return key;
            }
        }

        return VALUE_NOT_FOUND;
    }

    public int get(String value) {
        int result = getQuick(value);
        if (result == VALUE_NOT_FOUND) {
            throw new JournalInvalidSymbolValueException("Invalid value %s for symbol %s", value, column);
        } else {
            return result;
        }
    }

    public boolean valueExists(String value) {
        return getQuick(value) != VALUE_NOT_FOUND;
    }

    public String value(int key) {
        if (key >= size) {
            throw new JournalRuntimeException("Invalid symbol key: " + key);
        }
        String value = key < keyCache.size() ? keyCache.get(key) : null;
        if (value == null) {
            value = data.getString(key);
            cache(key, value);
        }
        return value;
    }

    public Iterable<String> values() {

        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {

                    private long current = 0;
                    private final long size = SymbolTable.this.size();


                    @Override
                    public boolean hasNext() {
                        return current < size;
                    }

                    @Override
                    public String next() {
                        return data.getString(current++);
                    }

                    @Override
                    public void remove() {
                        throw new JournalImmutableIteratorException();
                    }
                };
            }
        };
    }

    public int size() {
        return size;
    }

    public void close() {
        if (data != null) {
            data.close();
        }
        if (index != null) {
            index.close();
        }
        index = null;
        data = null;
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

    public void updateIndex(int oldSize, int newSize) throws JournalException {
        if (oldSize < newSize) {
            for (int i = oldSize; i < newSize; i++) {
                index.put(hashKey(data.getString(i)), i);
            }
        }
    }

    public VarcharColumn getDataColumn() {
        return data;
    }

    public SymbolTable preLoad() {
        for (int key = 0, size = (int) data.size(); key < size; key++) {
            String value = data.getString(key);
            valueCache.putIfAbsent(value, key);
            keyCache.add(value);

        }
        return this;
    }

    public long getIndexTxAddress() {
        return index.getTxAddress();
    }

    public void commit() {
        data.commit();
        index.commit();
    }

    private void cache(int key, String value) {
        valueCache.put(value, key);
        Lists.advance(keyCache, key);
        keyCache.set(key, value);
    }

    private void clearCache() {
        valueCache.clear();
        keyCache.clear();
    }

    private int hashKey(String value) {
        return value == null ? 0 : (value.hashCode() & 0x7fffffff) % capacity;
    }
}
