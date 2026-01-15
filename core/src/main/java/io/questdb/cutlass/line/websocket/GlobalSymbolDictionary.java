/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cutlass.line.websocket;

import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.ObjList;

/**
 * Global symbol dictionary that maps symbol strings to sequential integer IDs.
 * <p>
 * This dictionary is shared across all tables and columns within a client instance.
 * IDs are assigned sequentially starting from 0, ensuring contiguous ID space.
 * <p>
 * Thread safety: This class is NOT thread-safe. External synchronization is required
 * if accessed from multiple threads.
 */
public class GlobalSymbolDictionary {

    private final CharSequenceIntHashMap symbolToId;
    private final ObjList<String> idToSymbol;

    public GlobalSymbolDictionary() {
        this(64); // Default initial capacity
    }

    public GlobalSymbolDictionary(int initialCapacity) {
        this.symbolToId = new CharSequenceIntHashMap(initialCapacity);
        this.idToSymbol = new ObjList<>(initialCapacity);
    }

    /**
     * Gets or adds a symbol to the dictionary.
     * <p>
     * If the symbol already exists, returns its existing ID.
     * If the symbol is new, assigns the next sequential ID and returns it.
     *
     * @param symbol the symbol string (must not be null)
     * @return the global ID for this symbol (>= 0)
     * @throws IllegalArgumentException if symbol is null
     */
    public int getOrAddSymbol(String symbol) {
        if (symbol == null) {
            throw new IllegalArgumentException("symbol cannot be null");
        }

        int existingId = symbolToId.get(symbol);
        if (existingId != CharSequenceIntHashMap.NO_ENTRY_VALUE) {
            return existingId;
        }

        // Assign new ID
        int newId = idToSymbol.size();
        symbolToId.put(symbol, newId);
        idToSymbol.add(symbol);
        return newId;
    }

    /**
     * Gets the symbol string for a given ID.
     *
     * @param id the symbol ID
     * @return the symbol string
     * @throws IndexOutOfBoundsException if id is out of range
     */
    public String getSymbol(int id) {
        if (id < 0 || id >= idToSymbol.size()) {
            throw new IndexOutOfBoundsException("Invalid symbol ID: " + id + ", dictionary size: " + idToSymbol.size());
        }
        return idToSymbol.getQuick(id);
    }

    /**
     * Gets the ID for an existing symbol, or -1 if not found.
     *
     * @param symbol the symbol string
     * @return the symbol ID, or -1 if not in dictionary
     */
    public int getId(String symbol) {
        if (symbol == null) {
            return -1;
        }
        int id = symbolToId.get(symbol);
        return id == CharSequenceIntHashMap.NO_ENTRY_VALUE ? -1 : id;
    }

    /**
     * Returns the number of symbols in the dictionary.
     *
     * @return dictionary size
     */
    public int size() {
        return idToSymbol.size();
    }

    /**
     * Checks if the dictionary is empty.
     *
     * @return true if no symbols have been added
     */
    public boolean isEmpty() {
        return idToSymbol.size() == 0;
    }

    /**
     * Checks if the dictionary contains the given symbol.
     *
     * @param symbol the symbol to check
     * @return true if the symbol exists in the dictionary
     */
    public boolean contains(String symbol) {
        return symbol != null && symbolToId.get(symbol) != CharSequenceIntHashMap.NO_ENTRY_VALUE;
    }

    /**
     * Gets the symbols in the given ID range [fromId, toId).
     * <p>
     * This is used to extract the delta for sending to the server.
     * The range is inclusive of fromId and exclusive of toId.
     *
     * @param fromId start ID (inclusive)
     * @param toId   end ID (exclusive)
     * @return array of symbols in the range, or empty array if range is invalid/empty
     */
    public String[] getSymbolsInRange(int fromId, int toId) {
        if (fromId < 0 || toId < fromId || fromId >= idToSymbol.size()) {
            return new String[0];
        }

        int actualToId = Math.min(toId, idToSymbol.size());
        int count = actualToId - fromId;
        if (count <= 0) {
            return new String[0];
        }

        String[] result = new String[count];
        for (int i = 0; i < count; i++) {
            result[i] = idToSymbol.getQuick(fromId + i);
        }
        return result;
    }

    /**
     * Clears all symbols from the dictionary.
     * <p>
     * After clearing, the next symbol added will get ID 0.
     */
    public void clear() {
        symbolToId.clear();
        idToSymbol.clear();
    }
}
