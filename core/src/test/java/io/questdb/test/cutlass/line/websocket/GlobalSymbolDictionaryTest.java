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

package io.questdb.test.cutlass.line.websocket;

import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import org.junit.Test;

import static org.junit.Assert.*;

public class GlobalSymbolDictionaryTest {

    @Test
    public void testAddSymbol_assignsSequentialIds() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        assertEquals(0, dict.getOrAddSymbol("AAPL"));
        assertEquals(1, dict.getOrAddSymbol("GOOG"));
        assertEquals(2, dict.getOrAddSymbol("MSFT"));
        assertEquals(3, dict.getOrAddSymbol("TSLA"));

        assertEquals(4, dict.size());
    }

    @Test
    public void testAddSymbol_deduplicatesSameSymbol() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        int id1 = dict.getOrAddSymbol("AAPL");
        int id2 = dict.getOrAddSymbol("AAPL");
        int id3 = dict.getOrAddSymbol("AAPL");

        assertEquals(id1, id2);
        assertEquals(id2, id3);
        assertEquals(0, id1);
        assertEquals(1, dict.size());
    }

    @Test
    public void testClear() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        dict.getOrAddSymbol("AAPL");
        dict.getOrAddSymbol("GOOG");
        assertEquals(2, dict.size());

        dict.clear();

        assertTrue(dict.isEmpty());
        assertEquals(0, dict.size());
        assertFalse(dict.contains("AAPL"));
    }

    @Test
    public void testClear_thenAddRestartsFromZero() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        dict.getOrAddSymbol("AAPL");
        dict.getOrAddSymbol("GOOG");
        dict.clear();

        // New IDs should start from 0
        assertEquals(0, dict.getOrAddSymbol("MSFT"));
        assertEquals(1, dict.getOrAddSymbol("TSLA"));
    }

    @Test
    public void testContains() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        assertFalse(dict.contains("AAPL"));

        dict.getOrAddSymbol("AAPL");
        dict.getOrAddSymbol("GOOG");

        assertTrue(dict.contains("AAPL"));
        assertTrue(dict.contains("GOOG"));
        assertFalse(dict.contains("MSFT"));
        assertFalse(dict.contains(null));
    }

    @Test
    public void testCustomInitialCapacity() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary(1024);

        // Should work normally
        for (int i = 0; i < 100; i++) {
            assertEquals(i, dict.getOrAddSymbol("SYM_" + i));
        }
        assertEquals(100, dict.size());
    }

    @Test
    public void testGetId_returnsCorrectId() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        dict.getOrAddSymbol("AAPL");
        dict.getOrAddSymbol("GOOG");
        dict.getOrAddSymbol("MSFT");

        assertEquals(0, dict.getId("AAPL"));
        assertEquals(1, dict.getId("GOOG"));
        assertEquals(2, dict.getId("MSFT"));
    }

    @Test
    public void testGetId_returnsMinusOneForNull() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
        assertEquals(-1, dict.getId(null));
    }

    @Test
    public void testGetId_returnsMinusOneForUnknown() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
        dict.getOrAddSymbol("AAPL");

        assertEquals(-1, dict.getId("GOOG"));
        assertEquals(-1, dict.getId("UNKNOWN"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetOrAddSymbol_throwsForNull() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
        dict.getOrAddSymbol(null);
    }

    @Test
    public void testGetSymbol_returnsCorrectSymbol() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        dict.getOrAddSymbol("AAPL");
        dict.getOrAddSymbol("GOOG");
        dict.getOrAddSymbol("MSFT");

        assertEquals("AAPL", dict.getSymbol(0));
        assertEquals("GOOG", dict.getSymbol(1));
        assertEquals("MSFT", dict.getSymbol(2));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetSymbol_throwsForInvalidId() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
        dict.getOrAddSymbol("AAPL");
        dict.getSymbol(1); // Only id 0 exists
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetSymbol_throwsForNegativeId() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
        dict.getOrAddSymbol("AAPL");
        dict.getSymbol(-1);
    }

    @Test
    public void testIsEmpty() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        assertTrue(dict.isEmpty());

        dict.getOrAddSymbol("AAPL");
        assertFalse(dict.isEmpty());
    }

    @Test
    public void testLargeNumberOfSymbols() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        // Add 10000 symbols
        for (int i = 0; i < 10000; i++) {
            assertEquals(i, dict.getOrAddSymbol("SYMBOL_" + i));
        }

        assertEquals(10000, dict.size());

        // Verify retrieval
        for (int i = 0; i < 10000; i++) {
            assertEquals("SYMBOL_" + i, dict.getSymbol(i));
            assertEquals(i, dict.getId("SYMBOL_" + i));
        }
    }

    @Test
    public void testMixedSymbolsAcrossTables() {
        // Simulates symbols from multiple tables sharing the dictionary
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        // Table "trades": exchange column
        int nyse = dict.getOrAddSymbol("NYSE");      // 0
        int nasdaq = dict.getOrAddSymbol("NASDAQ");  // 1

        // Table "prices": currency column
        int usd = dict.getOrAddSymbol("USD");        // 2
        int eur = dict.getOrAddSymbol("EUR");        // 3

        // Table "orders": exchange column (reuses)
        int nyse2 = dict.getOrAddSymbol("NYSE");     // Still 0

        assertEquals(nyse, nyse2);
        assertEquals(4, dict.size());

        // All symbols accessible
        assertEquals("NYSE", dict.getSymbol(nyse));
        assertEquals("NASDAQ", dict.getSymbol(nasdaq));
        assertEquals("USD", dict.getSymbol(usd));
        assertEquals("EUR", dict.getSymbol(eur));
    }

    @Test
    public void testSpecialCharactersInSymbols() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        dict.getOrAddSymbol("");           // Empty string
        dict.getOrAddSymbol(" ");          // Space
        dict.getOrAddSymbol("a b c");      // With spaces
        dict.getOrAddSymbol("AAPL\u0000"); // With null char
        dict.getOrAddSymbol("\u00E9");     // Unicode
        dict.getOrAddSymbol("\uD83D\uDE00"); // Emoji

        assertEquals(6, dict.size());

        assertEquals("", dict.getSymbol(0));
        assertEquals(" ", dict.getSymbol(1));
        assertEquals("a b c", dict.getSymbol(2));
        assertEquals("AAPL\u0000", dict.getSymbol(3));
        assertEquals("\u00E9", dict.getSymbol(4));
        assertEquals("\uD83D\uDE00", dict.getSymbol(5));
    }
}
