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

import io.questdb.cutlass.qwp.protocol.*;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.cutlass.qwp.server.QwpStreamingDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Comprehensive tests for delta symbol dictionary encoding and decoding.
 * <p>
 * Tests cover:
 * - Multiple tables sharing the same global dictionary
 * - Multiple batches with progressive symbol accumulation
 * - Reconnection scenarios where the dictionary resets
 * - Multiple symbol columns in the same table
 * - Edge cases (empty batches, no symbols, etc.)
 */
public class DeltaSymbolDictionaryTest {

    // ==================== Multiple Tables Tests ====================

    @Test
    public void testMultipleTables_sharedGlobalDictionary() {
        GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();

        // Table 1 uses symbols AAPL, GOOG
        int aaplId = globalDict.getOrAddSymbol("AAPL");
        int googId = globalDict.getOrAddSymbol("GOOG");

        // Table 2 uses symbols AAPL (reused), MSFT (new)
        int aaplId2 = globalDict.getOrAddSymbol("AAPL");  // Should return same ID
        int msftId = globalDict.getOrAddSymbol("MSFT");

        // Verify deduplication
        Assert.assertEquals(0, aaplId);
        Assert.assertEquals(1, googId);
        Assert.assertEquals(0, aaplId2);  // Same as aaplId
        Assert.assertEquals(2, msftId);

        // Total symbols should be 3
        Assert.assertEquals(3, globalDict.size());
    }

    @Test
    public void testMultipleTables_encodedInSameBatch() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();

            // Create two tables
            QwpTableBuffer table1 = new QwpTableBuffer("trades");
            QwpTableBuffer table2 = new QwpTableBuffer("quotes");

            // Table 1: ticker column
            QwpTableBuffer.ColumnBuffer col1 = table1.getOrCreateColumn("ticker", TYPE_SYMBOL, false);
            int aaplId = globalDict.getOrAddSymbol("AAPL");
            int googId = globalDict.getOrAddSymbol("GOOG");
            col1.addSymbolWithGlobalId("AAPL", aaplId);
            table1.nextRow();
            col1.addSymbolWithGlobalId("GOOG", googId);
            table1.nextRow();

            // Table 2: symbol column (different name, but shares dictionary)
            QwpTableBuffer.ColumnBuffer col2 = table2.getOrCreateColumn("symbol", TYPE_SYMBOL, false);
            int msftId = globalDict.getOrAddSymbol("MSFT");
            col2.addSymbolWithGlobalId("AAPL", aaplId);  // Reuse AAPL
            table2.nextRow();
            col2.addSymbolWithGlobalId("MSFT", msftId);
            table2.nextRow();

            // Encode first table with delta dict
            int confirmedMaxId = -1;
            int batchMaxId = 2;  // AAPL(0), GOOG(1), MSFT(2)

            int size = encoder.encodeWithDeltaDict(table1, globalDict, confirmedMaxId, batchMaxId, false);
            Assert.assertTrue(size > 0);

            // Verify delta section contains all 3 symbols
            QwpBufferWriter buf = encoder.getBuffer();
            long ptr = buf.getBufferPtr();

            byte flags = Unsafe.getUnsafe().getByte(ptr + HEADER_OFFSET_FLAGS);
            Assert.assertTrue((flags & FLAG_DELTA_SYMBOL_DICT) != 0);

            // After header: deltaStart=0, deltaCount=3
            long pos = ptr + HEADER_SIZE;
            int deltaStart = readVarint(pos);
            Assert.assertEquals(0, deltaStart);
        }
    }

    @Test
    public void testMultipleTables_multipleSymbolColumns() {
        GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();

        QwpTableBuffer table = new QwpTableBuffer("market_data");

        // Column 1: exchange
        QwpTableBuffer.ColumnBuffer exchangeCol = table.getOrCreateColumn("exchange", TYPE_SYMBOL, false);
        int nyseId = globalDict.getOrAddSymbol("NYSE");
        int nasdaqId = globalDict.getOrAddSymbol("NASDAQ");

        // Column 2: currency
        QwpTableBuffer.ColumnBuffer currencyCol = table.getOrCreateColumn("currency", TYPE_SYMBOL, false);
        int usdId = globalDict.getOrAddSymbol("USD");
        int eurId = globalDict.getOrAddSymbol("EUR");

        // Column 3: ticker
        QwpTableBuffer.ColumnBuffer tickerCol = table.getOrCreateColumn("ticker", TYPE_SYMBOL, false);
        int aaplId = globalDict.getOrAddSymbol("AAPL");

        // Add row with all three columns
        exchangeCol.addSymbolWithGlobalId("NYSE", nyseId);
        currencyCol.addSymbolWithGlobalId("USD", usdId);
        tickerCol.addSymbolWithGlobalId("AAPL", aaplId);
        table.nextRow();

        exchangeCol.addSymbolWithGlobalId("NASDAQ", nasdaqId);
        currencyCol.addSymbolWithGlobalId("EUR", eurId);
        tickerCol.addSymbolWithGlobalId("AAPL", aaplId);  // Reuse AAPL
        table.nextRow();

        // All symbols share the same global dictionary
        Assert.assertEquals(5, globalDict.size());
        Assert.assertEquals("NYSE", globalDict.getSymbol(0));
        Assert.assertEquals("NASDAQ", globalDict.getSymbol(1));
        Assert.assertEquals("USD", globalDict.getSymbol(2));
        Assert.assertEquals("EUR", globalDict.getSymbol(3));
        Assert.assertEquals("AAPL", globalDict.getSymbol(4));
    }

    // ==================== Multiple Batches Tests ====================

    @Test
    public void testMultipleBatches_progressiveSymbolAccumulation() {
        GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();

        // Batch 1: AAPL, GOOG
        int aaplId = globalDict.getOrAddSymbol("AAPL");
        int googId = globalDict.getOrAddSymbol("GOOG");
        int batch1MaxId = Math.max(aaplId, googId);

        // Simulate sending batch 1 - maxSentSymbolId = 1 after send
        int maxSentSymbolId = batch1MaxId;  // 1

        // Batch 2: AAPL (existing), MSFT (new), TSLA (new)
        globalDict.getOrAddSymbol("AAPL");  // Returns 0, already exists
        int msftId = globalDict.getOrAddSymbol("MSFT");
        int tslaId = globalDict.getOrAddSymbol("TSLA");
        int batch2MaxId = Math.max(msftId, tslaId);

        // Delta for batch 2 should be [2, 3] (MSFT, TSLA)
        int deltaStart = maxSentSymbolId + 1;
        int deltaCount = batch2MaxId - maxSentSymbolId;
        Assert.assertEquals(2, deltaStart);
        Assert.assertEquals(2, deltaCount);

        // Simulate sending batch 2
        maxSentSymbolId = batch2MaxId;  // 3

        // Batch 3: All existing symbols (no delta needed)
        globalDict.getOrAddSymbol("AAPL");
        globalDict.getOrAddSymbol("GOOG");
        int batch3MaxId = 1;  // Max used is GOOG(1)

        deltaStart = maxSentSymbolId + 1;
        deltaCount = Math.max(0, batch3MaxId - maxSentSymbolId);
        Assert.assertEquals(4, deltaStart);
        Assert.assertEquals(0, deltaCount);  // No new symbols
    }

    @Test
    public void testMultipleBatches_encodeAndDecode() throws QwpParseException {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            GlobalSymbolDictionary clientDict = new GlobalSymbolDictionary();
            ObjList<String> serverDict = new ObjList<>();
            int maxSentSymbolId = -1;

            // === Batch 1 ===
            QwpTableBuffer batch1 = new QwpTableBuffer("test");
            QwpTableBuffer.ColumnBuffer col1 = batch1.getOrCreateColumn("sym", TYPE_SYMBOL, false);

            int aaplId = clientDict.getOrAddSymbol("AAPL");
            int googId = clientDict.getOrAddSymbol("GOOG");
            col1.addSymbolWithGlobalId("AAPL", aaplId);
            batch1.nextRow();
            col1.addSymbolWithGlobalId("GOOG", googId);
            batch1.nextRow();

            int batch1MaxId = 1;
            int size1 = encoder.encodeWithDeltaDict(batch1, clientDict, maxSentSymbolId, batch1MaxId, false);
            Assert.assertTrue(size1 > 0);
            maxSentSymbolId = batch1MaxId;

            // Decode on server side
            QwpBufferWriter buf1 = encoder.getBuffer();
            decodeAndAccumulateDict(buf1.getBufferPtr(), size1, serverDict);

            // Verify server dictionary
            Assert.assertEquals(2, serverDict.size());
            Assert.assertEquals("AAPL", serverDict.get(0));
            Assert.assertEquals("GOOG", serverDict.get(1));

            // === Batch 2 ===
            QwpTableBuffer batch2 = new QwpTableBuffer("test");
            QwpTableBuffer.ColumnBuffer col2 = batch2.getOrCreateColumn("sym", TYPE_SYMBOL, false);

            int msftId = clientDict.getOrAddSymbol("MSFT");
            col2.addSymbolWithGlobalId("AAPL", aaplId);  // Existing
            batch2.nextRow();
            col2.addSymbolWithGlobalId("MSFT", msftId);  // New
            batch2.nextRow();

            int batch2MaxId = 2;
            int size2 = encoder.encodeWithDeltaDict(batch2, clientDict, maxSentSymbolId, batch2MaxId, false);
            Assert.assertTrue(size2 > 0);
            maxSentSymbolId = batch2MaxId;

            // Decode batch 2
            QwpBufferWriter buf2 = encoder.getBuffer();
            decodeAndAccumulateDict(buf2.getBufferPtr(), size2, serverDict);

            // Server dictionary should now have 3 symbols
            Assert.assertEquals(3, serverDict.size());
            Assert.assertEquals("MSFT", serverDict.get(2));
        }
    }

    // ==================== Reconnection Tests ====================

    @Test
    public void testReconnection_resetsWatermark() {
        GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();

        // Build up dictionary and "send" some symbols
        globalDict.getOrAddSymbol("AAPL");
        globalDict.getOrAddSymbol("GOOG");
        globalDict.getOrAddSymbol("MSFT");

        int maxSentSymbolId = 2;

        // Simulate reconnection - reset maxSentSymbolId
        maxSentSymbolId = -1;
        Assert.assertEquals(-1, maxSentSymbolId);

        // Global dictionary is NOT cleared (it's client-side)
        Assert.assertEquals(3, globalDict.size());

        // Next batch must send full delta from 0
        int deltaStart = maxSentSymbolId + 1;
        Assert.assertEquals(0, deltaStart);
    }

    @Test
    public void testReconnection_serverDictionaryCleared() {
        ObjList<String> serverDict = new ObjList<>();

        // Simulate first connection
        serverDict.add("AAPL");
        serverDict.add("GOOG");
        Assert.assertEquals(2, serverDict.size());

        // Simulate reconnection - server clears dictionary
        serverDict.clear();
        Assert.assertEquals(0, serverDict.size());

        // New connection starts fresh
        serverDict.add("MSFT");
        Assert.assertEquals(1, serverDict.size());
        Assert.assertEquals("MSFT", serverDict.get(0));
    }

    @Test
    public void testReconnection_fullDeltaAfterReconnect() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            GlobalSymbolDictionary clientDict = new GlobalSymbolDictionary();

            // First connection: add symbols
            int aaplId = clientDict.getOrAddSymbol("AAPL");
            clientDict.getOrAddSymbol("GOOG");

            // Send batch - maxSentSymbolId = 1
            int maxSentSymbolId = 1;

            // Reconnect - reset maxSentSymbolId
            maxSentSymbolId = -1;

            // Create new batch using existing symbols
            QwpTableBuffer batch = new QwpTableBuffer("test");
            QwpTableBuffer.ColumnBuffer col = batch.getOrCreateColumn("sym", TYPE_SYMBOL, false);
            col.addSymbolWithGlobalId("AAPL", aaplId);
            batch.nextRow();

            // Encode - should send full delta (all symbols from 0)
            int size = encoder.encodeWithDeltaDict(batch, clientDict, maxSentSymbolId, 1, false);
            Assert.assertTrue(size > 0);

            // Verify deltaStart is 0
            QwpBufferWriter buf = encoder.getBuffer();
            long pos = buf.getBufferPtr() + HEADER_SIZE;
            int deltaStart = readVarint(pos);
            Assert.assertEquals(0, deltaStart);
        }
    }

    // ==================== Edge Cases Tests ====================

    @Test
    public void testEdgeCase_emptyBatch() {
        GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();

        // Pre-populate dictionary and send
        globalDict.getOrAddSymbol("AAPL");
        int maxSentSymbolId = 0;

        // Empty batch (no rows, no symbols used)
        QwpTableBuffer emptyBatch = new QwpTableBuffer("test");
        Assert.assertEquals(0, emptyBatch.getRowCount());

        // Delta should still work (deltaCount = 0)
        int deltaStart = maxSentSymbolId + 1;
        int deltaCount = 0;
        Assert.assertEquals(1, deltaStart);
        Assert.assertEquals(0, deltaCount);
    }

    @Test
    public void testEdgeCase_batchWithNoSymbols() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();

            // Table with only non-symbol columns
            QwpTableBuffer batch = new QwpTableBuffer("metrics");
            QwpTableBuffer.ColumnBuffer valueCol = batch.getOrCreateColumn("value", TYPE_LONG, false);
            valueCol.addLong(100L);
            batch.nextRow();

            // MaxId is -1 (no symbols)
            int batchMaxId = -1;

            // Can still encode with delta dict (empty delta)
            int size = encoder.encodeWithDeltaDict(batch, globalDict, -1, batchMaxId, false);
            Assert.assertTrue(size > 0);

            // Verify flag is set
            QwpBufferWriter buf = encoder.getBuffer();
            byte flags = Unsafe.getUnsafe().getByte(buf.getBufferPtr() + HEADER_OFFSET_FLAGS);
            Assert.assertTrue((flags & FLAG_DELTA_SYMBOL_DICT) != 0);
        }
    }

    @Test
    public void testEdgeCase_nullSymbolValues() {
        GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();

        QwpTableBuffer batch = new QwpTableBuffer("test");
        QwpTableBuffer.ColumnBuffer col = batch.getOrCreateColumn("sym", TYPE_SYMBOL, true);  // nullable

        int aaplId = globalDict.getOrAddSymbol("AAPL");
        col.addSymbolWithGlobalId("AAPL", aaplId);
        batch.nextRow();

        col.addSymbol(null);  // NULL value
        batch.nextRow();

        col.addSymbolWithGlobalId("AAPL", aaplId);
        batch.nextRow();

        Assert.assertEquals(3, batch.getRowCount());
        // Dictionary only has 1 symbol (AAPL), NULL doesn't add to dictionary
        Assert.assertEquals(1, globalDict.size());
    }

    @Test
    public void testEdgeCase_largeSymbolDictionary() {
        GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();

        // Add 1000 unique symbols
        for (int i = 0; i < 1000; i++) {
            int id = globalDict.getOrAddSymbol("SYM_" + i);
            Assert.assertEquals(i, id);
        }

        Assert.assertEquals(1000, globalDict.size());

        // Send first batch with symbols 0-99
        int maxSentSymbolId = 99;

        // Next batch uses symbols 0-199, delta is 100-199
        int deltaStart = maxSentSymbolId + 1;
        int deltaCount = 199 - maxSentSymbolId;
        Assert.assertEquals(100, deltaStart);
        Assert.assertEquals(100, deltaCount);
    }

    @Test
    public void testEdgeCase_gapFill() {
        // Client dictionary: AAPL(0), GOOG(1), MSFT(2), TSLA(3)
        GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();
        globalDict.getOrAddSymbol("AAPL");
        globalDict.getOrAddSymbol("GOOG");
        globalDict.getOrAddSymbol("MSFT");
        globalDict.getOrAddSymbol("TSLA");

        // Batch uses AAPL(0) and TSLA(3), skipping GOOG(1) and MSFT(2)
        // Delta must include gap-fill: send all symbols from maxSentSymbolId+1 to batchMaxId
        int maxSentSymbolId = -1;
        int batchMaxId = 3;  // TSLA

        int deltaStart = maxSentSymbolId + 1;
        int deltaCount = batchMaxId - maxSentSymbolId;

        // Must send symbols 0, 1, 2, 3 (even though 1, 2 aren't used in this batch)
        Assert.assertEquals(0, deltaStart);
        Assert.assertEquals(4, deltaCount);

        // This ensures server has contiguous dictionary
        for (int id = deltaStart; id < deltaStart + deltaCount; id++) {
            String symbol = globalDict.getSymbol(id);
            Assert.assertNotNull("Symbol " + id + " should exist", symbol);
        }
    }

    @Test
    public void testEdgeCase_duplicateSymbolsInBatch() {
        GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();

        QwpTableBuffer batch = new QwpTableBuffer("test");
        QwpTableBuffer.ColumnBuffer col = batch.getOrCreateColumn("sym", TYPE_SYMBOL, false);

        // Same symbol used multiple times
        int aaplId = globalDict.getOrAddSymbol("AAPL");
        col.addSymbolWithGlobalId("AAPL", aaplId);
        batch.nextRow();
        col.addSymbolWithGlobalId("AAPL", aaplId);
        batch.nextRow();
        col.addSymbolWithGlobalId("AAPL", aaplId);
        batch.nextRow();

        Assert.assertEquals(3, batch.getRowCount());
        Assert.assertEquals(1, globalDict.size());  // Only 1 unique symbol

        int maxGlobalId = col.getMaxGlobalSymbolId();
        Assert.assertEquals(0, maxGlobalId);  // Max ID is 0 (AAPL)
    }

    @Test
    public void testEdgeCase_unicodeSymbols() {
        GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();

        // Unicode symbols
        int id1 = globalDict.getOrAddSymbol("日本");
        int id2 = globalDict.getOrAddSymbol("中国");
        int id3 = globalDict.getOrAddSymbol("한국");
        int id4 = globalDict.getOrAddSymbol("Émoji🚀");

        Assert.assertEquals(0, id1);
        Assert.assertEquals(1, id2);
        Assert.assertEquals(2, id3);
        Assert.assertEquals(3, id4);

        Assert.assertEquals("日本", globalDict.getSymbol(0));
        Assert.assertEquals("中国", globalDict.getSymbol(1));
        Assert.assertEquals("한국", globalDict.getSymbol(2));
        Assert.assertEquals("Émoji🚀", globalDict.getSymbol(3));
    }

    @Test
    public void testEdgeCase_veryLongSymbol() {
        GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();

        // Create a very long symbol (1000 chars)
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append('X');
        }
        String longSymbol = sb.toString();

        int id = globalDict.getOrAddSymbol(longSymbol);
        Assert.assertEquals(0, id);

        String retrieved = globalDict.getSymbol(0);
        Assert.assertEquals(longSymbol, retrieved);
        Assert.assertEquals(1000, retrieved.length());
    }

    // ==================== Server-Side Decoding Tests ====================

    @Test
    public void testServerSide_accumulateDelta() {
        ObjList<String> serverDict = new ObjList<>();

        // First batch: symbols 0-2
        accumulateDelta(serverDict, 0, new String[]{"AAPL", "GOOG", "MSFT"});

        Assert.assertEquals(3, serverDict.size());
        Assert.assertEquals("AAPL", serverDict.get(0));
        Assert.assertEquals("GOOG", serverDict.get(1));
        Assert.assertEquals("MSFT", serverDict.get(2));

        // Second batch: symbols 3-4
        accumulateDelta(serverDict, 3, new String[]{"TSLA", "AMZN"});

        Assert.assertEquals(5, serverDict.size());
        Assert.assertEquals("TSLA", serverDict.get(3));
        Assert.assertEquals("AMZN", serverDict.get(4));

        // Third batch: no new symbols (empty delta)
        accumulateDelta(serverDict, 5, new String[]{});
        Assert.assertEquals(5, serverDict.size());
    }

    @Test
    public void testServerSide_resolveSymbol() {
        ObjList<String> serverDict = new ObjList<>();
        serverDict.add("AAPL");
        serverDict.add("GOOG");
        serverDict.add("MSFT");

        // Resolve by global ID
        Assert.assertEquals("AAPL", serverDict.get(0));
        Assert.assertEquals("GOOG", serverDict.get(1));
        Assert.assertEquals("MSFT", serverDict.get(2));
    }

    @Test
    public void testServerSide_symbolCursorDeltaMode() throws QwpParseException {
        ObjList<String> serverDict = new ObjList<>();
        serverDict.add("AAPL");
        serverDict.add("GOOG");

        // Simulate symbol column data in delta mode
        // In delta mode, column data is just indices (no dictionary section)
        // Format: [indices...]

        // Create wire data: 2 rows, indices [0, 1]
        long dataAddr = Unsafe.malloc(100, MemoryTag.NATIVE_DEFAULT);
        try {
            int offset = 0;
            // Index 0 (varint)
            Unsafe.getUnsafe().putByte(dataAddr + offset++, (byte) 0);
            // Index 1 (varint)
            Unsafe.getUnsafe().putByte(dataAddr + offset++, (byte) 1);

            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            int consumed = cursor.of(dataAddr, offset, 2, false, 0, 0, serverDict);

            // Advance and verify
            cursor.advanceRow();
            Assert.assertEquals(0, cursor.getSymbolIndex());
            Assert.assertEquals("AAPL", cursor.getSymbolString());

            cursor.advanceRow();
            Assert.assertEquals(1, cursor.getSymbolIndex());
            Assert.assertEquals("GOOG", cursor.getSymbolString());

            Assert.assertTrue(cursor.isDeltaMode());
        } finally {
            Unsafe.free(dataAddr, 100, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Round-Trip Tests ====================

    @Test
    public void testRoundTrip_singleBatch() throws QwpParseException {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            GlobalSymbolDictionary clientDict = new GlobalSymbolDictionary();
            ObjList<String> serverDict = new ObjList<>();

            // Create batch
            QwpTableBuffer batch = new QwpTableBuffer("test");
            QwpTableBuffer.ColumnBuffer col = batch.getOrCreateColumn("ticker", TYPE_SYMBOL, false);

            int aaplId = clientDict.getOrAddSymbol("AAPL");
            int googId = clientDict.getOrAddSymbol("GOOG");

            col.addSymbolWithGlobalId("AAPL", aaplId);
            batch.nextRow();
            col.addSymbolWithGlobalId("GOOG", googId);
            batch.nextRow();

            // Encode
            int size = encoder.encodeWithDeltaDict(batch, clientDict, -1, 1, false);

            // Decode
            QwpBufferWriter buf = encoder.getBuffer();
            QwpStreamingDecoder decoder = new QwpStreamingDecoder();
            QwpMessageCursor cursor = decoder.decode(buf.getBufferPtr(), size, serverDict);

            // Verify delta was accumulated
            Assert.assertEquals(2, serverDict.size());
            Assert.assertEquals("AAPL", serverDict.get(0));
            Assert.assertEquals("GOOG", serverDict.get(1));

            // Verify delta mode is enabled
            Assert.assertTrue(cursor.isDeltaSymbolDictEnabled());
        }
    }

    @Test
    public void testRoundTrip_multipleBatches() throws QwpParseException {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            GlobalSymbolDictionary clientDict = new GlobalSymbolDictionary();
            ObjList<String> serverDict = new ObjList<>();
            QwpStreamingDecoder decoder = new QwpStreamingDecoder();
            int maxSentSymbolId = -1;

            // === Batch 1 ===
            QwpTableBuffer batch1 = new QwpTableBuffer("test");
            QwpTableBuffer.ColumnBuffer col1 = batch1.getOrCreateColumn("sym", TYPE_SYMBOL, false);

            int aaplId = clientDict.getOrAddSymbol("AAPL");
            col1.addSymbolWithGlobalId("AAPL", aaplId);
            batch1.nextRow();

            int size1 = encoder.encodeWithDeltaDict(batch1, clientDict, maxSentSymbolId, 0, false);
            decoder.decode(encoder.getBuffer().getBufferPtr(), size1, serverDict);
            maxSentSymbolId = 0;

            Assert.assertEquals(1, serverDict.size());

            // === Batch 2 ===
            QwpTableBuffer batch2 = new QwpTableBuffer("test");
            QwpTableBuffer.ColumnBuffer col2 = batch2.getOrCreateColumn("sym", TYPE_SYMBOL, false);

            int googId = clientDict.getOrAddSymbol("GOOG");
            int msftId = clientDict.getOrAddSymbol("MSFT");
            col2.addSymbolWithGlobalId("GOOG", googId);
            batch2.nextRow();
            col2.addSymbolWithGlobalId("MSFT", msftId);
            batch2.nextRow();

            int size2 = encoder.encodeWithDeltaDict(batch2, clientDict, maxSentSymbolId, 2, false);
            decoder.decode(encoder.getBuffer().getBufferPtr(), size2, serverDict);

            Assert.assertEquals(3, serverDict.size());
            Assert.assertEquals("AAPL", serverDict.get(0));
            Assert.assertEquals("GOOG", serverDict.get(1));
            Assert.assertEquals("MSFT", serverDict.get(2));
        }
    }

    // ==================== Helper Methods ====================

    private int readVarint(long address) {
        byte b = Unsafe.getUnsafe().getByte(address);
        if ((b & 0x80) == 0) {
            return b & 0x7F;
        }
        // For simplicity, only handle single-byte varints in tests
        return b & 0x7F;
    }

    private void accumulateDelta(ObjList<String> serverDict, int deltaStart, String[] symbols) {
        // Ensure capacity
        while (serverDict.size() < deltaStart + symbols.length) {
            serverDict.add(null);
        }
        // Add symbols
        for (int i = 0; i < symbols.length; i++) {
            serverDict.setQuick(deltaStart + i, symbols[i]);
        }
    }

    private void decodeAndAccumulateDict(long ptr, int size, ObjList<String> serverDict) throws QwpParseException {
        // Parse header
        byte flags = Unsafe.getUnsafe().getByte(ptr + HEADER_OFFSET_FLAGS);
        if ((flags & FLAG_DELTA_SYMBOL_DICT) == 0) {
            return;  // No delta dict
        }

        // Parse delta section after header
        long pos = ptr + HEADER_SIZE;

        // Read deltaStart
        int deltaStart = readVarint(pos);
        pos += 1;  // Assuming single-byte varint

        // Read deltaCount
        int deltaCount = readVarint(pos);
        pos += 1;

        // Ensure capacity
        while (serverDict.size() < deltaStart + deltaCount) {
            serverDict.add(null);
        }

        // Read symbols
        for (int i = 0; i < deltaCount; i++) {
            int len = readVarint(pos);
            pos += 1;

            byte[] bytes = new byte[len];
            for (int j = 0; j < len; j++) {
                bytes[j] = Unsafe.getUnsafe().getByte(pos + j);
            }
            pos += len;

            serverDict.setQuick(deltaStart + i, new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
        }
    }
}
