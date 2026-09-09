/*+*****************************************************************************
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

package io.questdb.test.cutlass.qwp;

import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpArrayColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpNullBitmap;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpSymbolColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.protocol.QwpTimestampColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.cutlass.qwp.server.QwpStreamingDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_DOUBLE_ARRAY;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_SYMBOL;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_TIMESTAMP;

public class QwpParserCacheRetentionTest {

    private static final long ARRAY_CACHE_BASELINE_BYTES = 64L * (Long.BYTES + 2L * Integer.BYTES);
    private static final long GORILLA_CACHE_BASELINE_BYTES = 1024L * Long.BYTES;
    private static final long SYMBOL_INDEX_CACHE_BASELINE_BYTES = 16L * Integer.BYTES;

    @Test
    public void testAllNullArrayDoesNotGrowRowCaches() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int rowCount = 1_000_000;
            final int dataLength = 1 + QwpNullBitmap.sizeInBytes(rowCount);
            long address = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(address, (byte) 1);
                Unsafe.setMemory(address + 1, dataLength - 1, (byte) 0xFF);
                QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
                Assert.assertEquals(dataLength, cursor.of(address, dataLength, rowCount, TYPE_DOUBLE_ARRAY));
                Assert.assertEquals(64, cursor.getRowCacheCapacity());
                Assert.assertEquals(ARRAY_CACHE_BASELINE_BYTES, cursor.getRowCacheBytes());
                for (int i = 0; i < rowCount; i++) {
                    Assert.assertTrue(cursor.advanceRow());
                    Assert.assertTrue(cursor.isNull());
                }
            } finally {
                Unsafe.free(address, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testArrayPreflightAcceptsExactLowerBound() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int rowCount = 65;
            final int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            final int dataLength = 1 + bitmapSize + 5;
            long address = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(address, (byte) 1);
                Unsafe.setMemory(address + 1, bitmapSize, (byte) 0xFF);
                QwpNullBitmapTestUtil.clearNull(address + 1, 64);
                Unsafe.putByte(address + 1 + bitmapSize, (byte) 1);
                Unsafe.putInt(address + 2 + bitmapSize, 0);

                QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
                Assert.assertEquals(dataLength, cursor.of(address, dataLength, rowCount, TYPE_DOUBLE_ARRAY));
                Assert.assertTrue(cursor.getRowCacheCapacity() >= rowCount);
                for (int i = 0; i < 64; i++) {
                    Assert.assertTrue(cursor.advanceRow());
                    Assert.assertTrue(cursor.isNull());
                }
                Assert.assertFalse(cursor.advanceRow());
                Assert.assertFalse(cursor.isNull());
                Assert.assertEquals(1, cursor.getNDims());
                Assert.assertEquals(0, cursor.getDimSize(0));
                Assert.assertEquals(0, cursor.getTotalElements());
            } finally {
                Unsafe.free(address, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testArrayRejectsMissingNullBitmapFlagBeforeGrowingRowCaches() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
            QwpParseException exception = Assert.assertThrows(QwpParseException.class, () ->
                    cursor.of(0, 0, 1_000_000, TYPE_DOUBLE_ARRAY));
            assertInsufficientData(exception, "expected null bitmap flag");
            Assert.assertEquals(64, cursor.getRowCacheCapacity());
            Assert.assertEquals(ARRAY_CACHE_BASELINE_BYTES, cursor.getRowCacheBytes());
        });
    }

    @Test
    public void testArrayRejectsOneByteBelowGrowthLowerBound() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int rowCount = 65;
            final int rowDataLength = rowCount * 5 - 1;
            final int dataLength = 1 + rowDataLength;
            long address = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(address, (byte) 0);
                Unsafe.setMemory(address + 1, rowDataLength, (byte) 0);

                QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
                QwpParseException exception = Assert.assertThrows(QwpParseException.class, () ->
                        cursor.of(address, dataLength, rowCount, TYPE_DOUBLE_ARRAY));
                assertInsufficientData(exception, "65 non-null rows require at least 325 bytes");
                Assert.assertEquals(64, cursor.getRowCacheCapacity());
                Assert.assertEquals(ARRAY_CACHE_BASELINE_BYTES, cursor.getRowCacheBytes());
            } finally {
                Unsafe.free(address, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testArrayRejectsTinyPayloadBeforeGrowingRowCaches() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long address = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(address, (byte) 0);
                QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
                QwpParseException exception = Assert.assertThrows(QwpParseException.class, () ->
                        cursor.of(address, 1, 1_000_000, TYPE_DOUBLE_ARRAY));
                assertInsufficientData(exception, "non-null rows require at least");
                Assert.assertEquals(64, cursor.getRowCacheCapacity());
                Assert.assertEquals(ARRAY_CACHE_BASELINE_BYTES, cursor.getRowCacheBytes());
            } finally {
                Unsafe.free(address, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testArrayRejectsTruncatedNullBitmapBeforeGrowingRowCaches() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long address = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(address, (byte) 1);
                QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
                QwpParseException exception = Assert.assertThrows(QwpParseException.class, () ->
                        cursor.of(address, 1, 1_000_000, TYPE_DOUBLE_ARRAY));
                assertInsufficientData(exception, "expected null bitmap");
                Assert.assertEquals(64, cursor.getRowCacheCapacity());
                Assert.assertEquals(ARRAY_CACHE_BASELINE_BYTES, cursor.getRowCacheBytes());
            } finally {
                Unsafe.free(address, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDecoderReusableAfterColdRelease() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpTableBuffer failedBuffer = new QwpTableBuffer("cache_release");
                 QwpTableBuffer validBuffer = new QwpTableBuffer("cache_reuse");
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                QwpTableBuffer.ColumnBuffer failedArray = failedBuffer.getOrCreateColumn("a", TYPE_DOUBLE_ARRAY, true);
                QwpTableBuffer.ColumnBuffer failedSymbol = failedBuffer.getOrCreateColumn("s", TYPE_SYMBOL, true);
                QwpTableBuffer.ColumnBuffer failedTimestamp = failedBuffer.getOrCreateColumn("ts", TYPE_TIMESTAMP, true);
                for (int i = 0; i < 4_097; i++) {
                    failedArray.addDoubleArray(new double[]{i});
                    failedSymbol.addSymbol("symbol_" + i);
                    failedTimestamp.addLong(1_000L * i);
                    failedBuffer.nextRow();
                }

                int failedSize = encoder.encode(failedBuffer);
                long failedAddress = encoder.getBuffer().getBufferPtr();
                Unsafe.putShort(failedAddress + QwpConstants.HEADER_OFFSET_TABLE_COUNT, (short) 2);

                QwpMessageCursor message = decoder.decode(failedAddress, failedSize);
                message.nextTable();
                Assert.assertTrue(
                        message.getRetainedCacheBytes()
                                > ARRAY_CACHE_BASELINE_BYTES
                                + SYMBOL_INDEX_CACHE_BASELINE_BYTES
                                + GORILLA_CACHE_BASELINE_BYTES
                );
                Assert.assertThrows(QwpParseException.class, message::nextTable);
                Assert.assertEquals(
                        ARRAY_CACHE_BASELINE_BYTES
                                + SYMBOL_INDEX_CACHE_BASELINE_BYTES
                                + GORILLA_CACHE_BASELINE_BYTES,
                        message.getRetainedCacheBytes()
                );

                QwpTableBuffer.ColumnBuffer validArray = validBuffer.getOrCreateColumn("a", TYPE_DOUBLE_ARRAY, true);
                QwpTableBuffer.ColumnBuffer validSymbol = validBuffer.getOrCreateColumn("s", TYPE_SYMBOL, true);
                QwpTableBuffer.ColumnBuffer validTimestamp = validBuffer.getOrCreateColumn("ts", TYPE_TIMESTAMP, true);
                double[][] arrays = {{1.25, 2.5}, {-3.75}, {42.0, 43.0, 44.0}};
                String[] symbols = {"alpha", "beta", "gamma"};
                long[] timestamps = {1_000L, 2_000L, 3_000L};
                for (int i = 0; i < arrays.length; i++) {
                    validArray.addDoubleArray(arrays[i]);
                    validSymbol.addSymbol(symbols[i]);
                    validTimestamp.addLong(timestamps[i]);
                    validBuffer.nextRow();
                }

                int validSize = encoder.encode(validBuffer);
                QwpMessageCursor reusedMessage = decoder.decode(encoder.getBuffer().getBufferPtr(), validSize);
                Assert.assertSame(message, reusedMessage);
                QwpTableBlockCursor table = reusedMessage.nextTable();
                QwpTimestampColumnCursor timestampCursor = table.getTimestampColumn(2);
                Assert.assertEquals(1, timestampCursor.getGorillaDecodeCount());
                for (int row = 0; row < arrays.length; row++) {
                    Assert.assertTrue(table.hasNextRow());
                    table.nextRow();

                    QwpArrayColumnCursor arrayCursor = table.getArrayColumn(0);
                    Assert.assertEquals(1, arrayCursor.getNDims());
                    Assert.assertEquals(arrays[row].length, arrayCursor.getDimSize(0));
                    Assert.assertEquals(arrays[row].length, arrayCursor.getTotalElements());
                    for (int element = 0; element < arrays[row].length; element++) {
                        Assert.assertEquals(
                                arrays[row][element],
                                Unsafe.getDouble(arrayCursor.getValuesAddress() + (long) element * Double.BYTES),
                                0.0
                        );
                    }

                    Assert.assertEquals(symbols[row], table.getSymbolColumn(1).getSymbolCharSequence().toString());
                    Assert.assertEquals(timestamps[row], timestampCursor.getTimestamp());
                }
                Assert.assertFalse(table.hasNextRow());
            }
        });
    }

    @Test
    public void testDeltaSymbolRejectsOneMissingIndexBeforeGrowingIndexCache() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int rowCount = 17;
            final int indexCount = rowCount - 1;
            final int dataLength = 1 + indexCount;
            long address = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(address, dataLength, (byte) 0);
                ObjList<String> connectionDict = new ObjList<>();
                connectionDict.add("only");

                QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
                QwpParseException exception = Assert.assertThrows(QwpParseException.class, () ->
                        cursor.of(address, dataLength, rowCount, connectionDict));
                assertInsufficientData(exception, "17 indices require at least 17 bytes");
                Assert.assertEquals(16, cursor.getDecodedIndexCacheCapacity());
            } finally {
                Unsafe.free(address, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDeltaSymbolRejectsTinyPayloadBeforeGrowingIndexCache() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long address = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(address, (byte) 0);
                ObjList<String> connectionDict = new ObjList<>();
                connectionDict.add("only");
                QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
                QwpParseException exception = Assert.assertThrows(QwpParseException.class, () ->
                        cursor.of(address, 1, 1_000_000, connectionDict));
                assertInsufficientData(exception, "indices require at least");
                Assert.assertEquals(16, cursor.getDecodedIndexCacheCapacity());
            } finally {
                Unsafe.free(address, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testGorillaCacheReleasedAfterLateTableFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("cache_release");
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer.ColumnBuffer timestampColumn = buffer.getOrCreateColumn("ts", TYPE_TIMESTAMP, true);
                for (int i = 0; i < 4_097; i++) {
                    timestampColumn.addLong(1_000L * i);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer);
                long address = encoder.getBuffer().getBufferPtr();
                Unsafe.putShort(address + QwpConstants.HEADER_OFFSET_TABLE_COUNT, (short) 2);

                QwpMessageCursor cursor = new QwpMessageCursor();
                cursor.of(address, size, null);
                cursor.nextTable();
                Assert.assertTrue(cursor.getRetainedCacheBytes() > 1024L * Long.BYTES);
                Assert.assertThrows(QwpParseException.class, cursor::nextTable);
                Assert.assertEquals(GORILLA_CACHE_BASELINE_BYTES, cursor.getRetainedCacheBytes());
            }
        });
    }

    @Test
    public void testGorillaRejectsOneByteBelowGrowthLowerBound() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int rowCount = 1_027;
            final int remainingValues = rowCount - 2;
            final int gorillaDataLength = 128;
            final int dataLength = 1 + 1 + 2 * Long.BYTES + gorillaDataLength;
            long address = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(address, dataLength, (byte) 0);
                Unsafe.putByte(address + 1, QwpTimestampColumnCursor.ENCODING_GORILLA);
                Unsafe.putLong(address + 2, 1_000);
                Unsafe.putLong(address + 10, 2_000);

                QwpTimestampColumnCursor cursor = new QwpTimestampColumnCursor();
                QwpParseException exception = Assert.assertThrows(QwpParseException.class, () ->
                        cursor.of(address, dataLength, rowCount, TYPE_TIMESTAMP, true));
                assertInsufficientData(exception, remainingValues + " Gorilla values require at least 129 bytes");
                Assert.assertEquals(1024, cursor.getGorillaCacheCapacity());
            } finally {
                Unsafe.free(address, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testGorillaRejectsTinyPayloadBeforeGrowingDecodeCache() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long address = Unsafe.malloc(18, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(address, (byte) 0);
                Unsafe.putByte(address + 1, QwpTimestampColumnCursor.ENCODING_GORILLA);
                Unsafe.putLong(address + 2, 1_000);
                Unsafe.putLong(address + 10, 2_000);
                QwpTimestampColumnCursor cursor = new QwpTimestampColumnCursor();
                QwpParseException exception = Assert.assertThrows(QwpParseException.class, () ->
                        cursor.of(address, 18, 1_000_000, TYPE_TIMESTAMP, true));
                assertInsufficientData(exception, "Gorilla values require at least");
                Assert.assertEquals(1024, cursor.getGorillaCacheCapacity());
            } finally {
                Unsafe.free(address, 18, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testLaterColumnFailureReleasesPreviouslyGrownCaches() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("cache_release");
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer.ColumnBuffer arrayColumn = buffer.getOrCreateColumn("a", TYPE_DOUBLE_ARRAY, true);
                QwpTableBuffer.ColumnBuffer symbolColumn = buffer.getOrCreateColumn("s", TYPE_SYMBOL, true);
                for (int i = 0; i < 4_097; i++) {
                    arrayColumn.addDoubleArray(new double[]{i});
                    symbolColumn.addSymbol("symbol_" + i);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer);
                var writer = encoder.getBuffer();
                long address = writer.getBufferPtr();
                Unsafe.putInt(
                        address + QwpConstants.HEADER_OFFSET_PAYLOAD_LENGTH,
                        size - QwpConstants.HEADER_SIZE - 1
                );

                QwpMessageCursor cursor = new QwpMessageCursor();
                cursor.of(address, size - 1, null);
                Assert.assertThrows(QwpParseException.class, cursor::nextTable);
                Assert.assertEquals(
                        ARRAY_CACHE_BASELINE_BYTES + SYMBOL_INDEX_CACHE_BASELINE_BYTES,
                        cursor.getRetainedCacheBytes()
                );
            }
        });
    }

    @Test
    public void testLaterTableFailureReleasesPreviouslyGrownCaches() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("cache_release");
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer.ColumnBuffer arrayColumn = buffer.getOrCreateColumn("a", TYPE_DOUBLE_ARRAY, true);
                for (int i = 0; i < 4_097; i++) {
                    arrayColumn.addDoubleArray(new double[]{i});
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer);
                var writer = encoder.getBuffer();
                long address = writer.getBufferPtr();
                Unsafe.putShort(address + QwpConstants.HEADER_OFFSET_TABLE_COUNT, (short) 2);

                QwpMessageCursor cursor = new QwpMessageCursor();
                cursor.of(address, size, null);
                cursor.nextTable();
                Assert.assertTrue(cursor.getRetainedCacheBytes() > 2_048);
                Assert.assertThrows(QwpParseException.class, cursor::nextTable);
                Assert.assertEquals(ARRAY_CACHE_BASELINE_BYTES, cursor.getRetainedCacheBytes());
            }
        });
    }

    @Test
    public void testStandardSymbolRejectsImpossibleDictionaryBeforeGrowingFlyweights() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int dictionarySize = 500_000;
            final int dataLength = 500_004;
            long address = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                long p = address;
                Unsafe.putByte(p++, (byte) 0);
                p = QwpVarint.encode(p, dictionarySize);
                Assert.assertEquals(address + 4, p);
                Unsafe.setMemory(p, dictionarySize, (byte) 0);
                p += dictionarySize;
                Assert.assertEquals(address + dataLength, p);

                QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
                QwpParseException exception = Assert.assertThrows(QwpParseException.class, () ->
                        cursor.of(address, dataLength, 1));
                assertInsufficientData(exception, "dictionary entries and indices require at least");
                Assert.assertEquals(0, cursor.getDictionaryCacheSize());
            } finally {
                Unsafe.free(address, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testStandardSymbolRejectsMissingIndicesBeforeGrowingIndexCache() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int rowCount = 17;
            final int stringLength = rowCount;
            final int dataLength = 3 + stringLength;
            long address = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                long p = address;
                Unsafe.putByte(p++, (byte) 0);
                p = QwpVarint.encode(p, 1);
                p = QwpVarint.encode(p, stringLength);
                Unsafe.setMemory(p, stringLength, (byte) 'a');
                p += stringLength;
                Assert.assertEquals(address + dataLength, p);

                QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
                QwpParseException exception = Assert.assertThrows(QwpParseException.class, () ->
                        cursor.of(address, dataLength, rowCount));
                assertInsufficientData(exception, "indices require at least");
                Assert.assertEquals(16, cursor.getDecodedIndexCacheCapacity());
            } finally {
                Unsafe.free(address, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSymbolCacheReleasedAfterLateTableFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("cache_release");
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer.ColumnBuffer symbolColumn = buffer.getOrCreateColumn("s", TYPE_SYMBOL, true);
                for (int i = 0; i < 4_097; i++) {
                    symbolColumn.addSymbol("symbol_" + i);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer);
                long address = encoder.getBuffer().getBufferPtr();
                Unsafe.putShort(address + QwpConstants.HEADER_OFFSET_TABLE_COUNT, (short) 2);

                QwpMessageCursor cursor = new QwpMessageCursor();
                cursor.of(address, size, null);
                QwpSymbolColumnCursor symbolCursor = cursor.nextTable().getSymbolColumn(0);
                Object inflatedDictionaryCache = symbolCursor.getDictionaryCacheIdentity();
                Assert.assertTrue(cursor.getRetainedCacheBytes() > SYMBOL_INDEX_CACHE_BASELINE_BYTES);
                Assert.assertThrows(QwpParseException.class, cursor::nextTable);
                Assert.assertNotSame(inflatedDictionaryCache, symbolCursor.getDictionaryCacheIdentity());
                Assert.assertEquals(SYMBOL_INDEX_CACHE_BASELINE_BYTES, cursor.getRetainedCacheBytes());
            }
        });
    }

    private static void assertInsufficientData(QwpParseException exception, String messageFragment) {
        Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, exception.getErrorCode());
        Assert.assertTrue(exception.getFlyweightMessage().toString().contains(messageFragment));
    }
}
